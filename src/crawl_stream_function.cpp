// Streaming CRAWL table function for use in FROM clause
//
// Usage:
//   SELECT * FROM crawl_stream(['https://example.com', 'https://test.com'])
//   SELECT * FROM crawl_stream(['url1', 'url2'], user_agent := 'Bot/1.0')
//
// Returns rows as they are crawled (streaming), not blocking until all complete.

#include "crawl_stream_function.hpp"
#include "crawler_internal.hpp"
#include "crawler_utils.hpp"
#include "thread_utils.hpp"
#include "link_parser.hpp"
#include "rust_ffi.hpp"
#include "yyjson.hpp"

#include "duckdb/function/table_function.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/connection.hpp"

#include <thread>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <atomic>

namespace duckdb {

using namespace duckdb_yyjson;

// Build robots check request JSON
static string BuildRobotsCheckRequest(const string &url, const string &user_agent) {
	yyjson_mut_doc *doc = yyjson_mut_doc_new(nullptr);
	if (!doc)
		return "{}";

	yyjson_mut_val *root = yyjson_mut_obj(doc);
	yyjson_mut_doc_set_root(doc, root);
	yyjson_mut_obj_add_strcpy(doc, root, "url", url.c_str());
	yyjson_mut_obj_add_strcpy(doc, root, "user_agent", user_agent.c_str());

	size_t len = 0;
	char *json_str = yyjson_mut_write(doc, 0, &len);
	yyjson_mut_doc_free(doc);
	if (!json_str)
		return "{}";

	string result(json_str, len);
	free(json_str);
	return result;
}

// Parse robots check response
static bool ParseRobotsCheckResponse(const string &response_json, const string &path) {
	yyjson_doc *doc = yyjson_read(response_json.c_str(), response_json.size(), 0);
	if (!doc)
		return true; // Allow on error

	yyjson_val *root = yyjson_doc_get_root(doc);
	yyjson_val *allowed = yyjson_obj_get(root, "allowed");

	bool result = true;
	if (allowed && yyjson_is_bool(allowed)) {
		result = yyjson_get_bool(allowed);
	}

	yyjson_doc_free(doc);
	return result;
}

// Build batch crawl request JSON (for single URL)
static string BuildStreamCrawlRequest(const string &url, const string &user_agent, int timeout_ms) {
	yyjson_mut_doc *doc = yyjson_mut_doc_new(nullptr);
	if (!doc)
		return "{}";

	yyjson_mut_val *root = yyjson_mut_obj(doc);
	yyjson_mut_doc_set_root(doc, root);

	// URLs array
	yyjson_mut_val *urls_arr = yyjson_mut_arr(doc);
	yyjson_mut_arr_add_strcpy(doc, urls_arr, url.c_str());
	yyjson_mut_obj_add_val(doc, root, "urls", urls_arr);

	// Options
	yyjson_mut_obj_add_strcpy(doc, root, "user_agent", user_agent.c_str());
	yyjson_mut_obj_add_uint(doc, root, "timeout_ms", timeout_ms);
	yyjson_mut_obj_add_uint(doc, root, "concurrency", 1);
	yyjson_mut_obj_add_uint(doc, root, "delay_ms", 0);
	yyjson_mut_obj_add_bool(doc, root, "respect_robots", false); // Already checked manually

	size_t len = 0;
	char *json_str = yyjson_mut_write(doc, 0, &len);
	yyjson_mut_doc_free(doc);
	if (!json_str)
		return "{}";

	string result(json_str, len);
	free(json_str);
	return result;
}

// Parse stream crawl response (fills BatchCrawlEntry)
static bool ParseStreamCrawlResponse(const string &response_json, BatchCrawlEntry &entry) {
	yyjson_doc *doc = yyjson_read(response_json.c_str(), response_json.size(), 0);
	if (!doc)
		return false;

	yyjson_val *root = yyjson_doc_get_root(doc);

	// Check for error
	yyjson_val *error = yyjson_obj_get(root, "error");
	if (error && yyjson_is_str(error)) {
		entry.error = yyjson_get_str(error);
		yyjson_doc_free(doc);
		return false;
	}

	yyjson_val *results_arr = yyjson_obj_get(root, "results");
	if (!results_arr || !yyjson_is_arr(results_arr) || yyjson_arr_size(results_arr) == 0) {
		yyjson_doc_free(doc);
		return false;
	}

	yyjson_val *item = yyjson_arr_get_first(results_arr);

	// Parse fields
	yyjson_val *status_val = yyjson_obj_get(item, "status");
	if (status_val && yyjson_is_int(status_val)) {
		entry.status_code = (int)yyjson_get_int(status_val);
	}

	yyjson_val *ct_val = yyjson_obj_get(item, "content_type");
	if (ct_val && yyjson_is_str(ct_val)) {
		entry.content_type = yyjson_get_str(ct_val);
	}

	yyjson_val *body_val = yyjson_obj_get(item, "body");
	if (body_val && yyjson_is_str(body_val)) {
		entry.body = yyjson_get_str(body_val);
	}

	yyjson_val *error_val = yyjson_obj_get(item, "error");
	if (error_val && yyjson_is_str(error_val)) {
		entry.error = yyjson_get_str(error_val);
	}

	yyjson_val *time_val = yyjson_obj_get(item, "response_time_ms");
	if (time_val && yyjson_is_int(time_val)) {
		entry.elapsed_ms = yyjson_get_int(time_val);
	}

	yyjson_val *final_url_val = yyjson_obj_get(item, "final_url");
	if (final_url_val && yyjson_is_str(final_url_val)) {
		entry.final_url = yyjson_get_str(final_url_val);
	}

	yyjson_val *redirect_val = yyjson_obj_get(item, "redirect_count");
	if (redirect_val && yyjson_is_int(redirect_val)) {
		entry.redirect_count = (int)yyjson_get_int(redirect_val);
	}

	yyjson_doc_free(doc);
	return true;
}

// Bind data for streaming crawl
struct CrawlStreamBindData : public TableFunctionData {
	vector<string> urls;
	string source_query; // Alternative: query to execute for URLs
	string user_agent = "DuckDB-Crawler/1.0";
	double crawl_delay = 0.2;
	int timeout_seconds = 30;
	bool respect_robots_txt = false;
};

// Thread-safe result queue
struct StreamResultQueue {
	std::queue<BatchCrawlEntry> results;
	std::mutex mutex;
	std::condition_variable cv;
	std::atomic<bool> finished {false};
	std::atomic<int> active_workers {0};

	void Push(BatchCrawlEntry entry) {
		std::lock_guard<std::mutex> lock(mutex);
		results.push(std::move(entry));
		cv.notify_one();
	}

	bool TryPop(BatchCrawlEntry &entry, std::chrono::milliseconds timeout) {
		std::unique_lock<std::mutex> lock(mutex);
		if (!cv.wait_for(lock, timeout,
		                 [this] { return !results.empty() || (finished.load() && active_workers.load() == 0); })) {
			return false;
		}
		if (results.empty()) {
			return false;
		}
		entry = std::move(results.front());
		results.pop();
		return true;
	}

	bool IsComplete() const {
		return finished.load() && active_workers.load() == 0;
	}
};

// Global state for streaming crawl
struct CrawlStreamGlobalState : public GlobalTableFunctionState {
	std::unique_ptr<StreamResultQueue> result_queue;
	std::vector<std::thread> workers;
	std::atomic<bool> should_stop {false};
	std::atomic<idx_t> next_url_idx {0};
	bool workers_started = false;
	bool query_executed = false;
	std::mutex start_mutex;

	idx_t MaxThreads() const override {
		return 1; // Only one thread reads results
	}
};

// Worker function for streaming crawl
static void StreamCrawlWorker(CrawlStreamBindData &bind_data, CrawlStreamGlobalState &global_state, int worker_id) {
	(void)worker_id; // Unused
	global_state.result_queue->active_workers.fetch_add(1);

	// Cache robots.txt results per domain
	std::map<string, bool> robots_cache;
	std::mutex robots_mutex;

	while (!global_state.should_stop.load()) {
		// Get next URL to process
		idx_t url_idx = global_state.next_url_idx.fetch_add(1);
		if (url_idx >= bind_data.urls.size()) {
			break;
		}

		const string &url = bind_data.urls[url_idx];
		string domain = ExtractDomain(url);
		string path = ExtractPath(url);

		// Check robots.txt if needed
		bool robots_allow = true;
		if (bind_data.respect_robots_txt) {
			std::lock_guard<std::mutex> lock(robots_mutex);
			auto it = robots_cache.find(url);
			if (it != robots_cache.end()) {
				robots_allow = it->second;
			} else {
				// Check with Rust
				string robots_request = BuildRobotsCheckRequest(url, bind_data.user_agent);
				string robots_response = CheckRobotsWithRust(robots_request);
				robots_allow = ParseRobotsCheckResponse(robots_response, path);
				robots_cache[url] = robots_allow;
			}
		}

		if (!robots_allow) {
			continue;
		}

		// Fetch the URL using Rust
		string request_json = BuildStreamCrawlRequest(url, bind_data.user_agent, bind_data.timeout_seconds * 1000);
		string response_json = CrawlBatchWithRust(request_json);

		// Build result entry
		BatchCrawlEntry entry;
		entry.url = url;
		ParseStreamCrawlResponse(response_json, entry);

		// Extract structured data using Rust if successful
		if (entry.status_code >= 200 && entry.status_code < 300 && !entry.body.empty()) {
			bool is_html = (entry.content_type.find("text/html") != string::npos ||
			                entry.content_type.find("application/xhtml") != string::npos);
			if (is_html) {
				entry.jsonld = ExtractJsonLdWithRust(entry.body);
				entry.opengraph = ExtractOpenGraphWithRust(entry.body);
			}
		}

		// Push result to queue
		global_state.result_queue->Push(std::move(entry));

		// Respect crawl delay
		if (bind_data.crawl_delay > 0) {
			std::this_thread::sleep_for(std::chrono::milliseconds(static_cast<int>(bind_data.crawl_delay * 1000)));
		}
	}

	global_state.result_queue->active_workers.fetch_sub(1);
	if (global_state.result_queue->active_workers.load() == 0) {
		global_state.result_queue->finished.store(true);
		global_state.result_queue->cv.notify_all();
	}
}

// Bind function
static unique_ptr<FunctionData> CrawlStreamBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
	auto bind_data = make_uniq<CrawlStreamBindData>();

	// Read extension settings as defaults
	Value setting_value;
	if (context.TryGetCurrentSetting("crawler_user_agent", setting_value)) {
		bind_data->user_agent = setting_value.ToString();
	}
	if (context.TryGetCurrentSetting("crawler_default_delay", setting_value)) {
		bind_data->crawl_delay = setting_value.GetValue<double>();
	}
	if (context.TryGetCurrentSetting("crawler_timeout_ms", setting_value)) {
		bind_data->timeout_seconds = static_cast<int>(setting_value.GetValue<int64_t>() / 1000);
	}
	if (context.TryGetCurrentSetting("crawler_respect_robots", setting_value)) {
		bind_data->respect_robots_txt = setting_value.GetValue<bool>();
	}

	// First argument is list of URLs
	auto &url_list = ListValue::GetChildren(input.inputs[0]);
	for (auto &url_val : url_list) {
		if (!url_val.IsNull()) {
			bind_data->urls.push_back(StringValue::Get(url_val));
		}
	}

	// Named parameters
	for (auto &kv : input.named_parameters) {
		if (kv.first == "user_agent") {
			bind_data->user_agent = StringValue::Get(kv.second);
		} else if (kv.first == "crawl_delay") {
			bind_data->crawl_delay = kv.second.GetValue<double>();
		} else if (kv.first == "timeout") {
			bind_data->timeout_seconds = kv.second.GetValue<int>();
		} else if (kv.first == "respect_robots_txt") {
			bind_data->respect_robots_txt = kv.second.GetValue<bool>();
		}
	}

	// Define return columns
	return_types = {
	    LogicalType::VARCHAR, // url
	    LogicalType::INTEGER, // status_code
	    LogicalType::VARCHAR, // content_type
	    LogicalType::VARCHAR, // body
	    LogicalType::VARCHAR, // error
	    LogicalType::BIGINT,  // response_time_ms
	    LogicalType::BIGINT,  // content_length
	    LogicalType::VARCHAR, // jsonld
	    LogicalType::VARCHAR, // opengraph
	    LogicalType::VARCHAR, // meta
	};

	names = {"url",    "status_code", "content_type", "body", "error", "response_time_ms", "content_length",
	         "jsonld", "opengraph",   "meta"};

	return std::move(bind_data);
}

// Bind function for query-based crawl (accepts a SQL query string)
static unique_ptr<FunctionData> CrawlStreamBindQuery(ClientContext &context, TableFunctionBindInput &input,
                                                     vector<LogicalType> &return_types, vector<string> &names) {
	auto bind_data = make_uniq<CrawlStreamBindData>();

	// Read extension settings as defaults
	Value setting_value;
	if (context.TryGetCurrentSetting("crawler_user_agent", setting_value)) {
		bind_data->user_agent = setting_value.ToString();
	}
	if (context.TryGetCurrentSetting("crawler_default_delay", setting_value)) {
		bind_data->crawl_delay = setting_value.GetValue<double>();
	}
	if (context.TryGetCurrentSetting("crawler_timeout_ms", setting_value)) {
		bind_data->timeout_seconds = static_cast<int>(setting_value.GetValue<int64_t>() / 1000);
	}
	if (context.TryGetCurrentSetting("crawler_respect_robots", setting_value)) {
		bind_data->respect_robots_txt = setting_value.GetValue<bool>();
	}

	// First argument is a query string
	bind_data->source_query = StringValue::Get(input.inputs[0]);

	// Named parameters
	for (auto &kv : input.named_parameters) {
		if (kv.first == "user_agent") {
			bind_data->user_agent = StringValue::Get(kv.second);
		} else if (kv.first == "crawl_delay") {
			bind_data->crawl_delay = kv.second.GetValue<double>();
		} else if (kv.first == "timeout") {
			bind_data->timeout_seconds = kv.second.GetValue<int>();
		} else if (kv.first == "respect_robots_txt") {
			bind_data->respect_robots_txt = kv.second.GetValue<bool>();
		}
	}

	// Define return columns (same as list version)
	return_types = {
	    LogicalType::VARCHAR, // url
	    LogicalType::INTEGER, // status_code
	    LogicalType::VARCHAR, // content_type
	    LogicalType::VARCHAR, // body
	    LogicalType::VARCHAR, // error
	    LogicalType::BIGINT,  // response_time_ms
	    LogicalType::BIGINT,  // content_length
	    LogicalType::VARCHAR, // jsonld
	    LogicalType::VARCHAR, // opengraph
	    LogicalType::VARCHAR, // meta
	};

	names = {"url",    "status_code", "content_type", "body", "error", "response_time_ms", "content_length",
	         "jsonld", "opengraph",   "meta"};

	return std::move(bind_data);
}

// Global state init
static unique_ptr<GlobalTableFunctionState> CrawlStreamInitGlobal(ClientContext &context,
                                                                  TableFunctionInitInput &input) {
	auto state = make_uniq<CrawlStreamGlobalState>();
	state->result_queue = make_uniq<StreamResultQueue>();
	return std::move(state);
}

// Main function - called repeatedly to get results
static void CrawlStreamFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->CastNoConst<CrawlStreamBindData>();
	auto &global_state = data.global_state->Cast<CrawlStreamGlobalState>();

	// Start workers on first call
	{
		std::lock_guard<std::mutex> lock(global_state.start_mutex);

		// Execute source query if provided (and not already done)
		if (!global_state.query_executed && !bind_data.source_query.empty()) {
			global_state.query_executed = true;
			Connection conn(*context.db);
			auto query_result = conn.Query(bind_data.source_query);
			if (query_result->HasError()) {
				throw IOException("crawl_stream source query error: " + query_result->GetError());
			}
			// Collect URLs from first column
			while (auto chunk = query_result->Fetch()) {
				for (idx_t i = 0; i < chunk->size(); i++) {
					auto val = chunk->GetValue(0, i);
					if (!val.IsNull()) {
						bind_data.urls.push_back(val.ToString());
					}
				}
			}
		}

		if (!global_state.workers_started) {
			global_state.workers_started = true;

			// Start worker threads (use 4 workers or fewer if fewer URLs)
			int num_workers = std::min((int)bind_data.urls.size(), 4);
			if (num_workers < 1)
				num_workers = 1;
			for (int i = 0; i < num_workers; i++) {
				global_state.workers.emplace_back(StreamCrawlWorker, std::ref(bind_data), std::ref(global_state), i);
			}
		}
	}

	// Collect results into output chunk
	idx_t count = 0;
	while (count < STANDARD_VECTOR_SIZE) {
		BatchCrawlEntry entry;
		if (global_state.result_queue->TryPop(entry, std::chrono::milliseconds(50))) {
			output.SetValue(0, count, Value(entry.url));
			output.SetValue(1, count, Value(entry.status_code));
			output.SetValue(2, count, Value(entry.content_type));
			output.SetValue(3, count, Value(entry.body));
			output.SetValue(4, count, Value(entry.error));
			output.SetValue(5, count, Value::BIGINT(entry.elapsed_ms));
			output.SetValue(6, count, Value::BIGINT(static_cast<int64_t>(entry.body.size())));
			output.SetValue(7, count, Value(entry.jsonld));
			output.SetValue(8, count, Value(entry.opengraph));
			output.SetValue(9, count, Value(entry.meta));
			count++;
		} else if (global_state.result_queue->IsComplete()) {
			break;
		}
	}

	output.SetCardinality(count);

	// If no more results and workers are done, we're finished
	if (count == 0 && global_state.result_queue->IsComplete()) {
		// Join worker threads
		global_state.should_stop.store(true);
		for (auto &worker : global_state.workers) {
			if (worker.joinable()) {
				worker.join();
			}
		}
	}
}

void RegisterCrawlStreamFunction(ExtensionLoader &loader) {
	// Version 1: Accept list of URLs
	TableFunction list_func("crawl_stream", {LogicalType::LIST(LogicalType::VARCHAR)}, CrawlStreamFunction,
	                        CrawlStreamBind, CrawlStreamInitGlobal);
	list_func.named_parameters["user_agent"] = LogicalType::VARCHAR;
	list_func.named_parameters["crawl_delay"] = LogicalType::DOUBLE;
	list_func.named_parameters["timeout"] = LogicalType::INTEGER;
	list_func.named_parameters["respect_robots_txt"] = LogicalType::BOOLEAN;

	// Version 2: Accept query string
	TableFunction query_func("crawl_stream", {LogicalType::VARCHAR}, CrawlStreamFunction, CrawlStreamBindQuery,
	                         CrawlStreamInitGlobal);
	query_func.named_parameters["user_agent"] = LogicalType::VARCHAR;
	query_func.named_parameters["crawl_delay"] = LogicalType::DOUBLE;
	query_func.named_parameters["timeout"] = LogicalType::INTEGER;
	query_func.named_parameters["respect_robots_txt"] = LogicalType::BOOLEAN;

	// Register both as a function set
	TableFunctionSet crawl_stream_set("crawl_stream");
	crawl_stream_set.AddFunction(list_func);
	crawl_stream_set.AddFunction(query_func);

	loader.RegisterFunction(crawl_stream_set);
}

} // namespace duckdb
