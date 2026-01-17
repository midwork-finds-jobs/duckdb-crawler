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
#include "robots_parser.hpp"
#include "http_client.hpp"
#include "link_parser.hpp"
#include "rust_ffi.hpp"

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

// Bind data for streaming crawl
struct CrawlStreamBindData : public TableFunctionData {
    vector<string> urls;
    string source_query;  // Alternative: query to execute for URLs
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
    std::atomic<bool> finished{false};
    std::atomic<int> active_workers{0};

    void Push(BatchCrawlEntry entry) {
        std::lock_guard<std::mutex> lock(mutex);
        results.push(std::move(entry));
        cv.notify_one();
    }

    bool TryPop(BatchCrawlEntry &entry, std::chrono::milliseconds timeout) {
        std::unique_lock<std::mutex> lock(mutex);
        if (!cv.wait_for(lock, timeout, [this] {
            return !results.empty() || (finished.load() && active_workers.load() == 0);
        })) {
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
    std::atomic<bool> should_stop{false};
    std::atomic<idx_t> next_url_idx{0};
    bool workers_started = false;
    bool query_executed = false;
    std::mutex start_mutex;

    idx_t MaxThreads() const override {
        return 1; // Only one thread reads results
    }
};

// Worker function for streaming crawl
static void StreamCrawlWorker(
    CrawlStreamBindData &bind_data,
    CrawlStreamGlobalState &global_state,
    int worker_id
) {
    global_state.result_queue->active_workers.fetch_add(1);

    ThreadSafeDomainMap domain_states;

    while (!global_state.should_stop.load()) {
        // Get next URL to process
        idx_t url_idx = global_state.next_url_idx.fetch_add(1);
        if (url_idx >= bind_data.urls.size()) {
            break;
        }

        const string &url = bind_data.urls[url_idx];
        string domain = ExtractDomain(url);

        // Check robots.txt if needed
        bool robots_allow = true;
        if (bind_data.respect_robots_txt) {
            auto &domain_state = domain_states.GetOrCreate(domain);
            std::lock_guard<std::mutex> lock(domain_state.mutex);

            if (!domain_state.robots_fetched) {
                string robots_url = "https://" + domain + "/robots.txt";
                RetryConfig retry_config;
                auto response = HttpClient::Fetch(robots_url, retry_config, bind_data.user_agent);
                if (response.success) {
                    auto robots_data = RobotsParser::Parse(response.body);
                    domain_state.rules = RobotsParser::GetRulesForUserAgent(robots_data, bind_data.user_agent);
                }
                domain_state.robots_fetched = true;
            }

            robots_allow = RobotsParser::IsAllowed(domain_state.rules, ExtractPath(url));
        }

        if (!robots_allow) {
            continue;
        }

        // Fetch the URL
        RetryConfig retry_config;
        retry_config.max_retries = 2;
        // Note: timeout is controlled via global HttpSettings, not RetryConfig

        auto response = HttpClient::Fetch(url, retry_config, bind_data.user_agent);

        // Build result entry
        BatchCrawlEntry entry;
        entry.url = url;
        entry.status_code = response.status_code;
        entry.content_type = response.content_type;
        entry.body = response.body;
        entry.error = response.error;
        entry.elapsed_ms = 0;  // Not tracked in simple fetch
        entry.final_url = response.final_url;
        entry.redirect_count = response.redirect_count;

        // Extract structured data using Rust if successful
        if (response.success && !response.body.empty()) {
            bool is_html = (response.content_type.find("text/html") != string::npos ||
                           response.content_type.find("application/xhtml") != string::npos);
            if (is_html) {
                entry.jsonld = ExtractJsonLdWithRust(response.body);
                entry.opengraph = ExtractOpenGraphWithRust(response.body);
                // meta extracted via opengraph (contains meta tags)
            }
        }

        // Push result to queue
        global_state.result_queue->Push(std::move(entry));

        // Respect crawl delay
        if (bind_data.crawl_delay > 0) {
            std::this_thread::sleep_for(
                std::chrono::milliseconds(static_cast<int>(bind_data.crawl_delay * 1000)));
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
        LogicalType::VARCHAR,  // url
        LogicalType::INTEGER,  // status_code
        LogicalType::VARCHAR,  // content_type
        LogicalType::VARCHAR,  // body
        LogicalType::VARCHAR,  // error
        LogicalType::BIGINT,   // response_time_ms
        LogicalType::BIGINT,   // content_length
        LogicalType::VARCHAR,  // jsonld
        LogicalType::VARCHAR,  // opengraph
        LogicalType::VARCHAR,  // meta
    };

    names = {"url", "status_code", "content_type", "body", "error",
             "response_time_ms", "content_length", "jsonld", "opengraph", "meta"};

    return std::move(bind_data);
}

// Bind function for query-based crawl (accepts a SQL query string)
static unique_ptr<FunctionData> CrawlStreamBindQuery(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<string> &names) {
    auto bind_data = make_uniq<CrawlStreamBindData>();

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
        LogicalType::VARCHAR,  // url
        LogicalType::INTEGER,  // status_code
        LogicalType::VARCHAR,  // content_type
        LogicalType::VARCHAR,  // body
        LogicalType::VARCHAR,  // error
        LogicalType::BIGINT,   // response_time_ms
        LogicalType::BIGINT,   // content_length
        LogicalType::VARCHAR,  // jsonld
        LogicalType::VARCHAR,  // opengraph
        LogicalType::VARCHAR,  // meta
    };

    names = {"url", "status_code", "content_type", "body", "error",
             "response_time_ms", "content_length", "jsonld", "opengraph", "meta"};

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
            if (num_workers < 1) num_workers = 1;
            for (int i = 0; i < num_workers; i++) {
                global_state.workers.emplace_back(StreamCrawlWorker,
                    std::ref(bind_data), std::ref(global_state), i);
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
    TableFunction list_func("crawl_stream",
                            {LogicalType::LIST(LogicalType::VARCHAR)},
                            CrawlStreamFunction, CrawlStreamBind, CrawlStreamInitGlobal);
    list_func.named_parameters["user_agent"] = LogicalType::VARCHAR;
    list_func.named_parameters["crawl_delay"] = LogicalType::DOUBLE;
    list_func.named_parameters["timeout"] = LogicalType::INTEGER;
    list_func.named_parameters["respect_robots_txt"] = LogicalType::BOOLEAN;

    // Version 2: Accept query string
    TableFunction query_func("crawl_stream",
                             {LogicalType::VARCHAR},
                             CrawlStreamFunction, CrawlStreamBindQuery, CrawlStreamInitGlobal);
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
