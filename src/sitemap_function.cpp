// Sitemap table function for DuckDB Crawler
// Fetches and parses XML sitemaps

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "rust_ffi.hpp"
#include "yyjson.hpp"

namespace duckdb {

using namespace duckdb_yyjson;

//===--------------------------------------------------------------------===//
// Bind Data
//===--------------------------------------------------------------------===//

struct SitemapBindData : public TableFunctionData {
	string url;
	bool recursive = true;
	int max_depth = 5;
	bool discover_from_robots = false;
	string user_agent = "DuckDB-Crawler/1.0";
	int timeout_ms = 30000;
	string filter_pattern;
};

//===--------------------------------------------------------------------===//
// Global State
//===--------------------------------------------------------------------===//

struct SitemapEntry {
	string url;
	string lastmod;
	string changefreq;
	double priority;
	bool has_priority;
};

struct SitemapGlobalState : public GlobalTableFunctionState {
	vector<SitemapEntry> entries;
	idx_t current_idx = 0;
	bool fetched = false;

	idx_t MaxThreads() const override {
		return 1;
	}
};

//===--------------------------------------------------------------------===//
// Helper: Build request JSON
//===--------------------------------------------------------------------===//

static string BuildSitemapRequest(const SitemapBindData &bind_data) {
	yyjson_mut_doc *doc = yyjson_mut_doc_new(nullptr);
	if (!doc)
		return "{}";

	yyjson_mut_val *root = yyjson_mut_obj(doc);
	yyjson_mut_doc_set_root(doc, root);

	yyjson_mut_obj_add_strcpy(doc, root, "url", bind_data.url.c_str());
	yyjson_mut_obj_add_bool(doc, root, "recursive", bind_data.recursive);
	yyjson_mut_obj_add_uint(doc, root, "max_depth", bind_data.max_depth);
	yyjson_mut_obj_add_bool(doc, root, "discover_from_robots", bind_data.discover_from_robots);
	yyjson_mut_obj_add_strcpy(doc, root, "user_agent", bind_data.user_agent.c_str());
	yyjson_mut_obj_add_uint(doc, root, "timeout_ms", bind_data.timeout_ms);

	size_t len = 0;
	char *json_str = yyjson_mut_write(doc, 0, &len);
	yyjson_mut_doc_free(doc);

	if (!json_str)
		return "{}";
	string result(json_str, len);
	free(json_str);
	return result;
}

//===--------------------------------------------------------------------===//
// Helper: Parse response JSON
//===--------------------------------------------------------------------===//

static vector<SitemapEntry> ParseSitemapResponse(const string &json, const string &filter_pattern) {
	vector<SitemapEntry> entries;

	yyjson_doc *doc = yyjson_read(json.c_str(), json.length(), 0);
	if (!doc)
		return entries;

	yyjson_val *root = yyjson_doc_get_root(doc);
	yyjson_val *urls = yyjson_obj_get(root, "urls");

	if (urls && yyjson_is_arr(urls)) {
		size_t arr_size = yyjson_arr_size(urls);
		entries.reserve(arr_size);

		// Note: yyjson_arr_foreach macro causes stack corruption with DuckDB's allocator
		// Use simple for loop with yyjson_arr_get instead
		for (size_t i = 0; i < arr_size; i++) {
			yyjson_val *entry = yyjson_arr_get(urls, i);
			if (!entry)
				continue;

			SitemapEntry se;

			yyjson_val *url_val = yyjson_obj_get(entry, "url");
			if (url_val && yyjson_is_str(url_val)) {
				se.url = yyjson_get_str(url_val);
			} else {
				continue;
			}

			yyjson_val *lastmod_val = yyjson_obj_get(entry, "lastmod");
			if (lastmod_val && yyjson_is_str(lastmod_val)) {
				se.lastmod = yyjson_get_str(lastmod_val);
			}

			yyjson_val *changefreq_val = yyjson_obj_get(entry, "changefreq");
			if (changefreq_val && yyjson_is_str(changefreq_val)) {
				se.changefreq = yyjson_get_str(changefreq_val);
			}

			yyjson_val *priority_val = yyjson_obj_get(entry, "priority");
			if (priority_val && yyjson_is_num(priority_val)) {
				se.priority = yyjson_get_num(priority_val);
				se.has_priority = true;
			} else {
				se.priority = 0.5;
				se.has_priority = false;
			}

			entries.push_back(std::move(se));
		}
	}

	yyjson_doc_free(doc);
	return entries;
}

//===--------------------------------------------------------------------===//
// Bind Function
//===--------------------------------------------------------------------===//

static unique_ptr<FunctionData> SitemapBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<string> &names) {
	auto bind_data = make_uniq<SitemapBindData>();

	// First argument is the sitemap URL
	if (!input.inputs.empty() && !input.inputs[0].IsNull()) {
		bind_data->url = StringValue::Get(input.inputs[0]);
	} else {
		throw BinderException("sitemap() requires a URL argument");
	}

	// Named parameters
	for (auto &kv : input.named_parameters) {
		if (kv.first == "recursive") {
			bind_data->recursive = kv.second.GetValue<bool>();
		} else if (kv.first == "max_depth") {
			bind_data->max_depth = kv.second.GetValue<int>();
		} else if (kv.first == "discover") {
			bind_data->discover_from_robots = kv.second.GetValue<bool>();
		} else if (kv.first == "user_agent") {
			bind_data->user_agent = StringValue::Get(kv.second);
		} else if (kv.first == "timeout") {
			bind_data->timeout_ms = kv.second.GetValue<int>() * 1000;
		} else if (kv.first == "filter") {
			bind_data->filter_pattern = StringValue::Get(kv.second);
		}
	}

	// Return columns
	return_types.push_back(LogicalType::VARCHAR); // url
	names.push_back("url");

	return_types.push_back(LogicalType::VARCHAR); // lastmod
	names.push_back("lastmod");

	return_types.push_back(LogicalType::VARCHAR); // changefreq
	names.push_back("changefreq");

	return_types.push_back(LogicalType::DOUBLE); // priority
	names.push_back("priority");

	return std::move(bind_data);
}

//===--------------------------------------------------------------------===//
// Init Global
//===--------------------------------------------------------------------===//

static unique_ptr<GlobalTableFunctionState> SitemapInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<SitemapGlobalState>();
}

//===--------------------------------------------------------------------===//
// Table Function
//===--------------------------------------------------------------------===//

static void SitemapFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<SitemapBindData>();
	auto &state = data.global_state->Cast<SitemapGlobalState>();

	// Fetch sitemap on first call
	if (!state.fetched) {
		string request_json = BuildSitemapRequest(bind_data);
		string response_json = FetchSitemapWithRust(request_json);
		state.entries = ParseSitemapResponse(response_json, bind_data.filter_pattern);
		state.fetched = true;
	}

	// Return results
	idx_t count = 0;
	while (count < STANDARD_VECTOR_SIZE && state.current_idx < state.entries.size()) {
		const auto &entry = state.entries[state.current_idx++];

		output.SetValue(0, count, Value(entry.url));
		output.SetValue(1, count, entry.lastmod.empty() ? Value() : Value(entry.lastmod));
		output.SetValue(2, count, entry.changefreq.empty() ? Value() : Value(entry.changefreq));
		output.SetValue(3, count, entry.has_priority ? Value(entry.priority) : Value());

		count++;
	}

	output.SetCardinality(count);
}

//===--------------------------------------------------------------------===//
// Register Function
//===--------------------------------------------------------------------===//

void RegisterSitemapFunction(ExtensionLoader &loader) {
	TableFunction sitemap_func("sitemap", {LogicalType::VARCHAR}, SitemapFunction, SitemapBind, SitemapInitGlobal);

	// Named parameters
	sitemap_func.named_parameters["recursive"] = LogicalType::BOOLEAN;
	sitemap_func.named_parameters["max_depth"] = LogicalType::INTEGER;
	sitemap_func.named_parameters["discover"] = LogicalType::BOOLEAN;
	sitemap_func.named_parameters["user_agent"] = LogicalType::VARCHAR;
	sitemap_func.named_parameters["timeout"] = LogicalType::INTEGER;
	sitemap_func.named_parameters["filter"] = LogicalType::VARCHAR;

	loader.RegisterFunction(sitemap_func);
}

} // namespace duckdb
