// New crawl() table function - Rust HTTP + extraction
//
// Usage:
//   SELECT url, html.body, html.opengraph->>'title', html.schema->'Product'->>'name'
//   FROM crawl(
//       'SELECT url FROM my_urls',
//       extract = ['title := $("title")', 'price := $(".price")'],
//       state_table = 'crawl_state',
//       user_agent = 'Bot/1.0'
//   )
//
// Or with URL list:
//   SELECT * FROM crawl(['https://example.com'], user_agent = 'Bot/1.0')
//
// The 'html' column is a STRUCT containing:
//   - body: raw HTML content
//   - js: extracted JavaScript variables as JSON
//   - opengraph: OpenGraph meta tags as JSON
//   - schema: combined JSON-LD + microdata as JSON

#include "crawl_table_function.hpp"
#include "crawler_utils.hpp"
#include "rust_ffi.hpp"
#include "yyjson.hpp"

#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/catalog/catalog_transaction.hpp"

#include <set>
#include <map>

namespace duckdb {

using namespace duckdb_yyjson;

// Build batch crawl request JSON for Rust
static string BuildBatchCrawlRequest(const vector<string> &urls, const string &extraction_json,
                                     const string &user_agent, int timeout_ms, int concurrency, int delay_ms,
                                     bool respect_robots, const string &http_proxy = "",
                                     const string &http_proxy_username = "", const string &http_proxy_password = "",
                                     const std::map<string, string> &extra_headers = {}) {
	yyjson_mut_doc *doc = yyjson_mut_doc_new(nullptr);
	if (!doc)
		return "{}";

	yyjson_mut_val *root = yyjson_mut_obj(doc);
	yyjson_mut_doc_set_root(doc, root);

	// URLs array
	yyjson_mut_val *urls_arr = yyjson_mut_arr(doc);
	for (const auto &url : urls) {
		yyjson_mut_arr_add_strcpy(doc, urls_arr, url.c_str());
	}
	yyjson_mut_obj_add_val(doc, root, "urls", urls_arr);

	// Extraction specs (if any)
	if (!extraction_json.empty() && extraction_json != "{}") {
		yyjson_doc *ext_doc = yyjson_read(extraction_json.c_str(), extraction_json.size(), 0);
		if (ext_doc) {
			yyjson_val *ext_root = yyjson_doc_get_root(ext_doc);
			yyjson_mut_val *ext_copy = yyjson_val_mut_copy(doc, ext_root);
			yyjson_mut_obj_add_val(doc, root, "extraction", ext_copy);
			yyjson_doc_free(ext_doc);
		}
	}

	// Options
	yyjson_mut_obj_add_strcpy(doc, root, "user_agent", user_agent.c_str());
	yyjson_mut_obj_add_uint(doc, root, "timeout_ms", timeout_ms);
	yyjson_mut_obj_add_uint(doc, root, "concurrency", concurrency);
	yyjson_mut_obj_add_uint(doc, root, "delay_ms", delay_ms);
	yyjson_mut_obj_add_bool(doc, root, "respect_robots", respect_robots);

	// Proxy settings (from DuckDB http_proxy)
	if (!http_proxy.empty()) {
		yyjson_mut_obj_add_strcpy(doc, root, "http_proxy", http_proxy.c_str());
		if (!http_proxy_username.empty()) {
			yyjson_mut_obj_add_strcpy(doc, root, "http_proxy_username", http_proxy_username.c_str());
		}
		if (!http_proxy_password.empty()) {
			yyjson_mut_obj_add_strcpy(doc, root, "http_proxy_password", http_proxy_password.c_str());
		}
	}

	// Extra headers (from CREATE SECRET)
	if (!extra_headers.empty()) {
		yyjson_mut_val *headers_obj = yyjson_mut_obj(doc);
		for (const auto &kv : extra_headers) {
			yyjson_mut_obj_add_strcpy(doc, headers_obj, kv.first.c_str(), kv.second.c_str());
		}
		yyjson_mut_obj_add_val(doc, root, "extra_headers", headers_obj);
	}

	size_t len = 0;
	char *json_str = yyjson_mut_write(doc, 0, &len);
	yyjson_mut_doc_free(doc);

	if (!json_str)
		return "{}";

	string result_str(json_str, len);
	free(json_str);
	return result_str;
}

//===--------------------------------------------------------------------===//
// HTTP Secret Lookup
//===--------------------------------------------------------------------===//

// Look up HTTP secrets for a URL and populate extra_headers
static void ApplyHttpSecrets(ClientContext &context, const string &url, string &http_proxy, string &http_proxy_username,
                             string &http_proxy_password, std::map<string, string> &extra_headers) {
	auto &secret_manager = SecretManager::Get(context);
	auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);

	// Look up HTTP secret matching the URL
	auto secret_match = secret_manager.LookupSecret(transaction, url, "http");
	if (!secret_match.HasMatch()) {
		return;
	}

	auto &secret_entry = *secret_match.secret_entry;
	auto *kv_secret = dynamic_cast<const KeyValueSecret *>(secret_entry.secret.get());
	if (!kv_secret) {
		return; // Not a KeyValueSecret
	}

	// Get bearer_token and add as Authorization header
	Value bearer_token;
	if (kv_secret->TryGetValue("bearer_token", bearer_token) && !bearer_token.IsNull()) {
		extra_headers["Authorization"] = "Bearer " + bearer_token.ToString();
	}

	// Get extra_http_headers (MAP type)
	Value headers_val;
	if (kv_secret->TryGetValue("extra_http_headers", headers_val) && !headers_val.IsNull()) {
		if (headers_val.type().id() == LogicalTypeId::MAP) {
			auto &entries = MapValue::GetChildren(headers_val);
			for (auto &entry : entries) {
				auto &kv = StructValue::GetChildren(entry);
				if (kv.size() == 2 && !kv[0].IsNull() && !kv[1].IsNull()) {
					extra_headers[kv[0].ToString()] = kv[1].ToString();
				}
			}
		}
	}

	// Get proxy settings from secret (override DuckDB settings)
	Value proxy_val;
	if (kv_secret->TryGetValue("http_proxy", proxy_val) && !proxy_val.IsNull()) {
		http_proxy = proxy_val.ToString();
	}
	if (kv_secret->TryGetValue("http_proxy_username", proxy_val) && !proxy_val.IsNull()) {
		http_proxy_username = proxy_val.ToString();
	}
	if (kv_secret->TryGetValue("http_proxy_password", proxy_val) && !proxy_val.IsNull()) {
		http_proxy_password = proxy_val.ToString();
	}
}

//===--------------------------------------------------------------------===//
// Crawl Result Entry (parsed from Rust response)
//===--------------------------------------------------------------------===//

struct CrawlResultEntry {
	string url;
	int status_code = 0;
	string content_type;
	string body;
	string error;
	string extracted_json;
	int64_t response_time_ms = 0;
	int depth = 1; // Crawl depth (1 = initial URL)
};

// Parse batch crawl response from Rust
static vector<CrawlResultEntry> ParseBatchCrawlResponse(const string &response_json) {
	vector<CrawlResultEntry> results;

	yyjson_doc *doc = yyjson_read(response_json.c_str(), response_json.size(), 0);
	if (!doc)
		return results;

	yyjson_val *root = yyjson_doc_get_root(doc);

	// Check for error
	yyjson_val *error = yyjson_obj_get(root, "error");
	if (error && yyjson_is_str(error)) {
		yyjson_doc_free(doc);
		throw IOException("Rust crawl error: %s", yyjson_get_str(error));
	}

	yyjson_val *results_arr = yyjson_obj_get(root, "results");
	if (!results_arr || !yyjson_is_arr(results_arr)) {
		yyjson_doc_free(doc);
		return results;
	}

	size_t idx = 0;
	size_t max_idx = 0;
	yyjson_val *item;
	yyjson_arr_foreach(results_arr, idx, max_idx, item) {
		CrawlResultEntry entry;

		yyjson_val *url_val = yyjson_obj_get(item, "url");
		if (url_val && yyjson_is_str(url_val)) {
			entry.url = yyjson_get_str(url_val);
		}

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
		if (time_val && yyjson_is_uint(time_val)) {
			entry.response_time_ms = (int64_t)yyjson_get_uint(time_val);
		}

		// Extracted data
		yyjson_val *extracted = yyjson_obj_get(item, "extracted");
		if (extracted && !yyjson_is_null(extracted)) {
			size_t len = 0;
			char *json_str = yyjson_val_write(extracted, 0, &len);
			if (json_str) {
				entry.extracted_json = string(json_str, len);
				free(json_str);
			}
		}

		results.push_back(std::move(entry));
	}

	yyjson_doc_free(doc);
	return results;
}

//===--------------------------------------------------------------------===//
// Helper: Combine JSON-LD and Microdata into schema object
//===--------------------------------------------------------------------===//

static string CombineSchemaData(const string &jsonld, const string &microdata) {
	// Combine JSON-LD and microdata into a single schema object
	// Both are JSON objects keyed by @type, with array values
	yyjson_mut_doc *doc = yyjson_mut_doc_new(nullptr);
	if (!doc)
		return "{}";

	yyjson_mut_val *root = yyjson_mut_obj(doc);
	yyjson_mut_doc_set_root(doc, root);

	// Parse and merge JSON-LD (values are arrays)
	if (!jsonld.empty() && jsonld != "{}") {
		yyjson_doc *jld_doc = yyjson_read(jsonld.c_str(), jsonld.size(), 0);
		if (jld_doc) {
			yyjson_val *jld_root = yyjson_doc_get_root(jld_doc);
			if (yyjson_is_obj(jld_root)) {
				size_t idx, max;
				yyjson_val *key, *val;
				yyjson_obj_foreach(jld_root, idx, max, key, val) {
					yyjson_mut_val *key_copy = yyjson_val_mut_copy(doc, key);
					yyjson_mut_val *val_copy = yyjson_val_mut_copy(doc, val);
					yyjson_mut_obj_add(root, key_copy, val_copy);
				}
			}
			yyjson_doc_free(jld_doc);
		}
	}

	// Parse and merge microdata (values are arrays, merge with existing)
	if (!microdata.empty() && microdata != "{}") {
		yyjson_doc *md_doc = yyjson_read(microdata.c_str(), microdata.size(), 0);
		if (md_doc) {
			yyjson_val *md_root = yyjson_doc_get_root(md_doc);
			if (yyjson_is_obj(md_root)) {
				size_t idx, max;
				yyjson_val *key, *val;
				yyjson_obj_foreach(md_root, idx, max, key, val) {
					const char *key_str = yyjson_get_str(key);
					yyjson_mut_val *existing = yyjson_mut_obj_get(root, key_str);

					if (existing && yyjson_mut_is_arr(existing) && yyjson_is_arr(val)) {
						// Append microdata items to existing JSON-LD array
						size_t arr_idx, arr_max;
						yyjson_val *item;
						yyjson_arr_foreach(val, arr_idx, arr_max, item) {
							yyjson_mut_val *item_copy = yyjson_val_mut_copy(doc, item);
							yyjson_mut_arr_append(existing, item_copy);
						}
					} else if (!existing) {
						// Add new type from microdata
						yyjson_mut_val *key_copy = yyjson_val_mut_copy(doc, key);
						yyjson_mut_val *val_copy = yyjson_val_mut_copy(doc, val);
						yyjson_mut_obj_add(root, key_copy, val_copy);
					}
				}
			}
			yyjson_doc_free(md_doc);
		}
	}

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
// Helper: Build html struct value from response
//===--------------------------------------------------------------------===//

// Helper to create JSON value from string
static Value MakeJsonValue(const string &json_str) {
	if (json_str.empty() || json_str == "{}") {
		return Value(LogicalType::JSON()); // NULL JSON
	}
	return Value(json_str).DefaultCastAs(LogicalType::JSON());
}

// Helper to create MAP(VARCHAR, JSON) from schema JSON object
// Converts {"Product": {...}, "Organization": {...}} to MAP with those entries
static Value MakeSchemaMapValue(const string &schema_json) {
	auto map_type = LogicalType::MAP(LogicalType::VARCHAR, LogicalType::JSON());

	if (schema_json.empty() || schema_json == "{}") {
		return Value::MAP(LogicalType::VARCHAR, LogicalType::JSON(), vector<Value>(), vector<Value>());
	}

	yyjson_doc *doc = yyjson_read(schema_json.c_str(), schema_json.size(), 0);
	if (!doc) {
		return Value::MAP(LogicalType::VARCHAR, LogicalType::JSON(), vector<Value>(), vector<Value>());
	}

	yyjson_val *root = yyjson_doc_get_root(doc);
	if (!yyjson_is_obj(root)) {
		yyjson_doc_free(doc);
		return Value::MAP(LogicalType::VARCHAR, LogicalType::JSON(), vector<Value>(), vector<Value>());
	}

	vector<Value> keys;
	vector<Value> values;

	size_t idx, max;
	yyjson_val *key, *val;
	yyjson_obj_foreach(root, idx, max, key, val) {
		const char *key_str = yyjson_get_str(key);
		if (key_str) {
			keys.push_back(Value(key_str));

			// Serialize value back to JSON string
			size_t len = 0;
			char *val_str = yyjson_val_write(val, 0, &len);
			if (val_str) {
				values.push_back(Value(string(val_str, len)).DefaultCastAs(LogicalType::JSON()));
				free(val_str);
			} else {
				values.push_back(Value(LogicalType::JSON()));
			}
		}
	}

	yyjson_doc_free(doc);
	return Value::MAP(LogicalType::VARCHAR, LogicalType::JSON(), keys, values);
}

static Value BuildHtmlStructValue(const string &body, const string &content_type, const string &url = "") {
	child_list_t<Value> html_values;

	bool is_html =
	    content_type.find("text/html") != string::npos || content_type.find("application/xhtml") != string::npos;

	if (is_html && !body.empty()) {
#if defined(RUST_PARSER_AVAILABLE) && RUST_PARSER_AVAILABLE
		string js_json = ExtractJsWithRust(body);
		string og_json = ExtractOpenGraphWithRust(body);
		string jsonld_json = ExtractJsonLdWithRust(body);
		string microdata_json = ExtractMicrodataWithRust(body);
		string schema_json = CombineSchemaData(jsonld_json, microdata_json);
		string readability_json = ExtractReadabilityWithRust(body, url);

		html_values.push_back(make_pair("document", Value(body)));
		html_values.push_back(make_pair("js", MakeJsonValue(js_json)));
		html_values.push_back(make_pair("opengraph", MakeJsonValue(og_json)));
		html_values.push_back(make_pair("schema", MakeSchemaMapValue(schema_json)));
		html_values.push_back(make_pair("readability", MakeJsonValue(readability_json)));
#else
		html_values.push_back(make_pair("document", Value(body)));
		html_values.push_back(make_pair("js", Value(LogicalType::JSON())));
		html_values.push_back(make_pair("opengraph", Value(LogicalType::JSON())));
		html_values.push_back(make_pair(
		    "schema", Value::MAP(LogicalType::VARCHAR, LogicalType::JSON(), vector<Value>(), vector<Value>())));
		html_values.push_back(make_pair("readability", Value(LogicalType::JSON())));
#endif
	} else {
		// Non-HTML content or empty body
		html_values.push_back(make_pair("document", body.empty() ? Value() : Value(body)));
		html_values.push_back(make_pair("js", Value(LogicalType::JSON())));
		html_values.push_back(make_pair("opengraph", Value(LogicalType::JSON())));
		html_values.push_back(make_pair(
		    "schema", Value::MAP(LogicalType::VARCHAR, LogicalType::JSON(), vector<Value>(), vector<Value>())));
		html_values.push_back(make_pair("readability", Value(LogicalType::JSON())));
	}

	return Value::STRUCT(std::move(html_values));
}

//===--------------------------------------------------------------------===//
// Bind Data
//===--------------------------------------------------------------------===//

struct CrawlBindData : public TableFunctionData {
	vector<string> urls;
	string source_query;
	string state_table;
	string user_agent = "DuckDB-Crawler/1.0";
	int timeout_ms = 30000;
	int batch_size = 10;            // URLs per Rust batch
	int concurrency = 4;            // Concurrent requests in Rust
	int delay_ms = 0;               // Min delay between requests to same domain
	bool respect_robots = false;    // Check robots.txt before fetching
	string follow_selector;         // CSS selector for link following (empty = no following)
	int max_depth = 1;              // Max crawl depth (1 = initial URLs only)
	bool use_cache = true;          // Enable HTTP response caching
	int cache_ttl_hours = 24;       // Cache TTL in hours
	int64_t max_results = -1;       // Max results to return (-1 = unlimited), for LIMIT pushdown
	idx_t reported_cardinality = 0; // Cardinality we report to optimizer (for LIMIT detection)
	// Proxy settings (from DuckDB http_proxy or CREATE SECRET)
	string http_proxy;
	string http_proxy_username;
	string http_proxy_password;
	std::map<string, string> extra_headers; // From CREATE SECRET extra_http_headers
};

// URL with depth tracking for link following
struct UrlWithDepth {
	string url;
	int depth;
};

//===--------------------------------------------------------------------===//
// Global State
//===--------------------------------------------------------------------===//

struct CrawlGlobalState : public GlobalTableFunctionState {
	vector<CrawlResultEntry> pending_results; // Results from current batch
	idx_t result_idx = 0;                     // Index into pending_results
	idx_t next_url_idx = 0;                   // Next URL from initial list
	std::set<string> processed_urls;          // Already crawled (from state table)
	vector<UrlWithDepth> url_queue;           // URLs to crawl with depth tracking
	idx_t queue_idx = 0;                      // Next index in url_queue
	bool initialized = false;
	bool finished = false;
	int64_t results_returned = 0;  // Count of results returned (for max_results)
	int64_t limit_from_query = -1; // LIMIT value pushed down from query (-1 = unlimited)

	idx_t MaxThreads() const override {
		return 1;
	}
};

//===--------------------------------------------------------------------===//
// State Table Management
//===--------------------------------------------------------------------===//

static void EnsureStateTable(Connection &conn, const string &table_name) {
	string sql = "CREATE TABLE IF NOT EXISTS " + QuoteSqlIdentifier(table_name) +
	             " ("
	             "url VARCHAR PRIMARY KEY, "
	             "http_status INTEGER, "
	             "extracted JSON, "
	             "crawled_at TIMESTAMP DEFAULT current_timestamp, "
	             "etag VARCHAR, "
	             "last_modified VARCHAR)";
	conn.Query(sql);
}

static std::set<string> LoadProcessedUrls(Connection &conn, const string &table_name) {
	std::set<string> urls;
	auto result = conn.Query("SELECT url FROM " + QuoteSqlIdentifier(table_name));
	if (!result->HasError()) {
		while (auto chunk = result->Fetch()) {
			for (idx_t i = 0; i < chunk->size(); i++) {
				auto val = chunk->GetValue(0, i);
				if (!val.IsNull()) {
					urls.insert(StringValue::Get(val));
				}
			}
		}
	}
	return urls;
}

static void SaveToStateTable(Connection &conn, const string &table_name, const CrawlResultEntry &entry) {
	string sql = "INSERT OR REPLACE INTO " + QuoteSqlIdentifier(table_name) +
	             " (url, http_status, extracted, crawled_at) VALUES ($1, $2, $3, current_timestamp)";
	Value extracted_val = entry.extracted_json.empty() ? Value() : Value(entry.extracted_json);
	conn.Query(sql, entry.url, entry.status_code, extracted_val);
}

//===--------------------------------------------------------------------===//
// HTTP Cache Table Management (__crawler_cache)
//===--------------------------------------------------------------------===//

static constexpr const char *CACHE_TABLE_NAME = "__crawler_cache";

static void EnsureCacheTable(Connection &conn) {
	string sql = "CREATE TABLE IF NOT EXISTS " + string(CACHE_TABLE_NAME) +
	             " ("
	             "url VARCHAR PRIMARY KEY, "
	             "status_code INTEGER, "
	             "content_type VARCHAR, "
	             "body VARCHAR, "
	             "error VARCHAR, "
	             "response_time_ms BIGINT, "
	             "cached_at TIMESTAMP DEFAULT current_timestamp)";
	conn.Query(sql);
}

// Get cached entries for URLs that are fresher than ttl_hours
// Uses batch query to avoid N+1 problem
static vector<CrawlResultEntry> GetCachedEntries(Connection &conn, const vector<string> &urls, int ttl_hours) {
	vector<CrawlResultEntry> cached;
	if (urls.empty())
		return cached;

	EnsureCacheTable(conn);

	// Build IN clause with properly quoted URLs
	string url_list;
	for (size_t i = 0; i < urls.size(); i++) {
		if (i > 0)
			url_list += ", ";
		url_list += EscapeSqlString(urls[i]);
	}

	// Single batch query instead of N queries
	string sql = "SELECT url, status_code, content_type, body, error, response_time_ms "
	             "FROM " +
	             string(CACHE_TABLE_NAME) +
	             " "
	             "WHERE url IN (" +
	             url_list +
	             ") "
	             "AND cached_at > current_timestamp - INTERVAL '" +
	             std::to_string(ttl_hours) + " hours'";

	auto result = conn.Query(sql);
	if (result->HasError()) {
		return cached;
	}

	// Process all results from single query
	while (true) {
		auto chunk = result->Fetch();
		if (!chunk || chunk->size() == 0)
			break;

		for (idx_t row = 0; row < chunk->size(); row++) {
			CrawlResultEntry entry;
			entry.url = chunk->GetValue(0, row).ToString();
			entry.status_code = chunk->GetValue(1, row).GetValue<int>();
			entry.content_type = chunk->GetValue(2, row).IsNull() ? "" : chunk->GetValue(2, row).ToString();
			entry.body = chunk->GetValue(3, row).IsNull() ? "" : chunk->GetValue(3, row).ToString();
			entry.error = chunk->GetValue(4, row).IsNull() ? "" : chunk->GetValue(4, row).ToString();
			entry.response_time_ms = chunk->GetValue(5, row).IsNull() ? 0 : chunk->GetValue(5, row).GetValue<int64_t>();
			cached.push_back(std::move(entry));
		}
	}
	return cached;
}

// Check which URLs are in cache and fresh
static std::set<string> GetCachedUrls(Connection &conn, const vector<string> &urls, int ttl_hours) {
	std::set<string> cached_urls;
	auto entries = GetCachedEntries(conn, urls, ttl_hours);
	for (const auto &entry : entries) {
		cached_urls.insert(entry.url);
	}
	return cached_urls;
}

static void SaveToCache(Connection &conn, const CrawlResultEntry &entry) {
	EnsureCacheTable(conn);
	string sql = "INSERT OR REPLACE INTO " + string(CACHE_TABLE_NAME) +
	             " (url, status_code, content_type, body, error, response_time_ms, cached_at) "
	             "VALUES ($1, $2, $3, $4, $5, $6, current_timestamp)";
	conn.Query(sql, entry.url, entry.status_code, entry.content_type.empty() ? Value() : Value(entry.content_type),
	           entry.body.empty() ? Value() : Value(entry.body), entry.error.empty() ? Value() : Value(entry.error),
	           entry.response_time_ms);
}

//===--------------------------------------------------------------------===//
// Bind Function
//===--------------------------------------------------------------------===//

static unique_ptr<FunctionData> CrawlBind(ClientContext &context, TableFunctionBindInput &input,
                                          vector<LogicalType> &return_types, vector<string> &names) {
	auto bind_data = make_uniq<CrawlBindData>();

	// Read extension settings as defaults
	Value setting_value;
	if (context.TryGetCurrentSetting("crawler_user_agent", setting_value)) {
		bind_data->user_agent = setting_value.ToString();
	}
	if (context.TryGetCurrentSetting("crawler_default_delay", setting_value)) {
		bind_data->delay_ms = static_cast<int>(setting_value.GetValue<double>() * 1000);
	}
	if (context.TryGetCurrentSetting("crawler_timeout_ms", setting_value)) {
		bind_data->timeout_ms = static_cast<int>(setting_value.GetValue<int64_t>());
	}
	if (context.TryGetCurrentSetting("crawler_respect_robots", setting_value)) {
		bind_data->respect_robots = setting_value.GetValue<bool>();
	}

	// Read DuckDB's http_proxy settings
	if (context.TryGetCurrentSetting("http_proxy", setting_value) && !setting_value.IsNull()) {
		bind_data->http_proxy = setting_value.ToString();
	}
	if (context.TryGetCurrentSetting("http_proxy_username", setting_value) && !setting_value.IsNull()) {
		bind_data->http_proxy_username = setting_value.ToString();
	}
	if (context.TryGetCurrentSetting("http_proxy_password", setting_value) && !setting_value.IsNull()) {
		bind_data->http_proxy_password = setting_value.ToString();
	}

	// First argument: URL list or single URL string
	auto &first_arg = input.inputs[0];
	if (first_arg.type().id() == LogicalTypeId::LIST) {
		auto &url_list = ListValue::GetChildren(first_arg);
		for (auto &url_val : url_list) {
			if (!url_val.IsNull()) {
				bind_data->urls.push_back(StringValue::Get(url_val));
			}
		}
	} else {
		// Single URL string
		bind_data->urls.push_back(StringValue::Get(first_arg));
	}

	// Named parameters
	for (auto &kv : input.named_parameters) {
		if (kv.first == "state_table") {
			bind_data->state_table = StringValue::Get(kv.second);
		} else if (kv.first == "user_agent") {
			bind_data->user_agent = StringValue::Get(kv.second);
		} else if (kv.first == "timeout") {
			bind_data->timeout_ms = kv.second.GetValue<int>() * 1000;
		} else if (kv.first == "workers") {
			bind_data->concurrency = kv.second.GetValue<int>();
		} else if (kv.first == "batch_size") {
			bind_data->batch_size = kv.second.GetValue<int>();
		} else if (kv.first == "delay") {
			bind_data->delay_ms = kv.second.GetValue<int>();
		} else if (kv.first == "respect_robots") {
			bind_data->respect_robots = kv.second.GetValue<bool>();
		} else if (kv.first == "follow") {
			bind_data->follow_selector = StringValue::Get(kv.second);
		} else if (kv.first == "max_depth") {
			bind_data->max_depth = kv.second.GetValue<int>();
			if (bind_data->max_depth < 1)
				bind_data->max_depth = 1;
		} else if (kv.first == "cache") {
			bind_data->use_cache = kv.second.GetValue<bool>();
		} else if (kv.first == "cache_ttl") {
			bind_data->cache_ttl_hours = kv.second.GetValue<int>();
		} else if (kv.first == "max_results") {
			bind_data->max_results = kv.second.GetValue<int64_t>();
		}
	}

	// Return columns
	return_types.push_back(LogicalType::VARCHAR); // url
	return_types.push_back(LogicalType::INTEGER); // status
	return_types.push_back(LogicalType::VARCHAR); // content_type

	// html STRUCT(document, js, opengraph, schema) - structured HTML content
	child_list_t<LogicalType> html_struct;
	html_struct.push_back(make_pair("document", LogicalType::VARCHAR)); // Raw HTML document
	html_struct.push_back(make_pair("js", LogicalType::JSON()));        // JSON type
	html_struct.push_back(make_pair("opengraph", LogicalType::JSON())); // JSON type
	// schema is MAP(VARCHAR, JSON) for easy access: schema['Product']->>'name'
	html_struct.push_back(make_pair("schema", LogicalType::MAP(LogicalType::VARCHAR, LogicalType::JSON())));
	html_struct.push_back(make_pair("readability", LogicalType::JSON())); // Readability extracted content
	return_types.push_back(LogicalType::STRUCT(html_struct));

	return_types.push_back(LogicalType::VARCHAR); // error
	return_types.push_back(LogicalType::VARCHAR); // extract
	return_types.push_back(LogicalType::BIGINT);  // response_time_ms
	return_types.push_back(LogicalType::INTEGER); // depth

	names.push_back("url");
	names.push_back("status");
	names.push_back("content_type");
	names.push_back("html");
	names.push_back("error");
	names.push_back("extract");
	names.push_back("response_time_ms");
	names.push_back("depth");

	return std::move(bind_data);
}

//===--------------------------------------------------------------------===//
// Cardinality Function (for LIMIT pushdown detection)
//===--------------------------------------------------------------------===//

// We report a large cardinality so LIMIT pushdown can be detected by comparing
// estimated_cardinality (after optimizer) with our reported value
static constexpr idx_t CRAWL_REPORTED_CARDINALITY = 1000000;

static unique_ptr<NodeStatistics> CrawlCardinality(ClientContext &context, const FunctionData *bind_data) {
	return make_uniq<NodeStatistics>(CRAWL_REPORTED_CARDINALITY, CRAWL_REPORTED_CARDINALITY);
}

//===--------------------------------------------------------------------===//
// Init Global
//===--------------------------------------------------------------------===//

static unique_ptr<GlobalTableFunctionState> CrawlInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto state = make_uniq<CrawlGlobalState>();

	// LIMIT pushdown: compare estimated_cardinality with our reported cardinality
	// If estimated < reported, LIMIT was applied by the optimizer
	if (input.op) {
		idx_t estimated = input.op->estimated_cardinality;
		// If estimated is less than our reported cardinality, LIMIT was applied
		if (estimated > 0 && estimated < CRAWL_REPORTED_CARDINALITY) {
			state->limit_from_query = static_cast<int64_t>(estimated);
		}
	}

	return std::move(state);
}

//===--------------------------------------------------------------------===//
// Main Function - Streaming with Rust HTTP + Link Following
//===--------------------------------------------------------------------===//

static void CrawlFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->CastNoConst<CrawlBindData>();
	auto &state = data.global_state->Cast<CrawlGlobalState>();

	// Initialize on first call
	if (!state.initialized) {
		state.initialized = true;

		Connection conn(*context.db);

		// Execute source query if provided
		if (!bind_data.source_query.empty()) {
			auto query_result = conn.Query(bind_data.source_query);
			if (query_result->HasError()) {
				throw IOException("crawl source query error: " + query_result->GetError());
			}
			while (auto chunk = query_result->Fetch()) {
				for (idx_t i = 0; i < chunk->size(); i++) {
					auto val = chunk->GetValue(0, i);
					if (!val.IsNull()) {
						bind_data.urls.push_back(val.ToString());
					}
				}
			}
		}

		// Load processed URLs from state table
		if (!bind_data.state_table.empty()) {
			EnsureStateTable(conn, bind_data.state_table);
			state.processed_urls = LoadProcessedUrls(conn, bind_data.state_table);
		}

		// Initialize URL queue with initial URLs at depth 1
		for (const auto &url : bind_data.urls) {
			state.url_queue.push_back({url, 1});
		}
	}

	// Connection for state table updates
	unique_ptr<Connection> conn_holder;
	Connection *conn = nullptr;
	if (!bind_data.state_table.empty()) {
		conn_holder = make_uniq<Connection>(*context.db);
		conn = conn_holder.get();
	}

	idx_t count = 0;

	// For LIMIT pushdown: yield ONE row at a time, then return to let executor decide
	// This allows LIMIT to take effect between HTTP requests
	while (count < 1) { // Changed from STANDARD_VECTOR_SIZE to 1 for streaming
		// Check for interrupt (Ctrl+C)
		if (IsInterrupted()) {
			state.finished = true;
			break;
		}

		// Check max_results limit (explicit param takes precedence over LIMIT pushdown)
		int64_t effective_limit = bind_data.max_results;
		if (effective_limit < 0 && state.limit_from_query >= 0) {
			effective_limit = state.limit_from_query;
		}
		if (effective_limit >= 0 && state.results_returned >= effective_limit) {
			state.finished = true;
			break;
		}

		// If we have pending results, yield ONE
		if (state.result_idx < state.pending_results.size()) {
			auto &entry = state.pending_results[state.result_idx++];

			output.SetValue(0, count, Value(entry.url));
			output.SetValue(1, count, Value(entry.status_code));
			output.SetValue(2, count, Value(entry.content_type));
			output.SetValue(3, count, BuildHtmlStructValue(entry.body, entry.content_type, entry.url));
			output.SetValue(4, count, entry.error.empty() ? Value() : Value(entry.error));
			output.SetValue(5, count, entry.extracted_json.empty() ? Value() : Value(entry.extracted_json));
			output.SetValue(6, count, Value::BIGINT(entry.response_time_ms));
			output.SetValue(7, count, Value::INTEGER(entry.depth));
			count++;
			state.results_returned++; // Track for max_results limit

			// Mark as processed (before extracting links to avoid re-queuing)
			state.processed_urls.insert(entry.url);

			// Extract links for following if configured and within max_depth
			if (!bind_data.follow_selector.empty() && entry.depth < bind_data.max_depth && entry.status_code >= 200 &&
			    entry.status_code < 300 && !entry.body.empty()) {
				auto links = ExtractLinksWithRust(entry.body, bind_data.follow_selector, entry.url);
				for (const auto &link : links) {
					// Only add if not already processed (don't add to processed_urls yet)
					if (state.processed_urls.count(link) == 0) {
						state.url_queue.push_back({link, entry.depth + 1});
					}
				}
			}
			if (conn) {
				SaveToStateTable(*conn, bind_data.state_table, entry);
			}
			break; // Return after ONE row to allow LIMIT to interrupt
		}

		// No more pending results - fetch ONE URL at a time for LIMIT pushdown
		state.pending_results.clear();
		state.result_idx = 0;

		// Get next single URL from queue (skip already processed)
		string url_to_fetch;
		int url_depth = 1;
		while (state.queue_idx < state.url_queue.size()) {
			auto &item = state.url_queue[state.queue_idx++];
			// Skip if already processed (handles duplicates and resumption from state table)
			if (state.processed_urls.count(item.url) == 0) {
				url_to_fetch = item.url;
				url_depth = item.depth;
				break;
			}
		}

		// No more URLs to fetch
		if (url_to_fetch.empty()) {
			state.finished = true;
			break;
		}

		Connection cache_conn(*context.db);

		// Check cache first
		CrawlResultEntry result;
		bool from_cache = false;

		if (bind_data.use_cache) {
			auto cached = GetCachedEntries(cache_conn, {url_to_fetch}, bind_data.cache_ttl_hours);
			if (!cached.empty()) {
				result = std::move(cached[0]);
				result.depth = url_depth;
				from_cache = true;
			}
		}

		// Fetch if not cached
		if (!from_cache) {
			// Apply HTTP secrets for this specific URL (may override global settings)
			string http_proxy = bind_data.http_proxy;
			string http_proxy_username = bind_data.http_proxy_username;
			string http_proxy_password = bind_data.http_proxy_password;
			std::map<string, string> extra_headers = bind_data.extra_headers;
			ApplyHttpSecrets(context, url_to_fetch, http_proxy, http_proxy_username, http_proxy_password,
			                 extra_headers);

			string request_json = BuildBatchCrawlRequest({url_to_fetch},
			                                             "{}", // No extraction specs
			                                             bind_data.user_agent, bind_data.timeout_ms,
			                                             1, // Single URL, single concurrency
			                                             bind_data.delay_ms, bind_data.respect_robots, http_proxy,
			                                             http_proxy_username, http_proxy_password, extra_headers);

			string response_json = CrawlBatchWithRust(request_json);
			auto fetched = ParseBatchCrawlResponse(response_json);

			if (!fetched.empty()) {
				result = std::move(fetched[0]);
				result.depth = url_depth;

				if (bind_data.use_cache) {
					SaveToCache(cache_conn, result);
				}
			}
		}

		// Add to pending results for immediate yield
		state.pending_results.push_back(std::move(result));
	}

	output.SetCardinality(count);
}

//===--------------------------------------------------------------------===//
// LATERAL Join Support (In-Out Function)
//===--------------------------------------------------------------------===//

struct CrawlLateralLocalState : public LocalTableFunctionState {
	// No state needed - each row is independent
};

static unique_ptr<LocalTableFunctionState> CrawlLateralInitLocal(ExecutionContext &context,
                                                                 TableFunctionInitInput &input,
                                                                 GlobalTableFunctionState *global_state) {
	return make_uniq<CrawlLateralLocalState>();
}

static OperatorResultType CrawlInOut(ExecutionContext &context, TableFunctionInput &data, DataChunk &input,
                                     DataChunk &output) {
	auto &bind_data = data.bind_data->CastNoConst<CrawlBindData>();

	if (input.size() == 0) {
		return OperatorResultType::NEED_MORE_INPUT;
	}

	idx_t count = 0;

	for (idx_t i = 0; i < input.size() && count < STANDARD_VECTOR_SIZE; i++) {
		Value url_val = input.GetValue(0, i);

		if (url_val.IsNull()) {
			output.SetValue(0, count, Value());
			output.SetValue(1, count, Value());
			output.SetValue(2, count, Value());
			output.SetValue(3, count, BuildHtmlStructValue("", ""));
			output.SetValue(4, count, Value("NULL URL"));
			output.SetValue(5, count, Value());
			output.SetValue(6, count, Value());
			count++;
			continue;
		}

		string url = StringValue::Get(url_val);
		if (url.empty())
			continue;

		// Build minimal batch request for single URL
		yyjson_mut_doc *doc = yyjson_mut_doc_new(nullptr);
		yyjson_mut_val *root = yyjson_mut_obj(doc);
		yyjson_mut_doc_set_root(doc, root);

		yyjson_mut_val *urls_arr = yyjson_mut_arr(doc);
		yyjson_mut_arr_add_strcpy(doc, urls_arr, url.c_str());
		yyjson_mut_obj_add_val(doc, root, "urls", urls_arr);

		yyjson_mut_obj_add_strcpy(doc, root, "user_agent", bind_data.user_agent.c_str());
		yyjson_mut_obj_add_uint(doc, root, "timeout_ms", bind_data.timeout_ms);
		yyjson_mut_obj_add_uint(doc, root, "concurrency", 1);

		size_t len = 0;
		char *json_str = yyjson_mut_write(doc, 0, &len);
		yyjson_mut_doc_free(doc);

		if (!json_str) {
			output.SetValue(0, count, Value(url));
			output.SetValue(1, count, Value());
			output.SetValue(2, count, Value());
			output.SetValue(3, count, BuildHtmlStructValue("", ""));
			output.SetValue(4, count, Value("Failed to serialize request"));
			output.SetValue(5, count, Value());
			output.SetValue(6, count, Value());
			count++;
			continue;
		}

		string request_json(json_str, len);
		free(json_str);

		string response_json = CrawlBatchWithRust(request_json);

		// Parse response
		yyjson_doc *resp_doc = yyjson_read(response_json.c_str(), response_json.size(), 0);
		if (!resp_doc) {
			output.SetValue(0, count, Value(url));
			output.SetValue(1, count, Value());
			output.SetValue(2, count, Value());
			output.SetValue(3, count, BuildHtmlStructValue("", ""));
			output.SetValue(4, count, Value("Failed to parse response"));
			output.SetValue(5, count, Value());
			output.SetValue(6, count, Value());
			count++;
			continue;
		}

		yyjson_val *resp_root = yyjson_doc_get_root(resp_doc);
		yyjson_val *results_arr = yyjson_obj_get(resp_root, "results");

		if (results_arr && yyjson_is_arr(results_arr) && yyjson_arr_size(results_arr) > 0) {
			yyjson_val *item = yyjson_arr_get_first(results_arr);

			yyjson_val *url_val_json = yyjson_obj_get(item, "url");
			yyjson_val *status_val = yyjson_obj_get(item, "status");
			yyjson_val *content_type_val = yyjson_obj_get(item, "content_type");
			yyjson_val *body_val = yyjson_obj_get(item, "body");
			yyjson_val *error_val = yyjson_obj_get(item, "error");
			yyjson_val *extracted_val = yyjson_obj_get(item, "extracted");
			yyjson_val *time_val = yyjson_obj_get(item, "response_time_ms");

			string result_url = url_val_json ? yyjson_get_str(url_val_json) : url;
			int status = status_val ? yyjson_get_int(status_val) : 0;
			string content_type = content_type_val ? yyjson_get_str(content_type_val) : "";
			string body = body_val ? yyjson_get_str(body_val) : "";
			string error = error_val ? yyjson_get_str(error_val) : "";
			int64_t response_time = time_val ? yyjson_get_int(time_val) : 0;

			string extracted_json;
			if (extracted_val) {
				char *ext_str = yyjson_val_write(extracted_val, 0, nullptr);
				if (ext_str) {
					extracted_json = ext_str;
					free(ext_str);
				}
			}

			output.SetValue(0, count, Value(result_url));
			output.SetValue(1, count, Value(status));
			output.SetValue(2, count, Value(content_type));
			output.SetValue(3, count, BuildHtmlStructValue(body, content_type, result_url));
			output.SetValue(4, count, error.empty() ? Value() : Value(error));
			output.SetValue(5, count, extracted_json.empty() ? Value() : Value(extracted_json));
			output.SetValue(6, count, Value::BIGINT(response_time));
		} else {
			output.SetValue(0, count, Value(url));
			output.SetValue(1, count, Value());
			output.SetValue(2, count, Value());
			output.SetValue(3, count, BuildHtmlStructValue("", "", url));
			output.SetValue(4, count, Value("No results"));
			output.SetValue(5, count, Value());
			output.SetValue(6, count, Value());
		}

		yyjson_doc_free(resp_doc);
		count++;
	}

	output.SetCardinality(count);
	return OperatorResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// Register Function
//===--------------------------------------------------------------------===//

void RegisterCrawlTableFunction(ExtensionLoader &loader) {
	// Named parameters helper
	auto add_params = [](TableFunction &func) {
		func.named_parameters["extract"] = LogicalType::LIST(LogicalType::VARCHAR);
		func.named_parameters["state_table"] = LogicalType::VARCHAR;
		func.named_parameters["user_agent"] = LogicalType::VARCHAR;
		func.named_parameters["timeout"] = LogicalType::INTEGER;
		func.named_parameters["workers"] = LogicalType::INTEGER;
		func.named_parameters["batch_size"] = LogicalType::INTEGER;
		func.named_parameters["delay"] = LogicalType::INTEGER;
		func.named_parameters["respect_robots"] = LogicalType::BOOLEAN;
		func.named_parameters["follow"] = LogicalType::VARCHAR;
		func.named_parameters["max_depth"] = LogicalType::INTEGER;
		func.named_parameters["cache"] = LogicalType::BOOLEAN;
		func.named_parameters["cache_ttl"] = LogicalType::INTEGER;
		func.named_parameters["max_results"] = LogicalType::BIGINT;
	};

	// crawl() with URL list (batch mode)
	TableFunction list_func("crawl", {LogicalType::LIST(LogicalType::VARCHAR)}, CrawlFunction, CrawlBind,
	                        CrawlInitGlobal);
	list_func.cardinality = CrawlCardinality; // Enable LIMIT pushdown detection
	add_params(list_func);

	// crawl() with single URL (also batch mode, no LATERAL)
	TableFunction single_func("crawl", {LogicalType::VARCHAR}, CrawlFunction, CrawlBind, CrawlInitGlobal);
	single_func.cardinality = CrawlCardinality; // Enable LIMIT pushdown detection
	add_params(single_func);

	TableFunctionSet crawl_set("crawl");
	crawl_set.AddFunction(list_func);
	crawl_set.AddFunction(single_func);
	loader.RegisterFunction(crawl_set);
}

} // namespace duckdb
