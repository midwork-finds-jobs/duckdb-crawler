// crawl() table function - Rust HTTP + extraction
//
// Registered as an in-out function, so one function covers every position:
//   SELECT * FROM crawl('https://example.com/')                    -- bare, single URL
//   SELECT * FROM crawl(['https://a.com/', 'https://b.com/'])      -- bare, URL list
//   SELECT c.* FROM urls u, LATERAL crawl(u.url) c                 -- per-row lateral
//
// Bare calls run as a table-scan source (the constant argument arrives as a
// re-delivered single-row input chunk; we ingest it once and finish by
// emitting 0 rows). LATERAL calls run as an operator (each input row arrives
// as its own chunk; link following may emit many rows per input row).
//
// The 'html' column is a STRUCT containing:
//   - body: raw HTML content
//   - js: extracted JavaScript variables as JSON
//   - opengraph: OpenGraph meta tags as JSON
//   - schema: combined JSON-LD + microdata as JSON

#include "crawl_table_function.hpp"
#include "crawler_compat.hpp"
#include "crawler_utils.hpp"
#include "rust_ffi.hpp"
#include "yyjson.hpp"
#include "pipeline_state.hpp"

#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/catalog/catalog_transaction.hpp"

#include <set>
#include <map>
#include <mutex>

namespace duckdb {

using namespace duckdb_yyjson;


// Build batch crawl request JSON for Rust
static string BuildBatchCrawlRequest(const vector<string> &urls,
                                      const string &user_agent,
                                      int timeout_ms,
                                      int concurrency,
                                      int delay_ms,
                                      bool respect_robots,
                                      const string &http_proxy = "",
                                      const string &http_proxy_username = "",
                                      const string &http_proxy_password = "",
                                      const std::map<string, string> &extra_headers = {}) {
    yyjson_mut_doc *doc = yyjson_mut_doc_new(nullptr);
    if (!doc) return "{}";

    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);

    // URLs array
    yyjson_mut_val *urls_arr = yyjson_mut_arr(doc);
    for (const auto &url : urls) {
        yyjson_mut_arr_add_strcpy(doc, urls_arr, url.c_str());
    }
    yyjson_mut_obj_add_val(doc, root, "urls", urls_arr);

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

    if (!json_str) return "{}";

    string result_str(json_str, len);
    free(json_str);
    return result_str;
}

//===--------------------------------------------------------------------===//
// HTTP Secret Lookup
//===--------------------------------------------------------------------===//

// Look up HTTP secrets for a URL and populate extra_headers
static void ApplyHttpSecrets(ClientContext &context, const string &url,
                              string &http_proxy, string &http_proxy_username, string &http_proxy_password,
                              std::map<string, string> &extra_headers) {
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
        return;  // Not a KeyValueSecret
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
    string final_url;
    int status_code = 0;
    string content_type;
    string body;
    string error;
    int64_t response_time_ms = 0;
    int depth = 1;  // Crawl depth (1 = initial URL)
};

// Parse batch crawl response from Rust
static vector<CrawlResultEntry> ParseBatchCrawlResponse(const string &response_json) {
    vector<CrawlResultEntry> results;

    yyjson_doc *doc = yyjson_read(response_json.c_str(), response_json.size(), 0);
    if (!doc) return results;

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

        yyjson_val *final_url_val = yyjson_obj_get(item, "final_url");
        if (final_url_val && yyjson_is_str(final_url_val)) {
            entry.final_url = yyjson_get_str(final_url_val);
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
    if (!doc) return "{}";

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

    if (!json_str) return "{}";

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
        return Value(LogicalType::JSON());  // NULL JSON
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

    bool is_html = content_type.find("text/html") != string::npos ||
                   content_type.find("application/xhtml") != string::npos;

    if (is_html && !body.empty()) {
#if defined(RUST_PARSER_AVAILABLE) && RUST_PARSER_AVAILABLE
        string js_json = ExtractJsWithRust(body);
        string og_json = ExtractOpenGraphWithRust(body);
        string meta_json = ExtractMetaWithRust(body);
        string jsonld_json = ExtractJsonLdWithRust(body);
        string microdata_json = ExtractMicrodataWithRust(body);
        string schema_json = CombineSchemaData(jsonld_json, microdata_json);
        string readability_json = ExtractReadabilityWithRust(body, url);
        string hydration_json = ExtractHydrationWithRust(body);

        html_values.push_back(make_pair("document", Value(body)));
        html_values.push_back(make_pair("js", MakeJsonValue(js_json)));
        html_values.push_back(make_pair("meta", MakeJsonValue(meta_json)));
        html_values.push_back(make_pair("opengraph", MakeJsonValue(og_json)));
        html_values.push_back(make_pair("schema", MakeSchemaMapValue(schema_json)));
        html_values.push_back(make_pair("readability", MakeJsonValue(readability_json)));
        html_values.push_back(make_pair("hydration", MakeSchemaMapValue(hydration_json)));
#else
        html_values.push_back(make_pair("document", Value(body)));
        html_values.push_back(make_pair("js", Value(LogicalType::JSON())));
        html_values.push_back(make_pair("meta", Value(LogicalType::JSON())));
        html_values.push_back(make_pair("opengraph", Value(LogicalType::JSON())));
        html_values.push_back(make_pair("schema", Value::MAP(LogicalType::VARCHAR, LogicalType::JSON(), vector<Value>(), vector<Value>())));
        html_values.push_back(make_pair("readability", Value(LogicalType::JSON())));
        html_values.push_back(make_pair("hydration", Value::MAP(LogicalType::VARCHAR, LogicalType::JSON(), vector<Value>(), vector<Value>())));
#endif
    } else {
        // Non-HTML content or empty body
        html_values.push_back(make_pair("document", body.empty() ? Value() : Value(body)));
        html_values.push_back(make_pair("js", Value(LogicalType::JSON())));
        html_values.push_back(make_pair("meta", Value(LogicalType::JSON())));
        html_values.push_back(make_pair("opengraph", Value(LogicalType::JSON())));
        html_values.push_back(make_pair("schema", Value::MAP(LogicalType::VARCHAR, LogicalType::JSON(), vector<Value>(), vector<Value>())));
        html_values.push_back(make_pair("readability", Value(LogicalType::JSON())));
        html_values.push_back(make_pair("hydration", Value::MAP(LogicalType::VARCHAR, LogicalType::JSON(), vector<Value>(), vector<Value>())));
    }

    return Value::STRUCT(std::move(html_values));
}

//===--------------------------------------------------------------------===//
// Bind Data
//===--------------------------------------------------------------------===//

struct CrawlBindData : public TableFunctionData {
    string state_table;
    string user_agent = "DuckDB-Crawler/1.0";
    int timeout_ms = 30000;
    bool timeout_explicit = false;  // named timeout := N (seconds) overrides SET crawler_timeout_ms
    int batch_size = 10;  // URLs per Rust batch
    int concurrency = 4;  // Concurrent requests in Rust
    int delay_ms = 0;     // Min delay between requests to same domain
    bool respect_robots = false;  // Check robots.txt before fetching
    string follow_selector;  // CSS selector for link following (empty = no following)
    int max_depth = 1;       // Max crawl depth (1 = initial URLs only)
    bool use_cache = true;   // Enable HTTP response caching
    int cache_ttl_hours = 24;  // Cache TTL in hours
    int64_t max_results = -1;  // Max results to return (-1 = unlimited), for LIMIT pushdown
    // Shared pipeline state for LIMIT pushdown across LATERAL calls (STREAM INTO)
    std::shared_ptr<PipelineState> pipeline_state;
    // Proxy settings (from DuckDB http_proxy or CREATE SECRET)
    string http_proxy;
    string http_proxy_username;
    string http_proxy_password;
    std::map<string, string> extra_headers;  // From CREATE SECRET extra_http_headers
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
    std::mutex lock;                           // Operator mode may run multi-threaded
    vector<CrawlResultEntry> pending_results;  // Results from current batch
    idx_t result_idx = 0;                      // Index into pending_results
    std::set<string> processed_urls;           // Already crawled (from state table)
    vector<UrlWithDepth> url_queue;            // URLs to crawl with depth tracking
    idx_t queue_idx = 0;                       // Next index in url_queue
    bool initialized = false;
    bool finished = false;                     // Limit reached or interrupted - stop everything
    int64_t results_returned = 0;              // Count of results returned (for max_results)
    int64_t limit_from_query = -1;             // LIMIT value pushed down from query (-1 = unlimited)
    // Bare-call (table-scan source) handling: the same constant input chunk is
    // re-delivered until we emit 0 rows, so ingest it exactly once
    bool source_mode = false;
    bool source_done = false;

    idx_t MaxThreads() const override { return 1; }
};

struct CrawlLocalState : public LocalTableFunctionState {
    bool chunk_ingested = false;  // Current input chunk's URLs already queued
};

//===--------------------------------------------------------------------===//
// State Table Management
//===--------------------------------------------------------------------===//

static void EnsureStateTable(Connection &conn, const string &table_name) {
    string sql = "CREATE TABLE IF NOT EXISTS " + QuoteSqlIdentifier(table_name) + " ("
                 "url VARCHAR PRIMARY KEY, "
                 "http_status INTEGER, "
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
                 " (url, http_status, crawled_at) VALUES ($1, $2, current_timestamp)";
    conn.Query(sql, entry.url, entry.status_code);
}

//===--------------------------------------------------------------------===//
// HTTP Cache Table Management (__crawler_cache)
//===--------------------------------------------------------------------===//

static constexpr const char* CACHE_TABLE_NAME = "__crawler_cache";

static void EnsureCacheTable(Connection &conn) {
    string sql = "CREATE TABLE IF NOT EXISTS " + string(CACHE_TABLE_NAME) + " ("
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
    if (urls.empty()) return cached;

    EnsureCacheTable(conn);

    // Build IN clause with properly quoted URLs
    string url_list;
    for (size_t i = 0; i < urls.size(); i++) {
        if (i > 0) url_list += ", ";
        url_list += EscapeSqlString(urls[i]);
    }

    // Single batch query instead of N queries
    string sql = "SELECT url, status_code, content_type, body, error, response_time_ms "
                 "FROM " + string(CACHE_TABLE_NAME) + " "
                 "WHERE url IN (" + url_list + ") "
                 "AND cached_at > current_timestamp - INTERVAL '" + std::to_string(ttl_hours) + " hours'";

    auto result = conn.Query(sql);
    if (result->HasError()) {
        return cached;
    }

    // Process all results from single query
    while (true) {
        auto chunk = result->Fetch();
        if (!chunk || chunk->size() == 0) break;

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
    conn.Query(sql, entry.url, entry.status_code,
               entry.content_type.empty() ? Value() : Value(entry.content_type),
               entry.body.empty() ? Value() : Value(entry.body),
               entry.error.empty() ? Value() : Value(entry.error),
               entry.response_time_ms);
}

//===--------------------------------------------------------------------===//
// Bind Function
//===--------------------------------------------------------------------===//

static unique_ptr<FunctionData> CrawlBind(ClientContext &context, TableFunctionBindInput &input,
                                           vector<LogicalType> &return_types,
                                           vector<CrawlerResultName> &names) {
    auto bind_data = make_uniq<CrawlBindData>();

    // Read extension settings as defaults
    Value setting_value;
    if (context.TryGetCurrentSetting("crawler_user_agent", setting_value)) {
        bind_data->user_agent = setting_value.ToString();
    }
    if (context.TryGetCurrentSetting("crawler_default_delay", setting_value)) {
        bind_data->delay_ms = static_cast<int>(setting_value.GetValue<double>() * 1000);
    }
    bind_data->timeout_ms = GetCrawlerTimeoutMs(context);
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

    // URLs arrive through the input chunk at execution time (in-out function).
    // Optional second positional argument: max_results - named parameters don't
    // work inside LATERAL, so LIMIT pushdown injection uses this positional form.
    if (input.inputs.size() > 1 && !input.inputs[1].IsNull()) {
        bind_data->max_results = input.inputs[1].GetValue<int64_t>();
    }

    // Named parameters
    for (auto &kv : input.named_parameters) {
        if (kv.first == "state_table") {
            bind_data->state_table = StringValue::Get(kv.second);
        } else if (kv.first == "user_agent") {
            bind_data->user_agent = StringValue::Get(kv.second);
        } else if (kv.first == "timeout") {
            bind_data->timeout_ms = kv.second.GetValue<int>() * 1000;
            bind_data->timeout_explicit = true;
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
            if (bind_data->max_depth < 1) bind_data->max_depth = 1;
        } else if (kv.first == "cache") {
            bind_data->use_cache = kv.second.GetValue<bool>();
        } else if (kv.first == "cache_ttl") {
            bind_data->cache_ttl_hours = kv.second.GetValue<int>();
        } else if (kv.first == "max_results") {
            bind_data->max_results = kv.second.GetValue<int64_t>();
        }
    }

    // Shared pipeline state for LIMIT pushdown across LATERAL calls
    // (created by STREAM INTO / CRAWLING MERGE before running the query;
    // max_results itself is enforced locally via results_returned)
    bind_data->pipeline_state = GetPipelineState(*context.db);

    // Return columns
    return_types.push_back(LogicalType::VARCHAR);  // url
    return_types.push_back(LogicalType::INTEGER);  // status
    return_types.push_back(LogicalType::VARCHAR);  // content_type

    // html STRUCT(document, js, meta, opengraph, schema, readability, hydration) - structured HTML content
    child_list_t<LogicalType> html_struct;
    html_struct.push_back(make_pair("document", LogicalType::VARCHAR)); // Raw HTML document
    html_struct.push_back(make_pair("js", LogicalType::JSON()));        // JSON type
    html_struct.push_back(make_pair("meta", LogicalType::JSON()));      // Meta tags JSON
    html_struct.push_back(make_pair("opengraph", LogicalType::JSON())); // JSON type
    // schema is MAP(VARCHAR, JSON) for easy access: schema['Product']->>'name'
    html_struct.push_back(make_pair("schema", LogicalType::MAP(LogicalType::VARCHAR, LogicalType::JSON())));
    html_struct.push_back(make_pair("readability", LogicalType::JSON()));  // Readability extracted content
    // hydration is MAP(VARCHAR, JSON) for SPA framework state: hydration['__NEXT_DATA__'], hydration['__pinia']
    html_struct.push_back(make_pair("hydration", LogicalType::MAP(LogicalType::VARCHAR, LogicalType::JSON())));
    return_types.push_back(LogicalType::STRUCT(html_struct));

    return_types.push_back(LogicalType::VARCHAR);  // final_url
    return_types.push_back(LogicalType::VARCHAR);  // error
    return_types.push_back(LogicalType::BIGINT);   // response_time_ms
    return_types.push_back(LogicalType::INTEGER);  // depth

    names.push_back("url");
    names.push_back("status");
    names.push_back("content_type");
    names.push_back("html");
    names.push_back("final_url");
    names.push_back("error");
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

static unique_ptr<GlobalTableFunctionState> CrawlInitGlobal(ClientContext &context,
                                                             TableFunctionInitInput &input) {
    auto state = make_uniq<CrawlGlobalState>();

    // Only PhysicalTableScan (bare call) passes its operator here;
    // PhysicalTableInOutFunction (LATERAL) does not
    state->source_mode = bool(input.op);

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

static unique_ptr<LocalTableFunctionState> CrawlInitLocal(ExecutionContext &context,
                                                           TableFunctionInitInput &input,
                                                           GlobalTableFunctionState *global_state) {
    return make_uniq<CrawlLocalState>();
}

//===--------------------------------------------------------------------===//
// Main In-Out Function - Streaming with Rust HTTP + Link Following
// Handles bare calls (table-scan source) and LATERAL joins (operator)
//===--------------------------------------------------------------------===//

static OperatorResultType CrawlInOut(ExecutionContext &context, TableFunctionInput &data,
                                     DataChunk &input, DataChunk &output) {
    auto &bind_data = data.bind_data->CastNoConst<CrawlBindData>();
    auto &state = data.global_state->Cast<CrawlGlobalState>();
    auto &local_state = data.local_state->Cast<CrawlLocalState>();
    auto &client = context.client;
    std::lock_guard<std::mutex> guard(state.lock);

    if (state.finished) {
        output.SetCardinality(0);
        return OperatorResultType::FINISHED;
    }

    // Initialize on first call
    if (!state.initialized) {
        state.initialized = true;

        // Load processed URLs from state table
        if (!bind_data.state_table.empty()) {
            Connection conn(*client.db);
            EnsureStateTable(conn, bind_data.state_table);
            state.processed_urls = LoadProcessedUrls(conn, bind_data.state_table);
        }
    }

    // Ingest URLs from the current input chunk once (guarded so the re-delivered
    // constant chunk of a bare call isn't ingested twice)
    if (!local_state.chunk_ingested && !(state.source_mode && state.source_done)) {
        for (idx_t i = 0; i < input.size(); i++) {
            Value url_val = input.GetValue(0, i);
            if (url_val.IsNull()) {
                continue;
            }
            if (url_val.type().id() == LogicalTypeId::LIST) {
                for (auto &child : ListValue::GetChildren(url_val)) {
                    if (!child.IsNull() && !StringValue::Get(child).empty()) {
                        state.url_queue.push_back({StringValue::Get(child), 1});
                    }
                }
            } else {
                string url = url_val.ToString();
                if (!url.empty()) {
                    state.url_queue.push_back({url, 1});
                }
            }
        }
        local_state.chunk_ingested = true;
        if (state.source_mode) {
            state.source_done = true;
        }
    }

    // Connection for state table updates
    unique_ptr<Connection> conn_holder;
    Connection *conn = nullptr;
    if (!bind_data.state_table.empty()) {
        conn_holder = make_uniq<Connection>(*client.db);
        conn = conn_holder.get();
    }

    idx_t count = 0;

    // For LIMIT pushdown: yield ONE row at a time, then return to let executor decide
    // This allows LIMIT to take effect between HTTP requests
    while (count < 1) {  // Changed from STANDARD_VECTOR_SIZE to 1 for streaming
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

        // Shared pipeline LIMIT reached (STREAM INTO across LATERAL calls)
        if (bind_data.pipeline_state && bind_data.pipeline_state->stopped.load()) {
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
            output.SetValue(4, count, entry.final_url.empty() ? Value() : Value(entry.final_url));
            output.SetValue(5, count, entry.error.empty() ? Value() : Value(entry.error));
            output.SetValue(6, count, Value::BIGINT(entry.response_time_ms));
            output.SetValue(7, count, Value::INTEGER(entry.depth));
            count++;
            state.results_returned++;  // Track for max_results limit

            // Decrement shared pipeline counter (LIMIT pushdown across LATERAL)
            if (bind_data.pipeline_state) {
                int64_t remaining = --bind_data.pipeline_state->remaining;
                if (remaining <= 0) {
                    bind_data.pipeline_state->stopped = true;
                }
            }

            // Mark as processed (before extracting links to avoid re-queuing)
            state.processed_urls.insert(entry.url);

            // Extract links for following if configured and within max_depth
            if (!bind_data.follow_selector.empty() &&
                entry.depth < bind_data.max_depth &&
                entry.status_code >= 200 && entry.status_code < 300 &&
                !entry.body.empty()) {
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
            break;  // Return after ONE row to allow LIMIT to interrupt
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

        // Queue drained (more input rows may still arrive in operator mode)
        if (url_to_fetch.empty()) {
            break;
        }

        Connection cache_conn(*client.db);

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
            ApplyHttpSecrets(client, url_to_fetch, http_proxy, http_proxy_username, http_proxy_password, extra_headers);

            // Re-read SET crawler_timeout_ms at execute: a SET after bind (or a
            // prepared statement) must still bound the fetch.
            int timeout_ms = bind_data.timeout_explicit ? bind_data.timeout_ms : GetCrawlerTimeoutMs(client);
            if (timeout_ms < 1) {
                timeout_ms = 1;
            }
            string request_json = BuildBatchCrawlRequest(
                {url_to_fetch},
                bind_data.user_agent,
                timeout_ms,
                1,  // Single URL, single concurrency
                bind_data.delay_ms,
                bind_data.respect_robots,
                http_proxy,
                http_proxy_username,
                http_proxy_password,
                extra_headers
            );

            string response_json = CrawlBatchWithRust(request_json);
            auto fetched = ParseBatchCrawlResponse(response_json);

            if (!fetched.empty()) {
                result = std::move(fetched[0]);
                result.depth = url_depth;

                if (bind_data.use_cache) {
                    SaveToCache(cache_conn, result);
                }
            } else {
                // Rust returns no entry for URLs filtered out by robots.txt
                result.url = url_to_fetch;
                result.depth = url_depth;
                result.error = "Blocked by robots.txt";
            }
        }

        // Add to pending results for immediate yield
        state.pending_results.push_back(std::move(result));
    }

    output.SetCardinality(count);

    if (state.finished) {
        // Limit reached or interrupted - stop the whole crawl
        return OperatorResultType::FINISHED;
    }
    if (count > 0) {
        // More work may remain for this chunk (queue/pending) - ask to be
        // called again with the same chunk
        return OperatorResultType::HAVE_MORE_OUTPUT;
    }
    // Chunk fully processed and queue drained - ready for the next chunk.
    // In source mode the 0-row output ends the scan (source_done stays set).
    local_state.chunk_ingested = false;
    return OperatorResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// Register Function
//===--------------------------------------------------------------------===//

void RegisterCrawlTableFunction(ExtensionLoader &loader) {
    // Named parameters helper
    auto add_params = [](TableFunction &func) {
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

    // All overloads are in-out functions: bare calls run as a table-scan
    // source, LATERAL calls as an operator. The optional BIGINT positional
    // argument is max_results (named parameters don't work inside LATERAL).
    auto make_inout = [&](vector<LogicalType> arguments) {
        TableFunction func("crawl", std::move(arguments), nullptr, CrawlBind,
                           CrawlInitGlobal, CrawlInitLocal);
        func.in_out_function = CrawlInOut;
        func.cardinality = CrawlCardinality;  // Enable LIMIT pushdown detection
        add_params(func);
        return func;
    };

    TableFunctionSet crawl_set("crawl");
    crawl_set.AddFunction(make_inout({LogicalType::VARCHAR}));
    crawl_set.AddFunction(make_inout({LogicalType::LIST(LogicalType::VARCHAR)}));
    crawl_set.AddFunction(make_inout({LogicalType::VARCHAR, LogicalType::BIGINT}));
    crawl_set.AddFunction(make_inout({LogicalType::LIST(LogicalType::VARCHAR), LogicalType::BIGINT}));
    loader.RegisterFunction(crawl_set);
}

} // namespace duckdb
