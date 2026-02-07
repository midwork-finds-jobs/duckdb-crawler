// crawl_url() - Table function with lateral join support
//
// Enables multi-step crawl pipelines:
//
//   -- Using LATERAL JOIN:
//   WITH seed AS (
//       SELECT url, css_select(body, 'a', 'attr:href') as link
//       FROM crawl(['https://example.com'])
//   )
//   SELECT s.url as source, c.*
//   FROM seed s, LATERAL crawl_url(s.link) c
//
//   -- Or with unnest for multiple links:
//   WITH seed AS (
//       SELECT url, body FROM crawl(['https://example.com'])
//   ),
//   links AS (
//       SELECT url, unnest(string_split(css_select(body, 'a', 'attr:href'), chr(10))) as link
//       FROM seed WHERE link IS NOT NULL
//   )
//   SELECT l.url as source, c.*
//   FROM links l, LATERAL crawl_url(l.link) c

#include "crawl_table_function.hpp"
#include "crawler_utils.hpp"
#include "rust_ffi.hpp"
#include "yyjson.hpp"
#include "pipeline_state.hpp"

#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/data_chunk.hpp"

#include <atomic>
#include <mutex>
#include <unordered_map>

namespace duckdb {

//===--------------------------------------------------------------------===//
// Shared Pipeline State - enables LIMIT pushdown across LATERAL calls
//===--------------------------------------------------------------------===//

// Global registry of pipeline states, keyed by database instance pointer
static std::mutex g_pipeline_mutex;
static std::unordered_map<uintptr_t, std::shared_ptr<PipelineState>> g_pipeline_states;

// Initialize pipeline limit for a database instance (call before running query)
void InitPipelineLimit(DatabaseInstance &db, int64_t limit) {
    uintptr_t key = reinterpret_cast<uintptr_t>(&db);
    std::lock_guard<std::mutex> lock(g_pipeline_mutex);

    auto state = std::make_shared<PipelineState>(limit);
    g_pipeline_states[key] = state;
}

// Get existing pipeline state for a database instance
std::shared_ptr<PipelineState> GetPipelineState(DatabaseInstance &db) {
    uintptr_t key = reinterpret_cast<uintptr_t>(&db);
    std::lock_guard<std::mutex> lock(g_pipeline_mutex);

    auto it = g_pipeline_states.find(key);
    if (it != g_pipeline_states.end()) {
        return it->second;
    }
    return nullptr;
}

// Clear pipeline state for a database instance
void ClearPipelineState(DatabaseInstance &db) {
    uintptr_t key = reinterpret_cast<uintptr_t>(&db);
    std::lock_guard<std::mutex> lock(g_pipeline_mutex);
    g_pipeline_states.erase(key);
}

using namespace duckdb_yyjson;

//===--------------------------------------------------------------------===//
// Helper: Crawl single URL using Rust (forward declaration)
//===--------------------------------------------------------------------===//

struct SingleCrawlResult {
    string url;
    string final_url;
    int status_code = 0;
    string content_type;
    string body;
    string error;
    string extracted_json;
    int64_t response_time_ms = 0;
};

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

static unique_ptr<SingleCrawlResult> GetCachedEntry(Connection &conn, const string &url, int ttl_hours) {
    EnsureCacheTable(conn);
    auto result = conn.Query(
        "SELECT url, status_code, content_type, body, error, response_time_ms "
        "FROM " + string(CACHE_TABLE_NAME) + " "
        "WHERE url = $1 AND cached_at > current_timestamp - INTERVAL '" + std::to_string(ttl_hours) + " hours'",
        url);

    if (!result->HasError()) {
        auto chunk = result->Fetch();
        if (chunk && chunk->size() > 0) {
            auto entry = make_uniq<SingleCrawlResult>();
            entry->url = chunk->GetValue(0, 0).ToString();
            entry->status_code = chunk->GetValue(1, 0).GetValue<int>();
            entry->content_type = chunk->GetValue(2, 0).IsNull() ? "" : chunk->GetValue(2, 0).ToString();
            entry->body = chunk->GetValue(3, 0).IsNull() ? "" : chunk->GetValue(3, 0).ToString();
            entry->error = chunk->GetValue(4, 0).IsNull() ? "" : chunk->GetValue(4, 0).ToString();
            entry->response_time_ms = chunk->GetValue(5, 0).IsNull() ? 0 : chunk->GetValue(5, 0).GetValue<int64_t>();
            return entry;
        }
    }
    return nullptr;
}

static void SaveToCache(Connection &conn, const SingleCrawlResult &result) {
    EnsureCacheTable(conn);
    string sql = "INSERT OR REPLACE INTO " + string(CACHE_TABLE_NAME) +
                 " (url, status_code, content_type, body, error, response_time_ms, cached_at) "
                 "VALUES ($1, $2, $3, $4, $5, $6, current_timestamp)";
    conn.Query(sql, result.url, result.status_code,
               result.content_type.empty() ? Value() : Value(result.content_type),
               result.body.empty() ? Value() : Value(result.body),
               result.error.empty() ? Value() : Value(result.error),
               result.response_time_ms);
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

    if (keys.empty()) {
        return Value::MAP(LogicalType::VARCHAR, LogicalType::JSON(), vector<Value>(), vector<Value>());
    }

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

        html_values.push_back(make_pair("document", Value(body)));
        html_values.push_back(make_pair("js", MakeJsonValue(js_json)));
        html_values.push_back(make_pair("meta", MakeJsonValue(meta_json)));
        html_values.push_back(make_pair("opengraph", MakeJsonValue(og_json)));
        html_values.push_back(make_pair("schema", MakeSchemaMapValue(schema_json)));
        html_values.push_back(make_pair("readability", MakeJsonValue(readability_json)));
#else
        html_values.push_back(make_pair("document", Value(body)));
        html_values.push_back(make_pair("js", Value(LogicalType::JSON())));
        html_values.push_back(make_pair("meta", Value(LogicalType::JSON())));
        html_values.push_back(make_pair("opengraph", Value(LogicalType::JSON())));
        html_values.push_back(make_pair("schema", Value::MAP(LogicalType::VARCHAR, LogicalType::JSON(), vector<Value>(), vector<Value>())));
        html_values.push_back(make_pair("readability", Value(LogicalType::JSON())));
#endif
    } else {
        html_values.push_back(make_pair("document", body.empty() ? Value() : Value(body)));
        html_values.push_back(make_pair("js", Value(LogicalType::JSON())));
        html_values.push_back(make_pair("meta", Value(LogicalType::JSON())));
        html_values.push_back(make_pair("opengraph", Value(LogicalType::JSON())));
        html_values.push_back(make_pair("schema", Value::MAP(LogicalType::VARCHAR, LogicalType::JSON(), vector<Value>(), vector<Value>())));
        html_values.push_back(make_pair("readability", Value(LogicalType::JSON())));
    }

    return Value::STRUCT(std::move(html_values));
}

//===--------------------------------------------------------------------===//
// Bind Data for crawl_url
//===--------------------------------------------------------------------===//

struct CrawlUrlBindData : public TableFunctionData {
    string user_agent = "DuckDB-Crawler/1.0";
    int timeout_ms = 30000;
    bool use_cache = true;      // Enable HTTP response caching
    int cache_ttl_hours = 24;   // Cache TTL in hours
    int64_t max_results = -1;   // Max results to return (-1 = unlimited)

    // Shared pipeline state for LIMIT pushdown across LATERAL calls
    std::shared_ptr<PipelineState> pipeline_state;

    CrawlUrlBindData() = default;
};

//===--------------------------------------------------------------------===//
// Local State - per-row execution state (streaming for LIMIT pushdown)
//===--------------------------------------------------------------------===//

struct CrawlUrlLocalState : public LocalTableFunctionState {
    idx_t current_row = 0;      // Current position in input chunk
    idx_t input_size = 0;       // Size of current input chunk
    bool chunk_initialized = false;
    int64_t results_returned = 0;  // Total results returned (for max_results)

    CrawlUrlLocalState() = default;

    void Reset() {
        current_row = 0;
        input_size = 0;
        chunk_initialized = false;
        // Note: results_returned persists across chunks
    }
};

//===--------------------------------------------------------------------===//
// Global State
//===--------------------------------------------------------------------===//

struct CrawlUrlGlobalState : public GlobalTableFunctionState {
    idx_t MaxThreads() const override { return 1; }
};

//===--------------------------------------------------------------------===//
// Helper: Crawl single URL using Rust
//===--------------------------------------------------------------------===//

static SingleCrawlResult CrawlSingleUrl(const string &url,
                                         const string &extraction_json,
                                         const string &user_agent,
                                         int timeout_ms) {
    SingleCrawlResult result;
    result.url = url;

    // Build minimal batch request for single URL
    yyjson_mut_doc *doc = yyjson_mut_doc_new(nullptr);
    if (!doc) {
        result.error = "Failed to create JSON request";
        return result;
    }

    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);

    // URLs array (single URL)
    yyjson_mut_val *urls_arr = yyjson_mut_arr(doc);
    yyjson_mut_arr_add_strcpy(doc, urls_arr, url.c_str());
    yyjson_mut_obj_add_val(doc, root, "urls", urls_arr);

    // Extraction specs
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
    yyjson_mut_obj_add_uint(doc, root, "concurrency", 1);
    yyjson_mut_obj_add_uint(doc, root, "delay_ms", 0);

    size_t len = 0;
    char *json_str = yyjson_mut_write(doc, 0, &len);
    yyjson_mut_doc_free(doc);

    if (!json_str) {
        result.error = "Failed to serialize JSON request";
        return result;
    }

    string request_json(json_str, len);
    free(json_str);

    // Call Rust
    string response_json = CrawlBatchWithRust(request_json);

    // Parse response
    yyjson_doc *resp_doc = yyjson_read(response_json.c_str(), response_json.size(), 0);
    if (!resp_doc) {
        result.error = "Failed to parse Rust response";
        return result;
    }

    yyjson_val *resp_root = yyjson_doc_get_root(resp_doc);

    // Check for error
    yyjson_val *error_val = yyjson_obj_get(resp_root, "error");
    if (error_val && yyjson_is_str(error_val)) {
        result.error = yyjson_get_str(error_val);
        yyjson_doc_free(resp_doc);
        return result;
    }

    // Get first result
    yyjson_val *results_arr = yyjson_obj_get(resp_root, "results");
    if (results_arr && yyjson_is_arr(results_arr) && yyjson_arr_size(results_arr) > 0) {
        yyjson_val *item = yyjson_arr_get_first(results_arr);

        yyjson_val *status_val = yyjson_obj_get(item, "status");
        if (status_val && yyjson_is_int(status_val)) {
            result.status_code = (int)yyjson_get_int(status_val);
        }

        yyjson_val *ct_val = yyjson_obj_get(item, "content_type");
        if (ct_val && yyjson_is_str(ct_val)) {
            result.content_type = yyjson_get_str(ct_val);
        }

        yyjson_val *body_val = yyjson_obj_get(item, "body");
        if (body_val && yyjson_is_str(body_val)) {
            result.body = yyjson_get_str(body_val);
        }

        yyjson_val *final_url_val = yyjson_obj_get(item, "final_url");
        if (final_url_val && yyjson_is_str(final_url_val)) {
            result.final_url = yyjson_get_str(final_url_val);
        }

        yyjson_val *err_val = yyjson_obj_get(item, "error");
        if (err_val && yyjson_is_str(err_val)) {
            result.error = yyjson_get_str(err_val);
        }

        yyjson_val *time_val = yyjson_obj_get(item, "response_time_ms");
        if (time_val && yyjson_is_uint(time_val)) {
            result.response_time_ms = (int64_t)yyjson_get_uint(time_val);
        }

        yyjson_val *extracted = yyjson_obj_get(item, "extracted");
        if (extracted && !yyjson_is_null(extracted)) {
            size_t ext_len = 0;
            char *ext_str = yyjson_val_write(extracted, 0, &ext_len);
            if (ext_str) {
                result.extracted_json = string(ext_str, ext_len);
                free(ext_str);
            }
        }
    }

    yyjson_doc_free(resp_doc);
    return result;
}

//===--------------------------------------------------------------------===//
// Bind Function
//===--------------------------------------------------------------------===//

static unique_ptr<FunctionData> CrawlUrlBind(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<string> &names) {
    auto bind_data = make_uniq<CrawlUrlBindData>();

    // Read extension settings as defaults
    Value setting_value;
    if (context.TryGetCurrentSetting("crawler_user_agent", setting_value)) {
        bind_data->user_agent = setting_value.ToString();
    }
    if (context.TryGetCurrentSetting("crawler_timeout_ms", setting_value)) {
        bind_data->timeout_ms = static_cast<int>(setting_value.GetValue<int64_t>());
    }

    // Check for optional second positional argument (max_results)
    // This enables LIMIT pushdown in LATERAL joins where named params don't work
    if (input.inputs.size() > 1 && !input.inputs[1].IsNull()) {
        bind_data->max_results = input.inputs[1].GetValue<int64_t>();
    }

    // Named parameters
    for (auto &kv : input.named_parameters) {
        if (kv.first == "user_agent") {
            bind_data->user_agent = StringValue::Get(kv.second);
        } else if (kv.first == "timeout") {
            bind_data->timeout_ms = kv.second.GetValue<int>() * 1000;
        } else if (kv.first == "cache") {
            bind_data->use_cache = kv.second.GetValue<bool>();
        } else if (kv.first == "cache_ttl") {
            bind_data->cache_ttl_hours = kv.second.GetValue<int>();
        } else if (kv.first == "max_results") {
            bind_data->max_results = kv.second.GetValue<int64_t>();
        }
    }

    // Return columns
    return_types.push_back(LogicalType::VARCHAR);  // url
    return_types.push_back(LogicalType::INTEGER);  // status
    return_types.push_back(LogicalType::VARCHAR);  // content_type

    // html STRUCT(document, js, meta, opengraph, schema, readability) - structured HTML content
    child_list_t<LogicalType> html_struct;
    html_struct.push_back(make_pair("document", LogicalType::VARCHAR));
    html_struct.push_back(make_pair("js", LogicalType::JSON()));        // JSON type
    html_struct.push_back(make_pair("meta", LogicalType::JSON()));      // Meta tags JSON
    html_struct.push_back(make_pair("opengraph", LogicalType::JSON())); // JSON type
    html_struct.push_back(make_pair("schema", LogicalType::MAP(LogicalType::VARCHAR, LogicalType::JSON())));  // MAP for schema['Product'] access
    html_struct.push_back(make_pair("readability", LogicalType::JSON()));  // Readability extracted content
    return_types.push_back(LogicalType::STRUCT(html_struct));

    return_types.push_back(LogicalType::VARCHAR);  // final_url
    return_types.push_back(LogicalType::VARCHAR);  // error
    return_types.push_back(LogicalType::VARCHAR);  // extract
    return_types.push_back(LogicalType::BIGINT);   // response_time_ms

    names.push_back("url");
    names.push_back("status");
    names.push_back("content_type");
    names.push_back("html");
    names.push_back("final_url");
    names.push_back("error");
    names.push_back("extract");
    names.push_back("response_time_ms");

    // Look up shared pipeline state for LIMIT pushdown across LATERAL calls
    // The state is created by stream_into_function BEFORE running the query
    bind_data->pipeline_state = GetPipelineState(*context.db);
    if (!bind_data->pipeline_state && bind_data->max_results > 0) {
        // Fallback: create state if max_results is set directly (non-LATERAL case)
        InitPipelineLimit(*context.db, bind_data->max_results);
        bind_data->pipeline_state = GetPipelineState(*context.db);
    }

    return std::move(bind_data);
}

//===--------------------------------------------------------------------===//
// Init Functions
//===--------------------------------------------------------------------===//

static unique_ptr<GlobalTableFunctionState> CrawlUrlInitGlobal(ClientContext &context,
                                                                 TableFunctionInitInput &input) {
    return make_uniq<CrawlUrlGlobalState>();
}

static unique_ptr<LocalTableFunctionState> CrawlUrlInitLocal(ExecutionContext &context,
                                                               TableFunctionInitInput &input,
                                                               GlobalTableFunctionState *global_state) {
    return make_uniq<CrawlUrlLocalState>();
}

//===--------------------------------------------------------------------===//
// In-Out Function - processes each input row (for LATERAL JOIN)
//===--------------------------------------------------------------------===//

static OperatorResultType CrawlUrlInOut(ExecutionContext &context, TableFunctionInput &data,
                                         DataChunk &input, DataChunk &output) {
    auto &bind_data = data.bind_data->CastNoConst<CrawlUrlBindData>();
    auto &local_state = data.local_state->Cast<CrawlUrlLocalState>();

    // Initialize chunk tracking on new input
    if (!local_state.chunk_initialized) {
        local_state.current_row = 0;
        local_state.input_size = input.size();
        local_state.chunk_initialized = true;
    }

    if (local_state.input_size == 0) {
        local_state.Reset();
        return OperatorResultType::NEED_MORE_INPUT;
    }

    // Process ONE URL at a time for LIMIT pushdown support
    // This allows executor to stop between HTTP requests when LIMIT is reached
    while (local_state.current_row < local_state.input_size) {
        // Check shared pipeline state (LIMIT pushdown across LATERAL calls)
        if (bind_data.pipeline_state && bind_data.pipeline_state->stopped.load()) {
            // Skip remaining rows - limit reached across all LATERAL calls
            local_state.current_row = local_state.input_size;
            output.SetCardinality(0);
            local_state.Reset();
            return OperatorResultType::NEED_MORE_INPUT;
        }

        // Check local max_results limit (fallback for non-shared mode)
        if (bind_data.max_results >= 0 && local_state.results_returned >= bind_data.max_results) {
            output.SetCardinality(0);
            return OperatorResultType::FINISHED;
        }

        idx_t i = local_state.current_row;

        // Get URL from input column 0
        Value url_val = input.GetValue(0, i);

        if (url_val.IsNull()) {
            // NULL input -> NULL output row with proper struct
            output.SetValue(0, 0, Value());
            output.SetValue(1, 0, Value());
            output.SetValue(2, 0, Value());
            output.SetValue(3, 0, BuildHtmlStructValue("", "", ""));
            output.SetValue(4, 0, Value());
            output.SetValue(5, 0, Value("NULL URL"));
            output.SetValue(6, 0, Value());
            output.SetValue(7, 0, Value());
            output.SetCardinality(1);
            local_state.current_row++;
            local_state.results_returned++;

            // Decrement shared pipeline counter (for LIMIT pushdown across LATERAL)
            if (bind_data.pipeline_state) {
                int64_t remaining = --bind_data.pipeline_state->remaining;
                if (remaining <= 0) {
                    bind_data.pipeline_state->stopped = true;
                }
            }

            // More rows in chunk? Return HAVE_MORE_OUTPUT to allow LIMIT to interrupt
            if (local_state.current_row < local_state.input_size) {
                return OperatorResultType::HAVE_MORE_OUTPUT;
            }
            local_state.Reset();
            return OperatorResultType::NEED_MORE_INPUT;
        }

        string url = StringValue::Get(url_val);

        // Skip empty URLs
        if (url.empty()) {
            local_state.current_row++;
            continue;
        }

        SingleCrawlResult result;
        bool from_cache = false;

        // Check cache first
        if (bind_data.use_cache) {
            Connection cache_conn(*context.client.db);
            auto cached = GetCachedEntry(cache_conn, url, bind_data.cache_ttl_hours);
            if (cached) {
                result = std::move(*cached);
                from_cache = true;
            }
        }

        // Crawl if not in cache
        if (!from_cache) {
            result = CrawlSingleUrl(url, "{}",  // No extraction specs
                                    bind_data.user_agent, bind_data.timeout_ms);

            // Save to cache
            if (bind_data.use_cache) {
                Connection cache_conn(*context.client.db);
                SaveToCache(cache_conn, result);
            }
        }

        // Set output values (single row)
        output.SetValue(0, 0, Value(result.url));
        output.SetValue(1, 0, Value(result.status_code));
        output.SetValue(2, 0, Value(result.content_type));
        output.SetValue(3, 0, BuildHtmlStructValue(result.body, result.content_type, result.url));
        output.SetValue(4, 0, result.final_url.empty() ? Value() : Value(result.final_url));
        output.SetValue(5, 0, result.error.empty() ? Value() : Value(result.error));
        output.SetValue(6, 0, result.extracted_json.empty() ? Value() : Value(result.extracted_json));
        output.SetValue(7, 0, Value::BIGINT(result.response_time_ms));
        output.SetCardinality(1);

        local_state.current_row++;
        local_state.results_returned++;

        // Decrement shared pipeline counter (for LIMIT pushdown across LATERAL)
        if (bind_data.pipeline_state) {
            int64_t remaining = --bind_data.pipeline_state->remaining;
            if (remaining <= 0) {
                bind_data.pipeline_state->stopped = true;
            }
        }

        // More rows in chunk? Return HAVE_MORE_OUTPUT to allow LIMIT to interrupt
        if (local_state.current_row < local_state.input_size) {
            return OperatorResultType::HAVE_MORE_OUTPUT;
        }

        // Chunk exhausted
        local_state.Reset();
        return OperatorResultType::NEED_MORE_INPUT;
    }

    // Should not reach here, but reset and request more input
    local_state.Reset();
    return OperatorResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// Register Function
//===--------------------------------------------------------------------===//

void RegisterCrawlUrlFunction(ExtensionLoader &loader) {
    // crawl_url(url VARCHAR) - for LATERAL JOIN
    TableFunction func("crawl_url", {LogicalType::VARCHAR}, nullptr, CrawlUrlBind,
                       CrawlUrlInitGlobal, CrawlUrlInitLocal);
    func.in_out_function = CrawlUrlInOut;

    // Named parameters
    func.named_parameters["extract"] = LogicalType::LIST(LogicalType::VARCHAR);
    func.named_parameters["user_agent"] = LogicalType::VARCHAR;
    func.named_parameters["timeout"] = LogicalType::INTEGER;
    func.named_parameters["cache"] = LogicalType::BOOLEAN;
    func.named_parameters["cache_ttl"] = LogicalType::INTEGER;
    func.named_parameters["max_results"] = LogicalType::BIGINT;

    loader.RegisterFunction(func);

    // crawl_url(url VARCHAR, max_results BIGINT) - for LATERAL with LIMIT pushdown
    // Named params don't work in LATERAL, so we need positional arg for max_results
    TableFunction func_with_limit("crawl_url", {LogicalType::VARCHAR, LogicalType::BIGINT},
                                   nullptr, CrawlUrlBind, CrawlUrlInitGlobal, CrawlUrlInitLocal);
    func_with_limit.in_out_function = CrawlUrlInOut;
    func_with_limit.named_parameters["extract"] = LogicalType::LIST(LogicalType::VARCHAR);
    func_with_limit.named_parameters["user_agent"] = LogicalType::VARCHAR;
    func_with_limit.named_parameters["timeout"] = LogicalType::INTEGER;
    func_with_limit.named_parameters["cache"] = LogicalType::BOOLEAN;
    func_with_limit.named_parameters["cache_ttl"] = LogicalType::INTEGER;

    loader.RegisterFunction(func_with_limit);
}

} // namespace duckdb
