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

#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/data_chunk.hpp"

namespace duckdb {

using namespace duckdb_yyjson;

//===--------------------------------------------------------------------===//
// Helper: Combine JSON-LD and Microdata into schema object
//===--------------------------------------------------------------------===//

static string CombineSchemaData(const string &jsonld, const string &microdata) {
    yyjson_mut_doc *doc = yyjson_mut_doc_new(nullptr);
    if (!doc) return "{}";

    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);

    // Parse and merge JSON-LD
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

    // Parse and merge microdata
    if (!microdata.empty() && microdata != "{}") {
        yyjson_doc *md_doc = yyjson_read(microdata.c_str(), microdata.size(), 0);
        if (md_doc) {
            yyjson_val *md_root = yyjson_doc_get_root(md_doc);
            if (yyjson_is_obj(md_root)) {
                size_t idx, max;
                yyjson_val *key, *val;
                yyjson_obj_foreach(md_root, idx, max, key, val) {
                    const char *key_str = yyjson_get_str(key);
                    if (!yyjson_mut_obj_get(root, key_str)) {
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

static Value BuildHtmlStructValue(const string &body, const string &content_type) {
    child_list_t<Value> html_values;

    bool is_html = content_type.find("text/html") != string::npos ||
                   content_type.find("application/xhtml") != string::npos;

    if (is_html && !body.empty()) {
#if defined(RUST_PARSER_AVAILABLE) && RUST_PARSER_AVAILABLE
        string js_json = ExtractJsWithRust(body);
        string og_json = ExtractOpenGraphWithRust(body);
        string jsonld_json = ExtractJsonLdWithRust(body);
        string microdata_json = ExtractMicrodataWithRust(body);
        string schema_json = CombineSchemaData(jsonld_json, microdata_json);

        html_values.push_back(make_pair("body", Value(body)));
        html_values.push_back(make_pair("js", MakeJsonValue(js_json)));
        html_values.push_back(make_pair("opengraph", MakeJsonValue(og_json)));
        html_values.push_back(make_pair("schema", MakeJsonValue(schema_json)));
#else
        html_values.push_back(make_pair("body", Value(body)));
        html_values.push_back(make_pair("js", Value(LogicalType::JSON())));
        html_values.push_back(make_pair("opengraph", Value(LogicalType::JSON())));
        html_values.push_back(make_pair("schema", Value(LogicalType::JSON())));
#endif
    } else {
        html_values.push_back(make_pair("body", body.empty() ? Value() : Value(body)));
        html_values.push_back(make_pair("js", Value(LogicalType::JSON())));
        html_values.push_back(make_pair("opengraph", Value(LogicalType::JSON())));
        html_values.push_back(make_pair("schema", Value(LogicalType::JSON())));
    }

    return Value::STRUCT(std::move(html_values));
}

//===--------------------------------------------------------------------===//
// Bind Data for crawl_url
//===--------------------------------------------------------------------===//

struct CrawlUrlBindData : public TableFunctionData {
    vector<CrawlExtractSpec> extract_specs;
    string extraction_request_json;
    string user_agent = "DuckDB-Crawler/1.0";
    int timeout_ms = 30000;

    CrawlUrlBindData() = default;
};

//===--------------------------------------------------------------------===//
// Local State - per-row execution state
//===--------------------------------------------------------------------===//

struct CrawlUrlLocalState : public LocalTableFunctionState {
    bool has_result = false;

    CrawlUrlLocalState() = default;
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

struct SingleCrawlResult {
    string url;
    int status_code = 0;
    string content_type;
    string body;
    string error;
    string extracted_json;
    int64_t response_time_ms = 0;
};

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

    // Named parameters
    for (auto &kv : input.named_parameters) {
        if (kv.first == "extract") {
            auto &spec_list = ListValue::GetChildren(kv.second);
            for (auto &spec_val : spec_list) {
                if (!spec_val.IsNull()) {
                    bind_data->extract_specs.push_back(ParseExtractSpec(StringValue::Get(spec_val)));
                }
            }
            bind_data->extraction_request_json = BuildRustExtractionRequest(bind_data->extract_specs);
        } else if (kv.first == "user_agent") {
            bind_data->user_agent = StringValue::Get(kv.second);
        } else if (kv.first == "timeout") {
            bind_data->timeout_ms = kv.second.GetValue<int>() * 1000;
        }
    }

    // Return columns
    return_types.push_back(LogicalType::VARCHAR);  // url
    return_types.push_back(LogicalType::INTEGER);  // status
    return_types.push_back(LogicalType::VARCHAR);  // content_type

    // html STRUCT(body, js, opengraph, schema) - structured HTML content
    child_list_t<LogicalType> html_struct;
    html_struct.push_back(make_pair("body", LogicalType::VARCHAR));
    html_struct.push_back(make_pair("js", LogicalType::JSON()));        // JSON type
    html_struct.push_back(make_pair("opengraph", LogicalType::JSON())); // JSON type
    html_struct.push_back(make_pair("schema", LogicalType::JSON()));    // Combined JSON-LD + microdata
    return_types.push_back(LogicalType::STRUCT(html_struct));

    return_types.push_back(LogicalType::VARCHAR);  // error
    return_types.push_back(LogicalType::VARCHAR);  // extract
    return_types.push_back(LogicalType::BIGINT);   // response_time_ms

    names.push_back("url");
    names.push_back("status");
    names.push_back("content_type");
    names.push_back("html");
    names.push_back("error");
    names.push_back("extract");
    names.push_back("response_time_ms");

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

    if (input.size() == 0) {
        return OperatorResultType::NEED_MORE_INPUT;
    }

    idx_t count = 0;

    for (idx_t i = 0; i < input.size() && count < STANDARD_VECTOR_SIZE; i++) {
        // Get URL from input column 0
        Value url_val = input.GetValue(0, i);

        if (url_val.IsNull()) {
            // NULL input -> NULL output row with proper struct
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

        // Skip empty URLs
        if (url.empty()) {
            continue;
        }

        // Crawl the URL
        auto result = CrawlSingleUrl(url, bind_data.extraction_request_json,
                                      bind_data.user_agent, bind_data.timeout_ms);

        // Set output values
        output.SetValue(0, count, Value(result.url));
        output.SetValue(1, count, Value(result.status_code));
        output.SetValue(2, count, Value(result.content_type));
        output.SetValue(3, count, BuildHtmlStructValue(result.body, result.content_type));
        output.SetValue(4, count, result.error.empty() ? Value() : Value(result.error));
        output.SetValue(5, count, result.extracted_json.empty() ? Value() : Value(result.extracted_json));
        output.SetValue(6, count, Value::BIGINT(result.response_time_ms));
        count++;
    }

    output.SetCardinality(count);
    return OperatorResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// Register Function
//===--------------------------------------------------------------------===//

void RegisterCrawlUrlFunction(ExtensionLoader &loader) {
    // crawl_url(url VARCHAR) - for LATERAL JOIN
    // First arg is ANY to accept column references
    TableFunction func("crawl_url", {LogicalType::VARCHAR}, nullptr, CrawlUrlBind,
                       CrawlUrlInitGlobal, CrawlUrlInitLocal);

    // Enable in_out_function for lateral join support
    func.in_out_function = CrawlUrlInOut;

    // Named parameters
    func.named_parameters["extract"] = LogicalType::LIST(LogicalType::VARCHAR);
    func.named_parameters["user_agent"] = LogicalType::VARCHAR;
    func.named_parameters["timeout"] = LogicalType::INTEGER;

    loader.RegisterFunction(func);
}

} // namespace duckdb
