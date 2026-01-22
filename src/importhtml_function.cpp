// read_html() table function for DuckDB Crawler
// Similar to Google Sheets =IMPORTHTML() - extracts tables from web pages

#include "importhtml_function.hpp"
#include "rust_ffi.hpp"
#include "yyjson.hpp"
#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include <set>

namespace duckdb {

using namespace duckdb_yyjson;

//===--------------------------------------------------------------------===//
// Bind Data
//===--------------------------------------------------------------------===//

// Column type detected during inference
enum class InferredType : uint8_t {
    VARCHAR = 0,
    BIGINT = 1,
    DOUBLE = 2
};

struct ReadHtmlBindData : public TableFunctionData {
    string url;
    string selector;
    size_t table_index = 0;  // 0-based index of which matching table to extract
    string user_agent = "DuckDB-Crawler/1.0";
    int timeout_ms = 30000;

    // Extracted table data (populated during bind)
    vector<string> headers;
    vector<vector<string>> rows;
    vector<InferredType> column_types;  // Inferred types per column
    idx_t num_columns = 0;
    idx_t num_rows = 0;
    string error;
};

//===--------------------------------------------------------------------===//
// Global State
//===--------------------------------------------------------------------===//

struct ReadHtmlGlobalState : public GlobalTableFunctionState {
    idx_t current_row = 0;

    idx_t MaxThreads() const override { return 1; }
};

//===--------------------------------------------------------------------===//
// Helper: Extract JS variable as table
//===--------------------------------------------------------------------===//

static void ExtractJsVariable(const string &html, ReadHtmlBindData &bind_data) {
    // Use ExtractPathWithRust to get the JS variable
    // Selector format: @$varname or script@$varname
    string json_result = ExtractPathWithRust(html, bind_data.selector);

    if (json_result.empty() || json_result == "null") {
        bind_data.error = "JS variable not found: " + bind_data.selector;
        return;
    }

    // Parse the JSON result
    yyjson_doc *doc = yyjson_read(json_result.c_str(), json_result.length(), 0);
    if (!doc) {
        bind_data.error = "Failed to parse JS variable as JSON";
        return;
    }

    yyjson_val *root = yyjson_doc_get_root(doc);

    // Handle array of objects -> table with columns from object keys
    if (yyjson_is_arr(root)) {
        // First pass: collect all unique keys from all objects
        std::set<string> all_keys;
        size_t idx, max_idx;
        yyjson_val *item;

        yyjson_arr_foreach(root, idx, max_idx, item) {
            if (yyjson_is_obj(item)) {
                size_t key_idx, key_max;
                yyjson_val *key, *val;
                yyjson_obj_foreach(item, key_idx, key_max, key, val) {
                    if (yyjson_is_str(key)) {
                        all_keys.insert(yyjson_get_str(key));
                    }
                }
            }
        }

        if (all_keys.empty()) {
            // Array of non-objects, treat as single column
            bind_data.headers.push_back("value");
            yyjson_arr_foreach(root, idx, max_idx, item) {
                vector<string> row;
                if (yyjson_is_str(item)) {
                    row.push_back(yyjson_get_str(item));
                } else if (yyjson_is_num(item)) {
                    char buf[64];
                    snprintf(buf, sizeof(buf), "%g", yyjson_get_num(item));
                    row.push_back(buf);
                } else if (yyjson_is_bool(item)) {
                    row.push_back(yyjson_get_bool(item) ? "true" : "false");
                } else if (yyjson_is_null(item)) {
                    row.push_back("");
                } else {
                    // For nested objects/arrays, serialize as JSON
                    size_t len;
                    char *json_str = yyjson_val_write(item, 0, &len);
                    if (json_str) {
                        row.push_back(string(json_str, len));
                        free(json_str);
                    } else {
                        row.push_back("");
                    }
                }
                bind_data.rows.push_back(std::move(row));
            }
        } else {
            // Array of objects - use keys as headers
            for (const auto &key : all_keys) {
                bind_data.headers.push_back(key);
            }

            // Second pass: extract values for each object
            yyjson_arr_foreach(root, idx, max_idx, item) {
                vector<string> row;
                for (const auto &key : all_keys) {
                    yyjson_val *val = yyjson_obj_get(item, key.c_str());
                    if (!val || yyjson_is_null(val)) {
                        row.push_back("");
                    } else if (yyjson_is_str(val)) {
                        row.push_back(yyjson_get_str(val));
                    } else if (yyjson_is_num(val)) {
                        char buf[64];
                        snprintf(buf, sizeof(buf), "%g", yyjson_get_num(val));
                        row.push_back(buf);
                    } else if (yyjson_is_bool(val)) {
                        row.push_back(yyjson_get_bool(val) ? "true" : "false");
                    } else {
                        // For nested objects/arrays, serialize as JSON
                        size_t len;
                        char *json_str = yyjson_val_write(val, 0, &len);
                        if (json_str) {
                            row.push_back(string(json_str, len));
                            free(json_str);
                        } else {
                            row.push_back("");
                        }
                    }
                }
                bind_data.rows.push_back(std::move(row));
            }
        }
    } else if (yyjson_is_obj(root)) {
        // Single object - each key becomes a row with key/value columns
        bind_data.headers.push_back("key");
        bind_data.headers.push_back("value");

        size_t key_idx, key_max;
        yyjson_val *key, *val;
        yyjson_obj_foreach(root, key_idx, key_max, key, val) {
            vector<string> row;
            row.push_back(yyjson_is_str(key) ? yyjson_get_str(key) : "");

            if (yyjson_is_str(val)) {
                row.push_back(yyjson_get_str(val));
            } else if (yyjson_is_num(val)) {
                char buf[64];
                snprintf(buf, sizeof(buf), "%g", yyjson_get_num(val));
                row.push_back(buf);
            } else if (yyjson_is_bool(val)) {
                row.push_back(yyjson_get_bool(val) ? "true" : "false");
            } else if (yyjson_is_null(val)) {
                row.push_back("");
            } else {
                size_t len;
                char *json_str = yyjson_val_write(val, 0, &len);
                if (json_str) {
                    row.push_back(string(json_str, len));
                    free(json_str);
                } else {
                    row.push_back("");
                }
            }
            bind_data.rows.push_back(std::move(row));
        }
    } else {
        // Scalar value - single row, single column
        bind_data.headers.push_back("value");
        vector<string> row;
        if (yyjson_is_str(root)) {
            row.push_back(yyjson_get_str(root));
        } else if (yyjson_is_num(root)) {
            char buf[64];
            snprintf(buf, sizeof(buf), "%g", yyjson_get_num(root));
            row.push_back(buf);
        } else if (yyjson_is_bool(root)) {
            row.push_back(yyjson_get_bool(root) ? "true" : "false");
        } else {
            row.push_back("");
        }
        bind_data.rows.push_back(std::move(row));
    }

    bind_data.num_columns = bind_data.headers.size();
    bind_data.num_rows = bind_data.rows.size();

    yyjson_doc_free(doc);
}

//===--------------------------------------------------------------------===//
// Helper: Fetch page and extract table
//===--------------------------------------------------------------------===//

static void FetchAndExtractTable(ReadHtmlBindData &bind_data) {
    // Build request JSON for CrawlBatchWithRust
    yyjson_mut_doc *doc = yyjson_mut_doc_new(nullptr);
    if (!doc) {
        bind_data.error = "Failed to create JSON document";
        return;
    }

    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);

    // URLs array
    yyjson_mut_val *urls_arr = yyjson_mut_arr(doc);
    yyjson_mut_arr_add_strcpy(doc, urls_arr, bind_data.url.c_str());
    yyjson_mut_obj_add_val(doc, root, "urls", urls_arr);

    yyjson_mut_obj_add_strcpy(doc, root, "user_agent", bind_data.user_agent.c_str());
    yyjson_mut_obj_add_uint(doc, root, "timeout_ms", bind_data.timeout_ms);
    yyjson_mut_obj_add_uint(doc, root, "concurrency", 1);

    size_t len = 0;
    char *json_str = yyjson_mut_write(doc, 0, &len);
    yyjson_mut_doc_free(doc);

    if (!json_str) {
        bind_data.error = "Failed to serialize request";
        return;
    }

    string request_json(json_str, len);
    free(json_str);

    // Fetch the page
    string response_json = CrawlBatchWithRust(request_json);

    // Parse response
    yyjson_doc *resp_doc = yyjson_read(response_json.c_str(), response_json.length(), 0);
    if (!resp_doc) {
        bind_data.error = "Failed to parse crawl response";
        return;
    }

    yyjson_val *resp_root = yyjson_doc_get_root(resp_doc);
    yyjson_val *results = yyjson_obj_get(resp_root, "results");

    if (!results || !yyjson_is_arr(results) || yyjson_arr_size(results) == 0) {
        yyjson_doc_free(resp_doc);
        bind_data.error = "No results from crawl";
        return;
    }

    yyjson_val *first_result = yyjson_arr_get_first(results);
    if (!first_result) {
        yyjson_doc_free(resp_doc);
        bind_data.error = "Empty result";
        return;
    }

    // Check for error
    yyjson_val *error_val = yyjson_obj_get(first_result, "error");
    if (error_val && !yyjson_is_null(error_val)) {
        bind_data.error = yyjson_get_str(error_val);
        yyjson_doc_free(resp_doc);
        return;
    }

    // Get body
    yyjson_val *body_val = yyjson_obj_get(first_result, "body");
    if (!body_val || !yyjson_is_str(body_val)) {
        yyjson_doc_free(resp_doc);
        bind_data.error = "No body in response";
        return;
    }

    string html = yyjson_get_str(body_val);
    yyjson_doc_free(resp_doc);

    if (html.empty()) {
        bind_data.error = "Empty HTML body";
        return;
    }

    // Check if selector is a JS variable path
    // Syntax: js=varname or script@js=varname (= is illegal in JS variable names)
    bool is_js_var = bind_data.selector.find("js=") != string::npos;

    if (is_js_var) {
        // Convert js=varname to @$varname for ExtractPathWithRust
        string converted_selector = bind_data.selector;
        size_t pos = converted_selector.find("js=");
        if (pos != string::npos) {
            // Replace js= with @$ for the Rust extractor
            converted_selector.replace(pos, 3, "@$");
        }
        // Temporarily swap selector for extraction
        string original_selector = bind_data.selector;
        bind_data.selector = converted_selector;
        ExtractJsVariable(html, bind_data);
        bind_data.selector = original_selector;  // Restore for error messages
        return;
    }

    // Extract table (pass URL for Wikipedia-specific handling, table_index for nth match)
    string table_json = ExtractTableWithRust(html, bind_data.selector, bind_data.url, bind_data.table_index);

    // Parse table JSON
    yyjson_doc *table_doc = yyjson_read(table_json.c_str(), table_json.length(), 0);
    if (!table_doc) {
        bind_data.error = "Failed to parse table extraction result";
        return;
    }

    yyjson_val *table_root = yyjson_doc_get_root(table_doc);

    // Check for extraction error
    yyjson_val *table_error = yyjson_obj_get(table_root, "error");
    if (table_error && !yyjson_is_null(table_error)) {
        bind_data.error = yyjson_get_str(table_error);
        yyjson_doc_free(table_doc);
        return;
    }

    // Get headers
    yyjson_val *headers_arr = yyjson_obj_get(table_root, "headers");
    if (headers_arr && yyjson_is_arr(headers_arr)) {
        size_t idx, max_idx;
        yyjson_val *val;
        yyjson_arr_foreach(headers_arr, idx, max_idx, val) {
            if (yyjson_is_str(val)) {
                bind_data.headers.push_back(yyjson_get_str(val));
            } else {
                bind_data.headers.push_back("");
            }
        }
    }

    // Get rows
    yyjson_val *rows_arr = yyjson_obj_get(table_root, "rows");
    if (rows_arr && yyjson_is_arr(rows_arr)) {
        size_t row_idx, row_max;
        yyjson_val *row_val;
        yyjson_arr_foreach(rows_arr, row_idx, row_max, row_val) {
            if (!yyjson_is_arr(row_val)) continue;

            vector<string> row;
            size_t col_idx, col_max;
            yyjson_val *cell_val;
            yyjson_arr_foreach(row_val, col_idx, col_max, cell_val) {
                if (yyjson_is_str(cell_val)) {
                    row.push_back(yyjson_get_str(cell_val));
                } else {
                    row.push_back("");
                }
            }
            bind_data.rows.push_back(std::move(row));
        }
    }

    bind_data.num_columns = bind_data.headers.size();
    bind_data.num_rows = bind_data.rows.size();

    yyjson_doc_free(table_doc);
}

//===--------------------------------------------------------------------===//
// Type Inference Helpers
//===--------------------------------------------------------------------===//

// Try to parse string as integer
static bool TryParseBigInt(const string &str, int64_t &result) {
    if (str.empty()) return true;  // Empty is compatible with any type

    char *end;
    errno = 0;
    long long val = strtoll(str.c_str(), &end, 10);

    // Must consume entire string (no trailing chars except whitespace)
    while (*end && isspace(*end)) end++;
    if (*end != '\0' || errno == ERANGE) return false;

    result = static_cast<int64_t>(val);
    return true;
}

// Try to parse string as double
static bool TryParseDouble(const string &str, double &result) {
    if (str.empty()) return true;  // Empty is compatible with any type

    char *end;
    errno = 0;
    double val = strtod(str.c_str(), &end);

    // Must consume entire string (no trailing chars except whitespace)
    while (*end && isspace(*end)) end++;
    if (*end != '\0' || errno == ERANGE) return false;

    result = val;
    return true;
}

// Infer column types based on all values
static void InferColumnTypes(ReadHtmlBindData &bind_data) {
    bind_data.column_types.resize(bind_data.num_columns, InferredType::BIGINT);  // Start optimistic

    for (idx_t col = 0; col < bind_data.num_columns; col++) {
        bool all_integers = true;
        bool all_doubles = true;
        bool has_non_empty = false;

        for (const auto &row : bind_data.rows) {
            if (col >= row.size()) continue;
            const string &val = row[col];
            if (val.empty()) continue;

            has_non_empty = true;

            int64_t int_val;
            double dbl_val;

            if (!TryParseBigInt(val, int_val)) {
                all_integers = false;
            }
            if (!TryParseDouble(val, dbl_val)) {
                all_doubles = false;
            }

            // Early exit if neither works
            if (!all_integers && !all_doubles) {
                break;
            }
        }

        // Determine final type
        if (!has_non_empty) {
            // All empty - keep as VARCHAR
            bind_data.column_types[col] = InferredType::VARCHAR;
        } else if (all_integers) {
            bind_data.column_types[col] = InferredType::BIGINT;
        } else if (all_doubles) {
            bind_data.column_types[col] = InferredType::DOUBLE;
        } else {
            bind_data.column_types[col] = InferredType::VARCHAR;
        }
    }
}

//===--------------------------------------------------------------------===//
// Bind Function
//===--------------------------------------------------------------------===//

static unique_ptr<FunctionData> ReadHtmlBind(ClientContext &context,
                                                TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types,
                                                vector<string> &names) {
    auto bind_data = make_uniq<ReadHtmlBindData>();

    // First argument: URL
    if (input.inputs[0].IsNull()) {
        throw BinderException("read_html() requires a URL argument");
    }
    bind_data->url = StringValue::Get(input.inputs[0]);

    // Second argument: CSS selector
    if (input.inputs.size() < 2 || input.inputs[1].IsNull()) {
        throw BinderException("read_html() requires a CSS selector argument (e.g., 'table', 'table.wikitable')");
    }
    bind_data->selector = StringValue::Get(input.inputs[1]);

    // Third argument (optional): table index (1-based like Google Sheets IMPORTHTML)
    if (input.inputs.size() >= 3 && !input.inputs[2].IsNull()) {
        int64_t idx = input.inputs[2].GetValue<int64_t>();
        if (idx < 1) {
            throw BinderException("read_html() index must be >= 1 (1-based, like Google Sheets =IMPORTHTML)");
        }
        bind_data->table_index = static_cast<size_t>(idx - 1);  // Convert to 0-based
    }

    // Named parameters
    for (auto &kv : input.named_parameters) {
        if (kv.first == "user_agent") {
            bind_data->user_agent = StringValue::Get(kv.second);
        } else if (kv.first == "timeout") {
            bind_data->timeout_ms = kv.second.GetValue<int>() * 1000;
        }
    }

    // Fetch and extract table during bind to determine schema
    FetchAndExtractTable(*bind_data);

    if (!bind_data->error.empty()) {
        throw BinderException("read_html() failed: " + bind_data->error);
    }

    if (bind_data->num_columns == 0) {
        throw BinderException("read_html() found no columns in the table");
    }

    // Infer column types based on data
    InferColumnTypes(*bind_data);

    // Define columns based on extracted headers and inferred types
    for (idx_t i = 0; i < bind_data->headers.size(); i++) {
        // Sanitize header name for SQL compatibility
        string col_name = bind_data->headers[i];
        if (col_name.empty()) {
            col_name = "column" + std::to_string(names.size() + 1);
        }
        // Replace spaces and special chars with underscores
        for (auto &c : col_name) {
            if (c == ' ' || c == '-' || c == '/' || c == '\\' || c == '(' || c == ')' || c == ',') {
                c = '_';
            }
        }
        names.push_back(col_name);

        // Use inferred type
        switch (bind_data->column_types[i]) {
            case InferredType::BIGINT:
                return_types.push_back(LogicalType::BIGINT);
                break;
            case InferredType::DOUBLE:
                return_types.push_back(LogicalType::DOUBLE);
                break;
            default:
                return_types.push_back(LogicalType::VARCHAR);
                break;
        }
    }

    return std::move(bind_data);
}

//===--------------------------------------------------------------------===//
// Init Global
//===--------------------------------------------------------------------===//

static unique_ptr<GlobalTableFunctionState> ReadHtmlInitGlobal(ClientContext &context,
                                                                   TableFunctionInitInput &input) {
    return make_uniq<ReadHtmlGlobalState>();
}

//===--------------------------------------------------------------------===//
// Table Function
//===--------------------------------------------------------------------===//

static void ReadHtmlFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
    auto &bind_data = data.bind_data->Cast<ReadHtmlBindData>();
    auto &state = data.global_state->Cast<ReadHtmlGlobalState>();

    idx_t count = 0;
    while (count < STANDARD_VECTOR_SIZE && state.current_row < bind_data.num_rows) {
        const auto &row = bind_data.rows[state.current_row++];

        // Output each cell with proper type conversion
        for (idx_t col = 0; col < bind_data.num_columns; col++) {
            if (col >= row.size() || row[col].empty()) {
                output.SetValue(col, count, Value());  // NULL for missing/empty cells
                continue;
            }

            const string &val = row[col];

            switch (bind_data.column_types[col]) {
                case InferredType::BIGINT: {
                    int64_t int_val;
                    if (TryParseBigInt(val, int_val)) {
                        output.SetValue(col, count, Value::BIGINT(int_val));
                    } else {
                        output.SetValue(col, count, Value());  // NULL on parse failure
                    }
                    break;
                }
                case InferredType::DOUBLE: {
                    double dbl_val;
                    if (TryParseDouble(val, dbl_val)) {
                        output.SetValue(col, count, Value::DOUBLE(dbl_val));
                    } else {
                        output.SetValue(col, count, Value());  // NULL on parse failure
                    }
                    break;
                }
                default:
                    output.SetValue(col, count, Value(val));
                    break;
            }
        }

        count++;
    }

    output.SetCardinality(count);
}

//===--------------------------------------------------------------------===//
// Register Function
//===--------------------------------------------------------------------===//

void RegisterReadHtmlFunction(ExtensionLoader &loader) {
    // 2-argument version: read_html(url, selector)
    TableFunction read_html_func("read_html",
                                   {LogicalType::VARCHAR, LogicalType::VARCHAR},
                                   ReadHtmlFunction,
                                   ReadHtmlBind,
                                   ReadHtmlInitGlobal);

    // Named parameters
    read_html_func.named_parameters["user_agent"] = LogicalType::VARCHAR;
    read_html_func.named_parameters["timeout"] = LogicalType::INTEGER;

    // 3-argument version: read_html(url, selector, table_index) - like Google Sheets IMPORTHTML
    TableFunction read_html_func_idx("read_html",
                                      {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::INTEGER},
                                      ReadHtmlFunction,
                                      ReadHtmlBind,
                                      ReadHtmlInitGlobal);
    read_html_func_idx.named_parameters["user_agent"] = LogicalType::VARCHAR;
    read_html_func_idx.named_parameters["timeout"] = LogicalType::INTEGER;

    loader.RegisterFunction(read_html_func);
    loader.RegisterFunction(read_html_func_idx);
}

} // namespace duckdb
