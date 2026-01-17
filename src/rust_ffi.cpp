// Rust HTML Parser FFI wrapper for DuckDB Crawler
// Only compiled when RUST_PARSER_AVAILABLE is defined

#include "rust_ffi.hpp"
#include "yyjson.hpp"
#include <sstream>
#include <cctype>

#if defined(RUST_PARSER_AVAILABLE) && RUST_PARSER_AVAILABLE

// Rust FFI declarations
extern "C" {
    struct ExtractionResultFFI {
        char *json_ptr;
        char *error_ptr;
    };

    ExtractionResultFFI extract_from_html(const char *html_ptr, size_t html_len,
                                           const char *request_json);
    ExtractionResultFFI extract_jsonld_ffi(const char *html_ptr, size_t html_len);
    ExtractionResultFFI extract_microdata_ffi(const char *html_ptr, size_t html_len);
    ExtractionResultFFI extract_opengraph_ffi(const char *html_ptr, size_t html_len);
    ExtractionResultFFI extract_js_ffi(const char *html_ptr, size_t html_len);
    ExtractionResultFFI extract_css_ffi(const char *html_ptr, size_t html_len,
                                         const char *selector);
    // Batch crawl + extract (HTTP in Rust)
    ExtractionResultFFI crawl_batch_ffi(const char *request_json);
    // Sitemap fetching (simple API - returns char* directly)
    char *fetch_sitemap_simple(const char *request_json);
    void free_rust_string(char *ptr);
    // Robots.txt checking
    ExtractionResultFFI check_robots_ffi(const char *request_json);
    void free_extraction_result(ExtractionResultFFI result);
    const char *rust_parser_version();
    // Signal handling for graceful shutdown
    void set_interrupted(bool value);
    bool is_interrupted();
    // Link extraction
    ExtractionResultFFI extract_links_ffi(const char *html_ptr, size_t html_len,
                                           const char *selector, const char *base_url);
    // Element extraction (returns text, html, and all attributes)
    ExtractionResultFFI extract_element_ffi(const char *html_ptr, size_t html_len,
                                             const char *selector);
}

namespace duckdb {

using namespace duckdb_yyjson;

// RAII wrapper for ExtractionResultFFI
class RustResult {
public:
    explicit RustResult(ExtractionResultFFI result) : result_(result) {}
    ~RustResult() { free_extraction_result(result_); }

    bool HasError() const { return result_.error_ptr != nullptr; }
    std::string GetError() const {
        return result_.error_ptr ? std::string(result_.error_ptr) : "";
    }
    std::string GetJson() const {
        return result_.json_ptr ? std::string(result_.json_ptr) : "";
    }

private:
    ExtractionResultFFI result_;
};

// Parse arrow notation: jsonld->'Product'->>'name' -> ["Product", "name"]
static std::vector<std::string> ParseArrowPath(const std::string &expr, size_t start_pos,
                                                bool &is_text) {
    std::vector<std::string> path;
    is_text = false;

    size_t pos = start_pos;
    while (pos < expr.length()) {
        // Find next arrow
        size_t arrow = expr.find("->", pos);
        if (arrow == std::string::npos) break;

        // Check for ->> (text output)
        bool text_arrow = (arrow + 2 < expr.length() && expr[arrow + 2] == '>');
        if (text_arrow) is_text = true;

        size_t key_start = text_arrow ? arrow + 3 : arrow + 2;

        // Skip whitespace
        while (key_start < expr.length() && std::isspace(expr[key_start])) {
            key_start++;
        }

        if (key_start >= expr.length()) break;

        // Extract key
        std::string key;
        if (expr[key_start] == '\'') {
            size_t key_end = expr.find('\'', key_start + 1);
            if (key_end != std::string::npos) {
                key = expr.substr(key_start + 1, key_end - key_start - 1);
                pos = key_end + 1;
            } else {
                break;
            }
        } else if (expr[key_start] == '"') {
            size_t key_end = expr.find('"', key_start + 1);
            if (key_end != std::string::npos) {
                key = expr.substr(key_start + 1, key_end - key_start - 1);
                pos = key_end + 1;
            } else {
                break;
            }
        } else if (expr[key_start] == '[') {
            // Array index: [0]
            size_t bracket_end = expr.find(']', key_start);
            if (bracket_end != std::string::npos) {
                key = expr.substr(key_start + 1, bracket_end - key_start - 1);
                pos = bracket_end + 1;
            } else {
                break;
            }
        } else {
            // Unquoted key
            size_t key_end = expr.find("->", key_start);
            if (key_end == std::string::npos) {
                key = expr.substr(key_start);
                pos = expr.length();
            } else {
                key = expr.substr(key_start, key_end - key_start);
                pos = key_end;
            }
            // Trim whitespace
            while (!key.empty() && std::isspace(key.back())) {
                key.pop_back();
            }
        }

        if (!key.empty()) {
            path.push_back(key);
        }
    }

    return path;
}

// Parse jQuery-like CSS selector: $('selector').text or $('selector').attr['href']
static bool ParseCssSelector(const std::string &expr, std::string &selector,
                              std::string &accessor) {
    // Check for $( prefix
    if (expr.length() < 4 || expr[0] != '$' || expr[1] != '(') {
        return false;
    }

    // Find matching quote
    char quote = expr[2];
    if (quote != '\'' && quote != '"') {
        return false;
    }

    size_t quote_end = expr.find(quote, 3);
    if (quote_end == std::string::npos) {
        return false;
    }

    selector = expr.substr(3, quote_end - 3);

    // Find closing paren and dot
    size_t paren_close = expr.find(')', quote_end);
    if (paren_close == std::string::npos || paren_close + 1 >= expr.length() ||
        expr[paren_close + 1] != '.') {
        return false;
    }

    // Parse accessor: text, html, or attr['name']
    std::string acc_str = expr.substr(paren_close + 2);

    if (acc_str == "text") {
        accessor = "text";
    } else if (acc_str == "html") {
        accessor = "html";
    } else if (acc_str.substr(0, 5) == "attr[") {
        // attr['href'] or attr["href"]
        size_t attr_start = 5;
        char attr_quote = acc_str[attr_start];
        if (attr_quote == '\'' || attr_quote == '"') {
            size_t attr_end = acc_str.find(attr_quote, attr_start + 1);
            if (attr_end != std::string::npos) {
                accessor = "attr:" + acc_str.substr(attr_start + 1, attr_end - attr_start - 1);
            } else {
                return false;
            }
        } else {
            return false;
        }
    } else {
        return false;
    }

    return true;
}

// Build extraction request JSON for Rust FFI
std::string BuildExtractionRequest(const std::vector<ExtractSpec> &specs) {
    yyjson_mut_doc *doc = yyjson_mut_doc_new(nullptr);
    if (!doc) return "{}";

    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);

    yyjson_mut_val *specs_arr = yyjson_mut_arr(doc);

    for (const auto &spec : specs) {
        yyjson_mut_val *spec_obj = yyjson_mut_obj(doc);

        // Check for COALESCE
        if (spec.is_coalesce && !spec.coalesce_paths.empty()) {
            // Build alternatives array
            yyjson_mut_val *alts = yyjson_mut_arr(doc);

            for (const auto &alt_expr : spec.coalesce_paths) {
                yyjson_mut_val *alt_obj = yyjson_mut_obj(doc);

                // Parse each alternative path
                size_t arrow = alt_expr.find("->");
                if (arrow != std::string::npos) {
                    std::string source = alt_expr.substr(0, arrow);
                    bool is_text = false;
                    auto path_vec = ParseArrowPath(alt_expr, arrow, is_text);

                    yyjson_mut_obj_add_strcpy(doc, alt_obj, "source", source.c_str());

                    yyjson_mut_val *path_arr = yyjson_mut_arr(doc);
                    for (const auto &p : path_vec) {
                        yyjson_mut_arr_add_strcpy(doc, path_arr, p.c_str());
                    }
                    yyjson_mut_obj_add_val(doc, alt_obj, "path", path_arr);
                    yyjson_mut_obj_add_bool(doc, alt_obj, "return_text", is_text);
                }

                yyjson_mut_obj_add_strcpy(doc, alt_obj, "alias", spec.alias.c_str());
                yyjson_mut_arr_append(alts, alt_obj);
            }

            // Use first alternative as main, rest as alternatives
            // For now, set source to "coalesce" and include all paths
            yyjson_mut_obj_add_str(doc, spec_obj, "source", "jsonld");
            yyjson_mut_val *empty_path = yyjson_mut_arr(doc);
            yyjson_mut_obj_add_val(doc, spec_obj, "path", empty_path);
            yyjson_mut_obj_add_val(doc, spec_obj, "alternatives", alts);
            yyjson_mut_obj_add_strcpy(doc, spec_obj, "alias", spec.alias.c_str());
            yyjson_mut_obj_add_bool(doc, spec_obj, "return_text", true);
        }
        // Check for CSS source (already parsed by crawl_parser)
        else if (spec.source == ExtractSource::CSS) {
            yyjson_mut_obj_add_str(doc, spec_obj, "source", "css");
            yyjson_mut_val *empty_path = yyjson_mut_arr(doc);
            yyjson_mut_obj_add_val(doc, spec_obj, "path", empty_path);
            // spec.path has the selector, spec.transform has the accessor
            yyjson_mut_obj_add_strcpy(doc, spec_obj, "selector", spec.path.c_str());
            yyjson_mut_obj_add_strcpy(doc, spec_obj, "accessor", spec.transform.c_str());
            yyjson_mut_obj_add_strcpy(doc, spec_obj, "alias", spec.alias.c_str());
            yyjson_mut_obj_add_bool(doc, spec_obj, "return_text", true);

            // Add JSON cast and array expansion fields
            yyjson_mut_obj_add_bool(doc, spec_obj, "is_json_cast", spec.is_json_cast);
            yyjson_mut_obj_add_bool(doc, spec_obj, "expand_array", spec.expand_array);
            if (!spec.array_field.empty()) {
                yyjson_mut_obj_add_strcpy(doc, spec_obj, "array_field", spec.array_field.c_str());
            }
            if (!spec.json_path.empty()) {
                yyjson_mut_obj_add_strcpy(doc, spec_obj, "json_path", spec.json_path.c_str());
            }
        }
        // Arrow notation: jsonld->'Product'->>'name'
        else {
            size_t arrow = spec.expression.find("->");
            if (arrow != std::string::npos) {
                std::string source = spec.expression.substr(0, arrow);
                bool is_text = false;
                auto path_vec = ParseArrowPath(spec.expression, arrow, is_text);

                yyjson_mut_obj_add_strcpy(doc, spec_obj, "source", source.c_str());

                yyjson_mut_val *path_arr = yyjson_mut_arr(doc);
                for (const auto &p : path_vec) {
                    yyjson_mut_arr_add_strcpy(doc, path_arr, p.c_str());
                }
                yyjson_mut_obj_add_val(doc, spec_obj, "path", path_arr);
                yyjson_mut_obj_add_bool(doc, spec_obj, "return_text", is_text || spec.is_text);
            } else {
                // Fallback: treat as simple source name
                yyjson_mut_obj_add_strcpy(doc, spec_obj, "source", spec.expression.c_str());
                yyjson_mut_val *empty_path = yyjson_mut_arr(doc);
                yyjson_mut_obj_add_val(doc, spec_obj, "path", empty_path);
                yyjson_mut_obj_add_bool(doc, spec_obj, "return_text", spec.is_text);
            }

            yyjson_mut_obj_add_strcpy(doc, spec_obj, "alias", spec.alias.c_str());

            // Add JSON cast and array expansion fields for arrow notation
            yyjson_mut_obj_add_bool(doc, spec_obj, "is_json_cast", spec.is_json_cast);
            yyjson_mut_obj_add_bool(doc, spec_obj, "expand_array", spec.expand_array);
            if (!spec.array_field.empty()) {
                yyjson_mut_obj_add_strcpy(doc, spec_obj, "array_field", spec.array_field.c_str());
            }
            if (!spec.json_path.empty()) {
                yyjson_mut_obj_add_strcpy(doc, spec_obj, "json_path", spec.json_path.c_str());
            }
        }

        yyjson_mut_arr_append(specs_arr, spec_obj);
    }

    yyjson_mut_obj_add_val(doc, root, "specs", specs_arr);

    size_t len = 0;
    char *json_str = yyjson_mut_write(doc, 0, &len);
    yyjson_mut_doc_free(doc);

    if (!json_str) return "{}";

    std::string result(json_str, len);
    free(json_str);
    return result;
}

std::string ExtractWithRust(const std::string &html, const std::string &request_json) {
    if (html.empty() || request_json.empty()) {
        return "{}";
    }

    auto ffi_result = extract_from_html(html.c_str(), html.length(), request_json.c_str());
    RustResult result(ffi_result);

    if (result.HasError()) {
        return "{}";
    }

    return result.GetJson();
}

std::vector<std::string> ParseExtractionResult(const std::string &result_json,
                                                const std::vector<ExtractSpec> &specs) {
    std::vector<std::string> results;
    results.reserve(specs.size());

    if (result_json.empty()) {
        results.resize(specs.size(), "");
        return results;
    }

    yyjson_doc *doc = yyjson_read(result_json.c_str(), result_json.length(), 0);
    if (!doc) {
        results.resize(specs.size(), "");
        return results;
    }

    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *values_obj = yyjson_obj_get(root, "values");

    if (values_obj && yyjson_is_obj(values_obj)) {
        // Values is a map of alias -> value
        for (const auto &spec : specs) {
            yyjson_val *val = yyjson_obj_get(values_obj, spec.alias.c_str());
            if (val && !yyjson_is_null(val)) {
                if (yyjson_is_str(val)) {
                    results.push_back(yyjson_get_str(val));
                } else {
                    // Serialize non-string values
                    size_t len = 0;
                    char *str = yyjson_val_write(val, 0, &len);
                    if (str) {
                        results.push_back(std::string(str, len));
                        free(str);
                    } else {
                        results.push_back("");
                    }
                }
            } else {
                results.push_back("");
            }
        }
    } else {
        results.resize(specs.size(), "");
    }

    yyjson_doc_free(doc);
    return results;
}

ExpandedExtractionResult ParseExtractionResultExpanded(const std::string &result_json,
                                                        const std::vector<ExtractSpec> &specs) {
    ExpandedExtractionResult result;
    result.values.reserve(specs.size());
    result.expanded.resize(specs.size());
    result.has_expansion = false;

    if (result_json.empty()) {
        result.values.resize(specs.size(), "");
        return result;
    }

    yyjson_doc *doc = yyjson_read(result_json.c_str(), result_json.length(), 0);
    if (!doc) {
        result.values.resize(specs.size(), "");
        return result;
    }

    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *values_obj = yyjson_obj_get(root, "values");
    yyjson_val *expanded_obj = yyjson_obj_get(root, "expanded_values");

    // Process each spec
    for (size_t i = 0; i < specs.size(); i++) {
        const auto &spec = specs[i];

        // Check for expanded values first
        if (expanded_obj && yyjson_is_obj(expanded_obj)) {
            yyjson_val *exp_arr = yyjson_obj_get(expanded_obj, spec.alias.c_str());
            if (exp_arr && yyjson_is_arr(exp_arr)) {
                size_t idx = 0;
                size_t max_val = 0;
                yyjson_val *val;
                yyjson_arr_foreach(exp_arr, idx, max_val, val) {
                    if (yyjson_is_str(val)) {
                        result.expanded[i].push_back(yyjson_get_str(val));
                    } else if (!yyjson_is_null(val)) {
                        size_t len = 0;
                        char *str = yyjson_val_write(val, 0, &len);
                        if (str) {
                            result.expanded[i].push_back(std::string(str, len));
                            free(str);
                        }
                    }
                }
                if (!result.expanded[i].empty()) {
                    result.has_expansion = true;
                    result.values.push_back("");  // Placeholder for expanded value
                    continue;
                }
            }
        }

        // Fall back to regular values
        if (values_obj && yyjson_is_obj(values_obj)) {
            yyjson_val *val = yyjson_obj_get(values_obj, spec.alias.c_str());
            if (val && !yyjson_is_null(val)) {
                if (yyjson_is_str(val)) {
                    result.values.push_back(yyjson_get_str(val));
                } else {
                    size_t len = 0;
                    char *str = yyjson_val_write(val, 0, &len);
                    if (str) {
                        result.values.push_back(std::string(str, len));
                        free(str);
                    } else {
                        result.values.push_back("");
                    }
                }
            } else {
                result.values.push_back("");
            }
        } else {
            result.values.push_back("");
        }
    }

    yyjson_doc_free(doc);
    return result;
}

bool IsRustParserAvailable() {
    return true;
}

std::string GetRustParserVersion() {
    const char *ver = rust_parser_version();
    return ver ? std::string(ver) : "unknown";
}

// Convenience extractors

std::string ExtractJsonLdWithRust(const std::string &html) {
    if (html.empty()) return "{}";
    auto ffi_result = extract_jsonld_ffi(html.c_str(), html.length());
    RustResult result(ffi_result);
    return result.HasError() ? "{}" : result.GetJson();
}

std::string ExtractMicrodataWithRust(const std::string &html) {
    if (html.empty()) return "{}";
    auto ffi_result = extract_microdata_ffi(html.c_str(), html.length());
    RustResult result(ffi_result);
    return result.HasError() ? "{}" : result.GetJson();
}

std::string ExtractOpenGraphWithRust(const std::string &html) {
    if (html.empty()) return "{}";
    auto ffi_result = extract_opengraph_ffi(html.c_str(), html.length());
    RustResult result(ffi_result);
    return result.HasError() ? "{}" : result.GetJson();
}

std::string ExtractJsWithRust(const std::string &html) {
    if (html.empty()) return "{}";
    auto ffi_result = extract_js_ffi(html.c_str(), html.length());
    RustResult result(ffi_result);
    return result.HasError() ? "{}" : result.GetJson();
}

std::string ExtractCssWithRust(const std::string &html, const std::string &selector) {
    if (html.empty() || selector.empty()) return "[]";
    auto ffi_result = extract_css_ffi(html.c_str(), html.length(), selector.c_str());
    RustResult result(ffi_result);
    return result.HasError() ? "[]" : result.GetJson();
}

std::string CrawlBatchWithRust(const std::string &request_json) {
    if (request_json.empty()) return "{\"results\":[]}";
    auto ffi_result = crawl_batch_ffi(request_json.c_str());
    RustResult result(ffi_result);
    if (result.HasError()) {
        return "{\"error\":\"" + result.GetError() + "\"}";
    }
    return result.GetJson();
}

std::string FetchSitemapWithRust(const std::string &request_json) {
    if (request_json.empty()) return "{\"urls\":[],\"sitemaps\":[],\"errors\":[]}";

    // Use simple FFI that returns char* directly (avoids struct-by-value issues)
    char *json_ptr = fetch_sitemap_simple(request_json.c_str());
    if (!json_ptr) {
        return "{\"urls\":[],\"sitemaps\":[],\"errors\":[\"Null result from Rust\"]}";
    }

    // Copy to std::string before freeing
    std::string result(json_ptr);

    // Free the Rust-allocated string
    free_rust_string(json_ptr);

    return result;
}

std::string CheckRobotsWithRust(const std::string &request_json) {
    if (request_json.empty()) return "{\"allowed\":true,\"crawl_delay\":null,\"sitemaps\":[]}";
    auto ffi_result = check_robots_ffi(request_json.c_str());
    RustResult result(ffi_result);
    if (result.HasError()) {
        // On error, default to allowed
        return "{\"allowed\":true,\"crawl_delay\":null,\"sitemaps\":[]}";
    }
    return result.GetJson();
}

void SetInterrupted(bool value) {
    set_interrupted(value);
}

bool IsInterrupted() {
    return is_interrupted();
}

std::vector<std::string> ExtractLinksWithRust(const std::string &html, const std::string &selector,
                                               const std::string &base_url) {
    std::vector<std::string> result;
    if (html.empty()) return result;

    auto ffi_result = extract_links_ffi(html.c_str(), html.length(),
                                         selector.c_str(), base_url.c_str());
    RustResult rust_result(ffi_result);

    if (rust_result.HasError()) {
        return result;
    }

    std::string json = rust_result.GetJson();
    if (json.empty()) return result;

    // Parse JSON array of URLs
    yyjson_doc *doc = yyjson_read(json.c_str(), json.length(), 0);
    if (!doc) return result;

    yyjson_val *root = yyjson_doc_get_root(doc);
    if (yyjson_is_arr(root)) {
        size_t idx, max_idx;
        yyjson_val *val;
        yyjson_arr_foreach(root, idx, max_idx, val) {
            if (yyjson_is_str(val)) {
                result.push_back(yyjson_get_str(val));
            }
        }
    }

    yyjson_doc_free(doc);
    return result;
}

std::string ExtractElementWithRust(const std::string &html, const std::string &selector) {
    if (html.empty() || selector.empty()) return "null";

    auto ffi_result = extract_element_ffi(html.c_str(), html.length(), selector.c_str());
    RustResult rust_result(ffi_result);

    if (rust_result.HasError()) {
        return "null";
    }

    return rust_result.GetJson();
}

} // namespace duckdb

#else // RUST_PARSER_AVAILABLE not defined

namespace duckdb {

std::string BuildExtractionRequest(const std::vector<ExtractSpec> &specs) {
    (void)specs;
    return "{}";
}

std::string ExtractWithRust(const std::string &html, const std::string &request_json) {
    (void)html;
    (void)request_json;
    return "{}";
}

std::vector<std::string> ParseExtractionResult(const std::string &result_json,
                                                const std::vector<ExtractSpec> &specs) {
    (void)result_json;
    return std::vector<std::string>(specs.size(), "");
}

ExpandedExtractionResult ParseExtractionResultExpanded(const std::string &result_json,
                                                        const std::vector<ExtractSpec> &specs) {
    (void)result_json;
    ExpandedExtractionResult result;
    result.values.resize(specs.size(), "");
    result.expanded.resize(specs.size());
    result.has_expansion = false;
    return result;
}

bool IsRustParserAvailable() {
    return false;
}

std::string GetRustParserVersion() {
    return "not available";
}

std::string ExtractJsonLdWithRust(const std::string &html) {
    (void)html;
    return "{}";
}

std::string ExtractMicrodataWithRust(const std::string &html) {
    (void)html;
    return "{}";
}

std::string ExtractOpenGraphWithRust(const std::string &html) {
    (void)html;
    return "{}";
}

std::string ExtractJsWithRust(const std::string &html) {
    (void)html;
    return "{}";
}

std::string ExtractCssWithRust(const std::string &html, const std::string &selector) {
    (void)html;
    (void)selector;
    return "[]";
}

std::string CrawlBatchWithRust(const std::string &request_json) {
    (void)request_json;
    return "{\"error\":\"Rust parser not available\"}";
}

std::string FetchSitemapWithRust(const std::string &request_json) {
    (void)request_json;
    return "{\"urls\":[],\"sitemaps\":[],\"errors\":[\"Rust parser not available\"]}";
}

std::string CheckRobotsWithRust(const std::string &request_json) {
    (void)request_json;
    return "{\"allowed\":true,\"crawl_delay\":null,\"sitemaps\":[]}";
}

void SetInterrupted(bool value) {
    (void)value;
    // No-op when Rust parser not available
}

bool IsInterrupted() {
    return false;
}

std::vector<std::string> ExtractLinksWithRust(const std::string &html, const std::string &selector,
                                               const std::string &base_url) {
    (void)html;
    (void)selector;
    (void)base_url;
    return {};
}

std::string ExtractElementWithRust(const std::string &html, const std::string &selector) {
    (void)html;
    (void)selector;
    return "null";
}

} // namespace duckdb

#endif // RUST_PARSER_AVAILABLE
