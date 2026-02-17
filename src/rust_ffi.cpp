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

ExtractionResultFFI extract_from_html(const char *html_ptr, size_t html_len, const char *request_json);
ExtractionResultFFI extract_jsonld_ffi(const char *html_ptr, size_t html_len);
ExtractionResultFFI extract_microdata_ffi(const char *html_ptr, size_t html_len);
ExtractionResultFFI extract_opengraph_ffi(const char *html_ptr, size_t html_len);
ExtractionResultFFI extract_js_ffi(const char *html_ptr, size_t html_len);
ExtractionResultFFI extract_css_ffi(const char *html_ptr, size_t html_len, const char *selector);
// Readability extraction
ExtractionResultFFI extract_readability_ffi(const char *html_ptr, size_t html_len, const char *url);
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
ExtractionResultFFI extract_links_ffi(const char *html_ptr, size_t html_len, const char *selector,
                                      const char *base_url);
// Element extraction (returns text, html, and all attributes)
ExtractionResultFFI extract_element_ffi(const char *html_ptr, size_t html_len, const char *selector);
// Unified path extraction: css@attr[*].json.path
ExtractionResultFFI extract_path_ffi(const char *html_ptr, size_t html_len, const char *path);
// HTML table extraction (url is used to detect Wikipedia for special handling)
// table_index: 0-based index of which matching element to extract
ExtractionResultFFI extract_table_ffi(const char *html_ptr, size_t html_len, const char *selector, const char *url,
                                      size_t table_index);
}

namespace duckdb {

using namespace duckdb_yyjson;

// RAII wrapper for ExtractionResultFFI
class RustResult {
public:
	explicit RustResult(ExtractionResultFFI result) : result_(result) {
	}
	~RustResult() {
		free_extraction_result(result_);
	}

	bool HasError() const {
		return result_.error_ptr != nullptr;
	}
	std::string GetError() const {
		return result_.error_ptr ? std::string(result_.error_ptr) : "";
	}
	std::string GetJson() const {
		return result_.json_ptr ? std::string(result_.json_ptr) : "";
	}

private:
	ExtractionResultFFI result_;
};

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

bool IsRustParserAvailable() {
	return true;
}

std::string GetRustParserVersion() {
	const char *ver = rust_parser_version();
	return ver ? std::string(ver) : "unknown";
}

// Convenience extractors

std::string ExtractJsonLdWithRust(const std::string &html) {
	if (html.empty())
		return "{}";
	auto ffi_result = extract_jsonld_ffi(html.c_str(), html.length());
	RustResult result(ffi_result);
	return result.HasError() ? "{}" : result.GetJson();
}

std::string ExtractMicrodataWithRust(const std::string &html) {
	if (html.empty())
		return "{}";
	auto ffi_result = extract_microdata_ffi(html.c_str(), html.length());
	RustResult result(ffi_result);
	return result.HasError() ? "{}" : result.GetJson();
}

std::string ExtractOpenGraphWithRust(const std::string &html) {
	if (html.empty())
		return "{}";
	auto ffi_result = extract_opengraph_ffi(html.c_str(), html.length());
	RustResult result(ffi_result);
	return result.HasError() ? "{}" : result.GetJson();
}

std::string ExtractJsWithRust(const std::string &html) {
	if (html.empty())
		return "{}";
	auto ffi_result = extract_js_ffi(html.c_str(), html.length());
	RustResult result(ffi_result);
	return result.HasError() ? "{}" : result.GetJson();
}

std::string ExtractCssWithRust(const std::string &html, const std::string &selector) {
	if (html.empty() || selector.empty())
		return "[]";
	auto ffi_result = extract_css_ffi(html.c_str(), html.length(), selector.c_str());
	RustResult result(ffi_result);
	return result.HasError() ? "[]" : result.GetJson();
}

std::string ExtractReadabilityWithRust(const std::string &html, const std::string &url) {
	if (html.empty())
		return "{}";
	auto ffi_result = extract_readability_ffi(html.c_str(), html.length(), url.c_str());
	RustResult result(ffi_result);
	return result.HasError() ? "{}" : result.GetJson();
}

std::string CrawlBatchWithRust(const std::string &request_json) {
	if (request_json.empty())
		return "{\"results\":[]}";
	auto ffi_result = crawl_batch_ffi(request_json.c_str());
	RustResult result(ffi_result);
	if (result.HasError()) {
		return "{\"error\":\"" + result.GetError() + "\"}";
	}
	return result.GetJson();
}

std::string FetchSitemapWithRust(const std::string &request_json) {
	if (request_json.empty())
		return "{\"urls\":[],\"sitemaps\":[],\"errors\":[]}";

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
	if (request_json.empty())
		return "{\"allowed\":true,\"crawl_delay\":null,\"sitemaps\":[]}";
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
	if (html.empty())
		return result;

	auto ffi_result = extract_links_ffi(html.c_str(), html.length(), selector.c_str(), base_url.c_str());
	RustResult rust_result(ffi_result);

	if (rust_result.HasError()) {
		return result;
	}

	std::string json = rust_result.GetJson();
	if (json.empty())
		return result;

	// Parse JSON array of URLs
	yyjson_doc *doc = yyjson_read(json.c_str(), json.length(), 0);
	if (!doc)
		return result;

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
	if (html.empty() || selector.empty())
		return "null";

	auto ffi_result = extract_element_ffi(html.c_str(), html.length(), selector.c_str());
	RustResult rust_result(ffi_result);

	if (rust_result.HasError()) {
		return "null";
	}

	return rust_result.GetJson();
}

std::string ExtractPathWithRust(const std::string &html, const std::string &path) {
	if (html.empty() || path.empty())
		return "null";

	auto ffi_result = extract_path_ffi(html.c_str(), html.length(), path.c_str());
	RustResult rust_result(ffi_result);

	if (rust_result.HasError()) {
		return "null";
	}

	return rust_result.GetJson();
}

std::string ExtractTableWithRust(const std::string &html, const std::string &selector, const std::string &url,
                                 size_t table_index) {
	if (html.empty() || selector.empty()) {
		return "{\"headers\":[],\"rows\":[],\"num_columns\":0,\"num_rows\":0,\"error\":\"Empty input\"}";
	}

	auto ffi_result = extract_table_ffi(html.c_str(), html.length(), selector.c_str(), url.c_str(), table_index);
	RustResult rust_result(ffi_result);

	if (rust_result.HasError()) {
		return "{\"headers\":[],\"rows\":[],\"num_columns\":0,\"num_rows\":0,\"error\":\"" + rust_result.GetError() +
		       "\"}";
	}

	return rust_result.GetJson();
}

} // namespace duckdb

#else // RUST_PARSER_AVAILABLE not defined

namespace duckdb {

std::string ExtractWithRust(const std::string &html, const std::string &request_json) {
	(void)html;
	(void)request_json;
	return "{}";
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

std::string ExtractReadabilityWithRust(const std::string &html, const std::string &url) {
	(void)html;
	(void)url;
	return "{}";
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

std::string ExtractPathWithRust(const std::string &html, const std::string &path) {
	(void)html;
	(void)path;
	return "null";
}

std::string ExtractTableWithRust(const std::string &html, const std::string &selector, const std::string &url,
                                 size_t table_index) {
	(void)html;
	(void)selector;
	(void)url;
	(void)table_index;
	return "{\"headers\":[],\"rows\":[],\"num_columns\":0,\"num_rows\":0,\"error\":\"Rust parser not available\"}";
}

} // namespace duckdb

#endif // RUST_PARSER_AVAILABLE
