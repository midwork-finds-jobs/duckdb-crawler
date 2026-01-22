#pragma once

#include <string>
#include <vector>

namespace duckdb {

// Call Rust parser to extract data from HTML
// Returns JSON string with extracted values
std::string ExtractWithRust(const std::string &html, const std::string &request_json);

// Check if Rust parser is available
bool IsRustParserAvailable();

// Get Rust parser version
std::string GetRustParserVersion();

// Convenience extractors - return JSON strings
std::string ExtractJsonLdWithRust(const std::string &html);
std::string ExtractMicrodataWithRust(const std::string &html);
std::string ExtractOpenGraphWithRust(const std::string &html);
std::string ExtractJsWithRust(const std::string &html);
std::string ExtractCssWithRust(const std::string &html, const std::string &selector);

// Extract article content using readability algorithm
// Returns JSON: {"title": "...", "content": "<html>", "text_content": "...", "length": 123, "excerpt": "..."}
std::string ExtractReadabilityWithRust(const std::string &html, const std::string &url);

// Batch crawl + extract (HTTP done in Rust)
// Takes JSON request: {"urls": [...], "extraction": {...}, "user_agent": "...", "timeout_ms": 30000, "concurrency": 4}
// Returns JSON response: {"results": [{url, status, content_type, body, error, extracted, response_time_ms}, ...]}
std::string CrawlBatchWithRust(const std::string &request_json);

// Fetch and parse sitemap(s)
// Takes JSON request: {"url": "...", "recursive": true, "max_depth": 5, "discover_from_robots": false}
// Returns JSON response: {"urls": [{url, lastmod, changefreq, priority}, ...], "sitemaps": [...], "errors": [...]}
std::string FetchSitemapWithRust(const std::string &request_json);

// Check robots.txt for a URL
// Takes JSON request: {"url": "...", "user_agent": "..."}
// Returns JSON response: {"allowed": true, "crawl_delay": 1.0, "sitemaps": [...]}
std::string CheckRobotsWithRust(const std::string &request_json);

// Signal handling for graceful shutdown
void SetInterrupted(bool value);
bool IsInterrupted();

// Extract links from HTML using CSS selector
// Returns vector of absolute URLs
std::vector<std::string> ExtractLinksWithRust(const std::string &html, const std::string &selector,
                                               const std::string &base_url);

// Extract element as JSON with text, html, and attr map
// Returns JSON: {"text": "...", "html": "...", "attr": {"key": "value", ...}}
std::string ExtractElementWithRust(const std::string &html, const std::string &selector);

// Extract using unified path syntax: css@attr[*].json.path
// Examples:
//   input#jobs@value         -> attribute value as string
//   input#jobs@value[*]      -> JSON array
//   input#jobs@value[*].id   -> array of 'id' fields
std::string ExtractPathWithRust(const std::string &html, const std::string &path);

// Extract HTML table using CSS selector
// Returns JSON: {"headers": [...], "rows": [[...], ...], "num_columns": N, "num_rows": M, "error": null}
// url is used to detect Wikipedia pages for special handling (removes citation references)
// table_index is 0-based index of which matching table to extract
std::string ExtractTableWithRust(const std::string &html, const std::string &selector, const std::string &url, size_t table_index);

} // namespace duckdb
