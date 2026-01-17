#pragma once

#include <string>
#include <vector>
#include "crawl_parser.hpp"

namespace duckdb {

// Build JSON request from ExtractSpecs for Rust FFI
std::string BuildExtractionRequest(const std::vector<ExtractSpec> &specs);

// Call Rust parser to extract data from HTML
// Returns JSON string with extracted values
std::string ExtractWithRust(const std::string &html, const std::string &request_json);

// Parse Rust extraction result JSON into value vector
std::vector<std::string> ParseExtractionResult(const std::string &result_json,
                                                const std::vector<ExtractSpec> &specs);

// Expanded extraction result for array expansion
struct ExpandedExtractionResult {
	std::vector<std::string> values;  // Regular values (one per spec)
	std::vector<std::vector<std::string>> expanded;  // Expanded arrays (one per spec, empty if not expanded)
	bool has_expansion;  // True if any spec has expand_array=true and returned multiple values
};

// Parse Rust extraction result with support for expanded arrays
ExpandedExtractionResult ParseExtractionResultExpanded(const std::string &result_json,
                                                        const std::vector<ExtractSpec> &specs);

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

} // namespace duckdb
