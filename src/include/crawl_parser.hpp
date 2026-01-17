#pragma once

#include "duckdb.hpp"
#include "duckdb/parser/parser_extension.hpp"

namespace duckdb {

// Statement types for CRAWL parser
enum class CrawlStatementType : uint8_t {
	CRAWL,     // CRAWL (...) INTO table
	STREAM     // STREAM (query) INTO table - incremental writes
};

// Extraction source types
enum class ExtractSource : uint8_t {
	JSONLD,
	MICRODATA,
	OPENGRAPH,
	META,
	JS,
	CSS
};

// Specification for a single EXTRACT column
struct ExtractSpec {
	string expression;     // e.g., "jsonld.Product.name" or "css '.price::text'"
	string alias;          // e.g., "name"
	bool is_text;          // true for ->> (text), false for -> (json)

	// Enhanced fields for new syntax
	ExtractSource source;  // Source type
	string path;           // Path within source (dot notation)
	string target_type;    // Optional type: DECIMAL, INTEGER, BOOLEAN, VARCHAR
	string transform;      // Optional transform: parse_price, trim, lowercase

	// For COALESCE
	bool is_coalesce;      // true if this is a COALESCE expression
	vector<string> coalesce_paths;  // Paths for COALESCE (source.path format)

	// For JSON cast and array expansion (::json, [*], .field syntax)
	bool is_json_cast;     // true if ::json suffix present
	bool expand_array;     // true if [*] array expansion requested
	string array_field;    // field to extract from each array element (e.g., "id" for [*].id)
	string json_path;      // additional -> navigation after ::json cast

	ExtractSpec() : is_text(false), source(ExtractSource::JSONLD), is_coalesce(false),
	                is_json_cast(false), expand_array(false) {}
};

// Parsed data from CRAWL statement
struct CrawlParseData : public ParserExtensionParseData {
	CrawlStatementType statement_type = CrawlStatementType::CRAWL;
	string source_query;      // The SELECT query for URLs or hostnames
	string target_table;      // Table to insert results into
	string user_agent;        // Required user_agent parameter

	// Optional parameters with defaults
	double default_crawl_delay = 1.0;
	double min_crawl_delay = 0.0;
	double max_crawl_delay = 60.0;
	int timeout_seconds = 30;
	bool respect_robots_txt = true;
	bool log_skipped = true;

	// SITES mode specific options
	string url_filter;        // LIKE pattern to filter discovered URLs (e.g., '%/product/%')
	double sitemap_cache_hours = 24.0;  // Hours to cache sitemap discovery results
	bool update_stale = false;  // Re-crawl stale URLs based on sitemap lastmod/changefreq

	// Rate limiting and retry options
	int max_retry_backoff_seconds = 600;  // Max Fibonacci backoff for 429/5XX/timeout (10 min default)
	int max_parallel_per_domain = 8;      // Max concurrent requests per domain (if no crawl-delay)
	int max_total_connections = 32;       // Global max concurrent connections across all domains

	// Resource limits
	int64_t max_response_bytes = 10 * 1024 * 1024;  // Max response size (10MB default)
	bool compress = true;                  // Request gzip/deflate compression

	// Content-Type filtering (comma-separated, supports wildcards like "text/*")
	string accept_content_types;  // Only accept these types (empty = accept all)
	string reject_content_types;  // Reject these types (checked after accept)

	// Link-following fallback (when sitemap not found)
	bool follow_links = false;            // Enable link-based crawling fallback
	bool allow_subdomains = false;        // Follow links to subdomains (e.g., blog.example.com)
	int max_crawl_pages = 10000;          // Safety limit on pages to discover
	int max_crawl_depth = 10;             // Max link depth from start URL
	bool respect_nofollow = true;         // Skip rel="nofollow" links and nofollow meta
	bool follow_canonical = true;         // Replace URL with canonical if different

	// Multi-threading
	int num_threads = 0;                  // 0 = auto (hardware_concurrency)

	// Row limit (LIMIT clause) - stops crawl after N matching rows inserted
	int64_t row_limit = 0;                // 0 = no limit

	// Structured data extraction
	bool extract_js = true;               // Extract JS variables (can be disabled for performance)

	// EXTRACT clause - custom column extraction
	vector<ExtractSpec> extract_specs;    // Empty = use default schema

	unique_ptr<ParserExtensionParseData> Copy() const override;
	string ToString() const override;
};

// Parsed data from STREAM statement
struct StreamParseData : public ParserExtensionParseData {
	string source_query;      // The SELECT query to stream
	string target_table;      // Table to insert results into
	int64_t batch_size = 100; // Rows per batch write

	unique_ptr<ParserExtensionParseData> Copy() const override;
	string ToString() const override;
};

// Parser extension for CRAWL and STREAM statements
class CrawlParserExtension : public ParserExtension {
public:
	CrawlParserExtension();

	static ParserExtensionParseResult ParseCrawl(ParserExtensionInfo *info, const string &query);
	static ParserExtensionPlanResult PlanCrawl(ParserExtensionInfo *info, ClientContext &context,
	                                           unique_ptr<ParserExtensionParseData> parse_data);
};

} // namespace duckdb
