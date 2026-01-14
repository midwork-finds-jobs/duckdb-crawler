#pragma once

#include "duckdb.hpp"
#include "duckdb/parser/parser_extension.hpp"

namespace duckdb {

// Crawl modes
enum class CrawlMode : uint8_t {
	URLS,   // Crawl specific URLs directly
	SITES   // Discover sitemaps from hostnames and crawl all URLs found
};

// Parsed data from CRAWL statement
struct CrawlParseData : public ParserExtensionParseData {
	CrawlMode mode = CrawlMode::URLS;
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

	unique_ptr<ParserExtensionParseData> Copy() const override;
	string ToString() const override;
};

// Parser extension for CRAWL statement
class CrawlParserExtension : public ParserExtension {
public:
	CrawlParserExtension();

	static ParserExtensionParseResult ParseCrawl(ParserExtensionInfo *info, const string &query);
	static ParserExtensionPlanResult PlanCrawl(ParserExtensionInfo *info, ClientContext &context,
	                                           unique_ptr<ParserExtensionParseData> parse_data);
};

} // namespace duckdb
