#pragma once

//===--------------------------------------------------------------------===//
// crawler_internal.hpp - Shared Types for Crawler Modules
//===--------------------------------------------------------------------===//
// Contains struct definitions shared between crawler modules

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "thread_utils.hpp"

#include <atomic>
#include <mutex>
#include <vector>
#include <string>

namespace duckdb {

// Global connection counter (defined in crawler_function.cpp)
extern std::atomic<int> g_active_connections;

//===--------------------------------------------------------------------===//
// BatchCrawlEntry - Single crawl result for batch processing
//===--------------------------------------------------------------------===//
struct BatchCrawlEntry {
	std::string url;
	std::string surt_key;
	int status_code;
	std::string body;
	std::string content_type;
	int64_t elapsed_ms;
	std::string timestamp_expr; // SQL expression for timestamp
	std::string error;
	std::string etag;
	std::string last_modified;
	std::string content_hash;
	std::string final_url; // Final URL after redirects
	int redirect_count;    // Number of redirects followed
	// Structured data extraction
	std::string jsonld;    // JSON-LD data as JSON
	std::string opengraph; // OpenGraph data as JSON
	std::string meta;      // Meta tags as JSON
	std::string hydration; // Hydration data as JSON
	std::string js;        // JavaScript variables as JSON
	bool is_update;
};

//===--------------------------------------------------------------------===//
// Utility functions
//===--------------------------------------------------------------------===//

// Adaptive rate limiting (defined in crawler_function.cpp)
void UpdateAdaptiveDelay(DomainState &state, double response_ms, double max_delay);

} // namespace duckdb
