#pragma once

#include "duckdb.hpp"

namespace duckdb {

// Register the crawl_stream table function for streaming crawl results
void RegisterCrawlStreamFunction(ExtensionLoader &loader);

} // namespace duckdb
