#pragma once

#include "duckdb.hpp"

namespace duckdb {

class ExtensionLoader;

void RegisterCrawlerFunction(ExtensionLoader &loader);
void RegisterCrawlIntoFunction(ExtensionLoader &loader);

} // namespace duckdb
