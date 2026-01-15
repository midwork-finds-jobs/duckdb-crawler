#pragma once

#include "duckdb.hpp"

namespace duckdb {

class ExtensionLoader;

void RegisterCrawlIntoFunction(ExtensionLoader &loader);

} // namespace duckdb
