#pragma once

#include "duckdb.hpp"

namespace duckdb {

// Register the $() CSS extraction scalar function
void RegisterCssExtractFunction(ExtensionLoader &loader);

} // namespace duckdb
