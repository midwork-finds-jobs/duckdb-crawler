#pragma once

#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

// Register read_html() table function
void RegisterReadHtmlFunction(ExtensionLoader &loader);

} // namespace duckdb
