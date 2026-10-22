#pragma once

// MERGE query nodes are part of the DuckDB 2.0 parser API and are absent from
// DuckDB 1.x, which may nevertheless ship duckdb/common/identifier.hpp.
// Feature detection is used because extension builds do not expose DuckDB's
// internal CMake version variables to extension CMakeLists files.
#if __has_include("duckdb/parser/query_node/merge_query_node.hpp")
#define CRAWLER_DUCKDB_V2 1
#else
#define CRAWLER_DUCKDB_V2 0
#endif
