#pragma once

#include <string>
#include <vector>

namespace duckdb {

// Segment in a JSON path expression like jsonld->'Product'->>'name'
struct JsonPathSegment {
	std::string key;  // Object key (e.g., "Product", "name")
	int array_index;  // For array access (e.g., [0]), -1 if not array access
	bool return_text; // true for ->> (text output), false for -> (JSON output)

	JsonPathSegment() : array_index(-1), return_text(false) {
	}
};

// Parsed JSON path expression
struct JsonPath {
	std::string base_column; // Source column: jsonld, opengraph, meta, hydration, js
	std::vector<JsonPathSegment> segments;
	bool is_text_output; // Final output is text (last operator was ->>)
};

// Parse a JSON path expression like "jsonld->'Product'->>'name'"
// Returns parsed path structure
JsonPath ParseJsonPath(const std::string &expr);

// Evaluate a JSON path on a JSON string
// Returns extracted value as string (empty string if path not found)
std::string EvaluateJsonPath(const std::string &json_str, const JsonPath &path);

} // namespace duckdb
