#pragma once

#include <string>
#include <vector>
#include <unordered_map>

namespace duckdb {

// Represents a single JSON-LD object with its @type
struct JsonLdObject {
	std::string type; // @type value (e.g., "Product", "Organization")
	std::string json; // Full JSON string of the object
};

// Result of JSON-LD extraction
struct JsonLdResult {
	// Map of @type -> JSON objects (can have multiple of same type)
	// e.g., {"Product": [...], "Organization": [...], "BreadcrumbList": [...]}
	std::unordered_map<std::string, std::vector<std::string>> by_type;

	// Merged JSON object keyed by type: {"Product": {...}, "Organization": {...}}
	// If multiple objects of same type, uses first one
	std::string as_json;

	// Whether any JSON-LD was found
	bool found = false;
};

// Extract all JSON-LD from HTML content
// Handles:
// - Multiple <script type="application/ld+json"> blocks
// - @graph arrays containing multiple objects
// - Nested @type objects
JsonLdResult ExtractJsonLd(const std::string &html);

// Extract JSON-LD and return as single JSON string keyed by @type
// Returns: {"Product": {...}, "Organization": {...}} or empty string if none found
std::string ExtractJsonLdAsJson(const std::string &html);

} // namespace duckdb
