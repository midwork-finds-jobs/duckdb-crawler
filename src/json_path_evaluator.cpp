#include "json_path_evaluator.hpp"
#include "yyjson.hpp"
#include <algorithm>
#include <cctype>

using namespace duckdb_yyjson;

namespace duckdb {

// Helper to trim whitespace
static std::string TrimStr(const std::string &str) {
	size_t start = 0;
	size_t end = str.length();
	while (start < end && std::isspace(str[start])) {
		start++;
	}
	while (end > start && std::isspace(str[end - 1])) {
		end--;
	}
	return str.substr(start, end - start);
}

// Parse a JSON path expression like "jsonld->'Product'->'offers'->>'price'"
JsonPath ParseJsonPath(const std::string &expr) {
	JsonPath result;
	result.is_text_output = false;

	std::string trimmed = TrimStr(expr);
	if (trimmed.empty()) {
		return result;
	}

	// Find first arrow operator to split base column
	size_t first_arrow = trimmed.find("->");
	if (first_arrow == std::string::npos) {
		// No arrow, whole expression is the base column
		result.base_column = trimmed;
		return result;
	}

	result.base_column = TrimStr(trimmed.substr(0, first_arrow));

	// Parse remaining segments
	size_t pos = first_arrow;
	while (pos < trimmed.length()) {
		// Check for ->> or ->
		bool is_text = false;
		if (pos + 2 < trimmed.length() && trimmed.substr(pos, 3) == "->>") {
			is_text = true;
			pos += 3;
		} else if (pos + 1 < trimmed.length() && trimmed.substr(pos, 2) == "->") {
			is_text = false;
			pos += 2;
		} else {
			break; // No more arrows
		}

		// Skip whitespace
		while (pos < trimmed.length() && std::isspace(trimmed[pos])) {
			pos++;
		}

		if (pos >= trimmed.length()) {
			break;
		}

		JsonPathSegment segment;
		segment.return_text = is_text;
		segment.array_index = -1;

		// Check for array index [n]
		if (trimmed[pos] == '[') {
			size_t bracket_end = trimmed.find(']', pos);
			if (bracket_end != std::string::npos) {
				std::string index_str = trimmed.substr(pos + 1, bracket_end - pos - 1);
				try {
					segment.array_index = std::stoi(index_str);
				} catch (...) {
					segment.array_index = 0;
				}
				pos = bracket_end + 1;
			}
		}
		// Check for quoted key 'key'
		else if (trimmed[pos] == '\'') {
			size_t quote_end = trimmed.find('\'', pos + 1);
			if (quote_end != std::string::npos) {
				segment.key = trimmed.substr(pos + 1, quote_end - pos - 1);
				pos = quote_end + 1;
			}
		}
		// Check for quoted key "key"
		else if (trimmed[pos] == '"') {
			size_t quote_end = trimmed.find('"', pos + 1);
			if (quote_end != std::string::npos) {
				segment.key = trimmed.substr(pos + 1, quote_end - pos - 1);
				pos = quote_end + 1;
			}
		}
		// Unquoted key or number
		else {
			size_t key_end = pos;
			while (key_end < trimmed.length() && trimmed[key_end] != '-' && !std::isspace(trimmed[key_end])) {
				key_end++;
			}
			std::string key = TrimStr(trimmed.substr(pos, key_end - pos));

			// Check if it's a number (array index shorthand)
			bool is_number = !key.empty() && std::all_of(key.begin(), key.end(), ::isdigit);
			if (is_number) {
				segment.array_index = std::stoi(key);
			} else {
				segment.key = key;
			}
			pos = key_end;
		}

		result.segments.push_back(segment);
		result.is_text_output = is_text; // Last operator determines output type
	}

	return result;
}

// Evaluate a JSON path on a JSON string
std::string EvaluateJsonPath(const std::string &json_str, const JsonPath &path) {
	if (json_str.empty() || path.segments.empty()) {
		return "";
	}

	// Parse JSON
	yyjson_doc *doc = yyjson_read(json_str.c_str(), json_str.length(), 0);
	if (!doc) {
		return "";
	}

	yyjson_val *root = yyjson_doc_get_root(doc);
	if (!root) {
		yyjson_doc_free(doc);
		return "";
	}

	yyjson_val *current = root;

	// Traverse path segments
	for (const auto &segment : path.segments) {
		if (!current) {
			break;
		}

		if (segment.array_index >= 0) {
			// Array access
			if (!yyjson_is_arr(current)) {
				current = nullptr;
				break;
			}
			current = yyjson_arr_get(current, segment.array_index);
		} else if (!segment.key.empty()) {
			// Object key access
			if (!yyjson_is_obj(current)) {
				current = nullptr;
				break;
			}
			current = yyjson_obj_get(current, segment.key.c_str());
		}
	}

	std::string result;
	if (current) {
		if (path.is_text_output) {
			// Return as text (string value or JSON string representation)
			if (yyjson_is_str(current)) {
				result = yyjson_get_str(current);
			} else if (yyjson_is_null(current)) {
				result = "";
			} else {
				// Serialize non-string values to JSON text
				char *json_text = yyjson_val_write(current, 0, nullptr);
				if (json_text) {
					result = json_text;
					free(json_text);
				}
			}
		} else {
			// Return as JSON
			char *json_text = yyjson_val_write(current, 0, nullptr);
			if (json_text) {
				result = json_text;
				free(json_text);
			}
		}
	}

	yyjson_doc_free(doc);
	return result;
}

} // namespace duckdb
