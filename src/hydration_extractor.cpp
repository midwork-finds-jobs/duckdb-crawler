#include "hydration_extractor.hpp"
#include <libxml/HTMLparser.h>
#include <libxml/tree.h>
#include "duckdb/common/string_util.hpp"
#include "yyjson.hpp"

namespace duckdb {

using namespace duckdb_yyjson;

// RAII wrapper for xmlDoc
class HydrationDocGuard {
public:
	explicit HydrationDocGuard(xmlDocPtr doc) : doc_(doc) {
	}
	~HydrationDocGuard() {
		if (doc_) {
			xmlFreeDoc(doc_);
		}
	}
	xmlDocPtr get() const {
		return doc_;
	}
	operator bool() const {
		return doc_ != nullptr;
	}

private:
	xmlDocPtr doc_;
};

// Helper: get attribute value from xmlNode
static std::string GetAttribute(xmlNodePtr node, const char *attr) {
	xmlChar *value = xmlGetProp(node, BAD_CAST attr);
	if (!value) {
		return "";
	}
	std::string result(reinterpret_cast<char *>(value));
	xmlFree(value);
	return result;
}

// Validate JSON string using yyjson
static bool IsValidJson(const std::string &json) {
	if (json.empty()) {
		return false;
	}
	yyjson_doc *doc = yyjson_read(json.c_str(), json.size(), 0);
	if (!doc) {
		return false;
	}
	yyjson_doc_free(doc);
	return true;
}

// Extract JSON object starting from a position in string
// Returns the JSON string if valid, empty string otherwise
static std::string ExtractJsonObject(const std::string &content, size_t start_pos) {
	if (start_pos >= content.size()) {
		return "";
	}

	// Find the opening brace or bracket
	size_t json_start = content.find_first_of("{[", start_pos);
	if (json_start == std::string::npos) {
		return "";
	}

	char open_char = content[json_start];
	char close_char = (open_char == '{') ? '}' : ']';

	// Count braces to find matching closing brace
	int depth = 1;
	bool in_string = false;
	bool escape_next = false;

	for (size_t i = json_start + 1; i < content.size() && depth > 0; i++) {
		char c = content[i];

		if (escape_next) {
			escape_next = false;
			continue;
		}

		if (c == '\\' && in_string) {
			escape_next = true;
			continue;
		}

		if (c == '"' && !escape_next) {
			in_string = !in_string;
			continue;
		}

		if (!in_string) {
			if (c == open_char) {
				depth++;
			} else if (c == close_char) {
				depth--;
				if (depth == 0) {
					std::string json = content.substr(json_start, i - json_start + 1);
					// Validate with yyjson
					if (IsValidJson(json)) {
						return json;
					}
					return "";
				}
			}
		}
	}

	return "";
}

// Known hydration patterns to look for in JavaScript
static const std::vector<std::string> HYDRATION_PATTERNS = {
    "__NEXT_DATA__", "__NUXT__",         "__INITIAL_STATE__", "__PRELOADED_STATE__",
    "__DATA__",      "__APOLLO_STATE__", "__RELAY_STORE__",   "__REDUX_STATE__"};

// Extract hydration data from script content (JavaScript assignments)
static void ExtractFromScriptContent(const std::string &content, HydrationResult &result) {
	for (const auto &pattern : HYDRATION_PATTERNS) {
		// Look for window.VAR = or VAR = followed by JSON
		std::vector<std::string> prefixes = {"window." + pattern + " = ", "window." + pattern + "=", pattern + " = ",
		                                     pattern + "="};

		for (const auto &prefix : prefixes) {
			size_t pos = content.find(prefix);
			if (pos != std::string::npos) {
				size_t json_start = pos + prefix.size();
				std::string json = ExtractJsonObject(content, json_start);
				if (!json.empty() && result.data.find(pattern) == result.data.end()) {
					result.data[pattern] = json;
					result.found = true;
				}
			}
		}
	}
}

// Find hydration data in script tags
static void FindHydrationScripts(xmlNodePtr node, HydrationResult &result) {
	for (xmlNodePtr cur = node; cur; cur = cur->next) {
		if (cur->type == XML_ELEMENT_NODE) {
			if (xmlStrcasecmp(cur->name, BAD_CAST "script") == 0) {
				std::string id = GetAttribute(cur, "id");
				std::string type = GetAttribute(cur, "type");

				// Check for Next.js style: <script id="__NEXT_DATA__" type="application/json">
				if (!id.empty()) {
					// Check if it's a known pattern
					for (const auto &pattern : HYDRATION_PATTERNS) {
						if (id == pattern) {
							// Get content
							xmlChar *content = xmlNodeGetContent(cur);
							if (content) {
								std::string content_str(reinterpret_cast<char *>(content));
								xmlFree(content);

								// Trim and validate
								size_t start = content_str.find_first_not_of(" \t\n\r");
								size_t end = content_str.find_last_not_of(" \t\n\r");
								if (start != std::string::npos && end != std::string::npos) {
									std::string json = content_str.substr(start, end - start + 1);
									if (IsValidJson(json)) {
										result.data[pattern] = json;
										result.found = true;
									}
								}
							}
							break;
						}
					}
				}

				// Check for JavaScript with hydration assignments
				// Only process non-json script tags (actual JavaScript)
				if (type.empty() || StringUtil::Lower(type) == "text/javascript") {
					xmlChar *content = xmlNodeGetContent(cur);
					if (content) {
						std::string content_str(reinterpret_cast<char *>(content));
						xmlFree(content);
						ExtractFromScriptContent(content_str, result);
					}
				}
			}

			// Recurse into children
			if (cur->children) {
				FindHydrationScripts(cur->children, result);
			}
		}
	}
}

// Build combined JSON output using yyjson
static std::string BuildOutputJson(const HydrationResult &result) {
	if (!result.found) {
		return "";
	}

	yyjson_mut_doc *doc = yyjson_mut_doc_new(nullptr);
	if (!doc) {
		return "";
	}

	yyjson_mut_val *root = yyjson_mut_obj(doc);
	yyjson_mut_doc_set_root(doc, root);

	for (const auto &pair : result.data) {
		// Parse the JSON and embed it
		yyjson_doc *obj_doc = yyjson_read(pair.second.c_str(), pair.second.size(), 0);
		if (obj_doc) {
			yyjson_val *obj_root = yyjson_doc_get_root(obj_doc);
			if (obj_root) {
				yyjson_mut_val *copy = yyjson_val_mut_copy(doc, obj_root);
				yyjson_mut_obj_add_val(doc, root, pair.first.c_str(), copy);
			}
			yyjson_doc_free(obj_doc);
		}
	}

	size_t len = 0;
	char *json_str = yyjson_mut_write(doc, 0, &len);
	yyjson_mut_doc_free(doc);

	if (!json_str) {
		return "";
	}

	std::string result_str(json_str, len);
	free(json_str);
	return result_str;
}

HydrationResult ExtractHydration(const std::string &html) {
	HydrationResult result;

	if (html.empty()) {
		return result;
	}

	// Parse HTML with libxml2
	HydrationDocGuard doc(htmlReadMemory(html.c_str(), static_cast<int>(html.size()), nullptr, "UTF-8",
	                                     HTML_PARSE_RECOVER | HTML_PARSE_NOERROR | HTML_PARSE_NOWARNING));

	if (!doc) {
		return result;
	}

	xmlNodePtr root = xmlDocGetRootElement(doc.get());
	if (!root) {
		return result;
	}

	// Find hydration scripts
	FindHydrationScripts(root, result);

	// Build combined JSON output
	result.as_json = BuildOutputJson(result);

	return result;
}

std::string ExtractHydrationAsJson(const std::string &html) {
	auto result = ExtractHydration(html);
	return result.as_json;
}

} // namespace duckdb
