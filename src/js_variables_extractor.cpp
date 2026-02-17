#include "js_variables_extractor.hpp"
#include <libxml/HTMLparser.h>
#include <libxml/tree.h>
#include <regex>
#include "yyjson.hpp"

namespace duckdb {

using namespace duckdb_yyjson;

// RAII wrapper for xmlDoc
class JsVarsDocGuard {
public:
	explicit JsVarsDocGuard(xmlDocPtr doc) : doc_(doc) {
	}
	~JsVarsDocGuard() {
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

// Validate and parse JSON, returns doc or nullptr
static yyjson_doc *TryParseJson(const std::string &json) {
	if (json.empty()) {
		return nullptr;
	}
	return yyjson_read(json.c_str(), json.size(), YYJSON_READ_ALLOW_TRAILING_COMMAS | YYJSON_READ_ALLOW_COMMENTS);
}

// Strip JavaScript comments while preserving strings
// Handles // single-line and /* multi-line */ comments
static std::string StripComments(const std::string &script) {
	std::string result;
	result.reserve(script.size());

	size_t i = 0;
	while (i < script.size()) {
		// Check for string start
		if (script[i] == '"' || script[i] == '\'' || script[i] == '`') {
			char quote = script[i];
			result += script[i++];

			// Copy string content, handling escapes
			while (i < script.size()) {
				if (script[i] == '\\' && i + 1 < script.size()) {
					result += script[i++];
					result += script[i++];
				} else if (script[i] == quote) {
					result += script[i++];
					break;
				} else {
					result += script[i++];
				}
			}
			continue;
		}

		// Check for single-line comment
		if (i + 1 < script.size() && script[i] == '/' && script[i + 1] == '/') {
			// Skip until end of line
			while (i < script.size() && script[i] != '\n') {
				i++;
			}
			// Keep the newline for statement boundary detection
			if (i < script.size()) {
				result += '\n';
				i++;
			}
			continue;
		}

		// Check for multi-line comment
		if (i + 1 < script.size() && script[i] == '/' && script[i + 1] == '*') {
			i += 2;
			// Skip until */
			while (i + 1 < script.size() && !(script[i] == '*' && script[i + 1] == '/')) {
				i++;
			}
			if (i + 1 < script.size()) {
				i += 2; // Skip */
			}
			// Add space to preserve token boundaries
			result += ' ';
			continue;
		}

		result += script[i++];
	}

	return result;
}

// Decode JavaScript escape sequences: \xNN (hex) and \uNNNN (unicode)
static std::string DecodeJsEscapes(const std::string &s) {
	std::string result;
	result.reserve(s.size());

	for (size_t i = 0; i < s.size(); i++) {
		if (s[i] == '\\' && i + 1 < s.size()) {
			char next = s[i + 1];
			if (next == 'x' && i + 3 < s.size()) {
				// \xNN hex escape
				std::string hex = s.substr(i + 2, 2);
				try {
					char decoded = static_cast<char>(std::stoi(hex, nullptr, 16));
					result += decoded;
					i += 3;
					continue;
				} catch (...) {
				}
			} else if (next == 'u' && i + 5 < s.size()) {
				// \uNNNN unicode escape
				std::string hex = s.substr(i + 2, 4);
				try {
					int codepoint = std::stoi(hex, nullptr, 16);
					// Simple UTF-8 encoding for BMP characters
					if (codepoint < 0x80) {
						result += static_cast<char>(codepoint);
					} else if (codepoint < 0x800) {
						result += static_cast<char>(0xC0 | (codepoint >> 6));
						result += static_cast<char>(0x80 | (codepoint & 0x3F));
					} else {
						result += static_cast<char>(0xE0 | (codepoint >> 12));
						result += static_cast<char>(0x80 | ((codepoint >> 6) & 0x3F));
						result += static_cast<char>(0x80 | (codepoint & 0x3F));
					}
					i += 5;
					continue;
				} catch (...) {
				}
			} else if (next == 'n') {
				result += '\n';
				i++;
				continue;
			} else if (next == 'r') {
				result += '\r';
				i++;
				continue;
			} else if (next == 't') {
				result += '\t';
				i++;
				continue;
			} else if (next == '\\') {
				result += '\\';
				i++;
				continue;
			} else if (next == '"') {
				result += '"';
				i++;
				continue;
			} else if (next == '\'') {
				result += '\'';
				i++;
				continue;
			} else if (next == '/') {
				result += '/';
				i++;
				continue;
			} else {
				// Unknown escape - just output the character (JS treats \X as X for non-special chars)
				result += next;
				i++;
				continue;
			}
		}
		result += s[i];
	}
	return result;
}

// Extract JSON from JSON.parse('...') or JSON.parse("...")
// Returns decoded JSON string or empty if not found/invalid
static std::string ExtractJsonParse(const std::string &content, size_t start_pos) {
	// Check for JSON.parse(
	if (start_pos + 11 > content.size())
		return "";
	if (content.substr(start_pos, 11) != "JSON.parse(")
		return "";

	size_t pos = start_pos + 11;

	// Skip whitespace
	while (pos < content.size() && std::isspace(static_cast<unsigned char>(content[pos]))) {
		pos++;
	}

	if (pos >= content.size())
		return "";

	// Must be string literal
	char quote = content[pos];
	if (quote != '"' && quote != '\'')
		return "";

	pos++; // Skip opening quote
	size_t str_start = pos;

	// Find end of string, handling escapes
	while (pos < content.size()) {
		if (content[pos] == '\\' && pos + 1 < content.size()) {
			pos += 2; // Skip escape sequence
			continue;
		}
		if (content[pos] == quote) {
			break;
		}
		pos++;
	}

	if (pos >= content.size())
		return "";

	std::string encoded = content.substr(str_start, pos - str_start);
	std::string decoded = DecodeJsEscapes(encoded);

	// Validate with yyjson
	yyjson_doc *doc = TryParseJson(decoded);
	if (doc) {
		yyjson_doc_free(doc);
		return decoded;
	}
	return "";
}

// Extract JSON object/array starting from position
// Returns empty string if not valid JSON
static std::string ExtractJsonValue(const std::string &content, size_t start_pos) {
	if (start_pos >= content.size()) {
		return "";
	}

	// Skip whitespace
	while (start_pos < content.size() && (content[start_pos] == ' ' || content[start_pos] == '\t' ||
	                                      content[start_pos] == '\n' || content[start_pos] == '\r')) {
		start_pos++;
	}

	if (start_pos >= content.size()) {
		return "";
	}

	// Check for JSON.parse() pattern first
	if (start_pos + 11 <= content.size() && content.substr(start_pos, 11) == "JSON.parse(") {
		std::string parsed = ExtractJsonParse(content, start_pos);
		if (!parsed.empty()) {
			return parsed;
		}
	}

	char first_char = content[start_pos];

	// Only extract objects and arrays (not primitives)
	if (first_char != '{' && first_char != '[') {
		return "";
	}

	char open_char = first_char;
	char close_char = (open_char == '{') ? '}' : ']';

	int depth = 1;
	bool in_string = false;
	bool escape_next = false;

	for (size_t i = start_pos + 1; i < content.size() && depth > 0; i++) {
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
			if (c == '{' || c == '[') {
				depth++;
			} else if (c == '}' || c == ']') {
				depth--;
				if (depth == 0) {
					std::string json = content.substr(start_pos, i - start_pos + 1);
					// Validate with yyjson
					yyjson_doc *doc = TryParseJson(json);
					if (doc) {
						yyjson_doc_free(doc);
						return json;
					}
					return "";
				}
			}
		}
	}

	return "";
}

// Check if character is valid for JS identifier
static bool IsIdentifierChar(char c) {
	return std::isalnum(static_cast<unsigned char>(c)) || c == '_' || c == '$';
}

// Extract variable name from assignment pattern
// Returns variable name and position after the = sign
static std::pair<std::string, size_t> ExtractAssignment(const std::string &script, size_t pos) {
	// Skip leading whitespace
	while (pos < script.size() && std::isspace(static_cast<unsigned char>(script[pos]))) {
		pos++;
	}

	if (pos >= script.size()) {
		return {"", 0};
	}

	std::string keyword;
	size_t keyword_start = pos;

	// Check for var/let/const/window
	if (script.substr(pos, 4) == "var " || script.substr(pos, 4) == "let ") {
		keyword = script.substr(pos, 3);
		pos += 4;
	} else if (script.substr(pos, 6) == "const ") {
		keyword = "const";
		pos += 6;
	} else if (script.substr(pos, 7) == "window.") {
		keyword = "window";
		pos += 7;
	} else {
		return {"", 0};
	}

	// Skip whitespace after keyword
	while (pos < script.size() && std::isspace(static_cast<unsigned char>(script[pos]))) {
		pos++;
	}

	// Extract variable name
	size_t name_start = pos;
	while (pos < script.size() && IsIdentifierChar(script[pos])) {
		pos++;
	}

	if (pos == name_start) {
		return {"", 0};
	}

	std::string var_name = script.substr(name_start, pos - name_start);

	// Skip whitespace before =
	while (pos < script.size() && std::isspace(static_cast<unsigned char>(script[pos]))) {
		pos++;
	}

	// Must have = sign
	if (pos >= script.size() || script[pos] != '=') {
		return {"", 0};
	}

	pos++; // Skip =

	// Skip whitespace after =
	while (pos < script.size() && std::isspace(static_cast<unsigned char>(script[pos]))) {
		pos++;
	}

	return {var_name, pos};
}

// Find all variable assignments in script content
static void ExtractVariablesFromScript(const std::string &raw_script, JsVariablesResult &result) {
	// Strip comments first to avoid false positives from commented-out code
	std::string script = StripComments(raw_script);
	size_t pos = 0;

	while (pos < script.size()) {
		// Find start of potential assignment (var, let, const, window.)
		size_t next_var = script.find("var ", pos);
		size_t next_let = script.find("let ", pos);
		size_t next_const = script.find("const ", pos);
		size_t next_window = script.find("window.", pos);

		// Find earliest match
		size_t earliest = std::string::npos;
		if (next_var != std::string::npos && next_var < earliest)
			earliest = next_var;
		if (next_let != std::string::npos && next_let < earliest)
			earliest = next_let;
		if (next_const != std::string::npos && next_const < earliest)
			earliest = next_const;
		if (next_window != std::string::npos && next_window < earliest)
			earliest = next_window;

		if (earliest == std::string::npos) {
			break;
		}

		// Verify it's at a valid position (start of statement)
		bool valid_start = (earliest == 0);
		if (!valid_start && earliest > 0) {
			char prev = script[earliest - 1];
			valid_start = (prev == ';' || prev == '\n' || prev == '\r' || prev == '{' || prev == '}' || prev == '(' ||
			               prev == ')');
		}

		if (!valid_start) {
			pos = earliest + 1;
			continue;
		}

		auto [var_name, value_pos] = ExtractAssignment(script, earliest);

		if (var_name.empty() || value_pos == 0) {
			pos = earliest + 1;
			continue;
		}

		// Try to extract JSON value
		std::string json_value = ExtractJsonValue(script, value_pos);

		if (!json_value.empty()) {
			// Don't overwrite if already found (first occurrence wins)
			if (result.variables.find(var_name) == result.variables.end()) {
				result.variables[var_name] = json_value;
				result.found = true;
			}
		}

		pos = value_pos + 1;
	}
}

// Build combined JSON output using yyjson
static std::string BuildOutputJson(const JsVariablesResult &result) {
	if (!result.found) {
		return "";
	}

	yyjson_mut_doc *doc = yyjson_mut_doc_new(nullptr);
	if (!doc) {
		return "";
	}

	yyjson_mut_val *root = yyjson_mut_obj(doc);
	yyjson_mut_doc_set_root(doc, root);

	for (const auto &pair : result.variables) {
		// Parse the JSON value and embed it
		yyjson_doc *val_doc = yyjson_read(pair.second.c_str(), pair.second.size(), 0);
		if (val_doc) {
			yyjson_val *val_root = yyjson_doc_get_root(val_doc);
			if (val_root) {
				yyjson_mut_val *copy = yyjson_val_mut_copy(doc, val_root);
				yyjson_mut_obj_add_val(doc, root, pair.first.c_str(), copy);
			}
			yyjson_doc_free(val_doc);
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

JsVariablesResult ExtractJsVariables(const std::string &html) {
	JsVariablesResult result;

	if (html.empty()) {
		return result;
	}

	// Parse HTML with libxml2
	JsVarsDocGuard doc(htmlReadMemory(html.c_str(), static_cast<int>(html.size()), nullptr, "UTF-8",
	                                  HTML_PARSE_RECOVER | HTML_PARSE_NOERROR | HTML_PARSE_NOWARNING));

	if (!doc) {
		return result;
	}

	xmlNodePtr root = xmlDocGetRootElement(doc.get());
	if (!root) {
		return result;
	}

	// Recursively find script elements
	std::function<void(xmlNodePtr)> findScripts = [&](xmlNodePtr node) {
		for (xmlNodePtr cur = node; cur; cur = cur->next) {
			if (cur->type == XML_ELEMENT_NODE) {
				if (xmlStrcasecmp(cur->name, BAD_CAST "script") == 0) {
					// Skip scripts with type that isn't JavaScript
					xmlChar *type = xmlGetProp(cur, BAD_CAST "type");
					bool is_js = true;
					if (type) {
						std::string type_str(reinterpret_cast<char *>(type));
						xmlFree(type);
						// Skip non-JS types like application/ld+json, text/template, etc.
						if (type_str.find("javascript") == std::string::npos && type_str != "text/javascript" &&
						    type_str != "module" && !type_str.empty()) {
							is_js = false;
						}
					}

					if (is_js) {
						xmlChar *content = xmlNodeGetContent(cur);
						if (content) {
							std::string script_content(reinterpret_cast<char *>(content));
							xmlFree(content);
							ExtractVariablesFromScript(script_content, result);
						}
					}
				}

				if (cur->children) {
					findScripts(cur->children);
				}
			}
		}
	};

	findScripts(root);

	// Build combined JSON
	result.as_json = BuildOutputJson(result);

	return result;
}

std::string ExtractJsVariablesAsJson(const std::string &html) {
	auto result = ExtractJsVariables(html);
	return result.as_json;
}

} // namespace duckdb
