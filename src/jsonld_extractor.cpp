#include "jsonld_extractor.hpp"
#include <libxml/HTMLparser.h>
#include <libxml/tree.h>
#include <libxml/xpath.h>
#include "duckdb/common/string_util.hpp"
#include "yyjson.hpp"

namespace duckdb {

using namespace duckdb_yyjson;

// RAII wrapper for xmlDoc
class XmlDocGuard {
public:
	explicit XmlDocGuard(xmlDocPtr doc) : doc_(doc) {
	}
	~XmlDocGuard() {
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

// RAII wrapper for yyjson_doc
class YyjsonDocGuard {
public:
	explicit YyjsonDocGuard(yyjson_doc *doc) : doc_(doc) {
	}
	~YyjsonDocGuard() {
		if (doc_) {
			yyjson_doc_free(doc_);
		}
	}
	yyjson_doc *get() const {
		return doc_;
	}
	operator bool() const {
		return doc_ != nullptr;
	}

private:
	yyjson_doc *doc_;
};

// Extract @type value from yyjson object
static std::string ExtractTypeFromVal(yyjson_val *obj) {
	if (!obj || !yyjson_is_obj(obj)) {
		return "";
	}

	yyjson_val *type_val = yyjson_obj_get(obj, "@type");
	if (!type_val) {
		return "";
	}

	// @type can be a string or array of strings
	if (yyjson_is_str(type_val)) {
		return yyjson_get_str(type_val);
	} else if (yyjson_is_arr(type_val)) {
		// Return first type in array
		yyjson_val *first = yyjson_arr_get_first(type_val);
		if (first && yyjson_is_str(first)) {
			return yyjson_get_str(first);
		}
	}
	return "";
}

// Serialize yyjson value back to string
static std::string SerializeJson(yyjson_val *val) {
	if (!val) {
		return "";
	}
	size_t len = 0;
	char *json_str = yyjson_val_write(val, 0, &len);
	if (!json_str) {
		return "";
	}
	std::string result(json_str, len);
	free(json_str);
	return result;
}

// Process a single JSON object - extract @type and add to result
static void ProcessJsonObject(yyjson_val *obj, JsonLdResult &result) {
	std::string type = ExtractTypeFromVal(obj);
	if (!type.empty()) {
		std::string json_str = SerializeJson(obj);
		if (!json_str.empty()) {
			result.by_type[type].push_back(json_str);
			result.found = true;
		}
	}
}

// Process @graph array
static void ProcessGraph(yyjson_val *graph_arr, JsonLdResult &result) {
	if (!graph_arr || !yyjson_is_arr(graph_arr)) {
		return;
	}

	size_t idx, max;
	yyjson_val *item;
	yyjson_arr_foreach(graph_arr, idx, max, item) {
		if (yyjson_is_obj(item)) {
			ProcessJsonObject(item, result);
		}
	}
}

// Process a single JSON-LD document (can be object, array, or contain @graph)
static void ProcessJsonLdDocument(const std::string &json_content, JsonLdResult &result) {
	if (json_content.empty()) {
		return;
	}

	// Parse JSON with yyjson
	yyjson_read_err err;
	YyjsonDocGuard doc(yyjson_read_opts(const_cast<char *>(json_content.c_str()), json_content.size(),
	                                    YYJSON_READ_ALLOW_TRAILING_COMMAS | YYJSON_READ_ALLOW_COMMENTS,
	                                    nullptr, // allocator
	                                    &err));

	if (!doc) {
		// Invalid JSON, skip
		return;
	}

	yyjson_val *root = yyjson_doc_get_root(doc.get());
	if (!root) {
		return;
	}

	// Check if root is an array (multiple objects)
	if (yyjson_is_arr(root)) {
		size_t idx, max;
		yyjson_val *item;
		yyjson_arr_foreach(root, idx, max, item) {
			if (yyjson_is_obj(item)) {
				// Check for @graph in each item
				yyjson_val *graph = yyjson_obj_get(item, "@graph");
				if (graph && yyjson_is_arr(graph)) {
					ProcessGraph(graph, result);
				} else {
					ProcessJsonObject(item, result);
				}
			}
		}
	} else if (yyjson_is_obj(root)) {
		// Check for @graph
		yyjson_val *graph = yyjson_obj_get(root, "@graph");
		if (graph && yyjson_is_arr(graph)) {
			ProcessGraph(graph, result);
		} else {
			ProcessJsonObject(root, result);
		}
	}
}

// Find all <script type="application/ld+json"> content using libxml2
static std::vector<std::string> FindJsonLdScripts(const std::string &html) {
	std::vector<std::string> scripts;

	if (html.empty()) {
		return scripts;
	}

	// Parse HTML with libxml2
	XmlDocGuard doc(htmlReadMemory(html.c_str(), static_cast<int>(html.size()),
	                               nullptr, // URL
	                               "UTF-8", // encoding
	                               HTML_PARSE_RECOVER | HTML_PARSE_NOERROR | HTML_PARSE_NOWARNING));

	if (!doc) {
		return scripts;
	}

	// Get root element
	xmlNodePtr root = xmlDocGetRootElement(doc.get());
	if (!root) {
		return scripts;
	}

	// Recursively search for script elements
	std::function<void(xmlNodePtr)> findScripts = [&](xmlNodePtr node) {
		for (xmlNodePtr cur = node; cur; cur = cur->next) {
			if (cur->type == XML_ELEMENT_NODE) {
				// Check if this is a script tag
				if (xmlStrcasecmp(cur->name, BAD_CAST "script") == 0) {
					// Check type attribute
					xmlChar *type = xmlGetProp(cur, BAD_CAST "type");
					if (type) {
						std::string type_str(reinterpret_cast<char *>(type));
						xmlFree(type);

						// Case-insensitive comparison
						if (StringUtil::Lower(type_str) == "application/ld+json") {
							// Get text content
							xmlChar *content = xmlNodeGetContent(cur);
							if (content) {
								std::string content_str(reinterpret_cast<char *>(content));
								xmlFree(content);

								// Trim whitespace
								size_t start = content_str.find_first_not_of(" \t\n\r");
								size_t end = content_str.find_last_not_of(" \t\n\r");
								if (start != std::string::npos && end != std::string::npos) {
									scripts.push_back(content_str.substr(start, end - start + 1));
								}
							}
						}
					}
				}

				// Recurse into children
				if (cur->children) {
					findScripts(cur->children);
				}
			}
		}
	};

	findScripts(root);
	return scripts;
}

// Build final JSON output keyed by @type
static std::string BuildOutputJson(const JsonLdResult &result) {
	if (!result.found) {
		return "";
	}

	// Use yyjson to build output
	yyjson_mut_doc *doc = yyjson_mut_doc_new(nullptr);
	if (!doc) {
		return "";
	}

	yyjson_mut_val *root = yyjson_mut_obj(doc);
	yyjson_mut_doc_set_root(doc, root);

	for (const auto &pair : result.by_type) {
		const std::string &type = pair.first;
		const std::vector<std::string> &objects = pair.second;

		if (objects.size() == 1) {
			// Single object - parse and embed directly
			yyjson_doc *obj_doc = yyjson_read(objects[0].c_str(), objects[0].size(), 0);
			if (obj_doc) {
				yyjson_val *obj_root = yyjson_doc_get_root(obj_doc);
				if (obj_root) {
					yyjson_mut_val *copy = yyjson_val_mut_copy(doc, obj_root);
					yyjson_mut_obj_add_val(doc, root, type.c_str(), copy);
				}
				yyjson_doc_free(obj_doc);
			}
		} else {
			// Multiple objects - create array
			yyjson_mut_val *arr = yyjson_mut_arr(doc);
			for (const auto &obj_json : objects) {
				yyjson_doc *obj_doc = yyjson_read(obj_json.c_str(), obj_json.size(), 0);
				if (obj_doc) {
					yyjson_val *obj_root = yyjson_doc_get_root(obj_doc);
					if (obj_root) {
						yyjson_mut_val *copy = yyjson_val_mut_copy(doc, obj_root);
						yyjson_mut_arr_append(arr, copy);
					}
					yyjson_doc_free(obj_doc);
				}
			}
			yyjson_mut_obj_add_val(doc, root, type.c_str(), arr);
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

JsonLdResult ExtractJsonLd(const std::string &html) {
	JsonLdResult result;

	// Find all JSON-LD script blocks
	auto scripts = FindJsonLdScripts(html);

	// Process each script block
	for (const auto &script : scripts) {
		ProcessJsonLdDocument(script, result);
	}

	// Build combined JSON output
	result.as_json = BuildOutputJson(result);

	return result;
}

std::string ExtractJsonLdAsJson(const std::string &html) {
	auto result = ExtractJsonLd(html);
	return result.as_json;
}

} // namespace duckdb
