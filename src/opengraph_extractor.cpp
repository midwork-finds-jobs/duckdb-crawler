#include "opengraph_extractor.hpp"
#include <libxml/HTMLparser.h>
#include <libxml/tree.h>
#include "duckdb/common/string_util.hpp"
#include "yyjson.hpp"

namespace duckdb {

using namespace duckdb_yyjson;

// RAII wrapper for xmlDoc
class HtmlDocGuard {
public:
	explicit HtmlDocGuard(xmlDocPtr doc) : doc_(doc) {
	}
	~HtmlDocGuard() {
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

// Helper: get attribute value from xmlNode, returns empty string if not found
static std::string GetAttribute(xmlNodePtr node, const char *attr) {
	xmlChar *value = xmlGetProp(node, BAD_CAST attr);
	if (!value) {
		return "";
	}
	std::string result(reinterpret_cast<char *>(value));
	xmlFree(value);
	return result;
}

// Decode HTML entities using libxml2
static std::string DecodeHtmlEntities(const std::string &str) {
	if (str.empty()) {
		return str;
	}

	// xmlDecodeEntitiesReentrant requires a document context
	// For standalone decoding, we parse a minimal HTML doc
	std::string wrapped = "<!DOCTYPE html><html><body>" + str + "</body></html>";
	HtmlDocGuard doc(htmlReadMemory(wrapped.c_str(), static_cast<int>(wrapped.size()), nullptr, "UTF-8",
	                                HTML_PARSE_RECOVER | HTML_PARSE_NOERROR | HTML_PARSE_NOWARNING));

	if (!doc) {
		return str;
	}

	xmlNodePtr root = xmlDocGetRootElement(doc.get());
	if (!root) {
		return str;
	}

	// Navigate to body content
	std::function<xmlNodePtr(xmlNodePtr)> findBody = [&](xmlNodePtr node) -> xmlNodePtr {
		for (xmlNodePtr cur = node; cur; cur = cur->next) {
			if (cur->type == XML_ELEMENT_NODE) {
				if (xmlStrcasecmp(cur->name, BAD_CAST "body") == 0) {
					return cur;
				}
				if (cur->children) {
					xmlNodePtr found = findBody(cur->children);
					if (found)
						return found;
				}
			}
		}
		return nullptr;
	};

	xmlNodePtr body = findBody(root);
	if (!body) {
		return str;
	}

	xmlChar *content = xmlNodeGetContent(body);
	if (!content) {
		return str;
	}

	std::string result(reinterpret_cast<char *>(content));
	xmlFree(content);
	return result;
}

// Extract meta tags with specified attribute (property or name)
static void ExtractMetaTags(xmlNodePtr node, const char *attr_name,
                            std::vector<std::pair<std::string, std::string>> &results) {
	for (xmlNodePtr cur = node; cur; cur = cur->next) {
		if (cur->type == XML_ELEMENT_NODE) {
			// Check if this is a meta tag
			if (xmlStrcasecmp(cur->name, BAD_CAST "meta") == 0) {
				std::string prop = GetAttribute(cur, attr_name);
				if (!prop.empty()) {
					std::string content = GetAttribute(cur, "content");
					if (!content.empty()) {
						// Decode HTML entities in content
						content = DecodeHtmlEntities(content);
						results.emplace_back(prop, content);
					}
				}
			}

			// Recurse into children
			if (cur->children) {
				ExtractMetaTags(cur->children, attr_name, results);
			}
		}
	}
}

// Extract canonical link
static std::string ExtractCanonical(xmlNodePtr node) {
	for (xmlNodePtr cur = node; cur; cur = cur->next) {
		if (cur->type == XML_ELEMENT_NODE) {
			// Check if this is a link tag
			if (xmlStrcasecmp(cur->name, BAD_CAST "link") == 0) {
				std::string rel = GetAttribute(cur, "rel");
				if (StringUtil::Lower(rel) == "canonical") {
					return GetAttribute(cur, "href");
				}
			}

			// Recurse into children
			if (cur->children) {
				std::string result = ExtractCanonical(cur->children);
				if (!result.empty()) {
					return result;
				}
			}
		}
	}
	return "";
}

// Build JSON output for OpenGraph using yyjson
static std::string BuildOpenGraphJson(const OpenGraphResult &result) {
	if (!result.found) {
		return "";
	}

	yyjson_mut_doc *doc = yyjson_mut_doc_new(nullptr);
	if (!doc) {
		return "";
	}

	yyjson_mut_val *root = yyjson_mut_obj(doc);
	yyjson_mut_doc_set_root(doc, root);

	// Add og:* properties
	for (const auto &prop : result.properties) {
		yyjson_mut_obj_add_strcpy(doc, root, prop.first.c_str(), prop.second.c_str());
	}

	// Add twitter:* as nested object if present
	if (!result.twitter.empty()) {
		yyjson_mut_val *twitter = yyjson_mut_obj(doc);
		for (const auto &prop : result.twitter) {
			yyjson_mut_obj_add_strcpy(doc, twitter, prop.first.c_str(), prop.second.c_str());
		}
		yyjson_mut_obj_add_val(doc, root, "twitter", twitter);
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

// Build JSON output for MetaTags using yyjson
static std::string BuildMetaTagsJson(const MetaTagsResult &result) {
	if (!result.found) {
		return "";
	}

	yyjson_mut_doc *doc = yyjson_mut_doc_new(nullptr);
	if (!doc) {
		return "";
	}

	yyjson_mut_val *root = yyjson_mut_obj(doc);
	yyjson_mut_doc_set_root(doc, root);

	if (!result.description.empty()) {
		yyjson_mut_obj_add_strcpy(doc, root, "description", result.description.c_str());
	}
	if (!result.keywords.empty()) {
		yyjson_mut_obj_add_strcpy(doc, root, "keywords", result.keywords.c_str());
	}
	if (!result.author.empty()) {
		yyjson_mut_obj_add_strcpy(doc, root, "author", result.author.c_str());
	}
	if (!result.canonical.empty()) {
		yyjson_mut_obj_add_strcpy(doc, root, "canonical", result.canonical.c_str());
	}
	if (!result.robots.empty()) {
		yyjson_mut_obj_add_strcpy(doc, root, "robots", result.robots.c_str());
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

OpenGraphResult ExtractOpenGraph(const std::string &html) {
	OpenGraphResult result;

	if (html.empty()) {
		return result;
	}

	// Parse HTML with libxml2
	HtmlDocGuard doc(htmlReadMemory(html.c_str(), static_cast<int>(html.size()), nullptr, "UTF-8",
	                                HTML_PARSE_RECOVER | HTML_PARSE_NOERROR | HTML_PARSE_NOWARNING));

	if (!doc) {
		return result;
	}

	xmlNodePtr root = xmlDocGetRootElement(doc.get());
	if (!root) {
		return result;
	}

	// Extract og:* properties (using property attribute)
	std::vector<std::pair<std::string, std::string>> property_tags;
	ExtractMetaTags(root, "property", property_tags);

	for (const auto &tag : property_tags) {
		const std::string &prop = tag.first;
		const std::string &content = tag.second;

		if (prop.size() > 3 && prop.substr(0, 3) == "og:") {
			std::string key = prop.substr(3); // Remove "og:" prefix
			result.properties[key] = content;
			result.found = true;

			// Map common fields
			if (key == "title") {
				result.title = content;
			} else if (key == "description") {
				result.description = content;
			} else if (key == "image") {
				result.image = content;
			} else if (key == "url") {
				result.url = content;
			} else if (key == "type") {
				result.type = content;
			} else if (key == "site_name") {
				result.site_name = content;
			}
		}
	}

	// Extract twitter:* properties (using name attribute)
	std::vector<std::pair<std::string, std::string>> name_tags;
	ExtractMetaTags(root, "name", name_tags);

	for (const auto &tag : name_tags) {
		const std::string &name = tag.first;
		const std::string &content = tag.second;

		if (name.size() > 8 && name.substr(0, 8) == "twitter:") {
			std::string key = name.substr(8); // Remove "twitter:" prefix
			result.twitter[key] = content;
			result.found = true;
		}
	}

	// Build JSON output
	result.as_json = BuildOpenGraphJson(result);

	return result;
}

std::string ExtractOpenGraphAsJson(const std::string &html) {
	auto result = ExtractOpenGraph(html);
	return result.as_json;
}

MetaTagsResult ExtractMetaTags(const std::string &html) {
	MetaTagsResult result;

	if (html.empty()) {
		return result;
	}

	// Parse HTML with libxml2
	HtmlDocGuard doc(htmlReadMemory(html.c_str(), static_cast<int>(html.size()), nullptr, "UTF-8",
	                                HTML_PARSE_RECOVER | HTML_PARSE_NOERROR | HTML_PARSE_NOWARNING));

	if (!doc) {
		return result;
	}

	xmlNodePtr root = xmlDocGetRootElement(doc.get());
	if (!root) {
		return result;
	}

	// Extract meta tags with name attribute
	std::vector<std::pair<std::string, std::string>> name_tags;
	ExtractMetaTags(root, "name", name_tags);

	for (const auto &tag : name_tags) {
		const std::string &name = StringUtil::Lower(tag.first);
		const std::string &content = tag.second;

		if (name == "description") {
			result.description = content;
			result.found = true;
		} else if (name == "keywords") {
			result.keywords = content;
			result.found = true;
		} else if (name == "author") {
			result.author = content;
			result.found = true;
		} else if (name == "robots") {
			result.robots = content;
			result.found = true;
		}
	}

	// Extract canonical link
	result.canonical = ExtractCanonical(root);
	if (!result.canonical.empty()) {
		result.found = true;
	}

	// Build JSON output
	result.as_json = BuildMetaTagsJson(result);

	return result;
}

} // namespace duckdb
