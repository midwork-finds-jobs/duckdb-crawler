#include "link_parser.hpp"
#include <algorithm>
#include <cctype>
#include <set>

namespace duckdb {

// Helper: Convert string to lowercase
static std::string ToLower(const std::string &str) {
	std::string result = str;
	std::transform(result.begin(), result.end(), result.begin(), [](unsigned char c) { return std::tolower(c); });
	return result;
}

// Helper: Trim whitespace
static std::string Trim(const std::string &str) {
	size_t start = 0;
	size_t end = str.length();
	while (start < end && std::isspace(static_cast<unsigned char>(str[start]))) {
		start++;
	}
	while (end > start && std::isspace(static_cast<unsigned char>(str[end - 1]))) {
		end--;
	}
	return str.substr(start, end - start);
}

// Helper: Find attribute value in tag (handles both " and ' quotes)
static std::string ExtractAttribute(const std::string &tag, const std::string &attr) {
	std::string lower_tag = ToLower(tag);
	std::string lower_attr = ToLower(attr);

	// Look for attr= or attr =
	size_t pos = lower_tag.find(lower_attr);
	while (pos != std::string::npos) {
		// Check it's not part of another attribute name
		if (pos > 0 && std::isalnum(static_cast<unsigned char>(lower_tag[pos - 1]))) {
			pos = lower_tag.find(lower_attr, pos + 1);
			continue;
		}

		// Skip to =
		size_t eq_pos = pos + lower_attr.length();
		while (eq_pos < lower_tag.length() && std::isspace(static_cast<unsigned char>(lower_tag[eq_pos]))) {
			eq_pos++;
		}

		if (eq_pos >= lower_tag.length() || lower_tag[eq_pos] != '=') {
			pos = lower_tag.find(lower_attr, pos + 1);
			continue;
		}

		eq_pos++; // Skip =

		// Skip whitespace after =
		while (eq_pos < tag.length() && std::isspace(static_cast<unsigned char>(tag[eq_pos]))) {
			eq_pos++;
		}

		if (eq_pos >= tag.length()) {
			return "";
		}

		char quote = tag[eq_pos];
		if (quote == '"' || quote == '\'') {
			// Quoted value
			size_t value_start = eq_pos + 1;
			size_t value_end = tag.find(quote, value_start);
			if (value_end == std::string::npos) {
				return "";
			}
			return tag.substr(value_start, value_end - value_start);
		} else {
			// Unquoted value - read until whitespace or >
			size_t value_start = eq_pos;
			size_t value_end = value_start;
			while (value_end < tag.length() && !std::isspace(static_cast<unsigned char>(tag[value_end])) &&
			       tag[value_end] != '>') {
				value_end++;
			}
			return tag.substr(value_start, value_end - value_start);
		}
	}

	return "";
}

// Helper: Check if rel attribute contains "nofollow"
static bool HasNoFollowRel(const std::string &rel) {
	std::string lower_rel = ToLower(rel);
	// rel can have multiple values separated by spaces
	return lower_rel.find("nofollow") != std::string::npos;
}

// Helper: Normalize path (resolve . and ..)
static std::string NormalizePath(const std::string &path) {
	std::vector<std::string> segments;
	size_t pos = 0;

	while (pos < path.length()) {
		size_t next = path.find('/', pos);
		if (next == std::string::npos) {
			next = path.length();
		}

		std::string segment = path.substr(pos, next - pos);

		if (segment == "..") {
			if (!segments.empty() && segments.back() != "..") {
				segments.pop_back();
			}
		} else if (segment != "." && !segment.empty()) {
			segments.push_back(segment);
		}

		pos = next + 1;
	}

	std::string result = "/";
	for (size_t i = 0; i < segments.size(); i++) {
		result += segments[i];
		if (i < segments.size() - 1) {
			result += "/";
		}
	}

	// Preserve trailing slash if original had one
	if (path.length() > 1 && path.back() == '/' && result.back() != '/') {
		result += "/";
	}

	return result;
}

std::string LinkParser::ExtractDomain(const std::string &url) {
	size_t proto_end = url.find("://");
	if (proto_end == std::string::npos) {
		return "";
	}

	size_t domain_start = proto_end + 3;
	size_t domain_end = url.find('/', domain_start);
	if (domain_end == std::string::npos) {
		domain_end = url.find('?', domain_start);
	}
	if (domain_end == std::string::npos) {
		domain_end = url.find('#', domain_start);
	}
	if (domain_end == std::string::npos) {
		domain_end = url.length();
	}

	std::string domain = url.substr(domain_start, domain_end - domain_start);

	// Remove port if present
	size_t port_pos = domain.find(':');
	if (port_pos != std::string::npos) {
		domain = domain.substr(0, port_pos);
	}

	return ToLower(domain);
}

std::string LinkParser::ExtractPath(const std::string &url) {
	size_t proto_end = url.find("://");
	if (proto_end == std::string::npos) {
		return "/";
	}

	size_t path_start = url.find('/', proto_end + 3);
	if (path_start == std::string::npos) {
		return "/";
	}

	// Remove query string and fragment
	size_t path_end = url.find('?', path_start);
	if (path_end == std::string::npos) {
		path_end = url.find('#', path_start);
	}
	if (path_end == std::string::npos) {
		path_end = url.length();
	}

	return url.substr(path_start, path_end - path_start);
}

std::string LinkParser::ExtractBaseDomain(const std::string &hostname) {
	std::string domain = ToLower(hostname);
	// Remove www. prefix
	if (domain.length() > 4 && domain.substr(0, 4) == "www.") {
		domain = domain.substr(4);
	}
	return domain;
}

bool LinkParser::IsSameDomain(const std::string &url, const std::string &base_domain, bool allow_subdomains) {
	std::string url_domain = ExtractDomain(url);
	if (url_domain.empty()) {
		return false;
	}

	std::string base = ExtractBaseDomain(base_domain);
	std::string url_base = ExtractBaseDomain(url_domain);

	if (url_base == base) {
		return true;
	}

	if (allow_subdomains) {
		// Check if url_domain ends with .base_domain
		std::string suffix = "." + base;
		if (url_domain.length() > suffix.length() &&
		    url_domain.substr(url_domain.length() - suffix.length()) == suffix) {
			return true;
		}
	}

	return false;
}

std::string LinkParser::ResolveUrl(const std::string &base_url, const std::string &href) {
	if (href.empty()) {
		return "";
	}

	std::string trimmed_href = Trim(href);

	// Already absolute
	if (trimmed_href.find("://") != std::string::npos) {
		return trimmed_href;
	}

	// Protocol-relative (//example.com/path)
	if (trimmed_href.length() >= 2 && trimmed_href[0] == '/' && trimmed_href[1] == '/') {
		size_t proto_end = base_url.find("://");
		if (proto_end != std::string::npos) {
			return base_url.substr(0, proto_end + 1) + trimmed_href;
		}
		return "https:" + trimmed_href;
	}

	// Extract base components
	size_t proto_end = base_url.find("://");
	if (proto_end == std::string::npos) {
		return "";
	}

	size_t domain_start = proto_end + 3;
	size_t path_start = base_url.find('/', domain_start);

	std::string base_origin = (path_start != std::string::npos) ? base_url.substr(0, path_start) : base_url;

	// Absolute path (/path)
	if (trimmed_href[0] == '/') {
		return base_origin + trimmed_href;
	}

	// Relative path (path or ../path)
	std::string base_path = (path_start != std::string::npos) ? base_url.substr(path_start) : "/";

	// Remove query string and fragment from base path
	size_t query_pos = base_path.find('?');
	if (query_pos != std::string::npos) {
		base_path = base_path.substr(0, query_pos);
	}
	size_t frag_pos = base_path.find('#');
	if (frag_pos != std::string::npos) {
		base_path = base_path.substr(0, frag_pos);
	}

	// Remove filename from base path (keep directory)
	size_t last_slash = base_path.rfind('/');
	if (last_slash != std::string::npos) {
		base_path = base_path.substr(0, last_slash + 1);
	}

	std::string combined_path = base_path + trimmed_href;
	return base_origin + NormalizePath(combined_path);
}

std::vector<ExtractedLink> LinkParser::ExtractLinks(const std::string &html, const std::string &base_url) {
	std::vector<ExtractedLink> links;
	std::set<std::string> seen_urls;

	size_t pos = 0;
	while (pos < html.length()) {
		// Find <a (case-insensitive)
		size_t tag_start = std::string::npos;
		for (size_t i = pos; i + 2 < html.length(); i++) {
			if (html[i] == '<' && (html[i + 1] == 'a' || html[i + 1] == 'A') &&
			    (std::isspace(static_cast<unsigned char>(html[i + 2])) || html[i + 2] == '>')) {
				tag_start = i;
				break;
			}
		}

		if (tag_start == std::string::npos) {
			break;
		}

		// Find closing >
		size_t tag_end = html.find('>', tag_start);
		if (tag_end == std::string::npos) {
			break;
		}

		std::string tag = html.substr(tag_start, tag_end - tag_start + 1);

		// Extract href attribute
		std::string href = ExtractAttribute(tag, "href");
		if (!href.empty()) {
			std::string lower_href = ToLower(href);

			// Skip javascript:, mailto:, tel:, data:, #
			if (lower_href.find("javascript:") != 0 && lower_href.find("mailto:") != 0 &&
			    lower_href.find("tel:") != 0 && lower_href.find("data:") != 0 && href[0] != '#') {
				// Resolve to absolute URL
				std::string absolute_url = ResolveUrl(base_url, href);

				// Remove fragment from URL for deduplication
				size_t frag = absolute_url.find('#');
				if (frag != std::string::npos) {
					absolute_url = absolute_url.substr(0, frag);
				}

				// Deduplicate
				if (!absolute_url.empty() && seen_urls.find(absolute_url) == seen_urls.end()) {
					seen_urls.insert(absolute_url);

					// Check for rel="nofollow"
					std::string rel = ExtractAttribute(tag, "rel");
					bool nofollow = HasNoFollowRel(rel);

					links.push_back({absolute_url, nofollow});
				}
			}
		}

		pos = tag_end + 1;
	}

	return links;
}

std::string LinkParser::ExtractCanonical(const std::string &html, const std::string &base_url) {
	// Look for <link rel="canonical" href="...">
	std::string lower_html = ToLower(html);

	size_t pos = 0;
	while (pos < lower_html.length()) {
		size_t link_start = lower_html.find("<link", pos);
		if (link_start == std::string::npos) {
			break;
		}

		size_t link_end = html.find('>', link_start);
		if (link_end == std::string::npos) {
			break;
		}

		std::string tag = html.substr(link_start, link_end - link_start + 1);
		std::string rel = ExtractAttribute(tag, "rel");

		if (ToLower(rel) == "canonical") {
			std::string href = ExtractAttribute(tag, "href");
			if (!href.empty()) {
				return ResolveUrl(base_url, href);
			}
		}

		pos = link_end + 1;
	}

	return "";
}

bool LinkParser::HasNoFollowMeta(const std::string &html) {
	// Look for <meta name="robots" content="...nofollow...">
	std::string lower_html = ToLower(html);

	size_t pos = 0;
	while (pos < lower_html.length()) {
		size_t meta_start = lower_html.find("<meta", pos);
		if (meta_start == std::string::npos) {
			break;
		}

		size_t meta_end = html.find('>', meta_start);
		if (meta_end == std::string::npos) {
			break;
		}

		std::string tag = html.substr(meta_start, meta_end - meta_start + 1);
		std::string name = ExtractAttribute(tag, "name");

		if (ToLower(name) == "robots") {
			std::string content = ExtractAttribute(tag, "content");
			if (ToLower(content).find("nofollow") != std::string::npos) {
				return true;
			}
		}

		pos = meta_end + 1;
	}

	return false;
}

bool LinkParser::HasNoIndexMeta(const std::string &html) {
	// Look for <meta name="robots" content="...noindex...">
	std::string lower_html = ToLower(html);

	size_t pos = 0;
	while (pos < lower_html.length()) {
		size_t meta_start = lower_html.find("<meta", pos);
		if (meta_start == std::string::npos) {
			break;
		}

		size_t meta_end = html.find('>', meta_start);
		if (meta_end == std::string::npos) {
			break;
		}

		std::string tag = html.substr(meta_start, meta_end - meta_start + 1);
		std::string name = ExtractAttribute(tag, "name");

		if (ToLower(name) == "robots") {
			std::string content = ExtractAttribute(tag, "content");
			if (ToLower(content).find("noindex") != std::string::npos) {
				return true;
			}
		}

		pos = meta_end + 1;
	}

	return false;
}

} // namespace duckdb
