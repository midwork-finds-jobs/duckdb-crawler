#pragma once

#include <string>
#include <vector>

namespace duckdb {

struct ExtractedLink {
	std::string url;
	bool nofollow; // rel="nofollow" present
};

class LinkParser {
public:
	// Extract all <a href="..."> links from HTML
	// base_url: URL of the page (for resolving relative URLs)
	static std::vector<ExtractedLink> ExtractLinks(const std::string &html, const std::string &base_url);

	// Resolve relative URL to absolute
	static std::string ResolveUrl(const std::string &base_url, const std::string &href);

	// Extract canonical URL from <link rel="canonical">
	static std::string ExtractCanonical(const std::string &html, const std::string &base_url);

	// Check for <meta name="robots" content="nofollow">
	static bool HasNoFollowMeta(const std::string &html);

	// Check for <meta name="robots" content="noindex">
	static bool HasNoIndexMeta(const std::string &html);

	// Check if URL belongs to same domain (or allowed subdomain)
	static bool IsSameDomain(const std::string &url, const std::string &base_domain, bool allow_subdomains);

	// Extract base domain (removes www. prefix)
	static std::string ExtractBaseDomain(const std::string &hostname);

	// Extract domain from URL
	static std::string ExtractDomain(const std::string &url);

	// Extract path from URL
	static std::string ExtractPath(const std::string &url);
};

} // namespace duckdb
