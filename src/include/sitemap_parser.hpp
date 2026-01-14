#pragma once

#include <string>
#include <vector>

namespace duckdb {

struct SitemapEntry {
	std::string loc;           // URL
	std::string lastmod;       // Last modification date (optional)
	std::string changefreq;    // Change frequency (optional)
	std::string priority;      // Priority (optional)
};

struct SitemapData {
	std::vector<SitemapEntry> urls;           // URLs from sitemap
	std::vector<std::string> sitemap_urls;    // Nested sitemaps (sitemap index)
	bool is_index = false;                    // True if this is a sitemap index
};

class SitemapParser {
public:
	// Parse sitemap XML content
	static SitemapData Parse(const std::string &xml_content);

	// Common sitemap locations to bruteforce
	static std::vector<std::string> GetCommonSitemapPaths();

	// Extract sitemap URLs from robots.txt content
	static std::vector<std::string> ExtractSitemapsFromRobotsTxt(const std::string &robots_content);
};

} // namespace duckdb
