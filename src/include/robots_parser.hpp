#pragma once

#include <string>
#include <vector>
#include <unordered_map>

namespace duckdb {

// Rules for a specific user-agent
struct RobotsRules {
	double crawl_delay = -1.0;  // -1 means not set
	std::vector<std::string> disallow;
	std::vector<std::string> allow;

	bool HasCrawlDelay() const { return crawl_delay >= 0; }
};

// Parsed robots.txt content
struct RobotsData {
	// Rules keyed by user-agent (lowercase)
	std::unordered_map<std::string, RobotsRules> user_agents;
	// Sitemap URLs found in robots.txt
	std::vector<std::string> sitemaps;
};

class RobotsParser {
public:
	// Parse full robots.txt content
	static RobotsData Parse(const std::string &robots_txt_content);

	// Get rules for a specific user-agent (with fallback to *)
	static RobotsRules GetRulesForUserAgent(const RobotsData &data, const std::string &user_agent);

	// Check if a URL path is allowed for given rules
	static bool IsAllowed(const RobotsRules &rules, const std::string &path);

	// Legacy: just extract sitemap URLs
	static std::vector<std::string> ParseSitemapUrls(const std::string &robots_txt_content);
};

} // namespace duckdb
