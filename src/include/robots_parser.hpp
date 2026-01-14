#pragma once

#include <string>
#include <vector>
#include <unordered_map>

namespace duckdb {

// Rules for a specific user-agent
struct RobotsRules {
	double crawl_delay = -1.0;   // -1 means not set
	double request_rate = -1.0;  // Derived from Request-rate: n/m (m/n seconds per request)
	std::vector<std::string> disallow;
	std::vector<std::string> allow;

	bool HasCrawlDelay() const { return crawl_delay >= 0 || request_rate >= 0; }

	// Get effective delay (lower of crawl_delay or request_rate, if both set)
	double GetEffectiveDelay() const {
		if (crawl_delay >= 0 && request_rate >= 0) {
			return std::max(crawl_delay, request_rate);  // Use stricter limit
		}
		if (crawl_delay >= 0) return crawl_delay;
		if (request_rate >= 0) return request_rate;
		return -1.0;
	}
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
