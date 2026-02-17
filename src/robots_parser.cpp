#include "robots_parser.hpp"
#include <sstream>
#include <algorithm>
#include <cctype>

namespace duckdb {

// Trim whitespace from both ends of a string
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

// Convert string to lowercase
static std::string ToLower(const std::string &str) {
	std::string result = str;
	std::transform(result.begin(), result.end(), result.begin(), [](unsigned char c) { return std::tolower(c); });
	return result;
}

// Case-insensitive starts with check
static bool StartsWithCaseInsensitive(const std::string &str, const std::string &prefix) {
	if (str.length() < prefix.length()) {
		return false;
	}

	for (size_t i = 0; i < prefix.length(); i++) {
		if (std::tolower(static_cast<unsigned char>(str[i])) != std::tolower(static_cast<unsigned char>(prefix[i]))) {
			return false;
		}
	}

	return true;
}

// Extract value after "Key: value"
static std::string ExtractValue(const std::string &line, size_t prefix_len) {
	return Trim(line.substr(prefix_len));
}

RobotsData RobotsParser::Parse(const std::string &robots_txt_content) {
	RobotsData data;

	std::istringstream stream(robots_txt_content);
	std::string line;

	std::string current_user_agent;
	RobotsRules *current_rules = nullptr;

	while (std::getline(stream, line)) {
		line = Trim(line);

		// Skip empty lines and comments
		if (line.empty() || line[0] == '#') {
			continue;
		}

		// Check for User-agent directive
		if (StartsWithCaseInsensitive(line, "user-agent:")) {
			std::string ua = ToLower(ExtractValue(line, 11));
			if (!ua.empty()) {
				current_user_agent = ua;
				// Create entry if doesn't exist
				if (data.user_agents.find(ua) == data.user_agents.end()) {
					data.user_agents[ua] = RobotsRules();
				}
				current_rules = &data.user_agents[ua];
			}
			continue;
		}

		// Check for Crawl-delay directive
		if (StartsWithCaseInsensitive(line, "crawl-delay:") && current_rules) {
			std::string delay_str = ExtractValue(line, 12);
			try {
				double delay = std::stod(delay_str);
				if (delay >= 0) {
					current_rules->crawl_delay = delay;
				}
			} catch (...) {
				// Ignore invalid crawl-delay values
			}
			continue;
		}

		// Check for Request-rate directive (format: n/m meaning n requests per m seconds)
		if (StartsWithCaseInsensitive(line, "request-rate:") && current_rules) {
			std::string rate_str = ExtractValue(line, 13);
			try {
				size_t slash_pos = rate_str.find('/');
				if (slash_pos != std::string::npos) {
					double n = std::stod(rate_str.substr(0, slash_pos));
					double m = std::stod(rate_str.substr(slash_pos + 1));
					if (n > 0 && m > 0) {
						// Convert to seconds per request (m/n)
						current_rules->request_rate = m / n;
					}
				}
			} catch (...) {
				// Ignore invalid request-rate values
			}
			continue;
		}

		// Check for Disallow directive
		if (StartsWithCaseInsensitive(line, "disallow:") && current_rules) {
			std::string path = ExtractValue(line, 9);
			if (!path.empty()) {
				current_rules->disallow.push_back(path);
			}
			continue;
		}

		// Check for Allow directive
		if (StartsWithCaseInsensitive(line, "allow:") && current_rules) {
			std::string path = ExtractValue(line, 6);
			if (!path.empty()) {
				current_rules->allow.push_back(path);
			}
			continue;
		}

		// Check for Sitemap directive (global, not per user-agent)
		if (StartsWithCaseInsensitive(line, "sitemap:")) {
			std::string url = ExtractValue(line, 8);
			if (!url.empty()) {
				data.sitemaps.push_back(url);
			}
			continue;
		}
	}

	return data;
}

RobotsRules RobotsParser::GetRulesForUserAgent(const RobotsData &data, const std::string &user_agent) {
	std::string ua_lower = ToLower(user_agent);

	// Try exact match first
	auto it = data.user_agents.find(ua_lower);
	if (it != data.user_agents.end()) {
		return it->second;
	}

	// Try prefix match (e.g., "mybot" matches "MyBot/1.0")
	for (const auto &entry : data.user_agents) {
		if (entry.first != "*" && ua_lower.find(entry.first) == 0) {
			return entry.second;
		}
	}

	// Fall back to wildcard
	it = data.user_agents.find("*");
	if (it != data.user_agents.end()) {
		return it->second;
	}

	// No rules found - return empty (allow all)
	return RobotsRules();
}

bool RobotsParser::IsAllowed(const RobotsRules &rules, const std::string &path) {
	// Check Allow rules first (more specific)
	for (const auto &allow : rules.allow) {
		if (path.find(allow) == 0) {
			return true;
		}
	}

	// Check Disallow rules
	for (const auto &disallow : rules.disallow) {
		if (disallow.empty()) {
			continue; // Empty disallow means allow all
		}
		if (path.find(disallow) == 0) {
			return false;
		}
	}

	// Default: allowed
	return true;
}

std::vector<std::string> RobotsParser::ParseSitemapUrls(const std::string &robots_txt_content) {
	RobotsData data = Parse(robots_txt_content);
	return data.sitemaps;
}

} // namespace duckdb
