#include "sitemap_parser.hpp"
#include <regex>
#include <sstream>

namespace duckdb {

// Simple XML tag extraction helper
static std::string ExtractTagContent(const std::string &xml, const std::string &tag, size_t start_pos, size_t &end_pos) {
	std::string open_tag = "<" + tag + ">";
	std::string close_tag = "</" + tag + ">";

	size_t tag_start = xml.find(open_tag, start_pos);
	if (tag_start == std::string::npos) {
		// Try with attributes: <tag ...>
		std::string open_tag_attr = "<" + tag;
		tag_start = xml.find(open_tag_attr, start_pos);
		if (tag_start == std::string::npos) {
			end_pos = std::string::npos;
			return "";
		}
		// Find the closing >
		size_t attr_end = xml.find('>', tag_start);
		if (attr_end == std::string::npos) {
			end_pos = std::string::npos;
			return "";
		}
		tag_start = attr_end + 1;
	} else {
		tag_start += open_tag.length();
	}

	size_t tag_end = xml.find(close_tag, tag_start);
	if (tag_end == std::string::npos) {
		end_pos = std::string::npos;
		return "";
	}

	end_pos = tag_end + close_tag.length();
	return xml.substr(tag_start, tag_end - tag_start);
}

// Find all occurrences of a tag block
static std::vector<std::string> FindAllBlocks(const std::string &xml, const std::string &tag) {
	std::vector<std::string> blocks;
	std::string open_tag = "<" + tag;
	std::string close_tag = "</" + tag + ">";

	size_t pos = 0;
	while (pos < xml.length()) {
		size_t block_start = xml.find(open_tag, pos);
		if (block_start == std::string::npos) {
			break;
		}

		size_t block_end = xml.find(close_tag, block_start);
		if (block_end == std::string::npos) {
			break;
		}

		block_end += close_tag.length();
		blocks.push_back(xml.substr(block_start, block_end - block_start));
		pos = block_end;
	}

	return blocks;
}

SitemapData SitemapParser::Parse(const std::string &xml_content) {
	SitemapData result;

	// Check if this is a sitemap index
	if (xml_content.find("<sitemapindex") != std::string::npos) {
		result.is_index = true;

		// Extract all <sitemap> blocks
		auto sitemap_blocks = FindAllBlocks(xml_content, "sitemap");
		for (const auto &block : sitemap_blocks) {
			size_t end_pos;
			std::string loc = ExtractTagContent(block, "loc", 0, end_pos);
			if (!loc.empty()) {
				// Trim whitespace
				size_t start = loc.find_first_not_of(" \t\n\r");
				size_t end = loc.find_last_not_of(" \t\n\r");
				if (start != std::string::npos && end != std::string::npos) {
					result.sitemap_urls.push_back(loc.substr(start, end - start + 1));
				}
			}
		}
	} else {
		// Regular sitemap - extract all <url> blocks
		auto url_blocks = FindAllBlocks(xml_content, "url");
		for (const auto &block : url_blocks) {
			SitemapEntry entry;
			size_t end_pos;

			entry.loc = ExtractTagContent(block, "loc", 0, end_pos);
			if (!entry.loc.empty()) {
				// Trim whitespace
				size_t start = entry.loc.find_first_not_of(" \t\n\r");
				size_t end = entry.loc.find_last_not_of(" \t\n\r");
				if (start != std::string::npos && end != std::string::npos) {
					entry.loc = entry.loc.substr(start, end - start + 1);
				}
			}

			entry.lastmod = ExtractTagContent(block, "lastmod", 0, end_pos);
			entry.changefreq = ExtractTagContent(block, "changefreq", 0, end_pos);
			entry.priority = ExtractTagContent(block, "priority", 0, end_pos);

			if (!entry.loc.empty()) {
				result.urls.push_back(std::move(entry));
			}
		}
	}

	return result;
}

std::vector<std::string> SitemapParser::GetCommonSitemapPaths() {
	return {
	    "/sitemap.xml",
	    "/sitemap_index.xml",
	    "/sitemap-index.xml",
	    "/sitemapindex.xml",
	    "/sitemap/sitemap.xml",
	    "/sitemaps/sitemap.xml",
	    "/sitemap1.xml",
	    "/sitemap-1.xml",
	    "/post-sitemap.xml",
	    "/page-sitemap.xml",
	    "/product-sitemap.xml",
	    "/category-sitemap.xml",
	    "/wp-sitemap.xml"
	};
}

std::vector<std::string> SitemapParser::ExtractSitemapsFromRobotsTxt(const std::string &robots_content) {
	std::vector<std::string> sitemaps;

	std::istringstream stream(robots_content);
	std::string line;

	while (std::getline(stream, line)) {
		// Remove carriage return if present
		if (!line.empty() && line.back() == '\r') {
			line.pop_back();
		}

		// Skip empty lines and comments
		if (line.empty() || line[0] == '#') {
			continue;
		}

		// Check for Sitemap directive (case-insensitive)
		std::string lower_line = line;
		for (auto &c : lower_line) {
			c = std::tolower(c);
		}

		if (lower_line.find("sitemap:") == 0) {
			// Extract URL after "Sitemap:"
			std::string url = line.substr(8); // Length of "Sitemap:"

			// Trim whitespace
			size_t start = url.find_first_not_of(" \t");
			size_t end = url.find_last_not_of(" \t\r\n");

			if (start != std::string::npos && end != std::string::npos) {
				sitemaps.push_back(url.substr(start, end - start + 1));
			}
		}
	}

	return sitemaps;
}

} // namespace duckdb
