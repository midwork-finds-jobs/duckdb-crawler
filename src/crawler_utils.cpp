#include "crawler_utils.hpp"
#include <zlib.h>
#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstring>
#include <ctime>
#include <sstream>
#include <vector>
#include <cctype>

namespace duckdb {

//===--------------------------------------------------------------------===//
// Error Classification
//===--------------------------------------------------------------------===//

const char* ErrorTypeToString(CrawlErrorType type) {
	switch (type) {
		case CrawlErrorType::NONE: return "";
		case CrawlErrorType::NETWORK_TIMEOUT: return "network_timeout";
		case CrawlErrorType::NETWORK_DNS_FAILURE: return "network_dns_failure";
		case CrawlErrorType::NETWORK_CONNECTION_REFUSED: return "network_connection_refused";
		case CrawlErrorType::NETWORK_SSL_ERROR: return "network_ssl_error";
		case CrawlErrorType::HTTP_CLIENT_ERROR: return "http_client_error";
		case CrawlErrorType::HTTP_SERVER_ERROR: return "http_server_error";
		case CrawlErrorType::HTTP_RATE_LIMITED: return "http_rate_limited";
		case CrawlErrorType::ROBOTS_DISALLOWED: return "robots_disallowed";
		case CrawlErrorType::CONTENT_TOO_LARGE: return "content_too_large";
		case CrawlErrorType::CONTENT_TYPE_REJECTED: return "content_type_rejected";
		case CrawlErrorType::MAX_RETRIES_EXCEEDED: return "max_retries_exceeded";
		default: return "unknown";
	}
}

CrawlErrorType ClassifyError(int status_code, const std::string &error_msg) {
	if (status_code == 429) return CrawlErrorType::HTTP_RATE_LIMITED;
	if (status_code >= 500 && status_code < 600) return CrawlErrorType::HTTP_SERVER_ERROR;
	if (status_code >= 400 && status_code < 500) return CrawlErrorType::HTTP_CLIENT_ERROR;
	if (status_code <= 0) {
		// Network error - classify from message
		if (error_msg.find("timeout") != std::string::npos ||
		    error_msg.find("Timeout") != std::string::npos) {
			return CrawlErrorType::NETWORK_TIMEOUT;
		}
		if (error_msg.find("DNS") != std::string::npos ||
		    error_msg.find("resolve") != std::string::npos) {
			return CrawlErrorType::NETWORK_DNS_FAILURE;
		}
		if (error_msg.find("SSL") != std::string::npos ||
		    error_msg.find("certificate") != std::string::npos) {
			return CrawlErrorType::NETWORK_SSL_ERROR;
		}
		if (error_msg.find("refused") != std::string::npos ||
		    error_msg.find("connect") != std::string::npos) {
			return CrawlErrorType::NETWORK_CONNECTION_REFUSED;
		}
		return CrawlErrorType::NETWORK_TIMEOUT;  // Default network error
	}
	return CrawlErrorType::NONE;
}

//===--------------------------------------------------------------------===//
// Compression Utilities
//===--------------------------------------------------------------------===//

std::string DecompressGzip(const std::string &compressed_data) {
	if (compressed_data.empty()) {
		return "";
	}

	z_stream zs;
	memset(&zs, 0, sizeof(zs));

	// Use inflateInit2 with 16+MAX_WBITS to handle gzip format
	if (inflateInit2(&zs, 16 + MAX_WBITS) != Z_OK) {
		return "";
	}

	zs.next_in = reinterpret_cast<Bytef*>(const_cast<char*>(compressed_data.data()));
	zs.avail_in = static_cast<uInt>(compressed_data.size());

	std::string decompressed;
	char buffer[32768];

	int ret;
	do {
		zs.next_out = reinterpret_cast<Bytef*>(buffer);
		zs.avail_out = sizeof(buffer);

		ret = inflate(&zs, Z_NO_FLUSH);

		if (ret != Z_OK && ret != Z_STREAM_END && ret != Z_BUF_ERROR) {
			inflateEnd(&zs);
			return "";
		}

		size_t have = sizeof(buffer) - zs.avail_out;
		decompressed.append(buffer, have);
	} while (ret != Z_STREAM_END);

	inflateEnd(&zs);
	return decompressed;
}

bool IsGzippedData(const std::string &data) {
	return data.size() >= 2 &&
	       static_cast<unsigned char>(data[0]) == 0x1f &&
	       static_cast<unsigned char>(data[1]) == 0x8b;
}

//===--------------------------------------------------------------------===//
// Backoff and Rate Limiting
//===--------------------------------------------------------------------===//

int FibonacciBackoffSeconds(int n, int max_seconds) {
	if (n <= 1) return 3;
	int a = 3, b = 3;
	for (int i = 2; i <= n; i++) {
		int temp = a + b;
		a = b;
		b = temp;
		if (b > max_seconds) return max_seconds;
	}
	return std::min(b, max_seconds);
}

//===--------------------------------------------------------------------===//
// Date/Time Utilities
//===--------------------------------------------------------------------===//

std::string ParseAndValidateServerDate(const std::string &server_date) {
	if (server_date.empty()) {
		return "";
	}

	// Get current time
	auto now = std::chrono::system_clock::now();
	auto now_time_t = std::chrono::system_clock::to_time_t(now);

	// Parse HTTP date - format: "Day, DD Mon YYYY HH:MM:SS GMT"
	struct tm tm_parsed = {};
	const char *months[] = {"Jan", "Feb", "Mar", "Apr", "May", "Jun",
	                        "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};

	int day, year, hour, min, sec;
	char month_str[4] = {0};

	int parsed = sscanf(server_date.c_str(), "%*[^,], %d %3s %d %d:%d:%d",
	                    &day, month_str, &year, &hour, &min, &sec);

	if (parsed != 6) {
		return "";
	}

	// Convert month string to number
	int month = -1;
	for (int i = 0; i < 12; i++) {
		if (strncmp(month_str, months[i], 3) == 0) {
			month = i;
			break;
		}
	}

	if (month < 0) {
		return "";
	}

	// Build tm struct
	tm_parsed.tm_year = year - 1900;
	tm_parsed.tm_mon = month;
	tm_parsed.tm_mday = day;
	tm_parsed.tm_hour = hour;
	tm_parsed.tm_min = min;
	tm_parsed.tm_sec = sec;

	// Convert to time_t (assuming GMT)
	time_t server_time = timegm(&tm_parsed);
	if (server_time == -1) {
		return "";
	}

	// Check if server time is within 15 minutes of local time
	double diff_seconds = difftime(server_time, now_time_t);
	if (std::abs(diff_seconds) > 15 * 60) {
		return "";
	}

	// Format as ISO timestamp for DuckDB
	char buf[32];
	struct tm *gmt = gmtime(&server_time);
	strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", gmt);
	return std::string(buf);
}

//===--------------------------------------------------------------------===//
// URL Utilities
//===--------------------------------------------------------------------===//

std::string ExtractDomain(const std::string &url) {
	size_t proto_end = url.find("://");
	if (proto_end == std::string::npos) {
		return "";
	}
	size_t domain_start = proto_end + 3;
	size_t domain_end = url.find('/', domain_start);
	if (domain_end == std::string::npos) {
		domain_end = url.length();
	}
	std::string domain = url.substr(domain_start, domain_end - domain_start);

	// Remove port if present
	size_t port_pos = domain.find(':');
	if (port_pos != std::string::npos) {
		domain = domain.substr(0, port_pos);
	}

	return domain;
}

std::string ExtractPath(const std::string &url) {
	size_t proto_end = url.find("://");
	if (proto_end == std::string::npos) {
		return "/";
	}
	size_t path_start = url.find('/', proto_end + 3);
	if (path_start == std::string::npos) {
		return "/";
	}
	return url.substr(path_start);
}

std::string GenerateSurtKey(const std::string &url) {
	size_t proto_end = url.find("://");
	if (proto_end == std::string::npos) {
		return url;
	}
	size_t domain_start = proto_end + 3;
	size_t domain_end = url.find('/', domain_start);
	if (domain_end == std::string::npos) {
		domain_end = url.length();
	}

	std::string domain = url.substr(domain_start, domain_end - domain_start);

	// Remove port if present
	size_t port_pos = domain.find(':');
	if (port_pos != std::string::npos) {
		domain = domain.substr(0, port_pos);
	}

	// Convert to lowercase
	std::transform(domain.begin(), domain.end(), domain.begin(), ::tolower);

	// Strip 'www.' prefix per SURT convention
	if (domain.substr(0, 4) == "www.") {
		domain = domain.substr(4);
	}

	// Split domain by '.' and reverse
	std::vector<std::string> parts;
	size_t pos = 0;
	size_t prev = 0;
	while ((pos = domain.find('.', prev)) != std::string::npos) {
		parts.push_back(domain.substr(prev, pos - prev));
		prev = pos + 1;
	}
	parts.push_back(domain.substr(prev));

	// Build reversed domain with commas
	std::string surt;
	for (auto it = parts.rbegin(); it != parts.rend(); ++it) {
		if (!surt.empty()) {
			surt += ",";
		}
		surt += *it;
	}

	// Add path
	surt += ")";
	if (domain_end < url.length()) {
		surt += url.substr(domain_end);
	} else {
		surt += "/";
	}

	return surt;
}

std::string GenerateDomainSurt(const std::string &hostname) {
	if (hostname.empty()) {
		return "";
	}

	std::string domain = hostname;
	std::transform(domain.begin(), domain.end(), domain.begin(), ::tolower);

	// Remove port if present
	size_t port_pos = domain.find(':');
	if (port_pos != std::string::npos) {
		domain = domain.substr(0, port_pos);
	}

	// Strip 'www.' prefix
	if (domain.substr(0, 4) == "www.") {
		domain = domain.substr(4);
	}

	// Split and reverse
	std::vector<std::string> parts;
	size_t pos = 0;
	size_t prev = 0;
	while ((pos = domain.find('.', prev)) != std::string::npos) {
		parts.push_back(domain.substr(prev, pos - prev));
		prev = pos + 1;
	}
	parts.push_back(domain.substr(prev));

	std::string surt;
	for (auto it = parts.rbegin(); it != parts.rend(); ++it) {
		if (!surt.empty()) {
			surt += ",";
		}
		surt += *it;
	}
	surt += ")";

	return surt;
}

std::string GenerateContentHash(const std::string &content) {
	if (content.empty()) {
		return "";
	}
	std::hash<std::string> hasher;
	size_t hash_value = hasher(content);
	char buf[17];
	snprintf(buf, sizeof(buf), "%016zx", hash_value);
	return std::string(buf);
}

//===--------------------------------------------------------------------===//
// Content-Type Utilities
//===--------------------------------------------------------------------===//

bool ContentTypeMatches(const std::string &content_type, const std::string &pattern) {
	if (pattern.empty()) {
		return false;
	}
	// Extract main type from content_type
	std::string ct = content_type;
	size_t semicolon = ct.find(';');
	if (semicolon != std::string::npos) {
		ct = ct.substr(0, semicolon);
	}
	// Trim whitespace
	while (!ct.empty() && std::isspace(ct.back())) ct.pop_back();
	while (!ct.empty() && std::isspace(ct.front())) ct.erase(ct.begin());

	// Convert both to lowercase
	std::string ct_lower = ct;
	std::string pat_lower = pattern;
	std::transform(ct_lower.begin(), ct_lower.end(), ct_lower.begin(), ::tolower);
	std::transform(pat_lower.begin(), pat_lower.end(), pat_lower.begin(), ::tolower);

	// Check for wildcard (e.g., "text/*")
	if (pat_lower.length() >= 2 && pat_lower.substr(pat_lower.length() - 2) == "/*") {
		std::string prefix = pat_lower.substr(0, pat_lower.length() - 1);
		return ct_lower.find(prefix) == 0;
	}

	return ct_lower == pat_lower;
}

bool IsContentTypeAcceptable(const std::string &content_type,
                             const std::string &accept_types,
                             const std::string &reject_types) {
	if (accept_types.empty() && reject_types.empty()) {
		return true;
	}

	// Check accept list first
	if (!accept_types.empty()) {
		bool accepted = false;
		std::istringstream accept_stream(accept_types);
		std::string pattern;
		while (std::getline(accept_stream, pattern, ',')) {
			while (!pattern.empty() && std::isspace(pattern.back())) pattern.pop_back();
			while (!pattern.empty() && std::isspace(pattern.front())) pattern.erase(pattern.begin());
			if (ContentTypeMatches(content_type, pattern)) {
				accepted = true;
				break;
			}
		}
		if (!accepted) {
			return false;
		}
	}

	// Check reject list
	if (!reject_types.empty()) {
		std::istringstream reject_stream(reject_types);
		std::string pattern;
		while (std::getline(reject_stream, pattern, ',')) {
			while (!pattern.empty() && std::isspace(pattern.back())) pattern.pop_back();
			while (!pattern.empty() && std::isspace(pattern.front())) pattern.erase(pattern.begin());
			if (ContentTypeMatches(content_type, pattern)) {
				return false;
			}
		}
	}

	return true;
}

} // namespace duckdb
