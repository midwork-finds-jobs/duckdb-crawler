#include "crawler_function.hpp"
#include "robots_parser.hpp"
#include "sitemap_parser.hpp"
#include "http_client.hpp"
#include "link_parser.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/appender.hpp"
#include <set>

#include <algorithm>
#include <atomic>
#include <csignal>
#include <chrono>
#include <thread>
#include <mutex>
#include <sstream>
#include <unordered_map>
#include <queue>
#include <deque>
#include <vector>
#include <ctime>
#include <cmath>
#include <future>
#include <zlib.h>

namespace duckdb {

// Error classification for better analytics
enum class CrawlErrorType : uint8_t {
	NONE = 0,
	NETWORK_TIMEOUT = 1,
	NETWORK_DNS_FAILURE = 2,
	NETWORK_CONNECTION_REFUSED = 3,
	NETWORK_SSL_ERROR = 4,
	HTTP_CLIENT_ERROR = 5,
	HTTP_SERVER_ERROR = 6,
	HTTP_RATE_LIMITED = 7,
	ROBOTS_DISALLOWED = 8,
	CONTENT_TOO_LARGE = 9,
	CONTENT_TYPE_REJECTED = 10,
	MAX_RETRIES_EXCEEDED = 11
};

static const char* ErrorTypeToString(CrawlErrorType type) {
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

// Classify error from HTTP response
static CrawlErrorType ClassifyError(int status_code, const std::string &error_msg) {
	if (status_code == 429) return CrawlErrorType::HTTP_RATE_LIMITED;
	if (status_code >= 500 && status_code < 600) return CrawlErrorType::HTTP_SERVER_ERROR;
	if (status_code >= 400 && status_code < 500) return CrawlErrorType::HTTP_CLIENT_ERROR;
	if (status_code <= 0) {
		// Network error - try to classify from message
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

// Decompress gzip data (for .gz sitemap files)
// Returns empty string on error
static std::string DecompressGzip(const std::string &compressed_data) {
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

// Check if data starts with gzip magic bytes (0x1f 0x8b)
static bool IsGzippedData(const std::string &data) {
	return data.size() >= 2 &&
	       static_cast<unsigned char>(data[0]) == 0x1f &&
	       static_cast<unsigned char>(data[1]) == 0x8b;
}

// Global signal flag for graceful shutdown
static std::atomic<bool> g_shutdown_requested(false);
static std::atomic<int> g_sigint_count(0);
static std::chrono::steady_clock::time_point g_last_sigint_time;

// Global connection counter for rate limiting across all domains
static std::atomic<int> g_active_connections(0);

// Registry for background crawls - maps target_table to shutdown flag
struct BackgroundCrawlState {
	std::atomic<bool> shutdown_requested{false};
	std::atomic<bool> running{false};
	std::thread thread;
	std::string status;
	int64_t rows_crawled = 0;
	std::chrono::steady_clock::time_point start_time;
};

static std::mutex g_background_crawls_mutex;
static std::unordered_map<std::string, std::shared_ptr<BackgroundCrawlState>> g_background_crawls;

// Get or create background crawl state for a target table
static std::shared_ptr<BackgroundCrawlState> GetBackgroundCrawlState(const std::string &target_table) {
	std::lock_guard<std::mutex> lock(g_background_crawls_mutex);
	auto it = g_background_crawls.find(target_table);
	if (it != g_background_crawls.end()) {
		return it->second;
	}
	auto state = std::make_shared<BackgroundCrawlState>();
	g_background_crawls[target_table] = state;
	return state;
}

// Check if a background crawl should stop
static bool ShouldStopBackgroundCrawl(const std::string &target_table) {
	std::lock_guard<std::mutex> lock(g_background_crawls_mutex);
	auto it = g_background_crawls.find(target_table);
	if (it != g_background_crawls.end()) {
		return it->second->shutdown_requested.load();
	}
	return false;
}

// Signal handler
static void SignalHandler(int signum) {
	if (signum == SIGINT) {
		auto now = std::chrono::steady_clock::now();
		auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(now - g_last_sigint_time).count();

		g_sigint_count++;
		g_last_sigint_time = now;

		if (g_sigint_count >= 2 && elapsed < 3) {
			// Double Ctrl+C within 3 seconds - force exit
			std::exit(1);
		}

		g_shutdown_requested = true;
	}
}

// Domain state for rate limiting and 429 handling
struct DomainState {
	std::chrono::steady_clock::time_point last_crawl_time;
	double crawl_delay_seconds = 1.0;
	RobotsRules rules;
	bool robots_fetched = false;
	int urls_crawled = 0;
	int urls_failed = 0;
	int urls_skipped = 0;

	// 429 blocking
	std::chrono::steady_clock::time_point blocked_until;
	int consecutive_429s = 0;
	bool has_crawl_delay = false;  // True if robots.txt specified crawl-delay

	// Parallel tracking
	int active_requests = 0;

	// Adaptive rate limiting
	double average_response_ms = 0;  // Exponential moving average
	double min_crawl_delay_seconds = 0;  // Floor from robots.txt or default
	int response_count = 0;  // Number of responses for EMA warmup
};

// URL queue entry with retry tracking and scheduling
struct UrlQueueEntry {
	std::string url;
	int retry_count;
	bool is_update;
	std::chrono::steady_clock::time_point earliest_fetch;

	UrlQueueEntry(const std::string &u, int rc, bool upd)
	    : url(u), retry_count(rc), is_update(upd), earliest_fetch(std::chrono::steady_clock::now()) {}

	UrlQueueEntry(const std::string &u, int rc, bool upd, std::chrono::steady_clock::time_point ef)
	    : url(u), retry_count(rc), is_update(upd), earliest_fetch(ef) {}

	// For priority queue: earlier times have higher priority
	bool operator>(const UrlQueueEntry &other) const {
		return earliest_fetch > other.earliest_fetch;
	}
};

// Fibonacci backoff: 3, 3, 6, 9, 15, 24, 39, 63, 102, 165, 267...
static int FibonacciBackoffSeconds(int n, int max_seconds) {
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

// Adaptive rate limiting: adjust delay based on response times
// Uses exponential moving average (EMA) with alpha=0.2
static void UpdateAdaptiveDelay(DomainState &state, double response_ms, double max_delay) {
	// Update EMA (alpha=0.2 for smoothing)
	constexpr double alpha = 0.2;
	if (state.response_count == 0) {
		state.average_response_ms = response_ms;
	} else {
		state.average_response_ms = alpha * response_ms + (1.0 - alpha) * state.average_response_ms;
	}
	state.response_count++;

	// Need at least 3 samples before adapting
	if (state.response_count < 3) {
		return;
	}

	// If response is significantly slower than average, increase delay
	if (response_ms > 2.0 * state.average_response_ms) {
		state.crawl_delay_seconds = std::min(state.crawl_delay_seconds * 1.5, max_delay);
	}
	// If response is significantly faster than average, decrease delay (but respect floor)
	else if (response_ms < 0.5 * state.average_response_ms) {
		state.crawl_delay_seconds = std::max(state.crawl_delay_seconds * 0.9, state.min_crawl_delay_seconds);
	}
}

// Parse HTTP date format: "Tue, 14 Jan 2025 12:00:00 GMT"
// Returns empty string if parsing fails
static std::string ParseAndValidateServerDate(const std::string &server_date) {
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

	// Find components manually since strptime may not be available
	// Expected format: "Tue, 14 Jan 2025 12:00:00 GMT"
	int day, year, hour, min, sec;
	char month_str[4] = {0};

	int parsed = sscanf(server_date.c_str(), "%*[^,], %d %3s %d %d:%d:%d",
	                    &day, month_str, &year, &hour, &min, &sec);

	if (parsed != 6) {
		return "";  // Failed to parse
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
		return "";  // Invalid month
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
		// Server clock is off by more than 15 minutes - don't trust it
		return "";
	}

	// Server time is trustworthy - format as ISO timestamp for DuckDB
	char buf[32];
	struct tm *gmt = gmtime(&server_time);
	strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", gmt);
	return std::string(buf);
}

// Crawl result for a single URL
struct CrawlResult {
	std::string url;
	int http_status;
	std::string body;
	std::string content_type;
	std::string error;
	int64_t elapsed_ms;
	std::chrono::system_clock::time_point crawled_at;
};

// Extract domain from URL
static std::string ExtractDomain(const std::string &url) {
	// Simple domain extraction: find :// then extract until next /
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

// Extract path from URL
static std::string ExtractPath(const std::string &url) {
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

// Generate SURT key (Sort-friendly URI Reordering Transform) like Common Crawl
// Example: https://www.example.com/path?q=1 → com,example)/path?q=1
// Note: 'www' prefix is stripped per SURT convention
static std::string GenerateSurtKey(const std::string &url) {
	// Extract domain
	size_t proto_end = url.find("://");
	if (proto_end == std::string::npos) {
		return url;  // Invalid URL, return as-is
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

	// Convert domain to lowercase
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

	// Add path (everything after domain)
	surt += ")";
	if (domain_end < url.length()) {
		surt += url.substr(domain_end);
	} else {
		surt += "/";
	}

	return surt;
}

// Generate domain SURT prefix for fast lookups (without trailing path)
// Example: "www.example.com" → "com,example)"
// Example: "example.com" → "com,example)"
static std::string GenerateDomainSurt(const std::string &hostname) {
	if (hostname.empty()) {
		return "";
	}

	// Convert to lowercase
	std::string domain = hostname;
	std::transform(domain.begin(), domain.end(), domain.begin(), ::tolower);

	// Remove port if present
	size_t port_pos = domain.find(':');
	if (port_pos != std::string::npos) {
		domain = domain.substr(0, port_pos);
	}

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
	surt += ")";

	return surt;
}

// Generate content hash for deduplication (uses std::hash, returns hex string)
static std::string GenerateContentHash(const std::string &content) {
	if (content.empty()) {
		return "";
	}
	std::hash<std::string> hasher;
	size_t hash_value = hasher(content);
	// Convert to hex string (16 chars for 64-bit hash)
	char buf[17];
	snprintf(buf, sizeof(buf), "%016zx", hash_value);
	return std::string(buf);
}

// Check if content-type matches a pattern (supports wildcards like "text/*")
static bool ContentTypeMatches(const std::string &content_type, const std::string &pattern) {
	if (pattern.empty()) {
		return false;
	}
	// Extract main type from content_type (e.g., "text/html; charset=utf-8" -> "text/html")
	std::string ct = content_type;
	size_t semicolon = ct.find(';');
	if (semicolon != std::string::npos) {
		ct = ct.substr(0, semicolon);
	}
	// Trim whitespace
	while (!ct.empty() && std::isspace(ct.back())) ct.pop_back();
	while (!ct.empty() && std::isspace(ct.front())) ct.erase(ct.begin());

	// Convert both to lowercase for comparison
	std::string ct_lower = ct;
	std::string pat_lower = pattern;
	std::transform(ct_lower.begin(), ct_lower.end(), ct_lower.begin(), ::tolower);
	std::transform(pat_lower.begin(), pat_lower.end(), pat_lower.begin(), ::tolower);

	// Check for wildcard (e.g., "text/*")
	if (pat_lower.length() >= 2 && pat_lower.substr(pat_lower.length() - 2) == "/*") {
		std::string prefix = pat_lower.substr(0, pat_lower.length() - 1);  // "text/"
		return ct_lower.find(prefix) == 0;
	}

	// Exact match
	return ct_lower == pat_lower;
}

// Check if content-type is acceptable (matches accept list, doesn't match reject list)
static bool IsContentTypeAcceptable(const std::string &content_type,
                                     const std::string &accept_types,
                                     const std::string &reject_types) {
	// If no filters, accept all
	if (accept_types.empty() && reject_types.empty()) {
		return true;
	}

	// Check accept list first (if specified)
	if (!accept_types.empty()) {
		bool accepted = false;
		std::istringstream accept_stream(accept_types);
		std::string pattern;
		while (std::getline(accept_stream, pattern, ',')) {
			// Trim whitespace
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

	// Check reject list (if specified)
	if (!reject_types.empty()) {
		std::istringstream reject_stream(reject_types);
		std::string pattern;
		while (std::getline(reject_stream, pattern, ',')) {
			// Trim whitespace
			while (!pattern.empty() && std::isspace(pattern.back())) pattern.pop_back();
			while (!pattern.empty() && std::isspace(pattern.front())) pattern.erase(pattern.begin());
			if (ContentTypeMatches(content_type, pattern)) {
				return false;  // Rejected
			}
		}
	}

	return true;
}

// Bind data for the crawler function
struct CrawlerBindData : public TableFunctionData {
	std::vector<std::string> urls;
	std::string user_agent;
	double default_crawl_delay;
	double min_crawl_delay;
	double max_crawl_delay;
	int timeout_seconds;
	bool respect_robots_txt;
	bool log_skipped;
};

// Global state for the crawler function
struct CrawlerGlobalState : public GlobalTableFunctionState {
	std::mutex mutex;
	size_t current_url_index = 0;
	std::unordered_map<std::string, DomainState> domain_states;

	// Statistics
	int total_crawled = 0;
	int total_failed = 0;
	int total_skipped = 0;
	int total_cancelled = 0;
	std::chrono::steady_clock::time_point start_time;

	idx_t MaxThreads() const override {
		return 1; // Single-threaded for now to respect rate limits properly
	}
};

// Local state for the crawler function
struct CrawlerLocalState : public LocalTableFunctionState {
	bool finished = false;
};

static unique_ptr<FunctionData> CrawlerBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<string> &names) {
	auto bind_data = make_uniq<CrawlerBindData>();

	// Get user_agent parameter (required)
	bool has_user_agent = false;
	for (auto &kv : input.named_parameters) {
		if (kv.first == "user_agent") {
			bind_data->user_agent = StringValue::Get(kv.second);
			has_user_agent = true;
		} else if (kv.first == "default_crawl_delay") {
			bind_data->default_crawl_delay = kv.second.GetValue<double>();
		} else if (kv.first == "min_crawl_delay") {
			bind_data->min_crawl_delay = kv.second.GetValue<double>();
		} else if (kv.first == "max_crawl_delay") {
			bind_data->max_crawl_delay = kv.second.GetValue<double>();
		} else if (kv.first == "timeout_seconds") {
			bind_data->timeout_seconds = kv.second.GetValue<int>();
		} else if (kv.first == "respect_robots_txt") {
			bind_data->respect_robots_txt = kv.second.GetValue<bool>();
		} else if (kv.first == "log_skipped") {
			bind_data->log_skipped = kv.second.GetValue<bool>();
		}
	}

	if (!has_user_agent) {
		throw BinderException("crawl_urls requires 'user_agent' parameter");
	}

	// Set defaults
	if (bind_data->default_crawl_delay == 0) {
		bind_data->default_crawl_delay = 1.0;
	}
	if (bind_data->max_crawl_delay == 0) {
		bind_data->max_crawl_delay = 60.0;
	}
	if (bind_data->timeout_seconds == 0) {
		bind_data->timeout_seconds = 30;
	}
	bind_data->respect_robots_txt = true; // Default
	bind_data->log_skipped = true; // Default

	// Get URLs from the first argument (should be a list of strings)
	if (input.inputs.size() < 1) {
		throw BinderException("crawl_urls requires a list of URLs as first argument");
	}

	auto &urls_value = input.inputs[0];
	if (urls_value.type().id() == LogicalTypeId::LIST) {
		auto &list_children = ListValue::GetChildren(urls_value);
		for (auto &child : list_children) {
			bind_data->urls.push_back(StringValue::Get(child));
		}
	} else {
		throw BinderException("crawl_urls first argument must be a list of URLs");
	}

	// Define output schema
	names.emplace_back("url");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("surt_key");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("http_status");
	return_types.emplace_back(LogicalType::INTEGER);

	names.emplace_back("body");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("content_type");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("elapsed_ms");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("crawled_at");
	return_types.emplace_back(LogicalType::TIMESTAMP);

	names.emplace_back("error");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("etag");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("last_modified");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("content_hash");
	return_types.emplace_back(LogicalType::VARCHAR);

	return std::move(bind_data);
}

static unique_ptr<GlobalTableFunctionState> CrawlerInitGlobal(ClientContext &context,
                                                               TableFunctionInitInput &input) {
	auto state = make_uniq<CrawlerGlobalState>();
	state->start_time = std::chrono::steady_clock::now();

	// Reset signal state
	g_shutdown_requested = false;
	g_sigint_count = 0;

	// Install signal handler
	std::signal(SIGINT, SignalHandler);

	return std::move(state);
}

static unique_ptr<LocalTableFunctionState> CrawlerInitLocal(ExecutionContext &context,
                                                             TableFunctionInitInput &input,
                                                             GlobalTableFunctionState *global_state) {
	return make_uniq<CrawlerLocalState>();
}

static void CrawlerFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->CastNoConst<CrawlerBindData>();
	auto &global_state = data.global_state->Cast<CrawlerGlobalState>();
	auto &local_state = data.local_state->Cast<CrawlerLocalState>();

	if (local_state.finished || g_shutdown_requested) {
		output.SetCardinality(0);
		return;
	}

	std::lock_guard<std::mutex> lock(global_state.mutex);

	// Check if we have more URLs
	if (global_state.current_url_index >= bind_data.urls.size()) {
		local_state.finished = true;
		output.SetCardinality(0);
		return;
	}

	// Process one URL at a time
	auto &url = bind_data.urls[global_state.current_url_index];
	global_state.current_url_index++;

	std::string domain = ExtractDomain(url);
	std::string path = ExtractPath(url);

	// Get or create domain state
	auto &domain_state = global_state.domain_states[domain];

	// Fetch robots.txt if not already done
	if (bind_data.respect_robots_txt && !domain_state.robots_fetched) {
		std::string robots_url = "https://" + domain + "/robots.txt";
		RetryConfig retry_config;
		retry_config.max_retries = 2;

		auto response = HttpClient::Fetch(context, robots_url, retry_config, bind_data.user_agent);

		if (response.success) {
			auto robots_data = RobotsParser::Parse(response.body);
			domain_state.rules = RobotsParser::GetRulesForUserAgent(robots_data, bind_data.user_agent);

			// Set crawl delay
			if (domain_state.rules.HasCrawlDelay()) {
				domain_state.crawl_delay_seconds = domain_state.rules.GetEffectiveDelay();
			} else {
				domain_state.crawl_delay_seconds = bind_data.default_crawl_delay;
			}

			// Clamp to min/max
			domain_state.crawl_delay_seconds = std::max(domain_state.crawl_delay_seconds, bind_data.min_crawl_delay);
			domain_state.crawl_delay_seconds = std::min(domain_state.crawl_delay_seconds, bind_data.max_crawl_delay);
			// Set floor for adaptive rate limiting
			domain_state.min_crawl_delay_seconds = domain_state.crawl_delay_seconds;
		} else {
			// robots.txt not found or error - use default delay, allow all
			domain_state.crawl_delay_seconds = bind_data.default_crawl_delay;
			domain_state.min_crawl_delay_seconds = bind_data.default_crawl_delay;
		}

		domain_state.robots_fetched = true;
	}

	// Check if URL is allowed by robots.txt
	if (bind_data.respect_robots_txt && !RobotsParser::IsAllowed(domain_state.rules, path)) {
		global_state.total_skipped++;
		domain_state.urls_skipped++;

		if (bind_data.log_skipped) {
			// Return skipped result
			std::string surt_key = GenerateSurtKey(url);
			output.SetCardinality(1);
			output.SetValue(0, 0, Value(url));
			output.SetValue(1, 0, Value(surt_key));
			output.SetValue(2, 0, Value(-1)); // Special status for robots.txt disallow
			output.SetValue(3, 0, Value());
			output.SetValue(4, 0, Value());
			output.SetValue(5, 0, Value(0));
			output.SetValue(6, 0, Value::TIMESTAMP(Timestamp::GetCurrentTimestamp()));
			output.SetValue(7, 0, Value("robots.txt disallow"));
			output.SetValue(8, 0, Value());   // etag
			output.SetValue(9, 0, Value());  // last_modified
			output.SetValue(10, 0, Value());  // content_hash
			return;
		} else {
			// Skip silently, try next URL
			output.SetCardinality(0);
			return;
		}
	}

	// Wait for crawl delay
	auto now = std::chrono::steady_clock::now();
	auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - domain_state.last_crawl_time).count();
	auto required_delay_ms = static_cast<int64_t>(domain_state.crawl_delay_seconds * 1000);

	if (elapsed < required_delay_ms) {
		auto wait_time = required_delay_ms - elapsed;
		std::this_thread::sleep_for(std::chrono::milliseconds(wait_time));
	}

	// Check for shutdown after waiting
	if (g_shutdown_requested) {
		global_state.total_cancelled++;
		output.SetCardinality(0);
		return;
	}

	// Fetch URL
	auto fetch_start = std::chrono::steady_clock::now();

	RetryConfig retry_config;
	retry_config.max_retries = 3;

	auto response = HttpClient::Fetch(context, url, retry_config, bind_data.user_agent);

	auto fetch_end = std::chrono::steady_clock::now();
	auto fetch_elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(fetch_end - fetch_start).count();

	// Update domain state
	domain_state.last_crawl_time = fetch_end;

	if (response.success) {
		global_state.total_crawled++;
		domain_state.urls_crawled++;
	} else {
		global_state.total_failed++;
		domain_state.urls_failed++;
	}

	// Return result
	std::string surt_key = GenerateSurtKey(url);
	output.SetCardinality(1);
	output.SetValue(0, 0, Value(url));
	output.SetValue(1, 0, Value(surt_key));
	output.SetValue(2, 0, Value(response.status_code));
	output.SetValue(3, 0, response.body.empty() ? Value() : Value(response.body));
	output.SetValue(4, 0, response.content_type.empty() ? Value() : Value(response.content_type));
	output.SetValue(5, 0, Value(fetch_elapsed_ms));
	output.SetValue(6, 0, Value::TIMESTAMP(Timestamp::GetCurrentTimestamp()));
	output.SetValue(7, 0, response.error.empty() ? Value() : Value(response.error));
	output.SetValue(8, 0, response.etag.empty() ? Value() : Value(response.etag));
	output.SetValue(9, 0, response.last_modified.empty() ? Value() : Value(response.last_modified));
	std::string content_hash = GenerateContentHash(response.body);
	output.SetValue(10, 0, content_hash.empty() ? Value() : Value(content_hash));
}

void RegisterCrawlerFunction(ExtensionLoader &loader) {
	TableFunction crawl_func("crawl_urls", {LogicalType::LIST(LogicalType::VARCHAR)}, CrawlerFunction, CrawlerBind,
	                         CrawlerInitGlobal);
	crawl_func.init_local = CrawlerInitLocal;

	// Named parameters
	crawl_func.named_parameters["user_agent"] = LogicalType::VARCHAR;
	crawl_func.named_parameters["default_crawl_delay"] = LogicalType::DOUBLE;
	crawl_func.named_parameters["min_crawl_delay"] = LogicalType::DOUBLE;
	crawl_func.named_parameters["max_crawl_delay"] = LogicalType::DOUBLE;
	crawl_func.named_parameters["timeout_seconds"] = LogicalType::INTEGER;
	crawl_func.named_parameters["respect_robots_txt"] = LogicalType::BOOLEAN;
	crawl_func.named_parameters["log_skipped"] = LogicalType::BOOLEAN;

	loader.RegisterFunction(crawl_func);
}

//===--------------------------------------------------------------------===//
// crawl_into_internal - Used by CRAWL CONCURRENTLY parser extension
//===--------------------------------------------------------------------===//

struct CrawlIntoBindData : public TableFunctionData {
	int statement_type = 0;       // 0=CRAWL
	std::string source_query;
	std::string target_table;
	std::string user_agent;
	double default_crawl_delay = 1.0;
	double min_crawl_delay = 0.0;
	double max_crawl_delay = 60.0;
	int timeout_seconds = 30;
	bool respect_robots_txt = true;
	bool log_skipped = true;
	std::string url_filter;       // LIKE pattern for filtering URLs
	double sitemap_cache_hours = 24.0;  // Hours to cache sitemap discovery results
	bool update_stale = false;    // Re-crawl stale URLs based on sitemap lastmod/changefreq
	int max_retry_backoff_seconds = 600;   // Max Fibonacci backoff for 429/5XX/timeout
	int max_parallel_per_domain = 8;       // Max concurrent requests per domain (if no crawl-delay)
	int max_total_connections = 32;        // Global max concurrent connections
	int64_t max_response_bytes = 10 * 1024 * 1024;  // Max response size (10MB)
	bool compress = true;                  // Request gzip compression
	std::string accept_content_types;      // Only accept these content types
	std::string reject_content_types;      // Reject these content types
	// Link-following fallback
	bool follow_links = false;
	bool allow_subdomains = false;
	int max_crawl_pages = 10000;
	int max_crawl_depth = 10;
	bool respect_nofollow = true;
	bool follow_canonical = true;
};

struct CrawlIntoGlobalState : public GlobalTableFunctionState {
	std::mutex mutex;
	bool executed = false;
	int64_t rows_inserted = 0;

	// Progress tracking
	std::atomic<bool> discovery_started{false};
	std::atomic<int64_t> total_urls{0};
	std::atomic<int64_t> processed_urls{0};

	idx_t MaxThreads() const override {
		return 1;
	}
};

static unique_ptr<FunctionData> CrawlIntoBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<string> &names) {
	auto bind_data = make_uniq<CrawlIntoBindData>();

	// Get parameters from positional arguments (set by PlanCrawl)
	// Order: statement_type, source_query, target_table, user_agent, delays..., url_filter, sitemap_cache_hours,
	//        update_stale, max_retry_backoff_seconds, max_parallel_per_domain, max_total_connections, max_response_bytes,
	//        compress, accept_content_types, reject_content_types, follow_links, allow_subdomains, max_crawl_pages,
	//        max_crawl_depth, respect_nofollow, follow_canonical
	if (input.inputs.size() >= 26) {
		bind_data->statement_type = input.inputs[0].GetValue<int32_t>();
		bind_data->source_query = StringValue::Get(input.inputs[1]);
		bind_data->target_table = StringValue::Get(input.inputs[2]);
		bind_data->user_agent = StringValue::Get(input.inputs[3]);
		bind_data->default_crawl_delay = input.inputs[4].GetValue<double>();
		bind_data->min_crawl_delay = input.inputs[5].GetValue<double>();
		bind_data->max_crawl_delay = input.inputs[6].GetValue<double>();
		bind_data->timeout_seconds = input.inputs[7].GetValue<int>();
		bind_data->respect_robots_txt = input.inputs[8].GetValue<bool>();
		bind_data->log_skipped = input.inputs[9].GetValue<bool>();
		bind_data->url_filter = StringValue::Get(input.inputs[10]);
		bind_data->sitemap_cache_hours = input.inputs[11].GetValue<double>();
		bind_data->update_stale = input.inputs[12].GetValue<bool>();
		bind_data->max_retry_backoff_seconds = input.inputs[13].GetValue<int>();
		bind_data->max_parallel_per_domain = input.inputs[14].GetValue<int>();
		bind_data->max_total_connections = input.inputs[15].GetValue<int>();
		bind_data->max_response_bytes = input.inputs[16].GetValue<int64_t>();
		bind_data->compress = input.inputs[17].GetValue<bool>();
		bind_data->accept_content_types = StringValue::Get(input.inputs[18]);
		bind_data->reject_content_types = StringValue::Get(input.inputs[19]);
		bind_data->follow_links = input.inputs[20].GetValue<bool>();
		bind_data->allow_subdomains = input.inputs[21].GetValue<bool>();
		bind_data->max_crawl_pages = input.inputs[22].GetValue<int>();
		bind_data->max_crawl_depth = input.inputs[23].GetValue<int>();
		bind_data->respect_nofollow = input.inputs[24].GetValue<bool>();
		bind_data->follow_canonical = input.inputs[25].GetValue<bool>();
	} else {
		throw BinderException("crawl_into_internal: insufficient parameters (expected 26, got " +
		                      std::to_string(input.inputs.size()) + ")");
	}

	// Return schema: just count of rows changed
	names.emplace_back("Count");
	return_types.emplace_back(LogicalType::BIGINT);

	return std::move(bind_data);
}

static unique_ptr<GlobalTableFunctionState> CrawlIntoInitGlobal(ClientContext &context,
                                                                  TableFunctionInitInput &input) {
	auto state = make_uniq<CrawlIntoGlobalState>();

	// Reset signal state
	g_shutdown_requested = false;
	g_sigint_count = 0;

	// Install signal handler
	std::signal(SIGINT, SignalHandler);

	return std::move(state);
}

// Helper: Check if URL matches LIKE pattern (simple SQL LIKE implementation)
static bool MatchesLikePattern(const std::string &url, const std::string &pattern) {
	if (pattern.empty()) {
		return true;
	}

	// Convert SQL LIKE pattern to simple matching
	// % = any characters, _ = single character
	size_t url_pos = 0;
	size_t pat_pos = 0;
	size_t url_len = url.length();
	size_t pat_len = pattern.length();

	size_t star_pos = std::string::npos;
	size_t match_pos = 0;

	while (url_pos < url_len) {
		if (pat_pos < pat_len && (pattern[pat_pos] == url[url_pos] || pattern[pat_pos] == '_')) {
			url_pos++;
			pat_pos++;
		} else if (pat_pos < pat_len && pattern[pat_pos] == '%') {
			star_pos = pat_pos;
			match_pos = url_pos;
			pat_pos++;
		} else if (star_pos != std::string::npos) {
			pat_pos = star_pos + 1;
			match_pos++;
			url_pos = match_pos;
		} else {
			return false;
		}
	}

	while (pat_pos < pat_len && pattern[pat_pos] == '%') {
		pat_pos++;
	}

	return pat_pos == pat_len;
}

// Helper: Ensure sitemap cache table exists with SURT-based indexing
static void EnsureSitemapCacheTable(Connection &conn) {
	// Check if table exists and has old schema (hostname or domain_surt column)
	auto check_result = conn.Query("SELECT 1 FROM _crawl_sitemap_cache LIMIT 0");
	if (!check_result->HasError()) {
		// Table exists - check if it has the old schema
		auto col_check = conn.Query("SELECT hostname FROM _crawl_sitemap_cache LIMIT 0");
		auto col_check2 = conn.Query("SELECT domain_surt FROM _crawl_sitemap_cache LIMIT 0");
		if (!col_check->HasError() || !col_check2->HasError()) {
			// Old schema - drop and recreate
			conn.Query("DROP TABLE _crawl_sitemap_cache");
		}
	}

	// Create table with SURT-based schema (query by surt_key LIKE 'fi,normal)%')
	conn.Query("CREATE TABLE IF NOT EXISTS _crawl_sitemap_cache ("
	           "surt_key VARCHAR PRIMARY KEY, "  // SURT of URL, e.g. "fi,normal)/path"
	           "url VARCHAR NOT NULL, "
	           "lastmod TIMESTAMP, "
	           "changefreq VARCHAR, "
	           "priority DOUBLE, "
	           "discovered_at TIMESTAMP DEFAULT NOW())");
}

// Helper: Convert changefreq to hours threshold
static double ChangefreqToHours(const std::string &changefreq) {
	if (changefreq.empty()) {
		return 168.0;  // Default: weekly
	}
	std::string freq = StringUtil::Lower(changefreq);
	if (freq == "always") return 0.0;
	if (freq == "hourly") return 1.0;
	if (freq == "daily") return 24.0;
	if (freq == "weekly") return 168.0;
	if (freq == "monthly") return 720.0;
	if (freq == "yearly") return 8760.0;
	if (freq == "never") return 87600.0;  // ~10 years
	return 168.0;  // Default: weekly
}

// Helper: Check if URL is stale based on sitemap lastmod or changefreq
// Returns true if the URL should be re-crawled
static bool IsUrlStale(Connection &conn, const std::string &url, const std::string &target_table) {
	// Get crawled_at from target table
	auto crawled_result = conn.Query(
	    "SELECT crawled_at FROM " + target_table + " WHERE url = $1", url);

	if (crawled_result->HasError()) {
		return false;  // Can't determine, assume not stale
	}

	auto crawled_chunk = crawled_result->Fetch();
	if (!crawled_chunk || crawled_chunk->size() == 0) {
		return false;  // URL not found in target, not a stale update
	}

	auto crawled_val = crawled_chunk->GetValue(0, 0);
	if (crawled_val.IsNull()) {
		return true;  // No crawled_at, consider stale
	}

	// Get sitemap metadata for this URL using SURT key
	std::string surt_key = GenerateSurtKey(url);
	auto cache_result = conn.Query(
	    "SELECT lastmod, changefreq FROM _crawl_sitemap_cache WHERE surt_key = $1", surt_key);

	if (cache_result->HasError()) {
		return false;
	}

	auto cache_chunk = cache_result->Fetch();
	if (!cache_chunk || cache_chunk->size() == 0) {
		return false;  // No sitemap metadata
	}

	auto lastmod_val = cache_chunk->GetValue(0, 0);
	auto changefreq_val = cache_chunk->GetValue(1, 0);

	// If lastmod exists and is after crawled_at, URL is stale
	if (!lastmod_val.IsNull()) {
		// Compare timestamps
		auto compare_result = conn.Query(
		    "SELECT $1::TIMESTAMP > $2::TIMESTAMP",
		    lastmod_val, crawled_val);
		if (!compare_result->HasError()) {
			auto cmp_chunk = compare_result->Fetch();
			if (cmp_chunk && cmp_chunk->size() > 0) {
				auto cmp_val = cmp_chunk->GetValue(0, 0);
				if (!cmp_val.IsNull() && cmp_val.GetValue<bool>()) {
					return true;  // lastmod > crawled_at
				}
			}
		}
	}

	// Fall back to changefreq-based staleness
	std::string changefreq = changefreq_val.IsNull() ? "" : StringValue::Get(changefreq_val);
	double hours_threshold = ChangefreqToHours(changefreq);

	// Check if enough time has passed since last crawl
	auto age_result = conn.Query(
	    "SELECT EXTRACT(EPOCH FROM (NOW() - $1::TIMESTAMP)) / 3600.0 > $2",
	    crawled_val, hours_threshold);

	if (!age_result->HasError()) {
		auto age_chunk = age_result->Fetch();
		if (age_chunk && age_chunk->size() > 0) {
			auto age_val = age_chunk->GetValue(0, 0);
			if (!age_val.IsNull() && age_val.GetValue<bool>()) {
				return true;  // Age exceeds threshold
			}
		}
	}

	return false;
}

// Helper: Get cached sitemap URLs if still valid (using SURT prefix matching)
static std::vector<std::string> GetCachedSitemapUrls(Connection &conn, const std::string &hostname,
                                                      double cache_hours) {
	std::vector<std::string> urls;

	// Generate domain SURT prefix for matching (e.g., "fi,normal)" for normal.fi)
	std::string surt_prefix = GenerateDomainSurt(hostname);

	// Query using LIKE prefix match on surt_key
	auto result = conn.Query(
	    "SELECT url FROM _crawl_sitemap_cache "
	    "WHERE surt_key LIKE $1 AND discovered_at > NOW() - INTERVAL '" + std::to_string(cache_hours) + " hours'",
	    surt_prefix + "%");

	if (!result->HasError()) {
		while (auto chunk = result->Fetch()) {
			for (idx_t i = 0; i < chunk->size(); i++) {
				auto val = chunk->GetValue(0, i);
				if (!val.IsNull()) {
					urls.push_back(StringValue::Get(val));
				}
			}
		}
	}

	return urls;
}

// Helper: Cache discovered sitemap URLs with SURT keys for fast lookups
static void CacheSitemapUrls(Connection &conn, const std::string &hostname,
                              const std::vector<SitemapEntry> &entries) {
	// Generate domain SURT prefix for this hostname
	std::string surt_prefix = GenerateDomainSurt(hostname);

	// Clear old cache for this domain using LIKE prefix
	conn.Query("DELETE FROM _crawl_sitemap_cache WHERE surt_key LIKE $1", surt_prefix + "%");

	// Insert entries with SURT keys
	for (const auto &entry : entries) {
		std::string surt_key = GenerateSurtKey(entry.loc);

		// Parse lastmod - DuckDB handles ISO8601 and date-only formats
		Value lastmod_val = entry.lastmod.empty() ? Value() : Value(entry.lastmod);
		Value changefreq_val = entry.changefreq.empty() ? Value() : Value(entry.changefreq);
		Value priority_val = entry.priority.empty() ? Value() : Value(std::stod(entry.priority));

		conn.Query("INSERT OR REPLACE INTO _crawl_sitemap_cache (surt_key, url, lastmod, changefreq, priority) "
		           "VALUES ($1, $2, $3, $4, $5)",
		           surt_key, entry.loc, lastmod_val, changefreq_val, priority_val);
	}
}

//===--------------------------------------------------------------------===//
// Discovery Status Cache (tracks sitemap vs link-crawl vs not_found)
//===--------------------------------------------------------------------===//

struct DiscoveryStatus {
	bool is_valid = false;
	std::string method;  // "sitemap", "link_crawl", "not_found"
	int urls_found = 0;
};

static void EnsureDiscoveryStatusTable(Connection &conn) {
	// Check if table exists and has old schema (hostname column)
	auto check_result = conn.Query("SELECT 1 FROM _crawl_sitemap_discovery_status LIMIT 0");
	if (!check_result->HasError()) {
		auto col_check = conn.Query("SELECT hostname FROM _crawl_sitemap_discovery_status LIMIT 0");
		if (!col_check->HasError()) {
			// Old schema - drop and recreate
			conn.Query("DROP TABLE _crawl_sitemap_discovery_status");
		}
	}

	conn.Query("CREATE TABLE IF NOT EXISTS _crawl_sitemap_discovery_status ("
	           "domain_surt VARCHAR PRIMARY KEY, "  // SURT key for fast lookups
	           "discovery_method VARCHAR, "
	           "discovered_at TIMESTAMP DEFAULT NOW(), "
	           "urls_found INTEGER DEFAULT 0)");
}

static DiscoveryStatus GetDiscoveryStatus(Connection &conn, const std::string &hostname, double cache_hours) {
	DiscoveryStatus status;
	std::string domain_surt = GenerateDomainSurt(hostname);

	auto result = conn.Query(
	    "SELECT discovery_method, urls_found FROM _crawl_sitemap_discovery_status "
	    "WHERE domain_surt = $1 AND discovered_at > NOW() - INTERVAL '" + std::to_string(cache_hours) + " hours'",
	    domain_surt);

	if (!result->HasError()) {
		auto chunk = result->Fetch();
		if (chunk && chunk->size() > 0) {
			status.is_valid = true;
			auto method_val = chunk->GetValue(0, 0);
			auto count_val = chunk->GetValue(1, 0);
			status.method = method_val.IsNull() ? "" : StringValue::Get(method_val);
			status.urls_found = count_val.IsNull() ? 0 : count_val.GetValue<int>();
		}
	}
	return status;
}

static void UpdateDiscoveryStatus(Connection &conn, const std::string &hostname,
                                   const std::string &method, int urls_found) {
	std::string domain_surt = GenerateDomainSurt(hostname);
	conn.Query("INSERT OR REPLACE INTO _crawl_sitemap_discovery_status "
	           "(domain_surt, discovery_method, discovered_at, urls_found) "
	           "VALUES ($1, $2, NOW(), $3)",
	           domain_surt, method, urls_found);
}

// Result struct for batch inserts (different from CrawlResult used by crawl() function)
struct BatchCrawlEntry {
	std::string url;
	std::string surt_key;
	int status_code;
	std::string body;
	std::string content_type;
	int64_t elapsed_ms;
	std::string timestamp_expr;  // SQL expression for timestamp
	std::string error;
	std::string etag;
	std::string last_modified;
	std::string content_hash;
	bool is_update;
};

// Flush batch of results to database
static int64_t FlushBatch(Connection &conn, const std::string &target_table,
                           std::vector<BatchCrawlEntry> &batch) {
	if (batch.empty()) {
		return 0;
	}

	int64_t rows_changed = 0;

	for (const auto &result : batch) {
		if (result.is_update) {
			auto update_sql = "UPDATE " + target_table +
			                  " SET surt_key = $2, http_status = $3, body = $4, content_type = $5, "
			                  "elapsed_ms = $6, crawled_at = " + result.timestamp_expr + ", error = $7, "
			                  "etag = $8, last_modified = $9, content_hash = $10 "
			                  "WHERE url = $1";

			auto update_result = conn.Query(update_sql, result.url, result.surt_key, result.status_code,
			                                 result.body.empty() ? Value() : Value(result.body),
			                                 result.content_type.empty() ? Value() : Value(result.content_type),
			                                 result.elapsed_ms,
			                                 result.error.empty() ? Value() : Value(result.error),
			                                 result.etag.empty() ? Value() : Value(result.etag),
			                                 result.last_modified.empty() ? Value() : Value(result.last_modified),
			                                 result.content_hash.empty() ? Value() : Value(result.content_hash));

			if (!update_result->HasError()) {
				rows_changed++;
			}
		} else {
			auto insert_sql = "INSERT INTO " + target_table +
			                  " (url, surt_key, http_status, body, content_type, elapsed_ms, crawled_at, error, etag, last_modified, content_hash) "
			                  "VALUES ($1, $2, $3, $4, $5, $6, " + result.timestamp_expr + ", $7, $8, $9, $10)";

			auto insert_result = conn.Query(insert_sql, result.url, result.surt_key, result.status_code,
			                                 result.body.empty() ? Value() : Value(result.body),
			                                 result.content_type.empty() ? Value() : Value(result.content_type),
			                                 result.elapsed_ms,
			                                 result.error.empty() ? Value() : Value(result.error),
			                                 result.etag.empty() ? Value() : Value(result.etag),
			                                 result.last_modified.empty() ? Value() : Value(result.last_modified),
			                                 result.content_hash.empty() ? Value() : Value(result.content_hash));

			if (!insert_result->HasError()) {
				rows_changed++;
			}
		}
	}

	batch.clear();
	return rows_changed;
}

//===--------------------------------------------------------------------===//
// Ensure target table exists for crawl results
static void EnsureTargetTable(Connection &conn, const std::string &target_table) {
	conn.Query("CREATE TABLE IF NOT EXISTS \"" + target_table + "\" ("
	           "url VARCHAR PRIMARY KEY, "
	           "surt_key VARCHAR, "
	           "http_status INTEGER, "
	           "body VARCHAR, "
	           "content_type VARCHAR, "
	           "elapsed_ms BIGINT, "
	           "crawled_at TIMESTAMP, "
	           "error VARCHAR, "
	           "etag VARCHAR, "
	           "last_modified VARCHAR, "
	           "content_hash VARCHAR, "
	           "error_type VARCHAR)");
}

// Input type for CRAWL SITES
enum class SiteInputType {
	HOSTNAME,        // "example.com" - discover sitemap, fallback to spider
	HOSTNAME_PATH,   // "example.com/blog/" - discover sitemap, spider from path
	DIRECT_SITEMAP,  // "https://example.com/sitemap.xml" - fetch sitemap directly
	PAGE_URL         // "https://example.com/page?q=1" - discover sitemap, spider from URL
};

struct ParsedSiteInput {
	std::string hostname;
	std::string start_url;      // For spider fallback
	std::string sitemap_url;    // If direct sitemap provided
	SiteInputType type;
};

// Check if URL looks like a sitemap
static bool IsSitemapUrl(const std::string &url) {
	std::string lower = url;
	std::transform(lower.begin(), lower.end(), lower.begin(), ::tolower);

	// Check common sitemap patterns
	if (lower.find("/sitemap") != std::string::npos) return true;
	if (lower.length() > 4 && lower.substr(lower.length() - 4) == ".xml") return true;
	if (lower.find("sitemap_index") != std::string::npos) return true;

	return false;
}

// Helper: Check if a string looks like a valid URL or hostname
// Valid formats: "example.com", "example.com/path", "https://example.com", "http://example.com/path"
static bool IsValidUrlOrHostname(const std::string &input) {
	if (input.empty()) return false;

	// Must contain at least one dot (domain)
	if (input.find('.') == std::string::npos) return false;

	// Check for full URL with protocol
	if (input.find("://") != std::string::npos) {
		// Must start with http:// or https://
		if (input.substr(0, 7) != "http://" && input.substr(0, 8) != "https://") {
			return false;
		}
		// Must have something after protocol
		size_t start = input.find("://") + 3;
		if (start >= input.length()) return false;
		// Check domain part has a dot
		std::string rest = input.substr(start);
		size_t slash = rest.find('/');
		std::string domain = (slash != std::string::npos) ? rest.substr(0, slash) : rest;
		if (domain.find('.') == std::string::npos) return false;
		return true;
	}

	// Hostname or hostname/path format
	// Extract hostname part (before first slash)
	size_t slash_pos = input.find('/');
	std::string hostname = (slash_pos != std::string::npos) ? input.substr(0, slash_pos) : input;

	// Hostname must have a dot
	if (hostname.find('.') == std::string::npos) return false;

	// Basic hostname validation: alphanumeric, dots, and hyphens
	for (char c : hostname) {
		if (!std::isalnum(c) && c != '.' && c != '-') return false;
	}

	// Can't start or end with dot or hyphen
	if (hostname.front() == '.' || hostname.front() == '-') return false;
	if (hostname.back() == '.' || hostname.back() == '-') return false;

	return true;
}

// Helper: Parse site input - handles various formats:
// - "example.com" → discover sitemap, spider from /
// - "example.com/blog/" → discover sitemap, spider from /blog/
// - "https://example.com/sitemap.xml" → fetch sitemap directly
// - "https://example.com/page?q=1" → discover sitemap, spider from page URL
static ParsedSiteInput ParseSiteInput(const std::string &input) {
	ParsedSiteInput result;

	// Check if it's a full URL
	if (input.find("://") != std::string::npos) {
		result.hostname = LinkParser::ExtractDomain(input);

		if (IsSitemapUrl(input)) {
			// Direct sitemap URL
			result.type = SiteInputType::DIRECT_SITEMAP;
			result.sitemap_url = input;
			result.start_url = "https://" + result.hostname + "/";
		} else {
			// Page URL - use as spider start point
			result.type = SiteInputType::PAGE_URL;
			result.start_url = input;
		}
	} else {
		// Could be "example.com" or "example.com/path/"
		size_t slash_pos = input.find('/');
		if (slash_pos != std::string::npos) {
			result.hostname = input.substr(0, slash_pos);
			std::string path = input.substr(slash_pos);
			result.start_url = "https://" + result.hostname + path;
			result.type = SiteInputType::HOSTNAME_PATH;
		} else {
			result.hostname = input;
			result.start_url = "https://" + result.hostname + "/";
			result.type = SiteInputType::HOSTNAME;
		}
	}

	return result;
}

//===--------------------------------------------------------------------===//
// Link-Based URL Discovery (BFS Crawling)
//===--------------------------------------------------------------------===//

// Discover URLs by following links (used as fallback when no sitemap)
static std::vector<std::string> DiscoverUrlsViaLinks(
    DatabaseInstance &db,
    const std::string &start_url,
    const std::string &base_hostname,
    const std::string &user_agent,
    double default_crawl_delay,
    bool allow_subdomains,
    int max_pages,
    int max_depth,
    bool respect_nofollow,
    bool follow_canonical,
    DomainState &domain_state) {

	std::vector<std::string> discovered_urls;
	std::set<std::string> visited_urls;
	std::set<std::string> queued_urls;

	// BFS queue: (url, depth)
	std::queue<std::pair<std::string, int>> url_queue;
	url_queue.push({start_url, 0});
	queued_urls.insert(start_url);

	std::string base_domain = LinkParser::ExtractBaseDomain(base_hostname);

	RetryConfig retry_config;
	retry_config.max_retries = 2;

	int pages_crawled = 0;

	while (!url_queue.empty() && !g_shutdown_requested && pages_crawled < max_pages) {
		auto front = url_queue.front();
		url_queue.pop();
		const std::string &url = front.first;
		int depth = front.second;

		if (depth > max_depth) {
			continue;
		}

		if (visited_urls.count(url)) {
			continue;
		}
		visited_urls.insert(url);

		std::string url_domain = LinkParser::ExtractDomain(url);
		std::string url_path = LinkParser::ExtractPath(url);

		// Check robots.txt (only for base domain, assume fetched already)
		if (url_domain == base_hostname && domain_state.robots_fetched) {
			if (!RobotsParser::IsAllowed(domain_state.rules, url_path)) {
				continue;  // Disallowed by robots.txt
			}
		}

		// Respect rate limiting
		auto now = std::chrono::steady_clock::now();
		auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
		    now - domain_state.last_crawl_time).count();
		auto required_delay_ms = static_cast<int64_t>(domain_state.crawl_delay_seconds * 1000);

		if (elapsed < required_delay_ms) {
			std::this_thread::sleep_for(std::chrono::milliseconds(required_delay_ms - elapsed));
		}

		// Fetch page
		auto response = HttpClient::Fetch(db, url, retry_config, user_agent);
		domain_state.last_crawl_time = std::chrono::steady_clock::now();

		if (!response.success || response.status_code != 200) {
			continue;
		}

		// Check if HTML
		if (response.content_type.find("text/html") == std::string::npos) {
			discovered_urls.push_back(url);
			pages_crawled++;
			continue;
		}

		// Check for nofollow meta tag
		if (respect_nofollow && LinkParser::HasNoFollowMeta(response.body)) {
			discovered_urls.push_back(url);
			pages_crawled++;
			continue;  // Don't follow links from this page
		}

		// Check for canonical URL
		if (follow_canonical) {
			std::string canonical = LinkParser::ExtractCanonical(response.body, url);
			if (!canonical.empty() && canonical != url) {
				// Use canonical URL instead
				if (!visited_urls.count(canonical) && !queued_urls.count(canonical)) {
					if (LinkParser::IsSameDomain(canonical, base_domain, allow_subdomains)) {
						queued_urls.insert(canonical);
						url_queue.push({canonical, depth});
					}
				}
				continue;  // Skip this URL, use canonical
			}
		}

		discovered_urls.push_back(url);
		pages_crawled++;

		// Extract links from page
		auto links = LinkParser::ExtractLinks(response.body, url);

		for (const auto &link : links) {
			// Skip nofollow links if respect_nofollow is enabled
			if (respect_nofollow && link.nofollow) {
				continue;
			}

			// Skip already visited or queued
			if (visited_urls.count(link.url) || queued_urls.count(link.url)) {
				continue;
			}

			// Check domain policy
			if (!LinkParser::IsSameDomain(link.url, base_domain, allow_subdomains)) {
				continue;
			}

			// Only follow http/https
			if (link.url.find("https://") != 0 && link.url.find("http://") != 0) {
				continue;
			}

			queued_urls.insert(link.url);
			url_queue.push({link.url, depth + 1});
		}
	}

	return discovered_urls;
}

// Result from parallel sitemap discovery
struct SitemapDiscoveryResult {
	std::string hostname;
	std::string start_url;  // Full URL including path
	std::vector<std::string> urls;
	DomainState domain_state;
};

// Thread-safe sitemap discovery for parallel execution (with link fallback support)
// Handles various input formats:
// - "example.com" → robots.txt → bruteforce sitemaps → spider (if follow_links)
// - "example.com/path/" → robots.txt → bruteforce sitemaps → spider from path
// - "https://example.com/sitemap.xml" → fetch sitemap directly (still check robots.txt)
// - "https://example.com/page?q=1" → robots.txt → sitemaps → spider from page URL
static SitemapDiscoveryResult DiscoverSitemapUrlsThreadSafe(DatabaseInstance &db,
                                                             const std::string &input,
                                                             const std::string &user_agent,
                                                             double default_crawl_delay, double cache_hours,
                                                             bool follow_links, bool allow_subdomains,
                                                             int max_crawl_pages, int max_crawl_depth,
                                                             bool respect_nofollow, bool follow_canonical) {
	// Parse input to determine type and extract hostname/URLs
	auto parsed = ParseSiteInput(input);

	SitemapDiscoveryResult result;
	result.hostname = parsed.hostname;
	result.start_url = parsed.start_url;
	result.domain_state.crawl_delay_seconds = default_crawl_delay;
	result.domain_state.min_crawl_delay_seconds = default_crawl_delay;

	// Create own connection for thread safety
	Connection conn(db);

	// Check discovery status cache first (skip for direct sitemap URLs)
	if (parsed.type != SiteInputType::DIRECT_SITEMAP) {
		EnsureDiscoveryStatusTable(conn);
		auto status = GetDiscoveryStatus(conn, parsed.hostname, cache_hours);

		if (status.is_valid) {
			if (status.method == "sitemap" || status.method == "link_crawl") {
				// Check sitemap URL cache
				EnsureSitemapCacheTable(conn);
				auto cached_urls = GetCachedSitemapUrls(conn, parsed.hostname, cache_hours);
				if (!cached_urls.empty()) {
					result.urls = cached_urls;
					return result;
				}
			} else if (status.method == "not_found" && !follow_links) {
				// Previously found nothing and no fallback enabled
				return result;
			}
			// If not_found but follow_links now enabled, retry
		}
	}

	// Ensure cache table exists
	EnsureSitemapCacheTable(conn);

	// Try sitemap discovery
	std::vector<SitemapEntry> sitemap_entries;
	std::set<std::string> processed_sitemaps;
	std::vector<std::string> to_process;

	RetryConfig retry_config;
	retry_config.max_retries = 2;

	// Step 1: Always fetch robots.txt for rate limiting (even for direct sitemap URLs)
	std::string robots_url = "https://" + parsed.hostname + "/robots.txt";
	auto robots_response = HttpClient::Fetch(db, robots_url, retry_config, user_agent);

	if (robots_response.success) {
		// Parse robots rules for rate limiting
		auto robots_data = RobotsParser::Parse(robots_response.body);
		result.domain_state.rules = RobotsParser::GetRulesForUserAgent(robots_data, user_agent);
		result.domain_state.has_crawl_delay = result.domain_state.rules.HasCrawlDelay();
		if (result.domain_state.has_crawl_delay) {
			result.domain_state.crawl_delay_seconds = result.domain_state.rules.GetEffectiveDelay();
		}
		result.domain_state.min_crawl_delay_seconds = result.domain_state.crawl_delay_seconds;
		result.domain_state.robots_fetched = true;

		// Extract sitemaps from robots.txt (unless we have a direct sitemap URL)
		if (parsed.type != SiteInputType::DIRECT_SITEMAP) {
			auto sitemaps_from_robots = SitemapParser::ExtractSitemapsFromRobotsTxt(robots_response.body);
			for (auto &sm : sitemaps_from_robots) {
				to_process.push_back(sm);
			}
		}
	}

	// Step 2: Handle based on input type
	if (parsed.type == SiteInputType::DIRECT_SITEMAP) {
		// Direct sitemap URL provided - use it directly
		to_process.push_back(parsed.sitemap_url);
	} else if (to_process.empty()) {
		// No sitemaps from robots.txt - bruteforce common locations
		auto common_paths = SitemapParser::GetCommonSitemapPaths();
		for (auto &path : common_paths) {
			to_process.push_back("https://" + parsed.hostname + path);
		}
	}

	// Step 3: Fetch and parse sitemaps (handle sitemap indexes recursively)
	while (!to_process.empty() && !g_shutdown_requested) {
		std::string sitemap_url = to_process.back();
		to_process.pop_back();

		if (processed_sitemaps.count(sitemap_url)) {
			continue;
		}
		processed_sitemaps.insert(sitemap_url);

		auto response = HttpClient::Fetch(db, sitemap_url, retry_config, user_agent);
		if (!response.success || response.status_code != 200) {
			continue;
		}

		// Decompress gzipped sitemaps (check magic bytes, not URL extension)
		// Note: DuckDB's http_get may auto-decompress, so we check actual data
		std::string body = response.body;
		if (IsGzippedData(body)) {
			body = DecompressGzip(response.body);
			if (body.empty()) {
				continue;  // Decompression failed
			}
		}

		auto sitemap_data = SitemapParser::Parse(body);

		if (sitemap_data.is_index) {
			for (auto &nested_url : sitemap_data.sitemap_urls) {
				if (!processed_sitemaps.count(nested_url)) {
					to_process.push_back(nested_url);
				}
			}
		} else {
			for (auto &entry : sitemap_data.urls) {
				sitemap_entries.push_back(entry);
			}
		}
	}

	// If sitemap found, cache and return
	if (!sitemap_entries.empty()) {
		CacheSitemapUrls(conn, parsed.hostname, sitemap_entries);
		UpdateDiscoveryStatus(conn, parsed.hostname, "sitemap", sitemap_entries.size());

		for (const auto &entry : sitemap_entries) {
			result.urls.push_back(entry.loc);
		}
		return result;
	}

	// No sitemap found - try link-based discovery if enabled
	// For PAGE_URL type, always try spider from the provided URL (if follow_links)
	// For HOSTNAME/HOSTNAME_PATH, spider from start_url
	if (follow_links) {
		auto link_urls = DiscoverUrlsViaLinks(
		    db, parsed.start_url, parsed.hostname, user_agent, default_crawl_delay,
		    allow_subdomains, max_crawl_pages, max_crawl_depth,
		    respect_nofollow, follow_canonical, result.domain_state);

		if (!link_urls.empty()) {
			// Cache discovered URLs
			std::vector<SitemapEntry> entries;
			for (const auto &url : link_urls) {
				entries.push_back({url, "", "", ""});
			}
			CacheSitemapUrls(conn, parsed.hostname, entries);
			UpdateDiscoveryStatus(conn, parsed.hostname, "link_crawl", link_urls.size());

			result.urls = link_urls;
			return result;
		}
	}

	// Nothing found - cache negative result (but not for direct sitemap - those should just return empty)
	if (parsed.type != SiteInputType::DIRECT_SITEMAP) {
		UpdateDiscoveryStatus(conn, parsed.hostname, "not_found", 0);
	}
	return result;
}

// Helper: Discover sitemaps for a hostname (with caching)
static std::vector<std::string> DiscoverSitemapUrls(ClientContext &context, Connection &conn,
                                                     const std::string &hostname,
                                                     const std::string &user_agent,
                                                     std::unordered_map<std::string, DomainState> &domain_states,
                                                     double default_crawl_delay, double cache_hours) {
	// Check cache first
	EnsureSitemapCacheTable(conn);
	auto cached_urls = GetCachedSitemapUrls(conn, hostname, cache_hours);
	if (!cached_urls.empty()) {
		return cached_urls;
	}

	// No valid cache - discover sitemaps
	std::vector<SitemapEntry> sitemap_entries;  // Keep full metadata
	std::set<std::string> processed_sitemaps;
	std::vector<std::string> to_process;

	RetryConfig retry_config;
	retry_config.max_retries = 2;

	// Step 1: Try robots.txt for Sitemap directives
	std::string robots_url = "https://" + hostname + "/robots.txt";
	auto robots_response = HttpClient::Fetch(context, robots_url, retry_config, user_agent);

	if (robots_response.success) {
		auto sitemaps_from_robots = SitemapParser::ExtractSitemapsFromRobotsTxt(robots_response.body);
		for (auto &sm : sitemaps_from_robots) {
			to_process.push_back(sm);
		}

		// Also parse robots rules for rate limiting
		auto &domain_state = domain_states[hostname];
		auto robots_data = RobotsParser::Parse(robots_response.body);
		domain_state.rules = RobotsParser::GetRulesForUserAgent(robots_data, user_agent);
		domain_state.has_crawl_delay = domain_state.rules.HasCrawlDelay();
		if (domain_state.has_crawl_delay) {
			domain_state.crawl_delay_seconds = domain_state.rules.GetEffectiveDelay();
		} else {
			domain_state.crawl_delay_seconds = default_crawl_delay;
		}
		// Set floor for adaptive rate limiting
		domain_state.min_crawl_delay_seconds = domain_state.crawl_delay_seconds;
		domain_state.robots_fetched = true;
	}

	// Step 2: If no sitemaps found, bruteforce common locations
	if (to_process.empty()) {
		auto common_paths = SitemapParser::GetCommonSitemapPaths();
		for (auto &path : common_paths) {
			to_process.push_back("https://" + hostname + path);
		}
	}

	// Step 3: Fetch and parse sitemaps (handle sitemap indexes recursively)
	while (!to_process.empty() && !g_shutdown_requested) {
		std::string sitemap_url = to_process.back();
		to_process.pop_back();

		if (processed_sitemaps.count(sitemap_url)) {
			continue;
		}
		processed_sitemaps.insert(sitemap_url);

		auto response = HttpClient::Fetch(context, sitemap_url, retry_config, user_agent);
		if (!response.success || response.status_code != 200) {
			continue;
		}

		// Decompress gzipped sitemaps (check magic bytes, not URL extension)
		std::string body = response.body;
		if (IsGzippedData(body)) {
			body = DecompressGzip(response.body);
			if (body.empty()) {
				continue;  // Decompression failed
			}
		}

		auto sitemap_data = SitemapParser::Parse(body);

		if (sitemap_data.is_index) {
			// Sitemap index - add nested sitemaps to process
			for (auto &nested_url : sitemap_data.sitemap_urls) {
				if (!processed_sitemaps.count(nested_url)) {
					to_process.push_back(nested_url);
				}
			}
		} else {
			// Regular sitemap - keep full entry with metadata
			for (auto &entry : sitemap_data.urls) {
				sitemap_entries.push_back(entry);
			}
		}
	}

	// Cache discovered entries with metadata for future runs
	if (!sitemap_entries.empty()) {
		CacheSitemapUrls(conn, hostname, sitemap_entries);
	}

	// Return just URLs for compatibility
	std::vector<std::string> urls;
	for (const auto &entry : sitemap_entries) {
		urls.push_back(entry.loc);
	}
	return urls;
}

static void CrawlIntoFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->CastNoConst<CrawlIntoBindData>();
	auto &global_state = data.global_state->Cast<CrawlIntoGlobalState>();

	std::lock_guard<std::mutex> lock(global_state.mutex);

	if (global_state.executed) {
		output.SetCardinality(0);
		return;
	}
	global_state.executed = true;

	Connection conn(*context.db);

	// Execute source query to get URLs or hostnames
	auto query_result = conn.Query(bind_data.source_query);
	if (query_result->HasError()) {
		throw IOException("CRAWL source query error: " + query_result->GetError());
	}

	// Collect values from first column - must be valid URLs or hostnames
	std::vector<std::string> source_values;
	std::vector<std::string> invalid_values;
	while (auto chunk = query_result->Fetch()) {
		for (idx_t i = 0; i < chunk->size(); i++) {
			auto val = chunk->GetValue(0, i);
			if (!val.IsNull()) {
				// Check if value is a string type
				if (val.type().id() != LogicalTypeId::VARCHAR) {
					// Try to convert to string representation
					std::string str_val = val.ToString();
					if (!IsValidUrlOrHostname(str_val)) {
						invalid_values.push_back(str_val);
						continue;
					}
					source_values.push_back(str_val);
				} else {
					std::string str_val = StringValue::Get(val);
					if (!IsValidUrlOrHostname(str_val)) {
						invalid_values.push_back(str_val);
						continue;
					}
					source_values.push_back(str_val);
				}
			}
		}
	}

	// Report invalid values
	if (!invalid_values.empty()) {
		std::string msg = "CRAWL source query returned invalid URL/hostname values: ";
		for (size_t i = 0; i < std::min(invalid_values.size(), (size_t)5); i++) {
			if (i > 0) msg += ", ";
			msg += "'" + invalid_values[i] + "'";
		}
		if (invalid_values.size() > 5) {
			msg += " (and " + std::to_string(invalid_values.size() - 5) + " more)";
		}
		msg += ". Expected format: 'example.com', 'example.com/path', or 'https://example.com/path'";
		throw IOException(msg);
	}

	if (source_values.empty()) {
		output.SetCardinality(1);
		output.SetValue(0, 0, Value::BIGINT(0));
		return;
	}

	// Prepare domain states for rate limiting
	std::unordered_map<std::string, DomainState> domain_states;

	// Ensure target table exists for results
	EnsureTargetTable(conn, bind_data.target_table);

	auto crawl_start_time = std::chrono::steady_clock::now();

	// Mark discovery as started for progress bar
	global_state.discovery_started.store(true);

	// Discover URLs from hostnames/sites - always use discovery mode
	std::vector<std::string> urls;
	auto &db = DatabaseInstance::GetDatabase(context);

	// Limit parallel discovery to max_parallel_per_domain (or 8 if not set)
	int max_parallel = std::min((int)source_values.size(), bind_data.max_parallel_per_domain);
	if (max_parallel < 1) max_parallel = 8;

	std::vector<std::future<SitemapDiscoveryResult>> futures;
	std::vector<SitemapDiscoveryResult> results;

	size_t idx = 0;
	while (idx < source_values.size() && !g_shutdown_requested) {
		// Launch batch of async tasks
		futures.clear();
		for (int i = 0; i < max_parallel && idx < source_values.size(); i++, idx++) {
			const auto &input = source_values[idx];
			futures.push_back(std::async(std::launch::async,
			    DiscoverSitemapUrlsThreadSafe,
			    std::ref(db), input, bind_data.user_agent,
			    bind_data.default_crawl_delay, bind_data.sitemap_cache_hours,
			    bind_data.follow_links, bind_data.allow_subdomains,
			    bind_data.max_crawl_pages, bind_data.max_crawl_depth,
			    bind_data.respect_nofollow, bind_data.follow_canonical));
		}

		// Collect results from this batch
		for (auto &f : futures) {
			if (g_shutdown_requested) break;
			try {
				results.push_back(f.get());
			} catch (...) {
				// Silently skip failed discoveries
			}
		}
	}

	// Merge results into domain_states and urls
	for (auto &res : results) {
		domain_states[res.hostname] = res.domain_state;
		for (const auto &url : res.urls) {
			if (bind_data.url_filter.empty() || MatchesLikePattern(url, bind_data.url_filter)) {
				urls.push_back(url);
			}
		}
	}

	if (urls.empty()) {
		output.SetCardinality(1);
		output.SetValue(0, 0, Value::BIGINT(0));
		return;
	}

	// Get existing URLs from target table
	std::set<std::string> existing_urls;
	auto existing_query = conn.Query("SELECT DISTINCT url FROM " + bind_data.target_table);
	if (!existing_query->HasError()) {
		while (auto chunk = existing_query->Fetch()) {
			for (idx_t i = 0; i < chunk->size(); i++) {
				auto val = chunk->GetValue(0, i);
				if (!val.IsNull()) {
					existing_urls.insert(StringValue::Get(val));
				}
			}
		}
	}

	// Build URL priority queue: sorted by earliest_fetch time
	// Using greater<> so earliest times come first (min-heap)
	std::priority_queue<UrlQueueEntry, std::vector<UrlQueueEntry>, std::greater<UrlQueueEntry>> url_queue;

	auto now = std::chrono::steady_clock::now();

	// Build queue from discovered URLs
	for (const auto &url : urls) {
		if (existing_urls.find(url) == existing_urls.end()) {
			// New URL - can be fetched immediately
			url_queue.push(UrlQueueEntry(url, 0, false, now));
		}
	}

	// If update_stale enabled, check existing URLs for staleness and add them
	if (bind_data.update_stale && !existing_urls.empty()) {
		// Ensure sitemap cache exists for staleness checks
		EnsureSitemapCacheTable(conn);

		for (const auto &url : urls) {
			if (existing_urls.find(url) != existing_urls.end()) {
				// Existing URL - check if stale
				if (IsUrlStale(conn, url, bind_data.target_table)) {
					url_queue.push(UrlQueueEntry(url, 0, true, now));  // is_update = true
				}
			}
		}
	}

	if (url_queue.empty()) {
		output.SetCardinality(1);
		output.SetValue(0, 0, Value::BIGINT(0));
		return;
	}

	int64_t rows_changed = 0;

	// Batch insert configuration
	const size_t BATCH_SIZE = 100;
	std::vector<BatchCrawlEntry> result_batch;
	result_batch.reserve(BATCH_SIZE);
	int64_t urls_total = static_cast<int64_t>(url_queue.size());

	// Set total for progress bar
	global_state.total_urls.store(urls_total);
	global_state.processed_urls.store(0);

	// Process URL priority queue with retry logic
	// Check both global shutdown and specific crawl stop signal
	auto should_stop = [&]() {
		return g_shutdown_requested || ShouldStopBackgroundCrawl(bind_data.target_table);
	};

	while (!url_queue.empty() && !should_stop()) {
		auto entry = url_queue.top();
		url_queue.pop();

		// Wait if the entry's earliest fetch time is in the future
		auto now = std::chrono::steady_clock::now();
		if (entry.earliest_fetch > now) {
			auto wait_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
			    entry.earliest_fetch - now).count();
			if (wait_ms > 0 && wait_ms < 60000) {  // Wait max 60s at a time
				std::this_thread::sleep_for(std::chrono::milliseconds(wait_ms));
			}
			now = std::chrono::steady_clock::now();
		}

		if (should_stop()) {
			break;
		}

		std::string domain = ExtractDomain(entry.url);
		std::string path = ExtractPath(entry.url);

		auto &domain_state = domain_states[domain];

		// Check if domain is blocked (429/5XX backoff)
		if (domain_state.blocked_until > now) {
			// Domain is blocked - re-queue with updated earliest_fetch if under retry limit
			if (entry.retry_count < 5) {
				entry.retry_count++;
				entry.earliest_fetch = domain_state.blocked_until;
				url_queue.push(entry);
			}
			continue;
		}

		// Fetch robots.txt if needed (may already be fetched in SITES mode)
		if (bind_data.respect_robots_txt && !domain_state.robots_fetched) {
			std::string robots_url = "https://" + domain + "/robots.txt";
			RetryConfig retry_config;

			auto response = HttpClient::Fetch(context, robots_url, retry_config, bind_data.user_agent);

			if (response.success) {
				auto robots_data = RobotsParser::Parse(response.body);
				domain_state.rules = RobotsParser::GetRulesForUserAgent(robots_data, bind_data.user_agent);

				// Track if crawl-delay was specified (affects parallel request limit)
				domain_state.has_crawl_delay = domain_state.rules.HasCrawlDelay();

				if (domain_state.has_crawl_delay) {
					domain_state.crawl_delay_seconds = domain_state.rules.GetEffectiveDelay();
				} else {
					domain_state.crawl_delay_seconds = bind_data.default_crawl_delay;
				}

				domain_state.crawl_delay_seconds = std::max(domain_state.crawl_delay_seconds, bind_data.min_crawl_delay);
				domain_state.crawl_delay_seconds = std::min(domain_state.crawl_delay_seconds, bind_data.max_crawl_delay);
				// Set floor for adaptive rate limiting
				domain_state.min_crawl_delay_seconds = domain_state.crawl_delay_seconds;
			} else {
				domain_state.crawl_delay_seconds = bind_data.default_crawl_delay;
				domain_state.min_crawl_delay_seconds = bind_data.default_crawl_delay;
				domain_state.has_crawl_delay = false;  // No robots.txt = allow parallel
			}

			domain_state.robots_fetched = true;
		}

		// Check robots.txt rules
		if (bind_data.respect_robots_txt && !RobotsParser::IsAllowed(domain_state.rules, path)) {
			if (bind_data.log_skipped) {
				std::string surt_key = GenerateSurtKey(entry.url);
				auto insert_sql = "INSERT INTO " + bind_data.target_table +
				                  " (url, surt_key, http_status, body, content_type, elapsed_ms, crawled_at, error, etag, last_modified, content_hash) "
				                  "VALUES ($1, $2, $3, NULL, NULL, 0, NOW(), $4, NULL, NULL, NULL)";
				auto insert_result = conn.Query(insert_sql, entry.url, surt_key, -1, "robots.txt disallow");
				if (!insert_result->HasError()) {
					rows_changed++;
				}
			}
			continue;
		}

		// Check parallel limit for domains without crawl-delay
		if (!domain_state.has_crawl_delay) {
			if (domain_state.active_requests >= bind_data.max_parallel_per_domain) {
				// Re-queue with small delay to let other domains proceed
				entry.earliest_fetch = std::chrono::steady_clock::now() + std::chrono::milliseconds(50);
				url_queue.push(entry);
				continue;
			}
			domain_state.active_requests++;
		} else {
			// Wait for crawl delay (serial mode for domains with crawl-delay)
			now = std::chrono::steady_clock::now();
			auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - domain_state.last_crawl_time).count();
			auto required_delay_ms = static_cast<int64_t>(domain_state.crawl_delay_seconds * 1000);

			if (elapsed < required_delay_ms) {
				auto wait_time = required_delay_ms - elapsed;
				std::this_thread::sleep_for(std::chrono::milliseconds(wait_time));
			}
		}

		if (should_stop()) {
			if (!domain_state.has_crawl_delay) {
				domain_state.active_requests--;
			}
			break;
		}

		// Check global connection limit
		while (g_active_connections >= bind_data.max_total_connections && !should_stop()) {
			std::this_thread::sleep_for(std::chrono::milliseconds(10));
		}
		if (should_stop()) {
			if (!domain_state.has_crawl_delay) {
				domain_state.active_requests--;
			}
			break;
		}

		// Fetch URL
		g_active_connections++;
		auto fetch_start = std::chrono::steady_clock::now();
		RetryConfig retry_config;
		auto response = HttpClient::Fetch(context, entry.url, retry_config, bind_data.user_agent, bind_data.compress);
		auto fetch_end = std::chrono::steady_clock::now();
		auto fetch_elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(fetch_end - fetch_start).count();
		g_active_connections--;

		domain_state.last_crawl_time = fetch_end;

		// Decrement active requests for parallel mode
		if (!domain_state.has_crawl_delay) {
			domain_state.active_requests--;
		}

		// Check response size limit
		if (response.success && static_cast<int64_t>(response.body.size()) > bind_data.max_response_bytes) {
			response.success = false;
			response.error = "Response too large: " + std::to_string(response.body.size()) + " bytes";
			response.body.clear();  // Don't store oversized content
		}

		// Check content-type filter
		if (response.success && !IsContentTypeAcceptable(response.content_type,
		                                                   bind_data.accept_content_types,
		                                                   bind_data.reject_content_types)) {
			response.success = false;
			response.error = "Content-Type rejected: " + response.content_type;
			response.body.clear();  // Don't store rejected content
		}

		// Check for retryable errors (429, 5XX, network errors)
		bool is_retryable = (response.status_code == 429 ||
		                     (response.status_code >= 500 && response.status_code <= 504) ||
		                     response.status_code <= 0);

		if (is_retryable) {
			// Calculate backoff using Fibonacci
			domain_state.consecutive_429s++;

			int backoff_seconds;
			if (response.status_code == 429 && !response.retry_after.empty()) {
				// Use Retry-After header if provided
				backoff_seconds = HttpClient::ParseRetryAfter(response.retry_after) / 1000;
				if (backoff_seconds <= 0) {
					backoff_seconds = FibonacciBackoffSeconds(domain_state.consecutive_429s,
					                                           bind_data.max_retry_backoff_seconds);
				}
			} else {
				backoff_seconds = FibonacciBackoffSeconds(domain_state.consecutive_429s,
				                                           bind_data.max_retry_backoff_seconds);
			}

			// Block domain
			domain_state.blocked_until = std::chrono::steady_clock::now() +
			                              std::chrono::seconds(backoff_seconds);

			// Re-queue URL if under retry limit
			if (entry.retry_count < 5) {
				entry.retry_count++;
				entry.earliest_fetch = domain_state.blocked_until;  // Schedule after backoff
				url_queue.push(entry);
			}
			continue;
		}

		// Success - clear consecutive error count and update adaptive delay
		domain_state.consecutive_429s = 0;
		UpdateAdaptiveDelay(domain_state, static_cast<double>(fetch_elapsed_ms), bind_data.max_crawl_delay);

		// Get timestamp - prefer server Date header if valid (within 15 min of local time)
		std::string validated_date = ParseAndValidateServerDate(response.server_date);
		std::string timestamp_expr = validated_date.empty() ? "NOW()" : ("'" + validated_date + "'::TIMESTAMP");

		// Generate SURT key and content hash for deduplication
		std::string surt_key = GenerateSurtKey(entry.url);
		std::string content_hash = GenerateContentHash(response.body);

		// Add to batch
		result_batch.push_back(BatchCrawlEntry{
			entry.url,
			surt_key,
			response.status_code,
			response.body,
			response.content_type,
			fetch_elapsed_ms,
			timestamp_expr,
			response.error,
			response.etag,
			response.last_modified,
			content_hash,
			entry.is_update
		});

		// Flush batch if full
		if (result_batch.size() >= BATCH_SIZE) {
			rows_changed += FlushBatch(conn, bind_data.target_table, result_batch);
		}

		// Update progress for progress bar
		global_state.processed_urls.fetch_add(1);
	}

	// Flush any remaining results
	if (!result_batch.empty()) {
		rows_changed += FlushBatch(conn, bind_data.target_table, result_batch);
	}

	global_state.rows_inserted = rows_changed;

	output.SetCardinality(1);
	output.SetValue(0, 0, Value::BIGINT(rows_changed));
}

//===--------------------------------------------------------------------===//
// stop_crawl_internal - Used by STOP CRAWL INTO parser extension
//===--------------------------------------------------------------------===//

struct StopCrawlBindData : public TableFunctionData {
	std::string target_table;
};

struct StopCrawlGlobalState : public GlobalTableFunctionState {
	std::mutex mutex;
	bool executed = false;

	idx_t MaxThreads() const override {
		return 1;
	}
};

static unique_ptr<FunctionData> StopCrawlBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<string> &names) {
	auto bind_data = make_uniq<StopCrawlBindData>();

	if (input.inputs.size() >= 1) {
		bind_data->target_table = StringValue::Get(input.inputs[0]);
	}

	// Return status message
	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("status");

	return std::move(bind_data);
}

static unique_ptr<GlobalTableFunctionState> StopCrawlInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<StopCrawlGlobalState>();
}

static void StopCrawlFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->CastNoConst<StopCrawlBindData>();
	auto &global_state = data.global_state->Cast<StopCrawlGlobalState>();

	std::lock_guard<std::mutex> lock(global_state.mutex);

	if (global_state.executed) {
		output.SetCardinality(0);
		return;
	}
	global_state.executed = true;

	// Find and stop the background crawl
	std::string status;
	{
		std::lock_guard<std::mutex> crawl_lock(g_background_crawls_mutex);
		auto it = g_background_crawls.find(bind_data.target_table);
		if (it != g_background_crawls.end()) {
			auto &state = it->second;
			if (state->running.load()) {
				state->shutdown_requested = true;
				status = "Stopping crawl into " + bind_data.target_table;
			} else {
				status = "No active crawl into " + bind_data.target_table;
			}
		} else {
			status = "No crawl found for " + bind_data.target_table;
		}
	}

	output.SetCardinality(1);
	output.SetValue(0, 0, Value(status));
}

// Progress callback for CRAWL function - returns fraction 0.0 to 1.0
static double CrawlIntoProgress(ClientContext &context, const FunctionData *bind_data_p,
                                 const GlobalTableFunctionState *gstate_p) {
	if (!gstate_p) {
		return -1.0;  // Unknown progress
	}

	auto &gstate = gstate_p->Cast<CrawlIntoGlobalState>();

	int64_t total = gstate.total_urls.load();
	int64_t processed = gstate.processed_urls.load();

	if (total <= 0) {
		// During discovery phase, show small progress to indicate activity
		if (gstate.discovery_started.load()) {
			return 0.005;  // 0.5% - show "discovering..." progress
		}
		return -1.0;  // Not started yet
	}

	return static_cast<double>(processed) / static_cast<double>(total);
}

void RegisterCrawlIntoFunction(ExtensionLoader &loader) {
	TableFunction crawl_into_func("crawl_into_internal",
	                               {LogicalType::INTEGER,  // statement_type (0=CRAWL)
	                                LogicalType::VARCHAR,  // source_query
	                                LogicalType::VARCHAR,  // target_table
	                                LogicalType::VARCHAR,  // user_agent
	                                LogicalType::DOUBLE,   // default_crawl_delay
	                                LogicalType::DOUBLE,   // min_crawl_delay
	                                LogicalType::DOUBLE,   // max_crawl_delay
	                                LogicalType::INTEGER,  // timeout_seconds
	                                LogicalType::BOOLEAN,  // respect_robots_txt
	                                LogicalType::BOOLEAN,  // log_skipped
	                                LogicalType::VARCHAR,  // url_filter
	                                LogicalType::DOUBLE,   // sitemap_cache_hours
	                                LogicalType::BOOLEAN,  // update_stale
	                                LogicalType::INTEGER,  // max_retry_backoff_seconds
	                                LogicalType::INTEGER,  // max_parallel_per_domain
	                                LogicalType::INTEGER,  // max_total_connections
	                                LogicalType::BIGINT,   // max_response_bytes
	                                LogicalType::BOOLEAN,  // compress
	                                LogicalType::VARCHAR,  // accept_content_types
	                                LogicalType::VARCHAR,  // reject_content_types
	                                LogicalType::BOOLEAN,  // follow_links
	                                LogicalType::BOOLEAN,  // allow_subdomains
	                                LogicalType::INTEGER,  // max_crawl_pages
	                                LogicalType::INTEGER,  // max_crawl_depth
	                                LogicalType::BOOLEAN,  // respect_nofollow
	                                LogicalType::BOOLEAN}, // follow_canonical
	                               CrawlIntoFunction, CrawlIntoBind, CrawlIntoInitGlobal);

	// Set progress callback for progress bar integration
	crawl_into_func.table_scan_progress = CrawlIntoProgress;

	loader.RegisterFunction(crawl_into_func);

	// Register stop_crawl_internal function
	TableFunction stop_crawl_func("stop_crawl_internal",
	                               {LogicalType::VARCHAR},  // target_table
	                               StopCrawlFunction, StopCrawlBind, StopCrawlInitGlobal);

	loader.RegisterFunction(stop_crawl_func);
}

} // namespace duckdb
