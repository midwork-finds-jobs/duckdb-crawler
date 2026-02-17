#include "http_client.hpp"
#include <cstdlib>
#include <cstring>
#include <algorithm>
#include <cctype>

namespace duckdb {

// Determine HTTP version at compile time based on available features
#if defined(CRAWLER_HTTP3_SUPPORT) && CRAWLER_HTTP3_SUPPORT
static constexpr long CRAWLER_HTTP_VERSION = CURL_HTTP_VERSION_3;
static constexpr const char *CRAWLER_HTTP_VERSION_STR = "HTTP/3";
#elif defined(CRAWLER_HTTP2_SUPPORT) && CRAWLER_HTTP2_SUPPORT
static constexpr long CRAWLER_HTTP_VERSION = CURL_HTTP_VERSION_2TLS;
static constexpr const char *CRAWLER_HTTP_VERSION_STR = "HTTP/2";
#else
static constexpr long CRAWLER_HTTP_VERSION = CURL_HTTP_VERSION_1_1;
static constexpr const char *CRAWLER_HTTP_VERSION_STR = "HTTP/1.1";
#endif

// Global connection pool (singleton)
static HttpConnectionPool *g_connection_pool = nullptr;

// Global HTTP settings with mutex for thread safety
static HttpSettings g_http_settings;
static std::mutex g_http_settings_mutex;

HttpConnectionPool &GetConnectionPool() {
	if (!g_connection_pool) {
		g_connection_pool = new HttpConnectionPool();
	}
	return *g_connection_pool;
}

void SetHttpSettings(const HttpSettings &settings) {
	std::lock_guard<std::mutex> lock(g_http_settings_mutex);
	g_http_settings = settings;
}

const HttpSettings &GetHttpSettings() {
	std::lock_guard<std::mutex> lock(g_http_settings_mutex);
	return g_http_settings;
}

void InitializeHttpClient() {
	curl_global_init(CURL_GLOBAL_DEFAULT);
	// Initialize connection pool
	GetConnectionPool();
}

void CleanupHttpClient() {
	if (g_connection_pool) {
		delete g_connection_pool;
		g_connection_pool = nullptr;
	}
	curl_global_cleanup();
}

HttpConnectionPool::HttpConnectionPool() : initialized_(true) {
}

HttpConnectionPool::~HttpConnectionPool() {
	std::lock_guard<std::mutex> lock(pool_mutex_);
	for (CURL *handle : available_handles_) {
		curl_easy_cleanup(handle);
	}
	available_handles_.clear();
	initialized_ = false;
}

CURL *HttpConnectionPool::AcquireHandle() {
	std::lock_guard<std::mutex> lock(pool_mutex_);
	if (!available_handles_.empty()) {
		CURL *handle = available_handles_.back();
		available_handles_.pop_back();
		curl_easy_reset(handle); // Reset for reuse but keep connection alive
		return handle;
	}
	return curl_easy_init();
}

void HttpConnectionPool::ReleaseHandle(CURL *handle) {
	if (!handle)
		return;
	std::lock_guard<std::mutex> lock(pool_mutex_);
	if (initialized_ && available_handles_.size() < 100) { // Max 100 pooled handles
		available_handles_.push_back(handle);
	} else {
		curl_easy_cleanup(handle);
	}
}

std::string HttpConnectionPool::GetHttpVersionString() {
	return CRAWLER_HTTP_VERSION_STR;
}

bool HttpClient::IsRetryable(int status_code) {
	// Network errors (connection failures)
	if (status_code <= 0) {
		return true;
	}
	// 429 is NOT auto-retryable here - crawler handles domain-level blocking
	// Server errors
	if (status_code >= 500 && status_code <= 504) {
		return true;
	}
	return false;
}

int HttpClient::ParseRetryAfter(const std::string &retry_after) {
	if (retry_after.empty()) {
		return 0;
	}

	// Try to parse as integer (seconds)
	try {
		int seconds = std::stoi(retry_after);
		return seconds * 1000; // Convert to milliseconds
	} catch (...) {
		// Could be HTTP-date format, but for simplicity we'll just return 0
		// and fall back to exponential backoff
		return 0;
	}
}

// Callback data structures
struct WriteData {
	std::string *body;
};

struct HeaderData {
	std::string content_type;
	std::string retry_after;
	std::string server_date;
	std::string etag;
	std::string last_modified;
	int64_t content_length = -1;
};

// Write callback for response body
static size_t WriteCallback(void *contents, size_t size, size_t nmemb, void *userp) {
	size_t total_size = size * nmemb;
	WriteData *data = static_cast<WriteData *>(userp);
	data->body->append(static_cast<char *>(contents), total_size);
	return total_size;
}

// Helper to trim whitespace
static std::string TrimString(const std::string &str) {
	size_t start = str.find_first_not_of(" \t\r\n");
	if (start == std::string::npos)
		return "";
	size_t end = str.find_last_not_of(" \t\r\n");
	return str.substr(start, end - start + 1);
}

// Helper to lowercase string
static std::string ToLower(const std::string &str) {
	std::string result = str;
	std::transform(result.begin(), result.end(), result.begin(), [](unsigned char c) { return std::tolower(c); });
	return result;
}

// Header callback for response headers
static size_t HeaderCallback(char *buffer, size_t size, size_t nitems, void *userdata) {
	size_t total_size = size * nitems;
	HeaderData *headers = static_cast<HeaderData *>(userdata);

	std::string header(buffer, total_size);

	// Find colon separator
	size_t colon_pos = header.find(':');
	if (colon_pos != std::string::npos) {
		std::string name = ToLower(TrimString(header.substr(0, colon_pos)));
		std::string value = TrimString(header.substr(colon_pos + 1));

		if (name == "content-type") {
			headers->content_type = value;
		} else if (name == "retry-after") {
			headers->retry_after = value;
		} else if (name == "date") {
			headers->server_date = value;
		} else if (name == "etag") {
			headers->etag = value;
		} else if (name == "last-modified") {
			headers->last_modified = value;
		} else if (name == "content-length") {
			try {
				headers->content_length = std::stoll(value);
			} catch (...) {
				headers->content_length = -1;
			}
		}
	}

	return total_size;
}

HttpResponse HttpClient::ExecuteHttpGet(const std::string &url, const std::string &user_agent, bool compress,
                                        const std::string &if_none_match, const std::string &if_modified_since) {
	HttpResponse response;

	auto &pool = GetConnectionPool();
	CURL *curl = pool.AcquireHandle();
	if (!curl) {
		response.error = "Failed to acquire curl handle";
		return response;
	}

	// Response data
	std::string body;
	WriteData write_data {&body};
	HeaderData header_data;

	// Set URL
	curl_easy_setopt(curl, CURLOPT_URL, url.c_str());

	// Set HTTP version based on compile-time detection
	curl_easy_setopt(curl, CURLOPT_HTTP_VERSION, CRAWLER_HTTP_VERSION);

	// Set callbacks
	curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
	curl_easy_setopt(curl, CURLOPT_WRITEDATA, &write_data);
	curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, HeaderCallback);
	curl_easy_setopt(curl, CURLOPT_HEADERDATA, &header_data);

	// Set user agent
	if (!user_agent.empty()) {
		curl_easy_setopt(curl, CURLOPT_USERAGENT, user_agent.c_str());
	}

	// Enable compression
	if (compress) {
		curl_easy_setopt(curl, CURLOPT_ACCEPT_ENCODING, "gzip, deflate");
	}

	// Apply global HTTP settings (from DuckDB configuration)
	const HttpSettings &settings = GetHttpSettings();

	// Set timeout from settings
	curl_easy_setopt(curl, CURLOPT_TIMEOUT, static_cast<long>(settings.timeout_seconds));
	curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, std::max(10L, static_cast<long>(settings.timeout_seconds / 3)));

	// Keep-alive setting
	curl_easy_setopt(curl, CURLOPT_TCP_KEEPALIVE, settings.keep_alive ? 1L : 0L);

	// Proxy settings
	if (!settings.proxy.empty()) {
		curl_easy_setopt(curl, CURLOPT_PROXY, settings.proxy.c_str());
		if (!settings.proxy_username.empty()) {
			curl_easy_setopt(curl, CURLOPT_PROXYUSERNAME, settings.proxy_username.c_str());
		}
		if (!settings.proxy_password.empty()) {
			curl_easy_setopt(curl, CURLOPT_PROXYPASSWORD, settings.proxy_password.c_str());
		}
	}

	// Follow redirects
	curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
	curl_easy_setopt(curl, CURLOPT_MAXREDIRS, 10L);

	// Build custom headers for conditional requests
	struct curl_slist *custom_headers = nullptr;
	if (!if_none_match.empty()) {
		std::string header = "If-None-Match: " + if_none_match;
		custom_headers = curl_slist_append(custom_headers, header.c_str());
	}
	if (!if_modified_since.empty()) {
		std::string header = "If-Modified-Since: " + if_modified_since;
		custom_headers = curl_slist_append(custom_headers, header.c_str());
	}
	if (custom_headers) {
		curl_easy_setopt(curl, CURLOPT_HTTPHEADER, custom_headers);
	}

	// Perform the request
	CURLcode res = curl_easy_perform(curl);

	// Get response info
	if (res == CURLE_OK) {
		long status_code;
		curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &status_code);
		response.status_code = static_cast<int>(status_code);

		// Get actual HTTP version used
		long http_version;
		curl_easy_getinfo(curl, CURLINFO_HTTP_VERSION, &http_version);
		response.http_version = http_version;

		// Get redirect info
		char *effective_url = nullptr;
		curl_easy_getinfo(curl, CURLINFO_EFFECTIVE_URL, &effective_url);
		if (effective_url) {
			response.final_url = effective_url;
		}
		long redirect_count = 0;
		curl_easy_getinfo(curl, CURLINFO_REDIRECT_COUNT, &redirect_count);
		response.redirect_count = static_cast<int>(redirect_count);

		response.body = std::move(body);
		response.content_type = std::move(header_data.content_type);
		response.retry_after = std::move(header_data.retry_after);
		response.server_date = std::move(header_data.server_date);
		response.etag = std::move(header_data.etag);
		response.last_modified = std::move(header_data.last_modified);
		response.content_length = header_data.content_length;

		// 304 Not Modified is also considered success for conditional requests
		response.success = (response.status_code >= 200 && response.status_code < 300) || response.status_code == 304;
	} else {
		response.error = curl_easy_strerror(res);
		response.status_code = 0;
		response.success = false;
	}

	// Cleanup
	if (custom_headers) {
		curl_slist_free_all(custom_headers);
	}
	pool.ReleaseHandle(curl);

	return response;
}

HttpResponse HttpClient::Fetch(const std::string &url, const RetryConfig &config, const std::string &user_agent,
                               bool compress, const std::string &if_none_match, const std::string &if_modified_since) {
	// Single attempt - crawler handles all retries with Fibonacci backoff
	return ExecuteHttpGet(url, user_agent, compress, if_none_match, if_modified_since);
}

} // namespace duckdb
