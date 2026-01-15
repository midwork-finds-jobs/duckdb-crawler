#pragma once

#include <string>
#include <vector>
#include <mutex>
#include <curl/curl.h>

namespace duckdb {

struct HttpResponse {
	int status_code = 0;
	std::string body;
	std::string content_type;
	std::string retry_after;
	std::string server_date;      // Date header from server
	std::string etag;             // ETag header for conditional requests
	std::string last_modified;    // Last-Modified header for conditional requests
	std::string error;
	int64_t content_length = -1;  // -1 if unknown
	bool success = false;
	long http_version = 0;        // HTTP version used (CURL_HTTP_VERSION_*)
	std::string final_url;        // Final URL after redirects
	int redirect_count = 0;       // Number of redirects followed
};

struct RetryConfig {
	int max_retries = 5;
	int initial_backoff_ms = 100;
	double backoff_multiplier = 2.0;
	int max_backoff_ms = 30000;
};

// HTTP settings read from DuckDB configuration
struct HttpSettings {
	int timeout_seconds = 30;
	bool keep_alive = true;
	std::string proxy;
	std::string proxy_username;
	std::string proxy_password;
};

// Thread-safe connection pool for curl handles
class HttpConnectionPool {
public:
	HttpConnectionPool();
	~HttpConnectionPool();

	// Disable copy/move
	HttpConnectionPool(const HttpConnectionPool&) = delete;
	HttpConnectionPool& operator=(const HttpConnectionPool&) = delete;

	// Get a curl easy handle (reuses from pool or creates new)
	CURL* AcquireHandle();
	// Return handle to pool for reuse
	void ReleaseHandle(CURL* handle);

	// Get HTTP version string for logging
	static std::string GetHttpVersionString();

private:
	std::mutex pool_mutex_;
	std::vector<CURL*> available_handles_;
	bool initialized_ = false;
};

// Global connection pool access
HttpConnectionPool& GetConnectionPool();

// Initialize HTTP client (call in extension load)
void InitializeHttpClient();
// Cleanup HTTP client (call in extension unload)
void CleanupHttpClient();

// Global HTTP settings (thread-safe)
void SetHttpSettings(const HttpSettings &settings);
const HttpSettings& GetHttpSettings();

class HttpClient {
public:
	// Simplified interface - no longer needs DuckDB context
	static HttpResponse Fetch(const std::string &url, const RetryConfig &config,
	                          const std::string &user_agent = "", bool compress = true,
	                          const std::string &if_none_match = "", const std::string &if_modified_since = "");

	static int ParseRetryAfter(const std::string &retry_after);

private:
	static HttpResponse ExecuteHttpGet(const std::string &url,
	                                    const std::string &user_agent, bool compress,
	                                    const std::string &if_none_match, const std::string &if_modified_since);
	static bool IsRetryable(int status_code);
};

} // namespace duckdb
