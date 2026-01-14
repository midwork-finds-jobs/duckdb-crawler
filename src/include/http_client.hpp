#pragma once

#include "duckdb.hpp"
#include <string>
#include <map>

namespace duckdb {

struct HttpResponse {
	int status_code = 0;
	std::string body;
	std::string content_type;
	std::string retry_after;
	std::string server_date;  // Date header from server
	std::string error;
	bool success = false;
};

struct RetryConfig {
	int max_retries = 5;
	int initial_backoff_ms = 100;
	double backoff_multiplier = 2.0;
	int max_backoff_ms = 30000;
};

class HttpClient {
public:
	static HttpResponse Fetch(ClientContext &context, const std::string &url, const RetryConfig &config,
	                          const std::string &user_agent = "");
	static int ParseRetryAfter(const std::string &retry_after);

private:
	static HttpResponse ExecuteHttpGet(DatabaseInstance &db, const std::string &url, const std::string &user_agent);
	static bool IsRetryable(int status_code);
};

} // namespace duckdb
