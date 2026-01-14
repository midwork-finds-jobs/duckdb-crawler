#include "http_client.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/common/string_util.hpp"
#include <thread>
#include <chrono>
#include <cmath>
#include <cstdlib>

namespace duckdb {

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

HttpResponse HttpClient::ExecuteHttpGet(DatabaseInstance &db, const std::string &url, const std::string &user_agent) {
	HttpResponse response;

	Connection conn(db);

	// Load http_request in this connection
	auto load_result = conn.Query("LOAD http_request");
	if (load_result->HasError()) {
		response.error = "Failed to load http_request: " + load_result->GetError();
		return response;
	}

	// Escape URL for SQL
	std::string escaped_url = StringUtil::Replace(url, "'", "''");

	// Build query - request headers to get Retry-After and Date
	std::string query;
	if (!user_agent.empty()) {
		std::string escaped_ua = StringUtil::Replace(user_agent, "'", "''");
		query = StringUtil::Format(
		    "SELECT status, decode(body) AS body, "
		    "content_type, "
		    "headers['retry-after'] AS retry_after, "
		    "headers['Date'] AS server_date"
		    "FROM http_get('%s', headers := {'User-Agent': '%s'})",
		    escaped_url, escaped_ua);
	} else {
		query = StringUtil::Format(
		    "SELECT status, decode(body) AS body, "
		    "content_type, "
		    "headers['retry-after'] AS retry_after, "
		    "headers['Date'] AS server_date"
		    "FROM http_get('%s')",
		    escaped_url);
	}

	auto result = conn.Query(query);

	if (result->HasError()) {
		response.error = result->GetError();
		response.status_code = 0;
		return response;
	}

	auto chunk = result->Fetch();
	if (!chunk || chunk->size() == 0) {
		response.error = "No response from HTTP request";
		response.status_code = 0;
		return response;
	}

	// Get status code
	auto status_val = chunk->GetValue(0, 0);
	response.status_code = status_val.IsNull() ? 0 : status_val.GetValue<int>();

	// Get body
	auto body_val = chunk->GetValue(1, 0);
	response.body = body_val.IsNull() ? "" : body_val.GetValue<std::string>();

	// Get content-type
	auto ct_val = chunk->GetValue(2, 0);
	response.content_type = ct_val.IsNull() ? "" : ct_val.GetValue<std::string>();

	// Get retry-after
	auto ra_val = chunk->GetValue(3, 0);
	response.retry_after = ra_val.IsNull() ? "" : ra_val.GetValue<std::string>();

	// Get server date
	auto date_val = chunk->GetValue(4, 0);
	response.server_date = date_val.IsNull() ? "" : date_val.GetValue<std::string>();

	response.success = (response.status_code >= 200 && response.status_code < 300);
	return response;
}

HttpResponse HttpClient::Fetch(ClientContext &context, const std::string &url, const RetryConfig &config,
                               const std::string &user_agent) {
	auto &db = DatabaseInstance::GetDatabase(context);

	// Single attempt - crawler handles all retries with Fibonacci backoff
	return ExecuteHttpGet(db, url, user_agent);
}

} // namespace duckdb
