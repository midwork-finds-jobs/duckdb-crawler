#pragma once

#include <string>
#include <cstdint>

namespace duckdb {

//===--------------------------------------------------------------------===//
// Error Classification
//===--------------------------------------------------------------------===//

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

const char* ErrorTypeToString(CrawlErrorType type);
CrawlErrorType ClassifyError(int status_code, const std::string &error_msg);

//===--------------------------------------------------------------------===//
// Compression Utilities
//===--------------------------------------------------------------------===//

// Decompress gzip data. Returns empty string on error.
std::string DecompressGzip(const std::string &compressed_data);

// Check if data starts with gzip magic bytes (0x1f 0x8b)
bool IsGzippedData(const std::string &data);

//===--------------------------------------------------------------------===//
// Backoff and Rate Limiting
//===--------------------------------------------------------------------===//

// Fibonacci backoff: 3, 3, 6, 9, 15, 24, 39, 63, 102, 165, 267...
int FibonacciBackoffSeconds(int n, int max_seconds);

//===--------------------------------------------------------------------===//
// Date/Time Utilities
//===--------------------------------------------------------------------===//

// Parse HTTP date format: "Tue, 14 Jan 2025 12:00:00 GMT"
// Returns ISO timestamp if valid and within 15 min of local time, else empty
std::string ParseAndValidateServerDate(const std::string &server_date);

//===--------------------------------------------------------------------===//
// URL Utilities
//===--------------------------------------------------------------------===//

// Extract domain from URL (without port)
std::string ExtractDomain(const std::string &url);

// Extract path from URL (including query string)
std::string ExtractPath(const std::string &url);

// Generate SURT key (Sort-friendly URI Reordering Transform)
// Example: https://www.example.com/path?q=1 → com,example)/path?q=1
std::string GenerateSurtKey(const std::string &url);

// Generate domain SURT prefix (without path)
// Example: "www.example.com" → "com,example)"
std::string GenerateDomainSurt(const std::string &hostname);

// Generate content hash for deduplication (hex string)
std::string GenerateContentHash(const std::string &content);

//===--------------------------------------------------------------------===//
// Content-Type Utilities
//===--------------------------------------------------------------------===//

// Check if content-type matches pattern (supports wildcards like "text/*")
bool ContentTypeMatches(const std::string &content_type, const std::string &pattern);

// Check if content-type is acceptable (matches accept list, not in reject list)
bool IsContentTypeAcceptable(const std::string &content_type,
                             const std::string &accept_types,
                             const std::string &reject_types);

//===--------------------------------------------------------------------===//
// SQL Safety Utilities
//===--------------------------------------------------------------------===//

// Validate identifier (table/column name) for SQL safety
// Allows: alphanumeric, underscore, period (schema.table), max 128 chars
bool IsValidSqlIdentifier(const std::string &identifier);

// Quote identifier for safe use in SQL (double quotes, escape embedded quotes)
std::string QuoteSqlIdentifier(const std::string &identifier);

} // namespace duckdb
