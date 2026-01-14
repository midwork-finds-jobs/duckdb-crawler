# DuckDB Crawler Extension - Technical Specification

## 1. Overview

### 1.1 Purpose

The DuckDB Crawler extension provides SQL-native web crawling capabilities directly within DuckDB. It enables crawling URLs while automatically respecting robots.txt directives, rate limits, and implementing intelligent retry logic with domain-level blocking.

### 1.2 Design Goals

1. **Politeness**: Respect website owners through robots.txt compliance and rate limiting
2. **Robustness**: Handle transient errors (429, 5XX, timeouts) with intelligent backoff
3. **Efficiency**: Support parallel requests where allowed, cache sitemap discovery
4. **SQL-Native**: Integrate seamlessly with DuckDB's query engine
5. **Graceful Degradation**: Handle interrupts and errors without data loss

### 1.3 Non-Goals

- JavaScript rendering (static HTML only)
- Cookie/session management
- Form submission
- Authentication flows
- Distributed crawling

---

## 2. Architecture

### 2.1 Components

```
┌─────────────────────────────────────────────────────────────────┐
│                        DuckDB Engine                            │
├─────────────────────────────────────────────────────────────────┤
│                    Parser Extension                             │
│              (CRAWL / CRAWL SITES syntax)                       │
├─────────────────────────────────────────────────────────────────┤
│                    Table Functions                              │
│         crawl_urls()    crawl_into_internal()                   │
├─────────────────────────────────────────────────────────────────┤
│     Robots Parser    │    Sitemap Parser    │    HTTP Client    │
├─────────────────────────────────────────────────────────────────┤
│                  http_request Extension                         │
│                    (Community Extension)                        │
└─────────────────────────────────────────────────────────────────┘
```

### 2.2 Files

| File | Purpose |
|------|---------|
| `crawler_extension.cpp` | Extension entry point, registers functions and parser |
| `crawler_function.cpp` | Main crawl logic, `crawl_urls()` and `crawl_into_internal()` |
| `crawl_parser.cpp` | CRAWL statement parser |
| `http_client.cpp` | HTTP fetch wrapper using http_request extension |
| `robots_parser.cpp` | robots.txt parsing and rule evaluation |
| `sitemap_parser.cpp` | XML sitemap and sitemap index parsing |

### 2.3 Dependencies

- **http_request**: Community extension for HTTP GET requests (auto-installed on extension load)

---

## 3. SQL Interface

### 3.1 CRAWL Statement

Custom SQL syntax for crawling URLs into a target table.

#### 3.1.1 CRAWL (URL Mode)

Crawl specific URLs directly.

```sql
CRAWL (SELECT url FROM my_urls) INTO crawl_results
WITH (
    user_agent 'MyBot/1.0 (+https://example.com/bot)',
    default_crawl_delay 1.0,
    min_crawl_delay 0.5,
    max_crawl_delay 30.0
);
```

#### 3.1.2 CRAWL SITES (Sitemap Discovery Mode)

Discover sitemaps from hostnames, then crawl all URLs found.

```sql
CRAWL SITES (SELECT 'example.com') INTO crawl_results
WHERE url LIKE '%/product/%'
WITH (
    user_agent 'MyBot/1.0',
    sitemap_cache_hours 24,
    update_stale true
);
```

### 3.2 Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `user_agent` | VARCHAR | **required** | User-Agent header, used for robots.txt matching |
| `default_crawl_delay` | DOUBLE | 1.0 | Delay in seconds when robots.txt has no Crawl-delay |
| `min_crawl_delay` | DOUBLE | 0.0 | Minimum delay, overrides robots.txt if lower |
| `max_crawl_delay` | DOUBLE | 60.0 | Maximum delay, caps robots.txt if higher |
| `timeout_seconds` | INTEGER | 30 | HTTP request timeout |
| `respect_robots_txt` | BOOLEAN | true | Parse and enforce robots.txt rules |
| `log_skipped` | BOOLEAN | true | Insert rows for skipped URLs (robots.txt disallow) |
| `url_filter` | VARCHAR | "" | SQL LIKE pattern for filtering URLs (SITES mode only) |
| `sitemap_cache_hours` | DOUBLE | 24.0 | Hours to cache sitemap discovery results |
| `update_stale` | BOOLEAN | false | Re-crawl existing URLs if sitemap indicates staleness |
| `max_retry_backoff_seconds` | INTEGER | 600 | Maximum Fibonacci backoff for retryable errors |
| `max_parallel_per_domain` | INTEGER | 8 | Max concurrent requests per domain (no crawl-delay) |

### 3.3 crawl_urls() Table Function

Lower-level function for direct URL crawling.

```sql
SELECT * FROM crawl_urls(
    ['https://example.com/page1', 'https://example.com/page2'],
    user_agent := 'MyBot/1.0',
    default_crawl_delay := 1.0
);
```

### 3.4 Output Schema

| Column | Type | Description |
|--------|------|-------------|
| `url` | VARCHAR | Crawled URL |
| `domain` | VARCHAR | Domain extracted from URL |
| `http_status` | INTEGER | HTTP status code, or -1 for robots.txt disallow |
| `body` | VARCHAR | Response body (HTML/text content) |
| `content_type` | VARCHAR | Content-Type header value |
| `elapsed_ms` | BIGINT | Request duration in milliseconds |
| `crawled_at` | TIMESTAMP | Timestamp when URL was crawled |
| `error` | VARCHAR | Error message if request failed |

---

## 4. robots.txt Handling

### 4.1 Parsing

The parser extracts:

1. **User-agent blocks**: Rules specific to user-agents
2. **Crawl-delay directive**: Per-agent rate limit
3. **Allow/Disallow rules**: Path-based access control
4. **Sitemap directives**: URLs to sitemaps (global, not per user-agent)

### 4.2 User-Agent Matching

Priority order:

1. Exact match (case-insensitive)
2. Prefix match (e.g., "MyBot" matches "MyBot/1.0")
3. Wildcard match (`User-agent: *`)
4. No rules found = allow all

### 4.3 Path Matching

```cpp
// Allow rules checked first (more specific wins)
for (const auto &allow : rules.allow) {
    if (path.find(allow) == 0) return true;
}

// Then Disallow rules
for (const auto &disallow : rules.disallow) {
    if (path.find(disallow) == 0) return false;
}

// Default: allowed
return true;
```

### 4.4 Crawl-Delay Enforcement

```cpp
crawl_delay = robots.txt value OR default_crawl_delay
crawl_delay = max(crawl_delay, min_crawl_delay)
crawl_delay = min(crawl_delay, max_crawl_delay)
```

### 4.5 has_crawl_delay Detection

If robots.txt specifies a `Crawl-delay` directive for the matched user-agent, the domain is marked as `has_crawl_delay = true`. This affects parallel request handling (see Section 7).

---

## 5. Sitemap Discovery (CRAWL SITES Mode)

### 5.1 Discovery Process

1. **Check cache**: Look for valid cached URLs in `_crawl_sitemap_cache`
2. **Fetch robots.txt**: Extract `Sitemap:` directives
3. **Bruteforce common paths** (if no sitemaps in robots.txt):
   - `/sitemap.xml`
   - `/sitemap_index.xml`
   - `/sitemap-index.xml`
   - `/sitemapindex.xml`
   - `/sitemap/sitemap.xml`
   - `/sitemaps/sitemap.xml`
   - `/sitemap1.xml`, `/sitemap-1.xml`
   - `/post-sitemap.xml`, `/page-sitemap.xml`
   - `/product-sitemap.xml`, `/category-sitemap.xml`
   - `/wp-sitemap.xml`
4. **Parse sitemaps recursively**: Handle sitemap indexes

### 5.2 Sitemap Cache Table

Automatically created: `_crawl_sitemap_cache`

```sql
CREATE TABLE IF NOT EXISTS _crawl_sitemap_cache (
    hostname VARCHAR,
    url VARCHAR,
    lastmod TIMESTAMP,
    changefreq VARCHAR,
    priority DOUBLE,
    discovered_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (hostname, url)
);
```

### 5.3 Cache Invalidation

Cache entries older than `sitemap_cache_hours` are ignored and sitemaps are re-discovered.

### 5.4 URL Filtering

When `url_filter` is specified (e.g., `%/product/%`), only matching URLs from sitemaps are crawled. Uses SQL LIKE pattern matching:

- `%` = any characters
- `_` = single character

---

## 6. Staleness Detection (update_stale Mode)

### 6.1 Purpose

Re-crawl existing URLs when sitemap metadata indicates content has changed.

### 6.2 Staleness Criteria

A URL is considered stale if ANY of these conditions are met:

1. **lastmod > crawled_at**: Sitemap's `<lastmod>` is newer than last crawl time
2. **changefreq threshold exceeded**: Time since last crawl exceeds the changefreq interval

### 6.3 changefreq to Hours Mapping

| changefreq | Hours Threshold |
|------------|-----------------|
| always | 0 |
| hourly | 1 |
| daily | 24 |
| weekly | 168 |
| monthly | 720 |
| yearly | 8760 |
| never | 87600 (~10 years) |
| (empty/default) | 168 (weekly) |

### 6.4 Processing Order

1. **New URLs first**: URLs not in target table (INSERT)
2. **Stale URLs second**: Existing URLs marked stale (UPDATE)

---

## 7. Rate Limiting and Parallel Requests

### 7.1 Domain-Level Tracking

Each domain maintains state:

```cpp
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
    bool has_crawl_delay = false;

    // Parallel tracking
    int active_requests = 0;
};
```

### 7.2 Serial vs Parallel Mode

| Condition | Mode | Behavior |
|-----------|------|----------|
| robots.txt has Crawl-delay | Serial | One request at a time, wait `crawl_delay` between requests |
| No Crawl-delay specified | Parallel | Up to `max_parallel_per_domain` concurrent requests |

### 7.3 Parallel Request Limiting

For domains without crawl-delay:

```cpp
if (domain_state.active_requests >= max_parallel_per_domain) {
    url_queue.push_back(entry);  // Re-queue, try later
    continue;
}
domain_state.active_requests++;
// ... make request ...
domain_state.active_requests--;
```

---

## 8. Error Handling and Retry Logic

### 8.1 Retryable Errors

| Error Type | Handling |
|------------|----------|
| 429 Too Many Requests | Domain-level blocking with Fibonacci backoff |
| 500-504 Server Errors | Domain-level blocking with Fibonacci backoff |
| Network errors (status <= 0) | Domain-level blocking with Fibonacci backoff |
| 4XX Client Errors (except 429) | Not retried, inserted as failed |

### 8.2 Fibonacci Backoff Algorithm

Sequence starts at 3 seconds: **3, 3, 6, 9, 15, 24, 39, 63, 102, 165, 267...**

```cpp
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
```

### 8.3 Retry-After Header

If 429 response includes `Retry-After` header with seconds value, that value is used instead of Fibonacci backoff.

### 8.4 Domain Blocking

When a retryable error occurs:

1. Increment `consecutive_429s` counter
2. Calculate backoff: `FibonacciBackoffSeconds(consecutive_429s, max_retry_backoff_seconds)`
3. Set `blocked_until = now + backoff_seconds`
4. **All URLs from that domain** are blocked until `blocked_until`
5. On success, reset `consecutive_429s = 0`

### 8.5 URL Queue with Retry Tracking

```cpp
struct UrlQueueEntry {
    std::string url;
    int retry_count;       // Per-URL retry counter
    bool is_update;        // True if UPDATE vs INSERT
};
```

### 8.6 Per-URL Retry Limit

- Maximum 5 retries per URL
- After 5 retries, URL is skipped (not inserted/updated)
- When only blocked URLs remain in queue, wait for shortest block to expire (max 60s at a time)

---

## 9. HTTP Client

### 9.1 Request Flow

```
HttpClient::Fetch()
    └── ExecuteHttpGet()
        └── http_get() from http_request extension
```

### 9.2 Single-Attempt Design

The HTTP client makes a **single attempt** per call. All retry logic is handled at the crawler level with domain-level blocking.

```cpp
HttpResponse HttpClient::Fetch(...) {
    // Single attempt - crawler handles all retries
    return ExecuteHttpGet(db, url, user_agent);
}
```

### 9.3 Response Headers Captured

- `status`: HTTP status code
- `body`: Response body (decoded)
- `content_type`: Content-Type header
- `retry-after`: For 429 handling
- `Date`: Server timestamp for crawled_at

---

## 10. Timestamp Handling

### 10.1 Server Date Validation

The server's `Date` header is used for `crawled_at` if:

1. Header is present and parseable (HTTP date format)
2. Server time is within 15 minutes of local time

```cpp
static std::string ParseAndValidateServerDate(const std::string &server_date) {
    // ... parse HTTP date format ...

    double diff_seconds = difftime(server_time, now_time_t);
    if (std::abs(diff_seconds) > 15 * 60) {
        return "";  // Server clock is off, don't trust it
    }

    return formatted_timestamp;
}
```

### 10.2 Fallback

If server date is missing or invalid, `NOW()` is used.

---

## 11. Signal Handling

### 11.1 Graceful Shutdown

- Single Ctrl+C: Sets `g_shutdown_requested = true`, crawler finishes current request then stops
- Double Ctrl+C within 3 seconds: Immediate `exit(1)`

### 11.2 Implementation

```cpp
static std::atomic<bool> g_shutdown_requested(false);
static std::atomic<int> g_sigint_count(0);

static void SignalHandler(int signum) {
    if (signum == SIGINT) {
        g_sigint_count++;
        if (g_sigint_count >= 2 && elapsed < 3 seconds) {
            std::exit(1);  // Force exit
        }
        g_shutdown_requested = true;
    }
}
```

---

## 12. Data Flow

### 12.1 CRAWL SITES Mode Flow

```
1. Parse CRAWL SITES statement
2. Execute source query (get hostnames)
3. For each hostname:
   a. Check sitemap cache
   b. If cache miss: discover sitemaps
   c. Parse sitemaps recursively
   d. Cache discovered URLs with metadata
4. Filter URLs by url_filter pattern
5. Check existing URLs in target table
6. Build queue: new URLs first, then stale URLs
7. Process queue with rate limiting and retry logic
8. INSERT new rows, UPDATE stale rows
9. Return row count
```

### 12.2 Queue Processing Loop

```cpp
while (!url_queue.empty() && !g_shutdown_requested) {
    entry = url_queue.pop_front();
    domain = ExtractDomain(entry.url);

    // Check domain block
    if (domain.blocked_until > now) {
        if (entry.retry_count < 5) {
            entry.retry_count++;
            url_queue.push_back(entry);  // Re-queue at end
        }
        continue;
    }

    // Check parallel limit (if no crawl-delay)
    if (!domain.has_crawl_delay) {
        if (domain.active_requests >= max_parallel) {
            url_queue.push_back(entry);
            continue;
        }
    }

    // Enforce serial rate limit (if has crawl-delay)
    if (domain.has_crawl_delay) {
        wait_for_crawl_delay();
    }

    // Fetch URL
    response = HttpClient::Fetch(url);

    // Handle retryable errors
    if (is_retryable(response)) {
        backoff = FibonacciBackoffSeconds(++domain.consecutive_429s, max_backoff);
        domain.blocked_until = now + backoff;
        if (entry.retry_count < 5) {
            url_queue.push_back(entry);
        }
        continue;
    }

    // Success - reset error count
    domain.consecutive_429s = 0;

    // INSERT or UPDATE result
    if (entry.is_update) {
        UPDATE target SET ... WHERE url = entry.url;
    } else {
        INSERT INTO target VALUES (...);
    }
}
```

---

## 13. Limitations and Known Issues

### 13.1 Current Limitations

1. **Single-threaded**: Only one thread processes URLs (mutex-protected)
2. **No JavaScript**: Static HTML only, no client-side rendering
3. **HTTPS only**: Sitemap discovery assumes HTTPS
4. **No authentication**: No cookie, session, or auth header support
5. **Basic LIKE matching**: URL filter uses simple pattern matching, not full SQL LIKE semantics
6. **No redirect following**: HTTP redirects are not automatically followed (handled by http_request)

### 13.2 Potential Improvements

1. Multi-threaded processing with proper domain-level locking
2. Configurable redirect policy
3. Request body POST support
4. Custom header injection
5. Connection pooling
6. Resume from interruption (persist queue state)
7. Progress reporting callbacks

---

## 14. Usage Examples

### 14.1 Basic URL Crawling

```sql
-- Create target table
CREATE TABLE crawl_results (
    url VARCHAR PRIMARY KEY,
    domain VARCHAR,
    http_status INTEGER,
    body VARCHAR,
    content_type VARCHAR,
    elapsed_ms BIGINT,
    crawled_at TIMESTAMP,
    error VARCHAR
);

-- Crawl specific URLs
CRAWL (SELECT 'https://example.com/page1' UNION SELECT 'https://example.com/page2')
INTO crawl_results
WITH (user_agent 'MyBot/1.0');
```

### 14.2 Site-Wide Crawling with Sitemaps

```sql
-- Crawl all product pages from multiple sites
CRAWL SITES (SELECT hostname FROM sites_to_crawl)
INTO product_pages
WHERE url LIKE '%/product/%'
WITH (
    user_agent 'MyProductBot/1.0 (+https://example.com/bot)',
    sitemap_cache_hours 12,
    max_parallel_per_domain 4
);
```

### 14.3 Incremental Updates

```sql
-- Only re-crawl pages that have changed
CRAWL SITES (SELECT 'example.com')
INTO crawl_results
WITH (
    user_agent 'MyBot/1.0',
    update_stale true,
    sitemap_cache_hours 1
);
```

### 14.4 Conservative Crawling (Rate Limited Sites)

```sql
-- Slow, polite crawling for sensitive sites
CRAWL (SELECT url FROM urls_to_crawl)
INTO results
WITH (
    user_agent 'PoliteBot/1.0',
    default_crawl_delay 5.0,
    min_crawl_delay 3.0,
    max_retry_backoff_seconds 1800,
    max_parallel_per_domain 1
);
```

---

## 15. Configuration Reference

### 15.1 Extension Settings

Set via DuckDB configuration:

```sql
SET crawler_user_agent = 'MyBot/1.0';
SET crawler_default_delay = 2.0;
```

### 15.2 Per-Query Options

Via `WITH (...)` clause in CRAWL statement or named parameters in `crawl_urls()`.

### 15.3 Default Values Summary

| Setting | Default |
|---------|---------|
| default_crawl_delay | 1.0 seconds |
| min_crawl_delay | 0.0 seconds |
| max_crawl_delay | 60.0 seconds |
| timeout_seconds | 30 seconds |
| respect_robots_txt | true |
| log_skipped | true |
| sitemap_cache_hours | 24.0 hours |
| update_stale | false |
| max_retry_backoff_seconds | 600 seconds (10 minutes) |
| max_parallel_per_domain | 8 |
| max retries per URL | 5 (hardcoded) |

---

## 16. Appendix

### 16.1 Fibonacci Backoff Sequence

| Attempt | Backoff (seconds) |
|---------|-------------------|
| 1 | 3 |
| 2 | 3 |
| 3 | 6 |
| 4 | 9 |
| 5 | 15 |
| 6 | 24 |
| 7 | 39 |
| 8 | 63 |
| 9 | 102 |
| 10 | 165 |
| 11 | 267 |
| 12+ | capped at max_retry_backoff_seconds |

### 16.2 HTTP Status Code Handling

| Status | Behavior |
|--------|----------|
| 200-299 | Success, insert/update result |
| -1 | robots.txt disallow (synthetic status) |
| 0 | Network error, retryable |
| 400-428 | Client error, not retried |
| 429 | Too Many Requests, domain blocked with backoff |
| 430-499 | Client error, not retried |
| 500-504 | Server error, domain blocked with backoff |
| 505+ | Server error, not retried |

### 16.3 Common Sitemap Paths Checked

1. `/sitemap.xml`
2. `/sitemap_index.xml`
3. `/sitemap-index.xml`
4. `/sitemapindex.xml`
5. `/sitemap/sitemap.xml`
6. `/sitemaps/sitemap.xml`
7. `/sitemap1.xml`
8. `/sitemap-1.xml`
9. `/post-sitemap.xml`
10. `/page-sitemap.xml`
11. `/product-sitemap.xml`
12. `/category-sitemap.xml`
13. `/wp-sitemap.xml`
