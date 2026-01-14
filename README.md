# DuckDB Crawler Extension

A DuckDB extension for web crawling with automatic rate limiting, robots.txt compliance, and sitemap discovery.

## Features

- **CRAWL SQL syntax** - Native non-blocking SQL command for crawling
- **Automatic discovery** - Sitemap detection + link-following fallback
- **robots.txt compliance** - Crawl-delay, Request-rate, Disallow/Allow rules
- **Adaptive rate limiting** - Adjusts delay based on server response times
- **Parallel sitemap discovery** - Fast site enumeration
- **Conditional requests** - ETag/Last-Modified support for efficient re-crawling
- **Content deduplication** - SHA-256 content hash
- **SURT key normalization** - Common Crawl compatible URL keys
- **Sitemap caching** - SURT-indexed cache for fast domain lookups
- **Progress bar** - Built-in DuckDB progress tracking
- **Graceful shutdown** - Ctrl+C or `STOP CRAWL` stops cleanly

## Installation

```sql
INSTALL crawler FROM community;
LOAD crawler;
```

## Usage

### CRAWL - Site Discovery & Crawling

```sql
-- Discover URLs from sitemap and crawl
CRAWL (SELECT 'example.com')
INTO crawl_results
WITH (user_agent 'MyBot/1.0 (+https://example.com/bot)');

-- Filter discovered URLs with LIKE pattern
CRAWL (SELECT hostname FROM sites)
INTO products
WHERE url LIKE '%/product/%'
WITH (
    user_agent 'ProductBot/1.0',
    sitemap_cache_hours 48
);
```

### Flexible Input Formats

CRAWL accepts various input formats:

```sql
CRAWL (
    SELECT input FROM (VALUES
        -- Direct sitemap URL - fetched directly
        ('https://example.com/sitemap.xml'),

        -- Hostname only - robots.txt → bruteforce sitemaps → spider
        ('example.com'),

        -- Hostname with path - starts spider from /blog/ if no sitemap
        ('example.com/blog/'),

        -- Page URL - robots.txt → sitemaps → spider from this page
        ('https://example.com/category/shoes?page=1')
    ) t(input)
)
INTO results
WITH (user_agent 'MyBot/1.0', follow_links true);
```

### Link-Following Fallback

When a site has no sitemap, enable link-based discovery:

```sql
-- Crawl site by following links (no sitemap needed)
CRAWL (SELECT 'example.com')
INTO all_pages
WITH (
    user_agent 'MyBot/1.0',
    follow_links true,
    max_crawl_pages 1000,
    max_crawl_depth 5
);

-- Start from specific path
CRAWL (SELECT 'example.com/blog/')
INTO blog_posts
WITH (
    user_agent 'BlogCrawler/1.0',
    follow_links true,
    allow_subdomains false
);
```

### STOP CRAWL - Cancel Running Crawl

```sql
-- Stop a crawl in progress
STOP CRAWL INTO crawl_results;
```

### Full Example with Options

```sql
CRAWL (SELECT 'shop.example.com')
INTO product_pages
WHERE url LIKE '%/products/%'
WITH (
    user_agent 'ShopCrawler/1.0 (+https://mysite.com/bot)',
    default_crawl_delay 0.5,
    max_crawl_delay 10.0,
    timeout_seconds 30,
    max_parallel_per_domain 4,
    accept_content_types 'text/html',
    compress true
);
```

## Output Schema

The target table is created automatically with this schema:

| Column | Type | Description |
|--------|------|-------------|
| url | VARCHAR | Crawled URL |
| surt_key | VARCHAR | SURT-normalized URL key (Common Crawl format) |
| http_status | INTEGER | HTTP status code (200, 404, etc.) or -1 for disallowed |
| body | VARCHAR | Response body |
| content_type | VARCHAR | Content-Type header |
| elapsed_ms | BIGINT | Request time in milliseconds |
| crawled_at | TIMESTAMP | When the URL was crawled |
| error | VARCHAR | Error message if failed |
| error_type | VARCHAR | Classified error (network_timeout, http_rate_limited, etc.) |
| etag | VARCHAR | ETag header for conditional requests |
| last_modified | VARCHAR | Last-Modified header |
| content_hash | VARCHAR | SHA-256 hash of response body |

## Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| user_agent | VARCHAR | **required** | User-Agent header (used for robots.txt matching) |
| default_crawl_delay | DOUBLE | 1.0 | Default delay between requests (seconds) |
| min_crawl_delay | DOUBLE | 0.0 | Minimum delay floor |
| max_crawl_delay | DOUBLE | 60.0 | Maximum delay cap |
| timeout_seconds | INTEGER | 30 | HTTP request timeout |
| respect_robots_txt | BOOLEAN | true | Parse and respect robots.txt |
| log_skipped | BOOLEAN | true | Include skipped URLs in output |
| sitemap_cache_hours | DOUBLE | 24.0 | Hours to cache sitemap discovery |
| update_stale | BOOLEAN | false | Re-crawl URLs with newer lastmod in sitemap |
| max_retry_backoff_seconds | INTEGER | 600 | Max Fibonacci backoff for 429/5XX (10 min) |
| max_parallel_per_domain | INTEGER | 8 | Max concurrent requests per domain |
| max_total_connections | INTEGER | 32 | Global max concurrent connections |
| max_response_bytes | BIGINT | 10485760 | Max response size (10MB) |
| compress | BOOLEAN | true | Request gzip/deflate compression |
| accept_content_types | VARCHAR | '' | Only accept these types (comma-separated, e.g., 'text/html,text/*') |
| reject_content_types | VARCHAR | '' | Reject these types |
| follow_links | BOOLEAN | false | Enable link-based crawling when sitemap not found |
| allow_subdomains | BOOLEAN | false | Follow links to subdomains |
| max_crawl_pages | INTEGER | 10000 | Max pages to discover via link following |
| max_crawl_depth | INTEGER | 10 | Max link depth from start URL |
| respect_nofollow | BOOLEAN | true | Skip rel="nofollow" links |
| follow_canonical | BOOLEAN | true | Follow canonical URLs |

## Auxiliary Tables

The extension creates helper tables automatically:

- `_crawl_sitemap_cache` - Cached sitemap URLs indexed by SURT key for fast domain lookups
- `_crawl_sitemap_discovery_status` - Discovery method per domain (sitemap/link_crawl/not_found)

### Query Sitemap Cache

```sql
-- View cached URLs for a domain using SURT prefix
SELECT url, lastmod FROM _crawl_sitemap_cache
WHERE surt_key LIKE 'com,example)%'
ORDER BY lastmod DESC;
```

## robots.txt Compliance

The extension automatically:
1. Fetches and caches robots.txt for each domain
2. Parses Crawl-delay and Request-rate directives
3. Respects Disallow/Allow rules
4. Falls back to `User-agent: *` if no specific match
5. Discovers sitemaps from Sitemap: directives

## Error Classification

Errors are classified into types for analytics:

| Error Type | Description |
|------------|-------------|
| network_timeout | Connection or read timeout |
| network_dns_failure | DNS resolution failed |
| network_connection_refused | Connection refused |
| network_ssl_error | SSL/TLS error |
| http_client_error | 4XX status codes |
| http_server_error | 5XX status codes |
| http_rate_limited | 429 Too Many Requests |
| robots_disallowed | Blocked by robots.txt |
| content_too_large | Response exceeds max_response_bytes |
| content_type_rejected | Content-Type not accepted |

## Building from Source

```bash
git clone --recursive https://github.com/midwork-finds-jobs/duckdb-crawl.git
cd duckdb-crawl
make release GEN=ninja
```

## Testing

```bash
./build/release/duckdb -unsigned -c "
LOAD 'build/release/extension/crawler/crawler.duckdb_extension';
LOAD http_request;

CRAWL (SELECT 'httpbin.org')
INTO test_results
WITH (user_agent 'TestBot/1.0', follow_links true, max_crawl_pages 5);

SELECT url, http_status, length(body) as body_len FROM test_results;
"
```

## Dependencies

- Requires the `http_request` extension for HTTP requests

## License

MIT
