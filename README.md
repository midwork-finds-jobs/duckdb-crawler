# DuckDB Crawler Extension

A DuckDB extension for crawling URLs with automatic rate limiting based on robots.txt crawl-delay directives.

## Features

- Automatic robots.txt parsing and compliance
- Per-domain rate limiting based on Crawl-delay directive
- Disallow rule enforcement
- Graceful SIGINT (Ctrl+C) handling
- Configurable retry with exponential backoff

## Usage

```sql
LOAD 'crawler';

-- Basic crawl
SELECT * FROM crawl_urls(
    ['https://example.com/page1', 'https://example.com/page2'],
    user_agent := 'MyBot/1.0 (+https://example.com/bot)'
);

-- With options
SELECT * FROM crawl_urls(
    ['https://example.com/page1', 'https://example.com/page2'],
    user_agent := 'MyBot/1.0',
    default_crawl_delay := 1.0,
    min_crawl_delay := 0.5,
    max_crawl_delay := 30.0
);

-- Insert results into table
INSERT INTO crawl_results
SELECT * FROM crawl_urls(
    ['https://example.com/page1'],
    user_agent := 'MyBot/1.0'
);
```

## Output Schema

| Column | Type | Description |
|--------|------|-------------|
| url | VARCHAR | Crawled URL |
| domain | VARCHAR | Domain extracted from URL |
| http_status | INTEGER | HTTP status code (200, 404, etc.) or -1 for robots.txt disallow |
| body | VARCHAR | Response body (HTML) |
| content_type | VARCHAR | Content-Type header |
| elapsed_ms | BIGINT | Request time in milliseconds |
| crawled_at | TIMESTAMP | When the URL was crawled |
| error | VARCHAR | Error message if any |

## Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| user_agent | VARCHAR | **required** | User-Agent header (used for robots.txt matching) |
| default_crawl_delay | DOUBLE | 1.0 | Default delay between requests if not in robots.txt |
| min_crawl_delay | DOUBLE | 0.0 | Minimum delay even if robots.txt says 0 |
| max_crawl_delay | DOUBLE | 60.0 | Maximum delay (cap on robots.txt value) |
| timeout_seconds | INTEGER | 30 | HTTP request timeout |
| respect_robots_txt | BOOLEAN | true | Parse and respect robots.txt |
| log_skipped | BOOLEAN | true | Include skipped URLs in output |

## robots.txt Compliance

The extension automatically:
1. Fetches robots.txt for each domain on first request
2. Parses Crawl-delay directive for the configured user_agent
3. Respects Disallow rules (skipped URLs have status -1)
4. Falls back to `User-agent: *` if no specific match

## Building

```bash
git submodule update --init --recursive
make release
```

## Testing

```bash
duckdb -unsigned -c "
LOAD 'build/release/extension/crawler/crawler.duckdb_extension';
SELECT * FROM crawl_urls(['https://httpbin.org/html'], user_agent := 'TestBot/1.0');
"
```

## License

MIT
