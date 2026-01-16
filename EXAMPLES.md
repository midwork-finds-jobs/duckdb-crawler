# DuckDB Crawler Extension - Examples

## Basic Usage

### Crawl a website

```sql
-- Load extension (if not auto-loaded)
LOAD 'build/release/extension/crawler/crawler.duckdb_extension';

-- Crawl a site into a table
CRAWL (SELECT 'https://example.com/') INTO pages
WITH (user_agent 'MyBot/1.0', max_crawl_pages 100);

-- View results
SELECT url, http_status, content_type FROM pages LIMIT 10;
```

### Crawl localhost for testing

```sql
CRAWL (SELECT 'http://localhost:8080/test.html') INTO test_pages
WITH (user_agent 'TestBot/1.0', max_crawl_pages 1);
```

## Result Table Schema

The crawler creates a table with these columns:

| Column | Type | Description |
|--------|------|-------------|
| `url` | VARCHAR | Original URL (primary key) |
| `surt_key` | VARCHAR | SURT-formatted URL for sorting |
| `http_status` | INTEGER | HTTP status code (200, 404, etc.) |
| `body` | VARCHAR | Page HTML content |
| `content_type` | VARCHAR | MIME type (text/html, etc.) |
| `elapsed_ms` | BIGINT | Request time in milliseconds |
| `crawled_at` | TIMESTAMP | When page was fetched |
| `error` | VARCHAR | Error message if failed |
| `etag` | VARCHAR | HTTP ETag header |
| `last_modified` | VARCHAR | HTTP Last-Modified header |
| `content_hash` | VARCHAR | MD5 hash of body |
| `error_type` | VARCHAR | Error classification |
| `final_url` | VARCHAR | URL after redirects |
| `redirect_count` | INTEGER | Number of redirects |
| `jsonld` | VARCHAR | JSON-LD structured data |
| `opengraph` | VARCHAR | OpenGraph meta tags |
| `meta` | VARCHAR | HTML meta tags |
| `hydration` | VARCHAR | React/Next.js hydration data |
| `js` | VARCHAR | JavaScript variables |

## WITH Clause Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `user_agent` | string | **required** | Bot user agent string |
| `max_crawl_pages` | int | 1000 | Max pages to crawl |
| `max_crawl_depth` | int | 10 | Max link depth from start |
| `default_crawl_delay` | float | 1.0 | Seconds between requests |
| `min_crawl_delay` | float | 0.5 | Minimum delay (adaptive) |
| `max_crawl_delay` | float | 30.0 | Maximum delay (adaptive) |
| `timeout_seconds` | int | 30 | HTTP request timeout |
| `respect_robots_txt` | bool | true | Obey robots.txt rules |
| `follow_links` | bool | true | Spider discovered links |
| `allow_subdomains` | bool | false | Crawl subdomains |
| `max_parallel_per_domain` | int | 4 | Concurrent requests/domain |
| `max_total_connections` | int | 16 | Total concurrent requests |
| `max_response_bytes` | int | 10MB | Max response size |
| `compress` | bool | true | Accept gzip responses |
| `accept_content_types` | string | "" | Filter by content type |
| `reject_content_types` | string | "" | Reject content types |
| `threads` | int | auto | Worker thread count |

## Extracting Structured Data

### JSON-LD (Schema.org)

```sql
-- Get Product schema from e-commerce pages
SELECT
    url,
    jsonld::JSON->'@type' as schema_type,
    jsonld::JSON->'name' as product_name,
    jsonld::JSON->'offers'->'price' as price
FROM pages
WHERE jsonld IS NOT NULL
  AND jsonld != '';
```

### OpenGraph Meta Tags

```sql
-- Extract social sharing metadata
SELECT
    url,
    opengraph::JSON->>'og:title' as title,
    opengraph::JSON->>'og:description' as description,
    opengraph::JSON->>'og:image' as image
FROM pages
WHERE opengraph IS NOT NULL;
```

### JavaScript Variables

The `js` column extracts top-level JS variable assignments:

```sql
-- Extract JS variables from pages
SELECT
    url,
    js::JSON->>'__INITIAL_STATE__' as initial_state,
    js::JSON->>'productData' as product_data
FROM pages
WHERE js IS NOT NULL AND js != '';
```

Supported patterns:
- `var name = {...}` / `let name = {...}` / `const name = {...}`
- `window.name = {...}`
- `JSON.parse('[...]')` with hex (`\x22`) and unicode (`\uNNNN`) escapes

### Hydration Data (React/Next.js)

```sql
-- Extract Next.js page props
SELECT
    url,
    hydration::JSON->'__NEXT_DATA__'->'props' as page_props
FROM pages
WHERE hydration IS NOT NULL;
```

## Working with JSON Arrays

### Unnest JSON arrays

```sql
-- Extract items from a JSON array in js column
SELECT
    p.url,
    item->>'title' as title,
    item->>'price' as price
FROM pages p,
LATERAL unnest(
    CASE
        WHEN js::JSON->'products' IS NOT NULL
        THEN from_json(js::JSON->>'products', '["JSON"]')
        ELSE []
    END
) as t(item)
WHERE js IS NOT NULL;
```

### Parse JSON array directly

```sql
-- If js contains an array like: {"items": [{"a":1},{"a":2}]}
WITH parsed AS (
    SELECT
        url,
        json_extract(js::JSON, '$.items') as items_json
    FROM pages
    WHERE js IS NOT NULL
)
SELECT
    url,
    unnest(from_json(items_json::VARCHAR, '["JSON"]'))->>'a' as a_value
FROM parsed
WHERE items_json IS NOT NULL;
```

## Filtering and Analysis

### Find pages with errors

```sql
SELECT url, http_status, error, error_type
FROM pages
WHERE http_status >= 400 OR error IS NOT NULL
ORDER BY http_status DESC;
```

### Analyze content types

```sql
SELECT content_type, COUNT(*) as count
FROM pages
GROUP BY content_type
ORDER BY count DESC;
```

### Find slow pages

```sql
SELECT url, elapsed_ms
FROM pages
WHERE elapsed_ms > 5000
ORDER BY elapsed_ms DESC;
```

## Incremental Crawling

### Only crawl new/changed pages

```sql
-- Use update_stale to re-crawl pages with changed ETags
CRAWL (SELECT 'https://example.com/') INTO pages
WITH (user_agent 'MyBot/1.0', update_stale true);
```

### Resume interrupted crawl

```sql
-- The crawler automatically skips already-fetched URLs
-- Just run the same CRAWL command again
CRAWL (SELECT 'https://example.com/') INTO pages
WITH (user_agent 'MyBot/1.0', max_crawl_pages 10000);
```

## Multiple Sites

### Crawl multiple domains

```sql
CRAWL (
    SELECT column0 FROM (
        VALUES
            ('https://site1.com/'),
            ('https://site2.com/'),
            ('https://site3.com/')
    )
) INTO multi_site_pages
WITH (user_agent 'MyBot/1.0', max_crawl_pages 100);
```

### Crawl from a table of URLs

```sql
-- First create a table with URLs
CREATE TABLE urls_to_crawl (url VARCHAR);
INSERT INTO urls_to_crawl VALUES
    ('https://example1.com/page1'),
    ('https://example2.com/page2');

-- Then crawl them
CRAWL (SELECT url FROM urls_to_crawl) INTO crawled_pages
WITH (user_agent 'MyBot/1.0');
```
