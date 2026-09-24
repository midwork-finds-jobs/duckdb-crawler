# DuckDB Crawler Extension Specification

## Vision

SQL-native web crawler with streaming pipelines. Fetch, parse, and query HTML as structured data without blocking, without JavaScript execution, with full crawling standards compliance.

```sql
-- Dream syntax: multi-stage pipeline, no blocking, stops at LIMIT
SELECT html.schema['JobPosting']->>'title', html.schema['JobPosting']->>'baseSalary'
FROM crawl(crawl(['https://example.com/jobs']))
LIMIT 10;
```

---

## Core Principles

### 1. Streaming, Not Blocking

Every stage yields rows immediately. Later stages consume lazily.

```
Stage 1              Stage 2              Stage 3
┌──────────┐         ┌──────────┐         ┌──────────┐
│ Fetch    │──row──▶ │ Fetch    │──row──▶ │ Extract  │──▶ Result
│ Extract  │         │ Extract  │         │ Filter   │
│ Yield    │         │ Yield    │         │ LIMIT    │◀── Backpressure
└──────────┘         └──────────┘         └──────────┘
     ↑                    ↑                    ↑
  Produces            Consumes/           Consumes
  eagerly             Produces            (stops pipeline
                                          when satisfied)
```

**Anti-pattern (blocking):**
```sql
CRAWL ... INTO table1;  -- Blocks until 100% complete
CRAWL ... INTO table2;  -- Only then starts
```

**Target pattern:**
```sql
SELECT * FROM crawl(crawl(...), ...) LIMIT 10;  -- Streams, stops early
```

### 2. SQL with Standard Feel

Crawl results are tables. Extractions are columns. Composition via CTEs/subqueries.

```sql
-- Crawl is a table function
SELECT * FROM crawl(urls, options);

-- Extraction functions become columns
SELECT
    htmlpath(html.document, 'h1@text') as title,
    htmlpath(html.document, '.price@text') as price
FROM crawl(...);

-- Composable with standard SQL
WITH listings AS (
    SELECT * FROM crawl(sitemap('https://shop.com/sitemap.xml'))
    WHERE url LIKE '%/product/%'
)
SELECT * FROM crawl(listings)
WHERE htmlpath(html.document, '.stock@text')::VARCHAR = 'In stock';
```

### 3. HTML as Queryable AST

Parse HTML into structured representation. Query with CSS selectors and path expressions. No JavaScript execution.

```sql
-- CSS selectors
$('div.product h1')              -- Element text
$('a.link', 'attr:href')         -- Attribute value
$('script#data', 'html')         -- Inner HTML

-- Structured data extraction
jsonld.Product.name              -- JSON-LD by @type
og.title                         -- OpenGraph meta
meta.description                 -- Meta tags

-- JavaScript variable extraction (static analysis only, no execution)
js.window.__INITIAL_STATE__      -- Parse <script> AST, extract assignments
js.dataLayer[0]                  -- Array access in static JS
```

### 4. Async HTTP Pipeline

Concurrent requests with connection pooling. Per-domain rate limiting. Retry with backoff.

```
┌─────────────────────────────────────────────────────────────┐
│                    Async HTTP Engine (Rust)                 │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐        │
│  │ Worker  │  │ Worker  │  │ Worker  │  │ Worker  │        │
│  │ (tokio) │  │ (tokio) │  │ (tokio) │  │ (tokio) │        │
│  └────┬────┘  └────┬────┘  └────┬────┘  └────┬────┘        │
│       │            │            │            │              │
│  ┌────▼────────────▼────────────▼────────────▼────┐        │
│  │           Connection Pool (per domain)          │        │
│  │  example.com: [conn1, conn2]                    │        │
│  │  shop.com: [conn1]                              │        │
│  └─────────────────────────────────────────────────┘        │
│                                                             │
│  ┌─────────────────────────────────────────────────┐        │
│  │           Rate Limiter (per domain)             │        │
│  │  example.com: 1 req/sec (from robots.txt)       │        │
│  │  shop.com: 0.2 req/sec (default)                │        │
│  └─────────────────────────────────────────────────┘        │
└─────────────────────────────────────────────────────────────┘
```

### 5. Crawling Standards Compliance

Respect robots.txt. Discover via sitemaps. Honor rate limits.

```sql
-- Automatic robots.txt check (default: on)
SELECT * FROM crawl(urls, respect_robots = true);

-- Sitemap-based discovery
SELECT * FROM sitemap('https://example.com/sitemap.xml');

-- Recursive link following with depth limit
SELECT * FROM crawl(
    ['https://example.com/'],
    follow_links = true,
    max_depth = 3,
    url_pattern = '%/products/%'
);
```

---

## Architecture

### Component Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                         DuckDB Extension (C++)                       │
├─────────────────────────────────────────────────────────────────────┤
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐               │
│  │ crawl()      │  │ sitemap()    │  │ $() scalar   │               │
│  │ table func   │  │ table func   │  │ function     │               │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘               │
│         │                 │                 │                        │
│  ┌──────▼─────────────────▼─────────────────▼───────┐               │
│  │              Extraction Spec Parser              │               │
│  │  "title := $('h1')" → {source:css, selector:h1}  │               │
│  └──────────────────────┬───────────────────────────┘               │
│                         │ JSON                                       │
│  ┌──────────────────────▼───────────────────────────┐               │
│  │              State Manager                        │               │
│  │  - Progress tracking (resume on interrupt)        │               │
│  │  - Extracted value cache                          │               │
│  │  - Deduplication                                  │               │
│  └──────────────────────┬───────────────────────────┘               │
└─────────────────────────┼───────────────────────────────────────────┘
                          │ FFI
┌─────────────────────────▼───────────────────────────────────────────┐
│                         Rust Core                                    │
├─────────────────────────────────────────────────────────────────────┤
│  ┌──────────────────────────────────────────────────┐               │
│  │              Async HTTP Engine                    │               │
│  │  - reqwest + tokio runtime                        │               │
│  │  - Connection pooling                             │               │
│  │  - Concurrent requests (configurable)             │               │
│  │  - Automatic retry with backoff                   │               │
│  └──────────────────────┬───────────────────────────┘               │
│                         │                                            │
│  ┌──────────────────────▼───────────────────────────┐               │
│  │              HTML Parser (scraper/select.rs)      │               │
│  │  - Parse to DOM tree                              │               │
│  │  - CSS selector evaluation                        │               │
│  │  - No JavaScript execution                        │               │
│  └──────────────────────┬───────────────────────────┘               │
│                         │                                            │
│  ┌──────────────────────▼───────────────────────────┐               │
│  │              Extractors                           │               │
│  │  - JSON-LD (by @type)                             │               │
│  │  - OpenGraph                                      │               │
│  │  - Meta tags                                      │               │
│  │  - JS static analysis (swc)                       │               │
│  │  - Microdata                                      │               │
│  └──────────────────────────────────────────────────┘               │
│                                                                      │
│  ┌──────────────────────────────────────────────────┐               │
│  │              Robots.txt Parser                    │               │
│  │  - Fetch and cache per domain                     │               │
│  │  - Crawl-delay extraction                         │               │
│  │  - Allow/Disallow rules                           │               │
│  └──────────────────────────────────────────────────┘               │
│                                                                      │
│  ┌──────────────────────────────────────────────────┐               │
│  │              Sitemap Parser                       │               │
│  │  - XML sitemap                                    │               │
│  │  - Sitemap index                                  │               │
│  │  - lastmod, priority, changefreq                  │               │
│  └──────────────────────────────────────────────────┘               │
└─────────────────────────────────────────────────────────────────────┘
```

### Data Flow

```
User Query                    Extension                      Network
───────────────────────────────────────────────────────────────────────

SELECT ...                   ┌─────────────┐
FROM crawl(                  │ Parse query │
  [...urls...]        ───▶   │ Build plan  │
)                            └──────┬──────┘
                                    │
LIMIT 10;                           ▼
                             ┌─────────────┐
                             │ URL Queue   │◀──── Sitemap/Links
                             │ (priority)  │
                             └──────┬──────┘
                                    │
                             ┌──────▼──────┐     ┌─────────────┐
                             │ Rate Limit  │────▶│ robots.txt  │
                             │ Check       │◀────│ (cached)    │
                             └──────┬──────┘     └─────────────┘
                                    │
                             ┌──────▼──────┐     ┌─────────────┐
                             │ Async HTTP  │────▶│   Server    │
                             │ (Rust)      │◀────│             │
                             └──────┬──────┘     └─────────────┘
                                    │
                             ┌──────▼──────┐
                             │ Parse HTML  │
                             │ (scraper)   │
                             └──────┬──────┘
                                    │
                             ┌──────▼──────┐
                             │ Extract     │
                             │ (CSS,jsonld)│
                             └──────┬──────┘
                                    │
                             ┌──────▼──────┐
                             │ Yield Row   │────▶ Result to DuckDB
                             │ (streaming) │
                             └──────┬──────┘
                                    │
                             ┌──────▼──────┐
                             │ State Save  │────▶ Progress table
                             │ (checkpoint)│
                             └─────────────┘
                                    │
                                    ▼
                             [LIMIT reached?]────▶ Stop pipeline
```

---

## API Reference

### crawl() Table Function

```sql
crawl(
    source,                    -- URLs: list, query string, or table function

    -- HTTP options
    user_agent = 'Bot/1.0',    -- User agent string
    timeout = 30,              -- Request timeout (seconds)
    workers = 4,               -- Concurrent requests

    -- Rate limiting
    crawl_delay = 0.2,         -- Seconds between requests (per domain)
    respect_robots = true,     -- Check robots.txt

    -- Link following
    follow_links = false,      -- Discover URLs from <a> tags
    max_depth = 1,             -- Maximum crawl depth
    url_pattern = '%',         -- SQL LIKE pattern for followed URLs

    -- State management
    state_table = 'name',      -- Table for progress tracking
    skip_cached = true,        -- Skip URLs already in state table
)
```

**Returns:**

| Column | Type | Description |
|--------|------|-------------|
| url | VARCHAR | Requested URL |
| final_url | VARCHAR | After redirects |
| status | INTEGER | HTTP status code |
| content_type | VARCHAR | Response content type |
| body | VARCHAR | Response body |
| response_time_ms | BIGINT | Request duration |
| error | VARCHAR | Error message if failed |
| depth | INTEGER | Crawl depth (if following links) |

### sitemap() Table Function

```sql
sitemap(
    url,                       -- Sitemap URL or site root
    recursive = false,         -- Follow sitemap index (bounded by crawler_timeout_ms)
    filter_pattern = '%',      -- SQL LIKE filter on URLs
)
```

**Returns:**

| Column | Type | Description |
|--------|------|-------------|
| url | VARCHAR | Page URL |
| lastmod | TIMESTAMP | Last modified |
| changefreq | VARCHAR | Change frequency hint |
| priority | DOUBLE | Priority hint (0-1) |

### $() Scalar Function

For use in SELECT on already-fetched HTML:

```sql
SELECT
    $(body, 'h1') as title,
    $(body, 'a.link', 'attr:href') as link
FROM my_pages;
```

---

## Crawling Standards

### robots.txt Compliance

```
1. Fetch robots.txt once per domain (cached)
2. Parse User-agent rules (match our user agent)
3. Check Allow/Disallow before each request
4. Extract Crawl-delay, respect it
5. Find Sitemap directives
```

```sql
-- Automatic (default)
SELECT * FROM crawl(urls, respect_robots = true);

-- Manual check
SELECT * FROM robots_check('https://example.com/page', 'MyBot/1.0');
-- Returns: {allowed: true, crawl_delay: 1.0, sitemaps: [...]}
```

### URL Discovery

**Sitemap-based:**
```sql
-- From sitemap.xml
SELECT url FROM sitemap('https://example.com/sitemap.xml');

-- Auto-discover from robots.txt
SELECT url FROM sitemap('https://example.com/', discover = true);
```

**Link-based:**
```sql
-- Follow links with depth limit
SELECT * FROM crawl(
    ['https://example.com/'],
    follow_links = true,
    max_depth = 3,
    url_pattern = '%example.com%'  -- Stay on domain
);
```

### Rate Limiting

```
Priority order:
1. robots.txt Crawl-delay (if respect_robots = true)
2. Explicit crawl_delay parameter
3. Default: 0.2 seconds

Per-domain enforcement (concurrent requests to same domain respect delay)
```

---

## State Management

### Progress Tracking

```sql
-- Enable with state_table parameter
SELECT * FROM crawl(urls, state_table = 'my_crawl');

-- State table schema (auto-created)
CREATE TABLE my_crawl (
    url VARCHAR PRIMARY KEY,
    http_status INTEGER,
    crawled_at TIMESTAMP DEFAULT current_timestamp,
    etag VARCHAR,
    last_modified VARCHAR
);
```

### Resume After Interrupt

```sql
-- First run (interrupted at 50%)
SELECT * FROM crawl(urls, state_table = 'progress');
-- ^C

-- Resume (skips already-crawled URLs)
SELECT * FROM crawl(urls, state_table = 'progress');
```

### Incremental Updates

```sql
-- Re-crawl only if changed (using ETag/Last-Modified)
SELECT * FROM crawl(
    urls,
    state_table = 'cache',
    conditional = true  -- Send If-None-Match / If-Modified-Since
);
```

---

## Implementation Status

### Phase 1: Core Streaming ✅
- [x] `crawl()` table function
- [x] Rust FFI for HTML parsing
- [x] State table for checkpointing
- [x] Basic streaming (yields rows)

### Phase 2: Rust HTTP ⏳
- [x] Async HTTP in Rust (reqwest + tokio)
- [x] `crawl_batch_ffi()` implemented
- [ ] Wire `crawl()` to use Rust HTTP
- [ ] Connection pooling per domain
- [ ] Per-domain rate limiting in Rust

### Phase 3: Crawling Standards
- [ ] robots.txt parsing and caching
- [ ] Crawl-delay enforcement
- [ ] Sitemap XML parser
- [ ] `sitemap()` table function

### Phase 4: Link Following
- [ ] HTML link extraction
- [ ] URL normalization
- [ ] Depth tracking
- [ ] URL pattern filtering
- [ ] Deduplication

### Phase 5: Advanced Features
- [ ] Conditional requests (ETag, Last-Modified)
- [ ] JavaScript static analysis (swc)
- [ ] Priority queue for URLs
- [ ] Retry with exponential backoff

---

## Non-Goals

- **JavaScript execution**: No headless browser, no JS runtime
- **Rendering**: No CSS layout, no visual extraction
- **Login/Session**: No cookie jar, no form submission
- **Proxy rotation**: Out of scope (use external proxy)
- **Distributed crawling**: Single-node only

---

## Example Workflows

### E-commerce Price Scraping

```sql
-- Get all product URLs from sitemap
WITH product_urls AS (
    SELECT url FROM sitemap('https://shop.com/sitemap.xml')
    WHERE url LIKE '%/product/%'
),
-- Crawl product pages, extract structured data
products AS (
    SELECT
        url,
        html.schema['Product']->>'name' as name,
        (html.schema['Product']->'offers'->>'price')::decimal as price,
        html.schema['Product']->>'sku' as sku
    FROM crawl(product_urls, state_table = 'product_cache')
)
SELECT * FROM products WHERE price < 100;
```

### Job Board Aggregation

```sql
-- Stage 1: Get job listing pages
WITH job_ids AS (
    SELECT
        url,
        UNNEST(from_json(htmlpath(html.document, 'script#jobs-data@text')::json->'$[*].id', '["varchar"]')) as job_id
    FROM crawl(['https://jobs.example.com/listings'])
),
-- Stage 2: Fetch individual job details (streams!)
job_details AS (
    SELECT
        html.schema['JobPosting']->>'title' as title,
        html.schema['JobPosting']->'hiringOrganization'->>'name' as company,
        html.schema['JobPosting']->>'baseSalary' as salary,
        html.schema['JobPosting']->'jobLocation'->>'address' as location
    FROM crawl('SELECT url || ''/job/'' || job_id FROM job_ids')
)
SELECT * FROM job_details LIMIT 100;  -- Stops after 100, doesn't crawl all!
```

### News Monitoring

```sql
-- Continuously check news sites (with caching)
SELECT
    url,
    html.schema['NewsArticle']->>'headline' as headline,
    html.schema['NewsArticle']->>'datePublished' as published
FROM crawl(
    sitemap('https://news.example.com/sitemap-news.xml'),
    state_table = 'news_cache',
    conditional = true  -- Only re-fetch if changed
)
WHERE (html.schema['NewsArticle']->>'datePublished')::timestamp > now() - interval '1 day';
```

### Multi-Site Aggregation

```sql
-- Crawl multiple sites with different extraction selectors
WITH sites AS (
    SELECT * FROM (VALUES
        ('https://site1.com/products', '.price@text', 'h1@text'),
        ('https://site2.com/items', '.amount@text', '.title@text')
    ) AS t(url, price_selector, name_selector)
)
SELECT
    s.url as source,
    htmlpath(c.html.document, s.name_selector) as name,
    htmlpath(c.html.document, s.price_selector) as price
FROM sites s,
LATERAL crawl(s.url) c;
```
