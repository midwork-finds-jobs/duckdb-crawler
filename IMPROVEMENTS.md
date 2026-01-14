# DuckDB Crawler - Improvement Proposals

Analysis of current implementation with proposed improvements organized by priority and category.

---

## Design Principles

| Principle | Description |
|-----------|-------------|
| **Fast** | Maximize throughput without being aggressive |
| **Good** | Reliable, correct, maintainable |
| **Cheap** | Minimize CPU, memory, bandwidth, storage |
| **Good Netizen** | Respect websites, be polite, identify yourself |

---

## Current State Assessment

### Strengths

| Area | Current Implementation |
|------|----------------------|
| robots.txt | Full compliance with Crawl-delay, Allow, Disallow |
| Rate limiting | Per-domain state, respects crawl-delay |
| Error handling | Fibonacci backoff, domain-level blocking |
| Sitemap support | Index recursion, metadata caching |
| SQL integration | Native CRAWL syntax, table functions |

### Weaknesses

| Area | Issue | Impact |
|------|-------|--------|
| Threading | Single-threaded processing | Slow |
| HTTP | New connection per request | Slow, wasteful |
| Memory | Full body in memory | High memory |
| Bandwidth | No compression negotiation | Wasteful |
| Storage | Raw HTML storage | Expensive |
| Resumability | No persistence | Data loss on crash |

---

## Improvement Proposals

### Category: FAST

#### F1. True Multi-Threading with Domain Isolation

**Problem**: Single mutex blocks all processing

**Current**:
```cpp
std::lock_guard<std::mutex> lock(global_state.mutex);
// All processing serialized
```

**Proposed**:
```cpp
// Per-domain locks, thread pool
struct DomainState {
    std::mutex mutex;  // Only locks this domain
    // ...
};

ThreadPool pool(num_threads);
for (auto& domain : domains) {
    pool.enqueue([&]() {
        process_domain(domain);  // Parallel across domains
    });
}
```

**Impact**: 10-50x throughput for multi-domain crawls

**Effort**: Medium

---

#### F2. HTTP Connection Pooling / Keep-Alive

**Problem**: New TCP+TLS handshake per request (~200-500ms overhead)

**Current**: Each `http_get()` creates new connection

**Proposed**:
- Reuse connections per domain
- HTTP/2 multiplexing where supported
- Configurable pool size per domain

**Impact**: 2-5x faster per-domain, reduced server load

**Effort**: Medium (depends on http_request extension capabilities)

---

#### F3. Batch Database Inserts

**Problem**: Row-by-row INSERT is slow

**Current**:
```cpp
for (each url) {
    conn.Query("INSERT INTO table VALUES (...)");
}
```

**Proposed**:
```cpp
std::vector<CrawlResult> batch;
batch.reserve(100);

for (each url) {
    batch.push_back(result);
    if (batch.size() >= 100) {
        bulk_insert(batch);
        batch.clear();
    }
}
```

**Impact**: 5-20x faster writes

**Effort**: Low

---

#### F4. Priority Queue for Domain Scheduling

**Problem**: Re-queued URLs cause inefficient iteration

**Current**: `std::deque` with push_back for blocked URLs

**Proposed**:
```cpp
struct UrlQueueEntry {
    std::string url;
    int retry_count;
    std::chrono::steady_clock::time_point earliest_fetch;
    // ...
};

// Priority queue sorted by earliest_fetch
std::priority_queue<UrlQueueEntry, vector, CompareByTime> queue;
```

**Impact**: Better scheduling, less busy-waiting

**Effort**: Low

---

#### F5. Parallel Sitemap Discovery

**Problem**: Sequential sitemap fetching for each hostname

**Current**:
```cpp
for (hostname : hostnames) {
    discover_sitemaps(hostname);  // Blocking
}
```

**Proposed**:
```cpp
std::vector<std::future<SitemapResult>> futures;
for (hostname : hostnames) {
    futures.push_back(async(discover_sitemaps, hostname));
}
// Collect results
```

**Impact**: N-fold faster for multi-site crawls

**Effort**: Low-Medium

---

#### F6. DNS Caching

**Problem**: Repeated DNS lookups for same domain

**Proposed**:
- Cache DNS results in memory
- Respect TTL
- Pre-resolve domains before crawling

**Impact**: Reduced latency, fewer DNS queries

**Effort**: Low

---

### Category: GOOD (Quality/Reliability)

#### G1. Persistent Queue for Resume

**Problem**: Crash loses all queue state

**Current**: In-memory `std::deque`

**Proposed**:
```sql
CREATE TABLE IF NOT EXISTS _crawl_queue (
    url VARCHAR PRIMARY KEY,
    retry_count INTEGER,
    is_update BOOLEAN,
    added_at TIMESTAMP,
    domain VARCHAR
);
```

On startup: load unprocessed URLs from `_crawl_queue`
On completion: delete from `_crawl_queue`

**Impact**: Crash recovery, long-running crawls

**Effort**: Medium

---

#### G2. URL Normalization

**Problem**: Same page crawled multiple times due to URL variations

**Examples**:
- `https://example.com/page` vs `https://example.com/page/`
- `https://example.com/page?utm_source=x` vs `https://example.com/page`
- `https://EXAMPLE.COM/page` vs `https://example.com/page`

**Proposed**:
```cpp
std::string NormalizeUrl(const std::string& url) {
    // 1. Lowercase scheme and host
    // 2. Remove default ports (80, 443)
    // 3. Remove trailing slash (configurable)
    // 4. Sort query parameters
    // 5. Remove tracking parameters (utm_*, fbclid, etc.)
    // 6. Decode unnecessary percent-encoding
}
```

**Impact**: Avoid duplicate crawls, save bandwidth

**Effort**: Medium

---

#### G3. Conditional Requests (ETag / Last-Modified)

**Problem**: Re-fetching unchanged content

**Current**: Always full GET

**Proposed**:
```sql
-- Store in crawl results
ALTER TABLE crawl_results ADD COLUMN etag VARCHAR;
ALTER TABLE crawl_results ADD COLUMN last_modified VARCHAR;

-- On re-crawl
If-None-Match: "stored_etag"
If-Modified-Since: stored_last_modified

-- Handle 304 Not Modified
if (status == 304) {
    // Update crawled_at only, keep existing body
}
```

**Impact**: Major bandwidth savings for update_stale

**Effort**: Medium

---

#### G4. Content Deduplication

**Problem**: Same content at different URLs

**Proposed**:
```sql
-- Add content hash column
ALTER TABLE crawl_results ADD COLUMN content_hash VARCHAR;

-- Before insert, check for duplicate
SELECT url FROM crawl_results
WHERE content_hash = hash(new_body)
LIMIT 1;
```

**Impact**: Detect mirrors, reduce storage

**Effort**: Low

---

#### G5. Redirect Handling

**Problem**: No visibility into redirect chains

**Current**: http_request follows redirects transparently

**Proposed**:
```sql
-- Track redirects
ALTER TABLE crawl_results ADD COLUMN final_url VARCHAR;
ALTER TABLE crawl_results ADD COLUMN redirect_count INTEGER;

-- Option to not follow redirects
WITH (follow_redirects false, max_redirects 5)
```

**Impact**: SEO analysis, detect redirect loops

**Effort**: Medium (depends on http_request)

---

#### G6. Structured Logging / Progress Reporting

**Problem**: No visibility during long crawls

**Proposed**:
```cpp
// Progress callback
void ReportProgress(const CrawlProgress& p) {
    // p.urls_total, p.urls_completed, p.urls_failed
    // p.current_domain, p.domains_completed
    // p.bytes_downloaded, p.elapsed_seconds
}

// Periodic status to table
INSERT INTO _crawl_progress (crawl_id, timestamp, ...)
```

**Impact**: Monitoring, debugging, user experience

**Effort**: Low-Medium

---

#### G7. Better Error Classification

**Problem**: Generic error messages

**Current**: `response.error` is free-form string

**Proposed**:
```cpp
enum class CrawlErrorType {
    NONE,
    NETWORK_TIMEOUT,
    NETWORK_DNS_FAILURE,
    NETWORK_CONNECTION_REFUSED,
    NETWORK_SSL_ERROR,
    HTTP_CLIENT_ERROR,
    HTTP_SERVER_ERROR,
    HTTP_RATE_LIMITED,
    ROBOTS_DISALLOWED,
    CONTENT_TOO_LARGE,
    CONTENT_TYPE_REJECTED
};
```

**Impact**: Better analytics, targeted retry logic

**Effort**: Low

---

### Category: CHEAP (Resource Efficiency)

#### C1. Content-Encoding Support (gzip/deflate)

**Problem**: Transferring uncompressed HTML

**Current**: No Accept-Encoding header

**Proposed**:
```cpp
headers["Accept-Encoding"] = "gzip, deflate, br";
// Decompress response body
```

**Impact**: 60-80% bandwidth reduction

**Effort**: Low (if http_request supports it)

---

#### C2. Response Size Limits

**Problem**: Huge files can exhaust memory

**Proposed**:
```cpp
// Config option
max_response_bytes = 10 * 1024 * 1024;  // 10MB default

// During fetch
if (content_length > max_response_bytes) {
    return {.status = -2, .error = "Response too large"};
}
```

**Impact**: Memory safety, avoid abuse

**Effort**: Low

---

#### C3. Content-Type Filtering

**Problem**: Downloading non-HTML content

**Proposed**:
```sql
WITH (
    accept_content_types 'text/html,application/xhtml+xml',
    reject_content_types 'image/*,video/*,application/pdf'
)
```

**Impact**: Skip unwanted downloads early

**Effort**: Low

---

#### C4. Streaming Body Processing

**Problem**: Full body loaded before processing

**Current**: `response.body` contains entire response

**Proposed**:
- Stream body to disk for large responses
- Option to hash without storing
- Chunked processing for extractors

**Impact**: Handle large files, reduce memory

**Effort**: High

---

#### C5. Compressed Storage Option

**Problem**: Raw HTML is verbose

**Proposed**:
```sql
-- Store compressed
ALTER TABLE crawl_results ADD COLUMN body_compressed BLOB;

-- Decompress on read
SELECT url, decompress(body_compressed) as body FROM crawl_results;
```

**Impact**: 5-10x storage reduction

**Effort**: Low (DuckDB has compression functions)

---

#### C6. robots.txt LRU Cache

**Problem**: robots.txt held in memory indefinitely

**Current**: `std::unordered_map<domain, DomainState>` grows unbounded

**Proposed**:
```cpp
LRUCache<std::string, DomainState> domain_cache(1000);

// Evict least-recently-used domains
// Re-fetch robots.txt if evicted and needed again
```

**Impact**: Bounded memory for large crawls

**Effort**: Low-Medium

---

### Category: GOOD NETIZEN (Politeness)

#### N1. Adaptive Rate Limiting

**Problem**: Fixed delays don't account for server load

**Current**: Static `crawl_delay_seconds`

**Proposed**:
```cpp
// Increase delay if response times are slow
if (response_time > 2 * average_response_time) {
    domain_state.crawl_delay_seconds *= 1.5;
}

// Decrease delay if consistently fast (floor at robots.txt value)
if (response_time < 0.5 * average_response_time) {
    domain_state.crawl_delay_seconds *= 0.9;
}
```

**Impact**: Adapt to server load, be gentler when needed

**Effort**: Low

---

#### N2. Time-of-Day Crawling

**Problem**: Crawling during peak hours

**Proposed**:
```sql
WITH (
    crawl_hours '00:00-06:00,22:00-24:00',  -- Off-peak only
    timezone 'America/New_York'
)
```

**Impact**: Reduce impact during business hours

**Effort**: Medium

---

#### N3. Request-Rate Support (robots.txt)

**Problem**: Only Crawl-delay supported, not Request-rate

**Current**: Ignores `Request-rate` directive

**Proposed**:
```
# robots.txt
Request-rate: 1/10  # 1 request per 10 seconds
```

```cpp
if (rules.HasRequestRate()) {
    // Use request_rate instead of crawl_delay
}
```

**Impact**: Better compliance with robots.txt

**Effort**: Low

---

#### N4. Honor Cache-Control Headers

**Problem**: Re-crawling content that shouldn't change

**Proposed**:
```cpp
// Parse Cache-Control header
if (cache_control.max_age > 0) {
    // Don't re-crawl until max_age expires
}
if (cache_control.no_store) {
    // Don't cache this response
}
```

**Impact**: Respect publisher caching intent

**Effort**: Medium

---

#### N5. Meta Robots Tag Support

**Problem**: Only robots.txt respected, not per-page directives

**Current**: Ignores `<meta name="robots" content="noindex">`

**Proposed**:
```cpp
// After fetching, check meta tags
if (HasMetaRobots(body, "nofollow")) {
    // Don't extract/follow links from this page
}
if (HasMetaRobots(body, "noindex")) {
    // Mark as "should not be stored" (optional)
}
```

**Impact**: Full robots compliance

**Effort**: Medium

---

#### N6. Accept-Encoding to Reduce Bandwidth

**Problem**: Servers send uncompressed data

**Proposed**: (Same as C1)
- Send `Accept-Encoding: gzip, deflate`
- Reduces bandwidth = less cost for website

**Impact**: Good for websites, good for us

**Effort**: Low

---

#### N7. Identify Bot Purpose in User-Agent

**Problem**: Generic user-agent gives no context

**Current**: `user_agent 'MyBot/1.0'`

**Best Practice**:
```
user_agent 'ConsumerResearchBot/1.0 (+https://nonprofit.org/bot; research@nonprofit.org)'
```

**Proposed**: Warn if user_agent doesn't include URL or contact

**Impact**: Webmasters can reach crawler operator

**Effort**: Very Low (documentation + optional warning)

---

#### N8. Global Concurrent Connection Limit

**Problem**: Many domains = many simultaneous connections

**Current**: `max_parallel_per_domain` but no global limit

**Proposed**:
```sql
WITH (
    max_parallel_per_domain 4,
    max_parallel_total 20  -- Global cap
)
```

**Impact**: Don't overwhelm network, be fair to all

**Effort**: Low

---

## Priority Matrix

### High Priority (Do First)

| ID | Improvement | Impact | Effort | Category |
|----|-------------|--------|--------|----------|
| F3 | Batch inserts | High | Low | Fast |
| C1 | gzip support | High | Low | Cheap |
| C2 | Response size limits | Medium | Low | Cheap |
| N8 | Global connection limit | Medium | Low | Netizen |
| G7 | Error classification | Medium | Low | Good |

### Medium Priority

| ID | Improvement | Impact | Effort | Category |
|----|-------------|--------|--------|----------|
| F1 | Multi-threading | Very High | Medium | Fast |
| F4 | Priority queue | Medium | Low | Fast |
| G1 | Persistent queue | High | Medium | Good |
| G2 | URL normalization | Medium | Medium | Good |
| G3 | Conditional requests | High | Medium | Good |
| N1 | Adaptive rate limiting | Medium | Low | Netizen |

### Lower Priority (Nice to Have)

| ID | Improvement | Impact | Effort | Category |
|----|-------------|--------|--------|----------|
| F2 | Connection pooling | High | Medium | Fast |
| F5 | Parallel sitemap discovery | Medium | Low | Fast |
| C4 | Streaming body | Medium | High | Cheap |
| C5 | Compressed storage | Medium | Low | Cheap |
| N2 | Time-of-day crawling | Low | Medium | Netizen |
| N5 | Meta robots support | Low | Medium | Netizen |

---

## Implementation Roadmap

### Phase 1: Quick Wins (1-2 weeks effort)

1. **C1**: Add Accept-Encoding header
2. **C2**: Response size limits
3. **F3**: Batch inserts
4. **G7**: Error classification enum
5. **N8**: Global connection limit
6. **F4**: Priority queue

### Phase 2: Core Improvements (2-4 weeks effort)

1. **F1**: Multi-threading with domain isolation
2. **G1**: Persistent queue for resume
3. **G2**: URL normalization
4. **G3**: Conditional requests (ETag/Last-Modified)
5. **N1**: Adaptive rate limiting

### Phase 3: Polish (ongoing)

1. **F2**: Connection pooling
2. **G5**: Redirect tracking
3. **G6**: Progress reporting
4. **C5**: Compressed storage option
5. **N4**: Cache-Control support
6. **N5**: Meta robots support

---

## Metrics to Track

| Metric | Current | Target | How to Measure |
|--------|---------|--------|----------------|
| URLs/second (single domain) | ~1 | ~5-10 | Benchmark |
| URLs/second (multi-domain) | ~1 | ~50-100 | Benchmark |
| Memory per 10K URLs | ? | <100MB | Profile |
| Bandwidth efficiency | 0% compression | 70%+ savings | Compare sizes |
| Resume success rate | 0% | 100% | Test crashes |
| robots.txt compliance | ~90% | 100% | Audit |

---

## Questions to Resolve

1. Does http_request extension support Keep-Alive/connection reuse?
2. Does http_request expose redirect chain information?
3. What's acceptable memory budget for large crawls?
4. Should compressed storage be default or opt-in?
5. How to handle sites that block based on crawl patterns?
6. Should we support HTTP/2 server push?
