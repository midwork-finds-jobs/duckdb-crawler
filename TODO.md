# DuckDB Crawler - TODO

## Decisions Made

1. **htmlpath() naming**: Keep `htmlpath()`, use `jq()` as primary alias for simple CSS selections
2. **MERGE unseen rows**: Keep unseen rows by default. Add `WHEN NOT MATCHED BY SOURCE` clause (SQL standard) between `WHEN MATCHED` and `LIMIT` for handling rows in target but not in source
3. **SET crawler_option**: Implement `SET crawler_*` settings for global configuration
4. **DuckDB http settings**: Read proxy and timeout from DuckDB's http_* settings
5. **CREATE SECRET**: Integrate with DuckDB secrets for bearer tokens and extra headers
6. **Per-domain overrides**: Not implementing (use WITH clause options instead)

## Open Questions

* Ensure that robots.txt sitemap still works too and sitemap honours the crawl-delay in robots.txt
    * Parse robots.txt into a cache which has 24 hours TTL
* Build example SQL for crawling blog posts, product pages, jobs, events. Create unittests for those too.

## In Progress

- [ ] WHEN NOT MATCHED BY SOURCE clause in STREAM INTO MERGE

## Implemented Features

- [x] SET crawler_* settings (user_agent, default_delay, respect_robots, timeout_ms, max_response_bytes)
- [x] DuckDB http_proxy settings integration
- [x] CREATE SECRET integration (bearer_token, extra_http_headers)

- [x] F2: Connection pooling (libcurl handle pool)
- [x] F3: Batch inserts
- [x] F5: Parallel sitemap discovery
- [x] G6: Progress reporting
- [x] G7: Error classification
- [x] C1: Compression (Accept-Encoding)
- [x] C2: Response size limits
- [x] C3: Content-Type filtering
- [x] N3: Request-rate support
- [x] N8: Global connection limit
- [x] ETag/Last-Modified headers
- [x] Content hash deduplication
- [x] SURT keys for URL normalization
- [x] Adaptive rate limiting
- [x] Priority queue scheduling
- [x] Gzip sitemap decompression
- [x] HTTP/2 support (libcurl + nghttp2)
- [x] G5: Redirect tracking (final_url, redirect_count columns)
- [x] N5: Meta robots tag support (noindex clears body, nofollow skips link extraction)
- [x] Large HTTP headers support (libcurl has no header size limit)
- [x] html.readability extraction (title, content, text_content, excerpt)
- [x] html.schema as MAP(VARCHAR, JSON) with array support for multiple items
