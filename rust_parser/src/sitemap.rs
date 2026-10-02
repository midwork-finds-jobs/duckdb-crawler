//! Sitemap XML parsing

use quick_xml::events::Event;
use quick_xml::Reader;
use std::time::Instant;

/// Single sitemap entry
#[derive(Debug, Clone, serde::Serialize)]
pub struct SitemapEntry {
    pub url: String,
    pub lastmod: Option<String>,
    pub changefreq: Option<String>,
    pub priority: Option<f64>,
}

/// Sitemap index entry (references other sitemaps)
#[derive(Debug, Clone, serde::Serialize)]
pub struct SitemapIndexEntry {
    pub url: String,
    pub lastmod: Option<String>,
}

/// Result of parsing a sitemap
#[derive(Debug, Clone, serde::Serialize)]
pub struct SitemapResult {
    /// URL entries from urlset
    pub urls: Vec<SitemapEntry>,
    /// Child sitemap references from sitemapindex
    pub sitemaps: Vec<SitemapIndexEntry>,
    /// Parsing errors (non-fatal)
    pub errors: Vec<String>,
}

/// Parse sitemap XML content
pub fn parse_sitemap(xml: &str) -> SitemapResult {
    let mut result = SitemapResult {
        urls: vec![],
        sitemaps: vec![],
        errors: vec![],
    };

    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(true);

    let mut buf = Vec::new();
    let mut current_tag = String::new();
    let mut in_url = false;
    let mut in_sitemap = false;

    // Current entry being built
    let mut url = String::new();
    let mut lastmod: Option<String> = None;
    let mut changefreq: Option<String> = None;
    let mut priority: Option<f64> = None;

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) => {
                let tag = String::from_utf8_lossy(e.name().as_ref()).to_string();
                current_tag = tag.clone();

                match tag.as_str() {
                    "url" => {
                        in_url = true;
                        url.clear();
                        lastmod = None;
                        changefreq = None;
                        priority = None;
                    }
                    "sitemap" => {
                        in_sitemap = true;
                        url.clear();
                        lastmod = None;
                    }
                    _ => {}
                }
            }
            Ok(Event::End(e)) => {
                let tag = String::from_utf8_lossy(e.name().as_ref()).to_string();

                match tag.as_str() {
                    "url" if in_url => {
                        if !url.is_empty() {
                            result.urls.push(SitemapEntry {
                                url: url.clone(),
                                lastmod: lastmod.clone(),
                                changefreq: changefreq.clone(),
                                priority,
                            });
                        }
                        in_url = false;
                    }
                    "sitemap" if in_sitemap => {
                        if !url.is_empty() {
                            result.sitemaps.push(SitemapIndexEntry {
                                url: url.clone(),
                                lastmod: lastmod.clone(),
                            });
                        }
                        in_sitemap = false;
                    }
                    _ => {}
                }
                current_tag.clear();
            }
            Ok(Event::Text(e)) => {
                let text = e.unescape().unwrap_or_default().to_string();

                if in_url || in_sitemap {
                    match current_tag.as_str() {
                        "loc" => url = text,
                        "lastmod" => lastmod = Some(text),
                        "changefreq" if in_url => changefreq = Some(text),
                        "priority" if in_url => priority = text.parse().ok(),
                        _ => {}
                    }
                }
            }
            Ok(Event::Eof) => break,
            Err(e) => {
                result.errors.push(format!("XML parse error: {}", e));
                break;
            }
            _ => {}
        }
        buf.clear();
    }

    result
}

/// Fetch and parse sitemap(s) using ureq (simple blocking HTTP)
pub fn fetch_sitemap_blocking(
    url: &str,
    user_agent: &str,
    deadline: Instant,
    recursive: bool,
    max_depth: usize,
) -> SitemapResult {
    let agent = ureq::Agent::new_with_config(
        ureq::Agent::config_builder()
            .user_agent(user_agent)
            .build(),
    );

    fetch_sitemap_internal_ureq(&agent, url, recursive, max_depth, 0, deadline)
}

fn fetch_sitemap_internal_ureq(
    agent: &ureq::Agent,
    url: &str,
    recursive: bool,
    max_depth: usize,
    current_depth: usize,
    deadline: Instant,
) -> SitemapResult {
    let mut result = SitemapResult {
        urls: vec![],
        sitemaps: vec![],
        errors: vec![],
    };

    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        result.errors.push(format!("sitemap timeout fetching {}", url));
        return result;
    }
    if current_depth > max_depth {
        return result;
    }

    // Fetch the sitemap
    let xml = match agent.get(url).config().timeout_global(Some(remaining)).build().call() {
        Ok(resp) => {
            if resp.status().is_success() {
                match resp.into_body().read_to_string() {
                    Ok(text) => text,
                    Err(e) => {
                        result
                            .errors
                            .push(format!("Failed to read {}: {}", url, e));
                        return result;
                    }
                }
            } else {
                result
                    .errors
                    .push(format!("HTTP {} for {}", resp.status(), url));
                return result;
            }
        }
        Err(e) => {
            result
                .errors
                .push(format!("Failed to fetch {}: {}", url, e));
            return result;
        }
    };

    // Parse the sitemap
    let parsed = parse_sitemap(&xml);
    result.urls.extend(parsed.urls);
    result.errors.extend(parsed.errors);

    // If recursive, fetch child sitemaps
    if recursive && !parsed.sitemaps.is_empty() {
        for sitemap_entry in parsed.sitemaps {
            let child_result = fetch_sitemap_internal_ureq(
                agent,
                &sitemap_entry.url,
                recursive,
                max_depth,
                current_depth + 1,
                deadline,
            );
            result.urls.extend(child_result.urls);
            result.sitemaps.extend(child_result.sitemaps);
            result.errors.extend(child_result.errors);
        }
    } else {
        result.sitemaps.extend(parsed.sitemaps);
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_urlset() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
        <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
            <url>
                <loc>https://example.com/page1</loc>
                <lastmod>2024-01-15</lastmod>
                <changefreq>daily</changefreq>
                <priority>0.8</priority>
            </url>
            <url>
                <loc>https://example.com/page2</loc>
            </url>
        </urlset>"#;

        let result = parse_sitemap(xml);
        assert_eq!(result.urls.len(), 2);
        assert_eq!(result.urls[0].url, "https://example.com/page1");
        assert_eq!(result.urls[0].lastmod, Some("2024-01-15".to_string()));
        assert_eq!(result.urls[0].priority, Some(0.8));
        assert_eq!(result.urls[1].url, "https://example.com/page2");
    }

    #[test]
    fn test_parse_sitemapindex() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
        <sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
            <sitemap>
                <loc>https://example.com/sitemap1.xml</loc>
                <lastmod>2024-01-15</lastmod>
            </sitemap>
            <sitemap>
                <loc>https://example.com/sitemap2.xml</loc>
            </sitemap>
        </sitemapindex>"#;

        let result = parse_sitemap(xml);
        assert_eq!(result.sitemaps.len(), 2);
        assert_eq!(result.sitemaps[0].url, "https://example.com/sitemap1.xml");
    }
}
