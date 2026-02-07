//! C FFI interface for the HTML parser

use crate::extractors::{extract_all, ExtractionRequest};
use std::ffi::{c_char, CStr, CString};
use std::ptr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

// Global interrupt flag for graceful shutdown
static INTERRUPTED: AtomicBool = AtomicBool::new(false);

/// Set the interrupt flag (called from C++ signal handler)
#[no_mangle]
pub extern "C" fn set_interrupted(value: bool) {
    INTERRUPTED.store(value, Ordering::SeqCst);
}

/// Check if interrupted
#[no_mangle]
pub extern "C" fn is_interrupted() -> bool {
    INTERRUPTED.load(Ordering::SeqCst)
}

/// FFI-safe extraction result
#[repr(C)]
pub struct ExtractionResultFFI {
    /// JSON-serialized result (caller must free with free_extraction_result)
    pub json_ptr: *mut c_char,
    /// Error message if failed (caller must free with free_extraction_result)
    pub error_ptr: *mut c_char,
}

/// Extract data from HTML
///
/// # Arguments
/// * `html_ptr` - Pointer to HTML string
/// * `html_len` - Length of HTML string
/// * `request_json` - JSON-serialized ExtractionRequest
///
/// # Returns
/// ExtractionResultFFI with either json_ptr or error_ptr set
///
/// # Safety
/// Caller must:
/// - Ensure html_ptr points to valid UTF-8 of html_len bytes
/// - Ensure request_json is a valid null-terminated C string
/// - Call free_extraction_result on the returned value
#[no_mangle]
pub unsafe extern "C" fn extract_from_html(
    html_ptr: *const c_char,
    html_len: usize,
    request_json: *const c_char,
) -> ExtractionResultFFI {
    // Parse HTML
    let html = match std::str::from_utf8(std::slice::from_raw_parts(html_ptr as *const u8, html_len))
    {
        Ok(s) => s,
        Err(e) => {
            return ExtractionResultFFI {
                json_ptr: ptr::null_mut(),
                error_ptr: string_to_ptr(format!("Invalid UTF-8 in HTML: {}", e)),
            };
        }
    };

    // Parse request JSON
    let request_str = match CStr::from_ptr(request_json).to_str() {
        Ok(s) => s,
        Err(e) => {
            return ExtractionResultFFI {
                json_ptr: ptr::null_mut(),
                error_ptr: string_to_ptr(format!("Invalid UTF-8 in request: {}", e)),
            };
        }
    };

    let request: ExtractionRequest = match serde_json::from_str(request_str) {
        Ok(r) => r,
        Err(e) => {
            return ExtractionResultFFI {
                json_ptr: ptr::null_mut(),
                error_ptr: string_to_ptr(format!("Invalid request JSON: {}", e)),
            };
        }
    };

    // Perform extraction
    let result = extract_all(html, &request);

    // Serialize result
    match serde_json::to_string(&result) {
        Ok(json) => ExtractionResultFFI {
            json_ptr: string_to_ptr(json),
            error_ptr: ptr::null_mut(),
        },
        Err(e) => ExtractionResultFFI {
            json_ptr: ptr::null_mut(),
            error_ptr: string_to_ptr(format!("Failed to serialize result: {}", e)),
        },
    }
}

/// Free an extraction result
///
/// # Safety
/// Must only be called with a result from extract_from_html
#[no_mangle]
pub unsafe extern "C" fn free_extraction_result(result: ExtractionResultFFI) {
    if !result.json_ptr.is_null() {
        drop(CString::from_raw(result.json_ptr));
    }
    if !result.error_ptr.is_null() {
        drop(CString::from_raw(result.error_ptr));
    }
}

/// Convert String to C pointer
fn string_to_ptr(s: String) -> *mut c_char {
    match CString::new(s) {
        Ok(cs) => cs.into_raw(),
        Err(_) => ptr::null_mut(),
    }
}

/// Get version string
#[no_mangle]
pub extern "C" fn rust_parser_version() -> *const c_char {
    static VERSION: &[u8] = b"0.1.0\0";
    VERSION.as_ptr() as *const c_char
}

// Convenience extractors for individual data types

/// Extract JSON-LD from HTML, returns JSON object keyed by @type
#[no_mangle]
pub unsafe extern "C" fn extract_jsonld_ffi(
    html_ptr: *const c_char,
    html_len: usize,
) -> ExtractionResultFFI {
    let html = match std::str::from_utf8(std::slice::from_raw_parts(html_ptr as *const u8, html_len)) {
        Ok(s) => s,
        Err(e) => {
            return ExtractionResultFFI {
                json_ptr: ptr::null_mut(),
                error_ptr: string_to_ptr(format!("Invalid UTF-8: {}", e)),
            };
        }
    };

    let document = scraper::Html::parse_document(html);
    let jsonld = crate::extractors::extract_jsonld_objects(&document);

    match serde_json::to_string(&jsonld) {
        Ok(json) => ExtractionResultFFI {
            json_ptr: string_to_ptr(json),
            error_ptr: ptr::null_mut(),
        },
        Err(e) => ExtractionResultFFI {
            json_ptr: ptr::null_mut(),
            error_ptr: string_to_ptr(format!("Serialization error: {}", e)),
        },
    }
}

/// Extract Microdata from HTML
#[no_mangle]
pub unsafe extern "C" fn extract_microdata_ffi(
    html_ptr: *const c_char,
    html_len: usize,
) -> ExtractionResultFFI {
    let html = match std::str::from_utf8(std::slice::from_raw_parts(html_ptr as *const u8, html_len)) {
        Ok(s) => s,
        Err(e) => {
            return ExtractionResultFFI {
                json_ptr: ptr::null_mut(),
                error_ptr: string_to_ptr(format!("Invalid UTF-8: {}", e)),
            };
        }
    };

    let document = scraper::Html::parse_document(html);
    let microdata = crate::extractors::extract_microdata(&document);

    match serde_json::to_string(&microdata) {
        Ok(json) => ExtractionResultFFI {
            json_ptr: string_to_ptr(json),
            error_ptr: ptr::null_mut(),
        },
        Err(e) => ExtractionResultFFI {
            json_ptr: ptr::null_mut(),
            error_ptr: string_to_ptr(format!("Serialization error: {}", e)),
        },
    }
}

/// Extract OpenGraph from HTML
#[no_mangle]
pub unsafe extern "C" fn extract_opengraph_ffi(
    html_ptr: *const c_char,
    html_len: usize,
) -> ExtractionResultFFI {
    let html = match std::str::from_utf8(std::slice::from_raw_parts(html_ptr as *const u8, html_len)) {
        Ok(s) => s,
        Err(e) => {
            return ExtractionResultFFI {
                json_ptr: ptr::null_mut(),
                error_ptr: string_to_ptr(format!("Invalid UTF-8: {}", e)),
            };
        }
    };

    let document = scraper::Html::parse_document(html);
    let og = crate::extractors::extract_opengraph(&document);

    match serde_json::to_string(&og) {
        Ok(json) => ExtractionResultFFI {
            json_ptr: string_to_ptr(json),
            error_ptr: ptr::null_mut(),
        },
        Err(e) => ExtractionResultFFI {
            json_ptr: ptr::null_mut(),
            error_ptr: string_to_ptr(format!("Serialization error: {}", e)),
        },
    }
}

/// Extract JS variables from HTML using SWC parser
#[no_mangle]
pub unsafe extern "C" fn extract_js_ffi(
    html_ptr: *const c_char,
    html_len: usize,
) -> ExtractionResultFFI {
    let html = match std::str::from_utf8(std::slice::from_raw_parts(html_ptr as *const u8, html_len)) {
        Ok(s) => s,
        Err(e) => {
            return ExtractionResultFFI {
                json_ptr: ptr::null_mut(),
                error_ptr: string_to_ptr(format!("Invalid UTF-8: {}", e)),
            };
        }
    };

    let document = scraper::Html::parse_document(html);
    let js_vars = crate::extractors::extract_js_variables(&document);

    match serde_json::to_string(&js_vars) {
        Ok(json) => ExtractionResultFFI {
            json_ptr: string_to_ptr(json),
            error_ptr: ptr::null_mut(),
        },
        Err(e) => ExtractionResultFFI {
            json_ptr: ptr::null_mut(),
            error_ptr: string_to_ptr(format!("Serialization error: {}", e)),
        },
    }
}

/// Extract meta tags from HTML
#[no_mangle]
pub unsafe extern "C" fn extract_meta_ffi(
    html_ptr: *const c_char,
    html_len: usize,
) -> ExtractionResultFFI {
    let html = match std::str::from_utf8(std::slice::from_raw_parts(html_ptr as *const u8, html_len)) {
        Ok(s) => s,
        Err(e) => {
            return ExtractionResultFFI {
                json_ptr: ptr::null_mut(),
                error_ptr: string_to_ptr(format!("Invalid UTF-8: {}", e)),
            };
        }
    };

    let document = scraper::Html::parse_document(html);
    let meta = crate::extractors::extract_meta_tags(&document);

    match serde_json::to_string(&meta) {
        Ok(json) => ExtractionResultFFI {
            json_ptr: string_to_ptr(json),
            error_ptr: ptr::null_mut(),
        },
        Err(e) => ExtractionResultFFI {
            json_ptr: ptr::null_mut(),
            error_ptr: string_to_ptr(format!("Serialization error: {}", e)),
        },
    }
}

/// Token-efficient page inventory for LLM agents
#[no_mangle]
pub unsafe extern "C" fn page_info_ffi(
    html_ptr: *const c_char,
    html_len: usize,
    url_ptr: *const c_char,
) -> ExtractionResultFFI {
    let html = match std::str::from_utf8(std::slice::from_raw_parts(html_ptr as *const u8, html_len)) {
        Ok(s) => s,
        Err(e) => {
            return ExtractionResultFFI {
                json_ptr: ptr::null_mut(),
                error_ptr: string_to_ptr(format!("Invalid UTF-8: {}", e)),
            };
        }
    };

    let url = match CStr::from_ptr(url_ptr).to_str() {
        Ok(s) => s,
        Err(e) => {
            return ExtractionResultFFI {
                json_ptr: ptr::null_mut(),
                error_ptr: string_to_ptr(format!("Invalid URL: {}", e)),
            };
        }
    };

    let result = crate::extractors::page_info(html, url);

    match serde_json::to_string(&result) {
        Ok(json) => ExtractionResultFFI {
            json_ptr: string_to_ptr(json),
            error_ptr: ptr::null_mut(),
        },
        Err(e) => ExtractionResultFFI {
            json_ptr: ptr::null_mut(),
            error_ptr: string_to_ptr(format!("Serialization error: {}", e)),
        },
    }
}

/// Extract article content using readability algorithm
#[no_mangle]
pub unsafe extern "C" fn extract_readability_ffi(
    html_ptr: *const c_char,
    html_len: usize,
    url_ptr: *const c_char,
) -> ExtractionResultFFI {
    let html = match std::str::from_utf8(std::slice::from_raw_parts(html_ptr as *const u8, html_len)) {
        Ok(s) => s,
        Err(e) => {
            return ExtractionResultFFI {
                json_ptr: ptr::null_mut(),
                error_ptr: string_to_ptr(format!("Invalid UTF-8: {}", e)),
            };
        }
    };

    let url = match CStr::from_ptr(url_ptr).to_str() {
        Ok(s) => s,
        Err(e) => {
            return ExtractionResultFFI {
                json_ptr: ptr::null_mut(),
                error_ptr: string_to_ptr(format!("Invalid URL: {}", e)),
            };
        }
    };

    let result = crate::extractors::extract_readability(html, url);

    match serde_json::to_string(&result) {
        Ok(json) => ExtractionResultFFI {
            json_ptr: string_to_ptr(json),
            error_ptr: ptr::null_mut(),
        },
        Err(e) => ExtractionResultFFI {
            json_ptr: ptr::null_mut(),
            error_ptr: string_to_ptr(format!("Serialization error: {}", e)),
        },
    }
}

/// Extract elements matching CSS selector
#[no_mangle]
pub unsafe extern "C" fn extract_css_ffi(
    html_ptr: *const c_char,
    html_len: usize,
    selector_ptr: *const c_char,
) -> ExtractionResultFFI {
    let html = match std::str::from_utf8(std::slice::from_raw_parts(html_ptr as *const u8, html_len)) {
        Ok(s) => s,
        Err(e) => {
            return ExtractionResultFFI {
                json_ptr: ptr::null_mut(),
                error_ptr: string_to_ptr(format!("Invalid UTF-8: {}", e)),
            };
        }
    };

    let selector_str = match CStr::from_ptr(selector_ptr).to_str() {
        Ok(s) => s,
        Err(e) => {
            return ExtractionResultFFI {
                json_ptr: ptr::null_mut(),
                error_ptr: string_to_ptr(format!("Invalid selector: {}", e)),
            };
        }
    };

    let document = scraper::Html::parse_document(html);
    let selector = match scraper::Selector::parse(selector_str) {
        Ok(s) => s,
        Err(_) => {
            return ExtractionResultFFI {
                json_ptr: string_to_ptr("[]".to_string()),
                error_ptr: ptr::null_mut(),
            };
        }
    };

    let results: Vec<String> = document
        .select(&selector)
        .map(|el| el.text().collect::<String>().trim().to_string())
        .collect();

    match serde_json::to_string(&results) {
        Ok(json) => ExtractionResultFFI {
            json_ptr: string_to_ptr(json),
            error_ptr: ptr::null_mut(),
        },
        Err(e) => ExtractionResultFFI {
            json_ptr: ptr::null_mut(),
            error_ptr: string_to_ptr(format!("Serialization error: {}", e)),
        },
    }
}

/// Extract links from HTML using a CSS selector
/// Returns JSON array of absolute URLs
#[no_mangle]
pub unsafe extern "C" fn extract_links_ffi(
    html_ptr: *const c_char,
    html_len: usize,
    selector_ptr: *const c_char,
    base_url_ptr: *const c_char,
) -> ExtractionResultFFI {
    let html = match std::str::from_utf8(std::slice::from_raw_parts(html_ptr as *const u8, html_len)) {
        Ok(s) => s,
        Err(e) => {
            return ExtractionResultFFI {
                json_ptr: ptr::null_mut(),
                error_ptr: string_to_ptr(format!("Invalid UTF-8: {}", e)),
            };
        }
    };

    let selector = match CStr::from_ptr(selector_ptr).to_str() {
        Ok(s) => s,
        Err(e) => {
            return ExtractionResultFFI {
                json_ptr: ptr::null_mut(),
                error_ptr: string_to_ptr(format!("Invalid selector: {}", e)),
            };
        }
    };

    let base_url = match CStr::from_ptr(base_url_ptr).to_str() {
        Ok(s) => s,
        Err(e) => {
            return ExtractionResultFFI {
                json_ptr: ptr::null_mut(),
                error_ptr: string_to_ptr(format!("Invalid base URL: {}", e)),
            };
        }
    };

    let links = crate::extractors::extract_links(html, selector, base_url);

    match serde_json::to_string(&links) {
        Ok(json) => ExtractionResultFFI {
            json_ptr: string_to_ptr(json),
            error_ptr: ptr::null_mut(),
        },
        Err(e) => ExtractionResultFFI {
            json_ptr: ptr::null_mut(),
            error_ptr: string_to_ptr(format!("Serialization error: {}", e)),
        },
    }
}

/// Extract element as struct with text, html, and attr map
/// Returns JSON: {"text": "...", "html": "...", "attr": {"key": "value", ...}}
#[no_mangle]
pub unsafe extern "C" fn extract_element_ffi(
    html_ptr: *const c_char,
    html_len: usize,
    selector_ptr: *const c_char,
) -> ExtractionResultFFI {
    let html = match std::str::from_utf8(std::slice::from_raw_parts(html_ptr as *const u8, html_len)) {
        Ok(s) => s,
        Err(e) => {
            return ExtractionResultFFI {
                json_ptr: ptr::null_mut(),
                error_ptr: string_to_ptr(format!("Invalid UTF-8: {}", e)),
            };
        }
    };

    let selector = match CStr::from_ptr(selector_ptr).to_str() {
        Ok(s) => s,
        Err(e) => {
            return ExtractionResultFFI {
                json_ptr: ptr::null_mut(),
                error_ptr: string_to_ptr(format!("Invalid selector: {}", e)),
            };
        }
    };

    match crate::extractors::extract_element(html, selector) {
        Some(element_data) => {
            match serde_json::to_string(&element_data) {
                Ok(json) => ExtractionResultFFI {
                    json_ptr: string_to_ptr(json),
                    error_ptr: ptr::null_mut(),
                },
                Err(e) => ExtractionResultFFI {
                    json_ptr: ptr::null_mut(),
                    error_ptr: string_to_ptr(format!("Serialization error: {}", e)),
                },
            }
        }
        None => ExtractionResultFFI {
            json_ptr: string_to_ptr("null".to_string()),
            error_ptr: ptr::null_mut(),
        },
    }
}

/// Extract using unified path syntax: css@attr[*].json.path
///
/// Examples:
/// - `input#jobs@value` -> attribute value as string
/// - `input#jobs@value[*]` -> JSON array of all elements
/// - `input#jobs@value[*].id` -> array of 'id' fields
#[no_mangle]
pub unsafe extern "C" fn extract_path_ffi(
    html_ptr: *const c_char,
    html_len: usize,
    path_ptr: *const c_char,
) -> ExtractionResultFFI {
    let html = match std::str::from_utf8(std::slice::from_raw_parts(html_ptr as *const u8, html_len)) {
        Ok(s) => s,
        Err(e) => {
            return ExtractionResultFFI {
                json_ptr: ptr::null_mut(),
                error_ptr: string_to_ptr(format!("Invalid UTF-8: {}", e)),
            };
        }
    };

    let path = match CStr::from_ptr(path_ptr).to_str() {
        Ok(s) => s,
        Err(e) => {
            return ExtractionResultFFI {
                json_ptr: ptr::null_mut(),
                error_ptr: string_to_ptr(format!("Invalid path: {}", e)),
            };
        }
    };

    match crate::extractors::extract_path(html, path) {
        Some(value) => {
            match serde_json::to_string(&value) {
                Ok(json) => ExtractionResultFFI {
                    json_ptr: string_to_ptr(json),
                    error_ptr: ptr::null_mut(),
                },
                Err(e) => ExtractionResultFFI {
                    json_ptr: ptr::null_mut(),
                    error_ptr: string_to_ptr(format!("Serialization error: {}", e)),
                },
            }
        }
        None => ExtractionResultFFI {
            json_ptr: string_to_ptr("null".to_string()),
            error_ptr: ptr::null_mut(),
        },
    }
}

// ============================================================================
// HTML Table Extraction
// ============================================================================

/// Extract HTML table using CSS selector
/// Returns JSON: {"headers": [...], "rows": [[...], ...], "num_columns": N, "num_rows": M}
/// url_ptr is used to detect Wikipedia pages for special handling
/// table_index: 0-based index of which matching element to extract (0 = first)
#[no_mangle]
pub unsafe extern "C" fn extract_table_ffi(
    html_ptr: *const c_char,
    html_len: usize,
    selector_ptr: *const c_char,
    url_ptr: *const c_char,
    table_index: usize,
) -> ExtractionResultFFI {
    let html = match std::str::from_utf8(std::slice::from_raw_parts(html_ptr as *const u8, html_len)) {
        Ok(s) => s,
        Err(e) => {
            return ExtractionResultFFI {
                json_ptr: ptr::null_mut(),
                error_ptr: string_to_ptr(format!("Invalid UTF-8: {}", e)),
            };
        }
    };

    let selector = match CStr::from_ptr(selector_ptr).to_str() {
        Ok(s) => s,
        Err(e) => {
            return ExtractionResultFFI {
                json_ptr: ptr::null_mut(),
                error_ptr: string_to_ptr(format!("Invalid selector: {}", e)),
            };
        }
    };

    let url = match CStr::from_ptr(url_ptr).to_str() {
        Ok(s) => s,
        Err(_) => "",
    };

    // Detect Wikipedia pages for special handling
    let is_wikipedia = url.contains("wikipedia.org");

    let result = crate::extractors::extract_table(html, selector, is_wikipedia, table_index);

    match serde_json::to_string(&result) {
        Ok(json) => ExtractionResultFFI {
            json_ptr: string_to_ptr(json),
            error_ptr: ptr::null_mut(),
        },
        Err(e) => ExtractionResultFFI {
            json_ptr: ptr::null_mut(),
            error_ptr: string_to_ptr(format!("Serialization error: {}", e)),
        },
    }
}

// ============================================================================
// Batch Crawl + Extract (HTTP in Rust)
// ============================================================================

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Request for batch crawling
#[derive(Debug, serde::Deserialize)]
struct BatchCrawlRequest {
    urls: Vec<String>,
    #[serde(default)]
    extraction: Option<ExtractionRequest>,
    #[serde(default = "default_user_agent")]
    user_agent: String,
    #[serde(default = "default_timeout")]
    timeout_ms: u64,
    #[serde(default = "default_concurrency")]
    concurrency: usize,
    #[serde(default)]
    delay_ms: u64, // Min delay between requests to same domain
    #[serde(default)]
    respect_robots: bool, // Check robots.txt before fetching
    #[serde(default)]
    http_proxy: Option<String>, // HTTP proxy URL (e.g., "http://proxy:8080")
    #[serde(default)]
    http_proxy_username: Option<String>,
    #[serde(default)]
    http_proxy_password: Option<String>,
    #[serde(default)]
    extra_headers: Option<std::collections::HashMap<String, String>>, // Extra HTTP headers
}

fn default_user_agent() -> String {
    "DuckDB-Crawler/1.0".to_string()
}

fn default_timeout() -> u64 {
    30000
}

fn default_concurrency() -> usize {
    4
}

/// Extract domain from URL
fn extract_domain(url: &str) -> String {
    url::Url::parse(url)
        .ok()
        .and_then(|u| u.host_str().map(|h| h.to_lowercase()))
        .unwrap_or_default()
}

/// Per-domain rate limiter
type DomainRateLimiter = Arc<Mutex<HashMap<String, std::time::Instant>>>;

/// Single crawl result
#[derive(Debug, serde::Serialize)]
struct CrawlResult {
    url: String,
    final_url: String,
    status: i32,
    content_type: String,
    body: String,
    error: Option<String>,
    extracted: Option<serde_json::Value>,
    response_time_ms: u64,
}

/// Batch crawl response
#[derive(Debug, serde::Serialize)]
struct BatchCrawlResponse {
    results: Vec<CrawlResult>,
}

/// Fetch a single URL with rate limiting and optional extraction
async fn fetch_and_extract(
    client: &reqwest::Client,
    url: String,
    extraction: &Option<ExtractionRequest>,
    rate_limiter: &DomainRateLimiter,
    delay_ms: u64,
) -> CrawlResult {
    let start = std::time::Instant::now();

    // Apply per-domain rate limiting
    if delay_ms > 0 {
        let domain = extract_domain(&url);
        let delay = Duration::from_millis(delay_ms);

        let wait_time = {
            let limiter = rate_limiter.lock().await;
            if let Some(last_access) = limiter.get(&domain) {
                let elapsed = last_access.elapsed();
                if elapsed < delay {
                    Some(delay - elapsed)
                } else {
                    None
                }
            } else {
                None
            }
        };

        if let Some(wait) = wait_time {
            tokio::time::sleep(wait).await;
        }

        // Update last access time
        {
            let mut limiter = rate_limiter.lock().await;
            limiter.insert(domain, std::time::Instant::now());
        }
    }

    match client.get(&url).send().await {
        Ok(response) => {
            let status = response.status().as_u16() as i32;
            let final_url = response.url().to_string();
            let content_type = response
                .headers()
                .get("content-type")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("")
                .to_string();

            match response.text().await {
                Ok(body) => {
                    let extracted = if let Some(req) = extraction {
                        let result = extract_all(&body, req);
                        // Convert HashMap to JSON Value
                        serde_json::to_value(&result.values).ok()
                    } else {
                        None
                    };

                    CrawlResult {
                        url,
                        final_url,
                        status,
                        content_type,
                        body,
                        error: None,
                        extracted,
                        response_time_ms: start.elapsed().as_millis() as u64,
                    }
                }
                Err(e) => CrawlResult {
                    url: url.clone(),
                    final_url: url,
                    status,
                    content_type,
                    body: String::new(),
                    error: Some(format!("Body read error: {}", e)),
                    extracted: None,
                    response_time_ms: start.elapsed().as_millis() as u64,
                },
            }
        }
        Err(e) => CrawlResult {
            url: url.clone(),
            final_url: url,
            status: 0,
            content_type: String::new(),
            body: String::new(),
            error: Some(e.to_string()),
            extracted: None,
            response_time_ms: start.elapsed().as_millis() as u64,
        },
    }
}

/// Batch crawl URLs with optional extraction
///
/// # Arguments
/// * `request_json` - JSON BatchCrawlRequest
///
/// # Returns
/// ExtractionResultFFI with JSON BatchCrawlResponse
#[no_mangle]
pub unsafe extern "C" fn crawl_batch_ffi(
    request_json: *const c_char,
) -> ExtractionResultFFI {
    let request_str = match CStr::from_ptr(request_json).to_str() {
        Ok(s) => s,
        Err(e) => {
            return ExtractionResultFFI {
                json_ptr: ptr::null_mut(),
                error_ptr: string_to_ptr(format!("Invalid UTF-8: {}", e)),
            };
        }
    };

    let request: BatchCrawlRequest = match serde_json::from_str(request_str) {
        Ok(r) => r,
        Err(e) => {
            return ExtractionResultFFI {
                json_ptr: ptr::null_mut(),
                error_ptr: string_to_ptr(format!("Invalid request: {}", e)),
            };
        }
    };

    // Build HTTP client with optional proxy
    let mut client_builder = reqwest::Client::builder()
        .user_agent(&request.user_agent)
        .timeout(Duration::from_millis(request.timeout_ms));

    // Configure proxy if provided
    if let Some(ref proxy_url) = request.http_proxy {
        if let Ok(mut proxy) = reqwest::Proxy::all(proxy_url) {
            // Add basic auth if credentials provided
            if let (Some(ref user), Some(ref pass)) = (&request.http_proxy_username, &request.http_proxy_password) {
                proxy = proxy.basic_auth(user, pass);
            }
            client_builder = client_builder.proxy(proxy);
        }
    }

    // Add extra headers if provided
    if let Some(ref headers) = request.extra_headers {
        let mut header_map = reqwest::header::HeaderMap::new();
        for (key, value) in headers {
            if let (Ok(name), Ok(val)) = (
                reqwest::header::HeaderName::from_bytes(key.as_bytes()),
                reqwest::header::HeaderValue::from_str(value),
            ) {
                header_map.insert(name, val);
            }
        }
        client_builder = client_builder.default_headers(header_map);
    }

    let client = match client_builder.build() {
        Ok(c) => c,
        Err(e) => {
            return ExtractionResultFFI {
                json_ptr: ptr::null_mut(),
                error_ptr: string_to_ptr(format!("Client build error: {}", e)),
            };
        }
    };

    // Run async crawl
    let runtime = match tokio::runtime::Runtime::new() {
        Ok(r) => r,
        Err(e) => {
            return ExtractionResultFFI {
                json_ptr: ptr::null_mut(),
                error_ptr: string_to_ptr(format!("Tokio runtime error: {}", e)),
            };
        }
    };

    let results = runtime.block_on(async {
        use futures::stream::{self, StreamExt};

        let concurrency = request.concurrency.max(1).min(32);
        let extraction = request.extraction.clone();
        let delay_ms = request.delay_ms;
        let respect_robots = request.respect_robots;
        let user_agent = request.user_agent.clone();
        let rate_limiter: DomainRateLimiter = Arc::new(Mutex::new(HashMap::new()));

        // Filter URLs by robots.txt if enabled
        let urls: Vec<String> = if respect_robots {
            let robots_cache = crate::robots::RobotsCache::new();
            let config = ureq::Agent::config_builder()
                .timeout_global(Some(Duration::from_secs(10)))
                .build();
            let blocking_agent = ureq::Agent::new_with_config(config);

            request.urls
                .into_iter()
                .filter(|url| {
                    let check = robots_cache.check_blocking(&blocking_agent, url, &user_agent);
                    check.allowed
                })
                .collect()
        } else {
            request.urls
        };

        // Process URLs with interrupt checking
        let mut results = Vec::new();
        let mut url_stream = stream::iter(urls)
            .map(|url| {
                let client = client.clone();
                let extraction = extraction.clone();
                let rate_limiter = rate_limiter.clone();
                async move { fetch_and_extract(&client, url, &extraction, &rate_limiter, delay_ms).await }
            })
            .buffer_unordered(concurrency);

        while let Some(result) = url_stream.next().await {
            results.push(result);
            // Check for interrupt after each result
            if INTERRUPTED.load(Ordering::SeqCst) {
                break;
            }
        }
        results
    });

    let response = BatchCrawlResponse { results };

    match serde_json::to_string(&response) {
        Ok(json) => ExtractionResultFFI {
            json_ptr: string_to_ptr(json),
            error_ptr: ptr::null_mut(),
        },
        Err(e) => ExtractionResultFFI {
            json_ptr: ptr::null_mut(),
            error_ptr: string_to_ptr(format!("Serialization error: {}", e)),
        },
    }
}

// ============================================================================
// Sitemap Fetching
// ============================================================================

/// Request for sitemap fetching
#[derive(Debug, serde::Deserialize)]
struct SitemapRequest {
    url: String,
    #[serde(default = "default_true")]
    recursive: bool,
    #[serde(default = "default_max_depth")]
    max_depth: usize,
    #[serde(default = "default_user_agent")]
    user_agent: String,
    #[serde(default = "default_timeout")]
    timeout_ms: u64,
    #[serde(default)]
    discover_from_robots: bool,
}

fn default_true() -> bool {
    true
}

fn default_max_depth() -> usize {
    5
}

/// Fetch and parse sitemap(s) - SIMPLE FFI (returns char* directly)
///
/// # Arguments
/// * `request_json` - JSON SitemapRequest
///
/// # Returns
/// JSON string pointer (caller must free with free_rust_string)
#[no_mangle]
pub unsafe extern "C" fn fetch_sitemap_simple(request_json: *const c_char) -> *mut c_char {
    // Wrap in catch_unwind to prevent panics from crashing the process
    let result = std::panic::catch_unwind(|| fetch_sitemap_simple_inner(request_json));

    match result {
        Ok(ptr) => ptr,
        Err(_) => {
            string_to_ptr("{\"urls\":[],\"sitemaps\":[],\"errors\":[\"Panic in sitemap fetch\"]}".to_string())
        }
    }
}

unsafe fn fetch_sitemap_simple_inner(request_json: *const c_char) -> *mut c_char {
    let request_str = match CStr::from_ptr(request_json).to_str() {
        Ok(s) => s,
        Err(e) => {
            return string_to_ptr(format!("{{\"urls\":[],\"sitemaps\":[],\"errors\":[\"Invalid UTF-8: {}\"]}}", e));
        }
    };

    let request: SitemapRequest = match serde_json::from_str(request_str) {
        Ok(r) => r,
        Err(e) => {
            return string_to_ptr(format!("{{\"urls\":[],\"sitemaps\":[],\"errors\":[\"Invalid request: {}\"]}}", e));
        }
    };

    let timeout_secs = (request.timeout_ms / 1000).max(1);

    let mut sitemap_urls = vec![request.url.clone()];

    // If discover_from_robots, first check robots.txt for sitemap URLs
    if request.discover_from_robots {
        let robots_cache = crate::robots::RobotsCache::new();
        let agent = ureq::Agent::new_with_config(
            ureq::Agent::config_builder()
                .timeout_global(Some(Duration::from_secs(timeout_secs)))
                .user_agent(&request.user_agent)
                .build(),
        );
        let sitemaps =
            robots_cache.get_sitemaps_blocking(&agent, &request.url, &request.user_agent);
        if !sitemaps.is_empty() {
            sitemap_urls = sitemaps;
        }
    }

    let mut combined = crate::sitemap::SitemapResult {
        urls: vec![],
        sitemaps: vec![],
        errors: vec![],
    };

    for sitemap_url in sitemap_urls {
        let result = crate::sitemap::fetch_sitemap_blocking(
            &sitemap_url,
            &request.user_agent,
            timeout_secs,
            request.recursive,
            request.max_depth,
        );
        combined.urls.extend(result.urls);
        combined.sitemaps.extend(result.sitemaps);
        combined.errors.extend(result.errors);
    }

    match serde_json::to_string(&combined) {
        Ok(json) => string_to_ptr(json),
        Err(e) => {
            string_to_ptr(format!("{{\"urls\":[],\"sitemaps\":[],\"errors\":[\"Serialization error: {}\"]}}", e))
        }
    }
}

/// Free a string allocated by Rust
#[no_mangle]
pub unsafe extern "C" fn free_rust_string(ptr: *mut c_char) {
    if !ptr.is_null() {
        drop(CString::from_raw(ptr));
    }
}

/// Fetch and parse sitemap(s) - OLD struct-based FFI (deprecated)
#[no_mangle]
pub unsafe extern "C" fn fetch_sitemap_ffi(request_json: *const c_char) -> ExtractionResultFFI {
    // Wrap in catch_unwind to prevent panics from crashing the process
    let result = std::panic::catch_unwind(|| fetch_sitemap_ffi_inner(request_json));

    match result {
        Ok(r) => r,
        Err(_) => ExtractionResultFFI {
            json_ptr: ptr::null_mut(),
            error_ptr: string_to_ptr("Panic in sitemap fetch".to_string()),
        },
    }
}

unsafe fn fetch_sitemap_ffi_inner(request_json: *const c_char) -> ExtractionResultFFI {
    // Use the simple version and wrap result
    let json_ptr = fetch_sitemap_simple(request_json);
    ExtractionResultFFI {
        json_ptr,
        error_ptr: ptr::null_mut(),
    }
}

// ============================================================================
// Robots.txt Checking
// ============================================================================

/// Request for robots.txt check
#[derive(Debug, serde::Deserialize)]
struct RobotsCheckRequest {
    url: String,
    #[serde(default = "default_user_agent")]
    user_agent: String,
    #[serde(default = "default_timeout")]
    timeout_ms: u64,
}

/// Check if URL is allowed by robots.txt
#[no_mangle]
pub unsafe extern "C" fn check_robots_ffi(request_json: *const c_char) -> ExtractionResultFFI {
    // Wrap in catch_unwind to prevent panics from crashing the process
    let result = std::panic::catch_unwind(|| check_robots_ffi_inner(request_json));

    match result {
        Ok(r) => r,
        Err(_) => ExtractionResultFFI {
            json_ptr: ptr::null_mut(),
            error_ptr: string_to_ptr("Panic in robots check".to_string()),
        },
    }
}

unsafe fn check_robots_ffi_inner(request_json: *const c_char) -> ExtractionResultFFI {
    let request_str = match CStr::from_ptr(request_json).to_str() {
        Ok(s) => s,
        Err(e) => {
            return ExtractionResultFFI {
                json_ptr: ptr::null_mut(),
                error_ptr: string_to_ptr(format!("Invalid UTF-8: {}", e)),
            };
        }
    };

    let request: RobotsCheckRequest = match serde_json::from_str(request_str) {
        Ok(r) => r,
        Err(e) => {
            return ExtractionResultFFI {
                json_ptr: ptr::null_mut(),
                error_ptr: string_to_ptr(format!("Invalid request: {}", e)),
            };
        }
    };

    let timeout_secs = (request.timeout_ms / 1000).max(1);

    // Build ureq agent
    let agent = ureq::Agent::new_with_config(
        ureq::Agent::config_builder()
            .timeout_global(Some(Duration::from_secs(timeout_secs)))
            .user_agent(&request.user_agent)
            .build(),
    );

    let robots_cache = crate::robots::RobotsCache::new();
    let result = robots_cache.check_blocking(&agent, &request.url, &request.user_agent);

    match serde_json::to_string(&result) {
        Ok(json) => ExtractionResultFFI {
            json_ptr: string_to_ptr(json),
            error_ptr: ptr::null_mut(),
        },
        Err(e) => ExtractionResultFFI {
            json_ptr: ptr::null_mut(),
            error_ptr: string_to_ptr(format!("Serialization error: {}", e)),
        },
    }
}
