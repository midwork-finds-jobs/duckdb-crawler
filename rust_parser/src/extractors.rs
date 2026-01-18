//! HTML content extractors

use scraper::{Html, Selector};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use swc_common::{sync::Lrc, SourceMap, FileName};
use swc_ecma_parser::{lexer::Lexer, Parser, StringInput, Syntax};
use swc_ecma_ast::*;

/// Extraction request from C++
#[derive(Debug, Clone, Deserialize)]
pub struct ExtractionRequest {
    pub specs: Vec<ExtractSpec>,
}

/// Single extraction specification
#[derive(Debug, Clone, Deserialize)]
pub struct ExtractSpec {
    /// Source type: jsonld, microdata, og, meta, css
    pub source: String,
    /// Path segments for structured data (e.g., ["Product", "name"])
    #[serde(default)]
    pub path: Vec<String>,
    /// CSS selector for css source
    #[serde(default)]
    pub selector: Option<String>,
    /// Accessor for CSS: text, html, or attr name
    #[serde(default)]
    pub accessor: Option<String>,
    /// Whether to return text (true) or JSON (false)
    #[serde(default)]
    pub return_text: bool,
    /// Output column alias
    pub alias: String,
    /// For COALESCE: alternative specs to try
    #[serde(default)]
    pub alternatives: Vec<ExtractSpec>,
    /// Whether to parse extracted value as JSON (::json suffix)
    #[serde(default)]
    pub is_json_cast: bool,
    /// Whether to expand arrays into multiple rows ([*] suffix)
    #[serde(default)]
    pub expand_array: bool,
    /// Field to extract from each array element (e.g., "id" for [*].id)
    #[serde(default)]
    pub array_field: Option<String>,
    /// Additional JSON path after cast (e.g., ->'items')
    #[serde(default)]
    pub json_path: Option<String>,
}

/// Extraction result
#[derive(Debug, Serialize)]
pub struct ExtractionResult {
    /// Map of alias -> extracted value (as JSON string or text)
    /// For non-expanded results: single value
    /// For expanded arrays: JSON array of values
    pub values: HashMap<String, Option<String>>,
    /// Map of alias -> array of expanded values (only for expand_array=true specs)
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub expanded_values: HashMap<String, Vec<String>>,
    /// Any errors encountered
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Extract all requested data from HTML
pub fn extract_all(html: &str, request: &ExtractionRequest) -> ExtractionResult {
    let document = Html::parse_document(html);
    let mut values = HashMap::new();
    let mut expanded_values = HashMap::new();

    // Pre-extract structured data once
    let jsonld_data = extract_jsonld_objects(&document);
    let microdata = extract_microdata(&document);
    let og_data = extract_opengraph(&document);
    let meta_data = extract_meta_tags(&document);
    let js_data = extract_js_variables(&document);

    for spec in &request.specs {
        let raw_value = extract_single(
            &document,
            spec,
            &jsonld_data,
            &microdata,
            &og_data,
            &meta_data,
            &js_data,
        );

        // Handle JSON cast and array expansion
        if spec.is_json_cast || spec.expand_array {
            if let Some(ref raw) = raw_value {
                // Parse as JSON
                match serde_json::from_str::<Value>(raw) {
                    Ok(json_val) => {
                        // Navigate json_path if specified
                        let target = if let Some(ref path) = spec.json_path {
                            navigate_json_path(&json_val, path)
                        } else {
                            Some(json_val)
                        };

                        if let Some(val) = target {
                            if spec.expand_array {
                                // Expand array into multiple values
                                if let Value::Array(arr) = val {
                                    let expanded: Vec<String> = if let Some(ref field) = spec.array_field {
                                        // Extract specific field from each element
                                        arr.iter()
                                            .filter_map(|item| {
                                                item.get(field).map(|v| {
                                                    if let Value::String(s) = v {
                                                        s.clone()
                                                    } else {
                                                        v.to_string()
                                                    }
                                                })
                                            })
                                            .collect()
                                    } else {
                                        // Return each element as-is
                                        arr.iter()
                                            .map(|v| {
                                                if let Value::String(s) = v {
                                                    s.clone()
                                                } else {
                                                    v.to_string()
                                                }
                                            })
                                            .collect()
                                    };
                                    expanded_values.insert(spec.alias.clone(), expanded);
                                    values.insert(spec.alias.clone(), None);
                                } else {
                                    // Not an array, just return the value
                                    values.insert(spec.alias.clone(), Some(val.to_string()));
                                }
                            } else {
                                // Just JSON cast, no expansion
                                values.insert(spec.alias.clone(), Some(val.to_string()));
                            }
                        } else {
                            values.insert(spec.alias.clone(), None);
                        }
                    }
                    Err(_) => {
                        // JSON parse failed, return raw value
                        values.insert(spec.alias.clone(), raw_value);
                    }
                }
            } else {
                values.insert(spec.alias.clone(), None);
            }
        } else {
            values.insert(spec.alias.clone(), raw_value);
        }
    }

    ExtractionResult { values, expanded_values, error: None }
}

/// Navigate a JSON value using arrow notation path
fn navigate_json_path(value: &Value, path: &str) -> Option<Value> {
    let mut current = value.clone();
    let mut remaining = path.trim();

    while !remaining.is_empty() {
        // Skip arrow
        if remaining.starts_with("->>") {
            remaining = &remaining[3..];
        } else if remaining.starts_with("->") {
            remaining = &remaining[2..];
        } else {
            break;
        }

        remaining = remaining.trim_start();

        // Extract key
        let key = if remaining.starts_with('\'') {
            let end = remaining[1..].find('\'')?;
            let k = &remaining[1..=end];
            remaining = &remaining[end + 2..];
            k.to_string()
        } else if remaining.starts_with('"') {
            let end = remaining[1..].find('"')?;
            let k = &remaining[1..=end];
            remaining = &remaining[end + 2..];
            k.to_string()
        } else if remaining.starts_with('[') {
            let end = remaining.find(']')?;
            let idx = &remaining[1..end];
            remaining = &remaining[end + 1..];
            idx.to_string()
        } else {
            let end = remaining.find("->").unwrap_or(remaining.len());
            let k = remaining[..end].trim();
            remaining = &remaining[end..];
            k.to_string()
        };

        // Navigate
        if let Ok(idx) = key.parse::<usize>() {
            current = current.get(idx)?.clone();
        } else {
            current = current.get(&key)?.clone();
        }
    }

    Some(current)
}

/// Extract a single value based on spec
fn extract_single(
    document: &Html,
    spec: &ExtractSpec,
    jsonld_data: &HashMap<String, Value>,
    microdata: &HashMap<String, Value>,
    og_data: &HashMap<String, String>,
    meta_data: &HashMap<String, String>,
    js_data: &HashMap<String, Value>,
) -> Option<String> {
    // Try main spec first
    let result = match spec.source.as_str() {
        "jsonld" => extract_from_jsonld(jsonld_data, &spec.path, spec.return_text),
        "microdata" => extract_from_microdata(microdata, &spec.path, spec.return_text),
        "og" => extract_from_og(og_data, &spec.path),
        "meta" => extract_from_meta(meta_data, &spec.path),
        "css" => extract_from_css(document, spec.selector.as_deref(), spec.accessor.as_deref()),
        "js" => extract_from_js(js_data, &spec.path, spec.return_text),
        _ => None,
    };

    // If no result and we have alternatives (COALESCE), try them
    if result.is_none() && !spec.alternatives.is_empty() {
        for alt in &spec.alternatives {
            let alt_result = extract_single(document, alt, jsonld_data, microdata, og_data, meta_data, js_data);
            if alt_result.is_some() {
                return alt_result;
            }
        }
    }

    result
}

/// Extract JSON-LD objects from script tags, keyed by @type
pub fn extract_jsonld_objects(document: &Html) -> HashMap<String, Value> {
    let mut result = HashMap::new();
    let selector = Selector::parse(r#"script[type="application/ld+json"]"#).unwrap();

    for element in document.select(&selector) {
        let text = element.text().collect::<String>();
        if let Ok(json) = serde_json::from_str::<Value>(&text) {
            collect_typed_objects(&json, &mut result);
        }
    }

    result
}

/// Recursively collect objects with @type, including from @graph
fn collect_typed_objects(value: &Value, result: &mut HashMap<String, Value>) {
    match value {
        Value::Object(obj) => {
            // Check for @graph array
            if let Some(Value::Array(graph)) = obj.get("@graph") {
                for item in graph {
                    collect_typed_objects(item, result);
                }
            }

            // Check for @type
            if let Some(type_val) = obj.get("@type") {
                let types = match type_val {
                    Value::String(s) => vec![s.clone()],
                    Value::Array(arr) => arr
                        .iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect(),
                    _ => vec![],
                };

                for t in types {
                    // Strip schema.org prefix if present
                    let clean_type = t
                        .strip_prefix("https://schema.org/")
                        .or_else(|| t.strip_prefix("http://schema.org/"))
                        .unwrap_or(&t)
                        .to_string();
                    result.insert(clean_type, value.clone());
                }
            }
        }
        Value::Array(arr) => {
            for item in arr {
                collect_typed_objects(item, result);
            }
        }
        _ => {}
    }
}

/// Navigate JSON-LD data by path
fn extract_from_jsonld(
    data: &HashMap<String, Value>,
    path: &[String],
    return_text: bool,
) -> Option<String> {
    if path.is_empty() {
        return None;
    }

    // First segment is the @type
    let type_name = &path[0];
    let obj = data.get(type_name)?;

    // Navigate remaining path
    let mut current = obj;
    for segment in path.iter().skip(1) {
        current = current.get(segment)?;
    }

    value_to_string(current, return_text)
}

/// Extract microdata from HTML
pub fn extract_microdata(document: &Html) -> HashMap<String, Value> {
    let mut result = HashMap::new();
    let selector = Selector::parse("[itemscope][itemtype]").unwrap();

    for element in document.select(&selector) {
        if let Some(itemtype) = element.value().attr("itemtype") {
            // Extract type name from URL
            let type_name = itemtype
                .rsplit('/')
                .next()
                .unwrap_or(itemtype)
                .to_string();

            let mut props = serde_json::Map::new();

            // Find all itemprop within this scope
            let prop_selector = Selector::parse("[itemprop]").unwrap();
            for prop_el in element.select(&prop_selector) {
                if let Some(prop_name) = prop_el.value().attr("itemprop") {
                    let value = prop_el
                        .value()
                        .attr("content")
                        .or_else(|| prop_el.value().attr("href"))
                        .or_else(|| prop_el.value().attr("src"))
                        .map(String::from)
                        .unwrap_or_else(|| prop_el.text().collect::<String>().trim().to_string());

                    props.insert(prop_name.to_string(), Value::String(value));
                }
            }

            result.insert(type_name, Value::Object(props));
        }
    }

    result
}

/// Navigate microdata by path
fn extract_from_microdata(
    data: &HashMap<String, Value>,
    path: &[String],
    return_text: bool,
) -> Option<String> {
    if path.is_empty() {
        return None;
    }

    let type_name = &path[0];
    let obj = data.get(type_name)?;

    let mut current = obj;
    for segment in path.iter().skip(1) {
        current = current.get(segment)?;
    }

    value_to_string(current, return_text)
}

/// Extract OpenGraph meta tags
pub fn extract_opengraph(document: &Html) -> HashMap<String, String> {
    let mut result = HashMap::new();
    let selector = Selector::parse(r#"meta[property^="og:"]"#).unwrap();

    for element in document.select(&selector) {
        if let (Some(prop), Some(content)) = (
            element.value().attr("property"),
            element.value().attr("content"),
        ) {
            let key = prop.strip_prefix("og:").unwrap_or(prop).to_string();
            result.insert(key, content.to_string());
        }
    }

    result
}

/// Extract from OpenGraph data
fn extract_from_og(data: &HashMap<String, String>, path: &[String]) -> Option<String> {
    if path.is_empty() {
        return None;
    }
    data.get(&path[0]).cloned()
}

/// Extract meta tags
fn extract_meta_tags(document: &Html) -> HashMap<String, String> {
    let mut result = HashMap::new();
    let selector = Selector::parse("meta[name]").unwrap();

    for element in document.select(&selector) {
        if let (Some(name), Some(content)) = (
            element.value().attr("name"),
            element.value().attr("content"),
        ) {
            result.insert(name.to_string(), content.to_string());
        }
    }

    result
}

/// Extract from meta tags
fn extract_from_meta(data: &HashMap<String, String>, path: &[String]) -> Option<String> {
    if path.is_empty() {
        return None;
    }
    data.get(&path[0]).cloned()
}

/// Extract using CSS selector
fn extract_from_css(
    document: &Html,
    selector_str: Option<&str>,
    accessor: Option<&str>,
) -> Option<String> {
    let selector_str = selector_str?;
    let selector = Selector::parse(selector_str).ok()?;
    let element = document.select(&selector).next()?;

    let accessor = accessor.unwrap_or("text");

    // Check for parent navigation: parent.text, parent.html, parent.attr:name
    if let Some(rest) = accessor.strip_prefix("parent.") {
        // Navigate to parent element
        let parent = element.parent()?;
        let parent_el = scraper::ElementRef::wrap(parent)?;

        return match rest {
            "html" => Some(parent_el.html()),
            "text" => {
                let text = parent_el.text().collect::<String>();
                Some(text.trim().to_string())
            }
            attr if attr.starts_with("attr:") => {
                let attr_name = attr.strip_prefix("attr:")?;
                parent_el.value().attr(attr_name).map(String::from)
            }
            _ => {
                let text = parent_el.text().collect::<String>();
                Some(text.trim().to_string())
            }
        };
    }

    // Check for children navigation: children.N.text, children.N.html, children.N.attr:name
    if let Some(rest) = accessor.strip_prefix("children.") {
        // Parse index and accessor: "0.text" or "1.attr:href"
        let dot_pos = rest.find('.')?;
        let index_str = &rest[..dot_pos];
        let child_accessor = &rest[dot_pos + 1..];
        let index: usize = index_str.parse().ok()?;

        // Get child elements (only element nodes, skip text nodes)
        let child_el = element.children()
            .filter_map(|node| scraper::ElementRef::wrap(node))
            .nth(index)?;

        return match child_accessor {
            "html" => Some(child_el.html()),
            "text" => {
                let text = child_el.text().collect::<String>();
                Some(text.trim().to_string())
            }
            attr if attr.starts_with("attr:") => {
                let attr_name = attr.strip_prefix("attr:")?;
                child_el.value().attr(attr_name).map(String::from)
            }
            _ => {
                let text = child_el.text().collect::<String>();
                Some(text.trim().to_string())
            }
        };
    }

    match accessor {
        "html" => Some(element.html()),
        attr if attr.starts_with("attr:") => {
            let attr_name = attr.strip_prefix("attr:")?;
            element.value().attr(attr_name).map(String::from)
        }
        _ => {
            // Default: text content
            let text = element.text().collect::<String>();
            Some(text.trim().to_string())
        }
    }
}

/// Extract element as structured data with text, html, and all attributes
/// Returns JSON: {"text": "...", "html": "...", "attr": {"href": "...", "class": "..."}}
/// Extract element data from HTML using CSS selector
///
/// Supports special syntax:
/// - `selector @attr` - returns just the attribute value as a string
/// - `selector` - returns full struct with text, html, attr map
pub fn extract_element(html: &str, selector: &str) -> Option<serde_json::Value> {
    let document = Html::parse_document(html);

    // Check for @attr suffix (e.g., "input#jobs @value")
    if let Some(at_pos) = selector.rfind(" @") {
        let css_selector = selector[..at_pos].trim();
        let attr_name = selector[at_pos + 2..].trim();

        let sel = Selector::parse(css_selector).ok()?;
        let element = document.select(&sel).next()?;

        // Return just the attribute value as a string
        let attr_value = element.value().attr(attr_name)?;
        return Some(serde_json::Value::String(attr_value.to_string()));
    }

    // Standard mode: return full struct
    let sel = Selector::parse(selector).ok()?;
    let element = document.select(&sel).next()?;

    // Get text content
    let text = element.text().collect::<String>().trim().to_string();

    // Get inner HTML
    let inner_html = element.inner_html();

    // Get all attributes as a map
    let mut attrs = serde_json::Map::new();
    for (name, value) in element.value().attrs() {
        attrs.insert(name.to_string(), serde_json::Value::String(value.to_string()));
    }

    Some(serde_json::json!({
        "text": text,
        "html": inner_html,
        "attr": attrs
    }))
}

/// Unified path selector for HTML extraction
///
/// Syntax: `css_selector@attr[*].json.path`
///
/// Examples:
/// - `input#jobs@value` - get value attribute of input#jobs
/// - `input#jobs@value[*]` - parse as JSON array, return all elements
/// - `input#jobs@value[*].id` - extract 'id' from each array element
/// - `.product@data-json.price` - get data-json attr, extract .price
/// - `script#config@text.settings.theme` - get text content, navigate JSON
///
/// Returns:
/// - Single value: JSON string
/// - Array with [*]: JSON array of values
pub fn extract_path(html: &str, path: &str) -> Option<serde_json::Value> {
    let document = Html::parse_document(html);

    // Parse the path syntax
    let (css_selector, attr_name, expand_array, json_path) = parse_path_syntax(path)?;

    // Get the CSS element
    let sel = Selector::parse(&css_selector).ok()?;
    let element = document.select(&sel).next()?;

    // Get the raw value (attribute or text)
    let raw_value = if attr_name == "text" || attr_name == "innerText" {
        element.text().collect::<String>().trim().to_string()
    } else if attr_name == "html" || attr_name == "innerHTML" {
        element.inner_html()
    } else {
        element.value().attr(&attr_name)?.to_string()
    };

    // If no JSON path and no array expansion, return raw string
    if json_path.is_empty() && !expand_array {
        return Some(serde_json::Value::String(raw_value));
    }

    // Parse as JSON
    let json_val: serde_json::Value = serde_json::from_str(&raw_value).ok()?;

    // Handle array expansion
    if expand_array {
        let arr = json_val.as_array()?;

        if json_path.is_empty() {
            // Return the whole array
            return Some(serde_json::Value::Array(arr.clone()));
        }

        // Extract field from each element
        let extracted: Vec<serde_json::Value> = arr
            .iter()
            .filter_map(|item| navigate_json(&item, &json_path))
            .collect();

        return Some(serde_json::Value::Array(extracted));
    }

    // Navigate JSON path without array expansion
    navigate_json(&json_val, &json_path)
}

/// Parse the unified path syntax
/// Returns: (css_selector, attr_name, expand_array, json_path_parts)
fn parse_path_syntax(path: &str) -> Option<(String, String, bool, Vec<String>)> {
    let mut remaining = path.trim();

    // Find @ for attribute (scan backwards to handle @ in CSS selectors)
    let at_pos = remaining.rfind('@')?;

    let css_selector = remaining[..at_pos].trim().to_string();
    remaining = &remaining[at_pos + 1..];

    // Check for [*] array expansion
    let expand_array;
    let after_bracket;
    if let Some(bracket_pos) = remaining.find("[*]") {
        expand_array = true;
        let attr_part = &remaining[..bracket_pos];
        after_bracket = &remaining[bracket_pos + 3..];
        remaining = attr_part;

        // Get JSON path after [*]
        let json_path = if after_bracket.starts_with('.') {
            after_bracket[1..].split('.').map(|s| s.to_string()).collect()
        } else {
            Vec::new()
        };

        let attr_name = remaining.split('.').next()?.to_string();
        return Some((css_selector, attr_name, expand_array, json_path));
    } else {
        expand_array = false;
    }

    // Parse attr.json.path
    let parts: Vec<&str> = remaining.split('.').collect();
    let attr_name = parts[0].to_string();
    let json_path: Vec<String> = parts[1..].iter().map(|s| s.to_string()).collect();

    Some((css_selector, attr_name, expand_array, json_path))
}

/// Navigate a JSON value by path parts
fn navigate_json(value: &serde_json::Value, path: &[String]) -> Option<serde_json::Value> {
    let mut current = value.clone();

    for part in path {
        current = if let Ok(idx) = part.parse::<usize>() {
            current.get(idx)?.clone()
        } else {
            current.get(part)?.clone()
        };
    }

    Some(current)
}

/// Extract JavaScript variables from script tags using AST parsing
pub fn extract_js_variables(document: &Html) -> HashMap<String, Value> {
    let mut result = HashMap::new();
    let selector = Selector::parse("script:not([type]), script[type='text/javascript']").unwrap();

    for element in document.select(&selector) {
        let script_text = element.text().collect::<String>();

        // Parse with SWC and extract variables
        if let Some(vars) = parse_js_and_extract_vars(&script_text) {
            for (name, value) in vars {
                result.insert(name, value);
            }
        }
    }

    result
}

/// Parse JavaScript source and extract variable declarations
fn parse_js_and_extract_vars(source: &str) -> Option<HashMap<String, Value>> {
    let cm: Lrc<SourceMap> = Default::default();
    let fm = cm.new_source_file(FileName::Anon.into(), source.to_string());

    let lexer = Lexer::new(
        Syntax::Es(Default::default()),
        Default::default(),
        StringInput::from(&*fm),
        None,
    );

    let mut parser = Parser::new_from(lexer);

    // Try to parse as script, ignoring errors (JS in HTML often has issues)
    let script = match parser.parse_script() {
        Ok(s) => s,
        Err(_) => return None,
    };

    let mut result = HashMap::new();

    for stmt in &script.body {
        extract_vars_from_stmt(stmt, &mut result);
    }

    Some(result)
}

/// Extract variable declarations from a statement
fn extract_vars_from_stmt(stmt: &Stmt, result: &mut HashMap<String, Value>) {
    match stmt {
        Stmt::Decl(Decl::Var(var_decl)) => {
            for decl in &var_decl.decls {
                if let Some(init) = &decl.init {
                    if let Pat::Ident(ident) = &decl.name {
                        let var_name = ident.sym.as_str().to_string();
                        if let Some(value) = expr_to_json(init) {
                            result.insert(var_name, value);
                        }
                    }
                }
            }
        }
        Stmt::Expr(expr_stmt) => {
            // Handle: varName = value (assignment expressions)
            if let Expr::Assign(assign) = &*expr_stmt.expr {
                if let AssignTarget::Simple(SimpleAssignTarget::Ident(ident)) = &assign.left {
                    let var_name = ident.sym.as_str().to_string();
                    if let Some(value) = expr_to_json(&assign.right) {
                        result.insert(var_name, value);
                    }
                }
            }
        }
        _ => {}
    }
}

/// Convert a JavaScript expression to a JSON Value
fn expr_to_json(expr: &Expr) -> Option<Value> {
    match expr {
        // String literals
        Expr::Lit(Lit::Str(s)) => Some(Value::String(s.value.as_str().unwrap_or("").to_string())),

        // Number literals
        Expr::Lit(Lit::Num(n)) => {
            if n.value.fract() == 0.0 && n.value >= i64::MIN as f64 && n.value <= i64::MAX as f64 {
                Some(Value::Number(serde_json::Number::from(n.value as i64)))
            } else {
                serde_json::Number::from_f64(n.value).map(Value::Number)
            }
        }

        // Boolean literals
        Expr::Lit(Lit::Bool(b)) => Some(Value::Bool(b.value)),

        // Null literal
        Expr::Lit(Lit::Null(_)) => Some(Value::Null),

        // Object literals: { key: value, ... }
        Expr::Object(obj) => {
            let mut map = serde_json::Map::new();
            for prop in &obj.props {
                if let PropOrSpread::Prop(prop) = prop {
                    if let Prop::KeyValue(kv) = &**prop {
                        let key = prop_name_to_string(&kv.key)?;
                        let value = expr_to_json(&kv.value)?;
                        map.insert(key, value);
                    }
                }
            }
            Some(Value::Object(map))
        }

        // Array literals: [a, b, c]
        Expr::Array(arr) => {
            let mut values = Vec::new();
            for elem in &arr.elems {
                if let Some(ExprOrSpread { expr, .. }) = elem {
                    if let Some(value) = expr_to_json(expr) {
                        values.push(value);
                    } else {
                        values.push(Value::Null);
                    }
                } else {
                    values.push(Value::Null);
                }
            }
            Some(Value::Array(values))
        }

        // JSON.parse('...') call
        Expr::Call(call) => {
            if is_json_parse_call(call) {
                if let Some(ExprOrSpread { expr: arg, .. }) = call.args.first() {
                    if let Expr::Lit(Lit::Str(s)) = &**arg {
                        // Parse the string as JSON
                        if let Some(str_val) = s.value.as_str() {
                            if let Ok(value) = serde_json::from_str(str_val) {
                                return Some(value);
                            }
                        }
                    }
                }
            }
            None
        }

        // Unary expressions: -5, +3
        Expr::Unary(unary) => {
            if unary.op == UnaryOp::Minus {
                if let Expr::Lit(Lit::Num(n)) = &*unary.arg {
                    let neg = -n.value;
                    if neg.fract() == 0.0 && neg >= i64::MIN as f64 && neg <= i64::MAX as f64 {
                        return Some(Value::Number(serde_json::Number::from(neg as i64)));
                    } else {
                        return serde_json::Number::from_f64(neg).map(Value::Number);
                    }
                }
            }
            None
        }

        // Template literals without expressions: `string`
        Expr::Tpl(tpl) if tpl.exprs.is_empty() => {
            if let Some(quasi) = tpl.quasis.first() {
                Some(Value::String(quasi.raw.as_str().to_string()))
            } else {
                None
            }
        }

        _ => None,
    }
}

/// Check if a call expression is JSON.parse(...)
fn is_json_parse_call(call: &CallExpr) -> bool {
    if let Callee::Expr(expr) = &call.callee {
        if let Expr::Member(member) = &**expr {
            if let Expr::Ident(obj) = &*member.obj {
                if obj.sym.as_ref() == "JSON" {
                    if let MemberProp::Ident(prop) = &member.prop {
                        return prop.sym.as_ref() == "parse";
                    }
                }
            }
        }
    }
    false
}

/// Convert property name to string
fn prop_name_to_string(name: &PropName) -> Option<String> {
    match name {
        PropName::Ident(ident) => Some(ident.sym.as_str().to_string()),
        PropName::Str(s) => s.value.as_str().map(|v| v.to_string()),
        PropName::Num(n) => Some(n.value.to_string()),
        _ => None,
    }
}

/// Navigate JS variable data by path
fn extract_from_js(
    data: &HashMap<String, Value>,
    path: &[String],
    return_text: bool,
) -> Option<String> {
    if path.is_empty() {
        return None;
    }

    // First segment is the variable name
    let var_name = &path[0];
    let obj = data.get(var_name)?;

    // Navigate remaining path
    let mut current = obj;
    for segment in path.iter().skip(1) {
        // Try as object key first, then as array index
        current = current.get(segment).or_else(|| {
            segment.parse::<usize>().ok().and_then(|idx| current.get(idx))
        })?;
    }

    value_to_string(current, return_text)
}

/// Convert JSON value to string
fn value_to_string(value: &Value, return_text: bool) -> Option<String> {
    if return_text {
        match value {
            Value::String(s) => Some(s.clone()),
            Value::Null => None,
            _ => Some(value.to_string()),
        }
    } else {
        Some(value.to_string())
    }
}

/// Extract links from HTML using a CSS selector
/// Returns a list of absolute URLs
pub fn extract_links(html: &str, selector: &str, base_url: &str) -> Vec<String> {
    let document = Html::parse_document(html);
    let mut links = Vec::new();

    // Parse the base URL for resolving relative links
    let base = match url::Url::parse(base_url) {
        Ok(u) => u,
        Err(_) => return links,
    };

    // Parse selector - default to 'a[href]' if empty
    let sel_str = if selector.is_empty() { "a[href]" } else { selector };
    let sel = match Selector::parse(sel_str) {
        Ok(s) => s,
        Err(_) => return links,
    };

    for element in document.select(&sel) {
        // Get href attribute
        if let Some(href) = element.value().attr("href") {
            // Skip empty, javascript:, mailto:, tel:, and anchor links
            let href_trimmed = href.trim();
            if href_trimmed.is_empty()
                || href_trimmed.starts_with("javascript:")
                || href_trimmed.starts_with("mailto:")
                || href_trimmed.starts_with("tel:")
                || href_trimmed.starts_with('#')
            {
                continue;
            }

            // Resolve relative URL
            match base.join(href_trimmed) {
                Ok(absolute_url) => {
                    // Only include http/https URLs
                    if absolute_url.scheme() == "http" || absolute_url.scheme() == "https" {
                        links.push(absolute_url.to_string());
                    }
                }
                Err(_) => continue,
            }
        }
    }

    links
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_jsonld_extraction() {
        let html = r#"
        <html>
        <head>
            <script type="application/ld+json">
            {
                "@context": "https://schema.org",
                "@type": "Product",
                "name": "Test Product",
                "offers": {
                    "@type": "Offer",
                    "price": "19.99"
                }
            }
            </script>
        </head>
        </html>
        "#;

        let document = Html::parse_document(html);
        let jsonld = extract_jsonld_objects(&document);

        assert!(jsonld.contains_key("Product"));
        let product = &jsonld["Product"];
        assert_eq!(product["name"], "Test Product");
        assert_eq!(product["offers"]["price"], "19.99");
    }

    #[test]
    fn test_css_extraction() {
        let html = r#"
        <html>
        <body>
            <div class="price">$19.99</div>
            <a class="link" href="/product/123">View</a>
        </body>
        </html>
        "#;

        let document = Html::parse_document(html);

        let price = extract_from_css(&document, Some("div.price"), Some("text"));
        assert_eq!(price, Some("$19.99".to_string()));

        let href = extract_from_css(&document, Some("a.link"), Some("attr:href"));
        assert_eq!(href, Some("/product/123".to_string()));
    }
}

#[test]
fn test_full_extraction() {
    let html = r#"
    <html>
    <head>
        <title>Test Product</title>
        <script type="application/ld+json">
        {
            "@context": "https://schema.org",
            "@type": "Product",
            "name": "Test Product",
            "offers": {
                "@type": "Offer",
                "price": "19.99"
            }
        }
        </script>
    </head>
    <body>
        <h1 class="product-name">Test Product</h1>
    </body>
    </html>
    "#;

    let request = ExtractionRequest {
        specs: vec![
            ExtractSpec {
                source: "jsonld".to_string(),
                path: vec!["Product".to_string(), "name".to_string()],
                selector: None,
                accessor: None,
                return_text: true,
                alias: "name".to_string(),
                alternatives: vec![],
                is_json_cast: false,
                expand_array: false,
                array_field: None,
                json_path: None,
            },
            ExtractSpec {
                source: "jsonld".to_string(),
                path: vec!["Product".to_string(), "offers".to_string(), "price".to_string()],
                selector: None,
                accessor: None,
                return_text: true,
                alias: "price".to_string(),
                alternatives: vec![],
                is_json_cast: false,
                expand_array: false,
                array_field: None,
                json_path: None,
            },
            ExtractSpec {
                source: "css".to_string(),
                path: vec![],
                selector: Some("title".to_string()),
                accessor: Some("text".to_string()),
                return_text: true,
                alias: "page_title".to_string(),
                alternatives: vec![],
                is_json_cast: false,
                expand_array: false,
                array_field: None,
                json_path: None,
            },
        ],
    };

    let result = extract_all(html, &request);
    println!("Result: {:?}", result);
    
    assert_eq!(result.values.get("name"), Some(&Some("Test Product".to_string())));
    assert_eq!(result.values.get("price"), Some(&Some("19.99".to_string())));
    assert_eq!(result.values.get("page_title"), Some(&Some("Test Product".to_string())));
}

#[test]
fn test_parent_accessor() {
    let html = r#"
    <html>
    <body>
        <div class="product-card" data-id="123">
            <span class="price">$29.99</span>
        </div>
        <a href="/category/tools" class="category-link">
            <span class="category-name">Tools</span>
        </a>
    </body>
    </html>
    "#;

    let document = Html::parse_document(html);

    // Test parent.attr - get parent's data-id attribute from price span
    let parent_attr = extract_from_css(&document, Some("span.price"), Some("parent.attr:data-id"));
    assert_eq!(parent_attr, Some("123".to_string()));

    // Test parent.attr - get parent's href from category name span
    let parent_href = extract_from_css(&document, Some("span.category-name"), Some("parent.attr:href"));
    assert_eq!(parent_href, Some("/category/tools".to_string()));

    // Test parent.text - get parent's full text
    let parent_text = extract_from_css(&document, Some("span.price"), Some("parent.text"));
    assert_eq!(parent_text, Some("$29.99".to_string()));
}

#[test]
fn test_children_accessor() {
    let html = r#"
    <html>
    <body>
        <ul class="menu">
            <li class="item"><a href="/home">Home</a></li>
            <li class="item"><a href="/about">About</a></li>
            <li class="item"><a href="/contact">Contact</a></li>
        </ul>
        <div class="container">
            <span class="first">First</span>
            <span class="second">Second</span>
        </div>
    </body>
    </html>
    "#;

    let document = Html::parse_document(html);

    // Test children[0].text - get first child's text
    let first_item = extract_from_css(&document, Some("ul.menu"), Some("children.0.text"));
    assert_eq!(first_item, Some("Home".to_string()));

    // Test children[1].text - get second child's text
    let second_item = extract_from_css(&document, Some("ul.menu"), Some("children.1.text"));
    assert_eq!(second_item, Some("About".to_string()));

    // Test children[0].html - get first child's HTML
    let first_html = extract_from_css(&document, Some("div.container"), Some("children.0.html"));
    assert!(first_html.unwrap().contains("First"));

    // Test nested: get href from first li's anchor
    let first_li = extract_from_css(&document, Some("ul.menu li:first-child a"), Some("attr:href"));
    assert_eq!(first_li, Some("/home".to_string()));
}

#[test]
fn test_attribute_selectors() {
    let html = r#"
    <html>
    <body>
        <ul>
            <li data-id="1">Item 1</li>
            <li data-id="2">Item 2</li>
            <li data-id="3">Item 3</li>
        </ul>
        <a href="/test" class="link primary">Link</a>
    </body>
    </html>
    "#;

    let document = Html::parse_document(html);

    // Test attribute existence [attr]
    let attr_exists = extract_from_css(&document, Some("li[data-id]"), Some("text"));
    assert_eq!(attr_exists, Some("Item 1".to_string()));

    // Test attribute equals [attr="value"]
    let attr_eq = extract_from_css(&document, Some(r#"li[data-id="2"]"#), Some("text"));
    assert_eq!(attr_eq, Some("Item 2".to_string()));

    // Test multiple classes
    let multi_class = extract_from_css(&document, Some("a.link.primary"), Some("text"));
    assert_eq!(multi_class, Some("Link".to_string()));

    // Test complex selector with attribute
    let complex = extract_from_css(&document, Some(r#"ul li[data-id="3"]"#), Some("text"));
    assert_eq!(complex, Some("Item 3".to_string()));
}

#[test]
fn test_js_extraction_json_parse() {
    let html = r#"
    <html>
    <head>
        <script>
        var jobs = JSON.parse('[{"id":"123","title":"Developer"}]');
        var page_id = '900000000022233';
        var meta = {"org_info":{"company_name":"Test Corp","id":"12345"},"page_id":"abc"};
        </script>
    </head>
    </html>
    "#;

    let document = Html::parse_document(html);
    let js_vars = extract_js_variables(&document);

    // Test JSON.parse extraction
    assert!(js_vars.contains_key("jobs"));
    let jobs = &js_vars["jobs"];
    assert!(jobs.is_array());
    assert_eq!(jobs[0]["id"], "123");
    assert_eq!(jobs[0]["title"], "Developer");

    // Test simple string extraction
    assert!(js_vars.contains_key("page_id"));
    assert_eq!(js_vars["page_id"], "900000000022233");

    // Test object literal extraction
    assert!(js_vars.contains_key("meta"));
    let meta = &js_vars["meta"];
    assert_eq!(meta["org_info"]["company_name"], "Test Corp");
    assert_eq!(meta["page_id"], "abc");
}

#[test]
fn test_js_extraction_escaped_strings() {
    let html = r#"
    <html>
    <head>
        <script>
        var data = JSON.parse('[{\x22name\x22:\x22Test\x22}]');
        </script>
    </head>
    </html>
    "#;

    let document = Html::parse_document(html);
    let js_vars = extract_js_variables(&document);

    assert!(js_vars.contains_key("data"));
    let data = &js_vars["data"];
    assert_eq!(data[0]["name"], "Test");
}

#[test]
fn test_extract_from_js() {
    let mut js_data = HashMap::new();
    js_data.insert(
        "meta".to_string(),
        serde_json::json!({
            "org_info": {
                "company_name": "Test Corp"
            }
        }),
    );
    js_data.insert(
        "jobs".to_string(),
        serde_json::json!([
            {"id": "1", "title": "Dev"},
            {"id": "2", "title": "Designer"}
        ]),
    );

    // Navigate nested object
    let result = extract_from_js(
        &js_data,
        &["meta".to_string(), "org_info".to_string(), "company_name".to_string()],
        true,
    );
    assert_eq!(result, Some("Test Corp".to_string()));

    // Navigate array by index
    let result = extract_from_js(
        &js_data,
        &["jobs".to_string(), "0".to_string(), "title".to_string()],
        true,
    );
    assert_eq!(result, Some("Dev".to_string()));

    // Get whole object as JSON
    let result = extract_from_js(
        &js_data,
        &["meta".to_string(), "org_info".to_string()],
        false,
    );
    assert!(result.is_some());
    assert!(result.unwrap().contains("company_name"));
}

#[test]
fn test_extract_links() {
    let html = r##"<!DOCTYPE html>
    <html>
    <body>
        <a href="/page1">Page 1</a>
        <a href="https://example.com/page2">Page 2</a>
        <a href="relative/path">Relative</a>
        <a href="javascript:void(0)">JS</a>
        <a href="mailto:test@test.com">Email</a>
        <a href="#anchor">Anchor</a>
    </body>
    </html>"##;

    let links = extract_links(html, "a[href]", "https://base.com/dir/");
    println!("Found links: {:?}", links);

    assert!(links.contains(&"https://base.com/page1".to_string()));
    assert!(links.contains(&"https://example.com/page2".to_string()));
    assert!(links.contains(&"https://base.com/dir/relative/path".to_string()));
    assert!(!links.iter().any(|l| l.contains("javascript:")));
    assert!(!links.iter().any(|l| l.contains("mailto:")));
    assert!(!links.iter().any(|l| l.contains("#anchor")));
}
