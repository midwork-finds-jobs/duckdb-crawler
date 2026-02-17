#pragma once

#include "duckdb.hpp"
#include <string>
#include <vector>

namespace duckdb {

// Parsed extraction spec: "name := $(selector)" or "name := jsonld.Type.field"
struct CrawlExtractSpec {
	string name;       // Output field name
	string source;     // css, jsonld, opengraph, meta, js
	string selector;   // CSS selector or path
	string accessor;   // text, html, attr:name, etc.
	bool as_json;      // ::json suffix - return as JSON type
	bool expand_array; // [*] suffix - expand array to multiple rows
};

// Parse extraction spec string into structured form
// Examples:
//   "title := $('title')"                  -> {name: title, source: css, selector: title, accessor: text}
//   "price := $('.price', 'attr:value')"   -> {name: price, source: css, selector: .price, accessor: attr:value}
//   "name := jsonld.Product.name"          -> {name: name, source: jsonld, selector: Product.name}
//   "jobs := $('input#jobs', 'attr:value')::json[*]" -> with as_json=true, expand_array=true
CrawlExtractSpec ParseExtractSpec(const string &spec);

// Build JSON request for Rust extraction from parsed specs
string BuildRustExtractionRequest(const vector<CrawlExtractSpec> &specs);

// Register the crawl() table function
void RegisterCrawlTableFunction(ExtensionLoader &loader);

// Register crawl_url() for lateral joins
// Usage: SELECT * FROM urls, LATERAL crawl_url(urls.url)
void RegisterCrawlUrlFunction(ExtensionLoader &loader);

} // namespace duckdb
