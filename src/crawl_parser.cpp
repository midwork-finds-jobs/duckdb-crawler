#include "crawl_parser.hpp"
#include "crawler_function.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// StreamParseData
//===--------------------------------------------------------------------===//
unique_ptr<ParserExtensionParseData> StreamParseData::Copy() const {
	auto copy = make_uniq<StreamParseData>();
	copy->source_query = source_query;
	copy->target_table = target_table;
	copy->batch_size = batch_size;
	return copy;
}

string StreamParseData::ToString() const {
	return "STREAM (" + source_query + ") INTO " + target_table;
}

//===--------------------------------------------------------------------===//
// CrawlParseData
//===--------------------------------------------------------------------===//
unique_ptr<ParserExtensionParseData> CrawlParseData::Copy() const {
	auto copy = make_uniq<CrawlParseData>();
	copy->statement_type = statement_type;
	copy->source_query = source_query;
	copy->target_table = target_table;
	copy->user_agent = user_agent;
	copy->default_crawl_delay = default_crawl_delay;
	copy->min_crawl_delay = min_crawl_delay;
	copy->max_crawl_delay = max_crawl_delay;
	copy->timeout_seconds = timeout_seconds;
	copy->respect_robots_txt = respect_robots_txt;
	copy->log_skipped = log_skipped;
	copy->url_filter = url_filter;
	copy->sitemap_cache_hours = sitemap_cache_hours;
	copy->update_stale = update_stale;
	copy->max_retry_backoff_seconds = max_retry_backoff_seconds;
	copy->max_parallel_per_domain = max_parallel_per_domain;
	copy->max_total_connections = max_total_connections;
	copy->max_response_bytes = max_response_bytes;
	copy->compress = compress;
	copy->accept_content_types = accept_content_types;
	copy->reject_content_types = reject_content_types;
	copy->follow_links = follow_links;
	copy->allow_subdomains = allow_subdomains;
	copy->max_crawl_pages = max_crawl_pages;
	copy->max_crawl_depth = max_crawl_depth;
	copy->respect_nofollow = respect_nofollow;
	copy->follow_canonical = follow_canonical;
	copy->num_threads = num_threads;
	copy->extract_js = extract_js;
	copy->extract_specs = extract_specs;
	return copy;
}

string CrawlParseData::ToString() const {
	return "CRAWL (" + source_query + ") INTO " + target_table;
}

//===--------------------------------------------------------------------===//
// Parser helpers
//===--------------------------------------------------------------------===//
static string Trim(const string &str) {
	size_t start = 0;
	size_t end = str.length();
	while (start < end && std::isspace(str[start])) {
		start++;
	}
	while (end > start && std::isspace(str[end - 1])) {
		end--;
	}
	return str.substr(start, end - start);
}

// Find keyword as standalone word (not part of identifier)
// Returns position or string::npos if not found
static size_t FindKeyword(const string &str, const string &keyword) {
	size_t pos = 0;
	while ((pos = str.find(keyword, pos)) != string::npos) {
		// Check if preceded by whitespace or start
		bool valid_start = (pos == 0 || !std::isalnum(str[pos - 1]) && str[pos - 1] != '_');
		// Check if followed by whitespace, '(', or end
		size_t after = pos + keyword.length();
		bool valid_end = (after >= str.length() ||
		                  std::isspace(str[after]) ||
		                  str[after] == '(' ||
		                  (!std::isalnum(str[after]) && str[after] != '_'));
		if (valid_start && valid_end) {
			return pos;
		}
		pos++;
	}
	return string::npos;
}

// Find matching closing parenthesis
static size_t FindClosingParen(const string &str, size_t open_pos) {
	int depth = 1;
	bool in_string = false;
	char string_char = 0;

	for (size_t i = open_pos + 1; i < str.length(); i++) {
		char c = str[i];

		// Handle string literals
		if (!in_string && (c == '\'' || c == '"')) {
			in_string = true;
			string_char = c;
		} else if (in_string && c == string_char) {
			// Check for escaped quote
			if (i + 1 < str.length() && str[i + 1] == string_char) {
				i++; // Skip escaped quote
			} else {
				in_string = false;
			}
		} else if (!in_string) {
			if (c == '(') {
				depth++;
			} else if (c == ')') {
				depth--;
				if (depth == 0) {
					return i;
				}
			}
		}
	}
	return string::npos;
}

// Parse WITH options: WITH (key1 value1, key2 value2, ...)
static bool ParseWithOptions(const string &options_str, CrawlParseData &data) {
	// Remove outer parentheses if present
	string opts = Trim(options_str);
	if (!opts.empty() && opts.front() == '(' && opts.back() == ')') {
		opts = opts.substr(1, opts.length() - 2);
	}

	// Simple parsing: split by comma (handles quoted strings)
	size_t pos = 0;
	while (pos < opts.length()) {
		// Skip whitespace
		while (pos < opts.length() && std::isspace(opts[pos])) {
			pos++;
		}
		if (pos >= opts.length()) {
			break;
		}

		// Find key
		size_t key_start = pos;
		while (pos < opts.length() && !std::isspace(opts[pos]) && opts[pos] != ',') {
			pos++;
		}
		string key = StringUtil::Lower(opts.substr(key_start, pos - key_start));

		// Skip whitespace
		while (pos < opts.length() && std::isspace(opts[pos])) {
			pos++;
		}

		// Find value
		string value;
		if (pos < opts.length() && opts[pos] == '\'') {
			// Quoted string
			pos++; // skip opening quote
			size_t value_start = pos;
			while (pos < opts.length() && opts[pos] != '\'') {
				pos++;
			}
			value = opts.substr(value_start, pos - value_start);
			if (pos < opts.length()) {
				pos++; // skip closing quote
			}
		} else {
			// Unquoted value
			size_t value_start = pos;
			while (pos < opts.length() && !std::isspace(opts[pos]) && opts[pos] != ',') {
				pos++;
			}
			value = opts.substr(value_start, pos - value_start);
		}

		// Skip comma
		while (pos < opts.length() && (std::isspace(opts[pos]) || opts[pos] == ',')) {
			pos++;
		}

		// Apply option
		if (key == "user_agent") {
			data.user_agent = value;
		} else if (key == "default_crawl_delay") {
			data.default_crawl_delay = std::stod(value);
		} else if (key == "min_crawl_delay") {
			data.min_crawl_delay = std::stod(value);
		} else if (key == "max_crawl_delay") {
			data.max_crawl_delay = std::stod(value);
		} else if (key == "timeout_seconds") {
			data.timeout_seconds = std::stoi(value);
		} else if (key == "respect_robots_txt") {
			data.respect_robots_txt = (StringUtil::Lower(value) == "true" || value == "1");
		} else if (key == "log_skipped") {
			data.log_skipped = (StringUtil::Lower(value) == "true" || value == "1");
		} else if (key == "sitemap_cache_hours") {
			data.sitemap_cache_hours = std::stod(value);
		} else if (key == "update_stale") {
			data.update_stale = (StringUtil::Lower(value) == "true" || value == "1");
		} else if (key == "max_retry_backoff_seconds") {
			data.max_retry_backoff_seconds = std::stoi(value);
		} else if (key == "max_parallel_per_domain") {
			data.max_parallel_per_domain = std::stoi(value);
		} else if (key == "max_total_connections") {
			data.max_total_connections = std::stoi(value);
		} else if (key == "max_response_bytes") {
			data.max_response_bytes = std::stoll(value);
		} else if (key == "compress") {
			data.compress = (StringUtil::Lower(value) == "true" || value == "1");
		} else if (key == "accept_content_types") {
			data.accept_content_types = value;
		} else if (key == "reject_content_types") {
			data.reject_content_types = value;
		} else if (key == "follow_links") {
			data.follow_links = (StringUtil::Lower(value) == "true" || value == "1");
		} else if (key == "allow_subdomains") {
			data.allow_subdomains = (StringUtil::Lower(value) == "true" || value == "1");
		} else if (key == "max_crawl_pages") {
			data.max_crawl_pages = std::stoi(value);
		} else if (key == "max_crawl_depth") {
			data.max_crawl_depth = std::stoi(value);
		} else if (key == "respect_nofollow") {
			data.respect_nofollow = (StringUtil::Lower(value) == "true" || value == "1");
		} else if (key == "follow_canonical") {
			data.follow_canonical = (StringUtil::Lower(value) == "true" || value == "1");
		} else if (key == "threads" || key == "num_threads") {
			data.num_threads = std::stoi(value);
		} else if (key == "extract_js") {
			data.extract_js = (StringUtil::Lower(value) == "true" || value == "1");
		}
	}

	return true;
}

// Helper: Parse source string to ExtractSource enum
static ExtractSource ParseSourceType(const string &source) {
	string lower = StringUtil::Lower(source);
	if (lower == "jsonld" || lower == "json-ld") return ExtractSource::JSONLD;
	if (lower == "microdata") return ExtractSource::MICRODATA;
	if (lower == "og" || lower == "opengraph") return ExtractSource::OPENGRAPH;
	if (lower == "meta") return ExtractSource::META;
	if (lower == "js" || lower == "javascript") return ExtractSource::JS;
	if (lower == "css") return ExtractSource::CSS;
	return ExtractSource::JSONLD; // Default
}

// Helper: Parse a path expression (dot notation or arrow notation)
// Converts both to source and path components
static void ParsePathExpression(const string &expr, ExtractSource &source, string &path) {
	// Check for dot notation: jsonld.Product.name
	size_t dot_pos = expr.find('.');
	if (dot_pos != string::npos && expr.find("->") == string::npos) {
		source = ParseSourceType(expr.substr(0, dot_pos));
		path = expr.substr(dot_pos + 1);
		return;
	}

	// Check for arrow notation: jsonld->'Product'->>'name'
	size_t arrow_pos = expr.find("->");
	if (arrow_pos != string::npos) {
		source = ParseSourceType(expr.substr(0, arrow_pos));

		// Convert arrow notation to dot notation
		string remainder = expr.substr(arrow_pos);
		string result_path;

		size_t pos = 0;
		while (pos < remainder.length()) {
			size_t next_arrow = remainder.find("->", pos);
			if (next_arrow == string::npos) break;

			bool is_text = (next_arrow + 2 < remainder.length() &&
			               remainder[next_arrow + 2] == '>');
			size_t key_start = is_text ? next_arrow + 3 : next_arrow + 2;

			while (key_start < remainder.length() && std::isspace(remainder[key_start])) {
				key_start++;
			}

			string key;
			if (key_start < remainder.length() && remainder[key_start] == '\'') {
				size_t key_end = remainder.find('\'', key_start + 1);
				if (key_end != string::npos) {
					key = remainder.substr(key_start + 1, key_end - key_start - 1);
					pos = key_end + 1;
				} else break;
			} else {
				size_t key_end = remainder.find("->", key_start);
				if (key_end == string::npos) {
					key = remainder.substr(key_start);
					pos = remainder.length();
				} else {
					key = remainder.substr(key_start, key_end - key_start);
					pos = key_end;
				}
				while (!key.empty() && std::isspace(key.back())) key.pop_back();
			}

			if (!key.empty()) {
				if (!result_path.empty()) result_path += ".";
				result_path += key;
			}
		}
		path = result_path;
		return;
	}

	// Check for CSS: css 'selector' or css "selector"
	string lower = StringUtil::Lower(expr);
	if (StringUtil::StartsWith(lower, "css ")) {
		source = ExtractSource::CSS;
		path = Trim(expr.substr(4));
		// Remove quotes
		if (path.length() >= 2 &&
		    ((path.front() == '\'' && path.back() == '\'') ||
		     (path.front() == '"' && path.back() == '"'))) {
			path = path.substr(1, path.length() - 2);
		}
		return;
	}

	// Default: assume jsonld with the expression as path
	source = ExtractSource::JSONLD;
	path = expr;
}

// Parse EXTRACT clause with enhanced syntax:
// - Simple: jsonld.Product.name as name
// - COALESCE: COALESCE(jsonld.Product.gtin13, microdata.Product.gtin) as gtin
// - Typed: price DECIMAL FROM css '.price::text' | parse_price
// - Legacy: jsonld->'Product'->>'name' as name
static bool ParseExtractClause(const string &extract_str, CrawlParseData &data) {
	// Remove outer parentheses
	string content = Trim(extract_str);
	if (!content.empty() && content.front() == '(' && content.back() == ')') {
		content = content.substr(1, content.length() - 2);
	}

	// Split by comma (respecting nested parentheses and quotes)
	vector<string> specs;
	size_t pos = 0;
	size_t start = 0;
	int paren_depth = 0;
	bool in_string = false;
	char string_char = 0;

	while (pos < content.length()) {
		char c = content[pos];

		if (!in_string && (c == '\'' || c == '"')) {
			in_string = true;
			string_char = c;
		} else if (in_string && c == string_char) {
			in_string = false;
		} else if (!in_string) {
			if (c == '(') {
				paren_depth++;
			} else if (c == ')') {
				paren_depth--;
			} else if (c == ',' && paren_depth == 0) {
				specs.push_back(Trim(content.substr(start, pos - start)));
				start = pos + 1;
			}
		}
		pos++;
	}
	if (start < content.length()) {
		specs.push_back(Trim(content.substr(start)));
	}

	for (const auto &spec_str : specs) {
		if (spec_str.empty()) continue;

		ExtractSpec spec;
		string spec_lower = StringUtil::Lower(spec_str);

		// Check for COALESCE syntax: COALESCE(path1, path2, ...) as alias
		if (StringUtil::StartsWith(spec_lower, "coalesce(") ||
		    StringUtil::StartsWith(spec_lower, "coalesce (")) {
			size_t paren_start = spec_str.find('(');
			size_t paren_end = FindClosingParen(spec_str, paren_start);
			if (paren_end == string::npos) continue;

			string args = spec_str.substr(paren_start + 1, paren_end - paren_start - 1);
			string after_paren = Trim(spec_str.substr(paren_end + 1));

			// Parse alias after COALESCE(...)
			string after_lower = StringUtil::Lower(after_paren);
			size_t as_pos = after_lower.find(" as ");
			if (as_pos == string::npos && StringUtil::StartsWith(after_lower, "as ")) {
				as_pos = 0;
			}
			if (as_pos != string::npos) {
				size_t alias_start = (as_pos == 0) ? 3 : as_pos + 4;
				spec.alias = Trim(after_paren.substr(alias_start));
			}

			// Parse coalesce arguments
			vector<string> coalesce_args;
			size_t arg_start = 0;
			int depth = 0;
			for (size_t i = 0; i <= args.length(); i++) {
				if (i == args.length() || (args[i] == ',' && depth == 0)) {
					coalesce_args.push_back(Trim(args.substr(arg_start, i - arg_start)));
					arg_start = i + 1;
				} else if (args[i] == '(') depth++;
				else if (args[i] == ')') depth--;
			}

			spec.is_coalesce = true;
			for (const auto &arg : coalesce_args) {
				ExtractSource src;
				string pth;
				ParsePathExpression(arg, src, pth);
				// Store as source.path format
				string source_name;
				switch (src) {
					case ExtractSource::JSONLD: source_name = "jsonld"; break;
					case ExtractSource::MICRODATA: source_name = "microdata"; break;
					case ExtractSource::OPENGRAPH: source_name = "og"; break;
					case ExtractSource::META: source_name = "meta"; break;
					case ExtractSource::JS: source_name = "js"; break;
					case ExtractSource::CSS: source_name = "css"; break;
				}
				spec.coalesce_paths.push_back(source_name + "." + pth);
			}
			spec.expression = spec_str;
			data.extract_specs.push_back(spec);
			continue;
		}

		// Check for jQuery-like CSS selector: $('selector').text or $('selector').attr['href']
		// Also supports ::json cast and [*] array expansion
		if (StringUtil::StartsWith(spec_str, "$(") || StringUtil::StartsWith(spec_str, "$( ")) {
			// First, extract alias if present
			string jquery_expr = spec_str;
			string jquery_alias;
			size_t jq_as_pos = spec_lower.rfind(" as ");
			if (jq_as_pos != string::npos) {
				jquery_expr = Trim(spec_str.substr(0, jq_as_pos));
				jquery_alias = Trim(spec_str.substr(jq_as_pos + 4));
			}

			// Parse jQuery-style selector
			size_t paren_start = jquery_expr.find('(');
			size_t quote_start = jquery_expr.find('\'', paren_start);
			if (quote_start == string::npos) {
				quote_start = jquery_expr.find('"', paren_start);
			}

			if (quote_start != string::npos) {
				char quote_char = jquery_expr[quote_start];
				size_t quote_end = jquery_expr.find(quote_char, quote_start + 1);
				if (quote_end != string::npos) {
					string selector = jquery_expr.substr(quote_start + 1, quote_end - quote_start - 1);

					// Find closing paren
					size_t paren_end = jquery_expr.find(')', quote_end);
					if (paren_end != string::npos && paren_end + 1 < jquery_expr.length() &&
					    jquery_expr[paren_end + 1] == '.') {
						// Parse accessor: .text, .html, .attr['href']
						string accessor_str = jquery_expr.substr(paren_end + 2);

						// Use alias from "as" clause, or auto-generate
						if (!jquery_alias.empty()) {
							spec.alias = jquery_alias;
						} else {
							// Auto-generate alias from selector
							size_t last_part = selector.rfind('.');
							if (last_part != string::npos) {
								spec.alias = selector.substr(last_part + 1);
							} else {
								spec.alias = "css_value";
							}
						}

						spec.source = ExtractSource::CSS;
						spec.path = selector;  // Store selector in path

						// Check for ::json suffix and parse JSON-related modifiers
						size_t json_cast_pos = accessor_str.find("::json");
						string base_accessor = accessor_str;
						if (json_cast_pos != string::npos) {
							spec.is_json_cast = true;
							base_accessor = accessor_str.substr(0, json_cast_pos);
							string json_suffix = accessor_str.substr(json_cast_pos + 6);  // after ::json

							// Check for [*] array expansion
							if (json_suffix.length() >= 3 && json_suffix.substr(0, 3) == "[*]") {
								spec.expand_array = true;
								string after_star = json_suffix.substr(3);

								// Check for .field path after [*]
								if (!after_star.empty() && after_star[0] == '.') {
									spec.array_field = after_star.substr(1);  // Skip the dot
								}
							}
							// Check for -> navigation after ::json
							else if (json_suffix.length() >= 2 && json_suffix.substr(0, 2) == "->") {
								spec.json_path = json_suffix;
							}
						}

						// Store accessor info (text, html, attr:name, or parent.*)
						if (base_accessor == "text") {
							spec.transform = "text";
						} else if (base_accessor == "html") {
							spec.transform = "html";
						} else if (StringUtil::StartsWith(base_accessor, "attr[")) {
							// Parse attr['href'] or attr["href"]
							size_t attr_quote = base_accessor.find('\'', 5);
							if (attr_quote == string::npos) {
								attr_quote = base_accessor.find('"', 5);
							}
							if (attr_quote != string::npos) {
								char attr_q = base_accessor[attr_quote];
								size_t attr_end = base_accessor.find(attr_q, attr_quote + 1);
								if (attr_end != string::npos) {
									string attr_name = base_accessor.substr(attr_quote + 1, attr_end - attr_quote - 1);
									spec.transform = "attr:" + attr_name;
								}
							}
						} else if (StringUtil::StartsWith(base_accessor, "parent.")) {
							// Parse parent.text, parent.html, parent.attr['name']
							string parent_accessor = base_accessor.substr(7);  // Skip "parent."
							if (parent_accessor == "text") {
								spec.transform = "parent.text";
							} else if (parent_accessor == "html") {
								spec.transform = "parent.html";
							} else if (StringUtil::StartsWith(parent_accessor, "attr[")) {
								// Parse parent.attr['href']
								size_t attr_quote = parent_accessor.find('\'', 5);
								if (attr_quote == string::npos) {
									attr_quote = parent_accessor.find('"', 5);
								}
								if (attr_quote != string::npos) {
									char attr_q = parent_accessor[attr_quote];
									size_t attr_end = parent_accessor.find(attr_q, attr_quote + 1);
									if (attr_end != string::npos) {
										string attr_name = parent_accessor.substr(attr_quote + 1, attr_end - attr_quote - 1);
										spec.transform = "parent.attr:" + attr_name;
									}
								}
							}
						} else if (StringUtil::StartsWith(base_accessor, "children[")) {
							// Parse children[N].text, children[N].html, children[N].attr['name']
							size_t bracket_end = base_accessor.find(']', 9);
							if (bracket_end != string::npos && bracket_end + 1 < base_accessor.length() &&
							    base_accessor[bracket_end + 1] == '.') {
								string index_str = base_accessor.substr(9, bracket_end - 9);
								string child_accessor = base_accessor.substr(bracket_end + 2);

								if (child_accessor == "text") {
									spec.transform = "children." + index_str + ".text";
								} else if (child_accessor == "html") {
									spec.transform = "children." + index_str + ".html";
								} else if (StringUtil::StartsWith(child_accessor, "attr[")) {
									// Parse children[N].attr['href']
									size_t attr_quote = child_accessor.find('\'', 5);
									if (attr_quote == string::npos) {
										attr_quote = child_accessor.find('"', 5);
									}
									if (attr_quote != string::npos) {
										char attr_q = child_accessor[attr_quote];
										size_t attr_end = child_accessor.find(attr_q, attr_quote + 1);
										if (attr_end != string::npos) {
											string attr_name = child_accessor.substr(attr_quote + 1, attr_end - attr_quote - 1);
											spec.transform = "children." + index_str + ".attr:" + attr_name;
										}
									}
								}
							}
						}

						spec.expression = spec_str;
						spec.is_text = true;
						data.extract_specs.push_back(spec);
						continue;
					}
				}
			}
		}

		// Check for typed extraction: alias TYPE FROM source_expr [| transform]
		// e.g., price DECIMAL FROM css '.price::text' | parse_price
		size_t from_pos = spec_lower.find(" from ");
		if (from_pos != string::npos) {
			// Parse: alias TYPE FROM source_expr [| transform]
			string before_from = Trim(spec_str.substr(0, from_pos));
			string after_from = Trim(spec_str.substr(from_pos + 6));

			// Parse alias and optional type from before_from
			vector<string> parts;
			size_t space_pos = 0;
			size_t part_start = 0;
			while ((space_pos = before_from.find(' ', part_start)) != string::npos) {
				if (space_pos > part_start) {
					parts.push_back(before_from.substr(part_start, space_pos - part_start));
				}
				part_start = space_pos + 1;
			}
			if (part_start < before_from.length()) {
				parts.push_back(before_from.substr(part_start));
			}

			if (parts.size() >= 1) {
				spec.alias = parts[0];
			}
			if (parts.size() >= 2) {
				spec.target_type = StringUtil::Upper(parts[1]);
			}

			// Parse transform (after |)
			size_t pipe_pos = after_from.find('|');
			string source_expr;
			if (pipe_pos != string::npos) {
				source_expr = Trim(after_from.substr(0, pipe_pos));
				spec.transform = Trim(after_from.substr(pipe_pos + 1));
			} else {
				source_expr = after_from;
			}

			ParsePathExpression(source_expr, spec.source, spec.path);
			spec.expression = spec_str;
			data.extract_specs.push_back(spec);
			continue;
		}

		// Standard syntax: expr [as alias]
		size_t as_pos = spec_lower.rfind(" as ");
		if (as_pos != string::npos) {
			spec.expression = Trim(spec_str.substr(0, as_pos));
			spec.alias = Trim(spec_str.substr(as_pos + 4));
		} else {
			spec.expression = spec_str;
			// Auto-generate alias from last path segment
			ParsePathExpression(spec_str, spec.source, spec.path);
			size_t last_dot = spec.path.rfind('.');
			if (last_dot != string::npos) {
				spec.alias = spec.path.substr(last_dot + 1);
			} else if (!spec.path.empty()) {
				spec.alias = spec.path;
			} else {
				spec.alias = spec_str;
			}
		}

		// Parse path if not already done
		if (spec.path.empty()) {
			ParsePathExpression(spec.expression, spec.source, spec.path);
		}

		// Determine text output mode
		spec.is_text = (spec.expression.find("->>") != string::npos);

		// Check for [*] array expansion in arrow notation
		// e.g., js->'jobs'->[*]->>'id' or jsonld->'offers'->[*]->'price'
		size_t star_pos = spec.expression.find("->[*]");
		if (star_pos != string::npos) {
			spec.expand_array = true;
			// Extract array field path after [*] (e.g., "id" from ->[*]->>'id')
			string after_star = spec.expression.substr(star_pos + 5);
			if (!after_star.empty()) {
				// Parse any path after [*]
				if (after_star.substr(0, 3) == "->>" || after_star.substr(0, 2) == "->") {
					// Extract the remaining path segments
					size_t key_start = (after_star[2] == '>') ? 3 : 2;
					while (key_start < after_star.length() && std::isspace(after_star[key_start])) {
						key_start++;
					}
					if (key_start < after_star.length()) {
						// Handle quoted or unquoted keys
						if (after_star[key_start] == '\'') {
							size_t key_end = after_star.find('\'', key_start + 1);
							if (key_end != string::npos) {
								spec.array_field = after_star.substr(key_start + 1, key_end - key_start - 1);
							}
						} else if (after_star[key_start] == '"') {
							size_t key_end = after_star.find('"', key_start + 1);
							if (key_end != string::npos) {
								spec.array_field = after_star.substr(key_start + 1, key_end - key_start - 1);
							}
						} else {
							// Unquoted - take until next arrow or end
							size_t key_end = after_star.find("->", key_start);
							if (key_end == string::npos) {
								spec.array_field = Trim(after_star.substr(key_start));
							} else {
								spec.array_field = Trim(after_star.substr(key_start, key_end - key_start));
							}
						}
					}
				} else if (after_star[0] == '.') {
					// Dot notation: [*].id
					spec.array_field = after_star.substr(1);
				}
			}
		}

		data.extract_specs.push_back(spec);
	}

	return !data.extract_specs.empty();
}

// Helper: Escape string for JSON
static string EscapeJsonString(const string &s) {
	string result;
	result.reserve(s.length() + 10);
	for (char c : s) {
		switch (c) {
			case '"': result += "\\\""; break;
			case '\\': result += "\\\\"; break;
			case '\n': result += "\\n"; break;
			case '\r': result += "\\r"; break;
			case '\t': result += "\\t"; break;
			default: result += c;
		}
	}
	return result;
}

// Helper: Convert ExtractSource enum to string
static string SourceToString(ExtractSource source) {
	switch (source) {
		case ExtractSource::JSONLD: return "jsonld";
		case ExtractSource::MICRODATA: return "microdata";
		case ExtractSource::OPENGRAPH: return "og";
		case ExtractSource::META: return "meta";
		case ExtractSource::JS: return "js";
		case ExtractSource::CSS: return "css";
		default: return "jsonld";
	}
}

// Serialize extract_specs to JSON string for parameter passing
static string SerializeExtractSpecs(const vector<ExtractSpec> &specs) {
	if (specs.empty()) {
		return "[]";
	}

	string json = "[";
	for (size_t i = 0; i < specs.size(); i++) {
		if (i > 0) {
			json += ",";
		}

		const auto &spec = specs[i];

		json += "{";
		json += "\"expr\":\"" + EscapeJsonString(spec.expression) + "\",";
		json += "\"alias\":\"" + EscapeJsonString(spec.alias) + "\",";
		json += "\"is_text\":" + string(spec.is_text ? "true" : "false") + ",";
		json += "\"source\":\"" + SourceToString(spec.source) + "\",";
		json += "\"path\":\"" + EscapeJsonString(spec.path) + "\",";
		json += "\"target_type\":\"" + EscapeJsonString(spec.target_type) + "\",";
		json += "\"transform\":\"" + EscapeJsonString(spec.transform) + "\",";
		json += "\"is_coalesce\":" + string(spec.is_coalesce ? "true" : "false") + ",";
		json += "\"is_json_cast\":" + string(spec.is_json_cast ? "true" : "false") + ",";
		json += "\"expand_array\":" + string(spec.expand_array ? "true" : "false") + ",";
		json += "\"array_field\":\"" + EscapeJsonString(spec.array_field) + "\",";
		json += "\"json_path\":\"" + EscapeJsonString(spec.json_path) + "\"";

		if (spec.is_coalesce && !spec.coalesce_paths.empty()) {
			json += ",\"coalesce_paths\":[";
			for (size_t j = 0; j < spec.coalesce_paths.size(); j++) {
				if (j > 0) json += ",";
				json += "\"" + EscapeJsonString(spec.coalesce_paths[j]) + "\"";
			}
			json += "]";
		}

		json += "}";
	}
	json += "]";
	return json;
}

//===--------------------------------------------------------------------===//
// CrawlParserExtension
//===--------------------------------------------------------------------===//
CrawlParserExtension::CrawlParserExtension() {
	parse_function = ParseCrawl;
	plan_function = PlanCrawl;
}

ParserExtensionParseResult CrawlParserExtension::ParseCrawl(ParserExtensionInfo *info, const string &query) {
	// Check if query starts with CRAWL or STREAM (case-insensitive)
	string trimmed = Trim(query);
	string lower = StringUtil::Lower(trimmed);

	// Handle STREAM statement
	if (StringUtil::StartsWith(lower, "stream")) {
		auto data = make_uniq<StreamParseData>();

		// Parse: STREAM (SELECT ...) INTO table_name [WITH (options)]
		size_t keyword_end = 6; // "stream" length

		// Find the opening parenthesis after STREAM
		size_t paren_start = trimmed.find('(', keyword_end);
		if (paren_start == string::npos) {
			return ParserExtensionParseResult("STREAM syntax error: expected '(' after STREAM");
		}

		// Find matching closing parenthesis
		size_t paren_end = FindClosingParen(trimmed, paren_start);
		if (paren_end == string::npos) {
			return ParserExtensionParseResult("STREAM syntax error: unmatched parenthesis");
		}

		// Extract source query
		data->source_query = Trim(trimmed.substr(paren_start + 1, paren_end - paren_start - 1));

		// Find INTO keyword
		string remainder = trimmed.substr(paren_end + 1);
		string remainder_lower = StringUtil::Lower(remainder);
		size_t into_pos = remainder_lower.find("into");
		if (into_pos == string::npos) {
			return ParserExtensionParseResult("STREAM syntax error: expected INTO after source query");
		}

		// Extract table name (until WITH or end)
		string after_into = Trim(remainder.substr(into_pos + 4));
		string after_into_lower = StringUtil::Lower(after_into);
		size_t with_pos = FindKeyword(after_into_lower, "with");

		if (with_pos != string::npos) {
			data->target_table = Trim(after_into.substr(0, with_pos));

			// Parse WITH options
			string after_with = Trim(after_into.substr(with_pos + 4));
			if (!after_with.empty() && after_with.front() == '(') {
				size_t with_paren_end = FindClosingParen(after_with, 0);
				if (with_paren_end != string::npos) {
					string options = after_with.substr(1, with_paren_end - 1);

					// Parse batch_size and resume options
					string options_lower = StringUtil::Lower(options);
					size_t batch_pos = options_lower.find("batch_size");
					if (batch_pos != string::npos) {
						size_t val_start = options.find_first_of("0123456789", batch_pos);
						if (val_start != string::npos) {
							size_t val_end = options.find_first_not_of("0123456789", val_start);
							string val = options.substr(val_start, val_end - val_start);
							data->batch_size = std::stoll(val);
						}
					}

				}
			}
		} else {
			data->target_table = Trim(after_into);
		}

		// Remove trailing semicolon
		if (!data->target_table.empty() && data->target_table.back() == ';') {
			data->target_table.pop_back();
			data->target_table = Trim(data->target_table);
		}

		if (data->target_table.empty()) {
			return ParserExtensionParseResult("STREAM syntax error: target table name is required");
		}

		return ParserExtensionParseResult(std::move(data));
	}

	if (!StringUtil::StartsWith(lower, "crawl")) {
		// Not a CRAWL statement, let default parser handle it
		return ParserExtensionParseResult();
	}

	auto data = make_uniq<CrawlParseData>();
	data->statement_type = CrawlStatementType::CRAWL;

	// Parse: CRAWL (SELECT ...) INTO table_name [WHERE ...] WITH (options)
	size_t keyword_end = 5; // "crawl" length

	// Find the opening parenthesis after CRAWL
	size_t paren_start = trimmed.find('(', keyword_end);
	if (paren_start == string::npos) {
		return ParserExtensionParseResult("CRAWL syntax error: expected '(' after CRAWL");
	}

	// Find matching closing parenthesis
	size_t paren_end = FindClosingParen(trimmed, paren_start);
	if (paren_end == string::npos) {
		return ParserExtensionParseResult("CRAWL syntax error: unmatched parenthesis");
	}

	// Extract source query
	data->source_query = Trim(trimmed.substr(paren_start + 1, paren_end - paren_start - 1));

	// Find INTO keyword
	string remainder = trimmed.substr(paren_end + 1);
	string remainder_lower = StringUtil::Lower(remainder);
	size_t into_pos = remainder_lower.find("into");
	if (into_pos == string::npos) {
		return ParserExtensionParseResult("CRAWL syntax error: expected INTO after source query");
	}

	// Extract table name (everything after INTO until WHERE, EXTRACT, WITH, or LIMIT)
	string after_into = Trim(remainder.substr(into_pos + 4));
	string after_into_lower = StringUtil::Lower(after_into);

	// Use FindKeyword to avoid matching keywords inside identifiers (e.g., test_extract)
	size_t where_pos = FindKeyword(after_into_lower, "where");
	size_t extract_pos = FindKeyword(after_into_lower, "extract");
	size_t with_pos = FindKeyword(after_into_lower, "with");
	size_t limit_pos = FindKeyword(after_into_lower, "limit");

	// Determine where table name ends (first keyword found)
	size_t table_end = string::npos;
	vector<size_t> positions = {where_pos, extract_pos, with_pos, limit_pos};
	for (size_t pos : positions) {
		if (pos != string::npos && (table_end == string::npos || pos < table_end)) {
			table_end = pos;
		}
	}

	if (table_end != string::npos) {
		data->target_table = Trim(after_into.substr(0, table_end));
	} else {
		data->target_table = Trim(after_into);
	}

	// Parse WHERE clause if present (must come before WITH)
	if (where_pos != string::npos && (with_pos == string::npos || where_pos < with_pos)) {
		string after_where = after_into.substr(where_pos + 5); // skip "where"
		string after_where_lower = StringUtil::Lower(after_where);

		// Find WITH position in remaining string
		size_t with_in_where = after_where_lower.find("with");
		string where_clause;
		if (with_in_where != string::npos) {
			where_clause = Trim(after_where.substr(0, with_in_where));
			// Update with_pos to be relative to after_into for later processing
			with_pos = where_pos + 5 + with_in_where;
		} else {
			where_clause = Trim(after_where);
		}

		// Parse WHERE clause: expect "url LIKE 'pattern'"
		string where_lower = StringUtil::Lower(where_clause);
		if (!StringUtil::StartsWith(where_lower, "url")) {
			return ParserExtensionParseResult("CRAWL syntax error: WHERE clause must start with 'url'");
		}

		// Find LIKE keyword
		size_t like_pos = where_lower.find("like");
		if (like_pos == string::npos) {
			return ParserExtensionParseResult("CRAWL syntax error: WHERE clause must use 'url LIKE pattern'");
		}

		// Extract pattern after LIKE
		string pattern_part = Trim(where_clause.substr(like_pos + 4));

		// Remove quotes if present
		if (pattern_part.length() >= 2 && pattern_part.front() == '\'' && pattern_part.back() == '\'') {
			data->url_filter = pattern_part.substr(1, pattern_part.length() - 2);
		} else {
			data->url_filter = pattern_part;
		}
	}

	// Parse EXTRACT clause if present
	if (extract_pos != string::npos) {
		string after_extract = after_into.substr(extract_pos + 7); // skip "extract"
		string after_extract_trimmed = Trim(after_extract);

		if (!after_extract_trimmed.empty() && after_extract_trimmed.front() == '(') {
			size_t extract_paren_end = FindClosingParen(after_extract_trimmed, 0);
			if (extract_paren_end != string::npos) {
				string extract_content = after_extract_trimmed.substr(0, extract_paren_end + 1);
				if (!ParseExtractClause(extract_content, *data)) {
					return ParserExtensionParseResult("CRAWL syntax error: invalid EXTRACT clause");
				}

				// Recalculate with_pos and limit_pos by searching in the remaining text
				string remaining_after_extract = after_extract_trimmed.substr(extract_paren_end + 1);
				string remaining_lower = StringUtil::Lower(remaining_after_extract);

				// Calculate proper offset: extract_pos + "extract" + trimmed whitespace + paren content + 1
				size_t trim_offset = after_extract.length() - after_extract_trimmed.length();
				size_t base_offset = extract_pos + 7 + trim_offset + extract_paren_end + 1;

				size_t with_in_remaining = FindKeyword(remaining_lower, "with");
				if (with_in_remaining != string::npos) {
					with_pos = base_offset + with_in_remaining;
				} else {
					with_pos = string::npos; // WITH not after EXTRACT
				}

				size_t limit_in_remaining = FindKeyword(remaining_lower, "limit");
				if (limit_in_remaining != string::npos) {
					limit_pos = base_offset + limit_in_remaining;
				} else {
					limit_pos = string::npos; // LIMIT not after EXTRACT
				}
			} else {
				return ParserExtensionParseResult("CRAWL syntax error: unmatched parenthesis in EXTRACT clause");
			}
		} else {
			return ParserExtensionParseResult("CRAWL syntax error: EXTRACT must be followed by parentheses");
		}
	}

	// Parse WITH clause if present
	if (with_pos != string::npos) {
		string after_with = after_into.substr(with_pos + 4);
		string after_with_trimmed = Trim(after_with);

		// Find the end of WITH (...) - need to match parentheses
		if (!after_with_trimmed.empty() && after_with_trimmed.front() == '(') {
			size_t with_paren_end = FindClosingParen(after_with_trimmed, 0);
			if (with_paren_end != string::npos) {
				string with_content = after_with_trimmed.substr(0, with_paren_end + 1);
				if (!ParseWithOptions(with_content, *data)) {
					return ParserExtensionParseResult("CRAWL syntax error: invalid WITH clause");
				}
			} else {
				return ParserExtensionParseResult("CRAWL syntax error: unmatched parenthesis in WITH clause");
			}
		} else {
			// WITH without parentheses - parse until LIMIT or end
			string with_content;
			if (limit_pos != string::npos && limit_pos > with_pos) {
				with_content = Trim(after_into.substr(with_pos + 4, limit_pos - with_pos - 4));
			} else {
				with_content = after_with_trimmed;
			}
			if (!ParseWithOptions(with_content, *data)) {
				return ParserExtensionParseResult("CRAWL syntax error: invalid WITH clause");
			}
		}
	}

	// Parse LIMIT clause if present - sets row_limit for early stopping
	if (limit_pos != string::npos) {
		string after_limit = Trim(after_into.substr(limit_pos + 5)); // skip "limit"
		// Remove trailing semicolon
		if (!after_limit.empty() && after_limit.back() == ';') {
			after_limit.pop_back();
			after_limit = Trim(after_limit);
		}
		// Parse the number
		try {
			int64_t limit_value = std::stoll(after_limit);
			if (limit_value > 0) {
				data->row_limit = limit_value;
			}
		} catch (...) {
			return ParserExtensionParseResult("CRAWL syntax error: LIMIT must be followed by a positive integer");
		}
	}

	// Remove trailing semicolon from table name if present
	if (!data->target_table.empty() && data->target_table.back() == ';') {
		data->target_table.pop_back();
		data->target_table = Trim(data->target_table);
	}

	// Validate required parameters
	if (data->user_agent.empty()) {
		return ParserExtensionParseResult("CRAWL syntax error: user_agent is required in WITH clause");
	}

	if (data->target_table.empty()) {
		return ParserExtensionParseResult("CRAWL syntax error: target table name is required");
	}

	return ParserExtensionParseResult(std::move(data));
}

ParserExtensionPlanResult CrawlParserExtension::PlanCrawl(ParserExtensionInfo *info, ClientContext &context,
                                                          unique_ptr<ParserExtensionParseData> parse_data) {
	auto &catalog = Catalog::GetSystemCatalog(context);
	ParserExtensionPlanResult result;

	// Check if this is a STREAM statement
	if (dynamic_cast<StreamParseData *>(parse_data.get())) {
		auto &stream_data = (StreamParseData &)*parse_data;

		// Look up the registered stream_into_internal function
		auto catalog_entry = catalog.GetEntry(context, CatalogType::TABLE_FUNCTION_ENTRY, DEFAULT_SCHEMA,
		                                       "stream_into_internal", OnEntryNotFound::THROW_EXCEPTION);
		auto &table_function_catalog_entry = catalog_entry->Cast<TableFunctionCatalogEntry>();

		if (table_function_catalog_entry.functions.functions.empty()) {
			throw BinderException("STREAM: stream_into_internal function not found");
		}

		result.function = table_function_catalog_entry.functions.functions[0];

		// Pass parameters: source_query, target_table, batch_size
		result.parameters.push_back(Value(stream_data.source_query));
		result.parameters.push_back(Value(stream_data.target_table));
		result.parameters.push_back(Value(stream_data.batch_size));

		result.requires_valid_transaction = true;
		result.return_type = StatementReturnType::CHANGED_ROWS;

		return result;
	}

	auto &data = (CrawlParseData &)*parse_data;

	// Look up the registered crawl_into_internal function from the catalog
	auto catalog_entry = catalog.GetEntry(context, CatalogType::TABLE_FUNCTION_ENTRY, DEFAULT_SCHEMA,
	                                       "crawl_into_internal", OnEntryNotFound::THROW_EXCEPTION);
	auto &table_function_catalog_entry = catalog_entry->Cast<TableFunctionCatalogEntry>();

	if (table_function_catalog_entry.functions.functions.empty()) {
		throw BinderException("CRAWL: crawl_into_internal function not found");
	}

	result.function = table_function_catalog_entry.functions.functions[0];

	// Pass all data as parameters
	// statement_type: 0=CRAWL_CONCURRENTLY
	result.parameters.push_back(Value(static_cast<int32_t>(data.statement_type)));
	result.parameters.push_back(Value(data.source_query));
	result.parameters.push_back(Value(data.target_table));
	result.parameters.push_back(Value(data.user_agent));
	result.parameters.push_back(Value(data.default_crawl_delay));
	result.parameters.push_back(Value(data.min_crawl_delay));
	result.parameters.push_back(Value(data.max_crawl_delay));
	result.parameters.push_back(Value(data.timeout_seconds));
	result.parameters.push_back(Value(data.respect_robots_txt));
	result.parameters.push_back(Value(data.log_skipped));
	result.parameters.push_back(Value(data.url_filter));
	result.parameters.push_back(Value(data.sitemap_cache_hours));
	result.parameters.push_back(Value(data.update_stale));
	result.parameters.push_back(Value(data.max_retry_backoff_seconds));
	result.parameters.push_back(Value(data.max_parallel_per_domain));
	result.parameters.push_back(Value(data.max_total_connections));
	result.parameters.push_back(Value(data.max_response_bytes));
	result.parameters.push_back(Value(data.compress));
	result.parameters.push_back(Value(data.accept_content_types));
	result.parameters.push_back(Value(data.reject_content_types));
	result.parameters.push_back(Value(data.follow_links));
	result.parameters.push_back(Value(data.allow_subdomains));
	result.parameters.push_back(Value(data.max_crawl_pages));
	result.parameters.push_back(Value(data.max_crawl_depth));
	result.parameters.push_back(Value(data.respect_nofollow));
	result.parameters.push_back(Value(data.follow_canonical));
	result.parameters.push_back(Value(data.num_threads));
	result.parameters.push_back(Value(data.extract_js));
	result.parameters.push_back(Value(SerializeExtractSpecs(data.extract_specs)));
	result.parameters.push_back(Value(data.row_limit));

	result.requires_valid_transaction = true;
	result.return_type = StatementReturnType::CHANGED_ROWS;

	return result;
}

} // namespace duckdb
