// Scalar $() function for CSS extraction in SELECT statements
//
// NEW Usage (returns STRUCT):
//   SELECT $(html, 'div.price').text as price FROM ...
//   SELECT $(html, 'a.link').attr['href'] as url FROM ...
//   SELECT $(html, 'h1').html as title_html FROM ...
//
// Returns STRUCT(text VARCHAR, html VARCHAR, attr MAP(VARCHAR, VARCHAR))

#include "css_extract_function.hpp"
#include "rust_ffi.hpp"
#include "yyjson.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"
#include "duckdb/common/vector_operations/ternary_executor.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

using namespace duckdb_yyjson;

// Define the element struct type: STRUCT(text VARCHAR, html VARCHAR, attr MAP(VARCHAR, VARCHAR))
static LogicalType GetElementStructType() {
	child_list_t<LogicalType> struct_children;
	struct_children.push_back(make_pair("text", LogicalType::VARCHAR));
	struct_children.push_back(make_pair("html", LogicalType::VARCHAR));
	struct_children.push_back(make_pair("attr", LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR)));
	return LogicalType::STRUCT(std::move(struct_children));
}

// Scalar function implementation: $(html, selector) -> STRUCT
static void CssExtractStructFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &html_vec = args.data[0];
	auto &selector_vec = args.data[1];

	for (idx_t i = 0; i < args.size(); i++) {
		auto html_val = html_vec.GetValue(i);
		auto selector_val = selector_vec.GetValue(i);

		if (html_val.IsNull() || selector_val.IsNull()) {
			FlatVector::SetNull(result, i, true);
			continue;
		}

		string html = html_val.ToString();
		string selector = selector_val.ToString();

		// Call Rust to extract element data
		string element_json = ExtractElementWithRust(html, selector);

		if (element_json == "null" || element_json.empty()) {
			FlatVector::SetNull(result, i, true);
			continue;
		}

		// Parse JSON result
		yyjson_doc *doc = yyjson_read(element_json.c_str(), element_json.length(), 0);
		if (!doc) {
			FlatVector::SetNull(result, i, true);
			continue;
		}

		yyjson_val *root = yyjson_doc_get_root(doc);

		// Extract text
		Value text_value;
		yyjson_val *text_val = yyjson_obj_get(root, "text");
		if (text_val && yyjson_is_str(text_val)) {
			text_value = Value(yyjson_get_str(text_val));
		} else {
			text_value = Value(LogicalType::VARCHAR);
		}

		// Extract html
		Value html_value;
		yyjson_val *html_val_json = yyjson_obj_get(root, "html");
		if (html_val_json && yyjson_is_str(html_val_json)) {
			html_value = Value(yyjson_get_str(html_val_json));
		} else {
			html_value = Value(LogicalType::VARCHAR);
		}

		// Extract attributes into MAP
		vector<Value> map_keys;
		vector<Value> map_values;

		yyjson_val *attr_obj = yyjson_obj_get(root, "attr");
		if (attr_obj && yyjson_is_obj(attr_obj)) {
			size_t idx, max;
			yyjson_val *key, *val;
			yyjson_obj_foreach(attr_obj, idx, max, key, val) {
				if (yyjson_is_str(key) && yyjson_is_str(val)) {
					map_keys.push_back(Value(yyjson_get_str(key)));
					map_values.push_back(Value(yyjson_get_str(val)));
				}
			}
		}

		Value attr_map = Value::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR, map_keys, map_values);

		// Build STRUCT value
		child_list_t<Value> struct_values;
		struct_values.push_back(make_pair("text", std::move(text_value)));
		struct_values.push_back(make_pair("html", std::move(html_value)));
		struct_values.push_back(make_pair("attr", std::move(attr_map)));

		result.SetValue(i, Value::STRUCT(std::move(struct_values)));

		yyjson_doc_free(doc);
	}

	result.SetVectorType(VectorType::FLAT_VECTOR);
}

// Legacy string-returning functions for backwards compatibility
static string CssExtractString(const string &html, const string &selector, const string &accessor) {
	if (html.empty() || selector.empty()) {
		return "";
	}

#if defined(RUST_PARSER_AVAILABLE) && RUST_PARSER_AVAILABLE
	yyjson_mut_doc *doc = yyjson_mut_doc_new(nullptr);
	if (!doc)
		return "";

	yyjson_mut_val *root = yyjson_mut_obj(doc);
	yyjson_mut_doc_set_root(doc, root);

	yyjson_mut_val *specs_arr = yyjson_mut_arr(doc);
	yyjson_mut_val *spec_obj = yyjson_mut_obj(doc);

	yyjson_mut_obj_add_str(doc, spec_obj, "source", "css");
	yyjson_mut_val *empty_path = yyjson_mut_arr(doc);
	yyjson_mut_obj_add_val(doc, spec_obj, "path", empty_path);
	yyjson_mut_obj_add_strcpy(doc, spec_obj, "selector", selector.c_str());
	yyjson_mut_obj_add_strcpy(doc, spec_obj, "accessor", accessor.c_str());
	yyjson_mut_obj_add_str(doc, spec_obj, "alias", "_result");
	yyjson_mut_obj_add_bool(doc, spec_obj, "return_text", true);
	yyjson_mut_obj_add_bool(doc, spec_obj, "is_json_cast", false);
	yyjson_mut_obj_add_bool(doc, spec_obj, "expand_array", false);

	yyjson_mut_arr_append(specs_arr, spec_obj);
	yyjson_mut_obj_add_val(doc, root, "specs", specs_arr);

	size_t len = 0;
	char *json_str = yyjson_mut_write(doc, 0, &len);
	yyjson_mut_doc_free(doc);

	if (!json_str)
		return "";

	string request_json(json_str, len);
	free(json_str);

	string result_json = ExtractWithRust(html, request_json);

	yyjson_doc *result_doc = yyjson_read(result_json.c_str(), result_json.length(), 0);
	if (!result_doc)
		return "";

	yyjson_val *result_root = yyjson_doc_get_root(result_doc);
	yyjson_val *values_obj = yyjson_obj_get(result_root, "values");

	string extracted;
	if (values_obj && yyjson_is_obj(values_obj)) {
		yyjson_val *val = yyjson_obj_get(values_obj, "_result");
		if (val && yyjson_is_str(val)) {
			extracted = yyjson_get_str(val);
		}
	}

	yyjson_doc_free(result_doc);
	return extracted;
#else
	return "";
#endif
}

// 3-argument version: css_select(html, selector, accessor) -> VARCHAR
static void CssSelectFunction3(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &html_vec = args.data[0];
	auto &selector_vec = args.data[1];
	auto &accessor_vec = args.data[2];

	TernaryExecutor::Execute<string_t, string_t, string_t, string_t>(
	    html_vec, selector_vec, accessor_vec, result, args.size(),
	    [&result](string_t html, string_t selector, string_t accessor) {
		    string extracted = CssExtractString(html.GetString(), selector.GetString(), accessor.GetString());
		    return StringVector::AddString(result, extracted);
	    });
}

// Discover all structured data in HTML document
static string DiscoverStructuredData(const string &html) {
	if (html.empty()) {
		return "{}";
	}

#if defined(RUST_PARSER_AVAILABLE) && RUST_PARSER_AVAILABLE
	string jsonld = ExtractJsonLdWithRust(html);
	string opengraph = ExtractOpenGraphWithRust(html);
	string js_vars = ExtractJsWithRust(html);

	yyjson_mut_doc *doc = yyjson_mut_doc_new(nullptr);
	if (!doc)
		return "{}";

	yyjson_mut_val *root = yyjson_mut_obj(doc);
	yyjson_mut_doc_set_root(doc, root);

	if (!jsonld.empty() && jsonld != "{}") {
		yyjson_doc *jld_doc = yyjson_read(jsonld.c_str(), jsonld.size(), 0);
		if (jld_doc) {
			yyjson_mut_val *jld_copy = yyjson_val_mut_copy(doc, yyjson_doc_get_root(jld_doc));
			yyjson_mut_obj_add_val(doc, root, "jsonld", jld_copy);
			yyjson_doc_free(jld_doc);
		}
	}

	if (!opengraph.empty() && opengraph != "{}") {
		yyjson_doc *og_doc = yyjson_read(opengraph.c_str(), opengraph.size(), 0);
		if (og_doc) {
			yyjson_mut_val *og_copy = yyjson_val_mut_copy(doc, yyjson_doc_get_root(og_doc));
			yyjson_mut_obj_add_val(doc, root, "opengraph", og_copy);
			yyjson_doc_free(og_doc);
		}
	}

	if (!js_vars.empty() && js_vars != "{}") {
		yyjson_doc *js_doc = yyjson_read(js_vars.c_str(), js_vars.size(), 0);
		if (js_doc) {
			yyjson_mut_val *js_copy = yyjson_val_mut_copy(doc, yyjson_doc_get_root(js_doc));
			yyjson_mut_obj_add_val(doc, root, "js_vars", js_copy);
			yyjson_doc_free(js_doc);
		}
	}

	size_t len = 0;
	char *json_str = yyjson_mut_write(doc, YYJSON_WRITE_PRETTY, &len);
	yyjson_mut_doc_free(doc);

	if (!json_str)
		return "{}";

	string result(json_str, len);
	free(json_str);
	return result;
#else
	return "{}";
#endif
}

static void DiscoverFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &html_vec = args.data[0];

	UnaryExecutor::Execute<string_t, string_t>(html_vec, result, args.size(), [&result](string_t html) {
		string discovered = DiscoverStructuredData(html.GetString());
		return StringVector::AddString(result, discovered);
	});
}

// jq with 3 args: jq(html, selector, attr_name) -> VARCHAR
// Returns the attribute value directly for easy JSON casting
static void JqAttrFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &html_vec = args.data[0];
	auto &selector_vec = args.data[1];
	auto &attr_vec = args.data[2];

	TernaryExecutor::Execute<string_t, string_t, string_t, string_t>(
	    html_vec, selector_vec, attr_vec, result, args.size(),
	    [&result](string_t html, string_t selector, string_t attr_name) {
		    if (html.GetSize() == 0 || selector.GetSize() == 0) {
			    return string_t();
		    }

		    // Build selector with @attr suffix for Rust
		    string full_selector = selector.GetString() + " @" + attr_name.GetString();
		    string element_json = ExtractElementWithRust(html.GetString(), full_selector);

		    if (element_json == "null" || element_json.empty()) {
			    return string_t();
		    }

		    // Rust returns quoted JSON string, unquote it
		    if (element_json.size() >= 2 && element_json[0] == '"' && element_json.back() == '"') {
			    string unquoted = element_json.substr(1, element_json.size() - 2);
			    // Unescape JSON string escapes
			    string result_str;
			    for (size_t i = 0; i < unquoted.size(); i++) {
				    if (unquoted[i] == '\\' && i + 1 < unquoted.size()) {
					    char next = unquoted[i + 1];
					    if (next == '"' || next == '\\' || next == '/') {
						    result_str += next;
						    i++;
					    } else if (next == 'n') {
						    result_str += '\n';
						    i++;
					    } else if (next == 'r') {
						    result_str += '\r';
						    i++;
					    } else if (next == 't') {
						    result_str += '\t';
						    i++;
					    } else {
						    result_str += unquoted[i];
					    }
				    } else {
					    result_str += unquoted[i];
				    }
			    }
			    return StringVector::AddString(result, result_str);
		    }

		    return StringVector::AddString(result, element_json);
	    });
}

// htmlpath(html, path) -> JSON
// Unified path syntax: css@attr[*].json.path
// Examples:
//   htmlpath(doc, 'input#jobs@value')           -> attribute value
//   htmlpath(doc, 'input#jobs@value[*]')        -> JSON array
//   htmlpath(doc, 'input#jobs@value[*].id')     -> array of 'id' fields
static void HtmlPathFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &html_vec = args.data[0];
	auto &path_vec = args.data[1];

	BinaryExecutor::Execute<string_t, string_t, string_t>(
	    html_vec, path_vec, result, args.size(), [&result](string_t html, string_t path) {
		    string json_result = ExtractPathWithRust(html.GetString(), path.GetString());
		    return StringVector::AddString(result, json_result);
	    });
}

void RegisterCssExtractFunction(ExtensionLoader &loader) {
	// htmlpath(html, path) -> JSON
	// Unified path syntax: css@attr[*].json.path
	ScalarFunction htmlpath_func("htmlpath", {LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::JSON(),
	                             HtmlPathFunction);
	loader.RegisterFunction(htmlpath_func);

	// jq(html, selector) -> STRUCT(text, html, attr MAP)
	// Named 'jq' for jQuery-like CSS selection syntax
	ScalarFunction css_struct_func("jq", {LogicalType::VARCHAR, LogicalType::VARCHAR}, GetElementStructType(),
	                               CssExtractStructFunction);
	loader.RegisterFunction(css_struct_func);

	// jq(html, selector, attr) -> VARCHAR
	// Returns just the attribute value for easy JSON casting
	// Usage: jq(html.document, 'input#jobs', 'value')::JSON[]
	ScalarFunction jq_attr_func("jq", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                            LogicalType::VARCHAR, JqAttrFunction);
	loader.RegisterFunction(jq_attr_func);

	// css_select with 3 arguments for backwards compatibility: css_select(html, selector, accessor) -> VARCHAR
	ScalarFunction css_select_func("css_select", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                               LogicalType::VARCHAR, CssSelectFunction3);
	loader.RegisterFunction(css_select_func);

	// discover() function
	ScalarFunction discover_func("discover", {LogicalType::VARCHAR}, LogicalType::VARCHAR, DiscoverFunction);
	loader.RegisterFunction(discover_func);
}

} // namespace duckdb
