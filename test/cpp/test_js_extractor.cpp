// Quick unit test for js_variables_extractor
// Compile: g++ -std=c++17 -I src/include -I duckdb/third_party/yyjson/include \
//          -I build/release/vcpkg_installed/arm64-osx/include/libxml2 \
//          test/cpp/test_js_extractor.cpp src/js_variables_extractor.cpp \
//          -lxml2 -o test_js_extractor

#include <iostream>
#include <cassert>
#include <string>
#include "js_variables_extractor.hpp"

using namespace duckdb;

void test_simple_var() {
	std::string html = R"(<html><script>var data = {"name": "test"};</script></html>)";
	auto result = ExtractJsVariables(html);
	assert(result.found);
	assert(result.variables.count("data") == 1);
	std::cout << "✓ test_simple_var\n";
}

void test_comment_stripping() {
	std::string html = R"(<html><script>
// var commented = {"bad": true};
var real = {"good": true};
/* var blocked = {"also_bad": true}; */
</script></html>)";
	auto result = ExtractJsVariables(html);
	assert(result.found);
	assert(result.variables.count("real") == 1);
	assert(result.variables.count("commented") == 0);
	assert(result.variables.count("blocked") == 0);
	std::cout << "✓ test_comment_stripping\n";
}

void test_window_assignment() {
	std::string html = R"(<html><script>window.__DATA__ = {"key": "value"};</script></html>)";
	auto result = ExtractJsVariables(html);
	assert(result.found);
	assert(result.variables.count("__DATA__") == 1);
	std::cout << "✓ test_window_assignment\n";
}

void test_multiple_vars() {
	std::string html = R"(<html><script>
var a = {"x": 1};
let b = {"y": 2};
const c = {"z": 3};
</script></html>)";
	auto result = ExtractJsVariables(html);
	assert(result.found);
	assert(result.variables.size() == 3);
	std::cout << "✓ test_multiple_vars\n";
}

void test_hex_encoded_json_parse() {
	// Pattern from zoho-career.html: JSON.parse with \x22 encoded quotes
	std::string html = R"(<html><script>
var jobs = JSON.parse('[{\x22name\x22:\x22test\x22}]');
</script></html>)";
	auto result = ExtractJsVariables(html);
	assert(result.found);
	assert(result.variables.count("jobs") == 1);
	assert(result.variables["jobs"].find("\"name\"") != std::string::npos);
	std::cout << "✓ test_hex_encoded_json_parse\n";
}

void test_unicode_json_parse() {
	std::string html = R"(<html><script>
var data = JSON.parse('{"greeting":"\u0048\u0065\u006c\u006c\u006f"}');
</script></html>)";
	auto result = ExtractJsVariables(html);
	assert(result.found);
	assert(result.variables.count("data") == 1);
	// \u0048\u0065\u006c\u006c\u006f = "Hello"
	assert(result.variables["data"].find("Hello") != std::string::npos);
	std::cout << "✓ test_unicode_json_parse\n";
}

void test_single_quote_json_parse() {
	std::string html = R"(<html><script>
var config = JSON.parse('{"api":"https://example.com"}');
</script></html>)";
	auto result = ExtractJsVariables(html);
	assert(result.found);
	assert(result.variables.count("config") == 1);
	std::cout << "✓ test_single_quote_json_parse\n";
}

int main() {
	std::cout << "Running js_variables_extractor tests...\n\n";

	test_simple_var();
	test_comment_stripping();
	test_window_assignment();
	test_multiple_vars();
	test_hex_encoded_json_parse();
	test_unicode_json_parse();
	test_single_quote_json_parse();

	std::cout << "\nAll tests passed!\n";
	return 0;
}
