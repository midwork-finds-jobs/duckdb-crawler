#define DUCKDB_EXTENSION_MAIN

#include "crawler_extension.hpp"
#include "crawler_function.hpp"
#include "crawl_parser.hpp"
#include "http_client.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/parser/parser_extension.hpp"

namespace duckdb {

static void LoadInternal(ExtensionLoader &loader) {
	auto &db = loader.GetDatabaseInstance();
	auto &config = DBConfig::GetConfig(db);

	// Register crawler_user_agent setting
	config.AddExtensionOption("crawler_user_agent",
	                          "User agent string for crawler HTTP requests",
	                          LogicalType::VARCHAR,
	                          Value("DuckDB-Crawler/1.0"));

	// Register crawler_default_delay setting
	config.AddExtensionOption("crawler_default_delay",
	                          "Default crawl delay in seconds if not in robots.txt",
	                          LogicalType::DOUBLE,
	                          Value(1.0));

	// Initialize HTTP client (libcurl with connection pooling)
	InitializeHttpClient();

	// Register crawl_into_internal() table function for CRAWL INTO syntax
	RegisterCrawlIntoFunction(loader);

	// Register CRAWL parser extension
	ParserExtension parser_ext;
	parser_ext.parse_function = CrawlParserExtension::ParseCrawl;
	parser_ext.plan_function = CrawlParserExtension::PlanCrawl;
	config.parser_extensions.push_back(std::move(parser_ext));
}

void CrawlerExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string CrawlerExtension::Name() {
	return "crawler";
}

std::string CrawlerExtension::Version() const {
#ifdef EXT_VERSION_CRAWLER
	return EXT_VERSION_CRAWLER;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(crawler, loader) {
	duckdb::LoadInternal(loader);
}

}
