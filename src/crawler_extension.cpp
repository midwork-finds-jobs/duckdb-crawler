#define DUCKDB_EXTENSION_MAIN

#include "crawler_extension.hpp"
#include "crawler_function.hpp"
#include "crawl_parser.hpp"
#include "css_extract_function.hpp"
#include "crawl_stream_function.hpp"
#include "crawl_table_function.hpp"
#include "stream_into_function.hpp"
#include "sitemap_function.hpp"
#include "http_client.hpp"
#include "rust_ffi.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/parser/parser_extension.hpp"
#include <csignal>
#include <atomic>

namespace duckdb {

// Store previous signal handler to restore on unload
static std::sig_atomic_t g_signal_handler_installed = 0;
static void (*g_previous_sigint_handler)(int) = SIG_DFL;

// Signal handler for graceful shutdown
static void CrawlerSignalHandler(int signum) {
	if (signum == SIGINT) {
		// Set the interrupt flag in Rust
		SetInterrupted(true);
		// Call previous handler if it was set
		if (g_previous_sigint_handler != SIG_DFL && g_previous_sigint_handler != SIG_IGN) {
			g_previous_sigint_handler(signum);
		}
	}
}

static void LoadInternal(ExtensionLoader &loader) {
	auto &db = loader.GetDatabaseInstance();
	auto &config = DBConfig::GetConfig(db);

	// Autoload json extension for structured data columns
	ExtensionHelper::TryAutoLoadExtension(db, "json");

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

	// Register $() scalar function for CSS extraction
	RegisterCssExtractFunction(loader);

	// Register crawl_stream table function for streaming crawl results
	RegisterCrawlStreamFunction(loader);

	// Register crawl() table function for clean FROM-based crawling
	RegisterCrawlTableFunction(loader);

	// Register crawl_url() for lateral joins
	RegisterCrawlUrlFunction(loader);

	// Register sitemap() table function for sitemap parsing
	RegisterSitemapFunction(loader);

	// Register stream_into_internal() for STREAM INTO syntax
	RegisterStreamIntoFunction(loader);

	// Install signal handler for graceful shutdown (only once)
	if (!g_signal_handler_installed) {
		g_previous_sigint_handler = std::signal(SIGINT, CrawlerSignalHandler);
		g_signal_handler_installed = 1;
		// Reset interrupt flag
		SetInterrupted(false);
	}

	// Register CRAWL and STREAM parser extension
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
