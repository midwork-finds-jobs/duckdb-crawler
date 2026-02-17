#pragma once

#include "duckdb.hpp"
#include "duckdb/parser/parser_extension.hpp"
#include "duckdb/parser/tableref.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/common/enums/merge_action_type.hpp"
#include "duckdb/parser/statement/insert_statement.hpp"

namespace duckdb {

// Single MERGE action (UPDATE, DELETE, or INSERT with optional condition)
struct CrawlingMergeAction {
	MergeActionType action_type = MergeActionType::MERGE_INSERT;
	unique_ptr<ParsedExpression> condition; // Optional AND condition
	InsertColumnOrder column_order = InsertColumnOrder::INSERT_BY_POSITION;

	// For UPDATE: SET clauses
	vector<string> set_columns;
	vector<unique_ptr<ParsedExpression>> set_expressions;

	// For INSERT: column list and values
	vector<string> insert_columns;
	vector<unique_ptr<ParsedExpression>> insert_expressions;

	CrawlingMergeAction() = default;
	CrawlingMergeAction(const CrawlingMergeAction &other);
	CrawlingMergeAction &operator=(const CrawlingMergeAction &other);
};

// Parsed data from CRAWLING MERGE INTO (uses DuckDB's MERGE parser)
struct CrawlingMergeParseData : public ParserExtensionParseData {
	// Target table (parsed AST)
	unique_ptr<TableRef> target;

	// Source query (parsed AST)
	unique_ptr<TableRef> source;

	// ON condition (parsed AST)
	unique_ptr<ParsedExpression> join_condition;

	// Alternative: USING (col1, col2) instead of ON
	vector<string> using_columns;

	// Actions by condition type (WHEN MATCHED, WHEN NOT MATCHED, etc.)
	map<MergeActionCondition, vector<CrawlingMergeAction>> actions;

	// Extracted join columns for UPDATE BY NAME exclusion (derived from join_condition)
	vector<string> join_columns;

	// Source query as SQL string (for LIMIT injection and execution)
	string source_query_sql;

	// Row limit for pushdown
	int64_t row_limit = 0;
	int64_t batch_size = 100;

	unique_ptr<ParserExtensionParseData> Copy() const override;
	string ToString() const override;
};

// Parser extension for CRAWLING statements
class CrawlParserExtension : public ParserExtension {
public:
	CrawlParserExtension();

	static ParserExtensionParseResult ParseCrawl(ParserExtensionInfo *info, const string &query);
	static ParserExtensionPlanResult PlanCrawl(ParserExtensionInfo *info, ClientContext &context,
	                                           unique_ptr<ParserExtensionParseData> parse_data);
};

} // namespace duckdb
