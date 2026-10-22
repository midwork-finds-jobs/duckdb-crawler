#pragma once

#include "crawler_compat_config.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/tableref.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/function/table_function.hpp"

#if CRAWLER_DUCKDB_V2
#include "duckdb/common/identifier.hpp"
#endif

namespace duckdb {

#if CRAWLER_DUCKDB_V2
using CrawlerResultName = Identifier;

inline const string &CrawlerIdentifierName(const Identifier &identifier) {
	return identifier.GetIdentifierName();
}

inline const vector<Identifier> &CrawlerColumnNames(const ColumnRefExpression &expression) {
	return expression.ColumnNames();
}

inline const ParsedExpression *CrawlerComparisonLeft(const ComparisonExpression &expression) {
	return &expression.Left();
}

inline const ParsedExpression *CrawlerComparisonRight(const ComparisonExpression &expression) {
	return &expression.Right();
}
#else
using CrawlerResultName = string;

inline const string &CrawlerIdentifierName(const string &identifier) {
	return identifier;
}

inline const vector<string> &CrawlerColumnNames(const ColumnRefExpression &expression) {
	return expression.column_names;
}

inline const ParsedExpression *CrawlerComparisonLeft(const ComparisonExpression &expression) {
	return expression.left.get();
}

inline const ParsedExpression *CrawlerComparisonRight(const ComparisonExpression &expression) {
	return expression.right.get();
}
#endif

//! Declare an optional named parameter. DuckDB 2.0 treats a keyword parameter without a default as required,
//! so it gets a NULL default that bind callbacks skip.
inline void CrawlerAddNamedParameter(TableFunction &function, const string &name, const LogicalType &type) {
#if CRAWLER_DUCKDB_V2
	function.GetSignature().AddKeywordOnly(Identifier(name), type, Value(type));
#else
	function.named_parameters[name] = type;
#endif
}

inline string CrawlerTableAlias(const TableRef &ref) {
	return CrawlerIdentifierName(ref.alias);
}

inline vector<string> CrawlerQueryResultNames(const QueryResult &result) {
	vector<string> names;
#if CRAWLER_DUCKDB_V2
	for (const auto &name : result.GetNames()) {
		names.push_back(CrawlerIdentifierName(name));
	}
#else
	names = result.names;
#endif
	return names;
}

inline const vector<LogicalType> &CrawlerQueryResultTypes(const QueryResult &result) {
#if CRAWLER_DUCKDB_V2
	return result.GetTypes();
#else
	return result.types;
#endif
}

} // namespace duckdb
