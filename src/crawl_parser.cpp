#include "crawl_parser.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/merge_into_statement.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// CrawlingMergeAction
//===--------------------------------------------------------------------===//
CrawlingMergeAction::CrawlingMergeAction(const CrawlingMergeAction &other) {
	action_type = other.action_type;
	condition = other.condition ? other.condition->Copy() : nullptr;
	column_order = other.column_order;
	set_columns = other.set_columns;
	for (auto &expr : other.set_expressions) {
		set_expressions.push_back(expr->Copy());
	}
	insert_columns = other.insert_columns;
	for (auto &expr : other.insert_expressions) {
		insert_expressions.push_back(expr->Copy());
	}
}

CrawlingMergeAction &CrawlingMergeAction::operator=(const CrawlingMergeAction &other) {
	if (this != &other) {
		action_type = other.action_type;
		condition = other.condition ? other.condition->Copy() : nullptr;
		column_order = other.column_order;
		set_columns = other.set_columns;
		set_expressions.clear();
		for (auto &expr : other.set_expressions) {
			set_expressions.push_back(expr->Copy());
		}
		insert_columns = other.insert_columns;
		insert_expressions.clear();
		for (auto &expr : other.insert_expressions) {
			insert_expressions.push_back(expr->Copy());
		}
	}
	return *this;
}

//===--------------------------------------------------------------------===//
// CrawlingMergeParseData
//===--------------------------------------------------------------------===//
unique_ptr<ParserExtensionParseData> CrawlingMergeParseData::Copy() const {
	auto copy = make_uniq<CrawlingMergeParseData>();
	copy->target = target ? target->Copy() : nullptr;
	copy->source = source ? source->Copy() : nullptr;
	copy->join_condition = join_condition ? join_condition->Copy() : nullptr;
	copy->using_columns = using_columns;
	for (auto &entry : actions) {
		auto &action_list = copy->actions[entry.first];
		for (auto &action : entry.second) {
			action_list.push_back(CrawlingMergeAction(action));
		}
	}
	copy->join_columns = join_columns;
	copy->source_query_sql = source_query_sql;
	copy->row_limit = row_limit;
	copy->batch_size = batch_size;
	return copy;
}

string CrawlingMergeParseData::ToString() const {
	string result = "CRAWLING MERGE INTO ";
	if (target) {
		result += target->ToString();
	}
	result += " USING ";
	if (source) {
		result += source->ToString();
	}
	if (join_condition) {
		result += " ON " + join_condition->ToString();
	}
	return result;
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

// Inject max_results parameter into crawl() and crawl_url() function calls
// This enables LIMIT pushdown through the pipeline by stopping HTTP fetches early
static string InjectMaxResultsIntoCrawlCalls(const string &query, int64_t limit) {
	if (limit <= 0) {
		return query;
	}

	string result = query;
	string limit_str = std::to_string(limit);

	// Process crawl() with named parameter (works outside LATERAL)
	// Process crawl_url() with positional parameter (works inside LATERAL)
	size_t pos = 0;
	while (pos < result.length()) {
		string lower_result = StringUtil::Lower(result);

		// Find next crawl( or crawl_url(
		size_t crawl_pos = lower_result.find("crawl(", pos);
		size_t crawl_url_pos = lower_result.find("crawl_url(", pos);

		// Determine which comes first
		bool is_crawl_url = false;
		size_t func_pos;

		if (crawl_pos == string::npos && crawl_url_pos == string::npos) {
			break;
		} else if (crawl_pos == string::npos) {
			func_pos = crawl_url_pos;
			is_crawl_url = true;
		} else if (crawl_url_pos == string::npos) {
			func_pos = crawl_pos;
			is_crawl_url = false;
		} else if (crawl_url_pos <= crawl_pos) {
			// crawl_url comes first or at same position (crawl_url contains crawl)
			func_pos = crawl_url_pos;
			is_crawl_url = true;
		} else {
			// crawl comes first, but check it's not part of crawl_url/crawl_stream
			if (func_pos > 0 && lower_result[crawl_pos - 1] == '_') {
				pos = crawl_pos + 1;
				continue;
			}
			size_t after_crawl = crawl_pos + 5;
			if (after_crawl < lower_result.length() && lower_result[after_crawl] == '_') {
				pos = crawl_pos + 1;
				continue;
			}
			func_pos = crawl_pos;
			is_crawl_url = false;
		}

		// Find opening paren
		size_t paren_pos = result.find('(', func_pos);
		if (paren_pos == string::npos) {
			pos = func_pos + 1;
			continue;
		}

		// Find matching closing paren
		size_t close_paren = FindClosingParen(result, paren_pos);
		if (close_paren == string::npos) {
			pos = func_pos + 1;
			continue;
		}

		// Check if max_results already exists in this call
		string call_content = StringUtil::Lower(result.substr(paren_pos, close_paren - paren_pos + 1));
		if (call_content.find("max_results") != string::npos) {
			pos = close_paren + 1;
			continue;
		}

		// Inject parameter based on function type
		string limit_param;
		if (is_crawl_url) {
			// crawl_url: use positional argument (works in LATERAL)
			limit_param = ", " + limit_str + "::BIGINT";
		} else {
			// crawl: use named parameter
			limit_param = ", max_results := " + limit_str + "::BIGINT";
		}

		// Insert before closing paren
		result = result.substr(0, close_paren) + limit_param + result.substr(close_paren);
		pos = close_paren + limit_param.length() + 1;
	}

	return result;
}

//===--------------------------------------------------------------------===//
// Parse CRAWLING MERGE INTO using DuckDB's MERGE parser
//===--------------------------------------------------------------------===//
// Syntax: CRAWLING MERGE INTO <target> USING <source> ON <condition>
//         [WHEN MATCHED [AND <cond>] THEN UPDATE BY NAME | DELETE]
//         [WHEN NOT MATCHED THEN INSERT BY NAME]
//         [LIMIT <n>]
//
// This strips "CRAWLING " and uses DuckDB's parser to parse the standard
// MERGE INTO statement, giving us proper AST instead of string matching.

// Helper to extract column names from join condition for UPDATE BY NAME exclusion
static void ExtractJoinColumns(ParsedExpression *expr, vector<string> &columns) {
	if (!expr)
		return;

	if (expr->type == ExpressionType::COLUMN_REF) {
		auto &col_ref = expr->Cast<ColumnRefExpression>();
		if (!col_ref.column_names.empty()) {
			columns.push_back(col_ref.column_names.back());
		}
	} else if (expr->type == ExpressionType::COMPARE_EQUAL || expr->type == ExpressionType::COMPARE_NOT_DISTINCT_FROM) {
		auto &comp = expr->Cast<ComparisonExpression>();
		ExtractJoinColumns(comp.left.get(), columns);
		ExtractJoinColumns(comp.right.get(), columns);
	}
}

static ParserExtensionParseResult ParseCrawlingMerge(const string &query) {
	string trimmed = Trim(query);
	string lower = StringUtil::Lower(trimmed);

	// Check for "CRAWLING MERGE INTO"
	if (!StringUtil::StartsWith(lower, "crawling merge into")) {
		return ParserExtensionParseResult("CRAWLING MERGE INTO syntax error: expected 'CRAWLING MERGE INTO'");
	}

	// Check for LIMIT clause before the merge statement (not standard SQL MERGE)
	// We handle LIMIT separately as it's our extension
	int64_t row_limit = 0;
	size_t limit_pos = lower.rfind(" limit ");
	string merge_query = trimmed.substr(9); // Strip "CRAWLING " prefix

	if (limit_pos != string::npos && limit_pos > lower.find("then")) {
		// Extract LIMIT value
		string after_limit = Trim(trimmed.substr(limit_pos + 7));
		if (!after_limit.empty() && after_limit.back() == ';') {
			after_limit.pop_back();
		}
		size_t num_end = 0;
		while (num_end < after_limit.length() && std::isdigit(after_limit[num_end])) {
			num_end++;
		}
		if (num_end > 0) {
			row_limit = std::stoll(after_limit.substr(0, num_end));
		}
		// Remove LIMIT from the query to pass to DuckDB parser
		merge_query = Trim(trimmed.substr(9, limit_pos - 9));
	}

	// Remove trailing semicolon if present
	if (!merge_query.empty() && merge_query.back() == ';') {
		merge_query.pop_back();
		merge_query = Trim(merge_query);
	}

	// Use DuckDB's parser to parse the MERGE INTO statement
	Parser parser;
	try {
		parser.ParseQuery(merge_query);
	} catch (std::exception &e) {
		return ParserExtensionParseResult("CRAWLING MERGE INTO syntax error: " + string(e.what()));
	}

	if (parser.statements.empty()) {
		return ParserExtensionParseResult("CRAWLING MERGE INTO syntax error: no statement parsed");
	}

	if (parser.statements[0]->type != StatementType::MERGE_INTO_STATEMENT) {
		return ParserExtensionParseResult("CRAWLING MERGE INTO syntax error: expected MERGE INTO statement, got " +
		                                  StatementTypeToString(parser.statements[0]->type));
	}

	// Extract the parsed MergeIntoStatement
	auto &merge_stmt = parser.statements[0]->Cast<MergeIntoStatement>();

	// Create our parse data with AST components
	auto data = make_uniq<CrawlingMergeParseData>();
	data->target = merge_stmt.target->Copy();
	data->source = merge_stmt.source->Copy();
	data->join_condition = merge_stmt.join_condition ? merge_stmt.join_condition->Copy() : nullptr;
	data->using_columns = merge_stmt.using_columns;
	data->row_limit = row_limit;

	// Extract join columns from condition for UPDATE BY NAME exclusion
	if (data->join_condition) {
		ExtractJoinColumns(data->join_condition.get(), data->join_columns);
	}

	// Convert MergeIntoActions to our CrawlingMergeActions
	for (auto &entry : merge_stmt.actions) {
		auto &action_list = data->actions[entry.first];
		for (auto &action : entry.second) {
			CrawlingMergeAction stream_action;
			stream_action.action_type = action->action_type;
			stream_action.condition = action->condition ? action->condition->Copy() : nullptr;
			stream_action.column_order = action->column_order;
			stream_action.insert_columns = action->insert_columns;
			for (auto &expr : action->expressions) {
				stream_action.insert_expressions.push_back(expr->Copy());
			}
			if (action->update_info) {
				stream_action.set_columns = action->update_info->columns;
				for (auto &expr : action->update_info->expressions) {
					stream_action.set_expressions.push_back(expr->Copy());
				}
			}
			action_list.push_back(std::move(stream_action));
		}
	}

	// Store source query as SQL string for execution
	// For SubqueryRef, extract the SELECT statement; for table refs, use ToString()
	string source_alias;
	if (data->source->type == TableReferenceType::SUBQUERY) {
		auto &subquery_ref = data->source->Cast<SubqueryRef>();
		data->source_query_sql = subquery_ref.subquery->ToString();
		source_alias = subquery_ref.alias;
	} else {
		// For table references, wrap in SELECT *
		data->source_query_sql = "SELECT * FROM " + data->source->ToString();
		source_alias = data->source->alias;
	}

	// Apply LIMIT pushdown to source query SQL if needed
	if (data->row_limit > 0) {
		data->source_query_sql = InjectMaxResultsIntoCrawlCalls(data->source_query_sql, data->row_limit);
	}

	// Validate - must have at least one action
	if (data->actions.empty()) {
		return ParserExtensionParseResult("CRAWLING MERGE INTO syntax error: at least one WHEN clause is required");
	}

	return ParserExtensionParseResult(std::move(data));
}

//===--------------------------------------------------------------------===//
// CrawlParserExtension
//===--------------------------------------------------------------------===//
CrawlParserExtension::CrawlParserExtension() {
	parse_function = ParseCrawl;
	plan_function = PlanCrawl;
}

ParserExtensionParseResult CrawlParserExtension::ParseCrawl(ParserExtensionInfo *info, const string &query) {
	// Only handle CRAWLING MERGE INTO statements
	// Table functions (crawl, crawl_url, htmlpath) are registered separately
	string trimmed = Trim(query);
	string lower = StringUtil::Lower(trimmed);

	// Handle CRAWLING MERGE INTO (uses DuckDB's MERGE parser)
	if (StringUtil::StartsWith(lower, "crawling merge into")) {
		return ParseCrawlingMerge(trimmed);
	}

	// Not a statement we handle, let default parser handle it
	return ParserExtensionParseResult();
}

ParserExtensionPlanResult CrawlParserExtension::PlanCrawl(ParserExtensionInfo *info, ClientContext &context,
                                                          unique_ptr<ParserExtensionParseData> parse_data) {
	auto &catalog = Catalog::GetSystemCatalog(context);
	ParserExtensionPlanResult result;

	// Check if this is a CRAWLING MERGE statement
	if (dynamic_cast<CrawlingMergeParseData *>(parse_data.get())) {
		auto &merge_data = (CrawlingMergeParseData &)*parse_data;

		// Look up the registered stream_merge_internal function
		auto catalog_entry = catalog.GetEntry(context, CatalogType::TABLE_FUNCTION_ENTRY, DEFAULT_SCHEMA,
		                                      "stream_merge_internal", OnEntryNotFound::THROW_EXCEPTION);
		auto &table_function_catalog_entry = catalog_entry->Cast<TableFunctionCatalogEntry>();

		if (table_function_catalog_entry.functions.functions.empty()) {
			throw BinderException("CRAWLING MERGE INTO: stream_merge_internal function not found");
		}

		result.function = table_function_catalog_entry.functions.functions[0];

		// Serialize AST components to strings for the executor
		// Target table name (from AST)
		string target_table = merge_data.target->ToString();

		// Source query SQL (already stored)
		string source_query = merge_data.source_query_sql;

		// Join condition as SQL
		string join_condition = merge_data.join_condition ? merge_data.join_condition->ToString() : "";

		// Serialize join_columns to comma-separated string
		string join_cols_str;
		for (size_t i = 0; i < merge_data.join_columns.size(); i++) {
			if (i > 0)
				join_cols_str += ",";
			join_cols_str += merge_data.join_columns[i];
		}

		// Extract action information from AST
		bool has_matched = merge_data.actions.count(MergeActionCondition::WHEN_MATCHED) > 0;
		bool has_not_matched = merge_data.actions.count(MergeActionCondition::WHEN_NOT_MATCHED_BY_TARGET) > 0;

		// Get first matched action details (simplified - assumes single action per condition)
		string matched_condition;
		int32_t matched_action = 0; // 0 = UPDATE, 1 = DELETE
		bool matched_update_by_name = false;

		if (has_matched) {
			auto &matched_actions = merge_data.actions.at(MergeActionCondition::WHEN_MATCHED);
			if (!matched_actions.empty()) {
				auto &action = matched_actions[0];
				if (action.condition) {
					matched_condition = action.condition->ToString();
				}
				if (action.action_type == MergeActionType::MERGE_DELETE) {
					matched_action = 1;
				} else {
					matched_action = 0;
					matched_update_by_name = (action.column_order == InsertColumnOrder::INSERT_BY_NAME);
				}
			}
		}

		// Get first not-matched action details
		bool not_matched_insert_by_name = false;
		if (has_not_matched) {
			auto &not_matched_actions = merge_data.actions.at(MergeActionCondition::WHEN_NOT_MATCHED_BY_TARGET);
			if (!not_matched_actions.empty()) {
				auto &action = not_matched_actions[0];
				not_matched_insert_by_name = (action.column_order == InsertColumnOrder::INSERT_BY_NAME);
			}
		}

		// Get WHEN NOT MATCHED BY SOURCE action details
		bool has_not_matched_by_source = merge_data.actions.count(MergeActionCondition::WHEN_NOT_MATCHED_BY_SOURCE) > 0;
		string not_matched_by_source_condition;
		int32_t not_matched_by_source_action = 0; // 0 = UPDATE, 1 = DELETE
		bool not_matched_by_source_update_by_name = false;
		string not_matched_by_source_set_clauses; // "col=expr;col=expr" format

		if (has_not_matched_by_source) {
			auto &nmbs_actions = merge_data.actions.at(MergeActionCondition::WHEN_NOT_MATCHED_BY_SOURCE);
			if (!nmbs_actions.empty()) {
				auto &action = nmbs_actions[0];
				if (action.condition) {
					not_matched_by_source_condition = action.condition->ToString();
				}
				if (action.action_type == MergeActionType::MERGE_DELETE) {
					not_matched_by_source_action = 1;
				} else {
					not_matched_by_source_action = 0;
					not_matched_by_source_update_by_name = (action.column_order == InsertColumnOrder::INSERT_BY_NAME);
					// Extract explicit SET clauses
					for (size_t i = 0; i < action.set_columns.size(); i++) {
						if (i > 0)
							not_matched_by_source_set_clauses += ";";
						not_matched_by_source_set_clauses += action.set_columns[i] + "=";
						if (i < action.set_expressions.size()) {
							not_matched_by_source_set_clauses += action.set_expressions[i]->ToString();
						}
					}
				}
			}
		}

		// Extract source alias from the source TableRef
		string source_alias;
		if (merge_data.source) {
			source_alias = merge_data.source->alias;
		}

		// Pass parameters to stream_merge_internal
		// Order: source_query, source_alias, target_table, join_condition, join_columns,
		//        has_matched, matched_condition, matched_action, matched_update_by_name,
		//        has_not_matched, not_matched_insert_by_name,
		//        has_not_matched_by_source, not_matched_by_source_condition, not_matched_by_source_action,
		//        not_matched_by_source_update_by_name, not_matched_by_source_set_clauses,
		//        row_limit, batch_size
		result.parameters.push_back(Value(source_query));
		result.parameters.push_back(Value(source_alias));
		result.parameters.push_back(Value(target_table));
		result.parameters.push_back(Value(join_condition));
		result.parameters.push_back(Value(join_cols_str));
		result.parameters.push_back(Value(has_matched));
		result.parameters.push_back(Value(matched_condition));
		result.parameters.push_back(Value(matched_action));
		result.parameters.push_back(Value(matched_update_by_name));
		result.parameters.push_back(Value(has_not_matched));
		result.parameters.push_back(Value(not_matched_insert_by_name));
		result.parameters.push_back(Value(has_not_matched_by_source));
		result.parameters.push_back(Value(not_matched_by_source_condition));
		result.parameters.push_back(Value(not_matched_by_source_action));
		result.parameters.push_back(Value(not_matched_by_source_update_by_name));
		result.parameters.push_back(Value(not_matched_by_source_set_clauses));
		result.parameters.push_back(Value(merge_data.row_limit));
		result.parameters.push_back(Value(merge_data.batch_size));

		result.requires_valid_transaction = true;
		result.return_type = StatementReturnType::CHANGED_ROWS;

		return result;
	}

	// Only CRAWLING MERGE INTO is supported; this should never be reached
	throw BinderException("CRAWLING parser: unexpected parse data type");
}

} // namespace duckdb
