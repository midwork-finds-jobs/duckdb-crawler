// stream_merge_internal - STREAM INTO with MERGE semantics
//
// Executes a query and performs UPSERT operations on a target table.
// Implements condition pushdown to skip HTTP requests for fresh URLs.
//
// Usage (via STREAM INTO statement):
//   STREAM INTO jobs
//   USING (SELECT * FROM crawl([...])) AS src
//   ON (src.url = jobs.url)
//   WHEN MATCHED AND age(jobs.crawled_at) > INTERVAL '24 hours' THEN UPDATE BY NAME
//   WHEN NOT MATCHED THEN INSERT BY NAME;

#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/common/string_util.hpp"
#include "crawler_utils.hpp"
#include "pipeline_state.hpp"
#include <unordered_set>
#include <regex>
#include <atomic>

namespace duckdb {

//===--------------------------------------------------------------------===//
// Condition Pushdown Helpers
//===--------------------------------------------------------------------===//

// Build query to find URLs that should be EXCLUDED from crawling
// These are URLs that exist in target AND don't match the WHEN MATCHED AND condition
static string BuildExclusionQuery(const string &target_table, const vector<string> &join_columns,
                                  const string &matched_condition) {
	if (join_columns.empty()) {
		return "";
	}

	// SELECT join_columns FROM target WHERE NOT (matched_condition)
	// These are rows that exist but WON'T be updated (fresh rows)
	string sql = "SELECT ";
	for (size_t i = 0; i < join_columns.size(); i++) {
		if (i > 0)
			sql += ", ";
		sql += QuoteSqlIdentifier(join_columns[i]);
	}
	sql += " FROM " + QuoteSqlIdentifier(target_table);

	if (!matched_condition.empty()) {
		// Rows where condition is FALSE should be excluded from crawling
		sql += " WHERE NOT (" + matched_condition + ")";
	}

	return sql;
}

// Get set of values to exclude from crawling
static unordered_set<string> GetExcludedValues(Connection &conn, const string &target_table,
                                               const vector<string> &join_columns, const string &matched_condition) {
	unordered_set<string> excluded;

	string query = BuildExclusionQuery(target_table, join_columns, matched_condition);
	if (query.empty()) {
		return excluded;
	}

	auto result = conn.Query(query);
	if (result->HasError()) {
		// Table might not exist yet, that's OK
		return excluded;
	}

	while (auto chunk = result->Fetch()) {
		for (idx_t row = 0; row < chunk->size(); row++) {
			// For single join column, just get the value
			// For multiple, concatenate with separator
			string key;
			for (idx_t col = 0; col < chunk->ColumnCount(); col++) {
				if (col > 0)
					key += "\x1F"; // Unit separator
				auto val = chunk->GetValue(col, row);
				if (!val.IsNull()) {
					key += val.ToString();
				}
			}
			excluded.insert(key);
		}
	}

	return excluded;
}

// Rewrite source query to exclude fresh URLs BEFORE crawl_url runs
// This prevents HTTP requests for URLs that wouldn't be updated anyway
//
// Strategy: Find LATERAL crawl_url pattern and inject WHERE filter on URL source
// The filter is placed BEFORE the comma that precedes LATERAL, causing DuckDB
// to filter rows before invoking crawl_url for each row.
//
// Example transformation:
//   FROM urls_to_crawl utc, LATERAL crawl_url(utc.url)
// Becomes:
//   FROM urls_to_crawl utc
//   WHERE utc.url NOT IN (SELECT url FROM __fresh),
//   LATERAL crawl_url(utc.url)
static string RewriteQueryWithExclusion(const string &source_query, const string &source_alias,
                                        const string &target_table, const vector<string> &join_columns,
                                        const string &matched_condition) {
	if (join_columns.empty() || matched_condition.empty()) {
		return source_query;
	}

	string query = source_query;
	string query_lower = StringUtil::Lower(query);

	// Build exclusion CTE
	string exclusion_cols;
	for (size_t i = 0; i < join_columns.size(); i++) {
		if (i > 0)
			exclusion_cols += ", ";
		exclusion_cols += QuoteSqlIdentifier(join_columns[i]);
	}

	string fresh_cte = "__stream_merge_fresh AS (\n";
	fresh_cte += "    SELECT " + exclusion_cols + " FROM " + QuoteSqlIdentifier(target_table);
	fresh_cte += " WHERE NOT (" + matched_condition + ")\n)";

	// Find LATERAL crawl_url pattern
	size_t lateral_pos = query_lower.find("lateral crawl_url");
	if (lateral_pos == string::npos) {
		// No LATERAL crawl_url found, can't optimize
		return source_query;
	}

	// Extract URL expression from crawl_url(expr)
	size_t paren_start = query.find('(', lateral_pos);
	if (paren_start == string::npos) {
		return source_query;
	}

	// Find matching closing paren
	int depth = 1;
	size_t paren_end = paren_start + 1;
	while (paren_end < query.length() && depth > 0) {
		if (query[paren_end] == '(')
			depth++;
		else if (query[paren_end] == ')')
			depth--;
		paren_end++;
	}

	if (depth != 0) {
		return source_query;
	}

	// Extract URL expression (e.g., "utc.url")
	string url_expr = query.substr(paren_start + 1, paren_end - paren_start - 2);
	while (!url_expr.empty() && std::isspace(url_expr.front()))
		url_expr.erase(0, 1);
	while (!url_expr.empty() && std::isspace(url_expr.back()))
		url_expr.pop_back();

	// Build the WHERE filter
	string filter = url_expr + " NOT IN (SELECT " + QuoteSqlIdentifier(join_columns[0]) + " FROM __stream_merge_fresh)";

	// Find the comma before LATERAL
	size_t comma_pos = query.rfind(',', lateral_pos);
	if (comma_pos == string::npos) {
		return source_query;
	}

	// Insert WHERE clause before the comma
	// Transform: "FROM tbl alias, LATERAL" -> "FROM tbl alias WHERE filter, LATERAL"
	string before_comma = query.substr(0, comma_pos);
	string after_comma = query.substr(comma_pos); // includes the comma

	// Check if there's already a WHERE clause before the comma
	string before_comma_lower = StringUtil::Lower(before_comma);
	size_t existing_where = before_comma_lower.rfind(" where ");

	string modified_query;
	if (existing_where != string::npos && existing_where > before_comma_lower.rfind(" from ")) {
		// Already has WHERE, add AND
		modified_query = before_comma + " AND " + filter + after_comma;
	} else {
		// No WHERE, add new one
		modified_query = before_comma + " WHERE " + filter + after_comma;
	}

	// Add the exclusion CTE
	string modified_lower = StringUtil::Lower(modified_query);
	size_t with_pos = modified_lower.find("with ");
	size_t select_pos = modified_lower.find("select");

	if (with_pos != string::npos && with_pos < select_pos) {
		// Already has WITH clause, append our CTE
		// Find the SELECT after WITH
		modified_query =
		    modified_query.substr(0, select_pos) + ", " + fresh_cte + "\n" + modified_query.substr(select_pos);
	} else {
		// No WITH clause, prepend
		modified_query = "WITH " + fresh_cte + "\n" + modified_query;
	}

	return modified_query;
}

//===--------------------------------------------------------------------===//
// MergeMatchedAction enum (must match header)
//===--------------------------------------------------------------------===//
enum class MergeAction : int32_t { UPDATE = 0, DELETE = 1 };

//===--------------------------------------------------------------------===//
// Bind Data
//===--------------------------------------------------------------------===//

struct CrawlingMergeBindData : public TableFunctionData {
	string source_query;
	string source_alias;
	string target_table;
	string join_condition;
	vector<string> join_columns;

	bool has_matched = false;
	string matched_condition;
	MergeAction matched_action = MergeAction::UPDATE;
	bool matched_update_by_name = false;

	bool has_not_matched = false;
	bool not_matched_insert_by_name = false;

	// WHEN NOT MATCHED BY SOURCE - rows in target but not in source
	bool has_not_matched_by_source = false;
	string not_matched_by_source_condition;
	MergeAction not_matched_by_source_action = MergeAction::UPDATE;
	bool not_matched_by_source_update_by_name = false;
	// For explicit SET clauses: column names and expressions as SQL strings
	vector<pair<string, string>> not_matched_by_source_set_clauses;

	int64_t row_limit = 0;
	int64_t batch_size = 100;
};

//===--------------------------------------------------------------------===//
// Global State
//===--------------------------------------------------------------------===//

struct CrawlingMergeGlobalState : public GlobalTableFunctionState {
	bool finished = false;
	int64_t rows_inserted = 0;
	int64_t rows_updated = 0;
	int64_t rows_deleted = 0;

	// Progress tracking for progress bar
	std::atomic<int64_t> total_rows {0};
	std::atomic<int64_t> processed_rows {0};

	idx_t MaxThreads() const override {
		return 1;
	}
};

//===--------------------------------------------------------------------===//
// Bind Function
//===--------------------------------------------------------------------===//

static unique_ptr<FunctionData> CrawlingMergeBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
	auto bind_data = make_uniq<CrawlingMergeBindData>();

	// Parameters from parser (see PlanCrawl in crawl_parser.cpp)
	bind_data->source_query = StringValue::Get(input.inputs[0]);
	bind_data->source_alias = StringValue::Get(input.inputs[1]);
	bind_data->target_table = StringValue::Get(input.inputs[2]);
	bind_data->join_condition = StringValue::Get(input.inputs[3]);

	// Parse join_columns from comma-separated string
	string join_cols_str = StringValue::Get(input.inputs[4]);
	if (!join_cols_str.empty()) {
		size_t pos = 0;
		while (pos < join_cols_str.length()) {
			size_t comma = join_cols_str.find(',', pos);
			if (comma == string::npos) {
				bind_data->join_columns.push_back(join_cols_str.substr(pos));
				break;
			}
			bind_data->join_columns.push_back(join_cols_str.substr(pos, comma - pos));
			pos = comma + 1;
		}
	}

	bind_data->has_matched = input.inputs[5].GetValue<bool>();
	bind_data->matched_condition = StringValue::Get(input.inputs[6]);
	bind_data->matched_action = static_cast<MergeAction>(input.inputs[7].GetValue<int32_t>());
	bind_data->matched_update_by_name = input.inputs[8].GetValue<bool>();
	bind_data->has_not_matched = input.inputs[9].GetValue<bool>();
	bind_data->not_matched_insert_by_name = input.inputs[10].GetValue<bool>();
	bind_data->has_not_matched_by_source = input.inputs[11].GetValue<bool>();
	bind_data->not_matched_by_source_condition = StringValue::Get(input.inputs[12]);
	bind_data->not_matched_by_source_action = static_cast<MergeAction>(input.inputs[13].GetValue<int32_t>());
	bind_data->not_matched_by_source_update_by_name = input.inputs[14].GetValue<bool>();
	// Parse SET clauses from "col1=expr1;col2=expr2" format
	string set_clauses_str = StringValue::Get(input.inputs[15]);
	if (!set_clauses_str.empty()) {
		size_t pos = 0;
		while (pos < set_clauses_str.length()) {
			size_t semi = set_clauses_str.find(';', pos);
			string clause =
			    semi == string::npos ? set_clauses_str.substr(pos) : set_clauses_str.substr(pos, semi - pos);
			size_t eq = clause.find('=');
			if (eq != string::npos) {
				string col = clause.substr(0, eq);
				string expr = clause.substr(eq + 1);
				bind_data->not_matched_by_source_set_clauses.push_back({col, expr});
			}
			if (semi == string::npos)
				break;
			pos = semi + 1;
		}
	}
	bind_data->row_limit = input.inputs[16].GetValue<int64_t>();
	bind_data->batch_size = input.inputs[17].GetValue<int64_t>();

	// Return three columns: rows_inserted, rows_updated, rows_deleted
	return_types.push_back(LogicalType::BIGINT);
	names.push_back("rows_inserted");
	return_types.push_back(LogicalType::BIGINT);
	names.push_back("rows_updated");
	return_types.push_back(LogicalType::BIGINT);
	names.push_back("rows_deleted");

	return std::move(bind_data);
}

//===--------------------------------------------------------------------===//
// Init Global
//===--------------------------------------------------------------------===//

static unique_ptr<GlobalTableFunctionState> CrawlingMergeInitGlobal(ClientContext &context,
                                                                    TableFunctionInitInput &input) {
	return make_uniq<CrawlingMergeGlobalState>();
}

//===--------------------------------------------------------------------===//
// SQL Generation Helpers
//===--------------------------------------------------------------------===//

// Build WHERE clause with values substituted from source row
static string BuildWhereClause(const CrawlingMergeBindData &bind_data, const vector<string> &col_names,
                               unique_ptr<DataChunk> &chunk, idx_t row) {
	// Parse join_condition and substitute alias.col references with actual values
	string where_clause = bind_data.join_condition;
	string alias_prefix = bind_data.source_alias + ".";
	string alias_prefix_lower = StringUtil::Lower(alias_prefix);

	// For each column in the source row
	for (idx_t col = 0; col < col_names.size(); col++) {
		// Replace alias.column with the actual value
		string col_ref = alias_prefix + col_names[col];
		string col_ref_lower = alias_prefix_lower + StringUtil::Lower(col_names[col]);

		// Find and replace (case-insensitive)
		string where_lower = StringUtil::Lower(where_clause);
		size_t pos = where_lower.find(col_ref_lower);
		while (pos != string::npos) {
			auto val = chunk->GetValue(col, row);
			string val_str = val.IsNull() ? "NULL" : val.ToSQLString();
			where_clause = where_clause.substr(0, pos) + val_str + where_clause.substr(pos + col_ref.length());
			where_lower = StringUtil::Lower(where_clause);
			pos = where_lower.find(col_ref_lower, pos + val_str.length());
		}
	}

	return where_clause;
}

// Check if row exists in target table
static bool CheckExists(Connection &conn, const CrawlingMergeBindData &bind_data, const vector<string> &col_names,
                        unique_ptr<DataChunk> &chunk, idx_t row) {
	string where_clause = BuildWhereClause(bind_data, col_names, chunk, row);
	string sql = "SELECT 1 FROM " + QuoteSqlIdentifier(bind_data.target_table) + " WHERE " + where_clause + " LIMIT 1";
	auto result = conn.Query(sql);
	if (result->HasError()) {
		return false;
	}
	auto check = result->Fetch();
	return check && check->size() > 0;
}

// Check if matched condition is satisfied
static bool CheckMatchedCondition(Connection &conn, const CrawlingMergeBindData &bind_data,
                                  const vector<string> &col_names, unique_ptr<DataChunk> &chunk, idx_t row) {
	if (bind_data.matched_condition.empty()) {
		return true; // No condition = always match
	}

	string where_clause = BuildWhereClause(bind_data, col_names, chunk, row);

	// Build query to check both join condition AND matched condition
	// The matched condition may reference target table columns
	string sql = "SELECT 1 FROM " + QuoteSqlIdentifier(bind_data.target_table) + " WHERE " + where_clause + " AND (" +
	             bind_data.matched_condition + ") LIMIT 1";
	auto result = conn.Query(sql);
	if (result->HasError()) {
		return false;
	}
	auto check = result->Fetch();
	return check && check->size() > 0;
}

// Build UPDATE BY NAME statement
static string BuildUpdateByName(const CrawlingMergeBindData &bind_data, const vector<string> &col_names,
                                const vector<LogicalType> &col_types, unique_ptr<DataChunk> &chunk, idx_t row) {
	string sql = "UPDATE " + QuoteSqlIdentifier(bind_data.target_table) + " SET ";

	// Set columns by name, excluding join columns
	bool first = true;
	for (idx_t col = 0; col < col_names.size(); col++) {
		// Skip join columns (they shouldn't be updated)
		bool is_join_col = false;
		string col_lower = StringUtil::Lower(col_names[col]);
		for (const auto &jc : bind_data.join_columns) {
			if (StringUtil::Lower(jc) == col_lower) {
				is_join_col = true;
				break;
			}
		}
		if (is_join_col)
			continue;

		if (!first)
			sql += ", ";
		first = false;

		auto val = chunk->GetValue(col, row);
		sql += QuoteSqlIdentifier(col_names[col]) + " = ";
		sql += val.IsNull() ? "NULL" : val.ToSQLString();
	}

	// Add WHERE clause
	sql += " WHERE " + BuildWhereClause(bind_data, col_names, chunk, row);

	return sql;
}

// Build DELETE statement
static string BuildDelete(const CrawlingMergeBindData &bind_data, const vector<string> &col_names,
                          unique_ptr<DataChunk> &chunk, idx_t row) {
	return "DELETE FROM " + QuoteSqlIdentifier(bind_data.target_table) + " WHERE " +
	       BuildWhereClause(bind_data, col_names, chunk, row);
}

// Build INSERT BY NAME statement using DuckDB's INSERT BY NAME syntax
static string BuildInsertByName(const CrawlingMergeBindData &bind_data, const vector<string> &col_names,
                                unique_ptr<DataChunk> &chunk, idx_t row) {
	// DuckDB supports: INSERT INTO table BY NAME SELECT ... AS col1, ... AS col2
	// Build: INSERT INTO target BY NAME (SELECT val1 AS col1, val2 AS col2, ...)
	string sql = "INSERT INTO " + QuoteSqlIdentifier(bind_data.target_table) + " BY NAME (SELECT ";

	for (idx_t col = 0; col < col_names.size(); col++) {
		if (col > 0)
			sql += ", ";
		auto val = chunk->GetValue(col, row);
		sql += val.IsNull() ? "NULL" : val.ToSQLString();
		sql += " AS " + QuoteSqlIdentifier(col_names[col]);
	}
	sql += ")";

	return sql;
}

//===--------------------------------------------------------------------===//
// Main Function - Merge Execution
//===--------------------------------------------------------------------===//

static void CrawlingMergeFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->CastNoConst<CrawlingMergeBindData>();
	auto &state = data.global_state->Cast<CrawlingMergeGlobalState>();

	if (state.finished) {
		output.SetCardinality(0);
		return;
	}

	Connection conn(*context.db);

	// Load the crawler extension in the new connection
	conn.Query("LOAD crawler");

	// Initialize pipeline state for LIMIT pushdown
	if (bind_data.row_limit > 0) {
		InitPipelineLimit(*context.db, bind_data.row_limit);
	}

	// CONDITION PUSHDOWN: If there's a WHEN MATCHED AND condition, rewrite the query
	// to exclude URLs that wouldn't be updated anyway (fresh URLs).
	// This prevents unnecessary HTTP requests - table lookup is much cheaper than HTTP.
	string effective_query = bind_data.source_query;

	if (!bind_data.matched_condition.empty() && !bind_data.join_columns.empty()) {
		// Check if target table exists first (use parameterized query to avoid injection)
		auto table_check =
		    conn.Query("SELECT 1 FROM information_schema.tables WHERE table_name = $1 LIMIT 1", bind_data.target_table);
		auto table_exists = table_check->Fetch();

		if (table_exists && table_exists->size() > 0) {
			// Table exists, apply pushdown optimization
			effective_query =
			    RewriteQueryWithExclusion(bind_data.source_query, bind_data.source_alias, bind_data.target_table,
			                              bind_data.join_columns, bind_data.matched_condition);
		}
	}

	// Execute source query (possibly rewritten with exclusion CTE)
	auto query_result = conn.Query(effective_query);
	if (query_result->HasError()) {
		// If rewritten query fails, fall back to original query
		if (effective_query != bind_data.source_query) {
			query_result = conn.Query(bind_data.source_query);
			if (query_result->HasError()) {
				throw IOException("STREAM INTO source query error: " + query_result->GetError());
			}
		} else {
			throw IOException("STREAM INTO source query error: " + query_result->GetError());
		}
	}

	// Get column names and types from result
	vector<string> col_names = query_result->names;
	vector<LogicalType> col_types = query_result->types;

	// Collect all chunks first to know total (enables progress bar)
	vector<unique_ptr<DataChunk>> all_chunks;
	int64_t total_rows = 0;
	while (auto chunk = query_result->Fetch()) {
		if (chunk->size() > 0) {
			total_rows += chunk->size();
			all_chunks.push_back(std::move(chunk));
		}
	}

	// Update progress state with total
	state.total_rows.store(total_rows);
	state.processed_rows.store(0);

	// Get first chunk for table creation
	unique_ptr<DataChunk> first_chunk;
	if (!all_chunks.empty()) {
		first_chunk = std::move(all_chunks[0]);
		all_chunks.erase(all_chunks.begin());
	}

	if (first_chunk && first_chunk->size() > 0) {
		// Check if table exists
		auto check_result =
		    conn.Query("SELECT 1 FROM information_schema.tables WHERE table_name = $1", bind_data.target_table);
		auto check_chunk = check_result->Fetch();
		if (!check_chunk || check_chunk->size() == 0) {
			// Create table with columns from query result
			string create_sql = "CREATE TABLE " + QuoteSqlIdentifier(bind_data.target_table) + " (";
			for (idx_t i = 0; i < col_names.size(); i++) {
				if (i > 0)
					create_sql += ", ";
				create_sql += QuoteSqlIdentifier(col_names[i]) + " " + col_types[i].ToString();
			}
			create_sql += ")";
			conn.Query(create_sql);
		}
	}

	int64_t rows_inserted = 0;
	int64_t rows_updated = 0;
	int64_t rows_deleted = 0;
	int64_t total_processed = 0;

	// Track join keys from source for NOT MATCHED BY SOURCE handling
	unordered_set<string> source_join_keys;

	// Helper to extract join key from source row
	auto get_join_key = [&](unique_ptr<DataChunk> &chunk, idx_t row) -> string {
		string key;
		for (const auto &jc : bind_data.join_columns) {
			// Find column index by name
			for (idx_t col = 0; col < col_names.size(); col++) {
				if (StringUtil::Lower(col_names[col]) == StringUtil::Lower(jc)) {
					if (!key.empty())
						key += "\x1F"; // Unit separator
					auto val = chunk->GetValue(col, row);
					if (!val.IsNull()) {
						key += val.ToString();
					}
					break;
				}
			}
		}
		return key;
	};

	auto process_row = [&](unique_ptr<DataChunk> &chunk, idx_t row) -> bool {
		// Check limit
		if (bind_data.row_limit > 0 && total_processed >= bind_data.row_limit) {
			return false; // Stop processing
		}

		// Track join key for NOT MATCHED BY SOURCE
		if (bind_data.has_not_matched_by_source && !bind_data.join_columns.empty()) {
			source_join_keys.insert(get_join_key(chunk, row));
		}

		// Check if row exists in target
		bool exists = CheckExists(conn, bind_data, col_names, chunk, row);

		if (exists && bind_data.has_matched) {
			// Row exists, check matched condition
			if (CheckMatchedCondition(conn, bind_data, col_names, chunk, row)) {
				if (bind_data.matched_action == MergeAction::DELETE) {
					string sql = BuildDelete(bind_data, col_names, chunk, row);
					auto result = conn.Query(sql);
					if (!result->HasError()) {
						rows_deleted++;
						total_processed++;
					}
				} else {
					// UPDATE BY NAME
					string sql = BuildUpdateByName(bind_data, col_names, col_types, chunk, row);
					auto result = conn.Query(sql);
					if (!result->HasError()) {
						rows_updated++;
						total_processed++;
					}
				}
			}
		} else if (!exists && bind_data.has_not_matched) {
			// Row doesn't exist, insert
			string sql = BuildInsertByName(bind_data, col_names, chunk, row);
			auto result = conn.Query(sql);
			if (!result->HasError()) {
				rows_inserted++;
				total_processed++;
			}
		}

		// Update progress bar
		state.processed_rows.fetch_add(1);

		return true; // Continue processing
	};

	// Process first chunk
	bool continue_processing = true;
	if (first_chunk && first_chunk->size() > 0) {
		for (idx_t row = 0; row < first_chunk->size() && continue_processing; row++) {
			continue_processing = process_row(first_chunk, row);
		}
	}

	// Process remaining chunks (already collected)
	for (auto &chunk : all_chunks) {
		if (!continue_processing)
			break;
		for (idx_t row = 0; row < chunk->size() && continue_processing; row++) {
			continue_processing = process_row(chunk, row);
		}
	}

	// Handle WHEN NOT MATCHED BY SOURCE - rows in target but not in source
	if (bind_data.has_not_matched_by_source && !bind_data.join_columns.empty()) {
		// Query all join keys from target
		string target_keys_sql = "SELECT ";
		for (size_t i = 0; i < bind_data.join_columns.size(); i++) {
			if (i > 0)
				target_keys_sql += ", ";
			target_keys_sql += QuoteSqlIdentifier(bind_data.join_columns[i]);
		}
		target_keys_sql += " FROM " + QuoteSqlIdentifier(bind_data.target_table);

		// Add optional condition
		if (!bind_data.not_matched_by_source_condition.empty()) {
			target_keys_sql += " WHERE " + bind_data.not_matched_by_source_condition;
		}

		auto target_result = conn.Query(target_keys_sql);
		if (!target_result->HasError()) {
			while (auto target_chunk = target_result->Fetch()) {
				for (idx_t row = 0; row < target_chunk->size(); row++) {
					// Build join key from target row
					string key;
					for (idx_t col = 0; col < target_chunk->ColumnCount(); col++) {
						if (col > 0)
							key += "\x1F";
						auto val = target_chunk->GetValue(col, row);
						if (!val.IsNull()) {
							key += val.ToString();
						}
					}

					// Check if this key was NOT in source
					if (source_join_keys.find(key) == source_join_keys.end()) {
						// Build WHERE clause for this target row
						string where_clause;
						for (idx_t col = 0; col < bind_data.join_columns.size(); col++) {
							if (col > 0)
								where_clause += " AND ";
							auto val = target_chunk->GetValue(col, row);
							where_clause += QuoteSqlIdentifier(bind_data.join_columns[col]) + " = ";
							where_clause += val.IsNull() ? "NULL" : val.ToSQLString();
						}

						if (bind_data.not_matched_by_source_action == MergeAction::DELETE) {
							// DELETE unmatched rows
							string sql =
							    "DELETE FROM " + QuoteSqlIdentifier(bind_data.target_table) + " WHERE " + where_clause;
							auto result = conn.Query(sql);
							if (!result->HasError()) {
								rows_deleted++;
							}
						} else {
							// UPDATE with explicit SET clauses
							if (!bind_data.not_matched_by_source_set_clauses.empty()) {
								string sql = "UPDATE " + QuoteSqlIdentifier(bind_data.target_table) + " SET ";
								bool first = true;
								for (const auto &clause : bind_data.not_matched_by_source_set_clauses) {
									if (!first)
										sql += ", ";
									first = false;
									sql += QuoteSqlIdentifier(clause.first) + " = " + clause.second;
								}
								sql += " WHERE " + where_clause;
								auto result = conn.Query(sql);
								if (!result->HasError()) {
									rows_updated++;
								}
							}
						}
					}
				}
			}
		}
	}

	state.rows_inserted = rows_inserted;
	state.rows_updated = rows_updated;
	state.rows_deleted = rows_deleted;
	state.finished = true;

	// Clean up pipeline state
	if (bind_data.row_limit > 0) {
		ClearPipelineState(*context.db);
	}

	// Return counts
	output.SetValue(0, 0, Value::BIGINT(rows_inserted));
	output.SetValue(1, 0, Value::BIGINT(rows_updated));
	output.SetValue(2, 0, Value::BIGINT(rows_deleted));
	output.SetCardinality(1);
}

//===--------------------------------------------------------------------===//
// Progress Callback
//===--------------------------------------------------------------------===//

static double CrawlingMergeProgress(ClientContext &context, const FunctionData *bind_data_p,
                                    const GlobalTableFunctionState *gstate_p) {
	if (!gstate_p) {
		return -1.0; // Unknown progress
	}

	auto &gstate = gstate_p->Cast<CrawlingMergeGlobalState>();

	int64_t total = gstate.total_rows.load();
	int64_t processed = gstate.processed_rows.load();

	if (total <= 0) {
		return -1.0; // Still collecting rows
	}

	return (static_cast<double>(processed) / static_cast<double>(total)) * 100.0;
}

//===--------------------------------------------------------------------===//
// Register Function
//===--------------------------------------------------------------------===//

void RegisterCrawlingMergeFunction(ExtensionLoader &loader) {
	// Parameters: source_query, source_alias, target_table, join_condition, join_columns,
	//             has_matched, matched_condition, matched_action, matched_update_by_name,
	//             has_not_matched, not_matched_insert_by_name,
	//             has_not_matched_by_source, not_matched_by_source_condition, not_matched_by_source_action,
	//             not_matched_by_source_update_by_name, not_matched_by_source_set_clauses,
	//             row_limit, batch_size
	TableFunction func("stream_merge_internal",
	                   {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                    LogicalType::VARCHAR, LogicalType::BOOLEAN, LogicalType::VARCHAR, LogicalType::INTEGER,
	                    LogicalType::BOOLEAN, LogicalType::BOOLEAN, LogicalType::BOOLEAN, LogicalType::BOOLEAN,
	                    LogicalType::VARCHAR, LogicalType::INTEGER, LogicalType::BOOLEAN,
	                    LogicalType::VARCHAR, // SET clauses as "col=expr;col=expr"
	                    LogicalType::BIGINT, LogicalType::BIGINT},
	                   CrawlingMergeFunction, CrawlingMergeBind, CrawlingMergeInitGlobal);

	// Set progress callback for progress bar integration
	func.table_scan_progress = CrawlingMergeProgress;

	loader.RegisterFunction(func);
}

} // namespace duckdb
