// stream_into_internal - Streaming query execution with incremental writes
//
// Executes a query and writes results in batches to a target table.
//
// Usage (via STREAM statement):
//   STREAM (SELECT * FROM crawl(['https://example.com'])) INTO my_results;
//   STREAM (SELECT ...) INTO results WITH (batch_size 50);

#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/parser.hpp"
#include "crawler_utils.hpp"
#include "pipeline_state.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Bind Data
//===--------------------------------------------------------------------===//

struct StreamIntoBindData : public TableFunctionData {
	string source_query;
	string target_table;
	int64_t batch_size = 100;
	int64_t row_limit = 0; // 0 = unlimited
};

//===--------------------------------------------------------------------===//
// Global State
//===--------------------------------------------------------------------===//

struct StreamIntoGlobalState : public GlobalTableFunctionState {
	bool finished = false;
	int64_t rows_inserted = 0;
	int64_t batches_written = 0;

	idx_t MaxThreads() const override {
		return 1;
	}
};

//===--------------------------------------------------------------------===//
// Bind Function
//===--------------------------------------------------------------------===//

static unique_ptr<FunctionData> StreamIntoBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<string> &names) {
	auto bind_data = make_uniq<StreamIntoBindData>();

	// Parameters from parser: source_query, target_table, batch_size, row_limit
	bind_data->source_query = StringValue::Get(input.inputs[0]);
	bind_data->target_table = StringValue::Get(input.inputs[1]);
	bind_data->batch_size = input.inputs[2].GetValue<int64_t>();
	bind_data->row_limit = input.inputs[3].GetValue<int64_t>();

	// Return single column: rows_inserted
	return_types.push_back(LogicalType::BIGINT);
	names.push_back("rows_inserted");

	return std::move(bind_data);
}

//===--------------------------------------------------------------------===//
// Init Global
//===--------------------------------------------------------------------===//

static unique_ptr<GlobalTableFunctionState> StreamIntoInitGlobal(ClientContext &context,
                                                                 TableFunctionInitInput &input) {
	return make_uniq<StreamIntoGlobalState>();
}

//===--------------------------------------------------------------------===//
// Main Function - Streaming Execution
//===--------------------------------------------------------------------===//

static void StreamIntoFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->CastNoConst<StreamIntoBindData>();
	auto &state = data.global_state->Cast<StreamIntoGlobalState>();

	if (state.finished) {
		output.SetCardinality(0);
		return;
	}

	Connection conn(*context.db);

	// Load the crawler extension in the new connection (required for crawl functions)
	conn.Query("LOAD crawler");

	// Initialize pipeline state for LIMIT pushdown to crawl functions
	// This allows crawl_url in LATERAL to respect the LIMIT
	if (bind_data.row_limit > 0) {
		InitPipelineLimit(*context.db, bind_data.row_limit);
	}

	// Execute source query
	auto query_result = conn.Query(bind_data.source_query);
	if (query_result->HasError()) {
		throw IOException("STREAM source query error: " + query_result->GetError());
	}

	// Create target table from first chunk if needed
	auto first_chunk = query_result->Fetch();

	if (first_chunk && first_chunk->size() > 0) {
		// Check if table exists
		auto check_result =
		    conn.Query("SELECT 1 FROM information_schema.tables WHERE table_name = $1", bind_data.target_table);
		auto check_chunk = check_result->Fetch();
		if (!check_chunk || check_chunk->size() == 0) {
			// Create table with columns from query result
			string create_sql = "CREATE TABLE " + QuoteSqlIdentifier(bind_data.target_table) + " (";
			for (idx_t i = 0; i < query_result->names.size(); i++) {
				if (i > 0)
					create_sql += ", ";
				create_sql += QuoteSqlIdentifier(query_result->names[i]) + " " + query_result->types[i].ToString();
			}
			create_sql += ")";
			conn.Query(create_sql);
		}
	}

	// Process chunks and insert rows
	int64_t total_inserted = 0;
	idx_t batch_rows = 0;
	bool limit_reached = false;

	auto insert_row = [&](unique_ptr<DataChunk> &chunk, idx_t row) -> bool {
		// Check if limit reached before inserting
		if (bind_data.row_limit > 0 && total_inserted >= bind_data.row_limit) {
			return false; // Stop processing
		}

		string insert_sql = "INSERT INTO " + QuoteSqlIdentifier(bind_data.target_table) + " VALUES (";
		for (idx_t col = 0; col < chunk->ColumnCount(); col++) {
			if (col > 0)
				insert_sql += ", ";
			auto val = chunk->GetValue(col, row);
			if (val.IsNull()) {
				insert_sql += "NULL";
			} else {
				insert_sql += val.ToSQLString();
			}
		}
		insert_sql += ")";
		conn.Query(insert_sql);
		total_inserted++;
		batch_rows++;

		if (batch_rows >= (idx_t)bind_data.batch_size) {
			state.batches_written++;
			batch_rows = 0;
		}

		return true; // Continue processing
	};

	// Process first chunk
	if (first_chunk && first_chunk->size() > 0) {
		for (idx_t row = 0; row < first_chunk->size(); row++) {
			if (!insert_row(first_chunk, row)) {
				limit_reached = true;
				break;
			}
		}
	}

	// Process remaining chunks (unless limit already reached)
	while (!limit_reached) {
		auto chunk = query_result->Fetch();
		if (!chunk || chunk->size() == 0)
			break;
		for (idx_t row = 0; row < chunk->size(); row++) {
			if (!insert_row(chunk, row)) {
				limit_reached = true;
				break;
			}
		}
	}

	state.rows_inserted = total_inserted;
	state.finished = true;

	// Clean up pipeline state
	if (bind_data.row_limit > 0) {
		ClearPipelineState(*context.db);
	}

	// Return rows_inserted count
	output.SetValue(0, 0, Value::BIGINT(total_inserted));
	output.SetCardinality(1);
}

//===--------------------------------------------------------------------===//
// Register Function
//===--------------------------------------------------------------------===//

void RegisterStreamIntoFunction(ExtensionLoader &loader) {
	TableFunction func("stream_into_internal",
	                   {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::BIGINT},
	                   StreamIntoFunction, StreamIntoBind, StreamIntoInitGlobal);

	loader.RegisterFunction(func);
}

} // namespace duckdb
