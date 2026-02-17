#pragma once

#include "duckdb/main/client_context.hpp"
#include <atomic>
#include <memory>

namespace duckdb {

// Shared pipeline state for LIMIT pushdown across table function calls
struct PipelineState {
	std::atomic<int64_t> remaining;
	std::atomic<bool> stopped;

	PipelineState(int64_t limit) : remaining(limit), stopped(false) {
	}
};

// Initialize pipeline limit for a database instance (call before running query)
void InitPipelineLimit(DatabaseInstance &db, int64_t limit);

// Get existing pipeline state for a database instance (returns nullptr if not set)
std::shared_ptr<PipelineState> GetPipelineState(DatabaseInstance &db);

// Clear pipeline state for a database instance
void ClearPipelineState(DatabaseInstance &db);

} // namespace duckdb
