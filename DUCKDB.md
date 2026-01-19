# DuckDB Deep Integration Guide

Learnings from building the crawler extension with Rust+C++ FFI, custom SQL syntax, and advanced table functions.

## Table of Contents
1. [Progress Bar Integration](#progress-bar-integration)
2. [Predicate Pushdown (LIMIT Detection)](#predicate-pushdown-limit-detection)
3. [LATERAL Join Support](#lateral-join-support)
4. [Parser Extension (Custom SQL)](#parser-extension-custom-sql)
5. [Rust FFI Integration](#rust-ffi-integration)
6. [Complex Return Types](#complex-return-types)
7. [CMake Build with Rust](#cmake-build-with-rust)

---

## Progress Bar Integration

DuckDB displays progress for long-running queries. Table functions can report progress via callback.

### Implementation

```cpp
// stream_merge_function.cpp

struct CrawlingMergeGlobalState : public GlobalTableFunctionState {
    std::atomic<int64_t> total_rows{0};
    std::atomic<int64_t> processed_rows{0};
};

// Progress callback - returns percentage (0-100) or -1 for unknown
static double CrawlingMergeProgress(ClientContext &context,
                                    const FunctionData *bind_data_p,
                                    const GlobalTableFunctionState *gstate_p) {
    auto &gstate = gstate_p->Cast<CrawlingMergeGlobalState>();
    int64_t total = gstate.total_rows.load();
    int64_t processed = gstate.processed_rows.load();

    if (total <= 0) {
        return -1.0;  // Unknown progress
    }
    return (static_cast<double>(processed) / static_cast<double>(total)) * 100.0;
}

// Register callback
TableFunction func("crawling_merge", ...);
func.table_scan_progress = CrawlingMergeProgress;
```

### Key Points
- Use `std::atomic<int64_t>` for thread-safe counters
- Return `-1.0` when total is unknown
- Register via `TableFunction::table_scan_progress`

---

## Predicate Pushdown (LIMIT Detection)

Enable early termination when user specifies `LIMIT N`.

### Pattern: Report High Cardinality

```cpp
// crawl_table_function.cpp

// Report artificially high cardinality to optimizer
static constexpr idx_t CRAWL_REPORTED_CARDINALITY = 1000000;

static unique_ptr<NodeStatistics> CrawlCardinality(ClientContext &context,
                                                   const FunctionData *bind_data) {
    return make_uniq<NodeStatistics>(CRAWL_REPORTED_CARDINALITY,
                                     CRAWL_REPORTED_CARDINALITY);
}

// Register
func.cardinality = CrawlCardinality;
```

### Pattern: Detect LIMIT in InitGlobal

```cpp
static unique_ptr<GlobalTableFunctionState> CrawlInitGlobal(
    ClientContext &context,
    TableFunctionInitInput &input) {

    auto state = make_uniq<CrawlGlobalState>();

    // Detect LIMIT via optimizer's estimated_cardinality
    if (input.op) {
        idx_t estimated = input.op->estimated_cardinality;
        // If estimated < reported, LIMIT was applied
        if (estimated > 0 && estimated < CRAWL_REPORTED_CARDINALITY) {
            state->limit_from_query = static_cast<int64_t>(estimated);
        }
    }
    return std::move(state);
}
```

### Pattern: Yield ONE Row Per Call

Critical for LIMIT to interrupt between rows:

```cpp
static void CrawlFunction(ClientContext &context,
                          TableFunctionInput &data,
                          DataChunk &output) {
    auto &state = data.global_state->Cast<CrawlGlobalState>();

    // Effective limit = min(user parameter, detected LIMIT)
    int64_t effective_limit = state.max_results;
    if (state.limit_from_query > 0) {
        effective_limit = std::min(effective_limit, state.limit_from_query);
    }

    // Process ONE row per call - allows LIMIT to interrupt
    while (count < 1) {
        if (state.results_returned >= effective_limit) {
            state.finished = true;
            break;
        }

        string url = GetNextUrl(state);
        CrawlResult result = FetchUrl(url);

        output.SetValue(0, 0, Value(result.url));
        output.SetCardinality(1);
        state.results_returned++;

        break;  // Return after ONE row
    }
}
```

---

## LATERAL Join Support

For `crawl_url()` used in LATERAL joins: `SELECT * FROM urls, LATERAL crawl_url(urls.url)`.

### In-Out Function Pattern

```cpp
// crawl_lateral_function.cpp

// Shared state across all LATERAL calls for LIMIT
struct PipelineState {
    std::atomic<int64_t> remaining;
    std::atomic<bool> stopped{false};
};

struct CrawlUrlLocalState : public LocalTableFunctionState {
    idx_t current_row = 0;
    idx_t input_size = 0;
};

static OperatorResultType CrawlUrlInOut(ExecutionContext &context,
                                        TableFunctionInput &data,
                                        DataChunk &input,
                                        DataChunk &output) {
    auto &local_state = data.local_state->Cast<CrawlUrlLocalState>();
    auto &bind_data = data.bind_data->Cast<CrawlUrlBindData>();

    while (local_state.current_row < local_state.input_size) {
        // Check pipeline-wide stop flag
        if (bind_data.pipeline_state && bind_data.pipeline_state->stopped.load()) {
            return OperatorResultType::NEED_MORE_INPUT;
        }

        Value url_val = input.GetValue(0, local_state.current_row);
        CrawlResult result = FetchUrl(url_val.ToString());

        output.SetValue(0, 0, Value(result.url));
        output.SetCardinality(1);
        local_state.current_row++;

        // Decrement shared counter
        if (bind_data.pipeline_state) {
            int64_t remaining = --bind_data.pipeline_state->remaining;
            if (remaining <= 0) {
                bind_data.pipeline_state->stopped = true;
            }
        }

        // More rows in this chunk? Signal HAVE_MORE_OUTPUT
        if (local_state.current_row < local_state.input_size) {
            return OperatorResultType::HAVE_MORE_OUTPUT;
        }
        return OperatorResultType::NEED_MORE_INPUT;
    }
    return OperatorResultType::NEED_MORE_INPUT;
}

// Register as in_out_function
TableFunction func("crawl_url", {LogicalType::VARCHAR}, nullptr, CrawlUrlBind);
func.in_out_function = CrawlUrlInOut;
func.init_local = CrawlUrlInitLocal;
```

### Return Types
- `NEED_MORE_INPUT`: Done with current input chunk
- `HAVE_MORE_OUTPUT`: More output rows pending from current input
- `FINISHED`: No more output

---

## Parser Extension (Custom SQL)

Add custom SQL syntax like `CRAWLING MERGE INTO ... USING ... ON ...`.

### Parse Data Structure

```cpp
// crawl_parser.cpp

struct CrawlingMergeAction {
    MergeAction action_type;  // UPDATE, INSERT, DELETE
    unique_ptr<Expression> condition;
    vector<string> set_columns;
    vector<unique_ptr<Expression>> set_expressions;
};

struct CrawlingMergeParseData : public ParserExtensionParseData {
    unique_ptr<TableRef> target;
    unique_ptr<TableRef> source;
    unique_ptr<Expression> join_condition;
    vector<string> using_columns;
    unordered_map<string, vector<CrawlingMergeAction>> actions;

    unique_ptr<ParserExtensionParseData> Copy() const override {
        // Deep copy implementation
    }
};
```

### Parser Extension Class

```cpp
class CrawlingParserExtension : public ParserExtension {
public:
    CrawlingParserExtension() {
        parse_function = CrawlingParse;
        plan_function = CrawlingPlan;
    }

    static ParserExtensionParseResult CrawlingParse(ParserExtensionInfo *info,
                                                    const string &query) {
        // Check if query starts with "CRAWLING"
        if (!StartsWithCaseInsensitive(query, "CRAWLING")) {
            return ParserExtensionParseResult();  // Not our syntax
        }

        // Parse the custom syntax
        auto parse_data = make_uniq<CrawlingMergeParseData>();
        // ... tokenize and parse ...

        return ParserExtensionParseResult(
            make_uniq_base<ParserExtensionParseData>(std::move(parse_data)));
    }

    static ParserExtensionPlanResult CrawlingPlan(ParserExtensionInfo *info,
                                                  ClientContext &context,
                                                  ParserExtensionParseData *parse_data) {
        auto &data = parse_data->Cast<CrawlingMergeParseData>();

        // Convert to table function call
        auto table_func = make_uniq<TableFunctionRef>();
        table_func->function = make_uniq<FunctionExpression>(
            "crawling_merge_impl", std::move(args));

        return ParserExtensionPlanResult(std::move(table_func));
    }
};

// Register in extension load
void CrawlerExtension::Load(DuckDB &db) {
    auto &config = DBConfig::GetConfig(*db.instance);
    config.parser_extensions.push_back(CrawlingParserExtension());
}
```

---

## Rust FFI Integration

Integrate Rust code for HTML parsing, async HTTP, etc.

### C++ Side

```cpp
// rust_ffi.hpp

extern "C" {
    struct ExtractionResultFFI {
        char *json_ptr;   // Caller must free via free_extraction_result
        char *error_ptr;  // Non-null if error occurred
    };

    ExtractionResultFFI extract_from_html(const char *html_ptr,
                                          size_t html_len,
                                          const char *request_json);
    void free_extraction_result(ExtractionResultFFI result);

    // Batch crawl with async HTTP
    ExtractionResultFFI crawl_batch_ffi(const char *request_json);

    // Signal handling
    void set_interrupted(bool interrupted);
}

// RAII wrapper for automatic cleanup
class RustResult {
    ExtractionResultFFI result_;
public:
    explicit RustResult(ExtractionResultFFI result) : result_(result) {}
    ~RustResult() { free_extraction_result(result_); }

    bool HasError() const { return result_.error_ptr != nullptr; }
    string GetError() const {
        return result_.error_ptr ? string(result_.error_ptr) : "";
    }
    string GetJson() const {
        return result_.json_ptr ? string(result_.json_ptr) : "{}";
    }
};

// Usage
string ExtractHtml(const string &html, const string &config_json) {
#if defined(RUST_PARSER_AVAILABLE)
    auto ffi = extract_from_html(html.c_str(), html.length(), config_json.c_str());
    RustResult result(ffi);
    if (result.HasError()) {
        throw InvalidInputException("Rust parser error: %s", result.GetError());
    }
    return result.GetJson();
#else
    return "{}";  // Fallback when Rust not available
#endif
}
```

### Rust Side

```rust
// rust_parser/src/ffi.rs

use std::ffi::{c_char, CStr, CString};

#[repr(C)]
pub struct ExtractionResultFFI {
    pub json_ptr: *mut c_char,
    pub error_ptr: *mut c_char,
}

impl ExtractionResultFFI {
    fn success(json: String) -> Self {
        Self {
            json_ptr: CString::new(json).unwrap().into_raw(),
            error_ptr: std::ptr::null_mut(),
        }
    }

    fn error(msg: String) -> Self {
        Self {
            json_ptr: std::ptr::null_mut(),
            error_ptr: CString::new(msg).unwrap().into_raw(),
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn extract_from_html(
    html_ptr: *const c_char,
    html_len: usize,
    request_json: *const c_char,
) -> ExtractionResultFFI {
    // Convert C strings to Rust
    let html = std::slice::from_raw_parts(html_ptr as *const u8, html_len);
    let html_str = match std::str::from_utf8(html) {
        Ok(s) => s,
        Err(e) => return ExtractionResultFFI::error(format!("Invalid UTF-8: {}", e)),
    };

    let request = CStr::from_ptr(request_json).to_string_lossy();

    // Parse and extract
    match do_extraction(html_str, &request) {
        Ok(result) => ExtractionResultFFI::success(serde_json::to_string(&result).unwrap()),
        Err(e) => ExtractionResultFFI::error(e.to_string()),
    }
}

#[no_mangle]
pub unsafe extern "C" fn free_extraction_result(result: ExtractionResultFFI) {
    if !result.json_ptr.is_null() {
        drop(CString::from_raw(result.json_ptr));
    }
    if !result.error_ptr.is_null() {
        drop(CString::from_raw(result.error_ptr));
    }
}

// Async batch crawl with tokio
#[no_mangle]
pub unsafe extern "C" fn crawl_batch_ffi(request_json: *const c_char) -> ExtractionResultFFI {
    let request = CStr::from_ptr(request_json).to_string_lossy();

    let runtime = tokio::runtime::Runtime::new().unwrap();
    let results = runtime.block_on(async {
        crawl_urls_concurrently(&request).await
    });

    match results {
        Ok(r) => ExtractionResultFFI::success(serde_json::to_string(&r).unwrap()),
        Err(e) => ExtractionResultFFI::error(e.to_string()),
    }
}
```

### Signal Handling for Graceful Shutdown

```rust
// Rust side
use std::sync::atomic::{AtomicBool, Ordering};

static INTERRUPTED: AtomicBool = AtomicBool::new(false);

#[no_mangle]
pub extern "C" fn set_interrupted(interrupted: bool) {
    INTERRUPTED.store(interrupted, Ordering::SeqCst);
}

pub fn is_interrupted() -> bool {
    INTERRUPTED.load(Ordering::SeqCst)
}

// Check in long-running operations
async fn crawl_urls_concurrently(urls: &[String]) -> Result<Vec<CrawlResult>> {
    for url in urls {
        if is_interrupted() {
            return Err(anyhow!("Interrupted by user"));
        }
        // ... fetch URL ...
    }
}
```

---

## Complex Return Types

### STRUCT with Nested JSON and MAP

```cpp
// crawl_table_function.cpp

static unique_ptr<FunctionData> CrawlBind(ClientContext &context,
                                          TableFunctionBindInput &input,
                                          vector<LogicalType> &return_types,
                                          vector<string> &names) {
    // Simple columns
    return_types.push_back(LogicalType::VARCHAR);  // url
    names.push_back("url");

    return_types.push_back(LogicalType::INTEGER);  // status
    names.push_back("status");

    // Complex STRUCT column
    child_list_t<LogicalType> html_struct;
    html_struct.push_back({"document", LogicalType::VARCHAR});
    html_struct.push_back({"js", LogicalType::JSON()});
    html_struct.push_back({"opengraph", LogicalType::JSON()});

    // MAP type: schema name -> JSON object
    html_struct.push_back({"schema",
        LogicalType::MAP(LogicalType::VARCHAR, LogicalType::JSON())});

    // Nested STRUCT
    child_list_t<LogicalType> readability_struct;
    readability_struct.push_back({"title", LogicalType::VARCHAR});
    readability_struct.push_back({"content", LogicalType::VARCHAR});
    readability_struct.push_back({"text_content", LogicalType::VARCHAR});
    html_struct.push_back({"readability", LogicalType::STRUCT(readability_struct)});

    return_types.push_back(LogicalType::STRUCT(html_struct));
    names.push_back("html");

    return make_uniq<CrawlBindData>();
}

// Build struct value
static Value BuildHtmlStructValue(const CrawlResult &result) {
    child_list_t<Value> html_values;

    html_values.push_back({"document", Value(result.body)});
    html_values.push_back({"js", Value::JSON(result.js_json)});
    html_values.push_back({"opengraph", Value::JSON(result.og_json)});

    // Build MAP value
    vector<Value> keys, values;
    for (auto &[name, json] : result.schemas) {
        keys.push_back(Value(name));
        values.push_back(Value::JSON(json));
    }
    html_values.push_back({"schema", Value::MAP(
        LogicalType::VARCHAR, LogicalType::JSON(), keys, values)});

    // Nested struct
    child_list_t<Value> readability_values;
    readability_values.push_back({"title", Value(result.title)});
    readability_values.push_back({"content", Value(result.content)});
    readability_values.push_back({"text_content", Value(result.text)});
    html_values.push_back({"readability", Value::STRUCT(readability_values)});

    return Value::STRUCT(std::move(html_values));
}
```

---

## CMake Build with Rust

### CMakeLists.txt Pattern

```cmake
# Rust parser integration (optional)
option(ENABLE_RUST_PARSER "Build Rust HTML parser" ON)

if(ENABLE_RUST_PARSER)
    find_program(CARGO_EXECUTABLE cargo)

    if(CARGO_EXECUTABLE)
        # Get installed targets
        execute_process(
            COMMAND rustup target list --installed
            OUTPUT_VARIABLE RUST_TARGETS
            OUTPUT_STRIP_TRAILING_WHITESPACE
        )

        # Detect platform - check DUCKDB_EXPLICIT_PLATFORM first (CI sets this)
        set(IS_MUSL_BUILD OFF)
        if("${DUCKDB_EXPLICIT_PLATFORM}" MATCHES "musl")
            set(IS_MUSL_BUILD ON)
        elseif("${CMAKE_CXX_COMPILER}" MATCHES "musl")
            set(IS_MUSL_BUILD ON)
        endif()

        # Select Rust target
        set(RUST_TARGET "")
        if("${OS_NAME}" STREQUAL "linux")
            if(IS_MUSL_BUILD)
                string(FIND "${RUST_TARGETS}" "x86_64-unknown-linux-musl" FOUND)
                if(NOT FOUND EQUAL -1)
                    set(RUST_TARGET "x86_64-unknown-linux-musl")
                else()
                    # Can't mix musl C with glibc Rust - disable
                    set(RUST_PARSER_AVAILABLE OFF)
                    message(STATUS "musl Rust target not installed, disabling Rust")
                endif()
            elseif("${OS_ARCH}" STREQUAL "arm64")
                set(RUST_TARGET "aarch64-unknown-linux-gnu")
            else()
                set(RUST_TARGET "x86_64-unknown-linux-gnu")
            endif()
        elseif(APPLE)
            if("${OSX_BUILD_ARCH}" STREQUAL "arm64")
                set(RUST_TARGET "aarch64-apple-darwin")
            else()
                set(RUST_TARGET "x86_64-apple-darwin")
            endif()
        elseif(WIN32)
            set(RUST_TARGET "x86_64-pc-windows-msvc")
        endif()

        # Build command
        set(RUST_BUILD_FLAG "--release")
        if(NOT "${RUST_TARGET}" STREQUAL "")
            set(RUST_TARGET_FLAG "--target=${RUST_TARGET}")
            set(RUST_TARGET_DIR "${RUST_TARGET}/")
        endif()

        # Library path
        if(WIN32)
            set(RUST_LIB_NAME "rust_parser.lib")
        else()
            set(RUST_LIB_NAME "librust_parser.a")
        endif()
        set(RUST_LIB_PATH ${CMAKE_CURRENT_SOURCE_DIR}/rust_parser/target/${RUST_TARGET_DIR}release/${RUST_LIB_NAME})

        # Custom build command
        add_custom_command(
            OUTPUT ${RUST_LIB_PATH}
            COMMAND ${CARGO_EXECUTABLE} build ${RUST_BUILD_FLAG} ${RUST_TARGET_FLAG}
            WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/rust_parser
            COMMENT "Building Rust parser..."
            DEPENDS
                rust_parser/Cargo.toml
                rust_parser/src/lib.rs
                rust_parser/src/ffi.rs
        )

        add_custom_target(rust_parser_build DEPENDS ${RUST_LIB_PATH})
        add_library(rust_parser STATIC IMPORTED GLOBAL)
        set_target_properties(rust_parser PROPERTIES IMPORTED_LOCATION ${RUST_LIB_PATH})

        set(RUST_PARSER_AVAILABLE ON)
    endif()
endif()

# Link Rust if available
if(RUST_PARSER_AVAILABLE)
    add_dependencies(${EXTENSION_NAME} rust_parser_build)
    target_link_libraries(${EXTENSION_NAME} ${RUST_LIB_PATH})

    # Platform-specific system libraries for Rust
    if(APPLE)
        target_link_libraries(${EXTENSION_NAME}
            "-framework Security" "-framework CoreFoundation")
    elseif(WIN32)
        target_link_libraries(${EXTENSION_NAME} ws2_32 userenv bcrypt ntdll)
    elseif(UNIX)
        target_link_libraries(${EXTENSION_NAME} dl pthread m)
    endif()

    target_compile_definitions(${EXTENSION_NAME} PRIVATE RUST_PARSER_AVAILABLE=1)
endif()
```

### Community Extensions description.yml

```yaml
extension:
  name: crawler
  requires_toolchains: rust
  # Exclude platforms where Rust can't build (wasm needs reqwest-wasm)
  excluded_platforms: "windows_amd64_mingw;wasm_mvp;wasm_eh;wasm_threads"

repo:
  github: username/repo
  ref: <full-commit-sha>
```

---

## Tips and Gotchas

1. **LIMIT Pushdown**: Must yield ONE row per function call, otherwise LIMIT can't interrupt
2. **musl Detection**: Check `DUCKDB_EXPLICIT_PLATFORM` env var, not just compiler path
3. **Rust FFI**: Always use RAII wrappers for automatic memory cleanup
4. **Progress**: Return -1.0 when total is unknown, 0-100 for percentage
5. **LATERAL**: Use `OperatorResultType::HAVE_MORE_OUTPUT` to allow interruption between rows
6. **Parser Extensions**: Return empty result if query doesn't match your syntax
7. **wasm**: openssl-sys can't compile for wasm - use `reqwest-wasm` or exclude platforms
