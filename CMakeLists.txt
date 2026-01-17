cmake_minimum_required(VERSION 3.5)

set(TARGET_NAME crawler)

# Dependencies via vcpkg
find_package(ZLIB REQUIRED)
find_package(CURL REQUIRED)
find_package(LibXml2 REQUIRED)

# Rust parser integration (optional - falls back to C++ extractors if not available)
option(ENABLE_RUST_PARSER "Build and link Rust HTML parser" ON)

if(ENABLE_RUST_PARSER)
    # Detect Rust toolchain
    find_program(CARGO_EXECUTABLE cargo)

    if(CARGO_EXECUTABLE)
        message(STATUS "Rust toolchain found: ${CARGO_EXECUTABLE}")

        # Determine build type for Rust
        if(CMAKE_BUILD_TYPE STREQUAL "Debug")
            set(RUST_BUILD_TYPE "debug")
            set(RUST_BUILD_FLAG "")
        else()
            set(RUST_BUILD_TYPE "release")
            set(RUST_BUILD_FLAG "--release")
        endif()

        # Platform-specific library name
        if(WIN32)
            set(RUST_LIB_NAME "rust_parser.lib")
        else()
            set(RUST_LIB_NAME "librust_parser.a")
        endif()

        set(RUST_PARSER_DIR ${CMAKE_CURRENT_SOURCE_DIR}/rust_parser)
        set(RUST_LIB_PATH ${RUST_PARSER_DIR}/target/${RUST_BUILD_TYPE}/${RUST_LIB_NAME})

        # Custom command to build Rust library
        add_custom_command(
            OUTPUT ${RUST_LIB_PATH}
            COMMAND ${CARGO_EXECUTABLE} build ${RUST_BUILD_FLAG}
            WORKING_DIRECTORY ${RUST_PARSER_DIR}
            COMMENT "Building Rust HTML parser..."
            DEPENDS
                ${RUST_PARSER_DIR}/Cargo.toml
                ${RUST_PARSER_DIR}/src/lib.rs
                ${RUST_PARSER_DIR}/src/ffi.rs
                ${RUST_PARSER_DIR}/src/extractors.rs
        )

        # Create imported library target
        add_custom_target(rust_parser_build DEPENDS ${RUST_LIB_PATH})

        add_library(rust_parser STATIC IMPORTED GLOBAL)
        set_target_properties(rust_parser PROPERTIES
            IMPORTED_LOCATION ${RUST_LIB_PATH}
        )
        add_dependencies(rust_parser rust_parser_build)

        set(RUST_PARSER_AVAILABLE ON)
        message(STATUS "Rust parser: ENABLED")
    else()
        message(STATUS "Rust toolchain not found - Rust parser: DISABLED")
        set(RUST_PARSER_AVAILABLE OFF)
    endif()
else()
    message(STATUS "Rust parser: DISABLED (ENABLE_RUST_PARSER=OFF)")
    set(RUST_PARSER_AVAILABLE OFF)
endif()

# Check for HTTP/2 support via curl
# vcpkg's curl with http2 feature links nghttp2 statically
include(CheckSymbolExists)
set(CMAKE_REQUIRED_LIBRARIES CURL::libcurl)
check_symbol_exists(CURL_HTTP_VERSION_2TLS "curl/curl.h" CURL_HAS_HTTP2)
if(CURL_HAS_HTTP2)
    message(STATUS "HTTP/2 support: ENABLED (curl has HTTP/2)")
    set(CRAWLER_HTTP2_SUPPORT ON)
else()
    message(STATUS "HTTP/2 support: DISABLED (curl lacks HTTP/2)")
    set(CRAWLER_HTTP2_SUPPORT OFF)
endif()

# Check for HTTP/3 support via curl
check_symbol_exists(CURL_HTTP_VERSION_3 "curl/curl.h" CURL_HAS_HTTP3)
if(CURL_HAS_HTTP3)
    message(STATUS "HTTP/3 support: ENABLED (curl has HTTP/3)")
    set(CRAWLER_HTTP3_SUPPORT ON)
else()
    message(STATUS "HTTP/3 support: DISABLED (curl lacks HTTP/3)")
    set(CRAWLER_HTTP3_SUPPORT OFF)
endif()

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)

set(EXTENSION_SOURCES
    src/crawler_extension.cpp
    src/crawler_function.cpp
    src/crawler_worker.cpp
    src/crawler_batch.cpp
    src/crawler_utils.cpp
    src/css_extract_function.cpp
    src/crawl_stream_function.cpp
    src/crawl_table_function.cpp
    src/crawl_lateral_function.cpp
    src/stream_into_function.cpp
    src/sitemap_function.cpp
    src/thread_utils.cpp
    src/robots_parser.cpp
    src/http_client.cpp
    src/crawl_parser.cpp
    src/sitemap_parser.cpp
    src/link_parser.cpp
    src/json_path_evaluator.cpp
)

# Add Rust FFI wrapper if Rust parser is available
if(RUST_PARSER_AVAILABLE)
    list(APPEND EXTENSION_SOURCES src/rust_ffi.cpp)
endif()

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

target_link_libraries(${EXTENSION_NAME} ZLIB::ZLIB CURL::libcurl LibXml2::LibXml2)
target_link_libraries(${LOADABLE_EXTENSION_NAME} ZLIB::ZLIB CURL::libcurl LibXml2::LibXml2)

# Link Rust parser if available
if(RUST_PARSER_AVAILABLE)
    # Add dependency to ensure Rust is built first
    add_dependencies(${EXTENSION_NAME} rust_parser_build)
    add_dependencies(${LOADABLE_EXTENSION_NAME} rust_parser_build)

    # Link the Rust static library
    target_link_libraries(${EXTENSION_NAME} ${RUST_LIB_PATH})
    target_link_libraries(${LOADABLE_EXTENSION_NAME} ${RUST_LIB_PATH})

    # Platform-specific system libraries required by Rust
    if(APPLE)
        target_link_libraries(${EXTENSION_NAME} "-framework Security" "-framework CoreFoundation")
        target_link_libraries(${LOADABLE_EXTENSION_NAME} "-framework Security" "-framework CoreFoundation")
    elseif(UNIX)
        target_link_libraries(${EXTENSION_NAME} dl pthread m)
        target_link_libraries(${LOADABLE_EXTENSION_NAME} dl pthread m)
    endif()

    # Enable Rust parser in C++ code
    target_compile_definitions(${EXTENSION_NAME} PRIVATE RUST_PARSER_AVAILABLE=1)
    target_compile_definitions(${LOADABLE_EXTENSION_NAME} PRIVATE RUST_PARSER_AVAILABLE=1)
endif()

# Set compile definitions based on detected features
if(CRAWLER_HTTP2_SUPPORT)
    target_compile_definitions(${EXTENSION_NAME} PRIVATE CRAWLER_HTTP2_SUPPORT=1)
    target_compile_definitions(${LOADABLE_EXTENSION_NAME} PRIVATE CRAWLER_HTTP2_SUPPORT=1)
endif()

if(CRAWLER_HTTP3_SUPPORT)
    target_compile_definitions(${EXTENSION_NAME} PRIVATE CRAWLER_HTTP3_SUPPORT=1)
    target_compile_definitions(${LOADABLE_EXTENSION_NAME} PRIVATE CRAWLER_HTTP3_SUPPORT=1)
endif()

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
