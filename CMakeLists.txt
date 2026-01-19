cmake_minimum_required(VERSION 3.5)

set(TARGET_NAME crawler)

# Dependencies via vcpkg
find_package(ZLIB REQUIRED)
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

        # Get installed Rust targets
        execute_process(
            COMMAND rustup target list --installed
            OUTPUT_VARIABLE RUST_TARGETS
            OUTPUT_STRIP_TRAILING_WHITESPACE
        )

        # Detect Rust target based on platform (following yardstick pattern)
        set(RUST_TARGET "")
        if("${OS_NAME}" STREQUAL "linux")
            if ("${OS_ARCH}" STREQUAL "arm64")
                set(RUST_TARGET "aarch64-unknown-linux-gnu")
            elseif("${CMAKE_CXX_COMPILER}" MATCHES "aarch64")
                set(RUST_TARGET "aarch64-unknown-linux-gnu")
            elseif("${CMAKE_CXX_COMPILER}" MATCHES "musl")
                # Check if musl target is installed
                string(FIND "${RUST_TARGETS}" "x86_64-unknown-linux-musl" MUSL_TARGET_FOUND)
                if(NOT MUSL_TARGET_FOUND EQUAL -1)
                    set(RUST_TARGET "x86_64-unknown-linux-musl")
                else()
                    # Fallback to gnu target for musl builds
                    set(RUST_TARGET "x86_64-unknown-linux-gnu")
                    message(STATUS "musl Rust target not installed, using gnu target")
                endif()
            else()
                set(RUST_TARGET "x86_64-unknown-linux-gnu")
            endif()
        elseif("${OS_NAME}" STREQUAL "osx" OR APPLE)
            if ("${OSX_BUILD_ARCH}" STREQUAL "arm64")
                set(RUST_TARGET "aarch64-apple-darwin")
            elseif ("${OSX_BUILD_ARCH}" STREQUAL "x86_64")
                set(RUST_TARGET "x86_64-apple-darwin")
            elseif ("${OS_ARCH}" STREQUAL "arm64")
                set(RUST_TARGET "aarch64-apple-darwin")
            elseif ("${OS_ARCH}" STREQUAL "amd64")
                set(RUST_TARGET "x86_64-apple-darwin")
            elseif(CMAKE_SYSTEM_PROCESSOR STREQUAL "arm64" OR CMAKE_SYSTEM_PROCESSOR STREQUAL "aarch64")
                set(RUST_TARGET "aarch64-apple-darwin")
            else()
                set(RUST_TARGET "x86_64-apple-darwin")
            endif()
        elseif(WIN32)
            if ("${OS_ARCH}" STREQUAL "arm64" OR CMAKE_HOST_SYSTEM_PROCESSOR STREQUAL "ARM64")
                set(RUST_TARGET "aarch64-pc-windows-msvc")
            else()
                set(RUST_TARGET "x86_64-pc-windows-msvc")
            endif()
        endif()

        # Set target flag if detected
        if(NOT "${RUST_TARGET}" STREQUAL "")
            set(RUST_TARGET_FLAG "--target=${RUST_TARGET}")
            set(RUST_TARGET_DIR "${RUST_TARGET}/")
            message(STATUS "Rust target: ${RUST_TARGET}")
        else()
            set(RUST_TARGET_FLAG "")
            set(RUST_TARGET_DIR "")
            message(STATUS "Rust target: default")
        endif()

        # Platform-specific library name
        if(WIN32)
            set(RUST_LIB_NAME "rust_parser.lib")
        else()
            set(RUST_LIB_NAME "librust_parser.a")
        endif()

        set(RUST_PARSER_DIR ${CMAKE_CURRENT_SOURCE_DIR}/rust_parser)
        set(RUST_LIB_PATH ${RUST_PARSER_DIR}/target/${RUST_TARGET_DIR}${RUST_BUILD_TYPE}/${RUST_LIB_NAME})

        # Custom command to build Rust library
        add_custom_command(
            OUTPUT ${RUST_LIB_PATH}
            COMMAND ${CARGO_EXECUTABLE} build ${RUST_BUILD_FLAG} ${RUST_TARGET_FLAG}
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

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)

set(EXTENSION_SOURCES
    src/crawler_extension.cpp
    src/crawler_function.cpp
    src/crawler_utils.cpp
    src/css_extract_function.cpp
    src/crawl_stream_function.cpp
    src/crawl_table_function.cpp
    src/crawl_lateral_function.cpp
    src/stream_merge_function.cpp
    src/sitemap_function.cpp
    src/thread_utils.cpp
    src/robots_parser.cpp
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

target_link_libraries(${EXTENSION_NAME} ZLIB::ZLIB LibXml2::LibXml2)
target_link_libraries(${LOADABLE_EXTENSION_NAME} ZLIB::ZLIB LibXml2::LibXml2)

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
        target_link_libraries(${EXTENSION_NAME} "-framework Security" "-framework CoreFoundation" "-framework SystemConfiguration")
        target_link_libraries(${LOADABLE_EXTENSION_NAME} "-framework Security" "-framework CoreFoundation" "-framework SystemConfiguration")
    elseif(WIN32)
        target_link_libraries(${EXTENSION_NAME} ws2_32 userenv bcrypt ntdll)
        target_link_libraries(${LOADABLE_EXTENSION_NAME} ws2_32 userenv bcrypt ntdll)
    elseif(UNIX)
        target_link_libraries(${EXTENSION_NAME} dl pthread m)
        target_link_libraries(${LOADABLE_EXTENSION_NAME} dl pthread m)
    endif()

    # Enable Rust parser in C++ code
    target_compile_definitions(${EXTENSION_NAME} PRIVATE RUST_PARSER_AVAILABLE=1)
    target_compile_definitions(${LOADABLE_EXTENSION_NAME} PRIVATE RUST_PARSER_AVAILABLE=1)
endif()

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
