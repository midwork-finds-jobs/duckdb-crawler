cmake_minimum_required(VERSION 3.5)

set(TARGET_NAME crawler)

# Dependencies via vcpkg
find_package(ZLIB REQUIRED)
find_package(CURL REQUIRED)

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
    src/crawler_utils.cpp
    src/thread_utils.cpp
    src/robots_parser.cpp
    src/http_client.cpp
    src/crawl_parser.cpp
    src/sitemap_parser.cpp
    src/link_parser.cpp
)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

target_link_libraries(${EXTENSION_NAME} ZLIB::ZLIB CURL::libcurl)
target_link_libraries(${LOADABLE_EXTENSION_NAME} ZLIB::ZLIB CURL::libcurl)

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
