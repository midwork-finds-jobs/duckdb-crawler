cmake_minimum_required(VERSION 3.5)

set(TARGET_NAME crawler)

# Dependencies via vcpkg
find_package(ZLIB REQUIRED)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)

set(EXTENSION_SOURCES
    src/crawler_extension.cpp
    src/crawler_function.cpp
    src/robots_parser.cpp
    src/http_client.cpp
    src/crawl_parser.cpp
    src/sitemap_parser.cpp
)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

target_link_libraries(${EXTENSION_NAME} ZLIB::ZLIB)
target_link_libraries(${LOADABLE_EXTENSION_NAME} ZLIB::ZLIB)

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
