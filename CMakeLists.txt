cmake_minimum_required(VERSION 4.0)

# Allow standalone build or as subdirectory
if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
    project(wpSQL VERSION 1.0.0 LANGUAGES C CXX)
    set(CMAKE_CXX_STANDARD 23)
    set(CMAKE_CXX_STANDARD_REQUIRED ON)
    set(WPSQL_STANDALONE TRUE)
else()
    set(WPSQL_STANDALONE FALSE)
endif()

# Library sources (logger is external)
set(WPSQL_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/src/checkSchema.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/src/dbbackup.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/src/lockTimeout.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/src/rDb.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/src/rDb-SQLite3.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/src/timefunctions.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/src/wpSQLDatabase.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/src/sqlite3/sqlite3.c
    ${CMAKE_CURRENT_SOURCE_DIR}/src/ZIP.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/src/ulid.cpp
)

# Library headers
set(WPSQL_HEADERS
    ${CMAKE_CURRENT_SOURCE_DIR}/include/rDb.h
    ${CMAKE_CURRENT_SOURCE_DIR}/include/ulid.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/timefunctions.h
    ${CMAKE_CURRENT_SOURCE_DIR}/include/wpSQLDatabase.h
)

# Handle logger dependency
if(WPSQL_STANDALONE)
    # When building standalone, use the logger submodule
    add_subdirectory(logger)
endif()
# When built as part of parent project, logger target should already exist

# Create the wpSQL library
add_library(wpSQL STATIC ${WPSQL_SOURCES} ${WPSQL_HEADERS})

# Set include directories for the library
# PUBLIC: consumers of wpSQL will also get these include paths
target_include_directories(wpSQL
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include/sqlite3>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
        $<INSTALL_INTERFACE:include>
)

# Set compile definitions for SQLite and other features
target_compile_definitions(wpSQL PUBLIC
    SQLITE_ENABLE_FTS5
    SQLITE_ENABLE_FTS4
    PARTIO_USE_ZLIB
)

# Link against pthread if needed
find_package(Threads REQUIRED)

# Link dependencies (logger provides spdlog transitively)
target_link_libraries(wpSQL PUBLIC Threads::Threads logger ${CMAKE_DL_LIBS})
if(NOT MSVC)
    target_link_libraries(wpSQL PUBLIC m)
endif()

# Add sample subdirectory only for standalone builds
if(WPSQL_STANDALONE)
    add_subdirectory(sample)
endif()
