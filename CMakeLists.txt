cmake_minimum_required(VERSION 4.0)
project(wpSQL VERSION 1.0.0 LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Library sources
set(WPSQL_SOURCES
    src/checkSchema.cpp
    src/dbbackup.cpp
    src/lockTimeout.cpp
    src/logging.cpp
    src/rDb.cpp
    src/rDb-SQLite3.cpp
    src/timefunctions.cpp
    src/wpSQLDatabase.cpp
    src/sqlite3/sqlite3.c
)

# Library headers
set(WPSQL_HEADERS
    include/logging.hpp
    include/rDb.h
    include/timefunctions.h
    include/wpSQLDatabase.h
)

# Create the wpSQL library
add_library(wpSQL STATIC ${WPSQL_SOURCES} ${WPSQL_HEADERS})

# Set include directories for the library
target_include_directories(wpSQL
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

# Set compile definitions for SQLite and other features
target_compile_definitions(wpSQL PUBLIC 
    SQLITE_ENABLE_FTS5 
    SQLITE_ENABLE_FTS4 
    PARTIO_USE_ZLIB
)

# Find spdlog
find_package(spdlog REQUIRED)

# Link against pthread if needed
find_package(Threads REQUIRED)

# Link SQLite3 (using the embedded sqlite3.c in the library) and math library
target_link_libraries(wpSQL PUBLIC Threads::Threads spdlog::spdlog ${CMAKE_DL_LIBS} m)

# Add the sample subdirectory
add_subdirectory(sample)
