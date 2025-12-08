cmake_minimum_required(VERSION 4.0)

# Sample executable sources
set(SAMPLE_SOURCES
    main.cpp
    member_db.cpp
    member_db_schema.cpp
)

# Find spdlog
find_package(spdlog REQUIRED)

# Create the sample executable
add_executable(sample ${SAMPLE_SOURCES})

# Set include directories to follow wpSQL project structure
target_include_directories(sample PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

# Link the wpSQL library to the sample executable
target_link_libraries(sample PRIVATE wpSQL spdlog::spdlog)
