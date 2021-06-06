cmake_minimum_required (VERSION 3.8)

add_library(hnswlib INTERFACE)
target_include_directories(hnswlib INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})