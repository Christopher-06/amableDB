cmake_minimum_required (VERSION 3.8)

add_library(hashing INTERFACE)
target_include_directories(hashing INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})