# Minimum CMake required
cmake_minimum_required(VERSION 3.5.0)

# Project
project(cache-oblivious-b-tree)

set(CMAKE_INCLUDE_CURRENT_DIR TRUE)
include_directories("${PROJECT_SOURCE_DIR}/include")

add_library(cobtree SHARED
  "${PROJECT_SOURCE_DIR}/src/cobtree.cc"
  "${PROJECT_SOURCE_DIR}/include/cobtree.h"
)

set(COBTREE_LIB cobtree)

add_subdirectory("${PROJECT_SOURCE_DIR}/test/")
