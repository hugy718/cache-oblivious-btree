# Minimum CMake required
cmake_minimum_required(VERSION 3.5.0)

# Project
project(cache-oblivious-b-tree)

set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
set(CMAKE_INCLUDE_CURRENT_DIR TRUE)
include_directories("${PROJECT_SOURCE_DIR}/include")

add_library(cobtree SHARED
  "${PROJECT_SOURCE_DIR}/src/block_device.cc"
  "${PROJECT_SOURCE_DIR}/include/block_device.h"
  "${PROJECT_SOURCE_DIR}/src/cache.cc"
  "${PROJECT_SOURCE_DIR}/include/cache.h"
  "${PROJECT_SOURCE_DIR}/src/cobtree.cc"
  "${PROJECT_SOURCE_DIR}/include/cobtree.h"
  "${PROJECT_SOURCE_DIR}/src/pma.cc"
  "${PROJECT_SOURCE_DIR}/include/pma.h"
  "${PROJECT_SOURCE_DIR}/src/type.cc"
  "${PROJECT_SOURCE_DIR}/include/type.h"
  "${PROJECT_SOURCE_DIR}/src/vebtree.cc"
  "${PROJECT_SOURCE_DIR}/include/vebtree.h"
)

set(COBTREE_LIB cobtree)

add_subdirectory("${PROJECT_SOURCE_DIR}/test/")
