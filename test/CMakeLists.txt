add_executable(pma-test pma-test.cc)
target_link_libraries(pma-test ${COBTREE_LIB})

add_executable(simple-example simple-example.cc)
target_link_libraries(simple-example ${COBTREE_LIB})