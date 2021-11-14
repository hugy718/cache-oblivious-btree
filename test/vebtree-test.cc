#include "vebtree.h"

#include <iostream>
#include <string>
#include <vector>

#define FANOUT 4

using namespace cobtree;

int main() {
  // cobtree configuration set up
  uint64_t veb_fanout = 4;
  uint64_t estimated_record_count = 1024;
  double pma_redundancy_factor = 1.2;
  PMADensityOption pma_density{0.8, 0.6, 0.2, 0.1};
  const std::string uid{"vebtree"};

  // set up cache
  uint64_t cache_size = 40*1024;
  Cache cache{cache_size};
  cache.set_block_size_for_stats(4096);

  // vebtree
  vEBTree tree{veb_fanout, estimated_record_count, pma_redundancy_factor,
    uid, pma_density, &cache};

    std::cout << "--------------insertion-----------------\n";
  for (uint64_t i = 1; i < 20; i++) {
    std::cout << "insert: " << i << " \n";
    auto success = tree.Insert(i, i);

    if (!success) { 
      printf("full!\n"); 
      break; 
    }
    std::cout << "---printing tree as pma--\n";
    tree.DebugPrintAsPMA();
    std::cout << "---printing tree in DFS---\n";
    tree.DebugPrintDFS();
  }

  std::cout << "--------------Get-----------------\n";
  for (uint64_t i = 1; i < 20; i++) {
    std::cout << "get: " << i << " \n";
    uint64_t pma_address;
    auto value = tree.Get(i, &pma_address);
    assert(value != UINT64_MAX);
    std::cout << value << "\n";
  }
  return 0;
}