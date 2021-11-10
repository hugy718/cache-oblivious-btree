
#include <iostream>
#include <string>
#include "cobtree.h"

using namespace cobtree;

int main(){
  // cobtree configuration set up
  uint64_t veb_fanout = 4;
  uint64_t estimated_record_count = 1024*1024;
  double pma_redundancy_factor_l1 = 1.2;
  double pma_redundancy_factor_l2 = 1.2;
  double pma_redundancy_factor_l3 = 1.2;
  PMADensityOption pma_density_l1{0.8, 0.6, 0.2, 0.1};
  PMADensityOption pma_density_l2{0.8, 0.6, 0.2, 0.1};
  PMADensityOption pma_density_l3{0.8, 0.6, 0.2, 0.1};
  const std::string uid{"cobtree"};

  // set up cache
  uint64_t cache_size = 1024*1024;
  Cache cache{cache_size};
  cache.set_block_size_for_stats(4096);

  CoBtree tree{veb_fanout, estimated_record_count, pma_redundancy_factor_l1, 
    pma_redundancy_factor_l2, pma_redundancy_factor_l3, uid, pma_density_l1, 
    pma_density_l2, pma_density_l3, &cache};

  tree.Insert(1, 10);
  tree.Insert(2, 20);
  uint64_t ret;
  if (tree.Get(1, &ret)) std::cout << ret << "\n";
  if (tree.Get(2, &ret)) std::cout << ret << "\n";
  tree.Insert(1, 30);
  if (tree.Get(1, &ret)) std::cout << ret << "\n";

  
  return 0;
}