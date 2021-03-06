#ifndef COBTREE_TYPE_H_
#define COBTREE_TYPE_H_

#include <cstdint>
#include <limits>
#include <string>

namespace cobtree {

struct NodeEntry{
  bool empty() const { return (key == UINT64_MAX) && (addr == UINT64_MAX); }
  uint64_t key = UINT64_MAX;
  uint64_t addr = UINT64_MAX; // children are stated as the unit idx of PMA.
};

struct Node {

  // to be handled by vEBTree.
  // // compare against the keys then find the output the correct address
  // uint64_t NextPos(uint64_t key, uint64_t children_count, bool* is_leaf);

  uint64_t parent_addr = UINT64_MAX;
  uint64_t height = UINT64_MAX; // store tree height makes us decideing whether it is a leaf in recursive tree context easier.
  // after this struct a node has 4d addresses preallocated to hold children space. for leaf node, the first key is the value (in cobtree the index pma segment id).
  // NodeEntry* children; 
};

struct L2Node {
  uint64_t key;
  uint64_t l3_segment_id;
};

struct L3Node {
  uint64_t key;
  uint64_t value;
};


// struct Data {
//   uint64_t cost;
//   std::string value;
// };

} // namespace cobtree
#endif  // COBTREE_TYPE_H_