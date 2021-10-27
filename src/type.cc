#include "type.h"

namespace cobtree {

uint64_t Node::NextPos(uint64_t key, uint64_t children_count, bool* is_leaf) {
    if (children->addr == UINT64_MAX) {
      // leaf node has no children and the first key in children buffer is set to value.
      // mark is_leaf as true
      *is_leaf = true;
      return children->key;
    }
    NodeEntry* it  = children;
    for (unsigned int i = 0; i < children_count; i++) {
      // next children has larger key or no more nodes
      if ((it->key > key) || (it->addr == UINT64_MAX)) return (--it)->addr;
      it++;
    }
    // return the last children
    return (--it)->addr;
  }

}  // namespace cobtree