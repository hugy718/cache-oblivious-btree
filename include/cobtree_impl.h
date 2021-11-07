#ifndef COBTREE_COBTREEIMPL_H_
#define COBTREE_COBTREEIMPL_H_

#include "cobtree.h"
#include "pma.h"
#include "vebtree.h"

namespace cobtree{

class CoBtreeImpl : public CoBtree{

 private:
  vEBTree tree_;
  PMA pma_index_;
  PMA pma_data_;
  // the up pointer from second level pma can be stored at the leading space in a segment
  // or we can store it elsewhere and retrieve it with O(1) cost (reading of such information of adjacent segments can amortize cost)
  // here we store it in memory for simplicity and do not account for the cost of retrieving such information in simulation (in analysis of the paper, this is not from the dominant term)
  std::vector<uint64_t> segment_to_leaf_address;
};

}
#endif  // COBTREE_COBTREEIMPL_H_