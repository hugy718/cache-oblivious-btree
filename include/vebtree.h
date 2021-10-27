#ifndef COBTREE_VEBTREE_H_
#define COBTREE_VEBTREE_H_

#include "cobtree.h"
#include "pma.h"

namespace cobtree {

class vEBTree : public CoBtree {
 public:

  uint64_t Get(uint64_t key);

  void Insert(uint64_t key, uint64_t value);
 
  void Update(uint64_t key, uint64_t value);
 
  void Delete(uint64_t key, uint64_t value);
  
 private:

  uint64_t depth_;
  uint64_t fanout_; // d
  // store the parent address and (key and address) of at most 4d children
  uint64_t node_size_; // 4d * (address size + key size) + address size 
  Node root_;
  bool use_pma;
  PMA pma_;
};

}  // namespace cobtree
#endif  // COBTREE_VEBTREE_H_