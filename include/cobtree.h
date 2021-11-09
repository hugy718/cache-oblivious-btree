#ifndef COBTREE_COBTREE_H_
#define COBTREE_COBTREE_H_

#include <string>
#include <memory>
#include <vector>
#include "cache.h"
#include "type.h"
#include "vebtree.h"
#include "pma.h"

namespace cobtree {

class CoBtree {
 public:
  CoBtree() = default;
  ~CoBtree() = default;

  // return if the value is found. if found, value store in value.
  bool Get(uint64_t key, uint64_t* value);

  // return false if insertion failed due to any level pma full.
  bool Insert(uint64_t key, uint64_t value);

 private:
  /**
   * @brief update the second level down pointer and separator keys. 
   *  (potentially add new item in second level if new segments are 
   *  generated in the bottom level)
   * 
   * @param l2_segment_id the l2 segment containing the item points to 
   *  the l3 segment where insertion happened
   * @param l3_insert_segment_id the l3 segment where insertion happened
   * @param insert_in_segment_idx the idx of l2 item that currently points 
   *  l3_insert_segment_id
   * @param l3_update_ctx the returned update context after l3 insertion
   * @param l3_update_ctx return update context of l2 pma
   * @return bool l2 pma insertion failed (no space)
   */
  bool L2Update(uint64_t l2_segment_id, 
    uint64_t l3_insert_segment_id, uint64_t l2_insert_in_segment_idx, 
    const PMAUpdateContext& l3_update_ctx, PMAUpdateContext* l2_update_ctx);

  // return false if l1 leaf insertion failed.
  bool L1Update(uint64_t l1_leaf_address, uint64_t l2_insert_segment_id,
    const PMAUpdateContext& l2_update_ctx);
  
  Cache* cache_; // refer to an abstract instance of cache.

  vEBTree tree_;
  PMA pma_index_;
  PMA pma_data_;
  // we do not have up pointers. as we insert, we store the address of item in the upper level that should be updated.
};
}  // namespace cobtree

#endif // COBTREE_COBTREE_H_