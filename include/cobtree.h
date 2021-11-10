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
  CoBtree() = delete;

  CoBtree(uint64_t veb_fanout, uint64_t estimated_record_count,
    double pma_redundancy_factor_l1, double pma_redundancy_factor_l2,
    double pma_redundancy_factor_l3, const std::string& uid, const PMADensityOption& pma_density_l1, const PMADensityOption& pma_density_l2,
    const PMADensityOption& pma_density_l3, Cache* cache) 
    : uid_prefix_(uid), uid_seqeunce_number_(0), cache_(cache),
      record_count_l3(estimated_record_count * pma_redundancy_factor_l3),
      item_count_l2(std::ceil(record_count_l3 / std::log2(record_count_l3))
        * pma_redundancy_factor_l2),
      leaf_count_l1(std::ceil(item_count_l2 / std::log2(item_count_l2))),
      tree_(veb_fanout, leaf_count_l1, pma_redundancy_factor_l1, 
        CreateUid(), pma_density_l1, cache_),
      pma_index_(CreateUid(), sizeof(L2Node), item_count_l2, 
        pma_density_l2, cache_),
      pma_data_(CreateUid(), sizeof(L3Node), record_count_l3,
        pma_density_l3, cache_) {
    // add some dummy node to intialize the structure
    L3Node record{0,0};
    PMAUpdateContext ctx;
    // add a dummy record
    pma_data_.Add(reinterpret_cast<const char*>(&record), 0, 
      pma_data_.segment_size()-1, &ctx);
    // add the first record in level 2 with smallest key 
    // and pointing to segment 0 in l3
    L2Node item{0,0};
    pma_index_.Add(reinterpret_cast<const char*>(&item), 0,
      pma_index_.segment_size()-1, &ctx);

    // the veb tree constructor will automatically create root node and a dummy leaf node. here we set the dummy leaf points to the segment 0 in l2
    uint64_t dummy_leaf_address;
    tree_.Get(0, &dummy_leaf_address);
    auto dummy_leaf = tree_.GetNode(dummy_leaf_address);
    vEBTree::get_children(dummy_leaf)->key = 0;
  }

  ~CoBtree() = default;

  // return if the value is found. if found, value store in value.
  bool Get(uint64_t key, uint64_t* value);

  // return false if insertion failed due to any level pma full.
  bool Insert(uint64_t key, uint64_t value);

  std::string CreateUid() {
    return uid_prefix_ + std::to_string(uid_seqeunce_number_++);
  }

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
  
  const std::string& uid_prefix_;
  uint64_t uid_seqeunce_number_;
  Cache* cache_; // refer to an abstract instance of cache.

  // some meta 
  uint64_t record_count_l3;
  uint64_t item_count_l2;
  uint64_t leaf_count_l1;

  vEBTree tree_;
  PMA pma_index_;
  PMA pma_data_;
  // we do not have up pointers. as we insert, we store the address of item in the upper level that should be updated.
};
}  // namespace cobtree

#endif // COBTREE_COBTREE_H_