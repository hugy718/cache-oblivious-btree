#ifndef COBTREE_PMA_H_
#define COBTREE_PMA_H_

#include <map>
#include <cassert>
#include <cmath>
#include "block_device.h"
#include "cache.h"

namespace cobtree {

struct PMASegment {
  char* content;
  uint64_t len;
};

struct UpdateContext {
  bool global_rebalance = false; // true when the whole array is reallocated
  std::map<uint64_t, uint64_t> id_to_key; // change of mapping from segment id and key. 
};

struct PMADensityOption {
  uint64_t upper_density_base_upper; // tau_d
  uint64_t upper_density_base_lower; // tau_0
  uint64_t lower_density_base_upper; // rho_0
  uint64_t lower_density_base_lower; // rho_d
};

class PMA {
 public:
  PMA(const std::string& id, int unit_size, int initial_unit_count, 
    double redundancy_factor, const PMADensityOption& option, Cache* cache)
    : id_(id), reallocate_count_(0), unit_size_(unit_size),
    option_(option), cache_(cache) {
      assert(cache);
      segment_size_ =  std::ceil(std::log10(initial_unit_count));
      segment_count_ = std::ceil(initial_unit_count 
        / std::log10(initial_unit_count));
      depth_  = std::ceil(std::log2(segment_count_));
      storage_.reset(new BlockDevice(segment_count_*segment_size_*unit_size_));
  }

  // the user will obtain the segment and perform get logic and additional rebalance (example vEBtree node rearrage).
  PMASegment Get(uint64_t segment_id) const; 

  uint64_t Add(const char* item, uint64_t segment_id, UpdateContext* ctx);

  // This when rewrite the segment will put the item at pos.
  // needed for
  uint64_t Add(const char* item, uint64_t segment_id, uint64_t pos, UpdateContext* ctx);

  inline uint64_t segment_size() const { return segment_size_; }

 protected:
  // check density threshold and perform merge or split
  // called by Add
  void Rebalance(uint64_t segment_id, UpdateContext* ctx);

  // reallocate a block device, copy contents over and update meta
  // called by Rebalance
  void Reallocate(UpdateContext* ctx);

  // helper function
  uint64_t calculate_upper_bound_density(int depth) const;
  uint64_t calculate_lower_bound_density(int depth) const;

  inline std::string GetCachekey(uint64_t segment_id) const {
    return std::string(id_+std::to_string(reallocate_count_)+std::to_string(segment_id));
  }

  // basic parameters
  const std::string id_;
  int reallocate_count_;
  int depth_; // the height of logical index binary tree = ceil(log(segment_count_)) 
  int unit_size_; // bytes per unit.
  uint64_t segment_size_; // in unit
  uint64_t segment_count_; 
  std::unique_ptr<BlockDevice> storage_; // total allocated space is segment_count_*segment_size_*unit_size_.
  Cache* cache_;

  // parameters controlling split, merge, and reallocate
  const PMADensityOption option_;
};
}  // namespace cobtree
#endif  // COBTREE_PMA_H_