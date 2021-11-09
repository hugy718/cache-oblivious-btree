#ifndef COBTREE_PMA_H_
#define COBTREE_PMA_H_

#include <cassert>
#include <cmath>
#include <vector>
#include <unordered_map>
#include "block_device.h"
#include "cache.h"

namespace cobtree {

// each segment is accessed as a char array, PMA is responsible to set the last empty entry to 0, if block device does not intialize the empty space as 0.
struct PMASegment {
  char* content;
  uint64_t len;
  uint64_t num_item;
};

struct SegmentInfo {
  SegmentInfo() = delete;
  SegmentInfo(uint64_t _segment_id, uint64_t _num_count)
  : segment_id(_segment_id), num_count(_num_count) {}

  uint64_t segment_id; // the segment id (can be deduced from the latter)
  uint64_t num_count; // the address where we see the first inserted element.
};

struct PMAUpdateContext {

  inline void clear() {
    filled_empty_segment = false;
    // global_rebalance = false;
    updated_segment.clear();
  }

  // copy assignment operator
  PMAUpdateContext& operator=(const PMAUpdateContext& other){
    if (this != &other) {
      filled_empty_segment = other.filled_empty_segment;
      updated_segment = other.updated_segment;
    }
    return *this;
  }

  bool filled_empty_segment = false; // when pma slowly grows the segment is filled one by one gradually. Exposing this information helps to index update.
  // bool global_rebalance = false; // true when the whole array is reallocated
  std::vector<SegmentInfo> updated_segment; // updated segment.
};

struct PMADensityOption {
  double upper_density_base_upper; // tau_d
  double upper_density_base_lower; // tau_0
  double lower_density_base_upper; // rho_0
  double lower_density_base_lower; // rho_d
};

class PMA {
 public:
  PMA(const std::string& id, uint64_t item_size, uint64_t estimated_item_count, 
    const PMADensityOption& option, Cache* cache)
    : id_(id), item_size_(item_size), 
    segment_size_(std::ceil(std::log2(estimated_item_count))),
    segment_count_(((estimated_item_count - 1) / segment_size_ + 1 
      + 1) >> 1 << 1), // make sure even number of segment count
    height_(std::ceil(std::log2(segment_count_))), cache_(cache),
    storage_(new BlockDevice(segment_count_*segment_size_*item_size_)),
    last_non_empty_segment_(0), item_count_(segment_count_, 0),
    option_(option) {
      assert(cache_);
      assert(segment_count_ * segment_size_ > estimated_item_count);
#ifndef NDEBUG
    printf("Debug print: The PMA contains %lu segment, each size of %lu. \
      each item has size %lu", segment_count_, segment_size_, item_size_);
#endif // NDEBUG
  }

  static std::string CreatePMACacheKey(const std::string& id, uint64_t segment_id) {
    return id + std::to_string(segment_id);
  }

  // the user will obtain the segment and perform get logic and additional rebalance (example vEBtree node rearrage).
  PMASegment Get(uint64_t segment_id) const;

  // This when rewrite the segment will put the item at pos.
  // needed for
  bool Add(const char* item, uint64_t segment_id, uint64_t pos, 
    PMAUpdateContext* ctx);

  inline uint64_t segment_size() const { return segment_size_; }
  inline uint64_t segment_count() const { return segment_count_; }
  inline uint64_t last_non_empty_segment() const {
    return last_non_empty_segment_; }

 private:

  // helper function
  inline int depth(int height) const { return height_ - height; }

  inline double UpperDensityThreshold(int height) {
    return option_.upper_density_base_lower + (option_.upper_density_base_upper 
      - option_.upper_density_base_lower) * depth(height) / (height-1);  
  }

  inline double LowerDensityThreshold(int heihgt) { /*not implemented*/ return 0.0;}

  // only called by Rebalance
  void RebalanceRange(uint64_t left_id, uint64_t right_id, uint64_t item_count,
    PMAUpdateContext* ctx);

  // return false if reallocate needed. true otherwise
  bool Rebalance(uint64_t segment_id, PMAUpdateContext *ctx);

  // // reallocate a block device, copy contents over and update meta
  // // called by Rebalance
  // void Reallocate(PMAUpdateContext* ctx);

  // return the logical height we are at
  inline void expand_rebalance_range(uint64_t* left, uint64_t* right, 
    uint64_t* item_count) const {
    auto num_segment = right - left + 1; // number of segment we have, also the number we need to add to the range
    while (num_segment > 0) {
      if (right - left + 1 == segment_count_) break;
      if (*left > 0) {
        *left--;
        *item_count += item_count_[*left];
        num_segment--;
      }
      if (*right < segment_count_-1) {
        *right++;
        *item_count += item_count_[*right];
        num_segment--;
      }
    }
  }

  // basic parameters
  const std::string id_;
  // int reallocate_count_;
  uint64_t item_size_; // bytes per unit.
  uint64_t segment_size_; // in unit
  uint64_t segment_count_;
  int height_; // the height of logical index binary tree = ceil(log(segment_count_))
  // For simplicity of simulation we store the item_count_;
  Cache* cache_;
  std::unique_ptr<BlockDevice> storage_; // total allocated space is segment_count_*segment_size_*unit_size_.
  uint64_t last_non_empty_segment_;
  // in practise this information can be kept in a header in the segment or separately. requiring at most 1 more IO to retrieve.
  std::vector<uint64_t> item_count_;

  // parameters controlling split, merge, and reallocate
  const PMADensityOption option_;
};
}  // namespace cobtree
#endif  // COBTREE_PMA_H_