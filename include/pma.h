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
};

struct SegmentInfo {
  SegmentInfo() = delete;
  SegmentInfo(uint64_t _segment_id, uint64_t _num_count)
  : original_key(0), segment_key(0), segment_id(_segment_id), num_count(_num_count) {}
  SegmentInfo(uint64_t _original_key, uint64_t _segment_key, uint64_t _segment_id,
    uint64_t _num_count) : original_key(_original_key), segment_key(_segment_key),
    segment_id(_segment_id), num_count(_num_count) {}

  uint64_t original_key; // the original smallest key in this pma. (facilitate updates on pma index.)
  uint64_t segment_key; // the new smallest key .
  uint64_t segment_id; // the segment id (can be deduced from the latter)
  uint64_t num_count; // the address where we see the first inserted element.
  uint64_t height;
};

struct PMAUpdateContext {

  inline void clear() {
    filled_empty_segment = false;
    global_rebalance = false;
    updated_segment.clear();
  }

  bool filled_empty_segment = false; // when pma slowly grows the segment is filled one by one gradually. Exposing this information helps to index update.
  bool global_rebalance = false; // true when the whole array is reallocated
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
  PMA(const std::string& id, int unit_size, const PMADensityOption& option, Cache* cache)
    : id_(id), reallocate_count_(0), unit_size_(unit_size),
    last_non_empty_segment_(0), cache_(cache),
    option_(option) {
      assert(cache);
      segment_size_ = 1;
      segment_count_ = 1;
      height_ = 1;
      storage_.reset(new BlockDevice(segment_count_*segment_size_*unit_size_));
      segment_infos_ = std::unordered_map<uint64_t, std::shared_ptr<SegmentInfo>>();
      segment_infos_.insert(std::make_pair<uint64_t, std::shared_ptr<SegmentInfo>>(0, std::shared_ptr<SegmentInfo>(new SegmentInfo(0, 0))));
  }

  // the user will obtain the segment and perform get logic and additional rebalance (example vEBtree node rearrage).
  PMASegment Get(uint64_t segment_id) const;

  uint64_t Add(const char* item, uint64_t segment_id, PMAUpdateContext* ctx);

  // This when rewrite the segment will put the item at pos.
  // needed for
  uint64_t Add(const char* item, uint64_t segment_id, uint64_t pos, PMAUpdateContext* ctx);

  inline uint64_t segment_size() const { return segment_size_; }

 protected:
  // check density threshold and perform merge or split
  // called by Add
  void Rebalance(uint64_t segment_id, PMAUpdateContext* ctx);

  // reallocate a block device, copy contents over and update meta
  // called by Rebalance
  void Reallocate(PMAUpdateContext* ctx);

  // helper function
  uint64_t calculate_upper_bound_density(int depth) const;
  uint64_t calculate_lower_bound_density(int depth) const;

  inline std::string GetCachekey(uint64_t segment_id) const {
    return std::string(id_+std::to_string(reallocate_count_)+std::to_string(segment_id));
  }

  // basic parameters
  const std::string id_;
  int reallocate_count_;
  int height_; // the height of logical index binary tree = ceil(log(segment_count_))
  int unit_size_; // bytes per unit.
  uint64_t segment_size_; // in unit
  uint64_t segment_count_;
  uint64_t last_non_empty_segment_;
  std::unique_ptr<BlockDevice> storage_; // total allocated space is segment_count_*segment_size_*unit_size_.
  Cache* cache_;
  double redundancy_factor_;
  std::unordered_map<uint64_t, std::shared_ptr<SegmentInfo>> segment_infos_;

  // parameters controlling split, merge, and reallocate
  const PMADensityOption option_;

  double UpperDensityThreshold(int depth);

    double LowerDensityThreshold(int depth);

  std::pair<double, double> GetTargentDensity(int depth);

  bool MustRebalance(uint64_t segment_id, std::shared_ptr<SegmentInfo> info);

  void expand(BlockDevice *newDevice, PMAUpdateContext *ctx);

  void Rebalance(uint64_t segment_id, std::shared_ptr<SegmentInfo> info, PMAUpdateContext *ctx);
};
}  // namespace cobtree
#endif  // COBTREE_PMA_H_