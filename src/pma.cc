#include "pma.h"

#include <cstring>

namespace cobtree {

  PMASegment PMA::Get(uint64_t segment_id) const {
    std::string cache_key{CreatePMACacheKey(id_, segment_id)};
    char* ptr = cache_->Get(cache_key);
    if (ptr == nullptr) {
      // ptr = storage_.get()->buffer_.get() + segment_id * segment_size_;
      auto read_len = storage_->Read((segment_id * segment_size_), segment_size_, ptr);
      assert(read_len == segment_size_); // the segment should have a space already allocated in block device.
      // add it to cache.
      cache_->Add(cache_key, ptr, read_len);
    }
    return PMASegment{ptr, segment_size_};
  }

  void PMA::Add(const char *item, uint64_t segment_id, uint64_t pos, PMAUpdateContext *ctx) {
    PMASegment segment = Get(segment_id);
    // by construction, when executed correctly, PMA never reaches a status where we have no free space in a segment.
    assert(pos > 0);
    // shift all item to the left of pos (pos inclusive) left by one position
    // to make space for insertion
    std::memcpy(segment.content, segment.content + item_size_, pos * item_size_);
    std::memcpy(segment.content + pos * item_size_, item, item_size_);
    item_count_[segment_id]++;
    if (pos = segment_size_ - 1) {
      ctx->updated_segment.emplace_back(segment_id);
    }

    // perform rebalance if needed.
    Rebalance(segment_id, ctx);
  }

  void PMA::RebalanceRange(uint64_t left, uint64_t right, uint64_t item_count,
    PMAUpdateContext* ctx) {
    
    // calculate the target item count for each segment
    auto num_segment = right - left + 1;
    uint64_t target_item_count = std::ceil((double) item_count / num_segment);


    // clear context and set if empty segment filled.
    ctx->clear();
    ctx->filled_empty_segment = (item_count_[right] == 0);
    // the left most segment might not have target_item_count;
    ctx->updated_segment.emplace_back(left, item_count - (right-left) * target_item_count);
    for (uint64_t i = left; i < right; i++) {
      ctx->updated_segment.emplace_back(i+1, target_item_count);  
    }

    auto src_segment_id = right;
    char* src_segment = Get(right).content;
    auto src_offset = segment_size_ - 1; 
    auto src_segment_item_count = item_count_[right]; 
    auto dest_segment_id = right;
    auto dest_segment = Get(right).content;
    auto dest_offset = segment_size_ - 1;
    auto curr_item_to_copy = target_item_count;
    while (num_segment > 0) {
      auto src_segment_left = src_offset - (segment_size_ - src_segment_item_count) + 1;
      auto actual_copy_count = std::min(curr_item_to_copy,  src_segment_left);
      
      std::memcpy(dest_segment + (dest_offset - actual_copy_count + 1) * item_size_,
        src_segment + (src_offset - actual_copy_count + 1) * item_size_,
        actual_copy_count * item_size_);
      
      // adjust src offset to copy
      if (actual_copy_count == src_segment_left) {
        // we have depleted the current source
        src_segment_id--;
        src_segment = Get(src_segment_id).content;
        src_offset = segment_size_ - 1; 
      } else {
        // there are still item in this source to copy
        assert(src_offset > actual_copy_count);
        src_offset -= actual_copy_count;
      }

      if (actual_copy_count == curr_item_to_copy) {
        // we have filled the current destination
        dest_segment--;
        dest_segment = Get(dest_segment_id).content;
        dest_offset = segment_size_ - 1;
        num_segment--;
        // the last segment might contain less than target_item_count
        curr_item_to_copy = (num_segment > 1) ? target_item_count
          : item_count - target_item_count * (right-left);
      } else {
        // we still need to copy more into this dest
        assert(dest_offset > actual_copy_count);
        dest_offset -= actual_copy_count;
      }
    }
  }

  bool PMA::Rebalance(uint64_t segment_id, PMAUpdateContext *ctx) {
    // fast path that the current element not exceeding density requirement
    if (item_count_[segment_id] < UpperDensityThreshold(1) * segment_size_) return true;

    // check if adding its neighbor is enough
    uint64_t left = segment_id;
    uint64_t right = segment_id;
    uint64_t item_count = item_count_[segment_id];
    // by construction we have even number of segments
    if (segment_count_ | 1ULL) {
      item_count += item_count_[++right];
    } else {
      item_count += item_count_[--left];
    }
    ctx->clear();
    auto rebalancing_height = 2;
    while (item_count >= UpperDensityThreshold(rebalancing_height) * segment_size_) {
      // fast path: when rebalancing within the current PMA size is not enough we return;
      if (rebalancing_height = height_) return false; // current reallocate is not implemented
      // continue expand the rebalance range.
      expand_rebalance_range(&left, &right, &item_count);
      rebalancing_height ++;
    }; 
    // perform rebalanc within the selected range.
    RebalanceRange(left, right, item_count, ctx);  
    return true;
  }

}  // namespace cobtree