#include "pma.h"

#include <cstring>
#include <iostream>
#include <queue>

namespace cobtree {

PMASegment PMA::Get(uint64_t segment_id) const {
  assert(segment_id < segment_count_);
  std::string cache_key{CreatePMACacheKey(id_, segment_id)};
  char* ptr = cache_->Get(cache_key);
  if (ptr == nullptr) {
    // ptr = storage_.get()->buffer_.get() + segment_id * segment_size_;
    auto read_len = storage_->Read((segment_id * segment_size_) * item_size_, 
      segment_size_ * item_size_, &ptr);
    assert(read_len == segment_size_* item_size_); // the segment should have a space already allocated in block device.
    // add it to cache.
    cache_->Add(cache_key, ptr, read_len);
  }
  return PMASegment{ptr, segment_size_ * item_size_, item_count_[segment_id]};
}

PMASegmentCopy PMA::GetCopy(uint64_t segment_id) const {
  auto segment = Get(segment_id);
  PMASegmentCopy cpy;
  cpy.segment_id = segment_id;
  cpy.len = segment.len;
  cpy.content.reset(new char[cpy.len]);
  std::memcpy(cpy.content.get(), segment.content, cpy.len);
  cpy.num_item = segment.num_item;
  return cpy;
}

bool PMA::Add(const char *item, uint64_t segment_id, uint64_t pos, PMAUpdateContext *ctx) {
  PMASegment segment = Get(segment_id);
  // by construction, when executed correctly, PMA never reaches a status where we have no free space in a segment.
  assert(pos > 0);
  // shift all item to the left of pos (pos inclusive) left by one position
  // to make space for insertion
  std::memcpy(segment.content, segment.content + item_size_, pos * item_size_);
  std::memcpy(segment.content + pos * item_size_, item, item_size_);
  item_count_[segment_id]++;

  // perform rebalance if needed.
  return Rebalance(segment_id, ctx);
}

// we want to ensure there is at least 1 item in every segment after redistribution.
// due to rounding. the redistribution can be for 27 item over 8
//  1 2 4 4 4 4 4 4

namespace {

struct RedistributionCtx {

  RedistributionCtx(uint64_t _start_segment_id, uint64_t _num_segment, 
    uint64_t item_count) : starting_segment(_start_segment_id), 
    num_segment(_num_segment) {
    assert(item_count > num_segment);
    target_item_per_segment = std::ceil((double) item_count / num_segment);
    auto remain = item_count - num_segment; // give one for each first
    first_match_target_segment = starting_segment + num_segment;
    while(remain >= target_item_per_segment - 1) {
      first_match_target_segment--;
      remain -= target_item_per_segment - 1; 
    }
    first_non_one_segment = first_match_target_segment;
    if (remain > 0) {
      non_target_non_one_value = 1 + remain;
      first_non_one_segment--;
    }
    printf("start segment: %lu, #segment: %lu, first non one: %lu,\
      first match target: %lu, target item: %lu\n", starting_segment,
      num_segment, first_non_one_segment, first_match_target_segment, target_item_per_segment);
    if (first_non_one_segment != first_match_target_segment) {
      printf("segment %lu has %lu item\n", first_non_one_segment, 
        non_target_non_one_value);
    }
  }

  uint64_t get_target_item(uint64_t segment_id) const {
    assert(segment_id >= starting_segment);
    assert(segment_id < num_segment+starting_segment);
    if (segment_id < first_non_one_segment) return 1;
    if (segment_id >= first_match_target_segment) {
      return target_item_per_segment;
    } 
    // guaranteed there is a non one non target value
    return non_target_non_one_value;
  }

  uint64_t starting_segment;
  uint64_t num_segment;
  uint64_t first_non_one_segment;
  uint64_t first_match_target_segment;
  uint64_t non_target_non_one_value;
  uint64_t target_item_per_segment;
};
}

void PMA::RebalanceRange(uint64_t left, uint64_t right, uint64_t item_count,
  PMAUpdateContext* ctx) {
  
  // calculate the target item count for each segment
  auto num_segment = right - left + 1;
  // create the redistribution context. it ensures that at least one item in a segment.
  RedistributionCtx redistribution_ctx{left, num_segment, item_count};

  // clear context and set if empty segment filled.
  ctx->clear();

  auto src_segment_id = right;
  char* src_segment = Get(right).content;
  auto src_offset = segment_size_ - 1; 
  auto src_segment_item_count = item_count_[right]; 
  auto dest_segment_id = right;
  auto dest_segment = Get(right).content;
  auto dest_offset = segment_size_ - 1;
  
  // { 
  //   if (src_segment_item_count > 0) {
  //     // printed src segment content as uint64_t.
  //     auto pitr = reinterpret_cast<uint64_t*>(src_segment + segment_size_ * item_size_ 
  //       - sizeof(uint64_t));\
  //     auto start = pitr;
  //     std::cout << "data in source component " << src_segment_id << "\n";  
  //     auto end = start - src_segment_item_count * item_size_ / sizeof(uint64_t);
  //     while (pitr != end) {
  //       std::cout << *pitr << " ";
  //       pitr--;
  //     }
  //     std::cout << "\n";
  //   }
  // }
  auto curr_item_to_copy = redistribution_ctx.get_target_item(right);
  auto num_item_left = item_count;
  // when dest is smaller than src, it copy the content and push it here. later src will read from the unmodified old state of segment.
  std::queue<PMASegmentCopy> src_cpy;
  while (num_item_left > 0) {
    auto src_segment_left = src_offset - (segment_size_ - src_segment_item_count) + 1;
    auto actual_copy_count = std::min(curr_item_to_copy,  src_segment_left);

    if (actual_copy_count > 0) {

      // #ifndef NDEBUG
      // { 
      //   // printed copied content as uint64_t.
      //   auto pitr = reinterpret_cast<uint64_t*>(src_segment + (src_offset - actual_copy_count + 1) * item_size_);\
      //   auto start = pitr;
      //   std::cout << "copied data from segment " << src_segment_id << " offset " 
      //     << src_offset << "\n";  
      //   while (pitr != start + actual_copy_count * item_size_ / sizeof(uint64_t)) {
      //     std::cout << *pitr << " ";
      //     pitr++;
      //   }
      //   std::cout << "\n";
      // }
      // #endif // NDEBUG
      std::memcpy(dest_segment + (dest_offset - actual_copy_count + 1) * item_size_,
        src_segment + (src_offset - actual_copy_count + 1) * item_size_,
        actual_copy_count * item_size_);

      // #ifndef NDEBUG
      // { 
      //   // printed copied content as uint64_t.
      //   auto pitr = reinterpret_cast<uint64_t*>(dest_segment + (dest_offset - actual_copy_count + 1) * item_size_);\
      //   auto start = pitr;
      //   std::cout  << " to segment " << dest_segment_id << " offset " << dest_offset << "\n";  
      //   while (pitr != start + actual_copy_count * item_size_ / sizeof(uint64_t)) {
      //     std::cout << *pitr << " ";
      //     pitr++;
      //   }
      //   std::cout << "\n";
      // }
      // #endif // NDEBUG
      num_item_left -= actual_copy_count;
    }
      // {
      //   auto segment = Get(1);
      //   // printed dest segment content as uint64_t.
      //   auto pitr = reinterpret_cast<uint64_t*>(segment.content + (segment_size_) * item_size_ 
      //     - sizeof(uint64_t));\
      //   auto start = pitr;
      //   std::cout << "data in dest component " << 1  << "\n";  
      //   while (pitr != start - redistribution_ctx.get_target_item(1) * item_size_ / sizeof(uint64_t)) {
      //     std::cout << *pitr << " ";
      //     pitr--;
      //   }
      //   std::cout << "\n";
      // }
    
    // adjust src offset to copy
    if ((actual_copy_count == src_segment_left)
      && (src_segment_id != left)) {
      // we have depleted the current source
      // clear our segment copy from queue, if we used it
      if (!src_cpy.empty() && src_cpy.front().segment_id == src_segment_id) {
        src_cpy.pop();
      }
      src_segment_id--;
      if (!src_cpy.empty()) {
        assert(src_segment_id == src_cpy.front().segment_id);
        src_segment = src_cpy.front().content.get();
      } else {
        src_segment = Get(src_segment_id).content;
      }
      // check if we should read from segment copy.
      src_offset = segment_size_ - 1; 
      src_segment_item_count = item_count_[src_segment_id];
      // { 
      //   if (src_segment_item_count > 0) {
      //     // printed src segment content as uint64_t.
      //     auto pitr = reinterpret_cast<uint64_t*>(src_segment + (segment_size_) * item_size_ 
      //       - sizeof(uint64_t));\
      //     auto start = pitr;
      //     std::cout << "data in source component " << src_segment_id  << "\n";  
      //     while (pitr != start - src_segment_item_count * item_size_ / sizeof(uint64_t)) {
      //       std::cout << *pitr << " ";
      //       pitr--;
      //     }
      //     std::cout << "\n";
      //   }
      // }
    } else {
      // there are still item in this source to copy
      assert(src_offset > actual_copy_count);
      src_offset -= actual_copy_count;
    }

    if ((actual_copy_count == curr_item_to_copy)
      &&(dest_segment_id != left)) {
      // we have filled the current destination
      dest_segment_id--;
      if (dest_segment_id < src_segment_id) {
        // we are modifying segments that would be sources later;
        src_cpy.push(GetCopy(dest_segment_id));
      }
      // { 
      //     // printed dest segment content as uint64_t.
      //     auto pitr = reinterpret_cast<uint64_t*>(dest_segment + (segment_size_) * item_size_ 
      //       - sizeof(uint64_t));\
      //     auto start = pitr;
      //     std::cout << "data in dest component " << dest_segment_id+1  << "\n";  
      //     while (pitr != start - redistribution_ctx.get_target_item(dest_segment_id+1) * item_size_ / sizeof(uint64_t)) {
      //       std::cout << *pitr << " ";
      //       pitr--;
      //     }
      //     std::cout << "\n";
      // }
      dest_segment = Get(dest_segment_id).content;
      dest_offset = segment_size_ - 1;
      curr_item_to_copy = redistribution_ctx.get_target_item(dest_segment_id);
    } else {
      // we still need to copy more into this dest
      assert(dest_offset > actual_copy_count);
      dest_offset -= actual_copy_count;
      curr_item_to_copy -= actual_copy_count;
    }
  }

  // prepare ctx and update item_count_
  ctx->num_filled_empty_segment = 0;
  for (uint64_t i = left; i < right+1; i++) {
    if (item_count_[i] == 0) ctx->num_filled_empty_segment++;
    auto final_item_count = redistribution_ctx.get_target_item(i); 
    item_count_[i] = final_item_count;
    ctx->updated_segment.emplace_back(i, final_item_count);  
    // {
    //   auto segment = Get(i);
    //   // printed dest segment content as uint64_t.
    //   auto pitr = reinterpret_cast<uint64_t*>(segment.content + (segment_size_) * item_size_ 
    //     - sizeof(uint64_t));\
    //   auto start = pitr;
    //   std::cout << "data in dest component " << i  << "\n";  
    //   while (pitr != start - item_count_[i] * item_size_ / sizeof(uint64_t)) {
    //     std::cout << *pitr << " ";
    //     pitr--;
    //   }
    //   std::cout << "\n";
    // }
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
  while ((item_count >= UpperDensityThreshold(rebalancing_height) 
    * segment_size_ * (right - left + 1))
    && (rebalancing_height <= height_)) {
    // fast path: when rebalancing within the current PMA size is not enough we return;
    // continue expand the rebalance range.
    expand_rebalance_range(&left, &right, &item_count);
    rebalancing_height ++;
  }; 
  if ((rebalancing_height > height_) 
    && (item_count >= UpperDensityThreshold(rebalancing_height) 
      * segment_size_ * (right - left + 1))) {
    printf("need reallocate!\n");
    return false; // current reallocate is not implemented
  } 

  // update the non empty segment count if needed
  last_non_empty_segment_ = std::max(last_non_empty_segment_, right);
  // perform rebalanc within the selected range.
  RebalanceRange(left, right, item_count, ctx);  
  return true;
}

}  // namespace cobtree