#include <cassert>
#include "cache.h"

namespace cobtree {

void Cache::Add(const std::string& id, char* src, uint64_t len) {
  assert(len < size_);
  if (Exist(id)) return;
  while (usage_ + len > size_) {
    const std::string& block_to_delete = fifo_list_.front();
    auto deleted_size = contents_[block_to_delete].len();
    usage_ -= deleted_size;
    block_transfer_count_ += (deleted_size - 1) / block_transfer_size_ + 1;
    contents_.erase(block_to_delete);    
    fifo_list_.pop_front();
  }

  fifo_list_.push_back(id);
  contents_[id].FillContent(src, len);
  block_transfer_count_ += (len - 1) / block_transfer_size_ + 1;
  usage_ += len;
}

}  // namespace cobtree