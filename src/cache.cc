#include <cassert>
#include "cache.h"

namespace cobtree {

void Cache::Add(const std::string& id, const char* src, uint64_t len) {
  assert(len < size_);
  if (Exist(id)) return;
  while (usage_ + len > size_) {
    const std::string& block_to_delete = fifo_list_.front();
    usage_ -= contents_[block_to_delete].len();
    contents_.erase(block_to_delete);    
    fifo_list_.pop_front();
  }

  fifo_list_.push_back(id);
  contents_[id].FillContent(src, len);
  usage_ += len;
}

}  // namespace cobtree