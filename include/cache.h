#ifndef COBTREE_CACHE_H_
#define COBTREE_CACHE_H_


#include <cstdint>
#include <cstring>
#include <list>
#include <map>
#include <memory>
#include <string>

namespace cobtree {

class CacheBlock {
 public:
  CacheBlock() : len_(0), content_(nullptr) {}
  inline void FillContent(char* src, uint64_t len) {
    len_ = len;
    content_ = src;
  }

  inline uint64_t len() const { return len_; }
  inline char* data() const { return content_; }

  ~CacheBlock() = default;

 private:
  uint64_t len_;
  char* content_;
};

class Cache {
 public:
  Cache() = delete;
  Cache(uint64_t size) : size_(size), usage_(0), 
    block_transfer_count_(0) {}
  ~Cache() = default;

  void Add(const std::string& id, char* src, uint64_t len);

  inline bool Exist(const std::string& id) const { 
    return contents_.find(id) != contents_.end(); 
  }
  
  char* Get(const std::string& id) { return contents_[id].data(); }

  // inform cache the block size to count number of transfer
  // the content added to cache can be multiple block size
  inline void set_block_size_for_stats(uint64_t block_size) {
    block_transfer_size_=block_size;
  } 

  // output the counted block transfer
  inline uint64_t recorded_block_transfer() const { return block_transfer_count_; }

  // reset the counted block transfer to 0
  inline void reset_block_transfer_stats() { block_transfer_count_ = 0; }

 private:
  const uint64_t size_; // M bytes
  uint64_t usage_; // bytes used
  std::map <std::string, CacheBlock> contents_;
  std::list<std::string> fifo_list_; // can be extended to other replacement policy
  uint64_t block_transfer_size_; // block size for us to count block transfer
  uint64_t block_transfer_count_; // +1 when a block sized content added to/evicted from cache
};

}  // namespace cobtree
#endif  // COBTREE_CACHE_H_