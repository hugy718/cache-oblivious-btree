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
  inline void FillContent(const char* src, uint64_t len) {
    len_ = len;
    content_.reset(new char[len]);
    std::memcpy(content_.get(), src, len);
  }

  inline uint64_t len() const { return len_; }
  inline char* data() const { return content_.get(); }

  ~CacheBlock() = default;

 private:
  uint64_t len_;
  char* content_;
};

class Cache {
 public:
  Cache() = delete;
  Cache(uint64_t size) : size_(size), usage_(0) {}
  ~Cache() = default;

  void Add(const std::string& id, const char* src, uint64_t len);

  inline bool Exist(const std::string& id) const { 
    return contents_.find(id) != contents_.end(); 
  }
  
  char* Get(const std::string& id) { return contents_[id].data(); }

 private:
  const uint64_t size_; // M bytes
  uint64_t usage_; // bytes used
  std::map <std::string, CacheBlock> contents_;
  std::list<std::string> fifo_list_; // can be extended to other replacement policy
};

}  // namespace cobtree
#endif  // COBTREE_CACHE_H_