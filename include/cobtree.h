#ifndef COBTREE_COBTREE_H_
#define COBTREE_COBTREE_H_

#include <string>
#include <memory>
#include "cache.h"
#include "type.h"

namespace cobtree {

class CoBtree {
 public:
  CoBtree() = default;
  ~CoBtree() = default;

  virtual uint64_t search(uint64_t key) = 0;

  std::string DebugString();

 protected:
  Cache* cache_; // refer to an abstract instance of cache.
};
}  // namespace cobtree

#endif // COBTREE_COBTREE_H_