#ifndef COBTREE_H_
#define COBTREE_H_

#include <string>

namespace cobtree {

class CoBtree {
 public:
  CoBtree() = default;
  ~CoBtree() = default;

  std::string DebugString();
};
}  // namespace cobtree

#endif // COBTREE_H_