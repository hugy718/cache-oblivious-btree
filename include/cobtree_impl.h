#ifndef COBTREE_COBTREEIMPL_H_
#define COBTREE_COBTREEIMPL_H_

#include "cobtree.h"
#include "pma.h"
#include "vebtree.h"

namespace cobtree{

class CoBtreeImpl : public CoBtree{

 private:
  vEBTree tree_;
  PMA pma_index_;
  PMA pma_data_;
};

}
#endif  // COBTREE_COBTREEIMPL_H_