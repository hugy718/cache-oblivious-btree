#include "pma.h"

namespace cobtree {

PMASegment PMA::Get(uint64_t segment_id) const {
  /*not implemented*/
  return PMASegment();
}

uint64_t PMA::Add(const char* item, uint64_t segment_id, PMAUpdateContext* ctx) {
  /*not implemented*/
  return 0;
}

uint64_t PMA::Add(const char* item, uint64_t segment_id, uint64_t pos, PMAUpdateContext* ctx) {
  /*not implemented*/
  // if () {
  //   Rebalance(segment_id, ctx);
  // } else if ( ) {
  //   Reallocate(ctx);
  // }
  return 0;
}

void PMA::Rebalance(uint64_t segment_id, PMAUpdateContext* ctx) { 
  /*not implemented*/
}

void PMA::Reallocate(PMAUpdateContext* ctx) {
  /*not implemented*/
}


}  // namespace cobtree