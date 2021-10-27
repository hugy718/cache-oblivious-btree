#include "vebtree.h"

namespace cobtree {

uint64_t vEBTree::Get(uint64_t key) {
  bool is_leaf = false;
  auto address = root_.NextPos(key, fanout_, &is_leaf);
  uint64_t pma_segment_size = pma_.segment_size();
  auto segment_id = address/pma_segment_size;
  auto segment = pma_.Get(segment_id);
  auto segment_offset = address - segment_id * pma_segment_size;
  assert(segment.len > segment_offset + node_size_);
  Node* node = reinterpret_cast<Node*>(segment.content + segment_offset);
  while(!is_leaf) {
    address = node->NextPos(key, fanout_, &is_leaf);
    if (is_leaf) break;
    auto new_segment_id = address/pma_segment_size;
    if (new_segment_id != segment_id) {
      segment_id = address/pma_segment_size;
      segment = pma_.Get(segment_id);
    }
    segment_offset = address - segment_id * pma_segment_size;
    assert(segment.len > segment_offset + node_size_);
    node = reinterpret_cast<Node*>(segment.content + segment_offset);
  }
  return address;
}

void vEBTree::Insert(uint64_t key, uint64_t value) {

}

void vEBTree::Update(uint64_t key, uint64_t value) {

}

void vEBTree::Delete(uint64_t key, uint64_t value) {

}


}  // namespace cobtree