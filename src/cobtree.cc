#include "cobtree.h"

#include <cassert>

namespace cobtree {

namespace {
struct L2Node {
  uint64_t key;
  uint64_t l3_segment_id;
};

struct L3Node {
  uint64_t key;
  uint64_t value;
};

struct L2GetReturn {
  uint64_t pos;
  uint64_t l3_segment_id;
};

L2GetReturn GetL2Item(uint64_t key, const PMASegment& l2_segment) {
  // by construction we should have l2 segment size being 
  // a multiple of L2Node size.
  auto item_size = sizeof(L2Node);
  assert((l2_segment.len % item_size) == 0); 
  auto num_element = l2_segment.num_item;
  assert(num_element < l2_segment.len / item_size);
  L2Node* item = reinterpret_cast<L2Node*>(l2_segment.content + l2_segment.len - item_size);

  auto last_id = item->l3_segment_id;
  auto pos = (l2_segment.len / item_size) - 1;
  while (num_element > 0 && item->key < key) {
    last_id = item->l3_segment_id;
    item -= item_size;
    pos--;
  }
  return {last_id, pos+1};
}

uint64_t GetRecordLocation(uint64_t key, const PMASegment& segment,
  bool* key_equal = nullptr) {
  // by construction we should have l3 segment size being 
  // a multiple of record size.
  auto item_size = sizeof(L3Node);
  assert((segment.len % item_size) == 0); 
  auto num_element = segment.num_item;
  assert(num_element < segment.len / item_size);
  L3Node* item = reinterpret_cast<L3Node*>(segment.content + segment.len - item_size);

  auto pos = (segment.len / item_size) - 1;
  while (num_element > 0 && item->key < key) {
    item -= item_size;
    num_element--;
    pos--;
  }
  if (key_equal) *key_equal = (item->key == key);
  return pos;
}

void UpdateRecord(uint64_t key, uint64_t value, 
  uint64_t record_idx, PMASegment* segment) {
  L3Node* item = reinterpret_cast<L3Node*>(segment->content 
    + record_idx * sizeof(L3Node));
  assert(item->key == key);
  item->value = value;  
}

typedef std::vector<SegmentInfo> SegmentInfos; 
SegmentInfos MergeSegmentUpdateInfo(const SegmentInfos& old, 
  const SegmentInfos& incoming){
  // setup the iterator
  auto old_it = old.begin();
  auto new_it = incoming.begin();
  // setup the return
  SegmentInfos merged_updated_segment; // updated segment.
  
  // first add the non-overlapping region in front
  while((old_it != old.end()) && (old_it->segment_id < new_it->segment_id)) {
    merged_updated_segment.emplace_back(old_it->segment_id, old_it->num_count);
    old_it++;
  }
  while((new_it != incoming.end()) 
    && (new_it->segment_id < old_it->segment_id)) {
    merged_updated_segment.emplace_back(new_it->segment_id, new_it->num_count);
    new_it++;
  }

  // overlapping region
  while ((old_it != old.end()) && (new_it != incoming.end())) {
    merged_updated_segment.emplace_back(new_it->segment_id, new_it->num_count);
    old_it++;
    new_it++;
  }

  // finish by adding the non-overlapping region at the end 
  while(old_it != old.end()) {
    merged_updated_segment.emplace_back(old_it->segment_id, old_it->num_count);
    old_it++;
  }
  while(new_it != incoming.end()) {
    merged_updated_segment.emplace_back(new_it->segment_id, new_it->num_count);
    new_it++;
  }
  return merged_updated_segment;
}

}  // anonymous namespace

bool CoBtree::L2Update(uint64_t l2_segment_id,
  uint64_t l3_insert_segment_id, uint64_t insert_in_segment_idx,
  const PMAUpdateContext& l3_update_ctx, PMAUpdateContext* l2_update_ctx){

  auto& l3_updated_segments = l3_update_ctx.updated_segment;
  auto insert_segment_it = l3_updated_segments.begin();
  while ((insert_segment_it != l3_updated_segments.end())
    && (l3_insert_segment_id < insert_segment_it->segment_id)){
    insert_segment_it++;
  }
  // must be in the update context by construction
  assert(insert_segment_it->segment_id == l3_insert_segment_id);

  if (insert_segment_it != l3_updated_segments.begin()) {
    // scan backward to update them 
    // (excluding the segment that insertion happens)
    auto l3_segment_it = insert_segment_it;
    auto curr_l2_segment_id = l2_segment_id; 
    auto curr_l2_segment = pma_index_.Get(curr_l2_segment_id); 
    auto l2_item = reinterpret_cast<L2Node*>(
      curr_l2_segment.content + insert_in_segment_idx * sizeof(L2Node)
    ); 
    auto curr_l2_segment_last_item = reinterpret_cast<L2Node*>(
        curr_l2_segment.content + (curr_l2_segment.len / sizeof(L2Node))
      ) - curr_l2_segment.num_item;
    do {
      // move to the previous item in the current l2 segment
      l2_item--;
      // move to the previous l3 segment
      l3_segment_it--;
      // get the smallest key in the current l3 segment
      auto curr_l3_segment = pma_data_.Get(l3_segment_it->segment_id);
      auto curr_l3_first_item = reinterpret_cast<L3Node*>(
        curr_l3_segment.content + curr_l3_segment.len - sizeof(L3Node)
      );
      // update key 
      l2_item->key = curr_l3_first_item->key;
      // check if we have reached the first non-empty item 
      // in the current l2 segment.
      if (l2_item == curr_l2_segment_last_item) {
        // move to the next segment
        assert(curr_l2_segment_id > 0);
        curr_l2_segment_id--;
        curr_l2_segment = pma_index_.Get(curr_l2_segment_id);
        l2_item = reinterpret_cast<L2Node*>(curr_l2_segment.content 
          - sizeof(L2Node)); 
        curr_l2_segment_last_item = reinterpret_cast<L2Node*>(
            curr_l2_segment.content + (curr_l2_segment.len / sizeof(L2Node))
          ) - curr_l2_segment.num_item;
      }
    } while (l3_segment_it != l3_updated_segments.begin());
  }
  // update l2 item pointing to the segment where insertion happen and afterwards
  auto curr_l2_segment_id = l2_segment_id; 
  auto curr_l2_segment = pma_index_.Get(curr_l2_segment_id); 
  auto l2_item = reinterpret_cast<L2Node*>(
    curr_l2_segment.content + insert_in_segment_idx * sizeof(L2Node)
  ); 
  while (insert_segment_it != l3_updated_segments.end()) {
    // get the smallest key in the current l3 segment
    auto curr_l3_segment = pma_data_.Get(insert_segment_it->segment_id);
    auto curr_l3_first_item = reinterpret_cast<L3Node*>(
      curr_l3_segment.content + curr_l3_segment.len - sizeof(L3Node)
    );
    // update key 
    l2_item->key = curr_l3_first_item->key;
    // move to the next l3 segment
    insert_segment_it++;
    l2_item++;
    // check if we have reached the end of current segment
    if (reinterpret_cast<char*>(l2_item) 
      == curr_l2_segment.content+curr_l2_segment.len) {
      curr_l2_segment_id++;
      assert(curr_l2_segment_id < pma_index_.segment_count());
      curr_l2_segment = pma_index_.Get(curr_l2_segment_id);
      if (curr_l2_segment.num_item == 0) break;
      l2_item = reinterpret_cast<L2Node*>(curr_l2_segment.content 
        + curr_l2_segment.len) - curr_l2_segment.num_item;
    }
  }
  
  // possibly we have new items to be added
  PMAUpdateContext aggregate_ctx;
  while (insert_segment_it != l3_updated_segments.end()) {
    auto curr_l3_segment = pma_data_.Get(insert_segment_it->segment_id);
    auto curr_l3_first_item = reinterpret_cast<L3Node*>(
      curr_l3_segment.content + curr_l3_segment.len - sizeof(L3Node)
    );
    L2Node new_item{curr_l3_first_item->key, insert_segment_it->segment_id};
    
    PMAUpdateContext ctx;
    auto success = pma_index_.Add(reinterpret_cast<const char*>(&new_item),
      pma_index_.last_non_empty_segment(), pma_index_.segment_count() -1, &ctx);
    if (!success) return false; 
    if(aggregate_ctx.updated_segment.empty()) aggregate_ctx = ctx; // fast path
    if(ctx.updated_segment.empty()) continue; // fast path
    // merge the new context into the aggregate
    aggregate_ctx.filled_empty_segment = ctx.filled_empty_segment;
    aggregate_ctx.updated_segment = MergeSegmentUpdateInfo(
      aggregate_ctx.updated_segment, ctx.updated_segment);
  }

  // scan forward to update them
  *l2_update_ctx = aggregate_ctx;
  return true;
}

bool CoBtree::L1Update(uint64_t l1_leaf_address, uint64_t l2_insert_segment_id,
  const PMAUpdateContext& l2_update_ctx) {
  auto& l2_updated_segments = l2_update_ctx.updated_segment;
  auto insert_segment_it = l2_updated_segments.begin();
  while ((insert_segment_it != l2_updated_segments.end())
    && (l2_insert_segment_id < insert_segment_it->segment_id)){
    insert_segment_it++;
  }
  // must be in the update context by construction
  assert(insert_segment_it->segment_id == l2_insert_segment_id);

  if (insert_segment_it != l2_updated_segments.begin()) {
    // scan backward to update them 
    // (excluding the segment that insertion happens)
    auto l2_segment_it = insert_segment_it;
    vEBTreeBackwardIterator leaf_it(&tree_, l1_leaf_address);
    do {
      // move to the previous item in the current l2 segment
      leaf_it.Prev();
      assert(leaf_it.valid());
      // move to the previous l3 segment
      l2_segment_it--;
      // get the smallest key in the current l3 segment
      auto curr_l2_segment = pma_index_.Get(l2_segment_it->segment_id);
      auto curr_l2_first_item = reinterpret_cast<L2Node*>(
        curr_l2_segment.content + curr_l2_segment.len - sizeof(L2Node)
      );
      // update key 
      vEBTree::get_children(leaf_it.node())->key = curr_l2_first_item->key;
    } while (l2_segment_it != l2_updated_segments.begin());
  }

  // update l1 leaf pointing to the segment where insertion happen and afterwards
  vEBTreeForwardIterator leaf_it(&tree_, l1_leaf_address);
  while (insert_segment_it != l2_updated_segments.end()) {
    // get the smallest key in the current l3 segment
    auto curr_l2_segment = pma_data_.Get(insert_segment_it->segment_id);
    auto curr_l2_first_item = reinterpret_cast<L2Node*>(
      curr_l2_segment.content + curr_l2_segment.len - sizeof(L2Node)
    );
    // update key 
    vEBTree::get_children(leaf_it.node())->key = curr_l2_first_item->key;
    // move to the next l3 segment
    insert_segment_it++;
    leaf_it.Next();

    // check if we have reached the last leaf
    if (!leaf_it.valid()) break;    
  }

  // possibly we have new items to be added
  uint64_t tree_node_size = sizeof(Node) + sizeof(NodeEntry) * tree_.fanout();
  bool l1_insert_success = false;
  while (insert_segment_it != l2_updated_segments.end()) {
    auto curr_l2_segment = pma_data_.Get(insert_segment_it->segment_id);
    auto curr_l2_first_item = reinterpret_cast<L2Node*>(
      curr_l2_segment.content + curr_l2_segment.len - sizeof(L2Node)
    );
    L2Node curr_l2_first_item_copy{curr_l2_first_item->key, 
      curr_l2_first_item->l3_segment_id};

    // add the leaf node to van Emde Boas Tree
    PMAUpdateContext ctx;
    l1_insert_success = tree_.Insert(curr_l2_first_item_copy.key, 
      insert_segment_it->segment_id);
    if (!l1_insert_success) return false; // l1 pma no space
  }
  return true;
}

bool CoBtree::Insert(uint64_t key, uint64_t value) {
  uint64_t vebleaf_address;
  auto l2_segment_id = tree_.Get(key, &vebleaf_address);
  auto l2_segment = pma_index_.Get(l2_segment_id);
  auto l2_item = GetL2Item(key, l2_segment);
  auto l3_segment_id = l2_item.l3_segment_id;
  auto l3_segment = pma_data_.Get(l3_segment_id);
  bool key_equal = false;
  auto pos = GetRecordLocation(key, l3_segment, &key_equal);
  if (key_equal) {
    // fast path perfrom update
    UpdateRecord(key, value, pos, &l3_segment);
    return true;
  } 
  // add new records to L3 and updates L2&L1 if needed
  PMAUpdateContext ctx;
  L3Node record {key, value};
  const char* src = reinterpret_cast<char*>(&record);
  auto l3_insert_success = pma_data_.Add(src, l2_item.l3_segment_id, pos, &ctx);
  if (!l3_insert_success) {
    printf("l3 pma full");
    return false;
  }

  // fast path, insertion did not cause l3 rebalance.
  if (ctx.updated_segment.empty()) return true;

  // update l2 segment down pointer needed
  PMAUpdateContext l2_update_ctx; 
  auto l2_update_success = L2Update(l2_segment_id, l3_segment_id, l2_item.pos,
    ctx, &l2_update_ctx);
  if (!l2_update_success) {
    printf("l2 pma full");
    return false;
  }
  
  // update l1 segment
  // new insertion to l1.
  if (l2_update_ctx.updated_segment.empty()) return true;
  auto l1_update_success = L1Update(vebleaf_address, l2_segment_id, 
    l2_update_ctx);
  if(!l1_update_success) printf("l1 pma full\\");
  return l1_update_success;
}

bool CoBtree::Get(uint64_t key, uint64_t* value) {
  assert(value);
  uint64_t vebleaf_address;
  auto l2_segment_id = tree_.Get(key, &vebleaf_address);
  auto l2_segment = pma_index_.Get(l2_segment_id);
  auto l2_item = GetL2Item(key, l2_segment);
  auto l3_segment_id = l2_item.l3_segment_id;
  auto l3_segment = pma_data_.Get(l3_segment_id);
  bool key_equal = false;
  auto pos = GetRecordLocation(key, l3_segment, &key_equal);
  if (!key_equal) return false; // value not founds
  auto item = reinterpret_cast<L3Node*>(l3_segment.content 
    + pos * sizeof(L3Node));
  *value = item->value;
  return true;
}

}  // namespace cobtree