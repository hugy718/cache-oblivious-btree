#include "vebtree.h"

#include <algorithm>
#include <cassert>
#include <iostream>
#include <stack>

namespace cobtree {

/**
 * @brief perfrom get in van Emde Boas layout tree. The value returned is 
 *  from the leaf value that has the largest key smaller than the lookup key.
 * 
 * @param key search key 
 * @param pma_address address in the PMA where vEB Tree is stored
 * @return uint64_t leaf value
 */
uint64_t vEBTree::Get(uint64_t key, uint64_t* pma_address, bool* match_key) {
  // bool is_leaf = false;
  // // obtain the root node and the address of the target child
  // auto last_address = root_address_;
  // auto address = child_to_search(GetNode(root_address_), key, &is_leaf);
  // // need to traverse down the tree
  // while(!is_leaf) {
  //   auto node = GetNode(address); 
  //   last_address = address;
  //   address = child_to_search(node, key, &is_leaf);
  //   if (is_leaf) break;
  // }
  // *pma_address = last_address;
  // return address;
  auto address = root_address_;
  auto node = GetNode(address);
  while (node->height != 1) {
    address = child_to_search(node, key, match_key);
    node = GetNode(address);
  }
  *pma_address = address;
  return get_children(node)->key;
}

Node* vEBTree::GetNode(uint64_t address) {
  auto segment_id = address / item_per_segment;
  auto segment = pma_.Get(segment_id);
  auto segment_offset = address - segment_id * item_per_segment; 
  assert(segment.len > segment_offset + node_size_);
  return reinterpret_cast<Node*>(segment.content 
    + segment_offset * node_size_); 
}

// Insert in our simulated use case of growing vEBTree, only insert at the tail end, after rebalance fill up new segemnts.
bool vEBTree::Insert(uint64_t key, uint64_t value) {
  // find the parent that we should add this child to
  bool match_key = false;
  // obtain the root node
  auto node = GetNode(root_address_);
  auto address = root_address_;

  // need to traverse down the tree until we are at the leaf.
  while(node->height != 1) {
    address = child_to_search(node, key, &match_key);
    node = GetNode(address);
  }

  // if the leaf with the same key exists, fast path to update it.
  if (match_key) {
    get_children(node)->key = value;
    return true;
  }
  // node insertion needed
  // create a new leaf node
  std::unique_ptr<char[]> buffer(new char[node_size_]);
  std::memset(buffer.get(), -1, node_size_);
  Node* new_leaf = reinterpret_cast<Node*>(buffer.get());
  new_leaf->height = 1;
  new_leaf->parent_addr = node->parent_addr;
  get_children(new_leaf)->key = value;
  
  uint64_t landed_address;
  PMAUpdateContext ctx;
  bool success;
  // minus 1 here because leaf are packed closely in van Emde Boas layout.
  // the new leaf should follow immediately after the search leaf.
  auto root_moved = AddNodeToPMA(new_leaf, address-1, &landed_address, 
    &ctx, &success);
  if (!success) return false; // pma no space.
  // change the root if moved
  if (root_moved) {
    root_address_ = (ctx.updated_segment.back().segment_id + 1) 
      * item_per_segment - 1;
  } 

  // add the leaf node to parent
  return AddChildToNode(GetNode(landed_address)->parent_addr, 
    landed_address, key);
}

// for now use a copy then insert. maybe better to implement its own logic
uint64_t vEBTree::MoveSubtree(uint64_t subtree_root_address, uint64_t height, uint64_t new_address) {
  auto tree_copy = CopySubtree(subtree_root_address, height, false);
  // update the parent of the subtree roots child pointer
  auto subtree_root = GetNode(subtree_root_address);
  auto parent = GetNode(subtree_root->parent_addr);
  for (auto child = get_children(parent);
    child < get_children(parent)+fanout_; child++) {
    if (child->addr == subtree_root_address) {
      child->addr = new_address;
      break;
    }
  }
  InsertSubtree(tree_copy, new_address);
  return tree_copy.node_count;
}

TreeCopy vEBTree::CopySubtree(uint64_t subtree_root_address, uint64_t height, bool top_part_only) {
  // calculate recursive tree context
  auto subtree_height = SubtreeHeight(height) >> ((top_part_only) ? 1 : 0);
  assert(subtree_height > 0);
  uint64_t cap_tree_size = (std::pow(fanout_, subtree_height) - 1)/(fanout_-1); 
  uint64_t leaf_height = height - subtree_height + 1;
  uint64_t last_address_to_copy = subtree_root_address;// termination criteria: the current node is at last_address_to_copy and it has leaf height

  // allocate a new space with size more than enought to accomodate all elements;
  std::unique_ptr<char[]> buffer(new char[cap_tree_size * node_size_]);
  std::memset(buffer.get(), -1, cap_tree_size * node_size_);
  
  std::vector<SegmentInfo> segment_source; // store the information of segments to be copied
  std::vector<uint64_t> empty_spaces{0}; // store the aggregate empty spaces for address readjustment, first segment to copy needs no adjustement wrt empty spaces

  // instantiate the two vectors.
  auto segment_id = subtree_root_address / item_per_segment;
  auto copy_segment_offset = subtree_root_address - segment_id*item_per_segment;
  uint64_t temp = 0;
  while ((temp < cap_tree_size) && (segment_id > 0)) {
    temp += segment_element_count[segment_id] - 
      (item_per_segment - copy_segment_offset) + 1;
    segment_source.emplace_back(segment_id, segment_element_count[segment_id]);
    empty_spaces.emplace_back(empty_spaces.back() 
      + item_per_segment - segment_element_count[segment_id]);
    segment_id--;
    copy_segment_offset = item_per_segment - 1;
  }
  // add the first segment
  if (temp < cap_tree_size) segment_source.emplace_back(SegmentInfo(segment_id, segment_element_count.front()));

  // the offset in the PMA to start copy
  segment_id = subtree_root_address / item_per_segment;
  auto copy_source_offset = subtree_root_address;
  copy_segment_offset = subtree_root_address - segment_id*item_per_segment;
  // dest_it
  Node* dest_it = reinterpret_cast<Node*>(buffer.get() + (cap_tree_size-1) * node_size_);
  bool finished = false;
  uint64_t node_count{0};
  for (auto source_it = segment_source.begin(); 
    (!finished && (source_it != segment_source.end())); 
    source_it++) {
    // num_element to copy in the current segment
    auto num_to_copy = source_it->num_count - (item_per_segment - 1 
      - copy_segment_offset);
    // load the segment
    auto segment = pma_.Get(source_it->segment_id);
    // point to the place to copy
    Node* node_it = reinterpret_cast<Node*>(segment.content+copy_segment_offset*node_size_);

    assert(reinterpret_cast<char*>(dest_it)-buffer.get()>=0);

    while (num_to_copy > 0) {
      node_count++;
      std::memcpy(dest_it, node_it, node_size_);
      // for non-root node we update the parent pointer
      if (dest_it->height != height) {
        dest_it->parent_addr = subtree_root_address - dest_it->parent_addr 
          - empty_spaces[segment_id - dest_it->parent_addr/item_per_segment]; 
      }
      // for non-leaf node we update the children address
      if (dest_it->height != leaf_height) {
        for (auto child = get_children(dest_it); 
          child < get_children(dest_it)+fanout_; child++) {
          if(child->addr == UINT64_MAX) break; // empty nodes encountered
          last_address_to_copy = std::min(last_address_to_copy, child->addr);
          // adjust children address. Note that for non-leaf node, its child will be copied.
          child->addr = subtree_root_address - child->addr 
            - empty_spaces[segment_id - child->addr/item_per_segment];
        }
      } else if (copy_source_offset == last_address_to_copy) {
        // we encounter the last leaf to copy
        finished = true;
        break;
      }
      copy_source_offset --;
      num_to_copy--;
      dest_it = get_next_node_in_segment(dest_it);
      node_it = get_next_node_in_segment(node_it);
    }

    // move to the next segment
    copy_source_offset = source_it->segment_id * item_per_segment - 1;
    copy_segment_offset = item_per_segment - 1; 
  }

  // debug print of tree copy
  {
    auto debug_node_it = reinterpret_cast<Node*>(buffer.get()
      + (cap_tree_size-1) * node_size_);
    auto debug_node_end = reinterpret_cast<Node*>(buffer.get() + (cap_tree_size - node_count) * node_size_);
    while (debug_node_it >= debug_node_end) {
      DebugPrintNode(debug_node_it);
      debug_node_it = get_next_node_in_segment(debug_node_it);
    }
  }

  return TreeCopy{height, leaf_height, node_count, cap_tree_size, 
    std::move(buffer)};
}

// Note: overwrite existing contents, the space should have been left for the tree to fill up
void vEBTree::InsertSubtree(const TreeCopy& tree, uint64_t new_address) {
  auto subtree_height = SubtreeHeight(tree.tree_root_height);
  // ultimate termination criteria
  uint64_t total_to_copy = tree.node_count;

  auto segment_id = new_address / item_per_segment;

  // initialize the destination information
  auto segment_offset = new_address - segment_id * item_per_segment;
  auto temp = segment_element_count[segment_id] - (item_per_segment   
    - segment_offset) + 1;  
  std::vector<SegmentInfo> segment_dest{SegmentInfo{segment_id, temp}}; // store the information of segments to be copied
  std::vector<uint64_t> empty_spaces; // additional offset needed for every segment difference. here the first entry is the number of space padded if the base offset exceeds the number of elements in the first segment to cover.
  while ((temp < tree.node_count) && segment_id > 0) {
    empty_spaces.emplace_back(item_per_segment - segment_element_count[segment_id]);
    segment_id--;
    temp += segment_element_count[segment_id];
    segment_dest.emplace_back(segment_id, segment_element_count[segment_id]);
  }

  auto copy_dest_offset = new_address;
  // points to start position of source.
  Node* source_it = reinterpret_cast<Node*>(tree.tree.get() 
    + (tree.total_size-1) * node_size_);

  for (auto dest_segment_it = segment_dest.begin(); dest_segment_it != segment_dest.end();
    dest_segment_it++) {
    auto segment = pma_.Get(dest_segment_it->segment_id);
    // point to the first position to copy.
    Node* dest_it = reinterpret_cast<Node*>(segment.content 
      + ((item_per_segment - 1
        - segment_element_count[dest_segment_it->segment_id]) 
        + dest_segment_it->num_count) * node_size_);
    auto num_to_copy = dest_segment_it->num_count;
    while(num_to_copy > 0 && total_to_copy > 0) {
      total_to_copy--;
      std::memcpy(dest_it, source_it, node_size_);
      if (dest_it->height != tree.tree_root_height) {
        // update parenet address
        auto offset_base = dest_it->parent_addr;
        auto adjusted_offset = offset_base;
        auto temp = 0;
        auto segment_space_remain = segment_dest[temp].num_count - 1;
        while (offset_base > segment_space_remain) {
          offset_base -= segment_space_remain;
          adjusted_offset += empty_spaces[temp];
          temp++;
          segment_space_remain = segment_dest[temp].num_count;
        } 
        dest_it->parent_addr = new_address - adjusted_offset;
      }

      // for non-leaf node we update the children address
      if (dest_it->height != tree.tree_leaf_height) {
        for (auto child = get_children(dest_it); 
          child < get_children(dest_it)+fanout_; child++) {
          if(child->addr == UINT64_MAX) break; // empty nodes encountered
          // adjust children address. Note that for non-leaf node, its child will be copied.
          auto offset_base = child->addr;
          auto adjusted_offset = offset_base;
          auto temp = 0;
          auto segment_space_remain = segment_dest[temp].num_count - 1;
          while (offset_base > segment_space_remain) {
            offset_base -= segment_space_remain;
            adjusted_offset += empty_spaces[temp];
            temp++;
            segment_space_remain = segment_dest[temp].num_count;
          } 
          child->addr = new_address - adjusted_offset;
        }
      } else {
        // need to rewrite their children parent pointer
        for (auto child = get_children(dest_it); 
          child < get_children(dest_it) + fanout_; child++) {
          if(child->addr == UINT64_MAX) break; // empty nodes encountered
          // adjust children's parent pointer
          auto child_node = GetNode(child->addr);
          child_node->parent_addr = copy_dest_offset; 
        }
      }
      num_to_copy--;
      dest_it = get_next_node_in_segment(dest_it);
      source_it = get_next_node_in_segment(source_it);
      copy_dest_offset--;
    }
    if (dest_segment_it->segment_id > 0) copy_dest_offset = item_per_segment 
      * (dest_segment_it->segment_id - 1) + item_per_segment - 1;
  }
}

namespace {
// helper class to adjust pointer in after a rebalance
// Note that rebalance only changes elements address and does not change the order among the elements.
// Note the insert_address is the intended insertion address not the landed address after rebalance.
// There may be hidden memory transfer cost hidden here. As the context for rebalancing take space 
// O(N/log^2{N}) which may not fit in cache layer.
struct RebalancePointerAdjustementCtx{
  RebalancePointerAdjustementCtx(const PMAUpdateContext& ctx, const std::vector<uint64_t>& old_element_count, uint64_t _segment_size, uint64_t _insert_address) 
    : segment_size(_segment_size), insert_address(_insert_address),
    insert_segment(insert_address/segment_size) {
      for (auto s : ctx.updated_segment) {
        segment_ctx.emplace_back(s.segment_id, 
          old_element_count[s.segment_id], s.num_count);
      }
  }

  // given an address that is before rebalance and return the address after rebalance.
  // return whether the address updated within the updating range.
  // The last argument is that when we calculate the address of inserted element.
  //  setting it true helps us to distinguish against the old item at that place.
   bool AdjustAddress(uint64_t address, uint64_t* ret, bool is_insert_address = false) const {
    // fast path for pointer to element ouside the rebalanced segments.
    if ((address < segment_ctx.front().segment_id*segment_size)
      || (address > (segment_ctx.back().segment_id+1)*segment_size)) {
      return false;   
    }

    assert(ret);
    // if the address fall in the segment for insertion and <= insert address
    // needs special adjustment (-1 when calculating )
    bool in_insert_segment_before_insert = (!is_insert_address) 
      && (address/segment_size == insert_segment) 
      && (address <= insert_address);
    // calculate compact address: the address if set the first position of the old segments as 0 and remove all empty spaces in between.
    // all nodes needs to shift for the empty space in the first rebalancing segment.
    auto compact_address = address
      - segment_ctx.front().segment_id*segment_size
      - ((in_insert_segment_before_insert) ? 1 : 0);
    // the segment id of the address with respective to the first segment id 
    //  in rebalancing context
    auto num_segment_from_start = address / segment_size 
      - segment_ctx.front().segment_id + 1; // needs to plus 1 here. num is at least 1 not 0. 
    auto segment_it = segment_ctx.begin();
    while(num_segment_from_start > 0) {
      compact_address -= (segment_size - segment_it->old_count);
      segment_it++;
      num_segment_from_start--;
    }

    // calculate new address, basically we need to add those empty spaces
    auto new_address = compact_address;
    auto curr_segment_id = 0; // offset from the first rebalance segment
    segment_it = segment_ctx.begin();
    while(new_address >= (curr_segment_id*segment_size)) {
      new_address += (segment_size - segment_it->new_count);
      segment_it++;
      curr_segment_id++;
    }
    new_address += segment_ctx.front().segment_id*segment_size;
    *ret = new_address;
    return true;
  }

  // just the reverse of the above.
  uint64_t RevertAddress(uint64_t address) const {
    // fast path for pointer to element ouside the rebalanced segments.
    if ((address < segment_ctx.front().segment_id*segment_size)
      || (address > (segment_ctx.back().segment_id+1)*segment_size)) {
      return address;   
    }
    // calculate compact address: the address if set the first position of the old segments as 0 and remove all empty spaces in between.
    auto compact_address = address
      - segment_ctx.front().segment_id*segment_size;
    auto num_segment_from_start = address / segment_size 
      - segment_ctx.front().segment_id + 1; // needs to plus 1 here. num is at least 1 not 0. 
    auto segment_it = segment_ctx.begin();
    while(num_segment_from_start > 0) {
      compact_address -= (segment_size - segment_it->new_count);
      segment_it++;
      num_segment_from_start--;
    }

    // calculate new address, basically we need to add those empty spaces
    auto old_address = compact_address;
    auto curr_segment_id = 0; // offset from the first rebalance segment
    segment_it = segment_ctx.begin();
    while(old_address >= (curr_segment_id * segment_size)) {
      old_address += (segment_size - segment_it->old_count);
      segment_it++;
      curr_segment_id++;
    }
    // additional adjustment needed for original address that is in front of the insert address
    bool in_insert_segment_before_insert = (old_address/segment_size == insert_segment) 
    && (old_address < insert_address);
    if (in_insert_segment_before_insert) old_address++;

    // adjust with the first segment in update context
    old_address += segment_ctx.front().segment_id * segment_size;
    return old_address;
  }

  struct CountChange {
    CountChange(uint64_t _segment_id, uint64_t _old_count, uint64_t _new_count)
      : segment_id(_segment_id), old_count(_old_count), new_count(_new_count) {}
    uint64_t segment_id;
    uint64_t old_count;
    uint64_t new_count;
  };

  uint64_t segment_size;
  uint64_t insert_address;
  uint64_t insert_segment;
  std::vector<CountChange> segment_ctx;
};
}  // anonymous namespace

bool vEBTree::AddNodeToPMA(const Node* node, uint64_t address, 
  uint64_t* landed_address, PMAUpdateContext* ctx, bool* success) {
  // insert to the designated segment first.
  assert(landed_address);
  assert(ctx);
  assert(success);
  ctx->clear(); 
  auto segment_id = address / item_per_segment;
  auto segment_offset = address - segment_id * item_per_segment;
  *success = pma_.Add(reinterpret_cast<const char*>(node), segment_id, 
    segment_offset, ctx);

  // update the count before address adjustment.
  segment_element_count[segment_id]++;

  // PMA will not return the insert segment in the ctx 
  // while for vEBTree we need to update address inside it
  // so we manually add it to ctx.
  if (ctx->updated_segment.empty()) {
    ctx->num_filled_empty_segment = 0;
    ctx->updated_segment.emplace_back(segment_id, segment_element_count[segment_id]);
  } 

  // update addresses
  RebalancePointerAdjustementCtx address_adjust{*ctx, 
    segment_element_count, item_per_segment, address};
  for (auto s : ctx->updated_segment) {
    auto segment = pma_.Get(s.segment_id); 
    auto node_it = reinterpret_cast<Node*>(segment.content 
      + (item_per_segment-1) * node_size_);
    auto num_elements = s.num_count;
    auto cur_address = (s.segment_id+1) * item_per_segment - 1;
    while (num_elements > 0) {
      if ((node_it->parent_addr != UINT64_MAX) && (!address_adjust.AdjustAddress(
        node_it->parent_addr, &(node_it->parent_addr)))) {
      // the parenet node is outside our updating ranges. we need to explicitly find it and update.
        auto old_address = address_adjust.RevertAddress(cur_address);
        auto parent_node = GetNode(node_it->parent_addr);
        for (auto child = get_children(parent_node); 
          child != get_children(parent_node) + fanout_; child++) {
          if (child->addr == UINT64_MAX) break;
          if (child->addr != old_address) continue;
          child->addr = cur_address;
          break;
        }
      }
      // for non-leaf node update children address
      if (node_it->height != 1) {
        for (auto child = get_children(node_it); 
          child < get_children(node_it) + fanout_; child++) {            
          // fast path, if we reach an empty child pointer
          if (child->addr == UINT64_MAX) break; 
          if (!address_adjust.AdjustAddress(child->addr, &(child->addr))) {
            // similar to above, if children outside the updating range we need to explicitly find it and update its parent pointer 
            auto child_node = GetNode(child->addr);
            child_node->parent_addr = cur_address;
          }
        }
      }
      node_it = get_next_node_in_segment(node_it);
      cur_address --;
      num_elements--;
    }
  }


  // we simply adjust the inserted address
  address_adjust.AdjustAddress(address, landed_address, true);
  // return if we expand into new segment. if so, need to adjust the root address.

  // the inserted node has not updated its children address
  node = GetNode(*landed_address);
  for (auto child = get_children(node);
    child < get_children(node) + fanout_; child++) {
    if (child->addr == UINT64_MAX) break;
    auto child_node = GetNode(child->addr);
    child_node->parent_addr = *landed_address;
  }

  // update the cached item count in vebtree
  for (auto u : ctx->updated_segment) {
    segment_element_count[u.segment_id] = u.num_count;
  }
  // debug print
  {
    std::cout << "A node has been added\n";
    DebugPrintAsPMA();
  }
  return ctx->num_filled_empty_segment != 0; 
}

// this function only add the entry under the parent node. 
// if we have the child node at hand we can easily figure out the key being its first child key.
bool vEBTree::AddChildToNode(uint64_t node_address, uint64_t child_address, uint64_t child_key) {
  auto node = GetNode(node_address);
  bool done = false;
  int child_count = 0;
  // the child needs insert in place to keep key order.
  // so we need to shift all child entry with larger key to make space
  NodeEntry buffer_shift_child;
  for(auto child = get_children(node); 
    child != get_children(node) + fanout_; child++) {
    // skip smaller childs
    if ((child->key < child_key)) {
      child_count++;
      continue;
    } 
    // cache the entry to be overwritten
    buffer_shift_child.key = child->key;
    buffer_shift_child.addr = child->addr;
    child->addr = child_address;
    child->key = child_key;
    done = true;
    if (buffer_shift_child.empty()) break; // reached the last entry
    child_key = buffer_shift_child.key;
    child_address = buffer_shift_child.addr;
  }
  assert(done);
  return (child_count == fanout_-1) 
    ? NodeSplit(node, node->height, node_address) : true;
}

// leaf node height = 1.
uint64_t vEBTree::SubtreeHeight(uint64_t height) const {
  // assert(height > 1);
  if (height == 0) return 0;
  uint64_t subtree_height_in_power_two = 0;
  while((height & 1ULL) != 1) {
    subtree_height_in_power_two ++;
    height >>= 1;
  }
  return 1 << subtree_height_in_power_two;
}

std::vector<uint64_t> vEBTree::GetLeafAddresses(Node* node, uint64_t height) {
  // for the last two level there will be no leaf subtrees
  if (height <= 2) return std::vector<uint64_t>{};
  auto subtree_height = SubtreeHeight(height);
  // for recursive tree with height less than 4, there is no division of upper and lower tree
  // so there is no leaf tree.
  // currently we don't cope with subtree height >= 8 because the splitting of the upper tree cannot be done via inserting the new node between the splitting node chile. That requires rebuilding the upper part for the splitting node and insertion node separately to a buffer space via a scan then copy back.
  if ((subtree_height >> 2) == 0) return std::vector<uint64_t>{};
  auto leaf_subtree_height = subtree_height >> 1;
  // auto leaf_height = height - subtree_height + 1;
  std::stack<uint64_t> search_address_stack;
  std::vector<uint64_t>  leaf_addresses;
  // DO DFS to obtain the leaf addresses
  // add the child nodes to stack first.
  for (auto subtree = get_children(node) + fanout_ - 1; 
    subtree >= get_children(node); subtree--) {
    if (subtree->addr== UINT64_MAX) continue;
      search_address_stack.push(subtree->addr);
  }
  while (!search_address_stack.empty()) {
    auto search_address = search_address_stack.top();
    search_address_stack.pop();
    auto node = GetNode(search_address);
    // to help order in descending pma address for leaf addresses.
    std::vector<uint64_t> leaf_address_buf{};
    for (auto subtree = get_children(node) + fanout_ - 1; 
      subtree >= get_children(node); subtree--) {
      if (subtree->addr== UINT64_MAX) continue;
      if (node->height == leaf_subtree_height + 1) {
        leaf_address_buf.push_back(subtree->addr); // we can directly return them
      } else  {
        search_address_stack.push(subtree->addr);
      }
    }
    for (auto l = leaf_address_buf.rbegin(); 
      l != leaf_address_buf.rend(); l++) {
      leaf_addresses.emplace_back(*l);
    }
  }
  return std::move(leaf_addresses);
}

// end with call to AddChildToNode, which potentially call NodeSplit on parent node.
bool vEBTree::NodeSplit(Node* node, uint64_t height, uint64_t node_address) {
  // handle root node split.
  if (node->height == root_height_) {
    auto success = AddNewRoot(node);
    if (!success) return false;
    // we need to reassign the node pointer.
    // the old node would be the first child of root
    auto new_root = GetNode(root_address_);
    node_address = get_children(new_root)->addr;
    node = GetNode(node_address);
    // height remain unchanged
  }

  // the splitting node is guaranteed not a root.
  auto partition_idx = fanout_/2;
  int curr_idx = 0;
  auto splitting_node_key = get_children(node)->key; // this is required to relocate the splitting node after new node insertion.
  // determine the new node insertion place before the subtrees that the new node will take control
  auto subtree_height = SubtreeHeight(height);
  auto insert_address = ( subtree_height > 1) 
    ? (get_children(node) + partition_idx) -> addr
    : (node_address - 1);
  // there should be a valid address otherwise NodeSplit should not be called
  assert(insert_address != UINT64_MAX); 

  // create a new node
  std::unique_ptr<char[]> new_node_buffer{new char[node_size_]};
  std::memset(new_node_buffer.get(), -1, node_size_);
  Node* new_node = reinterpret_cast<Node*>(new_node_buffer.get());
  new_node->height = node->height;
  new_node->parent_addr = node->parent_addr;
  // copy over the address of children to own;
  auto num_children_to_own = fanout_-partition_idx;
  auto new_child_itr = get_children(new_node);
  while (num_children_to_own > 0) {
    auto child_to_copy = get_children(node) + fanout_ - num_children_to_own;
    new_child_itr->key = child_to_copy->key;
    new_child_itr->addr = child_to_copy->addr;
    // reset children no longer control in the splitting node
    child_to_copy->key = UINT64_MAX;
    child_to_copy->addr = UINT64_MAX;
    num_children_to_own--;
    new_child_itr++;
  } 
  
  // actually all nodes including this one at insert address and before are shifted one places front. // if exceeds density requirement, need rebalance. 
  PMAUpdateContext ctx;
  uint64_t landed_address;
  bool success;
  auto root_moved = AddNodeToPMA(new_node, insert_address, &landed_address, 
    &ctx, &success);
  if (!success) return false;
  if (root_moved) {
    root_address_ = (ctx.updated_segment.back().segment_id + 1) 
      * item_per_segment - 1;
  } 

  // move each leaf tree still owned by the split node forwards
  // we need to get the original split node again after AddNodeToPMA
  
  new_node = GetNode(landed_address);
  auto original_splitting_node_addres = UINT64_MAX;
  auto new_node_parent = GetNode(new_node->parent_addr);
  for (auto child = get_children(new_node_parent);
    child < get_children(new_node_parent) + fanout_;
    child++) {
    if (child->key == splitting_node_key) {
      original_splitting_node_addres = child->addr;
      break;
    } 
  } 
  assert(original_splitting_node_addres != UINT64_MAX);
  node = GetNode(original_splitting_node_addres);
  std::vector<uint64_t> subtrees_to_move = GetLeafAddresses(node, height);
  if (!subtrees_to_move.empty()) {

  // copy the new node owned top subtree
  TreeCopy temp_tree = CopySubtree(landed_address, height, true);
#ifdef DEBUG
// the address should be already sorted from large to small
// this is just a checking
  printf("check address sorted (large to small):\n");
  for (auto i : subtrees_to_move) {
    printf(" %llu", i);
  }
  printf("\n");
#endif // DEBUG

  // note that move the tree will update their root parent node address pointing to them
  auto dest_address = landed_address;
  // for (auto tree_root_address : subtrees_to_move) {
  for (auto tree_it = subtrees_to_move.begin();
    tree_it != subtrees_to_move.end();
    tree_it++) {
    auto tree_root_address = *tree_it; 
    auto num_node_copied = MoveSubtree(tree_root_address, 
      (height >> 1), dest_address);
    // find the next dest position to copy;
    auto segment_it = dest_address / item_per_segment;
    auto segment_offset = dest_address - item_per_segment * segment_it;
    dest_address -= std::min(num_node_copied, 
      segment_element_count[segment_it] - (item_per_segment - 1 - segment_offset));
    num_node_copied -= std::min(num_node_copied, 
      segment_element_count[segment_it] - (item_per_segment - 1 - segment_offset));
    while (num_node_copied > 0) {
      // dest_address -= item_per_segment - segment_element_count[segment_it];
      dest_address -= item_per_segment - segment_element_count[segment_it];
      segment_it--;
      dest_address -= std::min(num_node_copied, segment_element_count[segment_it]);
      num_node_copied -= std::min(num_node_copied, segment_element_count[segment_it]);
    }
    // final adjustment to make sure the address is not in empty space
    if (dest_address < (segment_it + 1) * item_per_segment 
      - segment_element_count[segment_it]) {
      dest_address = segment_it * item_per_segment - 1;
    }
  }
  
  // insert the new subtree due to split after them.
  InsertSubtree(temp_tree, dest_address);
  // now we need to read the inserted node first
  new_node = GetNode(dest_address);
  landed_address = dest_address;
  }

  return AddChildToNode(new_node->parent_addr, landed_address,
    get_children(new_node)->key);
}


bool vEBTree::AddNewRoot(Node* old_root) {
  std::unique_ptr<char[]> new_root_buffer(new char[node_size_]);
  std::memset(new_root_buffer.get(), -1, node_size_);
  Node* new_root = reinterpret_cast<Node*>(new_root_buffer.get());
  new_root->height = ++root_height_;
  auto children = get_children(new_root);
  children->addr = root_address_;
  children->key = get_children(old_root)->key;

  uint64_t landed_address;
  PMAUpdateContext ctx;
  bool success;
  auto root_moved = AddNodeToPMA(new_root, root_address_, &landed_address, &ctx, &success);
  if (!success) return false;
  // cached info update
  if (root_moved) root_address_ = landed_address;
  // update old root parent pointer.
  new_root = GetNode(landed_address);
  old_root = GetNode(get_children(new_root)->addr);
  old_root->parent_addr = landed_address;
  return true;
}

namespace {

/**
 * @brief Get the idx of child with addr value;
 * 
 * @param node the children nodes to search
 * @param len number of children nodes to search
 * @param addr the address to match
 * @param checker true a child with matching value found; false otherwise
 * @return uint64_t return idx of the child with matching value
 */
uint64_t GetChildIdx(const NodeEntry* node, uint64_t len, uint64_t addr,
  bool* checker = nullptr) {
  assert(node);
  assert(len > 1);
  auto end = node + len;
  int curr_idx = 0;
  while (node != end) {
    if(node->addr == UINT64_MAX) break;
    if (node->addr == addr) {
      if (checker) *checker = true;
      return curr_idx;
    }
    curr_idx++;
    node++;
  } 
  if(checker) *checker = false;
  return len;
}

/**
 * @brief Get the Right Most Child Address 
 * 
 * @param node the children node to search
 * @param len number of children nodes to search
 * @param checker true a child with valid address found; false otherwise
 * @return uint64_t 
 */
uint64_t GetRightMostChildAddress(const NodeEntry* node, uint64_t len, 
  bool* checker = nullptr) {
  assert(node);
  assert(len > 1);
  auto curr = node + len - 1;
  int curr_idx = len - 1;
  if (checker) *checker = true;
  do {
    if(curr->addr != UINT64_MAX) return curr->addr;
    curr--;
  } while (curr != node); 
  if(checker) *checker = false;
  return len;
}

}  // anonymous namespace

void vEBTreeBackwardIterator::Prev() {
  if (!valid_) return; // valid_ set false means we are at the first leaf.
  auto curr_address = curr_parent_address_;
  auto curr = tree_->GetNode(curr_address);
  bool checker;
  auto idx = GetChildIdx(vEBTree::get_children(curr), tree_->fanout_,
    curr_address, &checker);
  assert(checker);
  // continue to move upward
  while (idx == 0) {
    // fast path, we were at the first leaf in tree. no more in front
    if(curr->height == tree_->root_height_) {
      valid_ = false;
      return;
    }
    curr_address = curr->parent_addr;
    curr = tree_->GetNode(curr_address);
    idx = GetChildIdx(vEBTree::get_children(curr), tree_->fanout_,
      curr_address, &checker);
    assert(checker);
  }

  // we are at a node where we have a child in front not visited
  // go all the way to the right most leaf under this unvisited child
  auto unvisited_child = vEBTree::get_children(curr) + (idx-1);
  curr = tree_->GetNode(unvisited_child->addr);
  while(curr->height != 1) {
    // move further down
    auto children = vEBTree::get_children(curr);
    curr_address = GetRightMostChildAddress(children,
      tree_->fanout_, &checker);
    assert(checker);
    curr = tree_->GetNode(curr_address);
  }
  curr_address_ = curr_address;
  curr_ = curr;
  curr_parent_address_ = curr_->parent_addr;
}

void vEBTreeForwardIterator::Next() {
  if (!valid_) return; // valid_ set false means we are at the last leaf.
  auto curr_address = curr_parent_address_;
  auto curr = tree_->GetNode(curr_address);
  bool checker;
  auto idx = GetChildIdx(vEBTree::get_children(curr), tree_->fanout_,
    curr_address, &checker);
  assert(checker);
  while (idx == GetRightMostChildAddress(vEBTree::get_children(curr),
    tree_->fanout_, &checker)) {
    if(curr->height == tree_->root_height_) {
      valid_ = false;
      return;
    }
    curr_address = curr->parent_addr;
    curr = tree_->GetNode(curr_address);
    idx = GetChildIdx(vEBTree::get_children(curr), tree_->fanout_,
      curr_address, &checker);
    assert(checker);
  }

  // we are at a node where we have a child in next not visited
  // go all the way to the left most leaf under this unvisited child
  auto unvisited_child = vEBTree::get_children(curr) + (idx+1);
  curr = tree_->GetNode(unvisited_child->addr);
  while(curr->height != 1) {
    curr_address = vEBTree::get_children(curr)->addr;
    assert(curr_address != UINT64_MAX);
    curr = tree_->GetNode(curr_address);
  }
  curr_address_ = curr_address;
  curr_ = curr;
  curr_parent_address_ = curr_->parent_addr;
}

void vEBTree::UpdateLeafKey(uint64_t leaf_address, uint64_t parent_address, uint64_t new_key) {
  auto curr_address = parent_address;
  Node* curr;
  uint64_t idx;
  do {
    curr = GetNode(curr_address);
    bool checker;
    idx = GetChildIdx(get_children(curr), fanout_, leaf_address, &checker);
    auto child_entry = get_children(curr) + idx;
    assert(child_entry->addr = curr_address);
    child_entry->key = new_key;
    curr_address = curr->parent_addr;
  } while (idx==0 && curr->height != root_height_);
}

void vEBTree::DebugPrintNode(const Node* node) const {
  // print the node header infomation
  std::cout << " (height " << ((node->height != UINT64_MAX)
    ? std::to_string(node->height) : "null"); 
  std::cout << " parent addr: " << ((node->parent_addr != UINT64_MAX)
    ? std::to_string(node->parent_addr) : "null");
  std::cout << " children: ";
  // print children information
  auto child = get_children(node);
  for (auto i = 0; i < fanout_; i++) {
    std::cout << " key: " << ((child->key !=UINT64_MAX) 
      ? std::to_string(child->key) : "null");
    std::cout << " addr: " << ((child->addr !=UINT64_MAX) 
      ? std::to_string(child->addr) : "null");
    child ++;
  }
  std::cout << ")\n";
}

void vEBTree::DebugPrintAsPMA() const {
  for (uint64_t segment_id = 0; segment_id < pma_.segment_count(); segment_id++) {
    // all segments after this are empty
    auto num_nodes = segment_element_count[segment_id];
    if (num_nodes == 0) break;
    auto segment = pma_.Get(segment_id);
    auto node = reinterpret_cast<Node*>(segment.content 
      + (item_per_segment - num_nodes) * node_size_);
    auto node_end = reinterpret_cast<Node*>(segment.content 
      + item_per_segment * node_size_);
    // for printing corresponding address in pma
    auto addr = (segment_id+1) * item_per_segment - num_nodes;
    while (node != node_end) {
      // print the pma address
      std::cout<< "PMA address: " << addr;
      addr++;
      DebugPrintNode(node);
      // move to the next node in the pma segment
      node = reinterpret_cast<Node*>(
        reinterpret_cast<char*>(node) + node_size_
      );
    }
  }
}

void vEBTree::DebugPrintDFS() {
  std::stack<uint64_t> dfs_idx_stack;
  dfs_idx_stack.push(0);
  auto node = GetNode(root_address_);
  auto curr_idx = 0;
  auto curr_address = root_address_;
  std::cout << "PMA address: " << curr_address;
  DebugPrintNode(node);
  while (!dfs_idx_stack.empty()) {
    if ((curr_idx >= fanout_) || (node->height == 1)) {
      curr_address = node->parent_addr;
      if(curr_address == UINT64_MAX) { break; /*root finished*/}
      node = GetNode(curr_address);
      curr_idx = dfs_idx_stack.top() + 1;
      dfs_idx_stack.pop();
      if (curr_idx != fanout_) dfs_idx_stack.push(curr_idx);
    } else {
      curr_address = (get_children(node)+curr_idx)->addr;
      if (curr_address == UINT64_MAX) {
        curr_idx = fanout_;
        dfs_idx_stack.pop();
        continue;
      }
      node = GetNode(curr_address);
      auto padding = std::string(dfs_idx_stack.size(), ' ');
      std::cout << padding << "PMA address: " << curr_address;
      DebugPrintNode(node);
      if (node->height == 1) continue;
      curr_idx = 0;
      dfs_idx_stack.push(curr_idx);
    }
  }
}

}  // namespace cobtree