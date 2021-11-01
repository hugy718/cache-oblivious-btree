#ifndef COBTREE_VEBTREE_H_
#define COBTREE_VEBTREE_H_

#include "cobtree.h"
#include "pma.h"

namespace cobtree {

struct SplitContext {
  Node* node;
  
};

struct TreeCopy {
  uint64_t tree_root_height; // height of the root in the vEBtree before copy.
  uint64_t node_count;
  uint64_t total_size;
  std::unique_ptr<char[]> tree;
};

class vEBTree : public CoBtree {
 public:

  uint64_t Get(uint64_t key);

  void Insert(uint64_t key, uint64_t value);
 
  void Update(uint64_t key, uint64_t new_key, uint64_t value);
  
 private:
  //  TODO: for some helper function, the leaf in overall vEBTree might need special treatment while they are leaf in a context of recursive subtree. needs to check through.

  // return number of nodes of moved tree. facilitate calculation of the end address.
  uint64_t MoveSubtree(uint64_t subtree_root_address, uint64_t height, uint64_t new_address);

  TreeCopy vEBTree::CopySubtree(uint64_t subtree_root_addresss, 
    uint64_t height);

  void InsertSubtree(const TreeCopy& tree_store, uint64_t new_address);
  
  // add the content under node to the address to PMA.
  // and then update the addresses
  // return if the root is moved. (we can update the cached value here)
  // return the node actual address after possibly rebalancing in landed_address 
  bool AddNodeToPMA(const Node* node, uint64_t address, 
    uint64_t* landed_address, PMAUpdateContext* ctx); 

  // this function only add the entry under the parent node. 
  // if we have the child node at hand we can easily figure out the key being its first child key.
  void AddChildToNode(uint64_t node_address, uint64_t child_address, uint64_t child_key);

  // calculate the subtree height for a node at this height in a vEBTree.
  uint64_t SubtreeHeight(uint64_t height) const ;

  // get the children of leaf node of the recursive subtree rooted at node.
  // return empty vector for node at height 1 (leaf of vEBTree) and 2.
  std::vector<uint64_t> GetLeafAddresses(Node* node, uint64_t height);

  // when the node children exceeds threshold call this method.
  bool NodeSplit(Node* node, uint64_t height);

  inline NodeEntry* get_children(Node* node) const {
    return reinterpret_cast<NodeEntry*>(node + sizeof(Node));
  }

  inline const NodeEntry* get_children(const Node* node) const {
    return reinterpret_cast<const NodeEntry*>(node + sizeof(Node));
  }

  // this essentially move the node stored immediately before this node.
  inline Node* get_next_node_in_segment(Node* node) const {
    return reinterpret_cast<Node*>(reinterpret_cast<char*>(node) 
      - node_size_);
  }  

  // check whether all node entries are full.
  inline bool children_exceeds_threshold(const Node* node) const {
    return (get_children(node)+fanout_-1)->addr != UINT16_MAX;
  }

  inline uint64_t child_to_search(const Node* node, uint64_t key, bool* is_leaf) {
    auto child = get_children(node);
    if (child->addr == UINT64_MAX) {
      // leaf node has no children and the first key in children buffer is set to value.
      // mark is_leaf as true
      *is_leaf = true;
      return child->key;
    }
    for (auto it  = child; it < child + fanout_; it++) {
      // next children has larger key or no more nodes
      if ((it->key > key) || (it->addr == UINT64_MAX)) return (--it)->addr;
      it++;
    }
    // return the last child address
    return (child + fanout_ - 1)->addr;
  }

  uint64_t depth_;
  uint64_t fanout_; // d
  // store the parent address and (key and address) of at most 4d children
  uint64_t node_size_; // 4d * (address size + key size) + address size 
  // Node root_; // not cached otherwise needs update everytime adding node (to avoid outdated addresses)
  uint64_t root_address_; // the position of leaf node in pma.
  bool use_pma;
  PMA pma_;

  // the segment_element_count can be stored at the leading space in a segment
  // or we can store it elsewhere and retrieve it with O(1) cost (reading of such information of adjacent segments can amortize cost).
  // here we store it in memory for simplicity and do not account for the cost of retrieving such information in simulation. (in analysis of the paper, this is not from the dominant term)
  std::vector<uint64_t> segment_element_count;

  // the up pointer from second level pma can be stored at the leading space in a segment
  // or we can store it elsewhere and retrieve it with O(1) cost (reading of such information of adjacent segments can amortize cost)
  // here we store it in memory for simplicity and do not account for the cost of retrieving such information in simulation (in analysis of the paper, this is not from the dominant term)
  std::vector<uint64_t> segment_to_leaf_address;
};

}  // namespace cobtree
#endif  // COBTREE_VEBTREE_H_