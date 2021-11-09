#ifndef COBTREE_VEBTREE_H_
#define COBTREE_VEBTREE_H_

#include "pma.h"

namespace cobtree {

struct TreeCopy {
  uint64_t tree_root_height; // height of the root in the vEBtree before copy.
  uint64_t node_count;
  uint64_t total_size;
  std::unique_ptr<char[]> tree;
};

// helper class 
class vEBTreeForwardIterator;
class vEBTreeBackwardIterator;

class vEBTree {
 public:
  vEBTree() = delete;
  vEBTree(uint64_t fanout, uint64_t estimated_unit_count, uint64_t pma_redundancy_factor, 
    const std::string& uid, const PMADensityOption& pma_options, Cache* cache)
    : fanout_(fanout), node_size_(sizeof(Node) + sizeof(NodeEntry) * fanout_),
      root_height_(2), // one leaf and one root will be created
      pma_(uid, node_size_, estimated_unit_count * pma_redundancy_factor, pma_options, cache),
      pma_segment_size_(pma_.segment_size()),
      root_address_(pma_segment_size_-1), // the initial root is at the end of the first segment
      segment_element_count(pma_segment_size_, 0) {
      assert(pma_segment_size_ > 10); // a segment needs to be reasonably large
      // create the fist leaf
      std::unique_ptr<char[]> first_leaf_buffer{ new char[node_size_] };
      std::memset(first_leaf_buffer.get(), -1, node_size_);
      Node* first_leaf = reinterpret_cast<Node*>(first_leaf_buffer.get());
      first_leaf->parent_addr = root_address_;
      first_leaf->height = 1;
      get_children(first_leaf)->key = 0; // points to the first segment in 2nd level PMA.
      auto segment = pma_.Get(0);
      std::memcpy((segment.content + pma_segment_size_ - 2 * node_size_),
        first_leaf_buffer.get(), node_size_);

      // create the root
      std::unique_ptr<char[]> first_root_buffer{ new char[node_size_] };
      std:memset(first_root_buffer.get(), -1, node_size_);
      Node* first_root = reinterpret_cast<Node*>(first_root_buffer.get());
      first_root->height = 2;
      auto child = get_children(first_root);
      child->key = 0; // in out fixed key range of uint64_t 0 is the smallest key.
      child->addr = pma_segment_size_ - 2;
      std::memcpy((segment.content + pma_segment_size_ - node_size_),
        first_root_buffer.get(), node_size_);
    }

  /**
   * @brief perfrom get in van Emde Boas layout tree. The value returned is 
   *  from the leaf value that has the largest key smaller than the lookup key.
   * 
   * @param key search key 
   * @param pma_address address in the PMA where vEB Tree is stored
   * @return uint64_t leaf value
   */
  uint64_t Get(uint64_t key, uint64_t* pma_address);

  // first level PMA rebalance can trigger update on the nodes key and its parents separator keys.
  // an API to return the node is helpful.
  Node* GetNode(uint64_t address);
 
  bool Insert(uint64_t key, uint64_t value);
  
  static NodeEntry* get_children(Node* node) {
    return reinterpret_cast<NodeEntry*>(node + sizeof(Node));
  }

  inline uint64_t fanout() const { return fanout_; }

 private:
  friend vEBTreeForwardIterator;
  friend vEBTreeBackwardIterator;
  //  TODO: for some helper function, the leaf in overall vEBTree might need special treatment while they are leaf in a context of recursive subtree. needs to check through.

  // return number of nodes of moved tree. facilitate calculation of the end address.
  uint64_t MoveSubtree(uint64_t subtree_root_address, uint64_t height, uint64_t new_address);

  TreeCopy CopySubtree(uint64_t subtree_root_addresss, 
    uint64_t height);

  void InsertSubtree(const TreeCopy& tree_store, uint64_t new_address);
  
  /**
   * @brief add the content under node to the address to PMA.
   * and then update the addresses
   * 
   * @param node 
   * @param address 
   * @param landed_address return the node actual address after possibly rebalancing in landed_address 
   * @param ctx return pma update context
   * @param success return if pma insertion succeeds (false if no space)
   * @return true the root is moved
   * @return false 
   */
  bool AddNodeToPMA(const Node* node, uint64_t address, 
    uint64_t* landed_address, PMAUpdateContext* ctx, bool* success); 

  // this function only add the entry under the parent node. 
  // if we have the child node at hand we can easily figure out the key being its first child key.
  // return false if pma no space
  bool AddChildToNode(uint64_t node_address, uint64_t child_address, uint64_t child_key);

  // calculate the subtree height for a node at this height in a vEBTree.
  uint64_t SubtreeHeight(uint64_t height) const ;

  // get the children of leaf node of the recursive subtree rooted at node.
  // return empty vector for node at height 1 (leaf of vEBTree) and 2.
  std::vector<uint64_t> GetLeafAddresses(Node* node, uint64_t height);

  // when the node children exceeds threshold call this method.
  bool NodeSplit(Node* node, uint64_t height);

  // root_address_ and root_height_ will be updated by this function
  // return false if no more space
  bool AddNewRoot(Node* old_root);


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

  uint64_t fanout_; // 4d
  // store the parent address and (key and address) of at most 4d children
  uint64_t node_size_; // 4d * (address size + key size) + address size )
  uint64_t root_height_; // the height of root node in pma.
  PMA pma_;
  uint64_t pma_segment_size_; // cached pma segment size
  uint64_t root_address_; // the position of root node in pma.

  // the segment_element_count can be stored at the leading space in a segment
  // or we can store it elsewhere and retrieve it with O(1) cost (reading of such information of adjacent segments can amortize cost).
  // here we store it in memory for simplicity and do not account for the cost of retrieving such information in simulation. (in analysis of the paper, this is not from the dominant term)
  std::vector<uint64_t> segment_element_count;
};

class vEBTreeBackwardIterator {
 public:
  vEBTreeBackwardIterator(vEBTree* tree, uint64_t leaf_address)
    : valid_(true), curr_address_(leaf_address), tree_(tree), 
    curr_(tree_->GetNode(leaf_address)) {}
  
  bool valid() const { return valid_ && (curr_->height == 1); }

  // move to the previous leaf node
  void Prev();
  
  // check valid() first
  Node* node() { return curr_; }

 private:
  bool valid_;
  uint64_t curr_address_;
  vEBTree* tree_;
  Node* curr_;
};

class vEBTreeForwardIterator {
 public:
  vEBTreeForwardIterator(vEBTree* tree, uint64_t leaf_address)
    : valid_(true), curr_address_(leaf_address), tree_(tree), 
    curr_(tree_->GetNode(leaf_address)) {}
  
  bool valid() const { return valid_ && (curr_->height == 1); }

  // move to the next leaf node
  void Next();

  // check valid() first
  Node* node() { return curr_; }

 private:
  bool valid_;
  uint64_t curr_address_;
  vEBTree* tree_;
  Node* curr_;
};

}  // namespace cobtree
#endif  // COBTREE_VEBTREE_H_