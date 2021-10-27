#ifndef COBTREE_BLOCKDEVICE_H_
#define COBTREE_BLOCKDEVICE_H_

#include <cstdint>
#include <memory>
#include <string>
#include <set>
#include "type.h"

#define BLOCKSIZE 4096

namespace cobtree {

class BlockDevice {
 public:
  BlockDevice(uint64_t size) 
    : block_size_(BLOCKSIZE),
    buffer_size_(AdjustForBlockSize(BLOCKSIZE, size)),
    buffer_(new char[buffer_size_]) {}

  BlockDevice(uint64_t block_size, uint64_t size) 
    : block_size_(block_size),
    buffer_size_(AdjustForBlockSize(block_size, size)),
    buffer_(new char[buffer_size_]) {}
  
  // return the number of bytes read
  uint64_t Read(uint64_t offset, uint64_t len, char* ret, uint64_t* cost);
  
  void Write(const char* data, uint64_t offset, uint64_t len);

  inline uint64_t block_size() const { return block_size_; };  // B
  // uint64_t cache_size();  // M

  static uint64_t AdjustForBlockSize(uint64_t block_size, uint64_t size) {
    return (size + block_size -1) / block_size * block_size;
  }

 private:
  const uint64_t block_size_;
  // std::set<uint64_t> in_memory_;
  const uint64_t buffer_size_;
  std::unique_ptr<char> buffer_;

};

}  // namespace cobtree
#endif  // COBTREE_BLOCKDEVICE_H_