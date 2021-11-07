#include <cstring>
#include "block_device.h"

namespace cobtree {

// return the number of bytes read
uint64_t BlockDevice::Read(uint64_t offset, uint64_t len, 
  char* ret) {
  ret = buffer_.get() + offset;
  return (offset + len > buffer_size_) ? (buffer_size_ - offset) : len; 
}

void BlockDevice::Write(const char* data, uint64_t offset, 
  uint64_t len) {
  if (offset + len > buffer_size_) return; // no op if exceeds the buffer space
  std::memcpy(buffer_.get() + offset, data, len);
}

}  // namespace cobtree