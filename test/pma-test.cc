#include <iostream>
#include <string>
#include <vector>
#include "pma.h"

using namespace cobtree;

struct Record{
  uint64_t key;
  uint64_t value;
};

uint64_t find_segment(uint64_t key, const std::vector<uint64_t> segment_keys) {
  uint64_t pos = 0;
  for (auto i : segment_keys) {
    if (i <= key) return pos;
    pos++;
  }
  return UINT64_MAX;
}

uint64_t find_position(uint64_t key, const PMA& pma, uint64_t segment_id) {
  // by construction we should have l3 segment size being 
  // a multiple of record size.
  auto item_size = sizeof(Record);
  auto segment = pma.Get(segment_id);
  assert((segment.len % item_size) == 0); 
  auto num_element = segment.num_item;
  assert(num_element < segment.len / item_size);
  Record* item = reinterpret_cast<Record*>(segment.content + segment.len - item_size);

  auto pos = (segment.len / item_size) - 1;
  while (num_element > 0 && item->key < key) {
    item--;
    num_element--;
    pos--;
  }
  return pos;
}

uint64_t find_value(uint64_t key, const PMA& pma, uint64_t segment_id) {
  // by construction we should have l3 segment size being 
  // a multiple of record size.
  auto item_size = sizeof(Record);
  auto segment = pma.Get(segment_id);
  assert((segment.len % item_size) == 0); 
  auto num_element = segment.num_item;
  assert(num_element < segment.len / item_size);
  Record* item = reinterpret_cast<Record*>(segment.content + segment.len - item_size);

  auto pos = (segment.len / item_size) - 1;
  while (num_element > 0 && item->key < key) {
    item--;
    num_element--;
    pos--;
  }
  return (item->key == key) ? item->value : UINT64_MAX;
}

void update_segment_keys(const PMAUpdateContext& ctx,
  std::vector<uint64_t>* segment_keys, const PMA& pma) {
  for (auto s : ctx.updated_segment) {
    // find the first key
    auto segment = pma.Get(s.segment_id);
    auto record = reinterpret_cast<const Record*>(segment.content 
      + (pma.segment_size()-1) * sizeof(Record));
    (*segment_keys)[s.segment_id] = record->key;
  }
}

void print_segment(const PMASegment& segment) {
  auto num_record = segment.num_item;
  if (num_record == 0) return;
  auto it = reinterpret_cast<Record*> (segment.content + segment.len - sizeof(Record));
  while (num_record > 0) {
    std::cout << it->key << " " << it->value << " ";
    it--;
    num_record--;
  }
  std::cout << "\n"; 
}

int main(){
  // cobtree configuration set up
  uint64_t veb_fanout = 4;
  uint64_t estimated_record_count = 1024;
  double pma_redundancy_factor = 1.2;
  PMADensityOption pma_density{0.8, 0.6, 0.2, 0.1};
  const std::string uid{"pma-test"};

  // set up cache
  uint64_t cache_size = 40*1024;
  Cache cache{cache_size};
  cache.set_block_size_for_stats(4096);

  PMA pma{uid, sizeof(Record), estimated_record_count*pma_redundancy_factor,
    pma_density, &cache};

  // index
  auto segment_keys = std::vector<uint64_t>(pma.segment_count(), 0);

  // add the first dummy element
  Record record{0,0};
  PMAUpdateContext ctx0;
  pma.Add(reinterpret_cast<char*>(&record), 0, pma.segment_size()-1, &ctx0);
  
  std::cout << "--------------insertion-----------------\n";
  for (uint64_t i = 1; i < 700; i++) {
    if (i == 456) {
      printf("stop here!\n");
    }
    std::cout << "insert: " << i << " \n";
    Record rec{i, i+10};
    auto segment_id = find_segment(i, segment_keys);
    assert(segment_id != UINT64_MAX);
    auto pos = find_position(i, pma, segment_id);
    PMAUpdateContext ctx;
    auto success = pma.Add(reinterpret_cast<char*>(&rec), segment_id, 
      pos, &ctx);

    if (!success) { 
      printf("full!\n"); 
      break; 
    }
    if(!ctx.updated_segment.empty()) {
      update_segment_keys(ctx, &segment_keys, pma);
    } 
    {
      // print the segment content to see where went wrong
      uint64_t print_segment_id = 0;
      bool last = false;
      while (!last || (segment_keys[print_segment_id] != 0)) {
        if (segment_keys[print_segment_id] == 0) last = true;
        std::cout << "print content of segment " << print_segment_id << ":\n";
        print_segment(pma.Get(print_segment_id));
        print_segment_id++;
      }
    }
  }
  
  std::cout << "--------------Get-----------------\n";
  for (uint64_t i = 1; i < 700; i++) {
    std::cout << "get: " << i << " \n";
    auto segment_id = find_segment(i, segment_keys);
    assert(segment_id != UINT64_MAX);
    auto value = find_value(i, pma, segment_id);
    assert(value != UINT64_MAX);
    std::cout << value << "\n";
  }
  
  return 0;
}