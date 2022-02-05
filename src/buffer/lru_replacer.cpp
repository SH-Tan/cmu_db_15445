//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"
#include <mutex>
#include "common/config.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {
  capacity_ = num_pages;  // Initialization the capacity
}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::lock_guard<std::mutex> lck(latch_);

  if (map_.empty()) {
    return false;
  }

  *frame_id = lru_list_.back();
  map_.erase(*frame_id);
  lru_list_.pop_back();

  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lck(latch_);
  if (map_.count(frame_id) == 0) {
    return;
  }

  auto iter = map_[frame_id];
  lru_list_.erase(iter);
  map_.erase(frame_id);
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lck(latch_);
  if (map_.count(frame_id) != 0) {
    return;
  }

  if (lru_list_.size() == capacity_) {
    return;
  }

  lru_list_.push_front(frame_id);
  map_.emplace(frame_id, lru_list_.begin());
}

size_t LRUReplacer::Size() { return lru_list_.size(); }

}  // namespace bustub
