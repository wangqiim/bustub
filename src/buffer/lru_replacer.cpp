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

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) : size_(0), capacity_(num_pages) {
  cache_map_.clear();
  cache_list_.clear();
}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::lock_guard<std::mutex> guard(latch_);
  if (this->size_ == 0) {
    return false;
  }
  frame_id_t back = cache_list_.back();
  this->cache_map_.erase(back);
  this->cache_list_.pop_back();
  *frame_id = back;
  this->size_--;
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(latch_);
  if (this->cache_map_.find(frame_id) != this->cache_map_.end()) {
    this->cache_list_.erase(this->cache_map_[frame_id]);
    this->cache_map_.erase(frame_id);
    this->size_--;
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(latch_);
  if (this->cache_map_.find(frame_id) == this->cache_map_.end()) {
    this->cache_list_.push_front(frame_id);
    this->cache_map_[frame_id] = cache_list_.begin();
    this->size_++;
  }
  if (this->size_ > this->capacity_) {
    frame_id_t back = cache_list_.back();
    this->cache_list_.pop_back();
    this->cache_map_.erase(back);
    this->size_--;
  }
}

size_t LRUReplacer::Size() {
  std::lock_guard<std::mutex> guard(latch_);
  return this->size_;
}

}  // namespace bustub
