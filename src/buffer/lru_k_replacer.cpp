//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <shared_mutex>
#include <utility>
#include "common/config.h"
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  bool find_less_k_node = false;
  bool find_more_k_node = false;
  frame_id_t less_k_frame;
  frame_id_t more_k_frame;
  size_t min_less_k_visit = SIZE_MAX;
  size_t min_more_k_visit = SIZE_MAX;

  std::unique_lock<std::shared_mutex> replacer_lock(latch_);
  for (const auto &[key, node] : node_store_) {
    if (node.is_evictable_) {
      if (node.k_ < k_) {
        if (min_less_k_visit > node.history_.front()) {
          min_less_k_visit = node.history_.front();
          less_k_frame = node.fid_;
          find_less_k_node = true;
        }
      } else {
        if (min_more_k_visit > node.history_.back()) {
          min_more_k_visit = node.history_.back();
          more_k_frame = node.fid_;
          find_more_k_node = true;
        }
      }
    }
  }

  if (find_less_k_node) {
    node_store_.erase(less_k_frame);

    replacer_lock.unlock();
    evictable_frames_size_.fetch_sub(1);
    *frame_id = less_k_frame;
  } else if (find_more_k_node) {
    node_store_.erase(more_k_frame);

    replacer_lock.unlock();
    evictable_frames_size_.fetch_sub(1);
    *frame_id = more_k_frame;
  } else {
    frame_id = nullptr;
    return false;
  }

  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  if (static_cast<size_t>(frame_id) >= replacer_size_) {
    throw Exception("The frame_id is more than replacer's maxSize\n");
  }

  std::lock_guard<std::shared_mutex> replacer_lock(latch_);
  if (node_store_.find(frame_id) == node_store_.end()) {
    node_store_.emplace(frame_id, LRUKNode());
  }

  ++node_store_.at(frame_id).k_;
  node_store_.at(frame_id).fid_ = frame_id;
  node_store_.at(frame_id).history_.push_back(current_timestamp_++);
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  if (static_cast<size_t>(frame_id) >= replacer_size_) {
    return;
  }

  std::unique_lock<std::shared_mutex> replacer_lock(latch_);
  auto iter = node_store_.find(frame_id);
  if (iter != node_store_.end() && iter->second.is_evictable_ != set_evictable) {
    iter->second.is_evictable_ = set_evictable;
    replacer_lock.unlock();
    if (set_evictable) {
      evictable_frames_size_.fetch_add(1);
    } else {
      evictable_frames_size_.fetch_sub(1);
    }
  }
}

// only called when frame_id exists
void LRUKReplacer::Remove(frame_id_t frame_id) {
  if (static_cast<size_t>(frame_id) >= replacer_size_) {
    return;
  }

  std::unique_lock<std::shared_mutex> replacer_lock(latch_);
  auto iter = node_store_.find(frame_id);
  if (iter != node_store_.end() && iter->second.is_evictable_) {
    node_store_.erase(frame_id);
    replacer_lock.unlock();
    evictable_frames_size_.fetch_sub(1);
  }
}

auto LRUKReplacer::Size() -> size_t { return evictable_frames_size_.load(); }

}  // namespace bustub
