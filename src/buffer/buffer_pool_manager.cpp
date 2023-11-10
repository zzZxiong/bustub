//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"
#include <algorithm>
#include <cassert>
#include <mutex>
#include <shared_mutex>
#include <utility>

#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  frame_id_t frame_id;
  bool create_from_list = false;
  bool create_from_replacer = false;

  std::lock_guard<std::shared_mutex> lk(latch_);
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
    create_from_list = true;
  }

  if (!create_from_list && replacer_->Evict(&frame_id)) {
    pages_[frame_id].RLatch();
    if (pages_[frame_id].is_dirty_) {
      auto promise = disk_scheduler_->CreatePromise();
      auto future = promise.get_future();
      disk_scheduler_->Schedule({true, pages_[frame_id].data_, pages_[frame_id].page_id_, std::move(promise)});
      future.get();
    }
    page_table_.erase(pages_[frame_id].page_id_);
    pages_[frame_id].RUnlatch();

    pages_[frame_id].WLatch();
    pages_[frame_id].ResetMemory();
    pages_[frame_id].page_id_ = INVALID_PAGE_ID;
    pages_[frame_id].pin_count_ = 0;
    pages_[frame_id].is_dirty_ = false;
    pages_[frame_id].WUnlatch();

    create_from_replacer = true;
  }

  if (!create_from_list && !create_from_replacer) {
    page_id = nullptr;
    return nullptr;
  }

  *page_id = AllocatePage();
  page_table_[*page_id] = frame_id;

  pages_[frame_id].WLatch();
  pages_[frame_id].page_id_ = *page_id;
  ++pages_[frame_id].pin_count_;
  pages_[frame_id].WUnlatch();

  replacer_->RecordAccess(frame_id);

  return pages_ + frame_id;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::lock_guard<std::shared_mutex> lk(latch_);
  auto iter = page_table_.find(page_id);
  if (iter != page_table_.end()) {
    pages_[iter->second].WLatch();
    ++pages_[iter->second].pin_count_;
    pages_[iter->second].WUnlatch();

    replacer_->RecordAccess(iter->second);
    replacer_->SetEvictable(iter->second, false);

    return pages_ + iter->second;
  }

  frame_id_t frame_id;
  bool create_from_list = false;
  bool create_from_replacer = false;

  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
    create_from_list = true;
  }

  if (!create_from_list && replacer_->Evict(&frame_id)) {
    pages_[frame_id].RLatch();
    if (pages_[frame_id].is_dirty_) {
      auto promise = disk_scheduler_->CreatePromise();
      auto future = promise.get_future();
      disk_scheduler_->Schedule({true, pages_[frame_id].data_, pages_[frame_id].page_id_, std::move(promise)});
      future.get();
    }
    page_table_.erase(pages_[frame_id].page_id_);
    pages_[frame_id].RUnlatch();

    pages_[frame_id].WLatch();
    pages_[frame_id].ResetMemory();
    pages_[frame_id].page_id_ = INVALID_PAGE_ID;
    pages_[frame_id].pin_count_ = 0;
    pages_[frame_id].is_dirty_ = false;
    pages_[frame_id].WUnlatch();

    create_from_replacer = true;
  }

  if (!create_from_list && !create_from_replacer) {
    return nullptr;
  }

  page_table_[page_id] = frame_id;

  pages_[frame_id].WLatch();
  pages_[frame_id].page_id_ = page_id;
  ++pages_[frame_id].pin_count_;
  pages_[frame_id].WUnlatch();

  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();

  pages_[frame_id].RLatch();
  disk_scheduler_->Schedule({false, pages_[frame_id].data_, pages_[frame_id].page_id_, std::move(promise)});
  pages_[frame_id].RUnlatch();
  future.get();

  return pages_ + frame_id;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::shared_lock<std::shared_mutex> lk(latch_);
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    return false;
  }
  frame_id_t frame_id = iter->second;

  pages_[frame_id].RLatch();
  int pin = pages_[frame_id].pin_count_;
  pages_[frame_id].RUnlatch();

  if (pin == 0) {
    return false;
  }

  pages_[frame_id].WLatch();
  if (--pages_[frame_id].pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  pages_[frame_id].is_dirty_ |= is_dirty;
  pages_[frame_id].WUnlatch();

  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }

  std::shared_lock<std::shared_mutex> lk(latch_);
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    return false;
  }

  frame_id_t frame_id = iter->second;
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  pages_[frame_id].RLatch();
  disk_scheduler_->Schedule({true, pages_[frame_id].data_, pages_[frame_id].page_id_, std::move(promise)});
  pages_[frame_id].RUnlatch();
  future.get();

  pages_[frame_id].WLatch();
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].WUnlatch();

  return true;
}

void BufferPoolManager::FlushAllPages() {
  std::shared_lock<std::shared_mutex> lk(latch_);
  std::for_each(page_table_.begin(), page_table_.end(), [&, this](std::pair<const page_id_t, frame_id_t> iter) {
    pages_[iter.second].RLatch();
    bool need_flush = pages_[iter.second].IsDirty();
    pages_[iter.second].RUnlatch();

    if (need_flush) {
      FlushPage(iter.first);
    }
  });
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::lock_guard<std::shared_mutex> lk(latch_);
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    return true;
  }

  frame_id_t frame_id = iter->second;

  pages_[frame_id].RLatch();
  int pin = pages_[frame_id].GetPinCount();
  pages_[frame_id].RUnlatch();

  if (pin != 0) {
    return false;
  }

  replacer_->SetEvictable(frame_id, true);
  replacer_->Remove(frame_id);

  pages_[frame_id].WLatch();
  pages_[frame_id].ResetMemory();
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  pages_[frame_id].pin_count_ = 0;
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].WUnlatch();

  page_table_.erase(page_id);
  free_list_.push_back(frame_id);
  DeallocatePage(page_id);

  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { return {this, nullptr}; }

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, nullptr}; }

}  // namespace bustub
