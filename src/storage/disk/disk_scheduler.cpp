//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_scheduler.cpp
//
// Identification: src/storage/disk/disk_scheduler.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/disk/disk_scheduler.h"
#include "common/exception.h"
#include "storage/disk/disk_manager.h"

namespace bustub {

DiskScheduler::DiskScheduler(DiskManager *disk_manager) : disk_manager_(disk_manager) {
  // TODO(P1): remove this line after you have implemented the disk scheduler API

  // Spawn the background thread
  background_thread_.emplace([&] { StartWorkerThread(); });
}

DiskScheduler::~DiskScheduler() {
  // Put a `std::nullopt` in the queue to signal to exit the loop
  request_queue_.Put(std::nullopt);
  if (background_thread_.has_value()) {
    background_thread_->join();
  }
}

void DiskScheduler::Schedule(DiskRequest r) { request_queue_.Put(std::optional<DiskRequest>(std::move(r))); }

void DiskScheduler::StartWorkerThread() {
  while (auto data = request_queue_.Get()) {
    if (data->is_write_) {
      disk_manager_->WritePage(data->page_id_, data->data_);
    } else {
      disk_manager_->ReadPage(data->page_id_, data->data_);
    }
    data->callback_.set_value(true);
  }
}

}  // namespace bustub
