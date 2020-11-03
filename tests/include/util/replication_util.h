// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef INCLUDE_UTIL_REPLICATION_UTIL_H_
#define INCLUDE_UTIL_REPLICATION_UTIL_H_

#include <mutex>
#include <memory>
#include <chrono>
#include <glog/logging.h>
#include <condition_variable>

#include "slash/include/slash_status.h"

#include "include/util/callbacks.h"

namespace test {

using slash::Status;
using namespace util;

bool operator==(const Status& lhs, const Status& rhs) {
  if (lhs.ok() != rhs.ok()
      || lhs.IsNotFound() != rhs.IsNotFound()
      || lhs.IsCorruption() != rhs.IsCorruption()
      || lhs.IsNotSupported() != rhs.IsNotSupported()
      || lhs.IsInvalidArgument() != rhs.IsInvalidArgument()
      || lhs.IsIOError() != rhs.IsIOError()
      || lhs.IsEndFile() != rhs.IsEndFile()
      || lhs.IsIncomplete() != rhs.IsIncomplete()
      || lhs.IsComplete() != rhs.IsComplete()
      || lhs.IsTimeout() != rhs.IsTimeout()
      || lhs.IsAuthFailed() != rhs.IsAuthFailed()
      || lhs.IsBusy() != rhs.IsBusy()) {
    return false;
  }
  return true;
}

bool operator!=(const Status& lhs, const Status& rhs) {
  return !(lhs == rhs);
}

bool CheckDepEqual(const Status& lhs, const Status& rhs) {
  return lhs.ToString() == rhs.ToString();
}

class Waiter {
 public:
  Waiter()
    : status_(), target_(0), current_(0) { }
  Waiter(const int target)
    : status_(), target_(target), current_(0) {}
  void Reset(int new_target) {
    std::lock_guard<std::mutex> lk(mu_);
    target_ = new_target;
    current_ = 0;
  }
  Status status() {
    std::unique_lock<std::mutex> lk(mu_);
    return status_;
  }
  void TryNotify(const Status& status) {
    std::unique_lock<std::mutex> lk(mu_);
    current_++;
    status_ = status;
    LOG(INFO) << "current: " << current_ << ", target: " << target_; 
    if (!status_.ok() || current_ >= target_) {
      cond_.notify_one();
      LOG(INFO) << "notify " << current_ << ":" << target_ << ", status " << status_.ToString();
    }
  }
  void Wait() {
    std::unique_lock<std::mutex> lk(mu_);
    cond_.wait(lk, [this] { return target_ <= current_ || !status_.ok(); });
    LOG(INFO) << "wait " << current_ << ":" << target_ << ", status " << status_.ToString();
  }
  // Return true when condition is met, otherwise return false to indicate
  // timeout.
  bool WaitFor(const uint64_t wait_ms) {
    std::unique_lock<std::mutex> lk(mu_);
    auto condition_met = cond_.wait_for(lk, std::chrono::milliseconds(wait_ms),
                                   [this] { return target_ <= current_ || !status_.ok(); });
    LOG(INFO) << "wait " << current_ << ":" << target_ << ", status " << status_.ToString()
              << ", condition_met " << condition_met;
    return condition_met;
  }

 private:
  std::mutex mu_;
  Status status_;
  int target_;
  int current_;
  std::condition_variable cond_;
};

class EntryClosure : public Closure {
 public:
  EntryClosure(const slash::Status& expected_status, Waiter* waiter)
    : expected_status_(expected_status), waiter_(waiter) {}

 private:
  virtual ~EntryClosure() = default;
  virtual void run() {
    if (waiter_ == nullptr) return;
    Status s;
    if (!CheckDepEqual(expected_status_, status_)) {
      s = Status::Corruption("Expected: " + expected_status_.ToString()
                             + ", Actual: " + status_.ToString());
    }
    waiter_->TryNotify(s);
  }

  slash::Status expected_status_;
  Waiter* waiter_;
};

} // namespace test

#endif // INCLUDE_UTIL_REPLICATION_UTIL_H_
