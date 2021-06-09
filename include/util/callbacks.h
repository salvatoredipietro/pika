// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_INCLUDE_UTIL_CALLBACKS_H_
#define PIKA_INCLUDE_UTIL_CALLBACKS_H_

#include <string>

#include "slash/include/slash_status.h"

namespace util {

using slash::Status;

// RAII of resource
template <typename Resource>
class ResourceGuard {
 public:
  ResourceGuard() : res_(nullptr) { }
  explicit ResourceGuard(Resource* res) : res_(res) { }
  ~ResourceGuard() {
    if (res_ != nullptr) {
      delete res_;
      res_ = nullptr;
    }
  }
  ResourceGuard(const ResourceGuard&) = delete;
  void operator=(const ResourceGuard&) = delete;

  void Reset(Resource* res) {
    if (res_ != nullptr) {
      delete res_;
      res_ = nullptr;
    }
    res_ = res;
  }
  Resource* Release() {
    Resource* cur = res_;
    res_ = nullptr;
    return cur;
  }
 private:
  Resource* res_;
};

// Self-deleting closure, the resources will be freed automatically
// when the Run/Release method has been invoked.
class Closure {
 public:
  Closure() : status_(Status::OK()) { }
  Closure(const Closure&) = delete;
  Closure& operator=(const Closure&) = delete;

  Status status() const { return status_; }
  void set_status(const Status& status) { status_ = status; }

  void Run() {
    run();
    Release();
  }
  void Release() { delete this; }

 protected:
  // Must be instantiated dynamically, and in order to prevent
  // double free as we are self-deleting.
  //
  // NOTE: All derived classes inherits this should keep the same
  // to prevent double free.
  virtual ~Closure() = default;
  virtual void run() = 0;
  Status status_;
};

// The closure_->Run will be invoked automatically when the guard ends its lifetime.
class ClosureGuard {
 public:
  ClosureGuard() : closure_(nullptr) { }
  explicit ClosureGuard(Closure* closure) : closure_(closure) { }

  ~ClosureGuard() {
    if (closure_ != nullptr) {
      closure_->Run();
      closure_ = nullptr;
    }
  }

  void Reset(Closure* closure) {
    if (closure_ != nullptr) {
      closure_->Run();
      closure_ = nullptr;
    }
    closure_ = closure;
  }

  Closure* Release() {
    Closure* cur_closure = closure_;
    closure_ = nullptr;
    return cur_closure;
  }

 private:
  Closure* closure_;
};

} // namespace util

#endif // PIKA_INCLUDE_UTIL_CALLBACKS_H_
