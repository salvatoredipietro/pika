// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_INCLUDE_UTIL_MPSC_H_
#define PIKA_INCLUDE_UTIL_MPSC_H_

#include <atomic>

#include "include/util/callbacks.h"

namespace util {

class MPSCNode {
 public:
  MPSCNode() : next_(nullptr), visited_(false) { }
  virtual ~MPSCNode() = default;

  MPSCNode(const MPSCNode&) = delete;
  MPSCNode& operator=(const MPSCNode&) = delete;

  virtual void Run() { }

  bool visited() const { return visited_; }
  void set_visited(const bool visited) { visited_ = visited; }

  MPSCNode* next() const { return next_; }
  void set_next(MPSCNode* const next) { next_ = next; }

  constexpr static MPSCNode* UnlinkedNode = reinterpret_cast<MPSCNode*>(-1);

 private:
  MPSCNode* next_;
  bool visited_;
};

class MPSCQueue {
 public:
  MPSCQueue() : head_(nullptr) {}

  MPSCQueue(const MPSCQueue&) = delete;
  MPSCQueue& operator=(const MPSCQueue&) = delete;

  // return true when the queue is empty before current operation.
  //
  // NOTE: caller should wait all pending nodes visited before destruct.
  //       A stop node can be enqueued into the queue.
  bool Enqueue(MPSCNode* node);
  bool Dequeue(MPSCNode* old_head, MPSCNode** new_iterate_tail);

  class Iterator {
   public:
    Iterator(MPSCNode* head)
      : cur_(head) {}
    ~Iterator() = default;
 
    MPSCNode& operator*() const { return *cur_; }
    MPSCNode* operator->() const { return cur_; }
    MPSCNode* Current() { return cur_; }
    void operator++(int) {
      operator++();
    }
    Iterator& operator++();
    bool Valid() const { return cur_ != nullptr && !cur_->visited(); }
 
   private:
    MPSCNode* cur_;
  };

 private:
  // head_ indicate the newest node
  std::atomic<MPSCNode*> head_;
};

class MPSCQueueConsumer {
 public:
  using MPSCFunction = uint64_t (*) (void* arg, MPSCQueue::Iterator&);
  MPSCQueueConsumer(MPSCFunction func, void* func_arg,
                    MPSCQueue* queue, MPSCNode* head,
                    uint64_t max_consumed_number)
    : func_(func), func_arg_(func_arg), max_consumed_number_(max_consumed_number),
    queue_(queue), iterate_head_(head), iterate_tail_(nullptr) {}

  MPSCQueueConsumer(const MPSCQueueConsumer&) = delete;
  MPSCQueueConsumer& operator=(const MPSCQueueConsumer&) = delete;

  // Consume the queue until there are no more pending nodes
  // or reach the number limit indicated by max_consumed_number_.
  //
  // @return bool: return true to indicate that the current loop
  //               has reached the number limit. Otherwise, it
  //               indicates that there are no more noods to iterate
  //               through in the queue right now.
  bool Consume();

 private:
  // Execute the MPSCNode one by one through iterator,
  // return the iterated count.
  uint64_t ExecuteFunction(MPSCNode* head);

  MPSCFunction func_;
  void* func_arg_;
  uint64_t max_consumed_number_;
  MPSCQueue* queue_;
  // point to the node which should be consumed first.
  MPSCNode* iterate_head_;
  // point to the last consumed node..
  MPSCNode* iterate_tail_;
};

} // namespace util

#endif // PIKA_INCLUDE_UTIL_MPSC_H_
