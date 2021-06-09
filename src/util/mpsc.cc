// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/util/mpsc.h"

#include <thread> // std::this_thread::yield

namespace util {

bool MPSCQueue::Enqueue(MPSCNode* node) {
  node->set_next(MPSCNode::UnlinkedNode);
  // release sync with Dequeue
  MPSCNode* const prev_head = head_.exchange(node, std::memory_order_release);
  node->set_next(prev_head);
  return prev_head == nullptr;
}

bool MPSCQueue::Dequeue(MPSCNode* old_head, MPSCNode** new_iterate_tail) {
  assert(old_head->next() == nullptr);
  MPSCNode* new_head = old_head;
  // acquire sync with Enqueue
  if (head_.compare_exchange_strong(new_head, nullptr, std::memory_order_acquire)) {
    // No more nodes(head isn't changed), reset the head_ to nullptr.
    return false;
  }
  assert(new_head != old_head);
  // The current status may look like:
  // ### new_head--->...--->old_head<---...<---old_tail
  // ###                       |
  // ###                       v
  // ###                     nullptr
  //
  // after reversing the links between [new_head, old_head], it turns to:
  // ### new_head<---...<---old_head<---...<---old_tail
  // ###   |
  // ###   v
  // ### nullptr
  //
  // The purpose of reversing the link is to ensure the FIFO consumption order.
  // [old_tail, old_head] should have been iterated before this call.
  *new_iterate_tail = new_head;
  MPSCNode* cur = new_head;
  MPSCNode* prev = nullptr;
  while (cur != old_head) {
    while (cur->next() == MPSCNode::UnlinkedNode) {
      // The head_ has been exchanged, but the link has
      // not yet been established.
      std::this_thread::yield();
    }
    MPSCNode* next = cur->next();
    cur->set_next(prev);
    prev = cur;
    cur = next;
  }
  old_head->set_next(prev);
  return true;
}

MPSCQueue::Iterator& MPSCQueue::Iterator::operator++() {
  // mark the cur_ to be visited, step to next
  if (cur_ == nullptr) {
    return *this;
  }
  if (!cur_->visited()) {
    cur_->set_visited(true);
  }
  cur_ = cur_->next();
  return *this;
}
//
//void MPSCQueueConsumer::Run(void* arg) {
//  MPSCQueueConsumer* consumer = static_cast<MPSCQueueConsumer*>(arg);
//  ResourceGuard<MPSCQueueConsumer> guard(consumer);
//  consumer->Consume();
//}

bool MPSCQueueConsumer::Consume() {
  // It's not the first time to consume the queue
  if (iterate_tail_ != nullptr) {
    assert(iterate_tail_ == iterate_head_);
    bool more_nodes = queue_->Dequeue(iterate_tail_/*the old_head in queue*/, &iterate_tail_);
    if (!more_nodes) {
      delete iterate_head_;
      return false;
    }
  }
  uint64_t iterated_num = 0;
  while (true) {
    if (iterate_head_->visited()) {
      MPSCNode* next = iterate_head_->next();
      delete iterate_head_;
      iterate_head_ = next;
    }
    // Iterate the nodes in range: [iterate_head_, iterate_tail_]
    iterated_num += ExecuteFunction(iterate_head_);
    // Release the nodes in range: [iterate_head_, iterate_tail_)
    while (iterate_head_->next() != nullptr) {
      MPSCNode* next = iterate_head_->next();
      delete iterate_head_;
      iterate_head_ = next;
    }
    if (iterate_tail_ == nullptr) {
      // This is the first iteration,
      // which means iterate_tail_ should equals to iterate_head_.
      //
      // The begin status must be:
      // ### iterate_head_->nullptr
      iterate_tail_ = iterate_head_;
    }
    if (iterated_num >= max_consumed_number_) {
      // iterate_head_ and iterate_tail_ should both point to the last
      // iterated element when reach here.
      return true;
    }
    // Adjust the iterate_tail_ to the end position for the next iteration
    bool more_nodes = queue_->Dequeue(iterate_tail_/*the old_head in queue*/, &iterate_tail_);
    if (!more_nodes) {
      delete iterate_head_;
      return false;
    }
    // The next iteration range: [iterate_head_, iterate_tail_]
    //
    // NOTE: iterate_head_ is the tail of the last iteration,
    // we should skip it in next iteration.
  }
  return false;
}

uint64_t MPSCQueueConsumer::ExecuteFunction(MPSCNode* iterate_head_) {
  MPSCQueue::Iterator iterator(iterate_head_);
  return func_(func_arg_, iterator);
}

} // namespace util
