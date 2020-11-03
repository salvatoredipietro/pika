// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_INCLUDE_UTIL_TIMER_H
#define PIKA_INCLUDE_UTIL_TIMER_H

#include <string>
#include <mutex>
#include <memory>

#include "include/util/thread_pool.h"
#include "include/util/task_executor.h"

namespace util {

// Timer is a wrapper of TimerTask
class Timer : public std::enable_shared_from_this<Timer> {
 public:
  Timer();
  virtual ~Timer() = default;
  void Initialize(thread::ThreadPool* timer_thread,
                  TaskExecutor* executor,
                  uint64_t timeout_ms,
                  bool repeated);
  // Start the timer. If the timer has been stopped, nothing will change.
  // Otherwise, the timer task will be scheduled if there is no one.
  void Start();
  // Restart schedule the task if it has not been scheduled.
  // There must be a ongoing timer task after Restart.
  //
  // The difference between Start and Restart is: Restart will
  // start the timer if it has been stopped, and Reset the timer
  // task if there is one.
  void Restart();
  // Stop the timer, and the ongoing task will be canceled.
  void Stop();

  // Cancel the ongoing task, and restart a new task
  void Reset();
  // Update the timeout_ms_ to timeout_ms, and invoke Reset()
  void Reset(uint64_t timeout_ms);

 protected:
  virtual void run() = 0;
  virtual uint64_t AdjustTimeout(uint64_t timeout_ms) { return timeout_ms; }

 private:
  friend class TimerTaskImpl;
  friend class TimedoutHandler;
  void UnsafeReset();
  // Invoked in thread_ when the task timedout.
  static void timedout(void* arg);
  // Call the related timedout function.
  static void RunTimedout(void* arg);
  void RunTimedout();
  void ScheduleTimerTask();

  // Thread for listening to timeout task.
  thread::ThreadPool* timer_thread_;
  // Context for processing the task when timedout
  // to avoid blocking other tasks.
  TaskExecutor* executor_;

  // mu_ protected the following fields.
  std::mutex mu_;
  // The task will be automatically scheduled again
  // once the run() is completed if repeated_ is true.
  bool repeated_;
  // indicate if the timer has been stopped.
  bool stopped_;
  // indicate if the task_ has been scheduled.
  bool scheduled_;
  std::shared_ptr<thread::TimerTask> task_;
  // The delay time to excute the task_
  uint64_t timeout_ms_;
};

} // namespace util

#endif // PIKA_INCLUDE_UTIL_TIMER_H
