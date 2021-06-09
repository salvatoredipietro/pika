// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_INCLUDE_UTIL_THREAD_POOL_H
#define PIKA_INCLUDE_UTIL_THREAD_POOL_H

#include <atomic>
#include <thread>
#include <memory>
#include <vector>
#include <queue>
#include <limits>
#include <functional>
#include <condition_variable>

namespace util {

namespace thread {

enum ThreadPoolStartRet {
  kStartOK    = 0,
  kStartErr   = 1,
};

enum ThreadPoolStopRet {
  kStopOK    = 0,
  kStopErr   = 1,
};

enum ThreadPoolScheRet {
  kScheOK    = 0,
  kScheErr   = 1,
  kScheBusy  = 2,
};

using Func = std::function<void()>;

class Task : public std::enable_shared_from_this<Task> {
 public:
  virtual ~Task() = default;

  virtual void Run() = 0;
};

class CancellableTask : public Task {
 public:
  CancellableTask() : state_(static_cast<uint8_t>(State::kWaiting)) {}
  virtual ~CancellableTask() = default;

  // Try to cancel the pending task.
  // Returns a boolean to indicate whether
  // the cancellation was successful.
  bool Cancel();

  // Derived classes should override the run() method
  // to customize the runtime behavior instead.
  virtual void Run() final override;

  bool IsWaiting() {
    return state_.load(std::memory_order_relaxed) == static_cast<uint8_t>(State::kWaiting);
  }
  bool IsExecuting() {
    return state_.load(std::memory_order_relaxed) == static_cast<uint8_t>(State::kExecuting);
  }
  bool IsExecuted() {
    return state_.load(std::memory_order_relaxed) == static_cast<uint8_t>(State::kExecuted);
  }

 protected:
  virtual void run() = 0;
  // 0 : is waiting to be executed.
  // 1 : is being executed.
  // 2 : has been executed.
  enum class State : uint8_t {
    kWaiting   = 0,
    kExecuting = 1,
    kExecuted  = 2,
  };
  std::atomic<uint8_t> state_;
};

class TimerTask : public CancellableTask {
 public:
  TimerTask(uint64_t timeout_ms)
    : exec_time_us_(0), timeout_ms_(timeout_ms) { }
  virtual ~TimerTask() = default;

  uint64_t timeout_ms() const { return timeout_ms_; }
  void set_timeout_ms(uint64_t timeout_ms) {
    timeout_ms_ = timeout_ms;
  }

  uint64_t exec_time_us() const { return exec_time_us_; }
  void UpdateExecuteTime();

  bool operator > (const TimerTask& task) const {
    return exec_time_us_ > task.exec_time_us_;
  }
  bool operator < (const TimerTask& task) const {
    return exec_time_us_ < task.exec_time_us_;
  }

 protected:
  uint64_t exec_time_us_;
  uint64_t timeout_ms_;
};

class ThreadPool {
 public:
  constexpr static size_t kDefaultThreadNum = 1;
  constexpr static size_t kDefaultMaxQueueSize = std::numeric_limits<size_t>::max();
  explicit ThreadPool(size_t thread_num, size_t max_queue_size = kDefaultMaxQueueSize);
  ~ThreadPool();

  int Start();
  int Stop(bool wait_for_tasks = false);

  int Schedule(void (*f) (void*), void* arg);
  int DelaySchedule(void (*f) (void*), void* arg, uint64_t delay_time_ms);
  int ScheduleFunction(const Func& func);
  int ScheduleFunction(Func&& func);
  int ScheduleTask(std::shared_ptr<Task> task);
  int ScheduleTimerTask(std::shared_ptr<TimerTask> timer_task);
 
 private:
  static void ThreadMainWrapper(void* arg);
  void ThreadMain(size_t thread_id);

  struct TimerTaskCompare
  {
    bool operator()(const std::shared_ptr<TimerTask>& lhs,
                    const std::shared_ptr<TimerTask>& rhs) {
      return *lhs > *rhs;
    }
  };

  size_t thread_num_;
  size_t max_queue_size_;

  std::mutex mu_;
  std::atomic<bool> started_;
  std::atomic<bool> stopped_;
  bool wait_for_tasks_;
  bool should_stop_;
  std::condition_variable signal_;
  std::queue<std::shared_ptr<Task>> queue_;
  std::priority_queue<std::shared_ptr<TimerTask>,
                      std::vector<std::shared_ptr<TimerTask>>,
                      TimerTaskCompare> timer_queue_;
  std::vector<std::thread> threads_;
};

} // nemspace thread

} // namespace util

#endif // PIKA_INCLUDE_UTIL_THREAD_POOL_H
