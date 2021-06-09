// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/util/thread_pool.h"

#include <sys/time.h>

#include "include/util/callbacks.h"

namespace util {

namespace thread {

void TimerTask::UpdateExecuteTime() {
  struct timeval now;
  gettimeofday(&now, nullptr);
  exec_time_us_ = now.tv_sec * 1000000 + now.tv_usec + timeout_ms_ * 1000;
}

bool CancellableTask::Cancel() {
  uint8_t expected_state = static_cast<uint8_t>(State::kWaiting);
  return state_.compare_exchange_strong(expected_state,
                                        static_cast<uint8_t>(State::kExecuted),
                                        std::memory_order_acquire);
}

void CancellableTask::Run() {
  uint8_t expected_state = static_cast<uint8_t>(State::kWaiting);
  if (state_.compare_exchange_strong(expected_state,
                                     static_cast<uint8_t>(State::kExecuting),
                                     std::memory_order_relaxed)) {
    run();
    state_.store(static_cast<uint8_t>(State::kExecuted),
                 std::memory_order_release);
    return;
  }
  // has been cancelled.
  if (expected_state == static_cast<uint8_t>(State::kExecuted)) {
    return;
  }
}

ThreadPool::ThreadPool(size_t thread_num, size_t max_queue_size)
  : thread_num_(thread_num),
  max_queue_size_(max_queue_size),
  started_(false), stopped_(true),
  wait_for_tasks_(false),
  should_stop_(false) { }

ThreadPool::~ThreadPool() {
  Stop(false);
}

struct WrapperArg {
  ThreadPool* tp;
  size_t thread_id;
  WrapperArg(ThreadPool* _tp, size_t _thread_id)
    : tp(_tp), thread_id(_thread_id) {}
};

int ThreadPool::Start() {
  // We use acquire order to synchronize-with the release order store in
  // the following cases to ensure that prior critical sections
  // happen-before this critical section:
  //
  // 1) the release operation in Stop() method makes sure
  //    that the thread_pool has been stopped.
  // 2) the release operation in the following makes sure
  //    that the thread_pool won't be started multiple times.
  if (started_.load(std::memory_order_acquire)) {
    return kStartErr;
  }
  {
    std::unique_lock<std::mutex> lk(mu_);
    if (started_.load(std::memory_order_acquire)) {
      return kStartErr;
    }

    for (size_t i = 0; i < thread_num_; i++) {
      std::thread t(&ThreadMainWrapper, new WrapperArg(this, i));
      threads_.push_back(std::move(t));
    }
    started_.store(true, std::memory_order_release);
    stopped_.store(false, std::memory_order_release);
  }
  return kStartOK;
}

int ThreadPool::Stop(bool wait_for_tasks) {
  // We use acquire order to synchronize-with the release order store in
  // the following cases to ensure that prior critical sections
  // happen-before this critical section:
  //
  // 1) the release operation in Start() method makes sure
  //    that the thread_pool has been started.
  // 2) the release operation in following makes sure
  //    that the thread_pool won't be stopped multiple times.
  if (stopped_.load(std::memory_order_acquire)) {
    return kStopErr;
  }
  {
    std::unique_lock<std::mutex> lk(mu_);
    if (stopped_.load(std::memory_order_acquire)) {
      return kStopErr;
    }

    should_stop_ = true;
    wait_for_tasks_ = wait_for_tasks;

    stopped_.store(true, std::memory_order_release);
  }

  /*started_ is true, stopped_ is true*/

  // Only one thread invoke Stop() method can reach here,
  signal_.notify_all();

  for (auto& thread : threads_) {
    thread.join();
  }

  {
    std::unique_lock<std::mutex> lk(mu_);
    threads_.clear();

    should_stop_ = false;
    wait_for_tasks_ = false;

    // Before we have stopped the thread_pool, it
    // should not be started again.
    started_.store(false, std::memory_order_release);
  }
  return kStopOK;
}

class FunctionWrapper : public Task {
 public:
  explicit FunctionWrapper(Func&& func)
    : func_(std::move(func)) { }

  virtual void Run() override {
    func_();
  }

 private:
  Func func_;
};

class TimerFunctionWrapper : public TimerTask {
 public:
  TimerFunctionWrapper(Func&& func, const uint64_t delay_time_ms)
    : TimerTask(delay_time_ms) ,func_(std::move(func)) {}

 private:
  virtual void run() override {
    func_();
  }
  Func func_;
};

int ThreadPool::Schedule(void (*f) (void*), void* arg) {
  auto func = [f, arg] { f(arg); };
  return ScheduleFunction(std::move(func));
}

int ThreadPool::DelaySchedule(void (*f) (void*), void* arg, uint64_t delay_time_ms) {
  auto func = [f, arg] { f(arg); };
  auto timer_task = std::make_shared<TimerFunctionWrapper>(std::move(func), delay_time_ms);
  return ScheduleTimerTask(timer_task);
}

int ThreadPool::ScheduleFunction(const Func& func) {
  auto func_copy = func;
  return ScheduleFunction(std::move(func_copy));
}

int ThreadPool::ScheduleFunction(Func&& func) {
  auto task = std::make_shared<FunctionWrapper>(std::move(func));
  return ScheduleTask(task);
}

int ThreadPool::ScheduleTask(std::shared_ptr<Task> task) {
  std::unique_lock<std::mutex> lk(mu_);
  if (should_stop_) {
    return kScheErr;
  }
  if (queue_.size() >= max_queue_size_) {
    return kScheBusy;
  }
  queue_.push(std::move(task));
  signal_.notify_one();
  return kScheOK;
}

int ThreadPool::ScheduleTimerTask(std::shared_ptr<TimerTask> timer_task) {
  timer_task->UpdateExecuteTime();

  std::unique_lock<std::mutex> lk(mu_);
  if (should_stop_) {
    return kScheErr;
  }
  if (timer_queue_.size() >= max_queue_size_) {
    return kScheBusy;
  }
  timer_queue_.push(std::move(timer_task));
  signal_.notify_one();
  return kScheOK;
}

void ThreadPool::ThreadMainWrapper(void* arg) {
  auto wrapper_arg = static_cast<WrapperArg*>(arg);
  ResourceGuard<WrapperArg> guard(wrapper_arg);
  wrapper_arg->tp->ThreadMain(wrapper_arg->thread_id);
}

void ThreadPool::ThreadMain(size_t thread_id) {
  while (true) {
    // wait until:
    // 1) queue_ is not empty
    // or 2) timer_queue_ is not empty
    // or 3) should_stop_ is true
    std::unique_lock<std::mutex> lk(mu_);
    while (!should_stop_ && queue_.empty() && timer_queue_.empty()) {
      signal_.wait(lk);
    }

    if (should_stop_) {
      // When the wait_for_tasks_ is turned on,
      // wait for all tasks in the queue to finish executing.
      if (!wait_for_tasks_ || queue_.empty() || timer_queue_.empty()) {
        break;
      }
    }

    // 1. Do the cron tasks
    if (!timer_queue_.empty()) {
      struct timeval now;
      gettimeofday(&now, nullptr);
      uint64_t unow = now.tv_sec * 1000000 + now.tv_usec;
      auto timer_task = timer_queue_.top();
      if (unow >= timer_task->exec_time_us()) {
        timer_queue_.pop();
        lk.unlock();

        timer_task->Run(); // run outof lock
        continue;
      } else if (!should_stop_ && queue_.empty()) {
        std::chrono::microseconds t(timer_task->exec_time_us() - unow);
        signal_.wait_for(lk, t);
        continue;
      }
    }

    // 2. Do the normal tasks
    if (!queue_.empty()) {
      auto task = queue_.front();
      queue_.pop();
      lk.unlock();

      task->Run(); // run outof lock
    }
  }
}

} // namespace thread

} // namespace util
