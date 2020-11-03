// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/util/timer.h"

namespace util {

Timer::Timer()
  : timer_thread_(nullptr),
  executor_(nullptr),
  repeated_(false),
  stopped_(false),
  scheduled_(false),
  task_(nullptr),
  timeout_ms_(0) {
}

void Timer::Initialize(thread::ThreadPool* timer_thread,
                       TaskExecutor* executor,
                       uint64_t timeout_ms,
                       bool repeated) {
  std::unique_lock<std::mutex> lk(mu_);
  timer_thread_ = timer_thread;
  executor_ = executor;
  repeated_ = repeated;
  timeout_ms_ = timeout_ms;
}

void Timer::Start() {
  std::unique_lock<std::mutex> lk(mu_);
  if (stopped_) {
    return;
  }
  if (scheduled_) {
    return;
  }
  ScheduleTimerTask();
}

void Timer::Restart() {
  std::unique_lock<std::mutex> lk(mu_);
  stopped_ = false;
  if (!scheduled_) {
    // task has not been scheduled, schedule it.
    ScheduleTimerTask();
  } else if (task_ != nullptr) {
    // task has been scheduled, cancel it.
    if (task_->Cancel()) {
      // Cancel success, reschedule it.
      ScheduleTimerTask();
    }
  }
}

void Timer::Stop() {
  std::unique_lock<std::mutex> lk(mu_);
  if (stopped_) {
    return;
  }
  stopped_ = true;
  if (scheduled_ && task_ != nullptr) {
    if (task_->Cancel()) {
      // Cancel success
      scheduled_ = false;
    }
  }
}

void Timer::Reset() {
  std::unique_lock<std::mutex> lk(mu_);
  UnsafeReset();
}

void Timer::Reset(uint64_t timeout_ms) {
  std::unique_lock<std::mutex> lk(mu_);
  timeout_ms_ = timeout_ms;
  UnsafeReset();
}

// REQUIRES: mu_ must be held.
void Timer::UnsafeReset() {
  if (stopped_) {
    return;
  }
  if (scheduled_ && task_ != nullptr) {
    if (task_->Cancel()) {
      ScheduleTimerTask();
    }
  }
}

void Timer::RunTimedout() {
  // run the timedout function outof lock
  run();

  std::unique_lock<std::mutex> lk(mu_);
  task_ = nullptr;
  if (stopped_ || !repeated_) {
    scheduled_ = false;
    return;
  }
  ScheduleTimerTask();
}

class TimedoutHandler : public thread::Task {
 public:
  TimedoutHandler(std::shared_ptr<Timer> timer)
    : timer_(timer) {}

  virtual void Run() override;

 private:
  std::weak_ptr<Timer> timer_;
};

void TimedoutHandler::Run() {
  auto timer = timer_.lock();
  if (timer == nullptr) {
    // Timer has been destroyed.
    return;
  }
  timer->RunTimedout();
}

class TimerTaskImpl : public thread::TimerTask {
 public:
  TimerTaskImpl(std::shared_ptr<Timer> timer, const uint64_t delay_time_ms)
    : TimerTask(delay_time_ms), timer_(timer) {}

 private:
  virtual void run() override;

  std::weak_ptr<Timer> timer_;
};

void TimerTaskImpl::run() {
  auto timer = timer_.lock();
  if (timer == nullptr) {
    // Timer has been destroyed.
    return;
  }
  if (timer->executor_ != nullptr) {
    // Call the timedout function in executor_.
    auto timedout_handler = std::make_shared<TimedoutHandler>(timer);
    timer->executor_->ScheduleTask(std::static_pointer_cast<thread::Task>(timedout_handler));
    return;
  }
  // Call the timedout function in timer_thread_(will block other tasks).
  timer->RunTimedout();
}

// REQUIRES: mu_ must be held
void Timer::ScheduleTimerTask() {
  if (timer_thread_ == nullptr) {
    return;
  }
  task_ = std::make_shared<TimerTaskImpl>(shared_from_this(), timeout_ms_);
  task_->set_timeout_ms(AdjustTimeout(timeout_ms_));
  timer_thread_->ScheduleTimerTask(std::static_pointer_cast<thread::TimerTask>(task_));
  scheduled_ = true;
}

} // namespace util
