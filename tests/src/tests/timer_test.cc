// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <unistd.h>
#include <atomic>
#include <mutex>
#include <condition_variable>

#include "gtest/gtest.h"
#include "gmock/gmock.h"

#include "include/deps/common_env.h"
#include "include/util/callbacks.h"
#include "include/util/thread_pool.h"
#include "include/util/timer.h"

namespace test {

using namespace util;

class TimerTest : public ::testing::Test {
 protected:
  TimerTest() : timer_thread_(1) {}
  ~TimerTest() = default;

  void SetUp() override {
    timer_thread_.Start();
  }

  void TearDown() override {
    timer_thread_.Stop(false);
  }

  thread::ThreadPool timer_thread_;
};

class RepeatedCountTimer : public Timer {
 public:
  RepeatedCountTimer(const std::string& id)
    : id_(id), count_(0) {}

  uint32_t count() { return count_.load(std::memory_order_relaxed); }

 protected:
  virtual void run() override {
    count_.fetch_add(1, std::memory_order_relaxed);
  }

 private:
  const std::string id_;
  std::atomic<uint32_t> count_;
};

struct NotifyTimerTask : public thread::TimerTask {
 public:
  NotifyTimerTask(const uint64_t timeout_ms)
    : thread::TimerTask(timeout_ms), wnotified_(false),
    rnotified_(false), executed_(false) {}

  void NotifyRun() {
    std::lock_guard<std::mutex> lk(mu_);
    wnotified_.store(true, std::memory_order_release);
    wsignal_.notify_one();
  }

  void WaitExecuted() {
    std::unique_lock<std::mutex> lk(mu_);
    rsignal_.wait(lk, [this] {
        return rnotified_.load(std::memory_order_acquire); });
  }
  
  virtual void run() override {

    std::unique_lock<std::mutex> lk(mu_);
    wsignal_.wait(lk, [this] {
        return wnotified_.load(std::memory_order_acquire); });
    executed_.store(true, std::memory_order_release);

    rnotified_.store(true, std::memory_order_release);
    rsignal_.notify_one();
  }

  bool IsExecuted() {
    return executed_.load(std::memory_order_acquire);
  }

 private:
  std::mutex mu_;
  std::atomic<bool> wnotified_;
  std::atomic<bool> rnotified_;
  std::atomic<bool> executed_;
  std::condition_variable wsignal_;
  std::condition_variable rsignal_;
};

TEST_F(TimerTest, ScheduleTimerTask) {
  const uint64_t timeout_ms = 1000; /*1s*/
  const auto& notify_timer_task = std::make_shared<NotifyTimerTask>(timeout_ms);
  timer_thread_.ScheduleTimerTask(notify_timer_task);
  ASSERT_TRUE(notify_timer_task->IsWaiting());

  // wait timeout
  usleep(500 * 1000/*0.5s*/);
  ASSERT_TRUE(notify_timer_task->IsWaiting());

  // timedout, wait notified
  usleep(1500 * 1000/*1.5s*/);
  ASSERT_TRUE(notify_timer_task->IsExecuting());

  // notified, wait executed
  notify_timer_task->NotifyRun();
  usleep(500 * 1000/*0.5s*/);
  ASSERT_TRUE(notify_timer_task->IsExecuted());
  ASSERT_TRUE(notify_timer_task->IsExecuted());

  notify_timer_task->WaitExecuted();
}

TEST_F(TimerTest, CancelTimerTask) {
  const uint64_t timeout_ms = 1000; /*1s*/
  const auto& notify_timer_task = std::make_shared<NotifyTimerTask>(timeout_ms);
  timer_thread_.ScheduleTimerTask(notify_timer_task);
  ASSERT_TRUE(notify_timer_task->IsWaiting());

  // wait timeout
  usleep(500 * 1000/*0.5s*/);
  ASSERT_TRUE(notify_timer_task->IsWaiting());

  // cancel
  notify_timer_task->Cancel();

  notify_timer_task->NotifyRun();
  usleep(1000 * 1000/*1s*/);

  // timedout, notified, but canceled
  ASSERT_FALSE(notify_timer_task->IsExecuted());
}

TEST_F(TimerTest, TimerStartAndStop) {
  auto timer = std::make_shared<RepeatedCountTimer>("test_timer");
  const uint64_t init_timeout_ms  = 100; /*100 ms*/
  const uint32_t before_start_count = timer->count();
  timer->Initialize(&timer_thread_, nullptr, init_timeout_ms, true/*repeated*/);
  timer->Start();

  uint64_t sleep_us = init_timeout_ms * 10 * 1000; /*10 times of init_timeout_ms*/

  usleep(sleep_us);
  const uint32_t delta_1 = timer->count() - before_start_count;

  // 1. Reset timeout_ms
  const uint64_t reset_timeout_ms = init_timeout_ms << 1; /*2 times of init_timeout_ms*/
  timer->Reset(reset_timeout_ms);

  usleep(sleep_us);
  const uint32_t delta_2 = timer->count() - delta_1 - before_start_count;

  ASSERT_TRUE(delta_2 < delta_1);

  // 2. Stop timer_task
  timer->Stop();
  const uint32_t after_stop_count = timer->count();
  usleep(sleep_us);
  const uint32_t after_stop_a_while_count = timer->count();
  ASSERT_TRUE(after_stop_a_while_count <= after_stop_count + 1 /*concurrent executing with Stop() method*/);
  usleep(sleep_us);
  ASSERT_EQ(timer->count(), after_stop_a_while_count);

  // 3. Restart timer_task
  const uint32_t before_restart_count = timer->count();
  timer->Restart();
  usleep(sleep_us);
  const uint32_t delta_3 = timer->count() - before_restart_count;
  ASSERT_TRUE(delta_3 >= 1);
}

TEST_F(TimerTest, TimerRepeated) {
  auto timer = std::make_shared<RepeatedCountTimer>("test_timer");
  const uint64_t init_timeout_ms  = 100; /*100 ms*/
  timer->Initialize(&timer_thread_, nullptr, init_timeout_ms, true/*repeated*/);
  timer->Start();

  sleep(10); /*10s*/

  ASSERT_GT(timer->count(), 10 * 9);
}

TEST_F(TimerTest, MultiTimerRepeated) {
  auto timer_1 = std::make_shared<RepeatedCountTimer>("t1");
  auto timer_2 = std::make_shared<RepeatedCountTimer>("t2");
  const uint64_t timer_1_init_timeout_ms  = 100; /*100 ms*/
  const uint64_t timer_2_init_timeout_ms  = 1000; /*1000 ms*/
  timer_1->Initialize(&timer_thread_, nullptr, timer_1_init_timeout_ms,
                      true/*repeated*/);
  timer_2->Initialize(&timer_thread_, nullptr, timer_2_init_timeout_ms,
                      true/*repeated*/);
  timer_1->Start();
  timer_2->Start();

  sleep(10); /*10s*/

  ASSERT_GT(timer_2->count(), (1000/timer_2_init_timeout_ms) * 8);
  ASSERT_GT(timer_1->count(), (1000/timer_1_init_timeout_ms) * 8);
}

}  // namespace test

int main(int argc, char** argv) {
  ::testing::AddGlobalTestEnvironment(new test::Env(argc, argv));
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
