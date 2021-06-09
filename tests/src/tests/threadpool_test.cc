// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <memory>

#include "gtest/gtest.h"
#include "gmock/gmock.h"

#include "slash/include/env.h"
#include "slash/include/slash_status.h"

#include "include/util/hooks.h"
#include "include/util/thread_pool.h"
#include "include/deps/common_env.h"
#include "include/util/replication_util.h"

namespace test {

using slash::Status;

class TestThreadPool
  : public ::testing::Test,
    public ::testing::WithParamInterface<int> {
 protected:
  TestThreadPool() : thread_pool_size_(0), thread_pool_(nullptr) {}
  void SetUp() override {
    thread_pool_size_ = GetParam();
  }
  void TearDown() override { }

 protected:
  int thread_pool_size_;
  std::unique_ptr<util::thread::ThreadPool> thread_pool_;
};

INSTANTIATE_TEST_SUITE_P(DifferentThreadPoolSize, TestThreadPool, ::testing::Values(1, 2, 5));

TEST_P(TestThreadPool, Restart) {
  thread_pool_ = util::make_unique<util::thread::ThreadPool>(thread_pool_size_);
  auto ret = thread_pool_->Start();
  ASSERT_EQ(ret, util::thread::kStartOK);

  ret = thread_pool_->Start();
  ASSERT_EQ(ret, util::thread::kStartErr);

  ret = thread_pool_->Stop(true);
  ASSERT_EQ(ret, util::thread::kStopOK);

  ret = thread_pool_->Stop(true);
  ASSERT_EQ(ret, util::thread::kStopErr);

  ret = thread_pool_->Start();
  ASSERT_EQ(ret, util::thread::kStartOK);
}

struct scheFuncArg {
  int cnt;
  Waiter waiter;
  scheFuncArg() : cnt(0) {} 
};

static void scheFunc(void* arg) {
  auto func_arg = static_cast<scheFuncArg*>(arg);
  func_arg->cnt++;
  func_arg->waiter.TryNotify(Status::OK());
}

TEST_P(TestThreadPool, ScheduleFunc) {
  constexpr int kInitializeCnt = 0;
  thread_pool_ = util::make_unique<util::thread::ThreadPool>(thread_pool_size_);
  auto ret = thread_pool_->Start();
  ASSERT_EQ(ret, util::thread::kStartOK);

  scheFuncArg func_arg;
  func_arg.cnt = kInitializeCnt;
  func_arg.waiter.Reset(1);
  thread_pool_->Schedule(&scheFunc, &func_arg);
  func_arg.waiter.Wait();
  ASSERT_NE(func_arg.cnt, kInitializeCnt);
}

class CntStep {
 public:
  CntStep(const int cnt, int step)
    : cnt_(cnt), step_(step) { }

  void step() { cnt_ += step_; }
  int cnt() const { return cnt_; }

 private:
  int cnt_;
  int step_;
};

class StepCntTask
  : public CntStep,
    public util::thread::Task {
 public:
  StepCntTask(const int cnt, int step)
    : CntStep(cnt, step), waiter_(1) { }

  void wait() {
    waiter_.Wait();
  }
  virtual void Run() override {
    step();
    waiter_.TryNotify(Status::OK());
  }

 private:
  Waiter waiter_;
};

TEST_P(TestThreadPool, ScheduleTask) {
  constexpr int kInitializeCnt = 0;
  constexpr int kStep = 10;
  thread_pool_ = util::make_unique<util::thread::ThreadPool>(thread_pool_size_);
  auto ret = thread_pool_->Start();
  ASSERT_EQ(ret, util::thread::kStartOK);

  auto sche_task = std::make_shared<StepCntTask>(kInitializeCnt, kStep);
  thread_pool_->ScheduleTask(sche_task);
  sche_task->wait();

  ASSERT_EQ(sche_task->cnt(), kInitializeCnt + kStep);
}

class StepCntTimerTask
  : public CntStep,
    public util::thread::TimerTask {
 public:
  StepCntTimerTask(const int cnt, int step, uint64_t timeout_ms);
  void wait() {
    waiter_.Wait();
  }
  virtual void run() override {
    step();
    end_time_ms_ = slash::NowMicros()/1000;
    waiter_.TryNotify(Status::OK());
  }
  uint64_t start_time_ms() const { return start_time_ms_; }
  uint64_t end_time_ms() const { return end_time_ms_; }

 private:
  uint64_t start_time_ms_;
  uint64_t end_time_ms_;
  Waiter waiter_;
};

StepCntTimerTask::StepCntTimerTask(const int cnt, int step, uint64_t timeout_ms)
  : CntStep(cnt, step), util::thread::TimerTask(timeout_ms),
  start_time_ms_(0), end_time_ms_(0), waiter_(1) {
  start_time_ms_ = slash::NowMicros()/1000;
}

TEST_P(TestThreadPool, ScheduleTimerTask) {
  constexpr int kInitializeCnt = 0;
  constexpr int kStep = 10;
  constexpr int kDelayTimems = 2000;
  thread_pool_ = util::make_unique<util::thread::ThreadPool>(thread_pool_size_);
  auto ret = thread_pool_->Start();
  ASSERT_EQ(ret, util::thread::kStartOK);

  auto sche_task = std::make_shared<StepCntTimerTask>(kInitializeCnt, kStep, kDelayTimems);
  thread_pool_->ScheduleTask(sche_task);
  sche_task->wait();

  ASSERT_EQ(sche_task->cnt(), kInitializeCnt + kStep);
  ASSERT_LT((sche_task->end_time_ms() - sche_task->start_time_ms()), kDelayTimems);
}

} // namespace test

int main(int argc, char** argv) {
  ::testing::AddGlobalTestEnvironment(new test::Env(argc, argv));
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
