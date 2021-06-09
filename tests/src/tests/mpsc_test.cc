// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <unistd.h>
#include <mutex>
#include <thread>
#include <algorithm>
#include <vector>

#include "gtest/gtest.h"
#include "gmock/gmock.h"

#include "include/deps/common_env.h"
#include "include/util/mpsc.h"
#include "include/util/hooks.h"
#include "include/util/random.h"
#include "include/util/callbacks.h"

namespace test {

using namespace util;

class MPSCEnv: public Env {
 public:
  constexpr static int kDefaultQueueSize = 1024*1024;
  constexpr static int kDefaultProducerNumber = 16;
  MPSCEnv(int argc, char** argv);

  int queue_size() const { return queue_size_; }
  int producer_number() const { return producer_number_; }

 private:
  int queue_size_;
  int producer_number_;
};

const std::string kQueueSize = "--queue_size";
const std::string kProducerNumber = "--producer_number";

static const char kMPSCEnvHelpMessage[] =
"You can use the following command line flags to control MPSC for Tests:\n"
"\n"
"MPSC Arguments Selection:\n"
"  --producer_number=\n"
"      concurent numbers of producer.\n"
"  --queue_size=\n"
"      producers will stop when the queue size is larger than it,\n"
"\n";

MPSCEnv::MPSCEnv(int argc, char** argv)
  : Env(argc, argv),
  queue_size_(kDefaultQueueSize),
  producer_number_(kDefaultProducerNumber) {
  for (int i = 1; i < argc; i++) {
    const std::string str = argv[i];
    if (str == "--help") {
      std::cout<<kMPSCEnvHelpMessage<<std::endl;
      break;
    }
    if (FlagParser::ParseIntFlag(str, kQueueSize, &queue_size_)
        || FlagParser::ParseIntFlag(str, kProducerNumber, &producer_number_)) {
      continue;
    }
  }
}

MPSCEnv* test_env;

class TestNode : public MPSCNode {
 public:
  TestNode(const uint32_t number) : number_(number) {}
  virtual void Run() override { }

  uint32_t number() const { return number_; }

 private:
  const uint32_t number_;
};

class Producer {
 public:
  using NewHeadCallback = std::function<void(MPSCNode* new_head)>;
  Producer(MPSCQueue* const queue, int worker_number, int max_queue_size,
           NewHeadCallback callback = nullptr);
  ~Producer();

  void Wait();
  void Stop();

  const std::vector<uint32_t>& numbers() const {
    std::lock_guard<std::mutex> lg(mu_);
    return numbers_;
  }

 private:
  void Produce();

 private:
  MPSCQueue* const queue_;
  NewHeadCallback new_head_callback_;
  std::atomic<bool> stop_;
  std::vector<std::thread> workers_;

  mutable std::mutex mu_;
  int max_queue_size_;
  mutable std::vector<uint32_t> numbers_;
};

Producer::Producer(MPSCQueue* const queue, int worker_number, int max_queue_size,
                   NewHeadCallback callback)
  : queue_(queue), new_head_callback_(callback), stop_(false), workers_(),
  max_queue_size_(max_queue_size), numbers_() {
  for (int i = 0; i < worker_number; i++) {
    std::thread worker(&Producer::Produce, this);
    workers_.push_back(std::move(worker));
  }
}

Producer::~Producer() {
  Stop();
}

void Producer::Wait() {
  for (size_t i = 0; i < workers_.size(); i++) {
    if (workers_[i].joinable()) {
      workers_[i].join();
    }
  }
}

void Producer::Stop() {
  if (stop_.load(std::memory_order_relaxed)) {
    return;
  }
  stop_.store(true, std::memory_order_relaxed);

  Wait();
}

void Producer::Produce() {
  bool new_head = false;
  WELL512RNG random_generator;
  while (!stop_.load(std::memory_order_relaxed)) {
    const uint32_t number = random_generator.Generate();
    auto node = new TestNode(number);
    {
      std::lock_guard<std::mutex> lk(mu_);
      numbers_.push_back(number);
      new_head = queue_->Enqueue(node);
      if (new_head && new_head_callback_) {
        new_head_callback_(static_cast<MPSCNode*>(node));
      }
      if (max_queue_size_ > 0 && numbers_.size() >= static_cast<size_t>(max_queue_size_)) {
        break;
      }
    }
  }
}

class Consumer {
 public:
  Consumer(MPSCQueue* const queue, uint64_t max_consumed_once)
    : max_consumed_once_(max_consumed_once),
    queue_(queue), numbers_(), queue_consumer_(nullptr) { }
  ~Consumer();

  void StartConsume(MPSCNode* new_head);
  static uint64_t ConsumeFunc(void* arg, MPSCQueue::Iterator& iterator);

  const std::vector<uint32_t>& numbers() const  { return numbers_; }
  void Wait();

 private:
  const uint64_t max_consumed_once_;
  std::thread worker_thread_;
  MPSCQueue* const queue_;
  std::vector<uint32_t> numbers_;
  std::unique_ptr<MPSCQueueConsumer> queue_consumer_;
};

Consumer::~Consumer() {
  Wait();
}

void Consumer::StartConsume(MPSCNode* new_head) {
  Wait(); // wait current thread terminated
  queue_consumer_ = util::make_unique<MPSCQueueConsumer>(&Consumer::ConsumeFunc,
                                        static_cast<void*>(this),
                                        queue_, new_head, max_consumed_once_);
  auto task = [this] () {
    bool continuation = false;
    do {
      continuation = queue_consumer_->Consume();
    } while (continuation);
  };
  worker_thread_ = std::thread(std::move(task));
}

uint64_t Consumer::ConsumeFunc(void* arg, MPSCQueue::Iterator& iterator) {
  auto consumer = static_cast<Consumer*>(arg);
  uint64_t iterated_num = 0;
  for (; iterator.Valid(); iterator++) {
    TestNode* test_node = static_cast<TestNode*>(iterator.Current());
    consumer->numbers_.push_back(test_node->number());
    iterated_num++;
  }
  return iterated_num;
}

void Consumer::Wait() {
  if (worker_thread_.joinable()) {
    worker_thread_.join();
  }
}

class TestMPSC
  : public ::testing::Test,
    public ::testing::WithParamInterface<uint64_t> {
 protected:
  void SetUp() override {
    max_consumed_once_ = GetParam();
  }
  void TearDown() override { }

  uint64_t max_consumed_once_;
  MPSCQueue mpsc_queue_;
};

INSTANTIATE_TEST_SUITE_P(DifferentConsumedNumberOnce, TestMPSC, ::testing::Values(1, 10, 100));

TEST_P(TestMPSC, ProducerWithOrder) {
  Consumer* consumer = new Consumer(&mpsc_queue_, max_consumed_once_);
  ResourceGuard<Consumer> guard(consumer);
  auto new_head_callback = [consumer] (MPSCNode* new_head) {
    consumer->StartConsume(new_head);
  };
  Producer producer(&mpsc_queue_, test_env->producer_number()/*worker_number*/,
                    test_env->queue_size()/*max_queue_size*/,
                    new_head_callback);

  // wait producer
  producer.Wait();

  // wait consume
  consumer->Wait();

  const auto& producer_numbers = producer.numbers();
  const auto& consumer_numbers = consumer->numbers();
  ASSERT_EQ(producer_numbers.size(), consumer_numbers.size());
  ASSERT_TRUE(std::equal(producer_numbers.begin(), producer_numbers.end(), consumer_numbers.begin()));
}

} // namespace test

int main(int argc, char** argv) {
  ::test::test_env = static_cast<test::MPSCEnv*>(
      ::testing::AddGlobalTestEnvironment(new test::MPSCEnv(argc, argv)));
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
