// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <algorithm>

#include "gtest/gtest.h"
#include "gmock/gmock.h"
#include "slash/include/slash_status.h"

#include "include/deps/common_env.h"
#include "include/util/random.h"
#include "include/storage/pika_binlog_transverter.h"
#include "include/replication/pika_memory_log.h"

namespace test {

using slash::Status;
using Attributes = storage::BinlogItem::Attributes;
using namespace util;

class TestMemLog
  : public ::testing::Test,
    public ::testing::WithParamInterface<int> {
 protected:
  TestMemLog()
    : init_log_number_(0),
    first_index_(0), last_index_(0) {}
  constexpr static int kStartIndex = 1;
  virtual void SetUp() override {
    first_index_ = kStartIndex;
    init_log_number_ = GetParam();
    // index range: [0, init_log_number_)
    AppendLogs(init_log_number_);
  }
  virtual void TearDown() override {}

  void AppendLogs(int number);

  int init_log_number_;
  int first_index_;
  int last_index_;
  replication::MemLog mem_log_;
  WELL512RNG random_generator_;
};

void TestMemLog::AppendLogs(int number) {
  // index start from 1
  Attributes attrs;
  for (int i = 0; i < number; i++) {
    attrs.logic_id = first_index_++;
    mem_log_.AppendLog(replication::MemLog::LogItem(attrs, nullptr));
    last_index_ = attrs.logic_id;
  }
}

TEST_P(TestMemLog, CopyLogsByIndex) {
  const int index = std::max(kStartIndex, init_log_number_>>1);

  std::vector<replication::MemLog::LogItem> logs;
  // 1. copy logs before index
  replication::MemLog::ReadOptions options;
  options.direction = replication::MemLog::ReadOptions::Direction::kFront;
  mem_log_.CopyLogsByIndex(index, options, &logs);
  ASSERT_EQ(logs.size(), index - kStartIndex + 1);

  // 2. copy logs after index
  options.direction = replication::MemLog::ReadOptions::Direction::kBack;
  size_t max_count = last_index_ - index + 1;
  for (size_t i = 0 ; i < 10; i++) {
    logs.clear();
    options.count = random_generator_.Generate()%1000; // [0, 999]
    mem_log_.CopyLogsByIndex(index, options, &logs);
    ASSERT_EQ(logs.size(), std::min(options.count, max_count));
  }
}

TEST_P(TestMemLog, ConsumeLogsByIndex) {
  std::vector<replication::MemLog::LogItem> logs;
  const int index = std::max(kStartIndex, init_log_number_>>1);
  mem_log_.ConsumeLogsByIndex(index, &logs);
  ASSERT_EQ(logs.size(), index - kStartIndex + 1);
  ASSERT_EQ(mem_log_.Size(), last_index_ - kStartIndex + 1);
}

TEST_P(TestMemLog, PersistedAfterConsumed) {
  // consume [kStartIndex, consumed_index]
  std::vector<replication::MemLog::LogItem> logs;
  const int consumed_index = std::max(kStartIndex, init_log_number_>>1);
  mem_log_.ConsumeLogsByIndex(consumed_index, &logs);
  ASSERT_EQ(logs.size(), consumed_index - kStartIndex + 1);
  ASSERT_EQ(mem_log_.Size(), last_index_ - kStartIndex + 1);

  // persist [kStartIndex, persisted_index]
  const int persisted_index = std::max(kStartIndex, consumed_index>>1);
  mem_log_.UpdatePersistedLogsByIndex(persisted_index);

  // logs before persisted_index should have be purged
  ASSERT_EQ(mem_log_.Size(), last_index_ - persisted_index);
}

TEST_P(TestMemLog, ConsumedAfterPersisted) {
  // persist [kStartIndex, persisted_index]
  const int persisted_index = std::max(kStartIndex, init_log_number_>>1);
  mem_log_.UpdatePersistedLogsByIndex(persisted_index);
  ASSERT_EQ(mem_log_.Size(), last_index_ - kStartIndex + 1);

  // consume [kStartIndex, consumed_index]
  std::vector<replication::MemLog::LogItem> logs;
  const int consumed_index = std::max(kStartIndex, persisted_index>>1);
  mem_log_.ConsumeLogsByIndex(consumed_index, &logs);
  ASSERT_EQ(logs.size(), consumed_index - kStartIndex + 1);

  // logs before cosumed_index should have be purged
  ASSERT_EQ(mem_log_.Size(), last_index_ - consumed_index);
}

TEST_P(TestMemLog, PurgeLogsByIndex) {
  std::vector<replication::MemLog::LogItem> logs;
  // 1. index smaller than kStartIndex
  int purge_index = kStartIndex - 1;
  auto s = mem_log_.PurgeLogsByIndex(purge_index, &logs);
  ASSERT_TRUE(!s.ok());
  ASSERT_EQ(logs.size(), 0);
  ASSERT_EQ(mem_log_.Size(), last_index_ - kStartIndex + 1);

  // 2. index larger than last_index_
  logs.clear();
  purge_index = last_index_ + 1;
  s = mem_log_.PurgeLogsByIndex(purge_index, &logs);
  ASSERT_TRUE(!s.ok());
  ASSERT_EQ(logs.size(), 0);
  ASSERT_EQ(mem_log_.Size(), last_index_ - kStartIndex + 1);

  // 3. index in range [kStartIndex, last_index_]
  logs.clear();
  purge_index = kStartIndex + random_generator_.Generate()%(last_index_ - kStartIndex + 1);
  s = mem_log_.PurgeLogsByIndex(purge_index, &logs);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_EQ(logs.size(), purge_index - kStartIndex + 1);
  ASSERT_EQ(mem_log_.Size(), last_index_ - purge_index);
}

TEST_P(TestMemLog, TruncateToByIndex) {
  // 1. index smaller than kStartIndex
  int truncate_index = kStartIndex - 1;
  auto s = mem_log_.TruncateToByIndex(truncate_index);
  ASSERT_TRUE(!s.ok());
  ASSERT_EQ(mem_log_.Size(), last_index_ - kStartIndex + 1);

  // 2. index larger than last_index_
  truncate_index = last_index_ + 1;
  s = mem_log_.TruncateToByIndex(truncate_index);
  ASSERT_TRUE(!s.ok());
  ASSERT_EQ(mem_log_.Size(), last_index_ - kStartIndex + 1);

  // 3. index in range [kStartIndex, last_index_]
  truncate_index = kStartIndex + random_generator_.Generate()%(last_index_ - kStartIndex + 1);
  s = mem_log_.TruncateToByIndex(truncate_index);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_EQ(mem_log_.Size(), truncate_index - kStartIndex + 1);
}

TEST_P(TestMemLog, FindLogItemByIndex) {
  LogOffset found_offset;
  // 1. index smaller than kStartIndex
  int fetch_index = kStartIndex - 1;
  auto found = mem_log_.FindLogItemByIndex(fetch_index, &found_offset);
  ASSERT_FALSE(found);
  ASSERT_TRUE(found_offset.Empty());

  // 2. index larger than last_index_
  fetch_index = last_index_ + 1;
  found = mem_log_.FindLogItemByIndex(fetch_index, &found_offset);
  ASSERT_FALSE(found);
  ASSERT_TRUE(found_offset.Empty());

  // 3. index in range [kStartIndex, last_index_]
  fetch_index = kStartIndex + random_generator_.Generate()%(last_index_ - kStartIndex + 1);
  found = mem_log_.FindLogItemByIndex(fetch_index, &found_offset);
  ASSERT_TRUE(found);
  ASSERT_EQ(found_offset.l_offset.index, fetch_index);
}

INSTANTIATE_TEST_SUITE_P(DifferentInitializedNumber, TestMemLog, ::testing::Values(1, 10, 100, 1000));

}; // namespace test

int main(int argc, char** argv) {
  ::testing::AddGlobalTestEnvironment(new test::Env(argc, argv));
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
