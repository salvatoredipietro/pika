// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <unistd.h>
#include <memory>
#include <tuple>
#include <vector>
#include <string>
#include <algorithm>

#include "gtest/gtest.h"
#include "gmock/gmock.h"
#include "slash/include/slash_status.h"

#include "include/deps/storage_env.h"
#include "include/util/random.h"
#include "include/util/message_function.h"
#include "include/storage/pika_binlog_transverter.h"
#include "include/replication/pika_stable_log.h"

namespace test {

using slash::Status;
using Attributes = storage::BinlogItem::Attributes;
using namespace util;

BinlogEnv* test_env;

class TestStableLogBase : public ::testing::Test {
 public:
  using AttrsAndData = std::pair<Attributes, std::string>;
  using AttrsAndDataVec = std::vector<AttrsAndData>;

 protected:
  TestStableLogBase() : stable_log_(nullptr) {} 
  void Init(const replication::StableLogOptions& options);

  Status AppendItems(const int item_size, const int item_number, AttrsAndDataVec& vec);
  Status GetItems(const uint64_t start_index, int get_number, AttrsAndDataVec& vec);
  Status CompareAttrsAndDataVecs(const AttrsAndDataVec& lhs, const AttrsAndDataVec& rhs);

  std::unique_ptr<replication::StableLog> stable_log_;
};

void TestStableLogBase::Init(const replication::StableLogOptions& options) {
  stable_log_ = std::unique_ptr<replication::StableLog>(new replication::StableLog(options));
  stable_log_->Initialize();
}

Status TestStableLogBase::AppendItems(const int item_size, const int item_number, AttrsAndDataVec& vec) {
  Status s;
  uint32_t filenum = 0, term = 0;
  uint64_t offset = 0, index = 0;
  for (int i = 0; i < item_number; i++) {
    s = stable_log_->Logger()->GetProducerStatus(&filenum, &offset, &term, &index);
    if (!s.ok()) {
      break;
    }
    Attributes attrs;
    attrs.exec_time = 1024;
    attrs.content_type = storage::BinlogItem::Attributes::ContentType::TypeFirst;
    attrs.term_id = term;
    attrs.logic_id = index + 1;
    // persist start_offset
    attrs.filenum = filenum;
    attrs.offset = offset;
    std::string data = RandomAlphaData(item_size);

    stable_log_->Lock();
    std::string encoded_data = storage::PikaBinlogTransverter::BinlogEncode(attrs, data, {});
    s = stable_log_->AppendItem(attrs, slash::Slice(encoded_data.c_str(), encoded_data.size()));
    stable_log_->Unlock();

    if (!s.ok()) {
      break;
    }
    s = stable_log_->Logger()->GetProducerStatus(&filenum, &offset, &term, &index);
    if (!s.ok()) {
      break;
    }
    // update end_offset
    attrs.filenum = filenum;
    attrs.offset = offset;
    vec.push_back({attrs, data});
  }
  return s;
}

Status TestStableLogBase::GetItems(const uint64_t start_index, int get_number, AttrsAndDataVec& vec) {
  auto reader = stable_log_->GetPikaBinlogReader(start_index);
  if (reader == nullptr) {
    return Status::Corruption("reader is nullptr");
  }
  Status s;
  storage::BinlogItem log_item;
  std::string log_data;
  LogOffset end_offset;
  while (get_number--) {
    s = reader->Get(&log_data, &end_offset.b_offset.filenum,
                    &end_offset.b_offset.offset);
    if (s.IsEndFile()) {
      s = Status::OK();
      break;
    }
    if (!storage::PikaBinlogTransverter::BinlogDecode(log_data, &log_item)) {
      s = Status::Corruption("Binlog item decode failed");
      break;
    }
    // record end_offset into attrs.
    log_item.set_filenum(end_offset.b_offset.filenum);
    log_item.set_offset(end_offset.b_offset.offset);
    vec.push_back(std::pair<Attributes, std::string>{log_item.attributes(), log_item.content()});
  }
  return s;
}

static Status CompareAttrs(const Attributes& lhs, const Attributes& rhs) {
  if (lhs.term_id != rhs.term_id) {
    return Status::Corruption("terms are not equal");
  }
  if (lhs.logic_id != rhs.logic_id) {
    return Status::Corruption("indexes are not equal");
  }
  return Status::OK();
}

Status TestStableLogBase::CompareAttrsAndDataVecs(const AttrsAndDataVec& lhs,
                                                  const AttrsAndDataVec& rhs) {
  if (lhs.size() != rhs.size()) {
    return Status::Corruption("sizes are not equal");
  }
  Status s;
  const size_t size = lhs.size();
  for (size_t i = 0; i < size; i++) {
    s = CompareAttrs(lhs[i].first, rhs[i].first);
    if (!s.ok()) {
      break;
    }
    if (lhs[i].second != rhs[i].second) {
      s = Status::Corruption("contents are not equal");
      break;
    }
  }
  return s;
}

class TestStableLogReadWrite
  : public TestStableLogBase,
    public ::testing::WithParamInterface<std::tuple<int, bool>> {
 protected:
  TestStableLogReadWrite();
  virtual void SetUp() override {
    std::tie(binlog_file_size_, load_index_into_memory_) = GetParam();
    replication::StableLogOptions options;
    options.group_id = group_id_;
    options.log_path = log_path_;
    options.max_binlog_size = binlog_file_size_;
    options.load_index_into_memory = load_index_into_memory_;
    Init(options);
  }
  virtual void TearDown() override {
    std::string cmd_line = "rm -rf " + log_path_;
    system(cmd_line.c_str());
  }

  int binlog_file_size_;
  bool load_index_into_memory_;
  const ReplicationGroupID group_id_;
  const std::string log_path_;
};

TestStableLogReadWrite::TestStableLogReadWrite()
  : TestStableLogBase(),
  binlog_file_size_(0),
  load_index_into_memory_(false),
  group_id_("testdb", 1),
  log_path_(test_env->binlog_dir()) {
}

TEST_P(TestStableLogReadWrite, AppendAndRead) {
  const int item_size = 16/*16 bytes*/;
  const int item_number = 128;

  // append
  TestStableLogBase::AttrsAndDataVec append_info;
  auto s = AppendItems(item_size, item_number, append_info);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_EQ(append_info.size(), item_number);

  // read
  TestStableLogBase::AttrsAndDataVec read_info;
  const uint64_t start_index = append_info.front().first.logic_id;
  s = GetItems(start_index, item_number<<1, read_info);
  ASSERT_TRUE(s.ok()) << s.ToString();

  s = CompareAttrsAndDataVecs(append_info, read_info);
  ASSERT_TRUE(s.ok()) << s.ToString();
}

TEST_P(TestStableLogReadWrite, FindLogicOffset) {
  const int item_size = 16/*16 bytes*/;
  const int item_number = 128;

  // append
  TestStableLogBase::AttrsAndDataVec append_info;
  auto s = AppendItems(item_size, item_number, append_info);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_EQ(append_info.size(), item_number);

  const auto& first_index = stable_log_->first_offset().l_offset.index;
  const auto& last_index = stable_log_->last_offset().l_offset.index;
  ASSERT_EQ(append_info.front().first.logic_id, first_index);
  ASSERT_EQ(append_info.back().first.logic_id, last_index);

  LogOffset found_offset;
  uint64_t target_index;

  // 1. target_index smaller than first_index
  target_index = std::min(0UL, first_index - 1);
  s = stable_log_->FindLogicOffset(BinlogOffset(), target_index, &found_offset);
  ASSERT_FALSE(s.ok());

  // 2. target_index larger than last_index
  target_index = last_index + 1;
  s = stable_log_->FindLogicOffset(BinlogOffset(), target_index, &found_offset);
  ASSERT_FALSE(s.ok());

  // 3. target_index in range [first_index, last_index]
  for (size_t i = 0; i < 10; i++) {
    static WELL512RNG random_generator_;
    target_index = first_index + random_generator_.Generate()%(last_index - first_index + 1);
    s = stable_log_->FindLogicOffset(BinlogOffset(), target_index, &found_offset);
    ASSERT_TRUE(s.ok()) << s.ToString();

    TestStableLogBase::AttrsAndDataVec read_info;
    s = GetItems(target_index, 1, read_info);
    ASSERT_TRUE(s.ok()) << s.ToString();

    ASSERT_EQ(read_info.front().first.filenum, found_offset.b_offset.filenum);
    ASSERT_EQ(read_info.front().first.offset, found_offset.b_offset.offset);
  }
}

TEST_P(TestStableLogReadWrite, TruncateTo) {
  const int item_size = 16/*16 bytes*/;
  const int item_number = 128;

  // append
  TestStableLogBase::AttrsAndDataVec append_info;
  auto s = AppendItems(item_size, item_number, append_info);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_EQ(append_info.size(), item_number);

  for (int i = 0; i < 10; i++) {
    static WELL512RNG random_generator_;
    const auto& first_index = stable_log_->first_offset().l_offset.index;
    const auto& last_index = stable_log_->last_offset().l_offset.index;

    uint64_t truncate_index;
    if (first_index == last_index) {
      truncate_index = first_index;
    } else {
      // truncate_index in range (first_index, last_index)
      truncate_index = first_index + 1 +  random_generator_.Generate()%(last_index - first_index);
    }

    LogOffset truncate_offset;
    s = stable_log_->FindLogicOffset(BinlogOffset(), truncate_index, &truncate_offset);
    ASSERT_TRUE(s.ok()) << s.ToString();

    s = stable_log_->TruncateTo(truncate_offset);
    ASSERT_TRUE(s.ok()) << s.ToString();

    const auto& first_index_after_trunc = stable_log_->first_offset().l_offset.index;
    const auto& last_index_after_trunc = stable_log_->last_offset().l_offset.index;

    ASSERT_EQ(first_index_after_trunc, first_index);

    if (first_index == last_index) {
      ASSERT_EQ(first_index_after_trunc, last_index_after_trunc);
    } else {
      ASSERT_TRUE(first_index_after_trunc < last_index_after_trunc);
      ASSERT_TRUE(truncate_index > first_index_after_trunc);
    }

    TestStableLogBase::AttrsAndDataVec read_info;
    uint64_t read_index;
    // read after truncate_index
    read_index = truncate_index + 1;
    s = GetItems(read_index, 1, read_info);
    ASSERT_FALSE(s.ok());

    // read at truncate_index
    read_info.clear();
    read_index = truncate_index;
    s = GetItems(read_index, 1, read_info);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_EQ(read_info.front().first.term_id, truncate_offset.l_offset.term);
    ASSERT_EQ(read_info.front().first.logic_id, truncate_offset.l_offset.index);
    ASSERT_EQ(read_info.front().first.filenum, truncate_offset.b_offset.filenum);
    ASSERT_EQ(read_info.front().first.offset, truncate_offset.b_offset.offset);

    // read ahead truncate_index
    read_info.clear();
    s = GetItems(first_index_after_trunc, item_number<<1, read_info);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_EQ(read_info.size(), truncate_index - first_index_after_trunc + 1);
  }
}

TEST_P(TestStableLogReadWrite, ResetStableLog) {
  const int item_size = 16/*16 bytes*/;
  const int item_number = 128;

  // append
  TestStableLogBase::AttrsAndDataVec append_info;
  auto s = AppendItems(item_size, item_number, append_info);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_EQ(append_info.size(), item_number);

  const auto& last_offset = stable_log_->last_offset();

  // snapshot_offset extends than the last offset in local
  LogOffset snapshot_offset;
  snapshot_offset.l_offset.term = last_offset.l_offset.term + 1; 
  snapshot_offset.l_offset.index = last_offset.l_offset.index + 10; 
  snapshot_offset.b_offset.filenum = last_offset.b_offset.filenum;
  snapshot_offset.b_offset.offset = last_offset.b_offset.offset + 100; 

  s = stable_log_->ResetStableLog(snapshot_offset);
  ASSERT_TRUE(s.ok()) << s.ToString();

  const auto& first_index_after_reset = stable_log_->first_offset().l_offset.index;
  const auto& last_index_after_reset = stable_log_->last_offset().l_offset.index;
  ASSERT_EQ(first_index_after_reset, 0);
  ASSERT_EQ(first_index_after_reset, last_index_after_reset);

  // append
  append_info.clear();
  s = AppendItems(item_size, item_number, append_info);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_EQ(append_info.size(), item_number);

  const auto& first_index = stable_log_->first_offset().l_offset.index;
  const auto& last_index = stable_log_->last_offset().l_offset.index;
  ASSERT_EQ(append_info.front().first.logic_id, first_index);
  ASSERT_EQ(append_info.back().first.logic_id, last_index);
}

INSTANTIATE_TEST_SUITE_P(WithDifferentStartOptions, TestStableLogReadWrite,
    ::testing::Combine(
      ::testing::Values(0, 1024, 1024*1024)/*binlog_file_size(bytes)*/,
      ::testing::Values(true, false)/*load_index_into_memory*/));

class TestPurgeStableLog
  : public TestStableLogBase,
    public ::testing::WithParamInterface<std::tuple<int, int, int, bool>> {
 protected:
  constexpr static int kDefaultBinlogFileSize = 128;
  TestPurgeStableLog();
  virtual void SetUp() override {
    std::tie(expire_logs_nums_, expire_logs_days_, retain_logs_num_, load_index_into_memory_) = GetParam();
    replication::StableLogOptions options;
    options.group_id = group_id_;
    options.log_path = log_path_;
    options.max_binlog_size = kDefaultBinlogFileSize;
    options.load_index_into_memory = load_index_into_memory_;
    Init(options);
  }
  virtual void TearDown() override {
    std::string cmd_line = "rm -rf " + log_path_;
    system(cmd_line.c_str());
  }
  void ValidateLogsAfterPurged(const TestStableLogBase::AttrsAndDataVec& append_info,
                               uint32_t start_filenum_before_purge,
                               uint32_t end_filenum_before_purge,
                               uint32_t purged_to_filenum);

  int expire_logs_nums_;
  int expire_logs_days_;
  int retain_logs_num_;
  bool load_index_into_memory_;
  const ReplicationGroupID group_id_;
  const std::string log_path_;
  WELL512RNG random_generator_;
};

TestPurgeStableLog::TestPurgeStableLog()
  : expire_logs_nums_(0),
  expire_logs_days_(0),
  retain_logs_num_(0),
  load_index_into_memory_(false),
  group_id_("testdb", 1),
  log_path_(test_env->binlog_dir()) {
}

static bool FindFirstLogByFilenum(const TestStableLogBase::AttrsAndDataVec& vec,
                                  const uint32_t filenum,
                                  TestStableLogBase::AttrsAndData* data,
                                  size_t* index = nullptr) {
  auto iter = std::find_if(vec.begin(), vec.end(), [filenum](TestStableLogBase::AttrsAndData data) {
        return data.first.filenum == filenum;
      });
  if (iter == vec.end()) {
    return false;
  }
  *data = *iter;
  if (index != nullptr) {
    *index = iter - vec.begin();
  }
  return true;
}

void TestPurgeStableLog::ValidateLogsAfterPurged(const TestStableLogBase::AttrsAndDataVec& append_info,
                                                 uint32_t start_filenum_before_purge,
                                                 uint32_t end_filenum_before_purge,
                                                 uint32_t purged_to_filenum) {
  const uint32_t purged_num = purged_to_filenum - start_filenum_before_purge + 1;
  const uint32_t left_num = end_filenum_before_purge - purged_to_filenum;
  // files which have been purged should not be found
  {
    for (size_t loop = 0; loop < 3; loop++) {
      // [start_filenum_before_purge, purged_to_filenum]
      uint32_t read_start_filenum = start_filenum_before_purge + random_generator_.Generate()%purged_num;

      TestStableLogBase::AttrsAndData read_start_info;
      ASSERT_TRUE((FindFirstLogByFilenum(append_info, read_start_filenum, &read_start_info)))
        << read_start_filenum + " can not found in append_info";

      TestStableLogBase::AttrsAndDataVec read_info;
      auto s = GetItems(read_start_info.first.logic_id, append_info.size(), read_info);
      ASSERT_FALSE(s.ok());
      ASSERT_EQ(read_info.size(), 0);
    }
  }
  if (left_num == 0) {
    return;
  }
  // files after purged point must be valid
  {
    for (size_t loop = 0; loop < 3; loop++) {
      // (purged_to_filenum, init_end_filenum]
      uint32_t read_start_filenum = purged_to_filenum + 1 + random_generator_.Generate()%left_num;

      TestStableLogBase::AttrsAndData read_start_info;
      size_t index_in_append_info;
      ASSERT_TRUE((FindFirstLogByFilenum(append_info, read_start_filenum, &read_start_info, &index_in_append_info)))
        << read_start_filenum + " can not found in append_info";

      TestStableLogBase::AttrsAndDataVec compared_source(append_info.begin() + index_in_append_info, append_info.end());

      TestStableLogBase::AttrsAndDataVec read_info;
      auto s = GetItems(read_start_info.first.logic_id, append_info.size(), read_info);
      ASSERT_TRUE(s.ok()) << s.ToString();

      s = CompareAttrsAndDataVecs(compared_source, read_info);
      ASSERT_TRUE(s.ok()) << s.ToString();
    }
  }
}

TEST_P(TestPurgeStableLog, PurgeFilesManually) {
  const int item_size = kDefaultBinlogFileSize;
  const int item_number = 64;

  // append
  TestStableLogBase::AttrsAndDataVec append_info;
  auto s = AppendItems(item_size, item_number, append_info);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_EQ(append_info.size(), item_number);

  const uint32_t init_start_filenum = stable_log_->first_offset().b_offset.filenum;
  const uint32_t init_end_filenum = stable_log_->last_offset().b_offset.filenum;
  const uint32_t log_file_number = init_end_filenum - init_start_filenum + 1;

  auto purge_filter = [] (const uint32_t index) { return false; };
  bool manual = true;
  uint32_t to = 0;
  uint32_t start_filenum = init_start_filenum;
  uint32_t end_filenum = init_end_filenum;

  // 1. purge files with indexes in range (init_start_filenum, init_end_filenum)
  {
    to = start_filenum + 1 + random_generator_.Generate()%(end_filenum - start_filenum - 1);
    replication::PurgeLogsClosure* closure = new replication::PurgeLogsClosure(group_id_);
    stable_log_->PurgeFiles(static_cast<int>(log_file_number),
                            expire_logs_days_, to, manual, purge_filter, closure);
    ASSERT_TRUE(closure->status().ok()) << closure->status().ToString();

    ValidateLogsAfterPurged(append_info, start_filenum, init_end_filenum, to);
    if (HasFatalFailure()) return;

    start_filenum = stable_log_->first_offset().b_offset.filenum;
    ASSERT_EQ(to + 1, start_filenum);
  }

  // 2. purge all files 
  {
    start_filenum = stable_log_->first_offset().b_offset.filenum;
    end_filenum = stable_log_->last_offset().b_offset.filenum;
    to = end_filenum;
    replication::PurgeLogsClosure* closure = new replication::PurgeLogsClosure(group_id_);
    stable_log_->PurgeFiles(static_cast<int>(log_file_number),
                            expire_logs_days_, to, manual, purge_filter, closure);
    ASSERT_TRUE(closure->status().ok()) << closure->status().ToString();
    ASSERT_TRUE(stable_log_->first_offset().Empty());
    ASSERT_TRUE(stable_log_->last_offset().Empty());

    ValidateLogsAfterPurged(append_info, start_filenum, init_end_filenum, to);
    if (HasFatalFailure()) return;
  }
}

TEST_P(TestPurgeStableLog, PurgeFilesAutomatically) {
  const int item_size = kDefaultBinlogFileSize;
  const int item_number = 64;

  TestStableLogBase::AttrsAndDataVec append_info;
  auto s = AppendItems(item_size, item_number, append_info);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_EQ(append_info.size(), item_number);

  const auto init_start_offfset = stable_log_->first_offset();
  const auto init_end_offset = stable_log_->last_offset();

  const uint32_t init_start_filenum = init_start_offfset.b_offset.filenum;
  const uint32_t init_end_filenum = init_end_offset.b_offset.filenum;
  const uint32_t log_file_number = init_end_filenum - init_start_filenum + 1;

  auto purge_filter = [] (const uint32_t index) { return false; };
  bool manual = false;
  uint32_t to = 0;
  uint32_t start_filenum = init_start_filenum;

  replication::PurgeLogsClosure* closure = new replication::PurgeLogsClosure(group_id_);
  stable_log_->PurgeFiles(expire_logs_nums_, expire_logs_days_, to, manual, purge_filter, closure);
  ASSERT_TRUE(closure->status().ok()) << closure->status().ToString();

  const uint32_t expect_purged_number = log_file_number - expire_logs_nums_;
  to = std::min(init_end_filenum, start_filenum + expect_purged_number - 1);

  ValidateLogsAfterPurged(append_info, start_filenum, init_end_filenum, to);
  if (HasFatalFailure()) return;

  if (to == init_end_filenum) {
    ASSERT_TRUE(stable_log_->first_offset().Empty()) << stable_log_->first_offset().ToString();
    ASSERT_TRUE(stable_log_->last_offset().Empty()) << stable_log_->last_offset().ToString();
  } else {
    ASSERT_EQ(stable_log_->first_offset().b_offset.filenum, to + 1);
    ASSERT_EQ(stable_log_->last_offset().b_offset, init_end_offset.b_offset);
  }
}

INSTANTIATE_TEST_SUITE_P(WithDifferentPurgeOptions, TestPurgeStableLog,
    ::testing::Combine(
      ::testing::Values(0, 10, 20)/*expire_logs_nums*/,
      ::testing::Values(0, 1, 10)/*expire_logs_days*/,
      ::testing::Values(0, 1, 10)/*retain_logs_num*/,
      ::testing::Values(true, false)/*load_index_into_memory*/));

} // namespace test

int main(int argc, char** argv) {
  test::test_env = static_cast<test::BinlogEnv*>(
      ::testing::AddGlobalTestEnvironment(new test::BinlogEnv(argc, argv)));
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
