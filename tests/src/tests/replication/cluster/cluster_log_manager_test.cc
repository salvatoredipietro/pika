// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <unistd.h>
#include <vector>
#include <string>

#include "gtest/gtest.h"
#include "gmock/gmock.h"

#include "slash/include/slash_slice.h"
#include "slash/include/slash_status.h"

#include "include/deps/common_env.h"
#include "include/deps/storage_env.h"
#include "include/util/callbacks.h"
#include "include/util/random.h"
#include "include/util/message_function.h"
#include "include/util/replication_util.h"
#include "include/storage/pika_binlog_transverter.h"
#include "include/replication/pika_repl_transporter.h"
#include "include/replication/pika_repl_message_executor.h"
#include "include/replication/cluster/pika_cluster_log_manager.h"

namespace test {

using slash::Status;
using Attributes = storage::BinlogItem::Attributes;
using namespace util;

BinlogEnv* test_env;

class FakeDiskClosure : public replication::DiskClosure {
 public:
  FakeDiskClosure(const Status& expected_status, Waiter* waiter,
                  std::vector<Attributes>* attrs_recorder,
                  std::vector<std::string>* data_recorder)
    : waiter_(waiter), expected_status_(expected_status),
    attrs_recorder_(attrs_recorder), data_recorder_(data_recorder) {}
  FakeDiskClosure(const LogOffset& prev_offset, const uint32_t term,
                  const Status& expected_status, Waiter* waiter,
                  std::vector<Attributes>* attrs_recorder,
                  std::vector<std::string>* data_recorder)
    : replication::DiskClosure(prev_offset, term), waiter_(waiter),
    expected_status_(expected_status),
    attrs_recorder_(attrs_recorder),
    data_recorder_(data_recorder) {}

  bool IsSuccess() {
    return expected_status_ == status_;
  }

  virtual void Run() {
    RecordEntryInfo();
    if (waiter_ == nullptr) {
      return;
    }
    Status s;
    if (expected_status_ != status_) {
      s = Status::Corruption("Expected: " + expected_status_.ToString()
                             + ", Actual: " + status_.ToString());
    }
    waiter_->TryNotify(s);
  }

 private:
  void RecordEntryInfo();
  Waiter* waiter_;
  Status expected_status_;
  std::vector<Attributes>* attrs_recorder_;
  std::vector<std::string>* data_recorder_;
};

void FakeDiskClosure::RecordEntryInfo() {
  if (attrs_recorder_ == nullptr && data_recorder_ == nullptr) {
    return;
  }
  for (const auto& entry : entries_) {
    if (attrs_recorder_ != nullptr) {
      attrs_recorder_->push_back(entry.data_attrs);
    }
    if (data_recorder_ != nullptr) {
      data_recorder_->push_back(entry.data_slice.ToString());
    }
  }
}

class TestClusterLogManager : public ::testing::Test {
 protected:
  constexpr static int kAppendLogsLowerBoundary = 10;
  constexpr static int kAppendLogsUpperBoundary = 100;
  constexpr static int kAppendLogsSize = 16;
  constexpr static int kMaxBinlogFileSize = 10*1024*1024/*10 MB*/;
  TestClusterLogManager();

  void SetUp() override {
    Status s = log_manager_->Initialize();
    if (!s.ok()) {
      LOG(FATAL) << "initialize the log manager failed: " << s.ToString();
    }
    executor_.Start();
  }
  void TearDown() override {
    log_manager_->Close();
    executor_.Stop(false);
    std::string cmd_line = "rm -rf " + log_path_;
    system(cmd_line.c_str());
  }
  struct AppendOptions {
    uint32_t term = 1;
    int low = kAppendLogsLowerBoundary;
    int high = kAppendLogsUpperBoundary;
    int log_size = kAppendLogsSize;
    bool async = false;
    Waiter* waiter = nullptr;
    std::vector<Attributes>* entry_attrs = nullptr;
    std::vector<std::string>* entry_data = nullptr;
    AppendOptions() = default;
  };
  void AppendLogs(const AppendOptions& options);

  const ReplicationGroupID group_id_;
  const std::string log_path_;
  WELL512RNG random_generator_;
  TaskExecutor executor_;
  std::unique_ptr<replication::ClusterLogManager> log_manager_;
};

TestClusterLogManager::TestClusterLogManager()
  : group_id_("testdb", 1), log_path_(test_env->binlog_dir()),
  executor_(TaskExecutorOptions()),
  log_manager_(nullptr) {
  replication::ClusterLogManagerOptions options;
  options.executor = &executor_;
  options.group_id = group_id_;
  replication::StableLogOptions stable_log_options;
  stable_log_options.group_id = group_id_;
  stable_log_options.log_path = log_path_;
  stable_log_options.max_binlog_size = kMaxBinlogFileSize; 
  stable_log_options.retain_logs_num = 10;
  stable_log_options.load_index_into_memory = true;
  options.stable_log_options = stable_log_options;

  log_manager_ = std::unique_ptr<replication::ClusterLogManager>(
      new replication::ClusterLogManager(options));
}

void TestClusterLogManager::AppendLogs(const AppendOptions& options) {
  ASSERT_TRUE(options.low >= 0);
  ASSERT_TRUE(options.low <= options.high);
  ASSERT_TRUE(options.log_size >= 1);
  log_manager_->UpdateTerm(options.term);
  uint32_t number = options.low + random_generator_.Generate()%(options.high - options.low);
  ASSERT_TRUE(number >= 1);

  if (options.waiter != nullptr) {
    options.waiter->Reset(number);
  }
  Status s;
  while (number--) {
    auto repl_task = std::make_shared<ReplTask>(std::move(ZeroBinaryData(options.log_size)),
                                                nullptr, group_id_, PeerID(),
                                                ReplTask::Type::kOtherType,
                                                ReplTask::LogFormat::kUnkown);
    auto closure = new FakeDiskClosure(Status::OK(), options.waiter,
                                       options.entry_attrs, options.entry_data);
    s = log_manager_->AppendLog(repl_task, closure, options.async);
    if (!options.async || !s.ok()) {
      closure->Run();
      delete closure;
    }
    if (!s.ok()) {
      LOG(ERROR)  << "Append logs failed"
                  << ", number " << number
                  << ", error info " << s.ToString();
      break;
    }
  }
  ASSERT_TRUE(s.ok())  << "Append logs failed"
                       << ", number " << number
                       << ", error info " << s.ToString();
}

::testing::AssertionResult AssertLogicOffsetEqual(const char* m_expr,
                                                  const char* n_expr,
                                                  const Attributes& left_attrs,
                                                  const LogOffset& right_offset) {
    if (left_attrs.term_id == right_offset.l_offset.term
        && left_attrs.logic_id == right_offset.l_offset.index) {
      return ::testing::AssertionSuccess();
    }
    return ::testing::AssertionFailure() << "LogicOffset does not equal"
                                         << ", left_attrs: " << left_attrs.term_id << " " << left_attrs.logic_id
                                         << ", right_offset" << right_offset.l_offset.ToString();
}

::testing::AssertionResult AssertLogicOffsetEqual(const char* m_expr,
                                                  const char* n_expr,
                                                  const Attributes& left_attrs,
                                                  const Attributes& right_attrs) {
    if (left_attrs.term_id == right_attrs.term_id
        && left_attrs.logic_id == right_attrs.logic_id) {
      return ::testing::AssertionSuccess();
    }
    return ::testing::AssertionFailure() << "LogicOffset does not equal"
                                         << ", left_attrs: " << left_attrs.term_id << " " << left_attrs.logic_id
                                         << ", right_attrs" << right_attrs.term_id << " " << right_attrs.logic_id;
}

TEST_F(TestClusterLogManager, Recover) {
  // prepare local logs
  std::vector<Attributes> append_entry_attrs;
  AppendOptions options;
  options.entry_attrs = &append_entry_attrs;

  AppendLogs(options);
  if (HasFatalFailure()) return;

  // purge in-memory logs
  size_t append_count = append_entry_attrs.size();
  std::vector<replication::MemLog::LogItem> mem_logs;
  auto s = log_manager_->PurgeMemLogsByIndex(append_entry_attrs[append_count - 1].logic_id, &mem_logs);
  ASSERT_TRUE(s.ok()) << "PurgeMemLogsByIndex failed " + s.ToString();

  ASSERT_EQ(append_count, mem_logs.size());
  for (size_t i = 0 ; i < append_count; i++) {
    ASSERT_PRED_FORMAT2(AssertLogicOffsetEqual, append_entry_attrs[i], mem_logs[i].attrs);
  }

  // Recover from index
  uint32_t index = random_generator_.Generate()%append_count;  // [0, append_count)
  auto entry_attrs = append_entry_attrs[index];
  LogOffset committed_offset(BinlogOffset(entry_attrs.filenum, entry_attrs.offset),
                             LogicOffset(entry_attrs.term_id, entry_attrs.logic_id));
  log_manager_->Recover(committed_offset, committed_offset);

  mem_logs.clear();
  s = log_manager_->PurgeMemLogsByIndex(append_entry_attrs[append_count - 1].logic_id, &mem_logs);
  ASSERT_TRUE(s.ok() || index == (append_count-1))
    << "PurgeMemLogsByIndex failed " << s.ToString()
    << ", committed_offset " << committed_offset.ToString()
    << ", last entry id " << append_entry_attrs[append_count - 1].logic_id;

  ASSERT_EQ(mem_logs.size(), append_entry_attrs.back().logic_id - entry_attrs.logic_id);
  for (size_t i = 0; i < mem_logs.size(); i++) {
    ASSERT_PRED_FORMAT2(AssertLogicOffsetEqual, append_entry_attrs[index + 1 + i],
                        mem_logs[i].attrs);
  }
}

TEST_F(TestClusterLogManager, AsyncAppendLog) {
  std::vector<Attributes> append_entry_attrs;

  Waiter w(1);
  AppendOptions options;
  options.waiter = &w;
  options.async = true;
  options.entry_attrs = &append_entry_attrs;
  AppendLogs(options);
  if (HasFatalFailure()) return;
  w.WaitFor(500); // wait until logs have been persisted or error occurred.

  ASSERT_TRUE(w.status().ok()) << w.status().ToString();
}

TEST_F(TestClusterLogManager, AppendReplicaLogsWithPreOffsetUnmatched) {
  // 1. Prepare local logs
  std::vector<Attributes> local_entry_attrs;
  AppendOptions options;
  options.term = 0;
  options.entry_attrs = &local_entry_attrs;

  AppendLogs(options);
  if (HasFatalFailure()) return;

  size_t local_count = local_entry_attrs.size();

  std::vector<Attributes> append_entries;
  
  // 2. Append replicate logs into local
  // 2.1 prev index does not in local range.
  LogOffset prev_offset;
  prev_offset.l_offset.index = local_count + 1;
  prev_offset.l_offset.term = 0;

  Waiter w(1);
  replication::DiskClosure* closure = new FakeDiskClosure(prev_offset, 0, Status::Corruption(""), &w, &append_entries, nullptr);
  std::vector<replication::LogEntry> remote_append_entries{replication::LogEntry()};
  auto s = log_manager_->AppendReplicaLog(PeerID(), remote_append_entries/*remote append logs*/, closure);
  if (!s.ok()) {
    closure->set_status(s);
    closure->Run();
    delete closure;
  }
  w.Wait();

  ASSERT_TRUE(!s.ok()) << "AppendReplicaLog with unexpected success";

  auto new_last_offset = log_manager_->GetLastStableOffset(true, nullptr);
  ASSERT_PRED_FORMAT2(AssertLogicOffsetEqual, local_entry_attrs[local_count - 1], new_last_offset);

  // 2.2 prev index does in local range, but with different term
  prev_offset.l_offset.index = 1 + random_generator_.Generate()%local_count;
  prev_offset.l_offset.term = 1;
  w.Reset(1);
  closure = new FakeDiskClosure(prev_offset, 0, Status::Corruption(""), &w, &append_entries, nullptr);
  s = log_manager_->AppendReplicaLog(PeerID(), remote_append_entries/*remote append logs*/, closure);
  if (!s.ok()) {
    closure->set_status(s);
    closure->Run();
    delete closure;
  }
  w.Wait();
  ASSERT_TRUE(!s.ok()) << "AppendReplicaLog with unexpected success";

  new_last_offset = log_manager_->GetLastStableOffset(true, nullptr);
  ASSERT_PRED_FORMAT2(AssertLogicOffsetEqual, local_entry_attrs[local_count - 1], new_last_offset);
}

TEST_F(TestClusterLogManager, AppendReplicaLogsWithLocalIsLarger) {
  // 1. Prepare local logs
  std::vector<Attributes> local_entry_attrs;
  AppendOptions options;
  options.entry_attrs = &local_entry_attrs;

  AppendLogs(options);
  if (HasFatalFailure()) return;

  size_t local_count = local_entry_attrs.size();

  ASSERT_TRUE(local_count > 1);

  // index in range [1, local_count)
  size_t index = 1 + static_cast<size_t>(random_generator_.Generate()%(local_count-1));
  std::vector<replication::LogEntry> append_entries;
  append_entries.reserve(local_count - index);

  for (; index < local_count; index++) {
    const auto& attr = local_entry_attrs[index];
    std::string data = ZeroBinaryData(kAppendLogsSize);
    replication::LogEntry entry(attr, std::move(data));
    append_entries.push_back(std::move(entry));
  }
  std::vector<replication::LogEntry> saved_append_entries(append_entries.begin(),
                                                          append_entries.end());

  LogOffset prev_offset;
  prev_offset.l_offset.term = local_entry_attrs[index - 1].term_id;
  prev_offset.l_offset.index = local_entry_attrs[index - 1].logic_id;

  Waiter w(1);
  replication::DiskClosure* closure = new FakeDiskClosure(prev_offset, 0, Status::OK(), &w, nullptr, nullptr);
  auto s = log_manager_->AppendReplicaLog(PeerID(), append_entries/*remote append logs*/, closure);
  if (!s.ok()) {
    closure->set_status(s);
    closure->Run();
    delete closure;
  }
  w.Wait();

  ASSERT_TRUE(s.ok()) << "AppendReplicaLog failed " << s.ToString();

  LogOffset new_last_offset = log_manager_->GetLastStableOffset(true, nullptr);
  const auto& append_last_offset = saved_append_entries.back().GetLogOffset();
  ASSERT_EQ(new_last_offset.l_offset, append_last_offset.l_offset)
    << "After append entries, the matched last offset " << new_last_offset.l_offset.ToString()
    << " does not match the declared offset " << append_last_offset.l_offset.ToString();
}

TEST_F(TestClusterLogManager, AppendReplicaLogsWithLocalIsSmaller) {
  // 1. Prepare local logs
  std::vector<Attributes> local_entry_attrs;
  AppendOptions options;
  options.term = 0;
  options.entry_attrs = &local_entry_attrs;
  AppendLogs(options);
  if (HasFatalFailure()) return;

  size_t local_count = local_entry_attrs.size();
  std::vector<replication::LogEntry> append_entries;
  Attributes attrs;
  uint32_t old_term = options.term;
  size_t remote_index = 1;
  size_t old_term_number = local_count + random_generator_.Generate()%kAppendLogsLowerBoundary;
  for (; remote_index <= old_term_number; remote_index++) {
    attrs.logic_id = remote_index;
    attrs.term_id = old_term;
    std::string data = remote_index <= local_count ? ZeroBinaryData(kAppendLogsSize) : RandomAlphaData(kAppendLogsSize);
    replication::LogEntry entry(attrs, std::move(data));
    append_entries.push_back(std::move(entry));
  }
  uint32_t new_term = old_term++;
  size_t new_term_number = kAppendLogsLowerBoundary;
  for (; remote_index <= old_term_number + new_term_number; remote_index++) {
    attrs.logic_id = remote_index;
    attrs.term_id = new_term;
    std::string data = RandomAlphaData(kAppendLogsSize);
    replication::LogEntry entry(attrs, std::move(data));
    append_entries.push_back(std::move(entry));
  }
  // remote index:
  // [1, old_term_number] with term = 0;
  // [old_term_number + 1, old_term_number + kAppendLogsLowerBoundary] with term = 1;
  // 
  // old_term_number in range [local_count, local_count + kAppendLogsLowerBoundary).
  // make sure prev_offset in range [0, local_count) (prev_offset matched).
  uint32_t prev_index = random_generator_.Generate()%(local_count);
  ASSERT_TRUE(prev_index > 0);
  LogOffset prev_offset(append_entries[prev_index - 1].GetLogOffset());
  std::vector<replication::LogEntry> picked_append_entries(append_entries.begin() + prev_index,
                                                           append_entries.end());

  Waiter w(1);
  replication::DiskClosure* closure = new FakeDiskClosure(prev_offset, 0, Status::OK(), &w, nullptr, nullptr);
  auto s = log_manager_->AppendReplicaLog(PeerID(), picked_append_entries/*remote append logs*/, closure);
  if (!s.ok()) {
    closure->set_status(s);
    closure->Run();
    delete closure;
  }
  w.Wait();
  ASSERT_TRUE(s.ok()) << "AppendReplicaLog failed " << s.ToString();

  LogOffset new_last_offset = log_manager_->GetLastStableOffset(true, nullptr);
  const auto& append_last_offset = append_entries.back().GetLogOffset();
  ASSERT_EQ(new_last_offset.l_offset, append_last_offset.l_offset)
            << "After append entries, the returned last offset " << new_last_offset.l_offset.ToString()
            << " does not match the declared offset " << append_last_offset.l_offset.ToString();
}

TEST_F(TestClusterLogManager, AppendRemoteLogsWithConflicts) {
  // 1. Prepare local logs
  std::vector<Attributes> local_entry_attrs;
  AppendOptions options;
  options.term = 0;
  options.entry_attrs = &local_entry_attrs;
  AppendLogs(options);
  if (HasFatalFailure()) return;
  // local index [0, local_count) with term = 0.
  size_t local_count = local_entry_attrs.size();

  Attributes attrs;
  std::vector<replication::LogEntry> append_entries;
  uint32_t term = options.term;
  size_t remote_index = 1;
  // old_term_number in range [1, local_count)
  size_t old_term_number = 1 + random_generator_.Generate()%(local_count-1);
  size_t remote_count = local_count + kAppendLogsLowerBoundary;
  // remote index
  // [1, old_term_number] with term = 0
  // [old_term_number + 1, remote_count] with term = 1
  DLOG(INFO) << "remote index range from " << 1 <<  " to " << old_term_number << " with term 0"
             << ", range from " << old_term_number + 1 <<  " to " << remote_count << " with term 1";
  for (; remote_index <= remote_count; remote_index++) {
    attrs.term_id = remote_index <= old_term_number ? term : term + 1;
    attrs.logic_id = remote_index;
    std::string data = remote_index <= old_term_number ? ZeroBinaryData(kAppendLogsSize) : RandomAlphaData(kAppendLogsSize);
    replication::LogEntry entry(attrs, std::move(data));
    append_entries.push_back(std::move(entry));
  }
  // Copy the append_entries for later comparison
  std::vector<replication::LogEntry> saved_append_entries(append_entries.begin(), append_entries.end());
  const auto& append_last_offset = append_entries.back().GetLogOffset();

  Waiter w(1);
  replication::DiskClosure* closure = new FakeDiskClosure(LogOffset(), 0, Status::OK(), &w, nullptr, nullptr);
  auto s = log_manager_->AppendReplicaLog(PeerID(), append_entries/*remote append logs*/, closure);
  if (!s.ok()) {
    closure->set_status(s);
    closure->Run();
    delete closure;
  }
  w.Wait();
  ASSERT_TRUE(w.status().ok()) << w.status().ToString();
  ASSERT_TRUE(s.ok()) << "AppendReplicaLog failed " << s.ToString();

  LogOffset new_last_offset = log_manager_->GetLastStableOffset(true, nullptr);
  ASSERT_EQ(new_last_offset.l_offset, append_last_offset.l_offset)
    << "After append entries, the returned last offset " << new_last_offset.l_offset.ToString()
    << " does not match the declared offset " << append_last_offset.l_offset.ToString();

  // After append, logs in local should equal to remote,
  // the conflict logs should have be discarded.
  std::vector<replication::MemLog::LogItem> mem_logs;
  s = log_manager_->PurgeAllMemLogs(&mem_logs);
  ASSERT_TRUE(s.ok()) << "PurgeAllMemLogs failed " << s.ToString();
  ASSERT_EQ(mem_logs.size(), remote_count)
    << "The size of Logs in memory does not match"
    << ", expected " << remote_count << ", actual " << mem_logs.size();

  // compare with memory log
  for (size_t i = 0; i < remote_count; i++) {
    ASSERT_PRED_FORMAT2(AssertLogicOffsetEqual, saved_append_entries[i].attributes(), mem_logs[i].attrs);
    ASSERT_THAT(saved_append_entries[i].data(), ::testing::StrEq(mem_logs[i].task->log()))
      << "log data in index " << mem_logs[i].attrs.logic_id << " do not matched"
      << ", remote: " << saved_append_entries[i].data() << ", local: " << mem_logs[i].task->log();
  }

  // compare with stable log.
  const auto& start_offset = saved_append_entries.front().GetLogOffset();
  auto log_reader = log_manager_->GetLogReader(start_offset);
  ASSERT_TRUE(log_reader != nullptr);
  std::vector<std::string> get_log_data;
  std::vector<Attributes> get_log_attrs;
  std::string item_data;
  Attributes item_attrs;
  LogOffset item_end_offset;
  std::string decoded_data;
  while (1) {
    s = log_reader->GetLog(&item_data, &item_attrs, &item_end_offset);
    if (s.IsEndFile()) {
      break;
    }
    ASSERT_TRUE(s.ok()) << s.ToString();
    get_log_data.push_back(item_data);
    get_log_attrs.push_back(item_attrs);
  }
  ASSERT_EQ(get_log_data.size(), get_log_attrs.size());
  ASSERT_EQ(get_log_data.size(), saved_append_entries.size());

  for (size_t i = 0; i < remote_count; i++) {
    ASSERT_PRED_FORMAT2(AssertLogicOffsetEqual, saved_append_entries[i].attributes(), get_log_attrs[i]);
    ASSERT_THAT(saved_append_entries[i].data(), ::testing::StrEq(get_log_data[i]))
      << "log data in index " << get_log_attrs[i].logic_id << " do not matched"
      << ", remote: " << saved_append_entries[i].data() << ", local: " << get_log_data[i];
  }
}

TEST_F(TestClusterLogManager, LogReader) {
  std::vector<Attributes> local_entry_attrs;
  std::vector<std::string> local_entry_data;
  AppendOptions options;
  options.term = 0;
  options.entry_attrs = &local_entry_attrs;
  options.entry_data = &local_entry_data;
  AppendLogs(options);
  if (HasFatalFailure()) return;
  ASSERT_EQ(local_entry_data.size(), local_entry_attrs.size());

  const auto& first_entry_attrs = local_entry_attrs.front();
  LogOffset start_offset(BinlogOffset(), LogicOffset(first_entry_attrs.term_id, first_entry_attrs.logic_id));
  auto log_reader = log_manager_->GetLogReader(start_offset);
  ASSERT_TRUE(log_reader != nullptr);

  std::vector<std::string> get_log_data;
  std::vector<Attributes> get_log_attrs;
  std::string item_data;
  Attributes item_attrs;
  LogOffset item_end_offset;
  while (1) {
    auto s = log_reader->GetLog(&item_data, &item_attrs, &item_end_offset);
    if (s.IsEndFile()) {
      break;
    }
    ASSERT_TRUE(s.ok()) << s.ToString();
    get_log_data.push_back(item_data);
    get_log_attrs.push_back(item_attrs);
  }

  ASSERT_EQ(get_log_data.size(), get_log_attrs.size());
  ASSERT_EQ(get_log_data.size(), local_entry_data.size());

  const size_t entry_size = local_entry_data.size();
  for (size_t i = 0; i < entry_size; i++) {
    ASSERT_PRED_FORMAT2(AssertLogicOffsetEqual, get_log_attrs[i], local_entry_attrs[i]);
    ASSERT_THAT(get_log_data[i], ::testing::StrEq(local_entry_data[i]))
      << "log data in index " << get_log_attrs[i].logic_id << " do not matched"
      << ", got: " << get_log_data[i] << ", stored: " << local_entry_data[i];
  }

  //uint64_t lag = 0;
  //auto s = log_reader->GetLag(item_end_offset, &lag);
  //ASSERT_EQ(lag, 0);
}
 
} // namepsace test

int main(int argc, char** argv) {
  test::test_env = static_cast<test::BinlogEnv*>(
      ::testing::AddGlobalTestEnvironment(new test::BinlogEnv(argc, argv)));
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
