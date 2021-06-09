// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <cstdlib>
#include <vector>
#include <string>
#include <iterator>
#include <memory>
#include <set>
#include <tuple>

#include "gtest/gtest.h"
#include "gmock/gmock.h"
#include "slash/include/slash_status.h"

#include "proto/pika_inner_message.pb.h"
#include "include/pika_define.h"
#include "include/util/callbacks.h"
#include "include/util/message_decoder.h"
#include "include/util/message_function.h"
#include "include/deps/replication_mock.h"
#include "include/deps/replication_env.h"
#include "include/replication/classic/pika_classic_repl_manager.h"
#include "include/replication/classic/pika_classic_rg_node.h"

namespace test {

using slash::Status;
using namespace util;

ReplManagerEnv* test_env;

class ClassicRGNodeSuite {
 public:
  ClassicRGNodeSuite(const PeerID& local_id,
                     const std::string& log_path);
  ~ClassicRGNodeSuite();

  std::shared_ptr<replication::ClassicRGNode> GetReplicationGroupNode(
      const ReplicationGroupID& group_id);
  Status RemoveReplicationGroupNode(const ReplicationGroupID& group_id);
  const PeerID& local_id() const { return local_id_; }
  ::testing::NiceMock<MockStateMachine>* state_machine() { return state_machine_; }
  std::shared_ptr<::testing::NiceMock<MockTransporter>> transporter() { return transporter_; }
  std::shared_ptr<::testing::NiceMock<MockReplicationOptionsStorage>> options() { return options_; }
  replication::ClassicReplManager* rm() { return rm_; }

 private:
  PeerID local_id_;
  std::string log_path_;
  ::testing::NiceMock<MockReplicationGroupNodeMetaStorage>* meta_storage_;
  ::testing::NiceMock<MockStateMachine>* state_machine_;
  std::shared_ptr<::testing::NiceMock<MockTransporter>> transporter_;
  std::shared_ptr<::testing::NiceMock<MockReplicationOptionsStorage>> options_;
  replication::ClassicReplManager* rm_;
};

ClassicRGNodeSuite::ClassicRGNodeSuite(const PeerID& local_id,
                                       const std::string& log_path)
  : local_id_(local_id),
  log_path_(log_path),
  meta_storage_(nullptr),
  state_machine_(nullptr),
  transporter_(nullptr),
  options_(nullptr),
  rm_(nullptr) {
  meta_storage_ = new ::testing::NiceMock<MockReplicationGroupNodeMetaStorage>();
  state_machine_ = new ::testing::NiceMock<MockStateMachine>();
  transporter_ = std::make_shared<::testing::NiceMock<MockTransporter>>();
  options_ = std::make_shared<::testing::NiceMock<MockReplicationOptionsStorage>>();

  ON_CALL(*options_, classic_mode())
    .WillByDefault(::testing::Return(true));
  ON_CALL(*options_, sync_window_size())
    .WillByDefault(::testing::Return(10));
  ON_CALL(*options_, local_id())
    .WillByDefault([local_id] { return local_id; });
  ON_CALL(*options_, log_path())
    .WillByDefault([log_path] { return log_path; });
  ON_CALL(*options_, sync_send_timeout_ms())
    .WillByDefault(::testing::Return(30*1000/*30s*/));
  ON_CALL(*options_, sync_receive_timeout_ms())
    .WillByDefault(::testing::Return(30*1000/*30s*/));

  replication::ReplicationManagerOptions rm_options(static_cast<replication::StateMachine*>(state_machine_),
      std::dynamic_pointer_cast<replication::Transporter>(transporter_),
      std::dynamic_pointer_cast<replication::ReplicationOptionsStorage>(options_));
  rm_ = new replication::ClassicReplManager(rm_options);
}

ClassicRGNodeSuite::~ClassicRGNodeSuite() {
  if (meta_storage_ != nullptr) {
    delete meta_storage_;
    meta_storage_ = nullptr;
  }
  if (state_machine_ != nullptr) {
    delete state_machine_;
    state_machine_ = nullptr;
  }
  if (rm_ != nullptr) {
    delete rm_;
    rm_ = nullptr;
  }
}

std::shared_ptr<replication::ClassicRGNode> ClassicRGNodeSuite::GetReplicationGroupNode(
    const ReplicationGroupID& group_id) {
  replication::RGNodeInitializeConfig init_config;
  init_config.meta_storage = meta_storage_;
  replication::GroupIDAndInitConfMap id_init_conf_map{{group_id, init_config}};
  Status s = rm_->CreateReplicationGroupNodes(id_init_conf_map);
  if (!s.ok()) {
    return nullptr;
  }
  return std::dynamic_pointer_cast<replication::ClassicRGNode>(
      rm_->GetReplicationGroupNode(group_id));
}

Status ClassicRGNodeSuite::RemoveReplicationGroupNode(const ReplicationGroupID& group_id) {
  return rm_->RemoveReplicationGroupNodes({group_id});
}

struct LogOptions {
  constexpr static size_t kDefaultSizePerLogItem = 64/*bytes*/;
  size_t init_log_number = 10;
  size_t size_per_log_item = kDefaultSizePerLogItem;
  size_t purge_logs_num = 0;
  std::string ToString() const;
  LogOptions() = default;
  LogOptions(size_t _init_log_number,
             size_t _purge_logs_num = 0,
             size_t _size_per_log_item = kDefaultSizePerLogItem)
    : init_log_number(_init_log_number),
    size_per_log_item(_size_per_log_item),
    purge_logs_num(_purge_logs_num) { }
};

std::string LogOptions::ToString() const {
  return "init_log_number: " + std::to_string(init_log_number)
         + ", size_per_log_item: " + std::to_string(size_per_log_item)
         + ", purge_logs_num: " + std::to_string(purge_logs_num);
}

class ClassicRGNodeTest : public ::testing::Test {
 public:
  ClassicRGNodeTest()
    : local_id_(PeerID(test_env->local_ip(), test_env->local_port())),
    log_path_(test_env->binlog_dir()) {}
  ~ClassicRGNodeTest() = default;

  void SetUp() override {
    MakeDir(log_path_);
  }
  void TearDown() override {
    RemoveDir(log_path_);
  }
 protected:
  void FetchMessageString(std::shared_ptr<replication::ClassicRGNode>& node,
                          bool is_request, std::string& message);
  // Appeng logs to the node according to the options set in init_log_options,
  // @param log_items: the logs in log_items will be append to the node, and
  //                   if the size of log_items < init_log_options.init_log_number,
  //                   new random logs will be constructed and recorded to the end 
  //                   of log_items.
  Status InitLogsForRGNode(const ReplicationGroupID& group_id,
                           const LogOptions& init_log_options,
                           std::shared_ptr<replication::ClassicRGNode> node,
                           std::vector<std::string>& log_items);

  // Establish master-slave relationship between nodes, the master and slave should
  // have completed the SnapshotSync if success, otherwise fatal failure will be raised.
  void ConstructSnapshotSync(ClassicRGNodeSuite& master_rg_suite,
                             ClassicRGNodeSuite& slave_rg_suite,
                             std::shared_ptr<replication::ClassicRGNode>& master_node,
                             std::shared_ptr<replication::ClassicRGNode>& slave_node);

  // Establish master-slave relationship between nodes, the master and slave should
  // have completed the BinlogSync if success, otherwise fatal failure will be raised.
  void ConstructBinlogSync(ClassicRGNodeSuite& master_rg_suite,
                           ClassicRGNodeSuite& slave_rg_suite,
                           std::shared_ptr<replication::ClassicRGNode>& master_node,
                           std::shared_ptr<replication::ClassicRGNode>& slave_node);
  void MakeDir(const std::string& dir) {
    std::string cmd_line = "mkdir " + dir;
    system(cmd_line.c_str());
  }
  void RemoveDir(const std::string& dir) {
    std::string cmd_line = "rm -rf " + dir;
    system(cmd_line.c_str());
  }

  const PeerID local_id_;
  std::string log_path_;
};

void ClassicRGNodeTest::FetchMessageString(std::shared_ptr<replication::ClassicRGNode>& node,
    bool is_request, std::string& message) {
  replication::PeerMessageListMap peer_msgs;
  node->Messages(peer_msgs);
  const auto& peer_msg_pair = peer_msgs.begin();
  ASSERT_EQ(peer_msg_pair->second.size(), 1);
  auto& peer_msg = peer_msg_pair->second.front();
  if (is_request) {
    ASSERT_EQ(peer_msg.type, replication::MessageType::kRequest);
  } else {
    ASSERT_EQ(peer_msg.type, replication::MessageType::kResponse);
  }

  auto s = peer_msg.builder->Build(&message);
  ASSERT_TRUE(s.ok()) << s.ToString();
}

Status ClassicRGNodeTest::InitLogsForRGNode(const ReplicationGroupID& group_id,
    const LogOptions& init_log_options, std::shared_ptr<replication::ClassicRGNode> node,
    std::vector<std::string>& log_items) {
  std::vector<std::string> new_logs;
  for (size_t i = 0; i < init_log_options.init_log_number; i++) {
    std::string log;
    if (i < log_items.size()) {
     log = log_items[i];
    } else {
     log = RandomAlphaData(init_log_options.size_per_log_item);
     new_logs.push_back(log);
    }
    auto task = std::make_shared<ReplTask>(std::move(log), nullptr, group_id, PeerID(),
                  ReplTask::Type::kClientType,
                  ReplTask::LogFormat::kPB);
    node->Propose(std::move(task));
  }
  if (log_items.size() > init_log_options.init_log_number) {
    log_items.erase(log_items.begin() + init_log_options.init_log_number, log_items.end());
    return Status::OK();
  }
  log_items.insert(log_items.end(), new_logs.begin(), new_logs.end());
  if (log_items.size() > 0 && init_log_options.purge_logs_num > 0) {
    // filenum start from zero.
    uint32_t purged_to_filenum = static_cast<uint32_t>(init_log_options.purge_logs_num) - 1;
    if (!node->IsSafeToBePurged(purged_to_filenum) || node->IsPurging()) {
      return Status::Corruption("It's should be safe to purge log");
    }
    std::vector<uint32_t> purged_indexes;
    auto done = new ::testing::NiceMock<MockPurgeLogsClosure>(group_id);
    EXPECT_CALL(*done, run())
      .WillOnce([&purged_indexes, done] () { purged_indexes = done->PurgedFileIndexes(); });
    replication::PurgeLogArg* arg = new replication::PurgeLogArg();
    arg->expire_logs_days = 7;
    arg->expire_logs_nums = 1024;
    arg->done = done;
    arg->node = node;
    arg->force = true;
    arg->to = purged_to_filenum;
    arg->manual = true;
    replication::ReplicationGroupNode::DoPurgeStableLogs(arg);
    (purged_indexes.size(), init_log_options.purge_logs_num);
    std::stringstream ss;
    for (const auto& index : purged_indexes) {
      ss << index << " ";
    }
    DLOG(INFO) << "Replication group: " << group_id.ToString()
               << ", purged filenum indexes: " << ss.str();
    if (purged_indexes.size() != init_log_options.purge_logs_num) {
      return Status::Corruption("Purged filenum " + std::to_string(purged_indexes.size())
            + " does not equal to required number " + std::to_string(init_log_options.purge_logs_num));
    }
  }
  return Status::OK();
}

void ClassicRGNodeTest::ConstructSnapshotSync(
    ClassicRGNodeSuite& master_rg_suite,
    ClassicRGNodeSuite& slave_rg_suite,
    std::shared_ptr<replication::ClassicRGNode>& master_node,
    std::shared_ptr<replication::ClassicRGNode>& slave_node) {
  // Set master to slave
  Status s = slave_node->SetMaster(master_node->local_id(), true/*force_full_sync*/);
  ASSERT_TRUE(s.ok()) << s.ToString();
  {
    std::stringstream ss;
    bool in_syncing = slave_node->GetSlaveSyncState(ss);
    ASSERT_FALSE(in_syncing);
    ASSERT_THAT(ss.str(), ::testing::ContainsRegex(std::string("TrySnapshotSync")));
  }
  EXPECT_CALL(*(slave_rg_suite.state_machine()), OnSnapshotSyncStart(::testing::_))
    .WillOnce(::testing::Return(Status::OK()));
  slave_node->StepStateMachine();
  {
    std::stringstream ss;
    bool in_syncing = slave_node->GetSlaveSyncState(ss);
    ASSERT_FALSE(in_syncing);
    ASSERT_THAT(ss.str(), ::testing::ContainsRegex(std::string("TrySnapshotSyncSent")));
  }

  // Slave should have sent SnapshotSyncRequest to master
  std::string snapshot_sync_request_str;
  FetchMessageString(slave_node, true/*is_request*/, snapshot_sync_request_str);
  if (HasFatalFailure()) return;
  ASSERT_FALSE(snapshot_sync_request_str.empty());

  std::string snapshot_sync_response_str;
  auto snapshot_sync_closure = new ::testing::NiceMock<MockServerClosure>();
  EXPECT_CALL(*snapshot_sync_closure, run())
    .WillOnce([&snapshot_sync_response_str, snapshot_sync_closure](){
          snapshot_sync_response_str = snapshot_sync_closure->ReleaseResponse();
        });

  // Master handle SnapshotSyncRequest
  auto snapshot_sync_request = DecodeClassicMessage(snapshot_sync_request_str,
      slave_node->local_id(), InnerMessage::ClassicMessage::kRequestType,
      static_cast<replication::IOClosure*>(snapshot_sync_closure));
  ASSERT_TRUE(snapshot_sync_request != nullptr);
  master_rg_suite.rm()->HandlePeerMessage(snapshot_sync_request);
  ASSERT_FALSE(snapshot_sync_response_str.empty());

  // Slave handle SnapshotSyncResponse
  auto client_closure = new ::testing::NiceMock<MockClientClosure>();
  auto snapshot_sync_response = DecodeClassicMessage(snapshot_sync_response_str,
      master_node->local_id(), InnerMessage::ClassicMessage::kResponseType,
      static_cast<replication::IOClosure*>(client_closure));
  ASSERT_TRUE(snapshot_sync_response != nullptr);
  slave_rg_suite.rm()->HandlePeerMessage(snapshot_sync_response);
  {
    std::stringstream ss;
    bool in_syncing = slave_node->GetSlaveSyncState(ss);
    ASSERT_FALSE(in_syncing);
    ASSERT_THAT(ss.str(), ::testing::ContainsRegex(std::string("WaitSnapshotReceived")));
  }

  // Advance Slave to indicate snapshot has been received
  auto master_pstate = master_node->NodePersistentState();
  replication::Ready ready;
  ready.snapshot_offset = master_pstate.applied_offset;
  ready.applied_offset = master_pstate.applied_offset;
  ready.db_reloaded = true;
  ready.db_reloaded_error = false;
  slave_node->Advance(ready);
  {
    std::stringstream ss;
    bool in_syncing = slave_node->GetSlaveSyncState(ss);
    ASSERT_FALSE(in_syncing);
    ASSERT_THAT(ss.str(), ::testing::ContainsRegex(std::string("TrySync")));
  }
};

void ClassicRGNodeTest::ConstructBinlogSync(
    ClassicRGNodeSuite& master_rg_suite,
    ClassicRGNodeSuite& slave_rg_suite,
    std::shared_ptr<replication::ClassicRGNode>& master_node,
    std::shared_ptr<replication::ClassicRGNode>& slave_node) {
  // Set master to slave
  Status s = slave_node->SetMaster(master_node->local_id(), false/*force_full_sync*/);
  ASSERT_TRUE(s.ok()) << s.ToString();
  {
    std::stringstream ss;
    bool in_syncing = slave_node->GetSlaveSyncState(ss);
    ASSERT_FALSE(in_syncing);
    ASSERT_THAT(ss.str(), ::testing::ContainsRegex(std::string("TrySync")));
  }
  slave_node->StepStateMachine();
  {
    std::stringstream ss;
    bool in_syncing = slave_node->GetSlaveSyncState(ss);
    ASSERT_FALSE(in_syncing);
    ASSERT_THAT(ss.str(), ::testing::ContainsRegex(std::string("TrySyncSent")));
  }

  // Slave should have sent TrySyncRequest to master
  std::string try_sync_request_str;
  FetchMessageString(slave_node, true/*is_request*/, try_sync_request_str);
  if (HasFatalFailure()) return;
  ASSERT_FALSE(try_sync_request_str.empty());

  std::string try_sync_response_str;
  auto try_sync_closure = new ::testing::NiceMock<MockServerClosure>();
  EXPECT_CALL(*try_sync_closure, run())
    .WillOnce([&try_sync_response_str, try_sync_closure](){
          try_sync_response_str = try_sync_closure->ReleaseResponse();
        });

  // Master handle TrySyncRequest
  auto try_sync_request = DecodeClassicMessage(try_sync_request_str,
      slave_node->local_id(), InnerMessage::ClassicMessage::kRequestType,
      static_cast<replication::IOClosure*>(try_sync_closure));
  ASSERT_TRUE(try_sync_request != nullptr);
  master_rg_suite.rm()->HandlePeerMessage(try_sync_request);
  ASSERT_FALSE(try_sync_response_str.empty());

  // Slave handle TrySyncResponse
  auto client_closure = new ::testing::NiceMock<MockClientClosure>();
  auto try_sync_response = DecodeClassicMessage(try_sync_response_str,
      master_node->local_id(), InnerMessage::ClassicMessage::kResponseType,
      static_cast<replication::IOClosure*>(client_closure));
  ASSERT_TRUE(try_sync_response != nullptr);
  slave_rg_suite.rm()->HandlePeerMessage(try_sync_response);
  {
    std::stringstream ss;
    bool in_syncing = slave_node->GetSlaveSyncState(ss);
    ASSERT_TRUE(in_syncing);
  }

  // Slave should have sent BinlogSyncRequest to master
  std::string binlog_sync_request_str;
  FetchMessageString(slave_node, true/*is_request*/, binlog_sync_request_str);
  if (HasFatalFailure()) return;
  ASSERT_FALSE(binlog_sync_request_str.empty());

  // Master handle BinlogSyncRequest 
  auto binlog_sync_closure = new ::testing::NiceMock<MockServerClosure>();
  auto binlog_sync_request = DecodeClassicMessage(binlog_sync_request_str,
      slave_node->local_id(), InnerMessage::ClassicMessage::kRequestType,
      static_cast<replication::IOClosure*>(binlog_sync_closure));
  ASSERT_TRUE(binlog_sync_request != nullptr);
  master_rg_suite.rm()->HandlePeerMessage(binlog_sync_request);
  ASSERT_FALSE(binlog_sync_closure->ShouldClose());
};

TEST_F(ClassicRGNodeTest, InitializeWithoutPersistentContext) {
  ClassicRGNodeSuite rg_suite(local_id_, log_path_);
  const ReplicationGroupID group_id("table1", 1);
  auto node = rg_suite.GetReplicationGroupNode(group_id);
  ASSERT_TRUE(node != nullptr);
  Status s = node->Start();
  ASSERT_TRUE(s.ok()) << s.ToString();
  auto p_state = node->NodePersistentState();
  ASSERT_EQ(node->group_id(), group_id);
  ASSERT_EQ(node->local_id(), local_id_);
  ASSERT_TRUE(p_state.committed_offset.Empty());
  ASSERT_TRUE(p_state.applied_offset.Empty());
  ASSERT_TRUE(p_state.snapshot_offset.Empty());
  auto v_state = node->NodeVolatileState();
  ASSERT_TRUE(v_state.leader_id.Empty());
}

TEST_F(ClassicRGNodeTest, SetMaster) {
  ClassicRGNodeSuite rg_suite(local_id_, log_path_);
  const ReplicationGroupID group_id("table1", 1);
  auto node = rg_suite.GetReplicationGroupNode(group_id);
  ASSERT_TRUE(node != nullptr);
  Status s = node->Start();
  ASSERT_TRUE(s.ok()) << s.ToString();
  auto v_state = node->NodeVolatileState();
  ASSERT_TRUE(v_state.leader_id.Empty());

  const PeerID master_id(local_id_.Ip(), local_id_.Port() + 1024);
  {
    s = node->SetMaster(master_id, false/*force_full_sync*/);
    ASSERT_TRUE(s.ok()) << s.ToString();
    v_state = node->NodeVolatileState();
    ASSERT_EQ(v_state.leader_id, master_id);
    std::stringstream ss;
    bool in_syncing = node->GetSlaveSyncState(ss);
    ASSERT_FALSE(in_syncing);
    ASSERT_THAT(ss.str(), ::testing::ContainsRegex(std::string("TrySync")));
  }
  {
    s = node->SetMaster(master_id, true/*force_full_sync*/);
    ASSERT_TRUE(s.ok()) << s.ToString();
    v_state = node->NodeVolatileState();
    ASSERT_EQ(v_state.leader_id, master_id);
    std::stringstream ss;
    bool in_syncing = node->GetSlaveSyncState(ss);
    ASSERT_FALSE(in_syncing);
    ASSERT_THAT(ss.str(), ::testing::ContainsRegex(std::string("TrySnapshotSync")));
  }
  {
    const PeerID new_master_id(master_id.Ip(), master_id.Port() + 1024);
    s = node->SetMaster(new_master_id, true/*force_full_sync*/);
    ASSERT_TRUE(!s.ok());
    v_state = node->NodeVolatileState();
    ASSERT_EQ(v_state.leader_id, master_id);
  }
}

TEST_F(ClassicRGNodeTest, RemoveMaster) {
  ClassicRGNodeSuite rg_suite(local_id_, log_path_);
  const ReplicationGroupID group_id("table1", 1);
  auto node = rg_suite.GetReplicationGroupNode(group_id);
  ASSERT_TRUE(node != nullptr);
  Status s = node->Start();
  ASSERT_TRUE(s.ok()) << s.ToString();
  auto v_state = node->NodeVolatileState();
  ASSERT_TRUE(v_state.leader_id.Empty());

  const PeerID master_1(local_id_.Ip(), local_id_.Port() + 1024);
  const PeerID master_2(master_1.Ip(), master_1.Port() + 1024);
  // 1. remove master_1
  s = node->RemoveMaster(master_1);
  ASSERT_TRUE(!s.ok());

  // 2. set master to master_1
  s = node->SetMaster(master_1, false/*force_full_sync*/);
  ASSERT_TRUE(s.ok()) << s.ToString();
  v_state = node->NodeVolatileState();
  ASSERT_EQ(v_state.leader_id, master_1);

  // 3. remove master_2
  s = node->RemoveMaster(master_2);
  ASSERT_TRUE(!s.ok());
  v_state = node->NodeVolatileState();
  ASSERT_EQ(v_state.leader_id, master_1);

  // 4. remove master_1
  s = node->RemoveMaster(master_1);
  ASSERT_TRUE(s.ok());
  v_state = node->NodeVolatileState();
  ASSERT_TRUE(v_state.leader_id.Empty());
  std::stringstream ss;
  bool in_syncing = node->GetSlaveSyncState(ss);
  ASSERT_FALSE(in_syncing);
  ASSERT_THAT(ss.str(), ::testing::ContainsRegex(std::string("NoConnect")));
}

TEST_F(ClassicRGNodeTest, ResetMaster) {
  ClassicRGNodeSuite rg_suite(local_id_, log_path_);
  const ReplicationGroupID group_id("table1", 1);
  auto node = rg_suite.GetReplicationGroupNode(group_id);
  ASSERT_TRUE(node != nullptr);
  Status s = node->Start();
  ASSERT_TRUE(s.ok()) << s.ToString();
  auto v_state = node->NodeVolatileState();
  ASSERT_TRUE(v_state.leader_id.Empty());

  const PeerID master_1(local_id_.Ip(), local_id_.Port() + 1024);
  const PeerID master_2(master_1.Ip(), master_1.Port() + 1024);

  // 1. set master to master_1
  s = node->SetMaster(master_1, false/*force_full_sync*/);
  ASSERT_TRUE(s.ok()) << s.ToString();
  v_state = node->NodeVolatileState();
  ASSERT_EQ(v_state.leader_id, master_1);

  // 2. set master to master_2
  s = node->ResetMaster(master_2, false/*force_full_sync*/);
  ASSERT_TRUE(s.ok()) << s.ToString();
  v_state = node->NodeVolatileState();
  ASSERT_EQ(v_state.leader_id, master_2);
  // SendRemoveSlaveNodeRequest should be send to master_1
  replication::PeerMessageListMap peer_msgs;
  node->Messages(peer_msgs);
  const auto& peer_msg_pair = peer_msgs.begin();
  ASSERT_EQ(peer_msg_pair->second.size(), 1);
  auto& peer_msg = peer_msg_pair->second.front();
  ASSERT_EQ(peer_msg.type, replication::MessageType::kRequest);
  ASSERT_EQ(peer_msg_pair->first, master_1);
}

TEST_F(ClassicRGNodeTest, EnableAndDisableReplication) {
  const PeerID slave_id(local_id_);
  const PeerID master_id_1(slave_id.Ip(), slave_id.Port() + 1024);
  const PeerID master_id_2(slave_id.Ip(), slave_id.Port() + 2048);
  const ReplicationGroupID group_id("table1", 1);

  const std::string slave_log_path = log_path_ + "slave/";
  MakeDir(slave_log_path);
  ClassicRGNodeSuite slave_rg_suite(slave_id, slave_log_path);
  auto slave_node = slave_rg_suite.GetReplicationGroupNode(group_id);
  ASSERT_TRUE(slave_node != nullptr);
  Status s = slave_node->Start();
  ASSERT_TRUE(s.ok()) << s.ToString();

  // set master to master_id_1
  s = slave_node->SetMaster(master_id_1, false/*force_full_sync*/);
  ASSERT_TRUE(s.ok()) << s.ToString();
  auto v_state = slave_node->NodeVolatileState();
  ASSERT_EQ(v_state.leader_id, master_id_1);
  {
    std::stringstream ss;
    bool in_syncing = slave_node->GetSlaveSyncState(ss);
    ASSERT_FALSE(in_syncing);
    ASSERT_THAT(ss.str(), ::testing::ContainsRegex(std::string("TrySync")));
  }

  // disable replication, slave should have sent RemoveNodeRequest to master
  s = slave_node->DisableReplication();
  ASSERT_TRUE(s.ok()) << s.ToString();

  std::string remove_node_request_str;
  FetchMessageString(slave_node, true/*is_request*/, remove_node_request_str);
  if (HasFatalFailure()) return;
  ASSERT_FALSE(remove_node_request_str.empty());
  {
    std::stringstream ss;
    bool in_syncing = slave_node->GetSlaveSyncState(ss);
    ASSERT_FALSE(in_syncing);
    ASSERT_THAT(ss.str(), ::testing::ContainsRegex(std::string("NoConnect")));
  }

  // set master to master_id_2, slave should have sent RemoveNodeRequest to master
  s = slave_node->ResetMaster(master_id_2, false/*force_full_sync*/);
  ASSERT_FALSE(s.ok());
  FetchMessageString(slave_node, true/*is_request*/, remove_node_request_str);
  if (HasFatalFailure()) return;
  ASSERT_FALSE(remove_node_request_str.empty());
  v_state = slave_node->NodeVolatileState();
  ASSERT_EQ(v_state.leader_id, master_id_2);
  {
    std::stringstream ss;
    bool in_syncing = slave_node->GetSlaveSyncState(ss);
    ASSERT_FALSE(in_syncing);
    ASSERT_THAT(ss.str(), ::testing::ContainsRegex(std::string("NoConnect")));
  }

  // enable replication
  s = slave_node->EnableReplication(true/*force_full_sync*/, false, 0, 0);
  ASSERT_TRUE(s.ok()) << s.ToString();
  {
    std::stringstream ss;
    bool in_syncing = slave_node->GetSlaveSyncState(ss);
    ASSERT_FALSE(in_syncing);
    ASSERT_THAT(ss.str(), ::testing::ContainsRegex(std::string("TrySnapshotSync")));
  }
}

TEST_F(ClassicRGNodeTest, EstablishReplicationWhenMasterOwnSnapshotRecord) {
  // CASE: master owns snapshot_offset, which indicates the master
  //       was previously built from full synchronization(SnapshotSync)
  //       as a slave.
  //
  //       The following scenarios can lead to this situation:
  //       1) NodeB slaveof NodeA with full sync.
  //       2) NodeB lost connection with NodeA.
  //       3) Append data to NodeA.
  //       4) NodeC slaveof NodeA with full sync.
  //       5) NodeA is gone, NodeB is recover.
  //       6) NodeC become the master
  //       7) NodeB slaveof NodeC.
  //
  //       logs in NodeB should less than NodeC, and the last_offset in NodeB may present in NodeC,
  //       but the related point in NodeC is a padding entry(not a user entry). SnapshotSync should
  //       be constructed instead of BinlogSync in this case.
  //
  //       This situation will result in NodeB not being able to establish a master-slave relationship 
  //       with NodeC, which is a bug in old releases(<=3.4.0).
  
  const PeerID node_a_id(local_id_);
  const PeerID node_b_id(node_a_id.Ip(), node_a_id.Port() + 1024);
  const PeerID node_c_id(node_a_id.Ip(), node_a_id.Port() + 2048);
  const std::string node_a_log_path = log_path_ + "nodea/";
  const std::string node_b_log_path = log_path_ + "nodeb/";
  const std::string node_c_log_path = log_path_ + "nodec/";
  MakeDir(node_a_log_path);
  MakeDir(node_b_log_path);
  MakeDir(node_c_log_path);
  ClassicRGNodeSuite node_a_rg_suite(node_a_id, node_a_log_path);
  ClassicRGNodeSuite node_b_rg_suite(node_b_id, node_b_log_path);
  ClassicRGNodeSuite node_c_rg_suite(node_c_id, node_c_log_path);

  // setup: multi items for one log file
  ON_CALL(*(node_a_rg_suite.options()), binlog_file_size())
    .WillByDefault(::testing::Return(LogOptions::kDefaultSizePerLogItem*100));
  ON_CALL(*(node_b_rg_suite.options()), binlog_file_size())
    .WillByDefault(::testing::Return(LogOptions::kDefaultSizePerLogItem*100));
  ON_CALL(*(node_c_rg_suite.options()), binlog_file_size())
    .WillByDefault(::testing::Return(LogOptions::kDefaultSizePerLogItem*100));

  const ReplicationGroupID group_id("table1", 1);

  // 1) Start NodeA, and append some logs into NodeA
  const size_t InitLogNumber = 16;
  LogOptions node_a_log_options(InitLogNumber);
  std::vector<std::string> node_a_logs;
  auto node_a = node_a_rg_suite.GetReplicationGroupNode(group_id);
  ASSERT_TRUE(node_a != nullptr);
  Status s = node_a->Start();
  ASSERT_TRUE(s.ok()) << s.ToString();
  s = InitLogsForRGNode(group_id, node_a_log_options, node_a, node_a_logs);
  ASSERT_TRUE(s.ok()) << s.ToString();
  auto node_a_pstate = node_a->NodePersistentState();
  ASSERT_FALSE(node_a_pstate.committed_offset.Empty());
  ASSERT_FALSE(node_a_pstate.applied_offset.Empty());
  ASSERT_TRUE(node_a_pstate.snapshot_offset.Empty());
  ASSERT_EQ(node_a_pstate.committed_offset, node_a_pstate.applied_offset);

  // 2) Start NodeB, and do SnapshotSync between NodeB and NodeA
  auto node_b = node_b_rg_suite.GetReplicationGroupNode(group_id);
  ASSERT_TRUE(node_b != nullptr);
  s = node_b->Start();
  ASSERT_TRUE(s.ok()) << s.ToString();
  ConstructSnapshotSync(node_a_rg_suite, node_b_rg_suite, node_a/*master*/, node_b/*slave*/);
  if (HasFatalFailure()) return;
  auto node_b_pstate = node_b->NodePersistentState();
  ASSERT_FALSE(node_b_pstate.committed_offset.Empty());
  ASSERT_FALSE(node_b_pstate.applied_offset.Empty());
  ASSERT_EQ(node_b_pstate.snapshot_offset, node_b_pstate.applied_offset);
  ASSERT_EQ(node_b_pstate.applied_offset, node_a_pstate.applied_offset);

  // 3) Append more logs into NodeA
  const size_t AdditionalLogNum = 10;
  for (size_t i = 0; i < AdditionalLogNum; i++) {
    std::string log = RandomAlphaData(LogOptions::kDefaultSizePerLogItem);
    node_a_logs.push_back(log);
    auto task = std::make_shared<ReplTask>(std::move(log), nullptr, group_id, PeerID(),
                  ReplTask::Type::kClientType,
                  ReplTask::LogFormat::kPB);
    node_a->Propose(std::move(task));
  }
  node_a_pstate = node_a->NodePersistentState();

  // 4) Start NodeC, and do SnapshotSync between NodeC and NodeA
  auto node_c = node_c_rg_suite.GetReplicationGroupNode(group_id);
  ASSERT_TRUE(node_c != nullptr);
  s = node_c->Start();
  ASSERT_TRUE(s.ok()) << s.ToString();
  ConstructSnapshotSync(node_a_rg_suite, node_c_rg_suite, node_a/*master*/, node_c/*slave*/);
  if (HasFatalFailure()) return;
  auto node_c_pstate = node_c->NodePersistentState();
  ASSERT_FALSE(node_c_pstate.committed_offset.Empty());
  ASSERT_FALSE(node_c_pstate.applied_offset.Empty());
  ASSERT_EQ(node_c_pstate.applied_offset, node_c_pstate.snapshot_offset);
  ASSERT_EQ(node_c_pstate.applied_offset, node_a_pstate.applied_offset);
  ASSERT_GT(node_c_pstate.applied_offset, node_b_pstate.applied_offset);
  ASSERT_GT(node_c_pstate.snapshot_offset, node_b_pstate.snapshot_offset);

  // 5) Remove the current master of NodeB and NodeC, and set NodeC as the new master of NodeB.
  s = node_c->RemoveMaster(node_a->local_id());
  ASSERT_TRUE(s.ok()) << s.ToString();
  s = node_b->ResetMaster(node_c->local_id(), false/*force_full_sync*/);
  ASSERT_TRUE(s.ok()) << s.ToString();
  std::string remove_node_request_str;
  FetchMessageString(node_b, true/*is_request*/, remove_node_request_str);
  if (HasFatalFailure()) return;
  ASSERT_FALSE(remove_node_request_str.empty());
  {
    std::stringstream ss;
    bool in_syncing = node_b->GetSlaveSyncState(ss);
    ASSERT_FALSE(in_syncing);
    ASSERT_THAT(ss.str(), ::testing::ContainsRegex(std::string("TrySync")));
  }
  node_b->StepStateMachine();
  {
    std::stringstream ss;
    bool in_syncing = node_b->GetSlaveSyncState(ss);
    ASSERT_FALSE(in_syncing);
    ASSERT_THAT(ss.str(), ::testing::ContainsRegex(std::string("TrySyncSent")));
  }
  std::string try_sync_request_str;
  FetchMessageString(node_b, true/*is_request*/, try_sync_request_str);
  if (HasFatalFailure()) return;
  ASSERT_FALSE(try_sync_request_str.empty());

  std::string try_sync_response_str;
  auto try_sync_closure = new ::testing::NiceMock<MockServerClosure>();
  EXPECT_CALL(*try_sync_closure, run())
    .WillOnce([&try_sync_response_str, try_sync_closure](){
          try_sync_response_str = try_sync_closure->ReleaseResponse();
        });

  // NodeC handle TrySyncRequest
  auto try_sync_request = DecodeClassicMessage(try_sync_request_str,
      node_b->local_id(), InnerMessage::ClassicMessage::kRequestType,
      static_cast<replication::IOClosure*>(try_sync_closure));
  ASSERT_TRUE(try_sync_request != nullptr);
  node_c_rg_suite.rm()->HandlePeerMessage(try_sync_request);
  ASSERT_FALSE(try_sync_response_str.empty());

  // NodeB handle TrySyncResponse
  auto client_closure = new ::testing::NiceMock<MockClientClosure>();
  auto try_sync_response = DecodeClassicMessage(try_sync_response_str,
      node_c->local_id(), InnerMessage::ClassicMessage::kResponseType,
      static_cast<replication::IOClosure*>(client_closure));
  ASSERT_TRUE(try_sync_response != nullptr);
  node_b_rg_suite.rm()->HandlePeerMessage(try_sync_response);
  {
    std::stringstream ss;
    bool in_syncing = node_b->GetSlaveSyncState(ss);
    ASSERT_FALSE(in_syncing);
    ASSERT_THAT(ss.str(), ::testing::ContainsRegex(std::string("TrySnapshotSync")));
  }
}

TEST_F(ClassicRGNodeTest, StepStateMachine) {
  const uint64_t kSendTimeoutSeconds = 5;
  const uint64_t kRecvTimeoutSeconds = 20;

  const PeerID slave_id(local_id_);
  const PeerID master_id(slave_id.Ip(), slave_id.Port() + 1024);
  const std::string slave_log_path = log_path_ + "slave/";
  const std::string master_log_path = log_path_ + "master/";
  MakeDir(slave_log_path);
  MakeDir(master_log_path);
  ClassicRGNodeSuite slave_rg_suite(slave_id, slave_log_path);
  ClassicRGNodeSuite master_rg_suite(master_id, master_log_path);
  ON_CALL(*(slave_rg_suite.options()), sync_send_timeout_ms())
    .WillByDefault(::testing::Return(kSendTimeoutSeconds*1000));
  ON_CALL(*(slave_rg_suite.options()), sync_receive_timeout_ms())
    .WillByDefault(::testing::Return(kRecvTimeoutSeconds*1000));
  ON_CALL(*(master_rg_suite.options()), sync_send_timeout_ms())
    .WillByDefault(::testing::Return(kSendTimeoutSeconds*1000));
  ON_CALL(*(master_rg_suite.options()), sync_receive_timeout_ms())
    .WillByDefault(::testing::Return(kRecvTimeoutSeconds*1000));

  const ReplicationGroupID group_id("table1", 1);

  auto slave_node = slave_rg_suite.GetReplicationGroupNode(group_id);
  ASSERT_TRUE(slave_node != nullptr);
  Status s = slave_node->Start();
  ASSERT_TRUE(s.ok()) << s.ToString();
  auto master_node = master_rg_suite.GetReplicationGroupNode(group_id);
  ASSERT_TRUE(master_node != nullptr);
  s = master_node->Start();
  ASSERT_TRUE(s.ok()) << s.ToString();

  // 0. Append some data to master
  const size_t kLogNumber = 10;
  const size_t kLogSize = 64;
  std::vector<std::string> logs;
  logs.reserve(kLogNumber);
  for (size_t i = 0; i < kLogNumber; i++) {
    std::string log = RandomAlphaData(kLogSize);
    logs.push_back(log); // Copy to later check
    auto task = std::make_shared<ReplTask>(std::move(log), nullptr, group_id, PeerID(),
                  ReplTask::Type::kClientType,
                  ReplTask::LogFormat::kPB);
    master_node->Propose(std::move(task));
  }
  auto p_state = master_node->NodePersistentState();
  ASSERT_FALSE(p_state.committed_offset.Empty());
  ASSERT_FALSE(p_state.applied_offset.Empty());
  ASSERT_EQ(p_state.committed_offset, p_state.applied_offset);

  // 1. Set Master to master_id
  s = slave_node->SetMaster(master_id, false/*force_full_sync*/);
  ASSERT_TRUE(s.ok()) << s.ToString();
  auto v_state = slave_node->NodeVolatileState();
  ASSERT_EQ(v_state.leader_id, master_id);
  {
    std::stringstream ss;
    bool in_syncing = slave_node->GetSlaveSyncState(ss);
    ASSERT_FALSE(in_syncing);
    ASSERT_THAT(ss.str(), ::testing::ContainsRegex(std::string("TrySync")));
  }

  // 2 Step the state machine of slave
  slave_node->StepStateMachine();
  {
    std::stringstream ss;
    bool in_syncing = slave_node->GetSlaveSyncState(ss);
    ASSERT_FALSE(in_syncing);
    ASSERT_THAT(ss.str(), ::testing::ContainsRegex(std::string("TrySyncSent")));
  }
  // Slave should have sent TrySyncRequest to master
  std::string try_sync_request_str;
  FetchMessageString(slave_node, true/*is_request*/, try_sync_request_str);
  if (HasFatalFailure()) return;
  ASSERT_FALSE(try_sync_request_str.empty());

  std::string try_sync_response_str;
  auto try_sync_closure = new ::testing::NiceMock<MockServerClosure>();
  EXPECT_CALL(*try_sync_closure, run())
    .WillOnce([&try_sync_response_str, try_sync_closure](){
          try_sync_response_str = try_sync_closure->ReleaseResponse();
        });

  // Master handle TrySyncRequest
  auto try_sync_request = DecodeClassicMessage(try_sync_request_str,
      slave_node->local_id(), InnerMessage::ClassicMessage::kRequestType,
      static_cast<replication::IOClosure*>(try_sync_closure));
  ASSERT_TRUE(try_sync_request != nullptr);
  master_rg_suite.rm()->HandlePeerMessage(try_sync_request);
  {
    std::stringstream ss;
    master_node->GetSyncInfo(master_node->local_id(), ss);
    ASSERT_THAT(ss.str(), ::testing::ContainsRegex(slave_node->local_id().ToString()));
    ASSERT_THAT(ss.str(), ::testing::ContainsRegex(std::string("NotSync")));
  }
  ASSERT_FALSE(try_sync_response_str.empty());
  // Step the state machine of master
  //
  // Wait master-end send timeout, the heartbeat should be sent
  // as the slave has not switch to kFollowerBinlogSync.
  sleep(kSendTimeoutSeconds + 1); // exceed 1s
  master_node->StepStateMachine();
  {
    std::string heart_beat_str;
    FetchMessageString(master_node, false/*is_response*/, heart_beat_str);
    if (HasFatalFailure()) return;
    ASSERT_FALSE(heart_beat_str.empty());
  }

  // Slave Handle TrySyncResponse
  auto client_closure = new ::testing::NiceMock<MockClientClosure>();
  auto try_sync_response = DecodeClassicMessage(try_sync_response_str,
      master_node->local_id(), InnerMessage::ClassicMessage::kResponseType,
      static_cast<replication::IOClosure*>(client_closure));
  ASSERT_TRUE(try_sync_response != nullptr);
  slave_rg_suite.rm()->HandlePeerMessage(try_sync_response);
  {
    std::stringstream ss;
    bool in_syncing = slave_node->GetSlaveSyncState(ss);
    ASSERT_TRUE(in_syncing);
  }

  // Slave should have sent BinlogSyncRequest to master
  std::string binlog_sync_request_str;
  FetchMessageString(slave_node, true/*is_request*/, binlog_sync_request_str);
  if (HasFatalFailure()) return;
  ASSERT_FALSE(binlog_sync_request_str.empty());

  // Master handle BinlogSyncRequest
  auto binlog_sync_closure = new ::testing::NiceMock<MockServerClosure>();
  auto binlog_sync_request = DecodeClassicMessage(binlog_sync_request_str,
      slave_node->local_id(), InnerMessage::ClassicMessage::kRequestType,
      static_cast<replication::IOClosure*>(binlog_sync_closure));
  ASSERT_TRUE(binlog_sync_request != nullptr);
  master_rg_suite.rm()->HandlePeerMessage(binlog_sync_request);
  {
    // Slave should have changed to kFollowerBinlogSync state
    std::stringstream ss;
    master_node->GetSyncInfo(master_node->local_id(), ss);
    ASSERT_THAT(ss.str(), ::testing::ContainsRegex(slave_node->local_id().ToString()));
    ASSERT_THAT(ss.str(), ::testing::ContainsRegex(std::string("BinlogSync")));
  }
  ASSERT_FALSE(binlog_sync_closure->ShouldClose());

  // Master should have send BinlogSyncResponse to slave
  //master_node->StepStateMachine();
  std::string binlog_sync_response_str;
  FetchMessageString(master_node, false/*is_request*/, binlog_sync_response_str);
  if (HasFatalFailure()) return;
  ASSERT_FALSE(binlog_sync_response_str.empty());

  // Slave handle BinlogSyncResponse
  std::vector<replication::MemLog::LogItem> waitting_apply_logs;
  ON_CALL(*(slave_rg_suite.state_machine()), OnApply)
    .WillByDefault(::testing::WithArg<0>([&waitting_apply_logs]
          (std::vector<replication::MemLog::LogItem> logs) {
      waitting_apply_logs.insert(waitting_apply_logs.end(),
               std::make_move_iterator(logs.begin()),
               std::make_move_iterator(logs.end()));
    }));
  client_closure = new ::testing::NiceMock<MockClientClosure>();
  auto binlog_sync_response = DecodeClassicMessage(binlog_sync_response_str,
      master_node->local_id(), InnerMessage::ClassicMessage::kResponseType,
      static_cast<replication::IOClosure*>(client_closure));
  ASSERT_TRUE(binlog_sync_response != nullptr);
  slave_rg_suite.rm()->HandlePeerMessage(binlog_sync_response);
  {
    std::stringstream ss;
    bool in_syncing = slave_node->GetSlaveSyncState(ss);
    ASSERT_TRUE(in_syncing);
  }

  ASSERT_EQ(waitting_apply_logs.size(), logs.size());
  for (size_t i = 0; i < logs.size(); i++) {
    ASSERT_EQ(waitting_apply_logs[i].task->log(), logs[i]);
  }

  // Slave should have send BinlogSyncRequest to master
  binlog_sync_request_str.clear();
  FetchMessageString(slave_node, true/*is_request*/, binlog_sync_request_str);
  if (HasFatalFailure()) return;
  ASSERT_FALSE(binlog_sync_request_str.empty());
  // Master handle BinlogSyncRequest
  binlog_sync_closure = new ::testing::NiceMock<MockServerClosure>();
  binlog_sync_request = DecodeClassicMessage(binlog_sync_request_str,
      slave_node->local_id(), InnerMessage::ClassicMessage::kRequestType,
      static_cast<replication::IOClosure*>(binlog_sync_closure));
  ASSERT_TRUE(binlog_sync_request != nullptr);
  master_rg_suite.rm()->HandlePeerMessage(binlog_sync_request);
  ASSERT_FALSE(binlog_sync_closure->ShouldClose());
  {
    std::stringstream ss;
    master_node->GetSyncInfo(master_node->local_id(), ss);
    ASSERT_THAT(ss.str(), ::testing::ContainsRegex(slave_node->local_id().ToString()));
    ASSERT_THAT(ss.str(), ::testing::ContainsRegex(std::string("BinlogSync")));
  }
}

enum class ReplicateMode : uint8_t {
  kMasterAndSlaveMode = 0,
  kParitionMode       = 1,
};

enum class ConnectionEnd : uint8_t {
  kMasterEnd = 0,
  kSlaveEnd  = 1,
};

class ClassicRGNodeConnectionBrokenTest
  : public ClassicRGNodeTest,
    public ::testing::WithParamInterface<std::tuple<ReplicateMode, ConnectionEnd>> {
 public:
  void SetUp() override {
    std::tie(replicate_mode_, connection_end_) = GetParam();
    ClassicRGNodeTest::SetUp();
  }
  void TearDown() override {
    ClassicRGNodeTest::TearDown();
  }
 protected:
  ReplicateMode replicate_mode_;
  ConnectionEnd connection_end_;
};

TEST_P(ClassicRGNodeConnectionBrokenTest, ConnectionTimeout) {
  const PeerID slave_id(local_id_);
  const PeerID master_id(slave_id.Ip(), slave_id.Port() + 1024);
  const std::string slave_log_path = log_path_ + "slave/";
  const std::string master_log_path = log_path_ + "master/";
  MakeDir(slave_log_path);
  MakeDir(master_log_path);
  ClassicRGNodeSuite slave_rg_suite(slave_id, slave_log_path);
  ClassicRGNodeSuite master_rg_suite(master_id, master_log_path);

  const uint32_t SentTimeoutSeconds = 2;
  const uint32_t RecvTimeoutSeconds = 5;

  ON_CALL(*(slave_rg_suite.options()), classic_mode())
    .WillByDefault(::testing::Return(replicate_mode_ == ReplicateMode::kMasterAndSlaveMode));
  ON_CALL(*(slave_rg_suite.options()), sync_send_timeout_ms())
    .WillByDefault(::testing::Return(SentTimeoutSeconds * 1000));
  ON_CALL(*(slave_rg_suite.options()), sync_receive_timeout_ms())
    .WillByDefault(::testing::Return(RecvTimeoutSeconds * 1000));
  ON_CALL(*(master_rg_suite.options()), classic_mode())
    .WillByDefault(::testing::Return(replicate_mode_ == ReplicateMode::kMasterAndSlaveMode));
  ON_CALL(*(master_rg_suite.options()), sync_send_timeout_ms())
    .WillByDefault(::testing::Return(SentTimeoutSeconds * 1000));
  ON_CALL(*(master_rg_suite.options()), sync_receive_timeout_ms())
    .WillByDefault(::testing::Return(RecvTimeoutSeconds * 1000));

  const ReplicationGroupID group_id("table1", 1);
  MakeDir(slave_log_path + "log_" + group_id.TableName());
  MakeDir(master_log_path + "log_" + group_id.TableName());

  // 1) Start slave
  auto slave_node = slave_rg_suite.GetReplicationGroupNode(group_id);
  ASSERT_TRUE(slave_node != nullptr);
  Status s = slave_node->Start();
  ASSERT_TRUE(s.ok()) << s.ToString();
  // 2) Start master
  auto master_node = master_rg_suite.GetReplicationGroupNode(group_id);
  ASSERT_TRUE(master_node != nullptr);
  s = master_node->Start();
  ASSERT_TRUE(s.ok()) << s.ToString();
  // 3) ConstructBinlogSync between slave_node and master_node
  ConstructBinlogSync(master_rg_suite, slave_rg_suite, master_node, slave_node);
  if (HasFatalFailure()) return;

  if (connection_end_ == ConnectionEnd::kSlaveEnd) {
    // CASE 1: slave_end timeout, indicates master can not recieve the BinlogSyncRequest in time
    {
      std::stringstream ss;
      master_node->GetSyncInfo(master_node->local_id(), ss);
      ASSERT_THAT(ss.str(), ::testing::ContainsRegex(slave_node->local_id().ToString()));
      ASSERT_THAT(ss.str(), ::testing::ContainsRegex(std::string("BinlogSync")));
    }
    sleep(RecvTimeoutSeconds + 1);
    master_node->StepStateMachine();
    {
      std::stringstream ss;
      s = master_node->GetSyncInfo(slave_node->local_id(), ss);
      ASSERT_FALSE(s.ok());
    }
  } else {
    // CASE 2: master_end timeout, indicates slave can not recieve the BinlogSyncResponse in time
    {
      std::stringstream ss;
      bool in_syncing = slave_node->GetSlaveSyncState(ss);
      ASSERT_TRUE(in_syncing);
    }
    sleep(RecvTimeoutSeconds + 1);
    slave_node->StepStateMachine();
    {
      std::stringstream ss;
      bool in_syncing = slave_node->GetSlaveSyncState(ss);
      ASSERT_FALSE(in_syncing);
      ASSERT_THAT(ss.str(), ::testing::ContainsRegex(std::string("TrySync")));
    }
    slave_node->StepStateMachine();
    {
      std::stringstream ss;
      bool in_syncing = slave_node->GetSlaveSyncState(ss);
      ASSERT_FALSE(in_syncing);
      ASSERT_THAT(ss.str(), ::testing::ContainsRegex(std::string("TrySyncSent")));
    }
    std::string try_sync_request_str;
    FetchMessageString(slave_node, true/*is_request*/, try_sync_request_str);
    if (HasFatalFailure()) return;
    ASSERT_FALSE(try_sync_request_str.empty());
  }
}

TEST_P(ClassicRGNodeConnectionBrokenTest, EndUnreachable) {
  const PeerID slave_id(local_id_);
  const PeerID master_id(slave_id.Ip(), slave_id.Port() + 1024);
  const std::string slave_log_path = log_path_ + "slave/";
  const std::string master_log_path = log_path_ + "master/";
  MakeDir(slave_log_path);
  MakeDir(master_log_path);
  ClassicRGNodeSuite slave_rg_suite(slave_id, slave_log_path);
  ClassicRGNodeSuite master_rg_suite(master_id, master_log_path);

  const uint64_t kReconnectMasterIntervalMS = 5000/*5s*/;

  ON_CALL(*(slave_rg_suite.options()), classic_mode())
    .WillByDefault(::testing::Return(replicate_mode_ == ReplicateMode::kMasterAndSlaveMode));
  ON_CALL(*(slave_rg_suite.options()), reconnect_master_interval_ms())
    .WillByDefault(::testing::Return(kReconnectMasterIntervalMS));
  ON_CALL(*(master_rg_suite.options()), classic_mode())
    .WillByDefault(::testing::Return(replicate_mode_ == ReplicateMode::kMasterAndSlaveMode));

  const ReplicationGroupID group_id("table1", 1);
  MakeDir(slave_log_path + "log_" + group_id.TableName());
  MakeDir(master_log_path + "log_" + group_id.TableName());

  // 1) Start slave
  auto slave_node = slave_rg_suite.GetReplicationGroupNode(group_id);
  ASSERT_TRUE(slave_node != nullptr);
  Status s = slave_node->Start();
  ASSERT_TRUE(s.ok()) << s.ToString();
  // 2) Start master
  auto master_node = master_rg_suite.GetReplicationGroupNode(group_id);
  ASSERT_TRUE(master_node != nullptr);
  s = master_node->Start();
  ASSERT_TRUE(s.ok()) << s.ToString();
  // 3) ConstructBinlogSync between slave_node and master_node
  ConstructBinlogSync(master_rg_suite, slave_rg_suite, master_node, slave_node);
  if (HasFatalFailure()) return;

  if (connection_end_ == ConnectionEnd::kSlaveEnd) {
    // CASE 1: slave_end broken, master will drop the progress
    {
      std::stringstream ss;
      master_node->GetSyncInfo(master_node->local_id(), ss);
      ASSERT_THAT(ss.str(), ::testing::ContainsRegex(slave_node->local_id().ToString()));
      ASSERT_THAT(ss.str(), ::testing::ContainsRegex(std::string("BinlogSync")));
    }
    master_rg_suite.rm()->ReportTransportResult(PeerInfo(slave_node->local_id(), -1),
                                                replication::MessageReporter::ResultType::kFdClosed,
                                                replication::MessageReporter::DirectionType::kServer);
    {
      std::stringstream ss;
      s = master_node->GetSyncInfo(slave_node->local_id(), ss);
      ASSERT_FALSE(s.ok());
    }
  } else {
    // CASE 2: master_end broken
    slave_rg_suite.rm()->ReportTransportResult(PeerInfo(master_node->local_id(), -1),
                                               replication::MessageReporter::ResultType::kFdClosed,
                                               replication::MessageReporter::DirectionType::kClient);
    {
      std::stringstream ss;
      bool in_syncing = slave_node->GetSlaveSyncState(ss);
      ASSERT_FALSE(in_syncing);
      ASSERT_THAT(ss.str(), ::testing::ContainsRegex(std::string("NoConnect")));
    }
    sleep(kReconnectMasterIntervalMS/1000 + 1);
    // CASE 2-1: slave should reconnect to master.
    if (replicate_mode_ == ReplicateMode::kParitionMode) {
      slave_node->StepStateMachine();
      {
        std::stringstream ss;
        bool in_syncing = slave_node->GetSlaveSyncState(ss);
        ASSERT_FALSE(in_syncing);
        ASSERT_THAT(ss.str(), ::testing::ContainsRegex(std::string("TrySync")));
      }
      slave_node->StepStateMachine();
      {
        std::stringstream ss;
        bool in_syncing = slave_node->GetSlaveSyncState(ss);
        ASSERT_FALSE(in_syncing);
        ASSERT_THAT(ss.str(), ::testing::ContainsRegex(std::string("TrySyncSent")));
      }
    }
    // CASE 2-2: slave should do nothing.
    else {
      slave_node->StepStateMachine();
      std::stringstream ss;
      bool in_syncing = slave_node->GetSlaveSyncState(ss);
      ASSERT_FALSE(in_syncing);
      ASSERT_THAT(ss.str(), ::testing::ContainsRegex(std::string("NoConnect")));
    }
  }
}

INSTANTIATE_TEST_SUITE_P(TwoEndsBrokenInBothReplicateModes,
    ClassicRGNodeConnectionBrokenTest,
    ::testing::Combine(
      ::testing::Values(ReplicateMode::kMasterAndSlaveMode, ReplicateMode::kParitionMode)/*replicate mode*/,
      ::testing::Values(ConnectionEnd::kMasterEnd, ConnectionEnd::kSlaveEnd)/*end broken*/));

class ClassicRGNodeSyncPointTest
  : public ClassicRGNodeTest,
    public ::testing::WithParamInterface<std::tuple<LogOptions, LogOptions>> {
 public:
  void SetUp() override {
    std::tie(master_log_options_, slave_log_options_) = GetParam();
    ClassicRGNodeTest::SetUp();
  }
  void TearDown() override {
    ClassicRGNodeTest::TearDown();
  }

 protected:
  LogOptions master_log_options_;
  LogOptions slave_log_options_;
};

TEST_P(ClassicRGNodeSyncPointTest, MasterWithoutSnapshotRecord) {
  const PeerID slave_id(local_id_);
  const PeerID master_id(slave_id.Ip(), slave_id.Port() + 1024);
  const std::string slave_log_path = log_path_ + "slave/";
  const std::string master_log_path = log_path_ + "master/";
  MakeDir(slave_log_path);
  MakeDir(master_log_path);
  ClassicRGNodeSuite slave_rg_suite(slave_id, slave_log_path);
  ClassicRGNodeSuite master_rg_suite(master_id, master_log_path);

  // setup:
  // 1) one item for one log file
  // 2) retain no logs, logs can be all purged.
  ON_CALL(*(master_rg_suite.options()), binlog_file_size())
    .WillByDefault(::testing::Return(LogOptions::kDefaultSizePerLogItem));
  ON_CALL(*(master_rg_suite.options()), retain_logs_num())
    .WillByDefault(::testing::Return(0));
  ON_CALL(*(slave_rg_suite.options()), binlog_file_size())
    .WillByDefault(::testing::Return(LogOptions::kDefaultSizePerLogItem));
  ON_CALL(*(slave_rg_suite.options()), retain_logs_num())
    .WillByDefault(::testing::Return(0));

  const ReplicationGroupID group_id("table1", 1);

  std::vector<std::string> master_logs;
  auto master_node = master_rg_suite.GetReplicationGroupNode(group_id);
  ASSERT_TRUE(master_node != nullptr);
  Status s = master_node->Start();
  ASSERT_TRUE(s.ok()) << s.ToString();
  s = InitLogsForRGNode(group_id, master_log_options_, master_node, master_logs);
  ASSERT_TRUE(s.ok()) << s.ToString();

  // slave copy logs from master and adjust it according it's own LogOptions.
  std::vector<std::string> slave_logs(master_logs.begin(), master_logs.end());
  auto slave_node = slave_rg_suite.GetReplicationGroupNode(group_id);
  ASSERT_TRUE(slave_node != nullptr);
  s = slave_node->Start();
  ASSERT_TRUE(s.ok()) << s.ToString();
  s = InitLogsForRGNode(group_id, slave_log_options_, slave_node, slave_logs);
  ASSERT_TRUE(s.ok()) << s.ToString();

  // Set master to slave
  s = slave_node->SetMaster(master_id, false/*force_full_sync*/);
  ASSERT_TRUE(s.ok()) << s.ToString();
  slave_node->StepStateMachine();
  {
    std::stringstream ss;
    bool in_syncing = slave_node->GetSlaveSyncState(ss);
    ASSERT_FALSE(in_syncing);
    ASSERT_THAT(ss.str(), ::testing::ContainsRegex(std::string("TrySyncSent")));
  }

  // Slave should have sent TrySyncRequest to master
  std::string try_sync_request_str;
  FetchMessageString(slave_node, true/*is_request*/, try_sync_request_str);
  if (HasFatalFailure()) return;
  ASSERT_FALSE(try_sync_request_str.empty());

  std::string try_sync_response_str;
  auto try_sync_closure = new ::testing::NiceMock<MockServerClosure>();
  EXPECT_CALL(*try_sync_closure, run())
    .WillOnce([&try_sync_response_str, try_sync_closure](){
          try_sync_response_str = try_sync_closure->ReleaseResponse();
        });

  // Master handle TrySyncRequest
  auto try_sync_request = DecodeClassicMessage(try_sync_request_str,
      slave_node->local_id(), InnerMessage::ClassicMessage::kRequestType,
      static_cast<replication::IOClosure*>(try_sync_closure));
  ASSERT_TRUE(try_sync_request != nullptr);
  master_rg_suite.rm()->HandlePeerMessage(try_sync_request);
  ASSERT_FALSE(try_sync_response_str.empty());

  // Slave Handle TrySyncResponse
  auto client_closure = new ::testing::NiceMock<MockClientClosure>();
  auto try_sync_response = DecodeClassicMessage(try_sync_response_str,
      master_node->local_id(), InnerMessage::ClassicMessage::kResponseType,
      static_cast<replication::IOClosure*>(client_closure));
  ASSERT_TRUE(try_sync_response != nullptr);
  slave_rg_suite.rm()->HandlePeerMessage(try_sync_response);

  std::stringstream ss;
  bool in_syncing = slave_node->GetSlaveSyncState(ss);
  bool must_one = false;
  if (master_log_options_.init_log_number >= slave_log_options_.init_log_number) {
    // case_1: logs number in master larger than or equal to slave.
    if (master_log_options_.purge_logs_num <= 0) {
      ASSERT_TRUE(in_syncing);
      must_one = true;
    } else {
      if (master_log_options_.purge_logs_num >= slave_log_options_.init_log_number) {
        // case_1_1: logs in master are purged, slave does not own the purged logs.
        ASSERT_FALSE(in_syncing);
        ASSERT_THAT(ss.str(), ::testing::ContainsRegex(std::string("TrySnapshotSync")));
      } else {
        // case_1_2: logs in master are purged, slave own the purged logs.
        ASSERT_TRUE(in_syncing);
      }
      must_one = true;
    }
  } else if (master_log_options_.init_log_number < slave_log_options_.init_log_number) {
    // case_2: logs number in master smaller than slave.
    ASSERT_FALSE(in_syncing);
    ASSERT_THAT(ss.str(), ::testing::ContainsRegex(std::string("Error")));
    must_one = true;
  }
  ASSERT_TRUE(must_one);
}

INSTANTIATE_TEST_SUITE_P(SyncPointBetweenMasterAndSlave,
    ClassicRGNodeSyncPointTest,
    ::testing::Combine(
      ::testing::Values(LogOptions(5), LogOptions(10), LogOptions(3), LogOptions(8, 4))/*master options*/,
      ::testing::Values(LogOptions(3), LogOptions(10), LogOptions(5), LogOptions(0))/*slave options*/));

} // namespace test

int main(int argc, char** argv) {
  test::test_env = static_cast<test::ReplManagerEnv*>(
      ::testing::AddGlobalTestEnvironment(new test::ReplManagerEnv(argc, argv)));
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
