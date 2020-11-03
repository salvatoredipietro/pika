// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <unistd.h>
#include <memory>
#include <set>
#include <vector>
#include <atomic>
#include <sstream>
#include <thread>
#include <algorithm>
#include <unordered_set>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <google/protobuf/io/coded_stream.h>

#include "gtest/gtest.h"
#include "gmock/gmock.h"
#include "slash/include/slash_status.h"

#include "include/pika_define.h"
#include "include/deps/replication_mock.h"
#include "include/deps/replication_env.h"
#include "include/util/replication_util.h"
#include "include/util/message_function.h"
#include "include/replication/pika_repl_manager.h"
#include "include/replication/cluster/pika_cluster_rg_node.h"
#include "include/replication/cluster/pika_cluster_repl_manager.h"

namespace test {

ReplManagerEnv* test_env;

using slash::Status;
using TransferCancelCode = replication::LeaderTransferContext::CancelReason::Code;
using NodeSet = std::unordered_set<PeerID, hash_peer_id>;

class FakeClusterMetaStorage : public replication::ClusterMetaStorage {
 public:
  FakeClusterMetaStorage(const replication::Configuration& init_conf)
    : applied_offset_(),
    snapshot_offset_(),
    configuration_(init_conf),
    term_(0),
    voted_for_() { }

  virtual Status applied_offset(LogOffset* offset) override {
    *offset = applied_offset_;
    return Status::OK();
  }
  virtual Status set_applied_offset(const LogOffset& offset) override {
    if (offset.l_offset.index > applied_offset_.l_offset.index) {
      applied_offset_ = offset;
    }
    return Status::OK();
  }
  virtual Status snapshot_offset(LogOffset* offset) override {
    *offset = snapshot_offset_;
    return Status::OK();
  }
  virtual Status set_snapshot_offset(const LogOffset& offset) override {
    if (offset.l_offset.index > snapshot_offset_.l_offset.index) {
      snapshot_offset_ = offset;
    }
    return Status::OK();
  }
  virtual Status configuration(replication::Configuration* conf) override {
    *conf = configuration_;
    return Status::OK();
  }
  virtual Status set_configuration(const replication::Configuration& conf) override {
    configuration_ = conf;
    return Status::OK();
  }
  virtual Status ApplyConfiguration(const replication::Configuration& conf, const LogOffset& offset) override {
    set_configuration(conf);
    set_applied_offset(offset);
    return Status::OK();
  }
  virtual Status MetaSnapshot(LogOffset* applied_offset, replication::Configuration* conf) override {
    *applied_offset = applied_offset_;
    *conf = configuration_;
    return Status::OK();
  }
  virtual Status SetTermAndVotedFor(const PeerID& voted_for,
                                    uint32_t term) override {
    term_ = term;
    voted_for_ = voted_for;
    return Status::OK();
  }
  virtual Status GetTermAndVotedFor(uint32_t* term, PeerID* voted_for) override {
    *term = term_;
    *voted_for = voted_for_;
    return Status::OK();
  }
  virtual Status set_term(uint32_t term) override {
    term_ = term;
    return Status::OK();
  }
  virtual Status term(uint32_t* term) override {
    *term = term_;
    return Status::OK();
  }
  virtual Status set_voted_for(const PeerID& voted_for) override {
    voted_for_ = voted_for;
    return Status::OK();
  }
  virtual Status voted_for(PeerID* peer_id) override {
    *peer_id = voted_for_;
    return Status::OK();
  }
  virtual Status ResetOffset(const LogOffset& snapshot_offset,
                             const LogOffset& applied_offset) override {
    snapshot_offset_ = snapshot_offset;
    applied_offset_ = applied_offset;
    return Status::OK();
  }
  virtual Status ResetOffsetAndConfiguration(const LogOffset& snapshot_offset,
                                             const LogOffset& applied_offset,
                                             const replication::Configuration& configuration) override {
    snapshot_offset_ = snapshot_offset;
    applied_offset_ = applied_offset;
    configuration_ = configuration;
    return Status::OK();
  }

 private:
  LogOffset applied_offset_;
  LogOffset snapshot_offset_;
  replication::Configuration configuration_;
  uint32_t term_;
  PeerID voted_for_;
};

class RGNodeSuites {
  friend class ReplicationGroup;
 public:
  using NodePtr = std::shared_ptr<replication::ClusterRGNode>;
  using RMPtr = std::shared_ptr<replication::ClusterReplManager>;
  using CommittedItems = std::vector<replication::MemLog::LogItem>;
  using NiceMockOptionsStore = ::testing::NiceMock<MockReplicationOptionsStorage>;
  using NiceMockStateMachine = ::testing::NiceMock<MockStateMachine>;
  using StateMachinePtr = NiceMockStateMachine*;
  using MetaStoragePtr = FakeClusterMetaStorage*;
  RGNodeSuites(const ReplicationGroupID& group_id,
               const std::shared_ptr<NiceMockOptionsStore>& options_store,
               FakeClusterMetaStorage* meta_storage);
  ~RGNodeSuites();
  const ReplicationGroupID& group_id() const { return group_id_; }

  NodePtr GetNodePtr();
  RMPtr GetRMPtr() const { return rm_; }
  StateMachinePtr GetStateMachinePtr() { return state_machine_; }
  MetaStoragePtr GetMetaStoragePtr() { return meta_storage_; }
  Status Start();
  Status Restart();
  Status Stop();

  struct SnapshotData {
    std::vector<LogicOffset> dummy_data_offset;
    std::vector<std::string> committed_data;
    std::vector<std::string> committed_conf;
    replication::Configuration configuration;
    LogOffset applied_offset;
  };
  void LoadSnapshotData(const SnapshotData& snapshot_data);
  SnapshotData CreateSnapshotData();
  SnapshotData GetSnapshotData() { return snapshot_data_; }
  const std::vector<LogicOffset>& dummy_data_offset() const { return dummy_data_offset_; }
  const std::vector<std::string>& committed_data() const { return committed_data_; }
  const std::vector<std::string>& committed_conf() const { return committed_conf_; }
  replication::Configuration configuration() const { return configuration_; }

 private:
  void Initialize();
  void SetUpStateMachine();
  Status ApplyConfChange(const InnerMessage::ConfChange& change);
  const ReplicationGroupID group_id_;

  std::atomic<bool> started_;
  std::vector<LogicOffset> dummy_data_offset_;
  std::vector<std::string> committed_data_;
  std::vector<std::string> committed_conf_;
  replication::Configuration configuration_;
  SnapshotData snapshot_data_;
  NiceMockStateMachine* state_machine_;
  FakeClusterMetaStorage* meta_storage_;
  std::shared_ptr<NiceMockOptionsStore> options_store_;
  std::shared_ptr<replication::ClusterReplManager> rm_;
};

RGNodeSuites::RGNodeSuites(const ReplicationGroupID& group_id,
                           const std::shared_ptr<NiceMockOptionsStore>& options_store,
                           FakeClusterMetaStorage* meta_storage)
  : group_id_(group_id), started_(false), state_machine_(new NiceMockStateMachine()),
  meta_storage_(meta_storage), options_store_(options_store), rm_(nullptr) {

  Initialize();
}

RGNodeSuites::~RGNodeSuites() {
  // stop RGNode
  Stop();
  // stop ReplManager
  rm_->Stop();
  if (meta_storage_ != nullptr) {
    delete meta_storage_;
    meta_storage_ = nullptr;
  }
  if (state_machine_ != nullptr) {
    delete state_machine_;
    state_machine_ = nullptr;
  }
}

void RGNodeSuites::Initialize() {
  SetUpStateMachine();

  replication::ReplicationManagerOptions rm_options(
      static_cast<replication::StateMachine*>(state_machine_), nullptr,
      std::static_pointer_cast<replication::ReplicationOptionsStorage>(options_store_));
  rm_ = std::make_shared<replication::ClusterReplManager>(rm_options);
  rm_->Start();
}

void RGNodeSuites::SetUpStateMachine() {
  // setup state_machine
  ON_CALL(*state_machine_, PurgeDir)
    .WillByDefault([](const std::string& log_remove){
      std::string cmd_line = "rm -rf " + log_remove;
      system(cmd_line.c_str());
    });

  ON_CALL(*state_machine_, PurgelogsTaskSchedule)
    .WillByDefault([this](void (*function)(void*), void* arg){
        rm_->Schedule(function, arg, "", 0);
    });

  ON_CALL(*state_machine_, OnApply)
    .WillByDefault([this] (std::vector<replication::MemLog::LogItem> logs) {
      auto node = GetNodePtr();
      ASSERT_TRUE(node != nullptr);
      auto apply_normal_entry = [this, node](replication::MemLog::LogItem&& item) {
        if (!item.task->log().empty()) {
          committed_data_.push_back(std::move(item.task->ReleaseLog()));
        } else {
          dummy_data_offset_.push_back(item.GetLogOffset().l_offset);
        }
        node->Advance(replication::Ready(item.GetLogOffset(), LogOffset(), false, false));
        DLOG(INFO) << "ApplyNormalEntry " << node->local_id().ToString()
                   << ", offset: " << item.GetLogOffset().ToString();
      };
      auto apply_conf_entry = [this, node](replication::MemLog::LogItem&& item) {
        const auto& log_slice = item.task->LogSlice();
        InnerMessage::BaseEntry entry;
        ::google::protobuf::io::ArrayInputStream input(log_slice.data(), log_slice.size());
        ::google::protobuf::io::CodedInputStream decoder(&input);
        ASSERT_TRUE(entry.ParseFromCodedStream(&decoder) && decoder.ConsumedEntireMessage());
        ASSERT_EQ(entry.type(), InnerMessage::EntryType::kEntryConfChange);
        auto conf_change = entry.conf_change();
        ApplyConfChange(conf_change);
        node->ApplyConfChange(conf_change);
        committed_conf_.push_back(std::move(item.task->ReleaseLog()));
        node->Advance(replication::Ready(item.GetLogOffset(), LogOffset(), false, false));
        DLOG(INFO) << "ApplyConfChange "<< node->local_id().ToString()
                   << ", offset: " << item.GetLogOffset().ToString();
      };
      for (auto&& log : logs) {
        ClosureGuard guard(log.task->closure());
        if (log.task->log_format() == ReplTask::LogFormat::kRedis) {
          apply_normal_entry(std::move(log));
        } else {
          apply_conf_entry(std::move(log));
        }
      }
    });
}

RGNodeSuites::NodePtr RGNodeSuites::GetNodePtr() {
  return std::dynamic_pointer_cast<replication::ClusterRGNode>(
      rm_->GetReplicationGroupNode(group_id_));
}

Status RGNodeSuites::Start() {
  if (started_.load(std::memory_order_acquire)) {
    return Status::Busy("Already started!");
  }
  replication::RGNodeInitializeConfig init_config;
  init_config.meta_storage = meta_storage_;
  replication::GroupIDAndInitConfMap id_init_conf_map{{group_id_, init_config}};
  Status s = rm_->CreateReplicationGroupNodes(id_init_conf_map);
  if (!s.ok()) {
    return s;
  }
  s = rm_->StartReplicationGroupNode(group_id_);
  return s;
}

Status RGNodeSuites::Restart() {
  if (started_.load(std::memory_order_acquire)) {
    return Status::Busy("Already started!");
  }
  Status s = rm_->StartReplicationGroupNode(group_id_);
  started_.store(true, std::memory_order_release);
  return s;
}

Status RGNodeSuites::Stop() {
  Status s = rm_->StopReplicationGroupNode(group_id_);
  if (!s.ok()) {
    return s;
  }
  started_.store(false, std::memory_order_release);
  return s;
}

RGNodeSuites::SnapshotData RGNodeSuites::CreateSnapshotData() {
  SnapshotData data;
  auto node = GetNodePtr();
  if (node == nullptr) {
    return data;
  }
  data.applied_offset = node->NodePersistentState().applied_offset;
  data.dummy_data_offset = dummy_data_offset_;
  data.committed_data = committed_data_;
  data.committed_conf = committed_conf_;
  data.configuration = configuration_;
  return data;
}

void RGNodeSuites::LoadSnapshotData(const SnapshotData& snapshot_data) {
  snapshot_data_ = snapshot_data;
  dummy_data_offset_ = snapshot_data.dummy_data_offset;
  committed_data_ = snapshot_data.committed_data;
  committed_conf_ = snapshot_data.committed_conf;
  configuration_ = snapshot_data.configuration;
}

Status RGNodeSuites::ApplyConfChange(const InnerMessage::ConfChange& change) {
  using ChangeResult = replication::Configuration::ChangeResult;
  ChangeResult change_result;
  PeerID node_id(change.node_id().ip(), change.node_id().port());
  replication::Configuration conf;
  switch (change.type()) {
    case InnerMessage::ConfChangeType::kAddVoter: {
      change_result = configuration_.AddPeer(node_id, replication::PeerRole::kRoleVoter);
      break;
    }
    case InnerMessage::ConfChangeType::kAddLearner: {
      change_result = configuration_.AddPeer(node_id, replication::PeerRole::kRoleLearner);
      break;
    }
    case InnerMessage::ConfChangeType::kRemoveNode: {
      change_result = configuration_.RemovePeer(node_id);
      break;
    }
    case InnerMessage::ConfChangeType::kPromoteLearner: {
      change_result = configuration_.PromotePeer(node_id);
      break;
    }
  }
  if (!change_result.IsOK()) {
    return Status::Corruption(change_result.ToString());
  }
  return Status::OK();
}

class ReplicationGroup {
 public:
  struct Options {
    struct PeerOptions {
      bool load_index_into_memory = false;
      bool check_quorum = false;
      bool pre_vote = false;
      uint64_t election_timeout_ms = 1000;
      uint64_t heartbeat_timeout_ms = 100;
    };
    PeerOptions peer_options;
    ReplicationGroupID group_id;
    replication::Configuration conf;
    std::string base_log_path;
  };
  ReplicationGroup(const Options& options);

  Status StartAllPeers();
  Status StartPeer(const PeerID& peer_id, bool as_learner = false,
                   bool as_alone = false, bool new_cluster = true,
                   Options::PeerOptions* options = nullptr);
  Status RestartPeer(const PeerID& peer_id);
  Status StopPeer(const PeerID& peer_id);
  Status RemovePeer(const PeerID& peer_id);

  RGNodeSuites::StateMachinePtr GetStateMachinePtr(const PeerID& peer_id);
  RGNodeSuites::MetaStoragePtr GetMetaStoragePtr(const PeerID& peer_id);
  RGNodeSuites::NodePtr GetNodePtr(const PeerID& peer_id);
  RGNodeSuites::RMPtr GetRMPtr(const PeerID& peer_id);
  std::vector<RGNodeSuites::NodePtr> GetAllNodePtrs();
  std::vector<std::string> GetNodeCommittedData(const PeerID& peer_id);
  RGNodeSuites::SnapshotData GetNodeSnapshotData(const PeerID& peer_id);
  RGNodeSuites::SnapshotData CreateNodeSnapshotData(const PeerID& peer_id);
  Status LoadNodeSnapshotData(const PeerID& peer_id, const RGNodeSuites::SnapshotData& snapshot_data);

  PeerID WaitCurrentLeader(const int64_t timeout_ms);
  std::vector<PeerID> GetCurrentFollowers();

  Status CheckStateMachinesEqual(const NodeSet& peer_ids);
  Status CheckSnapshotDataEqual(const NodeSet& peer_ids);

 private:
  void initialize();
  const Options options_;
  // peer_and_suites_ records the started peers.
  std::unordered_map<PeerID, std::unique_ptr<RGNodeSuites>, hash_peer_id>  peer_and_suites_;
};

ReplicationGroup::ReplicationGroup(const Options& options)
  : options_(options) { }

Status ReplicationGroup::StartAllPeers() {
  Status s;
  for (const auto& voter : options_.conf.Voters()) {
    s = StartPeer(voter, false, false);
    if (!s.ok()) {
      return s;
    }
  }
  for (const auto& learner : options_.conf.Learners()) {
    s = StartPeer(learner, true, false);
    if (!s.ok()) {
      return s;
    }
  }
  return s;
}

PeerID ReplicationGroup::WaitCurrentLeader(const int64_t timeout_ms) {
  constexpr static int64_t kSleepTimeMS = 100;
  PeerID leader_id;
  auto wait_start_time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch());
  while (1) {
    for (const auto& iter : peer_and_suites_) {
      auto state = iter.second->GetNodePtr()->NodeVolatileState();
      if (!state.leader_id.Empty()) {
        leader_id = state.leader_id;
        break;
      }
    }
    auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch());
    auto elapsed_time_ms = (now_ms - wait_start_time_ms).count();
    if (elapsed_time_ms >= timeout_ms) {
      break;
    }
    auto sleep_time_ms = timeout_ms - elapsed_time_ms;
    if (sleep_time_ms > kSleepTimeMS) {
      sleep_time_ms = kSleepTimeMS;
    }
    usleep(sleep_time_ms * 1000);
  }
  return leader_id;
}

std::vector<PeerID> ReplicationGroup::GetCurrentFollowers() {
  std::vector<PeerID> followers;
  for (const auto& iter : peer_and_suites_) {
    auto state = iter.second->GetNodePtr()->NodeVolatileState();
    if (state.role.code() == replication::RoleState::Code::kStateFollower) {
      followers.push_back(iter.first);
    }
  }
  return followers;
}

Status ReplicationGroup::RestartPeer(const PeerID& peer_id) {
  auto iter = peer_and_suites_.find(peer_id);
  if (iter == peer_and_suites_.end()) {
    return Status::Corruption(peer_id.ToString() + " has not been initialized, invoke StartPeer instead.");
  }
  return iter->second->Restart();
}

Status ReplicationGroup::StartPeer(const PeerID& peer_id, bool as_learner, bool as_alone,
                                   bool new_cluster, Options::PeerOptions* opts) {
  auto iter = peer_and_suites_.find(peer_id);
  if (iter != peer_and_suites_.end()) {
    return Status::Busy(peer_id.ToString() + " already started!");
  }

  auto start_role = as_learner ? replication::PeerRole::kRoleLearner
                    : replication::PeerRole::kRoleVoter;
  replication::Configuration init_conf;
  if (!new_cluster) {
    // when join a existing cluster, it doesn't need to start with configuration,
    // which will be replicated by the current leader.
  } else if (as_alone) {
    auto res = init_conf.AddPeer(peer_id, start_role);
    if (!res.IsOK()) {
      return Status::Corruption(res.ToString());
    }
  } else {
    init_conf = options_.conf;
    if (!init_conf.Contains(peer_id, start_role)) {
      return Status::Corruption("Not exist in configuration");
    }
  }
  auto options_store = std::make_shared<RGNodeSuites::NiceMockOptionsStore>();
  Options::PeerOptions options = opts != nullptr ? *opts : options_.peer_options;

  ON_CALL(*options_store, local_id())
    .WillByDefault([peer_id] { return peer_id; });
  ON_CALL(*options_store, load_index_into_memory())
    .WillByDefault([options] { return options.load_index_into_memory; });
  ON_CALL(*options_store, check_quorum())
    .WillByDefault([options] { return options.check_quorum; });
  ON_CALL(*options_store, pre_vote())
    .WillByDefault([options] { return options.pre_vote; });
  ON_CALL(*options_store, heartbeat_timeout_ms())
    .WillByDefault([options] { return options.heartbeat_timeout_ms; });
  ON_CALL(*options_store, election_timeout_ms())
    .WillByDefault([options] { return options.election_timeout_ms; });
  ON_CALL(*options_store, sync_window_size())
    .WillByDefault([] { return 1024; });
  ON_CALL(*options_store, sync_thread_num())
    .WillByDefault([] { return 4/*avoid dead lock*/; });
  ON_CALL(*options_store, disk_thread_num())
    .WillByDefault([] { return 4; });
  ON_CALL(*options_store, max_conn_rbuf_size())
    .WillByDefault([] { return 1024*1024*64/*64MB*/; });
  ON_CALL(*options_store, max_consumed_number_once())
    .WillByDefault([] { return 2000; });

  const auto& base_log_path = options_.base_log_path + std::to_string(peer_id.Port()) + "/";
  ON_CALL(*options_store, log_path())
    .WillByDefault([base_log_path] { return base_log_path; });

  auto meta_storage = new FakeClusterMetaStorage(init_conf);
  auto node_suites = std::unique_ptr<RGNodeSuites>(
      new RGNodeSuites(options_.group_id, options_store, meta_storage));
  auto s = node_suites->Start();
  if (!s.ok()) {
    return s;
  }
  std::pair<PeerID, std::unique_ptr<RGNodeSuites>> pair(peer_id, std::move(node_suites));
  peer_and_suites_.insert(std::move(pair));
  return Status::OK();
}

Status ReplicationGroup::RemovePeer(const PeerID& peer_id) {
  auto iter = peer_and_suites_.find(peer_id);
  if (iter == peer_and_suites_.end()) {
    return Status::NotFound(peer_id.ToString() + " has not been started");
  }
  peer_and_suites_.erase(peer_id);
  return Status::OK();
}

Status ReplicationGroup::StopPeer(const PeerID& peer_id) {
  auto iter = peer_and_suites_.find(peer_id);
  if (iter == peer_and_suites_.end()) {
    return Status::NotFound(peer_id.ToString() + " has not been started");
  }
  auto s = iter->second->Stop();
  return s;
}

RGNodeSuites::StateMachinePtr ReplicationGroup::GetStateMachinePtr(const PeerID& peer_id) {
  auto iter = peer_and_suites_.find(peer_id);
  if (iter == peer_and_suites_.end()) {
    return nullptr;
  }
  return iter->second->GetStateMachinePtr();
}

RGNodeSuites::MetaStoragePtr ReplicationGroup::GetMetaStoragePtr(const PeerID& peer_id) {
  auto iter = peer_and_suites_.find(peer_id);
  if (iter == peer_and_suites_.end()) {
    return nullptr;
  }
  return iter->second->GetMetaStoragePtr();
}

RGNodeSuites::NodePtr ReplicationGroup::GetNodePtr(const PeerID& peer_id) {
  auto iter = peer_and_suites_.find(peer_id);
  if (iter == peer_and_suites_.end()) {
    return nullptr;
  }
  return iter->second->GetNodePtr();
}

RGNodeSuites::RMPtr ReplicationGroup::GetRMPtr(const PeerID& peer_id) {
  auto iter = peer_and_suites_.find(peer_id);
  if (iter == peer_and_suites_.end()) {
    return nullptr;
  }
  return iter->second->GetRMPtr();
}

std::vector<RGNodeSuites::NodePtr> ReplicationGroup::GetAllNodePtrs() {
  std::vector<RGNodeSuites::NodePtr> node_ptrs;
  for (const auto& iter : peer_and_suites_) {
    node_ptrs.push_back(iter.second->GetNodePtr());
  }
  return node_ptrs;
}

std::vector<std::string> ReplicationGroup::GetNodeCommittedData(const PeerID& peer_id) {
  auto iter = peer_and_suites_.find(peer_id);
  if (iter == peer_and_suites_.end()) {
    return {};
  }
  return iter->second->committed_data();
}

RGNodeSuites::SnapshotData ReplicationGroup::GetNodeSnapshotData(const PeerID& peer_id) {
  auto iter = peer_and_suites_.find(peer_id);
  if (iter == peer_and_suites_.end()) {
    return RGNodeSuites::SnapshotData();
  }
  return iter->second->GetSnapshotData();
}

RGNodeSuites::SnapshotData ReplicationGroup::CreateNodeSnapshotData(const PeerID& peer_id) {
  auto iter = peer_and_suites_.find(peer_id);
  if (iter == peer_and_suites_.end()) {
    return RGNodeSuites::SnapshotData();
  }
  return iter->second->CreateSnapshotData();
}

Status ReplicationGroup::LoadNodeSnapshotData(const PeerID& peer_id, const RGNodeSuites::SnapshotData& snapshot_data) {
  auto iter = peer_and_suites_.find(peer_id);
  if (iter == peer_and_suites_.end()) {
    return Status::NotFound(peer_id.ToString());
  }
  iter->second->LoadSnapshotData(snapshot_data);
  return Status::OK();
}

Status ReplicationGroup::CheckStateMachinesEqual(const NodeSet& peer_ids) {
  if (peer_ids.size() <= 1) {
    return Status::OK();
  }
  const auto& first_peer_id = *peer_ids.begin();
  auto iter = peer_and_suites_.find(first_peer_id);
  if (iter == peer_and_suites_.end()) {
    return Status::NotFound(first_peer_id.ToString() + " not start!");
  }
  auto LogDataFunc = [] (const std::string& id, const std::vector<std::string>& data) {
    std::stringstream ss;
    ss << "size: " << std::to_string(data.size()) << "\n";
    for (size_t i = 0; i < data.size(); i++) {
      ss << "[" << std::to_string(i) << "]" <<  " "  << data[i] << "\n";
    }
    DLOG(INFO) << "ID: " << id << ", info: " << ss.str();
  };
  auto LogOffsetFunc = [] (const std::string& id, const std::vector<LogicOffset>& data) {
    std::stringstream ss;
    ss << "size: " << std::to_string(data.size()) << "\n";
    for (size_t i = 0; i < data.size(); i++) {
      ss << "[" << std::to_string(i) << "]" <<  " "  << data[i].ToString() << "\n";
    }
    DLOG(INFO) << "ID: " << id << ", info: " << ss.str();
  };
  const auto& first_committed_data = iter->second->committed_data();
  const auto& first_dummy_data_offset = iter->second->dummy_data_offset();
  const auto& first_committed_conf = iter->second->committed_conf();
  for (const auto& peer_id : peer_ids) {
    if (peer_id == first_peer_id) {
      continue;
    }
    auto iter = peer_and_suites_.find(peer_id);
    if (iter == peer_and_suites_.end()) {
      return Status::NotFound(peer_id.ToString() + " not start!");
    }
    const auto& committed_data = iter->second->committed_data();
    if (!std::equal(first_committed_data.begin(), first_committed_data.end(), committed_data.begin())) {
      LogDataFunc(first_peer_id.ToString(), first_committed_data);
      LogDataFunc(peer_id.ToString(), committed_data);
      return Status::Corruption(first_peer_id.ToString() + " not equal to " + peer_id.ToString());
    }
    const auto& dummy_data_offset = iter->second->dummy_data_offset();
    if (!std::equal(first_dummy_data_offset.begin(), first_dummy_data_offset.end(), dummy_data_offset.begin())) {
      LogOffsetFunc(first_peer_id.ToString(), first_dummy_data_offset);
      LogOffsetFunc(peer_id.ToString(), dummy_data_offset);
      return Status::Corruption(first_peer_id.ToString() + " not equal to " + peer_id.ToString());
    }
    const auto& committed_conf = iter->second->committed_conf();
    if (!std::equal(first_committed_conf.begin(), first_committed_conf.end(), committed_conf.begin())) {
      LogDataFunc(first_peer_id.ToString(), first_committed_conf);
      LogDataFunc(peer_id.ToString(), committed_conf);
      return Status::Corruption(first_peer_id.ToString() + " not equal to " + peer_id.ToString());
    }
  }
  return Status::OK();
}

class TestClusterRGNode
  : public ::testing::Test, 
    public ::testing::WithParamInterface<bool> {
 protected:
  constexpr static int kPortStep = 5000;
  constexpr static replication::PeerRole kDefaultRole = replication::PeerRole::kRoleVoter;
  TestClusterRGNode()
    : local_id_(test_env->local_ip(), test_env->local_port()),
    log_path_(test_env->binlog_dir()) {}

  void SetUp() override {
    load_index_into_memory_ = GetParam();
  }

  void TearDown() override {
    std::string cmd_line = "rm -rf " + log_path_;
    system(cmd_line.c_str());
  }

  bool load_index_into_memory_;
  const PeerID local_id_;
  const std::string log_path_;
};

INSTANTIATE_TEST_SUITE_P(LoadIndexIntoMemory, TestClusterRGNode, ::testing::Bool());

constexpr replication::PeerRole TestClusterRGNode::kDefaultRole;

TEST_P(TestClusterRGNode, SingleNodeStartAndStop) {
  const uint64_t election_timeout_ms = 1000;
  const uint64_t heartbeat_timeout_ms = 100;
  const uint64_t wait_election_ms = 4 * election_timeout_ms;
  ReplicationGroup::Options options;
  options.peer_options.load_index_into_memory = load_index_into_memory_;
  options.peer_options.election_timeout_ms = election_timeout_ms;
  options.peer_options.heartbeat_timeout_ms = heartbeat_timeout_ms;
  options.group_id = ReplicationGroupID("db0", 1);
  options.base_log_path = log_path_;
  options.conf.AddPeer(local_id_, kDefaultRole);

  ReplicationGroup rg(options);
  auto s = rg.StartPeer(local_id_, false, true);
  ASSERT_TRUE(s.ok()) << s.ToString();

  auto node = rg.GetNodePtr(local_id_);
  ASSERT_TRUE(node != nullptr);

  usleep(wait_election_ms * 1000);

  auto state = node->NodeVolatileState();
  ASSERT_EQ(state.leader_id, local_id_);
  ASSERT_EQ(state.role.code(), replication::RoleState::Code::kStateLeader);

  // propose some tasks and wait for them to be applied.
  const int per_entry_size = 32;
  const size_t entry_number = 10;
  Waiter w(entry_number);
  std::vector<std::string> put_data;
  for (size_t i = 0; i < entry_number; i++) {
    std::string data = RandomAlphaData(per_entry_size);
    put_data.push_back(data);
    auto repl_task = std::make_shared<ReplTask>(std::move(data),
                                                new EntryClosure(Status::OK(), &w),
                                                options.group_id, local_id_,
                                                ReplTask::Type::kClientType,
                                                ReplTask::LogFormat::kRedis);
    node->Propose(repl_task);
  }
  w.Wait();

  const auto& committed_data = rg.GetNodeCommittedData(local_id_);
  ASSERT_TRUE(std::equal(put_data.begin(), put_data.end(), committed_data.begin()));

  rg.StopPeer(local_id_);
}

TEST_P(TestClusterRGNode, LoseQuorum) {
  const auto& node_1 = local_id_;
  PeerID node_2(node_1.Ip(), node_1.Port() + kPortStep);
  PeerID node_3(node_2.Ip(), node_2.Port() + kPortStep);

  const uint64_t election_timeout_ms = 200;

  ReplicationGroup::Options options;
  options.peer_options.load_index_into_memory = load_index_into_memory_;
  options.peer_options.election_timeout_ms = election_timeout_ms;
  options.conf.AddPeer(node_1, kDefaultRole);
  options.conf.AddPeer(node_2, kDefaultRole);
  options.conf.AddPeer(node_3, kDefaultRole);
  options.group_id = ReplicationGroupID("db0", 1);
  options.base_log_path = log_path_;
  ReplicationGroup rg(options);

  auto s = rg.StartPeer(node_1);
  ASSERT_TRUE(s.ok()) << s.ToString();

  auto node = rg.GetNodePtr(node_1);
  ASSERT_TRUE(node != nullptr);

  usleep(election_timeout_ms * 1000 * 3);
  
  auto state = node->NodeVolatileState();
  ASSERT_TRUE(state.leader_id.Empty());
  ASSERT_NE(state.role.code(), replication::RoleState::Code::kStateLeader);

  // propose some tasks and wait for them to be dropped.
  const int per_entry_size = 32;
  const size_t entry_number = 10;
  Waiter w(entry_number);
  for (size_t i = 0; i < entry_number; i++) {
    auto repl_task = std::make_shared<ReplTask>(RandomAlphaData(per_entry_size),
                                                new EntryClosure(Status::Corruption(""), &w),
                                                options.group_id, node_1, ReplTask::Type::kClientType,
                                                ReplTask::LogFormat::kRedis);
    node->Propose(repl_task);
  }
  w.Wait();
}

TEST_P(TestClusterRGNode, SteadyLeader) {
  // After leader election, leader should not change when the heartbeat is
  // sent at time.
  const uint64_t election_timeout_ms = 1000;
  const uint64_t wait_election_ms = 10 * election_timeout_ms;
  const uint64_t heartbeat_timeout_ms = 100;
  ReplicationGroup::Options options;
  options.peer_options.load_index_into_memory = load_index_into_memory_;
  options.peer_options.election_timeout_ms = election_timeout_ms;
  options.peer_options.heartbeat_timeout_ms = heartbeat_timeout_ms;
  options.group_id = ReplicationGroupID("db0", 1);
  options.base_log_path = log_path_;

  const int node_number = 3;
  NodeSet node_ids;
  for (int i = 0; i < node_number; i++) {
    PeerID node(local_id_.Ip(), local_id_.Port() + kPortStep*i);
    options.conf.AddPeer(node, kDefaultRole);
    node_ids.insert(std::move(node));
  }

  ReplicationGroup rg(options);
  auto s = rg.StartAllPeers();
  ASSERT_TRUE(s.ok()) << s.ToString();

  auto first_leader_id = rg.WaitCurrentLeader(wait_election_ms);
  ASSERT_TRUE(!first_leader_id.Empty());
  auto follower_ids = rg.GetCurrentFollowers();
  ASSERT_EQ(follower_ids.size(), node_number - 1);

  auto second_leader_id = rg.WaitCurrentLeader(wait_election_ms);
  ASSERT_TRUE(!second_leader_id.Empty());
  follower_ids = rg.GetCurrentFollowers();
  ASSERT_EQ(follower_ids.size(), node_number - 1);

  ASSERT_EQ(first_leader_id, second_leader_id);
}

TEST_P(TestClusterRGNode, TransferLeadership) {
  const uint64_t election_timeout_ms = 1000;
  const uint64_t wait_election_ms = 10 * election_timeout_ms;
  const uint64_t heartbeat_timeout_ms = 100;
  ReplicationGroup::Options options;
  options.peer_options.load_index_into_memory = load_index_into_memory_;
  options.peer_options.election_timeout_ms = election_timeout_ms;
  options.peer_options.heartbeat_timeout_ms = heartbeat_timeout_ms;
  options.group_id = ReplicationGroupID("db0", 1);
  options.base_log_path = log_path_;

  const int node_number = 3;
  NodeSet node_ids;
  for (int i = 0; i < node_number; i++) {
    PeerID node(local_id_.Ip(), local_id_.Port() + kPortStep*i);
    options.conf.AddPeer(node, kDefaultRole);
    node_ids.insert(std::move(node));
  }
  ReplicationGroup rg(options);
  auto s = rg.StartAllPeers();
  ASSERT_TRUE(s.ok()) << s.ToString();

  // wait leader election
  auto first_leader_id = rg.WaitCurrentLeader(wait_election_ms);
  ASSERT_TRUE(!first_leader_id.Empty());
  auto follower_ids = rg.GetCurrentFollowers();
  ASSERT_EQ(follower_ids.size(), node_number - 1);

  // propose a leaderTransfer task
  auto leader_rm = rg.GetRMPtr(first_leader_id);
  ASSERT_TRUE(leader_rm != nullptr);

  const auto& transferee = follower_ids[0];
  auto expected_s = Status::Corruption(
      replication::LeaderTransferContext::CancelReason::ToString(
        TransferCancelCode::kRoleChanged));
  Waiter w(1);
  leader_rm->LeaderTransfer(options.group_id, transferee,
                           new EntryClosure(expected_s, &w));
  w.WaitFor(wait_election_ms);
  ASSERT_TRUE(w.status().ok()) << w.status().ToString();

  auto second_leader_id = rg.WaitCurrentLeader(wait_election_ms);
  ASSERT_TRUE(!second_leader_id.Empty());

  ASSERT_EQ(second_leader_id, transferee);
  ASSERT_NE(second_leader_id, first_leader_id);
}

TEST_P(TestClusterRGNode, TransferLeadershipTimedout) {
  const uint64_t election_timeout_ms = 1000;
  const uint64_t wait_election_ms = 10 * election_timeout_ms;
  const uint64_t heartbeat_timeout_ms = 100;
  ReplicationGroup::Options options;
  options.peer_options.load_index_into_memory = load_index_into_memory_;
  options.peer_options.election_timeout_ms = election_timeout_ms;
  options.peer_options.heartbeat_timeout_ms = heartbeat_timeout_ms;
  options.group_id = ReplicationGroupID("db0", 1);
  options.base_log_path = log_path_;

  const int node_number = 2;
  NodeSet node_ids;
  for (int i = 0; i < node_number; i++) {
    PeerID node(local_id_.Ip(), local_id_.Port() + kPortStep*i);
    options.conf.AddPeer(node, kDefaultRole);
    node_ids.insert(std::move(node));
  }
  ReplicationGroup rg(options);
  auto s = rg.StartAllPeers();
  ASSERT_TRUE(s.ok()) << s.ToString();

  // wait leader election
  auto first_leader_id = rg.WaitCurrentLeader(wait_election_ms);
  ASSERT_TRUE(!first_leader_id.Empty());
  auto follower_ids = rg.GetCurrentFollowers();
  ASSERT_EQ(follower_ids.size(), node_number - 1);

  // propose some tasks
  const int per_entry_size = 32;
  const size_t entry_number = 2000;
  Waiter w(entry_number + 1);
  auto first_leader_node = rg.GetNodePtr(first_leader_id);
  for (size_t i = 0; i < entry_number; i++) {
    auto repl_task = std::make_shared<ReplTask>(RandomAlphaData(per_entry_size),
                                                new EntryClosure(Status::OK(), &w),
                                                options.group_id, first_leader_id,
                                                ReplTask::Type::kClientType,
                                                ReplTask::LogFormat::kRedis);
    first_leader_node->Propose(repl_task);
  }
  // propose a leaderTransfer task
  auto leader_rm = rg.GetRMPtr(first_leader_id);
  ASSERT_TRUE(leader_rm != nullptr);
  const auto& transferee = follower_ids[0];
  leader_rm->LeaderTransfer(options.group_id, transferee,
                            new EntryClosure(Status::Corruption(
                                 replication::LeaderTransferContext::CancelReason::ToString(
                                 TransferCancelCode::kTransferTimedout)), &w));
  // all pending entries should be replicated, and the transfer task 
  // should be canceled caused by the timeout.
  w.Wait();
  ASSERT_TRUE(w.status().ok()) << w.status().ToString();

  sleep(2);

  s = rg.CheckStateMachinesEqual(node_ids);
  ASSERT_TRUE(s.ok()) << s.ToString();
}

TEST_P(TestClusterRGNode, LeaderGoneAndRestart) {
  const uint64_t election_timeout_ms = 1000;
  const uint64_t wait_election_ms = 2 * election_timeout_ms;
  const uint64_t heartbeat_timeout_ms = 100;
  ReplicationGroup::Options options;
  options.peer_options.load_index_into_memory = load_index_into_memory_;
  options.peer_options.election_timeout_ms = election_timeout_ms;
  options.peer_options.heartbeat_timeout_ms = heartbeat_timeout_ms;
  options.group_id = ReplicationGroupID("db0", 1);
  options.base_log_path = log_path_;

  const int node_number = 3;
  NodeSet node_ids;
  for (int i = 0; i < node_number; i++) {
    PeerID node(local_id_.Ip(), local_id_.Port() + kPortStep*i);
    options.conf.AddPeer(node, kDefaultRole);
    node_ids.insert(std::move(node));
  }
  ReplicationGroup rg(options);
  auto s = rg.StartAllPeers();
  ASSERT_TRUE(s.ok()) << s.ToString();

  // wait leader election
  auto first_leader_id = rg.WaitCurrentLeader(wait_election_ms);
  ASSERT_TRUE(!first_leader_id.Empty());
  auto follower_ids = rg.GetCurrentFollowers();
  ASSERT_EQ(follower_ids.size(), node_number - 1);

  // propose some tasks and wait for them to be applied.
  auto first_leader_node = rg.GetNodePtr(first_leader_id);
  const int per_entry_size = 32;
  const size_t entry_number = 10;
  Waiter w(entry_number);
  for (size_t i = 0; i < entry_number; i++) {
    auto repl_task = std::make_shared<ReplTask>(RandomAlphaData(per_entry_size),
                                                new EntryClosure(Status::OK(), &w),
                                                options.group_id, first_leader_id,
                                                ReplTask::Type::kClientType,
                                                ReplTask::LogFormat::kRedis);
    first_leader_node->Propose(repl_task);
  }
  w.Wait();

  sleep(1); // sleep 1s to wait for all followers to catch up
  s = rg.CheckStateMachinesEqual(node_ids);
  ASSERT_TRUE(s.ok()) << s.ToString();

  // stop the leader
  s = rg.StopPeer(first_leader_id);
  ASSERT_TRUE(s.ok()) << s.ToString();

  // wait for the new leader
  auto second_leader_id = rg.WaitCurrentLeader(wait_election_ms);
  ASSERT_TRUE(!second_leader_id.Empty());

  ASSERT_NE(first_leader_id, second_leader_id);

  // propose some more tasks and wait for them to be applied
  auto second_leader_node = rg.GetNodePtr(second_leader_id);
  w.Reset(entry_number);
  for (size_t i = 0; i < entry_number; i++) {
    auto repl_task = std::make_shared<ReplTask>(RandomAlphaData(per_entry_size),
                                                new EntryClosure(Status::OK(), &w),
                                                options.group_id, second_leader_id,
                                                ReplTask::Type::kClientType,
                                                ReplTask::LogFormat::kRedis);
    second_leader_node->Propose(repl_task);
  }
  w.Wait();

  // first_leader_id restart
  s = rg.RestartPeer(first_leader_id);
  ASSERT_TRUE(s.ok()) << s.ToString();

  // sleep for a while to wait for the logs to be replicated to the old leader 
  sleep(5/*5s*/);

  s = rg.CheckStateMachinesEqual(node_ids);
  ASSERT_TRUE(s.ok()) << s.ToString();
}

TEST_P(TestClusterRGNode, AddVoter) {
  const uint64_t election_timeout_ms = 1000;
  const uint64_t wait_election_ms = 2 * election_timeout_ms;
  const uint64_t heartbeat_timeout_ms = 100;
  ReplicationGroup::Options options;
  options.peer_options.load_index_into_memory = load_index_into_memory_;
  options.peer_options.election_timeout_ms = election_timeout_ms;
  options.peer_options.heartbeat_timeout_ms = heartbeat_timeout_ms;
  options.group_id = ReplicationGroupID("db0", 1);
  options.base_log_path = log_path_;

  int node_number = 2;
  NodeSet node_ids;
  for (int i = 0; i < node_number; i++) {
    PeerID node(local_id_.Ip(), local_id_.Port() + kPortStep*i);
    options.conf.AddPeer(node, kDefaultRole);
    node_ids.insert(std::move(node));
  }
  ReplicationGroup rg(options);
  auto s = rg.StartAllPeers();
  ASSERT_TRUE(s.ok()) << s.ToString();

  // wait leader election
  auto leader_id = rg.WaitCurrentLeader(wait_election_ms);
  ASSERT_TRUE(!leader_id.Empty());
  auto follower_ids = rg.GetCurrentFollowers();
  ASSERT_EQ(follower_ids.size(), node_number - 1);

  // propose some tasks and wait for them to be applied
  auto leader_node = rg.GetNodePtr(leader_id);
  const int per_entry_size = 32;
  const size_t entry_number = 10;
  Waiter w(entry_number);
  for (size_t i = 0; i < entry_number; i++) {
    auto repl_task = std::make_shared<ReplTask>(RandomAlphaData(per_entry_size),
                                                new EntryClosure(Status::OK(), &w),
                                                options.group_id, leader_id,
                                                ReplTask::Type::kClientType,
                                                ReplTask::LogFormat::kRedis);
    leader_node->Propose(repl_task);
  }
  w.Wait();

  // propose an AddMember task
  const PeerID new_member(local_id_.Ip(), local_id_.Port() + kPortStep*node_number);
  node_number += 1;
  node_ids.insert(new_member);

  auto leader_rm = rg.GetRMPtr(leader_id);
  ASSERT_TRUE(leader_rm != nullptr);

  w.Reset(1);
  leader_rm->AddMember(options.group_id, new_member, replication::PeerRole::kRoleVoter,
                      new EntryClosure(Status::OK(), &w));
  w.Wait();

  // start the new member with empty configuration
  s = rg.StartPeer(new_member, false/*learner*/, false/*alone*/, false/*new_cluster*/);
  ASSERT_TRUE(s.ok()) << s.ToString();

  sleep(3/*3s*/);

  s = rg.CheckStateMachinesEqual(node_ids);
  ASSERT_TRUE(s.ok()) << s.ToString();

  // when the leader gone, the new leader should be elected
  s = rg.StopPeer(leader_id);
  ASSERT_TRUE(s.ok()) << s.ToString();

  auto new_leader_id = rg.WaitCurrentLeader(wait_election_ms);
  ASSERT_TRUE(!new_leader_id.Empty());
  follower_ids = rg.GetCurrentFollowers();
  ASSERT_EQ(follower_ids.size(), node_number - 2);

  ASSERT_NE(leader_id, new_leader_id);
}

TEST_P(TestClusterRGNode, AddLearner) {
  const uint64_t election_timeout_ms = 1000;
  const uint64_t wait_election_ms = 2 * election_timeout_ms;
  const uint64_t heartbeat_timeout_ms = 100;
  ReplicationGroup::Options options;
  options.peer_options.load_index_into_memory = load_index_into_memory_;
  options.peer_options.election_timeout_ms = election_timeout_ms;
  options.peer_options.heartbeat_timeout_ms = heartbeat_timeout_ms;
  options.group_id = ReplicationGroupID("db0", 1);
  options.base_log_path = log_path_;

  int node_number = 2;
  NodeSet node_ids;
  for (int i = 0; i < node_number; i++) {
    PeerID node(local_id_.Ip(), local_id_.Port() + kPortStep*i);
    options.conf.AddPeer(node, kDefaultRole);
    node_ids.insert(std::move(node));
  }
  ReplicationGroup rg(options);
  auto s = rg.StartAllPeers();
  ASSERT_TRUE(s.ok()) << s.ToString();

  // wait leader election
  auto leader_id = rg.WaitCurrentLeader(wait_election_ms);
  ASSERT_TRUE(!leader_id.Empty());

  // propose some tasks and wait for them to be applied
  auto leader_node = rg.GetNodePtr(leader_id);
  const int per_entry_size = 32;
  const size_t entry_number = 10;
  Waiter w(entry_number);
  for (size_t i = 0; i < entry_number; i++) {
    auto repl_task = std::make_shared<ReplTask>(RandomAlphaData(per_entry_size),
                                                new EntryClosure(Status::OK(), &w),
                                                options.group_id, leader_id,
                                                ReplTask::Type::kClientType,
                                                ReplTask::LogFormat::kRedis);
    leader_node->Propose(repl_task);
  }
  w.Wait();

  // propose an AddMember task
  const PeerID new_member(local_id_.Ip(), local_id_.Port() + kPortStep*node_number);
  node_number += 1;
  node_ids.insert(new_member);

  auto leader_rm = rg.GetRMPtr(leader_id);
  ASSERT_TRUE(leader_rm != nullptr);

  w.Reset(1);
  leader_rm->AddMember(options.group_id, new_member, replication::PeerRole::kRoleLearner,
                      new EntryClosure(Status::OK(), &w));
  w.Wait();

  // start the new member with empty configuration
  s = rg.StartPeer(new_member, true/*learner*/, false/*alone*/, false/*new_cluster*/);
  ASSERT_TRUE(s.ok()) << s.ToString();

  sleep(2/*2s*/);

  s = rg.CheckStateMachinesEqual(node_ids);
  ASSERT_TRUE(s.ok()) << s.ToString();

  // when the leader gone, the new leader cannot be elected
  s = rg.StopPeer(leader_id);
  ASSERT_TRUE(s.ok()) << s.ToString();

  //sleep(2/*2s*/);
  //auto new_leader_id = rg.WaitCurrentLeader(wait_election_ms);
  //ASSERT_TRUE(new_leader_id.Empty());
}

TEST_P(TestClusterRGNode, RemoveLeader) {
  const uint64_t election_timeout_ms = 1000;
  const uint64_t wait_election_ms = 2 * election_timeout_ms;
  const uint64_t heartbeat_timeout_ms = 100;
  ReplicationGroup::Options options;
  options.peer_options.load_index_into_memory = load_index_into_memory_;
  options.peer_options.election_timeout_ms = election_timeout_ms;
  options.peer_options.heartbeat_timeout_ms = heartbeat_timeout_ms;
  options.group_id = ReplicationGroupID("db0", 1);
  options.base_log_path = log_path_;

  const int node_number = 3;
  NodeSet node_ids;
  for (int i = 0; i < node_number; i++) {
    PeerID node(local_id_.Ip(), local_id_.Port() + kPortStep*i);
    options.conf.AddPeer(node, kDefaultRole);
    node_ids.insert(std::move(node));
  }
  ReplicationGroup rg(options);
  auto s = rg.StartAllPeers();
  ASSERT_TRUE(s.ok()) << s.ToString();

  // wait leader election
  auto first_leader_id = rg.WaitCurrentLeader(wait_election_ms);
  ASSERT_TRUE(!first_leader_id.Empty());
  auto follower_ids = rg.GetCurrentFollowers();
  ASSERT_EQ(follower_ids.size(), node_number - 1);

  // propose an RemoveMember task
  const PeerID remove_member_id = first_leader_id;
  auto remove_member_rm = rg.GetRMPtr(remove_member_id);
  ASSERT_TRUE(remove_member_rm != nullptr);
  Waiter w(1);
  remove_member_rm->RemoveMember(options.group_id, remove_member_id,
                                 new EntryClosure(Status::OK(), &w));
  w.Wait();

  node_ids.erase(remove_member_id);
  s = rg.StopPeer(remove_member_id);
  ASSERT_TRUE(s.ok()) << s.ToString();

  // wait leader election
  auto second_leader_id = rg.WaitCurrentLeader(wait_election_ms);
  ASSERT_TRUE(!second_leader_id.Empty());
  follower_ids = rg.GetCurrentFollowers();
  ASSERT_EQ(follower_ids.size(), node_number - 2);

  ASSERT_NE(first_leader_id, second_leader_id);

  // propose some tasks
  auto leader_node = rg.GetNodePtr(second_leader_id);
  const int per_entry_size = 32;
  const size_t entry_number = 10;
  w.Reset(entry_number);
  for (size_t i = 0; i < entry_number; i++) {
    auto repl_task = std::make_shared<ReplTask>(RandomAlphaData(per_entry_size),
                                                new EntryClosure(Status::OK(), &w),
                                                options.group_id, second_leader_id,
                                                ReplTask::Type::kClientType,
                                                ReplTask::LogFormat::kRedis);
    leader_node->Propose(repl_task);
  }
  w.Wait();

  s = rg.CheckStateMachinesEqual(node_ids);
  ASSERT_TRUE(s.ok()) << s.ToString();
}

TEST_P(TestClusterRGNode, RemoveFollower) {
  const uint64_t election_timeout_ms = 1000;
  const uint64_t wait_election_ms = 2 * election_timeout_ms;
  const uint64_t heartbeat_timeout_ms = 100;
  ReplicationGroup::Options options;
  options.peer_options.load_index_into_memory = load_index_into_memory_;
  options.peer_options.election_timeout_ms = election_timeout_ms;
  options.peer_options.heartbeat_timeout_ms = heartbeat_timeout_ms;
  options.group_id = ReplicationGroupID("db0", 1);
  options.base_log_path = log_path_;

  const int node_number = 3;
  NodeSet node_ids;
  for (int i = 0; i < node_number; i++) {
    PeerID node(local_id_.Ip(), local_id_.Port() + kPortStep*i);
    options.conf.AddPeer(node, kDefaultRole);
    node_ids.insert(std::move(node));
  }
  ReplicationGroup rg(options);
  auto s = rg.StartAllPeers();
  ASSERT_TRUE(s.ok()) << s.ToString();

  // wait leader election
  auto first_leader_id = rg.WaitCurrentLeader(wait_election_ms);
  ASSERT_TRUE(!first_leader_id.Empty());
  auto follower_ids = rg.GetCurrentFollowers();
  ASSERT_EQ(follower_ids.size(), node_number - 1);

  // propose an RemoveMember task
  const PeerID remove_member_id = follower_ids[0];
  auto first_leader_rm = rg.GetRMPtr(first_leader_id);
  ASSERT_TRUE(first_leader_rm != nullptr);
  Waiter w(1);
  first_leader_rm->RemoveMember(options.group_id, remove_member_id,
                                 new EntryClosure(Status::OK(), &w));
  w.Wait();

  node_ids.erase(remove_member_id);
  s = rg.StopPeer(remove_member_id);
  ASSERT_TRUE(s.ok()) << s.ToString();

  sleep(2/*2s*/);

  auto second_leader_id = rg.WaitCurrentLeader(wait_election_ms);
  ASSERT_TRUE(!second_leader_id.Empty());
  follower_ids = rg.GetCurrentFollowers();
  ASSERT_EQ(follower_ids.size(), node_number - 2);

  ASSERT_EQ(first_leader_id, second_leader_id);

  // propose some tasks
  auto leader_node = rg.GetNodePtr(second_leader_id);
  const int per_entry_size = 32;
  const size_t entry_number = 10;
  w.Reset(entry_number);
  for (size_t i = 0; i < entry_number; i++) {
    auto repl_task = std::make_shared<ReplTask>(RandomAlphaData(per_entry_size),
                                                new EntryClosure(Status::OK(), &w),
                                                options.group_id, second_leader_id,
                                                ReplTask::Type::kClientType,
                                                ReplTask::LogFormat::kRedis);
    leader_node->Propose(repl_task);
  }
  w.Wait();

  sleep(2);

  s = rg.CheckStateMachinesEqual(node_ids);
  ASSERT_TRUE(s.ok()) << s.ToString();
}

TEST_P(TestClusterRGNode, FollowerSnapshot) {
  const uint64_t election_timeout_ms = 1000;
  const uint64_t wait_election_ms = 2 * election_timeout_ms;
  const uint64_t heartbeat_timeout_ms = 100;
  ReplicationGroup::Options options;
  options.peer_options.load_index_into_memory = load_index_into_memory_;
  options.peer_options.election_timeout_ms = election_timeout_ms;
  options.peer_options.heartbeat_timeout_ms = heartbeat_timeout_ms;
  options.group_id = ReplicationGroupID("db0", 1);
  options.base_log_path = log_path_;

  int node_number = 2;
  NodeSet node_ids;
  for (int i = 0; i < node_number; i++) {
    PeerID node(local_id_.Ip(), local_id_.Port() + kPortStep*i);
    options.conf.AddPeer(node, kDefaultRole);
    node_ids.insert(std::move(node));
  }
  ReplicationGroup rg(options);
  auto s = rg.StartAllPeers();
  ASSERT_TRUE(s.ok()) << s.ToString();

  // wait leader election
  auto leader_id = rg.WaitCurrentLeader(wait_election_ms);
  ASSERT_TRUE(!leader_id.Empty());
  auto follower_ids = rg.GetCurrentFollowers();
  ASSERT_EQ(follower_ids.size(), node_number - 1);

  // propose some tasks
  auto leader_node = rg.GetNodePtr(leader_id);
  const int per_entry_size = 32;
  const size_t entry_number = 100;
  Waiter w(entry_number);
  for (size_t i = 0; i < entry_number; i++) {
    std::string data = RandomAlphaData(per_entry_size);
    auto repl_task = std::make_shared<ReplTask>(std::move(data),
                                                new EntryClosure(Status::OK(), &w),
                                                options.group_id, leader_id,
                                                ReplTask::Type::kClientType,
                                                ReplTask::LogFormat::kRedis);
    leader_node->Propose(repl_task);
  }
  w.Wait();
  ASSERT_TRUE(w.status().ok()) << w.status().ToString();

  sleep(2); // wait for followers

  s = rg.CheckStateMachinesEqual(node_ids);
  ASSERT_TRUE(s.ok()) << s.ToString();

  // purge some logs
  auto leader_rm = rg.GetRMPtr(leader_id);
  ASSERT_TRUE(leader_rm != nullptr);
  s = leader_rm->PurgeLogs(options.group_id, 10/*filenum*/, true/*manual*/);
  ASSERT_TRUE(s.ok()) << s.ToString();

  // propose an AddMember task
  const PeerID new_member_id(local_id_.Ip(), local_id_.Port() + kPortStep*node_number);
  node_number += 1;
  node_ids.insert(new_member_id);

  w.Reset(1);
  leader_rm->AddMember(options.group_id, new_member_id, replication::PeerRole::kRoleVoter,
                      new EntryClosure(Status::OK(), &w));
  w.Wait();

  // start the new member with empty configuration
  s = rg.StartPeer(new_member_id, false/*learner*/, false/*alone*/, false/*new_cluster*/);
  ASSERT_TRUE(s.ok()) << s.ToString();

  auto new_member_rm = rg.GetRMPtr(new_member_id);
  ASSERT_TRUE(new_member_rm != nullptr);

  auto async_try_send_snapshot = [&rg, leader_id] (const PeerID& peer_id,
      replication::SnapshotSyncClosure* closure) {
      auto snapshot_data = rg.CreateNodeSnapshotData(leader_id);
      rg.LoadNodeSnapshotData(peer_id, snapshot_data);

      ClosureGuard guard(closure);
      if (closure != nullptr) {
        closure->MarkMeta(replication::SnapshotSyncClosure::SnapshotSyncMetaInfo(peer_id,
                          snapshot_data.applied_offset, snapshot_data.configuration));
      }
  };

  auto leader_state_machine = rg.GetStateMachinePtr(leader_id);

  EXPECT_CALL(*leader_state_machine, TrySendSnapshot)
    .WillOnce([async_try_send_snapshot] (const PeerID& peer_id, const ReplicationGroupID& group_id,
                     const std::string& logger_filename, int32_t top,
                     replication::SnapshotSyncClosure* closure) {
      (void) group_id;
      (void) logger_filename;
      (void) top;

      std::thread aux_ctx(async_try_send_snapshot, peer_id, closure);
      aux_ctx.detach();
    });

  auto new_member_state_machine = rg.GetStateMachinePtr(new_member_id);
  EXPECT_CALL(*new_member_state_machine, CheckSnapshotReceived)
    .WillOnce([&rg] (const std::shared_ptr<replication::ReplicationGroupNode>& node,
                     LogOffset* snapshot_offset) -> Status {
      auto snapshot_data = rg.GetNodeSnapshotData(node->local_id());
      *snapshot_offset = snapshot_data.applied_offset;
      return Status::OK();
    });

  sleep(5); // wait snapshot completed

  w.Reset(10);
  for (size_t i = 0; i < 10; i++) {
    std::string data = RandomAlphaData(per_entry_size);
    auto repl_task = std::make_shared<ReplTask>(std::move(data),
                                                new EntryClosure(Status::OK(), &w),
                                                options.group_id, leader_id,
                                                ReplTask::Type::kClientType,
                                                ReplTask::LogFormat::kRedis);
    leader_node->Propose(repl_task);
  }
  w.Wait();
  ASSERT_TRUE(w.status().ok()) << w.status().ToString();

  sleep(5); // wait all follower completed

  // wait all
  s = rg.CheckStateMachinesEqual(node_ids);
  ASSERT_TRUE(s.ok()) << s.ToString();
}

TEST_P(TestClusterRGNode, PreVote) {
  const uint64_t election_timeout_ms = 500;
  const uint64_t wait_election_ms = 10 * election_timeout_ms;
  const uint64_t heartbeat_timeout_ms = 100;
  ReplicationGroup::Options options;
  options.peer_options.load_index_into_memory = load_index_into_memory_;
  options.peer_options.pre_vote = true;
  options.peer_options.election_timeout_ms = election_timeout_ms;
  options.peer_options.heartbeat_timeout_ms = heartbeat_timeout_ms;
  options.group_id = ReplicationGroupID("db0", 1);
  options.base_log_path = log_path_;

  PeerID node_a(local_id_.Ip(), local_id_.Port() + kPortStep * 1);
  PeerID node_b(local_id_.Ip(), local_id_.Port() + kPortStep * 2);
  options.conf.AddPeer(node_a, kDefaultRole);
  options.conf.AddPeer(node_b, kDefaultRole);
  NodeSet node_ids;
  node_ids.insert(node_a);
  node_ids.insert(node_b);

  ReplicationGroup rg(options);
  auto s = rg.StartAllPeers();
  ASSERT_TRUE(s.ok()) << s.ToString();

  auto first_leader_id = rg.WaitCurrentLeader(wait_election_ms);
  ASSERT_TRUE(!first_leader_id.Empty());
  auto first_follower_ids = rg.GetCurrentFollowers();
  ASSERT_EQ(first_follower_ids.size(), 1);
  const auto& first_follower_id = first_follower_ids[0];

  auto first_leader_meta_storage = rg.GetMetaStoragePtr(first_leader_id);
  uint32_t first_term;
  first_leader_meta_storage->term(&first_term);

  // Stop the leader, and the surviving node should not keep increasing
  // his term number to compete for elections.
  const auto& stop_member_id = first_leader_id;
  s = rg.StopPeer(stop_member_id);
  ASSERT_TRUE(s.ok()) << s.ToString();

  sleep(5);

  auto second_leader_id = rg.WaitCurrentLeader(wait_election_ms);
  ASSERT_TRUE(second_leader_id.Empty()) << second_leader_id.ToString();

  auto first_follower_meta_storage = rg.GetMetaStoragePtr(first_follower_id);
  uint32_t second_term;
  first_follower_meta_storage->term(&second_term);
  ASSERT_TRUE(second_term <= first_term + 1/*Leadership transfer*/);

  // restart the stopped member
  s = rg.RestartPeer(stop_member_id);
  ASSERT_TRUE(s.ok()) << s.ToString();

  auto third_leader_id = rg.WaitCurrentLeader(wait_election_ms);
  ASSERT_TRUE(!third_leader_id.Empty());
  auto third_follower_ids = rg.GetCurrentFollowers();
  ASSERT_EQ(third_follower_ids.size(), 1);

  auto third_leader_meta_storage = rg.GetMetaStoragePtr(third_leader_id);
  uint32_t third_term;
  third_leader_meta_storage->term(&third_term);
  ASSERT_TRUE(third_term > second_term);
}

TEST_P(TestClusterRGNode, CheckQuorum) {
  const uint64_t election_timeout_ms = 500;
  const uint64_t wait_election_ms = 10 * election_timeout_ms;
  const uint64_t heartbeat_timeout_ms = 100;
  ReplicationGroup::Options options;
  options.peer_options.load_index_into_memory = load_index_into_memory_;
  options.peer_options.pre_vote = true;
  options.peer_options.check_quorum = true;
  options.peer_options.election_timeout_ms = election_timeout_ms;
  options.peer_options.heartbeat_timeout_ms = heartbeat_timeout_ms;
  options.group_id = ReplicationGroupID("db0", 1);
  options.base_log_path = log_path_;

  PeerID node_a(local_id_.Ip(), local_id_.Port() + kPortStep * 1);
  PeerID node_b(local_id_.Ip(), local_id_.Port() + kPortStep * 2);
  options.conf.AddPeer(node_a, kDefaultRole);
  options.conf.AddPeer(node_b, kDefaultRole);
  NodeSet node_ids;
  node_ids.insert(node_a);
  node_ids.insert(node_b);

  ReplicationGroup rg(options);
  auto s = rg.StartAllPeers();
  ASSERT_TRUE(s.ok()) << s.ToString();

  auto first_leader_id = rg.WaitCurrentLeader(wait_election_ms);
  ASSERT_TRUE(!first_leader_id.Empty());
  auto first_follower_ids = rg.GetCurrentFollowers();
  ASSERT_EQ(first_follower_ids.size(), 1);
  const auto& first_follower_id = first_follower_ids[0];

  // Remove the follower, and leader should step down
  // when the election timedout.
  const auto& stop_member_id = first_follower_id;
  s = rg.RemovePeer(stop_member_id);
  ASSERT_TRUE(s.ok()) << s.ToString();

  sleep(5);

  auto second_leader_id = rg.WaitCurrentLeader(wait_election_ms);
  ASSERT_TRUE(second_leader_id.Empty());
}

} // namespace test 

int main(int argc, char** argv) {
  test::test_env = static_cast<test::ReplManagerEnv*>(
      ::testing::AddGlobalTestEnvironment(new test::ReplManagerEnv(argc, argv)));
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
