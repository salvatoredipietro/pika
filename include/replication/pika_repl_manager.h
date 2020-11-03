// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef REPLICATION_PIKA_REPL_MANAGER_H_
#define REPLICATION_PIKA_REPL_MANAGER_H_

#include <string>
#include <memory>
#include <unordered_map>
#include <queue>
#include <vector>
#include <set>

#include "slash/include/slash_status.h"

#include "proto/pika_inner_message.pb.h"
#include "include/pika_conf.h"
#include "include/pika_define.h"
#include "include/util/callbacks.h"
#include "include/util/task_executor.h"
#include "include/replication/pika_memory_log.h"
#include "include/replication/pika_repl_progress.h"
#include "include/replication/pika_repl_rg_node.h"
#include "include/replication/pika_repl_auxiliary.h"
#include "include/replication/pika_repl_transporter.h"

namespace replication {

using slash::Status;
using util::Closure;
using util::ClosureGuard;

class SnapshotSyncClosure : public Closure {
 public:
  struct SnapshotSyncMetaInfo {
    PeerID remote_id;
    LogOffset snapshot_offset;
    Configuration conf;
    SnapshotSyncMetaInfo() = default;
    SnapshotSyncMetaInfo(const PeerID& _remote_id,
                         const LogOffset& _snapshot_offset,
                         const Configuration& _conf)
      : remote_id(_remote_id),
      snapshot_offset(_snapshot_offset),
      conf(_conf) { }
  };
  SnapshotSyncClosure() = default;

  void MarkMeta(const SnapshotSyncMetaInfo& meta) { meta_ = meta; }
  const SnapshotSyncMetaInfo& Meta() const { return meta_; }

 protected:
  virtual ~SnapshotSyncClosure() = default;
  SnapshotSyncMetaInfo meta_;
};

class StateMachine {
 public:
  virtual ~StateMachine() = default;
  virtual void OnStop(const ReplicationGroupID& group_id) = 0;
  virtual void OnReplError(const ReplicationGroupID& group_id,
                           const Status& status) = 0;
  /*
   * Underlying ReplicationGroupNode will invoke this method to
   * notify the new leader.
   */
  virtual void OnLeaderChanged(const ReplicationGroupID& group_id,
                               const PeerID& leader_id) = 0;
  /*
   * Underlying ReplicationGroupNode will invoke this method to
   * notify that the commands represented by the logs can be
   * processed safely.
   */
  virtual void OnApply(std::vector<MemLog::LogItem> logs) = 0;
  /*
   * Underlying ReplicationGroupNode will invoke this method to
   * notify that the snapshot will be received in later. The relevant
   * environment should be prepared in advance to handle this event.
   */
  virtual Status OnSnapshotSyncStart(const ReplicationGroupID& group_id) = 0;
  /*
   * Underlying ReplicationGroupNode will invoke this method to
   * check if the snapshot has been received successfully.
   *
   * @param snapshot_offset(out): save the offset information of this snapshot.
   * @return: return Status::OK() when the snapshot has been received.
   */
  virtual Status CheckSnapshotReceived(const std::shared_ptr<ReplicationGroupNode>& node,
                                       LogOffset* snapshot_offset) = 0;
  /*
   * Underlying ReplicationGroupNode will invoke this method to
   * send the snapshot.
   *
   * @param SnapshotSyncClosure: the closure will be invoked when the snapshot
   *                             has been sent.
   */
  virtual void TrySendSnapshot(const PeerID& peer_id, const ReplicationGroupID& group_id,
                               const std::string& logger_filename,
                               int32_t top, SnapshotSyncClosure* closure) = 0;
  virtual void ReportLogAppendError(const ReplicationGroupID& group_id) = 0;
  virtual void PurgelogsTaskSchedule(void (*function)(void*), void* arg) = 0;
  virtual void PurgeDir(const std::string& dir) = 0;
};

class ReplicationOptionsStorage {
 public:
  virtual ~ReplicationOptionsStorage() = default;

  virtual std::string slaveof() = 0;
  virtual bool slave_read_only() = 0;
  virtual int slave_priority() = 0;
  virtual std::string masterauth() = 0;
  virtual std::string requirepass() = 0;
  virtual const std::vector<TableStruct>& table_structs() = 0;
  virtual bool classic_mode() = 0;
  virtual ReplicationProtocolType replication_protocol_type() = 0;
  virtual int expire_logs_nums() = 0;
  virtual int expire_logs_days() = 0;
  virtual uint32_t retain_logs_num() {
    return 10;
  }
  virtual uint64_t reconnect_master_interval_ms() {
    return 5000;
  }
  virtual int sync_window_size() = 0;
  virtual uint64_t sync_receive_timeout_ms() {
    return ProgressContext::kRecvKeepAliveTimeoutUS / 1000;
  }
  virtual uint64_t sync_send_timeout_ms() {
    return ProgressContext::kSendKeepAliveTimeoutUS / 1000;
  }
  virtual int binlog_file_size() = 0;
  virtual std::string log_path() = 0;
  virtual int max_conn_rbuf_size() = 0;
  virtual PeerID local_id() = 0;

  virtual int sync_thread_num() {
    return util::TaskExecutorOptions::kDefaultDispatchWorkerNum;
  }
  virtual int disk_thread_num() {
    return util::TaskExecutorOptions::kDefaultCompetitiveWorkerNum;
  }
  virtual bool load_index_into_memory() { return false; }
  virtual bool check_quorum() = 0;
  virtual bool pre_vote() = 0;
  virtual uint64_t heartbeat_timeout_ms() = 0;
  virtual uint64_t election_timeout_ms()  = 0;
  virtual uint64_t max_consumed_number_once() { return 128; }

  virtual void SetWriteBinlog(const std::string& value) = 0;
};

struct ReplicationManagerOptions {
  StateMachine* state_machine;
  std::shared_ptr<Transporter> transporter;
  std::shared_ptr<ReplicationOptionsStorage> options_store;
  ReplicationManagerOptions(StateMachine* _state_machine,
                            std::shared_ptr<Transporter> _transporter,
                            std::shared_ptr<ReplicationOptionsStorage> _options_store)
    : state_machine(_state_machine),
    transporter(std::move(_transporter)),
    options_store(std::move(_options_store)) {}
};

struct RGNodeInitializeConfig {
  ReplicationGroupNodeMetaStorage* meta_storage = nullptr;
  RGNodeInitializeConfig() = default;
};

using GroupIDAndInitConfMap = std::unordered_map<ReplicationGroupID,
      RGNodeInitializeConfig, hash_replication_group_id>;
using SanityCheckFilter = std::function<Status(const std::shared_ptr<ReplicationGroupNode>&)>;

class ReplicationManager : public MessageReporter {
 public:
  ReplicationManager(const ReplicationManagerOptions& rm_options,
                     TransporterOptions&& transporter_options);
  virtual ~ReplicationManager();

  virtual int Start();
  virtual int Stop();

 public:
  /*
   * ReplicationGroupNodesSantiyCheck check if the current status match the filter.
   * @param filter: if the replication group match the filter, then return true.
   * @return: return Status::OK() only when all the replication groups match the filter.
   */
  Status ReplicationGroupNodesSantiyCheck(const SanityCheckFilter& filter);
  Status CreateReplicationGroupNodesSanityCheck(const std::set<ReplicationGroupID>& group_ids);
  Status CreateReplicationGroupNodesSanityCheck(const GroupIDAndInitConfMap& id_init_conf_map);
  Status CreateReplicationGroupNodes(const std::set<ReplicationGroupID>& group_ids);
  Status CreateReplicationGroupNodes(const GroupIDAndInitConfMap& id_init_conf_map);
  std::shared_ptr<ReplicationGroupNode> GetReplicationGroupNode(const ReplicationGroupID& group_id);
  Status StartReplicationGroupNode(const ReplicationGroupID& group_id);
  Status StopReplicationGroupNode(const ReplicationGroupID& group_id);
  Status RemoveReplicationGroupNodesSanityCheck(const std::set<ReplicationGroupID>& group_ids);
  Status RemoveReplicationGroupNodes(const std::set<ReplicationGroupID>& group_ids);

  void SignalAuxiliary();
  Status GetSyncInfo(const ReplicationGroupID& group_id,
                     const PeerID& peer_id, std::stringstream& stream);
  Status GetReplicationGroupInfo(const ReplicationGroupID& group_id,
                                 std::stringstream& stream);
  void ReplicationStatus(std::string& info) { }
  Status PurgeLogs(const ReplicationGroupID& group_id,
                   uint32_t to = 0, bool manual = false);
  void ReportLogAppendError(const ReplicationGroupID& group_id);
  std::shared_ptr<ReplicationOptionsStorage> OptionsStorage() { return options_.options_store; }
  void Schedule(void (*function)(void *), void* arg,
                const std::string& hash_str = "", uint64_t time_after = 0);

 public:
  // Implement MessageReporter
  virtual void HandlePeerMessage(MessageReporter::Message* peer_msg) {
    handlePeerMessage(peer_msg);
  }
  virtual void ReportTransportResult(const PeerInfo& peer_info,
                                     const MessageReporter::ResultType& r_type,
                                     const MessageReporter::DirectionType& d_type) {
    reportTransportResult(peer_info, r_type, d_type);
  }
  virtual void PinServerStream(const PeerID& peer_id,
                               std::shared_ptr<pink::PbConn> server_stream) override;
  virtual void PinClientStream(const PeerID& peer_id,
                               std::shared_ptr<pink::PbConn> client_stream) override;
  virtual void UnpinPeerConnections(const PeerID& peer_id, const MessageReporter::DirectionType& d_type) override;

 public:
  virtual void InfoReplication(std::string& info) { }
  virtual bool IsReadonly(const ReplicationGroupID& group_id);
  virtual int SendToPeer();
  virtual void StepStateMachine();

  /*
   * Classic mode used
   */
  virtual bool IsMaster() { return true; }
  virtual bool IsSlave() { return false; }
  virtual void CurrentMaster(std::string& master_ip, int& master_port) { }
  virtual Status SetMaster(const std::string& master_ip,
                         int master_port, bool force_full_sync) {
    return Status::NotSupported("SetMaster");
  }
  virtual Status RemoveMaster() {
    return Status::NotSupported("RemoveMaster");
  }
  virtual Status ResetMaster(const std::set<ReplicationGroupID>& group_ids,
                             const PeerID& master_id, bool force_full_sync) {
    return Status::NotSupported("ResetMaster");
  }
  virtual Status DisableMasterForReplicationGroup(const ReplicationGroupID& group_id) {
    return Status::NotSupported("DisableMasterForReplicationGroup");
  }
  virtual Status EnableMasterForReplicationGroup(const ReplicationGroupID& group_id,
      bool force_full_sync, bool reset_file_offset, uint32_t filenum, uint64_t offset) {
    return Status::NotSupported("EnableMasterForReplicationGroup");
  }

  /*
   * Cluster mode used
   */
  virtual void AddMember(const ReplicationGroupID& group_id,
                         const PeerID& peer_id, const PeerRole& role,
                         Closure* done_closure) {
    ClosureGuard guard(done_closure);
    (void)guard;
    if (done_closure) {
      done_closure->set_status(Status::NotSupported("AddMember"));
    }
  }
  virtual void RemoveMember(const ReplicationGroupID& group_id,
                            const PeerID& peer_id, Closure* done_closure) {
    ClosureGuard guard(done_closure);
    (void)guard;
    if (done_closure) {
      done_closure->set_status(Status::NotSupported("RemoveMember"));
    }
  }
  virtual void PromoteMember(const ReplicationGroupID& group_id,
                             const PeerID& peer_id, Closure* done_closure) {
    ClosureGuard guard(done_closure);
    (void)guard;
    if (done_closure) {
      done_closure->set_status(Status::NotSupported("PromoteMember"));
    }
  }
  virtual void LeaderTransfer(const ReplicationGroupID& group_id,
                              const PeerID& peer_id, Closure* done_closure) {
    ClosureGuard guard(done_closure);
    (void)guard;
    if (done_closure) {
      done_closure->set_status(Status::NotSupported("LeaderTransfer"));
    }
  }

 protected:
  void ReportUnreachable(const PeerID& peer_id, const MessageReporter::DirectionType& d_type);
  std::vector<std::shared_ptr<ReplicationGroupNode>> CurrentReplicationGroups();

  using ReplicationGroups = std::unordered_map<ReplicationGroupID,
        std::shared_ptr<ReplicationGroupNode>, hash_replication_group_id>;

  pthread_rwlock_t rg_rw_;
  ReplicationGroups groups_map_;

  PikaReplAuxiliary auxiliary_;
  ReplicationManagerOptions options_;
  std::shared_ptr<Transporter> transporter_;
  util::TaskExecutor executor_;

 private:
  virtual Status createReplicationGroupNodes(const GroupIDAndInitConfMap& id_init_conf_map) = 0;
  virtual void handlePeerMessage(MessageReporter::Message* peer_msg) = 0;
  virtual void reportTransportResult(const PeerInfo& peer_info,
                                     const MessageReporter::ResultType& r_type,
                                     const MessageReporter::DirectionType& d_type) = 0;
  Status RemoveReplicationGroupNodes(const std::set<ReplicationGroupID>& group_ids,
                                     std::vector<std::string>& logs);
  void StopAllReplicationGroups();
};

} // namespace replication

#endif  // REPLICATION_PIKA_REPL_MANAGER_H_
