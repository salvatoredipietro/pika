// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef REPLICATION_PIKA_CLASSIC_RG_NODE_H_
#define REPLICATION_PIKA_CLASSIC_RG_NODE_H_

#include <sstream>
#include <string>
#include <memory>
#include <vector>
#include <set>
#include <atomic>
#include <mutex>
#include <glog/logging.h>

#include "slash/include/slash_mutex.h"
#include "slash/include/slash_status.h"

#include "proto/pika_inner_message.pb.h"
#include "include/util/callbacks.h"
#include "include/pika_define.h"
#include "include/replication/pika_memory_log.h"
#include "include/replication/pika_repl_rg_node.h"
#include "include/replication/pika_repl_transporter.h"
#include "include/replication/classic/pika_classic_log_manager.h"
#include "include/replication/classic/pika_classic_progress.h"
#include "include/replication/classic/pika_classic_message_executor.h"

namespace replication {

using slash::Status;
using util::Closure;

constexpr static int kDefaultDelayTimeout = 100; // milliseconds

class ClassicRGNode;

struct SlaveContextOptions {
  struct ConnectOptions {
    constexpr static uint64_t kDefaultReconnectIntervalMS = 5 * 1000; /*5s*/
    uint64_t receive_timeout_ms = ProgressContext::kRecvKeepAliveTimeoutUS / 1000;
    uint64_t send_timeout_ms = ProgressContext::kSendKeepAliveTimeoutUS / 1000;
    uint64_t reconnect_interval_ms = kDefaultReconnectIntervalMS;
    bool reconnect_when_master_gone = false;
    ConnectOptions() = default;
  };
  ReplicationGroupID group_id;
  PeerID local_id;
  StateMachine* fsm_caller;
  ReplicationManager* rm;
  ClassicLogManager* log_manager;
  ClassicMessageSender* msg_sender;
  std::weak_ptr<ClassicRGNode> node;
  SlaveContextOptions();
};

// SlaveContext control the state change when the ReplicationGroupNode
// is a slave. This class is thread-safe.
class SlaveContext {
 public:
  explicit SlaveContext(const SlaveContextOptions::ConnectOptions& connect_options);
  ~SlaveContext();
  DISALLOW_COPY(SlaveContext);

  class ReplState {
   public:
    enum Code {
      kNoConnect             = 0,
      kTrySync               = 1,
      kTrySyncSent           = 2,
      kTrySnapshotSync       = 3, // Full/Snapshot Sync
      kTrySnapshotSyncSent   = 4,
      kWaitSnapshotReceived  = 5,
      kBinlogSync            = 6,
      kError                 = 7,
    };
    ReplState(uint64_t send_timeout_ms, uint64_t recv_timeout_ms)
      : code_(Code::kNoConnect), ctx_(send_timeout_ms * 1000, recv_timeout_ms * 1000) { }
    ReplState(const Code& code, uint64_t send_timeout_ms,
              uint64_t recv_timeout_ms)
      : code_(code), ctx_(send_timeout_ms * 1000, recv_timeout_ms * 1000) { }
    std::string ToString() const;
    Code code() const { return code_; }
    void set_code(Code code) { code_ = code; }
    void SetLastRecvTime(uint64_t t) { ctx_.set_last_recv_time(t); }
    void SetLastSendTime(uint64_t t) { ctx_.set_last_send_time(t); }
    int32_t SessionId() const { return ctx_.session_id(); }
    void SetSessionId(int32_t s_id) { ctx_.set_session_id(s_id); }
    bool IsRecvTimeout(uint64_t now) { return ctx_.IsRecvTimeout(now); }
    bool IsSendTimeout(uint64_t now) { return ctx_.IsSendTimeout(now); }
    void Reset() {
      code_ = Code::kNoConnect;
      ctx_.Reset();
    }

   private:
    Code code_;
    ProgressContext ctx_;
  };
  ReplState repl_state() {
    slash::RWLock l(&rwlock_, false);
    return repl_state_;
  }
  void SetReplSession(int32_t session_id) {
    slash::RWLock l(&rwlock_, true);
    repl_state_.SetSessionId(session_id);
  }
  int32_t ReplSession() {
    slash::RWLock l(&rwlock_, false);
    return repl_state_.SessionId();
  }
  void Reset() {
    slash::RWLock l(&rwlock_, true);
    repl_state_.Reset();
  }
  void SetReplCode(const ReplState::Code& code) {
    slash::RWLock l(&rwlock_, true);
    repl_state_.set_code(code);
  }
  ReplState::Code ReplCode() {
    slash::RWLock l(&rwlock_, false);
    return repl_state_.code();
  }
  void SetReplLastRecvTime(uint64_t now) {
    slash::RWLock l(&rwlock_, true);
    repl_state_.SetLastRecvTime(now);
  }
  bool ReplRecvTimeout(uint64_t now) {
    slash::RWLock l(&rwlock_, false);
    return repl_state_.IsRecvTimeout(now);
  }
  PeerID master_id() {
    slash::RWLock l(&rwlock_, false);
    return master_id_;
  }
  void ReadLock() {
    pthread_rwlock_rdlock(&rwlock_);
  }
  void WriteLock() {
    pthread_rwlock_wrlock(&rwlock_);
  }
  void Unlock() {
    pthread_rwlock_unlock(&rwlock_);
  }

  void Init(const SlaveContextOptions& options);
  Status Start();
  void Step();
  Status IsSafeToBeRemoved();
  void ReportUnreachable(const PeerID& peer_id);
  bool GetSyncState(std::stringstream& out_of_sync);
  Status SetMaster(const PeerID& master_id, bool force_full_sync);
  Status RemoveMaster(const PeerID& master_id);
  Status ResetMaster(const PeerID& master_id, bool force_full_sync);
  // Keep the current master, but do not communicate with it
  Status DisableReplication();
  // Recommunicate with the current master
  Status EnableReplication(bool force_full_sync);

  Status SendTrySyncRequest();
  void HandleTrySyncResponse(const InnerMessage::InnerResponse* response);
  Status SendSnapshotSyncRequest();
  Status HandleSnapshotSyncResponse(const InnerMessage::InnerResponse* response);
  void HandleSnapshotSyncCompleted(bool success);
  // After received the entries from master,
  // slave send the BinlogSyncRequest to master
  // to indicate the persisted log entries.
  Status SendBinlogSyncRequest(const LogOffset& ack_start,
                               const LogOffset& ack_end,
                               bool is_first_send = false);
  bool HandleBinlogSyncResponse(const PeerID& peer_id,
                                const std::vector<int>* index,
                                InnerMessage::InnerResponse* response);
  Status SendRemoveSlaveNodeRequest(const PeerID& master_id, int32_t session_id);

  void NotifySnapshotReceived();

 private:
  Status UnsafeSetMaster(const PeerID& master_id, bool force_full_sync);
  void UnsafeRemoveMaster(const PeerID& master_id);
  bool IsWaitingSnapshot();
  // @return bool: return true to indicate it's timeout
  bool CheckRecvTimeout(uint64_t now);
  void ReconnectMaster();

 private:
  SlaveContextOptions options_;
  const SlaveContextOptions::ConnectOptions connect_options_;

  pthread_rwlock_t rwlock_;
  PeerID master_id_;
  ReplState repl_state_;
  bool enable_communication_;
  uint64_t last_connect_time_;

  std::atomic<bool> waiting_snapshot_;
};

struct MasterContextOptions {
  ReplicationGroupID group_id;
  PeerID local_id;
  StateMachine* fsm_caller;
  ReplicationManager* rm;
  ClassicProgressSet* progress_set;
  ClassicLogManager* log_manager;
  ClassicMessageSender* msg_sender;
  MasterContextOptions();
};

class MasterContext {
 public:
  MasterContext();
  ~MasterContext();
  DISALLOW_COPY(MasterContext);

  void Init(const MasterContextOptions& options);
  Status Start();
  Status IsReadyForWrite();
  Status IsSafeToBeRemoved();
  bool IsSafeToBePurged(uint32_t filenum);
  void ReportUnreachable(const PeerID& peer_id);
  void GetSyncState(std::stringstream& out_of_sync);
  Status GetSyncInfo(const PeerID& peer_id,
                     std::stringstream& stream);
  void Step();
  Status Propose(const std::shared_ptr<ReplTask>& task, LogOffset& log_offset);

  // Handle requests from slave
  bool HandleTrySyncRequest(const InnerMessage::InnerRequest* request,
                            InnerMessage::InnerResponse* response);
  bool HandleSnapshotSyncRequest(const InnerMessage::InnerRequest* request,
                                 InnerMessage::InnerResponse* response);
  bool HandleBinlogSyncRequest(const InnerMessage::InnerRequest* request,
                               InnerMessage::InnerResponse* response);
  Status HandleRemoveNodeRequest(const PeerID& peer_id,
                                 InnerMessage::InnerResponse* response);

 private:
  bool TrySyncOffsetCheck(const InnerMessage::InnerRequest::TrySync& try_sync_request,
                          InnerMessage::InnerResponse::TrySync* try_sync_response);
  bool TrySyncUpdateSlaveNode(const InnerMessage::InnerRequest::TrySync* try_sync_request,
                              InnerMessage::InnerResponse::TrySync* try_sync_response);
  Status AddLearner(const PeerID& peer_id, int32_t* session_id);
  Status RemoveLearner(const PeerID& peer_id);
  Status UpdateSlaveProgressContext(const PeerID& peer_id,
                                    const ProgressContext& ctx);
  Status ActivateSlaveProgress(const PeerID& peer_id,
                               const LogOffset& offset);
  Status UpdateSlaveProgress(const PeerID& peer_id,
                             const LogOffset& range_start,
                             const LogOffset& range_end);
  Status StepProgress(const PeerID& peer_id,
                      const LogOffset& start,
                      const LogOffset& end);
  Status SendHeartbeatToPeer(const std::shared_ptr<ClassicProgress>& peer_progress,
                             uint64_t now, BinlogTask& tasks);
  Status SendBinlogsToPeer(const std::shared_ptr<ClassicProgress>& peer_progress);
  Status SendBinlogsToPeer(const std::shared_ptr<ClassicProgress>& peer_progress,
                           BinlogTaskList& tasks);

 private:
  MasterContextOptions options_;
};

struct ClassicRGNodeOptions : public ReplicationGroupNodeOptions {
  SlaveContextOptions::ConnectOptions slave_connect_options;
  ClassicRGNodeOptions() = default;
};

class ClassicRGNode : public ReplicationGroupNode {
  friend class MasterContext;
  friend class SlaveContext;
 public:
  explicit ClassicRGNode(const ClassicRGNodeOptions& options);
  ~ClassicRGNode();

  virtual Status Start() override;
  virtual void Leave(std::string& log_remove) override;
  virtual bool IsReadonly() override;
  virtual void PurgeStableLogs(int expire_logs_nums, int expire_logs_days,
                               uint32_t to, bool manual, PurgeLogsClosure* done) override;
  virtual bool IsSafeToBePurged(uint32_t filenum) override;
  virtual Status IsReady() override;
  virtual Status IsSafeToBeRemoved() override;
  virtual Status StepStateMachine() override;
  virtual void ReportUnreachable(const PeerID& peer_id) override;
  virtual Status GetSyncInfo(const PeerID& peer_id,
                             std::stringstream& stream) override;
  virtual VolatileContext::State NodeVolatileState() override;
  virtual PersistentContext::State NodePersistentState() override;
  virtual MemberContext::MemberStatesMap NodeMemberStates() override;
  virtual void Messages(PeerMessageListMap& msgs) override;
  virtual Status Propose(const std::shared_ptr<ReplTask>& task) override;
  virtual Status Advance(const Ready& ready) override;

  void GetMasterSyncState(std::stringstream& out_of_sync);
  bool GetSlaveSyncState(std::stringstream& out_of_sync);

  Status SetMaster(const PeerID& master_id,
                   bool force_full_sync);
  Status DisableReplication();
  Status EnableReplication(bool force_full_sync, bool reset_file_offset,
                           uint32_t filenum, uint64_t offset);
  Status ResetMaster(const PeerID& master_id,
                     bool force_full_sync);
  Status RemoveMaster(const PeerID& master_id);
  bool HandleTrySyncRequest(const InnerMessage::InnerRequest* request,
                            InnerMessage::InnerResponse* response);
  void HandleTrySyncResponse(const InnerMessage::InnerResponse* response);
  bool HandleSnapshotSyncRequest(const InnerMessage::InnerRequest* request,
                           InnerMessage::InnerResponse* response);
  Status HandleSnapshotSyncResponse(const InnerMessage::InnerResponse* response);
  bool HandleBinlogSyncRequest(const InnerMessage::InnerRequest* request,
                               InnerMessage::InnerResponse* response);
  void HandleBinlogSyncResponse(const PeerID& peer_id,
                                const std::vector<int>* index,
                                InnerMessage::InnerResponse* response);
  Status HandleRemoveNodeRequest(const PeerID& peer_id,
                                 InnerMessage::InnerResponse* response);

 private:
  virtual Status Initialize() override;
  static void WaitSnapshotReceived(void* arg);
  void NotifySnapshotReceived();

  void TryToScheduleApplyLog(const LogOffset& committed_offset);
  Status ScheduleApplyLog(const LogOffset& committed_offset);
  bool UpdateCommittedOffset(const LogOffset& remote_committed_offset,
                             LogOffset* updated_committed_offset);

 private:
  PersistentContext persistent_ctx_;
  ReplicationGroupNodeMetaStorage* meta_storage_;
  ClassicProgressSet progress_set_;
  ClassicLogManager* log_manager_;
  ClassicMessageSender msg_sender_;

  // Node can be master and slave at the same time.
  MasterContext master_ctx_;
  SlaveContext slave_ctx_;
};

struct WaitSnapshotArg {
  std::weak_ptr<ClassicRGNode> node;
  LogOffset snapshot_offset;
  explicit WaitSnapshotArg(std::shared_ptr<ClassicRGNode> _node)
    : node(_node), snapshot_offset() { }
};

} // namespace replication

#endif // REPLICATION_PIKA_CLASSIC_RG_NODE_H_
