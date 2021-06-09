// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/replication/cluster/pika_cluster_rg_node.h"

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/stat.h>

#include "slash/include/env.h"
#include "pink/include/pink_cli.h"

#include "include/storage/pika_binlog_transverter.h"
#include "include/replication/pika_repl_manager.h"

namespace replication {

using util::Closure;
using storage::PikaBinlogTransverter;
using RoleStateCode = RoleState::Code;
using FollowerStateCode = Progress::FollowerState::Code;

std::string LeaderTransferContext::CancelReason::ToString() {
  return ToString(code_);
}

std::string LeaderTransferContext::CancelReason::ToString(const Code& code) {
  std::string return_str;
  switch (code) {
    case Code::kRoleChanged: {
      return_str = "Role is changed";
      break;
    }
    case Code::kTargetLeaderIsRemoved: {
      return_str = "Target leader is removed";
      break;
    }
    case Code::kTransferTimedout: {
      return_str = "Transfer is timedout";
      break;
    }
    case Code::kNewProposalIsSubmitted: {
      return_str = "A new proposal is submitted";
      break;
    }
  }
  return return_str;
}

LeaderTransferContext::~LeaderTransferContext() {
  if (done_closure_ != nullptr) {
    done_closure_->Release();
  }
}

void LeaderTransferContext::MarkLeaderTransfer(const PeerID& target_leader,
                                               Closure* done_closure) {
  leader_transferee_ = target_leader;
  Closure* last_closure = done_closure_;
  done_closure_ = done_closure;
  ScheduleTheClosure(last_closure);
}

void LeaderTransferContext::CancelLeaderTransfer(const CancelReason::Code& reason_code) {
  Closure* last_closure = done_closure_;
  done_closure_ = nullptr;
  if (last_closure == nullptr) {
    return;
  }
  leader_transferee_.Clear();
  last_closure->set_status(Status::Corruption(CancelReason::ToString(reason_code)));
  ScheduleTheClosure(last_closure);
}

void LeaderTransferContext::ScheduleTheClosure(Closure* closure) {
  if (closure != nullptr) {
    if (rm_ != nullptr) {
      rm_->Schedule(&LeaderTransferContext::RunTheClosure,
                    static_cast<void*>(closure), ""/*no hash*/,0/*no delay*/);
    } else {
      // NOTE: avoid dead-lock.
      LeaderTransferContext::RunTheClosure(static_cast<void*>(closure));
    }
  }
}

void LeaderTransferContext::RunTheClosure(void* arg) {
  Closure* closure = static_cast<Closure*>(arg);
  // closure should delete itself after Run().
  closure->Run();
}

/* VoteBox */

void VoteBox::RecordVote(const PeerID& peer_id, bool granted) {
  votes_[peer_id] = granted;
}

VoteBox::VoteResult VoteBox::Result() const {
  size_t granted = 0;
  size_t rejected = 0;
  for (const auto& voter : voters_) {
    auto iter = votes_.find(voter);
    if (iter == votes_.end()) {
      // have not received vote yet.
      continue;
    }
    if (iter->second) {
      granted += 1;
    } else {
      rejected += 1;
    }
  }
  size_t quorum = voters_.size() / 2 + 1;
  if (granted >= quorum) {
    return VoteResult::kVoteGranted;
  } else if (rejected >= quorum) {
    return VoteResult::kVoteRejected;
  }
  return VoteResult::kVotePending;
}

void VoteBox::ResetVotes(const std::vector<PeerID>& voters) {
  voters_.clear();
  voters_ = voters;
  votes_.clear();
}

/* PendingTaskBox */

// PendingTaskBox used to schedule committed tasks,
// and persist and retrieve the indexes (commit, apply and snapshot).
class ClusterRGNode::PendingTaskBox
  : public std::enable_shared_from_this<ClusterRGNode::PendingTaskBox> {
 public:
  struct Options {
    ReplicationGroupID group_id;
    PeerID local_id;
    std::string persistent_info_path;
    TaskExecutor* executor;
    StateMachine* fsm_caller;
    std::shared_ptr<ClusterRGNode> node;
  };
  explicit PendingTaskBox(const Options& options);
  Status Initialize(const LogOffset& latest_persisted_offset);

  Status Advance(const Ready& ready);
  Status TryToUpdateCommittedOffset(const LogicOffset& committed_offset);

  PersistentContext::State persistent_state() { return persistent_ctx_.DumpState(); }
  LogOffset snapshot_offset() { return persistent_ctx_.snapshot_offset(); }
  LogOffset applied_offset() { return persistent_ctx_.applied_offset(); }
  LogOffset committed_offset() { return persistent_ctx_.committed_offset(); }

  Status ResetSnapshotAndApplied(const LogOffset& snapshot_offset,
                                 const LogOffset& applied_offset) {
    persistent_ctx_.ResetPerssistentContext(snapshot_offset, applied_offset);
    return Status::OK();
  }

 private:
  struct ScheduleArg {
    std::shared_ptr<PendingTaskBox> box;
    ScheduleArg(const std::shared_ptr<PendingTaskBox>& _box)
      : box(_box) {}
  };
  static void ScheduleApplyLog(void* arg);

  const ReplicationGroupID group_id_;
  const PeerID local_id_;
  TaskExecutor* executor_;
  StateMachine* fsm_caller_;
  PersistentContext persistent_ctx_;
  std::weak_ptr<ClusterRGNode> node_;

  std::mutex apply_order_mutex_;
};

ClusterRGNode::PendingTaskBox::PendingTaskBox(const Options& options)
  : group_id_(options.group_id), local_id_(options.local_id),
  executor_(options.executor), fsm_caller_(options.fsm_caller),
  persistent_ctx_(options.persistent_info_path),
  node_(options.node) {
}

Status ClusterRGNode::PendingTaskBox::Initialize(const LogOffset& latest_persisted_offset) {
  Status s = persistent_ctx_.Initialize(latest_persisted_offset, true);
  if (!s.ok()) {
    LOG(ERROR) << "Replication group: " << group_id_.ToString()
               << ", initialize the persistent data failed: " << s.ToString();
  }
  return s;
}

Status ClusterRGNode::PendingTaskBox::TryToUpdateCommittedOffset(const LogicOffset& committed_offset) {
  DLOG(INFO) << "Replication group: " << group_id_.ToString()
             << " expected committed offset: " << committed_offset.ToString()
             << ", current committed offset: " << persistent_ctx_.committed_offset().l_offset.ToString();
  bool should_apply = persistent_ctx_.UpdateCommittedOffset(LogOffset(BinlogOffset(), committed_offset), true);
  if (should_apply) {
    ScheduleArg* arg = new ScheduleArg(shared_from_this());
    if (executor_ != nullptr) {
      executor_->Schedule(&ClusterRGNode::PendingTaskBox::ScheduleApplyLog,
                          static_cast<void*>(arg));
    } else {
      ScheduleApplyLog(arg);
    }
  }
  return Status::OK();
}

void ClusterRGNode::PendingTaskBox::ScheduleApplyLog(void* arg) {
  ScheduleArg* schedule_arg = static_cast<ScheduleArg*>(arg);
  util::ResourceGuard<ScheduleArg> guard(schedule_arg);
  const auto& box = schedule_arg->box;
  auto node = box->node_.lock();
  if (node == nullptr) {
    LOG(WARNING) << "Replication group: " << box->group_id_.ToString()
                 << " node has been destoryed";
    return;
  }

  // logs from PurgeLogs should be applied in order
  std::vector<MemLog::LogItem> logs;
  {
    std::unique_lock<std::mutex> lk(box->apply_order_mutex_);
    const LogicOffset& committed_offset = box->committed_offset().l_offset;
    if (committed_offset < box->persistent_ctx_.applied_offset().l_offset) {
      // NOTE: the committed offset may have been updated, which means the prior
      //       task may have applied the pending entries.
      DLOG(INFO) << "Replication group: " << node->group_id().ToString()
                 << " committed_offset " << committed_offset.ToString()
                 << ", current applied_offset: "
                 << box->persistent_ctx_.applied_offset().l_offset.ToString();
      return;
    }
    Status s = node->log_manager_.PurgeMemLogsByIndex(committed_offset.index, &logs);
    if (!s.ok()) {
      lk.unlock();

      DLOG(INFO) << "Replication group: " << node->group_id().ToString()
                 << " updated_committed_offset " << committed_offset.ToString()
                 << ", schedule apply log error: " << s.ToString();
      return;
    }
    for (const auto& log : logs) {
      box->persistent_ctx_.PrepareUpdateAppliedOffset(log.GetLogOffset());
    }
    box->fsm_caller_->OnApply(std::move(logs));
  }
}

Status ClusterRGNode::PendingTaskBox::Advance(const Ready& ready) {
  // Update applied index
  if (!ready.applied_offset.l_offset.Empty()) {
    persistent_ctx_.UpdateAppliedOffset(ready.applied_offset, true);
  }
  return Status::OK();
}

/* SnapshotHandler */

// SnapshotHandler used to handle kSnapshotSync requests from leader.
class ClusterRGNode::SnapshotHandler {
 public:
  struct Options {
    ReplicationGroupID group_id;
    uint32_t current_term;
    ClusterLogManager* log_manager;
    StateMachine* fsm_caller;
    std::shared_ptr<PendingTaskBox> box;
    std::shared_ptr<ClusterRGNode> node;
  };
  SnapshotHandler(const Options& options);

  enum class StepResult : uint8_t {
    kBusy                = 0,
    kMismatchedRequest   = 1,
    kDuplicatedRequest   = 2,
    kOutdatedRequest     = 3,
    kMetaDataMismatched  = 4,
    kPrepared            = 5,
    kCompleted           = 6,
    kError               = 7,
  };
  struct SnapshotInfo {
    LogOffset snapshot_offset;
    Configuration configuration;
    SnapshotInfo() : snapshot_offset(), configuration() { }
  };
  StepResult Step(const InnerMessage::RaftMessage* request);
  void ResetWithTerm(const uint32_t term);
  bool IsProcessing();
  std::string GetPendingStampString();
  SnapshotInfo latest_snapshot_info() const { return latest_snapshot_info_; }

 private:
  enum class Stage : uint8_t {
    kNone          = 0,
    kProcessing    = 1,
    kCompleted     = 2,
    kError         = 3,
  };
  struct SnapshotStamp {
    uint32_t term;
    PeerID peer_id;
    SnapshotStamp() : term(0), peer_id() { }
    SnapshotStamp(uint32_t _term, const PeerID& _peer_id)
      : term(_term), peer_id(_peer_id) { }
    bool Empty() const { return term == 0 && peer_id.Empty(); }
    void Clear() {
      term = 0;
      peer_id.Clear();
    }
    std::string ToString() const {
      return "[" + peer_id.ToString() + " " + std::to_string(term) + "]";
    }
    bool operator==(const SnapshotStamp& other) {
      return other.term == term && other.peer_id == peer_id;
    }
    bool operator!=(const SnapshotStamp& other) {
      return other.term != term || other.peer_id != peer_id;
    }
  };
  const ReplicationGroupID group_id_;
  uint32_t current_term_;
  Stage stage_;
  SnapshotInfo latest_snapshot_info_;
  SnapshotStamp pending_snapshot_stamp_;
  StateMachine* fsm_caller_;
  ClusterLogManager* log_manager_;
  std::shared_ptr<PendingTaskBox> box_;
  std::weak_ptr<ClusterRGNode> node_;
};

ClusterRGNode::SnapshotHandler::SnapshotHandler(const Options& options)
  : group_id_(options.group_id), current_term_(options.current_term),
  stage_(Stage::kNone), latest_snapshot_info_(), pending_snapshot_stamp_(),
  fsm_caller_(options.fsm_caller),
  log_manager_(options.log_manager),
  box_(options.box),
  node_(options.node) {
}

void ClusterRGNode::SnapshotHandler::ResetWithTerm(const uint32_t term) {
  if (current_term_ < term) {
    current_term_ = term;
  }
  stage_ = Stage::kNone;
  pending_snapshot_stamp_.Clear();
  // TODO: notify fsm_caller_ to reset the environment,
  //       any pending snapshot data should be discarded.
}

bool ClusterRGNode::SnapshotHandler::IsProcessing() {
  return stage_ == Stage::kProcessing;
}

std::string ClusterRGNode::SnapshotHandler::GetPendingStampString() {
  return pending_snapshot_stamp_.ToString();
}

ClusterRGNode::SnapshotHandler::StepResult ClusterRGNode::SnapshotHandler::Step(
    const InnerMessage::RaftMessage* request) {
  StepResult result = StepResult::kCompleted;
  while (1) {
    switch (stage_) {
      case Stage::kNone: {
        PeerID remote_id(request->from().ip(), request->from().port());
        const uint32_t remote_term = request->term();
        const SnapshotStamp remote_stamp(remote_term, remote_id);

        if (request->context() != kAdvanceSnapshot) {
          result = StepResult::kMismatchedRequest;
          break;
        }
        if (!pending_snapshot_stamp_.Empty() && pending_snapshot_stamp_ != remote_stamp) {
          result = StepResult::kBusy;
          break;
        }
        if (!pending_snapshot_stamp_.Empty()) {
          result = StepResult::kDuplicatedRequest;
          break;
        }
        // Prepare the environment
        auto s = fsm_caller_->OnSnapshotSyncStart(group_id_);
        if (!s.ok()) {
          stage_ = Stage::kError;
          result = StepResult::kError;
          break;
        }
        pending_snapshot_stamp_ = remote_stamp;
        stage_ = Stage::kProcessing;
        result = StepResult::kPrepared;
        // return for later step
        return result;
      }
      case Stage::kProcessing: {
        PeerID remote_id(request->from().ip(), request->from().port());
        const uint32_t remote_term = request->term();
        const SnapshotStamp remote_stamp(remote_term, remote_id);

        if (pending_snapshot_stamp_ != remote_stamp) {
          result = StepResult::kBusy;
          break;
        }
        Configuration new_conf;
        new_conf.ParseFrom(request->snap_meta().conf_state());
        LogOffset declare_snapshot_offset;
        ParseBinlogOffset(request->snap_meta().snapshot_offset(), &declare_snapshot_offset);
        LogOffset log_offset = log_manager_->GetLogOffset(declare_snapshot_offset.b_offset,
                                                          declare_snapshot_offset.l_offset.index);
        // Check if the snapshot is outdate:
        // 1. The snapshot index is less than the committed index in local.
        // 2. We already have the log in local with the declare_snapshot_offset.
        const auto& local_committed_offset = box_->committed_offset();
        if (declare_snapshot_offset.l_offset.index <= local_committed_offset.l_offset.index
            || log_offset.l_offset.term == declare_snapshot_offset.l_offset.term) {
          box_->TryToUpdateCommittedOffset(declare_snapshot_offset.l_offset);
          result = StepResult::kOutdatedRequest;
          break;
        }
        LogOffset received_snapshot_offset;
        auto s = fsm_caller_->CheckSnapshotReceived(node_.lock(), &received_snapshot_offset);
        if (!s.ok()) {
          stage_ = Stage::kError;
          result = StepResult::kError;
          break;
        }
        if (received_snapshot_offset != declare_snapshot_offset) {
          stage_ = Stage::kError;
          result = StepResult::kMetaDataMismatched;
          break;
        }

        // record the snapshot information for later retrieval
        latest_snapshot_info_.snapshot_offset = declare_snapshot_offset;
        latest_snapshot_info_.configuration = new_conf;

        stage_ = Stage::kCompleted;
        result = StepResult::kCompleted;
        break;
      }
      case Stage::kCompleted: {
        ResetWithTerm(0);
        return result;
      }
      case Stage::kError: {
        // There are three situations that may arise:
        // 1) we can not initialize the environment.
        // 2) we can not find the snapshot data.
        // 3) the recieved snapshot data own a mismatched meta
        //    declared by the request.
        ResetWithTerm(0);
        return result;
      }
    }
  }
  return result;
}

/* LogReplicator */

// LogReplicator used to reduce lock-contention(raft_mutex_) for leader,
// by taking care of all the normal requests and responses from the followers
// during the term.
//
// NOTE: the LogReplicator only exists in kStateLeader state, and will be reset
// when the term changed.
class ClusterRGNode::LogReplicator {
 public:
  struct Options {
    ReplicationGroupID group_id;
    PeerID local_id;
    uint32_t current_term;
    MemberContext* member_ctx;
    ClusterLogManager* log_manager;
    ClusterProgressSet* progress_set;
    MessageSender* msg_sender;
    std::shared_ptr<PendingTaskBox> box;
    StateMachine* fsm_caller;
    std::shared_ptr<ClusterRGNode> node;
  };
  explicit LogReplicator(const Options& options);
  DISALLOW_COPY(LogReplicator);

  void StartAll();
  const ReplicationGroupID& group_id() const { return group_id_; }
  Status ApplyConfChange(const InnerMessage::ConfChange& change);
  void ResetTermAndStopAll(const uint32_t new_term, const LogOffset& start_offset);
  void StopAndTryToTransferLeadership();
  void StartLeaderTransfer(const PeerID& peer_id, const uint64_t timeout_index);
  Status StopLeaderTransfer(const PeerID& peer_id);
  void StepLeaderProgress(const LogOffset& offset);
  void HandleAppendEntriesResponse(const InnerMessage::RaftMessage* response);
  void HandleHeartbeatResponse(const InnerMessage::RaftMessage* response);
  void BroadcastAppendEntries();
  void BroadcastHeartbeat();
  void OnSnapshotSendCompleted(const SnapshotSyncClosure::SnapshotSyncMetaInfo& meta,
                               const Status& status);

 private:
  void StopAllExceptFor(const PeerID& filter_id);
  // REQUIRES: progress->mutex should be held.
  void AppendEntriesToPeer(const std::shared_ptr<ClusterProgress>& progress);
  // REQUIRES: progress->mutex should be held.
  Status SendEntriesToPeer(const std::shared_ptr<ClusterProgress>& progress,
                           bool send_when_all_acked);
  // REQUIRES: progress->mutex should be held.
  void SendHeartbeatToPeer(const std::shared_ptr<ClusterProgress>& progress);
  // REQUIRES: progress->mutex should be held.
  void SendEmptyEntriesToPeer(const std::shared_ptr<ClusterProgress>& progress);
  // REQUIRES: progress->mutex should be held.
  void SendAdvanceSnapshotToPeer(const std::shared_ptr<ClusterProgress>& progress);
  // REQUIRES: progress->mutex should be held.
  void SendSnapshotToPeer(const std::shared_ptr<ClusterProgress>& progress);
  // REQUIRES: progress->mutex should be held.
  Status StepProgress(const std::shared_ptr<ClusterProgress>& progress,
                      const LogOffset& start, const LogOffset& end);
  // REQUIRES: progress->mutex should be held.
  void SendTimeoutNow(const std::shared_ptr<ClusterProgress>& progress);

 private:
  // TODO: make the lifetimes pointer variables more visible.
  ReplicationGroupID group_id_;
  PeerID local_id_;
  uint32_t current_term_;
  MemberContext* member_ctx_;
  ClusterLogManager* log_manager_;
  ClusterProgressSet* progress_set_;
  MessageSender* msg_sender_;
  std::shared_ptr<PendingTaskBox> box_;
  StateMachine* fsm_caller_;
  std::weak_ptr<ClusterRGNode> node_;
};

ClusterRGNode::LogReplicator::LogReplicator(const Options& options)
  : group_id_(options.group_id),
  local_id_(options.local_id),
  current_term_(options.current_term),
  member_ctx_(options.member_ctx),
  log_manager_(options.log_manager),
  progress_set_(options.progress_set),
  msg_sender_(options.msg_sender),
  box_(options.box),
  fsm_caller_(options.fsm_caller),
  node_(options.node) {
}

Status ClusterRGNode::LogReplicator::ApplyConfChange(const InnerMessage::ConfChange& change) {
  Status s;
  PeerID peer_id(change.node_id().ip(), change.node_id().port());
  if (!peer_id.Empty()) {
    switch (change.type()) {
      case InnerMessage::ConfChangeType::kAddVoter: {
        s = progress_set_->AddPeer(peer_id, PeerRole::kRoleVoter,
                                   log_manager_->GetLastPendingOffset(),
                                   current_term_);
        member_ctx_->AddMember(peer_id, PeerRole::kRoleVoter);
        break;
      }
      case InnerMessage::ConfChangeType::kAddLearner: {
        s = progress_set_->AddPeer(peer_id, PeerRole::kRoleLearner,
                                   log_manager_->GetLastPendingOffset(),
                                   current_term_);
        member_ctx_->AddMember(peer_id, PeerRole::kRoleLearner);
        break;
      }
      case InnerMessage::ConfChangeType::kRemoveNode: {
        s = progress_set_->RemovePeer(peer_id);
        member_ctx_->RemoveMember(peer_id);
        break;
      }
      case InnerMessage::ConfChangeType::kPromoteLearner: {
        s = progress_set_->PromotePeer(peer_id);
        member_ctx_->PromoteMember(peer_id);
        break;
      }
    }
    if (!s.ok()) {
      LOG(WARNING) << "Replication group: " << group_id_.ToString()
                   << local_id_.ToString()
                   << ", applyConfChange error: " << s.ToString();
      return s;
    }
  }
  return s;
}

void ClusterRGNode::LogReplicator::ResetTermAndStopAll(const uint32_t new_term,
                                                       const LogOffset& start_offset) {
  if (new_term <= current_term_) {
    LOG(WARNING) << "Replication group: " << group_id_.ToString()
                 << " reset term failed, the term cannot be decreased"
                 << ", current term: " << current_term_
                 << ", new term: " << new_term;
    return;
  }
  current_term_ = new_term;
  auto progress_vec = progress_set_->AllProgress(nullptr);
  for (const auto& progress : progress_vec) {
    {
      // Update the progress term, reset the status
      progress->WriteLock();
      progress->UnsafeMarkStopped(true);
      progress->UnsafeSetTerm(new_term);
      progress->UnsafeSetTimeoutIndex(0);
      progress->ResetMatchedOffset();
      progress->UnsafeBecomeNotSync();
      progress->UnsafeInitializeNextIndex(log_manager_->GetLastPendingOffset());
      progress->Unlock();
    }
  }
}

void ClusterRGNode::LogReplicator::StopAllExceptFor(const PeerID& filter_id) {
  auto progress_vec = progress_set_->AllProgress([filter_id] (const ClusterProgressSet::ProgressPtr& p) -> bool {
      return p->peer_id() == filter_id;
  });
  for (auto& progress : progress_vec) {
    {
      progress->WriteLock();
      progress->UnsafeMarkStopped(true);
      progress->Unlock();
    }
  }
}

void ClusterRGNode::LogReplicator::StartAll() {
  auto progress_vec = progress_set_->AllProgress(nullptr);
  for (auto& progress : progress_vec) {
    {
      progress->WriteLock();
      progress->UnsafeMarkStopped(false);
      progress->UnsafeSetLastRecvTime(slash::NowMicros());
      progress->Unlock();
    }
  }
}

void ClusterRGNode::LogReplicator::StopAndTryToTransferLeadership() {
  const PeerID& local_id = local_id_;
  auto progress_vec = progress_set_->AllProgress([local_id] (const ClusterProgressSet::ProgressPtr& p) -> bool {
      return p->peer_id() == local_id;
  });
  uint64_t max_matched_index = 0;
  PeerID picked_peer_id;
  std::shared_ptr<ClusterProgress> picked_progress = nullptr;
  // pick a candidate according to the following conditions:
  // 1) owns the latest matched offset.
  // 2) is a voter
  // 3) is in active
  for (auto& progress : progress_vec) {
    {
      progress->ReadLock();
      auto matched_index = progress->matched_index();
      if (max_matched_index < matched_index
          && !progress->UnsafeJudgeLearner()
          && progress->UnsafeIsActive()) {
        max_matched_index = matched_index;
        picked_progress = progress;
        picked_peer_id = picked_progress->UnsafeGetPeerID();
      }
      progress->Unlock();
    }
  }
  // stop the all progresses except for the picked one.
  StopAllExceptFor(picked_peer_id);
  if (max_matched_index == 0 || picked_progress == nullptr) {
    return;
  }
  // transfer leadership to the picked progress
  // and no need to wait the response.
  {
    picked_progress->WriteLock();
    SendTimeoutNow(picked_progress);
    picked_progress->UnsafeMarkStopped(true);
    picked_progress->Unlock();
  }
  LOG(INFO) << "Replication group: " << group_id_.ToString()
            << ", try to transfer leadership to: " << picked_peer_id.ToString();
}

void ClusterRGNode::LogReplicator::StartLeaderTransfer(const PeerID& peer_id, const uint64_t timeout_index) {
  auto progress = progress_set_->GetProgress(peer_id);
  if (progress == nullptr) {
    return;
  }
  {
    progress->WriteLock();
    progress->UnsafeSetTimeoutIndex(timeout_index);
    LOG(INFO) << "Replication group: " << group_id_.ToString()
              << ", start transfer leadership to: " << peer_id.ToString()
              << ", timeout index: " << timeout_index
              << ", current matched index: " << progress->matched_index();
    // SendTimeoutNow immediately, as the peer already has the latest log.
    if (progress->matched_index() >= timeout_index) {
      SendTimeoutNow(progress);
      progress->UnsafeSetTimeoutIndex(0);
    } else {
      AppendEntriesToPeer(progress);
    }
    progress->Unlock();
  }
}

Status ClusterRGNode::LogReplicator::StopLeaderTransfer(const PeerID& peer_id) {
  auto progress = progress_set_->GetProgress(peer_id);
  if (progress == nullptr) {
    return Status::NotFound(peer_id.ToString());
  }
  {
    progress->WriteLock();
    progress->UnsafeSetTimeoutIndex(0);
    progress->Unlock();
  }
  return Status::OK();
}

void ClusterRGNode::LogReplicator::StepLeaderProgress(const LogOffset& offset) {
  auto progress = progress_set_->GetProgress(local_id_);
  if (progress == nullptr) {
    // self has been removed, not process here
    return;
  }
  {
    progress->WriteLock();
    StepProgress(progress, offset, offset);
    progress->Unlock();
  }
}

void ClusterRGNode::LogReplicator::HandleAppendEntriesResponse(const InnerMessage::RaftMessage* response) {
  PeerID remote_id(response->from().ip(), response->from().port());
  auto progress = progress_set_->GetProgress(remote_id);
  if (progress == nullptr) {
    return;
  }
  {
    progress->WriteLock();
    progress->UnsafeSetLastRecvTime(slash::NowMicros());
    const auto& remote_term = response->term();
    const uint32_t current_term = progress->UnsafeGetTerm();
    if (progress->UnsafeGetStopped()) {
      progress->Unlock();

      LOG(INFO) << "Replication group: " << group_id_.ToString()
                << " receive a kMsgAppendResp from " << remote_id.ToString()
                << " at term " << current_term
                << ", remote term " << remote_term
                << ", the progress is shutting down";
      return;
    }
    LogOffset remote_last_offset;
    ParseBinlogOffset(response->last_log_offset(), &remote_last_offset);
    DLOG(INFO) << "Replication group: " << group_id_.ToString()
               << " receive a kMsgAppendResp from " << remote_id.ToString()
               << " at term " << current_term
               << ", remote term " << remote_term
               << ", rejected: " << response->reject()
               << ", last_log_offset in remote is " << remote_last_offset.ToString();
    if (remote_term > current_term) {
      progress->Unlock();

      auto node = node_.lock();
      if (node == nullptr) {
        return;
      }
      node->TryUpdateTerm(remote_term, PeerID());
      return;
    }
    // There are two situations that may arise:
    // 1. peer is temporarily unavaliable.
    // 2. peer has conflicts with us.
    if (response->reject()) {
      if (response->context() == kTemporarilyUnavailable) {
        // NOTE: kTemporarilyUnavailable is used to mark the peer is temporarily unavailable,
        //       which means the remote is alive but not ready to accept requests yet.
        //
        //       1) peer has not started yet,
        //       or 2) peer is processing snapshot request.
        progress->UnsafeBecomeNotSync();
        progress->Unlock();

        LOG(WARNING) << "Replication group: " << group_id_.ToString()
                     << " receive a kMsgAppendResp from " << remote_id.ToString()
                     << " at term " << current_term
                     << ", remote is not ready to accept requests yet, we reset the state to NotSync";
        return;
      }

      const auto& current_state = progress->UnsafeGetFollowerState().code();
      progress->UnsafeBecomeNotSync();

      if (current_state == FollowerStateCode::kFollowerPreSnapshotSync
          || current_state == FollowerStateCode::kFollowerSnapshotSync) {
        // initialize the next_index to the remote_last_offset when in SnapshotSync state.
        progress->UnsafeInitializeNextIndex(remote_last_offset);

        LOG(WARNING) << "Replication group: " << group_id_.ToString()
                     << " receive a SnapshotResp from " << remote_id.ToString()
                     << " at term " << current_term
                     << ", the last log offset in remote is " << remote_last_offset.ToString();
      } else if (!progress->UnsafeTryToDecreaseNextIndex(remote_last_offset)) {
        progress->UnsafeInitializeNextIndex(remote_last_offset);

        LOG(WARNING) << "Replication group: " << group_id_.ToString()
                     << " receive a kMsgAppendResp from " << remote_id.ToString()
                     << " at term " << current_term
                     << ", the last log offset in remote is " << remote_last_offset.ToString()
                     << ", decrease the next index failed.";
      }
    } else {
      if (remote_term != current_term) {
        progress->Unlock();

        LOG(WARNING) << "Replication group: " << group_id_.ToString()
                     << " receive a kMsgAppendResp from " << remote_id.ToString()
                     << " term mismatch, local term " << current_term
                     << " remote term " << remote_term;
        return;
      }
      do {
        // 1. update the committed index according to the response from follower
        LogOffset first_log_offset;
        ParseBinlogOffset(response->first_log_offset(), &first_log_offset);
        Status s = StepProgress(progress, first_log_offset, remote_last_offset);
        if (!s.ok()) {
          progress->UnsafeBecomeNotSync();
          progress->UnsafeInitializeNextIndex(remote_last_offset);

          LOG(WARNING) << "Replication group: " << group_id_.ToString()
                       << " receive a kMsgAppendResp from " << remote_id.ToString()
                       << " at term " << current_term
                       << ", updated the progress failed"
                       << ", acked_first_log_offset: " << first_log_offset.ToString()
                       << ", acked_last_log_offset: " << remote_last_offset.ToString()
                       << ", change the state to kNotSync";
          break;
        }
        // 2. when we in PreSnapshotSync stage, we should send the snapshot to peer.
        if (progress->UnsafeGetFollowerState().code() == FollowerStateCode::kFollowerPreSnapshotSync) {
          // The advance request has been received on the peer and the environment
          // has been prepared, so the snapshot data can be transferred securely right now.
          SendSnapshotToPeer(progress);
          return;
        }
        // 3. When we in ProbeSync or SnapshotSync stage, we should change to BinlogSync
        //    after received the positive response.
        if (!progress->UnsafeJudgeReadyForBinlogSync()) {
          LogOffset start_offset;
          progress->UnsafeGetNextIndex(&start_offset.l_offset.index, &start_offset.b_offset);
          auto log_reader = log_manager_->GetLogReader(start_offset);
          if (log_reader == nullptr) {
            // The logs may have been purged when waiting for the response,
            // change the state to NotSync for later ProbeSync (should trigger SnapshotSync).
            progress->UnsafeBecomeNotSync();

            LOG(WARNING) << "Replication group: " << group_id_.ToString()
                         << " receive a kMsgAppendResp from " << remote_id.ToString()
                         << " at term " << current_term
                         << ", remote_last_offset: " << remote_last_offset.ToString()
                         << ", start_offset: " << start_offset.ToString()
                         << ", change the state to BinlogSync failed as we cannot locate the next log";
            break;
          }
          Status s = progress->UnsafeBecomeBinlogSync(std::move(log_reader));
          if (!s.ok()) {
            progress->UnsafeBecomeNotSync();

            LOG(ERROR) << "Replication group: " << group_id_.ToString()
                       << " progress " << progress->UnsafeGetPeerID().ToString()
                       << " change the state to BinlogSync failed: " << s.ToString();
            break;
          }
          LOG(INFO) << "Replication group: " <<group_id_.ToString()
                    << " progress " << progress->UnsafeGetPeerID().ToString()
                    << " change the state to BinlogSync success.";
        }
      } while (0);
    }
    // the next_index_ should have been adjusted when reach here.
    AppendEntriesToPeer(progress);
    //options_.rm_->SignalAuxiliary();
    progress->Unlock();
  }
}

void ClusterRGNode::LogReplicator::HandleHeartbeatResponse(const InnerMessage::RaftMessage* response) {
  PeerID remote_id(response->from().ip(), response->from().port());
  auto progress = progress_set_->GetProgress(remote_id);
  if (progress == nullptr) {
    return;
  }
  {
    progress->WriteLock();
    progress->UnsafeSetLastRecvTime(slash::NowMicros());
    const auto& remote_term = response->term();
    const uint32_t current_term = progress->UnsafeGetTerm();
    if (progress->UnsafeGetStopped()) {
      progress->Unlock();

      LOG(INFO) << "Replication group: " << group_id_.ToString()
                << " receive a HeartbeatResponse from " << remote_id.ToString()
                << " at term " << current_term
                << ", the progress is shutting down";
      return;
    }
    DLOG(INFO) << "Replication group: " <<group_id_.ToString()
               << " receive HeartbeatResponse from " << remote_id.ToString()
               << ", remote term " << remote_term
               << ", local term " << current_term;
    if (remote_term > current_term) {
      progress->Unlock();

      auto node = node_.lock();
      if (node == nullptr) {
        return;
      }
      node->TryUpdateTerm(remote_term, PeerID());
      return;
    }
    AppendEntriesToPeer(progress);
    progress->Unlock();
  }
}

void ClusterRGNode::LogReplicator::BroadcastAppendEntries() {
  const PeerID& local_id = local_id_;
  auto progress_vec = progress_set_->AllProgress([local_id] (const ClusterProgressSet::ProgressPtr& p) -> bool {
      return p->peer_id() == local_id;
  });
  for (auto& progress : progress_vec) {
    {
      progress->WriteLock();
      if (progress->UnsafeGetStopped()) {
        progress->Unlock();
        continue;
      }
      AppendEntriesToPeer(progress);
      progress->Unlock();
    }
  }
}

void ClusterRGNode::LogReplicator::BroadcastHeartbeat() {
  const PeerID& local_id = local_id_;
  auto progress_vec = progress_set_->AllProgress([local_id] (const ClusterProgressSet::ProgressPtr& p) -> bool {
      return p->peer_id() == local_id;
  });
  for (auto& progress : progress_vec) {
    {
      progress->WriteLock();
      if (progress->UnsafeGetStopped()) {
        progress->Unlock();
        continue;
      }
      SendHeartbeatToPeer(progress);
      progress->Unlock();
    }
  }
}

void ClusterRGNode::LogReplicator::SendHeartbeatToPeer(const std::shared_ptr<ClusterProgress>& progress) {
  const LogOffset& committed_offset = box_->committed_offset();
  const uint32_t current_term = progress->UnsafeGetTerm();
  InnerMessage::RaftMessage msg;
  msg.set_type(InnerMessage::RaftMessageType::kMsgHeartbeat);
  msg.set_term(current_term);

  LogOffset matched_offset;
  matched_offset.l_offset = progress->matched_offset();
  LogOffset real_committed_offset;
  if (committed_offset.l_offset.index <= matched_offset.l_offset.index) {
    real_committed_offset = committed_offset;
  } else {
    real_committed_offset = matched_offset;
  }
  BuildBinlogOffset(real_committed_offset, msg.mutable_commit());

  auto group_id = msg.mutable_group_id();
  group_id->set_table_name(group_id_.TableName());
  group_id->set_partition_id(group_id_.PartitionID());
  auto from = msg.mutable_from();
  from->set_ip(local_id_.Ip());
  from->set_port(local_id_.Port());
  auto to = msg.mutable_to();
  const PeerID& remote_id = progress->UnsafeGetPeerID();
  to->set_ip(remote_id.Ip());
  to->set_port(remote_id.Port());

  std::string to_send;
  if (!msg.SerializeToString(&to_send)) {
    progress->UnsafeBecomeNotSync();

    LOG(ERROR) << "Replication group: " << group_id_.ToString()
               << " serialize heartbeat request failed, to  " << remote_id.ToString();
    return;
  }
  msg_sender_->AppendSimpleTask(remote_id, group_id_, local_id_, 0, MessageType::kRequest, std::move(to_send));

  DLOG(INFO) << "Replication group: " << group_id_.ToString()
             << " send HeartbeatRequest to follower " << progress->UnsafeGetPeerID().ToString()
             << " , committed_offset: " << committed_offset.ToString();
}

void ClusterRGNode::LogReplicator::AppendEntriesToPeer(const std::shared_ptr<ClusterProgress>& progress) {
  // There are three situations that may arise:
  // 1. the pending binlogs is full
  // 2. the snapshot is sending
  // 3. the probe request is sending
  if (progress->UnsafeJudgePaused()) {
    DLOG(INFO) << "Replication group: " <<group_id_.ToString()
               << " progress is paused " << progress->UnsafeGetPeerID().ToString()
               << ", local term " << progress->UnsafeGetTerm();
    return;
  }
  const auto& progress_state_code = progress->UnsafeGetFollowerState().code();
  switch (progress_state_code) {
    case FollowerStateCode::kFollowerNotSync: {
      // There are four situations that may arise:
      // 1. It's the first time to replicate.
      // 2. Peer unreachable (connection loss/timeout).
      // 3. Peer temporarily unavailable (has not been initialized).
      // 4. handle the response error.
      //
      // NOTE: when in this state, we do not send the real entries to peer,
      //       we just to check if the prev_offset is matched with peers and
      //       broadcast our committed offset.
      SendEmptyEntriesToPeer(progress);
      break;
    }
    case FollowerStateCode::kFollowerBinlogSync: {
      // return when there are no more logs need to be replicated.
      uint64_t next_index;
      BinlogOffset next_hint_offset;
      progress->UnsafeGetNextIndex(&next_index, &next_hint_offset);
      if (next_index > log_manager_->GetLastPendingOffset().l_offset.index) {
        DLOG(INFO) << "Replication group: " <<group_id_.ToString()
                   << ", peer " << progress->UnsafeGetPeerID().ToString()
                   << " current next index: " << next_index
                   << " larger than local last index: "
                   << log_manager_->GetLastPendingOffset().ToString();
        break;
      }
      Status s = SendEntriesToPeer(progress, true);
      if (!s.ok()) {
        progress->UnsafeBecomeNotSync();
        progress->UnsafeInitializeNextIndex(log_manager_->GetLastPendingOffset());

        LOG(ERROR) << "Replication group: " << group_id_.ToString()
                   << " prepare infligt binlogs for follower " << progress->UnsafeGetPeerID().ToString()
                   << " error: " << s.ToString()
                   << " , change the follower state to NotSync";
      }
      break;
    }
    default: {
      LOG(WARNING) << "Replication group: " <<group_id_.ToString()
                   << " progress " << progress->UnsafeGetPeerID().ToString()
                   << " in state " << progress->UnsafeGetFollowerState().ToString();
    }
  }
  return;
}

void ClusterRGNode::LogReplicator::SendEmptyEntriesToPeer(const std::shared_ptr<ClusterProgress>& progress) {
  uint64_t next_index;
  BinlogOffset next_hint_offset;
  progress->UnsafeGetNextIndex(&next_index, &next_hint_offset);
  LogOffset prev_offset = log_manager_->GetLogOffset(next_hint_offset, next_index - 1);
  if (prev_offset.l_offset.term == 0) {
    // There are three situations that may arise:
    // 1. The log has been purged.
    // 2. follower has conflict binlogs with leader.
    // 3. the old data may be taged with zero term, we send a
    //    snapshot (for backward compatibility).
    //
    // Before send snapshot to peer, an advance request should be sent at first
    // to notify peer to make the appropriate checks and preparations.
    SendAdvanceSnapshotToPeer(progress);
    return;
  }

  BinlogChip chip;
  chip.prev_offset = prev_offset;
  chip.committed_offset = box_->committed_offset();
  msg_sender_->AppendBinlogTask(progress->UnsafeGetPeerID(), group_id_, local_id_,
                                0, MessageType::kRequest, std::move(chip));

  // After send the empty entries to peer, we change the state to ProbeSync to block
  // the future writes until we have received the positive response from the peer.
  auto s = progress->UnsafeBecomeProbeSync();
  if (!s.ok()) {
    progress->UnsafeBecomeNotSync();
    progress->UnsafeInitializeNextIndex(log_manager_->GetLastPendingOffset());

    LOG(ERROR) << "Replication group: " << group_id_.ToString()
               << " progress " << progress->UnsafeGetPeerID().ToString()
               << " change the state to ProbeSync failed: " << s.ToString();
  }
  return;
}

class SnapshotSendCompletedClosure final : public SnapshotSyncClosure {
 public:
  explicit SnapshotSendCompletedClosure(const std::shared_ptr<ClusterRGNode>& rg_node)
    : rg_node_(rg_node) { }

 private:
  ~SnapshotSendCompletedClosure() = default;
  virtual void run() override {
    auto node = rg_node_.lock();
    if (node == nullptr) {
      return;
    }
    node->log_replicator_->OnSnapshotSendCompleted(meta_, status_);
  }
  std::weak_ptr<ClusterRGNode> rg_node_;
};

void ClusterRGNode::LogReplicator::SendAdvanceSnapshotToPeer(const std::shared_ptr<ClusterProgress>& progress) {
  progress->UnsafeBecomePreSnapshotSync();

  const auto& remote_id = progress->UnsafeGetPeerID();
  InnerMessage::RaftMessage msg;
  msg.set_type(InnerMessage::RaftMessageType::kMsgSnapshot);
  msg.set_term(progress->UnsafeGetTerm());
  msg.set_context(kAdvanceSnapshot);
  auto from = msg.mutable_from();
  from->set_ip(local_id_.Ip());
  from->set_port(local_id_.Port());
  auto to = msg.mutable_to();
  to->set_ip(remote_id.Ip());
  to->set_port(remote_id.Port());
  auto group_id = msg.mutable_group_id();
  group_id->set_table_name(group_id_.TableName());
  group_id->set_partition_id(group_id_.PartitionID());

  std::string to_send;
  if (!msg.SerializeToString(&to_send)) {
    progress->UnsafeBecomeNotSync();

    LOG(ERROR) << "Replication group: " << group_id_.ToString()
               << " serialize kMsgSnapshot failed, to " << remote_id.ToString();
    return;
  }
  msg_sender_->AppendSimpleTask(remote_id, group_id_, local_id_, 0, MessageType::kRequest, std::move(to_send));
}

void ClusterRGNode::LogReplicator::SendSnapshotToPeer(const std::shared_ptr<ClusterProgress>& progress) {
  progress->UnsafeBecomeSnapshotSync(0);

  DLOG(INFO) << "Replication group: " <<group_id_.ToString()
             << " send snapshot to peer " << progress->UnsafeGetPeerID().ToString()
             << ", local term " << progress->UnsafeGetTerm();

  SnapshotSendCompletedClosure* closure = new SnapshotSendCompletedClosure(node_.lock());
  const PeerID& peer_id = progress->UnsafeGetPeerID();
  // Release the lock to avoid dead-lock.
  progress->Unlock();

  fsm_caller_->TrySendSnapshot(peer_id, group_id_, log_manager_->LogFileName(),
                               box_->snapshot_offset().b_offset.filenum, closure);
}

void ClusterRGNode::LogReplicator::OnSnapshotSendCompleted(const SnapshotSyncClosure::SnapshotSyncMetaInfo& meta,
                                                           const Status& status) {
  const PeerID& remote_id = meta.remote_id;
  auto progress = progress_set_->GetProgress(remote_id);
  if (progress == nullptr) {
    LOG(WARNING) << "Replication group: " << group_id_.ToString()
                 << " the follower " << meta.remote_id.ToString()
                 << " has been removed when syncing DB";
    return;
  }
  {
    progress->WriteLock();
    if (progress->UnsafeGetStopped()) {
      progress->Unlock();

      LOG(WARNING) << "Replication group: " << group_id_.ToString()
                   << " peer " << meta.remote_id.ToString()
                   << " is shutting down.";
      return;
    }
    if (!status.ok()) {
      // Skip failed SnapshotSync, we will retry in another loop
      progress->UnsafeBecomeNotSync();
      progress->Unlock();

      LOG(ERROR) << "Replication group: " << group_id_.ToString()
                 << " send snapshot data to " << meta.remote_id.ToString()
                 << " failed " << status.ToString();
      return;
    } else {
      // Recorrect the pending snapshot index
      progress->UnsafeBecomeSnapshotSync(meta.snapshot_offset.l_offset.index);
    }
    DLOG(INFO) << "Replication group: " << group_id_.ToString()
               << " send snapshot data to " << meta.remote_id.ToString()
               << " successfully, now prepare to send meta "
               << meta.snapshot_offset.ToString();
    InnerMessage::RaftMessage msg;
    msg.set_type(InnerMessage::RaftMessageType::kMsgSnapshot);
    msg.set_term(progress->UnsafeGetTerm());
    auto from = msg.mutable_from();
    from->set_ip(local_id_.Ip());
    from->set_port(local_id_.Port());
    auto to = msg.mutable_to();
    to->set_ip(meta.remote_id.Ip());
    to->set_port(meta.remote_id.Port());
    auto group_id = msg.mutable_group_id();
    group_id->set_table_name(group_id_.TableName());
    group_id->set_partition_id(group_id_.PartitionID());
    auto snap_meta = msg.mutable_snap_meta();
    BuildBinlogOffset(meta.snapshot_offset, snap_meta->mutable_snapshot_offset());
    auto conf_state = snap_meta->mutable_conf_state();
    conf_state->CopyFrom(meta.conf.ConfState());

    std::string to_send;
    if (!msg.SerializeToString(&to_send)) {
      progress->UnsafeBecomeNotSync();
      progress->Unlock();

      LOG(ERROR) << "Replication group: " << group_id_.ToString()
                 << " serialize kMsgSnapshot failed, to  " << meta.remote_id.ToString();
      return;
    }
    msg_sender_->AppendSimpleTask(meta.remote_id, group_id_, local_id_, 0, MessageType::kRequest, std::move(to_send));
    progress->Unlock();
  }
}

Status ClusterRGNode::LogReplicator::SendEntriesToPeer(const std::shared_ptr<ClusterProgress>& progress,
                                                       bool send_when_all_acked) {
  std::list<BinlogChip> binlog_chips;
  Status s = progress->UnsafePrepareInfligtBinlog(binlog_chips, send_when_all_acked);
  if (!s.ok()) {
    return s;
  }
  const LogOffset& committed_offset = box_->committed_offset();
  BinlogTaskList tasks;
  for (auto& chip : binlog_chips) {
    chip.committed_offset = committed_offset;
    DLOG(INFO) << "Replication group: " << group_id_.ToString()
               << " send entrie to " << progress->UnsafeGetPeerID().ToString()
               << ", prev_log_offset: " << chip.prev_offset.ToString()
               << ", committed_offset: " << chip.committed_offset.ToString()
               << ", item attrs: " << chip.attrs.term_id << ":" << chip.attrs.logic_id
               << ":" << chip.attrs.filenum << ":" << chip.attrs.offset;
    tasks.emplace_back(group_id_, local_id_, 0, MessageType::kRequest, std::move(chip));
  }
  if (!tasks.empty()) {
    msg_sender_->AppendBinlogTasks(progress->UnsafeGetPeerID(), std::move(tasks));
  }
  return Status::OK();
}

Status ClusterRGNode::LogReplicator::StepProgress(const std::shared_ptr<ClusterProgress>& progress,
                                                  const LogOffset& start, const LogOffset& end) {
  LogicOffset self_persisted_offset;
  // Try to update the matched index
  if (progress->UnsafeGetPeerID() == local_id_) {
    progress->TryToUpdateMatchedOffset(end.l_offset);
    self_persisted_offset = end.l_offset;
  } else {
    Status s = progress->UnsafeStepOffset(start, end);
    if (!s.ok()) {
      return s;
    }
    auto self_progress = progress_set_->GetProgress(local_id_);
    if (self_progress == nullptr) {
      return Status::Corruption("Self has been removed from configuration");
    }
    self_persisted_offset = self_progress->matched_offset();
  }

  // We only commit the logs when we (the leader) have persisted them to avoid stale read.
  const auto& committed_offset = progress_set_->GetCommittedOffset();
  const auto& picked_committed_offset = self_persisted_offset.index < committed_offset.index
                                        ? self_persisted_offset : committed_offset;
  box_->TryToUpdateCommittedOffset(picked_committed_offset);

  // Judge if the follower has catched up, tell the follower to timeout now if so.
  const uint64_t timeout_index = progress->UnsafeGetTimeoutIndex();
  if (timeout_index > 0 && progress->matched_index() >= timeout_index) {
    SendTimeoutNow(progress);
    progress->UnsafeSetTimeoutIndex(0);
  }

  DLOG(INFO) << "Replication group: " << group_id_.ToString()
             << " step progress to " << progress->UnsafeGetPeerID().ToString()
             << ", start: " << start.ToString()
             << ", end: " << end.ToString()
             << ", progress set committed offset: " << committed_offset.ToString()
             << ", self_persisted_offset: " << self_persisted_offset.ToString()
             << ", picked_committed_offset: " << picked_committed_offset.ToString()
             << ", timeout_index: " << timeout_index
             << ", matched_index: " << progress->matched_index();
  return Status::OK();
}

void ClusterRGNode::LogReplicator::SendTimeoutNow(const std::shared_ptr<ClusterProgress>& progress) {
  const PeerID& peer_id = progress->UnsafeGetPeerID();
  InnerMessage::RaftMessage request;
  request.set_term(progress->UnsafeGetTerm());

  request.set_type(InnerMessage::RaftMessageType::kMsgTimeoutNow);
  auto from = request.mutable_from();
  from->set_ip(local_id_.Ip());
  from->set_port(local_id_.Port());
  auto to = request.mutable_to();
  to->set_ip(peer_id.Ip());
  to->set_port(peer_id.Port());
  auto group_id = request.mutable_group_id();
  group_id->set_table_name(group_id_.TableName());
  group_id->set_partition_id(group_id_.PartitionID());

  std::string to_send;
  if (!request.SerializeToString(&to_send)) {
    LOG(ERROR) << "Replication group: " << group_id_.ToString()
               << " serialize kMsgTimeoutNow failed, to  " << peer_id.ToString();
    return;
  }
  msg_sender_->AppendSimpleTask(peer_id, group_id_, local_id_, 0, MessageType::kRequest, std::move(to_send));
  LOG(INFO) << "Replication group: " << group_id_.ToString()
            << " send timeout now request to " << progress->UnsafeGetPeerID().ToString();
}

/* ClusterRGNode */

class GroupResetCompletedClosure final : public Closure {
 public:
  explicit GroupResetCompletedClosure(const std::shared_ptr<ClusterRGNode>& rg_node)
    : rg_node_(rg_node) { }

 private:
  ~GroupResetCompletedClosure() = default;
  virtual void run() override {
    if (rg_node_ == nullptr) {
      return;
    }
    LOG(INFO) << "Replication group: " << rg_node_->group_id_.ToString()
              << " reset completed.";
    std::unique_lock<std::mutex> lk(rg_node_->raft_mutex_);
    rg_node_->BecomeFollower(PeerID(), rg_node_->current_term());
  }
  std::shared_ptr<ClusterRGNode> rg_node_;
};

class LeaderDiskClosure : public DiskClosure {
 public:
  LeaderDiskClosure(const LogOffset& prev_offset,
                    const uint32_t term,
                    const std::shared_ptr<ClusterRGNode>& node)
    : DiskClosure(prev_offset, term), node_(node) {}

  virtual ~LeaderDiskClosure() = default;

  virtual void Run() override;

 private:
  std::shared_ptr<ClusterRGNode> node_;
};

void LeaderDiskClosure::Run() {
  if (!status_.ok()) {
    LOG(WARNING) << "Replication group: " << node_->group_id().ToString()
                 << " write logs into stable storage failed: " << status_.ToString();

    {
      std::unique_lock<std::mutex> lk(node_->raft_mutex_);
      node_->StepDown(PeerID(), node_->current_term(), status_);
    }
    return;
  }
  // After append entries into stable storage, update the leader progress.
  // We do not check term here, as it's safe to update self.
  const LogOffset& last_offset = node_->log_manager_.GetLastStableOffset(true/*in disk thread*/);
  node_->log_replicator_->StepLeaderProgress(last_offset);
}

class FollowerDiskClosure : public DiskClosure {
 public:
  FollowerDiskClosure(const PeerID& peer_id,
                      const LogOffset& prev_offset,
                      const LogOffset& first_log_offset,
                      const LogOffset& last_log_offset,
                      const LogOffset& leader_committed_offset,
                      const std::shared_ptr<ClusterRGNode>& node,
                      const uint32_t term)
    : DiskClosure(prev_offset, term), peer_id_(peer_id),
    first_log_offset_(first_log_offset),
    last_log_offset_(last_log_offset),
    leader_committed_offset_(leader_committed_offset),
    node_(node) {}

  virtual ~FollowerDiskClosure() = default;

  virtual void Run() override;

 private:
  const PeerID peer_id_;
  const LogOffset first_log_offset_;
  const LogOffset last_log_offset_;
  const LogOffset leader_committed_offset_;
  std::shared_ptr<ClusterRGNode> node_;
};

void FollowerDiskClosure::Run() {
  LogOffset last_offset_in_local = node_->log_manager_.GetLastStableOffset(true);
  uint32_t current_term;
  {
    std::unique_lock<std::mutex> l(node_->raft_mutex_);
    current_term = node_->current_term();
    if (!status_.ok() || term_ != current_term) {
      l.unlock();

      node_->SendAppendEntriesResponse(LogOffset(), last_offset_in_local,
                                       peer_id_, current_term, true);
      node_->fsm_caller_->OnSnapshotSyncStart(node_->group_id());
      LOG(WARNING) << "Replication group: " << node_->group_id().ToString()
                   << ", term may have changed or request handle failed"
                   << ", current term " << current_term
                   << ", created term " << term_
                   << ", prev_offset " << prev_offset_.ToString()
                   << ", leader_committed_offset " << leader_committed_offset_.ToString()
                   << ", last_offset_in_local " << last_offset_in_local.ToString();
      return;
    }
  }
  LogicOffset try_committed_offset;
  if (first_log_offset_.Empty()) {
    // No entries
    try_committed_offset = prev_offset_.l_offset < leader_committed_offset_.l_offset
                           ?  prev_offset_.l_offset : leader_committed_offset_.l_offset;
  } else {
    // Exist entries
    try_committed_offset = last_log_offset_.l_offset < leader_committed_offset_.l_offset
                           ? last_log_offset_.l_offset : leader_committed_offset_.l_offset;
  }
  node_->pending_task_box_->TryToUpdateCommittedOffset(try_committed_offset);
  node_->SendAppendEntriesResponse(first_log_offset_, last_offset_in_local, peer_id_, current_term, false);
}

ClusterRGNode::ClusterRGNode(const ClusterRGNodeOptions& options)
  : ReplicationGroupNode(options),
  options_(options),
  meta_storage_(nullptr),
  transfer_ctx_(options.rm),
  volatile_ctx_(PeerID(), RoleState()),
  conf_ctx_(options.conf),
  lease_ctx_(options.election_timeout_ms),
  member_ctx_(),
  pending_task_box_(nullptr),
  snapshot_handler_(nullptr),
  log_replicator_(nullptr),
  progress_set_(options.group_id, options.local_id,
                options.progress_options),
  log_manager_(options.log_options),
  msg_sender_(ClusterMessageSenderOptions(options.group_id,
              options.local_id, 0, 1)),
  vote_box_(),
  election_timer_(std::make_shared<ElectionTimer>()),
  check_quorum_timer_(nullptr),
  heartbeat_timer_(std::make_shared<HeartbeatTimer>()),
  transfer_timer_(std::make_shared<TransferTimer>()) {
  meta_storage_ = static_cast<ClusterMetaStorage*>(options.meta_storage);
  if (options.check_quorum) {
    check_quorum_timer_ = std::make_shared<CheckQuorumTimer>();
  }
  progress_set_.SetLogger(log_manager_.GetBinlogBuilder());
}

ClusterRGNode::~ClusterRGNode() {
  Stop();
}

Status ClusterRGNode::Initialize() {
  Status s = log_manager_.Initialize();
  if (!s.ok()) {
    LOG(ERROR) << "Replication group: " << group_id_.ToString()
               << ", initialize log mangager failed: " << s.ToString();
    return s;
  }

  PendingTaskBox::Options box_options;
  box_options.group_id = group_id_;
  box_options.local_id = local_id_;
  box_options.persistent_info_path =
    options_.log_options.stable_log_options.log_path;
  box_options.executor = options_.executor;
  box_options.fsm_caller = fsm_caller_;
  box_options.node = std::dynamic_pointer_cast<ClusterRGNode>(shared_from_this());
  pending_task_box_ = std::make_shared<PendingTaskBox>(box_options);
  // We get the last offset in local as there should not have any pending disk writes.
  s = pending_task_box_->Initialize(log_manager_.GetLastStableOffset(true));
  if (!s.ok()) {
    LOG(ERROR) << "Replication group: " << group_id_.ToString()
               << ", initialize task box failed: " << s.ToString();
    return s;
  }

  SnapshotHandler::Options snapshot_options;
  snapshot_options.group_id = group_id_;
  snapshot_options.current_term = 0;
  snapshot_options.log_manager = &log_manager_;
  snapshot_options.box = pending_task_box_;
  snapshot_options.fsm_caller = fsm_caller_;
  snapshot_options.node = std::dynamic_pointer_cast<ClusterRGNode>(shared_from_this());
  snapshot_handler_ = std::unique_ptr<SnapshotHandler>(new SnapshotHandler(snapshot_options));

  LogReplicator::Options replicator_options;
  replicator_options.group_id = group_id_;
  replicator_options.local_id = local_id_;
  replicator_options.current_term = 0;
  replicator_options.member_ctx = &member_ctx_;
  replicator_options.log_manager = &log_manager_;
  replicator_options.progress_set = &progress_set_;
  replicator_options.msg_sender = &msg_sender_;
  replicator_options.box = pending_task_box_;
  replicator_options.fsm_caller = fsm_caller_;
  replicator_options.node = std::dynamic_pointer_cast<ClusterRGNode>(shared_from_this());
  log_replicator_ = std::unique_ptr<LogReplicator>(new LogReplicator(replicator_options));

  election_timer_->Initialize(options_.timer_thread, options_.executor,
                             std::static_pointer_cast<ClusterRGNode>(shared_from_this()),
                             options_.election_timeout_ms,
                             true/*Reschedule itself after completed*/);
  if (check_quorum_timer_ != nullptr) {
    check_quorum_timer_->Initialize(options_.timer_thread, options_.executor,
                                    std::static_pointer_cast<ClusterRGNode>(shared_from_this()),
                                    options_.election_timeout_ms, true);
  }
  heartbeat_timer_->Initialize(options_.timer_thread, options_.executor,
                              std::static_pointer_cast<ClusterRGNode>(shared_from_this()),
                              options_.heartbeat_timeout_ms, true);
  transfer_timer_->Initialize(options_.timer_thread, options_.executor,
                             std::static_pointer_cast<ClusterRGNode>(shared_from_this()),
                             options_.election_timeout_ms, false/*One time*/);
  return Status::OK();
}

Status ClusterRGNode::Start() {
  if (started_.load(std::memory_order_acquire)) {
    return Status::Busy("Replication group has been started!");
  }
  Status s = Initialize();
  if (!s.ok()) {
    return s;
  }
  s = Recover();
  if (!s.ok()) {
    return s;
  }
  started_.store(true, std::memory_order_release);
  return s;
}

Status ClusterRGNode::Stop() {
  // NOTE: we do not check the started_ flag here, as the underlying
  //       UnsafeStop() method may have been invoked in ApplyConfChange,
  //       but the resources have not been cleaned up.
  Status s;
  {
    std::unique_lock<std::mutex> lk(raft_mutex_);
    s = UnsafeStop();
  }
  // NOTE: When we turn to kStateStop, any pending disk writes
  //       should be completed before next start. It may blocks
  //       the current thread. And the pending disk writes may
  //       access the raft_mutex_, so we handle it outof the mutex.
  //       In order to make the upper-level applications recognize
  //       that the current node is closed as quickly as possible,
  //       we clean the resources after the main logic(UnsafeStop).
  //
  // TODO: change the Stop() method to unblocking.
  log_manager_.Close();
  return s;
}

// REQUIRES: raft_mutex_ must be held.
Status ClusterRGNode::UnsafeStop() {
  if (!started_.load(std::memory_order_acquire)) {
    return Status::OK();
  }
  BecomeStop();
  started_.store(false, std::memory_order_release);
  return Status::OK();
}

Status ClusterRGNode::Recover() {
  // Recover the unapplied logs and persistent offsets from stable storage.
  Status s = RecoverPerssistentData();
  if (!s.ok()) {
    return s;
  }
  s = log_manager_.Recover(applied_offset(), snapshot_offset());
  if (!s.ok()) {
    return s;
  }

  uint32_t init_term = 0;
  {
    std::unique_lock<std::mutex> lk(raft_mutex_);
    init_term = current_term();
    msg_sender_.ResetTerm(init_term);
  }
  if (init_term != 0) {
    s = Restart();
  } else {
    s = StartAsNew();
  }
  return s;
}

Status ClusterRGNode::Restart() {
  std::unique_lock<std::mutex> lk(raft_mutex_);
  // Reconstruct the cluster according to the persisted configuration
  progress_set_.InitializeWithConfiguration(configuration(),
                                            log_manager_.GetLastPendingOffset(),
                                            current_term());
  BecomeFollower(PeerID(), current_term());
  return Status::OK();
}

Status ClusterRGNode::StartAsNew() {
  // TODO(LIBA-S): according the configuration to construct the cluster-id,
  // ignore all the messages from a different cluster-id.
  auto init_configuration = configuration();
  if (!init_configuration.Empty()) {
    {
      std::unique_lock<std::mutex> lk(raft_mutex_);
      // avoid term 0 (which will be used to mark not exist).
      const uint32_t init_term = 1;
      log_manager_.UpdateTerm(init_term);
      // we persist the configuration into stable storage at first,
      // to ensure the new peer (join the cluster later) can fetch
      // the cluster members through normal log replication.
      PersistTheInitConfiguration(init_configuration);
      // no need to wait the configuration applied
      progress_set_.InitializeWithConfiguration(init_configuration,
                                                log_manager_.GetLastPendingOffset(),
                                                init_term);
      BecomeFollower(PeerID(), init_term);
    }
    // force commit the pending entries outof the lock (init_configuration)
    pending_task_box_->TryToUpdateCommittedOffset(log_manager_.GetLastPendingOffset().l_offset);
  } else {
    std::unique_lock<std::mutex> lk(raft_mutex_);
    // If configuration is empty, waitting to start (join to a existing cluster).
    BecomeFollower(PeerID(), 0);
  }
  return Status::OK();
}

void ClusterRGNode::StartWithConfiguration(const Configuration& conf) {
  std::unique_lock<std::mutex> lk(raft_mutex_);
  set_configuration(conf);
  progress_set_.InitializeWithConfiguration(conf, log_manager_.GetLastPendingOffset(),
                                            current_term());
  BecomeFollower(PeerID(), current_term());
}

// REQUIRES: raft_mutex_ must be held.
void ClusterRGNode::PersistTheInitConfiguration(const Configuration& conf) {
  // persist the init configuration and apply
  std::vector<InnerMessage::ConfChange*> conf_changes;
  for (const auto& voter : conf.Voters()) {
    InnerMessage::ConfChange* conf_change = new InnerMessage::ConfChange();
    conf_change->set_type(InnerMessage::ConfChangeType::kAddVoter);
    auto node = conf_change->mutable_node_id();
    node->set_ip(voter.Ip());
    node->set_port(voter.Port());
    conf_changes.push_back(conf_change);
  }
  for (const auto& learner : conf.Learners()) {
    InnerMessage::ConfChange* conf_change = new InnerMessage::ConfChange();
    conf_change->set_type(InnerMessage::ConfChangeType::kAddLearner);
    auto node = conf_change->mutable_node_id();
    node->set_ip(learner.Ip());
    node->set_port(learner.Port());
    conf_changes.push_back(conf_change);
  }
  size_t conf_size = conf_changes.size();
  std::string to_store;
  for (size_t i = 0; i < conf_size; i++) {
    to_store.clear();
    InnerMessage::BaseEntry entry;
    entry.set_type(InnerMessage::EntryType::kEntryConfChange);
    entry.set_allocated_conf_change(conf_changes[i]);
    if (!entry.SerializeToString(&to_store)) {
      LOG(FATAL) << "Replication group: " << group_id_.ToString()
                 << ", Serialize ConfChangeEntry Failed";
    }
    auto repl_task = std::make_shared<ReplTask>(std::move(to_store), nullptr,
                                                group_id_, local_id_,
                                                ReplTask::Type::kOtherType, ReplTask::LogFormat::kPB);
    log_manager_.AppendLog(repl_task, nullptr, false/*async persisted*/);
  }
}

void ClusterRGNode::ForceStartAsAlone() {
  // When we have applied all pending entries, invoke the closure
  // to mark the end of reset.
  GroupResetCompletedClosure* reset_completed = new GroupResetCompletedClosure(
      std::static_pointer_cast<ClusterRGNode>(shared_from_this()));
  ClosureGuard guard(reset_completed);
  {
    std::unique_lock<std::mutex> lk(raft_mutex_);
    // Append ConfChangeEntries to log storage
    auto conf_changes = CreateConfChanges();
    std::string to_store;
    size_t len = conf_changes.size();
    for (size_t i = 0; i < len; i ++) {
      to_store.clear();
      InnerMessage::BaseEntry entry;
      entry.set_type(InnerMessage::EntryType::kEntryConfChange);
      entry.set_allocated_conf_change(conf_changes[i]);
      if (!entry.SerializeToString(&to_store)) {
        LOG(FATAL) << "Replication group: " << group_id_.ToString()
                   << ", Serialize ConfChangeEntry Failed";
      }
      auto repl_task = std::make_shared<ReplTask>(std::move(to_store),
                                                  (i == len - 1 ? guard.Release(): nullptr),
                                                  group_id_, local_id_,
                                                  ReplTask::Type::kOtherType, ReplTask::LogFormat::kPB);
      log_manager_.AppendLog(repl_task, nullptr, false/*async persisted*/);
    }
  }
  // force commit the pending entries
  pending_task_box_->TryToUpdateCommittedOffset(log_manager_.GetLastPendingOffset().l_offset);
}

std::vector<InnerMessage::ConfChange*> ClusterRGNode::CreateConfChanges() {
  std::vector<InnerMessage::ConfChange*> conf_changes;
  const auto& current_conf = configuration();
  bool self_in_conf = current_conf.Contains(local_id_);
  if (!self_in_conf) {
    // If we are not in configuration right now, add it.
    InnerMessage::ConfChange* conf_change = new InnerMessage::ConfChange();
    conf_change->set_type(InnerMessage::ConfChangeType::kAddVoter);
    auto node = conf_change->mutable_node_id();
    node->set_ip(local_id_.Ip());
    node->set_port(local_id_.Port());
    conf_changes.push_back(conf_change);
  }
  auto conf_state = current_conf.ConfState();
  // Remove all others.
  for (auto voter : conf_state.voter()) {
    InnerMessage::ConfChange* conf_change = new InnerMessage::ConfChange();
    conf_change->set_type(InnerMessage::ConfChangeType::kRemoveNode);
    auto node = conf_change->mutable_node_id();
    node->set_ip(voter.ip());
    node->set_port(voter.port());
    conf_changes.push_back(conf_change);
  }
  for (auto learner : conf_state.learner()) {
    InnerMessage::ConfChange* conf_change = new InnerMessage::ConfChange();
    conf_change->set_type(InnerMessage::ConfChangeType::kRemoveNode);
    auto node = conf_change->mutable_node_id();
    node->set_ip(learner.ip());
    node->set_port(learner.port());
    conf_changes.push_back(conf_change);
  }
  return conf_changes;
}

Status ClusterRGNode::ApplyConfChange(const InnerMessage::ConfChange& change) {
  std::unique_lock<std::mutex> lk(raft_mutex_);
  return UnsafeApplyConfChange(change);
}

Status ClusterRGNode::UnsafeApplyConfChange(const InnerMessage::ConfChange& change) {
  Status s = log_replicator_->ApplyConfChange(change);
  if (!s.ok()) {
    return s;
  }
  auto progress = progress_set_.GetProgress(local_id_);
  if (progress == nullptr && RoleCode() == RoleStateCode::kStateLeader) {
    // Self has been removed from the replication group.
    // For better failover, the transfer-leadership should be invoked.
    UnsafeStop();
    return s;
  }
  if (RoleCode() != RoleStateCode::kStateLeader) {
    return s;
  }
  // Cancel the transferring if the target leader is removed.
  if (!LeaderTransferee().Empty()
      && !progress_set_.IsExist(LeaderTransferee())) {
    CancelLeaderTransfer(LeaderTransferContext::CancelReason::Code::kTargetLeaderIsRemoved);
  }
  return s;
}

void ClusterRGNode::TryUpdateTerm(const uint32_t remote_term, const PeerID& remote_id) {
  std::unique_lock<std::mutex> lk(raft_mutex_);
  if (remote_term <= current_term()) {
    return;
  }
  // Step down to follower state
  StepDown(remote_id, remote_term, Status::Corruption("Receive a message with higher term"));
}

// REQUIRES: raft_mutex_ must be held
void ClusterRGNode::StepDown(const PeerID& leader_id, uint32_t remote_term, const Status& status) {
  if (!status.ok()) {
    LOG(INFO) << "Replication group: " << group_id_.ToString()
              << " step down at term: " << current_term()
              << ", status: " << status.ToString();
    fsm_caller_->OnReplError(group_id_, status);
  }
  BecomeFollower(leader_id, remote_term);
}

// REQUIRES: raft_mutex_ must be held
void ClusterRGNode::BecomeUninitialized() {
  ResetState(current_term());
  SetRoleCode(RoleStateCode::kStateUninitialized);
  LOG(INFO) << "Replication group: " << group_id_.ToString()
            << " becomes uninitailized at term: " << current_term();
}

// REQUIRES: raft_mutex_ must be held
void ClusterRGNode::BecomeStop() {
  ResetState(current_term());
  SetRoleCode(RoleStateCode::kStateStop);

  // stop timer
  election_timer_->Stop();
  heartbeat_timer_->Stop();
  transfer_timer_->Stop();

  // transfer leadership
  log_replicator_->StopAndTryToTransferLeadership();

  // notify stop
  fsm_caller_->OnStop(group_id_);
  LOG(INFO) << "Replication group: " << group_id_.ToString()
            << " becomes stop at term: " << current_term();
}

// REQUIRES: raft_mutex_ must be held
void ClusterRGNode::BecomeCandidate() {
  ResetState(current_term() + 1);
  set_voted_for(local_id_);
  SetRoleCode(RoleStateCode::kStateCandidate);
  LOG(INFO) << "Replication group: " << group_id_.ToString()
            << " becomes candidate at term: " << current_term();
}

// REQUIRES: raft_mutex_ must be held
void ClusterRGNode::BecomePreCandidate() {
  vote_box_.ResetVotes(progress_set_.Voters());
  set_leader_id(PeerID());
  SetRoleCode(RoleStateCode::kStatePreCandidate);
  LOG(INFO) << "Replication group: " << group_id_.ToString()
            << " becomes pre-candidate at term: " << current_term();
}

// REQUIRES: raft_mutex_ must be held
void ClusterRGNode::BecomeFollower(const PeerID& leader_id, uint32_t term) {
  ResetState(term);
  set_leader_id(leader_id);
  SetRoleCode(RoleStateCode::kStateFollower);
  fsm_caller_->OnLeaderChanged(group_id_, leader_id);
  LOG(INFO) << "Replication group: " << group_id_.ToString()
            << " becomes follower at term: " << term
            << " , leader is: " << leader_id.ToString();
}

// REQUIRES: raft_mutex_ must be held
Status ClusterRGNode::BecomeLeader() {
  ResetState(current_term());
  if (check_quorum_timer_ != nullptr) {
    check_quorum_timer_->Restart();
  }
  log_replicator_->StartAll();
  heartbeat_timer_->Restart();
  SetRoleCode(RoleStateCode::kStateLeader);
  set_leader_id(local_id_);
  // It's safe to delay any future changes before we have applied all
  // pending logs.
  //set_pending_conf_change_index(log_manager_.last_offset().l_offset.index);

  // Append a dummy entry to commit the previous logs when we start a new
  // term to avoid ghost logs.
  LeaderDiskClosure* disk_closure = new LeaderDiskClosure(
      LogOffset(), current_term(), std::dynamic_pointer_cast<ClusterRGNode>(shared_from_this()));
  auto repl_task = std::make_shared<ReplTask>("", nullptr, group_id_, local_id_,
                                              ReplTask::Type::kDummyType,
                                              ReplTask::LogFormat::kRedis);
  Status s = log_manager_.AppendLog(repl_task, disk_closure);
  if (!s.ok()) {
    disk_closure->Run();
    delete disk_closure;

    return s;
  }

  fsm_caller_->OnLeaderChanged(group_id_, local_id_);
  log_replicator_->BroadcastAppendEntries();
  rm_->SignalAuxiliary();
  LOG(INFO) << "Replication group: " << group_id_.ToString()
            << " becomes leader at term: " << current_term();
  return s;
}

// REQUIRES: raft_mutex_ should be held.
void ClusterRGNode::ResetState(const uint32_t term) {
  if (current_term() < term) {
    // NOTE: If the term is changed, we must ensure there are no flying requests
    //       will be sent to others (if we were a leader). Otherwise the old messages
    //       with new term will disrupt the current leader.
    log_manager_.UpdateTerm(term);
    set_term_and_voted_for(term, PeerID());
    // 1) stop the log_replicator to avoid new messages to be produced
    log_replicator_->ResetTermAndStopAll(term, log_manager_.GetLastPendingOffset());
    // 2) clear all pending messages
    msg_sender_.ResetTerm(term);
  }
  snapshot_handler_->ResetWithTerm(term);
  election_timer_->Restart();
  transfer_timer_->Stop();
  heartbeat_timer_->Stop();
  if (check_quorum_timer_ != nullptr) {
    check_quorum_timer_->Stop();
  }
  vote_box_.ResetVotes(progress_set_.Voters());
  set_leader_id(PeerID());
  update_lease_ctx(PeerID());
  CancelLeaderTransfer(LeaderTransferContext::CancelReason::Code::kRoleChanged);
  set_pending_conf_change_index(0);
}

void ClusterRGNode::StepMessages(InnerMessage::RaftMessage* in_msg) {
  auto msg_type = in_msg->type();
  switch (msg_type) {
    case InnerMessage::RaftMessageType::kMsgPreVote: {
      HandlePreVoteRequest(in_msg);
      return;
    }
    case InnerMessage::RaftMessageType::kMsgPreVoteResp: {
      HandlePreVoteResponse(in_msg);
      return;
    }
    case InnerMessage::RaftMessageType::kMsgVote: {
      HandleVoteRequest(in_msg);
      return;
    }
    case InnerMessage::RaftMessageType::kMsgVoteResp: {
      HandleVoteResponse(in_msg);
      return;
    }
    case InnerMessage::RaftMessageType::kMsgAppend: {
      HandleAppendEntriesRequest(in_msg);
      return;
    }
    case InnerMessage::RaftMessageType::kMsgAppendResp: {
      HandleAppendEntriesResponse(in_msg);
      return;
    }
    case InnerMessage::RaftMessageType::kMsgHeartbeat: {
      HandleHeartbeatRequest(in_msg);
      return;
    }
    case InnerMessage::RaftMessageType::kMsgHeartbeatResp: {
      HandleHeartbeatResponse(in_msg);
      return;
    }
    case InnerMessage::RaftMessageType::kMsgTimeoutNow: {
      HandleTimeoutNowRequest(in_msg);
      return;
    }
    case InnerMessage::RaftMessageType::kMsgSnapshot: {
      HandleSnapshotRequest(in_msg);
      return;
    }
  };
}

// REQUIRES: raft_mutex_ should be held.
void ClusterRGNode::StepMessageByRole(std::unique_lock<std::mutex>* lk,
                                      const InnerMessage::RaftMessage* msg) {
  switch (RoleCode()) {
    case RoleStateCode::kStateUninitialized: {
      LOG(INFO) << "Replication group: " << group_id_.ToString()
                << " ignore a message at term " << current_term()
                << ", as the local state is uninitailized.";
      break;
    }
    case RoleStateCode::kStatePreCandidate: {
      StepPreCandidate(lk, msg);
      break;
    }
    case RoleStateCode::kStateCandidate: {
      StepCandidate(lk, msg);
      break;
    }
    case RoleStateCode::kStateFollower: {
      StepFollower(lk, msg);
      break;
    }
    case RoleStateCode::kStateLeader: {
      LOG(WARNING) << "Replication group: " << group_id_.ToString()
                   << " receive a message at term " << current_term()
                   << " as the leader, which should not happen.";
      break;
    }
    case RoleStateCode::kStateStop: {
      LOG(ERROR) << "Replication group: " << group_id_.ToString()
                 << " receive a message at term " << current_term()
                 << " when in stop state.";
      break;
    }
    case RoleStateCode::kStateError: {
      LOG(ERROR) << "Replication group: " << group_id_.ToString()
                 << " receive a message at term " << current_term()
                 << " when in error state.";
      break;
    }
  }
}

class VoteRequestClosure : public LastOffsetClosure {
 public:
  VoteRequestClosure(const bool is_prev_vote,
                     const uint32_t remote_term,
                     const PeerID& remote_id,
                     const LogOffset& remote_last_offset,
                     const uint32_t closure_created_term,
                     std::shared_ptr<ClusterRGNode> node)
    : is_prev_vote_(is_prev_vote),
    remote_term_(remote_term), remote_id_(remote_id),
    remote_last_offset_(remote_last_offset),
    closure_created_term_(closure_created_term), node_(node) { }

 protected:
  virtual void run() override;
  virtual ~VoteRequestClosure() = default;

 private:
  const bool is_prev_vote_;
  const uint32_t remote_term_; // term for the VoteRequest
  const PeerID remote_id_;
  const LogOffset remote_last_offset_;
  const uint32_t closure_created_term_; // term for the Closure
  std::shared_ptr<ClusterRGNode> node_;
};

void VoteRequestClosure::run() {
  bool granted = false;
  uint32_t saved_current_term;
  uint32_t response_term;
  {
    std::unique_lock<std::mutex> lk(node_->raft_mutex_);
    saved_current_term = node_->current_term();
    if (closure_created_term_ != saved_current_term) {
      lk.unlock();
 
      LOG(WARNING) << "Replication group: " << node_->group_id().ToString()
                   << " term is changed when we handle (Pre)VoteRequest"
                   << ", current_term " << saved_current_term
                   << ", expected term " << closure_created_term_;
      return;
    }
    // judge offset
    granted = last_offset_.l_offset <= remote_last_offset_.l_offset;
    if (granted) {
      if (is_prev_vote_) {
        // Note: we response with the term in request when grant it to
        // avoid sender to ignore this response, if we have
        // a lower term (just recover from network partition).
        response_term = remote_term_;
      } else {
        // Save the vote
        node_->set_voted_for(remote_id_);
        // Reset the local election timer to speed up the time to reach agreement
        node_->election_timer_->Restart();
        response_term = remote_term_;
      }
    } else {
      response_term = saved_current_term;
    }
  }

  LOG(INFO) << "Replication group: " << node_->group_id().ToString()
            << " receive a " << (is_prev_vote_ ? "PreVote" : "Vote")
            << " message with term " << remote_term_
            << " from " << remote_id_.ToString()
            << ", local term is " << saved_current_term 
            << ", remote last offset: " << remote_last_offset_.ToString()
            << ", local last offset: " << last_offset_.ToString()
            << (granted ? "grant" : "reject") << " the request";
  node_->SendVoteResponse(remote_id_, response_term, is_prev_vote_, granted);
}

void ClusterRGNode::HandlePreVoteRequest(const InnerMessage::RaftMessage* request) {
  const auto& remote_term = request->term();
  PeerID remote_id(request->from().ip(), request->from().port());

  if (!started_.load(std::memory_order_acquire)) {
    LOG(INFO) << "Replication group: " << group_id_.ToString()
              << " ignore PreVoreRequest as we have not been started"
              << ", from " << remote_id.ToString()
              << ", remote term " << remote_term;
    return;
  }

  std::unique_lock<std::mutex> lk(raft_mutex_);
  const uint32_t saved_current_term = current_term();
  // Check term
  if (remote_term < saved_current_term) {
    lk.unlock();

    SendVoteResponse(remote_id, saved_current_term, true, false);
    LOG(INFO) << "Replication group: " << group_id_.ToString()
              << " ignore PreVoreRequest with lower term " << remote_term
              << " from " << remote_id.ToString()
              << ", local term is " << saved_current_term;
    return;
  } else if (remote_term > saved_current_term) {
    // Reject when in lease
    if (options_.check_quorum && !lease_is_expired()) {
      lk.unlock();

      SendVoteResponse(remote_id, saved_current_term, true, false);
      LOG(INFO) << "Replication group: " << group_id_.ToString()
                << " receive a PreVote message with term " << remote_term
                << " from " << remote_id.ToString()
                << ", local term is " << saved_current_term
                << " reject the request as the current lease is not expired"
                << ", current leader: " << leader_id().ToString();
      return;
    }
    // Do not change our term as the PreVoteMsg use a future term.
  }

  LogOffset remote_last_offset;
  ParseBinlogOffset(request->prev_log(), &remote_last_offset);
  VoteRequestClosure* closure = new VoteRequestClosure(true/*is vote request*/,
      remote_term, remote_id, remote_last_offset, current_term(),
      std::dynamic_pointer_cast<ClusterRGNode>(shared_from_this()));

  log_manager_.GetLastStableOffset(false/*in disk thread*/,
                                    static_cast<LastOffsetClosure*>(closure));
}

void ClusterRGNode::HandlePreVoteResponse(const InnerMessage::RaftMessage* response) {
  const auto& remote_term = response->term();
  PeerID remote_id(response->from().ip(), response->from().port());

  if (!started_.load(std::memory_order_acquire)) {
    LOG(INFO) << "Replication group: " << group_id_.ToString()
              << " ignore PreVoteResponse as we have not been started"
              << ", from " << remote_id.ToString()
              << ", remote term " << remote_term;
    return;
  }

  std::unique_lock<std::mutex> lk(raft_mutex_);
  const uint32_t saved_current_term = current_term();
  // check term
  if (remote_term > saved_current_term) {
    if (!response->reject()) {
      // We send the PreVote request in our future term. We will increment our
      // term when the request is granted. Otherwise, someone owns a higher term,
      // just follow it.
    } else {
      BecomeFollower(PeerID(), remote_term);
    }
  } else if (remote_term < saved_current_term) {
    lk.unlock();

    LOG(INFO) << "Replication group: " << group_id_.ToString()
              << " ignore PreVoteResponse with lower term " << remote_term
              << " from " << remote_id.ToString()
              << ", local term is " << saved_current_term;
    return;
  }
  StepMessageByRole(&lk, response);
}

void ClusterRGNode::HandleVoteRequest(const InnerMessage::RaftMessage* request) {
  const auto& remote_term = request->term();
  PeerID remote_id(request->from().ip(), request->from().port());
  bool ignore_lease = request->context() == kIgnoreLease;

  if (!started_.load(std::memory_order_acquire)) {
    LOG(INFO) << "Replication group: " << group_id_.ToString()
              << " ignore VoteRequest as we have not been started"
              << ", from " << remote_id.ToString()
              << ", remote term " << remote_term;
    return;
  }

  DLOG(INFO) << "Replication group: " << group_id_.ToString()
             << " receive VoteRequest from " << remote_id.ToString()
             << ", remote term " << remote_term;

  std::unique_lock<std::mutex> lk(raft_mutex_);
  uint32_t saved_current_term = current_term();
  // Check term
  if (remote_term < saved_current_term) {
    lk.unlock();

    LOG(INFO) << "Replication group: " << group_id_.ToString()
              << " ignore VoteRequest with lower term " << remote_term
              << " from " << remote_id.ToString()
              << ", local term is " << saved_current_term;
    return;
  } else if (remote_term > saved_current_term) {
    if (options_.check_quorum && !ignore_lease && !lease_is_expired()) {
      lk.unlock();

      LOG(INFO) << "Replication group: " << group_id_.ToString()
                << " receive a vote message with term " << remote_term
                << " from " << remote_id.ToString()
                << ", local term is " << saved_current_term
                << ", reject the request as the current lease is not expired"
                << ", current leader: " << leader_id().ToString();
      SendVoteResponse(remote_id, saved_current_term, false, false);
      return;
    }
    LOG(INFO) << "Replication group: " << group_id_.ToString()
              << " receive VoteRequest with higher term " << remote_term
              << " from " << remote_id.ToString()
              << ", local term is " << saved_current_term;
    BecomeFollower(PeerID(), remote_term);
  }
  // We can vote for the candiate_id when the following conditions are met:
  // 1) voted for a same node.
  // 2) have not voted for anyone.
  //
  // Note: we need to respond it whether we are voters or not. As we may a
  // learner in the past, and have being promoted to voter, but we have not
  // learned it (the ConfChangeEntry has not been synchronized to us yet).
  const PeerID& v_for = voted_for();
  if (!v_for.Empty() && v_for != remote_id) {
    lk.unlock();

    LOG(INFO) << "Replication group: " << group_id_.ToString()
              << " receive a vote message with higher term " << remote_term
              << " from " << remote_id.ToString()
              << ", local term is " << saved_current_term
              << " reject the request as we have voted for " << v_for.ToString();
    SendVoteResponse(remote_id, saved_current_term, false, false);
    return;
  }

  LogOffset remote_last_offset;
  ParseBinlogOffset(request->prev_log(), &remote_last_offset);
  VoteRequestClosure* closure = new VoteRequestClosure(
      false/*is vote request*/, remote_term, remote_id, remote_last_offset, current_term(),
      std::dynamic_pointer_cast<ClusterRGNode>(shared_from_this()));
  log_manager_.GetLastStableOffset(
      false/*in disk thread*/, static_cast<LastOffsetClosure*>(closure));
}

void ClusterRGNode::HandleVoteResponse(const InnerMessage::RaftMessage* response) {
  const auto& remote_term = response->term();
  PeerID remote_id(response->from().ip(), response->from().port());

  if (!started_.load(std::memory_order_acquire)) {
    LOG(INFO) << "Replication group: " << group_id_.ToString()
              << " ignore VoteResponse as we have not been started"
              << ", from " << remote_id.ToString()
              << ", remote term " << remote_term;
    return;
  }

  std::unique_lock<std::mutex> lk(raft_mutex_);
  const uint32_t saved_current_term = current_term();
  // check term
  if (remote_term > saved_current_term) {
    BecomeFollower(PeerID(), remote_term);
  } else if (remote_term < saved_current_term) {
    lk.unlock();

    LOG(INFO) << "Replication group: " << group_id_.ToString()
              << " ignore a message with lower term " << remote_term
              << " from " << remote_id.ToString()
              << ", local term is " << saved_current_term;
    return;
  }
  StepMessageByRole(&lk, response);
}

void ClusterRGNode::HandleAppendEntriesRequest(const InnerMessage::RaftMessage* request) {
  const auto& remote_term = request->term();
  PeerID remote_id(request->from().ip(), request->from().port());

  std::unique_lock<std::mutex> lk(raft_mutex_);
  const uint32_t saved_current_term = current_term();

  if (!started_.load(std::memory_order_acquire)) {
    lk.unlock();

    LOG(INFO) << "Replication group: " << group_id_.ToString()
              << " ignore AppendEntriesRequest as we have not been started"
              << ", from " << remote_id.ToString()
              << ", remote term " << remote_term;
    // We still send a response to the remote
    SendAppendEntriesResponse(LogOffset(), LogOffset(), remote_id, saved_current_term,
                              true/*reject*/, true);
    return;
  }

  DLOG(INFO) << "Replication group: " << options_.group_id.ToString()
             << " receive a AppendEntriesRequest from " << remote_id.ToString()
             << " remote term " << remote_term
             << ", current_term " << saved_current_term;

  // check term
  if (remote_term > saved_current_term) {
    BecomeFollower(remote_id, remote_term);
  } else if (remote_term < saved_current_term) {
    lk.unlock();

    LOG(INFO) << "Replication group: " << group_id_.ToString()
              << " ignore AppendEntriesRequest with lower term " << remote_term
              << " from " << remote_id.ToString()
              << ", local term is " << saved_current_term;
    return;
  }
  StepMessageByRole(&lk, request);
}

// REQUIRES: raft_mutex_ should be held.
void ClusterRGNode::HandleAppendEntriesRequest(std::unique_lock<std::mutex>* lk,
                                               const InnerMessage::RaftMessage* request) {
  PeerID remote_id(request->from().ip(), request->from().port());
  if (snapshot_handler_->IsProcessing()) {
    // Drop the request as there is a snapshot waitting to be handled.
    return;
  }
  const uint32_t saved_current_term = current_term();
  LogOffset prev_offset;
  ParseBinlogOffset(request->prev_log(), &prev_offset);
  LogOffset saved_committed_offset = committed_offset();
  DLOG(INFO) << "Replication group: " << group_id_.ToString()
             << " receive AppendEntriesRequest from " << remote_id.ToString()
             << ", prev_offset " << prev_offset.ToString()
             << ", local committed_offset " << saved_committed_offset.ToString()
             << ", local term is " << saved_current_term;
  if (prev_offset.l_offset.index < saved_committed_offset.l_offset.index) {
    lk->unlock();

    // All the entries before the committed point could be confirmed,
    // as the committed log entries must not be overwritten.
    SendAppendEntriesResponse(LogOffset(), saved_committed_offset,
                              remote_id, saved_current_term, false);
    return;
  }

  // Construct log entries.
  std::vector<LogEntry> entries;
  auto mutable_request = const_cast<InnerMessage::RaftMessage*>(request);
  size_t entries_size = mutable_request->entries_size();
  entries.reserve(entries_size);
  for (size_t index = 0; index < entries_size; index++) {
    auto entry = mutable_request->mutable_entries(index);
    BinlogItem::Attributes attributes;
    ParseBinlogAttributes(entry->attributes(), &attributes);
    entries.push_back(std::move(LogEntry(attributes, std::move(*entry->release_data()))));
  }

  LogOffset first_log_offset;
  LogOffset last_log_offset;
  if (entries_size != 0) {
    first_log_offset = entries.front().GetLogOffset();
    last_log_offset = entries.back().GetLogOffset();
  }
  DLOG(INFO) << "Replication group: " << group_id_.ToString()
             << " receive entry from " << remote_id.ToString()
             << ", first_log_offset: " << first_log_offset.ToString()
             << ", last_log_offset: " << last_log_offset.ToString();
  LogOffset leader_commit_offset;
  ParseBinlogOffset(request->commit(), &leader_commit_offset);

  // Try to append to local log storage
  FollowerDiskClosure* closure = new FollowerDiskClosure(
      remote_id, prev_offset, first_log_offset, last_log_offset, leader_commit_offset,
      std::dynamic_pointer_cast<ClusterRGNode>(shared_from_this()), current_term());
  Status s = log_manager_.AppendReplicaLog(remote_id, entries, closure);
  lk->unlock();

  // NOTE: Run the closure outof lock to avoid dead-lock
  if (!s.ok()) {
    closure->Run();
    delete closure;
  }
}

void ClusterRGNode::HandleAppendEntriesResponse(const InnerMessage::RaftMessage* response) {
  log_replicator_->HandleAppendEntriesResponse(response);
}

void ClusterRGNode::HandleHeartbeatRequest(const InnerMessage::RaftMessage* request) {
  const auto& remote_term = request->term();
  PeerID remote_id(request->from().ip(), request->from().port());

  if (!started_.load(std::memory_order_acquire)) {
    LOG(INFO) << "Replication group: " << group_id_.ToString()
              << " ignore HeartbeatRequest as we have not been started"
              << ", from " << remote_id.ToString()
              << ", remote term " << remote_term;
    return;
  }

  std::unique_lock<std::mutex> lk(raft_mutex_);
  const uint32_t saved_current_term = current_term();

  DLOG(INFO) << "Replication group: " << group_id_.ToString()
             << " receive HeartbeatRequest from " << remote_id.ToString()
             << ", remote term " << remote_term
             << ", local term is " << saved_current_term;

  // check term
  if (remote_term > saved_current_term) {
    BecomeFollower(remote_id, remote_term);
  } else if (remote_term < saved_current_term) {
    lk.unlock();

    LOG(INFO) << "Replication group: " << group_id_.ToString()
              << " ignore HeartbeatRequest with lower term " << remote_term
              << " from " << remote_id.ToString()
              << ", local term is " << saved_current_term;
    return;
  }
  StepMessageByRole(&lk, request);
}

// REQUIRES: raft_mutex_ should be held.
void ClusterRGNode::HandleHeartbeatRequest(std::unique_lock<std::mutex>* lk,
                                           const InnerMessage::RaftMessage* request) {
  LogOffset leader_committed;
  ParseBinlogOffset(request->commit(), &leader_committed);
  const uint32_t saved_current_term = current_term();
  lk->unlock();

  // NOTE: the committed_offset sent by leader has take the local logs into account.
  pending_task_box_->TryToUpdateCommittedOffset(leader_committed.l_offset);
  PeerID remote_id(request->from().ip(), request->from().port());
  SendHeartbeatResponse(saved_current_term, remote_id);
}

void ClusterRGNode::HandleHeartbeatResponse(const InnerMessage::RaftMessage* response) {
  const auto& remote_term = response->term();
  PeerID remote_id(response->from().ip(), response->from().port());

  if (!started_.load(std::memory_order_acquire)) {
    LOG(INFO) << "Replication group: " << group_id_.ToString()
              << " ignore HeartbeatResponse as we have not been started"
              << ", from " << remote_id.ToString()
              << ", remote term " << remote_term;
    return;
  }

  log_replicator_->HandleHeartbeatResponse(response);
}

void ClusterRGNode::HandleTimeoutNowRequest(const InnerMessage::RaftMessage* request) {
  const auto& remote_term = request->term();
  PeerID remote_id(request->from().ip(), request->from().port());

  if (!started_.load(std::memory_order_acquire)) {
    LOG(INFO) << "Replication group: " << group_id_.ToString()
              << " ignore TimeoutNowRequest as we have not been started"
              << ", from " << remote_id.ToString()
              << ", remote term " << remote_term;
    return;
  }

  std::unique_lock<std::mutex> lk(raft_mutex_);
  const uint32_t saved_current_term = current_term();
  // check term
  if (remote_term > saved_current_term) {
    BecomeFollower(PeerID(), remote_term);
  } else if (remote_term < saved_current_term) {
    lk.unlock();

    LOG(INFO) << "Replication group: " << group_id_.ToString()
              << " ignore TimeoutNowRequest with lower term " << remote_term
              << " from " << remote_id.ToString()
              << ", local term is " << saved_current_term;
    return;
  }
  StepMessageByRole(&lk, request);
}

// REQUIRES: raft_mutex_ should be held.
void ClusterRGNode::HandleTimeoutNowRequest(std::unique_lock<std::mutex>* lk,
                                            const InnerMessage::RaftMessage* request) {
  ElectForSelf(lk, false/*vote*/, true/*ignore lease*/);
}

void ClusterRGNode::HandleSnapshotRequest(const InnerMessage::RaftMessage* request) {
  const auto& remote_term = request->term();
  PeerID remote_id(request->from().ip(), request->from().port());

  if (!started_.load(std::memory_order_acquire)) {
    LOG(INFO) << "Replication group: " << group_id_.ToString()
              << " ignore HandleSnapshotRequest as we have not been started"
              << ", from " << remote_id.ToString()
              << ", remote term " << remote_term;
    return;
  }

  std::unique_lock<std::mutex> lk(raft_mutex_);
  const uint32_t saved_current_term = current_term();
  // check term
  if (remote_term > saved_current_term) {
    BecomeFollower(remote_id, remote_term);
  } else if (remote_term < saved_current_term) {
    lk.unlock();

    LOG(INFO) << "Replication group: " << group_id_.ToString()
              << " ignore SnapshotRequest with lower term " << remote_term
              << " from " << remote_id.ToString()
              << ", local term is " << saved_current_term;
    return;
  }
  StepMessageByRole(&lk, request);
}

// REQUIRES: raft_mutex_ should be held.
void ClusterRGNode::HandleSnapshotRequest(std::unique_lock<std::mutex>* lk,
                                          const InnerMessage::RaftMessage* request) {
  PeerID remote_id(request->from().ip(), request->from().port());
  const LogOffset saved_committed_offset = committed_offset();
  const uint32_t saved_current_term = current_term();

  auto result = snapshot_handler_->Step(request);
  using StepResult = SnapshotHandler::StepResult;
  switch (result) {
    case StepResult::kDuplicatedRequest: {
      lk->unlock();

      LOG(WARNING) << "Replication group: " << group_id_.ToString()
                   << " receive a duplicated request from " << remote_id.ToString()
                   << " at term " << saved_current_term;
      return;
    }
    case StepResult::kMismatchedRequest: {
      lk->unlock();

      LOG(WARNING) << "Replication group: " << group_id_.ToString()
                   << " receive a mismatched request from " << remote_id.ToString()
                   << " at term " << saved_current_term;
      SendAppendEntriesResponse(LogOffset(), saved_committed_offset, remote_id, saved_current_term, true);
      return;
    }
    case StepResult::kBusy: {
      lk->unlock();

      LOG(WARNING) << "Replication group: " << group_id_.ToString()
                   << " receive a snapshot from " << remote_id.ToString()
                   << " at term " << saved_current_term
                   << ", when there is a pending snapshot waitting to be handled"
                   << ", the pending snapshot stamp is " << snapshot_handler_->GetPendingStampString();
      SendAppendEntriesResponse(LogOffset(), saved_committed_offset, remote_id, saved_current_term, true, true);
      return;
    }
    case StepResult::kMetaDataMismatched: {
      lk->unlock();

      LOG(WARNING) << "Replication group: " << group_id_.ToString()
                   << " receive a snapshot from " << remote_id.ToString()
                   << " at term " << saved_current_term
                   << ", the declared meta does not equal to the one in snapshot.";
      SendAppendEntriesResponse(LogOffset(), saved_committed_offset, remote_id, saved_current_term, true);
      return;
    }
    case StepResult::kError: {
      lk->unlock();

      LOG(WARNING) << "Replication group: " << group_id_.ToString()
                   << " receive a snapshot from " << remote_id.ToString()
                   << " at term " << saved_current_term
                   << ", an unexpected error occurred";
      SendAppendEntriesResponse(LogOffset(), saved_committed_offset, remote_id, saved_current_term, true);
      return;
    }
    case StepResult::kOutdatedRequest: {
      lk->unlock();

      LOG(WARNING) << "Replication group: " << group_id_.ToString()
                   << " receive an outdated request from " << remote_id.ToString()
                   << " at term " << saved_current_term;
      SendAppendEntriesResponse(LogOffset(), saved_committed_offset, remote_id, saved_current_term, false);
      return;
    }
    case StepResult::kPrepared: {
      lk->unlock();

      LOG(INFO) << "Replication group: " << group_id_.ToString()
                << " receive an advance request from " << remote_id.ToString()
                << " at term " << saved_current_term;
      SendAppendEntriesResponse(LogOffset(), saved_committed_offset, remote_id, saved_current_term, false);
      return;
    }
    case StepResult::kCompleted: {
      const auto& latest_snapshot_info = snapshot_handler_->latest_snapshot_info();
      // 1. Reset meta information
      ResetPerssistentData(latest_snapshot_info.snapshot_offset, latest_snapshot_info.snapshot_offset,
                           latest_snapshot_info.configuration);
      // 2. Reload configuration
      log_manager_.Reset(latest_snapshot_info.snapshot_offset);
      progress_set_.InitializeWithConfiguration(latest_snapshot_info.configuration, LogOffset(), saved_current_term);
      lk->unlock();

      LOG(INFO) << "Replication group: " << group_id_.ToString()
                << " receive a snapshot from " << remote_id.ToString()
                << " at term " << saved_current_term
                << ", snapshot offset: " << latest_snapshot_info.snapshot_offset.ToString()
                << ", configuration: " << latest_snapshot_info.configuration.ToString();
      SendAppendEntriesResponse(LogOffset(), latest_snapshot_info.snapshot_offset, remote_id, saved_current_term, false);
      return;
    }
  }
}

void ClusterRGNode::SendVoteResponse(const PeerID& remote_id,
                                     const uint32_t term,
                                     const bool pre_vote_request,
                                     const bool granted) {
  InnerMessage::RaftMessage msg;
  msg.set_type(pre_vote_request
               ? InnerMessage::RaftMessageType::kMsgPreVoteResp
               : InnerMessage::RaftMessageType::kMsgVoteResp);
  msg.set_term(current_term());

  msg.set_reject(!granted);
  auto group_id = msg.mutable_group_id();
  group_id->set_table_name(group_id_.TableName());
  group_id->set_partition_id(group_id_.PartitionID());
  auto from = msg.mutable_from();
  from->set_ip(local_id_.Ip());
  from->set_port(local_id_.Port());
  auto to = msg.mutable_to();
  to->set_ip(remote_id.Ip());
  to->set_port(remote_id.Port());
  std::string to_send;
  if (!msg.SerializeToString(&to_send)) {
    LOG(ERROR) << "Replication group: " << group_id_.ToString()
               << " serialize PreVoteResponse failed, to  " << remote_id.ToString();
    return;
  }
  msg_sender_.AppendSimpleTask(remote_id, group_id_, local_id_, 0, MessageType::kResponse, std::move(to_send));
}

// REQUIRES: raft_mutex_ should be held.
void ClusterRGNode::StepCandidate(std::unique_lock<std::mutex>* lk,
                                  const InnerMessage::RaftMessage* in_msg) {
  const auto& remote_term = in_msg->term();
  PeerID remote_id(in_msg->from().ip(), in_msg->from().port());
  switch (in_msg->type()) {
    case InnerMessage::kMsgAppend: {
      BecomeFollower(remote_id, remote_term);
      HandleAppendEntriesRequest(lk, in_msg);
      break;
    }
    case InnerMessage::kMsgHeartbeat: {
      BecomeFollower(remote_id, remote_term);
      HandleHeartbeatRequest(lk, in_msg);
      break;
    }
    case InnerMessage::kMsgSnapshot: {
      BecomeFollower(remote_id, remote_term);
      HandleSnapshotRequest(lk, in_msg);
      break;
    }
    case InnerMessage::kMsgVoteResp: {
      auto result = RecordVote(remote_id, InnerMessage::kMsgVoteResp, !in_msg->reject());
      if (result == VoteBox::VoteResult::kVoteGranted) {
        // We win the Vote
        BecomeLeader();
      } else if (result == VoteBox::VoteResult::kVoteRejected) {
        // We lose the Vote
        BecomeFollower(PeerID(), current_term());
      } else {
        // The vote result is pending.
      }
      break;
    }
    default: {
      break;
    }
  }
  return;
}

// REQUIRES: raft_mutex_ should be held.
void ClusterRGNode::StepPreCandidate(std::unique_lock<std::mutex>* lk,
                                     const InnerMessage::RaftMessage* in_msg) {
  const auto& remote_term = in_msg->term();
  PeerID remote_id(in_msg->from().ip(), in_msg->from().port());
  switch (in_msg->type()) {
    case InnerMessage::kMsgAppend: {
      BecomeFollower(remote_id, remote_term);
      HandleAppendEntriesRequest(lk, in_msg);
      break;
    }
    case InnerMessage::kMsgHeartbeat: {
      BecomeFollower(remote_id, remote_term);
      HandleHeartbeatRequest(lk, in_msg);
      break;
    }
    case InnerMessage::kMsgSnapshot: {
      BecomeFollower(remote_id, remote_term);
      HandleSnapshotRequest(lk, in_msg);
      break;
    }
    case InnerMessage::kMsgPreVoteResp: {
      auto result = RecordVote(remote_id, InnerMessage::kMsgPreVoteResp, !in_msg->reject());
      if (result == VoteBox::VoteResult::kVoteGranted) {
        // We win the PreVote, step to VoteStage
        ElectForSelf(lk, false);
      } else if (result == VoteBox::VoteResult::kVoteRejected) {
        // We lose the PreVote
        BecomeFollower(PeerID(), current_term());
      } else {
        // The vote result is pending.
      }
      break;
    }
    default: {
      break;
    }
  }
  return;
}

// REQUIRES: raft_mutex_ should be held.
void ClusterRGNode::StepFollower(std::unique_lock<std::mutex>* lk,
                                 const InnerMessage::RaftMessage* in_msg) {
  PeerID remote_id(in_msg->from().ip(), in_msg->from().port());
  switch (in_msg->type()) {
    case InnerMessage::kMsgAppend: {
      election_timer_->Restart();
      set_leader_id(remote_id);
      update_lease_ctx(remote_id);
      HandleAppendEntriesRequest(lk, in_msg);
      break;
    }
    case InnerMessage::kMsgHeartbeat: {
      election_timer_->Restart();
      set_leader_id(remote_id);
      update_lease_ctx(remote_id);
      HandleHeartbeatRequest(lk, in_msg);
      break;
    }
    case InnerMessage::kMsgSnapshot: {
      election_timer_->Restart();
      set_leader_id(remote_id);
      update_lease_ctx(remote_id);
      HandleSnapshotRequest(lk, in_msg);
      break;
    }
    case InnerMessage::kMsgTimeoutNow: {
      HandleTimeoutNowRequest(lk, in_msg);
      break;
    }
    default: {
      break;
    }
  }
  return;
}

bool ClusterRGNode::IsPromotable() {
  // When the node is added to a exsiting cluster, it can not
  // elect for itself until it has catched up.
  bool is_voter;
  Status s = progress_set_.IsVoter(local_id_, &is_voter);
  return s.ok() && is_voter && log_manager_.status().ok();
}

void ClusterRGNode::HandleElectionTimedout() {
  if (RoleCode() == RoleStateCode::kStateLeader) {
    DLOG(INFO) << "Replication group: " << group_id_.ToString()
               << " ignore the election timedout, as we are already leader";
    return;
  }
  std::unique_lock<std::mutex> lk(raft_mutex_);
  if (!IsPromotable()) {
    LOG(WARNING) << "Replication group: " << group_id_.ToString()
                 << " ignore the election timedout, as we can not ElectForSelf";
    return;
  }
  if (pending_conf_change_index() < committed_offset().l_offset.index
      && pending_conf_change_index() > applied_offset().l_offset.index) {
    LOG(WARNING) << "Replication group: " << group_id_.ToString()
                 << " ignore the election timedout, as there is pending conf change to apply"
                 << ", pending_conf_change_index " << pending_conf_change_index()
                 << ", committed_offset " << committed_offset().ToString()
                 << ", applied_offset " << applied_offset().ToString();
    return;
  }
  LOG(INFO) << "Replication group: " << group_id_.ToString()
            << " start a new election at term " << current_term();
  ElectForSelf(&lk, options_.pre_vote);
}

class BroadcastVoteRequestClosure : public LastOffsetClosure {
 public:
  BroadcastVoteRequestClosure(const InnerMessage::RaftMessageType& msg_type,
                              const uint32_t msg_term,
                              const uint32_t create_term,
                              const bool ignore_lease,
                              const std::shared_ptr<ClusterRGNode>& node)
    : msg_type_(msg_type), msg_term_(msg_term), create_term_(create_term),
    ignore_lease_(ignore_lease), node_(node) { }

 protected:
  virtual void run() override;
  virtual ~BroadcastVoteRequestClosure() = default;

 private:
  const InnerMessage::RaftMessageType msg_type_;
  const uint32_t msg_term_;
  const uint32_t create_term_;
  const bool ignore_lease_;
  std::shared_ptr<ClusterRGNode> node_;
};

void BroadcastVoteRequestClosure::run() {
  std::unique_lock<std::mutex> lk(node_->raft_mutex_);
  const uint32_t saved_current_term = node_->current_term();
  if (create_term_ != saved_current_term) {
    lk.unlock();

    LOG(WARNING) << "Replication group: " << node_->group_id().ToString()
                 << " term is changed when we prepare (Pre)VoteRequest"
                 << ", current_term " << saved_current_term << ", expected term " << create_term_;
    return;
  }
  InnerMessage::RaftMessage msg;
  msg.set_term(msg_term_);
  msg.set_type(msg_type_);
  auto group_id = msg.mutable_group_id();
  group_id->set_table_name(node_->group_id().TableName());
  group_id->set_partition_id(node_->group_id().PartitionID());
  auto from = msg.mutable_from();
  from->set_ip(node_->local_id().Ip());
  from->set_port(node_->local_id().Port());
  BuildBinlogOffset(last_offset_, msg.mutable_prev_log());
  if (ignore_lease_) {
    msg.set_context(kIgnoreLease);
  }
  auto voters = node_->progress_set_.Voters();
  for (const auto& voter : voters) {
    if (voter == node_->local_id()) {
      continue;
    }
    auto to = msg.mutable_to();
    to->set_ip(voter.Ip());
    to->set_port(voter.Port());

    std::string to_send;
    if (!msg.SerializeToString(&to_send)) {
      lk.unlock();

      LOG(ERROR) << "Replication group: " << node_->group_id().ToString()
                 << " serialize vote msg failed, to  " << voter.ToString();
      return;
    }
    node_->msg_sender_.AppendSimpleTask(voter, node_->group_id(), node_->local_id(), 0,
                                        MessageType::kRequest, std::move(to_send));
    DLOG(INFO) << "Replication group: " << node_->group_id().ToString()
               << ", send vote message to " << voter.ToString()
               << ", local last_offset: " << last_offset_.ToString();
  }
}

// REQUIRES: raft_mutex_ should be held.
void ClusterRGNode::ElectForSelf(std::unique_lock<std::mutex>* lk,
                                 const bool pre_vote, const bool ignore_lease) {
  uint32_t msg_term = 0;
  InnerMessage::RaftMessageType msg_type;
  if (pre_vote) {
    // Send the msg in a future term
    BecomePreCandidate();
    msg_term = current_term() + 1;
    msg_type = InnerMessage::RaftMessageType::kMsgPreVote;
  } else {
    BecomeCandidate();
    msg_term = current_term();
    msg_type = InnerMessage::RaftMessageType::kMsgVote;
  }
  // Granted self
  if (RecordVote(local_id_, msg_type, true) == VoteBox::VoteResult::kVoteGranted) {
    if (pre_vote) {
      // PreVoteStage is granted, step to VoteStage.
      ElectForSelf(lk, false);
    } else {
      // VoteStage is granted, win the election.
      BecomeLeader();
    }
    return;
  }
  BroadcastVoteRequestClosure* closure = new BroadcastVoteRequestClosure(
      msg_type, msg_term, current_term(), ignore_lease,
      std::dynamic_pointer_cast<ClusterRGNode>(shared_from_this()));

  log_manager_.GetLastStableOffset(false/*in disk thread*/, closure);
}

// REQUIRES: raft_mutex_ must be held
VoteBox::VoteResult ClusterRGNode::RecordVote(const PeerID& peer_id,
                                              const InnerMessage::RaftMessageType& msg_type,
                                              const bool granted) {
  LOG(INFO) << "Replication group: " << group_id_.ToString()
            << " record vote response from " << peer_id.ToString()
            << " for " << (msg_type == InnerMessage::RaftMessageType::kMsgPreVote ? "PreVoteStage" : "VoteStage")
            << " at term " << current_term()
            << ", granted: " << (granted ? "true" : "false");
  vote_box_.RecordVote(peer_id, granted);
  return vote_box_.Result();
}

void ClusterRGNode::HandleTransferTimedout() {
  std::unique_lock<std::mutex> lk(raft_mutex_);
  LOG(WARNING) << "Replication group: " << group_id_.ToString()
               << " transfer leadership timedout at term " << current_term();
  CancelLeaderTransfer(LeaderTransferContext::CancelReason::Code::kTransferTimedout);
}

void ClusterRGNode::SendAppendEntriesResponse(const LogOffset& first_offset,
                                              const LogOffset& last_offset,
                                              const PeerID& peer_id,
                                              const uint32_t current_term,
                                              const bool reject,
                                              const bool temporarily_unavaliable) {
  InnerMessage::RaftMessage msg;
  msg.set_term(current_term);

  msg.set_reject(reject);
  if (temporarily_unavaliable) {
    msg.set_context(kTemporarilyUnavailable);
  }
  msg.set_type(InnerMessage::RaftMessageType::kMsgAppendResp);
  // Entries in range [first_offset, last_offset] will be acked
  // if reject is false.
  BuildBinlogOffset(first_offset, msg.mutable_first_log_offset());
  BuildBinlogOffset(last_offset, msg.mutable_last_log_offset());
  auto from = msg.mutable_from();
  from->set_ip(local_id_.Ip());
  from->set_port(local_id_.Port());
  auto to = msg.mutable_to();
  to->set_ip(peer_id.Ip());
  to->set_port(peer_id.Port());
  auto group_id = msg.mutable_group_id();
  group_id->set_table_name(group_id_.TableName());
  group_id->set_partition_id(group_id_.PartitionID());

  std::string to_send;
  if (!msg.SerializeToString(&to_send)) {
    LOG(ERROR) << "Replication group: " << group_id_.ToString()
               << " serialize kMsgAppendResp failed, to  " << peer_id.ToString();
    return;
  }
  msg_sender_.AppendSimpleTask(peer_id, group_id_, local_id_, 0, MessageType::kResponse, std::move(to_send));
}

void ClusterRGNode::HandleHeartbeatTimedout() {
  DLOG(INFO) << "Replication group: " << group_id_.ToString()
             << " heartbeat timedout ";
  log_replicator_->BroadcastHeartbeat();
}

void ClusterRGNode::HandleCheckQuorumTimedout() {
  DLOG(INFO) << "Replication group: " << group_id_.ToString()
             << " check quorum timedout ";
  std::unique_lock<std::mutex> lk(raft_mutex_);
  auto s = progress_set_.IsReady();
  if (!s.ok()) {
    StepDown(PeerID(), current_term(), Status::Corruption("Quorum inactive"));
  } else {
    // we can still communicate with quorum, update lease
    update_lease_ctx(leader_id());
  }
}

void ClusterRGNode::SendHeartbeatResponse(const uint32_t current_term,
                                          const PeerID& peer_id) {
  InnerMessage::RaftMessage msg;
  msg.set_term(current_term);

  msg.set_type(InnerMessage::RaftMessageType::kMsgHeartbeatResp);
  auto from = msg.mutable_from();
  from->set_ip(local_id_.Ip());
  from->set_port(local_id_.Port());
  auto to = msg.mutable_to();
  to->set_ip(peer_id.Ip());
  to->set_port(peer_id.Port());
  auto group_id = msg.mutable_group_id();
  group_id->set_table_name(group_id_.TableName());
  group_id->set_partition_id(group_id_.PartitionID());

  std::string to_send;
  if (!msg.SerializeToString(&to_send)) {
    LOG(ERROR) << "Replication group: " << group_id_.ToString()
               << " serialize kMsgHeartbeatResp failed, to  " << peer_id.ToString();
    return;
  }
  msg_sender_.AppendSimpleTask(peer_id, group_id_, local_id_, 0, MessageType::kResponse, std::move(to_send));
}

// REQUIRES: raft_mutex_ should be held.
void ClusterRGNode::MarkLeaderTransfer(const PeerID& target_leader,
                                       Closure* done_closure) {
  transfer_ctx_.MarkLeaderTransfer(target_leader, done_closure);
  log_replicator_->StartLeaderTransfer(target_leader, log_manager_.GetLastPendingOffset().l_offset.index);
}

// REQUIRES: raft_mutex_ should be held.
void ClusterRGNode::CancelLeaderTransfer(const LeaderTransferContext::CancelReason::Code& reason_code) {
  log_replicator_->StopLeaderTransfer(transfer_ctx_.LeaderTransferee());
  transfer_ctx_.CancelLeaderTransfer(reason_code);
}

void ClusterRGNode::ProposeLeaderTransfer(const PeerID& target_leader,
                                          Closure* done_closure) {
  ClosureGuard guard(done_closure);
  if (!started_.load(std::memory_order_acquire)) {
    if (done_closure) {
      done_closure->set_status(Status::NotFound("node has not been started"));
    }
    return;
  }
  std::unique_lock<std::mutex> lk(raft_mutex_);
  if (RoleCode() != RoleStateCode::kStateLeader) {
    if (done_closure) {
      done_closure->set_status(Status::NotFound("node is not leader"));
    }
    return;
  }
  bool is_voter = false;
  Status s = progress_set_.IsVoter(target_leader, &is_voter);
  if (!s.ok()) {
    if (done_closure) {
      done_closure->set_status(Status::NotFound("target leader is not found"));
    }
    return;
  }
  if (!is_voter) {
    if (done_closure) {
      done_closure->set_status(Status::NotSupported("target leader is a learner right now"));
    }
    return;
  }
  if (RoleCode() != RoleStateCode::kStateLeader) {
    if (done_closure) {
      done_closure->set_status(Status::NotSupported(local_id_.ToString() + " is not leader"));
    }
    return;
  }
  if (!LeaderTransferee().Empty()) {
    if (LeaderTransferee() == target_leader) {
      LOG(INFO) << "Replication group: " << group_id_.ToString()
                << " receive a leader-transfer request at term " << current_term()
                << ", ignore the request as the transferring to " << target_leader.ToString()
                << " is in progress";
      if (done_closure) {
        done_closure->set_status(Status::Busy("transferring is in progress"));
      }
      return;
    }
    CancelLeaderTransfer(LeaderTransferContext::CancelReason::Code::kNewProposalIsSubmitted);
    LOG(INFO) << "Replication group: " << group_id_.ToString()
              << " receive a leader-transfer request at term " << current_term()
              << ", cancel the previous transferring to " << LeaderTransferee().ToString();
  }
  if (target_leader == local_id_) {
    LOG(INFO) << "Replication group: " << group_id_.ToString()
              << " receive a leader-transfer request at term " << current_term()
              << ", ignore the transferring to " << target_leader.ToString()
              << " as it's already leader.";
    if (done_closure) {
      done_closure->set_status(Status::NotSupported("It's already leader"));
    }
    return;
  }
  transfer_timer_->Restart();
  MarkLeaderTransfer(target_leader, guard.Release());

  LOG(INFO) << "Replication group: " << group_id_.ToString()
            << " receive a leader transfer request at term " << current_term()
            << ", start the transferring to " << target_leader.ToString();
}

Status ClusterRGNode::Propose(const std::shared_ptr<ReplTask>& task) {
  Status s;
  auto closure = task->closure();
  ClosureGuard user_closure_guard(closure);
  {
    std::unique_lock<std::mutex> lk(raft_mutex_);
    if (RoleCode() != RoleStateCode::kStateLeader) {
      lk.unlock();

      s = Status::Corruption("is not a leader");
      if (closure) {
        closure->set_status(s);
      }
      LOG(WARNING) << "Replication group: " << group_id_.ToString()
                   << " is not a leader.";
      return s;
    }
    if (!LeaderTransferee().Empty()) {
      lk.unlock();

      s = Status::Corruption("proposal dropped when transfer leadership");
      if (closure) {
        closure->set_status(s);
      }
      LOG(ERROR) << "Replication group: " << group_id_.ToString()
                 << ", drop the proposal when transfer leadership.";
      return s;
    }
    auto progress = progress_set_.GetProgress(local_id_);
    if (progress == nullptr) {
      lk.unlock();

      s = Status::Corruption("proposal dropped when self not in progress");
      if (closure) {
        closure->set_status(s);
      }
      LOG(ERROR) << "Replication group: " << group_id_.ToString()
                 << ", self has been removed from progress"
                 << ", drop the proposal.";
      return s;
    }
    LeaderDiskClosure* disk_closure = new LeaderDiskClosure(LogOffset(), current_term(),
        std::dynamic_pointer_cast<ClusterRGNode>(shared_from_this()));
    s = log_manager_.AppendLog(task, disk_closure);
    if (!s.ok()) {
      lk.unlock();

      disk_closure->Run();
      delete disk_closure;

      if (closure) {
        closure->set_status(s);
      }
      return s;
    }
    // release the closure as they will be invoked in future
    user_closure_guard.Release();
  }
  log_replicator_->BroadcastAppendEntries();
  rm_->SignalAuxiliary();
  return s;
}

Status ClusterRGNode::Advance(const Ready& ready) {
  return pending_task_box_->Advance(ready);
}

void ClusterRGNode::ProposeConfChange(const std::shared_ptr<ReplTask>& task) {
  auto closure = task->closure();
  ClosureGuard guard(closure);
  {
    std::unique_lock<std::mutex> lk(raft_mutex_);
    if (applied_offset().l_offset.index < pending_conf_change_index()) {
      LOG(WARNING) << "Replication group: " << group_id_.ToString()
                   << " receive a ConfChangeEntry in term " << current_term()
                   << ", reject it as there is a pending ConfChangeEntry in "
                   << pending_conf_change_index()
                   << ", current applied index: " << applied_offset().ToString();
      if (closure) {
        closure->set_status(Status::Busy("There is a pending ConfChangeEntry."));
        return;
      }
    }
    // release the closure as it will be invoked in future.
    guard.Release();
  }
  Propose(task);
}

void ClusterRGNode::ReportUnreachable(const PeerID& peer_id) {
  std::unique_lock<std::mutex> lk(raft_mutex_);
  member_ctx_.ChangeMemberSurvivorship(peer_id, false);
  const auto& role_code = RoleCode();
  if (role_code == RoleStateCode::kStateFollower) {
    // The leader is gone
    if (leader_id() == peer_id) {
      LOG(WARNING) << "Replication group: " << group_id_.ToString()
                   << " , leader "<< peer_id.ToString() << " is unreachable.";
    }
  } else if (role_code == RoleStateCode::kStateLeader) {
    // The follower is gone
    auto progress = progress_set_.GetProgress(peer_id);
    if (progress == nullptr) {
      return;
    }
    progress->BecomeNotSync();
    LOG(WARNING) << "Replication group: " << group_id_.ToString()
                 << ", follower "<< peer_id.ToString() << " is unreachable"
                 << ", mark the state to kNotSync";
  } else {
    LOG(WARNING) << "Replication group: " << group_id_.ToString()
                 << " , peer "<< peer_id.ToString() << " is unreachable.";
  }
}

void ClusterRGNode::Leave(std::string& log_remove) {
  std::unique_lock<std::mutex> lk(raft_mutex_);
  UnsafeStop();
  log_remove = log_manager_.Leave();
}

Status ClusterRGNode::IsReady() {
  // Engouh follower
  return progress_set_.IsReady();
}

void ClusterRGNode::Messages(PeerMessageListMap& msgs) {
  msg_sender_.Messages(msgs);
}

Status ClusterRGNode::GetSyncInfo(const PeerID& peer_id,
                                  std::stringstream& stream) {
  //if (peer_id.Empty()) {
  //  // binlog offset section
  //  uint32_t filenum = 0;
  //  uint64_t offset = 0;
  //  log_manager_.GetProducerStatus(&filenum, &offset);
  //  stream << group_id_.ToString() << " binlog_offset="
  //         << filenum << " " << offset;

  //  // safety purge section
  //  progress_set_.GetSafetyPurgeBinlog(stream);

  //  const auto role_code = RoleCode();
  //  if (role_code == RoleStateCode::kStateLeader) {
  //    stream << "  Role: leader" << "\r\n";
  //    progress_set_.GetInfo(local_id_, stream);
  //  } else if (role_code == RoleStateCode::kStateFollower) {
  //    stream << "  Role: follower\r\n";
  //    stream << "  : " << leader_id().ToString() << "\r\n";
  //  }
  //  return Status::OK();
  //} else {
  //  progress_set_.GetInfo(peer_id, stream);
  //}
  return Status::OK();
}

VolatileContext::State ClusterRGNode::NodeVolatileState() {
  if (!started_.load(std::memory_order_acquire)) {
    return VolatileContext::State();
  }
  return volatile_ctx_.DumpState();
}

PersistentContext::State ClusterRGNode::NodePersistentState() {
  if (!started_.load(std::memory_order_acquire)) {
    return PersistentContext::State();
  }
  return pending_task_box_->persistent_state();
}

MemberContext::MemberStatesMap ClusterRGNode::NodeMemberStates() {
  if (!started_.load(std::memory_order_acquire)) {
    return MemberContext::MemberStatesMap{};
  }
  return member_ctx_.member_states();
}

bool ClusterRGNode::IsReadonly() {
  // TODO: for a follower, redeliver the requests to leader
  return RoleCode() != RoleStateCode::kStateLeader;
}

Status ClusterRGNode::IsSafeToBeRemoved() {
  // TODO: check the conditions more precise
  return Status::OK();
}

void ClusterRGNode::PurgeStableLogs(int expire_logs_nums, int expire_logs_days,
                                    uint32_t to, bool manual,
                                    PurgeLogsClosure* done) {
  auto node = shared_from_this();
  auto filter = [node] (const uint32_t index) -> bool {
    return !node->IsSafeToBePurged(index);
  };
  log_manager_.PurgeStableLogs(expire_logs_nums, expire_logs_days, to, manual, filter, done);
}

bool ClusterRGNode::IsSafeToBePurged(uint32_t filenum) {
  // We can noly purge logs that have been applied.
  if (applied_offset().b_offset.filenum < filenum) {
    return false;
  }
  // TODO: judge the peer progress
  return true;
}

Status ClusterRGNode::RecoverPerssistentData() {
  // 1. Get cluster configuration from meta_storage
  Status s = meta_storage_->configuration(&conf_ctx_.conf);
  if (!s.ok()) {
    LOG(ERROR) << "Replication group: " << group_id_.ToString()
               << " recover configuration from meta storage failed: " << s.ToString();
    return s;
  }
  // 2. Record the indexes (commit, apply and snapshot) into meta storage
  // NOTE: this path exists for backward compatibility.
  s = meta_storage_->ResetOffset(snapshot_offset(), applied_offset());
  if (!s.ok()) {
    LOG(ERROR) << "Replication group: " << group_id_.ToString()
               << " reset indexes into meta storage failed"
               << ", snapshot_offset: " << snapshot_offset().ToString()
               << ", applied_offset: " << applied_offset().ToString()
               << ", error info: " << s.ToString();
  }
  return s;
}

void ClusterRGNode::ResetPerssistentData(const LogOffset& snapshot_offset,
                                         const LogOffset& applied_offset,
                                         const Configuration& conf_state) {
  // 1. Update the indexes in task box
  Status s = pending_task_box_->ResetSnapshotAndApplied(snapshot_offset, applied_offset);
  if (!s.ok()) {
    LOG(ERROR) << "Replication group: " << group_id_.ToString()
               << " reset indexes in task box failed at term " << current_term()
               << ", snapshot_offset: " << snapshot_offset.ToString()
               << ", applied_offset: " << applied_offset.ToString()
               << ", error info: " << s.ToString();
    SetRoleCode(RoleStateCode::kStateError);
    return;
  }
  // 2. Update the indexes in meta storage
  s = meta_storage_->ResetOffsetAndConfiguration(snapshot_offset, applied_offset, conf_state);
  if (!s.ok()) {
    LOG(ERROR) << "Replication group: " << group_id_.ToString()
               << " reset indexes in meta storage failed at term " << current_term()
               << ", snapshot_offset: " << snapshot_offset.ToString()
               << ", applied_offset: " << applied_offset.ToString()
               << ", conf_state: " << conf_state.ToString()
               << ", error info: " << s.ToString();
    SetRoleCode(RoleStateCode::kStateError);
  }
}

void ClusterRGNode::set_configuration(const Configuration& configuration) {
  Status s = meta_storage_->set_configuration(configuration);
  if (!s.ok()) {
    LOG(ERROR) << "Replication group: " << group_id_.ToString()
               << " set configuration failed at term " << current_term()
               << ", conf_state: " << configuration.ToString()
               << ", error info: " << s.ToString();
    SetRoleCode(RoleStateCode::kStateError);
    return;
  }
  conf_ctx_.conf = configuration;
}

void ClusterRGNode::set_term(uint32_t term) {
  Status s = meta_storage_->set_term(term);
  if (!s.ok()) {
    LOG(ERROR) << "Replication group: " << group_id_.ToString()
               << " persistent term " << term
               << " to storage failed: " << s.ToString();
    SetRoleCode(RoleStateCode::kStateError);
  }
}

void ClusterRGNode::set_voted_for(const PeerID& peer_id) {
  // persistent to the meta storage
  Status s = meta_storage_->set_voted_for(peer_id);
  if (!s.ok()) {
    LOG(ERROR) << "Replication group: " << group_id_.ToString()
               << " persistent VotedFor " << peer_id.ToString()
               << " to storage failed: " << s.ToString();
    SetRoleCode(RoleStateCode::kStateError);
  }
}

void ClusterRGNode::set_term_and_voted_for(uint32_t term, const PeerID& peer_id) {
  // persistent to the meta storage
  Status s = meta_storage_->SetTermAndVotedFor(peer_id, term);
  if (!s.ok()) {
    LOG(ERROR) << "Replication group: " << group_id_.ToString()
               << " persistent VotedFor " << peer_id.ToString()
               << " and term " << term
               << " to storage failed: " << s.ToString();
    SetRoleCode(RoleStateCode::kStateError);
  }
}

uint32_t ClusterRGNode::current_term() {
  uint32_t term;
  Status s = meta_storage_->term(&term);
  if (!s.ok()) {
    LOG(ERROR) << "Replication group: " << group_id_.ToString()
               << " get term from meta storage failed: " << s.ToString();
    SetRoleCode(RoleStateCode::kStateError);
  }
  return term;
}

PeerID ClusterRGNode::voted_for() {
  PeerID voted_for;
  Status s = meta_storage_->voted_for(&voted_for);
  if (!s.ok()) {
    LOG(ERROR) << "Replication group: " << group_id_.ToString()
               << " get voted_for from meta storage failed: " << s.ToString();
    SetRoleCode(RoleStateCode::kStateError);
  }
  return voted_for;
}

LogOffset ClusterRGNode::committed_offset() { return pending_task_box_->committed_offset(); }
LogOffset ClusterRGNode::applied_offset() { return pending_task_box_->applied_offset(); }
LogOffset ClusterRGNode::snapshot_offset() { return pending_task_box_->snapshot_offset(); }

}  // namespace replication
