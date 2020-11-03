// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef REPLICAITON_PIKA_CLUSTER_RG_NODE_H_
#define REPLICATION_PIKA_CLUSTER_RG_NODE_H_

#include <glog/logging.h>
#include <sstream>
#include <string>
#include <memory>
#include <vector>
#include <set>
#include <atomic>
#include <mutex>
#include <unordered_map>

#include "slash/include/slash_mutex.h"
#include "slash/include/slash_status.h"

#include "proto/pika_inner_message.pb.h"
#include "include/util/callbacks.h"
#include "include/pika_define.h"
#include "include/replication/pika_log_manager.h"
#include "include/replication/pika_memory_log.h"
#include "include/replication/pika_repl_rg_node.h"
#include "include/replication/pika_repl_manager.h"
#include "include/replication/cluster/pika_cluster_log_manager.h"
#include "include/replication/cluster/pika_cluster_progress.h"
#include "include/replication/cluster/pika_cluster_raft_timer.h"
#include "include/replication/cluster/pika_cluster_message_executor.h"

namespace replication {

using slash::Status;
using util::Closure;

class ReplicationManager;

class LeaderTransferContext {
 public:
  class CancelReason {
   public:
    enum class Code : uint8_t {
      kRoleChanged             = 1,
      kTransferTimedout        = 2,
      kTargetLeaderIsRemoved   = 3,
      kNewProposalIsSubmitted  = 4,
    };
    std::string ToString();
    static std::string ToString(const Code& code);
    Code code() const { return code_; }
    void set_code(const Code& code) { code_ = code; }

   private:
    Code code_;
  };
  explicit LeaderTransferContext(ReplicationManager* rm)
    : rm_(rm), leader_transferee_(), done_closure_(nullptr) { }
  ~LeaderTransferContext();

  PeerID LeaderTransferee() const { return leader_transferee_; }
  void MarkLeaderTransfer(const PeerID& target_leader,
                          Closure* done_closure);
  void CancelLeaderTransfer(const CancelReason::Code& reason_code);

 private:
  void ScheduleTheClosure(Closure* closure);
  static void RunTheClosure(void* arg);
  // The context for execute the done_closure
  // once the transferring is completed or canceled.
  // Run the closure in another context to avoid blocking.
  ReplicationManager* rm_;
  PeerID leader_transferee_;
  Closure* done_closure_;
};

// LeaseContext used to mark the lease of the current leadership.
// The following event will be ignored when the lease is not expired:
// 1) vote itself.
// 2) recieve Vote or PreVote messages from others.
class LeaseContext {
 public:
  explicit LeaseContext(uint64_t lease_timeout_ms)
    : lease_timeout_ms_(lease_timeout_ms),
    leader_id_(),
    last_active_timestamp_(0) { }

  void Update(const PeerID& leader_id) {
    leader_id_ = leader_id;
    last_active_timestamp_ = slash::NowMicros();
  }
  bool IsExpired() const {
    return (slash::NowMicros() - last_active_timestamp_) >= lease_timeout_ms_;
  }
  void set_lease_timeout_ms(uint64_t timeout_ms) {
    lease_timeout_ms_ = timeout_ms;
  }
  const PeerID& leader_id() const {
    return leader_id_;
  }

 private:
  uint64_t lease_timeout_ms_;
  PeerID leader_id_;
  uint64_t last_active_timestamp_;
};

// VoteBox used to record the vote results.
class VoteBox {
 public:
  enum VoteResult {
    // There are nodes have not responsed to us.
    kVotePending  = 1,
    // The Granted votes have reached quorum.
    kVoteGranted  = 2,
    // The Reject votes have reached quorum.
    kVoteRejected = 3,
  };

  void RecordVote(const PeerID& peer_id, bool granted);
  VoteResult Result() const;
  void ResetVotes(const std::vector<PeerID>& voters);
 private:
  std::vector<PeerID> voters_;
  // key for voter, value for the vote.
  std::unordered_map<PeerID, bool, hash_peer_id> votes_;
};

// ClusterRGNodeMetaStorage used to persistent the RaftMeta
// 1) current term
// 2) voted for
// 3) snapshot_offset
// 4) applied_offset
// 5) replication Configuration
class ClusterMetaStorage : public ReplicationGroupNodeMetaStorage {
 public:
  ClusterMetaStorage() = default;
  virtual ~ClusterMetaStorage() = default;
  virtual Status SetTermAndVotedFor(const PeerID& voted_for,
                                    uint32_t term) = 0;
  virtual Status GetTermAndVotedFor(uint32_t* term,
                                    PeerID* voted_for) = 0;
  virtual Status set_term(uint32_t term) = 0;
  virtual Status term(uint32_t* term) = 0;
  virtual Status set_voted_for(const PeerID& voted_for) = 0;
  virtual Status voted_for(PeerID* peer_id) = 0;
};

struct ClusterRGNodeOptions : public ReplicationGroupNodeOptions {
  constexpr uint64_t static kDefaultElectionTimeoutMS = 10000;
  constexpr uint64_t static kDefaultHeartbeatTimeoutMS = 1000;
  // Init configuration
  Configuration conf;
  // If the current replication group permanently loses quorum
  // (more than (N-1)/2 voters have lost), we can not reach consensus
  // anymore, we must reset the replication group.
  bool force_start_as_alone;
  // If check_quorum is set to true, leader will step down
  // once it cannot recieve engouh responses (lose quorum) from followers
  // until election timedout.
  bool check_quorum;
  // If pre_vote is set to true, a PreVote stage will be perfomed when
  // a candidate intends to elect for itself.
  bool pre_vote;
  // If a follower does not receive any messages from the leader
  // of the current term until timedout, it will become candidate
  // and start the election. So the ElectionTimer must larger than
  // HeartbeatTimer. For better failover, the ElectionTimer = 10 *
  // HeartbeatTimer.
  //
  // In order to reduce the possibility of conflicts, the timeout is
  // usually shifted by a random amount from the specified value.
  // In practice, the value is usually taken in the following range:
  // [election_timeout_ms, 2 * election_timeout_ms - 1]
  uint64_t election_timeout_ms;
  // The leader will send the heartbeat messages to maintain its
  // lease if there are no more content to synchronize until timedout.
  uint64_t heartbeat_timeout_ms;
  util::thread::ThreadPool* timer_thread;
  util::TaskExecutor* executor;
  ClusterLogManagerOptions log_options;
  ClusterRGNodeOptions()
    : conf(),
    check_quorum(false),
    pre_vote(false),
    election_timeout_ms(5000 /*10s*/),
    heartbeat_timeout_ms(500 /*1s*/),
    timer_thread(nullptr),
    executor(nullptr),
    log_options() {
  }

  ClusterRGNodeOptions(const Configuration& _conf,
                       bool _check_quorum,
                       bool _pre_vote,
                       uint64_t _election_timeout_ms,
                       uint64_t _heartbeat_timeout_ms,
                       util::thread::ThreadPool* _timer_thread,
                       util::TaskExecutor* _executor,
                       const ClusterLogManagerOptions& _log_options)
    : conf(_conf),
    check_quorum(_check_quorum),
    pre_vote(_pre_vote),
    election_timeout_ms(_election_timeout_ms),
    heartbeat_timeout_ms(_heartbeat_timeout_ms),
    timer_thread(_timer_thread),
    executor(_executor),
    log_options(_log_options) {
  }
};

struct ConfigurationCtx {
  // use pending_conf_change_index_ to identify if there is a
  // ConfChangeEntry need to be committed, do not allow multiple
  // configuration changes at the same time to prevent brain splits.
  // TODO(LIBA-S): support consensus-joint
  uint64_t pending_conf_change_index;
  // the intialize configuration
  Configuration conf;

  explicit ConfigurationCtx(const Configuration& _conf)
    : pending_conf_change_index(0),
    conf(_conf) { }
};

const std::string kIgnoreLease            = "IL";
const std::string kTemporarilyUnavailable = "TU";
const std::string kAdvanceSnapshot        = "AS";

class ClusterRGNode : public ReplicationGroupNode {
  friend class LeaderDiskClosure;
  friend class FollowerDiskClosure;
  friend class GroupResetCompletedClosure;
  friend class SnapshotSendCompletedClosure;
  friend class BroadcastVoteRequestClosure;
  friend class VoteRequestClosure;
  friend class LogReplicator;
  friend class PendingTaskBox;
 public:
  explicit ClusterRGNode(const ClusterRGNodeOptions& options);
  ~ClusterRGNode();

  virtual Status Start() override;
  virtual Status Stop() override;
  virtual void Leave(std::string& log_remove) override;
  virtual bool IsReadonly() override;
  virtual void PurgeStableLogs(int expire_logs_nums, int expire_logs_days,
                               uint32_t to, bool manual, PurgeLogsClosure* done) override;
  virtual bool IsSafeToBePurged(uint32_t filenum) override;
  virtual Status IsReady() override;
  virtual Status IsSafeToBeRemoved() override;
  virtual void ReportUnreachable(const PeerID& peer_id) override;
  virtual Status GetSyncInfo(const PeerID& peer_id,
                             std::stringstream& stream) override;
  virtual VolatileContext::State NodeVolatileState() override;
  virtual PersistentContext::State NodePersistentState() override;
  virtual MemberContext::MemberStatesMap NodeMemberStates() override;
  virtual Status ApplyConfChange(const InnerMessage::ConfChange& conf_change) override;
  virtual void Messages(PeerMessageListMap& msgs) override;
  virtual Status Propose(const std::shared_ptr<ReplTask>& task) override;
  virtual Status Advance(const Ready& ready) override;

 public:
  void StepMessages(InnerMessage::RaftMessage* in_msg);
  void ProposeConfChange(const std::shared_ptr<ReplTask>& task);
  void ProposeLeaderTransfer(const PeerID& peer_id, Closure* closure);
  void StartWithConfiguration(const Configuration& configuration);
  void ForceStartAsAlone();

  void HandleElectionTimedout();
  void HandleHeartbeatTimedout();
  void HandleTransferTimedout();
  void HandleCheckQuorumTimedout();

  class PendingTaskBox;
  class LogReplicator;

 private:
  virtual Status Initialize() override;

  Status Recover();
  Status StartAsNew();
  Status Restart();
  std::vector<InnerMessage::ConfChange*> CreateConfChanges();
  void PersistTheInitConfiguration(const Configuration& conf);
  Status UnsafeApplyConfChange(const InnerMessage::ConfChange& conf_change);

  void TryUpdateTerm(const uint32_t remote_term, const PeerID& remote_id);
  void StepDown(const PeerID& leader_id, uint32_t term, const Status& status);
  void BecomeUninitialized();
  void BecomePreCandidate();
  void BecomeCandidate();
  void BecomeFollower(const PeerID& leader_id, uint32_t term);
  Status BecomeLeader();
  void BecomeStop();
  void ResetState(uint32_t term);
  void StepCandidate(std::unique_lock<std::mutex>* lk,
                     const InnerMessage::RaftMessage* in_msg);
  void StepPreCandidate(std::unique_lock<std::mutex>* lk,
                        const InnerMessage::RaftMessage* in_msg);
  void StepFollower(std::unique_lock<std::mutex>* lk,
                    const InnerMessage::RaftMessage* in_msg);
  void StepMessageByRole(std::unique_lock<std::mutex>* lk,
                         const InnerMessage::RaftMessage* msg);
  void HandlePreVoteRequest(const InnerMessage::RaftMessage* msg);
  void HandlePreVoteRequest(std::unique_lock<std::mutex>* lk,
                            const InnerMessage::RaftMessage* msg);
  void HandlePreVoteResponse(const InnerMessage::RaftMessage* response);
  void HandleVoteRequest(const InnerMessage::RaftMessage* msg);
  void HandleVoteRequest(std::unique_lock<std::mutex>* lk,
                         const InnerMessage::RaftMessage* msg);
  void HandleVoteResponse(const InnerMessage::RaftMessage* response);
  void HandleAppendEntriesRequest(const InnerMessage::RaftMessage* request);
  void HandleAppendEntriesRequest(std::unique_lock<std::mutex>* lk,
                                  const InnerMessage::RaftMessage* request);
  void SendAppendEntriesResponse(const LogOffset& first_offset,
                                 const LogOffset& last_offset,
                                 const PeerID& peer_id,
                                 const uint32_t current_term,
                                 const bool reject,
                                 const bool temporarily_unavaliable = false);
  void HandleAppendEntriesResponse(const InnerMessage::RaftMessage* response);
  void HandleHeartbeatRequest(const InnerMessage::RaftMessage* request);
  void HandleHeartbeatRequest(std::unique_lock<std::mutex>* lk,
                              const InnerMessage::RaftMessage* request);
  void SendHeartbeatResponse(const uint32_t current_term,
                             const PeerID& peer_id);
  void HandleHeartbeatResponse(const InnerMessage::RaftMessage* response);
  void HandleSnapshotRequest(const InnerMessage::RaftMessage* request);
  void HandleSnapshotRequest(std::unique_lock<std::mutex>* lk,
                             const InnerMessage::RaftMessage* request);
  void HandleTimeoutNowRequest(const InnerMessage::RaftMessage* request);
  void HandleTimeoutNowRequest(std::unique_lock<std::mutex>* lk,
                               const InnerMessage::RaftMessage* request);
  // Send Vote or PreVote(if pre_vote == true) requests to others
  // to compete a new leadership. The following conditions may trigger this behavior:
  // 1) the election_timer_ is timedout.
  // 2) recieved a TimeoutNow request from the current leader.
  //
  // @param pre_vote: if enabled, we will first step a PreVote stage.
  // @param ignore_lease: if enabled, we will force the receiver ignore
  //                      its follower_lease_.
  void ElectForSelf(std::unique_lock<std::mutex>* lk,
                    const bool pre_vote, const bool ignore_lease = false);
  void BroadcastVoteRequest(const InnerMessage::RaftMessageType& msg_type,
                            const uint32_t msg_term,
                            const bool ignore_lease);
  void SendVoteResponse(const PeerID& remote_id,
                        const uint32_t term,
                        const bool pre_vote_request,
                        const bool granted);
  VoteBox::VoteResult RecordVote(const PeerID& peer_id,
                                 const InnerMessage::RaftMessageType& msg_type,
                                 const bool granted);
  bool IsPromotable();

 private:
  Status UnsafeStop();
  Status RecoverPerssistentData();
  void ResetPerssistentData(const LogOffset& snapshot_offset,
                            const LogOffset& applied_offset,
                            const Configuration& conf_state);
  PeerID leader_id() const { return volatile_ctx_.leader_id(); }
  void set_leader_id(const PeerID& leader_id) { volatile_ctx_.set_leader_id(leader_id); }
  void MarkLeaderTransfer(const PeerID& target_leader, Closure* done_closure);
  void CancelLeaderTransfer(const LeaderTransferContext::CancelReason::Code& reason_code);
  PeerID LeaderTransferee() const { return transfer_ctx_.LeaderTransferee(); }
  void SetRoleCode(const RoleState::Code& code) { volatile_ctx_.SetRoleCode(code); }
  RoleState::Code RoleCode() const { return volatile_ctx_.RoleCode(); }
  uint32_t current_term();
  void set_term(uint32_t term);
  PeerID voted_for();
  void set_voted_for(const PeerID& peer_id);
  void set_term_and_voted_for(uint32_t term, const PeerID& peer_id);
  void set_configuration(const Configuration& configuration);
  Configuration configuration() { return conf_ctx_.conf; }
  uint64_t pending_conf_change_index() const { return conf_ctx_.pending_conf_change_index; }
  void set_pending_conf_change_index(uint64_t index) { conf_ctx_.pending_conf_change_index = index; }
  void update_lease_ctx(const PeerID& leader_id) { lease_ctx_.Update(leader_id); }
  bool lease_is_expired() const { return lease_ctx_.leader_id().Empty() || lease_ctx_.IsExpired(); }
  LogOffset committed_offset();
  LogOffset applied_offset();
  LogOffset snapshot_offset();

 private:
  class SnapshotHandler;
  const ClusterRGNodeOptions options_;

  ClusterMetaStorage* meta_storage_;
  LeaderTransferContext transfer_ctx_;
  VolatileContext volatile_ctx_;
  ConfigurationCtx conf_ctx_;
  LeaseContext lease_ctx_;
  MemberContext member_ctx_;

  std::mutex raft_mutex_;
  std::shared_ptr<PendingTaskBox> pending_task_box_;
  std::unique_ptr<SnapshotHandler> snapshot_handler_;
  std::unique_ptr<LogReplicator> log_replicator_;
  ClusterProgressSet progress_set_;
  ClusterLogManager log_manager_;
  ClusterMessageSender msg_sender_;

  VoteBox vote_box_;
  std::shared_ptr<ElectionTimer> election_timer_;
  std::shared_ptr<CheckQuorumTimer> check_quorum_timer_;
  // TODO: move the heartbeat_timer_ to LogReplicator
  std::shared_ptr<HeartbeatTimer> heartbeat_timer_;
  std::shared_ptr<TransferTimer> transfer_timer_;
};

}  // namespace replication

#endif  // REPLICAITON_PIKA_CLUSTER_RG_NODE_H_
