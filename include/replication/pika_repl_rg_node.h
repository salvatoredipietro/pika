// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef REPLICATION_PIKA_REPL_RG_NODE_H_
#define REPLICATION_PIKA_REPL_RG_NODE_H_

#include <glog/logging.h>
#include <sstream>
#include <string>
#include <memory>
#include <vector>
#include <set>
#include <atomic>
#include <mutex>
#include <utility>

#include "slash/include/slash_mutex.h"
#include "slash/include/slash_status.h"

#include "proto/pika_inner_message.pb.h"
#include "include/util/callbacks.h"
#include "include/pika_define.h"
#include "include/replication/pika_stable_log.h"
#include "include/replication/pika_memory_log.h"
#include "include/replication/pika_configuration.h"
#include "include/replication/pika_log_manager.h"
#include "include/replication/pika_repl_progress.h"
#include "include/replication/pika_repl_transporter.h"

namespace replication {

using slash::Status;
using Message = MessageReporter::Message;

// PersistentContext record the offset states,
// this class is thread-safe.
class PersistentContext {
 public:
  struct State {
    // The latest offset persisted by quorum.
    LogOffset committed_offset;
    // The latest index that has been applied.
    LogOffset applied_offset;
    // The snapshot_offset_ records the index information
    // corresponding to the latest snapshot, recived from the leader.
    // All binlog data prior to the snapshot point is unreliable,
    // as data padding may have been done internally. So any incremental
    // request before the current point should be rejected and replaced
    // by full synchronization.
    // Invariant: snapshot_offset_ <= applied_offset_ <= committed_offset_.
    LogOffset snapshot_offset;
    State()
      : committed_offset(),
      applied_offset(),
      snapshot_offset() { }
    State(const LogOffset& _committed_offset,
          const LogOffset& _applied_offset,
          const LogOffset& _snapshot_offset)
      : committed_offset(_committed_offset),
      applied_offset(_applied_offset),
      snapshot_offset(_snapshot_offset) { }
    State(const State& other)
      : committed_offset(other.committed_offset),
      applied_offset(other.applied_offset),
      snapshot_offset(other.snapshot_offset) { }
  };
  explicit PersistentContext(const std::string& path);
  ~PersistentContext();
  DISALLOW_COPY(PersistentContext);

  Status Initialize(const LogOffset& last_offset, bool by_logic = false);
  bool IsNew() const { return new_.load(std::memory_order_acquire); }
  void PrepareUpdateAppliedOffset(const LogOffset& offset);
  void ResetPerssistentContext(const LogOffset& snapshot_offset,
                               const LogOffset& applied_offset);
  bool UpdateCommittedOffset(const LogOffset& offset, bool by_logic = false);
  void UpdateAppliedOffset(const LogOffset& offset, bool by_logic = false,
                           bool ignore_window = false);
  void UpdateSnapshotOffset(const LogOffset& offset, bool by_logic = false);
  void ResetCommittedOffset(const LogOffset& committed_offset);
  void ResetAppliedOffset(const LogOffset& applied_offset);
  void ResetSnapshotOffset(const LogOffset& snapshot_offset);
  LogOffset committed_offset() {
    slash::RWLock l(&rwlock_, false);
    return state_.committed_offset;
  }
  LogOffset applied_offset() {
    slash::RWLock l(&rwlock_, false);
    return state_.applied_offset;
  }
  LogOffset snapshot_offset() {
    slash::RWLock l(&rwlock_, false);
    return state_.snapshot_offset;
  }
  State DumpState() {
    slash::RWLock l(&rwlock_, false);
    return State(state_);
  }
  std::string ToString() {
    std::stringstream tmp_stream;
    slash::RWLock l(&rwlock_, false);
    tmp_stream << "  committed_offset " << state_.committed_offset.ToString() << "\r\n";
    tmp_stream << "  applied_offset " << state_.applied_offset.ToString() << "\r\n";
    tmp_stream << "  snapshot_offset " << state_.snapshot_offset.ToString() << "\r\n";
    tmp_stream << "  Applied window " << applied_win_.ToStringStatus() << "\r\n";
    return tmp_stream.str();
  }

 private:
  // Check the invariant: snapshot <= applied <= committed.
  // And adjust the persisted offset appropriately.
  //
  // @param last_offset: the offset for the last persistent log in LogStorage.
  // @return: OK is returned when the state is valid, otherwise
  //          storage may be broken.
  Status UnsafeCheckAndAdjustPersistentOffset(const LogOffset& last_offset, bool by_logic);
  Status UnsafeStableSave();

  pthread_rwlock_t rwlock_;
  // Use new_ to indicate whether the underlying file already exists
  // (for backward compatibility).
  std::atomic<bool> new_;
  State state_;
  SyncWindow applied_win_;
  std::string path_;
  slash::RWFile *save_;
};

class RoleState {
 public:
  enum class Code : uint8_t {
    kStateUninitialized = 0,
    kStateLeader        = 1,
    kStatePreCandidate  = 2,
    kStateCandidate     = 3,
    kStateFollower      = 4,
    kStateStop          = 5,
    kStateError         = 6,
  };
  RoleState() : code_(Code::kStateUninitialized) { }
  explicit RoleState(const Code& code) : code_(code) { }
  std::string ToString() const;
  Code code() const { return code_; }
  void set_code(Code code) { code_ = code; }

 private:
  Code code_;
};

class VolatileContext {
 public:
  struct State {
    PeerID leader_id;
    RoleState role;
    State()
      : leader_id(), role() {}
    State(const PeerID& _leader_id,
          const RoleState& _role)
      : leader_id(_leader_id), role(_role) {}
  };
  VolatileContext(const PeerID& leader_id,
                  const RoleState& role)
    : leader_id_(leader_id), role_(role) {
    pthread_rwlock_init(&rw_lock_, NULL);
  }
  ~VolatileContext() {
    pthread_rwlock_destroy(&rw_lock_);
  }
  DISALLOW_COPY(VolatileContext);

  PeerID leader_id() const {
    slash::RWLock lk(&rw_lock_, false);
    return leader_id_;
  }
  void set_leader_id(const PeerID& leader_id) {
    slash::RWLock lk(&rw_lock_, true);
    leader_id_ = leader_id;
  }
  RoleState::Code RoleCode() const {
    slash::RWLock lk(&rw_lock_, false);
    return role_.code();
  }
  void SetRoleCode(const RoleState::Code& code) {
    slash::RWLock lk(&rw_lock_, true);
    role_.set_code(code);
  }
  State DumpState() {
    slash::RWLock lk(&rw_lock_, false);
    return State(leader_id_, role_);
  }

 private:
  mutable pthread_rwlock_t rw_lock_;
  mutable PeerID leader_id_;
  mutable RoleState role_;
};

class MemberContext {
 public:
  struct State {
    PeerRole peer_role;
    bool is_alive;
  };
  MemberContext();
  ~MemberContext();
  DISALLOW_COPY(MemberContext);

  void AddMember(const PeerID& peer_id, const PeerRole& peer_role);
  void RemoveMember(const PeerID& peer_id);
  void PromoteMember(const PeerID& peer_id);
  void ChangeMemberSurvivorship(const PeerID& peer_id, bool is_alive);
  using MemberStatesMap = std::unordered_map<PeerID, State, hash_peer_id>;
  MemberStatesMap member_states();

 private:
  pthread_rwlock_t rw_lock_;
  MemberStatesMap member_states_;
};

class ReplicationGroupNodeMetaStorage {
 public:
  ReplicationGroupNodeMetaStorage() = default;
  virtual ~ReplicationGroupNodeMetaStorage() = default;
  virtual Status applied_offset(LogOffset* offset) = 0;
  virtual Status set_applied_offset(const LogOffset& offset) = 0;
  virtual Status snapshot_offset(LogOffset* offset) = 0;
  virtual Status set_snapshot_offset(const LogOffset& offset) = 0;
  virtual Status configuration(Configuration* conf) = 0;
  virtual Status set_configuration(const Configuration& conf) = 0;
  virtual Status ApplyConfiguration(const Configuration& conf, const LogOffset& offset) = 0;
  virtual Status MetaSnapshot(LogOffset* applied_offset, Configuration* conf) = 0;
  virtual Status ResetOffset(const LogOffset& snapshot_offset,
                             const LogOffset& applied_offset) = 0;
  virtual Status ResetOffsetAndConfiguration(const LogOffset& snapshot_offset,
                                             const LogOffset& applied_offset,
                                             const Configuration& configuration) = 0;
};

class StateMachine;
class ReplicationManager;

struct ReplicationGroupNodeOptions {
  ReplicationGroupID group_id;
  PeerID local_id;
  ProgressOptions progress_options;
  LogManagerOptions log_options;
  ReplicationGroupNodeMetaStorage* meta_storage = nullptr;
  StateMachine* state_machine = nullptr;
  ReplicationManager* rm = nullptr;

  ReplicationGroupNodeOptions() = default;
  bool operator==(const ReplicationGroupNodeOptions& other) const {
    return group_id == other.group_id;
  }
  bool operator<(const ReplicationGroupNodeOptions& other) const {
    return group_id < other.group_id;
  }
};

struct Ready {
  LogOffset applied_offset;
  LogOffset snapshot_offset;
  bool db_reloaded;
  bool db_reloaded_error;
  Ready() = default;
  Ready(const LogOffset& _applied_offset,
        const LogOffset& _snapshot_offset,
        bool _db_reloaded,
        bool _db_reloaded_error)
    : applied_offset(_applied_offset),
    snapshot_offset(_snapshot_offset),
    db_reloaded(_db_reloaded),
    db_reloaded_error(_db_reloaded_error) { }
};

class ReplicationGroupNode : public std::enable_shared_from_this<ReplicationGroupNode> {
 public:
  explicit ReplicationGroupNode(const ReplicationGroupNodeOptions& options);
  virtual ~ReplicationGroupNode() = default;

 public:
  virtual Status Start();
  virtual Status Stop();
  // notify the underlying log storage to stop.
  // @param log_remove(out): the log will be removed in later.
  virtual void Leave(std::string& log_remove) = 0;
  virtual bool IsReadonly() = 0;
  virtual void PurgeStableLogs(int expire_logs_nums, int expire_logs_days,
                               uint32_t to, bool manual, PurgeLogsClosure* done) = 0;
  virtual bool IsSafeToBePurged(const uint32_t filenum) = 0;
  virtual Status IsReady() = 0;
  virtual Status IsSafeToBeRemoved() = 0;
  virtual Status StepStateMachine() {
    return Status::OK();
  }
  virtual void Messages(PeerMessageListMap& msgs) = 0;
  virtual void ReportUnreachable(const PeerID& peer_id) = 0;
  virtual Status GetSyncInfo(const PeerID& peer_id,
                             std::stringstream& stream) = 0;
  virtual VolatileContext::State NodeVolatileState() = 0;
  virtual PersistentContext::State NodePersistentState() = 0;
  virtual MemberContext::MemberStatesMap NodeMemberStates() = 0;
  // Propose a replicate task to the node.
  virtual Status Propose(const std::shared_ptr<ReplTask>& task) = 0;
  // Caller invokes this method to step replicate state, including:
  // 1). the index that applied by state machine.
  // 2). the event that DB reloaded by state machine.
  virtual Status Advance(const Ready& ready) = 0;
  virtual Status ApplyConfChange(const InnerMessage::ConfChange& conf_change) {
    return Status::NotSupported("ApplyConfChange");
  }

 public:
  const ReplicationGroupID& group_id() const { return group_id_; }
  const PeerID& local_id() const { return local_id_; }

  bool IsStarted();

  bool IsPurging();
  void ClearPurge();
  static void DoPurgeStableLogs(void *arg);

 protected:
  virtual Status Initialize();
  const ReplicationGroupID group_id_;
  const PeerID local_id_;

  StateMachine* fsm_caller_;
  ReplicationManager* rm_;
  std::atomic<bool> started_;
  std::atomic<bool> purging_;
};

struct PurgeLogArg {
  std::shared_ptr<ReplicationGroupNode> node;
  PurgeLogsClosure* done;
  int expire_logs_nums;
  int expire_logs_days;
  uint32_t to;
  bool manual;
  bool force;  // Ignore the delete window
};

} // namespace replication

#endif // REPLICATION_PIKA_REPL_RG_NODE_H_
