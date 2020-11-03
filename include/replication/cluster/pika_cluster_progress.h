// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef REPLICATION_PIKA_CLUSTER_PROGRESS_H_
#define REPLICATION_PIKA_CLUSTER_PROGRESS_H_

#include <pthread.h>
#include <string>
#include <vector>
#include <list>
#include <unordered_map>

#include "slash/include/slash_status.h"
#include "slash/include/slash_mutex.h"

#include "include/pika_define.h"
#include "include/replication/pika_configuration.h"
#include "include/replication/pika_repl_progress.h"

namespace replication {

using slash::Status;

class ClusterProgress : public Progress {
 public:
  ClusterProgress(const PeerID& peer_id,
                  const PeerID& local_id,
                  const ReplicationGroupID& rg_id,
                  const ProgressOptions& options,
                  bool is_learner);
  ~ClusterProgress();
  std::string ToStringStatus() {
    slash::RWLock l(&progress_mu_, false);
    return UnsafeToStringStatus();
  }
  Status StepOffset(const LogOffset& start, const LogOffset& end) {
    slash::RWLock l(&progress_mu_, true);
    return UnsafeStepOffset(start, end);
  }
  Status BecomeBinlogSync(std::unique_ptr<LogReader> reader) {
    slash::RWLock l(&progress_mu_, true);
    return UnsafeBecomeBinlogSync(std::move(reader));
  }
  Status BecomeProbeSync() {
    slash::RWLock l(&progress_mu_, true);
    return UnsafeBecomeProbeSync();
  }
  Status BecomePreSnapshotSync() {
    slash::RWLock l(&progress_mu_, true);
    return UnsafeBecomePreSnapshotSync();
  }
  Status BecomeSnapshotSync(const uint64_t pending_snapshot_index) {
    slash::RWLock l(&progress_mu_, true);
    return UnsafeBecomeSnapshotSync(pending_snapshot_index);
  }
  Status BecomeNotSync() {
    slash::RWLock l(&progress_mu_, true);
    return UnsafeBecomeNotSync();
  }
  Status PrepareInfligtBinlog(std::list<BinlogChip>& binlog_chips,
                              bool send_when_all_acked = false) {
    slash::RWLock l(&progress_mu_, true);
    return UnsafePrepareInfligtBinlog(binlog_chips, send_when_all_acked);
  }
  Status GetLag(uint64_t* lag) {
    slash::RWLock l(&progress_mu_, false);
    return UnsafeGetLag(lag);
  }
  bool IsPaused() {
    slash::RWLock l(&progress_mu_, false);
    return UnsafeJudgePaused();
  }
  bool IsReadyForBinlogSync() {
    slash::RWLock l(&progress_mu_, false);
    return UnsafeJudgeReadyForBinlogSync();
  }
  bool is_learner() {
    slash::RWLock l(&progress_mu_, false);
    return UnsafeJudgeLearner();
  }
  bool IsActive() {
    slash::RWLock l(&progress_mu_, false);
    return UnsafeIsActive();
  }
  void promote() {
    slash::RWLock l(&progress_mu_, true);
    UnsafePromote();
  }
  void set_timeout_index(const uint64_t index) {
    slash::RWLock l(&progress_mu_, true);
    UnsafeSetTimeoutIndex(index);
  }
  uint64_t timeout_index() {
    slash::RWLock l(&progress_mu_, false);
    return UnsafeGetTimeoutIndex();
  }
  uint32_t term() {
    slash::RWLock l(&progress_mu_, false);
    return UnsafeGetTerm();
  }
  void set_term(const uint32_t new_term) {
    slash::RWLock l(&progress_mu_, true);
    return UnsafeSetTerm(new_term);
  }
  void NextIndex(uint64_t* next_index, BinlogOffset* next_hint_offset) {
    slash::RWLock l(&progress_mu_, false);
    UnsafeGetNextIndex(next_index, next_hint_offset);
  }
  bool TryToIncreaseNextIndex(const LogOffset& declared_offset) {
    slash::RWLock l(&progress_mu_, true);
    return UnsafeTryToIncreaseNextIndex(declared_offset);
  }
  bool TryToDecreaseNextIndex(const LogOffset& declared_offset) {
    slash::RWLock l(&progress_mu_, true);
    return UnsafeTryToDecreaseNextIndex(declared_offset);
  }
  void InitializeNextIndex(const LogOffset& next_log) {
    slash::RWLock l(&progress_mu_, true);
    UnsafeInitializeNextIndex(next_log);
  }
  uint64_t matched_index() {
    slash::RWLock l(&matched_index_mu_, false);
    return matched_index_;
  }
  LogicOffset matched_offset() {
    slash::RWLock l(&matched_index_mu_, false);
    return matched_offset_;
  }
  bool IsStopped() {
    slash::RWLock l(&progress_mu_, false);
    return UnsafeGetStopped();
  }
  void MakeStopped(bool stopped) {
    slash::RWLock l(&progress_mu_, true);
    UnsafeMarkStopped(stopped);
  }
  void ResetMatchedOffset();
  bool TryToUpdateMatchedOffset(const LogicOffset& matched_offset);
  std::string UnsafeToStringStatus();
  Status UnsafeBecomeBinlogSync(std::unique_ptr<LogReader> reader);
  Status UnsafeBecomePreSnapshotSync();
  Status UnsafeBecomeSnapshotSync(const uint64_t pending_snapshot_index);
  Status UnsafeBecomeProbeSync();
  Status UnsafeBecomeNotSync();
  Status UnsafePrepareHeartbeat(uint64_t now, BinlogChip& binlog_chips);
  Status UnsafePrepareInfligtBinlog(std::list<BinlogChip>& binlog_chips, bool send_when_all_acked = false);
  Status UnsafeGetLag(uint64_t* lag);
  uint32_t UnsafeGetTerm() { return term_; }
  void UnsafeSetTerm(const uint32_t new_term) { term_ = new_term; }
  bool UnsafeJudgeLearner() { return is_learner_; }
  void UnsafePromote() { is_learner_ = false; }
  void UnsafeSetTimeoutIndex(const uint64_t index) { timeout_index_ = index; }
  uint64_t UnsafeGetTimeoutIndex() { return timeout_index_; }
  Status UnsafeStepOffset(const LogOffset& start, const LogOffset& end);
  bool UnsafeJudgePaused();
  bool UnsafeJudgeReadyForBinlogSync();
  void UnsafeGetNextIndex(uint64_t* next_index, BinlogOffset* next_hint_offset);
  bool UnsafeTryToIncreaseNextIndex(const LogOffset& declared_offset);
  bool UnsafeTryToDecreaseNextIndex(const LogOffset& declared_offset);
  void UnsafeInitializeNextIndex(const LogOffset& next_log);
  void UnsafeResetNextIndex();
  bool UnsafeGetStopped() { return is_stopped_; }
  void UnsafeMarkStopped(bool stopped) { is_stopped_ = stopped; }
  bool UnsafeIsActive() {
    return !UnsafeJudgeRecvTimeout(slash::NowMicros());
  }

 private:
  bool is_stopped_;
  bool is_learner_;
  uint32_t term_;

  uint64_t next_index_;
  // use next_hint_offset_ to help locate the log qualified by the next_index_
  BinlogOffset next_hint_offset_;

  uint64_t timeout_index_;
  uint64_t pending_snapshot_index_;

  pthread_rwlock_t matched_index_mu_;
  // invarant: matched_index_ < next_index_
  uint64_t matched_index_;
  // matched_offset_ records the term and index of the matched point.
  LogicOffset matched_offset_;
};

class ClusterProgressSet : public ProgressSet {
 public:
  using ProgressPtr = std::shared_ptr<ClusterProgress>;
  using ProgressPtrVec = std::vector<ProgressPtr>;
  using ProgressFilter = std::function<bool(const ProgressPtr&)>;

  ClusterProgressSet(const ReplicationGroupID& group_id,
                     const PeerID& local_id,
                     const ProgressOptions& options);
  ClusterProgressSet(const ClusterProgressSet& other);
  ~ClusterProgressSet();

  Status InitializeWithConfiguration(const Configuration& conf,
                                     const LogOffset& start_offset,
                                     const uint32_t term);
  Status IsReady();
  bool IsExist(const PeerID& peer_id);

  // Add a peer to the progress set.
  // @param peer_id : the remote peer id (ip:port).
  // @param role : the role in the replication group.
  // @param start_offset: the initialized next_index for replication.
  // @param term: the initialized term.
  Status AddPeer(const PeerID& peer_id, const PeerRole& role,
                 const LogOffset& start_offset, const uint32_t term);
  Status RemovePeer(const PeerID& peer_id);
  Status PromotePeer(const PeerID& peer_id);

  ProgressPtr GetProgress(const PeerID& peer_id);
  ProgressPtrVec AllProgress(ProgressFilter filter);

  std::vector<PeerID> Voters();
  std::vector<PeerID> Members();
  Status IsVoter(const PeerID& peer_id, bool* is_voter);
  Status IsLearner(const PeerID& peer_id, bool* is_learner);
  LogicOffset GetCommittedOffset();

 private:
  pthread_rwlock_t rw_lock_;
  Configuration configuration_;
  std::unordered_map<PeerID, ProgressPtr, hash_peer_id> progress_map_;
};

} // namespace replication

#endif // REPLICATION_PIKA_CLUSTER_PROGRESS_H_
