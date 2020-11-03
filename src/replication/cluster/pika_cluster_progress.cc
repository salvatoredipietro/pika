// Copyright (c) 2019-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/replication/cluster/pika_cluster_progress.h"

namespace replication {

using FollowerStateCode = Progress::FollowerState::Code;

/*ClusterProgress*/

ClusterProgress::ClusterProgress(const PeerID& peer_id,
                                 const PeerID& local_id,
                                 const ReplicationGroupID& rg_id,
                                 const ProgressOptions& options,
                                 bool is_learner)
  : Progress(peer_id, local_id, rg_id, options, 0),
  is_stopped_(false),
  is_learner_(is_learner),
  term_(0),
  next_index_(0),
  next_hint_offset_(),
  timeout_index_(0),
  pending_snapshot_index_(0),
  matched_index_(0),
  matched_offset_() {
  pthread_rwlock_init(&matched_index_mu_, NULL);
}

ClusterProgress::~ClusterProgress() {
  pthread_rwlock_destroy(&matched_index_mu_);
}

Status ClusterProgress::UnsafeGetLag(uint64_t* lag) {
  *lag = MaxLag;
  // TODO: according the matched index to locate the lag
  if (follower_state_.code() == FollowerStateCode::kFollowerBinlogSync) {
    if (log_reader_ == nullptr) {
      return Status::NotFound("log_reader is nullptr");
    }
  }
  return Status::OK();
}

std::string ClusterProgress::UnsafeToStringStatus() {
  std::stringstream tmp_stream;
  tmp_stream << "    follower_state: " << follower_state_.ToString() << "\r\n";
  tmp_stream << "    Binlog_sync_state: " << b_state_.ToString() << "\r\n";
  tmp_stream << "    sync_window: " << "\r\n" << sync_win_.ToStringStatus();
  tmp_stream << "    log_reader activated: " << (log_reader_ != nullptr) << "\r\n";
  tmp_stream << "    is_learner: " << is_learner_ << "\r\n";
  tmp_stream << "    next_index: " << next_index_ << "\r\n";
  tmp_stream << "    matched_index: " << matched_index_ << "\r\n";
  tmp_stream << "    matched_offset: " << matched_offset_.ToString() << "\r\n";
  tmp_stream << "    log_reader activated: " << (log_reader_ != nullptr) << "\r\n";
  return tmp_stream.str();
}

bool ClusterProgress::UnsafeJudgePaused() {
  switch (follower_state_.code()) {
    case FollowerStateCode::kFollowerNotSync: {
      return false;
    }
    case FollowerStateCode::kFollowerBinlogSync: {
      return sync_win_.IsFull() || sync_win_.RemainingSlots() == 0;
    }
    case FollowerStateCode::kFollowerProbeSync:
    case FollowerStateCode::kFollowerPreSnapshotSync:
    case FollowerStateCode::kFollowerSnapshotSync:
    default:
      return true;
  }
  return true;
}

bool ClusterProgress::UnsafeJudgeReadyForBinlogSync() {
  return follower_state_.code() == FollowerStateCode::kFollowerBinlogSync;
}

void ClusterProgress::UnsafeGetNextIndex(uint64_t* next_index, BinlogOffset* next_hint_offset) {
  *next_index = next_index_;
  *next_hint_offset = next_hint_offset_;
}

bool ClusterProgress::UnsafeTryToIncreaseNextIndex(const LogOffset& declared_offset) {
  DLOG(INFO) << "Replication group: " << context_.group_id().ToString()
             << " try to increase next index for " << context_.peer_id().ToString()
             << ", declared offset: " << declared_offset.ToString()
             << ", current status: "  << UnsafeToStringStatus();
  if (next_index_ <= declared_offset.l_offset.index) {
    next_index_ = declared_offset.l_offset.index + 1;
    next_hint_offset_ = declared_offset.b_offset;
    return true;
  }
  return false;
}

bool ClusterProgress::UnsafeTryToDecreaseNextIndex(const LogOffset& declared_offset) {
  DLOG(INFO) << "Replication group: " << context_.group_id().ToString()
             << " try to decrease next index for " << context_.peer_id().ToString()
             << ", declared offset: " << declared_offset.ToString()
             << ", current status: "  << UnsafeToStringStatus();
  const auto& state_code = follower_state_.code();
  switch (state_code) {
    case FollowerStateCode::kFollowerBinlogSync: {
      // We when enter into BinlogSync state, the sync points between
      // follower and leader should have been synced.
      if (declared_offset.l_offset.index <= matched_index_) {
         LOG(INFO) << "Replication group: " << context_.group_id().ToString()
                   << " receive an outdated message, remote declared offset: " << declared_offset.ToString()
                   << ", local matched index: " << matched_index_;
        return true;
      }
      UnsafeResetNextIndex();
      assert(next_index_ == matched_index_ + 1);
      break;
    }
    default: {
      if (declared_offset.l_offset.index + 1 < next_index_) {
        // follower contains less logs than us.
        next_index_ = declared_offset.l_offset.index + 1;
        next_hint_offset_ = declared_offset.b_offset;
      } else {
        // follower has conflict logs, decrease next by 1.
        // TODO(LIBA-S): adjust next to the first log in the declared term.
        if (next_index_ > 1) {
          next_index_ -= 1;
        } else {
          LOG(ERROR) << context_.ToString() << " next index equals to zero";
          return false;
        }
      }
    }
  }
  return true;
}

void ClusterProgress::UnsafeInitializeNextIndex(const LogOffset& next_log) {
  UnsafeResetNextIndex();
  next_index_ = next_log.l_offset.index;
  next_hint_offset_ = next_log.b_offset;
}

void ClusterProgress::UnsafeResetNextIndex() {
  // Discard all flying items, reset the next_index
  // to the index of the first item.
  SyncWinItem head_item = sync_win_.Head();
  next_index_ = head_item.offset_.l_offset.index;
  next_hint_offset_ = head_item.offset_.b_offset;

  UnsafeResetStatus();
}

Status ClusterProgress::UnsafeStepOffset(const LogOffset& start,
                                         const LogOffset& end) {
  const auto& state_code = follower_state_.code();
  switch (state_code) {
    case FollowerStateCode::kFollowerBinlogSync: {
      LogOffset updated_offset;
      // All items before end can be acked
      bool res = sync_win_.Update(SyncWinItem(start), SyncWinItem(end), &updated_offset, true);
      if (!res) {
        return Status::Corruption(context_.ToString() + " UpdateMatchedOffset failed");
      }
      if (updated_offset.l_offset == LogicOffset()) {
        return Status::OK();
      }
      TryToUpdateMatchedOffset(updated_offset.l_offset);
      break;
    }
    case FollowerStateCode::kFollowerProbeSync: {
      // ProbeSync has been acked by the peer, adjust the matched_index_ and next_index_
      // for later BinlogSync.
      TryToUpdateMatchedOffset(end.l_offset);
      UnsafeTryToIncreaseNextIndex(end);
      break;
    }
    case FollowerStateCode::kFollowerPreSnapshotSync: {
      // PreSnapshotSync has been acked by the peer, which means it's safe to
      // deliver the snapshot to the peer.
      break;
    }
    case FollowerStateCode::kFollowerSnapshotSync: {
      if (pending_snapshot_index_ != end.l_offset.index) {
        return Status::Corruption("The recorded snapshot_index does not match the acked index");
      }
      // SnapshotSync has been acked by the peer, adjust the matched_index_ and next_index_
      // for later BinlogSync.
      TryToUpdateMatchedOffset(end.l_offset);
      LogicOffset next_logic_offset(0/*ignore the term*/, end.l_offset.index + 1);
      UnsafeInitializeNextIndex(LogOffset(BinlogOffset(), next_logic_offset));
      break;
    }
    default:
      return Status::Corruption("unexpected state " + follower_state_.ToString());
  }
  return Status::OK();
}

void ClusterProgress::ResetMatchedOffset() {
  slash::RWLock l(&matched_index_mu_, true);
  matched_index_ = 0;
  matched_offset_.Clear();
}

bool ClusterProgress::TryToUpdateMatchedOffset(const LogicOffset& matched_offset) {
  slash::RWLock l(&matched_index_mu_, true);
  if (matched_index_ >= matched_offset.index) {
    LOG(WARNING) << "Replication group: " << context_.group_id().ToString()
                 << " maybe receive an outdated ack message from peer " << context_.peer_id().ToString()
                 << " in stage: " << follower_state_.ToString()
                 << ", local matched offset: " << matched_offset_.ToString()
                 << ", peer declared offset: " << matched_offset.ToString();
    return false;
  }
  matched_index_ = matched_offset.index;
  matched_offset_ = matched_offset;
  return true;
}

Status ClusterProgress::UnsafeBecomeBinlogSync(std::unique_ptr<LogReader> reader) {
  const auto& state_code = follower_state_.code();
  if (state_code != FollowerStateCode::kFollowerProbeSync
      && state_code != FollowerStateCode::kFollowerSnapshotSync) {
    return Status::Corruption("unexpected state " + follower_state_.ToString());
  }

  // when the ProbeSync or SnapshotSync completed, all entries before matched_offset
  // should have been replicated to the peer, we initialize the last_sent_offset_ to
  // the matched_offset.
  last_sent_offset_ = LogOffset(BinlogOffset(), matched_offset_);

  follower_state_.set_code(FollowerStateCode::kFollowerBinlogSync);
  b_state_.set_code(BinlogSyncState::Code::kReadFromFile);
  log_reader_ = std::move(reader);

  DLOG(INFO) << "Replication group: " << context_.group_id().ToString()
             << ", peer " << context_.peer_id().ToString() << " becomes kFollowerBinlogSync"
             << ", matched offset " << matched_offset_.ToString()
             << ", next index " << next_index_;
  return Status::OK();
}

Status ClusterProgress::UnsafeBecomePreSnapshotSync() {
  follower_state_.set_code(FollowerStateCode::kFollowerPreSnapshotSync);
  return Status::OK();
}

Status ClusterProgress::UnsafeBecomeSnapshotSync(const uint64_t pending_snapshot_index) {
  follower_state_.set_code(FollowerStateCode::kFollowerSnapshotSync);
  pending_snapshot_index_ = pending_snapshot_index;
  return Status::OK();
}

Status ClusterProgress::UnsafeBecomeProbeSync() {
  follower_state_.set_code(FollowerStateCode::kFollowerProbeSync);
  return Status::OK();
}

Status ClusterProgress::UnsafeBecomeNotSync() {
  follower_state_.set_code(FollowerStateCode::kFollowerNotSync);
  return Status::OK();
}

Status ClusterProgress::UnsafePrepareInfligtBinlog(std::list<BinlogChip>& binlog_chips,
                                                   bool send_when_all_acked) {
  if (follower_state_.code() != FollowerStateCode::kFollowerBinlogSync) {
    return Status::NotSupported("Follower does not in BinlogSync state, actual: " + follower_state_.ToString());
  }
  if (log_reader_ == nullptr) {
    return Status::Corruption("BinlogReader is nullptr");
  }
  if (send_when_all_acked && matched_index_ != next_index_ - 1) {
    return Status::OK();
  }
  Status s = UnsafeFetchBinlogs(binlog_chips);
  if (!s.ok()) {
    return s;
  }
  if (!binlog_chips.empty()) {
    next_index_ = binlog_chips.back().attrs.logic_id + 1;
  }
  return s;
}

/*ClusterProgressSet*/

ClusterProgressSet::ClusterProgressSet(const ReplicationGroupID& group_id,
                                       const PeerID& local_id,
                                       const ProgressOptions& options)
  : ProgressSet(group_id, local_id, options),
  configuration_(),
  progress_map_() {
  pthread_rwlock_init(&rw_lock_, NULL);
}

ClusterProgressSet::ClusterProgressSet(const ClusterProgressSet& other)
  : ProgressSet(other),
  configuration_(other.configuration_),
  progress_map_(other.progress_map_) {
  pthread_rwlock_init(&rw_lock_, NULL);
}

ClusterProgressSet::~ClusterProgressSet() {
  pthread_rwlock_destroy(&rw_lock_);
}

Status ClusterProgressSet::IsReady() {
  slash::RWLock l(&rw_lock_, false);
  // At least (n/2 + 1) voters should be active
  auto quorum = (configuration_.voter_size() >> 1) + 1;
  size_t active_voters = 0;
  Configuration::const_iterator begin, end;
  configuration_.Iterator(PeerRole::kRoleVoter, begin, end);
  for (auto peer_iter = begin; peer_iter != end; peer_iter++) {
    if (*peer_iter == local_id_) {
      active_voters++;
      continue;
    }
    auto iter = progress_map_.find(*peer_iter);
    if (iter != progress_map_.end() && iter->second->IsActive()) {
      active_voters++;
    }
  }
  if (active_voters >= quorum) {
    return Status::OK();
  }
  return Status::Incomplete("Not enough followers");
}

Status ClusterProgressSet::InitializeWithConfiguration(const Configuration& conf,
                                                       const LogOffset& start_offset,
                                                       const uint32_t term) {
  Status s;
  slash::RWLock l(&rw_lock_, true);
  progress_map_.clear();
  configuration_ = conf;

  Configuration::const_iterator begin, end;
  configuration_.Iterator(PeerRole::kRoleVoter, begin, end);
  for (auto peer_iter = begin; peer_iter != end; peer_iter++) {
    const auto& peer_id = *peer_iter;
    s = AddPeer(peer_id, PeerRole::kRoleVoter, start_offset, term);
    if (!s.ok()) {
      return s;
    }
  }
  configuration_.Iterator(PeerRole::kRoleLearner, begin, end);
  for (auto peer_iter = begin; peer_iter != end; peer_iter++) {
    const auto& peer_id = *peer_iter;
    s = AddPeer(peer_id, PeerRole::kRoleLearner, start_offset, term);
    if (!s.ok()) {
      return s;
    }
  }
  return Status::OK();
}

Status ClusterProgressSet::AddPeer(const PeerID& peer_id, const PeerRole& role,
                                   const LogOffset& start_offset, const uint32_t term) {
  ProgressPtr progress = nullptr;
  slash::RWLock l(&rw_lock_, true);
  auto iter = progress_map_.find(peer_id);
  if (iter != progress_map_.end()) {
    return Status::Corruption(peer_id.ToString() + " already in progress set");
  }
  progress = std::make_shared<ClusterProgress>(peer_id, local_id_, group_id_, options_,
                                               role == PeerRole::kRoleLearner);
  progress->set_term(term);
  progress->InitializeNextIndex(start_offset);
  progress->SetLastRecvTime(slash::NowMicros());

  progress_map_.insert({peer_id, progress});
  configuration_.AddPeer(peer_id, role);
  DLOG(INFO) << "Replication group: " << group_id_.ToString()
             << ", add a peer " << peer_id.ToString() << " to progress map";
  return Status::OK();
}

Status ClusterProgressSet::RemovePeer(const PeerID& peer_id) {
  slash::RWLock l(&rw_lock_, true);
  auto iter = progress_map_.find(peer_id);
  if (iter == progress_map_.end()) {
    return Status::NotFound(peer_id.ToString());
  }
  progress_map_.erase(iter);
  configuration_.RemovePeer(peer_id);
  return Status::OK();
}

Status ClusterProgressSet::PromotePeer(const PeerID& peer_id) {
  slash::RWLock l(&rw_lock_, true);
  auto p_iter = progress_map_.find(peer_id);
  if (p_iter == progress_map_.end()) {
    return Status::NotFound(peer_id.ToString());
  }
  auto s = configuration_.PromotePeer(peer_id);
  if (!s.IsOK()) {
    return Status::Corruption(s.ToString());
  }
  p_iter->second->promote();
  return Status::OK();
}

ClusterProgressSet::ProgressPtr ClusterProgressSet::GetProgress(const PeerID& peer_id) {
  slash::RWLock l(&rw_lock_, false);
  auto p_iter = progress_map_.find(peer_id);
  if (p_iter == progress_map_.end()) {
    return nullptr;
  }
  return p_iter->second;
}

ClusterProgressSet::ProgressPtrVec ClusterProgressSet::AllProgress(
    ClusterProgressSet::ProgressFilter filter) {
  ClusterProgressSet::ProgressPtrVec vec;
  slash::RWLock l(&rw_lock_, false);
  vec.reserve(progress_map_.size());
  for (const auto& pair : progress_map_) {
    if (filter && filter(pair.second)) {
      continue;
    }
    vec.push_back(pair.second);
  }
  return vec;
}

std::vector<PeerID> ClusterProgressSet::Voters() {
  slash::RWLock l(&rw_lock_, false);
  return configuration_.Voters();
}

std::vector<PeerID> ClusterProgressSet::Members() {
  slash::RWLock l(&rw_lock_, false);
  auto voters = configuration_.Voters();
  auto learners = configuration_.Learners();
  std::vector<PeerID> members;
  members.reserve(voters.size() + learners.size());
  members.insert(members.begin(), voters.begin(), voters.end());
  members.insert(members.begin(), learners.begin(), learners.end());
  return std::move(members);
}

Status ClusterProgressSet::IsVoter(const PeerID& peer_id, bool* is_voter) {
  slash::RWLock l(&rw_lock_, false);
  auto p_iter = progress_map_.find(peer_id);
  if (p_iter == progress_map_.end()) {
    return Status::NotFound(peer_id.ToString());
  }
  *is_voter = configuration_.IsVoter(peer_id);
  return Status::OK();
}

Status ClusterProgressSet::IsLearner(const PeerID& peer_id, bool* is_learner) {
  slash::RWLock l(&rw_lock_, false);
  auto p_iter = progress_map_.find(peer_id);
  if (p_iter == progress_map_.end()) {
    return Status::NotFound(peer_id.ToString());
  }
  *is_learner = configuration_.IsLearner(peer_id);
  return Status::OK();
}

bool ClusterProgressSet::IsExist(const PeerID& peer_id) {
  slash::RWLock l(&rw_lock_, false);
  return progress_map_.find(peer_id) != progress_map_.end();
}

LogicOffset ClusterProgressSet::GetCommittedOffset() {
  std::vector<LogicOffset> matched_offsets;
  {
    slash::RWLock l(&rw_lock_, false);
    Configuration::const_iterator begin, end;
    configuration_.Iterator(PeerRole::kRoleVoter, begin, end);
    for (auto peer_iter = begin; peer_iter != end; peer_iter++) {
      const auto& peer_id = *peer_iter;
      const auto& iter = progress_map_.find(peer_id);
      if (iter == progress_map_.end()) {
        // should never happen
        break;
      }
      matched_offsets.push_back(iter->second->matched_offset());
      DLOG(INFO) << "Replication group: " << group_id_.ToString()
                 << ", local_id: " << local_id_.ToString()
                 << ", peer: " << peer_id.ToString()
                 << ", matched offset: " << iter->second->matched_offset().ToString();
    }
  }
  std::sort(matched_offsets.begin(), matched_offsets.end());
  // The smallest index acked by a quorum is at matched_offsets[n - (n/2 + 1)]
  auto pos = matched_offsets.size() - (matched_offsets.size()/2  + 1);
  auto committed_offset = matched_offsets[pos];
  DLOG(INFO) << "Replication group: " << group_id_.ToString()
             << ", local_id: " << local_id_.ToString()
             << ", committed offset: " << committed_offset.ToString();
  return committed_offset;
}

} // namespace replication
