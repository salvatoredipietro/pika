// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/replication/pika_repl_rg_node.h"

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/stat.h>

#include "slash/include/env.h"
#include "pink/include/pink_cli.h"

#include "include/storage/pika_binlog_transverter.h"
#include "include/replication/pika_repl_manager.h"

namespace replication {

/* PersistentContext */

PersistentContext::PersistentContext(const std::string& path)
  : new_(false),
  state_(),
  applied_win_(SyncWindow::kDefaultMaxInflightNumber, SyncWindow::kDefaultMaxInflightBytes),
  path_(path + kContext),
  save_(nullptr) {
  pthread_rwlock_init(&rwlock_, NULL);
}

PersistentContext::~PersistentContext() {
  pthread_rwlock_destroy(&rwlock_);
  if (save_ != nullptr) {
    delete save_;
    save_ = nullptr;
  }
}

Status PersistentContext::UnsafeStableSave() {
  char *p = save_->GetData();
  memcpy(p, &(state_.applied_offset.b_offset.filenum), sizeof(uint32_t));
  p += 4;
  memcpy(p, &(state_.applied_offset.b_offset.offset), sizeof(uint64_t));
  p += 8;
  memcpy(p, &(state_.applied_offset.l_offset.term), sizeof(uint32_t));
  p += 4;
  memcpy(p, &(state_.applied_offset.l_offset.index), sizeof(uint64_t));
  p += 8;
  memcpy(p, &(state_.snapshot_offset.b_offset.filenum), sizeof(uint32_t));
  p += 4;
  memcpy(p, &(state_.snapshot_offset.b_offset.offset), sizeof(uint64_t));
  p += 8;
  memcpy(p, &(state_.snapshot_offset.l_offset.term), sizeof(uint32_t));
  p += 4;
  memcpy(p, &(state_.snapshot_offset.l_offset.index), sizeof(uint64_t));
  p += 8;
  memcpy(p, &(state_.committed_offset.b_offset.filenum), sizeof(uint32_t));
  p += 4;
  memcpy(p, &(state_.committed_offset.b_offset.offset), sizeof(uint64_t));
  p += 8;
  memcpy(p, &(state_.committed_offset.l_offset.term), sizeof(uint32_t));
  p += 4;
  memcpy(p, &(state_.committed_offset.l_offset.index), sizeof(uint64_t));
  return Status::OK();
}

Status PersistentContext::Initialize(const LogOffset& last_offset, bool by_logic) {
  slash::RWLock l(&rwlock_, true);
  if (!slash::FileExists(path_)) {
    Status s = slash::NewRWFile(path_, &save_);
    if (!s.ok()) {
      LOG(ERROR) << "PersistentContext new file failed " << s.ToString();
      return s;
    }
    UnsafeStableSave();
    new_.store(true, std::memory_order_release);
  } else {
    Status s = slash::NewRWFile(path_, &save_);
    if (!s.ok()) {
      LOG(ERROR) << "PersistentContext new file failed " << s.ToString();
      return s;
    }
  }
  if (save_->GetData() != NULL) {
    memcpy(reinterpret_cast<char*>(&(state_.applied_offset.b_offset.filenum)), save_->GetData(), sizeof(uint32_t));
    memcpy(reinterpret_cast<char*>(&(state_.applied_offset.b_offset.offset)), save_->GetData() + 4, sizeof(uint64_t));
    memcpy(reinterpret_cast<char*>(&(state_.applied_offset.l_offset.term)), save_->GetData() + 12, sizeof(uint32_t));
    memcpy(reinterpret_cast<char*>(&(state_.applied_offset.l_offset.index)), save_->GetData() + 16, sizeof(uint64_t));
    memcpy(reinterpret_cast<char*>(&(state_.snapshot_offset.b_offset.filenum)), save_->GetData() + 24, sizeof(uint32_t));
    memcpy(reinterpret_cast<char*>(&(state_.snapshot_offset.b_offset.offset)), save_->GetData() + 28, sizeof(uint64_t));
    memcpy(reinterpret_cast<char*>(&(state_.snapshot_offset.l_offset.term)), save_->GetData() + 36, sizeof(uint32_t));
    memcpy(reinterpret_cast<char*>(&(state_.snapshot_offset.l_offset.index)), save_->GetData() + 40, sizeof(uint64_t));
    memcpy(reinterpret_cast<char*>(&(state_.committed_offset.b_offset.filenum)), save_->GetData() + 48, sizeof(uint32_t));
    memcpy(reinterpret_cast<char*>(&(state_.committed_offset.b_offset.offset)), save_->GetData() + 52, sizeof(uint64_t));
    memcpy(reinterpret_cast<char*>(&(state_.committed_offset.l_offset.term)), save_->GetData() + 60, sizeof(uint32_t));
    memcpy(reinterpret_cast<char*>(&(state_.committed_offset.l_offset.index)), save_->GetData() + 64, sizeof(uint64_t));
    return UnsafeCheckAndAdjustPersistentOffset(last_offset, by_logic);
  } else {
    return Status::Corruption("PersistentContext init error");
  }
}

Status PersistentContext::UnsafeCheckAndAdjustPersistentOffset(const LogOffset& last_offset, bool by_logic) {
  if (new_.load(std::memory_order_acquire) && !last_offset.Empty()) {
    // There are no perssistent context for version less than v4.0,
    // so we construct a new one for backward compatibility.
    // TODO: remove this path in the future
    state_.committed_offset = last_offset;
    state_.applied_offset = last_offset;
    // Since we don't have any other information to determine whether
    // the current log is built based on snapshot data, we initialize it
    // to the latest log location to ensure that new followers never fail to
    // establish synchronization with us due to the impact of padding logs
    // after the actual snapshot point.
    state_.snapshot_offset = last_offset;
    UnsafeStableSave();
    return Status::OK();
  }
  if (by_logic) {
    if (state_.committed_offset.l_offset < state_.applied_offset.l_offset) {
      if (!state_.committed_offset.l_offset.Empty()) {
        return Status::Corruption("committed_offset is less than applied_offset, storage maybe broken");
      }
      // Only applied_offset is persistent in versions before v4.0,
      // so we initialize it to applied_offset.
      state_.committed_offset = state_.applied_offset;
      UnsafeStableSave();
    }
    if (state_.applied_offset.l_offset < state_.snapshot_offset.l_offset) {
      return Status::Corruption("applied_offset is less than snapshot_offset, storage maybe broken");
    }
  } else {
    if (state_.committed_offset < state_.applied_offset) {
      if (!state_.committed_offset.b_offset.Empty()) {
        return Status::Corruption("committed_offset is less than applied_offset, storage maybe broken");
      }
      // Only applied_offset is persistent in versions before v4.0,
      // so we initialize it to applied_offset.
      state_.committed_offset = state_.applied_offset;
      UnsafeStableSave();
    }
    if (state_.applied_offset < state_.snapshot_offset) {
      return Status::Corruption("applied_offset is less than snapshot_offset, storage maybe broken");
    }
  }
  return Status::OK();
}

void PersistentContext::PrepareUpdateAppliedOffset(const LogOffset& offset) {
  slash::RWLock l(&rwlock_, true);
  applied_win_.Push(SyncWinItem(offset));
}

void PersistentContext::ResetPerssistentContext(const LogOffset& snapshot_offset,
                                                const LogOffset& applied_offset) {
  slash::RWLock l(&rwlock_, true);
  state_.snapshot_offset = snapshot_offset;

  state_.applied_offset = applied_offset;
  applied_win_.Reset();

  state_.committed_offset = applied_offset;
  UnsafeStableSave();
}

bool PersistentContext::UpdateCommittedOffset(const LogOffset& offset, bool by_logic) {
  slash::RWLock l(&rwlock_, true);
  bool save = by_logic ? offset.l_offset > state_.committed_offset.l_offset
              : offset.b_offset > state_.committed_offset.b_offset;
  if (save) {
    state_.committed_offset = offset;
    UnsafeStableSave();
    return true;
  }
  return false;
}

void PersistentContext::UpdateAppliedOffset(const LogOffset& offset, bool by_logic,
                                            bool ignore_window) {
  slash::RWLock l(&rwlock_, true);
  LogOffset cur_offset;
  if (ignore_window) {
    cur_offset = offset;
  } else if (!applied_win_.Update(SyncWinItem(offset), SyncWinItem(offset), &cur_offset)) {
    return;
  }
  bool save = by_logic ? cur_offset.l_offset > state_.applied_offset.l_offset
              : cur_offset.b_offset > state_.applied_offset.b_offset;
  if (save) {
    state_.applied_offset = cur_offset;
    UnsafeStableSave();
  }
}

void PersistentContext::UpdateSnapshotOffset(const LogOffset& offset, bool by_logic) {
  slash::RWLock l(&rwlock_, true);
  bool save = by_logic ? offset.l_offset > state_.snapshot_offset.l_offset
              : offset.b_offset > state_.snapshot_offset.b_offset;
  if (save) {
    state_.snapshot_offset = offset;
    UnsafeStableSave();
  }
}

void PersistentContext::ResetCommittedOffset(const LogOffset& offset) {
  slash::RWLock l(&rwlock_, true);
  state_.committed_offset = offset;
  UnsafeStableSave();
}

void PersistentContext::ResetSnapshotOffset(const LogOffset& offset) {
  slash::RWLock l(&rwlock_, true);
  state_.snapshot_offset = offset;
  UnsafeStableSave();
}

void PersistentContext::ResetAppliedOffset(const LogOffset& offset) {
  slash::RWLock l(&rwlock_, true);
  state_.applied_offset = offset;
  applied_win_.Reset();
  UnsafeStableSave();
}

/* RoleState */

std::string RoleState::ToString() const {
  switch (code_) {
    case Code::kStateUninitialized:
      return "StateUninitialized";
    case Code::kStateLeader:
      return "StateLeader";
    case Code::kStatePreCandidate:
      return "StatePreCandidate";
    case Code::kStateCandidate:
      return "StateCandidate";
    case Code::kStateFollower:
      return "StateFollower";
    case Code::kStateStop:
      return "StateStop";
    case Code::kStateError:
      return "StateError";
  }
  return "Unknown";
}

/* MemberContext */

MemberContext::MemberContext() {
  pthread_rwlock_init(&rw_lock_, NULL);
}

MemberContext::~MemberContext() {
  pthread_rwlock_destroy(&rw_lock_);
}

void MemberContext::AddMember(const PeerID& peer_id, const PeerRole& peer_role) {
  slash::RWLock l(&rw_lock_, true);
  auto iter = member_states_.find(peer_id);
  if (iter != member_states_.end()) {
    return;
  }
  State st;
  st.peer_role = peer_role;
  st.is_alive = true;
  member_states_.insert({peer_id, st});
}

void MemberContext::RemoveMember(const PeerID& peer_id) {
  slash::RWLock l(&rw_lock_, true);
  member_states_.erase(peer_id);
}

void MemberContext::PromoteMember(const PeerID& peer_id) {
  slash::RWLock l(&rw_lock_, true);
  auto iter = member_states_.find(peer_id);
  if (iter == member_states_.end()) {
    return;
  }
  iter->second.peer_role = PeerRole::kRoleVoter;
}

void MemberContext::ChangeMemberSurvivorship(const PeerID& peer_id, bool is_alive) {
  slash::RWLock l(&rw_lock_, true);
  auto iter = member_states_.find(peer_id);
  if (iter == member_states_.end()) {
    return;
  }
  iter->second.is_alive = is_alive;
}

MemberContext::MemberStatesMap MemberContext::member_states() {
  slash::RWLock l(&rw_lock_, false);
  return member_states_;
}

/* ReplicationGroupNode */

ReplicationGroupNode::ReplicationGroupNode(const ReplicationGroupNodeOptions& options)
  : group_id_(options.group_id),
  local_id_(options.local_id),
  fsm_caller_(options.state_machine),
  rm_(options.rm),
  started_(false),
  purging_(false) {
}

Status ReplicationGroupNode::Initialize() {
  return Status::OK();
}

Status ReplicationGroupNode::Start() {
  if (started_.load(std::memory_order_acquire)) {
    return Status::Busy("node has already been started");
  }
  started_.store(true, std::memory_order_release);
  return Status::OK();
}

Status ReplicationGroupNode::Stop() {
  if (!started_.load(std::memory_order_acquire)) {
    return Status::OK();
  }
  started_.store(false, std::memory_order_release);
  return Status::OK();
}

bool ReplicationGroupNode::IsStarted() {
  return started_.load(std::memory_order_acquire);
}

bool ReplicationGroupNode::IsPurging() {
  bool expect = false;
  if (!purging_.compare_exchange_strong(expect, true)) {
    LOG(WARNING) << "purge process already exist";
    return true;
  }
  return false;
}

void ReplicationGroupNode::ClearPurge() {
  purging_ = false;
}

void ReplicationGroupNode::DoPurgeStableLogs(void *arg) {
  PurgeLogArg* purge_arg = static_cast<PurgeLogArg*>(arg);
  util::ResourceGuard<PurgeLogArg> guard(purge_arg);
  purge_arg->node->PurgeStableLogs(purge_arg->expire_logs_nums,
                                   purge_arg->expire_logs_days,
                                   purge_arg->to,
                                   purge_arg->manual,
                                   purge_arg->done);
  purge_arg->node->ClearPurge();
}

} // namespace replication
