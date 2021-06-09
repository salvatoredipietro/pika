// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef REPLICATION_PIKA_CLASSIC_PROGRESS_H_
#define REPLICATION_PIKA_CLASSIC_PROGRESS_H_

#include <string>
#include <vector>
#include <list>
#include <memory>
#include <unordered_map>

#include "slash/include/slash_status.h"
#include "slash/include/slash_mutex.h"

#include "include/pika_define.h"
#include "include/replication/pika_repl_progress.h"
#include "include/replication/pika_repl_message_executor.h"

namespace replication {

using slash::Status;

class ClassicProgress : public Progress {
 friend class ClassicProgressSet;
 public:
  ClassicProgress(const PeerID& peer_id,
                  const PeerID& local_id,
                  const ReplicationGroupID& rg_id,
                  const ProgressOptions& options,
                  int32_t session_id);
  std::string ToStringStatus() {
    slash::RWLock l(&progress_mu_, false);
    return UnsafeToStringStatus();
  }
  // Step the sync_win according to the response offsets.
  // The logs in [start, end] have been replicated to peer successfully.
  Status UpdateAckedOffset(const LogOffset& start, const LogOffset& end) {
    slash::RWLock l(&progress_mu_, true);
    return UnsafeUpdateAckedOffset(start, end);
  }
  Status BecomeBinlogSync(std::unique_ptr<LogReader> reader,
                          const LogOffset& last_offset) {
    slash::RWLock l(&progress_mu_, true);
    return UnsafeBecomeBinlogSync(std::move(reader), last_offset);
  }
  Status BecomeSnapshotSync() {
    slash::RWLock l(&progress_mu_, true);
    return UnsafeBecomeSnapshotSync();
  }
  Status BecomeProbeSync() {
    slash::RWLock l(&progress_mu_, true);
    return UnsafeBecomeProbeSync();
  }
  Status BecomeNotSync() {
    slash::RWLock l(&progress_mu_, true);
    return UnsafeBecomeNotSync();
  }
  Status PrepareInfligtBinlog(std::list<BinlogChip>& binlog_chips) {
    slash::RWLock l(&progress_mu_, true);
    return UnsafePrepareInfligtBinlog(binlog_chips);
  }
  Status PrepareHeartbeat(uint64_t now, BinlogChip& binlog_chips) {
    slash::RWLock l(&progress_mu_, true);
    return UnsafePrepareHeartbeat(now, binlog_chips);
  }
  Status GetLag(uint64_t* lag) {
    slash::RWLock l(&progress_mu_, false);
    return UnsafeGetLag(lag);
  }
  void set_acked_offset(const LogOffset& offset) {
    slash::RWLock l(&progress_mu_, true);
    UnsafeSetAckedOffset(offset);
  }
  LogOffset AckedOffset() {
    slash::RWLock l(&progress_mu_, false);
    return UnsafeGetAckedOffset();
  }

  std::string UnsafeToStringStatus();
  Status UnsafeUpdateAckedOffset(const LogOffset& start, const LogOffset& end);
  Status UnsafeBecomeBinlogSync(std::unique_ptr<LogReader> reader,
                                const LogOffset& last_offset);
  Status UnsafeBecomeSnapshotSync();
  Status UnsafeBecomeProbeSync();
  Status UnsafeBecomeNotSync();
  Status UnsafePrepareHeartbeat(uint64_t now, BinlogChip& binlog_chips);
  Status UnsafePrepareInfligtBinlog(std::list<BinlogChip>& binlog_chips);
  Status UnsafeGetLag(uint64_t* lag);
  LogOffset UnsafeGetAckedOffset() { return acked_offset_; }
  void UnsafeSetAckedOffset(const LogOffset& offset) { acked_offset_ = offset; }

 private:
  LogOffset acked_offset_;
};

class ClassicProgressSet : public ProgressSet {
 public:
  using ProgressPtr = std::shared_ptr<ClassicProgress>;
  using ProgressPtrVec = std::vector<ProgressPtr>;
  using ProgressFilter = std::function<bool(const ProgressPtr&)>;

  ClassicProgressSet(const ReplicationGroupID& group_id,
                     const PeerID& local_id,
                     const ProgressOptions& options);
  ClassicProgressSet(const ClassicProgressSet& other);
  ~ClassicProgressSet();

  Status IsReady();
  int NumOfMembers();
  bool IsSafeToPurge(uint32_t filenum);
  Status GetInfo(const PeerID& peer_id, std::stringstream& stream);
  void GetSafetyPurgeBinlog(std::stringstream& stream);

  Status AddPeer(const PeerID& peer_id, int32_t* session_id);
  Status RemovePeer(const PeerID& peer_id);

  ProgressPtr GetProgress(const PeerID& peer_id);
  ProgressPtrVec AllProgress(ProgressFilter filter);
  // Update the progress of peer.
  // @param peer_id : the remote peer id (ip:port).
  // @param start : the acked start offset.
  // @param end : the acked end offset.
  Status UpdateProgress(const PeerID& peer_id, const LogOffset& acked_start,
                        const LogOffset& acked_end);
  Status ChangeFollowerState(const PeerID& peer_id,
                             const Progress::FollowerState::Code& state);

 private:
  pthread_rwlock_t rw_lock_;
  int32_t session_id_;
  std::unordered_map<PeerID, ProgressPtr, hash_peer_id> progress_map_;
};

}

#endif // REPLICATION_PIKA_CLASSIC_PROGRESS_H_
