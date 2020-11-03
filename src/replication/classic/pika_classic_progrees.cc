// Copyright (c) 2019-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/replication/classic/pika_classic_progress.h"

namespace replication {

using FollowerStateCode = Progress::FollowerState::Code;

ClassicProgress::ClassicProgress(const PeerID& peer_id,
                                 const PeerID& local_id,
                                 const ReplicationGroupID& rg_id,
                                 const ProgressOptions& options,
                                 int32_t session_id)
  : Progress(peer_id, local_id, rg_id, options, session_id),
  acked_offset_() {
}

Status ClassicProgress::UnsafeGetLag(uint64_t* lag) {
  *lag = MaxLag;
  if (follower_state_.code() == FollowerStateCode::kFollowerBinlogSync) {
    if (log_reader_ == nullptr) {
      return Status::NotFound("log_reader is nullptr");
    }
    Status s = log_reader_->GetLag(acked_offset_, lag);
    if (!s.ok()) {
      LOG(ERROR) << "Replication group: " << context_.group_id().ToString()
                 << " GetLag error " << s.ToString();
    }
  }
  return Status::OK();
}

std::string ClassicProgress::UnsafeToStringStatus() {
  std::stringstream tmp_stream;
  tmp_stream << "    follower_state: " << follower_state_.ToString() << "\r\n";
  tmp_stream << "    Binlog_sync_state: " << b_state_.ToString() << "\r\n";
  tmp_stream << "    sync_window: " << "\r\n" << sync_win_.ToStringStatus();
  tmp_stream << "    log_reader activated: " << (log_reader_ != nullptr) << "\r\n";
  tmp_stream << "    sent_offset: " << last_sent_offset_.ToString() << "\r\n";
  tmp_stream << "    acked_offset: " << acked_offset_.ToString() << "\r\n";
  tmp_stream << "    log_reader activated: " << (log_reader_ != nullptr) << "\r\n";
  return tmp_stream.str();
}

Status ClassicProgress::UnsafeUpdateAckedOffset(const LogOffset& start, const LogOffset& end) {
  // If we are in SnapshotSync state, there are no flying items in sync_win.
  if (follower_state_.code() == FollowerStateCode::kFollowerSnapshotSync) {
    if (acked_offset_.l_offset.index >= end.l_offset.index) {
      LOG(WARNING) << "Replication group: " << context_.group_id().ToString()
                   << " recieves an outdated ack message from peer " << context_.peer_id().ToString()
                   << " in SnapshotSync stage, local acked_offset: " << acked_offset_.ToString()
                   << ", peer declare_offset: " << end.ToString();
    }
  } else if (follower_state_.code() == FollowerStateCode::kFollowerBinlogSync) {
    LogOffset updated_offset;
    bool res = sync_win_.Update(SyncWinItem(start), SyncWinItem(end), &updated_offset);
    if (!res) {
      return Status::Corruption(context_.ToString() + " UpdateAckedInfo failed");
    }
    if (updated_offset == LogOffset()) {
      return Status::OK();
    }
    UnsafeSetAckedOffset(updated_offset);
  }
  return Status::OK();
}

Status ClassicProgress::UnsafeBecomeBinlogSync(std::unique_ptr<LogReader> reader,
                                               const LogOffset& last_offset) {
  UnsafeResetStatus();
  follower_state_.set_code(FollowerStateCode::kFollowerBinlogSync);
  last_sent_offset_ = last_offset;
  acked_offset_ = last_offset;
  log_reader_ = std::move(reader);
  b_state_.set_code(BinlogSyncState::Code::kReadFromFile);
  DLOG(INFO) << "Replication group: " << context_.group_id().ToString()
             << ", peer " << context_.peer_id().ToString() << " became kFollowerBinlogSync"
             << ", last_offset " << last_offset.ToString();
  return Status::OK();
}

Status ClassicProgress::UnsafeBecomeSnapshotSync() {
  follower_state_.set_code(FollowerStateCode::kFollowerSnapshotSync);
  return Status::OK();
}

Status ClassicProgress::UnsafeBecomeProbeSync() {
  follower_state_.set_code(FollowerStateCode::kFollowerProbeSync);
  return Status::OK();
}

Status ClassicProgress::UnsafeBecomeNotSync() {
  follower_state_.set_code(FollowerStateCode::kFollowerNotSync);
  return Status::OK();
}

Status ClassicProgress::UnsafePrepareHeartbeat(uint64_t now, BinlogChip& binlog_chip) {
  if (context_.IsSendTimeout(now) && last_sent_offset_ == acked_offset_) {
    binlog_chip = std::move(BinlogChip(LogOffset(), LogOffset(),
                            BinlogItem::Attributes(), std::move("")));
    context_.set_last_send_time(now);
    DLOG(INFO) << "Replication group: " << context_.group_id().ToString()
               << ", sent_offset == acked_offset " << last_sent_offset_.ToString() 
               << ", there are no more logs need to send, send a heartbeat to slave "
               << context_.peer_id().ToString() << " to keepalive";
    return Status::OK();
  }
  return Status::Busy("There are logs need to send or within the timeout period");
}

Status ClassicProgress::UnsafePrepareInfligtBinlog(std::list<BinlogChip>& binlog_chips) {
  if (follower_state_.code() != FollowerStateCode::kFollowerBinlogSync) {
    return Status::NotSupported("Follower does not in BinlogSync state, actual: " + follower_state_.ToString());
  }
  if (log_reader_ == nullptr) {
    return Status::Corruption("BinlogReader is nullptr");
  }
  Status s = UnsafeFetchBinlogs(binlog_chips);
  if (!s.ok()) {
    return s;
  }
  return s;
}

/*ClassicProgressSet*/

ClassicProgressSet::ClassicProgressSet(const ReplicationGroupID& group_id,
                                       const PeerID& local_id,
                                       const ProgressOptions& options)
  : ProgressSet(group_id, local_id, options),
  session_id_(0),
  progress_map_() {
  pthread_rwlock_init(&rw_lock_, NULL);
}

ClassicProgressSet::ClassicProgressSet(const ClassicProgressSet& other)
  : ProgressSet(other),
  session_id_(other.session_id_),
  progress_map_(other.progress_map_) {
  pthread_rwlock_init(&rw_lock_, NULL);
}

ClassicProgressSet::~ClassicProgressSet() {
  pthread_rwlock_destroy(&rw_lock_);
}

Status ClassicProgressSet::AddPeer(const PeerID& peer_id, int32_t* session_id) {
  std::shared_ptr<ClassicProgress> progress;
  slash::RWLock l(&rw_lock_, true);
  auto iter = progress_map_.find(peer_id);
  if (iter != progress_map_.end()) {
    return Status::Corruption(peer_id.ToString() + " already in progress set");
  }
  progress = std::make_shared<ClassicProgress>(peer_id, local_id_, group_id_, options_, session_id_++);
  progress->SetLastRecvTime(slash::NowMicros());

  progress_map_.insert({peer_id, progress});
  DLOG(INFO) << "Replication group: " << group_id_.ToString()
             << ", add a peer " << peer_id.ToString() << " to progress map";
  if (session_id != nullptr) {
    *session_id = progress->session_id();
  }
  return Status::OK();
}

Status ClassicProgressSet::RemovePeer(const PeerID& peer_id) {
  slash::RWLock l(&rw_lock_, true);
  auto iter = progress_map_.find(peer_id);
  if (iter == progress_map_.end()) {
    return Status::NotFound(peer_id.ToString());
  }
  progress_map_.erase(iter);
  return Status::OK();
}

ClassicProgressSet::ProgressPtr ClassicProgressSet::GetProgress(const PeerID& peer_id) {
  slash::RWLock l(&rw_lock_, false);
  auto p_iter = progress_map_.find(peer_id);
  if (p_iter == progress_map_.end()) {
    return nullptr;
  }
  return p_iter->second;
}

ClassicProgressSet::ProgressPtrVec ClassicProgressSet::AllProgress(
    ClassicProgressSet::ProgressFilter filter) {
  ClassicProgressSet::ProgressPtrVec vec;
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

Status ClassicProgressSet::UpdateProgress(const PeerID& peer_id, const LogOffset& start,
                                          const LogOffset& end) {
  auto progress = GetProgress(peer_id);
  if (progress == nullptr) {
    return Status::NotFound(peer_id.ToString());
  }
  Status s;
  if (peer_id == local_id_ && start == end) {
    // Update self progress, just advance the acked_offset
    progress->set_acked_offset(start);
  } else {
    s = progress->UpdateAckedOffset(start, end);
  }
  return s;
}

Status ClassicProgressSet::ChangeFollowerState(const PeerID& peer_id,
                                               const Progress::FollowerState::Code& state) {
  auto progress = GetProgress(peer_id);
  if (progress == nullptr) {
    return Status::NotFound(peer_id.ToString());
  }
  switch (state) {
    case Progress::FollowerState::Code::kFollowerNotSync: {
      progress->BecomeNotSync();
      break;
    }
    case Progress::FollowerState::Code::kFollowerSnapshotSync: {
      progress->BecomeSnapshotSync();
      break;
    }
    case Progress::FollowerState::Code::kFollowerBinlogSync: {
      // This path is only used to mark local progress as thre is no need
      // to replicate to itself.
      progress->BecomeBinlogSync(nullptr, LogOffset());
      break;
    }
    default:
      return Status::NotSupported("follower state");
  }
  return Status::OK();
}

Status ClassicProgressSet::IsReady() {
  return Status::OK();
}

int ClassicProgressSet::NumOfMembers() {
  slash::RWLock l(&rw_lock_, false);
  return progress_map_.size();
}

Status ClassicProgressSet::GetInfo(const PeerID& peer_id, std::stringstream& stream) {
  Status s;
  if (peer_id == local_id_) {
    PeerID local_id(local_id_);
    auto ps = AllProgress([local_id](const ProgressPtr& progress) ->bool {
        return progress->peer_id() == local_id;
    });
    stream << "  connected_followers: " << ps.size() << "\r\n";
    int i = 0;
    uint64_t lag;
    for (const auto& progress : ps) {
      if (progress->peer_id() == local_id_) {
        continue;
      }
      progress->GetLag(&lag);
      stream << "  follower[" << i++ << "]: " << progress->peer_id().ToString() << "\r\n";
      stream << "  replication_status: " << progress->follower_state().ToString() << "\r\n";
      stream << "  lag: " << lag << "\r\n";
    }
  } else {
    auto progress = GetProgress(peer_id);
    if (progress == nullptr) {
      stream << "(" << group_id_.ToString() << ":not syncing)";
      return Status::NotFound(peer_id.ToString());
    }
    uint64_t lag;
    progress->GetLag(&lag);
    stream << "(" << group_id_.ToString() << ":" << lag << ")";
  }
  return Status::OK();
}

void ClassicProgressSet::GetSafetyPurgeBinlog(std::stringstream& stream) {
  BinlogOffset boffset;
  Status s = logger_->GetProducerStatus(&(boffset.filenum), &(boffset.offset));
  if (!s.ok()) {
    stream << ",safety_purge=error" << "\r\n";
    return;
  }
  PeerID local_id(local_id_);
  bool success = false;
  uint32_t purge_max = boffset.filenum;
  if (purge_max >= 10) {
    success = true;
    purge_max -= 10;
    auto ps = AllProgress([local_id](const ProgressPtr& progress) -> bool {
        return progress->peer_id() == local_id;
    });
    for (const auto& progress : ps) {
      {
        progress->ReadLock();
        const auto& state_code = progress->UnsafeGetFollowerState().code();
        const auto& acked_filenum = progress->UnsafeGetAckedOffset().b_offset.filenum;
        if (state_code == FollowerStateCode::kFollowerBinlogSync && acked_filenum > 0) {
          purge_max = std::min(acked_filenum - 1, purge_max);
        } else {
          success = false;
          break;
        }
        progress->Unlock();
      }
    }
  }
  if (success) {
    stream << ",safety_purge=" << kBinlogPrefix + std::to_string(static_cast<int32_t>(purge_max)) << "\r\n";
  } else {
    stream << ",safety_purge=none" << "\r\n";
  }
  return;
}

bool ClassicProgressSet::IsSafeToPurge(uint32_t index) {
  PeerID local_id(local_id_);
  auto ps = AllProgress([local_id](const ProgressPtr& progress) ->bool {
      return progress->peer_id() == local_id;
  });
  for (const auto& progress : ps) {
    {
      progress->ReadLock();
      const auto& state_code = progress->UnsafeGetFollowerState().code();
      const auto& acked_filenum = progress->UnsafeGetAckedOffset().b_offset.filenum;
      if (state_code == FollowerStateCode::kFollowerSnapshotSync
          || (state_code == FollowerStateCode::kFollowerBinlogSync && index >= acked_filenum)) {
        progress->Unlock();
        return false;
      }
      progress->Unlock();
    }
  }
  return true;
}

} // namespace replication
