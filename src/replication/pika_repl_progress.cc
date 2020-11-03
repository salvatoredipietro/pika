// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/replication/pika_repl_progress.h"

#include <limits>
#include <algorithm>

#include "pink/include/pink_define.h"

#include "include/storage/pika_binlog_transverter.h"

namespace replication {

using storage::BinlogItem;

/* SyncWindow */

SyncWindow::SyncWindow(uint64_t max_inflight_number, uint64_t max_inflight_bytes)
  : total_size_(0),
  max_inflight_number_(static_cast<size_t>(max_inflight_number)),
  max_inflight_bytes_(static_cast<size_t>(max_inflight_bytes)) { }

void SyncWindow::Push(const SyncWinItem& item) {
  win_.push_back(item);
  total_size_ += item.binlog_size_;
}

SyncWinItem SyncWindow::Head() {
  if (win_.size() != 0) {
    return win_[0];
  }
  return SyncWinItem();
}

SyncWinItem SyncWindow::Tail() {
  if (win_.size() != 0) {
    return win_[win_.size() - 1];
  }
  return SyncWinItem();
}

bool SyncWindow::Update(const SyncWinItem& start_item,
                        const SyncWinItem& end_item,
                        LogOffset* acked_offset,
                        bool update_to_end) {
  size_t start_pos = win_.size(), end_pos = win_.size();
  for (size_t i = 0; i < win_.size(); ++i) {
    if (win_[i] == start_item) {
      start_pos = i;
    }
    if (win_[i] == end_item) {
      end_pos = i;
      break;
    }
  }
  if (update_to_end) {
    if (end_pos == win_.size()) {
      // When `end` is matched, all items before `end` are matched too.
      // Reach here means the items may have been acked by the prior
      // requests, just return.
      return true;
    }
    start_pos = 0;
  } else {
    if (start_pos == win_.size() || end_pos == win_.size()) {
      LOG(WARNING) << "Ack offset Start: " << start_item.ToString()
                   << " End: " << end_item.ToString()
                   << " not found in binlog controller window. window status " << ToStringStatus();
      return false;
    }
  }
  for (size_t i = start_pos; i <= end_pos; ++i) {
    win_[i].acked_ = true;
    total_size_ -= win_[i].binlog_size_;
  }
  while (!win_.empty()) {
    if (win_[0].acked_) {
      *acked_offset = win_[0].offset_;
      win_.pop_front();
    } else {
      break;
    }
  }
  return true;
}

bool SyncWindow::IsFull() const {
  return total_size_ >= max_inflight_bytes_;
}

int SyncWindow::RemainingSlots() const {
  if (win_.size() >= max_inflight_number_) {
    return 0;
  }
  size_t remaining_number = max_inflight_number_ - win_.size();
  return remaining_number > 0 ? remaining_number : 0;
}

/* ProgressContext */
ProgressContext::ProgressContext(int send_timeout_us, int recv_timeout_us)
  : last_send_time_(0),
  last_recv_time_(0),
  send_timeout_(send_timeout_us),
  recv_timeout_(recv_timeout_us) {}

ProgressContext::ProgressContext(const PeerID& peer_id,
                                 int32_t session_id,
                                 int send_timeout_us,
                                 int recv_timeout_us)
  : peer_id_(peer_id),
  session_id_(session_id),
  last_send_time_(0),
  last_recv_time_(0),
  send_timeout_(send_timeout_us),
  recv_timeout_(recv_timeout_us) { }

ProgressContext::ProgressContext(const PeerID& peer_id,
                                 const PeerID& local_id,
                                 const ReplicationGroupID& group_id,
                                 int32_t session_id,
                                 int send_timeout_us,
                                 int recv_timeout_us)
  : peer_id_(peer_id),
  local_id_(local_id),
  group_id_(group_id),
  session_id_(session_id),
  last_send_time_(0),
  last_recv_time_(0),
  send_timeout_(send_timeout_us),
  recv_timeout_(recv_timeout_us) { }

Status ProgressContext::Update(const ProgressContext& ctx) {
  if (ctx.session_id_ != session_id_) {
    return Status::Corruption("sessions are not equal, local: "
                              + std::to_string(session_id_)
                              + ", remote: " + std::to_string(ctx.session_id_));
  }
  if (last_send_time_ < ctx.last_send_time_) {
    last_send_time_ = ctx.last_send_time_;
  }
  if (last_recv_time_ < ctx.last_recv_time_) {
    last_recv_time_ = ctx.last_recv_time_;
  }
  return Status::OK();
}

/* Progress */

std::string Progress::FollowerState::ToString() {
  switch (code_) {
    case Code::kFollowerNotSync:
      return "FollowerNotSync";
    case Code::kFollowerProbeSync:
      return "FollowerProbeSync";
    case Code::kFollowerPreSnapshotSync:
      return "FollowerPreSnapshotSync";
    case Code::kFollowerSnapshotSync:
      return "FollowerSnapshotSync";
    case Code::kFollowerBinlogSync:
      return "FollowerBinlogSync";
  }
  return "Unknown";
}

std::string Progress::BinlogSyncState::ToString() {
  switch (code_) {
    case Code::kNotSync:
      return "NotSync";
    case Code::kReadFromCache:
      return "ReadFromCache";
    case Code::kReadFromFile:
      return "ReadFromFile";
  }
  return "Unknown";
}

Progress::Progress(const PeerID& peer_id,
                   const PeerID& local_id,
                   const ReplicationGroupID& rg_id,
                   const ProgressOptions& options,
                   int32_t session_id)
  : context_(peer_id, local_id, rg_id, session_id,
      options.send_timeout_ms * 1000/*Change to microseconds*/,
      options.receive_timeout_ms * 1000/*Change to microseconds*/),
  sync_win_(options.max_inflight_number, options.max_inflight_bytes),
  follower_state_(),
  b_state_(),
  last_sent_offset_() {
    pthread_rwlock_init(&progress_mu_, NULL);
}

Progress::~Progress() {
  pthread_rwlock_destroy(&progress_mu_);
}

void Progress::UnsafeResetStatus() {
  log_reader_ = nullptr;
  sync_win_.Reset();
  last_sent_offset_.Clear();
}

Status Progress::UnsafeFetchBinlogs(std::list<BinlogChip>& binlog_chips) {
  int remain_slots = sync_win_.RemainingSlots();
  for (int i = 0; i < remain_slots && !sync_win_.IsFull(); ++i) {
    std::string msg;
    LogOffset end_offset;
    BinlogItem::Attributes log_attrs;
    Status s = log_reader_->GetLog(&msg, &log_attrs, &end_offset);
    if (s.IsEndFile() || s.IsNotFound()) {
      break;
    } else if (s.IsCorruption() || s.IsIOError()) {
      return Status::Corruption(context_.ToString() + " Read Binlog error:" + s.ToString());
    }
    log_attrs.filenum = end_offset.b_offset.filenum;
    log_attrs.offset = end_offset.b_offset.offset;
    BinlogOffset pending_b_offset = end_offset.b_offset;
    LogicOffset pending_l_offset = LogicOffset(log_attrs.term_id, log_attrs.logic_id);
    LogOffset pending_offset(pending_b_offset, pending_l_offset);

    DLOG(INFO) << "fetch item " << pending_offset.ToString()
               << ", sent_offset: " << last_sent_offset_.ToString();

    binlog_chips.emplace_back(last_sent_offset_, pending_offset, log_attrs, std::move(msg));

    sync_win_.Push(SyncWinItem(pending_offset, msg.size()));
    last_sent_offset_ = pending_offset;
    context_.set_last_send_time(slash::NowMicros());
  }
  return Status::OK();
}

/* ProgressSet */
ProgressSet::ProgressSet(const ReplicationGroupID& group_id,
                         const PeerID& local_id,
                         const ProgressOptions& options)
  : logger_(nullptr), group_id_(group_id),
  local_id_(local_id),
  options_(options) {
}

ProgressSet::ProgressSet(const ProgressSet& other)
  : logger_(other.logger_),
  group_id_(other.group_id_),
  local_id_(other.local_id_),
  options_(other.options_) {
}

} // namespace replication

