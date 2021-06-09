// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef REPLICATION_PIKA_REPL_PROGRESS_H_
#define REPLICATION_PIKA_REPL_PROGRESS_H_

#include <deque>
#include <memory>
#include <string>
#include <limits>
#include <set>

#include "slash/include/slash_status.h"
#include "slash/include/slash_mutex.h"

#include "include/pika_define.h"
#include "include/storage/pika_binlog.h"
#include "include/replication/pika_log_manager.h"
#include "include/replication/pika_repl_message_executor.h"

namespace replication {

using slash::Status;
using storage::BinlogBuilder;

struct SyncWinItem {
  LogOffset offset_;
  std::size_t binlog_size_;
  bool acked_;
  bool operator==(const SyncWinItem& other) const {
    if (!offset_.l_offset.Empty() || !other.offset_.l_offset.Empty()) {
      return offset_.l_offset == other.offset_.l_offset;
    }
    return offset_.b_offset.filenum == other.offset_.b_offset.filenum
      && offset_.b_offset.offset == other.offset_.b_offset.offset;
  }
  SyncWinItem()
    : offset_(LogOffset()), binlog_size_(0), acked_(false) {
  }
  explicit SyncWinItem(const LogOffset& offset, std::size_t binlog_size = 0)
    : offset_(offset), binlog_size_(binlog_size), acked_(false) {
  }
  std::string ToString() const {
    return offset_.ToString() + " binglog size: " + std::to_string(binlog_size_) +
      " acked: " + std::to_string(acked_);
  }
  bool Empty() {
    return offset_.Empty() && binlog_size_ == 0;
  }
};

class SyncWindow {
 public:
  constexpr static uint64_t kDefaultMaxInflightNumber = std::numeric_limits<uint64_t>::max();
  constexpr static uint64_t kDefaultMaxInflightBytes = pink::kProtoMaxMessage / 2;
  SyncWindow(uint64_t max_inflight_number, uint64_t max_inflight_bytes);
  SyncWinItem Head();
  SyncWinItem Tail();
  void Push(const SyncWinItem& item);
  bool Update(const SyncWinItem& start_item, const SyncWinItem& end_item,
              LogOffset* acked_offset, bool update_to_end = false);
  bool IsFull() const;
  int RemainingSlots() const;
  std::string ToStringStatus() const {
    if (win_.empty()) {
      return "      Size: " + std::to_string(win_.size()) + "\r\n";
    } else {
      std::string res;
      res += "      Size: " + std::to_string(win_.size()) + "\r\n";
      res += ("      Begin_item: " + win_.begin()->ToString() + "\r\n");
      res += ("      End_item: " + win_.rbegin()->ToString() + "\r\n");
      return res;
    }
  }
  std::size_t GetTotalBinlogSize() {
    return total_size_;
  }
  void Reset() {
    win_.clear();
    total_size_ = 0;
  }
  void set_max_inflight_number(uint64_t number) {
    max_inflight_number_ = static_cast<size_t>(number);
  }
  void set_max_inflight_bytes(uint64_t bytes) {
    max_inflight_bytes_ = static_cast<size_t>(bytes);
  }

 private:
  // TODO(whoiami) ring buffer maybe
  std::deque<SyncWinItem> win_;
  size_t total_size_;
  size_t max_inflight_number_;
  size_t max_inflight_bytes_;
};

class ProgressContext {
 public:
  constexpr static int kSendKeepAliveTimeoutUS = (2 * 1000000);  // 2s
  constexpr static int kRecvKeepAliveTimeoutUS = (20 * 1000000); // 20s
  ProgressContext(int send_timeout_us = kSendKeepAliveTimeoutUS,
                  int recv_timeout_us = kRecvKeepAliveTimeoutUS);
  ProgressContext(const PeerID& peer_id, int32_t session_id,
                  int send_timeout_us = kSendKeepAliveTimeoutUS,
                  int recv_timeout_us = kRecvKeepAliveTimeoutUS);
  ProgressContext(const PeerID& peer_id, const PeerID& local_id,
                  const ReplicationGroupID& group_id, int32_t session_id,
                  int send_timeout_us = kSendKeepAliveTimeoutUS,
                  int recv_timeout_us = kRecvKeepAliveTimeoutUS);
  const PeerID& peer_id() const { return peer_id_; }
  const PeerID& local_id() const { return local_id_; }
  const ReplicationGroupID& group_id() const { return group_id_; }
  int32_t session_id() const { return session_id_; }
  void set_session_id(int32_t s_id) { session_id_ = s_id; }
  uint64_t last_send_time() const { return last_send_time_; }
  void set_last_send_time(uint64_t t) { last_send_time_ = t; }
  void set_send_timeout(uint64_t timeout) { send_timeout_ = timeout; }
  uint64_t last_recv_time() const { return last_recv_time_; }
  void set_last_recv_time(uint64_t t) { last_recv_time_ = t; }
  void set_recv_timeout(uint64_t timeout) { recv_timeout_ = timeout; }
  bool IsRecvTimeout(uint64_t now) const {
    return last_recv_time_ + recv_timeout_ < now;
  }
  bool IsSendTimeout(uint64_t now) const {
    return last_send_time_ + send_timeout_ < now;
  }
  void Reset() {
    session_id_ = 0;
    last_send_time_ = 0;
    last_recv_time_ = 0;
  }
  std::string ToString() const {
    return "replication_group=" + group_id_.ToString() +
           ",peer_id=" + peer_id_.ToString() +
           ",session_id=" + std::to_string(session_id_);
  }
  Status Update(const ProgressContext& ctx);

 private:
  PeerID peer_id_;
  PeerID local_id_;
  ReplicationGroupID group_id_;
  int32_t session_id_;
  uint64_t last_send_time_;
  uint64_t last_recv_time_;
  int send_timeout_;
  int recv_timeout_;
};

struct ProgressOptions {
  uint64_t max_inflight_number = SyncWindow::kDefaultMaxInflightNumber;
  uint64_t max_inflight_bytes = SyncWindow::kDefaultMaxInflightBytes;
  uint64_t receive_timeout_ms = ProgressContext::kRecvKeepAliveTimeoutUS / 1000;
  uint64_t send_timeout_ms = ProgressContext::kSendKeepAliveTimeoutUS / 1000;
  ProgressOptions() = default;
  ProgressOptions(uint64_t _max_inflight_number, uint64_t _max_inflight_bytes,
                  uint64_t _receive_timeout_ms, uint64_t _send_timeout_ms)
    : max_inflight_number(_max_inflight_number), max_inflight_bytes(_max_inflight_bytes),
    receive_timeout_ms(_receive_timeout_ms), send_timeout_ms(_send_timeout_ms) { }
};

class Progress {
 public:
  class FollowerState {
   public:
    enum class Code {
      kFollowerNotSync          = 0,
      kFollowerProbeSync        = 1,
      kFollowerPreSnapshotSync  = 2,
      kFollowerSnapshotSync     = 3,
      kFollowerBinlogSync       = 4,
    };
    FollowerState() : code_(Code::kFollowerNotSync) {}
    explicit FollowerState(const Code& code) : code_(code) {}
    std::string ToString();
    void set_code(const Code& code) { code_ = code; }
    Code code() const { return code_; }

   private:
    Code code_;
  };
  class BinlogSyncState {
   public:
    enum class Code {
      kNotSync       = 0,
      kReadFromCache = 1,
      kReadFromFile  = 2,
    };
    BinlogSyncState() : code_(Code::kNotSync) {}
    explicit BinlogSyncState(const Code& code) : code_(code) {}
    std::string ToString();
    void set_code(Code code) { code_ = code; }
    Code code() const { return code_; }

   private:
    Code code_;
  };

 public:
  Progress(const PeerID& peer_id,
           const PeerID& local_id,
           const ReplicationGroupID& rg_id,
           const ProgressOptions& options,
           int32_t session_id);
  virtual ~Progress();
  FollowerState follower_state() {
    slash::RWLock l(&progress_mu_, false);
    return UnsafeGetFollowerState();
  }
  Status UpdateContext(const ProgressContext& ctx) {
    slash::RWLock l(&progress_mu_, true);
    return UnsafeUpdateContext(ctx);
  }
  void UpdateMaxInflightNumber(uint64_t number) {
    slash::RWLock l(&progress_mu_, true);
    UnsafeUpdateMaxInflightNumber(number);
  }
  void UpdateMaxInflightBytes(uint64_t bytes) {
    slash::RWLock l(&progress_mu_, true);
    UnsafeUpdateMaxInflightBytes(bytes);
  }
  void SetLastRecvTime(uint64_t now) {
    slash::RWLock l(&progress_mu_, true);
    UnsafeSetLastRecvTime(now);
  }
  void UpdateRecvTimeout(uint64_t timeout) {
    slash::RWLock l(&progress_mu_, true);
    UnsafeUpdateRecvTimeout(timeout);
  }
  bool IsRecvTimeout(uint64_t now) {
    slash::RWLock l(&progress_mu_, false);
    return UnsafeJudgeRecvTimeout(now);
  }
  void UpdateSendTimeout(uint64_t timeout) {
    slash::RWLock l(&progress_mu_, true);
    UnsafeUpdateSendTimeout(timeout);
  }
  bool IsSendTimeout(uint64_t now) {
    slash::RWLock l(&progress_mu_, false);
    return UnsafeJudgeSendTimeout(now);
  }
  PeerID peer_id() {
    slash::RWLock l(&progress_mu_, false);
    return UnsafeGetPeerID();
  }
  int32_t session_id() {
    slash::RWLock l(&progress_mu_, false);
    return UnsafeGetSessionID();
  }

  // We provide unsafe APIS
  void ReadLock() {
    pthread_rwlock_rdlock(&progress_mu_);
  }
  void WriteLock() {
    pthread_rwlock_wrlock(&progress_mu_);
  }
  void Unlock() {
    pthread_rwlock_unlock(&progress_mu_);
  }

  FollowerState UnsafeGetFollowerState() {
    return follower_state_;
  }
  Status UnsafeUpdateContext(const ProgressContext& ctx) {
    return context_.Update(ctx);
  }
  void UnsafeUpdateMaxInflightNumber(uint64_t number) {
    return sync_win_.set_max_inflight_number(number);
  }
  void UnsafeUpdateMaxInflightBytes(uint64_t bytes) {
    return sync_win_.set_max_inflight_bytes(bytes);
  }
  void UnsafeSetLastRecvTime(uint64_t now) {
    return context_.set_last_recv_time(now);
  }
  void UnsafeUpdateRecvTimeout(uint64_t timeout) {
    return context_.set_recv_timeout(timeout);
  }
  bool UnsafeJudgeRecvTimeout(uint64_t now) {
    return context_.IsRecvTimeout(now);
  }
  void UnsafeUpdateSendTimeout(uint64_t timeout) {
    return context_.set_send_timeout(timeout);
  }
  bool UnsafeJudgeSendTimeout(uint64_t now) {
    return context_.IsSendTimeout(now);
  }
  PeerID UnsafeGetPeerID() { return context_.peer_id(); }
  int32_t UnsafeGetSessionID() { return context_.session_id(); }

 protected:
  constexpr static uint64_t MaxLag = std::numeric_limits<uint64_t>::max();
  void UnsafeResetStatus();
  Status UnsafeFetchBinlogs(std::list<BinlogChip>& binlog_chips);

  pthread_rwlock_t progress_mu_;
  ProgressContext context_;
  SyncWindow sync_win_;
  FollowerState follower_state_;
  BinlogSyncState b_state_;
  LogOffset last_sent_offset_;
  std::unique_ptr<LogReader> log_reader_;
};

class ProgressSet {
 public:
  ProgressSet(const ReplicationGroupID& group_id,
              const PeerID& local_id,
              const ProgressOptions& options);
  ProgressSet(const ProgressSet& other);
  virtual ~ProgressSet() = default;

  void SetLogger(std::shared_ptr<BinlogBuilder> logger) { logger_ = logger; }

 protected:
  std::shared_ptr<BinlogBuilder> logger_;
  ReplicationGroupID group_id_;
  PeerID local_id_;
  ProgressOptions options_;
};

} // namespace replication

#endif  // REPLICATION_PIKA_REPL_PROGRESS_H_
