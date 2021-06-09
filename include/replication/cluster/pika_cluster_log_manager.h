// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef REPLICATION_PIKA_CLUSTER_LOG_MANAGER_H_
#define REPLICATION_PIKA_CLUSTER_LOG_MANAGER_H_

#include <vector>
#include <mutex>
#include <memory>

#include "slash/include/slash_status.h"
#include "slash/include/slash_slice.h"

#include "include/pika_define.h"
#include "include/util/mpsc.h"
#include "include/util/callbacks.h"
#include "include/util/task_executor.h"
#include "include/storage/pika_binlog.h"
#include "include/replication/pika_memory_log.h"
#include "include/replication/pika_log_manager.h"
#include "include/replication/pika_repl_message_executor.h"

namespace replication {

using slash::Status;
using util::MPSCNode;
using util::MPSCQueue;
using util::TaskExecutor;

class DiskClosure : public MPSCNode {
 public:
  DiskClosure()
    : status_(Status::OK()), prev_offset_(), term_(0) { }
  DiskClosure(const LogOffset& prev_offset, const uint32_t term)
    : status_(Status::OK()), prev_offset_(prev_offset), term_(term) {}
  virtual ~DiskClosure() = default;

  Status status() const { return status_; }
  void set_status(const Status& status) { status_ = status; }

  const LogOffset& prev_offset() const { return prev_offset_; }
  uint32_t term() const { return term_; }

  struct Entry {
    BinlogItem::Attributes data_attrs;
    slash::Slice data_slice;
    Entry(const BinlogItem::Attributes& attrs,
          const slash::Slice& slice)
      : data_attrs(attrs), data_slice(slice) {}
    LogicOffset GetLogicOffset() {
      return LogicOffset(data_attrs.term_id, data_attrs.logic_id);
    }
  };
  void PushEntry(const Entry& entry) {
    entries_.push_back(entry);
  }
  Entry& GetEntry(const size_t index) {
    return entries_[index];
  }
  size_t entries_size() const { return entries_.size(); }

 protected:
  Status status_;
  const LogOffset prev_offset_;
  const uint32_t term_;
  std::vector<Entry> entries_;
};

class LastOffsetClosure : public Closure {
 public:
  LastOffsetClosure() = default;
  virtual ~LastOffsetClosure() = default;

  const LogOffset& last_offset() const { return last_offset_; }
  void set_last_offset(const LogOffset& last_offset) {
    last_offset_ = last_offset;
  }

 protected:
  LogOffset last_offset_;
};

struct ClusterLogManagerOptions : public LogManagerOptions {
  // executor used to schedule DiskWrite tasks.
  TaskExecutor* executor = nullptr;
  uint64_t max_consumed_number_once = 128;
  ClusterLogManagerOptions() = default;
};

class ClusterLogManager : public LogManager {
  friend class TruncateDiskClosure;
  friend class ResetDiskClosure;
 public:
  explicit ClusterLogManager(const ClusterLogManagerOptions& options);
  virtual ~ClusterLogManager();

  virtual Status Initialize() override;
  // This method will block until all pending disk writes have
  // been completed.
  virtual void Close() override;

  // logs will be stored into memory_log_ and stable_logger_ at the same time
  // when async is set to false. Otherwise, the logs will only be recorded
  // in memory_log_ and persisted in backgroud.
  //
  // NOTE: when async is false, the user will be responsible for processing the
  //       closure regardless of the success of the execution result.
  //       when async is true, the user only need to be responsible for processing
  //       the closure when the execution result is not ok.
  Status AppendLog(const std::shared_ptr<ReplTask>& task, DiskClosure* closure, bool async = true);
  // logs will onlyu be recorded in memory_log_ and persisted in backgroud.
  //
  // NOTE: the user only need to be responsible for processing
  //       the closure when the execution result is not ok.
  Status AppendReplicaLog(const PeerID& peer_id, std::vector<LogEntry>& entries,
                          DiskClosure* closure);

  uint32_t CurrentTerm();
  void UpdateTerm(const uint32_t term);

  Status GetLag(const LogOffset& start_offset, uint64_t* lag);
  LogOffset GetLogOffset(const BinlogOffset& hint_offset,
                         const uint64_t target_index);
  Status GetLogsFromMemory(const uint64_t index, size_t count, std::vector<MemLog::LogItem>* logs);
  const LogOffset& GetLastPendingOffset();
  LogOffset GetLastStableOffset(const bool in_disk_thread = false,
                                LastOffsetClosure* closure = nullptr);
  virtual Status Reset(const LogOffset& offset) override;
  virtual std::unique_ptr<LogReader> GetLogReader(const LogOffset& start_offset) override;
  std::unique_ptr<PikaBinlogReader> GetBinlogReader(const uint64_t index, Status* s);
  void TryToClearMemLogs();

 private:
  void StopDiskWrites();
  void StartDiskWrite(DiskClosure* head);
  static uint64_t HandleDiskOperations(void* arg, MPSCQueue::Iterator& iterator);
  Status UnsafeAppendLogSync(const std::shared_ptr<ReplTask>& task, DiskClosure* closure);
  Status UnsafeAppendLogAsync(const std::shared_ptr<ReplTask>& task, DiskClosure* closure);

  Status HandleConflicts(const LogOffset& prev_offset, std::vector<LogEntry>& entries);
  Status LogIsMatched(const LogOffset& log_offset);
  LogOffset UnsafeGetLogOffset(const BinlogOffset& hint_offset,
                               const uint64_t target_index);
  void UnsafeTruncateTo(const LogOffset& truncate_offset_in_remote);
  void TruncateStableLog(const LogOffset& truncate_offset);
  void ResetStableLog(const LogOffset& snapshot_offset);

  std::mutex mutex_;
  MPSCQueue* disk_write_queue_;
  TaskExecutor* executor_;
  uint64_t max_consumed_number_once_;
  // logs can not be found when their indexes are
  // not in range [first_offset_, last_offset_]
  LogOffset first_offset_;
  LogOffset last_offset_;
  uint32_t current_term_;
  uint64_t next_index_; // next index for write
};

} // namespace replication

#endif // REPLICATION_PIKA_CLUSTER_LOG_MANAGER_H_
