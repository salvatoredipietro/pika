// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef REPLICATION_PIKA_LOG_MANAGER_H_
#define REPLICATION_PIKA_LOG_MANAGER_H_

#include <string>
#include <memory>
#include <utility>
#include <map>
#include <mutex>
#include <vector>

#include "slash/include/slash_mutex.h"
#include "slash/include/slash_status.h"
#include "slash/include/slash_slice.h"

#include "include/util/callbacks.h"
#include "include/pika_define.h"
#include "include/storage/pika_binlog_reader.h"
#include "include/storage/pika_binlog_transverter.h"
#include "include/replication/pika_memory_log.h"
#include "include/replication/pika_stable_log.h"

namespace replication {

using slash::Status;
using storage::BinlogItem;
using storage::BinlogBuilder;

class LogEntry {
 public:
  LogEntry() : data_(), attributes_() {}
  LogEntry(const BinlogItem::Attributes& attributes, std::string&& data)
    : data_(data), attributes_(attributes) {}
  LogEntry(const LogEntry&) = default;
  LogEntry& operator=(const LogEntry&) = default;
  LogEntry(LogEntry&& other) noexcept
    : data_(std::move(other.data_)),
    attributes_(other.attributes_) {}
  LogEntry& operator=(LogEntry&& other) noexcept {
    if (&other != this) {
      data_ = std::move(other.data_);
      attributes_ = other.attributes_;
    }
    return *this;
  }

  const std::string& data() const {
    return data_;
  }
  std::string ReleaseData() {
    return std::move(data_);
  }
  // caller must make sure that the data_ is alived
  // when the slice is used.
  slash::Slice DataSlice() {
    return slash::Slice(data_.data(), data_.size());
  }
  const BinlogItem::Attributes& attributes() const {
    return attributes_;
  }
  LogOffset GetLogOffset() {
    LogOffset offset;
    offset.b_offset = GetBinlogOffset();
    offset.l_offset = GetLogicOffset();
    return offset;
  }
  LogicOffset GetLogicOffset() {
    LogicOffset l_offset(attributes_.term_id, attributes_.logic_id);
    return l_offset;
  }
  BinlogOffset GetBinlogOffset() {
    BinlogOffset b_offset(attributes_.filenum, attributes_.offset);
    return b_offset;
  }

 private:
  std::string data_;
  BinlogItem::Attributes attributes_;
};

class LogReader {
 public:
  virtual ~LogReader() = default;

  virtual Status GetLag(const LogOffset& start_offset, uint64_t* lag) = 0;
  // @param log_item(out) : Binary data for sending.
  // @param log_attrs(out) : meta information.
  // @param log_offset(out) : the end offset of the log in storage.
  virtual Status GetLog(std::string* log_item, BinlogItem::Attributes* log_attrs,
                        LogOffset* log_end_offset) = 0;
};

struct LogManagerOptions {
  ReplicationGroupID group_id;
  StableLogOptions stable_log_options;
  LogManagerOptions() = default;
  LogManagerOptions(const ReplicationGroupID& _group_id,
                    const std::string& log_path,
                    const int max_binlog_size,
                    const int retain_logs_num,
                    const bool load_index_into_memory = false)
    : group_id(_group_id),
    stable_log_options(_group_id, log_path,
        max_binlog_size, retain_logs_num, load_index_into_memory) { }
};

class LogManager {
 public:
  explicit LogManager(const LogManagerOptions& options);
  virtual ~LogManager();

  // Caller should invoke Initialize method manually before any operations.
  virtual Status Initialize();
  Status Recover(const LogOffset& applied_offset, const LogOffset& snapshot_offset);

  std::string LogFileName();
  void set_status(const Status& status);
  Status status();

  virtual void Close();
  virtual std::string Leave();
  virtual Status Reset(const LogOffset& snapshot_offset) = 0;
  virtual std::unique_ptr<LogReader> GetLogReader(const LogOffset& start_offset) = 0;
  virtual std::shared_ptr<BinlogBuilder> GetBinlogBuilder();

  Status GetProducerStatus(uint32_t* filenum, uint64_t* pro_offset,
                           uint32_t* term = nullptr, uint64_t* logic_id = nullptr);
  Status PurgeMemLogsByOffset(const LogOffset& offset, std::vector<MemLog::LogItem>* logs);
  Status PurgeMemLogsByIndex(const uint64_t index, std::vector<MemLog::LogItem>* logs);
  Status PurgeAllMemLogs(std::vector<MemLog::LogItem>* logs);
  void PurgeStableLogs(int expire_logs_nums, int expire_logs_days, uint32_t to, bool manual,
                       const PurgeFilter& filter, PurgeLogsClosure* done);

 protected:
  LogOffset snapshot_offset();
  void set_snapshot_offset(const LogOffset& snapshot);
  Status AppendLogToStableStorage(const BinlogItem::Attributes& log_attrs,
                                  const slash::Slice& log_data,
                                  LogOffset* end_offset, bool log_encoded = false);
  Status UnsafeAppendLogToStableStorage(const BinlogItem::Attributes& log_attrs,
                                        const slash::Slice& log_data,
                                        LogOffset* end_offset, bool log_encoded = false);

 protected:
  const LogManagerOptions options_;

  std::unique_ptr<StableLog> stable_logger_;
  std::shared_ptr<MemLog> mem_logger_;

  pthread_rwlock_t state_mutex_;
  LogOffset snapshot_offset_;
  Status status_;
};

} // namespace replication

#endif // REPLICATION_PIKA_LOG_MANAGER_H_
