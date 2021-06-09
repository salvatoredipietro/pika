// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef REPLICATION_PIKA_STABLE_LOG_H_
#define REPLICATION_PIKA_STABLE_LOG_H_

#include <memory>
#include <string>
#include <map>

#include "slash/include/slash_status.h"
#include "slash/include/slash_mutex.h"

#include "include/util/callbacks.h"
#include "include/pika_define.h"
#include "include/storage/pika_binlog.h"
#include "include/storage/pika_binlog_reader.h"
#include "include/storage/pika_binlog_transverter.h"

namespace replication {

using slash::Status;
using util::Closure;
using util::ClosureGuard;
using storage::BinlogItem;
using storage::BinlogBuilder;
using storage::BinlogReader;
using storage::BinlogIndexer;
using storage::PikaBinlogReader;

struct StableLogOptions {
  ReplicationGroupID group_id;
  std::string log_path;
  int max_binlog_size = 0;
  int retain_logs_num = 10;
  bool load_index_into_memory = false;
  StableLogOptions() = default;
  StableLogOptions(const ReplicationGroupID& _group_id,
                   const std::string& _log_path,
                   const int _max_binlog_size,
                   const int _retain_logs_num,
                   const bool _load_index_into_memory)
    : group_id(_group_id), log_path(_log_path),
    max_binlog_size(_max_binlog_size),
    retain_logs_num(_retain_logs_num),
    load_index_into_memory(_load_index_into_memory) { }
};

using PurgeFilter = std::function<bool(const uint32_t index)>;

class PurgeLogsClosure : public Closure {
 public:
  explicit PurgeLogsClosure(const ReplicationGroupID& group_id)
    : group_id_(group_id) { }

  std::vector<uint32_t> PurgedFileIndexes() const { return purged_indexes_; }
  void AddPurgedIndex(const uint32_t index) { purged_indexes_.push_back(index); }

 protected:
  virtual void run() override { }
  virtual ~PurgeLogsClosure() = default;

  const ReplicationGroupID group_id_;
  std::vector<uint32_t> purged_indexes_;
};

class StableLog : public std::enable_shared_from_this<StableLog> {
 public:
  explicit StableLog(const StableLogOptions& options);
  ~StableLog();

  // Caller should invoke Initialize method manually before any operations.
  // This method will occupy the files_rwlock_, so it's thread-safe.
  Status Initialize();
  void Close();

  std::shared_ptr<BinlogBuilder> Logger() {
    return stable_logger_;
  }
  void SetFirstOffset(const LogOffset& offset) {
    slash::RWLock l(&offset_rwlock_, true);
    first_offset_ = offset;
  }
  LogOffset first_offset() {
    slash::RWLock l(&offset_rwlock_, false);
    return first_offset_;
  }
  void SetLastOffset(const LogOffset& offset) {
    slash::RWLock l(&offset_rwlock_, true);
    last_offset_ = offset;
  }
  LogOffset last_offset() {
    slash::RWLock l(&offset_rwlock_, false);
    return last_offset_;
  }
  // Any operations that may change the lastest writable binlog
  // (Append, truncate, reset) must hold the lock.
  void Lock() {
    stable_logger_->Lock();
  }
  void Unlock() {
    stable_logger_->Unlock();
  }
  // REQUIRES: stable_logger_->Lock() should he held.
  Status AppendItem(const BinlogItem::Attributes& attrs, const slash::Slice& data);
  // REQUIRES: stable_logger_->Lock() should he held.
  Status TruncateTo(const LogOffset& offset);
  // REQUIRES: stable_logger_->Lock() should he held.
  Status ResetStableLog(const LogOffset& snapshot_offset);
  // REQUIRES: stable_logger_->Lock() should he held.
  void RemoveStableLogDir(std::string& log_dir);
  std::unique_ptr<PikaBinlogReader> GetPikaBinlogReader(const uint64_t index,
                                                        Status* s = nullptr);
  void PurgeFiles(int expire_logs_nums, int expire_logs_days, uint32_t to, bool manual,
                  const PurgeFilter& filter, PurgeLogsClosure* done);
  Status FindLogicOffset(const BinlogOffset& start_offset, uint64_t target_index,
                         LogOffset* offset, bool return_end_offset = true);
  Status GetLastLogFromStorage(LogOffset* log_offset);

 private:
  void DoPurgeFiles(const std::map<uint32_t, std::string>& candidates,
                    const PurgeFilter& filter, PurgeLogsClosure* done);
  // REQUIRES: files_rwlock_ should he held.
  bool UnsafeReloadFirstOffset();
  // REQUIRES: files_rwlock_ should he held.
  void UnsafeClose();
  // REQUIRES: files_rwlock_ should he held.
  Status UnsafeFindLogicOffset(const BinlogOffset& start_offset, uint64_t target_index,
                               LogOffset* offset, bool return_end_offset = true);
  // REQUIRES: files_rwlock_ should he held.
  bool UnsafeGetBinlogFiles(std::map<uint32_t, std::string>* binlogs);
  // REQUIRES: files_rwlock_ should he held.
  void UnsafeUpdateFirstOffset(uint32_t filenum);
  // REQUIRES: files_rwlock_ should he held.
  void UnsafeUpdateLastOffset(uint32_t filenum);
  // REQUIRES: files_rwlock_ should he held.
  Status UnsafePurgeFileAfter(uint32_t filenum);
  // REQUIRES: files_rwlock_ should he held.
  Status UnsafeGetLogsBefore(const BinlogOffset& start_offset,
                             std::vector<LogOffset>* hints,
                             bool return_end_offset = true);
  // REQUIRES: files_rwlock_ should he held.
  Status UnsafeFindLogicOffsetFromStorage(const BinlogOffset& start_offset,
                                          uint64_t target_index,
                                          LogOffset* found_offset,
                                          bool return_end_offset = true);
  // REQUIRES: files_rwlock_ should he held.
  Status UnsafeFindLogicOffsetBySearchingBinlog(const BinlogOffset& hint_offset,
                                                uint64_t target_index,
                                                LogOffset* found_offset,
                                                bool return_end_offset = true);
  // REQUIRES: files_rwlock_ should he held.
  Status UnsafeFindBinlogFileNum(const std::map<uint32_t, std::string> binlogs,
                                 uint64_t target_index,
                                 uint32_t start_filenum,
                                 uint32_t* founded_filenum);
  // get binlog offset range [start_offset, end_offset]
  // start_offset 0,0 end_offset 1,129, result will include binlog (1,129)
  // start_offset 0,0 end_offset 1,0, result will NOT include binlog (1,xxx)
  // start_offset 0,0 end_offset 0,0, resulet will NOT include binlog(0,xxx)
  // REQUIRES: files_rwlock_ should he held.
  Status UnsafeGetBinlogOffset(const BinlogOffset& start_offset,
                               const BinlogOffset& end_offset,
                               std::vector<LogOffset>* log_offset,
                               bool return_end_offset = true);
  // REQUIRES: files_rwlock_ should he held.
  Status UnsafeGetBinlogOffset(const BinlogOffset& start_offset,
                               LogOffset* log_offset,
                               bool return_end_offset = true);
  // clean the class members
  // REQUIRES: files_rwlock_ should he held.
  void UnsafeClean();

 private:
  const StableLogOptions options_;
  std::shared_ptr<BinlogBuilder> stable_logger_;

  // first_offset_ is the first completed record and last_offset_ is the last.
  // In other words, we can only fetch logs in range [first_offset_, last_offset_].
  //
  // NOTE: The offsets will be reset to empty after Reset.
  pthread_rwlock_t offset_rwlock_;
  LogOffset first_offset_;
  LogOffset last_offset_;

  // files_rwlock_ used to protect the operations on log files (Iterate/Add/Purge).
  pthread_rwlock_t files_rwlock_;
  uint32_t start_filenum_;

  // following fields used to index items when options_.load_index_into_memory is enabled.
  using BinlogIndexerMap = std::map<uint32_t/*filenum*/, std::unique_ptr<BinlogIndexer>>;
  // indexer_rwlock_ used to protect the operations on indexer_map_ and the underlying
  // indexer.
  pthread_rwlock_t indexer_rwlock_;
  BinlogIndexerMap indexer_map_;

  // All readers share the reader_buffer_, make sure no race condition
  char* reader_buffer_;
  std::atomic<bool> started_;
};

} // namespace replication

#endif  // REPLICATION_PIKA_STABLE_LOG_H_
