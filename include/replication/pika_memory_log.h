// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef REPLICATION_PIKA_MEMORY_LOG_H_
#define REPLICATION_PIKA_MEMORY_LOG_H_

#include <pthread.h>
#include <string>
#include <vector>
#include <utility>
#include <mutex>
#include <memory>

#include "slash/include/slash_mutex.h"

#include "include/util/callbacks.h"
#include "include/pika_define.h"
#include "include/storage/pika_binlog.h"
#include "include/storage/pika_binlog_transverter.h"

namespace replication {

using slash::Status;
using storage::BinlogItem;

class MemLog {
 public:
  struct LogItem {
    BinlogItem::Attributes attrs;
    std::shared_ptr<ReplTask> task;
    LogItem(const BinlogItem::Attributes& _attrs,
            const std::shared_ptr<ReplTask>& _task)
      : attrs(_attrs), task(_task) { }
    LogOffset GetLogOffset() const {
      return LogOffset(BinlogOffset(attrs.filenum, attrs.offset),
                       LogicOffset(attrs.term_id, attrs.logic_id));
    }
  };
  struct ReadOptions {
    enum class Direction : uint8_t {
      kFront = 0,
      kBack  = 1,
    };
    Direction direction;
    // count is enabled only when the direction is set to kBack.
    size_t count;
    ReadOptions() : direction(Direction::kFront), count(1) { }
  };
  MemLog();
  ~MemLog();

  int Size();
  void AppendLog(const LogItem& item);
  void UpdatePersistedLogsByIndex(const uint64_t logic_index);
  Status ConsumeLogsByIndex(const uint64_t logic_index, std::vector<LogItem>* logs);
  Status CopyLogsByIndex(const uint64_t logic_index, const ReadOptions& read_options, std::vector<LogItem>* logs);
  Status PurgeLogsByOffset(const LogOffset& offset, std::vector<LogItem>* logs);
  Status PurgeLogsByIndex(const uint64_t logic_index, std::vector<LogItem>* logs);
  Status PurgeAllLogs(std::vector<LogItem>* logs);
  Status TruncateToByOffset(const LogOffset& offset);
  Status TruncateToByIndex(const uint64_t logic_index);

  void Reset();
  bool FindLogItemByIndex(const uint64_t logic_index, LogOffset* found_offset);

 private:
  Status UnsafeCopyLogsByIndex(const uint64_t logic_index, const ReadOptions& read_options,
                               std::vector<LogItem>* logs);
  void UnsafeTryToClearLogs();
  int InternalFindLogByBinlogOffset(const LogOffset& offset);
  int InternalFindLogByLogicIndex(const uint64_t logic_index);

  pthread_rwlock_t logs_mu_;
  std::vector<LogItem> logs_;
  // logs before persisted_index_ should have been persisted into stable storage,
  // it's safe to remove them from logs_ when they have been consumed at
  // the same time.
  size_t persisted_index_;
  // logs before consumed_index_ should have been committed and applied(consumed),
  // it's safe to remove them from logs_ when they have been persisted at
  // the same time.
  size_t consumed_index_;
};

} // namespace replication

#endif  // REPLICATION_PIKA_MEMORY_LOG_H_
