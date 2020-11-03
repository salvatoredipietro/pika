// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/replication/pika_memory_log.h"

#include "slash/include/slash_mutex.h"

#include <iterator>

namespace replication {

MemLog::MemLog() : persisted_index_(0), consumed_index_(0) {
  pthread_rwlock_init(&logs_mu_, NULL);

  // We append a dummy log into logs_ to adjust persisted_index_ and consumed_index_
  logs_.push_back(LogItem(BinlogItem::Attributes(), nullptr));
}

MemLog::~MemLog() {
  pthread_rwlock_destroy(&logs_mu_);
}

int MemLog::Size() {
  return static_cast<int>(logs_.size() - 1);
}

void MemLog::AppendLog(const LogItem& item) {
  slash::RWLock _(&logs_mu_, true);
  logs_.push_back(item);
}

void MemLog::UpdatePersistedLogsByIndex(const uint64_t logic_index) {
  slash::RWLock _(&logs_mu_, true);
  int index = InternalFindLogByLogicIndex(logic_index);
  if (index < 0) {
    return;
  }
  persisted_index_ = index;
  UnsafeTryToClearLogs();
}

Status MemLog::ConsumeLogsByIndex(const uint64_t logic_index, std::vector<LogItem>* logs) {
  slash::RWLock _(&logs_mu_, true);
  Status s = UnsafeCopyLogsByIndex(logic_index, ReadOptions(), logs);
  if (!s.ok()) {
    return s;
  }
  consumed_index_ = logic_index;
  UnsafeTryToClearLogs();
  return Status::OK();
}

Status MemLog::CopyLogsByIndex(const uint64_t logic_index, const ReadOptions& read_options,
                               std::vector<LogItem>* logs) {
  slash::RWLock _(&logs_mu_, false);
  return UnsafeCopyLogsByIndex(logic_index, read_options, logs);
}

Status MemLog::UnsafeCopyLogsByIndex(const uint64_t logic_index, const ReadOptions& read_options,
                                     std::vector<LogItem>* logs) {
  int index = InternalFindLogByLogicIndex(logic_index);
  if (index < 0) {
    return Status::NotFound("Cant find correct index");
  }
  if (read_options.direction == ReadOptions::Direction::kFront) {
    logs->insert(logs->end(),
                 logs_.begin() + 1/*skip dummy log*/,
                 logs_.begin() + index + 1);
  } else {
    auto read_count = std::min(read_options.count, logs_.size() - index);
    logs->insert(logs->end(),
                 logs_.begin() + index,
                 logs_.begin() + index + read_count);
  }
  return Status::OK();
}

// purge [begin, offset]
Status MemLog::PurgeLogsByOffset(const LogOffset& offset, std::vector<LogItem>* logs) {
  slash::RWLock _(&logs_mu_, true);
  int index = InternalFindLogByBinlogOffset(offset);
  if (index < 0) {
    return Status::NotFound("Cant find correct index");
  }
  logs->insert(logs->end(),
               logs_.begin() + 1/*skip dummy log*/,
               logs_.begin() + index + 1);
  logs_.erase(logs_.begin() + 1/*skip dummy log*/,
              logs_.begin() + index + 1);
  consumed_index_ = persisted_index_ = 0;
  return Status::OK();
}

Status MemLog::PurgeLogsByIndex(const uint64_t logic_index, std::vector<LogItem>* logs) {
  slash::RWLock _(&logs_mu_, true);
  int index = InternalFindLogByLogicIndex(logic_index);
  if (index < 0) {
    return Status::NotFound("Cant find correct index");
  }
  logs->insert(logs->end(),
               logs_.begin() + 1/*skip dummy log*/,
               logs_.begin() + index + 1);
  logs_.erase(logs_.begin() + 1/*skip dummy log*/,
              logs_.begin() + index + 1);
  consumed_index_ = persisted_index_ = 0;
  return Status::OK();
}

// purge all logs
Status MemLog::PurgeAllLogs(std::vector<LogItem>* logs) {
  slash::RWLock _(&logs_mu_, true);
  logs->insert(logs->end(),
               logs_.begin() + 1,
               logs_.end());
  logs_.erase(logs_.begin() + 1, logs_.end());
  consumed_index_ = persisted_index_ = 0;
  return Status::OK();
}

// keep mem_log [mem_log.begin, offset]
Status MemLog::TruncateToByOffset(const LogOffset& offset) {
  slash::RWLock _(&logs_mu_, true);
  int index = InternalFindLogByBinlogOffset(offset);
  if (index < 0) {
    return Status::Corruption("Cant find correct index");
  }
  logs_.erase(logs_.begin() + index + 1, logs_.end());
  consumed_index_ = persisted_index_ = 0;
  return Status::OK();
}

Status MemLog::TruncateToByIndex(const uint64_t logic_index) {
  slash::RWLock _(&logs_mu_, true);
  int index = InternalFindLogByLogicIndex(logic_index);
  if (index < 0) {
    return Status::Corruption("Cant find correct index");
  }
  logs_.erase(logs_.begin() + index + 1, logs_.end());
  consumed_index_ = persisted_index_ = 0;
  return Status::OK();
}

void MemLog::Reset() {
  slash::RWLock _(&logs_mu_, true);
  logs_.erase(logs_.begin() + 1/*skip dummy log*/, logs_.end());
  consumed_index_ = persisted_index_ = 0;
}

bool MemLog::FindLogItemByIndex(const uint64_t logic_index, LogOffset* found_offset) {
  slash::RWLock _(&logs_mu_, false);
  int index = InternalFindLogByLogicIndex(logic_index);
  if (index < 0) {
    return false;
  }
  const auto& attrs = logs_[index].attrs;
  found_offset->b_offset.filenum = attrs.filenum;
  found_offset->b_offset.offset = attrs.offset;
  found_offset->l_offset.term = attrs.term_id;
  found_offset->l_offset.index = attrs.logic_id;
  return true;
}

void MemLog::UnsafeTryToClearLogs() {
  const auto& size = logs_.size();
  const auto& clear_index = std::min(persisted_index_, consumed_index_);
  if (size > 1 && clear_index > 0 && clear_index < size) {
    logs_.erase(logs_.begin() + 1/*skip dummy log*/, logs_.begin() + clear_index + 1);
    persisted_index_ -= clear_index;
    consumed_index_ -= clear_index;
  }
}

int MemLog::InternalFindLogByLogicIndex(const uint64_t logic_index) {
  if (logs_.size() <= 1) {
    return -1;
  }
  // Logs should have been sorted by index
  const uint64_t start_index = logs_[1].attrs.logic_id;
  const uint64_t end_index = logs_.back().attrs.logic_id;
  if (logic_index < start_index || logic_index > end_index) {
    return -1;
  }
  return logic_index - start_index + 1;
}

int MemLog::InternalFindLogByBinlogOffset(const LogOffset& offset) {
  for (size_t i = 1; i < logs_.size(); ++i) {
    const auto& b_offset = BinlogOffset(logs_[i].attrs.filenum, logs_[i].attrs.offset);
    if (b_offset > offset.b_offset) {
      return -1;
    }
    if (b_offset == offset.b_offset) {
      return i;
    }
  }
  return -1;
}

} // namespace replication
