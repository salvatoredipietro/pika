// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/replication/pika_log_manager.h"

#include <glog/logging.h>

namespace replication {

using storage::NewFileName;
using storage::PikaBinlogTransverter;

LogManager::LogManager(const LogManagerOptions& options)
  : options_(options),
  stable_logger_(nullptr),
  mem_logger_(nullptr),
  status_(Status::OK()) {
  stable_logger_ = std::unique_ptr<StableLog>(new StableLog(options.stable_log_options));
  mem_logger_ = std::make_shared<MemLog>();

  pthread_rwlock_init(&state_mutex_, NULL);
}

LogManager::~LogManager() {
  Close();
  pthread_rwlock_destroy(&state_mutex_);
}

std::string LogManager::Leave() {
  Close();
  std::string log_remove;
  stable_logger_->RemoveStableLogDir(log_remove);
  return log_remove;
}

void LogManager::Close() {
  stable_logger_->Close();
  mem_logger_->Reset();
}

Status LogManager::Initialize() {
  Status s = stable_logger_->Initialize();
  if (!s.ok()) {
    LOG(ERROR) << "Replication group: " << options_.group_id.ToString()
               << ", initialize the stable log failed: " << s.ToString();
  }
  return s;
}

Status LogManager::Recover(const LogOffset& applied_offset, const LogOffset& snapshot_offset) {
  set_snapshot_offset(snapshot_offset);

  BinlogOffset recover_start_offset = applied_offset.b_offset; // the end offset of the applied
  if (!applied_offset.l_offset.Empty()) {
    // NOTE: when the l_offset is not empty, we should locate the b_offset
    //       manually, as the recorded value (applied_offset.b_offset) may
    //       be inaccurate.
    LogOffset next_start_offset;
    Status s = stable_logger_->FindLogicOffset(applied_offset.b_offset,
                                               applied_offset.l_offset.index + 1,
                                               &next_start_offset, false);
    if (s.IsNotFound()) {
      LOG(INFO) << "Replication group: " << options_.group_id.ToString()
                << " cannot found logs after applied offset: " << applied_offset.l_offset.ToString();
      return Status::OK();
    } else if (!s.ok()) {
      LOG(ERROR) << "Replication group: " << options_.group_id.ToString()
                 << " locate the first log after the applied offset " << applied_offset.l_offset.ToString()
                 << " failed, error info: " << s.ToString();
      return s;
    }
    recover_start_offset = next_start_offset.b_offset;
  }

  PikaBinlogReader binlog_reader;
  int res = binlog_reader.Seek(stable_logger_->Logger(), recover_start_offset.filenum,
                               recover_start_offset.offset);
  if (res != 0) {
    LOG(ERROR) << "Replication group: " << options_.group_id.ToString()
               << " seek Binlog error";
    return Status::Corruption("initialize the binlog reader failed");
  }
  // Load the unapplied logs from stable stroage into memory_log,
  // caller can process these logs from memory_log later.
  //
  // TODO(LIBA-S): we should not load all logs after applied
  // offset into memory to reduce memory used.
  while (1) {
    LogOffset offset;
    std::string binlog;
    Status s = binlog_reader.Get(&binlog, &(offset.b_offset.filenum),
                                 &(offset.b_offset.offset));
    if (s.IsEndFile()) {
      break;
    } else if (s.IsIncomplete()) {
      LOG(WARNING) << "Replication group: " << options_.group_id.ToString()
                   << " Read a BadRecord in recover stage"
                   << ", any logs before the record: " << offset.ToString()
                   << " should have been applied, just skip them";
      mem_logger_->Reset();
      continue;
    } else if (s.IsCorruption() || s.IsIOError()) {
      LOG(ERROR) << "Replication group: " << options_.group_id.ToString()
                 << " Read Binlog error";
      return s;
    }
    BinlogItem item;
    if (!PikaBinlogTransverter::BinlogDecode(binlog, &item)) {
      LOG(ERROR) << "Replication group: " << options_.group_id.ToString()
                 << " Binlog item decode failed";
      return s;
    }
    // store the endpoint of the item
    item.set_filenum(offset.b_offset.filenum);
    item.set_offset(offset.b_offset.offset);
    auto log_format = item.content_type() == BinlogItem::Attributes::ContentType::TypeFirst
                      ? ReplTask::LogFormat::kRedis : ReplTask::LogFormat::kPB;
    auto repl_task = std::make_shared<ReplTask>(std::move(item.ReleaseContent()), nullptr,
                                                options_.group_id, PeerID(),
                                                ReplTask::Type::kRedoType, log_format);
    mem_logger_->AppendLog(MemLog::LogItem(item.attributes(), repl_task));
  }
  return Status::OK();
}

std::string LogManager::LogFileName() {
  return stable_logger_->Logger()->filename();
}

Status LogManager::PurgeMemLogsByOffset(const LogOffset& offset,
                                        std::vector<MemLog::LogItem>* logs) {
  return mem_logger_->PurgeLogsByOffset(offset, logs);
}

Status LogManager::PurgeMemLogsByIndex(const uint64_t index,
                                       std::vector<MemLog::LogItem>* logs) {
  return mem_logger_->PurgeLogsByIndex(index, logs);
}

Status LogManager::PurgeAllMemLogs(std::vector<MemLog::LogItem>* logs) {
  return mem_logger_->PurgeAllLogs(logs);
}

void LogManager::PurgeStableLogs(int expire_logs_nums, int expire_logs_days,
                                 uint32_t to, bool manual,
                                 const PurgeFilter& filter, PurgeLogsClosure* done) {
  stable_logger_->PurgeFiles(expire_logs_nums, expire_logs_days, to, manual, filter, done);
}

void LogManager::set_snapshot_offset(const LogOffset& snapshot_offset) {
  slash::RWLock l(&state_mutex_, true);
  snapshot_offset_ = snapshot_offset;
}

LogOffset LogManager::snapshot_offset() {
  slash::RWLock l(&state_mutex_, false);
  return snapshot_offset_;
}

void LogManager::set_status(const Status& status) {
  slash::RWLock l(&state_mutex_, true);
  status_ = status;
}

Status LogManager::status() {
  slash::RWLock l(&state_mutex_, false);
  return status_;
}

std::shared_ptr<BinlogBuilder> LogManager::GetBinlogBuilder() {
  return stable_logger_->Logger();
}

Status LogManager::GetProducerStatus(uint32_t* filenum, uint64_t* pro_offset,
                                     uint32_t* term, uint64_t* logic_id) {
  return stable_logger_->Logger()->GetProducerStatus(filenum, pro_offset, term, logic_id);
}

Status LogManager::AppendLogToStableStorage(const BinlogItem::Attributes& log_attrs,
                                            const slash::Slice& log_data,
                                            LogOffset* end_offset,
                                            bool log_encoded) {
  Status s;
  {
    stable_logger_->Lock();
    s = UnsafeAppendLogToStableStorage(log_attrs, log_data, end_offset, log_encoded);
    stable_logger_->Unlock();
  }
  return s;
}

Status LogManager::UnsafeAppendLogToStableStorage(const BinlogItem::Attributes& log_attrs,
                                                  const slash::Slice& log_data,
                                                  LogOffset* end_offset,
                                                  bool log_encoded) {
  Status s;
  if (!log_encoded) {
    std::string binlog = PikaBinlogTransverter::BinlogEncode(log_attrs.exec_time,
                                                             log_attrs.term_id,
                                                             log_attrs.logic_id,
                                                             log_attrs.filenum,
                                                             log_attrs.offset,
                                                             log_data,
                                                             log_attrs.content_type,
                                                             {});
    // Note: the filenum and offset in item is the start offset of the record
    // (the actual start point may be larger than it).
    s = stable_logger_->AppendItem(log_attrs, slash::Slice(binlog.data(), binlog.size()));
    if (!s.ok()) {
      return s;
    }
  } else {
    s = stable_logger_->AppendItem(log_attrs, log_data);
    if (!s.ok()) {
      return s;
    }
  }
  if (end_offset != nullptr) {
    uint32_t filenum;
    uint64_t offset;
    // Note: we mark the BinlogOffset to the end of the record.
    stable_logger_->Logger()->GetProducerStatus(&filenum, &offset);
    *end_offset = LogOffset(BinlogOffset(filenum, offset),
                            LogicOffset(log_attrs.term_id, log_attrs.logic_id));
  }
  return Status::OK();
}

} // namespace replication
