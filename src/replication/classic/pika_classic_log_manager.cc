// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/replication/classic/pika_classic_log_manager.h"

#include "include/storage/pika_binlog_transverter.h"

namespace replication {

using storage::PikaBinlogTransverter;

/* ClassicLogReader */

class ClassicLogReader : public LogReader {
 public:
  ClassicLogReader(std::unique_ptr<PikaBinlogReader> binlog_reader)
    : binlog_reader_(std::move(binlog_reader)) {}
  ~ClassicLogReader() = default;

  virtual Status GetLag(const LogOffset& start_offset, uint64_t* lag) override;
  virtual Status GetLog(std::string* log_item, BinlogItem::Attributes* log_attrs,
                        LogOffset* log_end_offset) override;

 private:
  std::unique_ptr<PikaBinlogReader> binlog_reader_;
};

Status ClassicLogReader::GetLag(const LogOffset& start_offset, uint64_t* lag) {
  BinlogOffset binlog_offset;
  binlog_reader_->GetProducerStatus(&(binlog_offset.filenum), &(binlog_offset.offset));
  *lag = (binlog_offset.filenum - start_offset.b_offset.filenum) * binlog_reader_->BinlogFileSize()
          + (binlog_offset.offset - start_offset.b_offset.offset);
  return Status::OK();
}

Status ClassicLogReader::GetLog(std::string* log_item, BinlogItem::Attributes* log_attrs,
                                LogOffset* log_end_offset) {
  Status s = binlog_reader_->Get(log_item, &log_end_offset->b_offset.filenum,
                                 &log_end_offset->b_offset.offset);
  if (!s.ok()) {
    return s;
  }
  // We send the entrie log to peer for backward compatibility.
  if (!PikaBinlogTransverter::BinlogAttributesDecode(*log_item, log_attrs)) {
    return Status::Corruption("Binlog item decode failed");
  }
  return s;
}

/* ClassicLogManager */

ClassicLogManager::ClassicLogManager(const LogManagerOptions& options)
  : LogManager(options) {
}

Status ClassicLogManager::AppendLog(const std::shared_ptr<ReplTask>& task, LogOffset* log_offset) {
  uint32_t filenum = 0, term = 0;
  uint64_t offset = 0, index = 0;
  Status s = stable_logger_->Logger()->GetProducerStatus(&filenum, &offset,
                                                         &term, &index);
  if (!s.ok()) {
    return s;
  }
  auto type = task->log_format() == ReplTask::LogFormat::kRedis
              ? BinlogItem::Attributes::ContentType::TypeFirst
              : BinlogItem::Attributes::ContentType::TypeSecond;
  BinlogItem::Attributes attrs;
  attrs.content_type = type;
  attrs.exec_time = time(nullptr);
  attrs.term_id = term;
  attrs.logic_id = index + 1;
  attrs.filenum = filenum;
  attrs.offset = offset;
  s = AppendLogToStableStorage(attrs, task->LogSlice(), log_offset, false/*not_encoded*/);
  return s;
}

Status ClassicLogManager::AppendReplicaLog(const PeerID& peer_id, BinlogItem::Attributes log_attrs,
                                           std::string log_data) {
  Status s;
  {
    // Make sure stable log and mem log consistent
    stable_logger_->Lock();
    // precheck if prev_offset match && drop this log if this log exist
    LogOffset last_index = stable_logger_->last_offset();
    if (log_attrs.logic_id < last_index.l_offset.index) {
      LOG(WARNING) << "Replication group: " << options_.group_id.ToString()
                   << " Drop log from leader, remote index " << log_attrs.logic_id
                   << " less than local last index " << last_index.l_offset.index;
      stable_logger_->Unlock();
      return s;
    }
    auto repl_task = std::make_shared<ReplTask>(std::move(log_data),
                                                nullptr, options_.group_id, peer_id,
                                                ReplTask::Type::kReplicaType,
                                                ReplTask::LogFormat::kRedis);
    // log_attrs is the start point of the record in the master binlog.
    LogOffset end_offset_in_local;
    s = UnsafeAppendLogToStableStorage(log_attrs, repl_task->LogSlice(), &end_offset_in_local, false);
    if (!s.ok()) {
      stable_logger_->Unlock();
      return s;
    }
    log_attrs.filenum = end_offset_in_local.b_offset.filenum;
    log_attrs.offset = end_offset_in_local.b_offset.offset;
    mem_logger_->AppendLog(MemLog::LogItem(log_attrs, repl_task));
    stable_logger_->Unlock();
  }
  return s;
}

void ClassicLogManager::CheckOffset(const PeerID& peer_id,
                                    const BinlogOffset& follower_expect_offset,
                                    CheckResult& result, BinlogOffset& expect_offset) {
  result = CheckResult::kOK;
  Status s = stable_logger_->Logger()->GetProducerStatus(&(expect_offset.filenum),
                                                         &(expect_offset.offset));
  if (!s.ok()) {
    LOG(WARNING) << "Replication group: " << options_.group_id.ToString()
                 << " OffsetCheck Get binlog offset error " << s.ToString();
    result = CheckResult::kError;
    return;
  }

  if (expect_offset.filenum < follower_expect_offset.filenum
    || (expect_offset.filenum == follower_expect_offset.filenum
        && expect_offset.offset < follower_expect_offset.offset)) {
    LOG(WARNING) << "Replication group: " << options_.group_id.ToString()
                 << " Follower offset is larger than mine, Follower " << peer_id.ToString()
                 << ", last_offset info in follower: " << follower_expect_offset.ToString()
                 << ", last_offset info in local: " << expect_offset.ToString();
    result = CheckResult::kSyncPointLarger;
    return;
  }

  // There are two cases that log is purged:
  // 1) The log file has been deleted as the purge conditions are met.
  // 2) The log file exist, but the data is Incompleted (have been incorporated
  //    into the snapshot).
  std::string confile = storage::NewFileName(stable_logger_->Logger()->filename(),
                                             follower_expect_offset.filenum);
  if (!slash::FileExists(confile)) {
    LOG(INFO) << "Replication group: " << options_.group_id.ToString()
              << " binlog has been purged, may need full sync";
    result = CheckResult::kSyncPointBePurged;
    return;
  }
  if (!snapshot_offset().Empty() && follower_expect_offset <= snapshot_offset().b_offset) {
    LOG(INFO) << "Replication group: " << options_.group_id.ToString()
              << " binlog has incompleted records, need full sync";
    result = CheckResult::kSyncPointBePurged;
    return;
  }

  PikaBinlogReader reader;
  if (0 != reader.Seek(stable_logger_->Logger(),
        follower_expect_offset.filenum, follower_expect_offset.offset)) {
    LOG(WARNING) << "Replication group: " << options_.group_id.ToString()
                 << " can not construct binlog reader, need full sync";
    result = CheckResult::kSyncPointBePurged;
    return;
  }
  BinlogOffset seeked_offset;
  reader.GetReaderStatus(&(seeked_offset.filenum), &(seeked_offset.offset));
  if (seeked_offset.filenum != follower_expect_offset.filenum
      || seeked_offset.offset != follower_expect_offset.offset) {
    LOG(WARNING) << "Replication group: " << options_.group_id.ToString()
                 << " Follower offset is not a start point of cur log, Follower " << peer_id.ToString()
                 << ", cloest start point: " << seeked_offset.ToString()
                 << ", expected point: " << follower_expect_offset.ToString();
    result = CheckResult::kError;
    return;
  }
  return;
}

std::unique_ptr<LogReader> ClassicLogManager::GetLogReader(const LogOffset& start_offset) {
  auto binlog_reader = std::unique_ptr<PikaBinlogReader>(new PikaBinlogReader());
  int res = binlog_reader->Seek(stable_logger_->Logger(),
      start_offset.b_offset.filenum, start_offset.b_offset.offset);
  if (res != 0) {
    return nullptr;
  }
  return std::move(std::unique_ptr<LogReader>(
        static_cast<LogReader*>(new ClassicLogReader(std::move(binlog_reader)))));
}

LogOffset ClassicLogManager::last_offset() {
  return stable_logger_->last_offset();
}

Status ClassicLogManager::Reset(const LogOffset& snapshot_offset) {
  Status s;
  {
    stable_logger_->Lock();
    s = stable_logger_->ResetStableLog(snapshot_offset);
    if (!s.ok()) {
      stable_logger_->Unlock();

      LOG(WARNING) << "Replication group: " << options_.group_id.ToString()
                   << ", snapshot_offset " << snapshot_offset.ToString()
                   << ", reset context failed " << s.ToString();
      return s;
    }
    mem_logger_->Reset();
    set_snapshot_offset(snapshot_offset);
    stable_logger_->Unlock();
  }
  return s;
}

} // namespace replication
