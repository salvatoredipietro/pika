// Copyright (c) 2019-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/replication/cluster/pika_cluster_log_manager.h"

#include <future>

#include "include/util/hooks.h"
#include "include/storage/pika_binlog_transverter.h"

namespace replication {

using util::MPSCQueueConsumer;
using storage::PikaBinlogTransverter;

class TruncateDiskClosure : public DiskClosure {
 public:
  TruncateDiskClosure(const LogOffset& truncate_offset,
                      ClusterLogManager* log_manager)
    : truncate_offset_(truncate_offset),
    log_manager_(log_manager) { }
  ~TruncateDiskClosure() = default;

  virtual void Run() override;

 private:
  const LogOffset truncate_offset_;
  ClusterLogManager* const log_manager_;
};

void TruncateDiskClosure::Run() {
  log_manager_->TruncateStableLog(truncate_offset_);
}

class ResetDiskClosure : public DiskClosure {
 public:
  ResetDiskClosure(const LogOffset& snapshot_offset,
                   ClusterLogManager* log_manager)
    : snapshot_offset_(snapshot_offset),
    log_manager_(log_manager) {}
  ~ResetDiskClosure() = default;

  virtual void Run() override;

 private:
  const LogOffset snapshot_offset_;
  ClusterLogManager* const log_manager_;
};

void ResetDiskClosure::Run() {
  log_manager_->ResetStableLog(snapshot_offset_);
}

class LastOffsetDiskClosure : public DiskClosure {
 public:
  // take ownership of closure
  LastOffsetDiskClosure(ClusterLogManager* log_manager,
                        LastOffsetClosure* closure)
    : log_manager_(log_manager), closure_(closure) {}
  virtual ~LastOffsetDiskClosure() = default;

  virtual void Run() override;

 private:
  ClusterLogManager* const log_manager_;
  LastOffsetClosure* closure_;
};

void LastOffsetDiskClosure::Run() {
  const auto& last_offset = log_manager_->GetLastStableOffset(true);
  // TODO: Make sure no dead-lock
  if (closure_ != nullptr) {
    closure_->set_last_offset(last_offset);
    closure_->Run(); // self-deleting closure
    closure_ = nullptr;
  }
}

/* ClusterLogReader */

class ClusterLogReader : public LogReader {
 public:
  ClusterLogReader(const uint64_t start_index,
                   ClusterLogManager* log_manager);
 
  virtual Status GetLag(const LogOffset& start_offset, uint64_t* lag) override;
  virtual Status GetLog(std::string* log_item, BinlogItem::Attributes* log_attributes,
                        LogOffset* log_end_offset) override;

 private:
  Status GetLogsFromMemory(std::string* log_item, BinlogItem::Attributes* log_attributes,
                           LogOffset* log_end_offset);
  Status GetLogsFromStableStorage(std::string* log_item, BinlogItem::Attributes* log_attributes,
                                  LogOffset* log_end_offset);

 private:
  enum class FetchOrigin {
    kNone                 = 0,
    kFromMemory           = 1,
    kFromStableStorage    = 2,
  };
  FetchOrigin fetch_origin_;
  uint64_t next_index_;
  std::unique_ptr<PikaBinlogReader> binlog_reader_;
  ClusterLogManager* log_manager_;
};

ClusterLogReader::ClusterLogReader(const uint64_t start_index, ClusterLogManager* log_manager)
  : fetch_origin_(FetchOrigin::kNone), next_index_(start_index),
  log_manager_(log_manager) {}

Status ClusterLogReader::GetLag(const LogOffset& start_offset, uint64_t* lag) {
  return log_manager_->GetLag(start_offset, lag);
}

Status ClusterLogReader::GetLog(std::string* log_item, BinlogItem::Attributes* log_attributes,
                                LogOffset* log_end_offset) {
  Status s;
  switch (fetch_origin_) {
    case FetchOrigin::kNone:
    case FetchOrigin::kFromMemory: {
      s = GetLogsFromMemory(log_item, log_attributes, log_end_offset);
      if (s.ok() || !s.IsNotFound()) {
        break;
      }
    }
    case FetchOrigin::kFromStableStorage: {
      s = GetLogsFromStableStorage(log_item, log_attributes, log_end_offset);
      if (s.IsNotFound()) {
        fetch_origin_ = FetchOrigin::kFromMemory;
        binlog_reader_ = nullptr;
      } else if (s.ok()) {
        fetch_origin_ = FetchOrigin::kFromStableStorage;
      }
    }
  }
  return s;
}

Status ClusterLogReader::GetLogsFromMemory(std::string* log_item, BinlogItem::Attributes* log_attributes,
                                           LogOffset* log_end_offset) {
  std::vector<MemLog::LogItem> logs;
  Status s = log_manager_->GetLogsFromMemory(next_index_, 1, &logs);
  if (s.ok()) {
    log_item->assign(logs.front().task->log());
    *log_attributes = logs.front().attrs;
    DLOG(INFO) << "get from memory: " << next_index_;
    next_index_++;
  }
  return s;
}

Status ClusterLogReader::GetLogsFromStableStorage(std::string* log_item, BinlogItem::Attributes* log_attributes,
                                                  LogOffset* log_end_offset) {
  Status s;
  if (binlog_reader_ == nullptr) {
    binlog_reader_ = log_manager_->GetBinlogReader(next_index_, &s);
    if (!s.ok()) {
      return s;
    }
  }
  std::string log_data;
  s = binlog_reader_->Get(&log_data, &log_end_offset->b_offset.filenum,
                          &log_end_offset->b_offset.offset);
  if (!s.ok()) {
    return s;
  }
  storage::BinlogItem item;
  if (!PikaBinlogTransverter::BinlogDecode(log_data, &item)) {
    return Status::Corruption("Binlog item decode failed");
  }
  log_item->assign(item.content());
  *log_attributes = item.attributes();
  DLOG(INFO) << "get from stable storage: " << next_index_;
  next_index_++;
  return s;
}

/* ClusterLogManager */

ClusterLogManager::ClusterLogManager(const ClusterLogManagerOptions& options)
  : LogManager(options),
  disk_write_queue_(new MPSCQueue()),
  executor_(options.executor),
  max_consumed_number_once_(options.max_consumed_number_once),
  first_offset_(LogOffset()),
  last_offset_(LogOffset()),
  current_term_(0),
  next_index_(0) { }

ClusterLogManager::~ClusterLogManager() {
  Close();
  if (disk_write_queue_ != nullptr) {
    delete disk_write_queue_;
    disk_write_queue_ = nullptr;
  }
}

Status ClusterLogManager::Initialize() {
  Status s = LogManager::Initialize();
  if (!s.ok()) {
    return s;
  }
  // 1. initialize the first_offset_ and last_offset_ from stable storage.
  first_offset_ = stable_logger_->first_offset();
  last_offset_ = stable_logger_->last_offset();
  // 2. initialize the current_term_ and next_index_ from stable storage.
  uint32_t filenum = 0, term = 0;
  uint64_t offset = 0, index = 0;
  s = stable_logger_->Logger()->GetProducerStatus(&filenum, &offset, &term, &index);
  if (!s.ok()) {
    LOG(ERROR) << "Replication group: " << options_.group_id.ToString()
               << " GetProducerStatus failed";
    return s;
  }
  current_term_ = term;
  next_index_ = index + 1;
  return s;
}

class StopMarker : public DiskClosure {
 public:
  StopMarker(std::promise<bool> stop_promise)
    : stop_promise_(std::move(stop_promise)) {}

  virtual void Run() {
    stop_promise_.set_value(true);
  }

 private:
  std::promise<bool> stop_promise_;
};

void ClusterLogManager::StopDiskWrites() {
  // Wait all pending disk writes to be completed
  std::promise<bool> promise;
  std::future<bool> future = promise.get_future();
  auto stop_marker = static_cast<DiskClosure*>(new StopMarker(std::move(promise)));
  if (disk_write_queue_->Enqueue(stop_marker)) {
    StartDiskWrite(stop_marker);
  }
  future.wait();
  LOG(INFO) << "Replication group: " << options_.group_id.ToString()
            << ", all disk writes should have been completed";
}

void ClusterLogManager::Close() {
  // complete all pending disk writes
  StopDiskWrites();
  LogManager::Close();
}

uint32_t ClusterLogManager::CurrentTerm() {
  return stable_logger_->Logger()->term();
}

void ClusterLogManager::UpdateTerm(uint32_t term) {
  std::unique_lock<std::mutex> lk(mutex_);
  stable_logger_->Logger()->SetTerm(term);
  current_term_ = term;
}

std::unique_ptr<LogReader> ClusterLogManager::GetLogReader(const LogOffset& start_offset) {
  std::lock_guard<std::mutex> l(mutex_);
  LogOffset actual_start_offset = UnsafeGetLogOffset(start_offset.b_offset, start_offset.l_offset.index);
  if (actual_start_offset.l_offset.term == 0) {
    return nullptr;
  }
  return std::move(std::unique_ptr<LogReader>(
        static_cast<LogReader*>(new ClusterLogReader(start_offset.l_offset.index, this))));
}

std::unique_ptr<PikaBinlogReader> ClusterLogManager::GetBinlogReader(const uint64_t index, Status* s) {
  return std::move(stable_logger_->GetPikaBinlogReader(index, s));
}

uint64_t ClusterLogManager::HandleDiskOperations(void* arg, MPSCQueue::Iterator& iterator) {
  uint64_t iterated_num = 0;
  ClusterLogManager* log_manager = static_cast<ClusterLogManager*>(arg);
  for (; iterator.Valid(); iterator++) {
    DiskClosure* closure = static_cast<DiskClosure*>(iterator.Current());
    size_t entries_size = closure->entries_size();
    if (entries_size != 0) {
      // Write to stable storage
      Status s;
      for (size_t index = 0; index < entries_size; index++) {
        auto& entry = closure->GetEntry(index);
        // Fetch the filenum and offset
        s = log_manager->GetProducerStatus(&entry.data_attrs.filenum, &entry.data_attrs.offset);
        if (!s.ok()) {
          closure->set_status(s);
          log_manager->set_status(s);
          break;
        }
        s = log_manager->AppendLogToStableStorage(entry.data_attrs, entry.data_slice,
                                                  nullptr, false/*not encoded*/);
        DLOG(INFO) << "AppendLogToStableStorage, term: "  << entry.data_attrs.term_id
                   << ", index: " << entry.data_attrs.logic_id;
        if (!s.ok()) {
          closure->set_status(s);
          log_manager->set_status(s);
          break;
        }
      }
    }
    closure->Run();
    iterated_num++;
  }
  // After persist the logs into stable storage,
  // now it's safe to clean the related memory.
  log_manager->TryToClearMemLogs();
  return iterated_num;
}

void ClusterLogManager::TryToClearMemLogs() {
  mem_logger_->UpdatePersistedLogsByIndex(stable_logger_->last_offset().l_offset.index);
}

Status ClusterLogManager::AppendLog(const std::shared_ptr<ReplTask>& task,
                                    DiskClosure* closure, bool async) {
  Status s;
  std::unique_lock<std::mutex> lk(mutex_);
  if (async) {
    s = UnsafeAppendLogAsync(task, closure);
  } else {
    s = UnsafeAppendLogSync(task, closure);
  }
  return s;
}

Status ClusterLogManager::UnsafeAppendLogSync(const std::shared_ptr<ReplTask>& task, DiskClosure* closure) {
  uint32_t filenum = 0, term = 0;
  uint64_t offset = 0, index = 0;
  Status s = stable_logger_->Logger()->GetProducerStatus(&filenum, &offset, &term, &index);
  if (!s.ok()) {
    if (closure != nullptr) {
      closure->set_status(s);
    }
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

  mem_logger_->AppendLog(MemLog::LogItem(attrs, task));
  last_offset_ = LogOffset(BinlogOffset(attrs.filenum, attrs.offset),
                           LogicOffset(attrs.term_id, attrs.logic_id));

  // Append to stable_log_ synchronously.
  LogOffset end_offset;
  s = AppendLogToStableStorage(attrs, task->LogSlice(), &end_offset, false/*data_not_encoded*/);
  if (closure != nullptr) {
    closure->PushEntry(DiskClosure::Entry(attrs, task->LogSlice()));
    closure->set_status(s);
  }

  next_index_ = attrs.logic_id + 1;

  DLOG(INFO) << "Replication group: " << options_.group_id.ToString()
             << ", append log, term:  " <<attrs.term_id << ", index " <<attrs.logic_id
             << ", next_index " << next_index_
             << ", start_offset " << attrs.filenum << " " << attrs.offset
             << ", end_offset " << end_offset.b_offset.ToString();
  TryToClearMemLogs();
  return s;
}

Status ClusterLogManager::UnsafeAppendLogAsync(const std::shared_ptr<ReplTask>& task, DiskClosure* closure) {
  if (closure == nullptr) {
    return Status::Corruption("closure can not be nullptr when async is true");
  }
  // For a async write, we cannot decide the start offset of the task, as there may be some
  // flying write operations.
  auto type = task->log_format() == ReplTask::LogFormat::kRedis
              ? BinlogItem::Attributes::ContentType::TypeFirst
              : BinlogItem::Attributes::ContentType::TypeSecond;
  BinlogItem::Attributes attrs;
  attrs.content_type = type;
  attrs.exec_time = time(nullptr);
  attrs.term_id = current_term_;
  attrs.logic_id = next_index_++;

  mem_logger_->AppendLog(MemLog::LogItem(attrs, task));
  last_offset_ = LogOffset(BinlogOffset(), LogicOffset(attrs.term_id, attrs.logic_id));

  // Append to stable_log_ asynchronously.
  closure->PushEntry(DiskClosure::Entry(attrs, task->LogSlice()));
  if (disk_write_queue_->Enqueue(closure)) {
    StartDiskWrite(closure);
  }
  return Status::OK();
}

Status ClusterLogManager::AppendReplicaLog(const PeerID& peer_id,
                                           std::vector<LogEntry>& entries,
                                           DiskClosure* closure) {
  if (closure == nullptr) {
    return Status::Corruption("DiskClosure must not be nullptr");
  }

  std::lock_guard<std::mutex> l(mutex_);
  Status s = HandleConflicts(closure->prev_offset(), entries);
  if (!s.ok()) {
    closure->set_status(s);
    return s;
  }

  // 1. First append to memory log
  const auto& entries_size = entries.size();
  for (size_t index = 0; index < entries_size; index++) {
    auto& entry = entries[index];
    const auto& attrs = entry.attributes();
    auto format = attrs.content_type == BinlogItem::Attributes::ContentType::TypeFirst
                ? ReplTask::LogFormat::kRedis : ReplTask::LogFormat::kPB;
    auto repl_task = std::make_shared<ReplTask>(std::move(entry.ReleaseData()), nullptr,
                                                options_.group_id, peer_id,
                                                ReplTask::Type::kReplicaType, format);
    mem_logger_->AppendLog(MemLog::LogItem(attrs, repl_task));
    closure->PushEntry(DiskClosure::Entry(attrs, repl_task->LogSlice()));
  }

  // 2. Append to stable_log_ asynchronously.
  if (disk_write_queue_->Enqueue(closure)) {
    StartDiskWrite(closure);
  }
  return Status::OK();
}

Status ClusterLogManager::HandleConflicts(const LogOffset& prev_offset,
                                          std::vector<LogEntry>& entries) {
  // 1. Check the prev_log
  LOG(INFO) << "Replication group: " << options_.group_id.ToString()
            << " prev_log " << prev_offset.ToString()
            << " last_offset " << last_offset_.ToString();

  size_t entries_size = entries.size();
  if (prev_offset.l_offset == last_offset_.l_offset) {
    if (entries_size != 0) {
      last_offset_ = entries.back().GetLogOffset();
      next_index_ = last_offset_.l_offset.index + 1;
    }
    return Status::OK();
  }
  Status s = LogIsMatched(prev_offset);
  if (!s.ok()) {
    LOG(WARNING) << "Replication group: " << options_.group_id.ToString()
                 << " can not found a equal prev_log " << prev_offset.ToString()
                 << " in local, error info: " << s.ToString();
    return s;
  }

  if (entries_size == 0) {
    return Status::OK();
  }

  // Find the conflicts
  LogOffset conflict_offset;
  size_t index = 0;
  for (; index < entries_size; index++) {
    auto& entry = entries[index];
    const LogOffset& entry_offset = entry.GetLogOffset();
    s = LogIsMatched(entry_offset);
    if (!s.ok()) {
      LOG(WARNING) << "Replication group: " << options_.group_id.ToString()
                   << " find a conflict entry in log"
                   << ", conflict entry offset " << entry_offset.ToString()
                   << ", error info: " << s.ToString();
      conflict_offset = entry_offset;
      break;
    }
  }
  if (!conflict_offset.Empty()) {
    // Truncate the existed logs after the conflict_offset.
    LogOffset truncate_offset_in_remote = index == 0 ? prev_offset : entries[index - 1].GetLogOffset();
    UnsafeTruncateTo(truncate_offset_in_remote);
    last_offset_ = entries.back().GetLogOffset();
    next_index_ = last_offset_.l_offset.index + 1;
  }
  entries.erase(entries.begin(), entries.begin() + index);
  return Status::OK();
}

Status ClusterLogManager::LogIsMatched(const LogOffset& log_offset) {
  if (log_offset.l_offset.Empty()) {
    // It's the first log
    return Status::OK();
  }
  const auto& local_offset = UnsafeGetLogOffset(log_offset.b_offset,
                                                log_offset.l_offset.index);
  if (local_offset.l_offset != log_offset.l_offset) {
    return Status::Corruption("log in local term does not matched");
  }
  return Status::OK();
}

LogOffset ClusterLogManager::GetLogOffset(const BinlogOffset& hint_offset,
                                          const uint64_t target_index) {
  std::lock_guard<std::mutex> l(mutex_);
  return UnsafeGetLogOffset(hint_offset, target_index);
}

LogOffset ClusterLogManager::UnsafeGetLogOffset(const BinlogOffset& hint_offset,
                                                const uint64_t target_index) {
  // 1. Check if the log has been incorporated into the snapshot.
  const auto& s_offset = snapshot_offset();
  if (target_index < s_offset.l_offset.index) {
    return LogOffset();
  } else if (target_index == s_offset.l_offset.index) {
    return s_offset;
  }
  // 2. Check if the log is in range [first_offset_, last_offset_]
  if (target_index < first_offset_.l_offset.index
      || target_index > last_offset_.l_offset.index) {
    return LogOffset();
  }
  LogOffset found_offset;
  // 3.1 Get log from the memory at first as they may have not been persisted.
  if (mem_logger_->FindLogItemByIndex(target_index, &found_offset)) {
    return found_offset;
  }
  // 3.2 Get log from the stable storage
  Status s = stable_logger_->FindLogicOffset(hint_offset, target_index, &found_offset);
  if (!s.ok()) {
    LOG(WARNING) << "Replication group: " << options_.group_id.ToString()
                 << " FindLogicOffset failed " << s.ToString();
    return LogOffset();
  }
  return found_offset;
}

Status ClusterLogManager::GetLogsFromMemory(const uint64_t index, size_t count,
                                            std::vector<MemLog::LogItem>* logs) {
  //std::unique_lock<std::mutex> lk(mutex_);
  //if (index < first_offset_.l_offset.index) {
  //  return Status::Corruption("Smaller than the first index");
  //}
  //if (index > last_offset_.l_offset.index) {
  //  return Status::EndFile("Larger than the last index");
  //}
  MemLog::ReadOptions read_options;
  read_options.count = count;
  read_options.direction = MemLog::ReadOptions::Direction::kBack;
  return mem_logger_->CopyLogsByIndex(index, read_options, logs);
}

void ClusterLogManager::UnsafeTruncateTo(const LogOffset& truncate_offset_in_remote) {
  // 1. First truncate logs in memory, logs before committed_offset should not be truncated.
  mem_logger_->TruncateToByIndex(truncate_offset_in_remote.l_offset.index);

  // reset next_index_
  next_index_ = truncate_offset_in_remote.l_offset.index + 1;

  // 2. Second truncate logs in stable storage.
  TruncateDiskClosure* truncate_closure = new TruncateDiskClosure(truncate_offset_in_remote, this);
  if (disk_write_queue_->Enqueue(truncate_closure)) {
    StartDiskWrite(truncate_closure);
  }
}

void ClusterLogManager::TruncateStableLog(const LogOffset& truncate_offset) {
  stable_logger_->Lock();
  stable_logger_->TruncateTo(truncate_offset);
  stable_logger_->Unlock();
}

Status ClusterLogManager::Reset(const LogOffset& snapshot_offset) {
  std::lock_guard<std::mutex> l(mutex_);
  // 1. Reset the logs in memory.
  mem_logger_->Reset();
  set_snapshot_offset(snapshot_offset);
  //{
  //  slash::RWLock l(&state_mutex_, true);
  //  snapshot_offset_ = snapshot_offset;
  //  committed_offset_ = snapshot_offset;
  //}
  first_offset_ = LogOffset();
  last_offset_ = LogOffset();
  next_index_ =  snapshot_offset.l_offset.index + 1;
  // 2. Reset the logs in stable storage
  ResetDiskClosure* reset_closure = new ResetDiskClosure(snapshot_offset, this);
  if (disk_write_queue_->Enqueue(reset_closure)) {
    StartDiskWrite(reset_closure);
  }
  return Status::OK();
}

Status ClusterLogManager::GetLag(const LogOffset& start_offset, uint64_t* lag) {
  {
    std::lock_guard<std::mutex> l(mutex_);
    *lag = (last_offset_.b_offset.filenum - start_offset.b_offset.filenum)
            * options_.stable_log_options.max_binlog_size
            + (last_offset_.b_offset.offset - start_offset.b_offset.offset);
  }
  return Status::OK();
}

const LogOffset& ClusterLogManager::GetLastPendingOffset() {
  std::lock_guard<std::mutex> l(mutex_);
  return last_offset_;
}

LogOffset ClusterLogManager::GetLastStableOffset(bool in_disk_thread, LastOffsetClosure* closure) {
  // All operations on the log are prioritised in memory before
  // being written to stable storage asynchronously.
  //
  // There may be some pending disk writes, we should get from the disk thread.
  if (in_disk_thread) {
    // We already in the disk thread, just get it right now.
    return stable_logger_->last_offset();
  }

  // NOTE: the closure will be invoked when we get the last_offset in disk thread.
  std::lock_guard<std::mutex> l(mutex_);
  LastOffsetDiskClosure* last_offset_closure = new LastOffsetDiskClosure(this, closure);
  if (disk_write_queue_->Enqueue(last_offset_closure)) {
    StartDiskWrite(last_offset_closure);
  }
  return LogOffset();
}

class DiskWriteHandler : public util::thread::Task {
 public:
  DiskWriteHandler(std::unique_ptr<MPSCQueueConsumer> consumer,
                   TaskExecutor* executor)
    : executor_(executor), consumer_(std::move(consumer)) {}

  virtual void Run() override {
    bool continuation = consumer_->Consume();
    if (continuation) {
      // Give up execution privileges and wait for the next
      // dispatch to avoid blocking other tasks for a long time.
      executor_->ScheduleCompetitiveTask(shared_from_this());
      return;
    }
  }

 private:
  TaskExecutor* const executor_;
  std::unique_ptr<MPSCQueueConsumer> consumer_;
};

void ClusterLogManager::StartDiskWrite(DiskClosure* head) {
  auto consumer = util::make_unique<MPSCQueueConsumer>(
      &ClusterLogManager::HandleDiskOperations, this,
      disk_write_queue_, static_cast<MPSCNode*>(head), max_consumed_number_once_);
  auto handler = std::make_shared<DiskWriteHandler>(std::move(consumer), executor_);
  executor_->ScheduleCompetitiveTask(std::move(handler));
}

void ClusterLogManager::ResetStableLog(const LogOffset& snapshot_offset) {
  stable_logger_->Lock();
  stable_logger_->ResetStableLog(snapshot_offset);
  stable_logger_->Unlock();
}

} // namespace replication
