// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/replication/pika_stable_log.h"

#include <sys/stat.h>
#include <glog/logging.h>

#include "slash/include/env.h"

#include "include/util/callbacks.h"
#include "include/storage/pika_binlog_reader.h"
#include "include/storage/pika_binlog_transverter.h"

namespace replication {

using storage::BinlogItem;
using storage::PikaBinlogReader;
using storage::PikaBinlogTransverter;

StableLog::StableLog(const StableLogOptions& options)
  : options_(options), start_filenum_(0), reader_buffer_(nullptr),
  started_(false) {
  if (options.load_index_into_memory) {
    reader_buffer_ = new char[storage::kBlockSize];
  }
  stable_logger_ = std::make_shared<BinlogBuilder>(options_.log_path, options.max_binlog_size);
  pthread_rwlock_init(&offset_rwlock_, NULL);
  pthread_rwlock_init(&indexer_rwlock_, NULL);
  pthread_rwlock_init(&files_rwlock_, NULL);
}

StableLog::~StableLog() {
  Close();
  if (reader_buffer_ != nullptr) {
    delete[] reader_buffer_;
    reader_buffer_ = nullptr;
  }
  pthread_rwlock_destroy(&offset_rwlock_);
  pthread_rwlock_destroy(&indexer_rwlock_);
  pthread_rwlock_destroy(&files_rwlock_);
}

void StableLog::Close() {
  if (!started_.load(std::memory_order_acquire)) {
    return;
  }
  slash::RWLock l(&files_rwlock_, true);
  if (!started_.load(std::memory_order_acquire)) {
    return;
  }
  UnsafeClose();
  started_.store(false, std::memory_order_release);
}

void StableLog::UnsafeClose() {
  UnsafeClean();
  stable_logger_->Close();
}

Status StableLog::Initialize() {
  if (started_.load(std::memory_order_acquire)) {
    return Status::Busy("stable log has been initialized");
  }

  // NOTE: use exclusive lock to ensure concurrency security
  //       (synchronize with Close method).
  slash::RWLock l(&files_rwlock_, true);
  Status s = stable_logger_->Initialize();
  if (!s.ok()) {
    return Status::Busy("Initialize stable log failed");
  }
  std::map<uint32_t, std::string> binlogs;
  if (!UnsafeGetBinlogFiles(&binlogs)) {
    LOG(ERROR) << options_.log_path << " Could not get the binlog files!";
    return Status::Corruption("Could not get the binlog files");
  }
  if (binlogs.empty()) {
    return Status::OK();
  }
  start_filenum_ = binlogs.begin()->first;
  if (options_.load_index_into_memory) {
    slash::RWLock lk(&indexer_rwlock_, true);
    const std::string& base_filename = stable_logger_->filename();
    for (const auto& binlog : binlogs) {
      auto reader = std::unique_ptr<BinlogReader>(
                    new BinlogReader(base_filename, binlog.first, reader_buffer_));
      auto indexer = std::unique_ptr<BinlogIndexer>(new BinlogIndexer());
      auto s = indexer->Initialize(std::move(reader));
      if (!s.ok()) {
        LOG(ERROR) << options_.log_path << " Could not initialize the indexer!";
        return Status::Corruption("Could not initialize the indexer");
      }
      indexer_map_.emplace(binlog.first, std::move(indexer));
    }
    {
      slash::RWLock l(&offset_rwlock_, true);
      BinlogIndexer::Item item;
      indexer_map_.begin()->second->GetFirstItem(&item);
      first_offset_.b_offset.filenum = indexer_map_.begin()->first;
      first_offset_.b_offset.offset = item.offset_pair.end_offset;
      first_offset_.l_offset.term = item.term;
      first_offset_.l_offset.index = item.index;

      indexer_map_.rbegin()->second->GetLastItem(&item);
      last_offset_.b_offset.filenum = indexer_map_.rbegin()->first;
      last_offset_.b_offset.offset = item.offset_pair.end_offset;
      last_offset_.l_offset.term = item.term;
      last_offset_.l_offset.index = item.index;
    }
  } else {
    UnsafeUpdateFirstOffset(binlogs.begin()->first);
    UnsafeUpdateLastOffset(binlogs.rbegin()->first);
  }
  started_.store(true, std::memory_order_release);
  return Status::OK();
}

Status StableLog::FindLogicOffset(const BinlogOffset& start_hint_offset, uint64_t target_index,
                                  LogOffset* offset, bool return_end_offset) {
  if (!started_.load(std::memory_order_acquire)) {
    return Status::Corruption("Stable log has not been initialized");
  }
  slash::RWLock l(&files_rwlock_, false);
  return UnsafeFindLogicOffset(start_hint_offset, target_index, offset, return_end_offset);
}

Status StableLog::UnsafeFindLogicOffset(const BinlogOffset& start_hint_offset, uint64_t target_index,
                                        LogOffset* offset, bool return_end_offset) {
  Status s = Status::NotFound("can not locate the log with index " + std::to_string(target_index));
  if (options_.load_index_into_memory) {
    slash::RWLock lk(&indexer_rwlock_, false);
    BinlogIndexer::OffsetPair offset_pair;
    // step indexer_map_ to locate the end_offset of the target_index
    for (const auto& iter : indexer_map_) {
      BinlogIndexer::Item item;
      s = iter.second->GetItem(target_index, &item);
      if (s.ok()) {
        offset->l_offset.term = item.term;
        offset->l_offset.index = item.index;
        offset->b_offset.filenum = iter.first;
        offset->b_offset.offset = return_end_offset ? item.offset_pair.end_offset
                                  : item.offset_pair.start_offset;
        break;
      }
    }
  } else {
    s = UnsafeFindLogicOffsetFromStorage(start_hint_offset, target_index, offset, return_end_offset);
  }
  return s;
}

Status StableLog::UnsafeFindLogicOffsetFromStorage(const BinlogOffset& start_offset,
                                                   uint64_t target_index,
                                                   LogOffset* found_offset,
                                                   bool return_end_offset) {
  LogOffset possible_offset;
  Status s = UnsafeGetBinlogOffset(start_offset, &possible_offset, return_end_offset);
  if (!s.ok() || possible_offset.l_offset.index != target_index) {
    if (!s.ok()) {
      LOG(INFO) << "Replication group: " << options_.group_id.ToString()
                << " GetBinlogOffset res: " << s.ToString();
    } else {
      LOG(INFO) << "Replication group: " << options_.group_id.ToString()
                << " GetBinlogOffset res: " << s.ToString()
                << " possible_offset " << possible_offset.ToString()
                << " target_index " << target_index;
    }
    return UnsafeFindLogicOffsetBySearchingBinlog(start_offset, target_index,
                                                  found_offset, return_end_offset);
  }
  *found_offset = possible_offset;
  return Status::OK();
}

Status StableLog::UnsafeFindLogicOffsetBySearchingBinlog(const BinlogOffset& hint_offset,
                                                         uint64_t target_index,
                                                         LogOffset* found_offset,
                                                         bool return_end_offset) {
  LOG(INFO) << "Replication group: " << options_.group_id.ToString()
            << " FindLogicOffsetBySearchingBinlog hint offset " << hint_offset.ToString()
            << " target_index " << target_index;
  BinlogOffset start_offset;
  std::map<uint32_t, std::string> binlogs;
  if (!UnsafeGetBinlogFiles(&binlogs)) {
    return Status::Corruption("Get binlog files failed");
  }
  if (binlogs.empty()) {
    return Status::NotFound("Binlogs is empty");
  }
  if (binlogs.find(hint_offset.filenum) == binlogs.end()) {
    start_offset = BinlogOffset(binlogs.crbegin()->first, 0);
  } else {
    start_offset = hint_offset;
  }

  uint32_t found_filenum;
  Status s = UnsafeFindBinlogFileNum(binlogs, target_index, start_offset.filenum, &found_filenum);
  if (!s.ok()) {
    return s;
  }

  LOG(INFO) << options_.group_id.ToString() << "FindBinlogFilenum res " << found_filenum;
  BinlogOffset traversal_start(found_filenum, 0);
  BinlogOffset traversal_end(found_filenum + 1, 0);
  std::vector<LogOffset> offsets;
  s = UnsafeGetBinlogOffset(traversal_start, traversal_end, &offsets, return_end_offset);
  if (!s.ok()) {
    return s;
  }
  for (auto& offset : offsets) {
    if (offset.l_offset.index == target_index) {
      LOG(INFO) << options_.group_id.ToString() << "Found " << target_index << " " << offset.ToString();
      *found_offset = offset;
      return Status::OK();
    }
  }
  return Status::NotFound("Logic index not found");
}

Status StableLog::UnsafeFindBinlogFileNum(const std::map<uint32_t, std::string> binlogs,
                                          uint64_t target_index,
                                          uint32_t start_filenum,
                                          uint32_t* founded_filenum) {
  // low boundary & high boundary
  uint32_t lb_binlogs = binlogs.begin()->first;
  uint32_t hb_binlogs = binlogs.rbegin()->first;
  bool first_time_left = false;
  bool first_time_right = false;
  uint32_t filenum = start_filenum;
  while (1) {
    LogOffset first_offset;
    Status s = UnsafeGetBinlogOffset(BinlogOffset(filenum, 0), &first_offset);
    if (!s.ok()) {
      return s;
    }
    if (target_index < first_offset.l_offset.index) {
      if (first_time_right) {
        // last filenum
        filenum = filenum -1;
        break;
      }
      // move left
      first_time_left = true;
      if (filenum == 0 || filenum  - 1 < lb_binlogs) {
        return Status::NotFound(std::to_string(target_index) + " hit low boundary");
      }
      filenum = filenum - 1;
    } else if (target_index > first_offset.l_offset.index) {
      if (first_time_left) {
        break;
      }
      // move right
      first_time_right = true;
      if (filenum + 1 > hb_binlogs) {
        break;
      }
      filenum = filenum + 1;
    } else {
      break;
    }
  }
  *founded_filenum = filenum;
  return Status::OK();
}

Status StableLog::UnsafeGetBinlogOffset(const BinlogOffset& start_offset,
                                        const BinlogOffset& end_offset,
                                        std::vector<LogOffset>* log_offset,
                                        bool return_end_offset) {
  BinlogOffset possible_start_offset = start_offset;
  PikaBinlogReader binlog_reader;
  int res = binlog_reader.Seek(stable_logger_, start_offset.filenum, start_offset.offset);
  if (res) {
    return Status::Corruption("Binlog reader init failed");
  }
  while (1) {
    BinlogOffset b_offset;
    std::string binlog;
    Status s = binlog_reader.Get(&binlog, &(b_offset.filenum), &(b_offset.offset));
    if (s.IsEndFile()) {
      return Status::OK();
    } else if (s.IsIncomplete()) {
      return Status::Incomplete("Read a BadRecord");
    } else if (s.IsCorruption() || s.IsIOError()) {
      return Status::Corruption("Read Binlog error");
    }
    BinlogItem::Attributes attrs;
    if (!PikaBinlogTransverter::BinlogAttributesDecode(binlog, &attrs)) {
      return Status::Corruption("Binlog item decode failed");
    }
    LogOffset offset;
    if (return_end_offset) {
      offset.b_offset = b_offset;
    } else {
      // NOTE: The peisisted BinlogOffset information (filenum and offset) is the end point of
      //       the previous item, and its true starting point may be different from it. It is
      //       even possible that the filenums are not the same (roll to a new file), so a false
      //       negative error may be rasied when the file corresponding to the recorded filenum
      //       is purged. In order to handle this case, we should adjust the start_offset.
      if (possible_start_offset.filenum != b_offset.filenum) {
        possible_start_offset.filenum = b_offset.filenum;
        possible_start_offset.offset = 0;
      }
      offset.b_offset = possible_start_offset;
      possible_start_offset = b_offset;
    }
    offset.l_offset.term = attrs.term_id;
    offset.l_offset.index = attrs.logic_id;
    DLOG(INFO) << "find offset " << offset.ToString();
    if (b_offset > end_offset) {
      return Status::OK();
    }
    log_offset->push_back(offset);
  }
  return Status::OK();
}

Status StableLog::UnsafeGetBinlogOffset(const BinlogOffset& start_offset,
                                        LogOffset* log_offset,
                                        bool return_end_offset) {
  PikaBinlogReader binlog_reader;
  int res = binlog_reader.Seek(stable_logger_, start_offset.filenum, start_offset.offset);
  if (res) {
    return Status::Corruption("Binlog reader init failed");
  }
  BinlogOffset b_offset;
  std::string binlog;
  Status s = binlog_reader.Get(&binlog, &(b_offset.filenum), &(b_offset.offset));
  if (s.IsEndFile()) {
    return Status::OK();
  } else if (s.IsIncomplete()) {
    return Status::Incomplete("Read a BadRecord");
  } else if (s.IsCorruption() || s.IsIOError()) {
    return Status::Corruption("Read Binlog error");
  }
  BinlogItem::Attributes attrs;
  if (!PikaBinlogTransverter::BinlogAttributesDecode(binlog, &attrs)) {
    return Status::Corruption("Binlog item decode failed");
  }
  if (return_end_offset) {
    log_offset->b_offset = b_offset;
  } else {
    // for the same reason as above
    log_offset->b_offset = start_offset;
  }
  log_offset->l_offset.term = attrs.term_id;
  log_offset->l_offset.index = attrs.logic_id;
  DLOG(INFO) << "find offset " << log_offset->ToString();
  return Status::OK();
}

Status StableLog::GetLastLogFromStorage(LogOffset* log_offset) {
  if (!started_.load(std::memory_order_acquire)) {
    return Status::Corruption("Stable log has not been initialized");
  }
  slash::RWLock lk(&files_rwlock_, false);
  Status s = stable_logger_->GetProducerStatus(&log_offset->b_offset.filenum,
                                               &log_offset->b_offset.offset,
                                               &log_offset->l_offset.term,
                                               &log_offset->l_offset.index);
  if (!s.ok()) {
    return s;
  }
  std::vector<LogOffset> hints;
  s = UnsafeGetLogsBefore(log_offset->b_offset, &hints);
  if (!s.ok()) {
    return s;
  }
  if (hints.size() <= 0) {
    return Status::NotFound("Empty log");
  }
  *log_offset = hints[hints.size() - 1];
  return Status::OK();
}

Status StableLog::UnsafeGetLogsBefore(const BinlogOffset& start_offset,
                                      std::vector<LogOffset>* hints,
                                      bool return_end_offset) {
  BinlogOffset traversal_end = start_offset;
  BinlogOffset traversal_start(traversal_end.filenum, 0);
  traversal_start.filenum = traversal_start.filenum == 0
                            ? 0 : traversal_start.filenum - 1;
  std::map<uint32_t, std::string> binlogs;
  if (!UnsafeGetBinlogFiles(&binlogs)) {
    return Status::Corruption("Get binlog files failed");
  }
  if (binlogs.find(traversal_start.filenum) == binlogs.end()) {
    traversal_start.filenum = traversal_end.filenum;
  }
  std::vector<LogOffset> res;
  Status s = UnsafeGetBinlogOffset(traversal_start, traversal_end, &res, return_end_offset);
  if (!s.ok()) {
    return s;
  }
  if (res.size() > 100) {
    res.assign(res.end() - 100, res.end());
  }
  *hints = res;
  return Status::OK();
}

void StableLog::RemoveStableLogDir(std::string& log_dir) {
  // NOTE: we will close the stable log as we will discard the underlying resources,
  //       so it does not matter whether it has been initialized or not.
  slash::RWLock l(&files_rwlock_, true);
  UnsafeClose();
  std::string logpath = options_.log_path;
  if (logpath[logpath.length() - 1] == '/') {
    logpath.erase(logpath.length() - 1);
  }
  logpath.append("_deleting/");
  if (slash::RenameFile(options_.log_path, logpath.c_str())) {
    LOG(WARNING) << "Failed to move log to trash, error: " << strerror(errno);
    return;
  }
  log_dir = logpath;

  LOG(WARNING) << "Partition StableLog: " << options_.group_id.ToString()
               << " move to trash success";
}

bool StableLog::UnsafeGetBinlogFiles(std::map<uint32_t, std::string>* binlogs) {
  std::vector<std::string> children;
  int ret = slash::GetChildren(options_.log_path, children);
  if (ret != 0) {
    LOG(WARNING) << options_.log_path << " Get all files in log path failed! error:" << ret;
    return false;
  }

  int64_t index = 0;
  std::string sindex;
  std::vector<std::string>::iterator it;
  for (it = children.begin(); it != children.end(); ++it) {
    if ((*it).compare(0, kBinlogPrefixLen, kBinlogPrefix) != 0) {
      continue;
    }
    sindex = (*it).substr(kBinlogPrefixLen);
    if (slash::string2l(sindex.c_str(), sindex.size(), &index) == 1) {
      binlogs->insert(std::pair<uint32_t, std::string>(static_cast<uint32_t>(index), options_.log_path + *it));
    }
  }
  return true;
}

bool StableLog::UnsafeReloadFirstOffset() {
  std::map<uint32_t, std::string> binlogs;
  if (!UnsafeGetBinlogFiles(&binlogs)) {
    LOG(WARNING) << options_.log_path << " Could not get binlog files!";
    return false;
  }
  auto it = binlogs.begin();
  if (it != binlogs.end()) {
    UnsafeUpdateFirstOffset(it->first);
  } else {
    slash::RWLock l(&offset_rwlock_, true);
    first_offset_.Clear();
    last_offset_.Clear();
  }
  return true;
}

void StableLog::UnsafeUpdateFirstOffset(uint32_t filenum) {
  PikaBinlogReader binlog_reader;
  int res = binlog_reader.Seek(stable_logger_, filenum, 0);
  if (res) {
    LOG(WARNING) << "Binlog reader init failed";
    return;
  }

  BinlogItem::Attributes attrs;
  BinlogOffset offset;
  while (1) {
    std::string binlog;
    Status s = binlog_reader.Get(&binlog, &(offset.filenum), &(offset.offset));
    if (s.IsEndFile()) {
      return;
    }
    if (s.IsIncomplete()) {
      LOG(WARNING) << "Binlog read a BadRecord may be padded in DBSync stage";
      return;
    }
    if (!s.ok()) {
      LOG(WARNING) << "Binlog reader get failed";
      return;
    }
    if (!PikaBinlogTransverter::BinlogAttributesDecode(binlog, &attrs)) {
      LOG(WARNING) << "Binlog item decode failed";
      return;
    }
    // exec_time == 0, could be padding binlog
    if (attrs.exec_time != 0) {
      break;
    }
  }

  slash::RWLock l(&offset_rwlock_, true);
  first_offset_.b_offset = offset;
  first_offset_.l_offset.term = attrs.term_id;
  first_offset_.l_offset.index = attrs.logic_id;
  LOG(INFO) << "Reload first_offset: " << first_offset_.ToString();
}

void StableLog::UnsafeUpdateLastOffset(uint32_t filenum) {
  PikaBinlogReader binlog_reader;
  int res = binlog_reader.Seek(stable_logger_, filenum, 0);
  if (res) {
    LOG(WARNING) << "Binlog reader init failed";
    return;
  }

  BinlogItem::Attributes attrs;
  BinlogOffset offset;
  while (1) {
    std::string binlog;
    Status s = binlog_reader.Get(&binlog, &(offset.filenum), &(offset.offset));
    if (s.IsEndFile()) {
      break;
    }
    if (s.IsIncomplete()) {
      LOG(WARNING) << "Binlog read a BadRecord may be padded in DBSync stage";
      return;
    }
    if (!s.ok()) {
      LOG(WARNING) << "Binlog reader get failed";
      return;
    }
    if (!PikaBinlogTransverter::BinlogAttributesDecode(binlog, &attrs)) {
      LOG(WARNING) << "Binlog item decode failed";
      return;
    }
  }
  // exec_time == 0, could be padding binlog
  if (attrs.exec_time != 0) {
    slash::RWLock l(&offset_rwlock_, true);
    last_offset_.b_offset = offset;
    last_offset_.l_offset.term = attrs.term_id;
    last_offset_.l_offset.index = attrs.logic_id;
  }
}

Status StableLog::UnsafePurgeFileAfter(uint32_t filenum) {
  std::map<uint32_t, std::string> binlogs;
  bool res = UnsafeGetBinlogFiles(&binlogs);
  if (!res) {
    return Status::Corruption("GetBinlogFiles failed");
  }
  for (auto& it : binlogs) {
    if (it.first > filenum) {
      // Do delete
      Status s = slash::DeleteFile(it.second);
      if (!s.ok()) {
        return s;
      }
      LOG(WARNING) << "Delete file " << it.second;
    }
  }
  return Status::OK();
}

void StableLog::PurgeFiles(int expire_logs_nums, int expire_logs_days,
                           uint32_t to, bool manual,
                           const PurgeFilter& filter, PurgeLogsClosure* done) {
  ClosureGuard guard(done);
  std::map<uint32_t, std::string> binlogs;
  {
    slash::RWLock l(&files_rwlock_, false);
    if (!UnsafeGetBinlogFiles(&binlogs)) {
      LOG(WARNING) << "Replication group: " << options_.group_id.ToString()
                   << " Could not get binlog files!";
      if (done) {
        done->set_status(Status::Corruption("Get binlog files failed"));
      }
      return;
    }
  }
  int binlogs_size = binlogs.size();
  int remain_expire_num = binlogs_size - expire_logs_nums;
  int candidate_num = 0;
  std::map<uint32_t, std::string> candidates;
  struct stat file_stat;
  for (const auto& binlog : binlogs) {
    // Select the candidates to purge, when one of the following conditions is met:
    // 1) the logs with indexes smaller than 'to' will be picked when purge manually.
    // or 2) when purge automatically, the logs will be picked when:
    //    2.1) remain some logs (expire_logs_nums) and all others will be picked
    //    or 2.2) at least remain some logs (options_.retain_logs_num), and the rests which
    //            meet the date condition (expire_logs_days) will be picked.
    if ((manual && binlog.first <= to)
        || (remain_expire_num > 0)
        || ((binlogs_size - candidate_num) > options_.retain_logs_num
            && stat(binlog.second.c_str(), &file_stat) == 0
            && file_stat.st_mtime < time(NULL) - expire_logs_days * 24 * 3600)) {
      if (filter(binlog.first)) {
        LOG(WARNING) << "Replication group: " << options_.group_id.ToString()
                     << " Could not purge "<< binlog.first;
        break;
      }
      candidates.insert(binlog);
      ++candidate_num;
      --remain_expire_num;
    } else {
      // Break when face the first one not satisfied
      // Since the binlogs is order by the file index
      break;
    }
  }
  if (candidate_num <= 0) {
    return;
  }
  DoPurgeFiles(candidates, filter, static_cast<PurgeLogsClosure*>(guard.Release()));
}

void StableLog::DoPurgeFiles(const std::map<uint32_t, std::string>& candidates,
                             const PurgeFilter& filter, PurgeLogsClosure* done) {
  int deleted_num = 0;
  ClosureGuard guard(done);
  {
    slash::RWLock l(&files_rwlock_, true);
    for (const auto& candidate : candidates) {
      // 1. check filter again
      if (filter(candidate.first)) {
        LOG(WARNING) << "Replication group: " << options_.group_id.ToString()
                     << " Could not purge "<< (candidate.first);
        break;
      }
      // 2. Delete file
      Status s = slash::DeleteFile(candidate.second);
      if (!s.ok()) {
        LOG(WARNING) << "Replication group: " << options_.group_id.ToString()
                     << " Purge log file : " << (candidate.second)
                     <<  " failed! error: " << s.ToString();
        break;
      }
      deleted_num++;
      if (done) {
        done->AddPurgedIndex(candidate.first);
      }
      // 3. Reload index
      if (options_.load_index_into_memory) {
        slash::RWLock lk(&indexer_rwlock_, true);
        indexer_map_.erase(candidate.first);
        if (indexer_map_.empty()) {
          slash::RWLock l(&offset_rwlock_, true);
          first_offset_.Clear();
          last_offset_.Clear();
        } else {
          BinlogIndexer::Item item;
          indexer_map_.begin()->second->GetFirstItem(&item);
          slash::RWLock l(&offset_rwlock_, true);
          first_offset_.b_offset.filenum = indexer_map_.begin()->first;
          first_offset_.b_offset.offset = item.offset_pair.end_offset;
          first_offset_.l_offset.term = item.term;
          first_offset_.l_offset.index = item.index;
          start_filenum_ = first_offset_.b_offset.filenum;
        }
      }
    }
    if (deleted_num == 0) {
      return;
    }
    // reload first_offset_
    if (!options_.load_index_into_memory) {
      if (!UnsafeReloadFirstOffset()) {
        if (done) {
          done->set_status(Status::Corruption("ReloadFirstOffset failed"));
        }
        LOG(WARNING) << "Replication group: " << options_.group_id.ToString()
                     << " ReloadFirstOffset failed, delete  " << deleted_num << " binlog file";
        return;
      }
    }
  }
  LOG(INFO) << "Replication group: " << options_.group_id.ToString()
            << " Success purge "<< deleted_num << " binlog file";
}

std::unique_ptr<PikaBinlogReader> StableLog::GetPikaBinlogReader(const uint64_t index, Status* s) {
  uint32_t filenum = 0;
  uint32_t offset = 0;

  Status status;
  {
    slash::RWLock l(&files_rwlock_, false);
    if (options_.load_index_into_memory) {
      slash::RWLock lk(&indexer_rwlock_, false);
      BinlogIndexer::OffsetPair offset_pair;
      status = Status::NotFound(std::to_string(index));
      for (const auto& iter : indexer_map_) {
        if (iter.second->GetOffset(index, &offset_pair).ok()) {
          filenum = iter.first;
          offset = offset_pair.start_offset;
          status = Status::OK();
          DLOG(INFO) << "find in indexer: " << filenum <<  ":" << offset;
          break;
        }
      }
    } else {
      LogOffset found_offset;
      status = UnsafeFindLogicOffsetBySearchingBinlog(BinlogOffset(), index,
                                                      &found_offset, false/*start_offset*/);
      if (status.ok()) {
        filenum = found_offset.b_offset.filenum;
        offset = found_offset.b_offset.offset;
        DLOG(INFO) << "find in binlog: " << filenum <<  ":" << offset;
      }
    }
  }

  std::unique_ptr<PikaBinlogReader> binlog_reader = nullptr;
  if (status.ok()) {
    binlog_reader = std::unique_ptr<PikaBinlogReader>(new PikaBinlogReader());
    if (binlog_reader->Seek(stable_logger_, filenum, offset) != 0) {
      binlog_reader = nullptr;
      status = Status::Corruption("seek failed " + std::to_string(filenum)
                                  + "" + std::to_string(offset));
    }
  }
  if (s != nullptr) {
    *s = status;
  }
  return std::move(binlog_reader);
}

Status StableLog::AppendItem(const BinlogItem::Attributes& attrs,
                             const slash::Slice& data) {
  //slash::RWLock l(&files_rwlock_, true);
  Status s;
  // Append to stable storage
  s = stable_logger_->Put(data);
  if (!s.ok()) {
    return s;
  }
  uint32_t end_filenum;
  uint64_t end_offset;
  s = stable_logger_->GetProducerStatus(&end_filenum, &end_offset);
  if (!s.ok()) {
    return s;
  }

  // Update index
  if (options_.load_index_into_memory) {
    slash::RWLock lk(&indexer_rwlock_, true);
    if (indexer_map_.find(end_filenum) == indexer_map_.end()) {
      auto indexer = std::unique_ptr<BinlogIndexer>(new BinlogIndexer());
      indexer_map_.emplace(end_filenum, std::move(indexer));
    }
    indexer_map_[end_filenum]->UpdateLastItem(end_offset, attrs.logic_id, attrs.term_id);
  }
  // Update offset
  {
    slash::RWLock l(&offset_rwlock_, true);
    last_offset_.b_offset.filenum = end_filenum;
    last_offset_.b_offset.offset = end_offset;
    last_offset_.l_offset.term = attrs.term_id;
    last_offset_.l_offset.index = attrs.logic_id;
    // the first_offset_ will be set to empty, in following cases:
    // 1) start as new.
    // 2) after ResetStableLog is invoked.
    if (first_offset_.Empty()) {
      first_offset_ = last_offset_;
    }
  }
  DLOG(INFO) << "AppendItem " << last_offset_.ToString(); 
  return s;
}

Status StableLog::TruncateTo(const LogOffset& hint_offset) {
  slash::RWLock l(&files_rwlock_, true);
  LogOffset offset_in_local;
  Status s = UnsafeFindLogicOffset(hint_offset.b_offset,
                                   hint_offset.l_offset.index,
                                   &offset_in_local);
  if (!s.ok()) {
    return s;
  }
  s = UnsafePurgeFileAfter(offset_in_local.b_offset.filenum);
  if (!s.ok()) {
    return s;
  }
  s = stable_logger_->Truncate(offset_in_local.b_offset.filenum,
                               offset_in_local.b_offset.offset,
                               offset_in_local.l_offset.index);
  if (!s.ok()) {
    return s;
  }
  if (options_.load_index_into_memory) {
    slash::RWLock lk(&indexer_rwlock_, true);
    for (auto iter = indexer_map_.begin(); iter != indexer_map_.end();) {
      if (iter->first > offset_in_local.b_offset.filenum) {
        iter = indexer_map_.erase(iter);
      } else {
        iter++;
      }
    }
    auto iter = indexer_map_.find(offset_in_local.b_offset.filenum);
    if (iter != indexer_map_.end()) {
      s = iter->second->TruncateTo(offset_in_local.l_offset.index);
    }
  }
  {
    slash::RWLock l(&offset_rwlock_, true);
    last_offset_ = offset_in_local;
  }
  return s;
}

Status StableLog::ResetStableLog(const LogOffset& snapshot_offset) {
  slash::RWLock l(&files_rwlock_, true);
  Status s = stable_logger_->SetProducerStatus(
                              snapshot_offset.b_offset.filenum,
                              snapshot_offset.b_offset.offset,
                              snapshot_offset.l_offset.term,
                              snapshot_offset.l_offset.index);
  if (!s.ok()) {
    return s;
  }
  // After reset only one binlog file should be reserved.
  UnsafeClean();
  start_filenum_ = snapshot_offset.b_offset.filenum;
  return s;
}

void StableLog::UnsafeClean() {
  // 1. clean indexes
  if (options_.load_index_into_memory) {
    std::map<uint32_t, std::unique_ptr<BinlogIndexer>> empty_indexer_map;
    slash::RWLock lk(&indexer_rwlock_, true);
    indexer_map_.swap(empty_indexer_map);
  }
  // 2. clean offsets
  {
    slash::RWLock l(&offset_rwlock_, true);
    // NOTE: first_offset_ should be set to the offset of
    //       the first appended item in future.
    first_offset_ = LogOffset();
    last_offset_ = LogOffset();
  }
  // 3. clean start_filenum_
  start_filenum_ = 0;
}

} // namespace replication
