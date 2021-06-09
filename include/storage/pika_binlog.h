// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef STORAGE_PIKA_BINLOG_H_
#define STORAGE_PIKA_BINLOG_H_

#include <atomic>
#include <mutex>
#include <memory>

#include "slash/include/env.h"
#include "slash/include/slash_mutex.h"
#include "slash/include/slash_status.h"

#include "include/pika_define.h"

namespace storage {

/*
 * The size of Binlogfile
 */
//static uint64_t kBinlogSize = 128; 
//static const uint64_t kBinlogSize = 1024 * 1024 * 100;

/*
 * the block size that we read and write from write2file
 * the default size is 64KB
 */
static const size_t kBlockSize = 64 * 1024;

/*
 * Header is Type(1 byte), length (3 bytes), time (4 bytes)
 */
static const size_t kHeaderSize = 1 + 3 + 4;

std::string NewFileName(const std::string name, const uint32_t current);

using slash::Status;
using slash::Slice;

class Version {
 public:
  Version(slash::RWFile* save);
  ~Version();

  Status Init();

  // RWLock should be held when access members.
  Status StableSave();

  uint32_t pro_num_;
  uint64_t pro_offset_;
  uint64_t logic_id_;
  uint32_t term_;

  pthread_rwlock_t rwlock_;

  void debug() {
    slash::RWLock(&rwlock_, false);
    printf ("Current pro_num %u pro_offset %lu\n", pro_num_, pro_offset_);
  }

 private:
  slash::RWFile* save_;

  // No copying allowed;
  Version(const Version&);
  void operator=(const Version&);
};

enum RecordType {
  kZeroType = 0,
  kFullType = 1,
  kFirstType = 2,
  kMiddleType = 3,
  kLastType = 4,
  kEof = 5,
  kBadRecord = 6,
  kOldRecord = 7,
  // Add kUnkownRecord to distinguish IOError
  // from the kBadRecord, the artificially populated
  // data in DBSync state, which can be read correctly
  // from disk even though it does not have any real meaning.
  // TODO(LIBA-S): replace the kBadRecord with a more appropriate name.
  kUnkownRecord = 8,
};

// thread-unsafe.
class BinlogReader {
  friend class BinlogIndexer;
 public:
  BinlogReader(const std::string& base_filename, uint32_t filenum,
               char* backing_store = nullptr);
  ~BinlogReader();

  uint32_t filenum() const { return filenum_; }

  struct OpenOptions {
    uint32_t read_start_offset = 0;
    OpenOptions() = default;
  };
  Status Open(const OpenOptions& options);

  Status Consume(std::string* scratch, uint32_t* filenum, uint64_t* offset);

  void GetReadStatus(uint32_t* filenum, uint64_t* cur_offset) {
    *filenum = filenum_;
    *cur_offset = cur_offset_;
  }
 
 private:
  void Close();
  void SkipLength(uint32_t length);
  int SeekTo(uint32_t offset);
  bool GetNext(uint64_t* size);
  Status ConsumeItemAndSkipData(std::string* item_header, uint64_t* item_start_offset,
                                uint64_t* item_consumed_len);
  struct ReadOptions {
    constexpr static uint32_t kMaxLength = std::numeric_limits<uint32_t>::max();
    // The start point of the current record.
    uint32_t read_start_offset = 0;
    uint32_t read_len = kMaxLength;
    ReadOptions() = default;
  };
  unsigned int ReadNextPhysicalRecord(const ReadOptions& options, slash::Slice *result,
                                      uint64_t* start_offset, uint64_t* end_offset);

 private:
  const std::string base_filename_;
  const uint32_t filenum_;

  bool initialized_;
  slash::SequentialFile* read_handler_;

  // current read offset in the file (absolutely)
  uint64_t cur_offset_;
  // record end offset in a block (relative)
  uint64_t last_record_offset_;

  bool self_store_;
  char* backing_store_;
  Slice buffer_;
};

// BinlogIndexer is responsible for index a specific log file
class BinlogIndexer {
 public:
  BinlogIndexer();
  DISALLOW_COPY(BinlogIndexer);

  struct OffsetPair {
    uint64_t start_offset = 0;
    uint64_t end_offset = 0;
    OffsetPair() = default;
    OffsetPair(const uint64_t _start_offset, const uint64_t _end_offset)
      : start_offset(_start_offset), end_offset(_end_offset) {}
  };
  struct Item {
    OffsetPair offset_pair;
    uint64_t index = 0;
    uint32_t term = 0;
    Item() = default;
  };
  Status Initialize(std::unique_ptr<BinlogReader> reader);
  Status GetTerm(const uint64_t index, uint32_t* term);
  Status GetOffset(const uint64_t index, OffsetPair* offset_pair);
  Status GetItem(const uint64_t index, Item* item);
  Status GetFirstItem(Item* item);
  Status GetLastItem(Item* item);
  void UpdateLastItem(const uint64_t end_offset, const uint64_t index, const uint32_t term);
  Status TruncateTo(const uint64_t index);
  void set_filenum(const uint32_t filenum) { filenum_ = filenum; } 
  uint32_t filenum() const { return filenum_; }

 private:
  void Close();
  Status LoadIndexByStepFile(std::unique_ptr<BinlogReader> reader);

  bool initialized_;
  // The number of the current indexed file.
  uint32_t filenum_;

  // The indexes of items should be within the range [first_index_, last_index_]
  uint64_t first_index_;
  uint64_t last_index_;
  std::vector<std::pair<OffsetPair, uint32_t/*term*/>> item_offset_and_term_;
};

class BinlogBuilder {
 public:
  BinlogBuilder(const std::string& Binlog_path, const int file_size = 100 * 1024 * 1024);
  ~BinlogBuilder();

  BinlogBuilder(const BinlogBuilder&) = delete;
  void operator=(const BinlogBuilder&) = delete;

  void Lock() { mutex_.lock(); }
  void Unlock() { mutex_.unlock(); }

  // Caller should invoke Initialize method manually before any operations.
  // This method will occupy the mutex_, so it's thread-safe.
  Status Initialize();

  // Caller can reopen this builder by invoke initialize again.
  void Close();

  // REQUIRES: mutex_ must be held.
  Status Put(const slash::Slice& item);
  Status Put(const std::string &item);

  Status GetProducerStatus(uint32_t* filenum, uint64_t* pro_offset, uint32_t* term = NULL, uint64_t* logic_id = NULL);
  // REQUIRES: mutex_ must be held.
  Status SetProducerStatus(uint32_t filenum, uint64_t pro_offset, uint32_t term = 0, uint64_t index = 0);
  // REQUIRES: mutex_ must be held.
  Status Truncate(uint32_t pro_num, uint64_t pro_offset, uint64_t index);

  uint64_t file_size() {
    return file_size_;
  }

  std::string filename() {
    return filename_;
  }

  bool IsBinlogIoError() {
    return binlog_io_error_;
  }

  void SetTerm(uint32_t term) {
    slash::RWLock(&(version_->rwlock_), true);
    version_->term_ = term;
    version_->StableSave();
  }

  uint32_t term() {
    slash::RWLock(&(version_->rwlock_), true);
    return version_->term_;
  }

 private:
  Status Put(const char* item, int len);
  static Status AppendPadding(slash::WritableFile* file, uint64_t* len);

  void InitLogFile();
  Status EmitPhysicalRecord(RecordType t, const char *ptr, size_t n, int *temp_pro_offset);

  Status Produce(const Slice &item, int *pro_offset);

  std::atomic<bool> opened_;

  std::unique_ptr<Version> version_;
  std::unique_ptr<slash::WritableFile> queue_;

  std::mutex mutex_;

  uint32_t pro_num_;

  int block_offset_;

  //char* pool_;
  bool exit_all_consume_;
  const std::string binlog_path_;

  uint64_t file_size_;

  const std::string filename_;

  std::atomic<bool> binlog_io_error_;
  // Not use
  //int32_t retry_;
};

} // namespace storage

#endif // STORAGE_PIKA_BINLOG_H_
