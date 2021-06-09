// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef STORAGE_PIKA_BINLOG_READER_H_
#define STORAGE_PIKA_BINLOG_READER_H_

#include <string>
#include <memory>

#include "slash/include/slash_status.h"
#include "slash/include/env.h"
#include "slash/include/slash_slice.h"

#include "include/storage/pika_binlog.h"

namespace storage {

using slash::Status;
using slash::Slice;

// thread-unsafe
class PikaBinlogReader {
 public:
  PikaBinlogReader();
  PikaBinlogReader(uint32_t cur_filenum, uint64_t cur_offset);
  ~PikaBinlogReader();
  // offset indicates the end of the record
  Status Get(std::string* scratch, uint32_t* filenum, uint64_t* offset);
  // seek to the endpoint of the last record
  int Seek(const std::shared_ptr<BinlogBuilder>& logger, uint32_t filenum, uint64_t offset);
  void GetReaderStatus(uint32_t* cur_filenum, uint64_t* cur_offset);
  void GetProducerStatus(uint32_t* end_filenum, uint64_t* end_offset);
  uint64_t BinlogFileSize();

 private:
  bool ReadToTheEnd();
 
 private:
  char* backing_store_;
  std::shared_ptr<BinlogBuilder> writer_;
  std::unique_ptr<BinlogReader> reader_;
};

} // namespace storage 

#endif  // STORAGE_PIKA_BINLOG_READER_H_
