// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/storage/pika_binlog_reader.h"

#include <glog/logging.h>

namespace storage {

PikaBinlogReader::PikaBinlogReader(uint32_t cur_filenum,
    uint64_t cur_offset)
    : backing_store_(new char[kBlockSize]),
    writer_(nullptr),
    reader_(nullptr) {
}

PikaBinlogReader::PikaBinlogReader()
    : backing_store_(new char[kBlockSize]),
    writer_(nullptr),
    reader_(nullptr) {
}

PikaBinlogReader::~PikaBinlogReader() {
  if (backing_store_ != nullptr) {
    delete[] backing_store_;
    backing_store_ = nullptr;
  }
}

void PikaBinlogReader::GetReaderStatus(uint32_t* cur_filenum, uint64_t* cur_offset) {
  if (reader_ != nullptr) {
    reader_->GetReadStatus(cur_filenum, cur_offset);
  }
}

void PikaBinlogReader::GetProducerStatus(uint32_t* end_filenum, uint64_t* end_offset) {
  writer_->GetProducerStatus(end_filenum, end_offset);
}

bool PikaBinlogReader::ReadToTheEnd() {
  uint32_t pro_num;
  uint64_t pro_offset;
  Status s = writer_->GetProducerStatus(&pro_num, &pro_offset);
  if (!s.ok()) {
    LOG(WARNING) << "GetProducerStatus failed " << s.ToString();
    return false;
  }
  uint32_t cur_filenum;
  uint64_t cur_offset;
  reader_->GetReadStatus(&cur_filenum, &cur_offset);
  return (pro_num == cur_filenum && pro_offset == cur_offset);
}

uint64_t PikaBinlogReader::BinlogFileSize() {
  return writer_->file_size();
}

int PikaBinlogReader::Seek(const std::shared_ptr<BinlogBuilder>& writer, uint32_t filenum, uint64_t offset) {
  auto reader = std::unique_ptr<BinlogReader>(new BinlogReader(writer->filename(), filenum, backing_store_));
  BinlogReader::OpenOptions open_options;
  open_options.read_start_offset = offset;
  Status s = reader->Open(open_options);
  if (!s.ok()) {
    LOG(WARNING) << "Open BinlogReader failed " << s.ToString();
    return -1;
  }
  reader_ = std::move(reader);
  writer_ = writer;
  return 0;
}

// Get a whole message;
// Append to scratch;
// the status will be OK, IOError or Corruption, EndFile;
// the endpoint of the record is returned by filenum and offset.
Status PikaBinlogReader::Get(std::string* scratch, uint32_t* filenum, uint64_t* offset) {
  if (writer_ == nullptr || reader_ == NULL) {
    return Status::Corruption("Not seek");
  }
  scratch->clear();
  Status s = Status::OK();

  do {
    if (ReadToTheEnd()) {
      return Status::EndFile("End of cur log file");
    }
    s = reader_->Consume(scratch, filenum, offset);
    if (s.IsEndFile()) {
      // sleep 10ms wait produce thread generate the new binlog
      usleep(10000);

      auto next_reader = std::unique_ptr<BinlogReader>(
          new BinlogReader(writer_->filename(), reader_->filenum() + 1, backing_store_));
      auto open_status = next_reader->Open(BinlogReader::OpenOptions());
      if (open_status.IsNotFound()) {
        // new file may have not been generated
        return Status::EndFile("End of cur log file");
      } else if (!open_status.ok()) {
        // expose the unexpected error
        return open_status;
      }
      DLOG(INFO) << "BinlogReader roll to new binlog: " << reader_->filenum() + 1;
      reader_ = std::move(next_reader);
    } else {
      DLOG(INFO) << "BinlogReader consume got status: " << s.ToString();
      break;
    }
  } while (s.IsEndFile());
  // Expose the error to upper caller.
  return s;
}

} // namespace storage
