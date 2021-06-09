// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/storage/pika_binlog.h"

#include <sys/time.h>
#include <glog/logging.h>
#include <fcntl.h>

#include "include/pika_define.h"
#include "include/storage/pika_binlog_transverter.h"

namespace storage {

using slash::RWLock;

std::string NewFileName(const std::string name, const uint32_t current) {
  char buf[256];
  snprintf(buf, sizeof(buf), "%s%u", name.c_str(), current);
  return std::string(buf);
}

/*
 * Version
 */
Version::Version(slash::RWFile* save)
  : pro_num_(0),
    pro_offset_(0),
    logic_id_(0),
    term_(0),
    save_(save) {
  assert(save_ != NULL);
  pthread_rwlock_init(&rwlock_, NULL);
}

Version::~Version() {
  StableSave();
  if (save_ != nullptr) {
    delete save_;
    save_ = nullptr;
  }
  pthread_rwlock_destroy(&rwlock_);
}

Status Version::StableSave() {
  char *p = save_->GetData();
  memcpy(p, &pro_num_, sizeof(uint32_t));
  p += 4;
  memcpy(p, &pro_offset_, sizeof(uint64_t));
  p += 8;
  memcpy(p, &logic_id_, sizeof(uint64_t));
  p += 8;
  memcpy(p, &term_, sizeof(uint32_t));
  return Status::OK();
}

Status Version::Init() {
  Status s;
  if (save_->GetData() != NULL) {
    memcpy((char*)(&pro_num_), save_->GetData(), sizeof(uint32_t));
    memcpy((char*)(&pro_offset_), save_->GetData() + 4, sizeof(uint64_t));
    memcpy((char*)(&logic_id_), save_->GetData() + 12, sizeof(uint64_t));
    memcpy((char*)(&term_), save_->GetData() + 20, sizeof(uint32_t));
    return Status::OK();
  } else {
    return Status::Corruption("version init error");
  }
}

/*
 * BinlogReader
 */

BinlogReader::BinlogReader(const std::string& base_filename, uint32_t filenum,
                           char* backing_store)
  : base_filename_(base_filename),
  filenum_(filenum),
  initialized_(false),
  read_handler_(nullptr),
  cur_offset_(0),
  last_record_offset_(0),
  self_store_(false),
  backing_store_(backing_store),
  buffer_() {
  if (backing_store_ == nullptr) {
    backing_store_ = new char[kBlockSize];
    self_store_ = true;
  }
  last_record_offset_ = 0 % kBlockSize;
}

BinlogReader::~BinlogReader() {
  if (read_handler_ != nullptr) {
    delete read_handler_;
    read_handler_ = nullptr;
  }
  if (self_store_ && backing_store_ != nullptr) {
    delete[] backing_store_;
    backing_store_ = nullptr;
  }
}

Status BinlogReader::Open(const OpenOptions& options) {
  if (initialized_) {
    return Status::Busy("Already opened");
  }
  std::string filename = NewFileName(base_filename_, filenum_);
  if (!slash::FileExists(filename)) {
    return Status::NotFound(filename);
  }
  if (!slash::NewSequentialFile(filename, &read_handler_).ok()) {
    Close();
    return Status::Corruption("New swquential " + filename + " failed");
  }
  if (options.read_start_offset != 0) {
    if (0 != SeekTo(options.read_start_offset)) {
      Close();
      return Status::Corruption("SeekTo " + filename + " offset " + std::to_string(options.read_start_offset) + " failed");
    }
  }
  initialized_ = true;
  return Status::OK();
}

void BinlogReader::Close() {
  if (read_handler_ != nullptr) {
    delete read_handler_;
    read_handler_ = nullptr;
  }
  cur_offset_ = 0;
  last_record_offset_ = 0;
  initialized_ = false;
}

void BinlogReader::SkipLength(uint32_t length) {
  // cur_offset_: offset for next read
  // last_record_offset_: the record offset in a Block
  size_t left = static_cast<size_t>(length);
  size_t avail = 0;
  size_t fragment_length = 0;
  while (left > 0) {
    const int leftover = static_cast<int>(kBlockSize) - last_record_offset_;
    if (static_cast<size_t>(leftover) < kHeaderSize) {
      read_handler_->Skip(leftover);
      cur_offset_ += leftover;
      last_record_offset_ = 0;
    }

    // For the begin record in a block, we should skip the Record Header
    if (last_record_offset_ == 0) {
      read_handler_->Skip(kHeaderSize);
      cur_offset_ += kHeaderSize;
      last_record_offset_ += kHeaderSize;
    }

    avail = kBlockSize - last_record_offset_ - kHeaderSize;
    fragment_length = (left < avail) ? left : avail;

    // Skip the fragment_length
    read_handler_->Skip(fragment_length);
    cur_offset_ += fragment_length;
    last_record_offset_ += fragment_length;
    left -= fragment_length;
  }
}

int BinlogReader::SeekTo(uint32_t offset) {
  cur_offset_ = offset;
  last_record_offset_ = cur_offset_ % kBlockSize;

  slash::Status s;
  uint64_t start_block = (cur_offset_ / kBlockSize) * kBlockSize;
  s = read_handler_->Skip((cur_offset_ / kBlockSize) * kBlockSize);
  uint64_t block_offset = cur_offset_ % kBlockSize;
  uint64_t ret = 0;
  uint64_t res = 0;

  while (true) {
    if (res >= block_offset) {
      cur_offset_ = start_block + res;
      break;
    }
    ret = 0;
    if (GetNext(&ret)) {
      return -1;
    }
    res += ret;
  }
  last_record_offset_ = cur_offset_ % kBlockSize;
  return 0;
}

bool BinlogReader::GetNext(uint64_t* size) {
  uint64_t offset = 0;
  slash::Status s;
  bool is_error = false;

  while (true) {
    buffer_.clear();
    s = read_handler_->Read(kHeaderSize, &buffer_, backing_store_);
    if (!s.ok()) {
      is_error = true;
      return is_error;
    }

    const char* header = buffer_.data();
    const uint32_t a = static_cast<uint32_t>(header[0]) & 0xff;
    const uint32_t b = static_cast<uint32_t>(header[1]) & 0xff;
    const uint32_t c = static_cast<uint32_t>(header[2]) & 0xff;
    const unsigned int type = header[7];
    const uint32_t length = a | (b << 8) | (c << 16);

    // Handle the disk silence errors (#988)
    if (length > (kBlockSize - kHeaderSize)) {
      is_error = true;
      return is_error;
    }

    if (type == kFullType) {
      s = read_handler_->Skip(length);
      offset += kHeaderSize + length;
      break;
    } else if (type == kFirstType) {
      s = read_handler_->Skip(length);
      offset += kHeaderSize + length;
    } else if (type == kMiddleType) {
      s = read_handler_->Skip(length);
      offset += kHeaderSize + length;
    } else if (type == kLastType) {
      s = read_handler_->Skip(length);
      offset += kHeaderSize + length;
      break;
    } else if (type == kBadRecord) {
      s = read_handler_->Skip(length);
      offset += kHeaderSize + length;
      break;
    } else {
      is_error = true;
      break;
    }
  }
  *size = offset;
  return is_error;
}

unsigned int BinlogReader::ReadNextPhysicalRecord(const ReadOptions& options,
    slash::Slice *result, uint64_t* start_offset, uint64_t* end_offset) {
  slash::Status s;
  if (kBlockSize - last_record_offset_ <= kHeaderSize) {
    read_handler_->Skip(kBlockSize - last_record_offset_);
    cur_offset_ += (kBlockSize - last_record_offset_);
    last_record_offset_ = 0;
  }
  buffer_.clear();
  s = read_handler_->Read(kHeaderSize, &buffer_, backing_store_);
  if (s.IsEndFile()) {
    return kEof;
  } else if (!s.ok()) {
    return kUnkownRecord;
  }

  const char* header = buffer_.data();
  const uint32_t a = static_cast<uint32_t>(header[0]) & 0xff;
  const uint32_t b = static_cast<uint32_t>(header[1]) & 0xff;
  const uint32_t c = static_cast<uint32_t>(header[2]) & 0xff;
  const unsigned int type = header[7];
  const uint32_t length = a | (b << 8) | (c << 16);
  if (type == kZeroType || length == 0) {
    buffer_.clear();
    return kOldRecord;
  }

  // Handle the disk silence errors (#988)
  if (length > (kBlockSize - kHeaderSize)) {
    buffer_.clear();
    return kUnkownRecord;
  }

  const uint32_t skip_len = options.read_start_offset > length
                            ? length : options.read_start_offset;
  if (skip_len > 0) {
    s = read_handler_->Skip(skip_len);
    if (!s.ok()) {
      return kUnkownRecord;
    }
  }
  uint32_t left_len = length - skip_len;
  if (left_len > 0) {
    const uint32_t read_len = options.read_len > left_len ? left_len : options.read_len;
    buffer_.clear();
    s = read_handler_->Read(read_len, &buffer_, backing_store_);
    if (!s.ok()) {
      return kUnkownRecord;
    }
    *result = slash::Slice(buffer_.data(), buffer_.size());
    // skip the remaining
    left_len -= read_len;
    s = read_handler_->Skip(left_len);
    if (!s.ok()) {
      return kUnkownRecord;
    }
  }

  last_record_offset_ += kHeaderSize + length;
  if (s.ok()) {
    *start_offset = cur_offset_;
    cur_offset_ += (kHeaderSize + length);
    *end_offset = cur_offset_;
  }
  return type;
}

Status BinlogReader::ConsumeItemAndSkipData(std::string* item_header,
    uint64_t* item_start_offset, uint64_t* item_consumed_len) {
  Status s;

  // |----Record Header(8 Bytes)----|------------------Record data-----------------|
  //                                                    |
  //                                                    v
  //                                |----Item header(34Bytes)----|----Item data----|
  slash::Slice fragment;
  uint64_t fragment_start_offset;
  uint64_t fragment_end_offset;
  ReadOptions read_options;
  read_options.read_len = BINLOG_ITEM_HEADER_SIZE;
  *item_consumed_len = 0;
  do {
    const unsigned int record_type = ReadNextPhysicalRecord(read_options, &fragment,
        &fragment_start_offset, &fragment_end_offset);
    *item_consumed_len += (fragment_end_offset - fragment_start_offset - kHeaderSize);
    switch (record_type) {
      case kFullType:
      case kFirstType:
        *item_start_offset = fragment_start_offset;
      case kMiddleType:
      case kLastType:
        break;
      case kEof:
        return Status::EndFile("Eof");
      case kBadRecord:
        return Status::Incomplete("Read BadRecord");
      case kOldRecord:
        return Status::EndFile("Eof");
      case kUnkownRecord:
        return Status::IOError("Disk Read");
      default:
        return Status::IOError("Unknow reason");
    }
    item_header->append(fragment.data(), fragment.size());
    read_options.read_len -= fragment.size();
  } while (read_options.read_len > 0);
  return Status::OK();
}

Status BinlogReader::Consume(std::string* scratch, uint32_t* filenum, uint64_t* offset) {
  Status s;

  DLOG(INFO) << "Binlog Sender begin consumer a msg, start_offset: " << cur_offset_;
  *filenum = filenum_;
  slash::Slice fragment;
  uint64_t fragment_start_offset;
  uint64_t fragment_end_offset;
  ReadOptions read_options;
  while (true) {
    const unsigned int record_type = ReadNextPhysicalRecord(read_options,
        &fragment, &fragment_start_offset, &fragment_end_offset);

    switch (record_type) {
      case kFullType:
        *scratch = std::string(fragment.data(), fragment.size());
        s = Status::OK();
        break;
      case kFirstType:
        scratch->assign(fragment.data(), fragment.size());
        s = Status::NotFound("Middle Status");
        break;
      case kMiddleType:
        scratch->append(fragment.data(), fragment.size());
        s = Status::NotFound("Middle Status");
        break;
      case kLastType:
        scratch->append(fragment.data(), fragment.size());
        s = Status::OK();
        break;
      case kEof:
        LOG(INFO) << "Binlog Sender consumer reach the end, file number: " << filenum_;
        return Status::EndFile("Eof");
      case kBadRecord:
        LOG(WARNING) << "Read BadRecord record, will decode failed,"
                     << ", this record may dbsync padded record, not processed here";
        return Status::Incomplete("Read BadRecord");
      case kOldRecord:
        return Status::EndFile("Eof");
      case kUnkownRecord:
        return Status::IOError("Disk Read");
      default:
        return Status::IOError("Unknow reason");
    }
    if (s.ok()) {
      break;
    }
  }
  *offset = fragment_end_offset;
  DLOG(INFO) << "Binlog Sender consumer a msg, end_offset: " << fragment_end_offset;
  return Status::OK();
}

/*
 * BinlogIndexer
 */
BinlogIndexer::BinlogIndexer()
  : initialized_(false), filenum_(0),
  first_index_(0), last_index_(0) { }

Status BinlogIndexer::Initialize(std::unique_ptr<BinlogReader> reader) {
  if (initialized_) {
    return Status::Busy("Already initialized");
  }
  Status s = reader->Open(BinlogReader::OpenOptions());
  if (!s.ok() && s.IsBusy()) {
    Close();
    return s;
  }
  filenum_ = reader->filenum();
  s = LoadIndexByStepFile(std::move(reader));
  if (!s.ok()) {
    Close();
    return s;
  }
  initialized_ = true;
  return s;
}

Status BinlogIndexer::LoadIndexByStepFile(std::unique_ptr<BinlogReader> reader) {
  Status s;
  BinlogItem::Attributes attrs;
  std::string item_header;
  uint64_t item_start_offset;
  uint64_t item_end_offset;
  uint64_t item_consumed_len;
  bool first_item_fetched = false;
  while (true) {
    item_header.clear();
    item_start_offset = 0;
    s = reader->ConsumeItemAndSkipData(&item_header, &item_start_offset, &item_consumed_len);
    if (!s.ok()) {
      break;
    }
    if (!PikaBinlogTransverter::BinlogAttributesDecode(item_header, &attrs)) {
      s = Status::Corruption("Item decode failed");
      break;
    }
    // we find the item_header, skip the rests of the item
    reader->SkipLength(BINLOG_ITEM_HEADER_SIZE + attrs.content_length - item_consumed_len);
    reader->GetReadStatus(&filenum_, &item_end_offset);
    item_offset_and_term_.push_back({OffsetPair(item_start_offset, item_end_offset), attrs.term_id});
    if (!first_item_fetched) {
      first_index_ = attrs.logic_id;
      first_item_fetched = true;
    }
    last_index_ = attrs.logic_id;
    DLOG(INFO) << "load index, index " << last_index_ << ", filenum: " << filenum_
               << ", start_offset " << item_start_offset
               << ", end_offset " << item_end_offset;
  }
  return s.IsEndFile() ? Status::OK() : s;
}

Status BinlogIndexer::GetTerm(const uint64_t index, uint32_t* term) {
  if (index < first_index_ || index > last_index_) {
    return Status::NotFound("out of range");
  }
  const uint64_t loc = index - first_index_;
  *term = item_offset_and_term_[loc].second;
  return Status::OK();
}

Status BinlogIndexer::GetOffset(const uint64_t index, OffsetPair* offset_pair) {
  if (index < first_index_ || index > last_index_) {
    return Status::NotFound("out of range");
  }
  const uint64_t loc = index - first_index_;
  *offset_pair = item_offset_and_term_[loc].first;
  return Status::OK();
}

Status BinlogIndexer::GetItem(const uint64_t index, Item* item) {
  if (index < first_index_ || index > last_index_) {
    return Status::NotFound("out of range");
  }
  const uint64_t loc = index - first_index_;
  item->index = index;
  item->term = item_offset_and_term_[loc].second;
  item->offset_pair = item_offset_and_term_[loc].first;
  return Status::OK();
}

Status BinlogIndexer::GetFirstItem(Item* item) {
  const auto& iter = item_offset_and_term_.begin();
  if (iter == item_offset_and_term_.end()) {
    return Status::NotFound("first item");
  }
  item->index = first_index_;
  item->term = iter->second;
  item->offset_pair = iter->first;
  return Status::OK();
}

Status BinlogIndexer::GetLastItem(Item* item) {
  const auto& iter = item_offset_and_term_.rbegin();
  if (iter == item_offset_and_term_.rend()) {
    return Status::NotFound("last item");
  }
  item->index = last_index_;
  item->term = iter->second;
  item->offset_pair = iter->first;
  return Status::OK();
}

void BinlogIndexer::UpdateLastItem(const uint64_t end_offset,
                                   const uint64_t index,
                                   const uint32_t term) {
  const auto& current_size = item_offset_and_term_.size();
  if (current_size != 0) {
    assert(index == last_index_ + 1);
    last_index_ = index; 
  } else {
    assert(first_index_ == 0);
    assert(last_index_ == 0);
    first_index_ = index;
    last_index_ = index;
  }
  OffsetPair pair;
  pair.start_offset = 0;
  Item current_last_item;
  if (GetLastItem(&current_last_item).ok()) {
    pair.start_offset = current_last_item.offset_pair.end_offset;
  }
  pair.end_offset = end_offset;
  item_offset_and_term_.push_back({pair, term});
  DLOG(INFO) << "UpdateLastItem, term " << term
             << ", index " << index
             << ", start_offset " << pair.start_offset
             << ", end_offset " << pair.end_offset;
}

Status BinlogIndexer::TruncateTo(const uint64_t index) {
  if (index < first_index_ || index > last_index_) {
    return Status::Corruption("out of range");
  }
  const uint64_t loc = index - first_index_;
  item_offset_and_term_.erase(item_offset_and_term_.begin() + loc + 1,
                              item_offset_and_term_.end());
  const auto& last_item = item_offset_and_term_.rbegin();
  if (last_item == item_offset_and_term_.rend()) {
    // become empty
    Close();
    return Status::OK();
  }
  last_index_ = index;
  return Status::OK();
}

void BinlogIndexer::Close() {
  initialized_ = false;
  first_index_ = 0;
  last_index_ = 0;
  item_offset_and_term_.clear();
}

/*
 * BinlogBuilder
 */
BinlogBuilder::BinlogBuilder(const std::string& binlog_path, const int file_size) :
    opened_(false),
    version_(nullptr),
    queue_(nullptr),
    pro_num_(0),
    exit_all_consume_(false),
    binlog_path_(binlog_path),
    file_size_(file_size),
    filename_(binlog_path_ + kBinlogPrefix),
    binlog_io_error_(false) {
}

BinlogBuilder::~BinlogBuilder() {
  Close();
}

Status BinlogBuilder::Initialize() {
  if (opened_.load(std::memory_order_acquire)) {
    return Status::Busy("BinlogBuilder has been initialized");
  }

  // NOTE: synchronization with Put methods
  std::lock_guard<std::mutex> l(mutex_);
  if (opened_.load(std::memory_order_acquire)) {
    return Status::Busy("BinlogBuilder has been initialized");
  }

  // To intergrate with old version, we don't set mmap file size to 100M;
  //slash::SetMmapBoundSize(file_size);
  //slash::kMmapBoundSize = 1024 * 1024 * 100;
  Status s;
  slash::CreatePath(binlog_path_, 0755);
  const std::string manifest = binlog_path_ + kManifest;
  std::string profile;

  if (!slash::FileExists(manifest)) {
    LOG(INFO) << "BinlogBuilder: Manifest file not exist, we create a new one.";

    profile = NewFileName(filename_, pro_num_);
    slash::WritableFile* queue = nullptr;
    s = slash::NewWritableFile(profile, &queue);
    if (!s.ok()) {
      LOG(ERROR) << "BinlogBuilder: new " << filename_ << " " << s.ToString();
      return s;
    }
    queue_ = std::unique_ptr<slash::WritableFile>(queue);

    slash::RWFile* versionfile = nullptr;
    s = slash::NewRWFile(manifest, &versionfile);
    if (!s.ok()) {
      LOG(ERROR) << "BinlogBuilder: new versionfile error " << s.ToString();
      return s;
    }
    version_ = std::unique_ptr<Version>(new Version(versionfile));
    version_->StableSave();
  } else {
    LOG(INFO) << "BinlogBuilder: Find the exist file.";

    slash::RWFile* versionfile = nullptr;
    s = slash::NewRWFile(manifest, &versionfile);
    if (!s.ok()) {
      LOG(ERROR) << "BinlogBuilder: open versionfile error";
      return s;
    }
    version_ = std::unique_ptr<Version>(new Version(versionfile));
    version_->Init();
    pro_num_ = version_->pro_num_;

    profile = NewFileName(filename_, pro_num_);
    DLOG(INFO) << "BinlogBuilder: open profile " << profile;
    slash::WritableFile* queue = nullptr;
    s = slash::AppendWritableFile(profile, &queue, version_->pro_offset_);
    if (!s.ok()) {
      LOG(ERROR) << "BinlogBuilder: Open file " << profile << " error " << s.ToString();
      return s;
    }
    queue_ = std::unique_ptr<slash::WritableFile>(queue);

    uint64_t filesize = queue_->Filesize();
    DLOG(INFO) << "BinlogBuilder: filesize is " << filesize;
  }

  InitLogFile();
  return s;
}

void BinlogBuilder::Close() {
  if (!opened_.load(std::memory_order_acquire)) {
    return;
  }
  opened_.store(false, std::memory_order_release);
}

void BinlogBuilder::InitLogFile() {
  assert(queue_ != NULL);

  uint64_t filesize = queue_->Filesize();
  block_offset_ = filesize % kBlockSize;

  opened_.store(true, std::memory_order_release);
}

Status BinlogBuilder::GetProducerStatus(uint32_t* filenum, uint64_t* pro_offset,
    uint32_t* term, uint64_t* logic_id) {
  if (!opened_.load(std::memory_order_acquire)) {
    return Status::Busy("BinlogBuilder is not open yet");
  }

  slash::RWLock l(&(version_->rwlock_), false);

  if (filenum != nullptr) {
    *filenum = version_->pro_num_;
  }
  if (pro_offset != nullptr) {
    *pro_offset = version_->pro_offset_;
  }
  if (logic_id != nullptr) {
    *logic_id = version_->logic_id_;
  }
  if (term != nullptr) {
    *term = version_->term_;
  }
  return Status::OK();
}

Status BinlogBuilder::Put(const slash::Slice& item) {
  if (!opened_.load(std::memory_order_acquire)) {
    return Status::Busy("BinlogBuilder is not open yet");
  }
  Status s = Put(item.data(), item.size());
  if (!s.ok()) {
    binlog_io_error_.store(true);
  }
  return s;
}

Status BinlogBuilder::Put(const std::string& item) {
  if (!opened_.load(std::memory_order_acquire)) {
    return Status::Busy("BinlogBuilder is not open yet");
  }
  Status s = Put(item.c_str(), item.size());
  if (!s.ok()) {
    binlog_io_error_.store(true);
  }
  return s;
}

Status BinlogBuilder::Put(const char* item, int len) {
  Status s;

  /* Check to roll log file */
  uint64_t filesize = queue_->Filesize();
  if (filesize > file_size_) {
    pro_num_++;
    std::string profile = NewFileName(filename_, pro_num_);
    slash::WritableFile* queue = nullptr;
    s = slash::NewWritableFile(profile, &queue);
    if (!s.ok()) {
      return s;
    }
    queue_ = std::unique_ptr<slash::WritableFile>(queue);
    {
      slash::RWLock l(&(version_->rwlock_), true);
      version_->pro_offset_ = 0;
      version_->pro_num_ = pro_num_;
      version_->StableSave();
    }
    InitLogFile();
  }

  int pro_offset;
  s = Produce(Slice(item, len), &pro_offset);
  if (s.ok()) {
    slash::RWLock l(&(version_->rwlock_), true);
    version_->pro_offset_ = pro_offset;
    version_->logic_id_++;
    version_->StableSave();
  }

  return s;
}

Status BinlogBuilder::EmitPhysicalRecord(RecordType t, const char *ptr, size_t n, int *temp_pro_offset) {
    Status s;
    assert(n <= 0xffffff);
    assert(block_offset_ + kHeaderSize + n <= kBlockSize);

    char buf[kHeaderSize];

    uint64_t now;
    struct timeval tv;
    gettimeofday(&tv, NULL);
    now = tv.tv_sec;
    buf[0] = static_cast<char>(n & 0xff);
    buf[1] = static_cast<char>((n & 0xff00) >> 8);
    buf[2] = static_cast<char>(n >> 16);
    buf[3] = static_cast<char>(now & 0xff);
    buf[4] = static_cast<char>((now & 0xff00) >> 8);
    buf[5] = static_cast<char>((now & 0xff0000) >> 16);
    buf[6] = static_cast<char>((now & 0xff000000) >> 24);
    buf[7] = static_cast<char>(t);

    s = queue_->Append(Slice(buf, kHeaderSize));
    if (s.ok()) {
        s = queue_->Append(Slice(ptr, n));
        if (s.ok()) {
            s = queue_->Flush();
        }
    }
    block_offset_ += static_cast<int>(kHeaderSize + n);

    *temp_pro_offset += kHeaderSize + n;
    return s;
}

Status BinlogBuilder::Produce(const Slice &item, int *temp_pro_offset) {
  Status s;
  const char *ptr = item.data();
  size_t left = item.size();
  bool begin = true;

  {
    slash::RWLock l(&(version_->rwlock_), false);
    *temp_pro_offset = version_->pro_offset_;
  }
  do {
    const int leftover = static_cast<int>(kBlockSize) - block_offset_;
    assert(leftover >= 0);
    if (static_cast<size_t>(leftover) < kHeaderSize) {
      if (leftover > 0) {
        s = queue_->Append(Slice("\x00\x00\x00\x00\x00\x00\x00", leftover));
        if (!s.ok()) {
          return s;
        }
        *temp_pro_offset += leftover;
      }
      block_offset_ = 0;
    }

    const size_t avail = kBlockSize - block_offset_ - kHeaderSize;
    const size_t fragment_length = (left < avail) ? left : avail;
    RecordType type;
    const bool end = (left == fragment_length);
    if (begin && end) {
      type = kFullType;
    } else if (begin) {
      type = kFirstType;
    } else if (end) {
      type = kLastType;
    } else {
      type = kMiddleType;
    }

    s = EmitPhysicalRecord(type, ptr, fragment_length, temp_pro_offset);
    ptr += fragment_length;
    left -= fragment_length;
    begin = false;
  } while (s.ok() && left > 0);

  return s;
}
 
Status BinlogBuilder::AppendPadding(slash::WritableFile* file, uint64_t* len) {
  if (*len < kHeaderSize) {
    return Status::OK();
  }

  Status s;
  char buf[kBlockSize];
  uint64_t now;
  struct timeval tv;
  gettimeofday(&tv, NULL);
  now = tv.tv_sec;

  uint64_t left = *len;
  while (left > 0 && s.ok()) {
    uint32_t size = (left >= kBlockSize) ? kBlockSize : left;
    if (size < kHeaderSize) {
      break;
    } else {
      uint32_t bsize = size - kHeaderSize;
      std::string binlog(bsize, '*');
      buf[0] = static_cast<char>(bsize & 0xff);
      buf[1] = static_cast<char>((bsize & 0xff00) >> 8);
      buf[2] = static_cast<char>(bsize >> 16);
      buf[3] = static_cast<char>(now & 0xff);
      buf[4] = static_cast<char>((now & 0xff00) >> 8);
      buf[5] = static_cast<char>((now & 0xff0000) >> 16);
      buf[6] = static_cast<char>((now & 0xff000000) >> 24);
      // kBadRecord here
      buf[7] = static_cast<char>(kBadRecord);
      s = file->Append(Slice(buf, kHeaderSize));
      if (s.ok()) {
        s = file->Append(Slice(binlog.data(), binlog.size()));
        if (s.ok()) {
          s = file->Flush();
          left -= size;
        }
      }
    }
  }
  *len -= left;
  if (left != 0) {
    LOG(WARNING) << "AppendPadding left bytes: " << left  << " is less then kHeaderSize";
  }
  return s;
}

Status BinlogBuilder::SetProducerStatus(uint32_t pro_num, uint64_t pro_offset, uint32_t term, uint64_t index) {
  if (!opened_.load(std::memory_order_acquire)) {
    return Status::Busy("BinlogBuilder is not open yet");
  }

  // offset smaller than the first header
  if (pro_offset < 4) {
    pro_offset = 0;
  }

  // Delete all binlog files prior to pro_num
  for (uint32_t i = 0; i <= pro_num; i++) {
    std::string init_profile = NewFileName(filename_, i);
    if (slash::FileExists(init_profile)) {
      slash::DeleteFile(init_profile);
    }
  }

  std::string profile = NewFileName(filename_, pro_num);

  slash::WritableFile* queue = nullptr;
  Status s = slash::NewWritableFile(profile, &queue);
  if (!s.ok()) {
    return s;
  }
  BinlogBuilder::AppendPadding(queue, &pro_offset);
  queue_ = std::unique_ptr<slash::WritableFile>(queue);

  pro_num_ = pro_num;

  {
    slash::RWLock l(&(version_->rwlock_), true);
    version_->pro_num_ = pro_num;
    version_->pro_offset_ = pro_offset;
    version_->term_ = term;
    version_->logic_id_ = index;
    version_->StableSave();
  }

  InitLogFile();
  return Status::OK();
}

Status BinlogBuilder::Truncate(uint32_t pro_num, uint64_t pro_offset, uint64_t index) {
  std::string profile = NewFileName(filename_, pro_num);
  const int fd = open(profile.c_str(), O_RDWR | O_CLOEXEC, 0644);
  if (fd < 0) {
    return Status::IOError("fd open failed");
  }
  if (ftruncate(fd, pro_offset)) {
    return Status::IOError("ftruncate failed");
  }
  close(fd);

  pro_num_ = pro_num;
  {
    slash::RWLock l(&(version_->rwlock_), true);
    version_->pro_num_ = pro_num;
    version_->pro_offset_ = pro_offset;
    version_->logic_id_ = index;
    version_->StableSave();
  }

  slash::WritableFile* queue = nullptr;
  Status s = slash::AppendWritableFile(profile, &queue, version_->pro_offset_);
  if (!s.ok()) {
    return s;
  }
  queue_ = std::unique_ptr<slash::WritableFile>(queue);

  InitLogFile();

  return Status::OK();
}

} // namespace storage
