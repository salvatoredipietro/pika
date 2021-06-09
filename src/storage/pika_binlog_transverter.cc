// Copyright (c) 2018-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/storage/pika_binlog_transverter.h"

#include <sstream>
#include <assert.h>
#include <glog/logging.h>

#include "slash/include/slash_coding.h"

#include "include/pika_command.h"

namespace storage {

uint32_t BinlogItem::exec_time() const {
  return attributes_.exec_time;
}

uint32_t BinlogItem::term_id() const {
  return attributes_.term_id;
}

uint64_t BinlogItem::logic_id() const {
  return attributes_.logic_id;
}

uint32_t BinlogItem::filenum() const {
  return attributes_.filenum;
}

uint64_t BinlogItem::offset() const {
  return attributes_.offset;
}

uint32_t BinlogItem::content_length() const {
  return attributes_.content_length;
}

BinlogItem::Attributes BinlogItem::attributes() const {
  return attributes_;
}

std::string BinlogItem::content() const {
  return content_;
}

std::string BinlogItem::ReleaseContent() {
  return std::move(content_);
}

BinlogItem::Attributes::ContentType BinlogItem::content_type() const {
  return attributes_.content_type;
}

void BinlogItem::set_exec_time(uint32_t exec_time) {
  attributes_.exec_time = exec_time;
}

void BinlogItem::set_term_id(uint32_t term_id) {
  attributes_.term_id = term_id;
}

void BinlogItem::set_logic_id(uint64_t logic_id) {
  attributes_.logic_id = logic_id;
}

void BinlogItem::set_filenum(uint32_t filenum) {
  attributes_.filenum = filenum;
}

void BinlogItem::set_offset(uint64_t offset) {
  attributes_.offset = offset;
}

void BinlogItem::set_content_length(uint32_t content_length) {
  attributes_.content_length = content_length;
}

void BinlogItem::set_content_type(const Attributes::ContentType& content_type) {
  attributes_.content_type = content_type;
}

std::string BinlogItem::ToString() const {
  std::string str;
  str.append("exec_time: "  + std::to_string(attributes_.exec_time));
  str.append(",term_id: " + std::to_string(attributes_.term_id));
  str.append(",logic_id: "  + std::to_string(attributes_.logic_id));
  str.append(",filenum: "   + std::to_string(attributes_.filenum));
  str.append(",offset: "    + std::to_string(attributes_.offset));
  str.append(",content_length: "  + std::to_string(attributes_.content_length));
  str.append(",content_type: "  + std::to_string(static_cast<uint8_t>(attributes_.content_type)));
  str.append("\ncontent: ");
  for (size_t idx = 0; idx < content_.size(); ++idx) {
    if (content_[idx] == '\n') {
      str.append("\\n");
    } else if (content_[idx] == '\r') {
      str.append("\\r");
    } else {
      str.append(1, content_[idx]);
    }
  }
  str.append("\n");
  return str;
}

/*
 * *************************************************Binlog Item Format**************************************************
 * |  <Type>  | <Create Time> |  <Term Id>  | <Binlog Logic Id> | <File Num> | <Offset> | <Content Length> |       <Content>      |
 * | 2 Bytes  |    4 Bytes    |   4 Bytes   |      8 Bytes      |   4 Bytes  |  8 Bytes |     4 Bytes      | content length Bytes |
 * |---------------------------------------------- 34 Bytes -----------------------------------------------|
 *
 */
std::string PikaBinlogTransverter::BinlogEncode(uint32_t exec_time,
                                                uint32_t term_id,
                                                uint64_t logic_id,
                                                uint32_t filenum,
                                                uint64_t offset,
                                                const slash::Slice& content,
                                                const BinlogItem::Attributes::ContentType& content_type,
                                                const std::vector<std::string>& extends) {
  BinlogItem::Attributes attrs;
  attrs.exec_time = exec_time;
  attrs.term_id = term_id;
  attrs.logic_id = logic_id;
  attrs.filenum = filenum;
  attrs.offset = offset;
  attrs.content_type = content_type;
  std::string binlog(std::move(BinlogEncode(attrs, content, extends)));
  return binlog;
}

std::string PikaBinlogTransverter::BinlogEncode(const BinlogItem::Attributes& attrs,
                                                const slash::Slice& content,
                                                const std::vector<std::string>& extends) {
  std::string binlog;
  slash::PutFixed16(&binlog, static_cast<uint8_t>(attrs.content_type));
  slash::PutFixed32(&binlog, attrs.exec_time);
  slash::PutFixed32(&binlog, attrs.term_id);
  slash::PutFixed64(&binlog, attrs.logic_id);
  slash::PutFixed32(&binlog, attrs.filenum);
  slash::PutFixed64(&binlog, attrs.offset);
  uint32_t content_length = content.size();
  slash::PutFixed32(&binlog, content_length);
  binlog.append(content.data(), content.size());
  return binlog;
}

bool PikaBinlogTransverter::BinlogDecode(const std::string& binlog,
                                         BinlogItem* binlog_item) {
  std::string binlog_str = binlog;
  uint16_t content_type = 0;
  slash::GetFixed16(&binlog_str, &content_type);
  if (content_type == static_cast<uint8_t>(BinlogItem::Attributes::ContentType::TypeFirst)) {
    binlog_item->attributes_.content_type = BinlogItem::Attributes::ContentType::TypeFirst;
  } else if (content_type == static_cast<uint8_t>(BinlogItem::Attributes::ContentType::TypeSecond)) {
    binlog_item->attributes_.content_type = BinlogItem::Attributes::ContentType::TypeSecond;
  } else {
    LOG(ERROR) << "Binlog Item type error, actual type: " << content_type;
    return false;
  }
  slash::GetFixed32(&binlog_str, &binlog_item->attributes_.exec_time);
  slash::GetFixed32(&binlog_str, &binlog_item->attributes_.term_id);
  slash::GetFixed64(&binlog_str, &binlog_item->attributes_.logic_id);
  slash::GetFixed32(&binlog_str, &binlog_item->attributes_.filenum);
  slash::GetFixed64(&binlog_str, &binlog_item->attributes_.offset);
  slash::GetFixed32(&binlog_str, &binlog_item->attributes_.content_length);
  if (binlog_str.size() == binlog_item->content_length()) {
    binlog_item->content_.assign(binlog_str.data(), binlog_item->content_length());
  } else {
    LOG(ERROR) << "Binlog Item get content error, expect length:"
               << binlog_item->content_length() << " left length:" << binlog_str.size();
    return false;
  }
  return true;
}

bool PikaBinlogTransverter::BinlogAttributesDecode(const std::string& binlog,
                                                   BinlogItem::Attributes* binlog_attributes) {
  std::string binlog_str = binlog;
  uint16_t content_type;
  slash::GetFixed16(&binlog_str, &content_type);
  if (content_type == static_cast<uint8_t>(BinlogItem::Attributes::ContentType::TypeFirst)) {
    binlog_attributes->content_type = BinlogItem::Attributes::ContentType::TypeFirst;
  } else if (content_type == static_cast<uint8_t>(BinlogItem::Attributes::ContentType::TypeSecond)) {
    binlog_attributes->content_type = BinlogItem::Attributes::ContentType::TypeSecond;
  } else {
    LOG(ERROR) << "Binlog Item type error, actual type: " << content_type;
    return false;
  }
  slash::GetFixed32(&binlog_str, &binlog_attributes->exec_time);
  slash::GetFixed32(&binlog_str, &binlog_attributes->term_id);
  slash::GetFixed64(&binlog_str, &binlog_attributes->logic_id);
  slash::GetFixed32(&binlog_str, &binlog_attributes->filenum);
  slash::GetFixed64(&binlog_str, &binlog_attributes->offset);
  slash::GetFixed32(&binlog_str, &binlog_attributes->content_length);
  return true;
}

} // namespace storage
