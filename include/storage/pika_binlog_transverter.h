// Copyright (c) 2018-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef STORAGE_PIKA_BINLOG_TRANSVERTER_H_
#define STORAGE_PIKA_BINLOG_TRANSVERTER_H_

#include <vector>
#include <stdint.h>
#include <iostream>
#include <glog/logging.h>

#include "slash/include/slash_slice.h"

namespace storage {

/*
 * ***********************************************Binlog Item Format***********************************************
 * | <Type> | <Create Time> | <Term Id> | <Binlog Logic Id> | <File Num> | <Offset> | <Content Length> |      <Content>     |
 *  2 Bytes      4 Bytes      4 Bytes       8 Bytes            4 Bytes     8 Bytes        4 Bytes       content length Bytes
 *
 */
const int BINLOG_ENCODE_LEN = 34;

const int BINLOG_ITEM_HEADER_SIZE = 34;

class BinlogItem {
  public:
    struct Attributes {
      enum class ContentType : uint8_t {
        TypeFirst  = 1, /*content is encoded with the Redis format*/
        TypeSecond = 2, /*content is encoded with the PB format*/
      };
      uint32_t exec_time;
      uint32_t term_id;
      uint64_t logic_id;
      uint64_t offset;
      uint32_t filenum;
      uint32_t content_length;
      ContentType content_type;
      Attributes() :
        exec_time(0),
        term_id(0),
        logic_id(0),
        offset(0),
        filenum(0),
        content_length(0),
        content_type(ContentType::TypeFirst) {}
    };
    BinlogItem() : attributes_(), content_("") {}

    friend class PikaBinlogTransverter;

    uint32_t exec_time()   const;
    uint32_t term_id()   const;
    uint64_t logic_id()    const;
    uint32_t filenum()     const;
    uint64_t offset()      const;
    uint32_t content_length()  const;
    Attributes attributes() const;
    std::string content()  const;
    std::string ReleaseContent();
    std::string ToString() const;
    Attributes::ContentType content_type() const;

    void set_exec_time(uint32_t exec_time);
    void set_term_id(uint32_t term_id);
    void set_logic_id(uint64_t logic_id);
    void set_filenum(uint32_t filenum);
    void set_offset(uint64_t offset);
    void set_content_length(uint32_t content_length);
    void set_content_type(const Attributes::ContentType& content_type);

  private:
    Attributes attributes_;
    std::string content_;
    std::vector<std::string> extends_;
};

class PikaBinlogTransverter{
  public:
    PikaBinlogTransverter() {};
    static std::string BinlogEncode(uint32_t exec_time,
                                    uint32_t term_id,
                                    uint64_t logic_id,
                                    uint32_t filenum,
                                    uint64_t offset,
                                    const slash::Slice& content,
                                    const BinlogItem::Attributes::ContentType& type,
                                    const std::vector<std::string>& extends);
    static std::string BinlogEncode(const BinlogItem::Attributes& attrs,
                                    const slash::Slice& content,
                                    const std::vector<std::string>& extends);

    static bool BinlogDecode(const std::string& binlog,
                             BinlogItem* binlog_item);

    static bool BinlogAttributesDecode(const std::string& binlog,
                                       BinlogItem::Attributes* binlog_attributes);
};

} // namespace replication

#endif // STORAGE_PIKA_BINLOG_TRANSVERTER_H_
