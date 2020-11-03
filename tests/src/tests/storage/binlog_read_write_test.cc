// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <glog/logging.h>

#include "gtest/gtest.h"
#include "gmock/gmock.h"

#include "slash/include/slash_slice.h"
#include "slash/include/slash_status.h"

#include "include/pika_define.h"
#include "include/deps/storage_env.h"
#include "include/util/message_function.h"
#include "include/storage/pika_binlog.h"
#include "include/storage/pika_binlog_transverter.h"

namespace test {

using slash::Status;
BinlogEnv* binlog_env;

class TestBinlogReader
  : public ::testing::Test,
    public ::testing::WithParamInterface<int> {
 protected:
  TestBinlogReader();
  Status Initialize();
  void SetUp() override {
    item_number_ = GetParam();
  }
  void TearDown() override {
    std::string cmd_line = "rm -rf " + log_dir_;
    system(cmd_line.c_str());
  }
  using ItemPair = std::pair<storage::BinlogItem::Attributes/*item_attrs*/, std::string/*item_data*/>;
  using ItemPairVec = std::vector<ItemPair>;
  // Put data into storage(wrapped by builder)).
  // @return items_vec(out) : the items put into storage used to later check.
  slash::Status PutItemsIntoStore(ItemPairVec& items_vec);
  slash::Status PutItemIntoStore(const slash::Slice& data,
                                 storage::BinlogItem::Attributes* attrs);

  int item_number_;
  const std::string log_dir_;
  std::unique_ptr<storage::BinlogBuilder> builder_;
};

TestBinlogReader::TestBinlogReader()
  : log_dir_(binlog_env->binlog_dir()),
  builder_(nullptr) {
  builder_ = std::unique_ptr<storage::BinlogBuilder>(new storage::BinlogBuilder(log_dir_));

  Initialize();
}

Status TestBinlogReader::Initialize() {
  return builder_->Initialize();
}

slash::Status TestBinlogReader::PutItemsIntoStore(ItemPairVec& items_vec) {
  constexpr int kDefaultDataSize = 32/*32 Bytes*/;
  slash::Status s;
  items_vec.reserve(item_number_);
  for (int i = 0; i < item_number_; i++) {
    std::string data = RandomAlphaData(kDefaultDataSize);
    storage::BinlogItem::Attributes attrs;
    s = PutItemIntoStore(slash::Slice(data.c_str(), data.size()), &attrs);
    if (!s.ok()) {
      return s;
    }
    items_vec.push_back({attrs, std::move(data)});
  }
  return s;
}

slash::Status TestBinlogReader::PutItemIntoStore(const slash::Slice& data,
                                                 storage::BinlogItem::Attributes* attrs) {
  // 0. fetch attrs
  uint32_t filenum = 0, term = 0;
  uint64_t offset = 0, index = 0;
  auto s = builder_->GetProducerStatus(&filenum, &offset, &term, &index);
  if (!s.ok()) {
    return s;
  }
  attrs->exec_time = time(nullptr);
  attrs->term_id = term;
  attrs->logic_id = index + 1;
  attrs->filenum = filenum;
  attrs->offset = offset;

  // 1. produce item (combine data with attrs)
  std::string binlog = storage::PikaBinlogTransverter::BinlogEncode(attrs->exec_time,
      attrs->term_id, attrs->logic_id, attrs->filenum, attrs->offset,
      data, attrs->content_type, {});
  // 2. put item
  {
    builder_->Lock();
    s = builder_->Put(binlog);
    builder_->Unlock();
  }
  return s;
}

TEST_P(TestBinlogReader, PutAndRead) {
  // Put items into storage
  ItemPairVec items_vec;
  auto s = PutItemsIntoStore(items_vec);
  ASSERT_TRUE(s.ok()) << s.ToString();

  // Read Items from storage
  auto reader_for_indexer = std::unique_ptr<storage::BinlogReader>(
      new storage::BinlogReader(builder_->filename(), 0));
  storage::BinlogIndexer indexer;
  s = indexer.Initialize(std::move(reader_for_indexer));
  ASSERT_TRUE(s.ok()) << s.ToString();

  // Check indexes
  auto check_attrs_equal = [] (const storage::BinlogIndexer::Item& read_item,
                               const storage::BinlogItem::Attributes& put_item_attr) {
    return read_item.term == put_item_attr.term_id
           && read_item.index == put_item_attr.logic_id;
  };
  storage::BinlogIndexer::Item first_item;
  s = indexer.GetFirstItem(&first_item);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_TRUE(check_attrs_equal(first_item, items_vec.front().first));

  storage::BinlogIndexer::Item last_item;
  s = indexer.GetLastItem(&last_item);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_TRUE(check_attrs_equal(last_item, items_vec.back().first));

  const auto item_number = last_item.index - first_item.index + 1;
  ASSERT_EQ(items_vec.size(), item_number);

  // Check data consistency
  auto check_data_equal = [] (const std::string& read_data,
                              const std::string& put_data) {
    return read_data == put_data;
  };
  storage::BinlogReader reader(builder_->filename(), 0);
  s = reader.Open(storage::BinlogReader::OpenOptions());
  ASSERT_TRUE(s.ok()) << s.ToString();

  size_t index_in_items_vec = 0;
  for (auto index = first_item.index; index <= last_item.index; index++, index_in_items_vec++) {
    LOG(INFO) << "Index: " << index;
    uint32_t read_item_filenum;
    uint64_t read_item_end_offset;
    std::string read_item_encoded_data;
    s = reader.Consume(&read_item_encoded_data, &read_item_filenum, &read_item_end_offset);
    ASSERT_TRUE(s.ok()) << s.ToString();

    storage::BinlogIndexer::OffsetPair offset_pair;
    s = indexer.GetOffset(index, &offset_pair);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_EQ(read_item_end_offset, offset_pair.end_offset);

    storage::BinlogItem read_decoded_item;
    ASSERT_TRUE(storage::PikaBinlogTransverter::BinlogDecode(read_item_encoded_data, &read_decoded_item));
    ASSERT_TRUE(check_data_equal(read_decoded_item.content(), items_vec[index_in_items_vec].second));
    uint32_t term;
    s = indexer.GetTerm(index, &term);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_EQ(term, read_decoded_item.term_id());
    ASSERT_EQ(index, read_decoded_item.logic_id());
  }
}

INSTANTIATE_TEST_SUITE_P(DifferentItemNumber, TestBinlogReader, ::testing::Values(1, 10, 100, 1000));

} // namespace test

int main(int argc, char** argv) {
  test::binlog_env = static_cast<test::BinlogEnv*>(
  ::testing::AddGlobalTestEnvironment(new test::BinlogEnv(argc, argv)));
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
