// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/replication/pika_repl_message_executor.h"

#include <glog/logging.h>

namespace replication {

void ParseBinlogOffset(const InnerMessage::BinlogOffset& pb_offset,
                       LogOffset* offset) {
  offset->b_offset.filenum = pb_offset.filenum();
  offset->b_offset.offset = pb_offset.offset();
  offset->l_offset.term = pb_offset.term();
  offset->l_offset.index = pb_offset.index();
}

void BuildBinlogOffset(const LogOffset& offset,
                       InnerMessage::BinlogOffset* pb_offset) {
  pb_offset->set_filenum(offset.b_offset.filenum);
  pb_offset->set_offset(offset.b_offset.offset);
  pb_offset->set_term(offset.l_offset.term);
  pb_offset->set_index(offset.l_offset.index);
}

void ParseBinlogAttributes(const InnerMessage::BinlogAttributes& pb_attributes,
                           BinlogItem::Attributes* attributes) {
  attributes->exec_time = pb_attributes.exec_time();
  const auto& content_type = pb_attributes.content_type();
  if (content_type == static_cast<uint8_t>(BinlogItem::Attributes::ContentType::TypeFirst)) {
    attributes->content_type = BinlogItem::Attributes::ContentType::TypeFirst;
  } else {
    attributes->content_type = BinlogItem::Attributes::ContentType::TypeSecond;
  }
  attributes->term_id = pb_attributes.offset().term();
  attributes->logic_id = pb_attributes.offset().index();
  attributes->offset = pb_attributes.offset().offset();
  attributes->filenum = pb_attributes.offset().filenum();
}

void BuildBinlogAttributes(const BinlogItem::Attributes& attributes,
                           InnerMessage::BinlogAttributes* pb_attributes) {
  pb_attributes->set_exec_time(attributes.exec_time);
  pb_attributes->set_content_type(static_cast<uint8_t>(attributes.content_type));
  InnerMessage::BinlogOffset* pb_offset = pb_attributes->mutable_offset();
  pb_offset->set_filenum(attributes.filenum);
  pb_offset->set_offset(attributes.offset);
  pb_offset->set_term(attributes.term_id);
  pb_offset->set_index(attributes.logic_id);
}

/* MessageSender */

MessageSender::MessageSender(const MessageSenderOptions& options)
  : options_(options),
  pending_(false) {
  if (options_.batch_limit <= 0 || options_.batch_limit > kDefaultBatchLimit) {
    options_.batch_limit = kDefaultBatchLimit;
  }
}

bool MessageSender::Prepared() {
  std::lock_guard<std::mutex> guard(write_queue_mu_);
  return binlog_tasks_.size() > 0 || simple_tasks_.size() > 0;
}

void MessageSender::AppendSimpleTasks(const PeerID& peer_id, SimpleTaskList&& tasks) {
  std::lock_guard<std::mutex> guard(write_queue_mu_);
  UnsafeAppendSimpleTasks(peer_id, std::move(tasks));
}

void MessageSender::UnsafeAppendSimpleTasks(const PeerID& peer_id, SimpleTaskList&& tasks) {
  auto& peer_msg_list = simple_tasks_[peer_id]; 
  peer_msg_list.splice(peer_msg_list.end(), std::move(tasks));
  pending_.store(true, std::memory_order_relaxed);
}


void MessageSender::AppendBinlogTasks(const PeerID& peer_id, BinlogTaskList&& tasks) {
  std::lock_guard<std::mutex> guard(write_queue_mu_);
  UnsafeAppendBinlogTasks(peer_id, std::move(tasks));
}

void MessageSender::UnsafeAppendBinlogTasks(const PeerID& peer_id, BinlogTaskList&& tasks) {
  auto& peer_msg_list = binlog_tasks_[peer_id];
  peer_msg_list.splice(peer_msg_list.end(), std::move(tasks));
  pending_.store(true, std::memory_order_relaxed);
}

void MessageSender::BatchTasks(BinlogTaskListMap& binlog_tasks, SimpleTaskListMap& simple_tasks) {
  if (!pending_.load(std::memory_order_relaxed)) {
    return;
  }
  std::lock_guard<std::mutex> guard(write_queue_mu_);
  UnsafeBatchBinlogTasks(binlog_tasks);
  UnsafeBatchSimpleTasks(simple_tasks);
  if (binlog_tasks_.size() == 0 && simple_tasks_.size() == 0) {
    pending_.store(true, std::memory_order_relaxed);
  }
}

// REQUIRES: write_queue_mu_ must be held.
void MessageSender::UnsafeBatchBinlogTasks(BinlogTaskListMap& map) {
  for (auto& iter : binlog_tasks_) {
    auto& list = iter.second;
    size_t batch_index = list.size() > kBinlogSendBatchNum
                         ? kBinlogSendBatchNum : list.size();
    BinlogTaskList to_send;
    int batch_size = 0;
    for (size_t i = 0; i < batch_index; ++i) {
      auto& task = list.front();
      batch_size += task.data.size();
      if (batch_size > options_.batch_limit) {
        break;
      }
      to_send.push_back(std::move(task));
      list.pop_front();
    }
    if (to_send.size() != 0) {
      map.emplace(iter.first, std::move(to_send));
    }
  }
}

// REQUIRES: write_queue_mu_ must be held.
void MessageSender::UnsafeBatchSimpleTasks(SimpleTaskListMap& map) {
  for (auto& iter : simple_tasks_) {
    auto& list = iter.second;
    if (list.size() == 0) {
      continue;
    }
    map.emplace(iter.first, std::move(list));
  }
}

void MessageSender::Clear() {
  std::lock_guard<std::mutex> guard(write_queue_mu_);
  UnsafeClear();
}

void MessageSender::UnsafeClear() {
  binlog_tasks_.clear();
  simple_tasks_.clear();
  pending_.store(false, std::memory_order_release);
}

} // namespace replication
