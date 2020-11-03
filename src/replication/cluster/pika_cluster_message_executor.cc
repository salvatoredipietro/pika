// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/replication/cluster/pika_cluster_message_executor.h"

#include <glog/logging.h>

#include "include/util/hooks.h"
#include "include/replication/cluster/pika_cluster_repl_manager.h"
#include "include/replication/cluster/pika_cluster_rg_node.h"

namespace replication {

/*Message Handler*/
RaftMessageHandler::~RaftMessageHandler() {
  if (closure_ != nullptr) {
    // Self-deleting
    closure_->Run();
    closure_ = nullptr;
  }
  if (msg_ != nullptr) {
    delete msg_;
    msg_ = nullptr;
  }
}

bool RaftMessageHandler::isAppendRequest() {
  return msg_->type() == InnerMessage::RaftMessageType::kMsgAppend;
}

ReplicationGroupID RaftMessageHandler::GetMessageGroupID() {
  ReplicationGroupID group_id(msg_->group_id().table_name(), msg_->group_id().partition_id());
  return group_id;
}

bool RaftMessageHandler::isRequest(const InnerMessage::RaftMessageType& type) {
  return type == InnerMessage::RaftMessageType::kMsgHeartbeat
         || type == InnerMessage::RaftMessageType::kMsgAppend
         || type == InnerMessage::RaftMessageType::kMsgPreVote
         || type == InnerMessage::RaftMessageType::kMsgVote
         || type == InnerMessage::RaftMessageType::kMsgTimeoutNow
         || type == InnerMessage::RaftMessageType::kMsgSnapshot;
}

void RaftMessageHandler::Run() {
  PeerID peer_id(msg_->from().ip(), msg_->from().port());
  ReplicationGroupID group_id(msg_->group_id().table_name(),
                              msg_->group_id().partition_id());
  auto rg_node = rm_->GetReplicationGroupNode(group_id);
  if (rg_node == nullptr) {
    LOG(WARNING) << "Receive a raft message from " << peer_id.ToString()
                 << ", but the replication group " << group_id.ToString()
                 << " does not exist in local, ignore this message";
    return;
  }
  if (isRequest(msg_->type())) {
    // Pin the connections for later response
    rm_->PinServerStream(peer_id, closure_->Stream());
  }
  auto cluster_node = std::dynamic_pointer_cast<ClusterRGNode>(rg_node);
  if (cluster_node == nullptr) {
    return;
  }
  cluster_node->StepMessages(msg_);
}

/* ClusterMessageSender */

Status ClusterBinlogBuilder::Build(std::string* res) {
  InnerMessage::RaftMessage msg;
  msg.set_type(InnerMessage::RaftMessageType::kMsgAppend);
  msg.set_term(options_.term);
  auto from = msg.mutable_from();
  from->set_ip(options_.local_id.Ip());
  from->set_port(options_.local_id.Port());
  auto to = msg.mutable_to();
  to->set_ip(to_.Ip());
  to->set_port(to_.Port());
  auto group_id = msg.mutable_group_id();
  group_id->set_table_name(options_.group_id.TableName());
  group_id->set_partition_id(options_.group_id.PartitionID());

  LogOffset committed_offset;
  bool founded = false;
  for (auto& task : tasks_) {
    if (committed_offset.l_offset < task.data.committed_offset.l_offset) {
      committed_offset = task.data.committed_offset;
    }
    if (!founded) {
      BuildBinlogOffset(task.data.prev_offset, msg.mutable_prev_log());
      founded = true;
    }

    if (task.data.attrs.term_id != 0) {
      auto entry = msg.add_entries();
      entry->set_type(task.data.attrs.content_type == BinlogItem::Attributes::ContentType::TypeFirst
                      ? InnerMessage::EntryType::kEntryNormal
                      : InnerMessage::EntryType::kEntryConfChange);
      BuildBinlogAttributes(task.data.attrs, entry->mutable_attributes());
      entry->set_data(task.data.binlog);
    }
  }
  BuildBinlogOffset(committed_offset, msg.mutable_commit());
  if (!msg.SerializeToString(res)) {
    return Status::Corruption("Serialized Failed");
  }
  return Status::OK();
}

ClusterMessageSender::ClusterMessageSender(const ClusterMessageSenderOptions& options)
 : MessageSender(options),
 c_options_(options) { }

void ClusterMessageSender::Reset(const ClusterMessageSenderOptions& options) {
  std::lock_guard<std::mutex> guard(write_queue_mu_);
  c_options_ = options;
  UnsafeClear();
}

void ClusterMessageSender::ResetTerm(uint32_t term) {
  std::lock_guard<std::mutex> guard(write_queue_mu_);
  c_options_.term = term;
  UnsafeClear();
}

void ClusterMessageSender::Messages(PeerMessageListMap& msgs) {
  BinlogTaskListMap binlog_tasks;
  SimpleTaskListMap simple_tasks;
  BatchTasks(binlog_tasks, simple_tasks);

  for (auto& iter : binlog_tasks) {
    auto& task_vec = iter.second;
    if (task_vec.size() == 0) {
      continue;
    }
    const MessageType& type = task_vec.front().base.type;
    auto builder = util::make_unique<ClusterBinlogBuilder>(c_options_, iter.first, std::move(task_vec));
    PeerMessage msg(type, std::move(builder));
    msgs[iter.first].push_back(std::move(msg));
  }
  for (auto& iter : simple_tasks) {
    auto& task_vec = iter.second;
    auto& peer_msg = msgs[iter.first];
    for (auto& task : task_vec) {
      const MessageType& type = task.base.type;
      auto builder = util::make_unique<SimpleBuilder>(std::move(task));
      PeerMessage msg(type, std::move(builder));
      peer_msg.push_back(std::move(msg));
    }
  }
}

} // namespace replication
