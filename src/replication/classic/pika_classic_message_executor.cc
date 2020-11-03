// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/replication/classic/pika_classic_message_executor.h"

#include <vector>
#include <glog/logging.h>

#include "include/util/callbacks.h"
#include "include/util/hooks.h"
#include "include/replication/classic/pika_classic_rg_node.h"
#include "include/replication/classic/pika_classic_repl_manager.h"

namespace replication {

/*Message Handlers*/

void MetaSyncRequestHandler::Run() {
  InnerMessage::InnerRequest::MetaSync meta_sync_request = request_->meta_sync();
  InnerMessage::Node node = meta_sync_request.node();
  std::string masterauth = meta_sync_request.has_auth() ? meta_sync_request.auth() : "";

  InnerMessage::InnerResponse response;
  response.set_type(InnerMessage::kMetaSync);
  if (!rm_->OptionsStorage()->requirepass().empty()
    && rm_->OptionsStorage()->requirepass() != masterauth) {
    response.set_code(InnerMessage::kError);
    response.set_reply("Auth with master error, Invalid masterauth");
  } else {
    LOG(INFO) << "Receive MetaSync, Slave ip: " << node.ip() << ", Slave port:" << node.port();
    std::vector<TableStruct> table_structs = rm_->OptionsStorage()->table_structs();

    bool success = rm_->HandleMetaSyncRequest(PeerID(node.ip(), node.port()),
                                              closure_->Stream(),
                                              table_structs);
    if (!success) {
      response.set_code(InnerMessage::kOther);
      response.set_reply("Slave AlreadyExist");
    } else {
      response.set_code(InnerMessage::kOk);
      InnerMessage::InnerResponse_MetaSync* meta_sync = response.mutable_meta_sync();
      meta_sync->set_classic_mode(rm_->OptionsStorage()->classic_mode());
      for (const auto& table_struct : table_structs) {
        InnerMessage::InnerResponse_MetaSync_TableInfo* table_info = meta_sync->add_tables_info();
        table_info->set_table_name(table_struct.table_name);
        table_info->set_partition_num(table_struct.partition_num);
      }
    }
  }

  std::string reply_str;
  bool should_response = response.SerializeToString(&reply_str);
  closure_->SetResponse(std::move(reply_str), should_response);
}

void TrySyncRequestHandler::Run() {
  InnerMessage::InnerRequest::TrySync try_sync_request = request_->try_sync();
  InnerMessage::Partition partition_request = try_sync_request.partition();
  InnerMessage::BinlogOffset slave_boffset = try_sync_request.binlog_offset();
  InnerMessage::Node node = try_sync_request.node();
  std::string table_name = partition_request.table_name();
  uint32_t partition_id = partition_request.partition_id();

  InnerMessage::InnerResponse response;
  InnerMessage::InnerResponse::TrySync* try_sync_response = response.mutable_try_sync();
  try_sync_response->set_reply_code(InnerMessage::InnerResponse::TrySync::kError);
  InnerMessage::Partition* partition_response = try_sync_response->mutable_partition();
  partition_response->set_table_name(table_name);
  partition_response->set_partition_id(partition_id);

  response.set_type(InnerMessage::Type::kTrySync);

  std::shared_ptr<ReplicationGroupNode> rg_node = rm_->GetReplicationGroupNode(
      ReplicationGroupID(table_name, partition_id));
  auto classic_rg_node = std::dynamic_pointer_cast<ClassicRGNode>(rg_node);
  if (classic_rg_node == nullptr) {
    response.set_code(InnerMessage::kError);
    response.set_reply("Partition not found");
    LOG(WARNING) << "Table Name: " << table_name << " Partition ID: "
                 << partition_id << " Not Found, TrySync Error";
  } else {
    LOG(INFO) << "Table Name: " << table_name << " Partition ID: " << partition_id
              << " Receive Trysync, Follower ip: " << node.ip() << ", port:" << node.port()
              << ", filenum: " << slave_boffset.filenum() << ", pro_offset: " << slave_boffset.offset();
    response.set_code(InnerMessage::kOk);

    if (classic_rg_node->HandleTrySyncRequest(request_, &response)) {
      rm_->PinServerStream(PeerID(node.ip(), node.port()), closure_->Stream());
    }
  }

  std::string reply_str;
  bool should_response = response.SerializeToString(&reply_str);
  if (!should_response) {
    LOG(WARNING) << "Handle Try Sync Failed";
  }
  closure_->SetResponse(std::move(reply_str), should_response);
}

void SnapshotSyncRequestHandler::Run() {
  InnerMessage::InnerRequest::SnapshotSync snapshot_sync_request = request_->snapshot_sync();
  InnerMessage::Partition partition_request = snapshot_sync_request.partition();
  InnerMessage::BinlogOffset slave_boffset = snapshot_sync_request.binlog_offset();
  std::string table_name = partition_request.table_name();
  uint32_t partition_id = partition_request.partition_id();
  std::string partition_name = table_name + "_" + std::to_string(partition_id);

  InnerMessage::InnerResponse response;
  response.set_code(InnerMessage::kOk);
  response.set_type(InnerMessage::Type::kSnapshotSync);
  InnerMessage::InnerResponse::SnapshotSync* snapshot_sync_response = response.mutable_snapshot_sync();
  InnerMessage::Partition* partition_response = snapshot_sync_response->mutable_partition();
  partition_response->set_table_name(table_name);
  partition_response->set_partition_id(partition_id);

  LOG(INFO) << "Handle partition SnapshotSync Request";

  std::shared_ptr<ReplicationGroupNode> rg_node = rm_->GetReplicationGroupNode(
      ReplicationGroupID(table_name, partition_id));
  auto classic_rg_node = std::dynamic_pointer_cast<ClassicRGNode>(rg_node);
  if (classic_rg_node == nullptr) {
    LOG(WARNING) << "ReplicationGroupNode Partition: " << partition_name
                 << ", NotFound";
  } else {
    bool update_connection = classic_rg_node->HandleSnapshotSyncRequest(request_, &response);
    if (update_connection) {
      InnerMessage::Node node = snapshot_sync_request.node();
      rm_->PinServerStream(PeerID(node.ip(), node.port()), closure_->Stream());
    }
  }

  std::string reply_str;
  bool should_response = response.SerializeToString(&reply_str);
  if (!should_response) {
    LOG(WARNING) << "Handle SnapshotSync Failed";
  }
  closure_->SetResponse(std::move(reply_str), should_response);
}

void BinlogSyncRequestHandler::Run() {
  if (!request_->has_binlog_sync()) {
    LOG(WARNING) << "Pb parse error";
    return;
  }
  const InnerMessage::InnerRequest::BinlogSync& binlog_req = request_->binlog_sync();
  const std::string& table_name = binlog_req.table_name();
  uint32_t partition_id = binlog_req.partition_id();

  const InnerMessage::BinlogOffset& ack_range_start = binlog_req.ack_range_start();
  const InnerMessage::BinlogOffset& ack_range_end = binlog_req.ack_range_end();
  BinlogOffset b_range_start(ack_range_start.filenum(), ack_range_start.offset());
  BinlogOffset b_range_end(ack_range_end.filenum(), ack_range_end.offset());
  LogicOffset l_range_start(ack_range_start.term(), ack_range_start.index());
  LogicOffset l_range_end(ack_range_end.term(), ack_range_end.index());
  LogOffset range_start(b_range_start, l_range_start);
  LogOffset range_end(b_range_end, l_range_end);

  std::shared_ptr<ReplicationGroupNode> rg_node = rm_->GetReplicationGroupNode(
      ReplicationGroupID(table_name, partition_id));
  auto classic_rg_node = std::dynamic_pointer_cast<ClassicRGNode>(rg_node);
  if (classic_rg_node == nullptr) {
    LOG(WARNING) << "Replication group: " << table_name << ":" << partition_id
                 << ", NotFound";
    return;
  }

  bool close_connection = classic_rg_node->HandleBinlogSyncRequest(request_, nullptr);
  if (close_connection) {
    LOG(WARNING) << "Replication group: " << table_name << ":" << partition_id
                 << ", HandleBinlogSyncRequest failed, close the connection.";
    closure_->NotifyClose();
  }
  return;
}

void RemoveSlaveNodeRequestHandler::Run() {
  if (!request_->remove_slave_node_size()) {
    LOG(WARNING) << "Pb parse error";
    closure_->NotifyClose();
    return;
  }
  const InnerMessage::InnerRequest::RemoveSlaveNode& remove_slave_node_req = request_->remove_slave_node(0);
  const InnerMessage::Node& node = remove_slave_node_req.node();
  const InnerMessage::Partition& partition = remove_slave_node_req.partition();

  ReplicationGroupID group_id(partition.table_name(), partition.partition_id());
  auto rg_node = rm_->GetReplicationGroupNode(group_id);
  auto classic_rg_node = std::dynamic_pointer_cast<ClassicRGNode>(rg_node);
  if (classic_rg_node == nullptr) {
    LOG(WARNING) << "Replication group: " << group_id.ToString()
                 << ", NotFound";
  }
  InnerMessage::InnerResponse response;
  classic_rg_node->HandleRemoveNodeRequest(PeerID(node.ip(), node.port()),
                                           &response);
  std::string reply_str;
  bool should_response = response.SerializeToString(&reply_str);
  if (!should_response) {
    LOG(WARNING) << "Remove Slave Node Failed";
  }
  closure_->SetResponse(std::move(reply_str), should_response);
}

void MetaSyncResponseHandler::Run() {
  if (response_->code() == InnerMessage::kOther) {
    std::string reply = response_->has_reply() ? response_->reply() : "";
    // keep sending MetaSync
    LOG(WARNING) << "Meta Sync Failed: " << reply << " will keep sending MetaSync msg";
    return;
  }

  if (response_->code() != InnerMessage::kOk) {
    std::string reply = response_->has_reply() ? response_->reply() : "";
    LOG(WARNING) << "Meta Sync Failed: " << reply;
    rm_->MetaSyncError();
    closure_->NotifyClose();
    return;
  }

  const InnerMessage::InnerResponse_MetaSync meta_sync = response_->meta_sync();
  if (rm_->OptionsStorage()->classic_mode() != meta_sync.classic_mode()) {
    LOG(WARNING) << "Self in " << (rm_->OptionsStorage()->classic_mode() ? "classic" : "sharding")
                 << " mode, but master in " << (meta_sync.classic_mode() ? "classic" : "sharding")
                 << " mode, failed to establish master-slave relationship";
    rm_->MetaSyncError();
    closure_->NotifyClose();
    return;
  }

  std::vector<TableStruct> master_table_structs;
  for (int idx = 0; idx < meta_sync.tables_info_size(); ++idx) {
    InnerMessage::InnerResponse_MetaSync_TableInfo table_info = meta_sync.tables_info(idx);
    master_table_structs.push_back({table_info.table_name(),
            static_cast<uint32_t>(table_info.partition_num()), {0}});
  }

  std::vector<TableStruct> self_table_structs = rm_->OptionsStorage()->table_structs();
  if (!IsTableStructConsistent(self_table_structs, master_table_structs)) {
    LOG(WARNING) << "Self table structs(number of databases: " << self_table_structs.size()
                 << ") inconsistent with master(number of databases: " << master_table_structs.size()
                 << "), failed to establish master-slave relationship";
    rm_->MetaSyncError();
    closure_->NotifyClose();
    return;
  }

  rm_->HandleMetaSyncResponse();
  LOG(INFO) << "Finish to handle meta sync response";
}

bool MetaSyncResponseHandler::IsTableStructConsistent(
    const std::vector<TableStruct>& current_tables,
    const std::vector<TableStruct>& expect_tables) {
  if (current_tables.size() != expect_tables.size()) {
    return false;
  }
  for (const auto& table_struct : current_tables) {
    if (find(expect_tables.begin(), expect_tables.end(),
                table_struct) == expect_tables.end()) {
      return false;
    }
  }
  return true;
}


void SnapshotSyncResponseHandler::Run() {
  std::shared_ptr<ReplicationGroupNode> rg_node = rm_->GetReplicationGroupNode(group_id_);
  auto classic_rg_node = std::dynamic_pointer_cast<ClassicRGNode>(rg_node);
  if (classic_rg_node == nullptr) {
    LOG(WARNING) << "Replication group: " << group_id_.ToString() <<" Not Found";
    return;
  }
  classic_rg_node->HandleSnapshotSyncResponse(response_);
}

void TrySyncResponseHandler::Run() {
  if (response_->code() != InnerMessage::kOk) {
    std::string reply = response_->has_reply() ? response_->reply() : "";
    LOG(WARNING) << "Replication group: " << group_id_.ToString() << " TrySync Failed: " << reply;
    return;
  }

  std::shared_ptr<ReplicationGroupNode> rg_node = rm_->GetReplicationGroupNode(group_id_);
  auto classic_rg_node = std::dynamic_pointer_cast<ClassicRGNode>(rg_node);
  if (classic_rg_node == nullptr) {
    LOG(WARNING) << "Replication group: " << group_id_.ToString() << " Not Found";
    return;
  }

  classic_rg_node->HandleTrySyncResponse(response_);
}

void BinlogAppendHandler::Run() {
  std::shared_ptr<ReplicationGroupNode> rg_node = rm_->GetReplicationGroupNode(group_id_);
  auto classic_rg_node = std::dynamic_pointer_cast<ClassicRGNode>(rg_node);
  if (classic_rg_node == nullptr) {
    LOG(WARNING) << "Replication group: " << group_id_.ToString() << " Not found.";
    return;
  }
  classic_rg_node->HandleBinlogSyncResponse(peer_id_, indexes_, response_);
}


void RemoveSlaveNodeResponseHandler::Run() {
  if (response_->code() != InnerMessage::kOk) {
    std::string reply = response_->has_reply() ? response_->reply() : "";
    LOG(WARNING) << "Remove slave node Failed: " << reply;
    return;
  }
}

/* ClassicMessageSender */

Status ClassicBinlogBuilder::Build(std::string* res) {
  InnerMessage::InnerResponse response;
  BuildBinlogSyncResp(tasks_, &response);
  if (!response.SerializeToString(res)) {
    return Status::Corruption("Serialized Failed");
  }
  return Status::OK();
}

Status ClassicBinlogBuilder::BuildBinlogSyncResp(BinlogTaskList& tasks,
                                                 InnerMessage::InnerResponse* response) {
  response->set_code(InnerMessage::kOk);
  response->set_type(InnerMessage::Type::kBinlogSync);
  LogOffset pending_offset;
  for (const auto& task : tasks) {
    InnerMessage::InnerResponse::BinlogSync* binlog_sync = response->add_binlog_sync();
    binlog_sync->set_session_id(task.base.session_id);
    InnerMessage::Partition* partition = binlog_sync->mutable_partition();
    partition->set_table_name(task.base.group_id.TableName());
    partition->set_partition_id(task.base.group_id.PartitionID());
    pending_offset.b_offset.filenum = task.data.attrs.filenum;
    pending_offset.b_offset.offset = task.data.attrs.offset;
    pending_offset.l_offset.index = task.data.attrs.logic_id;
    BuildBinlogOffset(pending_offset, binlog_sync->mutable_binlog_offset());
    binlog_sync->mutable_binlog()->assign(std::move(task.data.binlog));
  }
  return Status::OK();
}

ClassicMessageSender::ClassicMessageSender(const MessageSenderOptions& options)
  : MessageSender(options) {
}

void ClassicMessageSender::Messages(PeerMessageListMap& msgs) {
  BinlogTaskListMap binlog_tasks;
  SimpleTaskListMap simple_tasks;
  BatchTasks(binlog_tasks, simple_tasks);

  for (auto& iter : binlog_tasks) {
    auto& task_vec = iter.second;
    if (task_vec.size() == 0) {
      continue;
    }
    const MessageType& type = task_vec.front().base.type;
    auto builder = util::make_unique<ClassicBinlogBuilder>(std::move(task_vec));
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

