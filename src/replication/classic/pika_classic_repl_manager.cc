// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/replication/classic/pika_classic_repl_manager.h"
#include "include/replication/classic/pika_classic_io_stream.h"
#include "include/replication/classic/pika_classic_rg_node.h"

namespace replication {

ClassicReplManager::ClassicReplManager(const ReplicationManagerOptions& options)
  : ReplicationManager(options, std::move(TransporterOptions(options.options_store->local_id(), this,
                                new ClassicClientStreamFactory(this, options.options_store->max_conn_rbuf_size()),
                                new ClassicServerStreamFactory(this)))),
  master_slave_controller_(nullptr) {
  if (options.options_store->classic_mode()) {
    master_slave_controller_ = new MasterSlaveController(this);
  }
}

ClassicReplManager::~ClassicReplManager() {
  if (master_slave_controller_ != nullptr) {
    delete master_slave_controller_;
    master_slave_controller_ = nullptr;
  }
}

int ClassicReplManager::Start() {
  int res = 0;
  if (master_slave_controller_ != nullptr) {
    res = master_slave_controller_->Start();
    if (0 != res) {
      LOG(ERROR) << "Start MasterSlaveController failed";
      return res;
    }
  }
  return ReplicationManager::Start();
}

int ClassicReplManager::Stop() {
  int res = 0;
  res = executor_.Stop(true);
  if (0 != res) {
    LOG(ERROR) << "Stop message executor failed";
    return res;
  }
  if (master_slave_controller_ != nullptr) {
    res = master_slave_controller_->Stop();
    if (0 != res) {
      LOG(ERROR) << "Stop MasterSlaveController failed";
      return res;
    }
  }
  return ReplicationManager::Stop();
}

void ClassicReplManager::InfoReplication(std::string& info_str) {
  if (master_slave_controller_ != nullptr) {
    master_slave_controller_->InfoReplication(info_str);
  }
}

void ClassicReplManager::GetMasterSyncState(std::stringstream& out_of_sync) {
  auto nodes = CurrentReplicationGroups();
  for (auto& node : nodes) {
    auto classic_node = std::dynamic_pointer_cast<ClassicRGNode>(node);
    classic_node->GetMasterSyncState(out_of_sync);
  }
}

bool ClassicReplManager::GetSlaveSyncState(std::stringstream& out_of_sync) {
  auto nodes = CurrentReplicationGroups();
  bool all_in_syncing = true;
  for (auto& node : nodes) {
    auto classic_node = std::dynamic_pointer_cast<ClassicRGNode>(node);
    if (!classic_node->GetSlaveSyncState(out_of_sync)) {
      all_in_syncing = false;
    }
  }
  return all_in_syncing;
}

bool ClassicReplManager::IsReadonly(const ReplicationGroupID& group_id) {
  if (master_slave_controller_ != nullptr) {
    return master_slave_controller_->IsReadonly();
  }
  return ReplicationManager::IsReadonly(group_id);
}

bool ClassicReplManager::IsMaster() {
  if (master_slave_controller_ != nullptr) {
    return master_slave_controller_->IsMaster();
  }
  return ReplicationManager::IsMaster();
}

bool ClassicReplManager::IsSlave() {
  if (master_slave_controller_ != nullptr) {
    return master_slave_controller_->IsSlave();
  }
  return ReplicationManager::IsSlave();
}

void ClassicReplManager::CurrentMaster(std::string& master_ip,
                                       int& master_port) {
  if (master_slave_controller_ != nullptr) {
    master_slave_controller_->CurrentMaster(master_ip, master_port);
  }
}

Status ClassicReplManager::SetMaster(const std::string& master_ip,
                                     int master_port,
                                     bool force_full_sync) {
  if (master_slave_controller_ != nullptr) {
    return master_slave_controller_->SetMaster(master_ip, master_port, force_full_sync);
  }
  return ReplicationManager::SetMaster(master_ip, master_port, force_full_sync);
}

void ClassicReplManager::SetMasterForRGNodes(const PeerID& master_id,
                                             bool force_full_sync) {
  Status s;
  std::shared_ptr<ClassicRGNode> classic_node = nullptr;
  auto nodes = CurrentReplicationGroups();
  for (const auto& node : nodes) {
    classic_node = std::dynamic_pointer_cast<ClassicRGNode>(node);
    s = classic_node->SetMaster(master_id, force_full_sync);
    if (!s.ok()) {
      LOG(WARNING) << s.ToString();
    }
  }
}

Status ClassicReplManager::DisableMasterForReplicationGroup(const ReplicationGroupID& group_id)  {
  if (!options_.options_store->classic_mode()) {
    return Status::NotSupported("non-classic-mode");
  }
  auto node = GetReplicationGroupNode(group_id);
  if (node == nullptr) {
    return Status::NotFound(group_id.ToString());
  }
  auto classic_node = std::dynamic_pointer_cast<ClassicRGNode>(node);
  return classic_node->DisableReplication();
}

Status ClassicReplManager::EnableMasterForReplicationGroup(const ReplicationGroupID& group_id,
    bool force_full_sync, bool reset_file_offset, uint32_t filenum, uint64_t offset) {
  if (!options_.options_store->classic_mode()) {
    return Status::NotSupported("non-classic-mode");
  }
  auto node = GetReplicationGroupNode(group_id);
  if (node == nullptr) {
    return Status::NotFound(group_id.ToString());
  }
  auto classic_node = std::dynamic_pointer_cast<ClassicRGNode>(node);
  return classic_node->EnableReplication(force_full_sync, reset_file_offset, filenum, offset);
}

Status ClassicReplManager::ResetMaster(const std::set<ReplicationGroupID>& group_ids,
    const PeerID& master_id, bool force_full_sync) {
  if (options_.options_store->classic_mode()) {
    return Status::NotSupported("classic_mode");
  }
  Status s;
  std::shared_ptr<ClassicRGNode> classic_node = nullptr;
  for (const auto& group_id : group_ids) {
    classic_node = std::dynamic_pointer_cast<ClassicRGNode>(GetReplicationGroupNode(group_id));
    if (classic_node == nullptr) {
      return Status::NotFound(group_id.ToString());
    }
    s = classic_node->ResetMaster(master_id, force_full_sync);
    if (!s.ok()) {
      break;
    }
  }
  return s;
}

Status ClassicReplManager::RemoveMaster() {
  if (master_slave_controller_ != nullptr) {
    std::string cur_master_ip;
    int cur_master_port;
    master_slave_controller_->RemoveMaster(cur_master_ip, cur_master_port);

    PeerID cur_master_id(cur_master_ip, cur_master_port);
    if (!cur_master_id.Empty()) {
      RemoveMasterForRGNodes(cur_master_id);
      transporter_->RemovePeer(cur_master_id);
      LOG(INFO) << "Remove Master Success, ip_port: " << cur_master_id.ToString();
    }
  }
  return Status::OK();
}

void ClassicReplManager::RemoveMasterForRGNodes(const PeerID& master_id) {
  Status s;
  std::shared_ptr<ClassicRGNode> classic_node = nullptr;
  auto nodes = CurrentReplicationGroups();
  for (const auto& node : nodes) {
    classic_node = std::dynamic_pointer_cast<ClassicRGNode>(node);
    s = classic_node->RemoveMaster(master_id);
    if (!s.ok()) {
      LOG(WARNING) << s.ToString();
    }
  }
}

void ClassicReplManager::handlePeerMessage(MessageReporter::Message* peer_msg) {
  if (peer_msg == nullptr) {
    LOG(WARNING) << "ClassicReplManager receive an empty peer message";
    return;
  }
  InnerMessage::ProtocolMessage* proto_msg = peer_msg->proto_msg();
  if (proto_msg == nullptr) {
    LOG(WARNING) << "ClassicReplManager receive an empty protocol message";
    return;
  }
  if (proto_msg->proto_type() != InnerMessage::ProtocolMessage::kClassicType) {
    LOG(WARNING) << "ClassicReplManager receive a non-classic protocol message";
    return;
  }
  InnerMessage::ClassicMessage* classic_msg = proto_msg->release_classic_msg();
  switch (classic_msg->msg_type()) {
    case InnerMessage::ClassicMessage::kRequestType: {
      HandleRequest(peer_msg->peer_id(), classic_msg->release_request(), peer_msg->release_closure());
      break;
    }
    case InnerMessage::ClassicMessage::kResponseType: {
      HandleResponse(peer_msg->peer_id(), classic_msg->release_response(), peer_msg->release_closure());
      break;
    }
  }
  return;
}

void ClassicReplManager::HandleRequest(const PeerID& peer_id, InnerMessage::InnerRequest* req, IOClosure* closure) {
  switch (req->type()) {
    case InnerMessage::kMetaSync:
    {
      auto meta_sync_request_handler = std::make_shared<MetaSyncRequestHandler>(peer_id, req, closure, this);
      executor_.ScheduleCompetitiveTask(std::static_pointer_cast<util::thread::Task>(meta_sync_request_handler));
      break;
    }
    case InnerMessage::kTrySync:
    {
      auto try_sync_request_handler = std::make_shared<TrySyncRequestHandler>(peer_id, req, closure, this);
      executor_.ScheduleCompetitiveTask(std::static_pointer_cast<util::thread::Task>(try_sync_request_handler));
      break;
    }
    case InnerMessage::kSnapshotSync:
    {
      auto snapshot_sync_request_handler = std::make_shared<SnapshotSyncRequestHandler>(peer_id, req, closure, this);
      executor_.ScheduleCompetitiveTask(std::static_pointer_cast<util::thread::Task>(snapshot_sync_request_handler));
      break;
    }
    case InnerMessage::kBinlogSync:
    {
      auto binlog_sync_request_handler = std::make_shared<BinlogSyncRequestHandler>(peer_id, req, closure, this);
      executor_.ScheduleCompetitiveTask(std::static_pointer_cast<util::thread::Task>(binlog_sync_request_handler));
      break;
    }
    case InnerMessage::kRemoveSlaveNode:
    {
      auto remove_slave_node_request_handler = std::make_shared<RemoveSlaveNodeRequestHandler>(peer_id, req, closure, this);
      executor_.ScheduleCompetitiveTask(std::static_pointer_cast<util::thread::Task>(remove_slave_node_request_handler));
      break;
    }
    default:
      break;
  }
}

void ClassicReplManager::HandleResponse(const PeerID& peer_id, InnerMessage::InnerResponse* response,
                                        IOClosure* closure) {
  if (response == nullptr) {
    MetaSyncError();
    return;
  }
  // All responses with the same group_id processed serially by schedule them into
  // the same executor.
  switch (response->type()) {
    case InnerMessage::kMetaSync:
    {
      auto meta_sync_response_handler = std::make_shared<MetaSyncResponseHandler>(peer_id, response, closure, this);
      executor_.ScheduleTask(std::static_pointer_cast<util::thread::Task>(meta_sync_response_handler));
      break;
    }
    case InnerMessage::kSnapshotSync:
    {
      const InnerMessage::InnerResponse_SnapshotSync snapshot_sync_response = response->snapshot_sync();
      const InnerMessage::Partition partition_response = snapshot_sync_response.partition();
      ReplicationGroupID group_id(partition_response.table_name(), partition_response.partition_id());
      auto snapshot_sync_response_handler = std::make_shared<SnapshotSyncResponseHandler>(group_id, peer_id, response, closure, this);
      executor_.ScheduleTask(std::static_pointer_cast<util::thread::Task>(snapshot_sync_response_handler), group_id.ToString());
      break;
    }
    case InnerMessage::kTrySync:
    {
      const InnerMessage::InnerResponse_TrySync& try_sync_response = response->try_sync();
      const InnerMessage::Partition& partition_response = try_sync_response.partition();
      ReplicationGroupID group_id(partition_response.table_name(), partition_response.partition_id());
      auto try_sync_response_handler = std::make_shared<TrySyncResponseHandler>(group_id, peer_id, response, closure, this);
      executor_.ScheduleTask(std::static_pointer_cast<util::thread::Task>(try_sync_response_handler), group_id.ToString());
      break;
    }
    case InnerMessage::kBinlogSync:
    {
      DispatchBinlogResponse(peer_id, response, closure);
      break;
    }
    case InnerMessage::kRemoveSlaveNode:
    {
      auto remove_slave_node_response_handler = std::make_shared<RemoveSlaveNodeResponseHandler>(peer_id, response, closure, this);
      executor_.ScheduleTask(std::static_pointer_cast<util::thread::Task>(remove_slave_node_response_handler));
      break;
    }
    default:
      break;
  }
  return;
}

void ClassicReplManager::DispatchBinlogResponse(const PeerID& peer_id, InnerMessage::InnerResponse* response,
                                                IOClosure* closure) {
  // partition to a bunch of binlog chips
  std::unordered_map<ReplicationGroupID, std::vector<int>*, hash_replication_group_id> par_binlog;
  for (int i = 0; i < response->binlog_sync_size(); ++i) {
    const InnerMessage::InnerResponse::BinlogSync& binlog_res = response->binlog_sync(i);
    // hash key: table + partition_id
    ReplicationGroupID p_info(binlog_res.partition().table_name(),
                              binlog_res.partition().partition_id());
    if (par_binlog.find(p_info) == par_binlog.end()) {
      par_binlog[p_info] = new std::vector<int>();
    }
    par_binlog[p_info]->push_back(i);
  }
  for (auto& binlog_nums : par_binlog) {
    ReplicationGroupID group_id(binlog_nums.first.TableName(), binlog_nums.first.PartitionID());
    auto rg_node = GetReplicationGroupNode(group_id);
    auto classic_rg_node = std::dynamic_pointer_cast<ClassicRGNode>(rg_node);
    if (classic_rg_node == nullptr) {
      LOG(WARNING) << "Replication group: " << group_id.ToString() << " not exist";
      break;
    }
    auto binlog_append_handler = std::make_shared<BinlogAppendHandler>(
        group_id, peer_id, response, binlog_nums.second, closure, this);
    executor_.ScheduleTask(std::static_pointer_cast<util::thread::Task>(
          binlog_append_handler), group_id.ToString());
  }
}

void ClassicReplManager::reportTransportResult(const PeerInfo& peer_info,
                                               const MessageReporter::ResultType& r_type,
                                               const MessageReporter::DirectionType& d_type) {
  switch (r_type) {
    case MessageReporter::ResultType::kFdTimedout:
    case MessageReporter::ResultType::kFdClosed: {
      if (master_slave_controller_ != nullptr) {
        master_slave_controller_->ReportUnreachable(peer_info);
      }
      ReplicationManager::ReportUnreachable(peer_info.peer_id, d_type);
      break;
    }
    case MessageReporter::ResultType::kResponseParseError: {
      if (master_slave_controller_ != nullptr) {
        master_slave_controller_->SyncError();
      }
      break;
    case ResultType::kOK:
    default:
      break;
    }
  }
}

bool ClassicReplManager::HandleMetaSyncRequest(const PeerID& peer_id,
                                               std::shared_ptr<pink::PbConn> stream,
                                               std::vector<TableStruct>& table_structs) {
  if (master_slave_controller_ != nullptr) {
    transporter_->PinServerStream(peer_id, std::dynamic_pointer_cast<ServerStream>(stream));
    table_structs = options_.options_store->table_structs();
    return master_slave_controller_->HandleMetaSyncRequest(peer_id, stream->fd(), table_structs);
  }
  return false;
}

void ClassicReplManager::HandleMetaSyncResponse() {
  if (master_slave_controller_ != nullptr) {
    master_slave_controller_->HandleMetaSyncResponse();
  }
}

void ClassicReplManager::StepStateMachine() {
  if (master_slave_controller_ != nullptr) {
    master_slave_controller_->StepStateMachine();
  }
  ReplicationManager::StepStateMachine();
}

int ClassicReplManager::SendToPeer() {
  int sent = 0;
  if (master_slave_controller_ != nullptr) {
    PeerMessageListMap msgs;
    master_slave_controller_->Messages(msgs);
    if (msgs.size() > 0) {
      auto s = transporter_->WriteToPeer(std::move(msgs));
      if (!s.ok()) {
        LOG(ERROR) << "Write to peer failed: " + s.ToString();
      } else {
        sent++;
      }
    }
  }
  sent += ReplicationManager::SendToPeer();
  return sent;
}

void ClassicReplManager::MetaSyncError() {
  if (master_slave_controller_ != nullptr) {
    master_slave_controller_->SyncError();
  }
}

std::string ClassicReplManager::MakeLogPath(const ReplicationGroupID& group_id) {
  const auto& base_path = options_.options_store->log_path();
  auto table_path = base_path + "log_" + group_id.TableName() + "/";
  if (options_.options_store->classic_mode()) {
    return std::move(table_path);
  }
  auto partition_path = table_path + std::to_string(group_id.PartitionID()) + "/";
  return std::move(partition_path);
}

Status ClassicReplManager::createReplicationGroupNodes(const GroupIDAndInitConfMap& id_init_conf_map) {
  Status s;
  slash::RWLock l(&rg_rw_, true);
  for (const auto& iter : id_init_conf_map) {
    const auto& group_id = iter.first;
    const auto& init_conf = iter.second;
    ClassicRGNodeOptions options;
    options.group_id = group_id;
    options.local_id = options_.options_store->local_id();

    SlaveContextOptions::ConnectOptions slave_connect_options;
    // When in classic_mode, master_slave_controller_ will trigger the reconnect operation.
    slave_connect_options.reconnect_when_master_gone = !options_.options_store->classic_mode();
    slave_connect_options.reconnect_interval_ms = options_.options_store->reconnect_master_interval_ms();
    slave_connect_options.receive_timeout_ms = options_.options_store->sync_receive_timeout_ms();
    slave_connect_options.send_timeout_ms = options_.options_store->sync_send_timeout_ms();

    LogManagerOptions log_options;
    log_options.group_id = group_id;
    log_options.stable_log_options.group_id = group_id;
    log_options.stable_log_options.log_path = MakeLogPath(group_id);
    log_options.stable_log_options.max_binlog_size = options_.options_store->binlog_file_size();
    log_options.stable_log_options.retain_logs_num = options_.options_store->retain_logs_num();

    ProgressOptions progress_options;
    progress_options.max_inflight_number = options_.options_store->sync_window_size();
    progress_options.receive_timeout_ms = options_.options_store->sync_receive_timeout_ms();
    progress_options.send_timeout_ms = options_.options_store->sync_send_timeout_ms();

    options.slave_connect_options = slave_connect_options;
    options.progress_options = progress_options;
    options.log_options = log_options;
    options.state_machine = options_.state_machine;
    options.meta_storage = init_conf.meta_storage;
    options.rm = this;
    groups_map_[group_id] = std::static_pointer_cast<ReplicationGroupNode>(
        std::make_shared<ClassicRGNode>(options));
  }
  return Status::OK();
}

} // namespace replication
