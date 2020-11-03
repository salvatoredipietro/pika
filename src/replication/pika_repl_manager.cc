// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/replication/pika_repl_manager.h"

namespace replication {

ReplicationManager::ReplicationManager(const ReplicationManagerOptions& rm_options,
                                       TransporterOptions&& transporter_options)
  : auxiliary_(this),
  options_(rm_options),
  transporter_(rm_options.transporter),
  executor_(util::TaskExecutorOptions(
        rm_options.options_store->sync_thread_num(),
        rm_options.options_store->disk_thread_num())) {
  if (transporter_ == nullptr) {
    transporter_ = std::make_shared<TransporterImpl>(std::move(transporter_options));
  }
  pthread_rwlock_init(&rg_rw_, NULL);
}

ReplicationManager::~ReplicationManager() {
  pthread_rwlock_destroy(&rg_rw_);
}

int ReplicationManager::Start() {
  int res = executor_.Start();
  if (res != 0) {
    LOG(ERROR) << "Start exexutor thread failed";
    return res;
  }
  res = transporter_->Start();
  if (res != 0) {
    LOG(ERROR) << "Start transporter failed";
    return res;
  }
  res = auxiliary_.Start();
  if (res != 0) {
    LOG(ERROR) << "Start auxiliary thread failed";
    return res;
  }
  return res;
}

int ReplicationManager::Stop() {
  // 1. Stop producing message;
  int res = auxiliary_.Stop();
  if (res != 0) {
    LOG(ERROR) << "Stop transporter failed";
    return res;
  }
  // 2. Stop sending/recieving to remote
  res = transporter_->Stop();
  if (res != 0) {
    LOG(ERROR) << "Stop auxiliary thread failed";
    return res;
  }
  // 3. Stop replication groups
  // NOTE: nodes should be stoppped before exexutor_ as there
  //       may be some cleanup tasks need to be completed.
  StopAllReplicationGroups();

  // 4. Stop executor
  res = executor_.Stop(true);
  if (res != 0) {
    LOG(ERROR) << "Start exexutor thread failed";
    return res;
  }
  return res;
}

void ReplicationManager::StopAllReplicationGroups() {
  Status s;
  slash::RWLock l(&rg_rw_, false);
  for (const auto& iter : groups_map_) {
    s = iter.second->Stop();
    if (!s.ok()) {
      LOG(ERROR) << "Stop replication group: " << iter.first.ToString()
                 << ", failed: " << s.ToString();
    } else {
      LOG(INFO) << "Stop replication group: " << iter.first.ToString() << " success.";
    }
  }
}

Status ReplicationManager::CreateReplicationGroupNodesSanityCheck(const GroupIDAndInitConfMap& id_init_conf_map) {
  for (const auto& iter : id_init_conf_map) {
    auto node = GetReplicationGroupNode(iter.first);
    if (node != nullptr) {
      return Status::Corruption("replication group: " + iter.first.ToString() + " already exist!");
    }
  }
  return Status::OK();
}

Status ReplicationManager::CreateReplicationGroupNodesSanityCheck(const std::set<ReplicationGroupID>& group_ids) {
  for (const auto& group_id : group_ids) {
    auto node = GetReplicationGroupNode(group_id);
    if (node != nullptr) {
      return Status::Corruption("replication group: " + group_id.ToString() + " already exist!");
    }
  }
  return Status::OK();
}

Status ReplicationManager::CreateReplicationGroupNodes(const std::set<ReplicationGroupID>& group_ids) {
  GroupIDAndInitConfMap id_init_conf_map;
  for (const auto& group_id : group_ids) {
    id_init_conf_map.insert({group_id, RGNodeInitializeConfig()});
  }
  return CreateReplicationGroupNodes(id_init_conf_map);
}

Status ReplicationManager::CreateReplicationGroupNodes(const GroupIDAndInitConfMap& id_init_conf_map) {
  Status s = CreateReplicationGroupNodesSanityCheck(id_init_conf_map);
  if (!s.ok()) {
    return s;
  }
  return createReplicationGroupNodes(id_init_conf_map);
}

std::shared_ptr<ReplicationGroupNode> ReplicationManager::GetReplicationGroupNode(const ReplicationGroupID& group_id) {
  slash::RWLock l(&rg_rw_, false);
  auto iter = groups_map_.find(group_id);
  if (iter == groups_map_.end()) {
    return nullptr;
  }
  return iter->second;
}

Status ReplicationManager::StartReplicationGroupNode(const ReplicationGroupID& group_id) {
  std::shared_ptr<ReplicationGroupNode> node = nullptr;
  {
    slash::RWLock l(&rg_rw_, false);
    auto iter = groups_map_.find(group_id);
    if (iter == groups_map_.end()) {
      return Status::NotFound(group_id.ToString());
    }
    node = iter->second;
  }
  return node->Start();
}

Status ReplicationManager::StopReplicationGroupNode(const ReplicationGroupID& group_id) {
  std::shared_ptr<ReplicationGroupNode> node = nullptr;
  {
    slash::RWLock l(&rg_rw_, false);
    auto iter = groups_map_.find(group_id);
    if (iter == groups_map_.end()) {
      return Status::NotFound(group_id.ToString());
    }
    node = iter->second;
  }
  return node->Stop();
}

Status ReplicationManager::RemoveReplicationGroupNodes(const std::set<ReplicationGroupID>& group_ids) {
  Status s = RemoveReplicationGroupNodesSanityCheck(group_ids);
  if (!s.ok()) {
    return s;
  }
  std::vector<std::string> logs;
  s = RemoveReplicationGroupNodes(group_ids, logs);
  if (!s.ok()) {
    return s;
  }
  for (const auto& log : logs) {
    options_.state_machine->PurgeDir(log);
  }
  return Status::OK();
}

Status ReplicationManager::ReplicationGroupNodesSantiyCheck(const SanityCheckFilter& filter) {
  Status s = Status::OK();
  auto nodes = CurrentReplicationGroups();
  for (const auto& node : nodes) {
    if (filter != nullptr) {
      s = filter(node);
    }
    if (!s.ok()) {
      break;
    }
  }
  return s;
}

Status ReplicationManager::PurgeLogs(const ReplicationGroupID& group_id,
                                     uint32_t to,
                                     bool manual) {
  auto node = GetReplicationGroupNode(group_id);
  if (node == nullptr) {
    return Status::NotFound(group_id.ToString());
  }
  if (node->IsPurging()) {
    return Status::OK();
  }
  PurgeLogArg* arg = new PurgeLogArg();
  arg->to = to;
  arg->manual = manual;
  arg->node = node;
  arg->expire_logs_nums = options_.options_store->expire_logs_nums();
  arg->expire_logs_days = options_.options_store->expire_logs_days();
  options_.state_machine->PurgelogsTaskSchedule(&ReplicationGroupNode::DoPurgeStableLogs,
                                               static_cast<void*>(arg));
  return Status::OK();
}

void ReplicationManager::PinServerStream(const PeerID& peer_id,
                                         std::shared_ptr<pink::PbConn> server_stream) {
  transporter_->PinServerStream(peer_id, std::dynamic_pointer_cast<ServerStream>(server_stream));
}

void ReplicationManager::PinClientStream(const PeerID& peer_id,
                                         std::shared_ptr<pink::PbConn> client_stream) {
  transporter_->PinClientStream(peer_id, std::dynamic_pointer_cast<ClientStream>(client_stream));
}

void ReplicationManager::UnpinPeerConnections(const PeerID& peer_id, const MessageReporter::DirectionType& d_type) {
  switch (d_type) {
    case MessageReporter::DirectionType::kClient: {
      transporter_->UnpinClientStream(peer_id);
      break;
    }
    case MessageReporter::DirectionType::kServer: {
      transporter_->UnpinServerStream(peer_id);
      break;
    }
    case MessageReporter::DirectionType::kClientAndServer: {
      transporter_->RemovePeer(peer_id);
      break;
    }
  }
}

void ReplicationManager::StepStateMachine() {
  auto nodes = CurrentReplicationGroups();
  for (const auto& node : nodes) {
    node->StepStateMachine();
  }
}

void ReplicationManager::SignalAuxiliary() {
  auxiliary_.Signal();
}

void ReplicationManager::Schedule(void (*function)(void *), void* arg,
                                  const std::string& hash_key, uint64_t time_after) {
  executor_.Schedule(function, arg, hash_key, time_after);
}

int ReplicationManager::SendToPeer() {
  int sent = 0;
  PeerMessageListMap msgs;
  auto nodes = CurrentReplicationGroups();
  for (const auto& node : nodes) {
    msgs.clear();
    node->Messages(msgs);
    auto s = transporter_->WriteToPeer(std::move(msgs));
    if (!s.ok()) {
      LOG(ERROR) << "Write to peer failed: " << s.ToString();
    } else {
      sent++;
    }
  }
  return sent;
}

void ReplicationManager::ReportLogAppendError(const ReplicationGroupID& group_id) {
  options_.state_machine->ReportLogAppendError(group_id);
}

std::vector<std::shared_ptr<ReplicationGroupNode>> ReplicationManager::CurrentReplicationGroups() {
  std::vector<std::shared_ptr<ReplicationGroupNode>> nodes;
  {
    slash::RWLock l(&rg_rw_, false);
    std::shared_ptr<ReplicationGroupNode> node = nullptr;
    nodes.reserve(groups_map_.size());
    for (const auto& pair : groups_map_) {
      nodes.push_back(pair.second);
    }
  }
  return std::move(nodes);
}

Status ReplicationManager::RemoveReplicationGroupNodes(const std::set<ReplicationGroupID>& group_ids,
                                                       std::vector<std::string>& logs) {
  std::string log_remove;
  for (const auto& group_id : group_ids) {
    log_remove.clear();
    auto node = GetReplicationGroupNode(group_id);
    if (node == nullptr) {
      LOG(WARNING) << "Replication group: " << group_id.ToString() << " not found";
      continue;
    }
    node->Leave(log_remove);
    if (!log_remove.empty()) {
      logs.push_back(std::move(log_remove));
    }
  }
  {
    slash::RWLock l(&rg_rw_, true);
    for (const auto& group_id : group_ids) {
      groups_map_.erase(group_id);
    }
  }
  return Status::OK();
}

void ReplicationManager::ReportUnreachable(const PeerID& peer_id, const MessageReporter::DirectionType& d_type) {
  // 1. Notify the RGNode
  auto nodes = CurrentReplicationGroups();
  for (const auto& node : nodes) {
    node->ReportUnreachable(peer_id);
  }
  // 2. Unpin the related stream
  UnpinPeerConnections(peer_id, d_type);
}

Status ReplicationManager::GetReplicationGroupInfo(const ReplicationGroupID& group_id,
                                                   std::stringstream& stream) {
  auto node = GetReplicationGroupNode(group_id);
  return node->GetSyncInfo(PeerID(), stream);
}

Status ReplicationManager::RemoveReplicationGroupNodesSanityCheck(const std::set<ReplicationGroupID>& group_ids) {
  Status s;
  for (const auto& group_id : group_ids) {
    auto node = GetReplicationGroupNode(group_id);
    if (node == nullptr) {
      return Status::NotFound("replication group: " + group_id.ToString());
    }
    s = node->IsSafeToBeRemoved();
    if (!s.ok()) {
      break;
    }
  }
  return s;
}

bool ReplicationManager::IsReadonly(const ReplicationGroupID& group_id) {
  auto node = GetReplicationGroupNode(group_id);
  if (node == nullptr) {
    return true;
  }
  return node->IsReadonly();
}

Status ReplicationManager::GetSyncInfo(const ReplicationGroupID& group_id,
                                       const PeerID& peer_id,
                                       std::stringstream& stream) {
  auto node = GetReplicationGroupNode(group_id);
  if (node == nullptr) {
    return Status::NotFound(group_id.ToString());
  }
  return node->GetSyncInfo(peer_id, stream);
}

} // namespace replication
