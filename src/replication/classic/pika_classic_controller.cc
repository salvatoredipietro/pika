// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/replication/classic/pika_classic_controller.h"

#include <glog/logging.h>

#include "proto/pika_inner_message.pb.h"
#include "include/replication/classic/pika_classic_repl_manager.h"

namespace replication {

std::string MasterSlaveController::ReplState::ToString() const {
  switch (code_) {
    case Code::kReplNoConnect:
      return "ReplNoConnect";
    case Code::kReplShouldMetaSync:
      return "ReplShouldMetaSync";
    case Code::kReplMetaSyncDone:
      return "ReplMetaSyncDone";
    case Code::kReplError:
      return "ReplError";
  }
  return "Unknown";
}

std::string MasterSlaveController::RoleState::ToString() const {
  switch (code_) {
    case Code::kRoleSingle:
      return "single";
    case Code::kRoleSlave:
      return "slave";
    case Code::kRoleMaster:
      return "master";
    case Code::kRoleMasterAndSlave:
      return "master_slave";
  }
  return "Unknown";
}

using RoleCode = MasterSlaveController::RoleState::Code;
using ReplCode = MasterSlaveController::ReplState::Code;

MasterSlaveController::MasterSlaveController(ClassicReplManager* rm)
  : rm_(rm),
  msg_sender_(MessageSenderOptions(ReplicationGroupID(), 0)),
  local_id_(rm_->OptionsStorage()->local_id()),
  repl_state_(ReplCode::kReplNoConnect),
  role_(RoleCode::kRoleSingle),
  last_meta_sync_timestamp_(0),
  first_meta_sync_(false),
  force_full_sync_(false) {
  UnsafeClearMaster();
  pthread_rwlock_init(&state_protector_, NULL);
}

MasterSlaveController::~MasterSlaveController() {
  pthread_rwlock_destroy(&state_protector_);
}

int MasterSlaveController::Start() {
  std::string slave_of = rm_->OptionsStorage()->slaveof();
  if (!slave_of.empty()) {
    int32_t sep = slave_of.find(":");
    std::string master_ip = slave_of.substr(0, sep);
    int32_t master_port = std::stoi(slave_of.substr(sep+1));
    if ((master_ip == "127.0.0.1" || master_ip == local_id_.Ip())
        && master_port == local_id_.Port()) {
      LOG(ERROR) << "you will slaveof yourself as the config file, please check";
      return -1;
    } else {
      Status s = SetMaster(master_ip, master_port, false);
      if (!s.ok()) {
        LOG(ERROR) << "SetMaster failed: " << s.ToString();
        return -1;
      }
    }
  }
  return 0;
}

int MasterSlaveController::Stop() {
  return 0;
}

void MasterSlaveController::Messages(PeerMessageListMap& msgs) {
  msg_sender_.Messages(msgs);
}

bool MasterSlaveController::IsReadonly() {
  slash::RWLock l(&state_protector_, false);
  return role_.IsSlave() && rm_->OptionsStorage()->slave_read_only();
}

std::string MasterSlaveController::master_ip() {
  slash::RWLock l(&state_protector_, false);
  return master_ip_;
}

int MasterSlaveController::master_port() {
  slash::RWLock(&state_protector_, false);
  return master_port_;
}

MasterSlaveController::RoleState MasterSlaveController::role_state() {
  slash::RWLock(&state_protector_, false);
  return role_;
}

MasterSlaveController::ReplState MasterSlaveController::repl_state() {
  slash::RWLock(&state_protector_, false);
  return repl_state_;
}

bool MasterSlaveController::force_full_sync() {
  slash::RWLock(&state_protector_, false);
  return force_full_sync_;
}

void MasterSlaveController::SetForceFullSync(bool v) {
  slash::RWLock(&state_protector_, true);
  force_full_sync_ = v;
}

void MasterSlaveController::BecomeMaster() {
  slash::RWLock l(&state_protector_, true);
  role_.AttachMaster();
}

void MasterSlaveController::SyncError() {
  slash::RWLock l(&state_protector_, true);
  repl_state_.set_code(ReplCode::kReplError);
  LOG(WARNING) << "Sync error, set repl_state to kReplError";
}

bool MasterSlaveController::IsSlave() {
  slash::RWLock l(&state_protector_, false);
  if (!role_.IsSlave()
      || repl_state_.code() != ReplCode::kReplMetaSyncDone) {
    return false;
  }
  return true;
}

bool MasterSlaveController::IsMaster() {
  slash::RWLock l(&state_protector_, false);
  return role_.IsMaster();
}

PeerID MasterSlaveController::CurrentMaster() {
  std::string master_ip;
  int master_port;
  CurrentMaster(master_ip, master_port);
  return PeerID(master_ip, master_port);
}

void MasterSlaveController::CurrentMaster(std::string& master_ip,
                                          int& master_port) {
  slash::RWLock l(&state_protector_, false);
  master_ip = master_ip_;
  master_port = master_port_;
}

void MasterSlaveController::RemoveMaster(std::string& master_ip,
                                         int& master_port) {
  slash::RWLock l(&state_protector_, true);
  repl_state_.set_code(ReplCode::kReplNoConnect);
  role_.DetachSlave();

  master_ip = master_ip_;
  master_port = master_port_;

  UnsafeClearMaster();
  return;
}

void MasterSlaveController::UnsafeClearMaster() {
  const auto& dummy_peer_id = PeerID::DummyPeerID();
  master_ip_ = dummy_peer_id.Ip();
  master_port_ = dummy_peer_id.Port();
}

Status MasterSlaveController::SetMaster(const std::string& master_ip,
                                        int master_port,
                                        bool force_full_sync) {
  slash::RWLock l(&state_protector_, true);
  if (!role_.IsSlave() && repl_state_.code() == ReplCode::kReplNoConnect) {
    master_ip_ = master_ip;
    if (master_ip == "127.0.0.1") {
      master_ip_ = local_id_.Ip();
    }
    master_port_ = master_port;
    role_.AttachSlave();
    repl_state_.set_code(ReplCode::kReplShouldMetaSync);
    first_meta_sync_ = true;
    force_full_sync_ = force_full_sync;
    return Status::OK();
  }
  return Status::Corruption("already be a connected slave");
}

void MasterSlaveController::FinishMetaSync() {
  slash::RWLock l(&state_protector_, true);
  assert(repl_state_.code() == ReplCode::kReplShouldMetaSync);
  repl_state_.set_code(ReplCode::kReplMetaSyncDone);
}

bool MasterSlaveController::MetaSyncDone() {
  slash::RWLock l(&state_protector_, false);
  return repl_state_.code() == ReplCode::kReplMetaSyncDone;
}

void MasterSlaveController::ResetMetaSyncStatus() {
  slash::RWLock l(&state_protector_, true);
  if (role_.IsSlave()) {
    // not change by slaveof no one, so set repl_state to kReplShouldMetaSync,
    // continue to connect master
    repl_state_.set_code(ReplCode::kReplShouldMetaSync);
  }
}

int MasterSlaveController::GetMetaSyncTimestamp() {
  slash::RWLock sp_l(&state_protector_, false);
  return last_meta_sync_timestamp_;
}

void MasterSlaveController::UpdateMetaSyncTimestamp() {
  slash::RWLock sp_l(&state_protector_, true);
  last_meta_sync_timestamp_ = time(NULL);
}

bool MasterSlaveController::IsFirstMetaSync() {
  slash::RWLock sp_l(&state_protector_, true);
  return first_meta_sync_;
}

void MasterSlaveController::SetFirstMetaSync(bool v) {
  slash::RWLock sp_l(&state_protector_, true);
  first_meta_sync_ = v;
}

void MasterSlaveController::StepStateMachine() {
  switch (repl_state().code()) {
    case ReplCode::kReplShouldMetaSync: {
      // Send MetaSync request to master when:
      // 1). Can not receive the response in time (connection is broken).
      // or 2). It is the first time to send meta sync.
      if (time(NULL) - GetMetaSyncTimestamp() >= kPikaMetaSyncMaxWaitTime
          || IsFirstMetaSync()) {
        Status s = SendMetaSyncRequest();
        if (s.ok()) {
          UpdateMetaSyncTimestamp();
          SetFirstMetaSync(false);
        }
      }
      break;
    }
    case ReplCode::kReplMetaSyncDone: {
      // We have handled the MetaSync response from master.
      // Nothing need to do.
      break;
    }
    case ReplCode::kReplNoConnect:
    case ReplCode::kReplError:
    default:
      break;
  }
  return;
}

void MasterSlaveController::ReportUnreachable(const PeerInfo& peer_info) {
  if (role_.IsMaster()) {
    // Maybe slave disconnect, we drop the connection with it if so.
    PeerID peer_id;
    if (DeleteSlave(peer_info.fd, &peer_id)) {
      rm_->UnpinPeerConnections(peer_id, MessageReporter::DirectionType::kClientAndServer);
      return;
    }
  }
  if (role_.IsSlave()) {
    // Maybe master disconnect, we reconnect to it with MetaSync if so.
    auto current_master = CurrentMaster();
    if (peer_info.peer_id != current_master) {
      DLOG(WARNING) << "Peer " << peer_info.peer_id.ToString() << " unreachable"
                    << ", current master is " << current_master.ToString();
      return;
    }
    if (repl_state().code() == ReplCode::kReplError) {
      LOG(WARNING) << "Master unreachable, do not reconnect when we are in ReplError state";
      return;
    }
    // Reset the state for later retry.
    ResetMetaSyncStatus();
    return;
  }
  // Should not reach here, as we are a single node
}

bool MasterSlaveController::DeleteSlave(int fd, PeerID* peer_id) {
  bool del = false;
  int slave_num = -1;
  {
    std::lock_guard<std::mutex> lk_guard(slave_mutex_);
    std::vector<SlaveItem>::iterator iter = slaves_.begin();
    while (iter != slaves_.end()) {
      if (iter->conn_fd == fd) {
        LOG(INFO) << "Delete Slave Success, ip_port: " << iter->peer_id.ToString();
        slaves_.erase(iter);
        *peer_id = iter->peer_id;
        del = true;
        break;
      }
      iter++;
    }
    slave_num = slaves_.size();
  }

  if (slave_num == 0) {
    slash::RWLock l(&state_protector_, true);
    // Remove master flag, when there are no more replicas
    role_.DetachMaster();
  }
  return del;
}

bool MasterSlaveController::TryAddSlave(const PeerID& peer_id, int peer_fd,
                                        const std::vector<TableStruct>& table_structs)  {
  std::lock_guard<std::mutex> lk_guard(slave_mutex_);
  std::vector<SlaveItem>::iterator iter = slaves_.begin();
  while (iter != slaves_.end()) {
    if (iter->peer_id == peer_id) {
      LOG(WARNING) << "Slave Already Exist, ip_port: " << peer_id.ToString();
      return false;
    }
    iter++;
  }

  // Not exist, so add new
  LOG(INFO) << "Add New Slave, " << peer_id.ToString();
  SlaveItem s;
  s.peer_id = peer_id;
  s.conn_fd = peer_fd;
  s.stage = SlaveItem::Stage::kStageOne;
  s.table_structs = table_structs;
  gettimeofday(&s.create_time, NULL);
  slaves_.push_back(s);
  return true;
}

bool MasterSlaveController::HandleMetaSyncRequest(const PeerID& peer_id, int peer_fd,
                                                  const std::vector<TableStruct>& table_structs)  {
  bool add_success = TryAddSlave(peer_id, peer_fd, table_structs);
  if (add_success) {
    BecomeMaster();
  }
  return add_success;
}

Status MasterSlaveController::SendMetaSyncRequest() {
  PeerID master_id = CurrentMaster();

  InnerMessage::InnerRequest request;
  request.set_type(InnerMessage::kMetaSync);
  InnerMessage::InnerRequest::MetaSync* meta_sync = request.mutable_meta_sync();
  InnerMessage::Node* node = meta_sync->mutable_node();
  node->set_ip(local_id_.Ip());
  node->set_port(local_id_.Port());

  std::string masterauth = rm_->OptionsStorage()->masterauth();
  if (!masterauth.empty()) {
    meta_sync->set_auth(masterauth);
  }

  std::string to_send;
  if (!request.SerializeToString(&to_send)) {
    LOG(WARNING) << "Serialize Meta Sync Request Failed, to Master " << master_id.ToString();
    return Status::Corruption("Serialize Failed");
  }

  msg_sender_.AppendSimpleTask(master_id, ReplicationGroupID(), local_id_, 0,
                               MessageType::kRequest, std::move(to_send));
  return Status::OK();
}

void MasterSlaveController::HandleMetaSyncResponse() {
  // 1. Enable binlog
  rm_->OptionsStorage()->SetWriteBinlog("yes");

  // 2. Active replication groups
  rm_->SetMasterForRGNodes(PeerID(master_ip(), master_port()), force_full_sync());

  // 3. Update state machine
  FinishMetaSync();
}

Status MasterSlaveController::InfoReplication(std::string& info_str) {
  Status s;
  std::stringstream tmp_stream;
  RoleState role;
  ReplState state;
  {
    slash::RWLock(&state_protector_, false);
    role = role_;
    state = repl_state_;
  }
  tmp_stream << "# Replication(";
  switch (role.code()) {
    case RoleCode::kRoleSingle:
    case RoleCode::kRoleMaster: {
      tmp_stream << "MASTER)\r\nrole:master\r\n";
      GetSlaveSyncInfo(tmp_stream);
      break;
    }
    case RoleCode::kRoleSlave: {
      tmp_stream << "SLAVE)\r\nrole:slave\r\n";
      std::stringstream out_of_sync;
      bool all_in_syncing = rm_->GetSlaveSyncState(out_of_sync);
      tmp_stream << "master_host:" << master_ip() << "\r\n";
      tmp_stream << "master_port:" << master_port() << "\r\n";
      tmp_stream << "master_link_status:" << (((state.code() == ReplCode::kReplMetaSyncDone)
                                             && all_in_syncing) ? "up" : "down") << "\r\n";
      tmp_stream << "slave_priority:" << rm_->OptionsStorage()->slave_priority() << "\r\n";
      tmp_stream << "slave_read_only:" << rm_->OptionsStorage()->slave_read_only() << "\r\n";
      if (!all_in_syncing) {
        tmp_stream <<"db_repl_state:" << out_of_sync.str() << "\r\n";
      }
      break;
    }
    case RoleCode::kRoleMasterAndSlave: {
      tmp_stream << "Master && SLAVE)\r\nrole:master&&slave\r\n";
      std::stringstream out_of_sync;
      bool all_in_syncing = rm_->GetSlaveSyncState(out_of_sync);
      tmp_stream << "master_host:" << master_ip() << "\r\n";
      tmp_stream << "master_port:" << master_port() << "\r\n";
      tmp_stream << "master_link_status:" << (((state.code() == ReplCode::kReplMetaSyncDone)
                                             && all_in_syncing) ? "up" : "down") << "\r\n";
      tmp_stream << "slave_read_only:" << rm_->OptionsStorage()->slave_read_only() << "\r\n";
      if (!all_in_syncing) {
        tmp_stream <<"db_repl_state:" << out_of_sync.str() << "\r\n";
      }
      break;
    }
    default: {
      info_str.append("ERR: server role is error\r\n");
      return Status::OK();
    }
  }
  rm_->GetMasterSyncState(tmp_stream);
  info_str.append(tmp_stream.str());
  return Status::OK();
}

void MasterSlaveController::GetSlaveSyncInfo(std::stringstream& stream) {
  size_t index = 0;
  std::stringstream tmp_stream;
  {
    std::lock_guard<std::mutex> lk_guard(slave_mutex_);
    for (const auto& slave : slaves_) {
      tmp_stream << "slave" << index++
                 << ":ip=" << slave.peer_id.Ip() << ",port=" << slave.peer_id.Port()
                 << ",conn_fd=" << slave.conn_fd << ",lag=";
      for (const auto& ts : slave.table_structs) {
        for (size_t idx = 0; idx < ts.partition_num; ++idx) {
          ReplicationGroupID group_id(ts.table_name, idx);
          Status s = rm_->GetSyncInfo(group_id, slave.peer_id, tmp_stream);
          if (!s.ok()) {
            LOG(WARNING) << s.ToString();
            continue;
          }
        }
      }
      tmp_stream << "\r\n";
    }
  }
  stream << "connected_slaves:" << static_cast<int32_t>(index) << "\r\n"
         << tmp_stream.str();
}

} // namespace replication

