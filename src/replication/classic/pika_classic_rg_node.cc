// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/replication/classic/pika_classic_rg_node.h"

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/stat.h>

#include "slash/include/env.h"
#include "pink/include/pink_cli.h"

#include "include/storage/pika_binlog_transverter.h"
#include "include/replication/pika_repl_manager.h"
#include "include/replication/pika_repl_transporter.h"

namespace replication {

using util::ResourceGuard;
using storage::PikaBinlogTransverter;
using ReplState = SlaveContext::ReplState;
using CheckResult = ClassicLogManager::CheckResult;
using ReplStateCode = ReplState::Code;
using FollowerStateCode = Progress::FollowerState::Code;

/* SlaveContext */
SlaveContextOptions::SlaveContextOptions()
  : group_id(),
  local_id(),
  fsm_caller(nullptr),
  rm(nullptr),
  log_manager(nullptr),
  msg_sender(nullptr),
  node() {
}

SlaveContext::SlaveContext(const SlaveContextOptions::ConnectOptions& connect_options)
  : options_(),
  connect_options_(connect_options),
  master_id_(),
  repl_state_(ReplState::kNoConnect,
      connect_options.send_timeout_ms,
      connect_options.receive_timeout_ms),
  enable_communication_(true),
  waiting_snapshot_(false) {
  pthread_rwlock_init(&rwlock_, NULL);
}

SlaveContext::~SlaveContext() {
  pthread_rwlock_destroy(&rwlock_);
}

void SlaveContext::Init(const SlaveContextOptions& options) {
  options_.group_id = options.group_id;
  options_.local_id = options.local_id;
  options_.log_manager = options.log_manager;
  options_.fsm_caller = options.fsm_caller;
  options_.rm = options.rm;
  options_.msg_sender = options.msg_sender;
  options_.node = options.node;
}

Status SlaveContext::Start() {
  //slash::RWLock l(&rwlock_, true);
  //if (!master_id_.Empty() && options_.local_id != master_id_) {
  //  // If there is a master, connect to it.
  //  repl_state_.set_code(ReplStateCode::kTrySync);
  //}
  return Status::OK();
}

std::string SlaveContext::ReplState::ToString() const {
  switch (code_) {
    case Code::kNoConnect:
      return "NoConnect";
    case Code::kTrySync:
      return "TrySync";
    case Code::kTrySyncSent:
      return "TrySyncSent";
    case Code::kTrySnapshotSync:
      return "TrySnapshotSync";
    case Code::kTrySnapshotSyncSent:
      return "TrySnapshotSyncSent";
    case Code::kWaitSnapshotReceived:
      return "WaitSnapshotReceived";
    case Code::kBinlogSync:
      return "BinlogSync";
    case Code::kError:
      return "Error";
  }
  return "Unknown";
}

bool SlaveContext::CheckRecvTimeout(uint64_t now) {
  // Can not recieve the heartbeat from master in time,
  // update state to kTrySync, and try reconnect to master later.
  if (ReplRecvTimeout(now)) {
    slash::RWLock l(&rwlock_, true);
    DLOG(INFO) << "Replication group: " << options_.group_id.ToString()
               << ", receive_timeout, timeout_ms: " << connect_options_.receive_timeout_ms;
    if (repl_state_.code() != ReplStateCode::kWaitSnapshotReceived
        && repl_state_.code() != ReplStateCode::kBinlogSync) {
      return false;
    }
    repl_state_.SetSessionId(0);
    repl_state_.set_code(ReplStateCode::kTrySync);
    return true;
  }
  return false;
}

Status SlaveContext::SetMaster(const PeerID& master_id, bool force_full_sync) {
  slash::RWLock l(&rwlock_, true);
  return UnsafeSetMaster(master_id, force_full_sync);
}

Status SlaveContext::UnsafeSetMaster(const PeerID& master_id, bool force_full_sync) {
  if (!master_id_.Empty() && (master_id_ != master_id)) {
    return Status::Corruption("Already be a slave of " + master_id_.ToString() + " cannot set"
        " to " + master_id.ToString());
  }
  master_id_ = master_id;
  if (!enable_communication_) {
    return Status::NotSupported(options_.group_id.ToString() + " is disabled.");
  }
  repl_state_.set_code(force_full_sync
                      ? ReplStateCode::kTrySnapshotSync
                      : ReplStateCode::kTrySync);
  repl_state_.SetSessionId(0);
  repl_state_.SetLastRecvTime(slash::NowMicros());
  LOG(INFO) << "Replication group: " << options_.group_id.ToString()
            << ", become slave and start sync with master: " << master_id_.ToString();
  return Status::OK();
}

Status SlaveContext::DisableReplication() {
  slash::RWLock l(&rwlock_, true);
  Status s = SendRemoveSlaveNodeRequest(master_id_, repl_state_.SessionId());
  if (!s.ok()) {
    return s;
  }
  repl_state_.set_code(ReplStateCode::kNoConnect);
  // We know who is the master, but we do not communicate with it
  enable_communication_ = false;
  LOG(INFO) << "Replication group: " << options_.group_id.ToString()
            << " Stop sync with current master " << master_id().ToString();
  return s;
}

Status SlaveContext::EnableReplication(bool force_full_sync) {
  slash::RWLock l(&rwlock_, true);
  if (enable_communication_) {
    return Status::Busy("replication already enabled");
  }
  // We must already know who is the master, reconnect to it.
  if (master_id_.Empty()) {
    return Status::NotFound("master unkown");
  }
  repl_state_.set_code(force_full_sync
                      ? ReplStateCode::kTrySnapshotSync
                      : ReplStateCode::kTrySync);
  repl_state_.SetSessionId(0);
  enable_communication_ = true;
  // We reserve the last_recv_time = 0 to fast reconnect.
  LOG(INFO) << "Replication group: " << options_.group_id.ToString()
            << ", become slave and start sync with master: " << master_id_.ToString();
  return Status::OK();
}

Status SlaveContext::ResetMaster(const PeerID& master_id, bool force_full_sync) {
  Status s;
  slash::RWLock l(&rwlock_, true);
  // If the new master does not equal to the current master(not empty),
  // then remove the current master.
  if (!master_id_.Empty() && master_id != master_id_) {
    s = SendRemoveSlaveNodeRequest(master_id_, repl_state_.SessionId());
    if (!s.ok()) {
      return s;
    }
    UnsafeRemoveMaster(master_id_);
  }
  // If the new master is empty, return.
  if (master_id.Empty()) {
    return s;
  }
  return UnsafeSetMaster(master_id, force_full_sync);
}

Status SlaveContext::RemoveMaster(const PeerID& master_id) {
  slash::RWLock l(&rwlock_, true);
  if (master_id_ != master_id) {
    return Status::Busy("The master of " + options_.group_id.ToString() + " is " + master_id_.ToString());
  }
  UnsafeRemoveMaster(master_id);
  return Status::OK();
}

void SlaveContext::UnsafeRemoveMaster(const PeerID& master_id) {
  // Remove the master when the declared master_id is the current master.
  if (master_id_ == master_id) {
    master_id_.Clear();
    // Reset state to kNoConnect.
    // And clear session_id, last_send_time and last_recv_time
    repl_state_.Reset();
  }
}

Status SlaveContext::IsSafeToBeRemoved() {
  // Can not remove a slave in connected state.
  slash::RWLock l(&rwlock_, false);
  const auto& repl_code = repl_state_.code();
  if (repl_code != ReplStateCode::kNoConnect && repl_code != ReplStateCode::kError) {
    return Status::Corruption("the slave of " + options_.group_id.ToString() + " is in "
        + repl_state_.ToString() + " state");
  }
  return Status::OK();
}

bool SlaveContext::IsWaitingSnapshot() {
  bool expect = false;
  if (!waiting_snapshot_.compare_exchange_strong(expect, true, std::memory_order_acq_rel)) {
    return true;
  }
  return false;
}

void SlaveContext::NotifySnapshotReceived() {
  waiting_snapshot_.store(false, std::memory_order_release);
}

bool SlaveContext::GetSyncState(std::stringstream& out_of_sync) {
  bool in_syncing = false;
  {
    ReadLock();
    if (repl_state_.code() == ReplStateCode::kBinlogSync) {
      in_syncing = true;
    } else {
      out_of_sync << "(" << options_.group_id.ToString() << ":"
                  << repl_state_.ToString() << ")";
    }
    Unlock();
  }
  return in_syncing;
}

void SlaveContext::ReportUnreachable(const PeerID& peer_id) {
  {
    WriteLock();
    if (master_id_ == peer_id) {
      // The master was gone
      repl_state_.SetSessionId(0);
      repl_state_.set_code(ReplStateCode::kNoConnect);
      Unlock();

      LOG(INFO) << "Replication group: " << options_.group_id.ToString()
                << ", master "<< peer_id.ToString() << " is unreachable"
                << ", set the state to kNoConnect.";
      return;
    }
    Unlock();
  }
}

void SlaveContext::ReconnectMaster() {
  if (!connect_options_.reconnect_when_master_gone) {
    return;
  }
  {
    WriteLock();
    const std::string& repl_state_str = repl_state_.ToString();
    if (repl_state_.code() != ReplStateCode::kNoConnect) {
      Unlock();

      LOG(INFO) << "Replication group: " << options_.group_id.ToString()
                << " is in " << repl_state_str
                << " state, does not need reconnect to master";
      return;
    }
    // Reconnect when:
    // 1) there is a master.
    // 2) the communication is enabled.
    // 3) reconnect_when_master_gone is enabled.
    // 4) enough time has passed since the last connect(TrySync or TrySnapshotSync).
    if (!master_id_.Empty() && enable_communication_
        && (last_connect_time_ + connect_options_.reconnect_interval_ms * 1000 < slash::NowMicros())) {
      repl_state_.SetSessionId(0);
      repl_state_.set_code(ReplStateCode::kTrySync);
      const PeerID master_id = master_id_;
      Unlock();

      LOG(INFO) << "Replication group: " << options_.group_id.ToString()
                << " reconnect to master " << master_id.ToString();
      return;
    }
    Unlock();
  }
}

void SlaveContext::Step() {
  switch (ReplCode()) {
    case ReplStateCode::kTrySync: {
      Status s = SendTrySyncRequest();
      if (s.ok()) {
        SetReplCode(ReplStateCode::kTrySyncSent);
      } else {
        SetReplCode(ReplStateCode::kError);
      }
      break;
    }
    case ReplStateCode::kTrySyncSent: {
      // After send TrySync request to master,
      // we should wait the response.
      break;
    }
    case ReplStateCode::kTrySnapshotSync: {
      Status s = options_.fsm_caller->OnSnapshotSyncStart(options_.group_id);
      if (!s.ok()) {
        SetReplCode(ReplStateCode::kError);
        break;
      }
      s = SendSnapshotSyncRequest();
      if (s.ok()) {
        SetReplCode(ReplStateCode::kTrySnapshotSyncSent);
      } else {
        SetReplCode(ReplStateCode::kError);
      }
    }
    case ReplStateCode::kTrySnapshotSyncSent: {
      // After send TrySnapshotSync request to master.
      // We should wait the response.
      break;
    }
    case ReplStateCode::kWaitSnapshotReceived: {
      // After receive TrySnapshotSync response from master.
      // We should wait the actual Snapshot data to be received.
      //
      // we check the snapshot in other context to increase concurrency.
      //
      // Master should send BinlogSyncRequest to keep-alive as the
      // snapshot may be large enough.
      if (CheckRecvTimeout(slash::NowMicros())) {
        break;
      }
      if (IsWaitingSnapshot()) {
        break;
      }
      WaitSnapshotArg* wait_arg = new WaitSnapshotArg(options_.node.lock());
      options_.rm->Schedule(&ClassicRGNode::WaitSnapshotReceived,
                            static_cast<void*>(wait_arg),
                            options_.group_id.ToString(),
                            kDefaultDelayTimeout);
      break;
    }
    case ReplStateCode::kBinlogSync: {
      // After determining the location of the synchronization point,
      // we should always in BinlogSync state.
      //
      // Master should send BinlogSyncRequest to keep-alive as there
      // may no more logs need to replicate.
      if (CheckRecvTimeout(slash::NowMicros())) {
        break;
      }
    }
    case ReplStateCode::kNoConnect: {
      // There are four cases:
      // (1) node has not been started.
      // (2) node do not have a master.
      // (3) node do have a master, but connection does not enabled.
      // (4) node do have a master, but connection is broken (PeerUnreachable).
      //
      // In case (4), we should reconnect to the master.
      ReconnectMaster();
    }
    case ReplStateCode::kError:
    default: {
      break;
    }
  }
}

Status SlaveContext::SendTrySyncRequest() {
  PeerID master_id;
  int32_t session_id;
  {
    ReadLock();
    master_id = master_id_;
    session_id = repl_state_.SessionId();
    last_connect_time_ = slash::NowMicros();
    Unlock();
  }

  BinlogOffset boffset;
  Status s = options_.log_manager->GetProducerStatus(&(boffset.filenum),
                                                     &(boffset.offset));
  if (!s.ok()) {
    return Status::Corruption("Get BinlogOffset error" + s.ToString());
  }

  std::string local_ip = options_.local_id.Ip();
  int local_port = options_.local_id.Port();
  std::string table_name = options_.group_id.TableName();
  uint32_t partition_id = options_.group_id.PartitionID();

  InnerMessage::InnerRequest request;
  request.set_type(InnerMessage::kTrySync);
  InnerMessage::InnerRequest::TrySync* try_sync = request.mutable_try_sync();
  InnerMessage::Node* node = try_sync->mutable_node();
  node->set_ip(local_ip);
  node->set_port(local_port);
  InnerMessage::Partition* partition = try_sync->mutable_partition();
  partition->set_table_name(table_name);
  partition->set_partition_id(partition_id);

  InnerMessage::BinlogOffset* binlog_offset = try_sync->mutable_binlog_offset();
  binlog_offset->set_filenum(boffset.filenum);
  binlog_offset->set_offset(boffset.offset);

  std::string to_send;
  if (!request.SerializeToString(&to_send)) {
    LOG(WARNING) << " Replication group: " << options_.group_id.ToString()
                 << " serialize TrySync Request Failed to  " << master_id.ToString();
    return Status::Corruption("Serialize Failed");
  }
  options_.msg_sender->AppendSimpleTask(master_id, options_.group_id, options_.local_id, session_id, 
                                        MessageType::kRequest, std::move(to_send));
  return Status::OK();
}

void SlaveContext::HandleTrySyncResponse(const InnerMessage::InnerResponse* response) {
  const InnerMessage::InnerResponse_TrySync& try_sync_response = response->try_sync();
  {
    WriteLock();
    const auto& repl_state_str = repl_state_.ToString();
    if (repl_state_.code() != ReplStateCode::kTrySyncSent) {
      Unlock();
      LOG(WARNING) << "Replication group: " << options_.group_id.ToString()
                   << " receive a TrySyncResponse in state " << repl_state_str;
      return;
    }
    if (try_sync_response.reply_code() == InnerMessage::InnerResponse::TrySync::kOk) {
      const auto& now = slash::NowMicros();
      repl_state_.SetSessionId(try_sync_response.session_id());
      repl_state_.SetLastRecvTime(now);
      repl_state_.SetLastSendTime(now);
      repl_state_.set_code(ReplStateCode::kBinlogSync);
      Unlock();

      BinlogOffset boffset;
      options_.log_manager->GetProducerStatus(&boffset.filenum, &boffset.offset);
      LogOffset offset(boffset, LogicOffset());
      SendBinlogSyncRequest(offset, offset, true);
      LOG(INFO)    << "Replication group: " << options_.group_id.ToString() << " TrySync Ok";
      return;
    } else if (try_sync_response.reply_code() == InnerMessage::InnerResponse::TrySync::kSyncPointBePurged) {
      repl_state_.set_code(ReplStateCode::kTrySnapshotSync);
      Unlock();

      LOG(INFO)    << "Replication group: " << options_.group_id.ToString() << " Need To Try SnapshotSync";
      return;
    } else if (try_sync_response.reply_code() == InnerMessage::InnerResponse::TrySync::kSyncPointLarger) {
      repl_state_.set_code(ReplStateCode::kError);
      Unlock();

      LOG(WARNING) << "Replication group: " << options_.group_id.ToString() << " TrySync Error, Because the invalid filenum and offset";
      return;
    } else if (try_sync_response.reply_code() == InnerMessage::InnerResponse::TrySync::kError) {
      repl_state_.set_code(ReplStateCode::kError);
      Unlock();

      LOG(WARNING) << "Replication group: " << options_.group_id.ToString() << " TrySync Error";
      return;
    }
    Unlock();
  }
}

Status SlaveContext::SendSnapshotSyncRequest() {
  PeerID master_id;
  int32_t session_id;
  {
    ReadLock();
    master_id = master_id_;
    session_id = repl_state_.SessionId();
    last_connect_time_ = slash::NowMicros();
    Unlock();
  }

  BinlogOffset boffset;
  Status s = options_.log_manager->GetProducerStatus(&(boffset.filenum),
                                                     &(boffset.offset));
  if (!s.ok()) {
    return Status::Corruption("Get BinlogOffset error" + s.ToString());
  }

  const std::string& table_name = options_.group_id.TableName();
  uint32_t partition_id = options_.group_id.PartitionID();
  const std::string& local_ip = options_.local_id.Ip();
  int local_port = options_.local_id.Port();

  InnerMessage::InnerRequest request;
  request.set_type(InnerMessage::kSnapshotSync);
  InnerMessage::InnerRequest::SnapshotSync* snapshot_sync = request.mutable_snapshot_sync();
  InnerMessage::Node* node = snapshot_sync->mutable_node();
  node->set_ip(local_ip);
  node->set_port(local_port);
  InnerMessage::Partition* partition = snapshot_sync->mutable_partition();
  partition->set_table_name(table_name);
  partition->set_partition_id(partition_id);

  InnerMessage::BinlogOffset* binlog_offset = snapshot_sync->mutable_binlog_offset();
  binlog_offset->set_filenum(boffset.filenum);
  binlog_offset->set_offset(boffset.offset);

  std::string to_send;
  if (!request.SerializeToString(&to_send)) {
    LOG(WARNING) << "Serialize Partition SnapshotSync Request Failed";
    return Status::Corruption("Serialize Failed");
  }

  options_.msg_sender->AppendSimpleTask(master_id, options_.group_id, options_.local_id, session_id,
                                        MessageType::kRequest, std::move(to_send));
  return Status::OK();
}

Status SlaveContext::HandleSnapshotSyncResponse(
    const InnerMessage::InnerResponse* response) {
  const InnerMessage::InnerResponse_SnapshotSync snapshot_sync_response = response->snapshot_sync();
  int32_t session_id = snapshot_sync_response.session_id();

  {
    WriteLock();
    const auto current_state = repl_state_.code();
    const std::string& state_str = repl_state_.ToString();
    if (current_state != ReplStateCode::kTrySnapshotSyncSent) {
      Unlock();

      LOG(WARNING) << " Replication group: " << options_.group_id.ToString()
                   << " recieve a SnapshotSyncResponse in " << state_str;
      return Status::Corruption("unexpected message");
    };
    if (response->code() != InnerMessage::kOk) {
      repl_state_.set_code(ReplStateCode::kError);
      Unlock();

      std::string reply = response->has_reply() ? response->reply() : "";
      LOG(WARNING) << " Replication group: " << options_.group_id.ToString()
                   << " SnapshotSync Failed: " << reply;
      return Status::Corruption("SnapshotSync response not OK");
    }
    repl_state_.SetSessionId(session_id);
    repl_state_.set_code(ReplStateCode::kWaitSnapshotReceived);
    Unlock();
  }

  LOG(INFO) << "Replication group: " << options_.group_id.ToString() << " Need wait until the SnapshotSync finished";
  return Status::OK();
}

void SlaveContext::HandleSnapshotSyncCompleted(bool success) {
  {
    WriteLock();
    const auto current_state = repl_state_.code();
    const std::string& state_str = repl_state_.ToString();
    if (current_state != ReplStateCode::kWaitSnapshotReceived) {
      Unlock();

      LOG(WARNING) << " Replication group: " << options_.group_id.ToString()
                   << " recieve a snapshot in " << state_str;
      return;
    };
    if (success) {
      repl_state_.set_code(ReplStateCode::kTrySync);
    } else {
      repl_state_.set_code(ReplStateCode::kError);
    }
    Unlock();
  }
}

Status SlaveContext::SendBinlogSyncRequest(const LogOffset& ack_start,
                                           const LogOffset& ack_end,
                                           bool is_first_send) {
  PeerID master_id;
  int32_t session_id;
  {
    ReadLock();
    master_id = master_id_;
    session_id = repl_state_.SessionId();
    Unlock();
  }

  const std::string& table_name = options_.group_id.TableName();
  uint32_t partition_id = options_.group_id.PartitionID();
  InnerMessage::InnerRequest request;
  request.set_type(InnerMessage::kBinlogSync);
  InnerMessage::InnerRequest::BinlogSync* binlog_sync = request.mutable_binlog_sync();
  InnerMessage::Node* node = binlog_sync->mutable_node();
  node->set_ip(options_.local_id.Ip());
  node->set_port(options_.local_id.Port());
  binlog_sync->set_table_name(table_name);
  binlog_sync->set_partition_id(partition_id);
  binlog_sync->set_first_send(is_first_send);

  InnerMessage::BinlogOffset* ack_range_start = binlog_sync->mutable_ack_range_start();
  ack_range_start->set_filenum(ack_start.b_offset.filenum);
  ack_range_start->set_offset(ack_start.b_offset.offset);
  ack_range_start->set_term(ack_start.l_offset.term);
  ack_range_start->set_index(ack_start.l_offset.index);

  InnerMessage::BinlogOffset* ack_range_end = binlog_sync->mutable_ack_range_end();
  ack_range_end->set_filenum(ack_end.b_offset.filenum);
  ack_range_end->set_offset(ack_end.b_offset.offset);
  ack_range_end->set_term(ack_end.l_offset.term);
  ack_range_end->set_index(ack_end.l_offset.index);

  binlog_sync->set_session_id(session_id);
  std::string to_send;
  if (!request.SerializeToString(&to_send)) {
    LOG(WARNING) << "Replication group: " << options_.group_id.ToString()
                 << " Serialize Partition BinlogSync Request Failed, to master " << master_id.ToString();
    return Status::Corruption("Serialize Failed");
  }

  options_.msg_sender->AppendSimpleTask(master_id, options_.group_id, options_.local_id, session_id,
                                        MessageType::kRequest, std::move(to_send));
  return Status::OK();
}

bool SlaveContext::HandleBinlogSyncResponse(const PeerID& peer_id,
                                            const std::vector<int>* index,
                                            InnerMessage::InnerResponse* response) {
  SetReplLastRecvTime(slash::NowMicros());

  LogOffset pb_begin, pb_end;
  bool only_keepalive = false;
  bool append_success = false;

  // find the first not keepalive binlogsync
  for (size_t i = 0; i < index->size(); ++i) {
    const InnerMessage::InnerResponse::BinlogSync& binlog_res = response->binlog_sync((*index)[i]);
    if (!binlog_res.binlog().empty()) {
      ParseBinlogOffset(binlog_res.binlog_offset(), &pb_begin);
      break;
    }
  }

  // find the last not keepalive binlogsync
  for (int i = index->size() - 1; i >= 0; i--) {
    const InnerMessage::InnerResponse::BinlogSync& binlog_res = response->binlog_sync((*index)[i]);
    if (!binlog_res.binlog().empty()) {
      ParseBinlogOffset(binlog_res.binlog_offset(), &pb_end);
      break;
    }
  }

  if (pb_begin == LogOffset()) {
    only_keepalive = true;
  }

  LogOffset ack_start;
  if (only_keepalive) {
    ack_start = LogOffset();
  } else {
    ack_start = pb_begin;
  }

  for (size_t i = 0; i < index->size(); ++i) {
    InnerMessage::InnerResponse::BinlogSync* binlog_res = response->mutable_binlog_sync((*index)[i]);
    // if pika are not current in BinlogSync state, we drop remain write binlog task
    if (ReplCode() != ReplStateCode::kBinlogSync) {
      return append_success;
    }
    if (ReplSession() != binlog_res->session_id()) {
      LOG(WARNING) << "Check SessionId Mismatch: " << master_id().ToString()
                   << " replication group: " << options_.group_id.ToString()
                   << " expected_session: " << binlog_res->session_id() << ", actual_session:"
                   << ReplSession();
      SetReplCode(ReplStateCode::kTrySync);
      return append_success;
    }

    // empty binlog treated as keepalive packet
    if (binlog_res->binlog().empty()) {
      continue;
    }

    std::string binlog = std::move(*(binlog_res->release_binlog()));
    BinlogItem::Attributes binlog_attributes;
    if (!PikaBinlogTransverter::BinlogAttributesDecode(binlog, &binlog_attributes)) {
      LOG(WARNING) << "Binlog item decode failed";
      SetReplCode(ReplStateCode::kTrySync);
      return append_success;
    }

    // Append to local log storage.
    std::string binlog_data = binlog.erase(0, storage::BINLOG_ITEM_HEADER_SIZE);
    Status s = options_.log_manager->AppendReplicaLog(peer_id, std::move(binlog_attributes),
                                                      std::move(binlog_data));
    if (!s.ok()) {
      LOG(WARNING) << "Repliction group: " << options_.group_id.ToString()
                   << " AppendReplicaLog failed: " << s.ToString();
      options_.rm->ReportLogAppendError(options_.group_id);
      SetReplCode(ReplStateCode::kTrySync);
      return append_success;
    }
    if (!append_success) {
      append_success = true;
    }
  }

  LogOffset ack_end;
  if (only_keepalive) {
    ack_end = LogOffset();
  } else {
    LogOffset productor_status;
    // Reply Ack to master immediately
    options_.log_manager->GetProducerStatus(&productor_status.b_offset.filenum,
                                            &productor_status.b_offset.offset,
                                            &productor_status.l_offset.term,
                                            &productor_status.l_offset.index);
    ack_end = productor_status;
    ack_end.l_offset.term = pb_end.l_offset.term;
  }

  SendBinlogSyncRequest(ack_start, ack_end);
  return append_success;
}

Status SlaveContext::SendRemoveSlaveNodeRequest(const PeerID& master_id, int32_t session_id) {

  std::string local_ip = options_.local_id.Ip();
  int local_port = options_.local_id.Port();
  std::string table_name = options_.group_id.TableName();
  uint32_t partition_id = options_.group_id.PartitionID();

  InnerMessage::InnerRequest request;
  request.set_type(InnerMessage::kRemoveSlaveNode);
  InnerMessage::InnerRequest::RemoveSlaveNode* remove_slave_node =
      request.add_remove_slave_node();
  InnerMessage::Node* node = remove_slave_node->mutable_node();
  node->set_ip(local_ip);
  node->set_port(local_port);

  InnerMessage::Partition* partition = remove_slave_node->mutable_partition();
  partition->set_table_name(table_name);
  partition->set_partition_id(partition_id);

  std::string to_send;
  if (!request.SerializeToString(&to_send)) {
    LOG(WARNING) << "Replication group: " << options_.group_id.ToString()
                 << " Serialize Remove Slave Node Failed, to  " << master_id.ToString();
    return Status::Corruption("Serialize Failed");
  }
  options_.msg_sender->AppendSimpleTask(master_id, options_.group_id, options_.local_id, session_id,
                                        MessageType::kRequest, std::move(to_send));
  return Status::OK();
}

/* MasterContext */
MasterContextOptions::MasterContextOptions()
  : group_id(),
  local_id(),
  fsm_caller(nullptr),
  rm(nullptr),
  progress_set(nullptr),
  log_manager(nullptr),
  msg_sender(nullptr) {
}

MasterContext::MasterContext()
  : options_() { }

MasterContext::~MasterContext() { }

void MasterContext::Init(const MasterContextOptions& options) {
  options_.group_id = options.group_id;
  options_.local_id = options.local_id;
  options_.progress_set = options.progress_set;
  options_.log_manager = options.log_manager;
  options_.fsm_caller = options.fsm_caller;
  options_.rm = options.rm;
  options_.msg_sender = options.msg_sender;
}

Status MasterContext::Start() {
  // Add self to the progress set
  options_.progress_set->AddPeer(options_.local_id, nullptr);
  options_.progress_set->ChangeFollowerState(options_.local_id, FollowerStateCode::kFollowerBinlogSync);
  return Status::OK();
}

Status MasterContext::IsReadyForWrite() {
  return options_.progress_set->IsReady();
}

Status MasterContext::IsSafeToBeRemoved() {
  // Can not remove a master with slaves.
  if (options_.progress_set->NumOfMembers() > 1) {
    return Status::Corruption("the master of " + options_.group_id.ToString() + " is in syncing");
  }
  return Status::OK();
}

bool MasterContext::IsSafeToBePurged(uint32_t filenum) {
  return options_.progress_set->IsSafeToPurge(filenum);
}

void MasterContext::GetSyncState(std::stringstream& out_of_sync) {
  uint32_t filenum = 0;
  uint64_t offset = 0;
  options_.log_manager->GetProducerStatus(&filenum, &offset);
  out_of_sync << options_.group_id.TableName()
              << " binlog_offset=" << filenum << " " << offset;
  options_.progress_set->GetSafetyPurgeBinlog(out_of_sync);
}

Status MasterContext::GetSyncInfo(const PeerID& peer_id,
                                  std::stringstream& stream) {
  return options_.progress_set->GetInfo(peer_id, stream);
}

void MasterContext::ReportUnreachable(const PeerID& peer_id) {
  Status s = RemoveLearner(peer_id);
  if (s.ok()) {
    // The slave was gone, remove it from the progress map.
    LOG(INFO) << "Replication group: " << options_.group_id.ToString()
              << ", slave "<< peer_id.ToString() << " is unreachable"
              << ", remove it from the progress map.";
  }
}

Status MasterContext::Propose(const std::shared_ptr<ReplTask>& task, LogOffset& log_offset) {
  Status s = options_.log_manager->AppendLog(task, &log_offset);
  if (!s.ok()) {
    return s;
  }

  StepProgress(options_.local_id, log_offset, log_offset);
  options_.rm->SignalAuxiliary();
  return s;
}

Status MasterContext::StepProgress(const PeerID& peer_id,
                                   const LogOffset& start,
                                   const LogOffset& end) {
  // Update current progress
  return options_.progress_set->UpdateProgress(peer_id, start, end);
}

void MasterContext::Step() {
  const PeerID& local_id = options_.local_id;
  auto progress_vec = options_.progress_set->AllProgress([local_id] (const std::shared_ptr<ClassicProgress>& p) -> bool {
      return p->peer_id() == local_id;
  });
  if (progress_vec.empty()) {
    return;
  }
  Status s;
  uint64_t now = slash::NowMicros();
  BinlogTaskList binlog_tasks;
  for (const auto& progress : progress_vec) {
    if (progress->IsRecvTimeout(now)) {
      LOG(WARNING) << "Replication group: " << options_.group_id.ToString()
                   << ", master " << options_.local_id.ToString()
                   << " can not recieve BinlogSyncRequest from slave " << progress->peer_id().ToString()
                   << " in time, remove the slave from progress map.";
      RemoveLearner(progress->peer_id());
      continue;
    }
    {
      // Ensure consistency of data delivery
      options_.msg_sender->Lock();
      s = SendBinlogsToPeer(progress, binlog_tasks);
      if (!s.ok() && !s.IsNotSupported()) {
        LOG(ERROR) << "Replication group: " << options_.group_id.ToString()
                   << " prepare infligt binlog for slave " << progress->peer_id().ToString()
                   << " error: " << s.ToString()
                   << " , remove the slave from progress map.";
        RemoveLearner(progress->peer_id());

        options_.msg_sender->Unlock();
        continue;
      }
      if (!binlog_tasks.empty()) {
        options_.msg_sender->UnsafeAppendBinlogTasks(progress->peer_id(), std::move(binlog_tasks));
        binlog_tasks.clear();

        options_.msg_sender->Unlock();
        continue;
      }
      BinlogTask heartbeat_task;
      s = SendHeartbeatToPeer(progress, now, heartbeat_task);
      if (!s.ok() && !s.IsBusy()) {
        LOG(WARNING) << "Replication group: " << options_.group_id.ToString()
                     << ", master " << options_.local_id.ToString()
                     << " can not send heartbeat to slave " << progress->peer_id().ToString();
      } else if (!s.IsBusy()) {
        options_.msg_sender->UnsafeAppendBinlogTask(progress->peer_id(),
                                                    std::move(heartbeat_task));
      }
      options_.msg_sender->Unlock();
    }
  }
}

Status MasterContext::SendBinlogsToPeer(const std::shared_ptr<ClassicProgress>& peer_progress) {
  Status s;
  BinlogTaskList tasks;
  {
    // Ensure consistency of data delivery
    options_.msg_sender->Lock();
    s = SendBinlogsToPeer(peer_progress, tasks);
    if (!s.ok()) {
      options_.msg_sender->Unlock();
      return s;
    }
    if (!tasks.empty()) {
      options_.msg_sender->UnsafeAppendBinlogTasks(peer_progress->peer_id(), std::move(tasks));
    }
    options_.msg_sender->Unlock();
  }
  return s;
}

Status MasterContext::SendBinlogsToPeer(const std::shared_ptr<ClassicProgress>& peer_progress,
                                        BinlogTaskList& tasks) {
  std::list<BinlogChip> binlog_chips;
  Status s = peer_progress->PrepareInfligtBinlog(binlog_chips);
  if (!s.ok()) {
    return s;
  }
  for (auto& chip : binlog_chips) {
    tasks.emplace_back(options_.group_id, options_.local_id, peer_progress->session_id(),
                       MessageType::kResponse, std::move(chip));
  }
  return Status::OK();
}

Status MasterContext::SendHeartbeatToPeer(const std::shared_ptr<ClassicProgress>& peer_progress,
                                          uint64_t now, BinlogTask& task) {
  BinlogChip binlog_chip;
  Status s = peer_progress->PrepareHeartbeat(now, binlog_chip);
  if (!s.ok()) {
    return s;
  }
  BinlogTask t(options_.group_id, options_.local_id, peer_progress->session_id(),
               MessageType::kResponse, std::move(binlog_chip));
  task = std::move(t);
  DLOG(INFO) << "Replication group: " << options_.group_id.ToString()
             << ", send heartbeat to peer " << peer_progress->peer_id().ToString();
  return Status::OK();
}

bool MasterContext::HandleTrySyncRequest(const InnerMessage::InnerRequest* request,
                                         InnerMessage::InnerResponse* response) {
  InnerMessage::InnerRequest::TrySync try_sync_request = request->try_sync();
  InnerMessage::InnerResponse::TrySync* try_sync_response = response->mutable_try_sync();

  bool success = TrySyncOffsetCheck(try_sync_request, try_sync_response);
  if (!success) {
    return false;
  }
  return TrySyncUpdateSlaveNode(&try_sync_request, try_sync_response);
}

bool MasterContext::TrySyncOffsetCheck(const InnerMessage::InnerRequest::TrySync& try_sync_request,
                                       InnerMessage::InnerResponse::TrySync* try_sync_response) {
  InnerMessage::Node node = try_sync_request.node();
  InnerMessage::BinlogOffset slave_boffset = try_sync_request.binlog_offset();

  BinlogOffset expect_offset;
  CheckResult result;
  LogOffset received_offset;
  ParseBinlogOffset(slave_boffset, &received_offset);
  options_.log_manager->CheckOffset(PeerID(node.ip(), node.port()), received_offset.b_offset,
                                    result, expect_offset);
  InnerMessage::BinlogOffset* node_expect_offset = try_sync_response->mutable_binlog_offset();
  node_expect_offset->set_filenum(expect_offset.filenum);
  node_expect_offset->set_offset(expect_offset.offset);
  bool ret = false;
  switch (result) {
    case CheckResult::kError: {
      try_sync_response->set_reply_code(InnerMessage::InnerResponse::TrySync::kError);
      break;
    }
    case CheckResult::kSyncPointLarger: {
      try_sync_response->set_reply_code(InnerMessage::InnerResponse::TrySync::kSyncPointLarger);
      break;
    }
    case CheckResult::kSyncPointBePurged: {
      try_sync_response->set_reply_code(InnerMessage::InnerResponse::TrySync::kSyncPointBePurged);
      break;
    }
    case CheckResult::kOK:
    default: {
      ret = true;
      break;
    }
  }
  return ret;
}

bool MasterContext::TrySyncUpdateSlaveNode(
    const InnerMessage::InnerRequest::TrySync* try_sync_request,
    InnerMessage::InnerResponse::TrySync* try_sync_response) {
  InnerMessage::Node node = try_sync_request->node();

  int32_t session_id;
  // Add the slave to progress map.
  Status s = AddLearner(PeerID(node.ip(), node.port()), &session_id);
  if (!s.ok()) {
    try_sync_response->set_reply_code(InnerMessage::InnerResponse::TrySync::kError);
    LOG(WARNING) << "Replication group: " << options_.group_id.ToString()
                 << " TrySync Failed, " << s.ToString();
    return false;
  }
  try_sync_response->set_reply_code(InnerMessage::InnerResponse::TrySync::kOk);
  try_sync_response->set_session_id(session_id);
  LOG(INFO) << "Replication group: " << options_.group_id.ToString()
            << " TrySync Success, Session: " << session_id;
  return true;
}

bool MasterContext::HandleBinlogSyncRequest(const InnerMessage::InnerRequest* request,
                                            InnerMessage::InnerResponse* response) {
  const InnerMessage::InnerRequest::BinlogSync& binlog_req = request->binlog_sync();
  const InnerMessage::Node& node = binlog_req.node();
  const InnerMessage::BinlogOffset& ack_range_start = binlog_req.ack_range_start();
  const InnerMessage::BinlogOffset& ack_range_end = binlog_req.ack_range_end();
  BinlogOffset b_range_start(ack_range_start.filenum(), ack_range_start.offset());
  BinlogOffset b_range_end(ack_range_end.filenum(), ack_range_end.offset());
  LogicOffset l_range_start(ack_range_start.term(), ack_range_start.index());
  LogicOffset l_range_end(ack_range_end.term(), ack_range_end.index());
  LogOffset range_start(b_range_start, l_range_start);
  LogOffset range_end(b_range_end, l_range_end);
  bool is_first_send = binlog_req.first_send();
  int32_t session_id = binlog_req.session_id();
  // Update progress
  PeerID peer_id(node.ip(), node.port());
  auto ctx = ProgressContext(peer_id, session_id);
  ctx.set_last_recv_time(slash::NowMicros());
  Status s = UpdateSlaveProgressContext(peer_id, ctx);
  if (!s.ok()) {
    LOG(WARNING) << "Replication group: " << options_.group_id.ToString()
                 << " Update the progress context of slave " << peer_id.ToString()
                 << " failed: " << s.ToString();
    return true;
  }

  if (is_first_send) {
    if (range_start.b_offset != range_end.b_offset) {
      LOG(WARNING) << "first binlogsync request pb argument invalid";
      return true;
    }

    Status s = ActivateSlaveProgress(peer_id, range_start);
    if (!s.ok()) {
      LOG(WARNING) << "Activate Binlog Sync failed " << peer_id.ToString() << " " << s.ToString();
      return true;
    }
    return false;
  }

  // not the first_send the range_ack cant be 0
  // set this case as ping
  if (range_start.b_offset == BinlogOffset() && range_end.b_offset == BinlogOffset()) {
    return false;
  }
  s = UpdateSlaveProgress(peer_id, range_start, range_end);
  if (!s.ok()) {
    LOG(WARNING) << "Update binlog ack failed " << options_.group_id.ToString() << " " << s.ToString();
    return false;
  }

  options_.rm->SignalAuxiliary();
  return false;
}

Status MasterContext::UpdateSlaveProgressContext(const PeerID& peer_id,
                                                 const ProgressContext& ctx) {
  auto progress = options_.progress_set->GetProgress(peer_id);
  if (progress == nullptr) {
    return Status::NotFound("progress " + peer_id.ToString());
  }
  return progress->UpdateContext(ctx);
}

Status MasterContext::ActivateSlaveProgress(const PeerID& peer_id,
                                            const LogOffset& offset) {
  auto progress = options_.progress_set->GetProgress(peer_id);
  if (progress == nullptr) {
    return Status::NotFound(peer_id.ToString());
  }
  auto log_reader = options_.log_manager->GetLogReader(offset);
  if (log_reader == nullptr) {
    return Status::Corruption("Init binlog_reader failed");
  }
  Status s = progress->BecomeBinlogSync(std::move(log_reader), offset);
  if (!s.ok()) {
    LOG(WARNING) << "Replication group: " << options_.group_id.ToString()
                 << " activate the progress of slave " << peer_id.ToString()
                 << " failed: " << s.ToString();
    return s;
  }
  return SendBinlogsToPeer(progress);
}

Status MasterContext::UpdateSlaveProgress(const PeerID& peer_id,
                                          const LogOffset& range_start,
                                          const LogOffset& range_end) {
  Status s = StepProgress(peer_id, range_start, range_end);
  if (!s.ok()) {
    return s;
  }
  DLOG(INFO) << "Replication group: " << options_.group_id.ToString()
             << " UpdateSlaveProgress, peer_id: " << peer_id.ToString()
             << ", range_start " << range_start.ToString()
             << ", range_end " << range_end.ToString();
  auto progress = options_.progress_set->GetProgress(peer_id);
  if (progress == nullptr) {
    return Status::NotFound(peer_id.ToString());
  }
  return SendBinlogsToPeer(progress);
}

bool MasterContext::HandleSnapshotSyncRequest(
    const InnerMessage::InnerRequest* request,
    InnerMessage::InnerResponse* response) {
  InnerMessage::InnerRequest::SnapshotSync snapshot_sync_request = request->snapshot_sync();
  InnerMessage::InnerResponse::SnapshotSync* snapshot_sync_response = response->mutable_snapshot_sync();
  InnerMessage::Node node = snapshot_sync_request.node();
  InnerMessage::BinlogOffset slave_boffset = snapshot_sync_request.binlog_offset();
  int32_t session_id;
  PeerID peer_id(node.ip(), node.port());
  Status s = AddLearner(peer_id, &session_id);
  if (!s.ok()) {
    response->set_code(InnerMessage::kError);
    snapshot_sync_response->set_session_id(-1);
    LOG(WARNING) << "Replication group: " << options_.group_id.ToString()
                 << " handle SnapshotSync Request Failed, " << s.ToString();
  } else {
    snapshot_sync_response->set_session_id(session_id);
    LOG(INFO) << "Replication group: " << options_.group_id.ToString()
              << " handle SnapshotSync Request Success, Session: " << session_id;
  }
  options_.progress_set->ChangeFollowerState(peer_id, FollowerStateCode::kFollowerSnapshotSync);
  options_.fsm_caller->TrySendSnapshot(PeerID(node.ip(), node.port()), options_.group_id,
                                       options_.log_manager->LogFileName(),
                                       slave_boffset.filenum(), nullptr);
  return true;
}

Status MasterContext::HandleRemoveNodeRequest(const PeerID& peer_id,
                                              InnerMessage::InnerResponse* response) {
  Status s = RemoveLearner(peer_id);
  if (!s.ok()) {
    return s;
  }
  response->set_code(InnerMessage::kOk);
  response->set_type(InnerMessage::Type::kRemoveSlaveNode);
  InnerMessage::InnerResponse::RemoveSlaveNode* remove_slave_node_response = response->add_remove_slave_node();
  InnerMessage::Partition* partition_response = remove_slave_node_response->mutable_partition();
  partition_response->set_table_name(options_.group_id.TableName());
  partition_response->set_partition_id(options_.group_id.PartitionID());
  InnerMessage::Node* node_response = remove_slave_node_response->mutable_node();
  node_response->set_ip(options_.local_id.Ip());
  node_response->set_port(options_.local_id.Port());
  return Status::OK();
}

Status MasterContext::AddLearner(const PeerID& peer_id,
                                 int32_t* session_id) {
  auto progress = options_.progress_set->GetProgress(peer_id);
  if (progress == nullptr) {
    return options_.progress_set->AddPeer(peer_id, session_id);
  }
  *session_id = progress->session_id();
  return Status::OK();
}

Status MasterContext::RemoveLearner(const PeerID& peer_id) {
  return options_.progress_set->RemovePeer(peer_id);
}

/* ClassicRGNode */

ClassicRGNode::ClassicRGNode(const ClassicRGNodeOptions& options)
  : ReplicationGroupNode(options),
  persistent_ctx_(options.log_options.stable_log_options.log_path),
  meta_storage_(options.meta_storage),
  progress_set_(options.group_id, options.local_id,
                options.progress_options),
  log_manager_(nullptr),
  msg_sender_(MessageSenderOptions(options.group_id, 0/*use default batch limit*/)),
  master_ctx_(),
  slave_ctx_(options.slave_connect_options) {
  log_manager_ = new ClassicLogManager(options.log_options);
  progress_set_.SetLogger(log_manager_->GetBinlogBuilder());
}

ClassicRGNode::~ClassicRGNode() {
  if (log_manager_ != nullptr) {
    delete log_manager_;
    log_manager_ = nullptr;
  }
}

Status ClassicRGNode::Initialize() {
  MasterContextOptions master_options;
  master_options.group_id = group_id_;
  master_options.local_id = local_id_;
  master_options.progress_set = &progress_set_;
  master_options.fsm_caller = fsm_caller_;
  master_options.rm = rm_;
  master_options.msg_sender = &msg_sender_;
  master_options.log_manager = log_manager_;
  master_ctx_.Init(master_options);

  SlaveContextOptions slave_options;
  slave_options.group_id = group_id_;
  slave_options.local_id = local_id_;
  slave_options.fsm_caller = fsm_caller_;
  slave_options.rm = rm_;
  slave_options.msg_sender = &msg_sender_;
  slave_options.log_manager = log_manager_;
  slave_options.node = std::static_pointer_cast<ClassicRGNode>(shared_from_this()),
  slave_ctx_.Init(slave_options);
  return Status::OK();
}

Status ClassicRGNode::Start() {
  if (started_.load(std::memory_order_acquire)) {
    return Status::Busy("node has already been started");
  }
  Status s = Initialize();
  if (!s.ok()) {
    return s;
  }
  // Recover the unapplied logs
  s = log_manager_->Initialize();
  if (!s.ok()) {
    return s;
  }
  s = persistent_ctx_.Initialize(log_manager_->last_offset());
  if (!s.ok()) {
    return s;
  }
  s = meta_storage_->ResetOffset(persistent_ctx_.snapshot_offset(), persistent_ctx_.applied_offset());
  if (!s.ok()) {
    return s;
  }
  DLOG(INFO) << "Replication group: " << group_id_.ToString()
             << ", committed_offset: " << persistent_ctx_.committed_offset().ToString()
             << ", applied_offset: " << persistent_ctx_.applied_offset().ToString()
             << ", snapshot_offset: " << persistent_ctx_.snapshot_offset().ToString();
  s = log_manager_->Recover(persistent_ctx_.applied_offset(), persistent_ctx_.snapshot_offset());
  if (!s.ok()) {
    return s;
  }
  TryToScheduleApplyLog(log_manager_->last_offset());

  // Start the context
  s = master_ctx_.Start();
  if (!s.ok()) {
    return s;
  }
  s = slave_ctx_.Start();
  if (!s.ok()) {
    return s;
  }
  started_.store(true, std::memory_order_release);
  return s;
}

void ClassicRGNode::Leave(std::string& log_remove) {
  log_remove = log_manager_->Leave();
}

Status ClassicRGNode::IsReady() {
  return master_ctx_.IsReadyForWrite();
}

void ClassicRGNode::Messages(PeerMessageListMap& msgs) {
  msg_sender_.Messages(msgs);
}

Status ClassicRGNode::IsSafeToBeRemoved() {
  // TODO(LIBA-S): mark the state to avoid concurrent problem
  Status s = master_ctx_.IsSafeToBeRemoved();
  if (!s.ok()) {
    return s;
  }
  return slave_ctx_.IsSafeToBeRemoved();
}

void ClassicRGNode::GetMasterSyncState(std::stringstream& out_of_sync) {
  master_ctx_.GetSyncState(out_of_sync);
}

bool ClassicRGNode::GetSlaveSyncState(std::stringstream& out_of_sync) {
  return slave_ctx_.GetSyncState(out_of_sync);
}

Status ClassicRGNode::GetSyncInfo(const PeerID& peer_id,
                                  std::stringstream& stream) {
  return master_ctx_.GetSyncInfo(peer_id, stream);
}

bool ClassicRGNode::IsReadonly() {
  return slave_ctx_.ReplCode() != ReplStateCode::kNoConnect;
}

VolatileContext::State ClassicRGNode::NodeVolatileState() {
  return VolatileContext::State(slave_ctx_.master_id(), RoleState());
}

PersistentContext::State ClassicRGNode::NodePersistentState() {
  return persistent_ctx_.DumpState();
}

MemberContext::MemberStatesMap ClassicRGNode::NodeMemberStates() {
  return MemberContext::MemberStatesMap{};
}

void ClassicRGNode::PurgeStableLogs(int expire_logs_nums, int expire_logs_days,
                                    uint32_t to, bool manual,
                                    PurgeLogsClosure* done) {
  auto node = shared_from_this();
  auto filter = [node] (const uint32_t index) -> bool {
    return !node->IsSafeToBePurged(index);
  };
  log_manager_->PurgeStableLogs(expire_logs_nums, expire_logs_days, to, manual, filter, done);
}

bool ClassicRGNode::IsSafeToBePurged(uint32_t filenum) {
  // We can only purge logs that have been applied.
  if (persistent_ctx_.applied_offset().b_offset.filenum < filenum) {
    return false;
  }
  return master_ctx_.IsSafeToBePurged(filenum);
}

void ClassicRGNode::ReportUnreachable(const PeerID& peer_id) {
  master_ctx_.ReportUnreachable(peer_id);
  slave_ctx_.ReportUnreachable(peer_id);
}

Status ClassicRGNode::SetMaster(const PeerID& master_id,
                                bool force_full_sync) {
  return slave_ctx_.SetMaster(master_id, force_full_sync);
}

Status ClassicRGNode::DisableReplication() {
  return slave_ctx_.DisableReplication();
}

Status ClassicRGNode::EnableReplication(bool force_full_sync, bool reset_file_offset,
                                        uint32_t filenum, uint64_t offset) {
  // The current node should not have connected to the master
  if (slave_ctx_.ReplCode() != ReplStateCode::kNoConnect
      && slave_ctx_.ReplCode() != ReplStateCode::kError) {
    return Status::Corruption("Slave is not in kNoConnect or kError state");
  }
  Status s;
  if (reset_file_offset) {
    // Reset the underlying storage to the point
    s = log_manager_->Reset(LogOffset(BinlogOffset(filenum, offset), LogicOffset()));
  }
  if (!s.ok()) {
    return s;
  }
  return slave_ctx_.EnableReplication(force_full_sync);
}

Status ClassicRGNode::ResetMaster(const PeerID& master_id,
                                  bool force_full_sync) {
  return slave_ctx_.ResetMaster(master_id, force_full_sync);
}

Status ClassicRGNode::RemoveMaster(const PeerID& master_id) {
  return slave_ctx_.RemoveMaster(master_id);
}

bool ClassicRGNode::HandleTrySyncRequest(const InnerMessage::InnerRequest* request,
                                         InnerMessage::InnerResponse* response) {
  return master_ctx_.HandleTrySyncRequest(request, response);
}

void ClassicRGNode::HandleTrySyncResponse(const InnerMessage::InnerResponse* response) {
  slave_ctx_.HandleTrySyncResponse(response);
}

bool ClassicRGNode::HandleSnapshotSyncRequest(const InnerMessage::InnerRequest* request,
                                              InnerMessage::InnerResponse* response) {
  return master_ctx_.HandleSnapshotSyncRequest(request, response); 
}

Status ClassicRGNode::HandleSnapshotSyncResponse(const InnerMessage::InnerResponse* response) {
  return slave_ctx_.HandleSnapshotSyncResponse(response);
}

bool ClassicRGNode::HandleBinlogSyncRequest(const InnerMessage::InnerRequest* request,
                                            InnerMessage::InnerResponse* response) {
  return master_ctx_.HandleBinlogSyncRequest(request, response);
}

void ClassicRGNode::HandleBinlogSyncResponse(const PeerID& peer_id,
                                             const std::vector<int>* index,
                                             InnerMessage::InnerResponse* response) {
  if (slave_ctx_.HandleBinlogSyncResponse(peer_id, index, response)) {
    TryToScheduleApplyLog(log_manager_->last_offset());
  }
}

Status ClassicRGNode::HandleRemoveNodeRequest(const PeerID& peer_id,
                                              InnerMessage::InnerResponse* response) {
  return master_ctx_.HandleRemoveNodeRequest(peer_id, response);
}

Status ClassicRGNode::StepStateMachine() {
  master_ctx_.Step();
  slave_ctx_.Step();
  return Status::OK();
}

Status ClassicRGNode::Propose(const std::shared_ptr<ReplTask>& task) {
  if (!started_.load(std::memory_order_acquire)) {
    LOG(ERROR) << "Replication group: " << group_id_.ToString()
               << ", has not been started"
               << ", drop the proposal.";
    return Status::Corruption("proposal dropped when the node has not been started");
  }
  LogOffset log_offset;
  Status s = master_ctx_.Propose(task, log_offset);
  if (!s.ok()) {
    return s;
  }

  persistent_ctx_.UpdateCommittedOffset(log_offset);
  persistent_ctx_.UpdateAppliedOffset(log_offset, false, true /*ignore_applied_window*/);
  DLOG(INFO) << "Replication group: " << group_id_.ToString()
             << ", propose a task, log_offset " << log_offset.ToBinlogString();
  return s;
}

Status ClassicRGNode::Advance(const Ready& ready) {
  if (ready.db_reloaded && slave_ctx_.ReplCode() == ReplStateCode::kWaitSnapshotReceived) {
    if (!ready.db_reloaded_error) {
      persistent_ctx_.ResetPerssistentContext(ready.snapshot_offset, ready.applied_offset);
      meta_storage_->ResetOffset(ready.snapshot_offset, ready.applied_offset);
      log_manager_->Reset(ready.snapshot_offset);
    }
    // After Snapshot reloaded, we should step the state_machine of slave context.
    slave_ctx_.HandleSnapshotSyncCompleted(!ready.db_reloaded_error);
  } else if (!ready.applied_offset.Empty()) {
    persistent_ctx_.UpdateAppliedOffset(ready.applied_offset);
  }
  return Status::OK();
}

void ClassicRGNode::TryToScheduleApplyLog(const LogOffset& committed_offset) {
  LogOffset updated_committed_offset;
  bool should_apply = UpdateCommittedOffset(committed_offset, &updated_committed_offset);

  if (should_apply) {
    Status s = ScheduleApplyLog(updated_committed_offset);
    if (!s.ok()) {
      LOG(ERROR) << "Replication group: " << group_id_.ToString()
                 << " update committed_offset to " << updated_committed_offset.ToBinlogString()
                 << ", schedule apply log error: " << s.ToString();
    }
  }
}

bool ClassicRGNode::UpdateCommittedOffset(const LogOffset& remote_committed_offset,
                                          LogOffset* updated_committed_offset) {
  bool updated = persistent_ctx_.UpdateCommittedOffset(remote_committed_offset);
  if (updated) {
    *updated_committed_offset = remote_committed_offset;
  }
  return updated;
}

Status ClassicRGNode::ScheduleApplyLog(const LogOffset& committed_offset) {
  // logs from PurgeLogs goes to InternalApply in order
  std::vector<MemLog::LogItem> logs;
  Status s = log_manager_->PurgeMemLogsByOffset(committed_offset, &logs);
  if (!s.ok()) {
    return Status::NotFound("committed offset not found " + committed_offset.ToBinlogString());
  }
  for (const auto& log : logs) {
    persistent_ctx_.PrepareUpdateAppliedOffset(log.GetLogOffset());
  }
  fsm_caller_->OnApply(std::move(logs));
  return Status::OK();
}

void ClassicRGNode::WaitSnapshotReceived(void* arg) {
  WaitSnapshotArg* wait_arg = static_cast<WaitSnapshotArg*>(arg);
  ResourceGuard<WaitSnapshotArg> guard(wait_arg);
  auto node = wait_arg->node.lock();
  if (node == nullptr) {
    LOG(WARNING) << "Replication group has been removed when in waiting state";
    return;
  }
  Status s = node->fsm_caller_->CheckSnapshotReceived(node, &wait_arg->snapshot_offset);
  if (s.IsIncomplete()) {
    // Not ready, post check task again.
    node->rm_->Schedule(&ClassicRGNode::WaitSnapshotReceived,
                        static_cast<void*>(guard.Release()),
                        node->group_id().ToString(), kDefaultDelayTimeout);
    return;
  }
  if (!s.ok()) {
    node->Advance(Ready(LogOffset(), LogOffset(), true, true));
  } else {
    node->Advance(Ready(wait_arg->snapshot_offset, wait_arg->snapshot_offset, true, false));
  }
  node->NotifySnapshotReceived();
}

void ClassicRGNode::NotifySnapshotReceived() {
  slave_ctx_.NotifySnapshotReceived();
}

} // namespace replication
