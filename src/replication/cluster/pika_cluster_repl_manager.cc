// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/replication/cluster/pika_cluster_repl_manager.h"

#include "include/util/hooks.h"
#include "include/replication/cluster/pika_cluster_rg_node.h"
#include "include/replication/cluster/pika_cluster_io_stream.h"

namespace replication {

ClusterReplManager::ClusterReplManager(const ReplicationManagerOptions& options)
  : ReplicationManager(options, std::move(TransporterOptions(options.options_store->local_id(), this,
                                          new ClusterClientStreamFactory(this, options.options_store->max_conn_rbuf_size()),
                                          new ClusterServerStreamFactory(this, options.options_store->max_conn_rbuf_size())))),
  timer_thread_(1) {
}

ClusterReplManager::~ClusterReplManager() { }

int ClusterReplManager::Start() {
  int res = timer_thread_.Start();
  if (0 != res) {
    LOG(ERROR) << "Start timer thread failed";
    return res;
  }
  LOG(INFO) << "timer thread started";
  return ReplicationManager::Start();
}

int ClusterReplManager::Stop() {
  int res = ReplicationManager::Stop();
  if (0 != res) {
    return res;
  }
  res = timer_thread_.Stop();
  if (0 != res) {
    LOG(ERROR) << "Stop timer thread failed";
    return res;
  }
  return res;
}

bool ClusterReplManager::IsReadonly(const ReplicationGroupID& group_id) {
  auto node = GetReplicationGroupNode(group_id);
  if (node == nullptr) {
    return false;
  }
  return node->IsReadonly();
}

Status ClusterReplManager::createReplicationGroupNodes(const GroupIDAndInitConfMap& id_init_conf_map) {
  slash::RWLock l(&rg_rw_, true);
  for (const auto& iter : id_init_conf_map) {
    const auto& group_id = iter.first;
    const auto& init_config = iter.second;
    ClusterRGNodeOptions options;
    options.group_id = group_id;
    options.pre_vote = options_.options_store->pre_vote();
    options.check_quorum = options_.options_store->check_quorum();
    options.local_id = options_.options_store->local_id();
    options.heartbeat_timeout_ms = options_.options_store->heartbeat_timeout_ms();
    options.election_timeout_ms = options_.options_store->election_timeout_ms();

    ClusterLogManagerOptions log_options;
    log_options.group_id = group_id;
    log_options.stable_log_options.group_id = group_id;
    log_options.stable_log_options.log_path = options_.options_store->log_path() + "log_"
                                              + group_id.TableName() + "/"
                                              + std::to_string(group_id.PartitionID()) + "/";
    log_options.stable_log_options.max_binlog_size = options_.options_store->binlog_file_size();
    log_options.stable_log_options.retain_logs_num = options_.options_store->retain_logs_num();
    log_options.stable_log_options.load_index_into_memory = options_.options_store->load_index_into_memory(); 
    log_options.executor = &executor_;
    log_options.max_consumed_number_once = options_.options_store->max_consumed_number_once();

    ProgressOptions progress_options;
    progress_options.max_inflight_number = options_.options_store->sync_window_size();
    progress_options.receive_timeout_ms = options_.options_store->election_timeout_ms();
    progress_options.send_timeout_ms = options_.options_store->sync_send_timeout_ms();

    options.progress_options = progress_options;
    options.state_machine = options_.state_machine;
    options.meta_storage = init_config.meta_storage;
    options.timer_thread = &timer_thread_;
    options.executor = &executor_;
    options.log_options = log_options;
    options.rm = this;
    groups_map_[group_id] = std::static_pointer_cast<ReplicationGroupNode>(
        std::make_shared<ClusterRGNode>(options));
  }
  return Status::OK();
}

void ClusterReplManager::handlePeerMessage(MessageReporter::Message* peer_msg) {
  if (peer_msg == nullptr) {
    LOG(WARNING) << "ClusterReplManager receive an empty peer message";
    return;
  }
  if (peer_msg->proto_msg() == nullptr) {
    LOG(WARNING) << "ClusterReplManager receive an empty protocol message";
    return;
  }
  if (peer_msg->proto_msg()->proto_type() != InnerMessage::ProtocolMessage::kClusterType) {
    LOG(WARNING) << "ClusterReplManager receive a non-cluster protocol message";
    return;
  }
  auto raft_msg_handler = std::make_shared<RaftMessageHandler>(
      peer_msg->proto_msg()->release_cluster_msg(), peer_msg->release_closure(), this);
  executor_.ScheduleTask(std::move(std::static_pointer_cast<util::thread::Task>(raft_msg_handler)),
                         raft_msg_handler->isAppendRequest() ?
                         raft_msg_handler->GetMessageGroupID().ToString() : "");
}

void ClusterReplManager::reportTransportResult(const PeerInfo& peer_info,
                                               const MessageReporter::ResultType& r_type,
                                               const MessageReporter::DirectionType& d_type) {
  switch (r_type) {
    case MessageReporter::ResultType::kFdClosed:
    case MessageReporter::ResultType::kFdTimedout: {
      // The connection is unreachable
      ReplicationManager::ReportUnreachable(peer_info.peer_id, d_type);
      break;
    }
    case ResultType::kResponseParseError:
    case ResultType::kOK:
    default:
      break;
  }
}

void ClusterReplManager::AddMember(const ReplicationGroupID& group_id,
                                   const PeerID& peer_id,
                                   const PeerRole& role,
                                   Closure* done_closure)  {
  ClosureGuard guard(done_closure);
  if (role == PeerRole::kRoleUnkown) {
    if (done_closure) {
      done_closure->set_status(Status::Corruption("role unkown"));
    }
    return;
  }
  auto node = GetReplicationGroupNode(group_id);
  if (node == nullptr) {
    if (done_closure) {
      done_closure->set_status(Status::NotFound(group_id.ToString()));
    }
    return;
  }
  auto cluster_node = std::dynamic_pointer_cast<ClusterRGNode>(node);
  if (cluster_node == nullptr) {
    if (done_closure) {
      done_closure->set_status(Status::Corruption("unexpected env"));
    }
    return;
  }
  InnerMessage::ConfChangeType type(role == PeerRole::kRoleVoter
                                    ? InnerMessage::ConfChangeType::kAddVoter
                                    : InnerMessage::ConfChangeType::kAddLearner);
  std::string log_data;
  Status s = createConfChangeEntryData(type, peer_id, &log_data);
  if (!s.ok()) {
    if (done_closure) {
      done_closure->set_status(s);
    }
    return;
  }
  auto repl_task = std::make_shared<ReplTask>(std::move(log_data),
                                              guard.Release(), group_id, peer_id,
                                              ReplTask::Type::kOtherType,
                                              ReplTask::LogFormat::kPB);
  cluster_node->ProposeConfChange(repl_task);
}

void ClusterReplManager::RemoveMember(const ReplicationGroupID& group_id,
                                      const PeerID& peer_id,
                                      Closure* done_closure) {
  ClosureGuard guard(done_closure);
  auto node = GetReplicationGroupNode(group_id);
  if (node == nullptr) {
    if (done_closure) {
      done_closure->set_status(Status::NotFound(group_id.ToString()));
    }
    return;
  }
  auto cluster_node = std::dynamic_pointer_cast<ClusterRGNode>(node);
  if (cluster_node == nullptr) {
    if (done_closure) {
      done_closure->set_status(Status::Corruption("unexpected env"));
    }
    return;
  }
  std::string log_data;
  Status s = createConfChangeEntryData(InnerMessage::ConfChangeType::kRemoveNode,
                                       peer_id, &log_data);
  if (!s.ok()) {
    if (done_closure) {
      done_closure->set_status(s);
    }
    return;
  }
  auto repl_task = std::make_shared<ReplTask>(std::move(log_data),
                                              guard.Release(), group_id, peer_id,
                                              ReplTask::Type::kOtherType,
                                              ReplTask::LogFormat::kPB);
  cluster_node->ProposeConfChange(repl_task);
}

void ClusterReplManager::PromoteMember(const ReplicationGroupID& group_id,
                                       const PeerID& peer_id,
                                       Closure* done_closure) {
  ClosureGuard guard(done_closure);
  auto node = GetReplicationGroupNode(group_id);
  if (node == nullptr) {
    if (done_closure) {
      done_closure->set_status(Status::NotFound(group_id.ToString()));
    }
    return;
  }
  auto cluster_node = std::dynamic_pointer_cast<ClusterRGNode>(node);
  if (cluster_node == nullptr) {
    if (done_closure) {
      done_closure->set_status(Status::Corruption("unexpected env"));
    }
    return;
  }
  std::string log_data;
  Status s = createConfChangeEntryData(InnerMessage::ConfChangeType::kPromoteLearner,
                                       peer_id, &log_data);
  if (!s.ok()) {
    if (done_closure) {
      done_closure->set_status(s);
    }
    return;
  }
  auto repl_task = std::make_shared<ReplTask>(std::move(log_data),
                                              guard.Release(), group_id, peer_id,
                                              ReplTask::Type::kOtherType,
                                              ReplTask::LogFormat::kPB);
  cluster_node->ProposeConfChange(repl_task);
}

void ClusterReplManager::LeaderTransfer(const ReplicationGroupID& group_id,
                                        const PeerID& peer_id,
                                        Closure* done_closure) {
  ClosureGuard guard(done_closure);
  auto node = GetReplicationGroupNode(group_id);
  if (node == nullptr) {
    if (done_closure) {
      done_closure->set_status(Status::NotFound(group_id.ToString()));
    }
    return;
  }
  auto cluster_node = std::dynamic_pointer_cast<ClusterRGNode>(node);
  if (cluster_node == nullptr) {
    if (done_closure) {
      done_closure->set_status(Status::Corruption("unexpected env"));
    }
    return;
  }
  cluster_node->ProposeLeaderTransfer(peer_id, guard.Release());
}

Status ClusterReplManager::createConfChangeEntryData(const InnerMessage::ConfChangeType& type,
                                                     const PeerID& peer_id,
                                                     std::string* data) {
  InnerMessage::ConfChange* conf_change = new InnerMessage::ConfChange();
  conf_change->set_type(type);
  auto node = conf_change->mutable_node_id();
  node->set_ip(peer_id.Ip());
  node->set_port(peer_id.Port());
  InnerMessage::BaseEntry entry;
  entry.set_type(InnerMessage::EntryType::kEntryConfChange);
  entry.set_allocated_conf_change(conf_change);
  if (!entry.SerializeToString(data)) {
    LOG(FATAL) << "Serialize ConfChangeEntry Failed";
  }
  return Status::OK();
}

} // namespace replication
