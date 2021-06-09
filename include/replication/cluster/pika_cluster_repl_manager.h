// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef REPLICATION_PIKA_CLUSTER_REPL_MANAGER_H_
#define REPLICATION_PIKA_CLUSTER_REPL_MANAGER_H_

#include <string>

#include "slash/include/slash_status.h"

#include "proto/pika_inner_message.pb.h"
#include "include/pika_define.h"
#include "include/util/thread_pool.h"
#include "include/replication/pika_repl_manager.h"
#include "include/replication/pika_configuration.h"
#include "include/replication/pika_repl_transporter.h"

namespace replication {

using slash::Status;
using util::Closure;

class ClusterReplManager : public ReplicationManager {
 public:
  explicit ClusterReplManager(const ReplicationManagerOptions& options);
  ~ClusterReplManager();

 public:
  virtual int Start() override;
  virtual int Stop() override;
  virtual bool IsReadonly(const ReplicationGroupID& group_id) override;
  virtual void AddMember(const ReplicationGroupID& group_id,
                         const PeerID& peer_id, const PeerRole& role,
                         Closure* done_closure) override;
  virtual void RemoveMember(const ReplicationGroupID& group_id,
                            const PeerID& peer_id,
                            Closure* done_closure) override;
  virtual void PromoteMember(const ReplicationGroupID& group_id,
                             const PeerID& peer_id,
                             Closure* done_closure) override;
  virtual void LeaderTransfer(const ReplicationGroupID& group_id,
                              const PeerID& peer_id,
                              Closure* done_closure) override;

 private:
  virtual Status createReplicationGroupNodes(const GroupIDAndInitConfMap& id_init_conf_map) override;
  virtual void handlePeerMessage(MessageReporter::Message* peer_msg) override;
  virtual void reportTransportResult(const PeerInfo& peer_info,
                                     const MessageReporter::ResultType& r_type,
                                     const MessageReporter::DirectionType& d_type) override;
  Status createConfChangeEntryData(const InnerMessage::ConfChangeType& type,
                                   const PeerID& peer_id, std::string* data);

 private:
  util::thread::ThreadPool timer_thread_;
};

} // namespace replication

#endif  //  REPLICATION_PIKA_CLUSTER_REPL_MANAGER_H_
