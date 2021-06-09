// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef REPLICATION_CLUSTER_PIKA_CLUSTER_MESSAGE_EXECUTOR_H_
#define REPLICATION_CLUSTER_PIKA_CLUSTER_MESSAGE_EXECUTOR_H_

#include <string>
#include <vector>
#include <memory>

#include "proto/pika_inner_message.pb.h"
#include "include/pika_define.h"
#include "include/util/thread_pool.h"
#include "include/replication/pika_repl_message_executor.h"
#include "include/replication/pika_repl_transporter.h"

namespace replication {

class ClusterReplManager;

class RaftMessageHandler : public util::thread::Task {
 public:
  // Responsible for managing the lifetime of closure and msg.
  RaftMessageHandler(InnerMessage::RaftMessage* msg,
                     IOClosure* closure, ClusterReplManager* rm)
    : msg_(msg), closure_(closure), rm_(rm) { }
  ~RaftMessageHandler();

  virtual void Run() override;
  ReplicationGroupID GetMessageGroupID();
  bool isAppendRequest();

 private:
  static bool isRequest(const InnerMessage::RaftMessageType& type);

  InnerMessage::RaftMessage* msg_;
  IOClosure* closure_;
  ClusterReplManager* const rm_;
};

struct ClusterMessageSenderOptions : public MessageSenderOptions {
  // term start from 1
  uint32_t term = 1;
  PeerID local_id;
  ClusterMessageSenderOptions() = default;
  explicit ClusterMessageSenderOptions(const ReplicationGroupID& _group_id,
                                       const PeerID& _local_id,
                                       int _batch_limit,
                                       uint32_t _term)
    : MessageSenderOptions(_group_id, _batch_limit),
    term(_term),
    local_id(_local_id) { }
};

class ClusterBinlogBuilder : public MessageBuilder {
 public:
  ClusterBinlogBuilder(const ClusterMessageSenderOptions& options,
                       const PeerID& to,
                       BinlogTaskList&& tasks)
    : options_(options), to_(to), tasks_(std::move(tasks)) { }
  virtual Status Build(std::string* msg) override;

 private:
  ClusterMessageSenderOptions options_;
  const PeerID to_;
  BinlogTaskList tasks_;
};

class ClusterMessageSender : public MessageSender {
 public:
  explicit ClusterMessageSender(const ClusterMessageSenderOptions& options);

  void Reset(const ClusterMessageSenderOptions& options);
  void ResetTerm(uint32_t term);
  virtual void Messages(PeerMessageListMap& msgs) override;

 private:
  ClusterMessageSenderOptions c_options_;
};

} // namespace replication

#endif // REPLICATION_CLUSTER_PIKA_CLUSTER_MESSAGE_EXECUTOR_H_
