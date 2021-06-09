// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_REPL_MESSAGE_EXECUTOR_H_
#define PIKA_REPL_MESSAGE_EXECUTOR_H_

#include <string>
#include <list>
#include <unordered_map>
#include <memory>
#include <queue>
#include <utility>
#include <mutex>

#include "slash/include/slash_status.h"

#include "proto/pika_inner_message.pb.h"
#include "include/pika_define.h"
#include "include/util/callbacks.h"
#include "include/storage/pika_binlog_transverter.h"
#include "include/replication/pika_repl_transporter.h"

namespace replication {

using storage::BinlogItem;
using slash::Status;

extern void ParseBinlogOffset(const InnerMessage::BinlogOffset& pb_offset,
                              LogOffset* offset);
extern void BuildBinlogOffset(const LogOffset& offset,
                              InnerMessage::BinlogOffset* pb_offset);
extern void ParseBinlogAttributes(const InnerMessage::BinlogAttributes& pb_attributes,
                                  BinlogItem::Attributes* attributes);
extern void BuildBinlogAttributes(const BinlogItem::Attributes& attributes,
                                  InnerMessage::BinlogAttributes* pb_attributes);

struct BinlogChip {
  LogOffset prev_offset;
  LogOffset committed_offset;
  BinlogItem::Attributes attrs;
  std::string binlog;

  BinlogChip()
    : prev_offset(),
    committed_offset(),
    attrs(),
    binlog() {}

  BinlogChip(const LogOffset& _prev_offset,
             const LogOffset& _committed_offset,
             const BinlogItem::Attributes& _attrs,
             std::string&& _binlog)
    : prev_offset(_prev_offset),
    committed_offset(_committed_offset),
    attrs(_attrs),
    binlog(std::move(_binlog)) {}

  BinlogChip(BinlogChip&& other) noexcept
    : prev_offset(other.prev_offset),
    committed_offset(other.committed_offset),
    attrs(other.attrs),
    binlog(std::move(other.binlog)) {
      other.prev_offset.Clear();
      other.committed_offset.Clear();
    }

  BinlogChip& operator=(BinlogChip&& other) noexcept {
    if (this != &other) {
      prev_offset = other.prev_offset;
      committed_offset = other.committed_offset;
      binlog = std::move(other.binlog);
      attrs = other.attrs;
      other.prev_offset.Clear();
      other.committed_offset.Clear();
    }
    return *this;
  }
  DISALLOW_COPY(BinlogChip);

  int size() { return binlog.size(); }
  LogOffset PendingOffset() {
    return LogOffset(BinlogOffset(attrs.filenum, attrs.offset),
                     LogicOffset(attrs.term_id, attrs.logic_id));
  }
  LogOffset GetPendingOffset() {
    return LogOffset(BinlogOffset(attrs.filenum, attrs.offset),
                     LogicOffset(attrs.term_id, attrs.logic_id));
  }
};

struct MessageSenderBaseInfo {
  int32_t session_id;
  PeerID from;
  ReplicationGroupID group_id;
  MessageType type;
  MessageSenderBaseInfo()
    : session_id(0), from(), group_id(), type(MessageType::kNone) { }
  MessageSenderBaseInfo(const ReplicationGroupID& _group_id,
                        const PeerID& _from,
                        uint32_t _session_id,
                        const MessageType& _type)
    : session_id(_session_id), from(_from), group_id(_group_id), type(_type) { }
};

struct SimpleTask {
  MessageSenderBaseInfo base;
  std::string data;

  SimpleTask() = default;
  SimpleTask(const ReplicationGroupID& _group_id,
             const PeerID& _from,
             int32_t _session_id,
             const MessageType& _type,
             std::string&& _data)
    : base(_group_id, _from, _session_id, _type),
    data(std::move(_data)) { }
  SimpleTask(SimpleTask&& other) noexcept
    : base(other.base),
    data(std::move(other.data)) { }
  SimpleTask& operator=(SimpleTask&& other) noexcept {
    if (this != &other) {
      base = other.base;
      data = std::move(other.data);
    }
    return *this;
  }
  DISALLOW_COPY(SimpleTask);
};

// For the purpose of batching binlogs, we distinguish it from SimpleTask
struct BinlogTask {
  BinlogTask(const ReplicationGroupID& _group_id,
             const PeerID& _from,
             int32_t _session_id,
             const MessageType& _type,
             BinlogChip&& _data)
    : base(_group_id, _from, _session_id, _type),
    data(std::move(_data)) { }
  BinlogTask() = default;
  BinlogTask(BinlogTask&& other) noexcept
    : base(other.base),
    data(std::move(other.data)) { }
  BinlogTask& operator=(BinlogTask&& other) noexcept {
    if (this != &other) {
      base = other.base;
      data = std::move(other.data);
    }
    return *this;
  }
  DISALLOW_COPY(BinlogTask);

  MessageSenderBaseInfo base;
  BinlogChip data;
};

using BinlogTaskList = std::list<BinlogTask>;
using SimpleTaskList = std::list<SimpleTask>;
using BinlogTaskListMap = std::unordered_map<PeerID, BinlogTaskList, hash_peer_id>;
using SimpleTaskListMap = std::unordered_map<PeerID, SimpleTaskList, hash_peer_id>;

class SimpleBuilder : public MessageBuilder {
 public:
  explicit SimpleBuilder(SimpleTask&& task)
    : task_(std::move(task)) { }
  ~SimpleBuilder() = default;
  virtual Status Build(std::string* msg) override {
    msg->swap(task_.data);
    return Status::OK();
  }

 private:
  SimpleTask task_;
};

struct MessageSenderOptions {
  ReplicationGroupID group_id;
  int batch_limit = 0;
  MessageSenderOptions() = default;
  explicit MessageSenderOptions(const ReplicationGroupID& _group_id,
                                int _batch_limit)
    : group_id(_group_id),
    batch_limit(_batch_limit) { }
};

class MessageSender {
 public:
  constexpr static int kDefaultBatchLimit = pink::kProtoMaxMessage >> 1;
  constexpr static int kBinlogSendBatchNum = 100;
  constexpr static int kBinlogSendPacketNum = 40;

  explicit MessageSender(const MessageSenderOptions& options);
  virtual ~MessageSender() = default;
  DISALLOW_COPY(MessageSender);

  bool Prepared();

  void Lock() {
    write_queue_mu_.lock();
  }

  void Unlock() {
    write_queue_mu_.unlock();
  }

  template<class... Args>
  void AppendBinlogTask(const PeerID& peer_id, Args&&... args);
  template<class... Args>
  void UnsafeAppendBinlogTask(const PeerID& peer_id, Args&&... tasks);
  void AppendBinlogTasks(const PeerID& peer_id, BinlogTaskList&& tasks);
  void UnsafeAppendBinlogTasks(const PeerID& peer_id, BinlogTaskList&& tasks);

  template<class... Args>
  void AppendSimpleTask(const PeerID& peer_id, Args&&... args);
  template<class... Args>
  void UnsafeAppendSimpleTask(const PeerID& peer_id, Args&&... args);
  void AppendSimpleTasks(const PeerID& peer_id, SimpleTaskList&& tasks);
  void UnsafeAppendSimpleTasks(const PeerID& peer_id, SimpleTaskList&& tasks);

  virtual void Messages(PeerMessageListMap& msgs) = 0;

 protected:
  void BatchTasks(BinlogTaskListMap& binlog_tasks, SimpleTaskListMap& simple_tasks);
  void UnsafeBatchBinlogTasks(BinlogTaskListMap& map);
  void UnsafeBatchSimpleTasks(SimpleTaskListMap& map);
  void Clear();
  void UnsafeClear();

  // readonly
  MessageSenderOptions options_;

  std::atomic<bool> pending_;
  std::mutex write_queue_mu_;
  BinlogTaskListMap binlog_tasks_;
  SimpleTaskListMap simple_tasks_;
};

template<class... Args>
void MessageSender::AppendSimpleTask(const PeerID& peer_id, Args&&... args) {
  std::lock_guard<std::mutex> guard(write_queue_mu_);
  UnsafeAppendSimpleTask(peer_id, std::forward<Args>(args)...);
}

template<class... Args>
void MessageSender::UnsafeAppendSimpleTask(const PeerID& peer_id, Args&&... args) {
  auto& peer_msg_list = simple_tasks_[peer_id];
  peer_msg_list.emplace_back(std::forward<Args>(args)...);
  pending_.store(true, std::memory_order_relaxed);
}

template<class... Args>
void MessageSender::AppendBinlogTask(const PeerID& peer_id, Args&&... args) {
  std::lock_guard<std::mutex> guard(write_queue_mu_);
  UnsafeAppendBinlogTask(peer_id, std::forward<Args>(args)...);
}

template<class... Args>
void MessageSender::UnsafeAppendBinlogTask(const PeerID& peer_id, Args&&... args) {
  auto& peer_msg_list = binlog_tasks_[peer_id];
  peer_msg_list.emplace_back(std::forward<Args>(args)...);
  pending_.store(true, std::memory_order_relaxed);
}

} // namespace replication

#endif // PIKA_REPL_MESSAGE_EXECUTOR_H_
