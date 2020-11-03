// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef REPLICATION_PIKA_CLASSIC_MESSAGE_EXECUTOR_H_
#define REPLICATION_PIKA_CLASSIC_MESSAGE_EXECUTOR_H_

#include <vector>
#include <memory>

#include "slash/include/slash_status.h"

#include "proto/pika_inner_message.pb.h"
#include "include/pika_define.h"
#include "include/util/thread_pool.h"
#include "include/replication/pika_repl_transporter.h"
#include "include/replication/pika_repl_message_executor.h"

namespace replication {

class ClassicReplManager;

class MessageHandlerBase : public util::thread::Task {
 public:
  // Responsible for managing the lifetime of closure.
  MessageHandlerBase(const PeerID& peer_id,
                     IOClosure* closure,
                     ClassicReplManager* rm)
    : peer_id_(peer_id), closure_(closure), rm_(rm) {}
  virtual ~MessageHandlerBase() {
    if (closure_ != nullptr) {
      // self-deleting
      closure_->Run();
      closure_ = nullptr;
    }
  }

 protected:
  const PeerID peer_id_;
  IOClosure* closure_;
  ClassicReplManager* const rm_;
};

class RequestHandler : public MessageHandlerBase {
 public:
  // Responsible for managing the lifetime of request.
  RequestHandler(const PeerID& peer_id,
                 const InnerMessage::InnerRequest* request,
                 IOClosure* closure,
                 ClassicReplManager* rm)
    : MessageHandlerBase(peer_id, closure, rm),
    request_(request) {}

  virtual ~RequestHandler() {
    if (request_ != nullptr) {
      delete request_;
      request_ = nullptr;
    }
  }

 protected:
  const InnerMessage::InnerRequest* request_;
};

class MetaSyncRequestHandler final : public RequestHandler {
 public:
  MetaSyncRequestHandler(const PeerID& peer_id,
                         const InnerMessage::InnerRequest* request,
                         IOClosure* closure,
                         ClassicReplManager* rm)
    : RequestHandler(peer_id, request, closure, rm) { }

  virtual void Run() override;
};

class TrySyncRequestHandler final : public RequestHandler {
 public:
  TrySyncRequestHandler(const PeerID& peer_id,
                        const InnerMessage::InnerRequest* request,
                        IOClosure* closure,
                        ClassicReplManager* rm)
    : RequestHandler(peer_id, request, closure, rm) { }

  virtual void Run() override;
};

class SnapshotSyncRequestHandler final : public RequestHandler {
 public:
  SnapshotSyncRequestHandler(const PeerID& peer_id,
                             const InnerMessage::InnerRequest* request,
                             IOClosure* closure,
                             ClassicReplManager* rm)
    : RequestHandler(peer_id, request, closure, rm) { }

  virtual void Run() override;
};

class BinlogSyncRequestHandler final : public RequestHandler {
 public:
  BinlogSyncRequestHandler(const PeerID& peer_id,
                           const InnerMessage::InnerRequest* request,
                           IOClosure* closure,
                           ClassicReplManager* rm)
    : RequestHandler(peer_id, request, closure, rm) { }

  virtual void Run() override;
};

class RemoveSlaveNodeRequestHandler final : public RequestHandler {
 public:
  RemoveSlaveNodeRequestHandler(const PeerID& peer_id,
                                const InnerMessage::InnerRequest* request,
                                IOClosure* closure,
                                ClassicReplManager* rm)
    : RequestHandler(peer_id, request, closure, rm) { }

  virtual void Run() override;
};

class ResponseHandler : public MessageHandlerBase {
 public:
  // Responsible for managing the lifetime of response.
  ResponseHandler(const PeerID& peer_id,
                  InnerMessage::InnerResponse* response,
                  IOClosure* closure,
                  ClassicReplManager* rm)
    : MessageHandlerBase(peer_id, closure, rm),
    response_(response) {}

  virtual ~ResponseHandler() {
    if (response_ != nullptr) {
      delete response_;
      response_ = nullptr;
    }
  }

 protected:
  InnerMessage::InnerResponse* response_;
};

class MetaSyncResponseHandler final : public ResponseHandler {
 public:
  MetaSyncResponseHandler(const PeerID& peer_id,
                          InnerMessage::InnerResponse* response,
                          IOClosure* closure,
                          ClassicReplManager* rm)
    : ResponseHandler(peer_id, response, closure, rm) { }

  virtual void Run() override;

 private:
  static bool IsTableStructConsistent(const std::vector<TableStruct>& current_tables,
                                      const std::vector<TableStruct>& expect_tables);
};

class SnapshotSyncResponseHandler final : public ResponseHandler {
 public:
  SnapshotSyncResponseHandler(const ReplicationGroupID& group_id,
                              const PeerID& peer_id,
                              InnerMessage::InnerResponse* response,
                              IOClosure* closure,
                              ClassicReplManager* rm)
    : ResponseHandler(peer_id, response, closure, rm), group_id_(group_id) { }

  virtual void Run() override;

 private:
  const ReplicationGroupID group_id_;
};

class TrySyncResponseHandler final : public ResponseHandler {
 public:
  TrySyncResponseHandler(const ReplicationGroupID& group_id,
                         const PeerID& peer_id,
                         InnerMessage::InnerResponse* response,
                         IOClosure* closure,
                         ClassicReplManager* rm)
    : ResponseHandler(peer_id, response, closure, rm), group_id_(group_id) { }

  virtual void Run() override;

 private:
  const ReplicationGroupID group_id_;
};

class BinlogAppendHandler final : public ResponseHandler {
 public:
  // Responsible for managing the lifetime of indexes.
  BinlogAppendHandler(const ReplicationGroupID& group_id,
                      const PeerID& peer_id,
                      InnerMessage::InnerResponse* response,
                      std::vector<int>* indexes,
                      IOClosure* closure,
                      ClassicReplManager* rm)
    : ResponseHandler(peer_id, response, closure, rm),
    group_id_(group_id),
    indexes_(indexes) { }

  virtual ~BinlogAppendHandler() {
    if (indexes_ != nullptr) {
      delete indexes_;
      indexes_ = nullptr;
    }
  }

  virtual void Run() override;

 private:
  const ReplicationGroupID group_id_;
  std::vector<int>* indexes_;
};

class RemoveSlaveNodeResponseHandler final : public ResponseHandler {
 public:
  RemoveSlaveNodeResponseHandler(const PeerID& peer_id,
                                 InnerMessage::InnerResponse* response,
                                 IOClosure* closure,
                                 ClassicReplManager* rm)
    : ResponseHandler(peer_id, response, closure, rm) {}

  virtual void Run() override;
};

class ClassicBinlogBuilder : public MessageBuilder {
 public:
  explicit ClassicBinlogBuilder(BinlogTaskList&& tasks)
    : tasks_(std::move(tasks)) { }

  virtual Status Build(std::string* msg) override;

 private:
  static Status BuildBinlogSyncResp(BinlogTaskList& tasks,
                                    InnerMessage::InnerResponse* response);
  BinlogTaskList tasks_;
};

class ClassicMessageSender : public MessageSender {
 public:
  explicit ClassicMessageSender(const MessageSenderOptions& options);

  virtual void Messages(PeerMessageListMap& msgs) override;
};

} // namespace replication

#endif // REPLICATION_PIKA_CLASSIC_MESSAGE_EXECUTOR_H_
