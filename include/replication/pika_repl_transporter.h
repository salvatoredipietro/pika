// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef REPLICATION_PIKA_REPL_TRANSPORTER_H_
#define REPLICATION_PIKA_REPL_TRANSPORTER_H_

#include <memory>
#include <utility>
#include <string>
#include <list>
#include <mutex>
#include <unordered_map>

#include "slash/include/slash_status.h"
#include "slash/include/slash_mutex.h"

#include "proto/pika_inner_message.pb.h"
#include "include/util/callbacks.h"
#include "include/pika_define.h"
#include "include/replication/pika_repl_io_stream.h"
#include "include/replication/pika_repl_io_thread.h"

namespace replication {

using slash::Status;

class MessageReporter {
 public:
  class Message {
   public:
    // Responsible for managing the lifetime of proto_msg and closure.
    Message(const PeerID& peer_id,
            InnerMessage::ProtocolMessage* proto_msg,
            IOClosure* closure);
    ~Message();
    DISALLOW_COPY(Message);

    const PeerID& peer_id() const { return peer_id_; }
    InnerMessage::ProtocolMessage* proto_msg() { return proto_msg_; }
    InnerMessage::ProtocolMessage* release_proto_msg();
    IOClosure* closure() { return closure_; }
    IOClosure* release_closure();

   private:
    PeerID peer_id_;
    InnerMessage::ProtocolMessage* proto_msg_;
    IOClosure* closure_;
  };
  enum class ResultType : uint8_t {
    kOK                 = 0,
    kResponseParseError = 1,
    kFdClosed           = 2,
    kFdTimedout         = 3,
  };
  enum class DirectionType : uint8_t {
    kClient             = 0,
    kServer             = 1,
    kClientAndServer    = 2,
  };

 public:
  MessageReporter() = default;
  virtual ~MessageReporter() = default;
  virtual void HandlePeerMessage(Message* msg) = 0;
  virtual void ReportTransportResult(const PeerInfo& peer_info,
                                     const ResultType& r_type,
                                     const DirectionType& d_type) = 0;
  virtual void PinServerStream(const PeerID& peer_id,
                               std::shared_ptr<pink::PbConn> server_stream) {}
  virtual void PinClientStream(const PeerID& peer_id,
                               std::shared_ptr<pink::PbConn> client_stream) {}
  virtual void UnpinPeerConnections(const PeerID& peer_id, const DirectionType& d_type) {}
};

// Handle the transport to the specific peer instance.
// There are two connections between peers.
//
// ---------------------------------------
// Local                             Remote
// clientStream -------dial------> ServerStream (Line1)
// ServerStream <------dial------- ClientStream (Line2)
// ---------------------------------------
//
// When we start a local Peer, we start a clientStream (connect to peer).
// Attach the ServerStream to the Peer, when we receive a connection from peer.
//
// 1) In Line1, we send the requests and then wait for the responses.
// 2) In Line2, we send the responses to the remote.
class Peer {
 public:
  Peer(const PeerID& peer_id,
       MessageReporter* reporter,
       PikaReplClientIOThread* client_thread);
  Peer(const PeerID& peer_id,
       const std::shared_ptr<ServerStream>& server_stream,
       const std::shared_ptr<ClientStream>& client_stream,
       MessageReporter* reporter,
       PikaReplClientIOThread* client_thread);

  void PinServerStream(std::shared_ptr<ServerStream> server_stream) {
    std::lock_guard<std::mutex> guard(mutex_);
    server_stream_ = server_stream;
    server_stream_->set_pinned(true);
  }

  void PinClientStream(std::shared_ptr<ClientStream> client_stream) {
    std::lock_guard<std::mutex> guard(mutex_);
    client_stream_ = client_stream;
    client_stream_->set_pinned(true);
  }

  /*
   * ResetServerStream remove the pined server stream.
   * @return return true when there are no other streams,
   *         which means it's safe to remove the peer.
   */
  bool ResetServerStream() {
    std::lock_guard<std::mutex> guard(mutex_);
    if (server_stream_ != nullptr) {
      server_stream_->set_pinned(false);
      server_stream_ = nullptr;
    }
    return client_stream_ == nullptr;
  }

  /*
   * ResetClientStream remove the pined client stream.
   * @return return true when there are no other streams,
   *         which means it's safe to remove the peer.
   */
  bool ResetClientStream() {
    std::lock_guard<std::mutex> guard(mutex_);
    if (client_stream_ != nullptr) {
      client_stream_->set_pinned(false);
      client_stream_ = nullptr;
    }
    return server_stream_ == nullptr;
  }

  bool ResetStreams() {
    std::lock_guard<std::mutex> guard(mutex_);
    client_stream_ = nullptr;
    server_stream_ = nullptr;
    return true;
  }

  void AddMember(const ReplicationGroupID& group_id) {
    std::lock_guard<std::mutex> guard(mutex_);
    member_ids_.insert(group_id);
  }

  /*
   * RemoveMember remove the replication group.
   * @return return true when there are no more groups.
   */
  bool RemoveMember(const ReplicationGroupID& group_id) {
    std::lock_guard<std::mutex> guard(mutex_);
    member_ids_.erase(group_id);
    return member_ids_.size() <= 0;
  }

  /*
   * Send the specific data to remote.
   * @param data : the data should be sent to peer.
   * @param is_response : mark which line should we pick to send.
   */
  Status SendToPeer(const std::string& data, bool is_response);

 private:
  PeerID peer_id_;
  MessageReporter* reporter_;

  std::mutex mutex_;
  std::set<ReplicationGroupID> member_ids_;
  std::shared_ptr<ClientStream> client_stream_;
  std::shared_ptr<ServerStream> server_stream_;
  PikaReplClientIOThread* client_thread_;
};

class TransporterOptions {
 public:
  // Transfer the ownership of stream_factory into the TransporterOptions.
  TransporterOptions(const PeerID& local_id,
                     MessageReporter* reporter_,
                     ClientStreamFactory* client_stream_factory,
                     ServerStreamFactory* server_stream_factory);
  TransporterOptions(TransporterOptions&& other) noexcept;
  TransporterOptions& operator=(TransporterOptions&& other) noexcept;
  ~TransporterOptions();
  void ReleaseTo(TransporterOptions* other);

  const PeerID& local_id() const { return local_id_; }
  MessageReporter* reporter() { return reporter_; }
  ClientStreamFactory* client_stream_factory() { return client_stream_factory_; }
  ServerStreamFactory* server_stream_factory() { return server_stream_factory_; }

  DISALLOW_COPY(TransporterOptions);

 private:
  PeerID local_id_;
  MessageReporter* reporter_;
  ClientStreamFactory* client_stream_factory_;
  ServerStreamFactory* server_stream_factory_;
};

enum class MessageType : uint8_t {
  kNone     = 0,
  // Response message will be dispatched to server-side stream
  kResponse = 1,
  // Request message will be dispatched to client-side stream
  kRequest  = 2,
};

class MessageBuilder {
 public:
  MessageBuilder() = default;
  virtual ~MessageBuilder() = default;
  virtual Status Build(std::string* msg) = 0;

 protected:
  static void BuildBinlogOffset(const LogOffset& offset,
                                InnerMessage::BinlogOffset* boffset) {
    boffset->set_filenum(offset.b_offset.filenum);
    boffset->set_offset(offset.b_offset.offset);
    boffset->set_term(offset.l_offset.term);
    boffset->set_index(offset.l_offset.index);
  }
};

struct PeerMessage {
  MessageType type;
  std::unique_ptr<MessageBuilder> builder;

  PeerMessage(const MessageType& _type,
              std::unique_ptr<MessageBuilder> _builder)
    : type(_type), builder(std::move(_builder)) { }
  PeerMessage(PeerMessage&& other) noexcept
    : type(other.type), builder(std::move(other.builder)) { }
  PeerMessage& operator=(PeerMessage&& other) noexcept {
    if (&other != this) {
      type = other.type;
      builder = std::move(other.builder);
    }
    return *this;
  }
};

using PeerMessageList = std::list<PeerMessage>;
using PeerMessageListMap = std::unordered_map<PeerID, PeerMessageList, hash_peer_id>;;

class Transporter {
 public:
  Transporter() = default;
  virtual ~Transporter() = default;

  virtual int Start() = 0;
  virtual int Stop() = 0;

  // Add a Peer stub
  virtual Status AddPeer(const PeerID& peer_id) = 0;
  // Remove a Peer stub
  virtual Status RemovePeer(const PeerID& peer_id) = 0;
  virtual Status WriteToPeer(PeerMessageListMap&& msgs) = 0;

  virtual bool PinServerStream(const PeerID& peer_id,
                               std::shared_ptr<ServerStream> server_stream) = 0;
  virtual bool PinClientStream(const PeerID& peer_id,
                               std::shared_ptr<ClientStream> client_stream) = 0;
  virtual void UnpinServerStream(const PeerID& peer_id) = 0;
  virtual void UnpinClientStream(const PeerID& peer_id) = 0;
};

class TransporterImpl : public Transporter {
 public:
  explicit TransporterImpl(TransporterOptions&& options);
  ~TransporterImpl();

  virtual int Start() override;
  virtual int Stop() override;

  virtual Status AddPeer(const PeerID& peer_id) override;
  virtual Status RemovePeer(const PeerID& peer_id) override;
  virtual Status WriteToPeer(PeerMessageListMap&& msgs) override;

  virtual bool PinServerStream(const PeerID& peer_id,
                               std::shared_ptr<ServerStream> server_stream) override;
  virtual bool PinClientStream(const PeerID& peer_id,
                               std::shared_ptr<ClientStream> client_stream) override;
  virtual void UnpinServerStream(const PeerID& peer_id) override;
  virtual void UnpinClientStream(const PeerID& peer_id) override;

 private:
  std::shared_ptr<Peer> GetOrAddPeer(const PeerID& peer_id, bool create_if_missing,
                                     std::shared_ptr<ServerStream> server_stream = nullptr,
                                     std::shared_ptr<ClientStream> client_stream = nullptr);
  using PeerMap = std::unordered_map<PeerID, std::shared_ptr<Peer>, hash_peer_id>;

  TransporterOptions options_;

  pthread_rwlock_t map_mu_;
  PeerMap peer_map_;

  PikaReplServerIOThread* server_thread_;
  PikaReplClientIOThread* client_thread_;
};

} // namespace replication

#endif  //  REPLICATION_PIKA_REPL_TRANSPORTER_H_
