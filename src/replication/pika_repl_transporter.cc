// Copyright (c) 2019-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/replication/pika_repl_transporter.h"

namespace replication {

MessageReporter::Message::Message(const PeerID& peer_id,
                                  InnerMessage::ProtocolMessage* proto_msg,
                                  IOClosure* closure)
  : peer_id_(peer_id), proto_msg_(proto_msg), closure_(closure) { }

MessageReporter::Message::~Message() {
  if (closure_ != nullptr) {
    closure_->Release();
    closure_ = nullptr;
  }
  if (proto_msg_ != nullptr) {
    delete proto_msg_;
    proto_msg_ = nullptr;
  }
}

InnerMessage::ProtocolMessage* MessageReporter::Message::release_proto_msg() {
  InnerMessage::ProtocolMessage* cur = proto_msg_;
  proto_msg_ = nullptr;
  return cur;
}

IOClosure* MessageReporter::Message::release_closure() {
  IOClosure* cur = closure_;
  closure_ = nullptr;
  return cur;
}

Peer::Peer(const PeerID& peer_id,
           MessageReporter* reporter,
           PikaReplClientIOThread* client_thread)
  : peer_id_(peer_id),
  reporter_(reporter),
  client_stream_(nullptr),
  server_stream_(nullptr),
  client_thread_(client_thread) {
}

Peer::Peer(const PeerID& peer_id,
           const std::shared_ptr<ServerStream>& server_stream,
           const std::shared_ptr<ClientStream>& client_stream,
           MessageReporter* reporter,
           PikaReplClientIOThread* client_thread)
  : peer_id_(peer_id),
  reporter_(reporter),
  client_stream_(client_stream),
  server_stream_(server_stream),
  client_thread_(client_thread) {
  if (server_stream_ != nullptr) {
    server_stream_->set_pinned(true);
  }
  if (client_stream_ != nullptr) {
    client_stream_->set_pinned(true);
  }
}

Status Peer::SendToPeer(const std::string& data, bool is_response) {
  // NOTE: we may write to a outdated stream which the underlying socket fd has been
  //       closed but we have not learned it. In this case, the old fd may have been
  //       reused by another stream, the data will be lost and the new stream will
  //       be woken up by mistake (It's ok right now as the underlying epoll
  //       will not reuse the fd until the closed event has been handled).
  //
  //       The two lines should be considered separately:
  //       1) For the messages in Line1 (client stream)
  //            we use client_thread_ to handle messages instead of client_stream_
  //          as the client_thread_ owns the consistent view.
  //       2) For the messages in Line2 (server stream)
  //            the pending messages should be cleaned up before the stream is closed(Unpin).
  //          But there is a time window that causes some data still alive. Some of them may
  //          be sent to the outdated stream which will cause the problem described as above.
  //          Some of them may be sent to the new stream, in this case, if the data to be
  //          sent is related to the stream, we should not send them on the new connection.
  //          Otherwise, it is ok to continue sending them.
  if (is_response) {
    // TODO: Attach a session id to the data, only send them to the stream with the
    //       same id.
    std::lock_guard<std::mutex> guard(mutex_);
    if (server_stream_ == nullptr) {
      return Status::NotFound("ServerStream");
    }
    if (server_stream_->WriteResp(data) != 0) {
      server_stream_->NotifyClose();
      return Status::Corruption("Write Resp Failed to " + peer_id_.ToString());
    }
    server_stream_->NotifyWrite();
    return Status::OK();
  }
  Status s = client_thread_->Write(peer_id_.Ip(), peer_id_.ConnectPort(), data);
  if (!s.ok()) {
    LOG(ERROR) << "Client Stream write failed: " + s.ToString();
  }
  return s;
}

TransporterOptions::TransporterOptions(const PeerID& local_id,
                                       MessageReporter* reporter,
                                       ClientStreamFactory* client_stream_factory,
                                       ServerStreamFactory* server_stream_factory)
  : local_id_(local_id),
  reporter_(reporter),
  client_stream_factory_(client_stream_factory),
  server_stream_factory_(server_stream_factory) { }

TransporterOptions::TransporterOptions(TransporterOptions&& other) noexcept
  : local_id_(other.local_id_),
  reporter_(other.reporter_),
  client_stream_factory_(other.client_stream_factory_),
  server_stream_factory_(other.server_stream_factory_) {
  other.local_id_.Clear();
  other.reporter_ = nullptr;
  other.client_stream_factory_ = nullptr;
  other.server_stream_factory_ = nullptr;
}

TransporterOptions& TransporterOptions::operator=(TransporterOptions&& other) noexcept {
  if (this != &other) {
    local_id_ = other.local_id_;
    reporter_ = reporter_;
    if (client_stream_factory_ != nullptr) {
      delete client_stream_factory_;
    }
    if (server_stream_factory_ != nullptr) {
      delete server_stream_factory_;
    }
    client_stream_factory_ = other.client_stream_factory_;
    server_stream_factory_ = other.server_stream_factory_;
    other.local_id_.Clear();
    other.client_stream_factory_ = nullptr;
    other.server_stream_factory_ = nullptr;
  }
  return *this;
}

TransporterOptions::~TransporterOptions() {
  if (client_stream_factory_ != nullptr) {
    delete client_stream_factory_;
    client_stream_factory_ = nullptr;
  }
  if (server_stream_factory_ != nullptr) {
    delete server_stream_factory_;
    server_stream_factory_ = nullptr;
  }
}

void TransporterOptions::ReleaseTo(TransporterOptions* other) {
  if (other == nullptr) {
    return;
  }
  if (other->client_stream_factory_ != nullptr) {
    delete other->client_stream_factory_;
  }
  other->client_stream_factory_ = client_stream_factory_;
  client_stream_factory_ = nullptr;
  if (other->server_stream_factory_ != nullptr) {
    delete other->server_stream_factory_;
  }
  other->server_stream_factory_ = server_stream_factory_;
  server_stream_factory_ = nullptr;
  other->local_id_ = local_id_;
  local_id_.Clear();
  other->reporter_ = reporter_;
  reporter_ = nullptr;
}

TransporterImpl::TransporterImpl(TransporterOptions&& options)
  : options_(std::move(options)) {
  std::set<std::string> ips;
  ips.insert(options_.local_id().Ip());
  ips.insert("0.0.0.0");
  client_thread_ = new PikaReplClientIOThread(options_.reporter(), options_.client_stream_factory(), 3000, 60);
  server_thread_ = new PikaReplServerIOThread(options_.reporter(), options_.server_stream_factory(),
                                              ips, options_.local_id().ConnectPort(), 3000);
  pthread_rwlock_init(&map_mu_, NULL);
}

TransporterImpl::~TransporterImpl() {
  if (server_thread_ != nullptr) {
    delete server_thread_;
    server_thread_ = nullptr;
  }
  if (client_thread_ != nullptr) {
    delete client_thread_;
    client_thread_ = nullptr;
  }
  pthread_rwlock_destroy(&map_mu_);
}

Status TransporterImpl::WriteToPeer(PeerMessageListMap&& msgs) {
  Status s;
  std::string data;
  for (auto& iter : msgs) {
    const PeerID& peer_id = iter.first;
    auto& peer_msgs = iter.second;
    for (auto& msg : peer_msgs) {
      data.clear();
      auto peer_ptr = GetOrAddPeer(peer_id, msg.type == MessageType::kRequest/*create_if_missing*/);
      if (peer_ptr == nullptr) {
        return Status::Corruption("Can not get or create peer: " + peer_id.ToString());
      }
      s = msg.builder->Build(&data);
      if (!s.ok()) {
        LOG(ERROR) << "Peer message build failed: " + s.ToString();
        continue;
      }
      s = peer_ptr->SendToPeer(data, msg.type == MessageType::kResponse);
      if (!s.ok()) {
        LOG(WARNING) << "Write to "<< peer_id.ToString() << ", error: " + s.ToString();
      }
    }
  }
  return s;
}

Status TransporterImpl::AddPeer(const PeerID& peer_id) {
  auto peer_ptr = GetOrAddPeer(peer_id, true);
  if (peer_ptr == nullptr) {
    return Status::Corruption("Create peer " + peer_id.ToString() + " failed");
  }
  return Status::OK();
}

Status TransporterImpl::RemovePeer(const PeerID& peer_id) {
  {
    slash::RWLock l(&map_mu_, true);
    auto iter = peer_map_.find(peer_id);
    if (iter != peer_map_.end()) {
      iter->second->ResetStreams();
      peer_map_.erase(peer_id);
    }
  }
  return Status::OK();
}

bool TransporterImpl::PinServerStream(const PeerID& peer_id,
                                      std::shared_ptr<ServerStream> server_stream) {
  if (server_stream != nullptr && server_stream->pinned()) {
    return true;
  }
  auto peer_ptr = GetOrAddPeer(peer_id, true, server_stream);
  if (peer_ptr == nullptr) {
    LOG(ERROR) << "Can not get or create peer: " << peer_id.ToString();
  }
  return peer_ptr != nullptr;
}

bool TransporterImpl::PinClientStream(const PeerID& peer_id,
                                      std::shared_ptr<ClientStream> client_stream) {
  if (client_stream != nullptr && client_stream->pinned()) {
    return true;
  }
  auto peer_ptr = GetOrAddPeer(peer_id, true, nullptr, client_stream);
  if (peer_ptr == nullptr) {
    LOG(ERROR) << "Can not get or create peer: " << peer_id.ToString();
  }
  return peer_ptr != nullptr;
}

void TransporterImpl::UnpinServerStream(const PeerID& peer_id) {
  // the server connection is broken(timedout, closed).
  //
  // There may be a race condition: Unpin a broken connection and pin a new connection.
  // we must ensure the unpin happens before pin.
  slash::RWLock l(&map_mu_, false);
  auto iter = peer_map_.find(peer_id);
  if (iter == peer_map_.end()) {
    return;
  }
  iter->second->ResetServerStream();
}

void TransporterImpl::UnpinClientStream(const PeerID& peer_id) {
  // the client connection is broken(timedout, closed).
  //
  // There may be a race condition: Unpin a broken connection and pin a new connection.
  // we must ensure the unpin happens before pin.
  slash::RWLock l(&map_mu_, false);
  auto iter = peer_map_.find(peer_id);
  if (iter == peer_map_.end()) {
    return;
  }
  iter->second->ResetClientStream();
}

std::shared_ptr<Peer> TransporterImpl::GetOrAddPeer(const PeerID& peer_id, bool create_if_missing,
                                                    std::shared_ptr<ServerStream> server_stream,
                                                    std::shared_ptr<ClientStream> client_stream) {
  std::shared_ptr<Peer> peer_ptr = nullptr;
  {
    slash::RWLock l(&map_mu_, false);
    auto iter = peer_map_.find(peer_id);
    if (iter != peer_map_.end()) {
      peer_ptr = iter->second;
      if (client_stream != nullptr) {
        peer_ptr->PinClientStream(client_stream);
      }
      if (server_stream != nullptr) {
        peer_ptr->PinServerStream(server_stream);
      }
      return peer_ptr;
    } else if (!create_if_missing) {
      return peer_ptr;
    }
  }
  {
    slash::RWLock l(&map_mu_, true);
    auto iter = peer_map_.find(peer_id);
    if (iter != peer_map_.end()) {
      peer_ptr = iter->second;
      if (client_stream != nullptr) {
        peer_ptr->PinClientStream(client_stream);
      }
      if (server_stream != nullptr) {
        peer_ptr->PinServerStream(server_stream);
      }
      return peer_ptr;
    }
    peer_ptr = std::make_shared<Peer>(peer_id, server_stream, client_stream,
                                      options_.reporter(), client_thread_);
    peer_map_.insert({peer_id, peer_ptr});
  }
  return peer_ptr;
}

int TransporterImpl::Start() {
  int ret = client_thread_->StartThread();
  if (ret != pink::kSuccess) {
    LOG(ERROR) << "Start Repl Client Error: " << ret
               << (ret == pink::kCreateThreadError ? ": create thread error " : ": other error");
    return ret;
  }
  ret = server_thread_->StartThread();
  if (ret != pink::kSuccess) {
    LOG(ERROR) << "Start Repl Server Error: " << ret
               << (ret == pink::kCreateThreadError ? ": create thread error " : ": other error");
    return ret;
  }
  return ret;
}

int TransporterImpl::Stop() {
  int ret = client_thread_->StopThread();
  if (ret != pink::kSuccess) {
    LOG(ERROR) << "Stop Repl Client failed.";
    return ret;
  }
  ret = server_thread_->StopThread();
  if (ret != pink::kSuccess) {
    LOG(ERROR) << "Stop Repl Server failed.";
    return ret;
  }
  return ret;
}

} // namespace replication
