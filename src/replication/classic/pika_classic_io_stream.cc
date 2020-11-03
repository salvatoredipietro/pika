// Copyright (c) 2019-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/replication/classic/pika_classic_io_stream.h"

#include <glog/logging.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <google/protobuf/io//coded_stream.h>

#include "proto/pika_inner_message.pb.h"
#include "include/replication/pika_repl_manager.h"

namespace replication {

ClassicClientStream::ClassicClientStream(int fd, std::string ip_port,
                                         pink::Thread* thread,
                                         void* worker_specific_data,
                                         pink::PinkEpoll* epoll,
                                         MessageReporter* reporter,
                                         int max_conn_rbuf_size)
    : ClientStream(fd, ip_port, thread, worker_specific_data, epoll, reporter, max_conn_rbuf_size) {
}

int ClassicClientStream::DealMessage() {
  PeerID peer_id;
  if (!peer_id.ParseFromIpPort(ip_port(), true/*is_connect_port*/)) {
    LOG(WARNING) << "ParseIpPort FAILED! " << " ip_port: " << ip_port();
    return -1;
  }
  InnerMessage::InnerResponse* response = new InnerMessage::InnerResponse();
  util::ResourceGuard<InnerMessage::InnerResponse> guard(response);

  ::google::protobuf::io::ArrayInputStream input(rbuf_ + cur_pos_ - header_len_, header_len_);
  ::google::protobuf::io::CodedInputStream decoder(&input);
  decoder.SetTotalBytesLimit(max_conn_rbuf_size_, max_conn_rbuf_size_);
  bool success = response->ParseFromCodedStream(&decoder) && decoder.ConsumedEntireMessage();
  if (!success) {
    LOG(WARNING) << "ParseFromArray FAILED! " << " msg_len: " << header_len_;
    reporter_->ReportTransportResult(PeerInfo(peer_id, fd()),
                                     MessageReporter::ResultType::kResponseParseError,
                                     MessageReporter::DirectionType::kClient);
    return -1;
  }
  InnerMessage::ClassicMessage* classic_msg = new InnerMessage::ClassicMessage();
  classic_msg->set_msg_type(InnerMessage::ClassicMessage::kResponseType);
  classic_msg->set_allocated_response(guard.Release());

  InnerMessage::ProtocolMessage* proto_msg = new InnerMessage::ProtocolMessage();
  proto_msg->set_proto_type(InnerMessage::ProtocolMessage::kClassicType);
  proto_msg->set_allocated_classic_msg(classic_msg);

  ClientStreamClosure* c = new ClientStreamClosure(std::static_pointer_cast<ClientStream>(shared_from_this()));
  Message peer_msg(peer_id, proto_msg, static_cast<IOClosure*>(c));
  reporter_->HandlePeerMessage(&peer_msg);
  return 0;
}

ClassicServerStream::ClassicServerStream(int fd, std::string ip_port,
                                         pink::Thread* thread,
                                         void* worker_specific_data,
                                         pink::PinkEpoll* epoll,
                                         MessageReporter* reporter)
   : ServerStream(fd, ip_port, thread, worker_specific_data, epoll, reporter) {
}

int ClassicServerStream::DealMessage() {
  PeerID peer_id;
  if (!peer_id.ParseFromIpPort(ip_port())) {
    LOG(WARNING) << "ParseIpPort FAILED! " << " ip_port: " << ip_port();
    return -1;
  }
  InnerMessage::InnerRequest* req = new InnerMessage::InnerRequest();
  util::ResourceGuard<InnerMessage::InnerRequest> guard(req);
  bool parse_res = req->ParseFromArray(rbuf_ + cur_pos_ - header_len_, header_len_);
  if (!parse_res) {
    LOG(WARNING) << "Pika repl server connection pb parse error.";
    return -1;
  }
  InnerMessage::ClassicMessage* classic_msg = new InnerMessage::ClassicMessage();
  classic_msg->set_msg_type(InnerMessage::ClassicMessage::kRequestType);
  classic_msg->set_allocated_request(guard.Release());

  InnerMessage::ProtocolMessage* proto_msg = new InnerMessage::ProtocolMessage();
  proto_msg->set_proto_type(InnerMessage::ProtocolMessage::kClassicType);
  proto_msg->set_allocated_classic_msg(classic_msg);

  ServerStreamClosure* c = new ServerStreamClosure(std::static_pointer_cast<ServerStream>(shared_from_this()));
  Message peer_msg(peer_id, proto_msg, static_cast<IOClosure*>(c));
  reporter_->HandlePeerMessage(&peer_msg);
  return 0;
}

} // namespace replication
