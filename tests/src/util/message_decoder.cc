// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/util/message_decoder.h"

namespace test {

using util::ResourceGuard;

InnerMessage::InnerRequest* ParseRequest(const std::string& msg) {
  InnerMessage::InnerRequest* req = new InnerMessage::InnerRequest();
  ResourceGuard<InnerMessage::InnerRequest> guard(req);
  bool parse_res = req->ParseFromString(msg);
  if (!parse_res) {
    return nullptr;
  }
  return guard.Release();
}

InnerMessage::InnerResponse* ParseResponse(const std::string& msg) {
  InnerMessage::InnerResponse* res = new InnerMessage::InnerResponse();
  ResourceGuard<InnerMessage::InnerResponse> guard(res);
  bool parse_res = res->ParseFromString(msg);
  if (!parse_res) {
    return nullptr;
  }
  return guard.Release();
}

replication::MessageReporter::Message* DecodeClassicMessage(
    const std::string& msg, const PeerID& peer_id,
    const InnerMessage::ClassicMessage::MessageType& type, replication::IOClosure* closure) {
  InnerMessage::ClassicMessage* classic_msg = new InnerMessage::ClassicMessage();
  classic_msg->set_msg_type(type);
  if (type == InnerMessage::ClassicMessage::kRequestType) {
    auto request = ParseRequest(msg);
    if (request == nullptr) return nullptr;
    classic_msg->set_allocated_request(request);
  } else {
    auto response = ParseResponse(msg);
    if (response == nullptr) return nullptr;
    classic_msg->set_allocated_response(response);
  }

  InnerMessage::ProtocolMessage* proto_msg = new InnerMessage::ProtocolMessage();
  proto_msg->set_proto_type(InnerMessage::ProtocolMessage::kClassicType);
  proto_msg->set_allocated_classic_msg(classic_msg);

  auto peer_msg = new replication::MessageReporter::Message(
      peer_id, proto_msg, closure);
  return peer_msg;
}

} // namespace test
