// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef INCLUDE_UTIL_MESSAGE_DECODER_H_
#define INCLUDE_UTIL_MESSAGE_DECODER_H_

#include <string>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <google/protobuf/io//coded_stream.h>

#include "proto/pika_inner_message.pb.h"
#include "include/pika_define.h"
#include "include/replication/pika_repl_io_stream.h"
#include "include/replication/pika_repl_transporter.h"

namespace test {

  extern InnerMessage::InnerRequest* ParseRequest(const std::string& msg); 

  extern InnerMessage::InnerResponse* ParseResponse(const std::string& msg);

  extern replication::MessageReporter::Message* DecodeClassicMessage(
            const std::string& msg, const PeerID& peer_id,
            const InnerMessage::ClassicMessage::MessageType& type,
            replication::IOClosure* closure);

} // namespace test

#endif // INCLUDE_UTIL_MESSAGE_DECODER_H_
