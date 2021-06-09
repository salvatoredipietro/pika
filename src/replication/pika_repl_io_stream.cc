// Copyright (c) 2019-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/replication/pika_repl_io_stream.h"

#include "include/pika_define.h"
#include "include/replication/pika_repl_transporter.h"

namespace replication {

void ClientStreamFactory::NotifyNewClientStream(std::shared_ptr<ClientStream> client_stream) const {
  if (reporter_ == nullptr || client_stream == nullptr) {
    return;
  }
  PeerID peer_id;
  if (!peer_id.ParseFromIpPort(client_stream->ip_port(), true)) {
    LOG(ERROR) << "Can not parse peer_id from the ip_port: " << client_stream->ip_port();
    return;
  }
  reporter_->PinClientStream(peer_id, client_stream);
}

} // namespace replication
