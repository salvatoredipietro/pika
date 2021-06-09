// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef REPLICATION_PIKA_CLASSIC_IO_STREAM_H_
#define REPLICATION_PIKA_CLASSIC_IO_STREAM_H_

#include <memory>
#include <string>

#include "pink/include/pb_conn.h"

#include "include/util/callbacks.h"
#include "include/replication/pika_repl_io_stream.h"

namespace replication {

/*
 * ClassicClientStream post the local messages to peer through the connection dialed by local.
 *
 * ClientSide
 *
 * The outgoing messages including:
 * 1) MetaSyncRequest
 * 2) DBSyncRequest
 * 3) TrySyncRequest
 * 4) BinlogSyncRequest
 *
 * The incoming messages including:
 * 1) MetaSyncResponse
 * 2) DBSyncResponse
 * 3) TrySyncResponse
 * 4) BinlogSyncResponse
 */ 
class ClassicClientStream : public ClientStream {
 public:
  ClassicClientStream(int fd,
                      std::string ip_port,
                      pink::Thread* thread,
                      void* worker_specific_data,
                      pink::PinkEpoll* epoll,
                      MessageReporter* reporter,
                      int max_conn_rbuf_size);
  ~ClassicClientStream() = default;

  virtual int DealMessage() override;
};

class ClassicClientStreamFactory :  public ClientStreamFactory {
 public:
  ClassicClientStreamFactory(MessageReporter* reporter,
                             int max_conn_rbuf_size)
    : ClientStreamFactory(reporter, max_conn_rbuf_size) { }
  ~ClassicClientStreamFactory() = default;

  virtual std::shared_ptr<pink::PinkConn> NewPinkConn(int connfd,
                                                      const std::string& ip_port,
                                                      pink::Thread* thread,
                                                      void* worker_specific_data,
                                                      pink::PinkEpoll* pink_epoll) const override {
    auto client_stream = std::make_shared<ClassicClientStream>(connfd,
                                                               ip_port,
                                                               thread,
                                                               worker_specific_data,
                                                               pink_epoll,
                                                               reporter_,
                                                               max_conn_rbuf_size_);
    NotifyNewClientStream(std::static_pointer_cast<ClientStream>(client_stream));
    return std::static_pointer_cast<pink::PinkConn>(client_stream);
  }
};

/*
 * ClassicServerStream receive the proactive messages from peer through the connection dialed by peer.
 *
 * ServerSide
 *
 * The incoming messages including:
 * 1) MetaSyncRequest
 * 2) DBSyncRequest
 * 3) TrySyncRequest
 * 4) BinlogSyncRequest
 *
 * The outgoing messages including:
 * 1) MetaSyncResponse
 * 2) DBSyncResponse
 * 3) TrySyncResponse
 * 4) BinlogSyncResponse
 */
class ClassicServerStream : public ServerStream {
 public:
  ClassicServerStream(int fd,
                      std::string ip_port,
                      pink::Thread* io_thread,
                      void* worker_specific_data,
                      pink::PinkEpoll* epoll,
                      MessageReporter* reporter);
  ~ClassicServerStream() = default;

  virtual int DealMessage() override;
};

class ClassicServerStreamFactory :  public ServerStreamFactory {
 public:
  explicit ClassicServerStreamFactory(MessageReporter* reporter)
    : ServerStreamFactory(reporter) { }
  ~ClassicServerStreamFactory() = default;

  virtual std::shared_ptr<pink::PinkConn> NewPinkConn(int connfd,
                                                      const std::string& ip_port,
                                                      pink::Thread* thread,
                                                      void* worker_specific_data,
                                                      pink::PinkEpoll* pink_epoll) const override {
    auto server_stream = std::make_shared<ClassicServerStream>(connfd,
                                                              ip_port,
                                                              thread,
                                                              worker_specific_data,
                                                              pink_epoll,
                                                              reporter_);
    return std::static_pointer_cast<pink::PinkConn>(server_stream);
  }
};

} // namespace replication

#endif // REPLICATION_PIKA_CLASSIC_IO_STREAM_H_
