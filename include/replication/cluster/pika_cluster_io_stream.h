// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef REPLICATION_PIKA_CLUSTER_IO_STREAM_H_
#define REPLICAITON_PIKA_CLUSTER_IO_STREAM_H_

#include <memory>
#include <string>

#include "pink/include/pb_conn.h"

#include "include/util/callbacks.h"
#include "include/replication/pika_repl_io_stream.h"

namespace replication {

/*
 * ClusterClientStream post the local messages to peer through the connection dialed by local.
 *
 * ClientSide
 *
 * The outgoing messages including:
 * 1) AppendEntriesRequest
 * 2) HeartbeatRequest
 * 3) SnapshotRequest
 * 4) TimeoutNowRequest
 * 5) VoteRequest
 * 6) PreVoteRequest
 *
 * The incoming messages including:
 * 1) AppendEntriesResponse
 * 2) HeartbeatResponse
 * 3) VoteResponse
 * 4) PreVoteResponse
 */

class ClusterClientStream : public ClientStream {
 public:
  ClusterClientStream(int fd,
                      std::string ip_port,
                      pink::Thread* thread,
                      void* worker_specific_data,
                      pink::PinkEpoll* epoll,
                      MessageReporter* reporter,
                      int max_conn_rbuf_size);
  ~ClusterClientStream() = default;

  virtual int DealMessage() override;
};

class ClusterClientStreamFactory :  public ClientStreamFactory {
 public:
  ClusterClientStreamFactory(MessageReporter* reporter,
                               int max_conn_rbuf_size)
    : ClientStreamFactory(reporter, max_conn_rbuf_size) { }
  ~ClusterClientStreamFactory() = default;

  virtual std::shared_ptr<pink::PinkConn> NewPinkConn(int connfd,
                                                      const std::string& ip_port,
                                                      pink::Thread* thread,
                                                      void* worker_specific_data,
                                                      pink::PinkEpoll* pink_epoll) const override {
    auto client_stream = std::make_shared<ClusterClientStream>(connfd,
                                                                 ip_port,
                                                                 thread,
                                                                 worker_specific_data,
                                                                 pink_epoll,
                                                                 reporter_,
                                                                 max_conn_rbuf_size_);
    return std::static_pointer_cast<pink::PinkConn>(client_stream);
  }
};

/*
 * ClusterServerStream receive the proactive messages from peer through the connection dialed by peer.
 *
 * ServerSide
 *
 * The incoming messages including:
 * 1) AppendEntriesRequest
 * 2) HeartbeatRequest
 * 3) SnapshotRequest
 * 4) TimeoutNowRequest
 * 5) VoteRequest
 * 6) PreVoteRequest
 *
 * The outgoing messages including:
 * 1) AppendEntriesResponse
 * 2) HeartbeatResponse
 * 3) VoteResponse
 * 4) PreVoteResponse
 */
class ClusterServerStream : public ServerStream {
 public:
  ClusterServerStream(int fd,
                        std::string ip_port,
                        pink::Thread* io_thread,
                        void* worker_specific_data,
                        pink::PinkEpoll* epoll,
                        MessageReporter* reporter,
                        int max_conn_rbuf_size);
  ~ClusterServerStream() = default;

  virtual int DealMessage() override;
 private:
  int max_conn_rbuf_size_;
};

class ClusterServerStreamFactory :  public ServerStreamFactory {
 public:
  ClusterServerStreamFactory(MessageReporter* reporter, int max_conn_rbuf_size)
    : ServerStreamFactory(reporter), max_conn_rbuf_size_(max_conn_rbuf_size) { }
  ~ClusterServerStreamFactory() = default;

  virtual std::shared_ptr<pink::PinkConn> NewPinkConn(int connfd,
                                                      const std::string& ip_port,
                                                      pink::Thread* thread,
                                                      void* worker_specific_data,
                                                      pink::PinkEpoll* pink_epoll) const override {
    auto server_stream = std::make_shared<ClusterServerStream>(connfd,
                                                                 ip_port,
                                                                 thread,
                                                                 worker_specific_data,
                                                                 pink_epoll,
                                                                 reporter_,
                                                                 max_conn_rbuf_size_);
    return std::static_pointer_cast<pink::PinkConn>(server_stream);
  }
 private:
  int max_conn_rbuf_size_;
};

} // namespace replication

#endif // REPLICATION_PIKA_CLUSTER_IO_STREAM_H_
