// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef REPLICATION_PIKA_REPL_IO_STREAM_H_
#define REPLICATION_PIKA_REPL_IO_STREAM_H_

#include <atomic>
#include <memory>
#include <string>
#include <utility>

#include "pink/include/pb_conn.h"

#include "include/util/callbacks.h"

namespace replication {

class MessageReporter;

class IOClosure : public util::Closure {
 public:
  IOClosure()
  : should_response_(false),
  should_close_(false),
  resp_str_() { }

  virtual std::shared_ptr<pink::PbConn> Stream() = 0;

  void NotifyClose() { should_close_ = true; }
  void SetResponse(std::string resp_str, bool should_response) {
    resp_str_ = std::move(resp_str);
    should_response_ = should_response;
  }

 protected:
  virtual ~IOClosure() = default;
  bool should_response_;
  bool should_close_;
  std::string resp_str_;
};

// ServerStream receive the proactive messages from peer
// through the connection dialed by peer.
class ServerStream : public pink::PbConn {
 public:
  ServerStream(int fd,
               std::string ip_port,
               pink::Thread* io_thread,
               void* worker_specific_data,
               pink::PinkEpoll* epoll,
               MessageReporter* reporter)
    : PbConn(fd, ip_port, io_thread, epoll),
     pinned_(false),
     reporter_(reporter) { }
  virtual ~ServerStream() = default;

  bool pinned() { return pinned_.load(std::memory_order_acquire); }
  void set_pinned(bool pinned) { pinned_.store(pinned, std::memory_order_release); }

 protected:
  // use pinned_ to mark if the stream have been recorded by application.
  std::atomic<bool> pinned_;
  MessageReporter* reporter_;
};

class ServerStreamClosure final : public IOClosure {
 public:
  explicit ServerStreamClosure(const std::shared_ptr<ServerStream>& server_stream)
    : server_stream_(server_stream) { }

  virtual std::shared_ptr<pink::PbConn> Stream() override {
    return server_stream_;
  };

 protected:
  virtual ~ServerStreamClosure() = default;

 private:
  virtual void run() override {
    if (server_stream_ == nullptr || !should_response_) {
      return;
    }
    if (server_stream_->WriteResp(resp_str_) != 0) {
      server_stream_->NotifyClose();
    } else {
      server_stream_->NotifyWrite();
    }
  }

  std::shared_ptr<ServerStream> server_stream_;
};

class ServerStreamFactory : public pink::ConnFactory {
 public:
  explicit ServerStreamFactory(MessageReporter* reporter)
    : reporter_(reporter) { }
  virtual ~ServerStreamFactory() = default;

 protected:
  MessageReporter* reporter_;
};

// ClientStream post the local messages to peer through the connection dialed by local.
class ClientStream : public pink::PbConn {
 public:
  ClientStream(int fd,
               std::string ip_port,
               pink::Thread* thread,
               void* worker_specific_data,
               pink::PinkEpoll* epoll,
               MessageReporter* reporter,
               int max_conn_rbuf_size)
    : PbConn(fd, ip_port, thread, epoll),
    pinned_(false),
    reporter_(reporter),
    max_conn_rbuf_size_(max_conn_rbuf_size) { }
  virtual ~ClientStream() = default;

  bool pinned() { return pinned_.load(std::memory_order_acquire); }
  void set_pinned(bool pinned) { pinned_.store(pinned, std::memory_order_release); }

 protected:
  // use pinned_ to mark if the stream have been recorded by application.
  std::atomic<bool> pinned_;
  MessageReporter* reporter_;
  int max_conn_rbuf_size_;
};

class ClientStreamFactory : public pink::ConnFactory {
 public:
  ClientStreamFactory(MessageReporter* reporter,
                      int max_conn_rbuf_size)
    : reporter_(reporter),
    max_conn_rbuf_size_(max_conn_rbuf_size) { }
  virtual ~ClientStreamFactory() = default;

 protected:
  void NotifyNewClientStream(std::shared_ptr<ClientStream> client_stream) const;

  MessageReporter* reporter_;
  int max_conn_rbuf_size_;
};

class ClientStreamClosure final : public IOClosure {
 public:
  explicit ClientStreamClosure(std::shared_ptr<ClientStream> client_stream)
    : client_stream_(client_stream) { }

  virtual std::shared_ptr<pink::PbConn> Stream() override {
    return client_stream_;
  };

 protected:
  virtual ~ClientStreamClosure() = default;
 
 private:
  virtual void run() override {
    if (client_stream_== nullptr) {
      return;
    }
    if (should_close_) {
      client_stream_->NotifyClose();
      return;
    }
  }
  std::shared_ptr<ClientStream> client_stream_;
};

} // namespace replication

#endif // REPLICATION_PIKA_REPL_IO_STREAM_H_
