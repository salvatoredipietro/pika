// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef REPLICATION_PIKA_REPL_IO_THREAD_H_
#define REPLICATION_PIKA_REPL_IO_THREAD_H_

#include <string>
#include <memory>

#include "pink/include/pink_conn.h"
#include "pink/include/client_thread.h"
#include "pink/src/holy_thread.h"

#include "include/replication/pika_repl_io_stream.h"

namespace replication {

class MessageReporter;

class PikaReplClientIOThread : public pink::ClientThread {
 public:
  PikaReplClientIOThread(MessageReporter* reporter,
                         pink::ConnFactory* conn_factory_,
                         int cron_interval,
                         int keepalive_timeout);
  virtual ~PikaReplClientIOThread() = default;
  int Start();

 private:
  class ReplClientStreamHandle : public pink::ClientHandle {
   public:
    ReplClientStreamHandle(MessageReporter* reporter) : reporter_(reporter) { }
    void CronHandle() const override {
    }
    void FdTimeoutHandle(int fd, const std::string& ip_port) const override;
    void FdClosedHandle(int fd, const std::string& ip_port) const override;
    bool AccessHandle(std::string& ip) const override {
      // ban 127.0.0.1 if you want to test this routine
      // if (ip.find("127.0.0.2") != std::string::npos) {
      //   std::cout << "AccessHandle " << ip << std::endl;
      //   return false;
      // }
      return true;
    }
    int CreateWorkerSpecificData(void** data) const override {
      return 0;
    }
    int DeleteWorkerSpecificData(void* data) const override {
      return 0;
    }
    void DestConnectFailedHandle(std::string ip_port, std::string reason) const override {
    }
   private:
    MessageReporter* reporter_;
  };

  MessageReporter* reporter_;
  ReplClientStreamHandle handle_;
};

class PikaReplServerIOThread : public pink::HolyThread {
 public:
  PikaReplServerIOThread(MessageReporter* reporter,
                         pink::ConnFactory* conn_factory,
                         const std::set<std::string>& ips,
                         int port,
                         int cron_interval);
  virtual ~PikaReplServerIOThread() = default;

  int ListenPort();

  MessageReporter* Reporter() const { return reporter_; }

 private:
  class ReplServerStreamHandle : public pink::ServerHandle {
   public:
    ReplServerStreamHandle(MessageReporter* reporter) : reporter_(reporter) { }
    virtual void FdClosedHandle(int fd, const std::string& ip_port) const override;
   private:
    MessageReporter* reporter_;
  };

  MessageReporter* reporter_;
  ReplServerStreamHandle handle_;
  int port_;
};

} // namespace replication

#endif  // REPLICATION_PIKA_REPL_IO_THREAD_H_
