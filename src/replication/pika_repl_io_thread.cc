// Copyright (c) 2019-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/replication/pika_repl_io_thread.h"

#include "include/pika_define.h"
#include "include/replication/pika_repl_manager.h"

namespace replication {

PikaReplClientIOThread::PikaReplClientIOThread(MessageReporter* reporter,
                                               pink::ConnFactory* conn_factory,
                                               int cron_interval,
                                               int keepalive_timeout)
  : ClientThread(conn_factory, cron_interval, keepalive_timeout, &handle_, NULL),
  reporter_(reporter),
  handle_(reporter) {
}

void PikaReplClientIOThread::ReplClientStreamHandle::FdClosedHandle(int fd, const std::string& ip_port) const {
  PeerID peer_id;
  if (!peer_id.ParseFromIpPort(ip_port, true/*is_connect_port*/)) {
    LOG(WARNING) << "Parse ip_port error " << ip_port;
    return;
  }
  DLOG(INFO) << "ReplClient Close conn, fd=" << fd
             << ", remote ip_port=" << ip_port
             << ", peer_id="<< peer_id.ToString();
  reporter_->ReportTransportResult(PeerInfo(peer_id, fd),
                                   MessageReporter::ResultType::kFdTimedout,
                                   MessageReporter::DirectionType::kClient);
}

void PikaReplClientIOThread::ReplClientStreamHandle::FdTimeoutHandle(int fd, const std::string& ip_port) const {
  PeerID peer_id;
  if (!peer_id.ParseFromIpPort(ip_port, true/*is_connect_port*/)) {
    LOG(WARNING) << "Parse ip_port error " << ip_port;
    return;
  }
  DLOG(INFO) << "ReplClient Timeout conn, fd=" << fd
             << ", remote ip_port=" << ip_port
             << ", peer_id="<< peer_id.ToString();
  reporter_->ReportTransportResult(PeerInfo(peer_id, fd),
                                   MessageReporter::ResultType::kFdTimedout,
                                   MessageReporter::DirectionType::kClient);
}

PikaReplServerIOThread::PikaReplServerIOThread(MessageReporter* reporter,
                                               pink::ConnFactory* conn_factory,
                                               const std::set<std::string>& ips,
                                               int port,
                                               int cron_interval)
  : HolyThread(ips, port, conn_factory, cron_interval, &handle_, true),
  reporter_(reporter),
  handle_(reporter),
  port_(port) {
  set_keepalive_timeout(180);
}

int PikaReplServerIOThread::ListenPort() {
  return port_;
}

void PikaReplServerIOThread::ReplServerStreamHandle::FdClosedHandle(int fd, const std::string& ip_port) const {
  PeerID peer_id;
  if (!peer_id.ParseFromIpPort(ip_port)) {
    LOG(WARNING) << "Parse ip_port error " << ip_port;
    return;
  }
  DLOG(INFO) << "ServerIOThread Close Slave Conn, fd=" << fd
             << ", remote ip_port: " << ip_port;
  reporter_->ReportTransportResult(PeerInfo(peer_id, fd),
                                   MessageReporter::ResultType::kFdClosed,
                                   MessageReporter::DirectionType::kServer);
}

} // namespace replication
