// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef INCLUDE_DEPS_REPLICATION_MOCK_H_
#define INCLUDE_DEPS_REPLICATION_MOCK_H_

#include "gtest/gtest.h"
#include "gmock/gmock.h"
#include "slash/include/slash_status.h"

#include "include/pika_define.h"
#include "include/replication/pika_repl_manager.h"
#include "include/replication/pika_stable_log.h"

namespace test {

using slash::Status;

class MockStateMachine : public replication::StateMachine {
 public:
  MockStateMachine() = default;
  virtual ~MockStateMachine() = default;

  MOCK_METHOD(void, OnStop, (const ReplicationGroupID& group_id), (override));
  MOCK_METHOD(void, OnReplError, (const ReplicationGroupID& group_id, const Status& status), (override));
  MOCK_METHOD(void, OnLeaderChanged, (const ReplicationGroupID& group_id, const PeerID& leader_id), (override));
  MOCK_METHOD(void, OnApply, (std::vector<replication::MemLog::LogItem> logs), (override));
  MOCK_METHOD(Status, OnSnapshotSyncStart,(const ReplicationGroupID& group_id), (override));
  MOCK_METHOD(void, PurgeDir, (const std::string& dir), (override));
  MOCK_METHOD(void, ReportLogAppendError, (const ReplicationGroupID& group_id), (override));
  MOCK_METHOD(void, PurgelogsTaskSchedule, (void (*function)(void*), void* arg), (override));
  MOCK_METHOD(Status, CheckSnapshotReceived,
      (const std::shared_ptr<replication::ReplicationGroupNode>& node, LogOffset* snapshot_offset), (override));
  MOCK_METHOD(void, TrySendSnapshot, (const PeerID& peer_id, const ReplicationGroupID& group_id,
      const std::string& logger_filename, int32_t top, replication::SnapshotSyncClosure* closure), (override));
};

class MockReplicationGroupNodeMetaStorage : public replication::ReplicationGroupNodeMetaStorage {
 public:
  MockReplicationGroupNodeMetaStorage() = default;
  virtual ~MockReplicationGroupNodeMetaStorage() = default;

  MOCK_METHOD(Status, applied_offset, (LogOffset* offset), (override));
  MOCK_METHOD(Status, set_applied_offset, (const LogOffset& offset), (override));
  MOCK_METHOD(Status, snapshot_offset, (LogOffset* offset), (override));
  MOCK_METHOD(Status, set_snapshot_offset, (const LogOffset& offset), (override));
  MOCK_METHOD(Status, configuration, (replication::Configuration* conf), (override));
  MOCK_METHOD(Status, set_configuration, (const replication::Configuration& conf), (override));
  MOCK_METHOD(Status, ApplyConfiguration, (const replication::Configuration& conf,
        const LogOffset& offset), (override));
  MOCK_METHOD(Status, MetaSnapshot, (LogOffset* applied_offset, replication::Configuration* conf), (override));
  MOCK_METHOD(Status, ResetOffset, (const LogOffset& snapshot_offset,
        const LogOffset& applied_offset), (override));
  MOCK_METHOD(Status, ResetOffsetAndConfiguration, (const LogOffset& snapshot_offset,
        const LogOffset& applied_offset, const replication::Configuration& configuration), (override));
};

class MockReplicationOptionsStorage : public replication::ReplicationOptionsStorage {
 public:
  MockReplicationOptionsStorage() = default;
  virtual ~MockReplicationOptionsStorage() = default;

  MOCK_METHOD(std::string, slaveof, (), (override));
  MOCK_METHOD(bool, slave_read_only, (), (override));
  MOCK_METHOD(int, slave_priority, (), (override));
  MOCK_METHOD(std::string, masterauth, (), (override));
  MOCK_METHOD(std::string, requirepass, (), (override));
  MOCK_METHOD(const std::vector<TableStruct>&, table_structs, (), (override));
  MOCK_METHOD(bool, classic_mode, (), (override));
  MOCK_METHOD(ReplicationProtocolType, replication_protocol_type, (), (override));
  MOCK_METHOD(int, expire_logs_nums, (), (override));
  MOCK_METHOD(int, expire_logs_days, (), (override));
  MOCK_METHOD(uint32_t, retain_logs_num, (), (override));
  MOCK_METHOD(uint64_t, reconnect_master_interval_ms, (), (override));
  MOCK_METHOD(int, sync_window_size, (), (override));
  MOCK_METHOD(uint64_t, sync_receive_timeout_ms, (), (override));
  MOCK_METHOD(uint64_t, sync_send_timeout_ms, (), (override));
  MOCK_METHOD(int, binlog_file_size, (), (override));
  MOCK_METHOD(std::string, log_path, (), (override));
  MOCK_METHOD(int, max_conn_rbuf_size, (), (override));
  MOCK_METHOD(PeerID, local_id, (), (override));
  MOCK_METHOD(int, sync_thread_num, (), (override));
  MOCK_METHOD(int, disk_thread_num, (), (override));
  MOCK_METHOD(void, SetWriteBinlog, (const std::string& value), (override));
  MOCK_METHOD(bool, load_index_into_memory, (), (override));
  MOCK_METHOD(bool, check_quorum, (), (override));
  MOCK_METHOD(bool, pre_vote, (), (override));
  MOCK_METHOD(uint64_t, heartbeat_timeout_ms, (), (override));
  MOCK_METHOD(uint64_t, election_timeout_ms, (), (override));
  MOCK_METHOD(uint64_t, max_consumed_number_once, (), (override));
};

class MockTransporter : public replication::Transporter {
 public:
  MockTransporter() = default;
  virtual ~MockTransporter() = default;

  MOCK_METHOD(int, Start, (), (override));
  MOCK_METHOD(int, Stop, (), (override));

  //virtual Status AddPeer(const PeerID& peer_id,
  //                       std::shared_ptr<replication::ServerStream> server_stream,
  //                       std::shared_ptr<replication::ClientStream> client_stream,
  //                       bool skip_when_exist) override {
  //  return AddPeerImpl(peer_id, server_stream, client_stream, skip_when_exist);
  //}
  //MOCK_METHOD(Status, AddPeerImpl, (const PeerID& peer_id,
  //      std::shared_ptr<replication::ServerStream> server_stream,
  //      std::shared_ptr<replication::ClientStream> client_stream,
  //      bool skip_when_exist));

  MOCK_METHOD(bool, PinServerStream, (const PeerID& peer_id, std::shared_ptr<replication::ServerStream> server_stream), (override));
  MOCK_METHOD(bool, PinClientStream, (const PeerID& peer_id, std::shared_ptr<replication::ClientStream> client_stream), (override));
  MOCK_METHOD(void, UnpinServerStream, (const PeerID& peer_id), (override));
  MOCK_METHOD(void, UnpinClientStream, (const PeerID& peer_id), (override));

  MOCK_METHOD(Status, AddPeer, (const PeerID& peer_id), (override));
  MOCK_METHOD(Status, RemovePeer, (const PeerID& peer_id), (override));
  MOCK_METHOD(Status, WriteToPeer, (replication::PeerMessageListMap&& msgs), (override));
};

class MockReplicationGroupNode : public replication::ReplicationGroupNode {
 public:
  MockReplicationGroupNode(const replication::ReplicationGroupNodeOptions& options)
    : replication::ReplicationGroupNode(options) {
      ReplicationGroupID group_id(group_id_);
      ON_CALL(*this, NodePersistentState())
        .WillByDefault([] { return replication::PersistentContext::State(); });
      ON_CALL(*this, NodeVolatileState())
        .WillByDefault([] { return replication::VolatileContext::State(PeerID(), replication::RoleState()); });
      ON_CALL(*this, Leave)
        .WillByDefault([group_id] (std::string& log_remove) { log_remove = group_id.ToString(); });
  }
  virtual ~MockReplicationGroupNode() = default;

  MOCK_METHOD(void, Leave, (std::string& log_remove), (override));
  MOCK_METHOD(void, PurgeStableLogs, (int expire_logs_nums, int expire_logs_days,
                                      uint32_t to, bool manual, replication::PurgeLogsClosure* done), (override));
  MOCK_METHOD(Status, Propose, (const std::shared_ptr<ReplTask>& task), (override));
  MOCK_METHOD(Status, Initialize, (), (override));
  MOCK_METHOD(Status, Start, (), (override));
  MOCK_METHOD(Status, Stop, (), (override));
  MOCK_METHOD(bool, IsReadonly, (), (override));
  MOCK_METHOD(Status, IsReady, (), (override));
  MOCK_METHOD(Status, IsSafeToBeRemoved, (), (override));
  MOCK_METHOD(Status, StepStateMachine, (), (override));
  MOCK_METHOD(replication::VolatileContext::State, NodeVolatileState, (), (override));
  MOCK_METHOD(replication::PersistentContext::State, NodePersistentState, (), (override));
  MOCK_METHOD(replication::MemberContext::MemberStatesMap, NodeMemberStates, (), (override));
  MOCK_METHOD(bool, IsSafeToBePurged, (uint32_t filenum), (override));
  MOCK_METHOD(void, Messages, (replication::PeerMessageListMap& msgs), (override));
  MOCK_METHOD(void, ReportUnreachable, (const PeerID& peer_id), (override));
  MOCK_METHOD(Status, Advance, (const replication::Ready& ready) ,(override));
  MOCK_METHOD(Status, ApplyConfChange, (const InnerMessage::ConfChange& conf_change), (override));
  MOCK_METHOD(Status, GetSyncInfo, (const PeerID& peer_id, std::stringstream& stream), (override));
};

//class MockPbConn : public pink::PbConn {
// public:
//  MockPbConn() : pink::PbConn(0, "127.0.0.1:9221", nullptr, nullptr) { }
//  MockPbConn(const int fd, const std::string &ip_port,
//             Thread *thread, PinkEpoll* epoll = NULL)
//    : pink::PbConn(fd, ip_port, thread, epoll);
//  ~MockPbConn() = default;
//
//  MOCK_METHOD(int, DealMessage, (), override);
//};

class MockServerStream : public replication::ServerStream {
 public:
  MockServerStream(int fd,
                   std::string ip_port,
                   pink::Thread* io_thread,
                   void* worker_specific_data,
                   pink::PinkEpoll* epoll,
                   replication::MessageReporter* reporter)
    : replication::ServerStream(fd, ip_port, io_thread,
        worker_specific_data, epoll, reporter) {}
  MockServerStream()
    : replication::ServerStream(0, "127.0.0.1:9221",
        nullptr, nullptr, nullptr, nullptr) { }
  ~MockServerStream() = default;

  MOCK_METHOD(int, DealMessage, (), (override));
};

class MockClientStream : public replication::ClientStream {
 public:
  constexpr static int kDefaultMaxConnRBUfSize = 1024 * 1024 * 8;
  MockClientStream(int fd,
                   std::string ip_port,
                   pink::Thread* thread,
                   void* worker_specific_data,
                   pink::PinkEpoll* epoll,
                   replication::MessageReporter* reporter,
                   int max_conn_rbuf_size)
    : replication::ClientStream(fd, ip_port, thread,
        worker_specific_data, epoll, reporter, max_conn_rbuf_size) {}
  MockClientStream()
    : replication::ClientStream(0, "127.0.0.1:9221", nullptr,
        nullptr, nullptr, nullptr, kDefaultMaxConnRBUfSize) { }
  ~MockClientStream() = default;

  MOCK_METHOD(int, DealMessage, (), (override));
};

class MockServerClosure : public replication::IOClosure {
 public:
  MockServerClosure() {
    ON_CALL(*this, Stream)
      .WillByDefault([](){
            return std::dynamic_pointer_cast<pink::PbConn>(
                std::make_shared<::testing::NiceMock<MockServerStream>>());
          });
  }

  MOCK_METHOD(std::shared_ptr<pink::PbConn>, Stream, (), (override));
  MOCK_METHOD(void, run, (), (override));

  std::string ReleaseResponse() {
    return std::move(resp_str_);
  }
  bool ShouldClose() {
    return should_close_;
  }

 protected:
  ~MockServerClosure() = default;
};

class MockClientClosure : public replication::IOClosure {
 public:
  MockClientClosure() {
    ON_CALL(*this, Stream)
      .WillByDefault([](){
            return std::dynamic_pointer_cast<pink::PbConn>(
                std::make_shared<::testing::NiceMock<MockClientStream>>());
          });
  }

  MOCK_METHOD(std::shared_ptr<pink::PbConn>, Stream, (), (override));
  MOCK_METHOD(void, run, (), (override));

 protected:
  ~MockClientClosure() = default;
};

class MockPurgeLogsClosure : public replication::PurgeLogsClosure {
 public:
  MockPurgeLogsClosure(const ReplicationGroupID& group_id)
    : replication::PurgeLogsClosure(group_id) { }
  MOCK_METHOD(void, run, (), (override));

 protected:
  ~MockPurgeLogsClosure() = default;
};

} // namespace test

#endif // INCLUDE_DEPS_REPLICATION_MOCK_H_
