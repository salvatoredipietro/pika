// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <memory>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <google/protobuf/io//coded_stream.h>

#include "gtest/gtest.h"
#include "gmock/gmock.h"
#include "slash/include/slash_status.h"

#include "proto/pika_inner_message.pb.h"
#include "include/util/message_decoder.h"
#include "include/util/callbacks.h"
#include "include/pika_define.h"
#include "include/deps/replication_mock.h"
#include "include/deps/replication_env.h"
#include "include/replication/classic/pika_classic_repl_manager.h"
#include "include/replication/classic/pika_classic_controller.h"

namespace test {

using slash::Status;
using namespace util;

ReplManagerEnv* test_env;

class ClassicReplManagerSuite {
 public:
  ClassicReplManagerSuite();
  ~ClassicReplManagerSuite();

  std::unique_ptr<replication::ClassicReplManager> GetReplicationManager();

  void set_local_id(const PeerID& local_id)  { local_id_ = local_id; }
  const PeerID& local_id() const { return local_id_; }
  ::testing::NiceMock<MockStateMachine>* state_machine() { return state_machine_; }
  std::shared_ptr<::testing::NiceMock<MockTransporter>> transporter() { return transporter_; }
  std::shared_ptr<::testing::NiceMock<MockReplicationOptionsStorage>> options() { return options_; }

 private:
  // options for ReplicationManager
  PeerID local_id_;
  ::testing::NiceMock<MockStateMachine>* state_machine_;
  std::shared_ptr<::testing::NiceMock<MockTransporter>> transporter_;
  std::shared_ptr<::testing::NiceMock<MockReplicationOptionsStorage>> options_;
};

ClassicReplManagerSuite::ClassicReplManagerSuite()
  : local_id_(PeerID(test_env->local_ip(), test_env->local_port())),
  state_machine_(nullptr),
  transporter_(nullptr),
  options_(nullptr) {
  state_machine_ = new ::testing::NiceMock<MockStateMachine>();
  transporter_ = std::make_shared<::testing::NiceMock<MockTransporter>>();
  options_ = std::make_shared<::testing::NiceMock<MockReplicationOptionsStorage>>();
  PeerID local_id(local_id_);
  ON_CALL(*options_, local_id())
    .WillByDefault([local_id] { return local_id; });
}

ClassicReplManagerSuite::~ClassicReplManagerSuite() {
  if (state_machine_ != nullptr) {
    delete state_machine_;
    state_machine_ = nullptr;
  }
}

std::unique_ptr<replication::ClassicReplManager> ClassicReplManagerSuite::GetReplicationManager() {
  replication::ReplicationManagerOptions rm_options(static_cast<replication::StateMachine*>(state_machine_),
      std::dynamic_pointer_cast<replication::Transporter>(transporter_),
      std::dynamic_pointer_cast<replication::ReplicationOptionsStorage>(options_));
  auto rm_ptr = std::unique_ptr<replication::ClassicReplManager>(new replication::ClassicReplManager(rm_options));
  return std::move(rm_ptr);
}

class ClassicReplManagerTest: public ::testing::Test {
 protected:
  ClassicReplManagerTest() = default;
  ~ClassicReplManagerTest() = default;

  void SetUp() override { }
  void TearDown() override { }
};

TEST_F(ClassicReplManagerTest, StartAndStop)  {
  ClassicReplManagerSuite rm_suite;
  auto rm = rm_suite.GetReplicationManager();
  ASSERT_EQ(rm->Start(), 0);
  ASSERT_EQ(rm->Stop(), 0);
}

TEST_F(ClassicReplManagerTest, InitializeWithMaster) {
  ClassicReplManagerSuite rm_suite;

  PeerID master_id(rm_suite.local_id().Ip(),
                   rm_suite.local_id().Port() + 2048);
  ON_CALL(*(rm_suite.options()), classic_mode())
    .WillByDefault(::testing::Return(true));
  EXPECT_CALL(*(rm_suite.options()), slaveof())
    .WillOnce(::testing::Return(master_id.ToString()))
    .WillOnce(::testing::Return(PeerID().ToString()))
    .WillOnce(::testing::Return(rm_suite.local_id().ToString()));
  // Initialize with master
  {
    auto rm = rm_suite.GetReplicationManager();
    ASSERT_EQ(rm->Start(), 0);
    std::string master_ip;
    int master_port;
    rm->CurrentMaster(master_ip, master_port);
    ASSERT_EQ(master_id, PeerID(master_ip, master_port));
    ASSERT_TRUE(!rm->IsMaster());
    ASSERT_TRUE(!rm->IsSlave());
    ASSERT_EQ(rm->Stop(), 0);
  }
  // Initialize without master
  {
    auto rm = rm_suite.GetReplicationManager();
    ASSERT_EQ(rm->Start(), 0);
    std::string master_ip;
    int master_port;
    rm->CurrentMaster(master_ip, master_port);
    ASSERT_EQ(PeerID(), PeerID(master_ip, master_port));
    ASSERT_TRUE(!rm->IsMaster());
    ASSERT_TRUE(!rm->IsSlave());
    ASSERT_EQ(rm->Stop(), 0);
  }
  // Initialize with master as self
  {
    auto rm = rm_suite.GetReplicationManager();
    ASSERT_NE(rm->Start(), 0);
  }
}

TEST_F(ClassicReplManagerTest, HandleMetaSyncRequestAndResponse) {
  ClassicReplManagerSuite rm_suite_a, rm_suite_b;
  rm_suite_a.set_local_id(PeerID(test_env->local_ip(), test_env->local_port()));
  rm_suite_b.set_local_id(PeerID(test_env->local_ip(),
                          test_env->local_port() + 1024));
  std::vector<TableStruct> empty_table_struct;
  // a is slave, b is master.
  ON_CALL(*(rm_suite_a.options()), classic_mode())
    .WillByDefault(::testing::Return(true));
  // Ensure that messages are executed synchronously.
  ON_CALL(*(rm_suite_a.options()), disk_thread_num())
    .WillByDefault(::testing::Return(0)); 
  ON_CALL(*(rm_suite_a.options()), sync_thread_num())
    .WillByDefault(::testing::Return(0)); 
  ON_CALL(*(rm_suite_a.options()), slaveof())
    .WillByDefault(::testing::Return(rm_suite_b.local_id().ToString()));
  ON_CALL(*(rm_suite_a.options()), table_structs())
    .WillByDefault(::testing::ReturnRef(empty_table_struct));
  ON_CALL(*(rm_suite_b.options()), classic_mode())
    .WillByDefault(::testing::Return(true));
  ON_CALL(*(rm_suite_b.options()), disk_thread_num())
    .WillByDefault(::testing::Return(0));
  ON_CALL(*(rm_suite_b.options()), sync_thread_num())
    .WillByDefault(::testing::Return(0));
  ON_CALL(*(rm_suite_b.options()), table_structs())
    .WillByDefault(::testing::ReturnRef(empty_table_struct));

  auto rm_a = rm_suite_a.GetReplicationManager();
  ASSERT_EQ(rm_a->Start(), 0);
  // TODO(LIBA-S): we stop the rm to avoid auxiliary thread
  // corrupt the test
  ASSERT_EQ(rm_a->Stop(), 0);
  auto rm_b = rm_suite_b.GetReplicationManager();
  ASSERT_EQ(rm_b->Start(), 0);
  ASSERT_EQ(rm_b->Stop(), 0);

  replication::PeerMessageListMap peer_msg_map;
  EXPECT_CALL(*(rm_suite_a.transporter()), WriteToPeer(::testing::_))
    .WillRepeatedly([&peer_msg_map] (replication::PeerMessageListMap&& msg_map) {
          peer_msg_map.swap(msg_map);
          return Status::OK();
        }); 
  // Slave sent MetaSyncRequest to master
  rm_a->StepStateMachine();
  int sent_number = rm_a->SendToPeer();
  ASSERT_EQ(sent_number, 1);
  ASSERT_EQ(peer_msg_map.size(), 1);
  const auto& peer_msg_pair = peer_msg_map.begin();
  auto& peer_msg = peer_msg_pair->second.front();
  ASSERT_EQ(peer_msg.type, replication::MessageType::kRequest);
  ASSERT_EQ(peer_msg_pair->first, rm_suite_b.local_id());

  std::string meta_sync_request;
  Status s = peer_msg.builder->Build(&meta_sync_request);
  ASSERT_TRUE(s.ok()) << s.ToString();

  // Master handle MetaSyncRequest
  std::string meta_sync_response;
  auto server_closure = new ::testing::NiceMock<MockServerClosure>();
  EXPECT_CALL(*server_closure, run())
    .WillOnce([&meta_sync_response, server_closure](){
          meta_sync_response = server_closure->ReleaseResponse();
        });

  auto request = DecodeClassicMessage(meta_sync_request,
      rm_suite_a.local_id(), InnerMessage::ClassicMessage::kRequestType,
      static_cast<replication::IOClosure*>(server_closure));
  ASSERT_TRUE(request != nullptr);
  ASSERT_TRUE(!rm_b->IsMaster());
  ASSERT_TRUE(!rm_b->IsSlave());
  rm_b->HandlePeerMessage(request);
  ASSERT_TRUE(rm_b->IsMaster());

  // Slave handle MetaSyncResponse
  auto client_closure = new ::testing::NiceMock<MockClientClosure>();
  auto response = DecodeClassicMessage(meta_sync_response,
      rm_suite_b.local_id(), InnerMessage::ClassicMessage::kResponseType,
      static_cast<replication::IOClosure*>(client_closure));
  ASSERT_TRUE(response != nullptr);
  ASSERT_TRUE(!rm_a->IsMaster());
  ASSERT_TRUE(!rm_a->IsSlave());
  rm_a->HandlePeerMessage(response);
  ASSERT_TRUE(rm_a->IsSlave());
}

} // namespace test

int main(int argc, char** argv) {
  test::test_env = static_cast<test::ReplManagerEnv*>(
      ::testing::AddGlobalTestEnvironment(new test::ReplManagerEnv(argc, argv)));
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
