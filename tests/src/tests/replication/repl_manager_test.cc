// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <chrono>

#include "gtest/gtest.h"
#include "gmock/gmock.h"
#include "slash/include/slash_status.h"

#include "include/pika_define.h"
#include "include/util/random.h"
#include "include/deps/replication_mock.h"
#include "include/deps/replication_env.h"
#include "include/replication/pika_repl_manager.h"

namespace test {

using slash::Status;
using namespace util;

ReplManagerEnv* test_env;

class MockReplicationManager : public replication::ReplicationManager {
 public:
  MockReplicationManager(const std::string& base_log_path,
                         const replication::ReplicationManagerOptions& rm_options,
                         replication::TransporterOptions&& t_options);
  virtual ~MockReplicationManager() = default;

  MOCK_METHOD(Status, createReplicationGroupNodes, (const replication::GroupIDAndInitConfMap& id_init_conf_map), (override));
  MOCK_METHOD(void, handlePeerMessage, (replication::MessageReporter::Message* peer_msg), (override));
  MOCK_METHOD(void, reportTransportResult, (const PeerInfo& peer_info, const replication::MessageReporter::ResultType& type,
              const replication::MessageReporter::DirectionType& d_type), (override));

 private:
  const std::string base_log_path_;
};

MockReplicationManager::MockReplicationManager(const std::string& base_log_path,
                                               const replication::ReplicationManagerOptions& rm_options,
                                               replication::TransporterOptions&& t_options)
  : replication::ReplicationManager(rm_options, std::move(t_options)), base_log_path_(base_log_path) {
  ON_CALL(*this, createReplicationGroupNodes)
    .WillByDefault([this] (const replication::GroupIDAndInitConfMap& id_init_conf_map){
          slash::RWLock l(&rg_rw_, true);
          for (const auto& iter : id_init_conf_map) {
            const auto& group_id = iter.first;
            replication::ReplicationGroupNodeOptions options;
            options.group_id = group_id;
            replication::LogManagerOptions log_options;
            log_options.stable_log_options. log_path = base_log_path_ + "log_"
                                                       + group_id.TableName() + "_"
                                                       + std::to_string(group_id.PartitionID()) + "/";
            options.log_options = log_options;
            groups_map_[group_id] = std::static_pointer_cast<replication::ReplicationGroupNode>(
                std::make_shared<::testing::NiceMock<MockReplicationGroupNode>>(options));
          }
          return Status::OK();
        });
}

class ReplicationManagerTest : public ::testing::Test {
 protected:
  ReplicationManagerTest();
  ~ReplicationManagerTest();

  void SetUp() override {
    std::string cmd_line = "mkdir " + base_log_path_;
    system(cmd_line.c_str());
  }
  void TearDown() override {
    std::string cmd_line = "rm -rf " + base_log_path_;
    system(cmd_line.c_str());
  }
  std::unique_ptr<MockReplicationManager> GetReplicationManager();

  MockStateMachine* state_machine_;
  std::shared_ptr<::testing::NiceMock<MockReplicationOptionsStorage>> options_;
  const PeerID local_id_;
  const std::string base_log_path_;
};

ReplicationManagerTest::ReplicationManagerTest()
  : state_machine_(nullptr), options_(nullptr),
  local_id_(test_env->local_ip(), test_env->local_port()),
  base_log_path_(test_env->binlog_dir()) {
  state_machine_ = new MockStateMachine();
  options_ = std::make_shared<::testing::NiceMock<MockReplicationOptionsStorage>>();
}

ReplicationManagerTest::~ReplicationManagerTest() {
  if (state_machine_ != nullptr) {
    delete state_machine_;
    state_machine_ = nullptr;
  }
}

std::unique_ptr<MockReplicationManager> ReplicationManagerTest::GetReplicationManager() {
  replication::ReplicationManagerOptions rm_options(state_machine_, nullptr, options_);
  auto rm_ptr = std::unique_ptr<MockReplicationManager>(new MockReplicationManager(base_log_path_,
      rm_options, std::move(replication::TransporterOptions(local_id_, nullptr, nullptr, nullptr))));
  return std::move(rm_ptr);
}

TEST_F(ReplicationManagerTest, StartAndStop) {
  auto rm = GetReplicationManager();
  ASSERT_EQ(rm->Start(), 0);
  ASSERT_EQ(rm->Stop(), 0);
}

TEST_F(ReplicationManagerTest, CreateReplicationGroupNode) {
  auto rm = GetReplicationManager();
  ReplicationGroupID group_1("table1", 1);
  std::set<ReplicationGroupID> group_ids{group_1};

  EXPECT_CALL(*rm, createReplicationGroupNodes)
    .Times(1);
  Status s = rm->CreateReplicationGroupNodes(group_ids);
  ASSERT_TRUE(s.ok()) << s.ToString();
  std::shared_ptr<replication::ReplicationGroupNode> node = nullptr;
  for (const auto& group_id : group_ids) {
    node = rm->GetReplicationGroupNode(group_id);
    ASSERT_TRUE(node != nullptr);
    ASSERT_EQ(node->group_id(), group_id);
  }
}

TEST_F(ReplicationManagerTest, CreateReplicationGroupNodeWhenAlreadyExist) {
  auto rm = GetReplicationManager();
  ReplicationGroupID group_1("table1", 1);
  std::set<ReplicationGroupID> group_ids{group_1};
  EXPECT_CALL(*rm, createReplicationGroupNodes)
    .Times(1);
  Status s = rm->CreateReplicationGroupNodes(group_ids);
  ASSERT_TRUE(s.ok()) << s.ToString();
  s = rm->CreateReplicationGroupNodes(group_ids);
  ASSERT_TRUE(!s.ok());
}

TEST_F(ReplicationManagerTest, RemoveReplicationGroupNode) {
  auto rm = GetReplicationManager();
  ReplicationGroupID group_1("table1", 1);
  ReplicationGroupID group_2("table1", 2);
  ReplicationGroupID group_3("table1", 3);
  std::set<ReplicationGroupID> group_ids{group_1, group_2, group_3};

  EXPECT_CALL(*rm, createReplicationGroupNodes)
    .Times(1);
  Status s = rm->CreateReplicationGroupNodes(group_ids);
  ASSERT_TRUE(s.ok()) << s.ToString();

  for (const auto& group_id : group_ids) {
    auto node = rm->GetReplicationGroupNode(group_id);
    ASSERT_TRUE(node != nullptr) << group_id.ToString();
  }
  EXPECT_CALL(*state_machine_, PurgeDir)
    .Times(group_ids.size());
  s = rm->RemoveReplicationGroupNodes(group_ids);
  ASSERT_TRUE(s.ok()) << s.ToString();
}

} // namespace test

int main(int argc, char** argv) {
  test::test_env = static_cast<test::ReplManagerEnv*>(
      ::testing::AddGlobalTestEnvironment(new test::ReplManagerEnv(argc, argv)));
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
