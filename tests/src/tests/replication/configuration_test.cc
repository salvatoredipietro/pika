// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <unistd.h>

#include "gtest/gtest.h"
#include "gmock/gmock.h"
#include "slash/include/slash_status.h"

#include "include/pika_define.h"
#include "include/deps/common_env.h"
#include "include/replication/pika_configuration.h"

namespace test {

using slash::Status;

class ConfigurationTest : public ::testing::Test {
 protected:
  ConfigurationTest() = default;
  ~ConfigurationTest() = default;

  void SetUp() override { }
  void TearDown() override { }

  replication::Configuration configuration_;
};

TEST_F(ConfigurationTest, AddPeer) {
  PeerID node_1("0.0.0.0", 1);
  PeerID node_2("0.0.0.0", 2);

  auto result = configuration_.AddPeer(node_1, replication::PeerRole::kRoleVoter);
  ASSERT_TRUE(result.IsOK()) << result.ToString();
  result = configuration_.AddPeer(node_1, replication::PeerRole::kRoleVoter);
  ASSERT_TRUE(result.IsAlreadyExist()) << result.ToString();

  result = configuration_.AddPeer(node_2, replication::PeerRole::kRoleLearner);
  ASSERT_TRUE(result.IsOK()) << result.ToString();
  result = configuration_.AddPeer(node_2, replication::PeerRole::kRoleLearner);
  ASSERT_TRUE(result.IsAlreadyExist()) << result.ToString();

  auto voters = configuration_.Voters();
  ASSERT_EQ(voters.size(), 1);
  ASSERT_TRUE(voters[0] == node_1);

  auto learners = configuration_.Learners();
  ASSERT_EQ(learners.size(), 1);
  ASSERT_TRUE(learners[0] == node_2);
}

TEST_F(ConfigurationTest, RemovePeer) {
  PeerID node_1("0.0.0.0", 1);
  PeerID node_2("0.0.0.0", 2);

  auto result = configuration_.AddPeer(node_1, replication::PeerRole::kRoleVoter);
  ASSERT_TRUE(result.IsOK()) << result.ToString();
  result = configuration_.AddPeer(node_2, replication::PeerRole::kRoleLearner);
  ASSERT_TRUE(result.IsOK()) << result.ToString();

  result = configuration_.RemovePeer(node_1);
  ASSERT_TRUE(result.IsOK()) << result.ToString();
  auto voters = configuration_.Voters();
  ASSERT_EQ(voters.size(), 0);
  result = configuration_.RemovePeer(node_1);
  ASSERT_TRUE(result.IsNotFound()) << result.ToString();
  ASSERT_FALSE(configuration_.Contains(node_1, replication::PeerRole::kRoleVoter));
  ASSERT_FALSE(configuration_.Contains(node_1, replication::PeerRole::kRoleLearner));

  result = configuration_.RemovePeer(node_2);
  ASSERT_TRUE(result.IsOK()) << result.ToString();
  auto learners = configuration_.Learners();
  ASSERT_EQ(learners.size(), 0);
  result = configuration_.RemovePeer(node_2);
  ASSERT_TRUE(result.IsNotFound()) << result.ToString();
  ASSERT_FALSE(configuration_.Contains(node_2, replication::PeerRole::kRoleVoter));
  ASSERT_FALSE(configuration_.Contains(node_2, replication::PeerRole::kRoleLearner));
}

TEST_F(ConfigurationTest, PromotePeer) {
  PeerID node_1("0.0.0.0", 1);
  PeerID node_2("0.0.0.0", 2);
  auto result = configuration_.AddPeer(node_1, replication::PeerRole::kRoleVoter);
  ASSERT_TRUE(result.IsOK()) << result.ToString();
  result = configuration_.AddPeer(node_2, replication::PeerRole::kRoleLearner);
  ASSERT_TRUE(result.IsOK()) << result.ToString();

  result = configuration_.PromotePeer(node_1);
  ASSERT_TRUE(result.IsNotLearner()) << result.ToString(); 
  auto voters = configuration_.Voters();
  ASSERT_EQ(voters.size(), 1);
  ASSERT_EQ(voters[0], node_1);

  result = configuration_.PromotePeer(node_2);
  ASSERT_TRUE(result.IsOK()) << result.ToString(); 
  ASSERT_EQ(configuration_.Learners().size(), 0);
  ASSERT_EQ(configuration_.Voters().size(), 2);
}

TEST_F(ConfigurationTest, Iterator) {
  std::unordered_set<PeerID, hash_peer_id> voters{{"0.0.0.0", 1},
                                    {"0.0.0.0", 2},
                                    {"0.0.0.0", 3},
                                    {"0.0.0.0", 4},
                                    {"0.0.0.0", 5}};
  std::unordered_set<PeerID, hash_peer_id> learners{{"0.0.0.1", 1},
                                      {"0.0.0.1", 2},
                                      {"0.0.0.1", 3},
                                      {"0.0.0.1", 4},
                                      {"0.0.0.1", 5}};
  for (const auto& voter : voters) {
    auto result = configuration_.AddPeer(voter, replication::PeerRole::kRoleVoter);
    ASSERT_TRUE(result.IsOK()) << result.ToString();
  }
  for (const auto& learner : learners) {
    auto result = configuration_.AddPeer(learner, replication::PeerRole::kRoleLearner);
    ASSERT_TRUE(result.IsOK()) << result.ToString();
  }
  replication::Configuration::const_iterator begin, end;
  configuration_.Iterator(replication::PeerRole::kRoleVoter, begin, end);
  size_t number = 0;
  for (; begin != end; begin++) {
    ASSERT_TRUE(voters.find(*begin) != voters.end());
    number++;
  }
  ASSERT_EQ(number, voters.size());
  configuration_.Iterator(replication::PeerRole::kRoleLearner, begin, end);
  number = 0;
  for (; begin != end; begin++) {
    ASSERT_TRUE(learners.find(*begin) != learners.end());
    number++;
  }
  ASSERT_EQ(number, learners.size());
}

TEST_F(ConfigurationTest, SerializeAndDeserialize) {
  PeerID node_1("0.0.0.0", 1);
  PeerID node_2("0.0.0.0", 2);
  auto result = configuration_.AddPeer(node_1, replication::PeerRole::kRoleVoter);
  ASSERT_TRUE(result.IsOK()) << result.ToString();
  result = configuration_.AddPeer(node_2, replication::PeerRole::kRoleLearner);
  ASSERT_TRUE(result.IsOK()) << result.ToString();
  auto encoded = configuration_.ConfState();
  replication::Configuration old_conf(configuration_);
  configuration_.Reset();
  configuration_.ParseFrom(encoded);
  ASSERT_EQ(old_conf.Voters(), configuration_.Voters());
  ASSERT_EQ(old_conf.Learners(), configuration_.Learners());
  ASSERT_THAT(old_conf.ToString(), ::testing::StrEq(configuration_.ToString()));
}

}  // namespace test

int main(int argc, char** argv) {
  ::testing::AddGlobalTestEnvironment(new test::Env(argc, argv));
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
