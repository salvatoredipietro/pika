// Copyright (c) 2019-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/replication/pika_configuration.h"

#include <sstream>

namespace replication {

std::string Configuration::ChangeResult::ToString() const {
  switch (code_) {
    case Code::kOK:
      return "OK";
    case Code::kNotFound:
      return "NotFound";
    case Code::kAlreadyExist:
      return "AlreadyExist";
    case Code::kNotLearner:
      return "NotLearner";
  }
  return "Unknown";
}

bool Configuration::Empty() const {
  return unkown_.size() == 0 && voters_.size() == 0 && learners_.size() == 0;
}

bool Configuration::Contains(const PeerID& peer, const PeerRole& role) const {
  bool contains = false;
  switch (role) {
    case PeerRole::kRoleUnkown: {
      contains = unkown_.find(peer) != unkown_.end();
      break;
    }
    case PeerRole::kRoleVoter: {
      contains = voters_.find(peer) != voters_.end();
      break;
    }
    case PeerRole::kRoleLearner: {
      contains = learners_.find(peer) != learners_.end();
      break;
    }
  }
  return contains;
}

Configuration::ChangeResult Configuration::AddPeer(const PeerID& peer, const PeerRole& role) {
  bool add_suc = false;
  switch (role) {
    case PeerRole::kRoleUnkown: {
      add_suc = unkown_.insert(peer).second;
      break;
    }
    case PeerRole::kRoleVoter: {
      add_suc = voters_.insert(peer).second;
      break;
    }
    case PeerRole::kRoleLearner: {
      add_suc = learners_.insert(peer).second;
      break;
    }
  }
  if (!add_suc) {
    return ChangeResult(ChangeResult::Code::kAlreadyExist);
  }
  return ChangeResult(ChangeResult::Code::kOK);
}

Configuration::ChangeResult Configuration::RemovePeer(const PeerID& peer) {
  bool erase_suc = false;
  erase_suc = unkown_.erase(peer) != 0;
  if (erase_suc) {
    return ChangeResult(ChangeResult::Code::kOK);
  }
  erase_suc = voters_.erase(peer) != 0;
  if (erase_suc) {
    return ChangeResult(ChangeResult::Code::kOK);
  }
  erase_suc = learners_.erase(peer) != 0;
  if (erase_suc) {
    return ChangeResult(ChangeResult::Code::kOK);
  }
  return ChangeResult(ChangeResult::Code::kNotFound);
}

Configuration::ChangeResult Configuration::PromotePeer(const PeerID& peer) {
  if (unkown_.find(peer) != unkown_.end()
      || voters_.find(peer) != voters_.end()) {
    return ChangeResult(ChangeResult::Code::kNotLearner);
  }
  if (learners_.erase(peer) == 0) {
    return ChangeResult(ChangeResult::Code::kNotFound);
  }
  voters_.insert(peer);
  return ChangeResult(ChangeResult::Code::kOK);
}

void Configuration::Iterator(const PeerRole& role, const_iterator& begin, const_iterator& end) {
  switch (role) {
    case PeerRole::kRoleUnkown: {
      begin = unkown_.begin();
      end = unkown_.end();
      return;
    }
    case PeerRole::kRoleVoter: {
      begin = voters_.begin();
      end = voters_.end();
      return;
    }
    case PeerRole::kRoleLearner: {
      begin = learners_.begin();
      end = learners_.end();
      return;
    }
  }
}

void Configuration::Reset() {
  unkown_.clear();
  voters_.clear();
  learners_.clear();
}

std::vector<PeerID> Configuration::Voters() const {
  std::vector<PeerID> voters;
  voters.reserve(voters_.size());
  for (auto iter : voters_) {
    voters.push_back(iter);
  }
  return std::move(voters);
}

std::vector<PeerID> Configuration::Learners() const {
  std::vector<PeerID> learners;
  learners.reserve(learners_.size());
  for (auto iter : learners_) {
    learners.push_back(iter);
  }
  return std::move(learners);
}

bool Configuration::IsVoter(const PeerID& peer_id) const {
  return voters_.find(peer_id) != voters_.end();
}

bool Configuration::IsLearner(const PeerID& peer_id) const {
  return learners_.find(peer_id) != learners_.end();
}

void Configuration::ParseFrom(const InnerMessage::ConfState& conf_state) {
  Reset();
  for (const auto& voter : conf_state.voter()) {
    AddPeer(PeerID(voter.ip(), voter.port()), PeerRole::kRoleVoter);
  }
  for (const auto& learner : conf_state.learner()) {
    AddPeer(PeerID(learner.ip(), learner.port()), PeerRole::kRoleLearner);
  }
}

InnerMessage::ConfState Configuration::ConfState() const {
  InnerMessage::ConfState conf_state;
  for (auto iter = voters_.begin(); iter != voters_.end(); iter++) {
    InnerMessage::Node* voter = conf_state.add_voter();
    voter->set_ip(iter->Ip());
    voter->set_port(iter->Port());
  }
  for (auto iter = learners_.begin(); iter != learners_.end(); iter++) {
    InnerMessage::Node* learner = conf_state.add_learner();
    learner->set_ip(iter->Ip());
    learner->set_port(iter->Port());
  }
  return std::move(conf_state);
}

std::string Configuration::ToString() const {
  std::stringstream ss;
  ss << "{";
  for (auto iter = voters_.begin(); iter != voters_.end(); iter++) {
    ss << "voter: " << iter->ToString() << " ";
  }
  for (auto iter = learners_.begin(); iter != learners_.end(); iter++) {
    ss << "learner: " << iter->ToString() << " ";
  }
  ss << "}";
  return ss.str();
}

} // namespace replication
