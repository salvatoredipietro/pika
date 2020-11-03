// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_CONFIGURATION_H_
#define PIKA_CONFIGURATION_H_

#include <unordered_set>
#include <string>
#include <vector>

#include "slash/include/slash_status.h"

#include "proto/pika_inner_message.pb.h"
#include "include/pika_define.h"

namespace replication {

using slash::Status;

/*
 * PeerRole indicates the behave when handle log replication.
 * The log could be applied only when the quorum of voters
 * have persisted it.
 */
enum PeerRole {
  kRoleUnkown   = 0,
  kRoleVoter    = 1,
  kRoleLearner  = 2,
};

class Configuration {
 public:
  class ChangeResult {
   public:
    enum Code {
      kOK           = 0,
      kNotFound     = 1,
      kAlreadyExist = 2,
      kNotLearner   = 3,
    };
    ChangeResult() : code_(Code::kOK) { }
    explicit ChangeResult(const Code& code) : code_(code) { }
    std::string ToString() const;
    Code code() const { return code_; }
    void set_code(Code code) { code_ = code; }
    bool IsOK() { return code_ == kOK; }
    bool IsNotFound() { return code_ == kNotFound; }
    bool IsAlreadyExist() { return code_ == kAlreadyExist; }
    bool IsNotLearner() { return code_ == kNotLearner; }

   private:
    Code code_;
  };

 public:
  Configuration() = default;
  ~Configuration() = default;

  bool Empty() const;
  bool Contains(const PeerID& peer, const PeerRole& role = kRoleUnkown) const;
  ChangeResult AddPeer(const PeerID& peer, const PeerRole& role = kRoleUnkown);
  ChangeResult RemovePeer(const PeerID& peer);
  ChangeResult PromotePeer(const PeerID& peer);
  using const_iterator = std::unordered_set<PeerID, hash_peer_id>::const_iterator;
  void Iterator(const PeerRole& role, const_iterator& begin, const_iterator& end);
  size_t voter_size() const { return voters_.size(); }
  size_t learners_size() const { return learners_.size(); }
  size_t unkown_size() const { return unkown_.size(); }
  std::vector<PeerID> Voters() const;
  std::vector<PeerID> Learners() const;
  bool IsVoter(const PeerID& peer_id) const;
  bool IsLearner(const PeerID& peer_id) const;
  void Reset();
  void ParseFrom(const InnerMessage::ConfState& conf_state);
  InnerMessage::ConfState ConfState() const;
  std::string ToString() const;

 private:
  // the following fields need to be exclusive
  std::unordered_set<PeerID, hash_peer_id> unkown_;
  std::unordered_set<PeerID, hash_peer_id> voters_;
  std::unordered_set<PeerID, hash_peer_id> learners_;
};

} // namespace replication

#endif // PIKA_CONFIGURATION_H_
