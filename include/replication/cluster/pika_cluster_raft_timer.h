// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef REPLICATION_PIKA_CLUSTER_RAFT_TIMER_H_
#define REPLICATION_PIKA_CLUSTER_RAFT_TIMER_H_

#include <memory>
#include <atomic>

#include "include/util/random.h"
#include "include/util/thread_pool.h"
#include "include/util/timer.h"
#include "include/util/task_executor.h"

namespace replication {

class ClusterRGNode;

class RaftTimer : public util::Timer {
 public:
  RaftTimer(const std::string& id) : id_(id) {}

  void Initialize(util::thread::ThreadPool* timer_thread,
                  util::TaskExecutor* executor,
                  std::shared_ptr<ClusterRGNode> rg_node,
                  uint64_t timeout_ms,
                  bool repeated);

  const std::string& id() const { return id_; }

 protected:
  const std::string id_;
  std::weak_ptr<ClusterRGNode> rg_node_;
};

class ElectionTimer : public RaftTimer {
 public:
  ElectionTimer() : RaftTimer("ElectionTimer") {}

 private:
  virtual void run() override;
  virtual uint64_t AdjustTimeout(uint64_t timeout_ms) override;
  util::WELL512RNG random_generator_;
};

class CheckQuorumTimer: public RaftTimer {
 public:
  CheckQuorumTimer() : RaftTimer("CheckQuorumTimer") {}

 private:
  virtual void run() override;
};

class HeartbeatTimer : public RaftTimer {
 public:
  HeartbeatTimer() : RaftTimer("HeartbeatTimer") {}

 private:
  virtual void run() override;
};

// The current leader needs to transfer its leadership within the specified time,
// otherwise the transferring is canceled to reduce the time when the cluster
// is unavailable. In practice, this transferring needs to be done before the
// election timeout.
class TransferTimer: public RaftTimer {
 public:
  TransferTimer() : RaftTimer("TransferTimer") {}

 private:
  virtual void run() override;
};

} // namespace replication

#endif // REPLICATION_PIKA_CLUSTER_RAFT_TIMER_H_
