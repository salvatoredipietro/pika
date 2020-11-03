// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/replication/cluster/pika_cluster_raft_timer.h"

#include <sys/time.h>

#include "include/util/callbacks.h"
#include "include/replication/cluster/pika_cluster_rg_node.h"

namespace replication {

void RaftTimer::Initialize(util::thread::ThreadPool* timer_thread,
                           util::TaskExecutor* executor,
                           std::shared_ptr<ClusterRGNode> rg_node,
                           uint64_t timeout_ms,
                           bool repeated) {
  util::Timer::Initialize(timer_thread, executor, timeout_ms, repeated);
  rg_node_ = rg_node;
}

void ElectionTimer::run() {
  auto rg_node = rg_node_.lock();
  if (rg_node == nullptr) {
    return;
  }
  rg_node->HandleElectionTimedout();
}

uint64_t ElectionTimer::AdjustTimeout(uint64_t timeout_ms) {
  // [n, 2n-1]
  return timeout_ms + random_generator_.Generate()%timeout_ms;
}

void CheckQuorumTimer::run() {
  auto rg_node = rg_node_.lock();
  if (rg_node == nullptr) {
    return;
  }
  rg_node->HandleCheckQuorumTimedout();
}

void HeartbeatTimer::run() {
  auto rg_node = rg_node_.lock();
  if (rg_node == nullptr) {
    return;
  }
  rg_node->HandleHeartbeatTimedout();
}

void TransferTimer::run() {
  auto rg_node = rg_node_.lock();
  if (rg_node == nullptr) {
    return;
  }
  rg_node->HandleTransferTimedout();
}

} // namespace replication

