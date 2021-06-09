// Copyright (c) 2019-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/replication/pika_repl_auxiliary.h"

#include "pink/include/pink_thread.h"
#include "slash/include/slash_mutex.h"

#include "include/replication/pika_repl_manager.h"

namespace replication {

class StateMachineLooper: public pink::Thread {
 public:
  StateMachineLooper(ReplicationManager* rm)
    : rm_(rm),
    mu_(),
    cv_(&mu_) {
      set_thread_name("StateMachineLooper");
  }
  virtual ~StateMachineLooper() {
    StopThread();
    LOG(INFO) << "StateMachineLooper " << thread_id() << " exit!!!";
  }
  void Signal() {
    mu_.Lock();
    cv_.Signal();
    mu_.Unlock();
  }

 private:
  virtual void* ThreadMain() override;
  ReplicationManager* rm_;
  slash::Mutex mu_;
  slash::CondVar cv_;
};

void* StateMachineLooper::ThreadMain() {
  while (!should_stop()) {
    rm_->StepStateMachine();
    // send to peer
    int res = rm_->SendToPeer();
    if (!res) {
      // sleep 100 ms
      mu_.Lock();
      cv_.TimedWait(100);
      mu_.Unlock();
    } else {
      //LOG_EVERY_N(INFO, 1000) << "Consume binlog number " << res;
    }
  }
  return NULL;
}

/* PikaReplAuxiliary */

PikaReplAuxiliary::PikaReplAuxiliary(ReplicationManager* rm, int executor_queue_size) {
  looper_ = new StateMachineLooper(rm);
  if (executor_queue_size < 0) {
    executor_queue_size = kDefaultThreadQueueSize;
  }
}

PikaReplAuxiliary::~PikaReplAuxiliary() {
  if (looper_ != nullptr) {
    delete looper_;
    looper_ = nullptr;
  }
  LOG(INFO) << "PikaReplAuxiliary exit!!!";
}

int PikaReplAuxiliary::Start() {
  int ret = -1;
  if (looper_ == nullptr) {
    return ret;
  }
  ret = looper_->StartThread();
  if (ret != 0) {
    LOG(ERROR) << "StateMachineLooper start failed";
    return ret;
  }
  return 0;
}

int PikaReplAuxiliary::Stop() {
  int res = 0;
  if (looper_ != nullptr) {
    res = looper_->StopThread();
  }
  if (res != 0) {
    LOG(ERROR) << "StateMachineLooper stop failed";
    return res;
  }
  LOG(INFO) << "StateMachineLooper stopped!";
  return 0;
}

void PikaReplAuxiliary::Signal() {
  if (looper_ == nullptr) {
    return;
  }
  looper_->Signal();
}

} // namespace replication
