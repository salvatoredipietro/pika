// Copyright (c) 2019-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_REPL_AUXILIARY_H_
#define PIKA_REPL_AUXILIARY_H_

#include <cstdint>

namespace replication {

class ReplicationManager;
class StateMachineLooper;

class PikaReplAuxiliary {
 public:
  constexpr static int kDefaultThreadQueueSize = 1024;
  PikaReplAuxiliary(ReplicationManager* rm,
                    int executor_queue_size = kDefaultThreadQueueSize);
  ~PikaReplAuxiliary();

  int Start();
  int Stop();
  void Signal();

 private:
  StateMachineLooper* looper_;
};

}  // namepsace replication

#endif // PIKA_REPL_AUXILIARY_H_
