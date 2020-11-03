// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_APPLY_PROCESSOR_H_
#define PIKA_APPLY_PROCESSOR_H_

#include <vector>
#include <utility>
#include <string>

#include "pink/include/bg_thread.h"
#include "pink/include/redis_parser.h"

#include "include/util/callbacks.h"
#include "include/pika_define.h"
#include "include/replication/pika_memory_log.h"

using util::Closure;
using ReplTaskType = ReplTask::Type;
using LogFormat = ReplTask::LogFormat;
using LogItem = replication::MemLog::LogItem;

class ApplyTaskArg;

class ApplyHandler {
 public:
  ApplyHandler(size_t max_queue_size, const std::string& name_prefix);
  ~ApplyHandler();

  int Start();

  void Stop();

  void Schedule(ApplyTaskArg* arg) {
    worker_->Schedule(&ApplyHandler::Apply, static_cast<void*>(arg));
  }

 private:
  friend class ApplyTaskArg;

  void SetCurrentTask(ApplyTaskArg* arg) {
    current_task_ = arg;
  }

  static void Apply(void* arg);
  static int DealMessage(pink::RedisParser* parser, const pink::RedisCmdArgsType& argv);
  int ApplyFromLog();
  int ApplyRedisLog();
  int ApplyPBLog();

 private:
  ApplyTaskArg* current_task_;
  pink::RedisParser redis_parser_;
  pink::BGThread* worker_;
};

class PikaApplyProcessor {
 public:
  PikaApplyProcessor(size_t worker_num,
                     size_t max_quequ_size,
                     const std::string& name_prefix = "ApplyProcessor");
  ~PikaApplyProcessor();
  int Start();
  void Stop();
  void ScheduleApplyTask(ApplyTaskArg* arg);

 private:
  std::vector<ApplyHandler*> apply_handlers_;
};

class ApplyTaskArg {
  friend class ApplyHandler;
 public:
  explicit ApplyTaskArg(LogItem&& _item)
    : item(std::move(_item)),
    handler(nullptr) { }

  ~ApplyTaskArg() {
    if (handler != nullptr) {
      handler->SetCurrentTask(nullptr);
    }
  }

  ApplyTaskArg(ApplyTaskArg&& other)
    : item(std::move(other.item)),
    handler(other.handler) {
    other.handler = nullptr;
  }

  DISALLOW_COPY(ApplyTaskArg);

  std::string HashStr() {
    return item.task->group_id().ToString();
  }

  void AttachHandler(ApplyHandler* _handler) {
    handler = _handler;
  }

  void LockApplyHandler() {
    if (handler == nullptr) {
      return;
    }
    handler->SetCurrentTask(this);
  }

  ApplyHandler* Handler() {
    return handler;
  }

  ReplicationGroupID group_id() {
    return item.task->group_id();
  }

  std::string PeerIpPort() {
    return item.task->peer_id().ToString();
  }

  const std::string& log_data() const {
    return item.task->log();
  }

  const LogFormat& log_format() const {
    return item.task->log_format();
  }

  LogOffset log_offset() const {
    return LogOffset(BinlogOffset(item.attrs.filenum, item.attrs.offset),
                     LogicOffset(item.attrs.term_id, item.attrs.logic_id));
  }

  const ReplTaskType& task_type() const {
    return item.task->type();
  }

  Closure* Done() {
    return item.task->ReleaseClosure();
  }

 private:
  LogItem item;
  ApplyHandler* handler;
};

#endif // PIKA_APPLY_PROCESSOR_H_
