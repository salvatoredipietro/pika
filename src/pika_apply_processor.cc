// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_apply_processor.h"

#include <glog/logging.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <google/protobuf/io//coded_stream.h>

#include "include/pika_server.h"
#include "include/pika_cmd_table_manager.h"
#include "proto/pika_inner_message.pb.h"

extern PikaConf* g_pika_conf;
extern PikaServer* g_pika_server;
extern PikaCmdTableManager* g_pika_cmd_table_manager;

using replication::Ready;
using storage::BINLOG_ENCODE_LEN;

PikaApplyProcessor::PikaApplyProcessor(
    size_t worker_num, size_t max_queue_size, const std::string& name_prefix) {
  for (size_t i = 0; i < worker_num; ++i) {
    ApplyHandler* apply_handler = new ApplyHandler(max_queue_size, name_prefix);
    apply_handlers_.push_back(apply_handler);
  }
}

PikaApplyProcessor::~PikaApplyProcessor() {
  for (size_t i = 0; i < apply_handlers_.size(); ++i) {
    delete apply_handlers_[i];
  }
  LOG(INFO) << "PikaApplyProcessor exit!!!";
}

int PikaApplyProcessor::Start() {
  int res = 0;
  for (size_t i = 0; i < apply_handlers_.size(); ++i) {
    res = apply_handlers_[i]->Start();
    if (res != pink::kSuccess) {
      return res;
    }
  }
  return res;
}

void PikaApplyProcessor::Stop() {
  for (size_t i = 0; i < apply_handlers_.size(); ++i) {
    apply_handlers_[i]->Stop();
  }
}

void PikaApplyProcessor::ScheduleApplyTask(ApplyTaskArg* apply_task_arg) {
  std::size_t index =
    std::hash<std::string>{}(apply_task_arg->HashStr()) % apply_handlers_.size();
  apply_task_arg->AttachHandler(apply_handlers_[index]);
  apply_handlers_[index]->Schedule(apply_task_arg);
}

ApplyHandler::ApplyHandler(size_t max_queue_size, const std::string& name_prefix) {
  worker_ = new pink::BGThread(max_queue_size);
  worker_->set_thread_name(name_prefix + "worker");
  pink::RedisParserSettings settings;
  settings.DealMessage = &(ApplyHandler::DealMessage);
  redis_parser_.RedisParserInit(REDIS_PARSER_REQUEST, settings);
  redis_parser_.data = this;
}

ApplyHandler::~ApplyHandler() {
  if (worker_ != nullptr) {
    delete worker_;
    worker_ = nullptr;
  }
}

int ApplyHandler::Start() {
  return worker_->StartThread();
}

void ApplyHandler::Stop() {
  worker_->StopThread();
}

void ApplyHandler::Apply(void* arg) {
  ApplyTaskArg* task_arg = static_cast<ApplyTaskArg*>(arg);
  util::ResourceGuard<ApplyTaskArg> task_guard(task_arg);

  task_arg->LockApplyHandler();
  Closure* done = task_arg->Done();
  switch (task_arg->task_type()) {
    case ReplTaskType::kClientType: {
      if (done == nullptr) {
        break;
      }
      LogClosure* closure = static_cast<LogClosure*>(done);
      closure->SetLogOffset(task_arg->log_offset());
      closure->Run();
      break;
    }
    case ReplTaskType::kRedoType:
    case ReplTaskType::kReplicaType:
    case ReplTaskType::kDummyType:
    case ReplTaskType::kOtherType: {
      ApplyHandler* handler = task_arg->Handler();
      if (0 != handler->ApplyFromLog()) {
        if (done != nullptr) {
          done->set_status(Status::Corruption("apply from log error"));
        }
      }
      if (done != nullptr) {
        done->Run();
      }
      break;
    }
    case ReplTaskType::kNoType:
    default:
      break;
  }
}

int ApplyHandler::ApplyFromLog() {
  const auto& log_format = current_task_->log_format();
  switch (log_format) {
    case LogFormat::kRedis:
      return ApplyRedisLog();
    case LogFormat::kPB:
      return ApplyPBLog();
    default:
      return -1;
  }
  return -1;
}

int ApplyHandler::ApplyRedisLog() {
  // Parse log to comand and execute it.
  const std::string& log = current_task_->log_data();
  const char* redis_parser_start = log.data();
  int redis_parser_len = static_cast<int>(log.size());
  int processed_len = 0;
  pink::RedisParserStatus ret = redis_parser_.ProcessInputBuffer(redis_parser_start,
                                                                 redis_parser_len,
                                                                 &processed_len);
  LogOffset offset = current_task_->log_offset();

  std::shared_ptr<Partition> partition = g_pika_server->GetTablePartitionById(current_task_->group_id().TableName(),
                                                                              current_task_->group_id().PartitionID());
  if (partition == nullptr) {
    LOG(WARNING) << "partition " << current_task_->group_id().ToString() << " not exist";
    return -1;
  }

  partition->ApplyNormalEntry(offset);

  std::shared_ptr<ReplicationGroupNode> node = g_pika_server->GetReplicationGroupNode(current_task_->group_id());
  if (node == nullptr) {
    LOG(WARNING) << "Replication group: " << current_task_->group_id().ToString()
                 << " not exist";
    return -1;
  }

  // We should advance applied index anyway.
  node->Advance(Ready(offset, LogOffset(), false, false));

  if (ret != pink::kRedisParserDone) {
    LOG(WARNING) << "Replication group: " << current_task_->group_id().ToString()
                 << "ApplyFromLog parse redis failed";
    return -1;
  }
  return 0;
}

int ApplyHandler::ApplyPBLog() {
  std::shared_ptr<ReplicationGroupNode> node = g_pika_server->GetReplicationGroupNode(current_task_->group_id());
  if (node == nullptr) {
    LOG(WARNING) << "Replication group: " << current_task_->group_id().ToString()
                 << " not exist";
    return -1;
  }
  const std::string& log = current_task_->log_data();
  InnerMessage::BaseEntry entry;
  ::google::protobuf::io::ArrayInputStream input(log.c_str(), log.size());
  ::google::protobuf::io::CodedInputStream decoder(&input);
  bool success = entry.ParseFromCodedStream(&decoder) && decoder.ConsumedEntireMessage();
  if (!success) {
    LOG(FATAL) << "Replication group: " << current_task_->group_id().ToString()
               << " parse PB log failed.";
  }
  auto entry_type = entry.type();
  switch (entry_type) {
    case InnerMessage::EntryType::kEntryConfChange: {
      auto conf_change = entry.conf_change();
      // persistent the configuration to storage.
      std::shared_ptr<Partition> partition = g_pika_server->GetTablePartitionById(current_task_->group_id().TableName(),
                                                                                  current_task_->group_id().PartitionID());
      if (partition == nullptr) {
        LOG(WARNING) << "Replication group: " << current_task_->group_id().ToString()
                     << " not exist.";
        break;
      }
      Status s = partition->ApplyConfChange(conf_change, current_task_->log_offset());
      if (!s.ok()) {
        LOG(WARNING) << "Replication group: " << current_task_->group_id().ToString()
                     << " ApplyConfChange failed: " << s.ToString();
        // Set id to empty to bypass underlying operation.
        PeerID empty_id;
        auto node = conf_change.mutable_node_id();
        node->set_ip(empty_id.Ip());
        node->set_port(empty_id.Port());
      }
      // After configuration persisted, notify underlying replication node.
      node->ApplyConfChange(conf_change);
    }
    default: {
      return -1;
    }
  }
  LogOffset offset = current_task_->log_offset();
  node->Advance(Ready(offset, LogOffset(), false, false));
  return 0;
}

int ApplyHandler::DealMessage(pink::RedisParser* parser, const pink::RedisCmdArgsType& argv) {
  std::string opt = argv[0];
  ApplyHandler* handler = static_cast<ApplyHandler*>(parser->data);
  ApplyTaskArg* current_task = handler->current_task_;
  std::string table_name = current_task->group_id().TableName();
  uint32_t partition_id = current_task->group_id().PartitionID();

  // Monitor related
  if (g_pika_server->HasMonitorClients()) {
    std::string monitor_message = std::to_string(1.0 * slash::NowMicros() / 1000000)
      + " [" + table_name + " " + current_task->PeerIpPort() + "]";
    for (const auto& item : argv) {
      monitor_message += " " + slash::ToRead(item);
    }
    g_pika_server->AddMonitorMessage(monitor_message);
  }

  std::shared_ptr<Cmd> c_ptr = g_pika_cmd_table_manager->GetCmd(slash::StringToLower(opt));
  if (!c_ptr) {
    LOG(WARNING) << "Command " << opt << " not in the command table";
    return -1;
  }

  // Initialize command
  c_ptr->Initial(argv, table_name);
  if (!c_ptr->res().ok()) {
    LOG(WARNING) << "Fail to initial command from binlog: " << opt;
    return -1;
  }

  g_pika_server->UpdateQueryNumAndExecCountTable(table_name, opt, c_ptr->is_write());

  // Applied to backend DB
  uint64_t start_us = 0;
  if (g_pika_conf->slowlog_slower_than() >= 0) {
    start_us = slash::NowMicros();
  }

  std::shared_ptr<Partition> partition = g_pika_server->GetTablePartitionById(table_name, partition_id);
  if (partition == nullptr) {
    LOG(WARNING) << "partition " << current_task->group_id().ToString() << " not exist";
    return -1;
  }
  // Add read lock for no suspend command
  if (!c_ptr->is_suspend()) {
    partition->DbRWLockReader();
  }

  c_ptr->Do(partition);

  if (!c_ptr->is_suspend()) {
    partition->DbRWUnLock();
  }

  if (g_pika_conf->slowlog_slower_than() >= 0) {
    int32_t start_time = start_us / 1000000;
    int64_t duration = slash::NowMicros() - start_us;
    if (duration > g_pika_conf->slowlog_slower_than()) {
      g_pika_server->SlowlogPushEntry(argv, start_time, duration);
      if (g_pika_conf->slowlog_write_errorlog()) {
        LOG(ERROR) << "command: " << argv[0] << ", start_time(s): " << start_time << ", duration(us): " << duration;
      }
    }
  }
  return 0;
}
