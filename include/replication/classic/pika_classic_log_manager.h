// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef REPLICATION_PIKA_CLASSIC_LOG_MANAGER_H_
#define REPLICATION_PIKA_CLASSIC_LOG_MANAGER_H_

#include <memory>
#include <string>

#include "slash/include/slash_status.h"

#include "include/pika_define.h"
#include "include/replication/pika_log_manager.h"

namespace replication {

class ClassicLogManager : public LogManager {
 public:
  enum class CheckResult : uint8_t {
    kOK                = 0,
    kError             = 1,
    kSyncPointLarger   = 2,
    kSyncPointBePurged = 3,
  };
  ClassicLogManager(const LogManagerOptions& options);
  ~ClassicLogManager() = default;

  virtual std::unique_ptr<LogReader> GetLogReader(const LogOffset& start_offset) override;
  virtual Status Reset(const LogOffset& snapshot_offset) override;

  void CheckOffset(const PeerID& peer_id, const BinlogOffset& offset,
                   CheckResult& result, BinlogOffset& expect);
  Status AppendLog(const std::shared_ptr<ReplTask>& task, LogOffset* log_offset);
  Status AppendReplicaLog(const PeerID& peer_id, BinlogItem::Attributes attribute, std::string log);
  LogOffset last_offset();
};

} // namespace replication

#endif // REPLICATION_PIKA_CLASSIC_LOG_MANAGER_H_
