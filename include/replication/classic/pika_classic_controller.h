// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef REPLICATION_PIKA_CLASSIC_CONTROLLER_H_
#define REPLICATION_PIKA_CLASSIC_CONTROLLER_H_

#include <string>
#include <sstream>
#include <vector>
#include <mutex>
#include <pthread.h>

#include "slash/include/slash_mutex.h"

#include "include/pika_conf.h"
#include "include/pika_define.h"
#include "include/replication/pika_repl_transporter.h"
#include "include/replication/classic/pika_classic_message_executor.h"

namespace replication {

//slave item
struct SlaveItem {
  enum Stage {
    kStageOne = 1,
    kStateTwo = 2,
  };
  PeerID peer_id;
  int conn_fd;
  Stage stage;
  std::vector<TableStruct> table_structs;
  struct timeval create_time;
};

class ClassicReplManager;

// MasterSlaveController emulate the classic mode.
class MasterSlaveController {
 public:
  constexpr static int kPikaMetaSyncMaxWaitTime = 10; // seconds
  // State machine for a slave instance.
  class ReplState {
   public:
    enum class Code : uint8_t {
      kReplNoConnect      = 0,
      kReplShouldMetaSync = 1,
      kReplMetaSyncDone   = 2,
      kReplError          = 3,
    };
    ReplState() : code_(Code::kReplNoConnect) { }
    explicit ReplState(const Code& code) : code_(code) { }
    std::string ToString() const;
    Code code() const { return code_; }
    void set_code(Code code) { code_ = code; }

   private:
    Code code_;
  };

  class RoleState {
   public:
    /*
     * (1) Single indicates no relationship has been constructed with others.
     * (2) Slave indicates the latest data should be fetched from master.
     * (3) Master indicates the latest data should be replicated to slave asynchronously.
     * (4) MasterAndSlave indicates the (2) And (3) at the same time.
     */
    enum Code : uint8_t {
      kRoleSingle          = 0x0,
      kRoleSlave           = 0x1,
      kRoleMaster          = 0x2,
      kRoleMasterAndSlave  = 0x3,
    };
    RoleState() : code_(kRoleSingle) { }
    explicit RoleState(const Code& code) : code_(code) { }
    std::string ToString() const;
    int code() const { return code_; }
    bool IsMaster() const { return code_ & kRoleMaster; }
    bool IsSlave() const { return code_ & kRoleSlave; }
    void DetachMaster() { code_ &= ~kRoleMaster; }
    void DetachSlave() { code_ &= ~kRoleSlave; }
    void AttachMaster() { code_ |= kRoleMaster; }
    void AttachSlave() { code_ |= kRoleSlave; }

   private:
    int code_;
  };

 public:
  MasterSlaveController(ClassicReplManager* rm);
  ~MasterSlaveController();
  MasterSlaveController(const MasterSlaveController&) = delete;
  MasterSlaveController& operator=(const MasterSlaveController&) = delete;

  int Start();
  int Stop();

  void StepStateMachine();
  void SyncError();
  void Messages(PeerMessageListMap& msgs);
  void ReportUnreachable(const PeerInfo& peer_info);
  bool TryAddSlave(const PeerID& peer_id, int peer_fd,
                   const std::vector<TableStruct>& table_structs);
  bool DeleteSlave(int fd, PeerID* peer_id);
  Status SendMetaSyncRequest();
  bool HandleMetaSyncRequest(const PeerID& peer_id, int peer_fd,
                             const std::vector<TableStruct>& table_structs);
  void HandleMetaSyncResponse();
  bool IsSlave();
  bool IsMaster();
  bool IsReadonly();
  PeerID CurrentMaster();
  void CurrentMaster(std::string& master_ip, int& master_port);
  void RemoveMaster(std::string& master_ip, int& master_port);
  Status SetMaster(const std::string& master_ip, int master_port,
                   bool force_full_sync);
  void BecomeMaster();
  Status InfoReplication(std::string& info_str);

 private:
  std::string master_ip();
  int master_port();
  RoleState role_state();
  ReplState repl_state();
  bool force_full_sync();
  void SetForceFullSync(bool v);
  void FinishMetaSync();
  bool MetaSyncDone();
  void ResetMetaSyncStatus();
  int GetMetaSyncTimestamp();
  void UpdateMetaSyncTimestamp();
  bool IsFirstMetaSync();
  void SetFirstMetaSync(bool v);
  void GetSlaveSyncInfo(std::stringstream& stream);
  void UnsafeClearMaster();

 private:
  ClassicReplManager* const rm_;
  ClassicMessageSender msg_sender_;
  const PeerID local_id_;

  pthread_rwlock_t state_protector_;
  ReplState repl_state_;
  RoleState role_;
  std::string master_ip_;
  int master_port_;
  int last_meta_sync_timestamp_;
  bool first_meta_sync_;
  bool force_full_sync_;

  std::mutex slave_mutex_;
  std::vector<SlaveItem> slaves_;
};

} // namespace replication

#endif // REPLICATION_PIKA_CLASSIC_CONTROLLER_H_
