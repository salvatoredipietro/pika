// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_CLASSIC_REPL_MANAGER_H_
#define PIKA_CLASSIC_REPL_MANAGER_H_

#include "include/replication/pika_repl_manager.h"

#include <memory>
#include <string>
#include <vector>

#include "include/replication/classic/pika_classic_message_executor.h"
#include "include/replication/classic/pika_classic_controller.h"

namespace replication {

class ClassicReplManager : public ReplicationManager {
  friend class MetaSyncRequestHandler;
  friend class MetaSyncResponseHandler;
  friend class MasterSlaveController;
 public:
  explicit ClassicReplManager(const ReplicationManagerOptions& options);
  ~ClassicReplManager();

  virtual int Start() override;
  virtual int Stop() override;
  virtual void InfoReplication(std::string& info) override;
  virtual bool IsReadonly(const ReplicationGroupID& group_id) override;
  virtual int SendToPeer() override;
  virtual void StepStateMachine() override;

  virtual bool IsMaster() override;
  virtual bool IsSlave() override;
  virtual Status SetMaster(const std::string& master_ip, int master_port,
                           bool force_full_sync) override;
  virtual Status RemoveMaster() override;
  virtual Status ResetMaster(const std::set<ReplicationGroupID>& group_ids,
                             const PeerID& master_id, bool force_full_sync) override;
  virtual Status DisableMasterForReplicationGroup(const ReplicationGroupID& group_id) override;
  virtual Status EnableMasterForReplicationGroup(const ReplicationGroupID& group_id,
      bool force_full_sync, bool reset_file_offset, uint32_t filenum, uint64_t offset) override;
  virtual void CurrentMaster(std::string& master_ip,
                            int& master_port) override;

  void GetMasterSyncState(std::stringstream& out_of_sync);
  bool GetSlaveSyncState(std::stringstream& out_of_sync);

 private:
  virtual Status createReplicationGroupNodes(const GroupIDAndInitConfMap& id_init_conf_map) override;
  virtual void handlePeerMessage(MessageReporter::Message* peer_msg) override;
  virtual void reportTransportResult(const PeerInfo& peer_info,
                                     const MessageReporter::ResultType& r_type,
                                     const MessageReporter::DirectionType& d_type) override;
  /*
   * Handle the request from peer
   * @param peer_id : the remote ip:port
   * @param request : the request information
   * @param done : the done->Run() will be invoked when we finish processing.
   */
  void HandleRequest(const PeerID& peer_id, InnerMessage::InnerRequest* request,
                     IOClosure* done);
  /*
   * Handle the response from peer
   * @param peer_id : the remote ip:port
   * @param request : the response information
   * @param done : the done->Run() will be invoked when we finish processing.
   */
  void HandleResponse(const PeerID& peer_id, InnerMessage::InnerResponse* response,
                      IOClosure* done);
  void DispatchBinlogResponse(const PeerID& peer_id, InnerMessage::InnerResponse* response,
                              IOClosure* done);
  void SetMasterForRGNodes(const PeerID& leader_id, bool force_full_sync);
  void RemoveMasterForRGNodes(const PeerID& leader_id);

  bool HandleMetaSyncRequest(const PeerID& peer_id,
                             std::shared_ptr<pink::PbConn> stream,
                             std::vector<TableStruct>& table_structs);
  void HandleMetaSyncResponse();
  void MetaSyncError();
  std::string MakeLogPath(const ReplicationGroupID& group_id);

 private:
  MasterSlaveController* master_slave_controller_;
};

} // namespace replication

#endif // PIKA_CLASSIC_REPL_MANAGER_H_
