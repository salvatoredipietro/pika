// Copyright (c) 2018-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_PARTITION_H_
#define PIKA_PARTITION_H_

#include "blackwidow/blackwidow.h"
#include "blackwidow/backupable.h"
#include "slash/include/scope_record_lock.h"
#include "slash/include/slash_status.h"

#include "include/pika_define.h"
#include "include/storage/pika_binlog.h"
#include "include/replication/pika_configuration.h"
#include "include/replication/pika_repl_rg_node.h"
#include "include/replication/cluster/pika_cluster_rg_node.h"

using slash::Status;
using replication::Configuration;
using replication::ClusterMetaStorage;
using replication::ReplicationGroupNode;
using replication::ReplicationGroupNodeMetaStorage;

class Cmd;

/*
 *Keyscan used
 */
struct KeyScanInfo {
  time_t start_time;
  std::string s_start_time;
  int32_t duration;
  std::vector<blackwidow::KeyInfo> key_infos; //the order is strings, hashes, lists, zsets, sets
  bool key_scaning_;
  KeyScanInfo() :
      start_time(0),
      s_start_time("1970-01-01 08:00:00"),
      duration(-3),
      key_infos({{0, 0, 0, 0}, {0, 0, 0, 0}, {0, 0, 0, 0}, {0, 0, 0, 0}, {0, 0, 0, 0}}),
      key_scaning_(false) {
  }
};

struct BgSaveInfo {
  bool bgsaving;
  time_t start_time;
  std::string s_start_time;
  std::string path;
  LogOffset offset;
  Configuration conf_state;
  BgSaveInfo() : bgsaving(false), offset(), conf_state() {}
  void Clear() {
    bgsaving = false;
    path.clear();
    offset = LogOffset();
    conf_state.Reset();
  }
};

const std::string kConfStateKeyPrefix = "ConfState";
const std::string kTermKeyPrefix = "Term";
const std::string kVotedForKeyPrefix = "VotedFor";
const std::string kKeyGlue = "#";

class MemoryBasedMetaStorage : public ReplicationGroupNodeMetaStorage {
 public:
  MemoryBasedMetaStorage();
  virtual ~MemoryBasedMetaStorage();

  virtual Status applied_offset(LogOffset* offset) override;
  virtual Status set_applied_offset(const LogOffset& offset) override;
  virtual Status snapshot_offset(LogOffset* offset) override;
  virtual Status set_snapshot_offset(const LogOffset& offset) override;
  virtual Status configuration(Configuration* conf) override;
  virtual Status set_configuration(const Configuration& conf) override;
  virtual Status ApplyConfiguration(const Configuration& conf, const LogOffset& offset) override;
  virtual Status MetaSnapshot(LogOffset* applied_offset, Configuration* conf) override;
  virtual Status ResetOffset(const LogOffset& snapshot_offset,
                             const LogOffset& applied_offset) override;
  virtual Status ResetOffsetAndConfiguration(const LogOffset& snapshot_offset,
                                             const LogOffset& applied_offset,
                                             const Configuration& configuration) override;

 private:
  // protect the following fields
  pthread_rwlock_t rwlock_;
  LogOffset applied_offset_;
  LogOffset snapshot_offset_;
  Configuration configuration_;
};

class DBBasedClusterMetaStorage : public ClusterMetaStorage {
 public:
  DBBasedClusterMetaStorage(const std::shared_ptr<blackwidow::BlackWidow>& db,
                              const std::string& partition_name);
  ~DBBasedClusterMetaStorage();

  virtual Status applied_offset(LogOffset* offset) override;
  virtual Status set_applied_offset(const LogOffset& offset) override;
  virtual Status snapshot_offset(LogOffset* offset) override;
  virtual Status set_snapshot_offset(const LogOffset& offset) override;
  virtual Status configuration(Configuration* conf) override;
  virtual Status set_configuration(const Configuration& conf) override;
  virtual Status ApplyConfiguration(const Configuration& conf, const LogOffset& offset) override;
  virtual Status MetaSnapshot(LogOffset* applied_offset, Configuration* conf) override;
  virtual Status SetTermAndVotedFor(const PeerID& voted_for,
                                    uint32_t term) override;
  virtual Status GetTermAndVotedFor(uint32_t* term,
                                    PeerID* voted_for) override;
  virtual Status set_term(uint32_t term) override;
  virtual Status term(uint32_t* term) override;
  virtual Status set_voted_for(const PeerID& voted_for) override;
  virtual Status voted_for(PeerID* peer_id) override;
  virtual Status ResetOffset(const LogOffset& snapshot_offset,
                             const LogOffset& applied_offset) override;
  virtual Status ResetOffsetAndConfiguration(const LogOffset& snapshot_offset,
                                             const LogOffset& applied_offset,
                                             const Configuration& configuration) override;

 private:
  void Initialize();
  std::string MakeConfStateKey();
  std::string MakeTermKey();
  std::string MakeVotedForKey();
  Status unsafe_set_configuration(const Configuration& conf);

 private:
  std::string partition_name_;
  std::shared_ptr<blackwidow::BlackWidow> db_;

  // protect the following fields
  pthread_rwlock_t rwlock_;
  LogOffset applied_offset_;
  LogOffset snapshot_offset_;
  Configuration configuration_;
  uint32_t term_;
  PeerID voted_for_;
};

class Partition : public std::enable_shared_from_this<Partition> {
 public:
  Partition(const std::string& table_name,
            uint32_t partition_id,
            const std::string& table_db_path,
            const std::shared_ptr<blackwidow::BlackWidow>& state_db);
  virtual ~Partition();

  std::string GetTableName() const;
  uint32_t GetPartitionId() const;
  std::string GetPartitionName() const;
  std::shared_ptr<blackwidow::BlackWidow> db() const;

  void Compact(const blackwidow::DataType& type);

  void DbRWLockWriter();
  void DbRWLockReader();
  void DbRWUnLock();

  slash::lock::LockMgr* LockMgr();

  Status PrepareRsync();
  Status TryUpdateMasterOffset(const std::shared_ptr<ReplicationGroupNode>& node,
                               LogOffset* offset);
  bool ChangeDb(const std::string& new_path);

  void Leave();
  void Close();
  void MoveToTrash();

  // Replicate state use
  void set_repl_status(const Status& status);
  Status repl_status();
  void set_leader_id(const PeerID& leader_id);
  PeerID leader_id();

  // BgSave use;
  bool IsBgSaving();
  void BgSavePartition();
  BgSaveInfo bgsave_info();

  // FlushDB & FlushSubDB use
  bool FlushDB();
  bool FlushSubDB(const std::string& db_name);

  // key scan info use
  Status GetKeyNum(std::vector<blackwidow::KeyInfo>* key_info);
  KeyScanInfo GetKeyScanInfo();

  Status ApplyNormalEntry(const LogOffset& offset);
  Status ApplyConfChange(const InnerMessage::ConfChange& change, const LogOffset& offset);

 private:
  int Start();
  int Stop();

  std::string table_name_;
  uint32_t partition_id_;

  std::string db_path_;
  std::string bgsave_sub_path_;
  std::string dbsync_path_;
  std::string partition_name_;

  bool opened_;

  pthread_rwlock_t db_rwlock_;
  slash::lock::LockMgr* lock_mgr_;
  std::shared_ptr<blackwidow::BlackWidow> db_;

  bool full_sync_;

  slash::Mutex key_info_protector_;
  KeyScanInfo key_scan_info_;

  // Record the current meta state
  ReplicationGroupNodeMetaStorage* meta_storage_;
  // Record the state of replication
  pthread_rwlock_t rg_state_rwlock_;
  Status repl_status_;
  PeerID leader_id_;

  // BgSave use
  static void DoBgSave(void* arg);
  bool RunBgsaveEngine();
  bool InitBgsaveEnv();
  bool InitBgsaveEngine();
  void ClearBgsave();
  void FinishBgsave();
  BgSaveInfo bgsave_info_;
  slash::Mutex bgsave_protector_;
  blackwidow::BackupEngine* bgsave_engine_;

  // key scan info use
  void InitKeyScan();

  /*
   * No allowed copy and copy assign
   */
  Partition(const Partition&);
  void operator=(const Partition&);
};

#endif
