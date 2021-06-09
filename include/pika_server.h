// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_SERVER_H_
#define PIKA_SERVER_H_

#include <sys/statfs.h>
#include <vector>
#include <memory>

#include "slash/include/slash_mutex.h"
#include "slash/include/slash_status.h"
#include "slash/include/slash_string.h"
#include "pink/include/bg_thread.h"
#include "pink/include/thread_pool.h"
#include "pink/include/pink_pubsub.h"
#include "blackwidow/blackwidow.h"
#include "blackwidow/backupable.h"

#include "include/pika_table.h"
#include "include/pika_monitor_thread.h"
#include "include/pika_rsync_service.h"
#include "include/pika_dispatch_thread.h"
#include "include/pika_client_processor.h"
#include "include/pika_apply_processor.h"
#include "include/pika_statistic.h"
#include "include/pika_conf.h"
#include "include/pika_define.h"
#include "include/storage/pika_binlog.h"
#include "include/replication/pika_memory_log.h"
#include "include/replication/pika_repl_manager.h"
#include "include/replication/pika_repl_rg_node.h"
#include "include/replication/pika_configuration.h"
#include "include/replication/classic/pika_classic_repl_manager.h"
#include "include/replication/cluster/pika_cluster_repl_manager.h"

using slash::Status;
using slash::Slice;

using replication::Configuration;
using replication::SnapshotSyncClosure;
using replication::SanityCheckFilter;
using replication::GroupIDAndInitConfMap;
using replication::ReplicationManager;
using replication::ReplicationGroupNode;
using replication::ReplicationManagerOptions;
using LogItem = replication::MemLog::LogItem;

/*
static std::set<std::string> MultiKvCommands {kCmdNameDel,
             kCmdNameMget,        kCmdNameKeys,              kCmdNameMset,
             kCmdNameMsetnx,      kCmdNameExists,            kCmdNameScan,
             kCmdNameScanx,       kCmdNamePKScanRange,       kCmdNamePKRScanRange,
             kCmdNameRPopLPush,   kCmdNameZUnionstore,       kCmdNameZInterstore,
             kCmdNameSUnion,      kCmdNameSUnionstore,       kCmdNameSInter,
             kCmdNameSInterstore, kCmdNameSDiff,             kCmdNameSDiffstore,
             kCmdNameSMove,       kCmdNameBitOp,             kCmdNamePfAdd,
             kCmdNamePfCount,     kCmdNamePfMerge,           kCmdNameGeoAdd,
             kCmdNameGeoPos,      kCmdNameGeoDist,           kCmdNameGeoHash,
             kCmdNameGeoRadius,   kCmdNameGeoRadiusByMember};
*/

static std::set<std::string> ClusterNotSupportCommands {
             kCmdNameMsetnx,      kCmdNameScan,              kCmdNameKeys,
             kCmdNameScanx,       kCmdNameZUnionstore,       kCmdNameZInterstore,
             kCmdNameSUnion,      kCmdNameSUnionstore,       kCmdNameSInter,
             kCmdNameSInterstore, kCmdNameSDiff,             kCmdNameSDiffstore,
             kCmdNameSMove,       kCmdNameBitOp,             kCmdNamePfAdd,
             kCmdNamePfCount,     kCmdNamePfMerge,           kCmdNameGeoAdd,
             kCmdNameGeoPos,      kCmdNameGeoDist,           kCmdNameGeoHash,
             kCmdNameGeoRadius,   kCmdNameGeoRadiusByMember, kCmdNamePKPatternMatchDel,
             kCmdNameSlaveof,     kCmdNameDbSlaveof,         kCmdNameMset,
             kCmdNameMget,        kCmdNameRPopLPush};

/*
static std::set<std::string> ShardingModeNotSupportCommands {
             kCmdNameMsetnx,      kCmdNameScan,              kCmdNameKeys,
             kCmdNameScanx,       kCmdNameZUnionstore,       kCmdNameZInterstore,
             kCmdNameSUnion,      kCmdNameSUnionstore,       kCmdNameSInter,
             kCmdNameSInterstore, kCmdNameSDiff,             kCmdNameSDiffstore,
             kCmdNameSMove,       kCmdNameBitOp,             kCmdNamePfAdd,
             kCmdNamePfCount,     kCmdNamePfMerge,           kCmdNameGeoAdd,
             kCmdNameGeoPos,      kCmdNameGeoDist,           kCmdNameGeoHash,
             kCmdNameGeoRadius,   kCmdNameGeoRadiusByMember, kCmdNamePKPatternMatchDel,
             kCmdNameSlaveof,     kCmdNameDbSlaveof};
*/


extern PikaConf *g_pika_conf;

enum TaskType {
  kCompactAll,
  kCompactStrings,
  kCompactHashes,
  kCompactSets,
  kCompactZSets,
  kCompactList,
  kPurgeLog,
  kStartKeyScan,
  kStopKeyScan,
  kBgSave,
};

class PikaServer : public replication::StateMachine {
 public:
  PikaServer();
  ~PikaServer();

  /*
   * Implement replication::StateMachine
   */
  virtual void OnStop(const ReplicationGroupID& group_id) override {}
  virtual void OnReplError(const ReplicationGroupID& group_id,
                           const Status& status) override;
  virtual void OnLeaderChanged(const ReplicationGroupID& group_id,
                               const PeerID& leader_id) override;
  virtual void OnApply(std::vector<LogItem> logs) override;
  virtual Status OnSnapshotSyncStart(const ReplicationGroupID& group_id) override;
  virtual Status CheckSnapshotReceived(const std::shared_ptr<ReplicationGroupNode>& node,
                                       LogOffset* snapshot_offset) override;
  virtual void TrySendSnapshot(const PeerID& peer_id,
                               const ReplicationGroupID& group_id,
                               const std::string& logger_filename,
                               int32_t top, SnapshotSyncClosure* closure) override;
  virtual void PurgeDir(const std::string& path) override;
  virtual void PurgelogsTaskSchedule(void (*function)(void*), void* arg) override;
  virtual void ReportLogAppendError(const ReplicationGroupID& group_id) override;

  /*
   * Server init info
   */
  bool ServerInit();

  void Start();
  void Exit();

  std::string host();
  int port();
  time_t start_time_s();
  bool readonly(const std::string& table, const std::string& key);
  bool ReadyForWrite(const std::string& table_name, const std::string& key);
  void SetDispatchQueueLimit(int queue_limit);
  blackwidow::BlackwidowOptions bw_options();

  /*
   * Table use
   */
  void InitTableStruct();
  Status AddTableStruct(std::string table_name, uint32_t num);
  Status DelTableStruct(std::string table_name);
  Status DelTableLog(const std::string& table_name);
  std::shared_ptr<Table> GetTable(const std::string& table_name);
  std::set<uint32_t> GetTablePartitionIds(const std::string& table_name);
  bool IsBgSaving();
  bool IsKeyScaning();
  bool IsCompacting();
  bool IsTableExist(const std::string& table_name);
  bool IsTablePartitionExist(const std::string& table_name, uint32_t partition_id);
  bool IsCommandSupport(const std::string& command);
  bool IsTableBinlogIoError(const std::string& table_name);
  Status DoSameThingSpecificTable(const TaskType& type, const std::set<std::string>& tables = {});

  /*
   * Partition use
   */
  void PartitionSetMaxCacheStatisticKeys(uint32_t max_cache_statistic_keys);
  void PartitionSetSmallCompactionThreshold(uint32_t small_compaction_threshold);
  std::shared_ptr<Partition> GetPartitionByDbName(const std::string& db_name);
  std::shared_ptr<Partition> GetTablePartitionById(const std::string& table_name,
                                                   uint32_t partition_id);
  std::shared_ptr<Partition> GetTablePartitionByKey(
                                  const std::string& table_name,
                                  const std::string& key);
  Status DoSameThingEveryPartition(const TaskType& type);

  /*
   * Master & Slave mode used
   */
  bool IsMaster();
  bool IsSlave();
  int32_t CountSyncSlaves();
  void CurrentMaster(std::string& master_ip, int& master_port);
  void RemoveMaster();
  Status SetMaster(const std::string& master_ip, int master_port,
                   bool force_full_sync);
  Status ResetMaster(const std::set<ReplicationGroupID>& group_ids,
                     const PeerID& master_id, bool force_full_sync);
  Status DisableMasterForReplicationGroup(const ReplicationGroupID& group_id);
  Status EnableMasterForReplicationGroup(const ReplicationGroupID& group_id,
      bool force_full_sync, bool reset_file_offset, uint32_t filenum, uint64_t offset);
 
  /*
   * Replication used
   */
  void InfoReplication(std::string& info);
  void ReplicationStatus(std::string& info);
  Status GetReplicationGroupInfo(const ReplicationGroupID& group_id, std::stringstream& stream);
  Status CreateReplicationGroupNodes(const std::set<ReplicationGroupID>& group_ids);
  Status CreateReplicationGroupNodes(const GroupIDAndInitConfMap& id_init_conf_map);
  Status CreateReplicationGroupNodesSanityCheck(const std::set<ReplicationGroupID>& group_ids);
  Status CreateReplicationGroupNodesSanityCheck(const GroupIDAndInitConfMap& id_init_conf_map);
  Status RemoveReplicationGroupNodes(const std::set<ReplicationGroupID>& group_ids);
  Status RemoveReplicationGroupNodesSanityCheck(const std::set<ReplicationGroupID>& group_ids);
  Status ReplicationGroupNodesSantiyCheck(const SanityCheckFilter& filter);
  std::shared_ptr<ReplicationGroupNode> GetReplicationGroupNode(const std::string& table_name,
                                                                uint32_t partition_id);
  std::shared_ptr<ReplicationGroupNode> GetReplicationGroupNode(const ReplicationGroupID& group_id);
  Status StartReplicationGroupNode(const ReplicationGroupID& group_id);
  Status StopReplicationGroupNode(const ReplicationGroupID& group_id);

  /*
   * PikaClientProcessor Process Task
   */
  void ScheduleClientPool(pink::TaskFunc func, void* arg);
  void ScheduleClientBgThreads(pink::TaskFunc func, void* arg, const std::string& hash_str);
  // for info debug
  size_t ClientProcessorThreadPoolCurQueueSize();

  /*
   * BGSave used
   */
  void BGSaveTaskSchedule(pink::TaskFunc func, void* arg);

  /*
   * PurgeLog used
   */
  void PurgeDirTaskSchedule(void (*function)(void*), void* arg);
  Status PurgeLogs(const ReplicationGroupID& group_id,
                   uint32_t to,
                   bool manual);

  /*
   * SnapshotSync used
   */
  void SnapshotSync(const std::string& ip, int port,
                    const std::string& table_name,
                    uint32_t partition_id,
                    SnapshotSyncClosure* closure);
  void SnapshotSyncSendFile(const std::string& ip, int port,
                            const std::string& table_name,
                            uint32_t partition_id,
                            SnapshotSyncClosure* closure);
  std::string SnapshotSyncTaskIndex(const std::string& ip, int port,
                                    const std::string& table_name,
                                    uint32_t partition_id);

  /*
   * Keyscan used
   */
  void KeyScanTaskSchedule(pink::TaskFunc func, void* arg);

  /*
   * Client used
   */
  void ClientKillAll();
  int ClientKill(const std::string &ip_port);
  int64_t ClientList(std::vector<ClientInfo> *clients = nullptr);

  /*
   * Monitor used
   */
  bool HasMonitorClients();
  void AddMonitorMessage(const std::string &monitor_message);
  void AddMonitorClient(std::shared_ptr<PikaClientConn> client_ptr);

  /*
   * Slowlog used
   */
  void SlowlogTrim();
  void SlowlogReset();
  uint32_t SlowlogLen();
  void SlowlogObtain(int64_t number, std::vector<SlowlogEntry>* slowlogs);
  void SlowlogPushEntry(const PikaCmdArgsType& argv, int32_t time, int64_t duration);

  /*
   * Statistic used
   */
  void ResetStat();
  uint64_t ServerQueryNum();
  uint64_t ServerCurrentQps();
  uint64_t accumulative_connections();
  void incr_accumulative_connections();
  void ResetLastSecQuerynum();
  void UpdateQueryNumAndExecCountTable(
      const std::string& table_name,
      const std::string& command, bool is_write);
  std::unordered_map<std::string, uint64_t> ServerExecCountTable();
  QpsStatistic ServerTableStat(const std::string& table_name);
  std::unordered_map<std::string, QpsStatistic> ServerAllTableStat();

  /*
   * PubSub used
   */
  int PubSubNumPat();
  int Publish(const std::string& channel, const std::string& msg);
  void EnablePublish(int fd);
  int UnSubscribe(std::shared_ptr<pink::PinkConn> conn,
                  const std::vector<std::string>& channels,
                  const bool pattern,
                  std::vector<std::pair<std::string, int>>* result);
  void Subscribe(std::shared_ptr<pink::PinkConn> conn,
                 const std::vector<std::string>& channels,
                 const bool pattern,
                 std::vector<std::pair<std::string, int>>* result);
  void PubSubChannels(const std::string& pattern,
                      std::vector<std::string>* result);
  void PubSubNumSub(const std::vector<std::string>& channels,
                    std::vector<std::pair<std::string, int>>* result);

  Status GetCmdRouting(std::vector<pink::RedisCmdArgsType>& redis_cmds,
      std::vector<Node>* dst, bool* all_local);

  // info debug use
  void ServerStatus(std::string* info);

  /*
   * BlackwidowOptions used
   */
  blackwidow::Status RewriteBlackwidowOptions(const blackwidow::OptionType& option_type,
                                              const std::unordered_map<std::string, std::string>& options);

  friend class Cmd;
  friend class InfoCmd;
  friend class PkClusterAddSlotsCmd;
  friend class PkClusterDelSlotsCmd;
  friend class PkClusterAddTableCmd;
  friend class PkClusterDelTableCmd;
  friend class PikaReplClientConn;
  friend class PkClusterInfoCmd;
  friend class PikaApplyProcessor;

 private:
  /*
   * TimingTask use
   */
  void DoTimingTask();
  void AutoCompactRange();
  void AutoPurge();
  void AutoDeleteExpiredDump();
  void AutoKeepAliveRSync();

  std::string host_;
  int port_;
  time_t start_time_s_;

  pthread_rwlock_t bw_options_rw_;
  blackwidow::BlackwidowOptions bw_options_;
  void InitBlackwidowOptions();
  std::shared_ptr<blackwidow::BlackWidow> state_db_;

  std::atomic<bool> exit_;

  /*
   * Table used
   */
  std::atomic<SlotState> slot_state_;
  pthread_rwlock_t tables_rw_;
  std::map<std::string, std::shared_ptr<Table>> tables_;

  /*
   * CronTask used
   */
  bool have_scheduled_crontask_;
  struct timeval last_check_compact_time_;

  /*
   * Communicate with the client used
   */
  int worker_num_;
  PikaClientProcessor* pika_client_processor_;
  PikaDispatchThread* pika_dispatch_thread_;

  /*
   * Replication used
   */
  ReplicationManager* pika_rm_;
  PikaApplyProcessor* pika_apply_processor_;

  /*
   * Bgsave used
   */
  pink::BGThread bgsave_thread_;

  /*
   * Purgelogs use
   */
  pink::BGThread purge_thread_;

  /*
   * DBSync used
   */
  slash::Mutex db_sync_protector_;
  std::unordered_set<std::string> db_sync_slaves_;

  /*
   * Keyscan used
   */
  pink::BGThread key_scan_thread_;

  /*
   * Monitor used
   */
  PikaMonitorThread* pika_monitor_thread_;

  /*
   * Rsync used
   */
  PikaRsyncService* pika_rsync_service_;

  /*
   * Pubsub used
   */
  pink::PubSubThread* pika_pubsub_thread_;

  /*
   * Slowlog used
   */
  uint64_t slowlog_entry_id_;
  pthread_rwlock_t slowlog_protector_;
  std::list<SlowlogEntry> slowlog_list_;

  /*
   * Statistic used
   */
  Statistic statistic_;

  PikaServer(PikaServer &ps);
  void operator =(const PikaServer &ps);
};

class FileBasedReplicationOptionsStorage : public replication::ReplicationOptionsStorage {
 public:
  FileBasedReplicationOptionsStorage(const PeerID& local_id,
                                     PikaConf* pika_conf)
    : local_id_(local_id), pika_conf_(pika_conf) { }
  ~FileBasedReplicationOptionsStorage() = default;

  virtual std::string slaveof() override { return pika_conf_->slaveof(); }
  virtual bool slave_read_only() override { return pika_conf_->slave_read_only(); }
  virtual int slave_priority() override { return pika_conf_->slave_priority(); }
  virtual std::string masterauth() override { return pika_conf_->masterauth(); }
  virtual std::string requirepass() override { return pika_conf_->requirepass(); }
  virtual const std::vector<TableStruct>& table_structs() override { return pika_conf_->table_structs(); }
  virtual bool classic_mode() override { return pika_conf_->classic_mode(); }
  virtual ReplicationProtocolType replication_protocol_type() override { return pika_conf_->replication_protocol_type(); }
  virtual int expire_logs_nums() override { return pika_conf_->expire_logs_nums(); }
  virtual int expire_logs_days() override { return pika_conf_->expire_logs_days(); }
  virtual int sync_window_size() override { return pika_conf_->sync_window_size(); }
  virtual int sync_thread_num() override { return pika_conf_->sync_thread_num(); }
  virtual int binlog_file_size() override { return pika_conf_->binlog_file_size(); }
  virtual std::string log_path() override { return pika_conf_->log_path(); }
  virtual int max_conn_rbuf_size() override { return pika_conf_->max_conn_rbuf_size(); }

  virtual void SetWriteBinlog(const std::string& value) override { pika_conf_->SetWriteBinlog(value); }
  virtual PeerID local_id() override { return local_id_; }
  virtual bool check_quorum() override { return pika_conf_->check_quorum(); }
  virtual bool pre_vote() override { return pika_conf_->pre_vote(); } 
  virtual uint64_t heartbeat_timeout_ms() override { return pika_conf_->heartbeat_timeout_ms(); }
  virtual uint64_t election_timeout_ms() override { return pika_conf_->election_timeout_ms(); }

 private:
  const PeerID local_id_;
  PikaConf* const pika_conf_;
};

#endif
