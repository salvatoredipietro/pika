// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_server.h"

#include <ctime>
#include <fstream>
#include <iterator>
#include <algorithm>
#include <ifaddrs.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/resource.h>

#include "slash/include/env.h"
#include "slash/include/rsync.h"
#include "pink/include/pink_cli.h"
#include "pink/include/redis_cli.h"
#include "pink/include/bg_thread.h"

#include "include/pika_server.h"
#include "include/pika_dispatch_thread.h"
#include "include/pika_cmd_table_manager.h"
#include "include/replication/classic/pika_classic_repl_manager.h"
#include "include/replication/cluster/pika_cluster_repl_manager.h"

extern PikaServer* g_pika_server;
extern PikaCmdTableManager* g_pika_cmd_table_manager;

using replication::Ready;
using storage::NewFileName;

std::string StateDBPath(const std::string& path) {
  static const std::string state_db_name("state_db");
  char buf[100];
  snprintf(buf, sizeof(buf), "%s/", state_db_name.data());
  return path + buf;
}

void DoPurgeDir(void* arg) {
  std::string path = *(static_cast<std::string*>(arg));
  LOG(INFO) << "Delete dir: " << path << " start";
  slash::DeleteDir(path);
  LOG(INFO) << "Delete dir: " << path << " done";
  delete static_cast<std::string*>(arg);
}

// SnapshotSync arg
struct SnapshotSyncArg {
  PikaServer* p;
  std::string ip;
  int port;
  std::string table_name;
  uint32_t partition_id;
  SnapshotSyncClosure* closure;
  SnapshotSyncArg(PikaServer* const _p,
                  const std::string& _ip,
                  int _port,
                  const std::string& _table_name,
                  uint32_t _partition_id,
                  SnapshotSyncClosure* _closure)
      : p(_p), ip(_ip), port(_port),
      table_name(_table_name), partition_id(_partition_id),
      closure(_closure) { }
};

void DoSnapshotSync(void* arg) {
  SnapshotSyncArg* dbsa = reinterpret_cast<SnapshotSyncArg*>(arg);
  PikaServer* const ps = dbsa->p;
  ps->SnapshotSyncSendFile(dbsa->ip, dbsa->port,
                           dbsa->table_name, dbsa->partition_id, dbsa->closure);
  delete dbsa;
}

PikaServer::PikaServer() :
  state_db_(nullptr),
  exit_(false),
  slot_state_(INFREE),
  have_scheduled_crontask_(false),
  last_check_compact_time_({0, 0}),
  slowlog_entry_id_(0) {

  //Init server ip host
  if (!ServerInit()) {
    LOG(FATAL) << "ServerInit iotcl error";
  }

  pthread_rwlockattr_t bw_options_rw_attr;
  pthread_rwlockattr_init(&bw_options_rw_attr);
  pthread_rwlockattr_setkind_np(&bw_options_rw_attr,
          PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);
  pthread_rwlock_init(&bw_options_rw_, &bw_options_rw_attr);


  InitBlackwidowOptions();

  if (!g_pika_conf->classic_mode()) {
    std::string state_db_path = StateDBPath(g_pika_conf->db_path());

    state_db_ = std::shared_ptr<blackwidow::BlackWidow>(new blackwidow::BlackWidow());
    state_db_->Open(bw_options(), state_db_path);
    assert(state_db_);
  }

  pthread_rwlockattr_t tables_rw_attr;
  pthread_rwlockattr_init(&tables_rw_attr);
  pthread_rwlockattr_setkind_np(&tables_rw_attr,
          PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);
  pthread_rwlock_init(&tables_rw_, &tables_rw_attr);

  // Create thread
  worker_num_ = std::min(g_pika_conf->thread_num(),
                         PIKA_MAX_WORKER_THREAD_NUM);

  std::set<std::string> ips;
  if (g_pika_conf->network_interface().empty()) {
    ips.insert("0.0.0.0");
  } else {
    ips.insert("127.0.0.1");
    ips.insert(host_);
  }
  // We estimate the queue size
  int worker_queue_limit = g_pika_conf->maxclients() / worker_num_ + 100;
  LOG(INFO) << "Worker queue limit is " << worker_queue_limit;
  pika_dispatch_thread_ = new PikaDispatchThread(ips, port_, worker_num_, 3000,
                                                 worker_queue_limit, g_pika_conf->max_conn_rbuf_size());
  pika_monitor_thread_ = new PikaMonitorThread();
  pika_rsync_service_ = new PikaRsyncService(g_pika_conf->db_sync_path(),
                                             g_pika_conf->port() + kPortShiftRSync);
  pika_pubsub_thread_ = new pink::PubSubThread();

  pika_client_processor_ = new PikaClientProcessor(g_pika_conf->thread_pool_size(), 100000);
  pika_apply_processor_ = new PikaApplyProcessor(g_pika_conf->thread_pool_size(), 100000);

  auto options_store = std::make_shared<FileBasedReplicationOptionsStorage>(
      PeerID(host_, port_), g_pika_conf);
  ReplicationManagerOptions options(this, nullptr, options_store);
  if (g_pika_conf->classic_mode()
      || g_pika_conf->replication_protocol_type() == kClassicProtocol) {
    pika_rm_ = new replication::ClassicReplManager(options);
  } else {
    pika_rm_ = new replication::ClusterReplManager(options);
  }

  pthread_rwlock_init(&slowlog_protector_, NULL);
}

PikaServer::~PikaServer() {
  pika_client_processor_->Stop();
  // DispatchThread will use queue of worker thread,
  // so we need to delete dispatch before worker.
  delete pika_dispatch_thread_;
  delete pika_pubsub_thread_;
  delete pika_client_processor_;
  delete pika_monitor_thread_;
  key_scan_thread_.StopThread();

  // clear tables before pika_rm_ to stop the underlying
  // replication groups.
  tables_.clear();

  // replication manager may invoke callbacks in apply_processor
  pika_rm_->Stop();
  pika_apply_processor_->Stop();
  bgsave_thread_.StopThread();

  delete pika_rm_;
  delete pika_apply_processor_;
  delete pika_rsync_service_;

  pthread_rwlock_destroy(&tables_rw_);
  pthread_rwlock_destroy(&slowlog_protector_);

  LOG(INFO) << "PikaServer " << pthread_self() << " exit!!!";
}

bool PikaServer::ServerInit() {
  std::string network_interface = g_pika_conf->network_interface();

  if (network_interface == "") {

    std::ifstream routeFile("/proc/net/route", std::ios_base::in);
    if (!routeFile.good())
    {
      return false;
    }

    std::string line;
    std::vector<std::string> tokens;
    while(std::getline(routeFile, line))
    {
      std::istringstream stream(line);
      std::copy(std::istream_iterator<std::string>(stream),
          std::istream_iterator<std::string>(),
          std::back_inserter<std::vector<std::string> >(tokens));

      // the default interface is the one having the second
      // field, Destination, set to "00000000"
      if ((tokens.size() >= 2) && (tokens[1] == std::string("00000000")))
      {
        network_interface = tokens[0];
        break;
      }

      tokens.clear();
    }
    routeFile.close();
  }
  LOG(INFO) << "Using Networker Interface: " << network_interface;

  struct ifaddrs * ifAddrStruct = NULL;
  struct ifaddrs * ifa = NULL;
  void * tmpAddrPtr = NULL;

  if (getifaddrs(&ifAddrStruct) == -1) {
    LOG(FATAL) << "getifaddrs failed: " << strerror(errno);
  }

  for (ifa = ifAddrStruct; ifa != NULL; ifa = ifa->ifa_next) {
    if (ifa->ifa_addr == NULL) {
      continue;
    }
    if (ifa ->ifa_addr->sa_family==AF_INET) { // Check it is
      // a valid IPv4 address
      tmpAddrPtr = &((struct sockaddr_in *)ifa->ifa_addr)->sin_addr;
      char addressBuffer[INET_ADDRSTRLEN];
      inet_ntop(AF_INET, tmpAddrPtr, addressBuffer, INET_ADDRSTRLEN);
      if (std::string(ifa->ifa_name) == network_interface) {
        host_ = addressBuffer;
        break;
      }
    } else if (ifa->ifa_addr->sa_family==AF_INET6) { // Check it is
      // a valid IPv6 address
      tmpAddrPtr = &((struct sockaddr_in6 *)ifa->ifa_addr)->sin6_addr;
      char addressBuffer[INET6_ADDRSTRLEN];
      inet_ntop(AF_INET6, tmpAddrPtr, addressBuffer, INET6_ADDRSTRLEN);
      if (std::string(ifa->ifa_name) == network_interface) {
        host_ = addressBuffer;
        break;
      }
    }
  }

  if (ifAddrStruct != NULL) {
    freeifaddrs(ifAddrStruct);
  }
  if (ifa == NULL) {
    LOG(FATAL) << "error network interface: " << network_interface << ", please check!";
  }

  port_ = g_pika_conf->port();
  LOG(INFO) << "host: " << host_ << " port: " << port_;
  return true;
}

void PikaServer::Start() {
  // We Init Table Struct Before Start The following thread
  InitTableStruct();

  pika_rm_->Start();

  int ret = 0;
  // start rsync first, rocksdb opened fd will not appear in this fork
  ret = pika_rsync_service_->StartRsync();
  if (0 != ret) {
    tables_.clear();
    LOG(FATAL) << "Start Rsync Error: bind port " +std::to_string(pika_rsync_service_->ListenPort()) + " failed"
      <<  ", Listen on this port to receive Master FullSync Data";
  }

  ret = pika_apply_processor_->Start();
  if (ret != pink::kSuccess) {
    tables_.clear();
    LOG(FATAL) << "Start PikaApplyProcessor Error: " << ret << (ret == pink::kCreateThreadError ? ": create thread error " : ": other error");
  }
  ret = pika_client_processor_->Start();
  if (ret != pink::kSuccess) {
    tables_.clear();
    LOG(FATAL) << "Start PikaClientProcessor Error: " << ret << (ret == pink::kCreateThreadError ? ": create thread error " : ": other error");
  }
  ret = pika_dispatch_thread_->StartThread();
  if (ret != pink::kSuccess) {
    tables_.clear();
    LOG(FATAL) << "Start Dispatch Error: " << ret << (ret == pink::kBindError ? ": bind port " + std::to_string(port_) + " conflict"
            : ": other error") << ", Listen on this port to handle the connected redis client";
  }
  ret = pika_pubsub_thread_->StartThread();
  if (ret != pink::kSuccess) {
    tables_.clear();
    LOG(FATAL) << "Start Pubsub Error: " << ret << (ret == pink::kBindError ? ": bind port conflict" : ": other error");
  }

  time(&start_time_s_);

  LOG(INFO) << "Pika Server going to start";
  while (!exit_) {
    DoTimingTask();
    // wake up every 10 second
    int try_num = 0;
    while (!exit_ && try_num++ < 5) {
      sleep(1);
    }
  }
  LOG(INFO) << "Goodbye...";
}

void PikaServer::Exit() {
  exit_ = true;
}

std::string PikaServer::host() {
  return host_;
}

int PikaServer::port() {
  return port_;
}

time_t PikaServer::start_time_s() {
  return start_time_s_;
}

bool PikaServer::IsMaster() {
  return pika_rm_->IsMaster();
}

bool PikaServer::IsSlave() {
  return pika_rm_->IsSlave();
}

int32_t PikaServer::CountSyncSlaves() {
  slash::MutexLock ldb(&db_sync_protector_);
  return db_sync_slaves_.size();
}

void PikaServer::CurrentMaster(std::string& master_ip, int& master_port) {
  pika_rm_->CurrentMaster(master_ip, master_port);
}

Status PikaServer::SetMaster(const std::string& master_ip,
                             int master_port,
                             bool force_full_sync) {
  return pika_rm_->SetMaster(master_ip, master_port, force_full_sync);
}

void PikaServer::RemoveMaster() {
  pika_rm_->RemoveMaster();
}

Status PikaServer::ResetMaster(const std::set<ReplicationGroupID>& group_ids,
                               const PeerID& master_id, bool force_full_sync) {
  return pika_rm_->ResetMaster(group_ids, master_id, force_full_sync);
}

Status PikaServer::DisableMasterForReplicationGroup(const ReplicationGroupID& group_id) {
  return pika_rm_->DisableMasterForReplicationGroup(group_id);
}

Status PikaServer::EnableMasterForReplicationGroup(const ReplicationGroupID& group_id,
    bool force_full_sync, bool reset_file_offset, uint32_t filenum, uint64_t offset) {
  return pika_rm_->EnableMasterForReplicationGroup(group_id, force_full_sync,
      reset_file_offset, filenum, offset);
}

Status PikaServer::PurgeLogs(const ReplicationGroupID& group_id,
                             uint32_t to,
                             bool manual) {
  return pika_rm_->PurgeLogs(group_id, to, manual);
}

void PikaServer::InfoReplication(std::string& info) {
  pika_rm_->InfoReplication(info);
}

void PikaServer::ReplicationStatus(std::string& info) {
  pika_rm_->ReplicationStatus(info);
}

Status PikaServer::GetReplicationGroupInfo(const ReplicationGroupID& group_id,
                                           std::stringstream& stream) {
  return pika_rm_->GetReplicationGroupInfo(group_id, stream);
}

std::shared_ptr<ReplicationGroupNode> PikaServer::GetReplicationGroupNode(const std::string& table_name,
                                                                          uint32_t partition_id) {
  return GetReplicationGroupNode(ReplicationGroupID(table_name, partition_id));
}

std::shared_ptr<ReplicationGroupNode> PikaServer::GetReplicationGroupNode(const ReplicationGroupID& group_id) {
  return pika_rm_->GetReplicationGroupNode(group_id);
}

Status PikaServer::StartReplicationGroupNode(const ReplicationGroupID& group_id) {
  return pika_rm_->StartReplicationGroupNode(group_id);
}

Status PikaServer::StopReplicationGroupNode(const ReplicationGroupID& group_id) {
  return pika_rm_->StopReplicationGroupNode(group_id);
}

Status PikaServer::CreateReplicationGroupNodes(const std::set<ReplicationGroupID>& group_ids) {
  return pika_rm_->CreateReplicationGroupNodes(group_ids);
}

Status PikaServer::CreateReplicationGroupNodes(const GroupIDAndInitConfMap& id_init_conf_map) {
  return pika_rm_->CreateReplicationGroupNodes(id_init_conf_map);
}

Status PikaServer::CreateReplicationGroupNodesSanityCheck(const std::set<ReplicationGroupID>& group_ids) {
  return pika_rm_->CreateReplicationGroupNodesSanityCheck(group_ids);
}

Status PikaServer::CreateReplicationGroupNodesSanityCheck(const GroupIDAndInitConfMap& id_init_conf_map) {
  return pika_rm_->CreateReplicationGroupNodesSanityCheck(id_init_conf_map);
}

Status PikaServer::RemoveReplicationGroupNodes(const std::set<ReplicationGroupID>& group_ids) {
  return pika_rm_->RemoveReplicationGroupNodes(group_ids);
}

Status PikaServer::RemoveReplicationGroupNodesSanityCheck(const std::set<ReplicationGroupID>& group_ids) {
  return pika_rm_->RemoveReplicationGroupNodesSanityCheck(group_ids);
}

Status PikaServer::ReplicationGroupNodesSantiyCheck(const SanityCheckFilter& filter) {
  return pika_rm_->ReplicationGroupNodesSantiyCheck(filter);
}

bool PikaServer::readonly(const std::string& table_name, const std::string& key) {
  std::shared_ptr<Table> table = GetTable(table_name);
  if (table == nullptr) {
    // swallow this error will process later
    return false;
  }
  uint32_t index = g_pika_cmd_table_manager->DistributeKey(
      key, table->PartitionNum());
  return pika_rm_->IsReadonly(ReplicationGroupID(table_name, index));
}

void PikaServer::OnReplError(const ReplicationGroupID& group_id, const Status& status) {
  auto partition = GetTablePartitionById(group_id.TableName(),
                                         group_id.PartitionID());
  if (partition == nullptr) {
    return;
  }
  partition->set_repl_status(status);
}

void PikaServer::OnLeaderChanged(const ReplicationGroupID& group_id,
                                 const PeerID& leader_id) {
  auto partition = GetTablePartitionById(group_id.TableName(),
                                         group_id.PartitionID());
  if (partition == nullptr) {
    return;
  }
  partition->set_leader_id(leader_id);
}

void PikaServer::OnApply(std::vector<LogItem> logs) {
  size_t num = logs.size();
  for (size_t i = 0; i < num; i++) {
    ApplyTaskArg* apply_task_arg = new ApplyTaskArg(std::move(logs[i]));
    pika_apply_processor_->ScheduleApplyTask(apply_task_arg);
  }
}

Status PikaServer::OnSnapshotSyncStart(const ReplicationGroupID& group_id) {
  auto partition = GetTablePartitionById(group_id.TableName(),
                                         group_id.PartitionID());
  if (partition == nullptr) {
    return Status::NotFound(group_id.ToString());
  }
  return partition->PrepareRsync();
}

Status PikaServer::CheckSnapshotReceived(const std::shared_ptr<ReplicationGroupNode>& node,
                                         LogOffset* snapshot_offset) {
  auto partition = GetTablePartitionById(node->group_id().TableName(),
                                         node->group_id().PartitionID());
  if (partition == nullptr) {
    return Status::NotFound(node->group_id().ToString());
  }
  return partition->TryUpdateMasterOffset(node, snapshot_offset);
}

void PikaServer::ReportLogAppendError(const ReplicationGroupID& group_id) {
  std::shared_ptr<Table> table = GetTable(group_id.TableName());
  if (table) {
    table->SetBinlogIoError();
  }  
}

bool PikaServer::ReadyForWrite(const std::string& table_name, const std::string& key) {
  std::shared_ptr<Table> table = GetTable(table_name);
  if (table == nullptr) {
    return false;
  }
  uint32_t index = g_pika_cmd_table_manager->DistributeKey(
      key, table->PartitionNum());

  auto node = GetReplicationGroupNode(ReplicationGroupID(table_name, index));
  if (node == nullptr) {
    LOG(WARNING) << "Replicaiton group: " << table_name << ":" << index
                 << ", NotFound";
    return false;
  }
  Status s = node->IsReady();
  if (!s.ok()) {
    return false;
  }
  return true;
}

void PikaServer::SetDispatchQueueLimit(int queue_limit) {
  rlimit limit;
  rlim_t maxfiles = g_pika_conf->maxclients() + PIKA_MIN_RESERVED_FDS;
  if (getrlimit(RLIMIT_NOFILE, &limit) == -1) {
    LOG(WARNING) << "getrlimit error: " << strerror(errno);
  } else if (limit.rlim_cur < maxfiles) {
    rlim_t old_limit = limit.rlim_cur;
    limit.rlim_cur = maxfiles;
    limit.rlim_max = maxfiles;
    if (setrlimit(RLIMIT_NOFILE, &limit) != -1) {
      LOG(WARNING) << "your 'limit -n ' of " << old_limit << " is not enough for Redis to start. pika have successfully reconfig it to " << limit.rlim_cur;
    } else {
      LOG(FATAL) << "your 'limit -n ' of " << old_limit << " is not enough for Redis to start. pika can not reconfig it(" << strerror(errno) << "), do it by yourself";
    }
  }
  pika_dispatch_thread_->SetQueueLimit(queue_limit);
}

blackwidow::BlackwidowOptions PikaServer::bw_options() {
  slash::RWLock rwl(&bw_options_rw_, false);
  return bw_options_;
}

void PikaServer::InitTableStruct() {
  std::string db_path = g_pika_conf->db_path();
  std::string log_path = g_pika_conf->log_path();
  std::vector<TableStruct> table_structs = g_pika_conf->table_structs();
  slash::RWLock rwl(&tables_rw_, true);
  for (const auto& table : table_structs) {
    std::string name = table.table_name;
    uint32_t num = table.partition_num;
    std::shared_ptr<Table> table_ptr = std::make_shared<Table>(
        name, num, db_path, log_path, state_db_);
    table_ptr->AddPartitions(table.partition_ids);
    tables_.emplace(name, table_ptr);
  }
}

Status PikaServer::AddTableStruct(std::string table_name, uint32_t num) {
  std::shared_ptr<Table> table = g_pika_server->GetTable(table_name);
  if (table) {
    return Status::Corruption("table already exist");
  }
  std::string db_path = g_pika_conf->db_path();
  std::string log_path = g_pika_conf->log_path();
  std::shared_ptr<Table> table_ptr = std::make_shared<Table>(
      table_name, num, db_path, log_path, state_db_);
  slash::RWLock rwl(&tables_rw_, true);
  tables_.emplace(table_name, table_ptr);
  return  Status::OK();
}

Status PikaServer::DelTableStruct(std::string table_name) {
  std::shared_ptr<Table> table = g_pika_server->GetTable(table_name);
  if (!table) {
    return Status::Corruption("table not found");
  }
  if (!table->TableIsEmpty()) {
    return Status::Corruption("table have partitions");
  }
  Status s = table->Leave();
  if (!s.ok()) {
    return s;
  }
  tables_.erase(table_name);
  return  Status::OK();
}

std::shared_ptr<Table> PikaServer::GetTable(const std::string &table_name) {
  slash::RWLock l(&tables_rw_, false);
  auto iter = tables_.find(table_name);
  return (iter == tables_.end()) ? NULL : iter->second;
}

Status PikaServer::DelTableLog(const std::string &table_name) {
  std::string table_log_path = g_pika_conf->log_path() + "log_" + table_name ;
  std::string table_log_path_tmp = table_log_path + "_deleting/";
  if (slash::RenameFile(table_log_path, table_log_path_tmp)) {
    LOG(WARNING) << "Failed to move log to trash, error: " << strerror(errno);
    return Status::Corruption("Failed to move log to trash");
  }
  PurgeDir(table_log_path_tmp);
  LOG(WARNING) << "Partition StableLog: " << table_name << " move to trash success";
  return Status::OK();
}

std::set<uint32_t> PikaServer::GetTablePartitionIds(const std::string& table_name) {
  std::set<uint32_t> empty;
  slash::RWLock l(&tables_rw_, false);
  auto iter = tables_.find(table_name);
  return (iter == tables_.end()) ? empty : iter->second->GetPartitionIds();
}

bool PikaServer::IsBgSaving() {
  slash::RWLock table_rwl(&tables_rw_, false);
  for (const auto& table_item : tables_) {
    slash::RWLock partition_rwl(&table_item.second->partitions_rw_, false);
    for (const auto& patition_item : table_item.second->partitions_) {
      if (patition_item.second->IsBgSaving()) {
        return true;
      }
    }
  }
  return false;
}

bool PikaServer::IsKeyScaning() {
  slash::RWLock table_rwl(&tables_rw_, false);
  for (const auto& table_item : tables_) {
    if (table_item.second->IsKeyScaning()) {
      return true;
    }
  }
  return false;
}

bool PikaServer::IsCompacting() {
  slash::RWLock table_rwl(&tables_rw_, false);
  for (const auto& table_item : tables_) {
    slash::RWLock partition_rwl(&table_item.second->partitions_rw_, false);
    for (const auto& partition_item : table_item.second->partitions_) {
      partition_item.second->DbRWLockReader();
      std::string task_type = partition_item.second->db()->GetCurrentTaskType();
      partition_item.second->DbRWUnLock();
      if (strcasecmp(task_type.data(), "no")) {
        return true;
      }
    }
  }
  return false;
}

bool PikaServer::IsTableExist(const std::string& table_name) {
  return GetTable(table_name) ? true : false;
}

bool PikaServer::IsTablePartitionExist(const std::string& table_name,
                                       uint32_t partition_id) {
  std::shared_ptr<Table> table_ptr = GetTable(table_name);
  if (!table_ptr) {
    return false;
  } else {
    return table_ptr->GetPartitionById(partition_id) ? true : false;
  }
}

bool PikaServer::IsCommandSupport(const std::string& command) {
  if (!g_pika_conf->classic_mode()) {
    // dont support multi key command
    // used the same list as sharding mode use
    bool res = !ClusterNotSupportCommands.count(command);
    if (!res) {
      return res;
    }
  }
  return true;
}

bool PikaServer::IsTableBinlogIoError(const std::string& table_name) {
  std::shared_ptr<Table> table = GetTable(table_name);
  return table ? table->IsBinlogIoError() : true;
}

// If no collection of specified tables is given, we execute task in all tables
Status PikaServer::DoSameThingSpecificTable(const TaskType& type, const std::set<std::string>& tables) {
  slash::RWLock rwl(&tables_rw_, false);
  for (const auto& table_item : tables_) {
    if (!tables.empty()
      && tables.find(table_item.first) == tables.end()) {
      continue;
    } else {
      switch (type) {
        case TaskType::kCompactAll:
          table_item.second->Compact(blackwidow::DataType::kAll);
          break;
        case TaskType::kCompactStrings:
          table_item.second->Compact(blackwidow::DataType::kStrings);
          break;
        case TaskType::kCompactHashes:
          table_item.second->Compact(blackwidow::DataType::kHashes);
          break;
        case TaskType::kCompactSets:
          table_item.second->Compact(blackwidow::DataType::kSets);
          break;
        case TaskType::kCompactZSets:
          table_item.second->Compact(blackwidow::DataType::kZSets);
          break;
        case TaskType::kCompactList:
          table_item.second->Compact(blackwidow::DataType::kLists);
          break;
        case TaskType::kStartKeyScan:
          table_item.second->KeyScan();
          break;
        case TaskType::kStopKeyScan:
          table_item.second->StopKeyScan();
          break;
        case TaskType::kBgSave:
          table_item.second->BgSaveTable();
          break;
        default:
          break;
      }
    }
  }
  return Status::OK();
}

void PikaServer::PartitionSetMaxCacheStatisticKeys(uint32_t max_cache_statistic_keys) {
  slash::RWLock rwl(&tables_rw_, false);
  for (const auto& table_item : tables_) {
    for (const auto& partition_item : table_item.second->partitions_) {
      partition_item.second->DbRWLockReader();
      partition_item.second->db()->SetMaxCacheStatisticKeys(max_cache_statistic_keys);
      partition_item.second->DbRWUnLock();
    }
  }
}

void PikaServer::PartitionSetSmallCompactionThreshold(uint32_t small_compaction_threshold) {
  slash::RWLock rwl(&tables_rw_, false);
  for (const auto& table_item : tables_) {
    for (const auto& partition_item : table_item.second->partitions_) {
      partition_item.second->DbRWLockReader();
      partition_item.second->db()->SetSmallCompactionThreshold(small_compaction_threshold);
      partition_item.second->DbRWUnLock();
    }
  }
}

// Only use in classic mode
std::shared_ptr<Partition> PikaServer::GetPartitionByDbName(const std::string& db_name) {
  std::shared_ptr<Table> table = GetTable(db_name);
  return table ? table->GetPartitionById(0) : NULL;
}

std::shared_ptr<Partition> PikaServer::GetTablePartitionById(
                                    const std::string& table_name,
                                    uint32_t partition_id) {
  std::shared_ptr<Table> table = GetTable(table_name);
  return table ? table->GetPartitionById(partition_id) : NULL;
}

std::shared_ptr<Partition> PikaServer::GetTablePartitionByKey(
                                    const std::string& table_name,
                                    const std::string& key) {
  std::shared_ptr<Table> table = GetTable(table_name);
  return table ? table->GetPartitionByKey(key) : NULL;
}

Status PikaServer::DoSameThingEveryPartition(const TaskType& type) {
  slash::RWLock rwl(&tables_rw_, false);
  for (const auto& table_item : tables_) {
    for (const auto& partition_item : table_item.second->partitions_) {
      switch (type) {
        case TaskType::kPurgeLog:
          {
            pika_rm_->PurgeLogs(ReplicationGroupID(table_item.second->GetTableName(),
                                                   partition_item.second->GetPartitionId()));
            break;
          }
        case TaskType::kCompactAll:
          partition_item.second->Compact(blackwidow::kAll);
          break;
        default:
          break;
      }
    }
  }
  return Status::OK();
}

void PikaServer::ScheduleClientPool(pink::TaskFunc func, void* arg) {
  pika_client_processor_->SchedulePool(func, arg);
}

void PikaServer::ScheduleClientBgThreads(
    pink::TaskFunc func, void* arg, const std::string& hash_str) {
  pika_client_processor_->ScheduleBgThreads(func, arg, hash_str);
}

size_t PikaServer::ClientProcessorThreadPoolCurQueueSize() {
  if (!pika_client_processor_) {
    return 0;
  }
  return pika_client_processor_->ThreadPoolCurQueueSize();
}

void PikaServer::BGSaveTaskSchedule(pink::TaskFunc func, void* arg) {
  bgsave_thread_.StartThread();
  bgsave_thread_.Schedule(func, arg);
}

void PikaServer::PurgelogsTaskSchedule(void (*function)(void*), void* arg) {
  purge_thread_.StartThread();
  purge_thread_.Schedule(function, arg);
}

void PikaServer::PurgeDir(const std::string& path) {
  std::string* dir_path = new std::string(path);
  PurgeDirTaskSchedule(&DoPurgeDir, static_cast<void*>(dir_path));
}

void PikaServer::PurgeDirTaskSchedule(void (*function)(void*), void* arg) {
  purge_thread_.StartThread();
  purge_thread_.Schedule(function, arg);
}

void PikaServer::SnapshotSync(const std::string& ip, int port,
                              const std::string& table_name,
                              uint32_t partition_id,
                              SnapshotSyncClosure* closure) {
  util::ClosureGuard guard(closure);
  {
    std::string task_index =
      SnapshotSyncTaskIndex(ip, port, table_name, partition_id);
    slash::MutexLock ml(&db_sync_protector_);
    if (db_sync_slaves_.find(task_index) != db_sync_slaves_.end()) {
      if (closure) {
        closure->set_status(Status::Corruption("SnapshotSync is being processed"));
      }
      return;
    }
    db_sync_slaves_.insert(task_index);
  }
  // Reuse the bgsave_thread_
  // Since we expect BgSave and SnapshotSync execute serially
  bgsave_thread_.StartThread();
  SnapshotSyncArg* arg = new SnapshotSyncArg(this, ip, port, table_name, partition_id,
                                 static_cast<SnapshotSyncClosure*>(guard.Release()));
  bgsave_thread_.Schedule(&DoSnapshotSync, reinterpret_cast<void*>(arg));
}

void PikaServer::TrySendSnapshot(const PeerID& peer_id,
                                 const ReplicationGroupID& group_id,
                                 const std::string& logger_filename,
                                 int32_t top, SnapshotSyncClosure* closure) {
  util::ClosureGuard guard(closure);
  std::string table_name = group_id.TableName();
  uint32_t partition_id = group_id.PartitionID();
  std::string ip = peer_id.Ip();
  int port = peer_id.Port() + kPortShiftRSync;

  std::shared_ptr<Partition> partition =
    GetTablePartitionById(table_name, partition_id);
  if (!partition) {
    LOG(WARNING) << "Partition: " << group_id.ToString()
                 << " Not Found, TrySendSnapshot Failed";
    if (closure) {
      closure->set_status(Status::NotFound(group_id.ToString()));
    }
    return;
  }
  BgSaveInfo bgsave_info = partition->bgsave_info();
  if (slash::IsDir(bgsave_info.path) != 0
    || !slash::FileExists(NewFileName(logger_filename, bgsave_info.offset.b_offset.filenum))
    || top - bgsave_info.offset.b_offset.filenum > kSnapshotSyncMaxGap) {
    // Need Bgsave first
    partition->BgSavePartition();
  }
  SnapshotSync(ip, port, table_name, partition_id,
               static_cast<SnapshotSyncClosure*>(guard.Release()));
}

void PikaServer::SnapshotSyncSendFile(const std::string& ip, int port,
                                      const std::string& table_name,
                                      uint32_t partition_id,
                                      SnapshotSyncClosure* closure) {
  util::ClosureGuard guard(closure);
  std::shared_ptr<Partition> partition = GetTablePartitionById(table_name, partition_id);
  if (!partition) {
    LOG(WARNING) << "Partition: " << table_name << ":" << partition_id
                 << " Not Found, SnapshotSync send file Failed";
    if (closure) {
      closure->set_status(Status::NotFound(table_name + ":" + std::to_string(partition_id)));
    }
    return;
  }

  BgSaveInfo bgsave_info = partition->bgsave_info();
  std::string bg_path = bgsave_info.path;
  uint32_t binlog_filenum = bgsave_info.offset.b_offset.filenum;
  uint64_t binlog_offset = bgsave_info.offset.b_offset.offset;
  uint32_t term = bgsave_info.offset.l_offset.term;
  uint64_t index = bgsave_info.offset.l_offset.index;

  if (closure) {
    closure->MarkMeta(SnapshotSyncClosure::SnapshotSyncMetaInfo(PeerID(ip, port),
          LogOffset(BinlogOffset(binlog_filenum, binlog_offset), LogicOffset(term, index)),
          bgsave_info.conf_state));
  }

  // Get all files need to send
  std::vector<std::string> descendant;
  int ret = 0;
  LOG(INFO) << "Partition: " << partition->GetPartitionName()
            << " Start Send files in " << bg_path << " to " << ip;
  ret = slash::GetChildren(bg_path, descendant);
  if (ret != 0) {
    std::string ip_port = slash::IpPortString(ip, port);
    slash::MutexLock ldb(&db_sync_protector_);
    db_sync_slaves_.erase(ip_port);
    LOG(WARNING) << "Partition: " << partition->GetPartitionName()
                 << " Get child directory when try to do sync failed, error: " << strerror(ret);
    if (closure) {
      closure->set_status(Status::Corruption("Get child directory failed"));
    }
    return;
  }

  std::string local_path, target_path;
  std::string remote_path = g_pika_conf->classic_mode() ? table_name : table_name + "/" + std::to_string(partition_id);
  std::vector<std::string>::const_iterator iter = descendant.begin();
  slash::RsyncRemote remote(ip, port, kSnapshotSyncModule, g_pika_conf->db_sync_speed() * 1024);
  std::string secret_file_path = g_pika_conf->db_sync_path();
  if (g_pika_conf->db_sync_path().back() != '/') {
    secret_file_path += "/";
  }
  secret_file_path += slash::kRsyncSubDir + "/" + kPikaSecretFile;

  for (; iter != descendant.end(); ++iter) {
    local_path = bg_path + "/" + *iter;
    target_path = remote_path + "/" + *iter;

    if (*iter == kBgsaveInfoFile) {
      continue;
    }

    if (slash::IsDir(local_path) == 0 &&
        local_path.back() != '/') {
      local_path.push_back('/');
      target_path.push_back('/');
    }

    // We need specify the speed limit for every single file
    ret = slash::RsyncSendFile(local_path, target_path, secret_file_path, remote);
    if (0 != ret) {
      LOG(WARNING) << "Partition: " << partition->GetPartitionName()
                   << " RSync send file failed! From: " << *iter
                   << ", To: " << target_path
                   << ", At: " << ip << ":" << port
                   << ", Error: " << ret;
      break;
    }
  }
  // Clear target path
  slash::RsyncSendClearTarget(bg_path + "/strings", remote_path + "/strings", secret_file_path, remote);
  slash::RsyncSendClearTarget(bg_path + "/hashes", remote_path + "/hashes", secret_file_path, remote);
  slash::RsyncSendClearTarget(bg_path + "/lists", remote_path + "/lists", secret_file_path, remote);
  slash::RsyncSendClearTarget(bg_path + "/sets", remote_path + "/sets", secret_file_path, remote);
  slash::RsyncSendClearTarget(bg_path + "/zsets", remote_path + "/zsets", secret_file_path, remote);

  pink::PinkCli* cli = pink::NewRedisCli();
  std::string lip(host_);
  if (cli->Connect(ip, port, "").ok()) {
    struct sockaddr_in laddr;
    socklen_t llen = sizeof(laddr);
    getsockname(cli->fd(), (struct sockaddr*) &laddr, &llen);
    lip = inet_ntoa(laddr.sin_addr);
    cli->Close();
    delete cli;
  } else {
    LOG(WARNING) << "Rsync try connect slave rsync service error"
        << ", slave rsync service(" << ip << ":" << port << ")";
    delete cli;
  }

  // Send info file at last
  if (0 == ret) {
    // need to modify the IP addr in the info file
    if (lip.compare(host_)) {
      std::ofstream fix;
      std::string fn = bg_path + "/" + kBgsaveInfoFile + "." + std::to_string(time(NULL));
      fix.open(fn, std::ios::in | std::ios::trunc);
      if (fix.is_open()) {
        fix << "0s\n" << lip << "\n" << port_ << "\n" << binlog_filenum << "\n" << binlog_offset << "\n";
        if (g_pika_conf->replication_protocol_type() == ReplicationProtocolType::kClusterProtocol) {
          fix << term << "\n" << index << "\n";
        }
        fix.close();
      }
      ret = slash::RsyncSendFile(fn, remote_path + "/" + kBgsaveInfoFile, secret_file_path, remote);
      slash::DeleteFile(fn);
      if (ret != 0) {
        LOG(WARNING) << "Partition: " << partition->GetPartitionName() << " Send Modified Info File Failed";
      }
    } else if (0 != (ret = slash::RsyncSendFile(bg_path + "/" + kBgsaveInfoFile, remote_path + "/" + kBgsaveInfoFile, secret_file_path, remote))) {
      LOG(WARNING) << "Partition: " << partition->GetPartitionName() << " Send Info File Failed";
    }
  }
  // remove slave
  {
    std::string task_index =
      SnapshotSyncTaskIndex(ip, port, table_name, partition_id);
    slash::MutexLock ml(&db_sync_protector_);
    db_sync_slaves_.erase(task_index);
  }

  if (closure) {
    if (ret != 0) {
      closure->set_status(Status::Corruption("Rsync failed"));
    }
  }

  if (0 == ret) {
    LOG(INFO) << "Partition: " << partition->GetPartitionName() << " RSync Send Files Success";
  }
}

std::string PikaServer::SnapshotSyncTaskIndex(const std::string& ip, int port,
                                              const std::string& table_name,
                                              uint32_t partition_id) {
  char buf[256];
  snprintf(buf, sizeof(buf), "%s:%d_%s:%d",
      ip.data(), port, table_name.data(), partition_id);
  return buf;
}

void PikaServer::KeyScanTaskSchedule(pink::TaskFunc func, void* arg) {
  key_scan_thread_.StartThread();
  key_scan_thread_.Schedule(func, arg);
}

void PikaServer::ClientKillAll() {
  pika_dispatch_thread_->ClientKillAll();
  pika_monitor_thread_->ThreadClientKill();
}

int PikaServer::ClientKill(const std::string &ip_port) {
  if (pika_dispatch_thread_->ClientKill(ip_port)
    || pika_monitor_thread_->ThreadClientKill(ip_port)) {
    return 1;
  }
  return 0;
}

int64_t PikaServer::ClientList(std::vector<ClientInfo> *clients) {
  int64_t clients_num = 0;
  clients_num += pika_dispatch_thread_->ThreadClientList(clients);
  clients_num += pika_monitor_thread_->ThreadClientList(clients);
  return clients_num;
}

bool PikaServer::HasMonitorClients() {
  return pika_monitor_thread_->HasMonitorClients();
}

void PikaServer::AddMonitorMessage(const std::string& monitor_message) {
  pika_monitor_thread_->AddMonitorMessage(monitor_message);
}

void PikaServer::AddMonitorClient(std::shared_ptr<PikaClientConn> client_ptr) {
  pika_monitor_thread_->AddMonitorClient(client_ptr);
}

void PikaServer::SlowlogTrim() {
  pthread_rwlock_wrlock(&slowlog_protector_);
  while (slowlog_list_.size() > static_cast<uint32_t>(g_pika_conf->slowlog_max_len())) {
    slowlog_list_.pop_back();
  }
  pthread_rwlock_unlock(&slowlog_protector_);
}

void PikaServer::SlowlogReset() {
  pthread_rwlock_wrlock(&slowlog_protector_);
  slowlog_list_.clear();
  pthread_rwlock_unlock(&slowlog_protector_);
}

uint32_t PikaServer::SlowlogLen() {
  RWLock l(&slowlog_protector_, false);
  return slowlog_list_.size();
}

void PikaServer::SlowlogObtain(int64_t number, std::vector<SlowlogEntry>* slowlogs) {
  pthread_rwlock_rdlock(&slowlog_protector_);
  slowlogs->clear();
  std::list<SlowlogEntry>::const_iterator iter = slowlog_list_.begin();
  while (number-- && iter != slowlog_list_.end()) {
    slowlogs->push_back(*iter);
    iter++;
  }
  pthread_rwlock_unlock(&slowlog_protector_);
}

void PikaServer::SlowlogPushEntry(const PikaCmdArgsType& argv, int32_t time, int64_t duration) {
  SlowlogEntry entry;
  uint32_t slargc = (argv.size() < SLOWLOG_ENTRY_MAX_ARGC)
      ? argv.size() : SLOWLOG_ENTRY_MAX_ARGC;

  for (uint32_t idx = 0; idx < slargc; ++idx) {
    if (slargc != argv.size() && idx == slargc - 1) {
      char buffer[32];
      sprintf(buffer, "... (%lu more arguments)", argv.size() - slargc + 1);
      entry.argv.push_back(std::string(buffer));
    } else {
      if (argv[idx].size() > SLOWLOG_ENTRY_MAX_STRING) {
        char buffer[32];
        sprintf(buffer, "... (%lu more bytes)", argv[idx].size() - SLOWLOG_ENTRY_MAX_STRING);
        std::string suffix(buffer);
        std::string brief = argv[idx].substr(0, SLOWLOG_ENTRY_MAX_STRING);
        entry.argv.push_back(brief + suffix);
      } else {
        entry.argv.push_back(argv[idx]);
      }
    }
  }

  pthread_rwlock_wrlock(&slowlog_protector_);
  entry.id = slowlog_entry_id_++;
  entry.start_time = time;
  entry.duration = duration;
  slowlog_list_.push_front(entry);
  pthread_rwlock_unlock(&slowlog_protector_);

  SlowlogTrim();
}

void PikaServer::ResetStat() {
  statistic_.server_stat.accumulative_connections.store(0);
  statistic_.server_stat.qps.querynum.store(0);
  statistic_.server_stat.qps.last_querynum.store(0);
}

uint64_t PikaServer::ServerQueryNum() {
  return statistic_.server_stat.qps.querynum.load();
}

uint64_t PikaServer::ServerCurrentQps() {
  return statistic_.server_stat.qps.last_sec_querynum.load();
}

uint64_t PikaServer::accumulative_connections() {
  return statistic_.server_stat.accumulative_connections.load();
}

void PikaServer::incr_accumulative_connections() {
  ++(statistic_.server_stat.accumulative_connections);
}

// only one thread invoke this right now
void PikaServer::ResetLastSecQuerynum() {
  statistic_.server_stat.qps.ResetLastSecQuerynum();
  statistic_.ResetTableLastSecQuerynum();
}

void PikaServer::UpdateQueryNumAndExecCountTable(const std::string& table_name,
    const std::string& command, bool is_write) {
  std::string cmd(command);
  statistic_.server_stat.qps.querynum++;
  statistic_.server_stat.exec_count_table[slash::StringToUpper(cmd)]++;
  statistic_.UpdateTableQps(table_name, command, is_write);
}

std::unordered_map<std::string, uint64_t> PikaServer::ServerExecCountTable() {
  std::unordered_map<std::string, uint64_t> res;
  for (auto& cmd : statistic_.server_stat.exec_count_table) {
    res[cmd.first] = cmd.second.load();
  }
  return res;
}

QpsStatistic PikaServer::ServerTableStat(const std::string& table_name) {
  return statistic_.TableStat(table_name);
}

std::unordered_map<std::string, QpsStatistic> PikaServer::ServerAllTableStat() {
  return statistic_.AllTableStat();
}

int PikaServer::PubSubNumPat() {
  return pika_pubsub_thread_->PubSubNumPat();
}

int PikaServer::Publish(const std::string& channel, const std::string& msg) {
  int receivers = pika_pubsub_thread_->Publish(channel, msg);
  return receivers;
}

void PikaServer::EnablePublish(int fd) {
  pika_pubsub_thread_->UpdateConnReadyState(fd, pink::PubSubThread::ReadyState::kReady);
}

int PikaServer::UnSubscribe(std::shared_ptr<pink::PinkConn> conn,
                            const std::vector<std::string>& channels,
                            bool pattern,
                            std::vector<std::pair<std::string, int>>* result) {
  int subscribed = pika_pubsub_thread_->UnSubscribe(conn, channels, pattern, result);
  return subscribed;
}

void PikaServer::Subscribe(std::shared_ptr<pink::PinkConn> conn,
                           const std::vector<std::string>& channels,
                           bool pattern,
                           std::vector<std::pair<std::string, int>>* result) {
  pika_pubsub_thread_->Subscribe(conn, channels, pattern, result);
}

void PikaServer::PubSubChannels(const std::string& pattern,
                      std::vector<std::string >* result) {
  pika_pubsub_thread_->PubSubChannels(pattern, result);
}

void PikaServer::PubSubNumSub(const std::vector<std::string>& channels,
                    std::vector<std::pair<std::string, int>>* result) {
  pika_pubsub_thread_->PubSubNumSub(channels, result);
}

/******************************* PRIVATE *******************************/

void PikaServer::DoTimingTask() {
  // Maybe schedule compactrange
  AutoCompactRange();
  // Purge log
  AutoPurge();
  // Delete expired dump
  AutoDeleteExpiredDump();
  // Cheek Rsync Status
  AutoKeepAliveRSync();
  // Reset server qps
  ResetLastSecQuerynum();
}

void PikaServer::AutoCompactRange() {
  struct statfs disk_info;
  int ret = statfs(g_pika_conf->db_path().c_str(), &disk_info);
  if (ret == -1) {
    LOG(WARNING) << "statfs error: " << strerror(errno);
    return;
  }

  uint64_t total_size = disk_info.f_bsize * disk_info.f_blocks;
  uint64_t free_size = disk_info.f_bsize * disk_info.f_bfree;
  std::string ci = g_pika_conf->compact_interval();
  std::string cc = g_pika_conf->compact_cron();

  if (ci != "") {
    std::string::size_type slash = ci.find("/");
    int interval = std::atoi(ci.substr(0, slash).c_str());
    int usage = std::atoi(ci.substr(slash+1).c_str());
    struct timeval now;
    gettimeofday(&now, NULL);
    if (last_check_compact_time_.tv_sec == 0 ||
      now.tv_sec - last_check_compact_time_.tv_sec >= interval * 3600) {
      gettimeofday(&last_check_compact_time_, NULL);
      if (((double)free_size / total_size) * 100 >= usage) {
        Status s = DoSameThingSpecificTable(TaskType::kCompactAll);
        if (s.ok()) {
          LOG(INFO) << "[Interval]schedule compactRange, freesize: " << free_size/1048576 << "MB, disksize: " << total_size/1048576 << "MB";
        } else {
          LOG(INFO) << "[Interval]schedule compactRange Failed, freesize: " << free_size/1048576 << "MB, disksize: " << total_size/1048576
            << "MB, error: " << s.ToString();
        }
      } else {
        LOG(WARNING) << "compact-interval failed, because there is not enough disk space left, freesize"
          << free_size/1048576 << "MB, disksize: " << total_size/1048576 << "MB";
      }
    }
    return;
  }

  if (cc != "") {
    bool have_week = false;
    std::string compact_cron, week_str;
    int slash_num = count(cc.begin(), cc.end(), '/');
    if (slash_num == 2) {
      have_week = true;
      std::string::size_type first_slash = cc.find("/");
      week_str = cc.substr(0, first_slash);
      compact_cron = cc.substr(first_slash + 1);
    } else {
      compact_cron = cc;
    }

    std::string::size_type colon = compact_cron.find("-");
    std::string::size_type underline = compact_cron.find("/");
    int week = have_week ? (std::atoi(week_str.c_str()) % 7) : 0;
    int start = std::atoi(compact_cron.substr(0, colon).c_str());
    int end = std::atoi(compact_cron.substr(colon+1, underline).c_str());
    int usage = std::atoi(compact_cron.substr(underline+1).c_str());
    std::time_t t = std::time(nullptr);
    std::tm* t_m = std::localtime(&t);

    bool in_window = false;
    if (start < end && (t_m->tm_hour >= start && t_m->tm_hour < end)) {
      in_window = have_week ? (week == t_m->tm_wday) : true;
    } else if (start > end && ((t_m->tm_hour >= start && t_m->tm_hour < 24) ||
          (t_m->tm_hour >= 0 && t_m->tm_hour < end))) {
      in_window = have_week ? false : true;
    } else {
      have_scheduled_crontask_ = false;
    }

    if (!have_scheduled_crontask_ && in_window) {
      if (((double)free_size / total_size) * 100 >= usage) {
        Status s = DoSameThingEveryPartition(TaskType::kCompactAll);
        if (s.ok()) {
          LOG(INFO) << "[Cron]schedule compactRange, freesize: " << free_size/1048576 << "MB, disksize: " << total_size/1048576 << "MB";
        } else {
          LOG(INFO) << "[Cron]schedule compactRange Failed, freesize: " << free_size/1048576 << "MB, disksize: " << total_size/1048576
            << "MB, error: " << s.ToString();
        }
        have_scheduled_crontask_ = true;
      } else {
        LOG(WARNING) << "compact-cron failed, because there is not enough disk space left, freesize"
          << free_size/1048576 << "MB, disksize: " << total_size/1048576 << "MB";
      }
    }
  }
}

void PikaServer::AutoPurge() {
  DoSameThingEveryPartition(TaskType::kPurgeLog);
}

void PikaServer::AutoDeleteExpiredDump() {
  std::string db_sync_prefix = g_pika_conf->bgsave_prefix();
  std::string db_sync_path = g_pika_conf->bgsave_path();
  int expiry_days = g_pika_conf->expire_dump_days();
  std::vector<std::string> dump_dir;

  // Never expire
  if (expiry_days <= 0) {
    return;
  }

  // Dump is not exist
  if (!slash::FileExists(db_sync_path)) {
    return;
  }

  // Directory traversal
  if (slash::GetChildren(db_sync_path, dump_dir) != 0) {
    return;
  }
  // Handle dump directory
  for (size_t i = 0; i < dump_dir.size(); i++) {
    if (dump_dir[i].substr(0, db_sync_prefix.size()) != db_sync_prefix || dump_dir[i].size() != (db_sync_prefix.size() + 8)) {
      continue;
    }

    std::string str_date = dump_dir[i].substr(db_sync_prefix.size(), (dump_dir[i].size() - db_sync_prefix.size()));
    char *end = NULL;
    std::strtol(str_date.c_str(), &end, 10);
    if (*end != 0) {
      continue;
    }

    // Parse filename
    int dump_year = std::atoi(str_date.substr(0, 4).c_str());
    int dump_month = std::atoi(str_date.substr(4, 2).c_str());
    int dump_day = std::atoi(str_date.substr(6, 2).c_str());

    time_t t = time(NULL);
    struct tm *now = localtime(&t);
    int now_year = now->tm_year + 1900;
    int now_month = now->tm_mon + 1;
    int now_day = now->tm_mday;

    struct tm dump_time, now_time;

    dump_time.tm_year = dump_year;
    dump_time.tm_mon = dump_month;
    dump_time.tm_mday = dump_day;
    dump_time.tm_hour = 0;
    dump_time.tm_min = 0;
    dump_time.tm_sec = 0;

    now_time.tm_year = now_year;
    now_time.tm_mon = now_month;
    now_time.tm_mday = now_day;
    now_time.tm_hour = 0;
    now_time.tm_min = 0;
    now_time.tm_sec = 0;

    long dump_timestamp = mktime(&dump_time);
    long now_timestamp = mktime(&now_time);
    // How many days, 1 day = 86400s
    int interval_days = (now_timestamp - dump_timestamp) / 86400;

    if (interval_days >= expiry_days) {
      std::string dump_file = db_sync_path + dump_dir[i];
      if (CountSyncSlaves() == 0) {
        LOG(INFO) << "Not syncing, delete dump file: " << dump_file;
        slash::DeleteDirIfExist(dump_file);
      } else {
        LOG(INFO) << "Syncing, can not delete " << dump_file << " dump file";
      }
    }
  }
}

void PikaServer::AutoKeepAliveRSync() {
  if (!pika_rsync_service_->CheckRsyncAlive()) {
    LOG(WARNING) << "The Rsync service is down, Try to restart";
    pika_rsync_service_->StartRsync();
  }
}

void PikaServer::InitBlackwidowOptions() {
  slash::RWLock rwl(&bw_options_rw_, true);

  // For rocksdb::Options
  bw_options_.options.create_if_missing = true;
  bw_options_.options.keep_log_file_num = 10;
  bw_options_.options.max_manifest_file_size = 64 * 1024 * 1024;
  bw_options_.options.max_log_file_size = 512 * 1024 * 1024;

  bw_options_.options.write_buffer_size =
      g_pika_conf->write_buffer_size();
  bw_options_.options.arena_block_size =
      g_pika_conf->arena_block_size();
  bw_options_.options.write_buffer_manager.reset(
          new rocksdb::WriteBufferManager(g_pika_conf->max_write_buffer_size()));
  bw_options_.options.max_write_buffer_number =
      g_pika_conf->max_write_buffer_number();
  bw_options_.options.target_file_size_base =
      g_pika_conf->target_file_size_base();
  bw_options_.options.max_background_flushes =
      g_pika_conf->max_background_flushes();
  bw_options_.options.max_background_compactions =
      g_pika_conf->max_background_compactions();
  bw_options_.options.max_open_files =
      g_pika_conf->max_cache_files();
  bw_options_.options.max_bytes_for_level_multiplier =
      g_pika_conf->max_bytes_for_level_multiplier();
  bw_options_.options.optimize_filters_for_hits =
      g_pika_conf->optimize_filters_for_hits();
  bw_options_.options.level_compaction_dynamic_level_bytes =
      g_pika_conf->level_compaction_dynamic_level_bytes();


  if (g_pika_conf->compression() == "none") {
    bw_options_.options.compression =
        rocksdb::CompressionType::kNoCompression;
  } else if (g_pika_conf->compression() == "snappy") {
    bw_options_.options.compression =
        rocksdb::CompressionType::kSnappyCompression;
  } else if (g_pika_conf->compression() == "zlib") {
    bw_options_.options.compression =
        rocksdb::CompressionType::kZlibCompression;
  } else if (g_pika_conf->compression() == "lz4") {
    bw_options_.options.compression =
        rocksdb::CompressionType::kLZ4Compression;
  } else if (g_pika_conf->compression() == "zstd") {
    bw_options_.options.compression =
        rocksdb::CompressionType::kZSTD;
  }

  // For rocksdb::BlockBasedTableOptions
  bw_options_.table_options.block_size = g_pika_conf->block_size();
  bw_options_.table_options.cache_index_and_filter_blocks =
      g_pika_conf->cache_index_and_filter_blocks();
  bw_options_.block_cache_size = g_pika_conf->block_cache();
  bw_options_.share_block_cache = g_pika_conf->share_block_cache();

  if (bw_options_.block_cache_size == 0) {
    bw_options_.table_options.no_block_cache = true;
  } else if (bw_options_.share_block_cache) {
    bw_options_.table_options.block_cache =
      rocksdb::NewLRUCache(bw_options_.block_cache_size);
  }

  // For Blackwidow small compaction
  bw_options_.statistics_max_size = g_pika_conf->max_cache_statistic_keys();
  bw_options_.small_compaction_threshold =
      g_pika_conf->small_compaction_threshold();
}

blackwidow::Status PikaServer::RewriteBlackwidowOptions(const blackwidow::OptionType& option_type,
    const std::unordered_map<std::string, std::string>& options_map) {
  blackwidow::Status s;
  for (const auto& table_item : tables_) {
    slash::RWLock partition_rwl(&table_item.second->partitions_rw_, true);
    for (const auto& partition_item: table_item.second->partitions_) {
      partition_item.second->DbRWLockWriter();
      s = partition_item.second->db()->SetOptions(option_type, blackwidow::ALL_DB, options_map);
      partition_item.second->DbRWUnLock();
      if (!s.ok()) return s;
    }
  }
  slash::RWLock rwl(&bw_options_rw_, true);
  s = bw_options_.ResetOptions(option_type, options_map);
  return s;
}

void PikaServer::ServerStatus(std::string* info) {
  std::stringstream tmp_stream;
  size_t q_size = ClientProcessorThreadPoolCurQueueSize();
  tmp_stream << "Client Processor thread-pool queue size: " << q_size << "\r\n";
  info->append(tmp_stream.str());
}
