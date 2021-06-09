// Copyright (c) 2018-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_partition.h"

#include <fstream>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <google/protobuf/io//coded_stream.h>

#include "slash/include/mutex_impl.h"

#include "include/pika_server.h"
#include "include/pika_conf.h"

extern PikaConf* g_pika_conf;
extern PikaServer* g_pika_server;

std::string PartitionPath(const std::string& table_path,
                          uint32_t partition_id) {
  char buf[100];
  snprintf(buf, sizeof(buf), "%u/", partition_id);
  return table_path + buf;
}

std::string PartitionName(const std::string& table_name,
                          uint32_t partition_id) {
  char buf[256];
  snprintf(buf, sizeof(buf), "(%s:%u)", table_name.data(), partition_id);
  return std::string(buf);
}

std::string BgsaveSubPath(const std::string& table_name,
                          uint32_t partition_id) {
  char buf[256];
  std::string partition_id_str = std::to_string(partition_id);
  snprintf(buf, sizeof(buf), "%s/%s", table_name.data(), partition_id_str.data());
  return std::string(buf);
}

std::string DbSyncPath(const std::string& sync_path,
                       const std::string& table_name,
                       const uint32_t partition_id,
                       bool classic_mode) {
  char buf[256];
  std::string partition_id_str = std::to_string(partition_id);
  if (classic_mode) {
    snprintf(buf, sizeof(buf), "%s/", table_name.data());
  } else {
    snprintf(buf, sizeof(buf), "%s/%s/", table_name.data(), partition_id_str.data());
  }
  return sync_path + buf;
}

Partition::Partition(const std::string& table_name,
                     uint32_t partition_id,
                     const std::string& table_db_path,
                     const std::shared_ptr<blackwidow::BlackWidow>& state_db) :
  table_name_(table_name),
  partition_id_(partition_id),
  meta_storage_(nullptr),
  repl_status_(Status::OK()),
  leader_id_(PeerID()),
  bgsave_engine_(NULL) {
  db_path_ = g_pika_conf->classic_mode() ?
      table_db_path : PartitionPath(table_db_path, partition_id_);
  bgsave_sub_path_ = g_pika_conf->classic_mode() ?
      table_name : BgsaveSubPath(table_name_, partition_id_);
  dbsync_path_ = DbSyncPath(g_pika_conf->db_sync_path(), table_name_,
          partition_id_,  g_pika_conf->classic_mode());
  partition_name_ = g_pika_conf->classic_mode() ?
      table_name : PartitionName(table_name_, partition_id_);

  if (g_pika_conf->classic_mode()
      || g_pika_conf->replication_protocol_type() == ReplicationProtocolType::kClassicProtocol) {
    meta_storage_ = new MemoryBasedMetaStorage();
  } else {
    meta_storage_ = new DBBasedClusterMetaStorage(state_db, partition_name_);
  }

  pthread_rwlockattr_t attr;
  pthread_rwlockattr_init(&attr);
  pthread_rwlockattr_setkind_np(&attr,
          PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);

  pthread_rwlock_init(&rg_state_rwlock_, &attr);
  pthread_rwlock_init(&db_rwlock_, &attr);

  db_ = std::shared_ptr<blackwidow::BlackWidow>(new blackwidow::BlackWidow());
  rocksdb::Status s = db_->Open(g_pika_server->bw_options(), db_path_);

  lock_mgr_ = new slash::lock::LockMgr(1000, 0, std::make_shared<slash::lock::MutexFactoryImpl>());

  opened_ = s.ok() ? true : false;
  assert(db_);
  assert(s.ok());

  Start();

  LOG(INFO) << partition_name_ << " DB Success";
}

Partition::~Partition() {
  Close();
  delete bgsave_engine_;
  pthread_rwlock_destroy(&db_rwlock_);
  pthread_rwlock_destroy(&rg_state_rwlock_);
  delete lock_mgr_;
}

int Partition::Start() {
  // Create and start the underlying replication group.
  ReplicationGroupID group_id(table_name_, partition_id_);
  replication::RGNodeInitializeConfig init_config;
  init_config.meta_storage = meta_storage_;
  replication::GroupIDAndInitConfMap id_init_conf_map{{group_id, init_config}};
  Status s = g_pika_server->CreateReplicationGroupNodes(id_init_conf_map);
  if (!s.ok()) {
    LOG(FATAL) << partition_name_ << " create the underlying replication group failed: " << s.ToString();
  }
  s = g_pika_server->StartReplicationGroupNode(group_id);
  if (!s.ok()) {
    LOG(FATAL) << partition_name_ << " start the underlying replication group failed: " << s.ToString();
  }
  DLOG(INFO) << "Replication group: " << group_id.ToString() << " started!";
  return 0;
}

int Partition::Stop() {
  // Stop the underlying replication group.
  ReplicationGroupID group_id(table_name_, partition_id_);
  Status s = g_pika_server->StopReplicationGroupNode(group_id);
  if (!s.ok()) {
    LOG(ERROR) << partition_name_ << " stop the underlying replication group failed: " << s.ToString();
    return -1;
  }
  return 0;
}

void Partition::Leave() {
  Close();
  MoveToTrash();
}

void Partition::Close() {
  Stop();
  if (meta_storage_ != nullptr) {
    delete meta_storage_;
    meta_storage_ = nullptr;
  }
  if (!opened_) {
    return;
  }
  slash::RWLock rwl(&db_rwlock_, true);
  db_.reset();
  opened_ = false;
}

// Before call this function, should
// close db and log first
void Partition::MoveToTrash() {
  if (opened_) {
    return;
  }

  std::string dbpath = db_path_;
  if (dbpath[dbpath.length() - 1] == '/') {
    dbpath.erase(dbpath.length() - 1);
  }
  dbpath.append("_deleting/");
  if (slash::RenameFile(db_path_, dbpath.c_str())) {
    LOG(WARNING) << "Failed to move db to trash, error: " << strerror(errno);
    return;
  }
  g_pika_server->PurgeDir(dbpath);

  LOG(WARNING) << "Partition DB: " << partition_name_ << " move to trash success";
}

std::string Partition::GetTableName() const {
  return table_name_;
}

uint32_t Partition::GetPartitionId() const {
  return partition_id_;
}

std::string Partition::GetPartitionName() const {
  return partition_name_;
}

std::shared_ptr<blackwidow::BlackWidow> Partition::db() const {
  return db_;
}

void Partition::Compact(const blackwidow::DataType& type) {
  if (!opened_) return;
  db_->Compact(type);
}

void Partition::DbRWLockWriter() {
  pthread_rwlock_wrlock(&db_rwlock_);
}

void Partition::DbRWLockReader() {
  pthread_rwlock_rdlock(&db_rwlock_);
}

void Partition::DbRWUnLock() {
  pthread_rwlock_unlock(&db_rwlock_);
}

slash::lock::LockMgr* Partition::LockMgr() {
  return lock_mgr_;
}

Status Partition::PrepareRsync() {
  if (!slash::DeleteDirIfExist(dbsync_path_)) {
    LOG(WARNING) << "Partition: " << partition_name_
                 << ", fail to delete dbsync_path " << dbsync_path_;
    return Status::Corruption("delete dbsync_path");
  }
  if (0 != slash::CreatePath(dbsync_path_ + "strings")
      || 0 != slash::CreatePath(dbsync_path_ + "hashes")
      || 0 != slash::CreatePath(dbsync_path_ + "lists")
      || 0 != slash::CreatePath(dbsync_path_ + "sets")
      || 0 != slash::CreatePath(dbsync_path_ + "zsets")) {
    LOG(WARNING) << "Partition: " << partition_name_
                 << ", fail to create dbsync_path " << dbsync_path_;
    return Status::Corruption("create dbsync_path");
  }
  return Status::OK();
}

// Try to update master offset
// This may happend when dbsync from master finished
// Here we do:
// 1, Check dbsync finished, got the new binlog offset
// 2, Replace the old db
// 3, Update master offset, and the PikaAuxiliaryThread cron will connect and do slaveof task with master
Status Partition::TryUpdateMasterOffset(const std::shared_ptr<ReplicationGroupNode>& node,
                                        LogOffset* log_offset) {
  std::string info_path = dbsync_path_ + kBgsaveInfoFile;
  if (!slash::FileExists(info_path)) {
    return Status::Incomplete("db info file " + info_path + " does not exist");
  }
  // Got new binlog offset
  std::ifstream is(info_path);
  if (!is) {
    LOG(WARNING) << "Partition: " << partition_name_
                 << ", Failed to open info file after db sync";
    return Status::Corruption("failed to open info file");
  }
  std::string line, master_ip;
  int lineno = 0;
  int64_t filenum = 0, offset = 0, term = 0, index = 0, tmp = 0, master_port = 0;
  while (std::getline(is, line)) {
    lineno++;
    if (lineno == 2) {
      master_ip = line;
    } else if (lineno > 2 && lineno < 8) {
      if (!slash::string2l(line.data(), line.size(), &tmp) || tmp < 0) {
        LOG(WARNING) << "Partition: " << partition_name_
                     << ", Format of info file after db sync error, line : " << line;
        is.close();
        return Status::Corruption("format of info file error");
      }
      if (lineno == 3) {
        master_port = tmp;
      } else if (lineno == 4) {
        filenum = tmp;
      } else if (lineno == 5) {
        offset = tmp;
      } else if (lineno == 6) {
        term = tmp;
      } else if (lineno == 7) {
        index = tmp;
      }
    } else if (lineno > 8) {
      LOG(WARNING) << "Partition: " << partition_name_
                   << ", Format of info file after db sync error, line : " << line;
      is.close();
      return Status::Corruption("format of info file error");
    }
  }
  is.close();
  LOG(INFO) << "Partition: " << partition_name_
            << " Information from dbsync info"
            << ", master_ip: " << master_ip
            << ", master_port: " << master_port
            << ", filenum: " << filenum
            << ", offset: " << offset
            << ", term: " << term
            << ", index: " << index;

  PeerID peer_id(master_ip, master_port);
  auto v_state = node->NodeVolatileState();
  // Sanity check
  if (peer_id != v_state.leader_id) {
    LOG(WARNING) << "Partition: " << partition_name_
                 << ", error master node ip port: " << peer_id.ToString()
                 << ", expect ip port: " << v_state.leader_id.ToString();
    return Status::Corruption("unexpected master node");
  }
  slash::DeleteFile(info_path);
  if (!ChangeDb(dbsync_path_)) {
    LOG(WARNING) << "Partition: " << partition_name_
                 << ", Failed to change db";
    return Status::Corruption("failed to change db");
  }
  *log_offset = LogOffset(BinlogOffset(filenum, offset), LogicOffset(term, index));
  return Status::OK();
}

/*
 * Change a new db locate in new_path
 * return true when change success
 * db remain the old one if return false
 */
bool Partition::ChangeDb(const std::string& new_path) {
  std::string tmp_path(db_path_);
  if (tmp_path.back() == '/') {
    tmp_path.resize(tmp_path.size() - 1);
  }
  tmp_path += "_bak";
  slash::DeleteDirIfExist(tmp_path);

  RWLock l(&db_rwlock_, true);
  LOG(INFO) << "Partition: "<< partition_name_
      << ", Prepare change db from: " << tmp_path;
  db_.reset();

  if (0 != slash::RenameFile(db_path_.c_str(), tmp_path)) {
    LOG(WARNING) << "Partition: " << partition_name_
        << ", Failed to rename db path when change db, error: " << strerror(errno);
    return false;
  }

  if (0 != slash::RenameFile(new_path.c_str(), db_path_.c_str())) {
    LOG(WARNING) << "Partition: " << partition_name_
        << ", Failed to rename new db path when change db, error: " << strerror(errno);
    return false;
  }

  db_.reset(new blackwidow::BlackWidow());
  rocksdb::Status s = db_->Open(g_pika_server->bw_options(), db_path_);
  assert(db_);
  assert(s.ok());
  slash::DeleteDirIfExist(tmp_path);
  LOG(INFO) << "Partition: " << partition_name_ << ", Change db success";
  return true;
}

bool Partition::IsBgSaving() {
  slash::MutexLock ml(&bgsave_protector_);
  return bgsave_info_.bgsaving;
}

void Partition::BgSavePartition() {
  slash::MutexLock l(&bgsave_protector_);
  if (bgsave_info_.bgsaving) {
    return;
  }
  bgsave_info_.bgsaving = true;
  BgTaskArg* bg_task_arg = new BgTaskArg();
  bg_task_arg->partition = shared_from_this();
  g_pika_server->BGSaveTaskSchedule(&DoBgSave, static_cast<void*>(bg_task_arg));
}

BgSaveInfo Partition::bgsave_info() {
  slash::MutexLock l(&bgsave_protector_);
  return bgsave_info_;
}

void Partition::DoBgSave(void* arg) {
  BgTaskArg* bg_task_arg = static_cast<BgTaskArg*>(arg);

  // Do BgSave
  bool success = bg_task_arg->partition->RunBgsaveEngine();

  // Some output
  BgSaveInfo info = bg_task_arg->partition->bgsave_info();
  std::ofstream out;
  out.open(info.path + "/" + kBgsaveInfoFile, std::ios::in | std::ios::trunc);
  if (out.is_open()) {
    out << (time(NULL) - info.start_time) << "s\n"
        << g_pika_server->host() << "\n"
        << g_pika_server->port() << "\n"
        << info.offset.b_offset.filenum << "\n"
        << info.offset.b_offset.offset << "\n";
    if (g_pika_conf->replication_protocol_type() == ReplicationProtocolType::kClusterProtocol) {
      out << info.offset.l_offset.term << "\n"
          << info.offset.l_offset.index << "\n";
    }
    out.close();
  }
  if (!success) {
    std::string fail_path = info.path + "_FAILED";
    slash::RenameFile(info.path.c_str(), fail_path.c_str());
  }
  bg_task_arg->partition->FinishBgsave();

  delete bg_task_arg;
}

bool Partition::RunBgsaveEngine() {
  // Prepare for Bgsaving
  if (!InitBgsaveEnv() || !InitBgsaveEngine()) {
    ClearBgsave();
    return false;
  }
  LOG(INFO) << partition_name_ << " after prepare bgsave";

  BgSaveInfo info = bgsave_info();
  LOG(INFO) << partition_name_ << " bgsave_info: path=" << info.path
            << ",  filenum=" << info.offset.b_offset.filenum
            << ", offset=" << info.offset.b_offset.offset;

  // Backup to tmp dir
  rocksdb::Status s = bgsave_engine_->CreateNewBackup(info.path);
  LOG(INFO) << partition_name_ << " create new backup finished.";

  if (!s.ok()) {
    LOG(WARNING) << partition_name_ << " create new backup failed :" << s.ToString();
    return false;
  }
  return true;
}

// Prepare engine, need bgsave_protector protect
bool Partition::InitBgsaveEnv() {
  slash::MutexLock l(&bgsave_protector_);
  // Prepare for bgsave dir
  bgsave_info_.start_time = time(NULL);
  char s_time[32];
  int len = strftime(s_time, sizeof(s_time), "%Y%m%d%H%M%S", localtime(&bgsave_info_.start_time));
  bgsave_info_.s_start_time.assign(s_time, len);
  std::string time_sub_path = g_pika_conf->bgsave_prefix() + std::string(s_time, 8);
  bgsave_info_.path = g_pika_conf->bgsave_path() + time_sub_path + "/" + bgsave_sub_path_;
  if (!slash::DeleteDirIfExist(bgsave_info_.path)) {
    LOG(WARNING) << partition_name_ << " remove exist bgsave dir failed";
    return false;
  }
  slash::CreatePath(bgsave_info_.path, 0755);
  // Prepare for failed dir
  if (!slash::DeleteDirIfExist(bgsave_info_.path + "_FAILED")) {
    LOG(WARNING) << partition_name_ << " remove exist fail bgsave dir failed :";
    return false;
  }
  return true;
}

// Prepare bgsave env, need bgsave_protector protect
bool Partition::InitBgsaveEngine() {
  delete bgsave_engine_;
  rocksdb::Status s = blackwidow::BackupEngine::Open(db().get(), &bgsave_engine_);
  if (!s.ok()) {
    LOG(WARNING) << partition_name_ << " open backup engine failed " << s.ToString();
    return false;
  }

  std::shared_ptr<ReplicationGroupNode> node = g_pika_server->GetReplicationGroupNode(table_name_, partition_id_);
  if (!node) {
    LOG(WARNING) << partition_name_ << " not found";
    return false;
  }

  {
    RWLock l(&db_rwlock_, true);
    {
      slash::MutexLock l(&bgsave_protector_);
      meta_storage_->MetaSnapshot(&bgsave_info_.offset, &bgsave_info_.conf_state);
    }
    s = bgsave_engine_->SetBackupContent();
    if (!s.ok()) {
      LOG(WARNING) << partition_name_ << " set backup content failed " << s.ToString();
      return false;
    }
  }
  return true;
}

void Partition::ClearBgsave() {
  slash::MutexLock l(&bgsave_protector_);
  bgsave_info_.Clear();
}

void Partition::FinishBgsave() {
  slash::MutexLock l(&bgsave_protector_);
  bgsave_info_.bgsaving = false;
}

bool Partition::FlushDB() {
  slash::RWLock rwl(&db_rwlock_, true);
  slash::MutexLock ml(&bgsave_protector_);
  if (bgsave_info_.bgsaving) {
    return false;
  }

  LOG(INFO) << partition_name_ << " Delete old db...";
  db_.reset();

  std::string dbpath = db_path_;
  if (dbpath[dbpath.length() - 1] == '/') {
    dbpath.erase(dbpath.length() - 1);
  }
  dbpath.append("_deleting/");
  slash::RenameFile(db_path_, dbpath.c_str());

  db_ = std::shared_ptr<blackwidow::BlackWidow>(new blackwidow::BlackWidow());
  rocksdb::Status s = db_->Open(g_pika_server->bw_options(), db_path_);
  assert(db_);
  assert(s.ok());
  LOG(INFO) << partition_name_ << " Open new db success";
  g_pika_server->PurgeDir(dbpath);
  return true;
}

bool Partition::FlushSubDB(const std::string& db_name) {
  slash::RWLock rwl(&db_rwlock_, true);
  slash::MutexLock ml(&bgsave_protector_);
  if (bgsave_info_.bgsaving) {
    return false;
  }

  LOG(INFO) << partition_name_ << " Delete old " + db_name + " db...";
  db_.reset();

  std::string dbpath = db_path_;
  if (dbpath[dbpath.length() - 1] != '/') {
    dbpath.append("/");
  }

  std::string sub_dbpath = dbpath + db_name;
  std::string del_dbpath = dbpath + db_name + "_deleting";
  slash::RenameFile(sub_dbpath, del_dbpath);

  db_ = std::shared_ptr<blackwidow::BlackWidow>(new blackwidow::BlackWidow());
  rocksdb::Status s = db_->Open(g_pika_server->bw_options(), db_path_);
  assert(db_);
  assert(s.ok());
  LOG(INFO) << partition_name_ << " open new " + db_name + " db success";
  g_pika_server->PurgeDir(del_dbpath);
  return true;
}

void Partition::InitKeyScan() {
  key_scan_info_.start_time = time(NULL);
  char s_time[32];
  int len = strftime(s_time, sizeof(s_time), "%Y-%m-%d %H:%M:%S", localtime(&key_scan_info_.start_time));
  key_scan_info_.s_start_time.assign(s_time, len);
  key_scan_info_.duration = -1;       // duration -1 mean the task in processing
}

KeyScanInfo Partition::GetKeyScanInfo() {
  slash::MutexLock l(&key_info_protector_);
  return key_scan_info_;
}

Status Partition::GetKeyNum(std::vector<blackwidow::KeyInfo>* key_info) {
  slash::MutexLock l(&key_info_protector_);
  if (key_scan_info_.key_scaning_) {
    *key_info = key_scan_info_.key_infos;
    return Status::OK();
  }
  InitKeyScan();
  key_scan_info_.key_scaning_ = true;
  key_scan_info_.duration = -2;   // duration -2 mean the task in waiting status,
                                  // has not been scheduled for exec
  rocksdb::Status s = db_->GetKeyNum(key_info);
  if (!s.ok()) {
    return Status::Corruption(s.ToString());
  }
  key_scan_info_.key_infos = *key_info;
  key_scan_info_.duration = time(NULL) - key_scan_info_.start_time;
  key_scan_info_.key_scaning_ = false;
  return Status::OK();
}

void Partition::set_repl_status(const Status& status) {
  slash::RWLock l(&rg_state_rwlock_, true);
  repl_status_ = status;
}

Status Partition::repl_status() {
  slash::RWLock l(&rg_state_rwlock_, false);
  return repl_status_;
}

void Partition::set_leader_id(const PeerID& leader_id) {
  slash::RWLock l(&rg_state_rwlock_, true);
  leader_id_ = leader_id;
}

PeerID Partition::leader_id() {
  slash::RWLock l(&rg_state_rwlock_, false);
  return leader_id_;
}


Status Partition::ApplyNormalEntry(const LogOffset& offset) {
  return meta_storage_->set_applied_offset(offset);
}

Status Partition::ApplyConfChange(const InnerMessage::ConfChange& change, const LogOffset& offset) {
  using ChangeResult = replication::Configuration::ChangeResult;
  ChangeResult change_result;
  PeerID node_id(change.node_id().ip(), change.node_id().port());
  Configuration conf;
  Status s = meta_storage_->configuration(&conf);
  if (!s.ok()) {
    return s;
  }
  switch (change.type()) {
    case InnerMessage::ConfChangeType::kAddVoter: {
      change_result = conf.AddPeer(node_id, replication::PeerRole::kRoleVoter);
      break;
    }
    case InnerMessage::ConfChangeType::kAddLearner: {
      change_result = conf.AddPeer(node_id, replication::PeerRole::kRoleLearner);
      break;
    }
    case InnerMessage::ConfChangeType::kRemoveNode: {
      change_result = conf.RemovePeer(node_id);
      break;
    }
    case InnerMessage::ConfChangeType::kPromoteLearner: {
      change_result = conf.PromotePeer(node_id);
      break;
    }
  }
  if (!change_result.IsOK()) {
    return Status::Corruption(change_result.ToString());
  }
  // persistent to storage
  return meta_storage_->ApplyConfiguration(conf, offset);
}

MemoryBasedMetaStorage::MemoryBasedMetaStorage() {
  pthread_rwlock_init(&rwlock_, NULL);
}

MemoryBasedMetaStorage::~MemoryBasedMetaStorage() {
  pthread_rwlock_destroy(&rwlock_);
}

Status MemoryBasedMetaStorage::applied_offset(LogOffset* offset) {
  slash::RWLock lk(&rwlock_, false);
  *offset = applied_offset_;
  return Status::OK();
}

Status MemoryBasedMetaStorage::set_applied_offset(const LogOffset& offset) {
  slash::RWLock lk(&rwlock_, true);
  if (offset.l_offset.index > applied_offset_.l_offset.index) {
    applied_offset_ = offset;
  }
  return Status::OK();
}

Status MemoryBasedMetaStorage::snapshot_offset(LogOffset* offset) {
  slash::RWLock lk(&rwlock_, false);
  *offset = snapshot_offset_;
  return Status::OK();
}

Status MemoryBasedMetaStorage::set_snapshot_offset(const LogOffset& offset) {
  slash::RWLock lk(&rwlock_, true);
  if (offset.l_offset.index > snapshot_offset_.l_offset.index) {
    snapshot_offset_ = offset;
  }
  return Status::OK();
}

Status MemoryBasedMetaStorage::configuration(Configuration* conf) {
  slash::RWLock lk(&rwlock_, false);
  *conf = configuration_;
  return Status::OK();
}

Status MemoryBasedMetaStorage::set_configuration(const Configuration& conf) {
  slash::RWLock lk(&rwlock_, true);
  configuration_ = conf;
  return Status::OK();
}

Status MemoryBasedMetaStorage::MetaSnapshot(LogOffset* applied_offset, Configuration* conf) {
  slash::RWLock lk(&rwlock_, false);
  *applied_offset = applied_offset_;
  *conf = configuration_;
  return Status::OK();
}

Status MemoryBasedMetaStorage::ApplyConfiguration(const Configuration& conf, const LogOffset& offset) {
  return Status::NotSupported("ApplyConfiguration");
}

Status MemoryBasedMetaStorage::ResetOffset(const LogOffset& snapshot_offset,
                                           const LogOffset& applied_offset) {
  slash::RWLock lk(&rwlock_, true);
  snapshot_offset_ = snapshot_offset;
  applied_offset_ = applied_offset;
  return Status::OK();
}

Status MemoryBasedMetaStorage::ResetOffsetAndConfiguration(const LogOffset& snapshot_offset,
                                                           const LogOffset& applied_offset,
                                                           const Configuration& configuration) {
  slash::RWLock lk(&rwlock_, true);
  configuration_ = configuration;
  snapshot_offset_ = snapshot_offset;
  applied_offset_ = applied_offset;
  return Status::OK();
}

DBBasedClusterMetaStorage::DBBasedClusterMetaStorage(const std::shared_ptr<blackwidow::BlackWidow>& db,
                                                         const std::string& partition_name)
  : partition_name_(partition_name),
  db_(db) {
  pthread_rwlock_init(&rwlock_, NULL);
  Initialize();
}

DBBasedClusterMetaStorage::~DBBasedClusterMetaStorage() {
  pthread_rwlock_unlock(&rwlock_);
}

void DBBasedClusterMetaStorage::Initialize() {
  // Reload meta info from persistent storage.
  // 1. Recover configuration
  slash::RWLock lk(&rwlock_, true);
  std::string key = MakeConfStateKey();
  std::string value;
  db_->Get(key, &value);
  InnerMessage::ConfState conf_state;
  ::google::protobuf::io::ArrayInputStream input(value.c_str(), value.size());
  ::google::protobuf::io::CodedInputStream decoder(&input);
  bool success = conf_state.ParseFromCodedStream(&decoder) && decoder.ConsumedEntireMessage();
  if (!success) {
    LOG(FATAL) << partition_name_ << " parse ConfState from db failed.";
  }
  configuration_.ParseFrom(conf_state);

  // 2. Recover term
  key = MakeTermKey();
  value.clear();
  db_->Get(key, &value);
  term_ = std::strtoull(value.c_str(), NULL, 10);

  // 3. Recover voted_for
  key = MakeVotedForKey();
  value.clear();
  db_->Get(key, &value);
  if (!voted_for_.ParseFromIpPort(value)) {
    LOG(FATAL) << partition_name_ << " parse VotedFor failed, value " << value;
  }
}

Status DBBasedClusterMetaStorage::applied_offset(LogOffset* offset) {
  slash::RWLock lk(&rwlock_, false);
  *offset = applied_offset_;
  return Status::OK();
}

Status DBBasedClusterMetaStorage::set_applied_offset(const LogOffset& offset) {
  slash::RWLock lk(&rwlock_, true);
  if (offset.l_offset.index > applied_offset_.l_offset.index) {
    applied_offset_ = offset;
  }
  // TODO(LIBA-S): persist it
  return Status::OK();
}

Status DBBasedClusterMetaStorage::snapshot_offset(LogOffset* offset) {
  slash::RWLock lk(&rwlock_, false);
  *offset = snapshot_offset_;
  return Status::OK();
}

Status DBBasedClusterMetaStorage::set_snapshot_offset(const LogOffset& offset) {
  slash::RWLock lk(&rwlock_, true);
  if (offset.l_offset.index > snapshot_offset_.l_offset.index) {
    snapshot_offset_ = offset;
  }
  // TODO(LIBA-S): persist it
  return Status::OK();
}

Status DBBasedClusterMetaStorage::configuration(Configuration* conf) {
  slash::RWLock lk(&rwlock_, false);
  *conf = configuration_;
  return Status::OK();
}

Status DBBasedClusterMetaStorage::set_configuration(const Configuration& conf) {
  slash::RWLock lk(&rwlock_, true);
  Status s = unsafe_set_configuration(conf);
  if (!s.ok()) {
    return s;
  }
  return Status::OK();
}

Status DBBasedClusterMetaStorage::unsafe_set_configuration(const Configuration& conf) {
  // persistent to storage
  InnerMessage::ConfState state = conf.ConfState();
  std::string to_store;
  if (!state.SerializeToString(&to_store)) {
    LOG(FATAL) << partition_name_ << " serialize ConfChange Failed";
  }
  std::string key = MakeConfStateKey();
  blackwidow::Status s = db_->Set(key, to_store);
  if (!s.ok()) {
    LOG(FATAL) << partition_name_ << " set ConfChange to db Failed " << s.ToString();
  }
  configuration_ = conf;
  return Status::OK();
}

Status DBBasedClusterMetaStorage::ApplyConfiguration(const Configuration& conf, const LogOffset& offset) {
  slash::RWLock lk(&rwlock_, true);
  Status s = unsafe_set_configuration(conf);
  if (!s.ok()) {
    return s;
  }
  if (offset.l_offset.index > applied_offset_.l_offset.index) {
    applied_offset_ = offset;
  }
  return Status::OK();
}

Status DBBasedClusterMetaStorage::SetTermAndVotedFor(const PeerID& voted_for,
                                                     uint32_t term) {
  slash::RWLock lk(&rwlock_, true);
  std::string term_key = MakeTermKey();
  blackwidow::Status s = db_->Set(term_key, std::to_string(term));
  if (!s.ok()) {
    LOG(WARNING) << partition_name_ << " set Term " << term
                 << " to db Failed " << s.ToString();
    return Status::Corruption("set db failed");
  }
  term_ = term;

  std::string voted_for_key = MakeVotedForKey();
  s = db_->Set(voted_for_key, voted_for.ToString());
  if (!s.ok()) {
    LOG(WARNING) << partition_name_ << " set VotedFor " << voted_for.ToString()
                 << " to db Failed " << s.ToString();
    return Status::Corruption("set db failed");
  }
  voted_for_ = voted_for;
  return Status::OK();
}

Status DBBasedClusterMetaStorage::GetTermAndVotedFor(uint32_t* term,
                                              PeerID* voted_for) {
  slash::RWLock lk(&rwlock_, false);
  *term = term_;
  *voted_for = voted_for_;
  return Status::OK();
}

Status DBBasedClusterMetaStorage::set_term(uint32_t term) {
  slash::RWLock lk(&rwlock_, true);
  std::string term_key = MakeTermKey();
  blackwidow::Status s = db_->Set(term_key, std::to_string(term));
  if (!s.ok()) {
    LOG(WARNING) << partition_name_ << " set Term " << term
                 << " to db Failed " << s.ToString();
    return Status::Corruption("set db failed");
  }
  term_ = term;
  return Status::OK();
}

Status DBBasedClusterMetaStorage::term(uint32_t* term) {
  slash::RWLock lk(&rwlock_, false);
  *term = term_;
  return Status::OK();
}

Status DBBasedClusterMetaStorage::set_voted_for(const PeerID& voted_for) {
  slash::RWLock lk(&rwlock_, true);
  std::string voted_for_key = MakeVotedForKey();
  blackwidow::Status s = db_->Set(voted_for_key, voted_for.ToString());
  if (!s.ok()) {
    LOG(WARNING) << partition_name_ << " set VotedFor " << voted_for.ToString()
                 << " to db Failed " << s.ToString();
    return Status::Corruption("set db failed");
  }
  voted_for_ = voted_for;
  return Status::OK();
}

Status DBBasedClusterMetaStorage::voted_for(PeerID* peer_id) {
  slash::RWLock lk(&rwlock_, false);
  *peer_id = voted_for_;
  return Status::OK();
}

Status DBBasedClusterMetaStorage::ResetOffset(const LogOffset& snapshot_offset,
                                              const LogOffset& applied_offset) {
  slash::RWLock lk(&rwlock_, true);
  snapshot_offset_ = snapshot_offset;
  applied_offset_ = applied_offset;
  return Status::OK();
}

Status DBBasedClusterMetaStorage::ResetOffsetAndConfiguration(const LogOffset& snapshot_offset,
                                                              const LogOffset& applied_offset,
                                                              const Configuration& configuration) {
  slash::RWLock lk(&rwlock_, true);
  Status s = unsafe_set_configuration(configuration);
  if (!s.ok()) {
    return s;
  }
  snapshot_offset_ = snapshot_offset;
  applied_offset_ = applied_offset;
  return Status::OK();
}

Status DBBasedClusterMetaStorage::MetaSnapshot(LogOffset* applied_offset, Configuration* conf) {
  slash::RWLock lk(&rwlock_, false);
  *applied_offset = applied_offset_;
  *conf = configuration_;
  return Status::OK();
}

std::string DBBasedClusterMetaStorage::MakeConfStateKey() {
  return std::move(std::string(kConfStateKeyPrefix + kKeyGlue + partition_name_));
}

std::string DBBasedClusterMetaStorage::MakeTermKey() {
  return std::move(std::string(kTermKeyPrefix + kKeyGlue + partition_name_));
}

std::string DBBasedClusterMetaStorage::MakeVotedForKey() {
  return std::move(std::string(kVotedForKeyPrefix + kKeyGlue + partition_name_));
}
