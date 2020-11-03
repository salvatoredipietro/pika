// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_DEFINE_H_
#define PIKA_DEFINE_H_

#include <glog/logging.h>
#include <string>
#include <functional>
#include <set>

#include "slash/include/slash_string.h"
#include "pink/include/redis_cli.h"

#include "include/util/callbacks.h"

#define PIKA_SYNC_BUFFER_SIZE           1000
#define PIKA_MAX_WORKER_THREAD_NUM      24
#define PIKA_SCAN_STEP_LENGTH           1000
#define PIKA_MAX_CONN_RBUF              (1 << 28) // 256MB
#define PIKA_MAX_CONN_RBUF_LB           (1 << 26) // 64MB
#define PIKA_MAX_CONN_RBUF_HB           (1 << 29) // 512MB
#define PIKA_SERVER_ID_MAX              65535

#define DISALLOW_COPY(TypeName)                 \
  TypeName(const TypeName&) = delete;           \
  TypeName& operator=(const TypeName&) = delete

struct TableStruct {
  TableStruct(const std::string& tn,
              const uint32_t pn,
              const std::set<uint32_t>& pi)
      : table_name(tn), partition_num(pn), partition_ids(pi) {}

  bool operator == (const TableStruct& table_struct) const {
    return table_name == table_struct.table_name
        && partition_num == table_struct.partition_num
        && partition_ids == table_struct.partition_ids;
  }
  std::string table_name;
  uint32_t partition_num;
  std::set<uint32_t> partition_ids;
};

/* Port shift */
const int kPortShiftRSync      = 1000;

const std::string kPikaPidFile = "pika.pid";
const std::string kPikaSecretFile = "rsync.secret";
const std::string kDefaultRsyncAuth = "default";

struct WorkerCronTask {
  int task;
  std::string ip_port;
};
typedef WorkerCronTask MonitorCronTask;
//task define
#define TASK_KILL 0
#define TASK_KILLALL 1

enum SlotState {
  INFREE = 0,
  INBUSY = 1,
};

//slowlog define
#define SLOWLOG_ENTRY_MAX_ARGC 32
#define SLOWLOG_ENTRY_MAX_STRING 128

//slowlog entry
struct SlowlogEntry {
  int64_t id;
  int64_t start_time;
  int64_t duration;
  pink::RedisCmdArgsType argv;
};

#define PIKA_MIN_RESERVED_FDS 5000

/*
 * the size of memory when we use memory mode
 * the default memory size is 2GB
 */
const int64_t kPoolSize = 1073741824;


const std::string kPikaMeta = "meta";
const std::string kContext  = "context";

/*
 * define common character
 *
 */
#define COMMA ','

/*
 * define reply between master and slave
 *
 */
const std::string kInnerReplOk = "ok";
const std::string kInnerReplWait = "wait";

const unsigned int kMaxBitOpInputKey = 12800;
const int kMaxBitOpInputBit = 21;
/*
 * db/snapshot sync
 */
const uint32_t kSnapshotSyncMaxGap = 50;
const std::string kSnapshotSyncModule = "document";

const std::string kBgsaveInfoFile = "info";

const std::string kBinlogPrefix = "write2file";
const size_t kBinlogPrefixLen = 10;
const std::string kManifest = "manifest";

/* replication Log offset*/

struct LogicOffset {
  uint32_t term;
  uint64_t index;
  LogicOffset()
    : term(0), index(0) {}
  LogicOffset(uint32_t _term, uint64_t _index)
    : term(_term), index(_index) {}
  LogicOffset(const LogicOffset& other) {
    term = other.term;
    index = other.index;
  }
  void Clear() noexcept {
    term = 0;
    index = 0;
  }
  bool Empty() const {
    return term == 0 && index == 0;
  }
  bool operator==(const LogicOffset& other) const {
    return term == other.term && index == other.index;
  }
  bool operator!=(const LogicOffset& other) const {
    return term != other.term || index != other.index;
  }
  bool operator<(const LogicOffset& other) const {
    return (term < other.term || (term == other.term && index < other.index));
  }
  bool operator>(const LogicOffset& other) const {
    return !(*this < other);
  }
  bool operator<=(const LogicOffset& other) const {
    return *this < other || *this == other;
  }

  std::string ToString() const {
    return "term: " + std::to_string(term) + " index: " + std::to_string(index);
  }
};

struct BinlogOffset {
  uint32_t filenum;
  uint64_t offset;
  BinlogOffset()
      : filenum(0), offset(0) {}
  BinlogOffset(uint32_t num, uint64_t off)
      : filenum(num), offset(off) {}
  BinlogOffset(const BinlogOffset& other) {
    filenum = other.filenum;
    offset = other.offset;
  }
  std::string ToString() const {
    return "filenum: " + std::to_string(filenum) + " offset: " + std::to_string(offset);
  }
  void Clear() noexcept {
    filenum = 0;
    offset = 0;
  }
  bool Empty() const {
    return filenum == 0 && offset == 0;
  }
  bool operator==(const BinlogOffset& other) const {
    if (filenum == other.filenum && offset == other.offset) {
      return true;
    }
    return false;
  }
  bool operator!=(const BinlogOffset& other) const {
    if (filenum != other.filenum || offset != other.offset) {
      return true;
    }
    return false;
  }

  bool operator>(const BinlogOffset& other) const {
    if (filenum > other.filenum
        || (filenum == other.filenum && offset > other.offset)) {
      return true;
    }
    return false;
  }
  bool operator<(const BinlogOffset& other) const {
    if (filenum < other.filenum
        || (filenum == other.filenum && offset < other.offset)) {
      return true;
    }
    return false;
  }
  bool operator<=(const BinlogOffset& other) const {
    if (filenum < other.filenum
        || (filenum == other.filenum && offset <= other.offset)) {
      return true;
    }
    return false;
  }
  bool operator>=(const BinlogOffset& other) const {
    if (filenum > other.filenum
        || (filenum == other.filenum && offset >= other.offset)) {
      return true;
    }
    return false;
  }
};

struct LogOffset {
  LogOffset(const LogOffset& _log_offset) {
    b_offset = _log_offset.b_offset;
    l_offset = _log_offset.l_offset;
  }
  LogOffset() : b_offset(), l_offset() {
  }
  LogOffset(BinlogOffset _b_offset, LogicOffset _l_offset)
    : b_offset(_b_offset), l_offset(_l_offset) {
  }
  void Clear() noexcept {
    b_offset.Clear();
    l_offset.Clear();
  }
  bool Empty() const {
    return b_offset.Empty() && l_offset.Empty();
  }
  bool operator<(const LogOffset& other) const {
    return b_offset < other.b_offset;
  }
  bool operator==(const LogOffset& other) const {
    return b_offset == other.b_offset;
  }
  bool operator!=(const LogOffset& other) const {
    return b_offset != other.b_offset;
  }
  bool operator<=(const LogOffset& other) const {
    return b_offset <= other.b_offset;
  }
  bool operator>=(const LogOffset& other) const {
    return b_offset >= other.b_offset;
  }
  bool operator>(const LogOffset& other) const {
    return b_offset > other.b_offset;
  }
  std::string ToBinlogString() const  {
    return b_offset.ToString();
  }
  std::string ToString() const  {
    return b_offset.ToString() + " " + l_offset.ToString();
  }
  BinlogOffset b_offset;
  LogicOffset  l_offset;
};

/* replication IDs */

// ReplicationGroupID specific a replication group
class ReplicationGroupID {
 public:
  ReplicationGroupID() : partition_id_(0) { }
  ReplicationGroupID(const std::string& table_name, uint32_t partition_id)
    : table_name_(table_name), partition_id_(partition_id) {
  }
  void Clear() {
    table_name_.clear();
    partition_id_ = 0;
  }
  std::string TableName() const { return table_name_; }
  uint32_t PartitionID() const { return partition_id_; }
  bool Empty() const { return table_name_.empty() && partition_id_ == 0; }
  bool operator==(const ReplicationGroupID& other) const {
    return table_name_ == other.table_name_ && partition_id_ == other.partition_id_;
  }
  bool operator<(const ReplicationGroupID& other) const {
    int ret = strcmp(table_name_.data(), other.table_name_.data());
    if (!ret) {
      return partition_id_ < other.partition_id_;
    }
    return ret < 0;
  }
  std::string ToString() const {
    return "(" + table_name_ + ":" + std::to_string(partition_id_) + ")";
  }
 private:
  std::string table_name_;
  uint32_t partition_id_;
};

struct hash_replication_group_id {
  size_t operator()(const ReplicationGroupID& cid) const {
    return std::hash<std::string>()(cid.TableName()) ^ std::hash<uint32_t>()(cid.PartitionID());
  }
};

// PeerID specific the peer addr of a replica
class PeerID {
 public:
  /*
   * The shift port to replication.
   * ip:port indicates the server address for client requests.
   * ip:port+kReplPortShift indicates the server address for replicate requests.
   */
  constexpr static int kReplPortShift = 2000;
  PeerID()
    : ip_(""), port_(-1) {}
  PeerID(const std::string& ip, int port)
    : ip_(ip), port_(port) { }
  PeerID(const PeerID& peer_id)
    : ip_(peer_id.ip_), port_(peer_id.port_) { }
  void SetIp(const std::string& ip) {
    ip_ = ip;
  }
  void SetPort(int port) {
    port_ = port;
  }
  void SetConnectPort(int connect_port) {
    port_ = connect_port - kReplPortShift;
    if (port_ < 0) {
      LOG(WARNING) << "SetConnectPort error, connect_port: " << connect_port
                   << ", repl_port_shift: " << kReplPortShift;
      port_ = 0;
    }
  }
  static const PeerID& DummyPeerID() {
    static const PeerID dummy_peer_id;
    return dummy_peer_id;
  }
  const std::string& Ip() const {
    return ip_;
  }
  int Port() const {
    return port_;
  }
  int ConnectPort() const {
    return port_ + kReplPortShift;
  }
  std::string ToString() const {
    return ip_ + ":" + std::to_string(port_);
  }
  bool ParseFromIpPort(const std::string& ip_port, bool is_connect_port = false) {
    std::string ip;
    int port = 0;
    if (!slash::ParseIpPortString(ip_port, ip, port)) {
      return false;
    }
    ip_ = ip;
    port_ = is_connect_port ? port - kReplPortShift : port;
    return true;
  }
  bool Empty() const {
    return ip_.empty() && port_ == -1;
  }
  void Clear() {
    ip_.clear();
    port_ = -1;
  }
  bool operator==(const PeerID& other) const {
    return ip_ == other.ip_ && port_ == other.port_;
  }
  bool operator!=(const PeerID& other) const {
    return ip_ != other.ip_ || port_ != other.port_;
  }
 private:
  std::string ip_;
  int port_;
};

struct hash_peer_id {
  size_t operator()(const PeerID& id) const {
    return std::hash<std::string>()(id.Ip()) ^ std::hash<int>()(id.Port());
  }
};

struct PeerInfo {
  PeerID peer_id;
  int fd;
  PeerInfo(const PeerID& _peer_id,
           int _fd)
    : peer_id(_peer_id), fd(_fd) { }
  PeerInfo(const std::string& ip,
           int port,
           int _fd)
    : peer_id(std::move(PeerID(ip, port))), fd(_fd) { }
};

class ReplTask {
 public:
  enum class Type : uint8_t {
    kNoType      = 0,
    // Recieved from client
    kClientType  = 1,
    // Produced by initialization
    kRedoType    = 2,
    // Recieved from peer
    kReplicaType = 3,
    // Generated when leader confirmation
    kDummyType   = 4,
    kOtherType   = 5,
  };
  enum class LogFormat : uint8_t {
    kUnkown = 0,
    kRedis  = 1,
    kPB     = 2,
  };
  ReplTask(std::string&& log,
           util::Closure* closure,
           const ReplicationGroupID& group_id,
           const PeerID& peer_id,
           const Type& type,
           const LogFormat& log_format)
    : group_id_(group_id),
    peer_id_(peer_id),
    log_(std::move(log)),
    log_format_(log_format),
    type_(type),
    closure_(closure) { }
  DISALLOW_COPY(ReplTask);

  const ReplicationGroupID& group_id() const { return group_id_; }
  const PeerID& peer_id() const { return peer_id_; }
  const std::string& log() const { return log_; }
  const LogFormat& log_format() const { return log_format_; }
  std::string ReleaseLog() { return std::move(log_); }
  slash::Slice LogSlice() { return slash::Slice(log_.data(), log_.size()); }
  util::Closure* ReleaseClosure() {
    util::Closure* tmp = closure_;
    closure_ = nullptr;
    return tmp;
  }
  util::Closure* closure() { return closure_; }
  const Type& type() const { return type_; }

 private:
  ReplicationGroupID group_id_;
  PeerID peer_id_;
  // The log field represents the data that should be persisted.
  std::string log_;
  LogFormat log_format_;
  Type type_;
  // The closure_ field is invoked when the current task is committed.
  // There are two cases that the done field is nullptr:
  // (1) the task is produced by initialization.
  // (2) the task is received from remote peer(leader).
  util::Closure* closure_;
};

#endif // PIKA_DEFINE_H_
