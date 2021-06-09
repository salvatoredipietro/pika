// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef INCLUDE_DEPS_REPLICATION_ENV_H_
#define INCLUDE_DEPS_REPLICATION_ENV_H_

#include <string>

#include "include/deps/storage_env.h"

namespace test {

const std::string kIpFlag = "--local_ip";
const std::string kPortFlag = "--local_port";

class ReplManagerEnv : public BinlogEnv {
 public:
  ReplManagerEnv(int argc, char** argv);
  ~ReplManagerEnv() = default;

  const std::string& local_ip() const { return local_ip_; }
  int local_port() const { return local_port_; }
 
 private:
  std::string local_ip_;
  int local_port_;
};

} // namespace test

#endif // INCLUDE_DEPS_REPLICATION_ENV_H_
