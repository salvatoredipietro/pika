// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef INCLUDE_DEPS_STORAGE_ENV_H_
#define INCLUDE_DEPS_STORAGE_ENV_H_

#include <string>

#include "include/deps/common_env.h"

namespace test {

const std::string kBinlogDirFlag = "--binlog_dir";
const std::string kDisableBinlogDirSuffixFlag = "--disable_binlog_dir_suffix";

class BinlogEnv : public Env {
 public:
  BinlogEnv(int argc, char** argv);
  ~BinlogEnv() = default;

  const std::string& binlog_dir() const { return binlog_dir_; }

 private:
  std::string binlog_dir_;
  bool disable_binlog_dir_suffix_;
};

} // namespace test

#endif // INCLUDE_DEPS_STORAGE_ENV_H_
