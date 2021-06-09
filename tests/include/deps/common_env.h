// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef INCLUDE_DEPS_COMMON_ENV_H_
#define INCLUDE_DEPS_COMMON_ENV_H_

#include <string>

#include "gtest/gtest.h"

namespace test {

const std::string kGlogFlag = "--glog";
const std::string kGlogDir = "--glog_dir";
const std::string kGlogMinLevel = "--glog_min_level";
const std::string kGlogAlsoLogToStderr = "--glog_alsologtostderr";
const std::string kGlogDisableDirSuffix = "--glog_disable_dir_suffix";

class FlagParser {
 public:
  static bool ParseBoolFlag(const std::string& str,
                            const std::string& flag,
                            bool *value);
  static bool ParseStringFlag(const std::string& str,
                              const std::string& flag,
                              std::string* value);
  static bool ParseIntFlag(const std::string& str,
                           const std::string& flag,
                           int* value);

 private:
  static bool ParseFlagValue(const std::string& str,
                             const std::string& flag,
                             bool skip_value,
                             std::string* value);
  static bool CaseInsensitiveCompare(const std::string& lhs,
                                     const std::string& rhs);
};

class GlogHandler {
 public:
  GlogHandler(int argc, char** argv);
  ~GlogHandler() = default;
  int Start();
  void Stop();

 private:
  bool enable_;
  bool also_log_to_stderr_;
  int min_level_;
  std::string dir_;
  bool disable_dir_suffix_;
};

class Env : public ::testing::Environment {
 public:
  Env(int argc, char** argv);
  ~Env() override;

  void SetUp() override;
  void TearDown() override;

 private:
  GlogHandler glog_handler_;
};

}  // namespace test

#endif  // INCLUDE_DEPS_COMMON_ENV_H_
