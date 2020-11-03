// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/deps/storage_env.h"

#include <chrono>

namespace test {

static const char kBinlogEnvHelpMessage[] =
"You can use the following command line flags to control binlog dir for Tests:\n"
"\n"
"Binlog Arguments Selection:\n"
"  --binlog_dir=./binlog/\n"
"      Log dir to save binlog files. The dir will be destroyed when finish testing,\n"
"      so we will add the time suffix to it to avoid conflicts.\n"
"      To drop the suffix, set --disable_binlog_dir_suffix to true.\n"
"  --disable_binlog_dir_suffix[=true|false]\n"
"      Used in conjunction with --binlog_dir, Default is false.\n"
"\n";

BinlogEnv::BinlogEnv(int argc, char** argv)
  : Env(argc, argv),
  binlog_dir_("./binlog/"),
  disable_binlog_dir_suffix_(false) {
  for (int i = 1; i < argc; i++) {
    const std::string str = argv[i];
    if (str == "--help") {
      std::cout<<kBinlogEnvHelpMessage<<std::endl;
      break;
    }
    if (FlagParser::ParseStringFlag(str, kBinlogDirFlag, &binlog_dir_)
        || FlagParser::ParseBoolFlag(str, kDisableBinlogDirSuffixFlag, &disable_binlog_dir_suffix_)) {
      continue;
    }
  }
  if (!disable_binlog_dir_suffix_) {
    size_t last_slash_pos = binlog_dir_.find_last_of("/");
    if (last_slash_pos != std::string::npos) {
      std::string remove_last_slash_str = binlog_dir_.substr(0, last_slash_pos);
      size_t second_to_last_slash_pos = remove_last_slash_str.find_last_of("/");
      if (second_to_last_slash_pos != std::string::npos) {
        std::string last_sub_dir = binlog_dir_.substr(second_to_last_slash_pos,
            last_slash_pos - second_to_last_slash_pos);
        if (last_sub_dir != "." && last_sub_dir != "..") {
          // remove the last slash
          binlog_dir_ = remove_last_slash_str;
        } else {
          binlog_dir_ += "binlog";
        }
      } else if (remove_last_slash_str != "." && remove_last_slash_str != ".." ) {
        binlog_dir_ = remove_last_slash_str;
      } else {
        binlog_dir_ += "binlog";
      }
    } else if (binlog_dir_ == "." || binlog_dir_ == "..") {
      binlog_dir_ += "/binlog";
    }
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
              std::chrono::system_clock::now().time_since_epoch());
    binlog_dir_ += "_" + std::to_string(ms.count()) + "/";
  }
}

} // namespace test

