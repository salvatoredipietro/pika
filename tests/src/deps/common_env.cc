// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/deps/common_env.h"

#include <chrono>
#include <unistd.h>
#include <cstdlib>
#include <iostream>
#include <glog/logging.h>

#include "slash/include/env.h"

namespace test {

bool FlagParser::ParseBoolFlag(const std::string& str,
                               const std::string& flag,
                               bool *value) {
  std::string flag_value;
  if (!ParseFlagValue(str, flag, true, &flag_value)) return false;
  *value = !(CaseInsensitiveCompare(flag_value, "f")
             || CaseInsensitiveCompare(flag_value, "false"));
  return true;
}

bool FlagParser::ParseStringFlag(const std::string& str,
                                 const std::string& flag,
                                 std::string* value) {
  if (!ParseFlagValue(str, flag, false, value)) return false;
  return true;
}

bool FlagParser::ParseIntFlag(const std::string& str,
                              const std::string& flag,
                              int* value) {
  std::string flag_value;
  if (!ParseFlagValue(str, flag, false, &flag_value)) return false;
  char* end = nullptr;
  *value = std::strtoull(flag_value.c_str(), &end, 10);
  if (*end != '\0') {
    std::cerr<<"flag "<< flag << " must followed by a int value (=int)." << std::endl;
    return false;
  }
  return true;
}

bool FlagParser::ParseFlagValue(const std::string& str,
                                const std::string& flag,
                                bool skip_value,
                                std::string* value) {
  if (str.empty() || flag.empty()) return false; 

  const size_t flag_len = flag.size();
  if (strncmp(str.c_str(), flag.c_str(), flag_len) != 0) return false; 

  // Skips the flag name.
  const std::string flag_val = str.substr(flag_len);

  if (flag_val.empty()) {
    // When skip_value is true, it's OK to not have a "=value" part.
    if (skip_value) {
      value->assign("");
      return true; 
    }
  } else if (flag_val[0] == '=') {
    value->assign(flag_val.substr(1));
    return true;
  }
  return false;
}

bool FlagParser::CaseInsensitiveCompare(const std::string& lhs,
                                        const std::string& rhs) {
  if (lhs.size() != rhs.size()) return false;
  return std::equal(lhs.begin(), lhs.end(), rhs.begin(),
                   [] (const char& c1, const char& c2) {
                     return (c1 == c2) || (std::toupper(c1) == std::toupper(c2));
                   });
}

static const char kGlogHelpMessage[] =
"You can use the following command line flags to control log behavior:\n"
"\n"
"GLOG Selection:\n"
"  --glog[=[true|false]]\n"
"      Enable glog when this flag is provided with positive value.\n"
"  --glog_dir=./glog/\n"
"      The log dir for glog, when --glog is enabled. The dir will be destroyed when finish\n"
"      testing, so we will add the time suffix to it to avoid conflicts, To drop the suffix,\n"
"      you should set --glog_disable_dir_suffix to true.\n"
"  --glog_disable_dir_suffix[=true|false]\n"
"      Used in conjunction with --glog_dir, Default is false.\n"
"  --glog_min_level=[0-5]\n"
"      The log level when --glog is enabled.\n"
"  --glog_alsologtostderr[=[true|false]]\n"
"      The logs will output to stderr automatically when --glog is enabled.\n"
"      You should set to false manually to close it.\n"
"\n";

GlogHandler::GlogHandler(int argc, char** argv)
  : enable_(false),
  also_log_to_stderr_(true),
  min_level_(0),
  dir_("./glog/"),
  disable_dir_suffix_(false) {
  for (int i = 1; i < argc; i++) {
    const std::string str = argv[i];
    if (str == "--help") {
      std::cout<<kGlogHelpMessage<<std::endl;
      break;
    }
    if (FlagParser::ParseBoolFlag(str, kGlogFlag, &enable_)
        || FlagParser::ParseIntFlag(str, kGlogMinLevel, &min_level_)
        || FlagParser::ParseBoolFlag(str, kGlogAlsoLogToStderr, &also_log_to_stderr_)
        || FlagParser::ParseBoolFlag(str, kGlogDisableDirSuffix, &disable_dir_suffix_)
        || FlagParser::ParseStringFlag(str, kGlogDir, &dir_)) {
      continue;
    }
  }
  if (!disable_dir_suffix_) {
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
              std::chrono::system_clock::now().time_since_epoch());
    dir_ += std::to_string(ms.count());
  }
}

int GlogHandler::Start() {
  if (enable_) {
    // Create log dir if not exist
    if (!slash::FileExists(dir_)) {
      slash::CreatePath(dir_); 
    }
    FLAGS_alsologtostderr = also_log_to_stderr_;
    std::cout<<"glog enabled"<<std::endl;
  }
  FLAGS_minloglevel = enable_ ? min_level_ : 5;
  FLAGS_max_log_size = 1800;
  FLAGS_logbufsecs = 0;
  FLAGS_log_dir = dir_;
  ::google::InitGoogleLogging("pika-test");
  return 0;
}

void GlogHandler::Stop() {
  ::google::ShutdownGoogleLogging();
}

Env::Env(int argc, char** argv)
  : glog_handler_(argc, argv) {
}

Env::~Env() {}

void Env::SetUp() {
  int res = glog_handler_.Start();
  if (res != 0) {
    std::exit(res);
  }
}

void Env::TearDown() {
  glog_handler_.Stop();
}

}  // namespace test
