// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/deps/replication_env.h"

namespace test {

static const char kReplManagerHelpMessage[] =
"You can use the following command line flags to deliver arguments to Tests:\n"
"\n"
"Test Arguments Selection:\n"
"  --local_ip=127.0.0.1\n"
"      Ip for replication server service to listen.\n"
"  --local_port=9221\n"
"      Port plus 2000 is the listen port for replication server service.\n"
"\n";

ReplManagerEnv::ReplManagerEnv(int argc, char** argv)
  : BinlogEnv(argc, argv),
  local_ip_("127.0.0.1"),
  local_port_(9221) {
  for (int i = 1; i < argc; i++) {
    const std::string str = argv[i];
    if (str == "--help") {
      std::cout<<kReplManagerHelpMessage<<std::endl;
      break;
    }
    if (FlagParser::ParseStringFlag(str, kIpFlag, &local_ip_)
        || FlagParser::ParseIntFlag(str, kPortFlag, &local_port_)) {
      continue;
    }
  }
}

} // namespace test
