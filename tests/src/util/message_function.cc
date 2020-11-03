// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/util/message_function.h"

#include "include/util/random.h"

namespace test {

std::string ZeroBinaryData(int size) {
  std::string s(size, '\0');
  return s;
}

std::string RandomAlphaData(int size) {
  std::string result;
  result.reserve(size);
  static util::WELL512RNG random_generator;
  static const char alphanum[] =
      "0123456789"
      "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
      "abcdefghijklmnopqrstuvwxyz";
  for (int i = 0; i < size; i++) {
    result += alphanum[random_generator.Generate()%(sizeof(alphanum) - 1)];
  }
  return result;
}

} // namespace test
