// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/util/random.h"

#include <chrono>

namespace util {

WELL512RNG::WELL512RNG()
  : index(0) {
  auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch());
  Init(ms.count());
}

WELL512RNG::WELL512RNG(uint32_t seed)
  : index(0) {
  Init(seed);
}

void WELL512RNG::Init(uint32_t seed) {
  const uint32_t mask = 0xffffffff;
  state[0]= seed & mask;
  for (uint16_t i = 1; i < 16; i++) {
    state[i] = (1812433253* (state[i - 1] ^ (state[i - 1] >> 30)) + i) & mask;
  }
}

uint32_t WELL512RNG::Generate() {
  uint32_t a, b, c, d;
  a = state[index];
  c = state[(index+13)&15];
  b = a^c^(a<<16)^(c<<15);
  c = state[(index+9)&15];
  c ^= (c>>11);
  a = state[index] = b^c;
  d = a^((a<<5)&0xDA442D24);
  index = (index + 15)&15;
  a = state[index];
  state[index] = a^b^d^(a<<2)^(b<<18)^(c<<28);
  return state[index];
}

} // namespace util
