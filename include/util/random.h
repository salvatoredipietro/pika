// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_INCLUDE_UTIL_RANDOM_H_
#define PIKA_INCLUDE_UTIL_RANDOM_H_

#include <stdint.h>

namespace util {

/*
 * A Random Number Generation Algorithm, introduced by
 * Matsumoto, L’Ecuyer, and Panneton in 2006.
 * www.iro.umontreal.ca/~lecuyer/papers.html
 */
class WELL512RNG {
 public:
  WELL512RNG();
  explicit WELL512RNG(uint32_t seed);

 public:
  // initialize state to random bits, used by MT19937
  void Init(uint32_t seed);
  // generate a 32-bits random number
  uint32_t Generate();

 private:
  uint32_t state[16];
  uint16_t index;
};

} // namespace util

#endif  // PIKA_RANDOM_H_
