// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_INCLUDE_UTIL_HOOKS_H_
#define PIKA_INCLUDE_UTIL_HOOKS_H_

#include <memory>
#include <utility>

namespace util {

template<class T, class... Args>
std::unique_ptr<T> make_unique(Args&&... args) {
  return std::unique_ptr<T>(new T(std::forward<Args>(args)...));
}

} // namespace util

#endif // PIKA_INCLUDE_UTIL_HOOKS_H_
