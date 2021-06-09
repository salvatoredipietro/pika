// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_INCLUDE_UTIL_TASK_EXECUTOR_H_
#define PIKA_INCLUDE_UTIL_TASK_EXECUTOR_H_

#include <memory>
#include <string>

#include "include/util/thread_pool.h"

namespace util {

struct TaskExecutorOptions {
  constexpr static size_t kDefaultCompetitiveWorkerNum = 8;
  // If competitive_worker_num <= 0, then all pending schedule tasks
  // will be executed in-place (the context of the caller), so to make
  // sure there are no race-conditions.
  //
  // There are competitive_worker_num workers fetch tasks from
  // the same queue competitively.
  size_t competitive_worker_num = kDefaultCompetitiveWorkerNum;
  size_t competitive_queue_size = thread::ThreadPool::kDefaultMaxQueueSize;

  constexpr static size_t kDefaultDispatchWorkerNum = 12;
  // If dispatch_worker_num <= 0, then all pending schedule tasks
  // will be executed in-place (the context of the caller), so to make
  // sure there are no race-conditions.
  //
  // There are dispatch_worker_num workers fetch tasks from their
  // own queue, independently.
  size_t dispatch_worker_num = kDefaultDispatchWorkerNum;
  size_t worker_queue_size = thread::ThreadPool::kDefaultMaxQueueSize;
  TaskExecutorOptions() = default;
  TaskExecutorOptions(const size_t _competitive_worker_num,
                      const size_t _dispatch_worker_num)
    : competitive_worker_num(_competitive_worker_num),
    dispatch_worker_num(_dispatch_worker_num) {}
};

class TaskExecutor {
 public:
  explicit TaskExecutor(const TaskExecutorOptions& options);
  ~TaskExecutor();

  int Start();
  int Stop(bool wait_for_tasks);

  void Schedule(void (*function)(void *), void* arg,
                const std::string& hash_key = "", uint64_t time_after = 0);
  void ScheduleTask(std::shared_ptr<thread::Task> task,
                    const std::string& hash_str = "");
  void ScheduleTimerTask(std::shared_ptr<thread::TimerTask> task,
                         const std::string& hash_str = "");
  void ScheduleCompetitive(void (*function)(void *), void* arg, uint64_t time_after = 0);
  void ScheduleCompetitiveTask(std::shared_ptr<thread::Task> task);
  void ScheduleCompetitiveTimerTask(std::shared_ptr<thread::TimerTask> task);

 private:
  class DispatchExecutor;
  class CompetitiveExecutor;

  DispatchExecutor* dispatch_executor_;
  CompetitiveExecutor* competitive_executor_;
};

} // namespace util

#endif // PIKA_INCLUDE_UTIL_TASK_EXECUTOR_H_
