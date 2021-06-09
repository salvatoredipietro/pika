// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/util/task_executor.h"

#include <functional>

#include "include/util/hooks.h"

namespace util {

// DispatchExecutor

class TaskExecutor::DispatchExecutor {
 public:
  DispatchExecutor(size_t worker_num, size_t queue_size);

  int Start();
  int Stop(bool wait_for_tasks);

  void Schedule(void (*function)(void *), void* arg,
                const std::string& hash_str, uint64_t time_after);
  void ScheduleTask(std::shared_ptr<thread::Task> task, const std::string& hash_str);
  void ScheduleTimerTask(std::shared_ptr<thread::TimerTask> task, const std::string& hash_str);

 private:
  size_t GetIndex(const std::string& hash_key) {
    if (hash_key.empty()) {
      return (next_index_++)%worker_num_;
    }
    return (str_hash_(hash_key))%worker_num_;
  }
  const int worker_num_;

  size_t next_index_;
  std::hash<std::string> str_hash_;
  std::vector<std::unique_ptr<thread::ThreadPool>> bg_workers_;
};

TaskExecutor::DispatchExecutor::DispatchExecutor(size_t worker_num, size_t thread_queue_size)
  : worker_num_(worker_num),
  next_index_(0) {
  for (int i = 0; i < worker_num_; ++i) {
    auto tp = util::make_unique<thread::ThreadPool>(1, thread_queue_size);
    //thread->set_thread_name("dispatch_executor_thread_" + std::to_string(i));
    bg_workers_.push_back(std::move(tp));
  }
}

int TaskExecutor::DispatchExecutor::Start() {
  int res = 0;
  for (int i = 0; i < worker_num_; i++) {
    res = bg_workers_[i]->Start();
    if (res != thread::ThreadPoolStartRet::kStartOK) {
      break;
    }
  }
  return res;
}

int TaskExecutor::DispatchExecutor::Stop(bool wait_for_tasks) {
  int res = 0;
  for (int i = 0; i < worker_num_; i++) {
    res = bg_workers_[i]->Stop(wait_for_tasks);
    if (res != thread::ThreadPoolStopRet::kStopOK) {
      break;
    }
  }
  return res;
}

void TaskExecutor::DispatchExecutor::Schedule(void (*function)(void *), void* arg,
                                              const std::string& hash_str, uint64_t time_after) {
  const size_t index = GetIndex(hash_str);
  if (time_after == 0) {
    bg_workers_[index]->Schedule(function, arg);
  } else {
    bg_workers_[index]->DelaySchedule(function, arg, time_after);
  }
}

void TaskExecutor::DispatchExecutor::ScheduleTask(std::shared_ptr<thread::Task> task,
                                                  const std::string& hash_str) {
  const size_t index = GetIndex(hash_str);
  bg_workers_[index]->ScheduleTask(std::move(task));
}

void TaskExecutor::DispatchExecutor::ScheduleTimerTask(std::shared_ptr<thread::TimerTask> timer_task,
                                                       const std::string& hash_str) {
  const size_t index = GetIndex(hash_str);
  bg_workers_[index]->ScheduleTimerTask(std::move(timer_task));
}

// CompetitiveExecutor

class TaskExecutor::CompetitiveExecutor {
 public:
  CompetitiveExecutor(size_t worker_num, size_t queue_size);

  int Start();
  int Stop(bool wait_for_tasks);

  void Schedule(void (*function)(void *), void* arg, uint64_t time_after);
  void ScheduleTask(std::shared_ptr<thread::Task> task);
  void ScheduleTimerTask(std::shared_ptr<thread::TimerTask> task);

 private:
  std::unique_ptr<thread::ThreadPool> thread_pool_;
};

TaskExecutor::CompetitiveExecutor::CompetitiveExecutor(size_t worker_num, size_t queue_size) {
  thread_pool_ = util::make_unique<thread::ThreadPool>(worker_num, queue_size);
}

int TaskExecutor::CompetitiveExecutor::Start() {
  return thread_pool_->Start();
}

int TaskExecutor::CompetitiveExecutor::Stop(bool wait_for_tasks) {
  return thread_pool_->Stop(wait_for_tasks);
}

void TaskExecutor::CompetitiveExecutor::Schedule(void (*function)(void *), void* arg, uint64_t time_after) {
  if (time_after == 0) {
    thread_pool_->Schedule(function, arg);
  } else {
    thread_pool_->DelaySchedule(function, arg, time_after);
  }
}

void TaskExecutor::CompetitiveExecutor::ScheduleTask(std::shared_ptr<thread::Task> task) {
  thread_pool_->ScheduleTask(task);
}

void TaskExecutor::CompetitiveExecutor::ScheduleTimerTask(std::shared_ptr<thread::TimerTask> timer_task) {
  thread_pool_->ScheduleTimerTask(timer_task);
}

// TaskExecutor

TaskExecutor::TaskExecutor(const TaskExecutorOptions& options)
  : dispatch_executor_(nullptr),
  competitive_executor_(nullptr) {

  if (options.competitive_worker_num > 0 && options.competitive_queue_size > 0) {
    competitive_executor_ = new CompetitiveExecutor(
          options.competitive_worker_num, options.competitive_queue_size);
  }
  if (options.dispatch_worker_num > 0 && options.worker_queue_size > 0) {
    dispatch_executor_ = new DispatchExecutor(
          options.dispatch_worker_num, options.worker_queue_size);
  }
}

TaskExecutor::~TaskExecutor() {
  if (dispatch_executor_ != nullptr) {
    delete dispatch_executor_;
    dispatch_executor_ = nullptr;
  }
  if (competitive_executor_ != nullptr) {
    delete competitive_executor_;
    competitive_executor_ = nullptr;
  }
}

int TaskExecutor::Start() {
  int res = 0;
  if (dispatch_executor_ != nullptr) {
    res = dispatch_executor_->Start();
    if (res != thread::ThreadPoolStartRet::kStartOK) {
      return res;
    }
  }
  if (competitive_executor_ != nullptr) {
    res = competitive_executor_->Start();
    if (res != thread::ThreadPoolStartRet::kStartOK) {
      return res;
    }
  }
  return res;
}

int TaskExecutor::Stop(bool wait_for_tasks) {
  int res = 0;
  if (competitive_executor_ != nullptr) {
    res = competitive_executor_->Stop(wait_for_tasks);
    if (res != thread::ThreadPoolStopRet::kStopOK) {
      return res;
    }
  }
  if (dispatch_executor_ != nullptr) {
    res = dispatch_executor_->Stop(wait_for_tasks);
    if (res != thread::ThreadPoolStopRet::kStopOK) {
      return res;
    }
  }
  return res;
}

void TaskExecutor::Schedule(void (*function)(void *), void* arg,
                            const std::string& hash_key, uint64_t time_after) {
  if (dispatch_executor_ != nullptr) {
    dispatch_executor_->Schedule(function, arg, hash_key, time_after);
    return;
  }
  // Do in local
  function(arg);
}

void TaskExecutor::ScheduleTask(std::shared_ptr<thread::Task> task,
                               const std::string& hash_str) {
  if (dispatch_executor_ != nullptr) {
    dispatch_executor_->ScheduleTask(std::move(task), hash_str);
    return;
  }
  // Do in local
  task->Run();
}

void TaskExecutor::ScheduleTimerTask(std::shared_ptr<thread::TimerTask> timer_task,
                                    const std::string& hash_str) {
  if (dispatch_executor_ != nullptr) {
    dispatch_executor_->ScheduleTimerTask(std::move(timer_task), hash_str);
    return;
  }
  // Do in local
  timer_task->Run();
}

void TaskExecutor::ScheduleCompetitive(void (*function)(void *), void* arg,
                                       uint64_t time_after) {
  if (competitive_executor_ != nullptr) {
    competitive_executor_->Schedule(function, arg, time_after);
    return;
  }
  // Do in local
  function(arg);
}

void TaskExecutor::ScheduleCompetitiveTask(std::shared_ptr<thread::Task> task) {
  if (competitive_executor_ != nullptr) {
    competitive_executor_->ScheduleTask(std::move(task));
    return;
  }
  // Do in local
  task->Run();
}

void TaskExecutor::ScheduleCompetitiveTimerTask(std::shared_ptr<thread::TimerTask> timer_task) {
  if (competitive_executor_ != nullptr) {
    competitive_executor_->ScheduleTimerTask(std::move(timer_task));
    return;
  }
  // Do in local
  timer_task->Run();
}

} // namespace util
