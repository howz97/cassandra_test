#include <chrono>
#include <condition_variable>
#include <mutex>
#include <vector>

#include "cass/include/cassandra.h"

using time_point = std::chrono::time_point<std::chrono::system_clock>;

#define STAT_GRAIN 10
#define STAT_MAX 10000
#define STAT_LEN STAT_MAX / STAT_GRAIN
#define RUN_SECONDS 3

struct CommonInfo {
  CassSession *session;
  const CassPrepared *prepared;
  CassUuidGen *uuid_gen;

  std::mutex mu; // protecting finished
  std::condition_variable cv;
  uint32_t finished = 0;
};

class Worker {
public:
  Worker(uint32_t id, uint32_t target, uint32_t batch_size, CommonInfo *info);
  ~Worker();
  void Execute();
  CassStatement *NewStatement();
  uint32_t AverageLantency();
  uint32_t QPS() { return (batch_executed_ * batch_size_) / RUN_SECONDS; };

  uint32_t id_;
  uint32_t target_partition_;
  uint32_t batch_size_;
  CassBatch *batch_{nullptr};
  uint32_t batch_executed_{0};
  time_point last_point_;
  time_point end_point_;
  uint32_t lantency_stat_[STAT_LEN] = {}; // ms
  uint32_t lantency_sum_{0};              // ms
  CommonInfo *info_;
};