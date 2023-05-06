#include <chrono>
#include <condition_variable>
#include <mutex>
#include <vector>

#include "blockingconcurrentqueue.h"
#include "cass/include/cassandra.h"
#include "spdlog/spdlog.h"
#include "util.h"

class Worker;
using time_point = std::chrono::time_point<std::chrono::system_clock>;
using CQueue =
    moodycamel::BlockingConcurrentQueue<std::pair<CassError, Worker *>>;

struct CommonInfo {
  CommonInfo(CassSession *session, const CassPrepared *prepared,
             CassUuidGen *uuid_gen, CQueue &q)
      : session(session), prepared(prepared), uuid_gen(uuid_gen), q(q){};
  CassSession *session;
  const CassPrepared *prepared;
  CassUuidGen *uuid_gen;

  std::mutex mu; // protecting finished
  std::condition_variable cv;
  uint32_t finished = 0;
  CQueue &q;
};

class Worker {
public:
  Worker(uint32_t id, uint32_t target, uint32_t batch_size, CommonInfo *info);
  ~Worker();
  void Execute();
  CassStatement *NewStatement();
  void Callback(CassError ce);
  uint32_t QPS() { return (stat_.Count() * batch_size_) / RUN_SECONDS; };

  uint32_t id_;
  uint32_t target_partition_;
  uint32_t batch_size_;
  CassBatch *batch_{nullptr};
  time_point last_point_;
  time_point end_point_;
  LatencyStat stat_;
  CommonInfo *info_;
};
