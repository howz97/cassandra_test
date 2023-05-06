#include <chrono>
#include <vector>

#include "cass/include/cassandra.h"
#include "spdlog/spdlog.h"
#include "util.h"

struct FutureElement {
  CassFuture *cass_future;
  CassBatch *cass_batch;
  std::chrono::time_point<std::chrono::system_clock> start_point;

  FutureElement(CassFuture *cass_future, CassBatch *cass_batch,
                std::chrono::time_point<std::chrono::system_clock> start_point)
      : cass_future(cass_future), cass_batch(cass_batch),
        start_point(start_point) {}
};

class CassBatchExecutor {
public:
  CassBatchExecutor(CassSession *session, int32_t batch_size);
  ~CassBatchExecutor();
  CassError AddBatchStatement(CassStatement *stmt);
  bool HasStatements();
  void Execute();
  CassError Wait_until(std::chrono::time_point<std::chrono::system_clock> end);
  // CassError Retry();
  uint8_t PendingFutureCount() { return futures_.size(); }
  bool IsFull() { return current_batch_size_ == batch_size_; }
  LatencyStat stat_;

private:
  CassBatch *batch_{nullptr};
  CassSession *session_{nullptr};
  std::vector<FutureElement> futures_;
  const int32_t batch_size_{0};
  int32_t current_batch_size_{0};
};