#include "worker.h"

#include <cassert>
#include <stdio.h>

#include "spdlog/include/spdlog/spdlog.h"

void call_back(CassFuture *future, void *data) {
  auto now = std::chrono::system_clock::now();
  Worker *worker = (Worker *)data;
  CassError ce = cass_future_error_code(future);
  if (ce == CASS_OK) {
    worker->batch_executed_++;
    uint32_t latency = std::chrono::duration_cast<std::chrono::milliseconds>(
                           now - worker->last_point_)
                           .count();
    worker->latency_sum_ += latency;
    if (latency >= STAT_MAX) {
      worker->latency_stat_[STAT_LEN - 1]++;
    } else {
      worker->latency_stat_[latency / STAT_GRAIN]++;
    }
    cass_batch_free(worker->batch_);
    worker->batch_ = nullptr;
  } else {
    spdlog::warn("future call_back get error: {}\n", cass_error_desc(ce));
  }
  if (now < worker->end_point_) {
    worker->Execute();
  } else {
    std::unique_lock lk(worker->info_->mu);
    worker->info_->finished++;
    lk.unlock();
    worker->info_->cv.notify_one();
  }
}

Worker::Worker(uint32_t id, uint32_t target, uint32_t batch_size,
               CommonInfo *info)
    : id_(id), target_partition_(target), batch_size_(batch_size), info_(info) {
  end_point_ =
      std::chrono::system_clock::now() + std::chrono::seconds(RUN_SECONDS);
  this->Execute();
};

Worker::~Worker() {
  if (batch_) {
    cass_batch_free(batch_);
    batch_ = nullptr;
  }
};

void Worker::Execute() {
  if (batch_ == nullptr) {
    batch_ = cass_batch_new(CASS_BATCH_TYPE_LOGGED);
    cass_batch_set_is_idempotent(batch_, cass_true);
    for (uint32_t i = 0; i < batch_size_; ++i) {
      CassStatement *stmt = NewStatement();
      CassError ce = cass_batch_add_statement(batch_, stmt);
      assert(ce == CASS_OK);
      cass_statement_free(stmt);
    }
  }
  last_point_ = std::chrono::system_clock::now();
  CassFuture *fut = cass_session_execute_batch(info_->session, batch_);
  CassError ce = cass_future_set_callback(fut, call_back, this);
  assert(ce == CASS_OK);
  cass_future_free(fut);
};

CassStatement *Worker::NewStatement() {
  CassStatement *stmt = cass_prepared_bind(info_->prepared);
  cass_statement_set_is_idempotent(stmt, cass_true);
  cass_statement_bind_int32_by_name(stmt, "partition_key", target_partition_);
  cass_statement_bind_string_by_name(stmt, "director", "AngLee");
  CassUuid uuid;
  cass_uuid_gen_random(info_->uuid_gen, &uuid);
  cass_statement_bind_uuid_by_name(stmt, "name", uuid);
  return stmt;
}

uint32_t Worker::AverageLatency() {
  if (batch_executed_ == 0) {
    return 0;
  }
  return latency_sum_ / batch_executed_;
}

void Worker::CheckValid() {
  uint32_t count = 0;
  for (uint32_t c : latency_stat_) {
    count += c;
  }
  assert(count == batch_executed_);
}
