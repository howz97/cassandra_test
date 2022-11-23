#include "cass_executor.h"

#include <cassert>
#include <iostream>

CassBatchExecutor::CassBatchExecutor(CassSession *session, int32_t batch_size)
    : session_(session), batch_size_(batch_size), current_batch_size_(0) {
  batch_ = cass_batch_new(CASS_BATCH_TYPE_LOGGED);
  cass_batch_set_is_idempotent(batch_, cass_true);
}

CassBatchExecutor::~CassBatchExecutor() {
  session_ = nullptr;
  if (batch_ != nullptr) {
    cass_batch_free(batch_);
    batch_ = nullptr;
  }
  for (auto fut_it = futures_.begin(); fut_it != futures_.end(); fut_it++) {
    if (fut_it->cass_future) {
      cass_future_free(fut_it->cass_future);
    }
    if (fut_it->cass_batch) {
      cass_batch_free(fut_it->cass_batch);
    }
  }
}

/**
 * @brief Add a new statement to batch.
 *
 * @param stmt
 * @return CassError
 */
CassError CassBatchExecutor::AddBatchStatement(CassStatement *stmt) {
  if (batch_ == nullptr) {
    batch_ = cass_batch_new(CASS_BATCH_TYPE_LOGGED);
    cass_batch_set_is_idempotent(batch_, cass_true);
  }

  assert(current_batch_size_ < batch_size_);
  CassError ce = cass_batch_add_statement(batch_, stmt);
  cass_statement_free(stmt);

  if (ce != CASS_OK) {
    std::cout << "Add Batch Statement Error: " << ce;
    return ce;
  }

  current_batch_size_++;
  return ce;
}

/**
 * @brief Send current batch request to Cassandra and add returned future to
 * futures_.
 *
 */
void CassBatchExecutor::Execute() {
  if (!batch_) {
    assert(current_batch_size_ == 0);
    return;
  }
  futures_.emplace_back(cass_session_execute_batch(session_, batch_), batch_,
                        std::chrono::system_clock::now());

  batch_ = nullptr;
  current_batch_size_ = 0;
}

/**
 * @brief Wait for previous batch futures to return. Remove success futures
 * from futures_ vector and keep the failing ones.
 */
CassError CassBatchExecutor::Wait_until(
    std::chrono::time_point<std::chrono::system_clock> end) {
  for (auto fut_it = futures_.begin(); fut_it != futures_.end();) {
    CassFuture *future = fut_it->cass_future;
    if (future == nullptr) // skip invalid future, it should be retried
    {
      fut_it++;
      continue;
    }
    CassError rc = cass_future_error_code(future);
    if (rc == CASS_OK) // remove successful futures
    {
      cass_batch_free(fut_it->cass_batch);
      batch_executed_++;
      auto lantency = std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now() - fut_it->start_point);
      sum_lantency_ms_ += lantency.count();
      fut_it = futures_.erase(fut_it);
    } else {
      // Print error sql for notice
      const char *error_message;
      size_t error_message_length;
      cass_future_error_message(future, &error_message, &error_message_length);
      std::cout << "CassBatchExecute Error: " << error_message;
      fut_it->cass_future = nullptr;
      ++fut_it;
    }
    // delete future pointer
    cass_future_free(future);
    if (std::chrono::system_clock::now() >= end) {
      return CASS_OK;
    }
  }

  if (futures_.size() == 0) {
    return CASS_OK;
  } else {
    return CASS_ERROR_LAST_ENTRY;
  }
}

/**
 * @brief Retry the failed batch requests in futures_ vector.
 *
 * @return CassError
 */
// CassError CassBatchExecutor::Retry() {
//   for (auto fut_it = futures_.begin(); fut_it != futures_.end(); fut_it++) {
//     if (fut_it->first == nullptr) {
//       fut_it->first = cass_session_execute_batch(session_, fut_it->second);
//     }
//   }

//   return Wait();
// }

bool CassBatchExecutor::HasStatements() { return current_batch_size_ > 0; }