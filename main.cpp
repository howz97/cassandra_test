#include <cassert>
#include <chrono>
#include <iostream>
#include <stdio.h>
#include <unistd.h>

#include "cass/include/cassandra.h"
#include "cass_executor.h"

#define AVARAGE_LOOP 1
#define QPS_SECONDS 2

CassSession *session = NULL;
const CassPrepared *prepared = NULL;
CassUuidGen *uuid_gen = NULL;
const char *directors[] = {"Bertluci", "AngLee", "WenJiang", "WoodyAllen"};

void print_error(CassFuture *future) {
  const char *message;
  size_t message_length;
  cass_future_error_message(future, &message, &message_length);
  fprintf(stderr, "Error: %.*s\n", (int)message_length, message);
}

CassCluster *create_cluster(const char *hosts) {
  CassCluster *cluster = cass_cluster_new();
  cass_cluster_set_contact_points(cluster, hosts);
  return cluster;
}

CassError connect_session(CassSession *session, const CassCluster *cluster) {
  CassError rc = CASS_OK;
  CassFuture *future = cass_session_connect(session, cluster);

  cass_future_wait(future);
  rc = cass_future_error_code(future);
  if (rc != CASS_OK) {
    print_error(future);
  }
  cass_future_free(future);

  return rc;
}

CassError execute_query(CassSession *session, const char *query) {
  CassError rc = CASS_OK;
  CassStatement *statement = cass_statement_new(query, 0);

  CassFuture *future = cass_session_execute(session, statement);
  cass_future_wait(future);

  rc = cass_future_error_code(future);
  if (rc != CASS_OK) {
    print_error(future);
  }

  cass_statement_free(statement);
  cass_future_free(future);
  return rc;
}

CassError prepare_insert(CassSession *session, const CassPrepared **prepared) {
  CassError rc = CASS_OK;
  const char *query =
      "INSERT INTO test.films (partition_key, director, name, published, showing) \
   VALUES (?, ?, ?, 2022, true);";

  CassFuture *future = cass_session_prepare(session, query);
  cass_future_wait(future);

  rc = cass_future_error_code(future);
  if (rc != CASS_OK) {
    print_error(future);
  } else {
    *prepared = cass_future_get_prepared(future);
  }

  cass_future_free(future);

  return rc;
}

CassError create_table() {
  return execute_query(session, "CREATE TABLE IF NOT EXISTS test.films ( \
                                                     partition_key int, \
                                                     director text, \
                                                     name uuid, \
                                                     published int, \
                                                     showing boolean, \
                                                     PRIMARY KEY (partition_key, director, name))");
}

CassStatement *insert_statement(int32_t pk, const char *director) {
  CassStatement *stmt = cass_prepared_bind(prepared);
  cass_statement_set_is_idempotent(stmt, cass_true);
  cass_statement_bind_int32_by_name(stmt, "partition_key", pk);
  cass_statement_bind_string_by_name(stmt, "director", director);
  CassUuid uuid;
  cass_uuid_gen_random(uuid_gen, &uuid);
  cass_statement_bind_uuid_by_name(stmt, "name", uuid);
  return stmt;
}

struct Result {
  int32_t qps = 0;
  int32_t lantency_ms = 0;
};

Result measure_qps(int num_fut, int batch_size, int num_part, bool clean_tbl) {
  assert(num_fut);
  assert(batch_size);
  assert(num_part);
  assert(num_part <= num_fut);

  if (clean_tbl) {
    assert(create_table() == CASS_OK);
  }
  CassBatchExecutor executor(session, batch_size);

  auto end =
      std::chrono::system_clock::now() + std::chrono::seconds(QPS_SECONDS);
  while (std::chrono::system_clock::now() < end) {
    for (int i = 0; i < num_fut; ++i) {
      for (int j = 0; j < batch_size; ++j) {
        auto stmt = insert_statement(i % num_part, "AngLee");
        assert(executor.AddBatchStatement(stmt) == CASS_OK);
      }
      executor.Execute();
    }
    CassError ce = executor.Wait_until(end);
    assert(ce == CASS_OK);
  }
  if (clean_tbl) {
    execute_query(session, "DROP TABLE test.films");
  }
  Result result;
  result.qps = (executor.BatchExecuted() * batch_size) / QPS_SECONDS;
  result.lantency_ms = executor.AvarageLantency();
  return result;
}

void test_batch_size(uint16_t max_batch, uint16_t min_batch) {
  std::cout << "test_batch_size"
            << "-----------------" << std::endl;
  for (int bs = max_batch; bs >= min_batch; bs /= 2) {
    fprintf(stdout, "batch_size=%d :", bs);
    Result result;
    for (int i = 0; i < AVARAGE_LOOP; ++i) {
      Result r = measure_qps(32, bs, 1, true);
      result.qps += r.qps;
      result.lantency_ms += r.lantency_ms;
    }
    result.qps /= AVARAGE_LOOP;
    result.lantency_ms /= AVARAGE_LOOP;
    fprintf(stdout, "[QPS=%d, Lantency=%dms]\n", result.qps,
            result.lantency_ms);
  }
  std::cout << "----------------------------------" << std::endl;
}

void test_num_future(const int max_fut, const int min_fut) {
  std::cout << "test_num_future"
            << "-----------------" << std::endl;
  for (int nf = max_fut; nf >= min_fut; nf /= 2) {
    fprintf(stdout, "num_fut=%d :", nf);
    Result result;
    for (int i = 0; i < AVARAGE_LOOP; ++i) {
      Result r = measure_qps(nf, 2, 1, true);
      result.qps += r.qps;
      result.lantency_ms += r.lantency_ms;
    }
    result.qps /= AVARAGE_LOOP;
    result.lantency_ms /= AVARAGE_LOOP;
    fprintf(stdout, "[QPS=%d, Lantency=%dms]\n", result.qps,
            result.lantency_ms);
  }
  std::cout << "----------------------------------" << std::endl;
}

void test_partition_size(const int max_part, const int min_part) {
  std::cout << "test_partition_size"
            << "-----------------" << std::endl;
  for (int p = max_part; p >= min_part; p /= 2) {
    fprintf(stdout, "num_part=%d :", p);
    Result result;
    for (int i = 0; i < AVARAGE_LOOP; ++i) {
      Result r = measure_qps(max_part, 16, p, true);
      result.qps += r.qps;
      result.lantency_ms += r.lantency_ms;
    }
    result.qps /= AVARAGE_LOOP;
    result.lantency_ms /= AVARAGE_LOOP;
    fprintf(stdout, "[QPS=%d, Lantency=%dms]\n", result.qps,
            result.lantency_ms);
  }
  std::cout << "----------------------------------" << std::endl;
}

void test_continuous_insert(const int32_t looptimes) {
  assert(create_table() == CASS_OK);
  std::cout << "test_continuous_insert"
            << "-----------------" << std::endl;
  for (int32_t i = 0; i < looptimes; ++i) {
    Result result = measure_qps(64, 256, 1, false);
    fprintf(stdout, "[QPS=%d, Lantency=%dms]\n", result.qps,
            result.lantency_ms);
    sleep(1);
  }
  std::cout << "----------------------------------" << std::endl;
  execute_query(session, "DROP TABLE test.films");
}

int main(int argc, char *argv[]) {
  CassCluster *cluster = NULL;
  const char *hosts = "127.0.0.1";
  if (argc > 1) {
    hosts = argv[1];
  }
  session = cass_session_new();
  uuid_gen = cass_uuid_gen_new();
  cluster = create_cluster(hosts);

  if (connect_session(session, cluster) != CASS_OK) {
    cass_uuid_gen_free(uuid_gen);
    cass_cluster_free(cluster);
    cass_session_free(session);
    return -1;
  }

  execute_query(session,
                "CREATE KEYSPACE IF NOT EXISTS test WITH replication = { \
                                                        'class': 'SimpleStrategy', \
                                                        'replication_factor': '1' }");
  create_table();
  if (prepare_insert(session, &prepared) == CASS_OK) {
    // test_batch_size(65535, 32);
    // test_num_future(16384 * 2, 512);
    // test_partition_size(2048, 1);
    test_continuous_insert(10);
    cass_prepared_free(prepared);
  }

  cass_uuid_gen_free(uuid_gen);
  cass_cluster_free(cluster);
  cass_session_free(session);

  return 0;
}