#include <cassert>
#include <chrono>
#include <iostream>
#include <stdio.h>
#include <unistd.h>

#include "cass/include/cassandra.h"
#include "worker.h"

#define AVARAGE_LOOP 1

CassSession *session = NULL;
const CassPrepared *prepared = NULL;
CassUuidGen *uuid_gen = NULL;

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

struct Result {
  int32_t qps = 0;
  int32_t ave_latency = 0;
  uint32_t latency_stat[STAT_LEN] = {};
  uint32_t batch_executed = 0;

  uint32_t LatencyPercentiles(double p) {
    assert(p <= 1);
    uint32_t left = batch_executed;
    int32_t i = STAT_LEN - 1;
    for (; i >= 0; --i) {
      if (double(left - latency_stat[i]) / double(batch_executed) >= p) {
        left -= latency_stat[i];
      } else {
        break;
      }
    }
    return (i + 1) * STAT_GRAIN;
  };
};

Result *generate_result(std::vector<Worker> workers) {
  Result *result = new Result;
  uint32_t valid_worker = 0;
  for (Worker &w : workers) {
    if (w.batch_executed_) {
      w.CheckValid();
      valid_worker++;
      result->qps += w.QPS();
      result->ave_latency += w.AverageLatency();
      for (uint32_t i = 0; i < STAT_LEN; ++i) {
        result->latency_stat[i] += w.latency_stat_[i];
      }
      result->batch_executed += w.batch_executed_;
    } else {
      printf("worker %d executed 0 batch\n", w.id_);
    }
  }
  if (valid_worker) {
    result->ave_latency /= valid_worker;
  }
  return result;
}

Result *measure_qps(uint32_t num_fut, uint32_t batch_size, uint32_t num_part,
                    bool clean_tbl) {
  // printf("measure_qps(%d, %d, %d, %d)\n", num_fut, batch_size, num_part,
  //        clean_tbl);
  assert(num_fut);
  assert(batch_size);
  assert(num_part);
  assert(num_part <= num_fut);
  if (clean_tbl) {
    assert(create_table() == CASS_OK);
  }

  CommonInfo info;
  {
    info.session = session;
    info.prepared = prepared;
    info.uuid_gen = uuid_gen;
  }
  std::vector<Worker> workers;
  workers.reserve(num_fut);
  for (uint32_t id = 0; id < num_fut; ++id) {
    workers.emplace_back(id, id % num_part, batch_size, &info);
  }
  {
    std::unique_lock lk(info.mu);
    info.cv.wait(lk, [&] { return info.finished == num_fut; });
  }

  if (clean_tbl) {
    execute_query(session, "DROP TABLE test.films");
  }
  return generate_result(workers);
}

void test_batch_size(uint16_t max_batch, uint16_t min_batch) {
  std::cout << "-----------------"
            << "test_batch_size" << std::endl;
  for (int bs = max_batch; bs >= min_batch; bs /= 2) {
    Result *result = measure_qps(3, bs, 1, true);
    printf("batch_size=%d : ", bs);
    printf("QPS=%d, Latency[ave=%dms, 99p=%dms]\n", result->qps,
           result->ave_latency, result->LatencyPercentiles(0.99));
    // for (uint32_t i = 0; i < STAT_LEN; ++i) {
    //   if (result->latency_stat[i]) {
    //     printf("(%d-%dms)=%d,", i * STAT_GRAIN, (i + 1) * STAT_GRAIN,
    //            result->latency_stat[i]);
    //   }
    // }
    // std::cout << std::endl;
  }
  std::cout << "----------------------------------" << std::endl;
}

void test_num_future(const int max_fut, const int min_fut) {
  std::cout << "-----------------"
            << "test_num_future" << std::endl;
  for (int nf = max_fut; nf >= min_fut; nf /= 2) {
    Result *result = measure_qps(nf, 2, 1, true);
    fprintf(stdout, "num_fut=%d :", nf);
    printf("QPS=%d, Latency[ave=%dms, 99p=%dms]\n", result->qps,
           result->ave_latency, result->LatencyPercentiles(0.99));
  }
  std::cout << "----------------------------------" << std::endl;
}

void test_partition_size(const int max_part, const int min_part) {
  std::cout << "-----------------"
            << "test_partition_size" << std::endl;
  for (int p = max_part; p >= min_part; p /= 2) {
    Result *result = measure_qps(max_part, 16, p, true);
    fprintf(stdout, "num_part=%d :", p);
    printf("QPS=%d, Latency[ave=%dms, 99p=%dms]\n", result->qps,
           result->ave_latency, result->LatencyPercentiles(0.99));
  }
  std::cout << "----------------------------------" << std::endl;
}

void test_continuous_insert(const int32_t looptimes) {
  assert(create_table() == CASS_OK);
  std::cout << "-----------------"
            << "test_continuous_insert" << std::endl;
  for (int32_t i = 0; i < looptimes; ++i) {
    Result *result = measure_qps(64, 256, 1, false);
    printf("QPS=%d, Latency[ave=%dms, 99p=%dms]\n", result->qps,
           result->ave_latency, result->LatencyPercentiles(0.99));
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
    test_batch_size(65535, 128);
    // test_num_future(16384 * 2, 512);
    // test_partition_size(2048, 1);
    // test_continuous_insert(10);
    cass_prepared_free(prepared);
  }

  cass_uuid_gen_free(uuid_gen);
  cass_cluster_free(cluster);
  cass_session_free(session);

  return 0;
}