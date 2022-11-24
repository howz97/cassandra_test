#include <cassert>
#include <chrono>
#include <iostream>
#include <stdio.h>
#include <unistd.h>

#include "cass/include/cassandra.h"
#include "worker.h"

#define AVARAGE_LOOP 1
#define TABLE_NAME "films"

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
  char buffer[256];
  sprintf(
      buffer,
      "INSERT INTO test.%s (partition_key, director, name, published, showing) \
          VALUES (?, ?, ?, 2022, true);",
      TABLE_NAME);
  CassFuture *future = cass_session_prepare(session, buffer);
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
  char buffer[1024];
  int n = sprintf(buffer, "CREATE TABLE IF NOT EXISTS test.%s ( \
                                                     partition_key int, \
                                                     director text, \
                                                     name uuid, \
                                                     published int, \
                                                     showing boolean, \
                                                     PRIMARY KEY (partition_key, director, name))",
                  TABLE_NAME);
  assert(n > 0);
  return execute_query(session, buffer);
}

CassError drop_table() {
  char buffer[256];
  int n = sprintf(buffer, "DROP TABLE test.%s", TABLE_NAME);
  assert(n > 0);
  return execute_query(session, buffer);
}

struct Result {
  int32_t qps = 0;
  int32_t ave_latency = 0;
  uint32_t latency_stat[STAT_LEN] = {};
  uint32_t batch_executed = 0;

  void Calculate(std::vector<Worker> workers) {
    uint32_t valid_worker = 0;
    for (Worker &w : workers) {
      if (w.batch_executed_) {
        w.CheckValid();
        valid_worker++;
        qps += w.QPS();
        ave_latency += w.AverageLatency();
        for (uint32_t i = 0; i < STAT_LEN; ++i) {
          latency_stat[i] += w.latency_stat_[i];
        }
        batch_executed += w.batch_executed_;
      } else {
        printf("worker %d executed 0 batch\n", w.id_);
      }
    }
    if (valid_worker) {
      ave_latency /= valid_worker;
    }
  }
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
  }
};

void measure(uint32_t num_fut, uint32_t batch_size, uint32_t num_part,
             bool clean_tbl, Result *result) {
  // printf("measure(%d, %d, %d, %d)\n", num_fut, batch_size, num_part,
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
    drop_table();
  }
  result->Calculate(workers);
}

void test_batch_size(uint16_t max_batch, uint16_t min_batch) {
  std::cout << "-----------------"
            << "test_batch_size" << std::endl;
  for (int bs = max_batch; bs >= min_batch; bs /= 2) {
    Result result;
    measure(3, bs, 1, true, &result);
    printf("batch_size=%d : ", bs);
    printf("QPS=%d, Latency[ave=%dms, 99p=%dms]\n", result.qps,
           result.ave_latency, result.LatencyPercentiles(0.99));
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
  uint32_t batch_size = 2;
  uint32_t num_part = 1;
  printf("-----------------test_num_partition batch_size=%d, num_part=%d\n",
         batch_size, num_part);
  for (int nf = max_fut; nf >= min_fut; nf /= 2) {
    Result result;
    measure(nf, batch_size, num_part, true, &result);
    fprintf(stdout, "num_fut=%d :", nf);
    printf("QPS=%d, Latency[ave=%dms, 99p=%dms]\n", result.qps,
           result.ave_latency, result.LatencyPercentiles(0.99));
  }
  std::cout << "----------------------------------" << std::endl;
}

void test_num_partition(const int max_part, const int min_part) {
  uint32_t concurrency = max_part;
  uint32_t batch_size = 1024;
  printf("-----------------test_num_partition concurrency=%d, batch_size=%d\n",
         concurrency, batch_size);
  for (int p = max_part; p >= min_part; p /= 2) {
    Result result;
    measure(concurrency, batch_size, p, true, &result);
    fprintf(stdout, "num_part=%d :", p);
    printf("QPS=%d, Latency[ave=%dms, 99p=%dms]\n", result.qps,
           result.ave_latency, result.LatencyPercentiles(0.99));
  }
  std::cout << "----------------------------------" << std::endl;
}

void test_continuous_insert(const int32_t looptimes) {
  // assert(create_table() == CASS_OK);
  uint32_t concurrency = 32;
  uint32_t batch_size = 1024;
  uint32_t num_part = 1;
  printf("-----------------test_continuous_insert concurrency=%d, "
         "batch_size=%d, num_part=%d\n",
         concurrency, batch_size, num_part);
  for (int32_t i = 0; i < looptimes; ++i) {
    Result result;
    measure(concurrency, batch_size, num_part, false, &result);
    printf("QPS=%d, Latency[ave=%dms, 99p=%dms]\n", result.qps,
           result.ave_latency, result.LatencyPercentiles(0.99));
    sleep(1);
  }
  std::cout << "----------------------------------" << std::endl;
  // assert(drop_table() == CASS_OK);
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
    // test_batch_size(65535, 128);
    // test_num_future(16384 * 2, 512);
    // test_partition_size(2048, 1);
    test_continuous_insert(100);
    cass_prepared_free(prepared);
  }

  cass_uuid_gen_free(uuid_gen);
  cass_cluster_free(cluster);
  cass_session_free(session);

  return 0;
}