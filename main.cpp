#include <cassert>
#include <chrono>
#include <iostream>
#include <stdio.h>
#include <unistd.h>

#include "cass/include/cassandra.h"
#include "cxxopts/include/cxxopts.hpp"
#include "spdlog/include/spdlog/sinks/basic_file_sink.h"
#include "spdlog/include/spdlog/spdlog.h"
#include "worker.h"

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

  void Calculate(std::vector<Worker> &workers) {
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
        spdlog::warn("worker {} executed 0 batch\n", w.id_);
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
  uint32_t concurrency = 1;
  uint32_t num_part = 1;
  spdlog::info("-----------------test_batch_size: concurrency={}, num_part={}",
               concurrency, num_part);
  for (int bs = max_batch; bs >= min_batch; bs /= 2) {
    Result result;
    measure(concurrency, bs, num_part, true, &result);
    spdlog::info("batch_size={} : QPS={}, Latency[ave={}ms, 99p={}ms]", bs,
                 result.qps, result.ave_latency,
                 result.LatencyPercentiles(0.99));
  }
  spdlog::info("-------------------------------------------------");
}

void test_num_future(const int max_fut, const int min_fut) {
  uint32_t batch_size = 1024;
  uint32_t num_part = 1024;
  spdlog::info("-----------------test_num_future: batch_size={}, num_part={}",
               batch_size, num_part);
  for (int nf = max_fut; nf >= min_fut; nf /= 2) {
    Result result;
    measure(nf, batch_size, num_part, true, &result);
    spdlog::info("num_fut={} : QPS={}, Latency[ave={}ms, 99p={}ms]", nf,
                 result.qps, result.ave_latency,
                 result.LatencyPercentiles(0.99));
  }
  spdlog::info("-------------------------------------------------");
}

void test_num_partition(const int max_part, const int min_part) {
  uint32_t concurrency = 1024;
  uint32_t batch_size = 1024;
  spdlog::info(
      "-----------------test_num_partition: concurrency={}, batch_size={}",
      concurrency, batch_size);
  for (int p = max_part; p >= min_part;) {
    Result result;
    measure(concurrency, batch_size, p, true, &result);
    spdlog::info("num_part={} : QPS={}, Latency[ave={}ms, 99p={}ms]", p,
                 result.qps, result.ave_latency,
                 result.LatencyPercentiles(0.99));
    if (p >= 16) {
      p /= 2;
    } else {
      p -= 2;
    }
  }
  spdlog::info("-------------------------------------------------");
}

void test_continuous_insert(uint32_t concurrency, uint32_t batch_size,
                            uint32_t num_part) {
  // assert(create_table() == CASS_OK);
  int32_t looptimes = 1;
  spdlog::info("-----------------test_continuous_insert: concurrency={}, "
               "batch_size={}, num_part={}",
               concurrency, batch_size, num_part);
  for (int32_t i = 0; i < looptimes; ++i) {
    Result result;
    measure(concurrency, batch_size, num_part, false, &result);
    spdlog::info("QPS={}, Latency[ave={}ms, 99p={}ms]", result.qps,
                 result.ave_latency, result.LatencyPercentiles(0.99));
  }
  spdlog::info("-------------------------------------------------");
  // assert(drop_table() == CASS_OK);
}

void cmd_run(int argc, char *argv[]) {
  cxxopts::Options options("test", "A brief description");
  options.add_options()("cl", "Concurrency level", cxxopts::value<uint32_t>())(
      "bsize", "Batch size", cxxopts::value<uint32_t>())(
      "npart", "Number of partitions", cxxopts::value<uint32_t>());
  auto result = options.parse(argc, argv);
  uint32_t concurrency = result["cl"].as<uint32_t>();
  uint32_t batch_size = result["bsize"].as<uint32_t>();
  uint32_t num_part = result["npart"].as<uint32_t>();
  assert(concurrency);
  assert(batch_size);
  assert(num_part);
  char logfile[64];
  int n = sprintf(logfile, "../output/%d_%d_%dp.log", concurrency, batch_size,
                  num_part);
  assert(n);
  auto logger = spdlog::basic_logger_mt("test_logger", logfile, false);
  spdlog::set_default_logger(logger);
  spdlog::flush_on(spdlog::level::info);
  test_continuous_insert(concurrency, batch_size, num_part);
}

int main(int argc, char *argv[]) {
  CassCluster *cluster = NULL;
  const char *hosts = "127.0.0.1";
  session = cass_session_new();
  uuid_gen = cass_uuid_gen_new();
  cluster = create_cluster(hosts);
  cass_cluster_set_num_threads_io(cluster, 2);

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
    assert(argc >= 2);
    std::string subcmd = argv[1];
    if (subcmd == "run") {
      cmd_run(argc, argv);
    } else if (subcmd == "batch") {
      test_batch_size(65535, 128);
    } else if (subcmd == "concurrency") {
      test_num_future(256, 1);
    } else if (subcmd == "partition") {
      test_num_partition(8, 1);
    } else {
      std::cout << "invalid subcmd" << std::endl;
    }
    cass_prepared_free(prepared);
  }

  cass_uuid_gen_free(uuid_gen);
  cass_cluster_free(cluster);
  cass_session_free(session);

  return 0;
}