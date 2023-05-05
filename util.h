#pragma once

#include <cassert>
#include <cstdint>
#define RUN_SECONDS 30
#define TABLE_NAME "films"

// statistics of latency in milliseconds
class LatencyStat {
  static constexpr uint32_t bucket_size = 5;
  static constexpr uint32_t bucket_num = 1000;

public:
  void Record(uint32_t latency) {
    counter_++;
    latency_sum_ += latency;
    if (latency >= bucket_size * bucket_num) {
      latency_stat_[bucket_num - 1]++;
    } else {
      latency_stat_[latency / bucket_size]++;
    }
  };
  uint32_t LatencyPercentiles(double p) {
    assert(p <= 1);
    uint32_t left = counter_;
    int32_t i = bucket_num - 1;
    for (; i >= 0; --i) {
      if (double(left - latency_stat_[i]) / double(counter_) >= p) {
        left -= latency_stat_[i];
      } else {
        break;
      }
    }
    return (i + 1) * bucket_size;
  }
  uint32_t AverageLatency() {
    if (counter_ == 0) {
      return 0;
    }
    return latency_sum_ / counter_;
  }
  void CheckValid() {
    uint32_t count = 0;
    for (uint32_t c : latency_stat_) {
      count += c;
    }
    assert(count == counter_);
  }
  LatencyStat &operator+=(const LatencyStat &rhs) {
    this->counter_ += rhs.counter_;
    this->latency_sum_ += rhs.latency_sum_;
    for (uint32_t i = 0; i < bucket_num; ++i) {
      this->latency_stat_[i] += rhs.latency_stat_[i];
    }
    return *this;
  };
  uint32_t Count() { return counter_; }

private:
  uint32_t latency_stat_[bucket_num] = {};
  uint32_t latency_sum_{0};
  uint32_t counter_{0};
};