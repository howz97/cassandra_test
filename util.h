#pragma once

#include <cassert>
#include <cstdint>
#define STAT_GRAIN 2
#define STAT_MAX 10000
#define STAT_LEN STAT_MAX / STAT_GRAIN
#define RUN_SECONDS 30

#define TABLE_NAME "films"

class LantencyStat {
public:
  void Record(uint32_t latency) {
    counter_++;
    latency_sum_ += latency;
    if (latency >= STAT_MAX) {
      latency_stat_[STAT_LEN - 1]++;
    } else {
      latency_stat_[latency / STAT_GRAIN]++;
    }
  };
  uint32_t LatencyPercentiles(double p) {
    assert(p <= 1);
    uint32_t left = counter_;
    int32_t i = STAT_LEN - 1;
    for (; i >= 0; --i) {
      if (double(left - latency_stat_[i]) / double(counter_) >= p) {
        left -= latency_stat_[i];
      } else {
        break;
      }
    }
    return (i + 1) * STAT_GRAIN;
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
  LantencyStat &operator+=(const LantencyStat &rhs) {
    this->counter_ += rhs.counter_;
    this->latency_sum_ += rhs.latency_sum_;
    for (uint32_t i = 0; i < STAT_LEN; ++i) {
      this->latency_stat_[i] += rhs.latency_stat_[i];
    }
    return *this;
  };
  uint32_t Count() { return counter_; }

private:
  uint32_t latency_stat_[STAT_LEN] = {}; // ms
  uint32_t latency_sum_{0};              // ms
  uint32_t counter_{0};
};