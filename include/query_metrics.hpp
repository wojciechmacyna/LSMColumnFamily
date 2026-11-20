#pragma once

#include <algorithm>
#include <numeric>
#include <vector>

#include <spdlog/spdlog.h>

#include "experiment_types.hpp"

// Template definitions are kept in a header so they can be used from
// multiple translation units without requiring explicit instantiations.

template <typename T>
TimingStatistics calculateNumericStatistics(const std::vector<T>& values) {
  if (values.empty()) {
    spdlog::warn(
        "calculateNumericStatistics called with empty vector. Returning zeroed "
        "statistics.");
    return TimingStatistics{};
  }

  std::vector<T> sorted_values = values;
  std::sort(sorted_values.begin(), sorted_values.end());

  TimingStatistics stats;
  stats.min = static_cast<long long>(sorted_values.front());
  stats.max = static_cast<long long>(sorted_values.back());

  if (sorted_values.size() % 2 == 0) {
    stats.median =
        static_cast<double>(sorted_values[sorted_values.size() / 2 - 1] +
                            sorted_values[sorted_values.size() / 2]) /
        2.0;
  } else {
    stats.median = static_cast<double>(sorted_values[sorted_values.size() / 2]);
  }

  stats.average = static_cast<double>(std::accumulate(
                      sorted_values.begin(), sorted_values.end(), 0LL)) /
                  sorted_values.size();
  return stats;
}

template <typename T>
CountStatistics calculateCountStatistics(const std::vector<T>& values) {
  if (values.empty()) {
    spdlog::warn(
        "calculateCountStatistics called with empty vector. Returning zeroed "
        "statistics.");
    return CountStatistics{};
  }

  std::vector<T> sorted_values = values;
  std::sort(sorted_values.begin(), sorted_values.end());

  CountStatistics stats;
  stats.min = static_cast<size_t>(sorted_values.front());
  stats.max = static_cast<size_t>(sorted_values.back());

  if (sorted_values.size() % 2 == 0) {
    stats.median =
        static_cast<double>(sorted_values[sorted_values.size() / 2 - 1] +
                            sorted_values[sorted_values.size() / 2]) /
        2.0;
  } else {
    stats.median = static_cast<double>(sorted_values[sorted_values.size() / 2]);
  }

  // Use 0ULL for size_t accumulation to avoid overflow issues with large counts
  // if T is smaller than size_t and to ensure the type of the sum is large
  // enough.
  unsigned long long sum = 0;
  for (const T& val : sorted_values) {
    sum += val;
  }
  stats.average = static_cast<double>(sum) / sorted_values.size();
  return stats;
}
