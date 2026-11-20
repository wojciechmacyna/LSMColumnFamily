#include "query_generation.hpp"

#include <spdlog/spdlog.h>

#include <algorithm>
#include <chrono>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "algorithm.hpp"
#include "bloom_tree.hpp"
#include "db_manager.hpp"
#include "stopwatch.hpp"
#include "pattern_query_experiment.hpp"

extern std::atomic<size_t> gBloomCheckCount;
extern std::atomic<size_t> gLeafBloomCheckCount;
extern std::atomic<size_t> gSSTCheckCount;

// Helper function to generate dynamic patterns based on column count
// Generates patterns where we gradually increase the number of existing values
// (t) from left to right: For 4 columns: [n,n,n,n], [t,n,n,n], [t,t,n,n],
// [t,t,t,n], [t,t,t,t]


std::vector<PatternQueryResult> runPatternQueriesWithCsvData(
    DBManager& dbManager, const std::map<std::string, BloomTree>& hierarchies,
    const std::vector<std::string>& columns, size_t dbSize, const DistributionConfig& config) {
  std::vector<PatternQueryResult> results;

  // Setup query trees
  if (hierarchies.empty() || columns.empty()) {
    spdlog::warn(
        "runPatternQueriesWithCsvData: Hierarchies map or "
        "columns vector is empty, skipping query execution.");
    return results;
  }

  std::vector<BloomTree> queryTrees;
  queryTrees.reserve(columns.size());
  for (const auto& column : columns) {
    auto it = hierarchies.find(column);
    if (it == hierarchies.end()) {
      spdlog::error(
          "runPatternQueriesWithCsvData: Hierarchy for column "
          "'{}' not found. Skipping query execution.",
          column);
      return results;
    }
    queryTrees.push_back(it->second);
  }

  if (queryTrees.empty()) {
    spdlog::error(
        "runPatternQueriesWithCsvData: No query trees were "
        "prepared, possibly due to missing hierarchies. Skipping query "
        "execution.");
    return results;
  }

  // Define non-existing record patterns
  // t = existing value, n = non-existing value
  std::vector<std::vector<bool>> patterns =
      generateDynamicPatterns(columns.size());

  // Log the generated patterns for debugging
  spdlog::info("Generated {} patterns for {} columns:", patterns.size(),
               columns.size());
  for (size_t i = 0; i < patterns.size(); ++i) {
    std::string patternStr = "[";
    for (size_t j = 0; j < patterns[i].size(); ++j) {
      patternStr += (patterns[i][j] ? "t" : "n");
      if (j < patterns[i].size() - 1) patternStr += ",";
    }
    patternStr += "]";
    spdlog::info("Pattern {}: {}", i, patternStr);
  }

  std::random_device rd;
  std::mt19937 generator(rd());
  
  // Previous implementation
  // std::uniform_int_distribution<size_t> distribution(1, dbSize);

  // New implementation: Normal distribution takes from all values in db
  // Ziphian only from the inserted values (values in Ziphian can be repeated.)

  size_t cardinality=0;
  if (config.type == DataDistribution::Normal) {
    cardinality=dbSize;
  }
  else {
    cardinality =
      std::max<size_t>(1, std::min(config.cardinality, dbSize));
  }

  std::uniform_int_distribution<size_t> distribution(1, cardinality);
 
  
  StopWatch stopwatch;
  std::vector<std::string> currentExpectedValues;
  results.reserve(patterns.size());

  for (int i = 0; i < patterns.size(); ++i) {
    currentExpectedValues.clear();
    currentExpectedValues.reserve(columns.size());

    const auto& pattern = patterns[i];

    size_t existingId = distribution(generator);

    std::string existingValueSuffix = "_value" + std::to_string(existingId);
    std::string nonExistingValueSuffix = "_wrong" + std::to_string(existingId);

    size_t existingCount = 0;
    for (size_t colIdx = 0; colIdx < columns.size() && colIdx < pattern.size();
         ++colIdx) {
      if (pattern[colIdx]) {
        existingCount++;
      }
    }

    double percentageExisting =
        static_cast<double>(existingCount) / columns.size() * 100.0;

    spdlog::info("Run: Using pattern index {} with {}% existing columns", i,
                 percentageExisting);

    // Generate values based on pattern
    for (size_t colIdx = 0; colIdx < columns.size() && colIdx < pattern.size();
         ++colIdx) {
      if (pattern[colIdx]) {
        // existing value
        currentExpectedValues.push_back(columns[colIdx] + existingValueSuffix);
      } else {
        // non-existing value
        currentExpectedValues.push_back(columns[colIdx] +
                                        nonExistingValueSuffix);
      }
    }

    PatternQueryResult result;
    result.percent = percentageExisting;

    // --- Hierarchical Multi-Column Query ---
    gBloomCheckCount = 0;
    gLeafBloomCheckCount = 0;
    gSSTCheckCount = 0;
    stopwatch.start();
    [[maybe_unused]] std::vector<std::string> hierarchicalMatches =
        multiColumnQueryHierarchical(queryTrees, columns, currentExpectedValues, "", "",
                                     dbManager);
    stopwatch.stop();
    result.hierarchicalMultiTime = stopwatch.elapsedMicros();
    result.multiCol_bloomChecks = gBloomCheckCount.load();
    result.multiCol_leafBloomChecks = gLeafBloomCheckCount.load();
    result.multiCol_sstChecks = gSSTCheckCount.load();

    // Calculate derived metrics
    result.multiCol_nonLeafBloomChecks = result.multiCol_bloomChecks - result.multiCol_leafBloomChecks;
    
    // Calculate per-column averages
    double numCols = static_cast<double>(columns.size());
    result.multiCol_bloomChecksPerColumn = static_cast<double>(result.multiCol_bloomChecks) / numCols;
    result.multiCol_leafBloomChecksPerColumn = static_cast<double>(result.multiCol_leafBloomChecks) / numCols;
    result.multiCol_sstChecksPerColumn = static_cast<double>(result.multiCol_sstChecks) / numCols;
    result.multiCol_nonLeafBloomChecksPerColumn = static_cast<double>(result.multiCol_nonLeafBloomChecks) / numCols;

    // --- Hierarchical Single Column Query ---
    gBloomCheckCount = 0;
    gLeafBloomCheckCount = 0;
    gSSTCheckCount = 0;
    stopwatch.start();
    [[maybe_unused]] std::vector<std::string> singlehierarchyMatches =
        dbManager.findUsingSingleHierarchy(queryTrees[0], columns,
                                           currentExpectedValues);
    stopwatch.stop();
    result.hierarchicalSingleTime = stopwatch.elapsedMicros();
    result.singleCol_bloomChecks = gBloomCheckCount.load();
    result.singleCol_leafBloomChecks = gLeafBloomCheckCount.load();
    result.singleCol_sstChecks = gSSTCheckCount.load();

    // Calculate derived metrics
    result.singleCol_nonLeafBloomChecks = result.singleCol_bloomChecks - result.singleCol_leafBloomChecks;
    
    // Calculate per-column averages
    result.singleCol_bloomChecksPerColumn = static_cast<double>(result.singleCol_bloomChecks) ;
    result.singleCol_leafBloomChecksPerColumn = static_cast<double>(result.singleCol_leafBloomChecks) ;
    result.singleCol_sstChecksPerColumn = static_cast<double>(result.singleCol_sstChecks) ;
    result.singleCol_nonLeafBloomChecksPerColumn = static_cast<double>(result.singleCol_nonLeafBloomChecks) ;

    results.push_back(result);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  return results;
}