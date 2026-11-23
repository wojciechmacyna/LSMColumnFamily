#include "experiment_runner.hpp"

#include "query_execution.hpp"
#include "query_generation.hpp"
#include "query_metrics.hpp"

#include <spdlog/spdlog.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <random>
#include <thread>
#include <utility>
#include <vector>

#include "algorithm.hpp"
#include "bloom_tree.hpp"
#include "db_manager.hpp"
#include "stopwatch.hpp"

extern std::atomic<size_t> gBloomCheckCount;
extern std::atomic<size_t> gLeafBloomCheckCount;
extern std::atomic<size_t> gSSTCheckCount;

AggregatedQueryTimings runStandardQueries(
    DBManager& dbManager, const std::map<std::string, BloomTree>& hierarchies,
    const std::vector<std::string>& columns, size_t dbSize, int numRuns,
    bool skipDbScan, const DistributionConfig& config, const std::map<std::string, std::vector<std::string>>*
    columnSstFiles) {
  AggregatedQueryTimings aggregated_timings;
  if (numRuns <= 0) {
    spdlog::warn(
        "runStandardQueries: numRuns is {} (<=0). Returning empty statistics.",
        numRuns);
    return aggregated_timings;
  }

std::vector<long long> globalScanTimes, hierarchicalMultiTimes,
    hierarchicalSingleTimes, hashIndexTimeStats,
    hierarchicalHashMultiTimes;

  std::vector<size_t> multiCol_bloomChecks_vec;
  std::vector<size_t> multiCol_leafBloomChecks_vec;
  std::vector<size_t> multiCol_sstChecks_vec;
  std::vector<size_t> multiCol_nonLeafBloomChecks_vec;
  std::vector<size_t> singleCol_bloomChecks_vec;
  std::vector<size_t> singleCol_leafBloomChecks_vec;
  std::vector<size_t> singleCol_sstChecks_vec;
  std::vector<size_t> singleCol_nonLeafBloomChecks_vec;

  // Reserve space in vectors
  globalScanTimes.reserve(numRuns);
  hierarchicalMultiTimes.reserve(numRuns);
  hierarchicalSingleTimes.reserve(numRuns);
  hashIndexTimeStats.reserve(numRuns);
  hierarchicalHashMultiTimes.reserve(numRuns);

  multiCol_bloomChecks_vec.reserve(numRuns);
  multiCol_leafBloomChecks_vec.reserve(numRuns);
  multiCol_sstChecks_vec.reserve(numRuns);
  multiCol_nonLeafBloomChecks_vec.reserve(numRuns);
  singleCol_bloomChecks_vec.reserve(numRuns);
  singleCol_leafBloomChecks_vec.reserve(numRuns);
  singleCol_sstChecks_vec.reserve(numRuns);
  singleCol_nonLeafBloomChecks_vec.reserve(numRuns);

  // --- Setup for generating expected values ---
  // This part is outside the loop as queryTrees are constant for all runs.
  if (hierarchies.empty() || columns.empty()) {
    spdlog::warn(
        "runStandardQueries: Hierarchies map or columns vector is empty, "
        "skipping query execution.");
    return aggregated_timings;
  }

  std::vector<BloomTree> queryTrees;
  queryTrees.reserve(columns.size());
  for (const auto& column : columns) {
    auto it = hierarchies.find(column);
    if (it == hierarchies.end()) {
      spdlog::error(
          "runStandardQueries: Hierarchy for column '{}' not found. Skipping "
          "query execution.",
          column);
      return AggregatedQueryTimings{};
    }
    queryTrees.push_back(it->second);
  }

  if (queryTrees.empty()) {
    spdlog::error(
        "runStandardQueries: No query trees were prepared, possibly due to "
        "missing hierarchies. Skipping query execution.");
    return aggregated_timings;
  }

  std::random_device rd;
  std::mt19937 generator(rd());


 // Prev implementation
  //std::uniform_int_distribution<size_t> distribution(1, dbSize);


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
  auto zipfDist = zipfDistribution(config.exponent, cardinality, dbSize);

  StopWatch stopwatch;
  std::vector<std::string> currentExpectedValues;

  for (int i = 0; i < numRuns; ++i) {
    currentExpectedValues.clear();
    currentExpectedValues.reserve(columns.size());

    size_t existingId;

    if (config.type == DataDistribution::Normal) {
    existingId = distribution(generator);
    } else {
      existingId = zipfDist(generator) + 1;
    }

    //size_t currentId = distribution(generator);
    std::string currentExpectedValueSuffix =
        "_value" + std::to_string(existingId);

    spdlog::info("Run {}: Using expected value suffix: {}", i + 1,
                 currentExpectedValueSuffix);

    for (const auto& column : columns) {
      currentExpectedValues.push_back(column + currentExpectedValueSuffix);
    }

    // --- Global Scan Query ---
    long long globalScanTime = 0;
    if (!skipDbScan && i == 0) {
      stopwatch.start();
      [[maybe_unused]] std::vector<std::string> globalMatches =
          dbManager.scanForRecordsInColumns(columns, currentExpectedValues);
      stopwatch.stop();
      globalScanTime = stopwatch.elapsedMicros();
    } else {
      globalScanTime = 0;
    }
    globalScanTimes.push_back(globalScanTime);

    // --- Hash Index Scan ---
    long long hashIndexTime = 0;
    stopwatch.start();

    auto results = dbManager.findUsingHashIndexes(
    columns,
    currentExpectedValues,
    *columnSstFiles);

    stopwatch.stop();
    hashIndexTime = stopwatch.elapsedMicros();
    hashIndexTimeStats.push_back(hashIndexTime);


    // --- Hierarchical Multi-Column Query ---
    gBloomCheckCount = 0;
    gLeafBloomCheckCount = 0;
    gSSTCheckCount = 0;
    stopwatch.start();
    [[maybe_unused]] std::vector<std::string> hierarchicalMatches =
        multiColumnQueryHierarchical(queryTrees, columns, currentExpectedValues, "", "",
                                     dbManager);
    stopwatch.stop();
    hierarchicalMultiTimes.push_back(stopwatch.elapsedMicros());
    multiCol_bloomChecks_vec.push_back(gBloomCheckCount.load());
    multiCol_leafBloomChecks_vec.push_back(gLeafBloomCheckCount.load());
    multiCol_sstChecks_vec.push_back(gSSTCheckCount.load());
    multiCol_nonLeafBloomChecks_vec.push_back(gBloomCheckCount.load() - gLeafBloomCheckCount.load());



    // --- Hierarchical Multi-Column Hash Index Query ---
    gBloomCheckCount = 0;
    gLeafBloomCheckCount = 0;
    gSSTCheckCount = 0;

    gHashIndexOpenCount = 0;
    gHashLookupCount = 0;
    gHashCandidateCount = 0;
    gHashVerificationCount = 0;

    stopwatch.start();
    [[maybe_unused]] std::vector<std::string> hierarchicalHashMatches =
        multiColumnQueryHierarchicalHashIndex(
            queryTrees, columns, currentExpectedValues, "", "", dbManager);
    stopwatch.stop();

    hierarchicalHashMultiTimes.push_back(stopwatch.elapsedMicros());

    if (hierarchicalMatches != hierarchicalHashMatches) {
    spdlog::error(
        "HashMultiHierarchyIndex returned different results than "
        "BloomTree Multi-Column Query. Run: {}",
        i + 1);
} else {
    spdlog::info(
        "HashMultiHierarchyIndex results match BloomTree Multi-Column Query. "
        "Run: {}",
        i + 1);
}

    // --- Hierarchical Single Column Query ---
    // Ensure queryTrees[0] is valid before dereferencing. Already checked by
    // queryTrees.empty()
    gBloomCheckCount = 0;
    gLeafBloomCheckCount = 0;
    gSSTCheckCount = 0;
    stopwatch.start();
    [[maybe_unused]] std::vector<std::string> singlehierarchyMatches =
        dbManager.findUsingSingleHierarchy(queryTrees[0], columns,
                                           currentExpectedValues);
    stopwatch.stop();
    hierarchicalSingleTimes.push_back(stopwatch.elapsedMicros());
    singleCol_bloomChecks_vec.push_back(gBloomCheckCount.load());
    singleCol_leafBloomChecks_vec.push_back(gLeafBloomCheckCount.load());
    singleCol_sstChecks_vec.push_back(gSSTCheckCount.load());
    singleCol_nonLeafBloomChecks_vec.push_back(gBloomCheckCount.load() - gLeafBloomCheckCount.load());

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  // Calculate statistics
  aggregated_timings.globalScanTimeStats =
      calculateNumericStatistics(globalScanTimes);
  aggregated_timings.hierarchicalMultiTimeStats =
      calculateNumericStatistics(hierarchicalMultiTimes);
  aggregated_timings.hierarchicalSingleTimeStats =
      calculateNumericStatistics(hierarchicalSingleTimes);
  aggregated_timings.hashIndexTimeStats =   
      calculateNumericStatistics(hashIndexTimeStats);  
  aggregated_timings.hierarchicalHashMultiTimeStats =
    calculateNumericStatistics(hierarchicalHashMultiTimes);

  aggregated_timings.multiCol_bloomChecksStats =
      calculateCountStatistics(multiCol_bloomChecks_vec);
  aggregated_timings.multiCol_leafBloomChecksStats =
      calculateCountStatistics(multiCol_leafBloomChecks_vec);
  aggregated_timings.multiCol_sstChecksStats =
      calculateCountStatistics(multiCol_sstChecks_vec);
  aggregated_timings.multiCol_nonLeafBloomChecksStats =
      calculateCountStatistics(multiCol_nonLeafBloomChecks_vec);

  aggregated_timings.singleCol_bloomChecksStats =
      calculateCountStatistics(singleCol_bloomChecks_vec);
  aggregated_timings.singleCol_leafBloomChecksStats =
      calculateCountStatistics(singleCol_leafBloomChecks_vec);
  aggregated_timings.singleCol_sstChecksStats =
      calculateCountStatistics(singleCol_sstChecks_vec);
  aggregated_timings.singleCol_nonLeafBloomChecksStats =
      calculateCountStatistics(singleCol_nonLeafBloomChecks_vec);

  // Store number of columns for reference
  aggregated_timings.numColumns = columns.size();

  // Calculate per-column statistics (divide by column count)
  double numCols = static_cast<double>(columns.size());
  std::vector<double> multiCol_bloomChecksPerColumn_vec, multiCol_leafBloomChecksPerColumn_vec, 
                      multiCol_sstChecksPerColumn_vec, multiCol_nonLeafBloomChecksPerColumn_vec;
  std::vector<double> singleCol_bloomChecksPerColumn_vec, singleCol_leafBloomChecksPerColumn_vec, 
                      singleCol_sstChecksPerColumn_vec, singleCol_nonLeafBloomChecksPerColumn_vec;

  for (size_t i = 0; i < multiCol_bloomChecks_vec.size(); ++i) {
    multiCol_bloomChecksPerColumn_vec.push_back(static_cast<double>(multiCol_bloomChecks_vec[i]) / numCols);
    multiCol_leafBloomChecksPerColumn_vec.push_back(static_cast<double>(multiCol_leafBloomChecks_vec[i]) / numCols);
    multiCol_sstChecksPerColumn_vec.push_back(static_cast<double>(multiCol_sstChecks_vec[i]) / numCols);
    multiCol_nonLeafBloomChecksPerColumn_vec.push_back(static_cast<double>(multiCol_nonLeafBloomChecks_vec[i]) / numCols);

    singleCol_bloomChecksPerColumn_vec.push_back(static_cast<double>(singleCol_bloomChecks_vec[i]) );
    singleCol_leafBloomChecksPerColumn_vec.push_back(static_cast<double>(singleCol_leafBloomChecks_vec[i]) );
    singleCol_sstChecksPerColumn_vec.push_back(static_cast<double>(singleCol_sstChecks_vec[i]) );
    singleCol_nonLeafBloomChecksPerColumn_vec.push_back(static_cast<double>(singleCol_nonLeafBloomChecks_vec[i]) );
  }

  aggregated_timings.multiCol_bloomChecksPerColumnStats = 
      calculateNumericStatistics(multiCol_bloomChecksPerColumn_vec);
  aggregated_timings.multiCol_leafBloomChecksPerColumnStats = 
      calculateNumericStatistics(multiCol_leafBloomChecksPerColumn_vec);
  aggregated_timings.multiCol_sstChecksPerColumnStats = 
      calculateNumericStatistics(multiCol_sstChecksPerColumn_vec);
  aggregated_timings.multiCol_nonLeafBloomChecksPerColumnStats = 
      calculateNumericStatistics(multiCol_nonLeafBloomChecksPerColumn_vec);

  aggregated_timings.singleCol_bloomChecksPerColumnStats = 
      calculateNumericStatistics(singleCol_bloomChecksPerColumn_vec);
  aggregated_timings.singleCol_leafBloomChecksPerColumnStats = 
      calculateNumericStatistics(singleCol_leafBloomChecksPerColumn_vec);
  aggregated_timings.singleCol_sstChecksPerColumnStats = 
      calculateNumericStatistics(singleCol_sstChecksPerColumn_vec);
  aggregated_timings.singleCol_nonLeafBloomChecksPerColumnStats = 
      calculateNumericStatistics(singleCol_nonLeafBloomChecksPerColumn_vec);

  return aggregated_timings;
}

AggregatedQueryTimings runStandardQueriesWithTarget(
    DBManager& dbManager, const std::map<std::string, BloomTree>& hierarchies,
    const std::vector<std::string>& columns, size_t dbSize, int numRuns,
    bool skipDbScan, std::vector<std::string> currentExpectedValues) {
  AggregatedQueryTimings aggregated_timings;

  std::vector<long long> globalScanTimes, hierarchicalMultiTimes,
      hierarchicalSingleTimes;
  std::vector<size_t> multiCol_bloomChecks_vec;
  std::vector<size_t> multiCol_leafBloomChecks_vec;
  std::vector<size_t> multiCol_sstChecks_vec;
  std::vector<size_t> multiCol_nonLeafBloomChecks_vec;
  std::vector<size_t> singleCol_bloomChecks_vec;
  std::vector<size_t> singleCol_leafBloomChecks_vec;
  std::vector<size_t> singleCol_sstChecks_vec;
  std::vector<size_t> singleCol_nonLeafBloomChecks_vec;

  // Reserve space in vectors
  globalScanTimes.reserve(numRuns);
  hierarchicalMultiTimes.reserve(numRuns);
  hierarchicalSingleTimes.reserve(numRuns);
  multiCol_bloomChecks_vec.reserve(numRuns);
  multiCol_leafBloomChecks_vec.reserve(numRuns);
  multiCol_sstChecks_vec.reserve(numRuns);
  multiCol_nonLeafBloomChecks_vec.reserve(numRuns);
  singleCol_bloomChecks_vec.reserve(numRuns);
  singleCol_leafBloomChecks_vec.reserve(numRuns);
  singleCol_sstChecks_vec.reserve(numRuns);
  singleCol_nonLeafBloomChecks_vec.reserve(numRuns);

  std::vector<BloomTree> queryTrees;
  queryTrees.reserve(columns.size());
  for (const auto& column : columns) {
    auto it = hierarchies.find(column);
    if (it == hierarchies.end()) {
      spdlog::error(
          "runStandardQueries: Hierarchy for column '{}' not found. Skipping "
          "query execution.",
          column);
      return AggregatedQueryTimings{};
    }
    queryTrees.push_back(it->second);
  }

  if (queryTrees.empty()) {
    spdlog::error(
        "runStandardQueries: No query trees were prepared, possibly due to "
        "missing hierarchies. Skipping query execution.");
    return aggregated_timings;
  }

  StopWatch stopwatch;
  long long globalScanTime = 0;
  for (int i = 0; i < numRuns; ++i) {
    // --- Global Scan Query ---
    if (!skipDbScan && i == 0) {
      stopwatch.start();
      [[maybe_unused]] std::vector<std::string> globalMatches =
          dbManager.scanForRecordsInColumns(columns, currentExpectedValues);
      stopwatch.stop();
      globalScanTime = stopwatch.elapsedMicros();
    } else {
      globalScanTime = 0;
    }
    globalScanTimes.push_back(globalScanTime);

    // --- Hierarchical Multi-Column Query ---
    gBloomCheckCount = 0;
    gLeafBloomCheckCount = 0;
    gSSTCheckCount = 0;
    stopwatch.start();
    [[maybe_unused]] std::vector<std::string> hierarchicalMatches =
        multiColumnQueryHierarchical(queryTrees,columns,  currentExpectedValues, "", "",
                                     dbManager);
    stopwatch.stop();
    hierarchicalMultiTimes.push_back(stopwatch.elapsedMicros());
    multiCol_bloomChecks_vec.push_back(gBloomCheckCount.load());
    multiCol_leafBloomChecks_vec.push_back(gLeafBloomCheckCount.load());
    multiCol_sstChecks_vec.push_back(gSSTCheckCount.load());
    multiCol_nonLeafBloomChecks_vec.push_back(gBloomCheckCount.load() - gLeafBloomCheckCount.load());

    // --- Hierarchical Single Column Query ---
    // Ensure queryTrees[0] is valid before dereferencing. Already checked by
    // queryTrees.empty()
    gBloomCheckCount = 0;
    gLeafBloomCheckCount = 0;
    gSSTCheckCount = 0;
    stopwatch.start();
    [[maybe_unused]] std::vector<std::string> singlehierarchyMatches =
        dbManager.findUsingSingleHierarchy(queryTrees[0], columns,
                                           currentExpectedValues);
    stopwatch.stop();
    hierarchicalSingleTimes.push_back(stopwatch.elapsedMicros());
    singleCol_bloomChecks_vec.push_back(gBloomCheckCount.load());
    singleCol_leafBloomChecks_vec.push_back(gLeafBloomCheckCount.load());
    singleCol_sstChecks_vec.push_back(gSSTCheckCount.load());
    singleCol_nonLeafBloomChecks_vec.push_back(gBloomCheckCount.load() - gLeafBloomCheckCount.load());

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  // Calculate statistics
  aggregated_timings.globalScanTimeStats =
      calculateNumericStatistics(globalScanTimes);
  aggregated_timings.hierarchicalMultiTimeStats =
      calculateNumericStatistics(hierarchicalMultiTimes);
  aggregated_timings.hierarchicalSingleTimeStats =
      calculateNumericStatistics(hierarchicalSingleTimes);

  aggregated_timings.multiCol_bloomChecksStats =
      calculateCountStatistics(multiCol_bloomChecks_vec);
  aggregated_timings.multiCol_leafBloomChecksStats =
      calculateCountStatistics(multiCol_leafBloomChecks_vec);
  aggregated_timings.multiCol_sstChecksStats =
      calculateCountStatistics(multiCol_sstChecks_vec);
  aggregated_timings.multiCol_nonLeafBloomChecksStats =
      calculateCountStatistics(multiCol_nonLeafBloomChecks_vec);

  aggregated_timings.singleCol_bloomChecksStats =
      calculateCountStatistics(singleCol_bloomChecks_vec);
  aggregated_timings.singleCol_leafBloomChecksStats =
      calculateCountStatistics(singleCol_leafBloomChecks_vec);
  aggregated_timings.singleCol_sstChecksStats =
      calculateCountStatistics(singleCol_sstChecks_vec);
  aggregated_timings.singleCol_nonLeafBloomChecksStats =
      calculateCountStatistics(singleCol_nonLeafBloomChecks_vec);

  // Store number of columns for reference
  aggregated_timings.numColumns = columns.size();

  // Calculate per-column statistics (divide by column count)
  double numCols = static_cast<double>(columns.size());
  std::vector<double> multiCol_bloomChecksPerColumn_vec, multiCol_leafBloomChecksPerColumn_vec, 
                      multiCol_sstChecksPerColumn_vec, multiCol_nonLeafBloomChecksPerColumn_vec;
  std::vector<double> singleCol_bloomChecksPerColumn_vec, singleCol_leafBloomChecksPerColumn_vec, 
                      singleCol_sstChecksPerColumn_vec, singleCol_nonLeafBloomChecksPerColumn_vec;

  for (size_t i = 0; i < multiCol_bloomChecks_vec.size(); ++i) {
    multiCol_bloomChecksPerColumn_vec.push_back(static_cast<double>(multiCol_bloomChecks_vec[i]) / numCols);
    multiCol_leafBloomChecksPerColumn_vec.push_back(static_cast<double>(multiCol_leafBloomChecks_vec[i]) / numCols);
    multiCol_sstChecksPerColumn_vec.push_back(static_cast<double>(multiCol_sstChecks_vec[i]) / numCols);
    multiCol_nonLeafBloomChecksPerColumn_vec.push_back(static_cast<double>(multiCol_nonLeafBloomChecks_vec[i]) / numCols);

    singleCol_bloomChecksPerColumn_vec.push_back(static_cast<double>(singleCol_bloomChecks_vec[i]) );
    singleCol_leafBloomChecksPerColumn_vec.push_back(static_cast<double>(singleCol_leafBloomChecks_vec[i]) );
    singleCol_sstChecksPerColumn_vec.push_back(static_cast<double>(singleCol_sstChecks_vec[i]) );
    singleCol_nonLeafBloomChecksPerColumn_vec.push_back(static_cast<double>(singleCol_nonLeafBloomChecks_vec[i]) );
  }

  aggregated_timings.multiCol_bloomChecksPerColumnStats = 
      calculateNumericStatistics(multiCol_bloomChecksPerColumn_vec);
  aggregated_timings.multiCol_leafBloomChecksPerColumnStats = 
      calculateNumericStatistics(multiCol_leafBloomChecksPerColumn_vec);
  aggregated_timings.multiCol_sstChecksPerColumnStats = 
      calculateNumericStatistics(multiCol_sstChecksPerColumn_vec);
  aggregated_timings.multiCol_nonLeafBloomChecksPerColumnStats = 
      calculateNumericStatistics(multiCol_nonLeafBloomChecksPerColumn_vec);

  aggregated_timings.singleCol_bloomChecksPerColumnStats = 
      calculateNumericStatistics(singleCol_bloomChecksPerColumn_vec);
  aggregated_timings.singleCol_leafBloomChecksPerColumnStats = 
      calculateNumericStatistics(singleCol_leafBloomChecksPerColumn_vec);
  aggregated_timings.singleCol_sstChecksPerColumnStats = 
      calculateNumericStatistics(singleCol_sstChecksPerColumn_vec);
  aggregated_timings.singleCol_nonLeafBloomChecksPerColumnStats = 
      calculateNumericStatistics(singleCol_nonLeafBloomChecksPerColumn_vec);

  return aggregated_timings;
}

std::vector<MixedQueryResult> runMixedQueriesWithCsvData(
    DBManager& dbManager,
    const std::map<std::string, BloomTree>& hierarchies,
    const std::vector<std::string>& columns,
    size_t dbSize,
    int numQueries,
    double realDataPercentage,
    const DistributionConfig& config,
    const std::map<std::string, std::vector<std::string>>* columnSstFiles)
{
    std::vector<MixedQueryResult> results;

    if (numQueries <= 0 || columns.empty() || dbSize == 0) {
        spdlog::warn(
            "runMixedQueriesWithCsvData: Invalid experiment parameters.");
        return results;
    }

    // Prepare query trees.
    auto queryTrees = prepareQueryTrees(hierarchies, columns);

    if (queryTrees.empty()) {
        return results;
    }

    // Initialize query generation.
    auto generationContext = createQueryGenerationContext(
        columns.size(),
        dbSize,
        numQueries,
        realDataPercentage,
        config);

    if (generationContext.patterns.size() < 2) {
        spdlog::error(
            "runMixedQueriesWithCsvData: At least two query patterns are required.");
        return results;
    }

    results.reserve(numQueries);

    // Execute queries.
    for (int queryIdx = 0; queryIdx < numQueries; ++queryIdx) {

        // Generate query.
        GeneratedQuery query = generateMixedQuery(
            queryIdx,
            columns,
            generationContext,
            config);

        MixedQueryResult result;
        result.queryIndex = queryIdx;
        result.isRealData = query.isRealData;

        // Execute search strategies.
        runHashIndexQueries(
            dbManager,
            columns,
            query.values,
            columnSstFiles,
            result);

        runHashSingleHierarchyQuery(
            dbManager,
            queryTrees[0],
            columns,
            query.values,
            result);

        runHierarchicalQueries(
            dbManager,
            queryTrees,
            columns,
            query.values,
            result);

        runHierarchicalHashIndexQuery(
            dbManager,
            queryTrees,
            columns,
            query.values,
            result);

        results.push_back(std::move(result));

        std::this_thread::sleep_for(
            std::chrono::milliseconds(10));
    }

    return results;
}

std::vector<MixedQueryResult> runMixedQueriesWithRealData(
    DBManager& dbManager,
    const std::map<std::string, BloomTree>& hierarchies,
    const std::vector<std::string>& columns,
    RealDataQueryContext& realDataContext,
    int numQueries,
    double positiveQueryPercentage,
    const std::map<std::string, std::vector<std::string>>*
        columnSstFiles)
{
    if (numQueries <= 0) {
        throw std::invalid_argument(
            "Number of queries must be greater than zero.");
    }

    if (columns.empty()) {
        throw std::invalid_argument(
            "Column list cannot be empty.");
    }

    if (positiveQueryPercentage < 0.0 ||
        positiveQueryPercentage > 100.0) {
        throw std::invalid_argument(
            "Positive query percentage must be between 0 and 100.");
    }

    if (positiveQueryPercentage > 0.0 &&
        realDataContext.positiveRecords.empty()) {
        throw std::invalid_argument(
            "No positive records available for query generation.");
    }

    // Prepare query trees.
    auto queryTrees = prepareQueryTrees(hierarchies, columns);

    if (queryTrees.empty()) {
        throw std::runtime_error(
            "Failed to prepare query trees.");
    }

    std::vector<MixedQueryResult> results;
    results.reserve(numQueries);

    // ---------------------------------------------------------
    // 1. Determine the exact number of positive and negative
    //    queries.
    // ---------------------------------------------------------

    const size_t positiveQueries = static_cast<size_t>(
        std::llround(
            numQueries * positiveQueryPercentage / 100.0));

    const size_t negativeQueries =
        static_cast<size_t>(numQueries) - positiveQueries;

    // ---------------------------------------------------------
    // 2. Prepare query schedule.
    // ---------------------------------------------------------

    std::vector<bool> querySchedule;
    querySchedule.reserve(numQueries);

    querySchedule.insert(
        querySchedule.end(),
        positiveQueries,
        true);

    querySchedule.insert(
        querySchedule.end(),
        negativeQueries,
        false);

    // Shuffle query types to avoid executing all positive
    // queries before all negative queries.
    std::mt19937_64 scheduleRng(12345);

    std::shuffle(
        querySchedule.begin(),
        querySchedule.end(),
        scheduleRng);

    spdlog::info(
        "RealDataExperiment: Starting {} queries "
        "({} positive, {} negative, {:.1f}% positive).",
        numQueries,
        positiveQueries,
        negativeQueries,
        positiveQueryPercentage);

    // ---------------------------------------------------------
    // 3. Generate and execute queries.
    // ---------------------------------------------------------

    size_t generatedPositiveQueries = 0;
    size_t generatedNegativeQueries = 0;

    for (int queryIdx = 0; queryIdx < numQueries; ++queryIdx) {

        GeneratedRealDataQuery query;

        const bool generatePositive = querySchedule[queryIdx];

        // -----------------------------------------------------
        // Generate positive or negative query.
        // -----------------------------------------------------

        if (generatePositive) {

            query = generatePositiveRealDataQuery(
                realDataContext);

            ++generatedPositiveQueries;

        } else {

            const size_t changedColumns =
                static_cast<size_t>(queryIdx % 3 + 1);

            const bool generated =
                generateNegativeRealDataQuery(
                    dbManager,
                    columns,
                    realDataContext,
                    query,
                    changedColumns,
                    100);

            if (!generated) {
                spdlog::warn(
                    "RealDataExperiment: Could not generate "
                    "negative query {}. Skipping.",
                    queryIdx);

                continue;
            }

            ++generatedNegativeQueries;
        }

        // -----------------------------------------------------
        // Initialize query result.
        // -----------------------------------------------------

        MixedQueryResult result{};

        result.queryIndex = queryIdx;
        result.isRealData = query.isPositive;
        result.matchingColumns = query.matchingColumns;

        // -----------------------------------------------------
        // 4. Hash Index Scan - all SST indexes.
        // -----------------------------------------------------

        runHashIndexQueries(
            dbManager,
            columns,
            query.values,
            columnSstFiles,
            result);

        // -----------------------------------------------------
        // 5. Hash Index + BloomTree - single hierarchy.
        // -----------------------------------------------------

        runHashSingleHierarchyQuery(
            dbManager,
            queryTrees[0],
            columns,
            query.values,
            result);

        // -----------------------------------------------------
        // 6. Hierarchical BloomTree queries.
        // -----------------------------------------------------

        runHierarchicalQueries(
            dbManager,
            queryTrees,
            columns,
            query.values,
            result);


        runHierarchicalHashIndexQuery(
            dbManager,
            queryTrees,
            columns,
            query.values,
            result);

        // -----------------------------------------------------
        // Store results.
        // -----------------------------------------------------

        results.push_back(std::move(result));

        if ((queryIdx + 1) % 100 == 0) {
            spdlog::info(
                "RealDataExperiment: processed {} / {} queries",
                queryIdx + 1,
                numQueries);
        }
    }

    // ---------------------------------------------------------
    // 7. Summary.
    // ---------------------------------------------------------

    spdlog::info(
        "RealDataExperiment: Finished. "
        "Generated positive: {}, negative: {}, total: {}.",
        generatedPositiveQueries,
        generatedNegativeQueries,
        results.size());

    return results;
}
