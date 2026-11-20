#include "real_data_experiment.hpp"

#include <spdlog/spdlog.h>

#include <cstdint>
#include <stdexcept>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "bloom_manager.hpp"
#include "bloom_tree.hpp"
#include "db_manager.hpp"
#include "stopwatch.hpp"
#include "sst_bloom_infrastructure.hpp"


RealDataQueryContext createRealDataQueryContext(
    DBManager& dbManager,
    const std::vector<std::string>& columns,
    size_t sampleSize,
    uint64_t seed)
{
    RealDataScanResult scanResult =
        dbManager.scanAndSampleRecords(columns, sampleSize, seed);

    RealDataQueryContext context;

    context.positiveRecords = std::move(scanResult.sampledRecords);
    context.combinationHashes =
        std::move(scanResult.combinationHashes);

    context.recordsScanned = scanResult.recordsScanned;
    context.queryRng.seed(seed);

    context.distinctValues.resize(columns.size());

    // Collect distinct values from the sampled records.
    std::vector<std::unordered_set<std::string>> uniqueValues(
        columns.size());

    for (const auto& record : context.positiveRecords) {
        for (size_t i = 0; i < record.values.size(); ++i) {
            if (uniqueValues[i].insert(record.values[i]).second) {
                context.distinctValues[i].push_back(record.values[i]);
            }
        }
    }

    spdlog::info(
        "RealDataContext: records={}, sample={}, hash entries={}",
        context.recordsScanned,
        context.positiveRecords.size(),
        context.combinationHashes.size());

    for (size_t i = 0; i < columns.size(); ++i) {
        spdlog::info(
            "RealDataContext: column='{}', sampled distinct values={}",
            columns[i],
            context.distinctValues[i].size());
    }

    return context;
}

RealDataQueryContext loadRealDataQueryContext(
    DBManager& dbManager,
    const std::vector<std::string>& columns,
    size_t sampleSize,
    uint64_t seed)
{
    if (columns.empty()) {
        throw std::invalid_argument("Columns cannot be empty.");
    }

    auto scanResult = dbManager.scanAndSampleRecords(
        columns, sampleSize, seed);

    if (scanResult.sampledRecords.empty()) {
        throw std::runtime_error("No records found in the database.");
    }

    RealDataQueryContext context;
    context.positiveRecords = std::move(scanResult.sampledRecords);
    context.combinationHashes = std::move(scanResult.combinationHashes);
    context.recordsScanned = scanResult.recordsScanned;
    context.queryRng.seed(seed);

    context.distinctValues.resize(columns.size());

    std::vector<std::unordered_set<std::string>> distinctSets(
        columns.size());

    for (const auto& record : context.positiveRecords) {
        if (record.values.size() != columns.size()) {
            throw std::runtime_error("Invalid sampled record.");
        }

        for (size_t i = 0; i < columns.size(); ++i) {
            if (distinctSets[i].insert(record.values[i]).second) {
                context.distinctValues[i].push_back(record.values[i]);
            }
        }
    }

    spdlog::info(
        "RealDataContext: scanned={} sampled={} uniqueCombinations={}",
        context.recordsScanned,
        context.positiveRecords.size(),
        context.combinationHashes.size());

    for (size_t i = 0; i < columns.size(); ++i) {
        spdlog::info(
            "RealDataContext: column={} sampledDistinctValues={}",
            columns[i],
            context.distinctValues[i].size());
    }

    return context;
}

void runRealDataExperiment(
    const std::string& baseDir,
    const std::string& dbName,
    size_t sampleSize,
    int numQueries)
{
    // Retain the existing parameter for API compatibility.
    (void)baseDir;

    const std::vector<std::string> columns = {
        "pickup_location",
        "dropoff_location",
        "payment_type",
        "passenger_count",
        "vendor_id",
        "ratecode_id"
    };

    constexpr uint64_t seed = 12345;

    spdlog::info(
        "Starting NYC Taxi real-data experiment: DB='{}'",
        dbName);

    DBManager dbManager;
    BloomManager bloomManager;

    TestParams params{
        dbName,
        1'000'000,
        5,
        1,
        20000,
        4'000'000,
        3
    };

    StopWatch stopwatch;

    // Open the existing database.
    dbManager.openDB(dbName, columns);

    // Discover SST files.
    stopwatch.start();

    auto columnSstFiles =
        scanSstFilesAsync(columns, dbManager, params);

    stopwatch.stop();

    const auto sstScanTime = stopwatch.elapsedMicros();

    spdlog::info(
        "RealData: SST scan time={} us",
        sstScanTime);

    // Build BloomTree hierarchies.
    stopwatch.start();

    auto hierarchies =
        buildHierarchies(columnSstFiles, bloomManager, params);

    stopwatch.stop();

    const auto bloomBuildTime = stopwatch.elapsedMicros();

    spdlog::info(
        "RealData: BloomTree build time={} us",
        bloomBuildTime);

    // Build hash indexes.
    stopwatch.start();

    const auto hashIndexSize =
        buildHashIndexesAsync(columnSstFiles);

    stopwatch.stop();

    const auto hashBuildTime = stopwatch.elapsedMicros();

    spdlog::info(
        "RealData: Hash Index build time={} us, size={} bytes",
        hashBuildTime,
        hashIndexSize);

    // Prepare the real-data query context.
    stopwatch.start();

    auto realDataContext =
        createRealDataQueryContext(
            dbManager,
            columns,
            sampleSize,
            seed);

    stopwatch.stop();

    const auto contextBuildTime = stopwatch.elapsedMicros();

    spdlog::info(
        "RealData: Query context preparation={} us",
        contextBuildTime);

    const std::vector<double> positiveQueryRatios = {
        10.0,
        25.0,
        50.0,
        75.0,
        90.0
    };

    for (double positiveRatio : positiveQueryRatios) {
        spdlog::info("==============================================");
        spdlog::info(
            "Starting NYC Taxi experiment: {:.0f}% positive queries",
            positiveRatio);
        spdlog::info("==============================================");

        // Reset the query generator to the same initial state for each ratio.
        auto experimentContext = realDataContext;

        auto results = runMixedQueriesWithRealData(
            dbManager,
            hierarchies,
            columns,
            experimentContext,
            numQueries,
            positiveRatio,
            &columnSstFiles);

        auto aggregatedResults =
            aggregateMixedQueryResults(results);

        saveRealDataExperimentResults(
            "csv/real_data_comparison.csv",
            aggregatedResults,
            experimentContext.recordsScanned);
    }

    spdlog::info(
        "NYC Taxi real-data experiment completed. Configurations tested: {}.",
        positiveQueryRatios.size());
}
