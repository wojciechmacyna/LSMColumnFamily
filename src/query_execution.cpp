#include "query_execution.hpp"

#include <atomic>
#include <stdexcept>
#include <vector>

#include <spdlog/spdlog.h>

#include "algorithm.hpp"
#include "bloom_tree.hpp"
#include "db_manager.hpp"
#include "sst_hash_index.hpp"
#include "stopwatch.hpp"

// Query execution counters are defined by the existing database/index modules.
extern std::atomic<size_t> gBloomCheckCount;
extern std::atomic<size_t> gLeafBloomCheckCount;
extern std::atomic<size_t> gSSTCheckCount;
extern std::atomic<size_t> gHashIndexOpenCount;
extern std::atomic<size_t> gHashLookupCount;
extern std::atomic<size_t> gHashCandidateCount;
extern std::atomic<size_t> gHashVerificationCount;

/**
 * @brief Executes multi-column and single-column hash-index queries.
 *
 * Resets and captures the hash-index counters independently for each strategy.
 * If the SSTable file map is unavailable, both hash-index measurements are skipped.
 */
void runHashIndexQueries(
    DBManager& dbManager,
    const std::vector<std::string>& columns,
    const std::vector<std::string>& values,
    const std::map<std::string, std::vector<std::string>>* columnSstFiles,
    MixedQueryResult& result)
{
    if (columnSstFiles == nullptr) {
        spdlog::warn(
            "Hash Index queries skipped: columnSstFiles is null.");
        return;
    }

    StopWatch stopwatch;

    // Hash Index Multi-Column
    gHashIndexOpenCount = 0;
    gHashLookupCount = 0;
    gHashCandidateCount = 0;
    gHashVerificationCount = 0;

    stopwatch.start();

    [[maybe_unused]] auto multiResults =
        dbManager.findUsingHashIndexes(
            columns,
            values,
            *columnSstFiles);

    stopwatch.stop();

    result.hashIndexTime = stopwatch.elapsedMicros();
    result.hashIndexOpenCount = gHashIndexOpenCount.load();
    result.hashLookupCount = gHashLookupCount.load();
    result.hashCandidateCount = gHashCandidateCount.load();
    result.hashVerificationCount = gHashVerificationCount.load();

    // Hash Index Single-Column
    gHashIndexOpenCount = 0;
    gHashLookupCount = 0;
    gHashCandidateCount = 0;
    gHashVerificationCount = 0;

    stopwatch.start();

    [[maybe_unused]] auto singleResults =
        dbManager.findUsingHashIndexesSingleColumn(
            columns,
            values,
            *columnSstFiles);

    stopwatch.stop();

    result.hashSingleIndexTime = stopwatch.elapsedMicros();
    result.hashSingleIndexOpenCount = gHashIndexOpenCount.load();
    result.hashSingleLookupCount = gHashLookupCount.load();
    result.hashSingleCandidateCount = gHashCandidateCount.load();
    result.hashSingleVerificationCount =
        gHashVerificationCount.load();
}


/**
 * @brief Executes the single-hierarchy query accelerated by per-SSTable hash indexes.
 *
 * Records the total execution time and logs the timing breakdown returned by DBManager.
 */
void runHashSingleHierarchyQuery(
    DBManager& dbManager,
    BloomTree& queryTree,
    const std::vector<std::string>& columns,
    const std::vector<std::string>& values,
    MixedQueryResult& result)
{
    StopWatch stopwatch;
    HashHierarchyTimings timings;

    stopwatch.start();

    [[maybe_unused]] auto matches =
        dbManager.findUsingSingleHierarchyAndHashIndexes(
            queryTree,
            columns,
            values,
            &timings);

    stopwatch.stop();

    result.hashSingleHierarchyIndexTime =
        stopwatch.elapsedMicros();

    spdlog::info(
        "HashSingleHierarchy: Bloom={} us, Open={} us, "
        "Lookup={} us, RangeFilter={} us, SortUnique={} us, "
        "RocksDBVerify={} us, Total={} us",
        timings.bloomTime,
        timings.openTime,
        timings.lookupTime,
        timings.rangeFilterTime,
        timings.sortUniqueTime,
        timings.rocksDBVerifyTime,
        timings.totalTime);
}


/**
 * @brief Executes multi-column and single-hierarchy BloomTree query strategies.
 *
 * Resets Bloom-filter and SSTable counters before each strategy, then stores
 * elapsed times, raw counters, non-leaf Bloom checks, and per-column metrics.
 * The single-column strategy uses the first query tree, as in the original implementation.
 *
 * @throws std::invalid_argument if columns or queryTrees is empty.
 */
void runHierarchicalQueries(
    DBManager& dbManager,
    std::vector<BloomTree>& queryTrees,
    const std::vector<std::string>& columns,
    const std::vector<std::string>& values,
    MixedQueryResult& result)
{
    StopWatch stopwatch;

    if (columns.empty() || queryTrees.empty()) {
        throw std::invalid_argument(
            "Columns and queryTrees must not be empty.");
    }

    const double numCols = static_cast<double>(columns.size());

    // ---------------------------------------------------------
    // Hierarchical Multi-Column Query
    // ---------------------------------------------------------

    gBloomCheckCount = 0;
    gLeafBloomCheckCount = 0;
    gSSTCheckCount = 0;

    stopwatch.start();

    [[maybe_unused]] auto multiMatches =
        multiColumnQueryHierarchical(
            queryTrees,
            columns,
            values,
            "",
            "",
            dbManager);

    stopwatch.stop();

    result.hierarchicalMultiTime = stopwatch.elapsedMicros();

    result.multiCol_bloomChecks = gBloomCheckCount.load();
    result.multiCol_leafBloomChecks = gLeafBloomCheckCount.load();
    result.multiCol_sstChecks = gSSTCheckCount.load();

    result.multiCol_nonLeafBloomChecks =
        result.multiCol_bloomChecks -
        result.multiCol_leafBloomChecks;

    result.multiCol_bloomChecksPerColumn =
        static_cast<double>(result.multiCol_bloomChecks) / numCols;

    result.multiCol_leafBloomChecksPerColumn =
        static_cast<double>(result.multiCol_leafBloomChecks) / numCols;

    result.multiCol_sstChecksPerColumn =
        static_cast<double>(result.multiCol_sstChecks) / numCols;

    result.multiCol_nonLeafBloomChecksPerColumn =
        static_cast<double>(result.multiCol_nonLeafBloomChecks) / numCols;

    // ---------------------------------------------------------
    // Hierarchical Single-Column Query
    // ---------------------------------------------------------

    gBloomCheckCount = 0;
    gLeafBloomCheckCount = 0;
    gSSTCheckCount = 0;

    stopwatch.start();

    [[maybe_unused]] auto singleMatches =
        dbManager.findUsingSingleHierarchy(
            queryTrees[0],
            columns,
            values);

    stopwatch.stop();

    result.hierarchicalSingleTime = stopwatch.elapsedMicros();

    result.singleCol_bloomChecks = gBloomCheckCount.load();
    result.singleCol_leafBloomChecks = gLeafBloomCheckCount.load();
    result.singleCol_sstChecks = gSSTCheckCount.load();

    result.singleCol_nonLeafBloomChecks =
        result.singleCol_bloomChecks -
        result.singleCol_leafBloomChecks;

    // Single hierarchy: counts for one column.
    result.singleCol_bloomChecksPerColumn =
        static_cast<double>(result.singleCol_bloomChecks);

    result.singleCol_leafBloomChecksPerColumn =
        static_cast<double>(result.singleCol_leafBloomChecks);

    result.singleCol_sstChecksPerColumn =
        static_cast<double>(result.singleCol_sstChecks);

    result.singleCol_nonLeafBloomChecksPerColumn =
        static_cast<double>(result.singleCol_nonLeafBloomChecks);
}


/**
 * @brief Executes a multi-column BloomTree query accelerated by per-SSTable hash indexes.
 *
 * Records execution time and BloomTree/hash-index operation counters.
 */
void runHierarchicalHashIndexQuery(
    DBManager& dbManager,
    std::vector<BloomTree>& queryTrees,
    const std::vector<std::string>& columns,
    const std::vector<std::string>& values,
    MixedQueryResult& result)
{
    if (columns.empty() || queryTrees.empty()) {
        throw std::invalid_argument(
            "Columns and queryTrees must not be empty.");
    }

    StopWatch stopwatch;

    gBloomCheckCount = 0;
    gLeafBloomCheckCount = 0;
    gSSTCheckCount = 0;

    gHashIndexOpenCount = 0;
    gHashLookupCount = 0;
    gHashCandidateCount = 0;
    gHashVerificationCount = 0;

    stopwatch.start();

    [[maybe_unused]] auto matches =
        multiColumnQueryHierarchicalHashIndex(
            queryTrees,
            columns,
            values,
            "",
            "",
            dbManager);

    stopwatch.stop();

    result.hierarchicalHashMultiTime = stopwatch.elapsedMicros();

    result.hierarchicalHashMultiBloomChecks =
        gBloomCheckCount.load();

    result.hierarchicalHashMultiLeafBloomChecks =
        gLeafBloomCheckCount.load();

    result.hierarchicalHashMultiSstChecks =
        gSSTCheckCount.load();

    result.hierarchicalHashMultiIndexOpenCount =
        gHashIndexOpenCount.load();

    result.hierarchicalHashMultiLookupCount =
        gHashLookupCount.load();

    result.hierarchicalHashMultiCandidateCount =
        gHashCandidateCount.load();

    result.hierarchicalHashMultiVerificationCount =
        gHashVerificationCount.load();
}

