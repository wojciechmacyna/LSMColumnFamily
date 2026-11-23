#include "query_execution.hpp"
#include "comprehensive_query_analysis.hpp"
#include "experiment_runner.hpp"

#include <spdlog/spdlog.h>

#include <cstddef>
#include <vector>

// Internal helper for accumulating hash-index counters.
namespace {

struct HashIndexCounters {
    size_t indexOpen = 0;
    size_t lookup = 0;
    size_t candidates = 0;
    size_t verification = 0;

    size_t singleIndexOpen = 0;
    size_t singleLookup = 0;
    size_t singleCandidates = 0;
    size_t singleVerification = 0;

    void add(const MixedQueryResult& result) {
        indexOpen += result.hashIndexOpenCount;
        lookup += result.hashLookupCount;
        candidates += result.hashCandidateCount;
        verification += result.hashVerificationCount;

        singleIndexOpen += result.hashSingleIndexOpenCount;
        singleLookup += result.hashSingleLookupCount;
        singleCandidates += result.hashSingleCandidateCount;
        singleVerification += result.hashSingleVerificationCount;
    }
};

} // namespace

std::vector<AccumulatedQueryMetrics> runComprehensiveQueryAnalysis(
    DBManager& dbManager,
    const std::map<std::string, BloomTree>& hierarchies,
    const std::vector<std::string>& columns,
    size_t dbSize,
    int numQueriesPerScenario,
    const DistributionConfig& config,
    const std::map<std::string, std::vector<std::string>>* columnSstFiles)
{
    std::vector<AccumulatedQueryMetrics> accumulatedResults;

    const std::vector<double> realDataPercentages = {
        0.0, 10.0, 20.0, 40.0, 60.0, 80.0, 100.0
    };

    spdlog::info(
        "Starting comprehensive analysis with {} queries per scenario",
        numQueriesPerScenario);

    for (double percentage : realDataPercentages) {

        spdlog::info("Running scenario with {}% real data", percentage);

        // --------------------------------------------------
        // 1. Execute queries
        // --------------------------------------------------

        std::vector<MixedQueryResult> results =
            runMixedQueriesWithCsvData(
                dbManager,
                hierarchies,
                columns,
                dbSize,
                numQueriesPerScenario,
                percentage,
                config,
                columnSstFiles);

        if (results.empty()) {
            spdlog::warn(
                "No results returned for {}% real data scenario",
                percentage);
            continue;
        }

        // --------------------------------------------------
        // 2. Initialize metrics
        // --------------------------------------------------

        AccumulatedQueryMetrics metrics{};

        metrics.realDataPercentage = percentage;
        metrics.totalQueries = results.size();
        metrics.realQueries = 0;
        metrics.falseQueries = 0;
        metrics.numColumns = columns.size();

        // --------------------------------------------------
        // 3. Initialize time counters
        // --------------------------------------------------

        long long totalMultiTime = 0;
        long long totalSingleTime = 0;
        long long totalHashIndexTime = 0;
        long long totalHashSingleIndexTime = 0;
        long long totalHashSingleHierarchyIndexTime = 0;

        long long realMultiTimeSum = 0;
        long long realSingleTimeSum = 0;
        long long realHashIndexTimeSum = 0;
        long long realHashSingleIndexTimeSum = 0;
        long long realHashSingleHierarchyIndexTimeSum = 0;

        long long falseMultiTimeSum = 0;
        long long falseSingleTimeSum = 0;
        long long falseHashIndexTimeSum = 0;
        long long falseHashSingleIndexTimeSum = 0;
        long long falseHashSingleHierarchyIndexTimeSum = 0;

        long long totalHierarchicalHashMultiTime = 0;
        long long realHierarchicalHashMultiTimeSum = 0;
        long long falseHierarchicalHashMultiTimeSum = 0;

        // --------------------------------------------------
        // 4. Initialize BloomTree counters
        // --------------------------------------------------

        size_t totalMultiBloomChecks = 0;
        size_t totalMultiLeafBloomChecks = 0;
        size_t totalMultiSSTChecks = 0;
        size_t totalMultiNonLeafBloomChecks = 0;

        size_t totalSingleBloomChecks = 0;
        size_t totalSingleLeafBloomChecks = 0;
        size_t totalSingleSSTChecks = 0;
        size_t totalSingleNonLeafBloomChecks = 0;

        size_t realMultiBloomChecksSum = 0;
        size_t realMultiSSTChecksSum = 0;

        size_t falseMultiBloomChecksSum = 0;
        size_t falseMultiSSTChecksSum = 0;

        // --------------------------------------------------
        // 5. Initialize Hash Index counters
        // --------------------------------------------------

        HashIndexCounters totalHash;
        HashIndexCounters realHash;
        HashIndexCounters falseHash;

        // --------------------------------------------------
        // 6. Process query results
        // --------------------------------------------------

        for (const auto& result : results) {

            // Time measurements
            totalMultiTime += result.hierarchicalMultiTime;
            totalSingleTime += result.hierarchicalSingleTime;
            totalHashIndexTime += result.hashIndexTime;
            totalHashSingleIndexTime += result.hashSingleIndexTime;
            totalHashSingleHierarchyIndexTime +=
                result.hashSingleHierarchyIndexTime;
            totalHierarchicalHashMultiTime +=
                result.hierarchicalHashMultiTime;

            // BloomTree measurements
            totalMultiBloomChecks += result.multiCol_bloomChecks;
            totalMultiLeafBloomChecks += result.multiCol_leafBloomChecks;
            totalMultiSSTChecks += result.multiCol_sstChecks;
            totalMultiNonLeafBloomChecks += result.multiCol_nonLeafBloomChecks;

            totalSingleBloomChecks += result.singleCol_bloomChecks;
            totalSingleLeafBloomChecks += result.singleCol_leafBloomChecks;
            totalSingleSSTChecks += result.singleCol_sstChecks;
            totalSingleNonLeafBloomChecks += result.singleCol_nonLeafBloomChecks;

            // Hash Index measurements
            totalHash.add(result);

            // Separate real and false data
            if (result.isRealData) {

                metrics.realQueries++;

                realMultiTimeSum += result.hierarchicalMultiTime;
                realSingleTimeSum += result.hierarchicalSingleTime;
                realHashIndexTimeSum += result.hashIndexTime;
                realHashSingleIndexTimeSum += result.hashSingleIndexTime;
                realHashSingleHierarchyIndexTimeSum +=
                    result.hashSingleHierarchyIndexTime;
                realHierarchicalHashMultiTimeSum +=
                    result.hierarchicalHashMultiTime;

                realMultiBloomChecksSum += result.multiCol_bloomChecks;
                realMultiSSTChecksSum += result.multiCol_sstChecks;

                realHash.add(result);
            }
            else {

                metrics.falseQueries++;

                falseMultiTimeSum += result.hierarchicalMultiTime;
                falseSingleTimeSum += result.hierarchicalSingleTime;
                falseHashIndexTimeSum += result.hashIndexTime;
                falseHashSingleIndexTimeSum += result.hashSingleIndexTime;
                falseHashSingleHierarchyIndexTimeSum +=
                    result.hashSingleHierarchyIndexTime;
                falseHierarchicalHashMultiTimeSum +=
                    result.hierarchicalHashMultiTime;

                falseMultiBloomChecksSum += result.multiCol_bloomChecks;
                falseMultiSSTChecksSum += result.multiCol_sstChecks;

                falseHash.add(result);
            }
        }

        // --------------------------------------------------
        // 7. Calculate averages for all queries
        // --------------------------------------------------

        const double queryCount = static_cast<double>(metrics.totalQueries);

        metrics.avgHierarchicalMultiTime =
            totalMultiTime / queryCount;

        metrics.avgHierarchicalSingleTime =
            totalSingleTime / queryCount;

        metrics.avgHashIndexTime =
            totalHashIndexTime / queryCount;

        metrics.avgHashSingleIndexTime =
            totalHashSingleIndexTime / queryCount;

        metrics.avgHashSingleHierarchyIndexTime =
            totalHashSingleHierarchyIndexTime / queryCount;

        metrics.avgHashMultiHierarchyIndexTime =
            totalHierarchicalHashMultiTime / queryCount;

        // BloomTree averages
        metrics.avgMultiBloomChecks =
            totalMultiBloomChecks / queryCount;

        metrics.avgMultiLeafBloomChecks =
            totalMultiLeafBloomChecks / queryCount;

        metrics.avgMultiSSTChecks =
            totalMultiSSTChecks / queryCount;

        metrics.avgMultiNonLeafBloomChecks =
            totalMultiNonLeafBloomChecks / queryCount;

        metrics.avgSingleBloomChecks =
            totalSingleBloomChecks / queryCount;

        metrics.avgSingleLeafBloomChecks =
            totalSingleLeafBloomChecks / queryCount;

        metrics.avgSingleSSTChecks =
            totalSingleSSTChecks / queryCount;

        metrics.avgSingleNonLeafBloomChecks =
            totalSingleNonLeafBloomChecks / queryCount;

        // Hash Index averages
        metrics.avgHashIndexOpenCount =
            totalHash.indexOpen / queryCount;

        metrics.avgHashLookupCount =
            totalHash.lookup / queryCount;

        metrics.avgHashCandidateCount =
            totalHash.candidates / queryCount;

        metrics.avgHashVerificationCount =
            totalHash.verification / queryCount;

        metrics.avgHashSingleIndexOpenCount =
            totalHash.singleIndexOpen / queryCount;

        metrics.avgHashSingleLookupCount =
            totalHash.singleLookup / queryCount;

        metrics.avgHashSingleCandidateCount =
            totalHash.singleCandidates / queryCount;

        metrics.avgHashSingleVerificationCount =
            totalHash.singleVerification / queryCount;

        // --------------------------------------------------
        // 8. Calculate per-column averages
        // --------------------------------------------------

        const double numCols =
            static_cast<double>(metrics.numColumns);

        if (numCols > 0) {

            metrics.avgMultiBloomChecksPerColumn =
                metrics.avgMultiBloomChecks / numCols;

            metrics.avgMultiLeafBloomChecksPerColumn =
                metrics.avgMultiLeafBloomChecks / numCols;

            metrics.avgMultiSSTChecksPerColumn =
                metrics.avgMultiSSTChecks / numCols;

            metrics.avgMultiNonLeafBloomChecksPerColumn =
                metrics.avgMultiNonLeafBloomChecks / numCols;
        }

        // Single-column hierarchy already represents one column.
        metrics.avgSingleBloomChecksPerColumn =
            metrics.avgSingleBloomChecks;

        metrics.avgSingleLeafBloomChecksPerColumn =
            metrics.avgSingleLeafBloomChecks;

        metrics.avgSingleSSTChecksPerColumn =
            metrics.avgSingleSSTChecks;

        metrics.avgSingleNonLeafBloomChecksPerColumn =
            metrics.avgSingleNonLeafBloomChecks;

        // --------------------------------------------------
        // 9. Calculate averages for real data queries
        // --------------------------------------------------

        if (metrics.realQueries > 0) {

            const double count =
                static_cast<double>(metrics.realQueries);

            metrics.avgRealDataMultiTime =
                realMultiTimeSum / count;

            metrics.avgRealDataSingleTime =
                realSingleTimeSum / count;

            metrics.avgRealHashIndexTime =
                realHashIndexTimeSum / count;

            metrics.avgRealHashSingleIndexTime =
                realHashSingleIndexTimeSum / count;

            metrics.avgRealHashSingleHierarchyIndexTime =
                realHashSingleHierarchyIndexTimeSum / count;

            metrics.avgRealMultiBloomChecks =
                realMultiBloomChecksSum / count;

            metrics.avgRealMultiSSTChecks =
                realMultiSSTChecksSum / count;

            metrics.avgRealMultiBloomChecksPerColumn =
                numCols > 0 ? metrics.avgRealMultiBloomChecks / numCols : 0.0;

            metrics.avgRealMultiSSTChecksPerColumn =
                numCols > 0 ? metrics.avgRealMultiSSTChecks / numCols : 0.0;

            // Hash Index - Multi-Column
            metrics.avgRealHashIndexOpenCount =
                realHash.indexOpen / count;

            metrics.avgRealHashLookupCount =
                realHash.lookup / count;

            metrics.avgRealHashCandidateCount =
                realHash.candidates / count;

            metrics.avgRealHashVerificationCount =
                realHash.verification / count;

            // Hash Index - Single-Column
            metrics.avgRealHashSingleIndexOpenCount =
                realHash.singleIndexOpen / count;

            metrics.avgRealHashSingleLookupCount =
                realHash.singleLookup / count;

            metrics.avgRealHashSingleCandidateCount =
                realHash.singleCandidates / count;

            metrics.avgRealHashSingleVerificationCount =
                realHash.singleVerification / count;

            metrics.avgRealHashMultiHierarchyIndexTime =
                realHierarchicalHashMultiTimeSum / count;
        }

        // --------------------------------------------------
        // 10. Calculate averages for false data queries
        // --------------------------------------------------

        if (metrics.falseQueries > 0) {

            const double count =
                static_cast<double>(metrics.falseQueries);

            metrics.avgFalseDataMultiTime =
                falseMultiTimeSum / count;

            metrics.avgFalseDataSingleTime =
                falseSingleTimeSum / count;

            metrics.avgFalseHashIndexTime =
                falseHashIndexTimeSum / count;

            metrics.avgFalseHashSingleIndexTime =
                falseHashSingleIndexTimeSum / count;

            metrics.avgFalseHashSingleHierachyIndexTime =
                falseHashSingleHierarchyIndexTimeSum / count;

            metrics.avgFalseMultiBloomChecks =
                falseMultiBloomChecksSum / count;

            metrics.avgFalseMultiSSTChecks =
                falseMultiSSTChecksSum / count;

            metrics.avgFalseMultiBloomChecksPerColumn =
                numCols > 0 ? metrics.avgFalseMultiBloomChecks / numCols : 0.0;

            metrics.avgFalseMultiSSTChecksPerColumn =
                numCols > 0 ? metrics.avgFalseMultiSSTChecks / numCols : 0.0;

            // Hash Index - Multi-Column
            metrics.avgFalseHashIndexOpenCount =
                falseHash.indexOpen / count;

            metrics.avgFalseHashLookupCount =
                falseHash.lookup / count;

            metrics.avgFalseHashCandidateCount =
                falseHash.candidates / count;

            metrics.avgFalseHashVerificationCount =
                falseHash.verification / count;

            // Hash Index - Single-Column
            metrics.avgFalseHashSingleIndexOpenCount =
                falseHash.singleIndexOpen / count;

            metrics.avgFalseHashSingleLookupCount =
                falseHash.singleLookup / count;

            metrics.avgFalseHashSingleCandidateCount =
                falseHash.singleCandidates / count;

            metrics.avgFalseHashSingleVerificationCount =
                falseHash.singleVerification / count;

            metrics.avgFalseHashMultiHierarchyIndexTime =
                falseHierarchicalHashMultiTimeSum / count;
        }

        // --------------------------------------------------
        // 11. Store scenario results
        // --------------------------------------------------

        accumulatedResults.push_back(metrics);

        spdlog::info(
            "Scenario {}% complete: {} real queries (avg: {:.2f} us), "
            "{} false queries (avg: {:.2f} us)",
            percentage,
            metrics.realQueries,
            metrics.avgRealDataMultiTime,
            metrics.falseQueries,
            metrics.avgFalseDataMultiTime);
    }

    spdlog::info(
        "Comprehensive analysis completed with {} scenarios",
        accumulatedResults.size());

    return accumulatedResults;
}