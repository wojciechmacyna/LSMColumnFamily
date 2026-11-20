
#pragma once

#include <cstddef>
#include <cstdint>
#include <random>
#include <string>
#include <vector>

struct TimingStatistics {
    long long min = 0;
    long long max = 0;
    double median = 0.0;
    double average = 0.0;
};

struct CountStatistics {
    size_t min = 0;
    size_t max = 0;
    double median = 0.0;
    double average = 0.0;
};

struct PatternQueryResult {
    double percent;
    long long hierarchicalMultiTime;
    long long hierarchicalSingleTime;
    size_t multiCol_bloomChecks;
    size_t multiCol_leafBloomChecks;
    size_t multiCol_sstChecks;
    size_t singleCol_bloomChecks;
    size_t singleCol_leafBloomChecks;
    size_t singleCol_sstChecks;

    size_t multiCol_nonLeafBloomChecks;
    size_t singleCol_nonLeafBloomChecks;

    double multiCol_bloomChecksPerColumn;
    double multiCol_leafBloomChecksPerColumn;
    double multiCol_sstChecksPerColumn;
    double multiCol_nonLeafBloomChecksPerColumn;
    double singleCol_bloomChecksPerColumn;
    double singleCol_leafBloomChecksPerColumn;
    double singleCol_sstChecksPerColumn;
    double singleCol_nonLeafBloomChecksPerColumn;
};

struct MixedQueryResult {
    int queryIndex;
    bool isRealData;
    long long hierarchicalMultiTime;
    long long hierarchicalSingleTime;
    long long hashSingleHierarchyIndexTime;

    size_t multiCol_bloomChecks;
    size_t multiCol_leafBloomChecks;
    size_t multiCol_sstChecks;
    size_t singleCol_bloomChecks;
    size_t singleCol_leafBloomChecks;
    size_t singleCol_sstChecks;

    double hashIndexTime = 0.0;
    size_t hashIndexOpenCount = 0;
    size_t hashLookupCount = 0;
    size_t hashCandidateCount = 0;
    size_t hashVerificationCount = 0;

    double hashSingleIndexTime = 0.0;
    size_t hashSingleIndexOpenCount = 0;
    size_t hashSingleLookupCount = 0;
    size_t hashSingleCandidateCount = 0;
    size_t hashSingleVerificationCount = 0;
    size_t matchingColumns = 0;

    size_t multiCol_nonLeafBloomChecks;
    size_t singleCol_nonLeafBloomChecks;

    double multiCol_bloomChecksPerColumn;
    double multiCol_leafBloomChecksPerColumn;
    double multiCol_sstChecksPerColumn;
    double multiCol_nonLeafBloomChecksPerColumn;
    double singleCol_bloomChecksPerColumn;
    double singleCol_leafBloomChecksPerColumn;
    double singleCol_sstChecksPerColumn;
    double singleCol_nonLeafBloomChecksPerColumn;
};

struct AccumulatedQueryMetrics {
    double realDataPercentage;
    int totalQueries;
    int realQueries;
    int falseQueries;
    size_t numColumns;

    double avgHierarchicalMultiTime;
    double avgHierarchicalSingleTime;
    double avgHashIndexTime;
    double avgHashSingleIndexTime;
    double avgHashSingleHierarchyIndexTime;
    double avgRealDataMultiTime;
    double avgRealDataSingleTime;
    double avgRealHashIndexTime;
    double avgRealHashSingleIndexTime;
    double avgRealHashSingleHierarchyIndexTime;
    double avgFalseDataMultiTime;
    double avgFalseDataSingleTime;
    double avgFalseHashIndexTime;
    double avgFalseHashSingleIndexTime;
    double avgFalseHashSingleHierachyIndexTime;

    double avgMultiBloomChecks;
    double avgMultiLeafBloomChecks;
    double avgMultiSSTChecks;
    double avgSingleBloomChecks;
    double avgSingleLeafBloomChecks;
    double avgSingleSSTChecks;

    double avgMultiNonLeafBloomChecks;
    double avgSingleNonLeafBloomChecks;

    double avgMultiBloomChecksPerColumn;
    double avgMultiLeafBloomChecksPerColumn;
    double avgMultiSSTChecksPerColumn;
    double avgMultiNonLeafBloomChecksPerColumn;
    double avgSingleBloomChecksPerColumn;
    double avgSingleLeafBloomChecksPerColumn;
    double avgSingleSSTChecksPerColumn;
    double avgSingleNonLeafBloomChecksPerColumn;

    double avgRealMultiBloomChecks;
    double avgRealMultiSSTChecks;
    double avgFalseMultiBloomChecks;
    double avgFalseMultiSSTChecks;

    double avgRealMultiBloomChecksPerColumn;
    double avgRealMultiSSTChecksPerColumn;
    double avgFalseMultiBloomChecksPerColumn;
    double avgFalseMultiSSTChecksPerColumn;

    double avgHashIndexOpenCount = 0.0;
    double avgHashLookupCount = 0.0;
    double avgHashCandidateCount = 0.0;
    double avgHashVerificationCount = 0.0;

    double avgHashSingleIndexOpenCount = 0.0;
    double avgHashSingleLookupCount = 0.0;
    double avgHashSingleCandidateCount = 0.0;
    double avgHashSingleVerificationCount = 0.0;

    double avgRealHashIndexOpenCount = 0.0;
    double avgRealHashLookupCount = 0.0;
    double avgRealHashCandidateCount = 0.0;
    double avgRealHashVerificationCount = 0.0;

    double avgRealHashSingleIndexOpenCount = 0.0;
    double avgRealHashSingleLookupCount = 0.0;
    double avgRealHashSingleCandidateCount = 0.0;
    double avgRealHashSingleVerificationCount = 0.0;

    double avgFalseHashIndexOpenCount = 0.0;
    double avgFalseHashLookupCount = 0.0;
    double avgFalseHashCandidateCount = 0.0;
    double avgFalseHashVerificationCount = 0.0;

    double avgFalseHashSingleIndexOpenCount = 0.0;
    double avgFalseHashSingleLookupCount = 0.0;
    double avgFalseHashSingleCandidateCount = 0.0;
    double avgFalseHashSingleVerificationCount = 0.0;
};

struct AggregatedQueryTimings {
    TimingStatistics globalScanTimeStats;
    TimingStatistics hierarchicalMultiTimeStats;
    TimingStatistics hierarchicalSingleTimeStats;
    TimingStatistics hashIndexTimeStats;

    CountStatistics multiCol_bloomChecksStats;
    CountStatistics multiCol_leafBloomChecksStats;
    CountStatistics multiCol_sstChecksStats;

    CountStatistics singleCol_bloomChecksStats;
    CountStatistics singleCol_leafBloomChecksStats;
    CountStatistics singleCol_sstChecksStats;

    CountStatistics multiCol_nonLeafBloomChecksStats;
    CountStatistics singleCol_nonLeafBloomChecksStats;

    TimingStatistics multiCol_bloomChecksPerColumnStats;
    TimingStatistics multiCol_leafBloomChecksPerColumnStats;
    TimingStatistics multiCol_sstChecksPerColumnStats;
    TimingStatistics multiCol_nonLeafBloomChecksPerColumnStats;
    TimingStatistics singleCol_bloomChecksPerColumnStats;
    TimingStatistics singleCol_leafBloomChecksPerColumnStats;
    TimingStatistics singleCol_sstChecksPerColumnStats;
    TimingStatistics singleCol_nonLeafBloomChecksPerColumnStats;

    size_t numColumns;
};

struct QueryGenerationContext {
    std::vector<std::vector<bool>> patterns;
    std::vector<bool> isRealDataQuery;
    std::mt19937_64 queryRng;
    std::uniform_int_distribution<size_t> uniformDist;
    std::discrete_distribution<size_t> zipfDist;
};

struct GeneratedQuery {
    std::vector<std::string> values;
    bool isRealData = false;
};

struct RealDataExperimentResult {
    double realDataPercentage = 0.0;

    double avgRealDataMultiTime = 0.0;
    double avgRealDataSingleTime = 0.0;
    double avgRealHashIndexTime = 0.0;
    double avgRealHashSingleIndexTime = 0.0;
    double avgRealHashSingleHierarchyIndexTime = 0.0;

    double avgFalseDataMultiTime = 0.0;
    double avgFalseDataSingleTime = 0.0;
    double avgFalseHashIndexTime = 0.0;
    double avgFalseHashSingleIndexTime = 0.0;
    double avgFalseHashSingleHierarchyIndexTime = 0.0;

    double avgHierarchicalMultiTime = 0.0;
    double avgHierarchicalSingleTime = 0.0;
    double avgHashIndexTime = 0.0;
    double avgHashSingleIndexTime = 0.0;
    double avgHashSingleHierarchyIndexTime = 0.0;

    size_t positiveQueries = 0;
    size_t negativeQueries = 0;
    size_t totalQueries = 0;
};
