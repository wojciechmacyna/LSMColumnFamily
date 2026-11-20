#pragma once

#include <future>
#include <map>
#include <string>
#include <vector>
#include <random>
#include <unordered_set>

#include <fstream>
#include <filesystem>
#include <stdexcept>


#include "test_params.hpp"
#include "db_manager.hpp"

// Forward declarations
class DBManager;
class BloomManager;
class BloomTree;

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
  
  // Derived metrics: bloom checks minus leaf checks
  size_t multiCol_nonLeafBloomChecks;
  size_t singleCol_nonLeafBloomChecks;
  
  // Per-column averages (total counts divided by column count)
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

  // Hash Index - Multi-Column
double hashIndexTime = 0.0;
size_t hashIndexOpenCount = 0;
size_t hashLookupCount = 0;
size_t hashCandidateCount = 0;
size_t hashVerificationCount = 0;

// Hash Index - Single-Column
double hashSingleIndexTime = 0.0;
size_t hashSingleIndexOpenCount = 0;
size_t hashSingleLookupCount = 0;
size_t hashSingleCandidateCount = 0;
size_t hashSingleVerificationCount = 0;
size_t matchingColumns = 0;
  
  // Derived metrics: bloom checks minus leaf checks
  size_t multiCol_nonLeafBloomChecks;
  size_t singleCol_nonLeafBloomChecks;
  
  // Per-column averages (total counts divided by column count)
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
  size_t numColumns;  // Number of columns for reference
  
  // Average timing metrics
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
  
  // Average check metrics
  double avgMultiBloomChecks;
  double avgMultiLeafBloomChecks;
  double avgMultiSSTChecks;
  double avgSingleBloomChecks;
  double avgSingleLeafBloomChecks;
  double avgSingleSSTChecks;
  
  // Derived average check metrics
  double avgMultiNonLeafBloomChecks;
  double avgSingleNonLeafBloomChecks;
  
  // Per-column averages (dividing total by column count)
  double avgMultiBloomChecksPerColumn;
  double avgMultiLeafBloomChecksPerColumn;
  double avgMultiSSTChecksPerColumn;
  double avgMultiNonLeafBloomChecksPerColumn;
  double avgSingleBloomChecksPerColumn;
  double avgSingleLeafBloomChecksPerColumn;
  double avgSingleSSTChecksPerColumn;
  double avgSingleNonLeafBloomChecksPerColumn;
  
  // Separate averages for real vs false data
  double avgRealMultiBloomChecks;
  double avgRealMultiSSTChecks;
  double avgFalseMultiBloomChecks;
  double avgFalseMultiSSTChecks;
  
  // Per-column averages for real vs false data
  double avgRealMultiBloomChecksPerColumn;
  double avgRealMultiSSTChecksPerColumn;
  double avgFalseMultiBloomChecksPerColumn;
  double avgFalseMultiSSTChecksPerColumn;

  // Average Hash Index counters
double avgHashIndexOpenCount = 0.0;
double avgHashLookupCount = 0.0;
double avgHashCandidateCount = 0.0;
double avgHashVerificationCount = 0.0;

double avgHashSingleIndexOpenCount = 0.0;
double avgHashSingleLookupCount = 0.0;
double avgHashSingleCandidateCount = 0.0;
double avgHashSingleVerificationCount = 0.0;

// Average Real Hash Index counters
double avgRealHashIndexOpenCount = 0.0;
double avgRealHashLookupCount = 0.0;
double avgRealHashCandidateCount = 0.0;
double avgRealHashVerificationCount = 0.0;

double avgRealHashSingleIndexOpenCount = 0.0;
double avgRealHashSingleLookupCount = 0.0;
double avgRealHashSingleCandidateCount = 0.0;
double avgRealHashSingleVerificationCount = 0.0;

// Average False Hash Index counters
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
  
  // Derived statistics: bloom checks minus leaf checks
  CountStatistics multiCol_nonLeafBloomChecksStats;
  CountStatistics singleCol_nonLeafBloomChecksStats;
  
  // Per-column statistics (dividing by column count)
  TimingStatistics multiCol_bloomChecksPerColumnStats;
  TimingStatistics multiCol_leafBloomChecksPerColumnStats;
  TimingStatistics multiCol_sstChecksPerColumnStats;
  TimingStatistics multiCol_nonLeafBloomChecksPerColumnStats;
  TimingStatistics singleCol_bloomChecksPerColumnStats;
  TimingStatistics singleCol_leafBloomChecksPerColumnStats;
  TimingStatistics singleCol_sstChecksPerColumnStats;
  TimingStatistics singleCol_nonLeafBloomChecksPerColumnStats;
  
  size_t numColumns;  // Number of columns for reference
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



void runRealDataExperiment(
    const std::string& baseDir,
    const std::string& dbName,
    size_t sampleSize,
    int numQueries);


uint64_t buildHashIndexesAsync(
    const std::map<std::string, std::vector<std::string>>& columnSstFiles);


std::map<std::string, std::vector<std::string>> scanSstFilesAsync(
    const std::vector<std::string>& columns, DBManager& dbManager,
    const TestParams& params, const bool scanInsertedFiles=false);

std::map<std::string, BloomTree> buildHierarchies(
    const std::map<std::string, std::vector<std::string>>& columnSstFiles,
    BloomManager& bloomManager, const TestParams& params);

std::map<std::string, std::vector<Node*>> buildLeaves(
    const std::map<std::string, std::vector<std::string>>& columnSstFiles,
    BloomManager& bloomManager,
    const TestParams& params
);


RealDataExperimentResult aggregateMixedQueryResults(
    const std::vector<MixedQueryResult>& results);

void saveRealDataExperimentResults(
    const std::string& filename,
    const RealDataExperimentResult& result,
    size_t dbSize);

    void saveRealDataStructureStats(
    const std::string& filename,
    const std::map<std::string, std::vector<std::string>>& columnSstFiles,
    const std::map<std::string, BloomTree>& hierarchies,
    uint64_t hashIndexSizeBytes);

RealDataQueryContext loadRealDataQueryContext(
    DBManager& dbManager,
    const std::vector<std::string>& columns,
    size_t sampleSize,
    uint64_t seed);

RealDataQueryContext createRealDataQueryContext(
    DBManager& dbManager,
    const std::vector<std::string>& columns,
    size_t sampleSize,
    uint64_t seed);

std::vector<MixedQueryResult> runMixedQueriesWithRealData(
    DBManager& dbManager,
    const std::map<std::string, BloomTree>& hierarchies,
    const std::vector<std::string>& columns,
    RealDataQueryContext& realDataContext,
    int numQueries,
    double positiveQueryPercentage,
    const std::map<std::string, std::vector<std::string>>* columnSstFiles);


void appendSstBatch(
    const std::vector<std::string>& columns,
    DBManager& dbManager,
    BloomManager& bloomManager,
    const TestParams& params,
    std::map<std::string, BloomTree>& hierarchies
);


// It takes numRuns values to search using three methods: scan, multi and single.
// The results are avg or median calculated from numRuns
AggregatedQueryTimings runStandardQueries(
    DBManager& dbManager, const std::map<std::string, BloomTree>& hierarchies,
    const std::vector<std::string>& columns,
    size_t dbSizeForExpectedValues,  // This might need to become a list or
                                     // generate multiple values
    int numRuns = 10,  // Added numRuns parameter with a default value
    bool skipDbScan = false, const DistributionConfig& config = {}, const std::map<std::string, std::vector<std::string>>* columnSstFiles = nullptr);

AggregatedQueryTimings runStandardQueriesWithTarget(
    DBManager& dbManager, const std::map<std::string, BloomTree>& hierarchies,
    const std::vector<std::string>& columns, size_t dbSize, int numRuns,
    bool skipDbScan, std::vector<std::string> currentExpectedValues);

// Helper function to generate dynamic patterns based on column count
std::vector<std::vector<bool>> generateDynamicPatterns(size_t numColumns);

// Test function to verify pattern generation (for debugging)
void testPatternGeneration();

std::vector<BloomTree> prepareQueryTrees(
    const std::map<std::string, BloomTree>& hierarchies,
    const std::vector<std::string>& columns);


GeneratedQuery generateMixedQuery(
    int queryIdx,
    const std::vector<std::string>& columns,
    QueryGenerationContext& context,
    const DistributionConfig& config);


QueryGenerationContext createQueryGenerationContext(
    size_t numColumns,
    size_t dbSize,
    int numQueries,
    double realDataPercentage,
    const DistributionConfig& config);

    void runHashIndexQueries(
    DBManager& dbManager,
    const std::vector<std::string>& columns,
    const std::vector<std::string>& values,
    const std::map<std::string, std::vector<std::string>>* columnSstFiles,
    MixedQueryResult& result);

    void runHashSingleHierarchyQuery(
    DBManager& dbManager,
    BloomTree& queryTree,
    const std::vector<std::string>& columns,
    const std::vector<std::string>& values,
    MixedQueryResult& result);
    
    void runHierarchicalQueries(
    DBManager& dbManager,
    std::vector<BloomTree>& queryTrees,
    const std::vector<std::string>& columns,
    const std::vector<std::string>& values,
    MixedQueryResult& result);


// Function to run pattern queries and collect individual results
std::vector<PatternQueryResult> runPatternQueriesWithCsvData(
    DBManager& dbManager, const std::map<std::string, BloomTree>& hierarchies,
    const std::vector<std::string>& columns, size_t dbSize, const DistributionConfig& config = {});

// Function to run N queries with X% real data and (100-X)% false data
std::vector<MixedQueryResult> runMixedQueriesWithCsvData(
    DBManager& dbManager, const std::map<std::string, BloomTree>& hierarchies,
    const std::vector<std::string>& columns, size_t dbSize, int numQueries, 
    double realDataPercentage, const DistributionConfig& config = {},
    const std::map<std::string, std::vector<std::string>>* columnSstFiles = nullptr);

// Function to run comprehensive analysis across multiple real data percentages
std::vector<AccumulatedQueryMetrics> runComprehensiveQueryAnalysis(
    DBManager& dbManager, const std::map<std::string, BloomTree>& hierarchies,
    const std::vector<std::string>& columns, size_t dbSize, int numQueriesPerScenario, const DistributionConfig& config = {},
    const std::map<std::string, std::vector<std::string>>* columnSstFiles = nullptr);

    
void initialDB(DBManager& dbManager, BloomManager& bloomManager,   const std::vector<std::string>& columns, 
  TestParams params, std::map<std::string, BloomTree>& hierarchies,  
  std::map<std::string, std::vector<std::string>> &columnSstFiles, const DistributionConfig config);

void runNYCTaxiImport();

void writeCsvHeader(const std::string& filename, const std::string& headerLine);

double getProbabilityOfFalsePositive(size_t bloomSize, int numHashFunctions,
                                     size_t itemsPerPartition);

// Helper function to calculate statistics
template <typename T>
TimingStatistics calculateNumericStatistics(const std::vector<T>& values);

template <typename T>
CountStatistics calculateCountStatistics(const std::vector<T>& values);