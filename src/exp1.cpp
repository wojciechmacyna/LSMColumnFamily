#include "exp1.hpp"

#include <spdlog/spdlog.h>

#include <boost/asio/post.hpp>
#include <boost/asio/thread_pool.hpp>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <future>
#include <iostream>
#include <map>
#include <string>
#include <thread>
#include <vector>

#include "algorithm.hpp"
#include "bloom_tree.hpp"
#include "bloom_manager.hpp"
#include "db_manager.hpp"
#include "exp_utils.hpp"
#include "stopwatch.hpp"
#include "exp_utils.hpp"

extern void clearBloomFilterFiles(const std::string& dbDir);
extern boost::asio::thread_pool globalThreadPool;


void runExp1(std::string baseDir, bool initMode, std::string sharedDbName,
             int defaultNumRecords, bool skipDbScan, const DistributionConfig& config) {


  const std::vector<std::string> columns = {"phone", "mail", "address"};
  //const std::vector<int> dbSizes = {10'000'000, defaultNumRecords, 100'000'000, 200'000'000};
  const std::vector<int> dbSizes = {5'000'000};


  for (const auto& dbSize : dbSizes) {
    std::string dbName = (dbSize == defaultNumRecords)
                             ? sharedDbName
                             : baseDir + "/exp1_db_" + std::to_string(dbSize);
    TestParams params = {dbName, dbSize, 5, 1, 20000, 4'000'000, 3};

    // Run stundard queries            
    const int numStandardQueries = 1;

    // Run comprehensive analysis across different real data percentages
    const int numQueriesPerScenario = 10;

    DBManager dbManager;
    BloomManager bloomManager;
    std::map<std::string, BloomTree> hierarchies;
    std::map<std::string, std::vector<std::string>> columnSstFiles;
    

    // Initializing databases
    initialDB(dbManager, bloomManager,  columns, params, hierarchies, columnSstFiles, config);
    

    // Run standard queries first
    AggregatedQueryTimings timings = runStandardQueries(
        dbManager, hierarchies, columns, dbSize, numStandardQueries, skipDbScan, config, &columnSstFiles);

    writeCsvHeader("csv/exp_1_basic_metrics.csv",
                 "dbSize,globalScanTime,hierarchicalSingleTime,hierarchicalMultiTime, hashScanTime");
    // Write basic metrics - timings only (4 columns)
    std::ofstream basic_metrics("csv/exp_1_basic_metrics.csv", std::ios::app);
    if (basic_metrics) {
      basic_metrics << dbSize << ","
                    << timings.globalScanTimeStats.average << ","
                    << timings.hierarchicalSingleTimeStats.average << ","
                    << timings.hierarchicalMultiTimeStats.average << ","
                    << timings.hashIndexTimeStats.average << "\n";
      basic_metrics.close();
    }

    // Write basic checks - check counts only (6 columns)
    writeCsvHeader("csv/exp_1_basic_checks.csv",
                 "dbSize,multibloomChecks, multiColleafBloomChecks, multiSstChecks, singlebloomChecks, singleleafBloomChecks, singleSstChecks");
    std::ofstream basic_checks("csv/exp_1_basic_checks.csv", std::ios::app);
    if (basic_checks) {
      basic_checks << dbSize << ","
                   << timings.multiCol_bloomChecksStats.average << ","
                   << timings.multiCol_leafBloomChecksStats.average << ","
                   << timings.multiCol_sstChecksStats.average << ","
                   << timings.singleCol_bloomChecksStats.average << ","
                   << timings.singleCol_leafBloomChecksStats.average << ","
                   << timings.singleCol_sstChecksStats.average << "\n";
      basic_checks.close();
    }

 
    
    spdlog::info("ExpBloomMetrics: Running comprehensive analysis for {} columns with {} queries per scenario", 
                 columns.size(), numQueriesPerScenario);
    std::vector<AccumulatedQueryMetrics> comprehensiveResults = runComprehensiveQueryAnalysis(
        dbManager, hierarchies, columns, dbSize, numQueriesPerScenario, config , &columnSstFiles);
    
    spdlog::info("ExpBloomMetrics: Generated {} comprehensive analysis results for {} columns", 
                 comprehensiveResults.size(), columns.size());



    // Write comprehensive analysis to focused CSV files
    writeCsvHeader("csv/exp_1_mixed_query_summary.csv",
                 "dbSize,realDataPercentage,totalQueries,realQueries,falseQueries,"
                 "avgMultiTime,avgSingleTime,avgHashTime, avgMultiBloomChecks,avgMultiSSTChecks");
    std::ofstream mixed_query_summary("csv/exp_1_mixed_query_summary.csv", std::ios::app);


    writeCsvHeader("csv/exp_1_timing_comparison.csv",
                 "dbSize,realDataPercentage,cardinality,"
                 "avgRealMultiTime,avgRealSingleTime,avgRealHashTime,avgRealHashSingleTime,avgRealHashSingleHierarchyTime,"
                 "avgFalseMultiTime,avgFalseSingleTime,avgFalseHashTime, avgFalseHashSingleTime, avgFalseHashSingleHierarchyTime,"
                 "avgHierarchicalMultiTime,avgHierarchicalSingleTime, avgHashIndexTime, avgHashSingleIndexTime, avgHashSingleHierarchyIndexTime,");
    std::ofstream timing_comparison("csv/exp_1_timing_comparison.csv", std::ios::app);

    writeCsvHeader("csv/exp_1_comprehensive_checks.csv",
                 "dbSize,realDataPercentage,"
                 "avgMultiBloomChecks,avgMultiLeafBloomChecks,avgMultiNonLeafBloomChecks,avgMultiSSTChecks,"
                 "avgSingleBloomChecks,avgSingleLeafBloomChecks,avgSingleNonLeafBloomChecks,avgSingleSSTChecks,"
                 "avgRealMultiBloomChecks,avgRealMultiSSTChecks,avgFalseMultiBloomChecks,avgFalseMultiSSTChecks");

    std::ofstream comprehensive_checks("csv/exp_1_comprehensive_checks.csv", std::ios::app);

    writeCsvHeader("csv/exp_1_per_column_stats.csv",
                 "dbSize,realDataPercentage,numColumns,"
                 "avgMultiBloomPerCol,avgMultiLeafPerCol,avgMultiNonLeafPerCol,avgMultiSSTPerCol,"
                 "avgSingleBloomPerCol,avgSingleLeafPerCol,avgSingleNonLeafPerCol,avgSingleSSTPerCol,"
                 "avgRealMultiBloomPerCol,avgRealMultiSSTPerCol,avgFalseMultiBloomPerCol,avgFalseMultiSSTPerCol");

    std::ofstream per_column_stats("csv/exp_1_per_column_stats.csv", std::ios::app);

    writeCsvHeader("csv/expJ1.csv",
                 "dbSize,realDataPercentage,cardinality,"
                 "avgRealMultiTime,avgRealSingleTime,avgRealHashIndexTime,"
                 "avgHierarchicalMultiTime,avgHierarchicalSingleTime,avgHashIndexTimeTime");

    std::ofstream expJ1("csv/expJ1.csv", std::ios::app);


    for (const auto& result : comprehensiveResults) {
      // Mixed query summary (8 columns)
      if (mixed_query_summary) {
        mixed_query_summary << dbSize << ","
                           << result.realDataPercentage << "," << result.totalQueries << ","
                           << result.realQueries << "," << result.falseQueries << ","
                           << result.avgHierarchicalMultiTime << "," << result.avgHierarchicalSingleTime << "," << result.avgHashIndexTime << ","
                           << result.avgMultiBloomChecks << "," << result.avgMultiSSTChecks << "\n";
      }

      // Timing comparison (8 columns)
      if (timing_comparison) {
        timing_comparison << dbSize << "," << result.realDataPercentage << "," << config.cardinality << "," 
                         << result.avgRealDataMultiTime << "," << result.avgRealDataSingleTime << "," << result.avgRealHashIndexTime 
                         << "," << result.avgRealHashSingleIndexTime << "," << result.avgRealHashSingleHierarchyIndexTime << ","
                         << result.avgFalseDataMultiTime << "," << result.avgFalseDataSingleTime << "," << result.avgFalseHashIndexTime << "," 
                         << result.avgFalseHashSingleIndexTime << "," << result.avgFalseHashSingleHierachyIndexTime << ","
                         << result.avgHierarchicalMultiTime << "," << result.avgHierarchicalSingleTime << "," << result.avgHashIndexTime << "," 
                         << result.avgHashSingleIndexTime << "," << result.avgHashSingleHierarchyIndexTime << "\n";
      }


      if (expJ1){
        expJ1 << dbSize << "," << result.realDataPercentage << ","
              << config.cardinality << ","
              << result.avgRealDataMultiTime << "," << result.avgRealDataSingleTime << "," << result.avgRealHashIndexTime << ","
              << result.avgHierarchicalMultiTime << "," << result.avgHierarchicalSingleTime << "," << result.avgHashIndexTime << "\n";
      }

      // Comprehensive checks (14 columns)
      if (comprehensive_checks) {
        comprehensive_checks << dbSize << "," << result.realDataPercentage << ","
                            << result.avgMultiBloomChecks << "," << result.avgMultiLeafBloomChecks << ","
                            << result.avgMultiNonLeafBloomChecks << "," << result.avgMultiSSTChecks << ","
                            << result.avgSingleBloomChecks << "," << result.avgSingleLeafBloomChecks << ","
                            << result.avgSingleNonLeafBloomChecks << "," << result.avgSingleSSTChecks << ","
                            << result.avgRealMultiBloomChecks << "," << result.avgRealMultiSSTChecks << ","
                            << result.avgFalseMultiBloomChecks << "," << result.avgFalseMultiSSTChecks << "\n";
      }

      // Per-column stats (14 columns)
      if (per_column_stats) {
        per_column_stats << dbSize << "," << result.realDataPercentage << "," << result.numColumns << ","
                        << result.avgMultiBloomChecksPerColumn << "," << result.avgMultiLeafBloomChecksPerColumn << ","
                        << result.avgMultiNonLeafBloomChecksPerColumn << "," << result.avgMultiSSTChecksPerColumn << ","
                        << result.avgSingleBloomChecksPerColumn << "," << result.avgSingleLeafBloomChecksPerColumn << ","
                        << result.avgSingleNonLeafBloomChecksPerColumn << "," << result.avgSingleSSTChecksPerColumn << ","
                        << result.avgRealMultiBloomChecksPerColumn << "," << result.avgRealMultiSSTChecksPerColumn << ","
                        << result.avgFalseMultiBloomChecksPerColumn << "," << result.avgFalseMultiSSTChecksPerColumn << "\n";
      }
    }
    mixed_query_summary.close();
    timing_comparison.close();
    comprehensive_checks.close();
    per_column_stats.close();
    expJ1.close();


    dbManager.closeDB();
  }
}

