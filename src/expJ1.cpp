#include <vector>
#include <algorithm>
#include <string>
#include <filesystem>

#include "db_manager.hpp"
#include "expJ1.hpp"
#include "exp1.hpp"
#include "exp_utils.hpp"
#include "bloom_manager.hpp"


void runExpJ1(std::string baseDir, bool initMode, std::string sharedDbName,
             int defaultNumRecords, bool skipDbScan){

        DistributionConfig config;
        config.type = DataDistribution::Zipfian;
        config.exponent = 1.0;

        const std::vector<std::string> columns = {"phone", "mail", "address"};

        // const std::vector<double> capacityPercentages = {1.0};
             const std::vector<double> capacityPercentages = {0.01, 0.1, 1.0};
        int dbSize = defaultNumRecords;        

        for (const double percentage : capacityPercentages)
        {
            config.cardinality = std::max<size_t>(1,static_cast<size_t>(defaultNumRecords * percentage / 100.0));

            std::string dbName = baseDir + "/dist_shared_db" + std::to_string(percentage);

            TestParams params = {dbName, dbSize, 5, 1, 20000, 4'000'000, 3};

           // Run stundard queries            
           const int numStandardQueries = 1;

           // Run comprehensive analysis across different real data percentages
           const int numQueriesPerScenario = 10;

           DBManager dbManager;
           BloomManager bloomManager;
           std::map<std::string, BloomTree> hierarchies;
           std::map<std::string, std::vector<std::string>> columnSstFiles;
                
           initialDB(dbManager, bloomManager,  columns, params, hierarchies, columnSstFiles, config);




           spdlog::info("ExpBloomMetrics: Running comprehensive analysis for {} columns with {} queries per scenario", 
                 columns.size(), numQueriesPerScenario);
    std::vector<AccumulatedQueryMetrics> comprehensiveResults = runComprehensiveQueryAnalysis(
        dbManager, hierarchies, columns, dbSize, numQueriesPerScenario, config , &columnSstFiles);
    
    spdlog::info("ExpBloomMetrics: Generated {} comprehensive analysis results for {} columns", 
                 comprehensiveResults.size(), columns.size());



    writeCsvHeader("csv/expJ1.csv",
                 "dbSize,realDataPercentage,cardinality,"
                 "avgRealMultiTime,avgRealSingleTime,avgRealHashTime,avgRealHashSingleTime,avgRealHashSingleHierarchyTime,"
                 "avgFalseMultiTime,avgFalseSingleTime,avgFalseHashTime, avgFalseHashSingleTime, avgFalseHashSingleHierarchyTime,"
                 "avgHierarchicalMultiTime,avgHierarchicalSingleTime, avgHashIndexTime, avgHashSingleIndexTime, avgHashSingleHierarchyIndexTime,");
        std::ofstream expJ1("csv/expJ1.csv", std::ios::app);

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

  


    for (const auto& result : comprehensiveResults) {

 
      // Timing comparison (8 columns)
      if (expJ1) {
        expJ1 << dbSize << "," << result.realDataPercentage << "," << config.cardinality << "," 
                         << result.avgRealDataMultiTime << "," << result.avgRealDataSingleTime << "," << result.avgRealHashIndexTime 
                         << "," << result.avgRealHashSingleIndexTime << "," << result.avgRealHashSingleHierarchyIndexTime << ","
                         << result.avgFalseDataMultiTime << "," << result.avgFalseDataSingleTime << "," << result.avgFalseHashIndexTime << "," 
                         << result.avgFalseHashSingleIndexTime << "," << result.avgFalseHashSingleHierachyIndexTime << ","
                         << result.avgHierarchicalMultiTime << "," << result.avgHierarchicalSingleTime << "," << result.avgHashIndexTime << "," 
                         << result.avgHashSingleIndexTime << "," << result.avgHashSingleHierarchyIndexTime << "\n";
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

    comprehensive_checks.close();
    per_column_stats.close();
    expJ1.close();


    dbManager.closeDB();
        };

}