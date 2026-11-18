#include <vector>
#include <algorithm>
#include <string>
#include <filesystem>

#include "db_manager.hpp"
#include "expJ1.hpp"
#include "exp1.hpp"
#include "exp_utils.hpp"
#include "bloom_manager.hpp"

void runExpJ1ForConfig(
    const std::string& baseDir,
    size_t defaultNumRecords,
    const DistributionConfig& config,
    double capacityPercentage)
{
    const int dbSize = static_cast<int>(defaultNumRecords);

    const size_t cardinality = std::max<size_t>(
        1,
        static_cast<size_t>(
            defaultNumRecords * capacityPercentage / 100.0));

    DistributionConfig currentConfig = config;
    currentConfig.cardinality = cardinality;

    std::string distributionName;

    switch (currentConfig.type) {
      case DataDistribution::Normal:
          distributionName = "Normal";
          break;

      case DataDistribution::Zipfian:
          distributionName = "Zipfian";
          break;

      case DataDistribution::Mixed:
          distributionName = "Mixed";
          break;
}

const std::string dbName =
    baseDir + "/expJ1_" +
    distributionName + "_" +
    std::to_string(dbSize) + "_" +
    std::to_string(capacityPercentage) + "_" +
    std::to_string(cardinality) + "_" +
    std::to_string(currentConfig.exponent) + "_" +
    std::to_string(currentConfig.zipfRatio);

    TestParams params{
        dbName,
        dbSize,
        5,
        1,
        20000,
        4'000'000,
        3
    };

    const std::vector<std::string> columns{
        "phone", "mail", "address"
    };

    DBManager dbManager;
    BloomManager bloomManager;

    std::map<std::string, BloomTree> hierarchies;

    std::map<std::string, std::vector<std::string>> columnSstFiles;

    // Otwarcie i inicjalizacja bazy oraz struktur indeksowych.
    initialDB(
        dbManager,
        bloomManager,
        columns,
        params,
        hierarchies,
        columnSstFiles,
        currentConfig
    );

    // Analiza zapytań.
    auto comprehensiveResults = runComprehensiveQueryAnalysis(
        dbManager,
        hierarchies,
        columns,
        dbSize,
        10,
        currentConfig,
        &columnSstFiles
    );

    spdlog::info(
            "ExpJ1: Generated {} comprehensive analysis results "
            "for {} columns",
            comprehensiveResults.size(),
            columns.size()
        );

        // --------------------------------------------------
        // CSV headers
        // --------------------------------------------------

        writeCsvHeader(
            "csv/expJ1.csv",
            "dbSize,distribution,exponent,zipfRatio,cardinality,"
            "realDataPercentage,"
            "avgRealMultiTime,avgRealSingleTime,"
            "avgRealHashTime,avgRealHashSingleTime,"
            "avgRealHashSingleHierarchyTime,"
            "avgFalseMultiTime,avgFalseSingleTime,"
            "avgFalseHashTime,avgFalseHashSingleTime,"
            "avgFalseHashSingleHierarchyTime,"
            "avgHierarchicalMultiTime,avgHierarchicalSingleTime,"
            "avgHashIndexTime,avgHashSingleIndexTime,"
            "avgHashSingleHierarchyIndexTime"
        );

        std::ofstream expJ1(
            "csv/expJ1.csv",
            std::ios::app
        );

        writeCsvHeader(
            "csv/exp_1_comprehensive_checks.csv",
            "dbSize,realDataPercentage,"
            "avgMultiBloomChecks,avgMultiLeafBloomChecks,"
            "avgMultiNonLeafBloomChecks,avgMultiSSTChecks,"
            "avgSingleBloomChecks,avgSingleLeafBloomChecks,"
            "avgSingleNonLeafBloomChecks,avgSingleSSTChecks,"
            "avgRealMultiBloomChecks,avgRealMultiSSTChecks,"
            "avgFalseMultiBloomChecks,avgFalseMultiSSTChecks"
        );

        std::ofstream comprehensive_checks(
            "csv/exp_1_comprehensive_checks.csv",
            std::ios::app
        );

        writeCsvHeader(
            "csv/exp_1_per_column_stats.csv",
            "dbSize,realDataPercentage,numColumns,"
            "avgMultiBloomPerCol,avgMultiLeafPerCol,"
            "avgMultiNonLeafPerCol,avgMultiSSTPerCol,"
            "avgSingleBloomPerCol,avgSingleLeafPerCol,"
            "avgSingleNonLeafPerCol,avgSingleSSTPerCol,"
            "avgRealMultiBloomPerCol,avgRealMultiSSTPerCol,"
            "avgFalseMultiBloomPerCol,avgFalseMultiSSTPerCol"
        );

        std::ofstream per_column_stats(
            "csv/exp_1_per_column_stats.csv",
            std::ios::app
        );

        // --------------------------------------------------
        // Save results
        // --------------------------------------------------

        for (const auto& result : comprehensiveResults) {

            // Timing comparison
            if (expJ1) {
                expJ1
                    << dbSize << ","
                    << distributionName << ","
                    << currentConfig.exponent << ","
                    << currentConfig.zipfRatio << ","
                    << currentConfig.cardinality << ","
                    << result.realDataPercentage << ","

                    << result.avgRealDataMultiTime << ","
                    << result.avgRealDataSingleTime << ","
                    << result.avgRealHashIndexTime << ","
                    << result.avgRealHashSingleIndexTime << ","
                    << result.avgRealHashSingleHierarchyIndexTime << ","

                    << result.avgFalseDataMultiTime << ","
                    << result.avgFalseDataSingleTime << ","
                    << result.avgFalseHashIndexTime << ","
                    << result.avgFalseHashSingleIndexTime << ","
                    << result.avgFalseHashSingleHierachyIndexTime << ","

                    << result.avgHierarchicalMultiTime << ","
                    << result.avgHierarchicalSingleTime << ","
                    << result.avgHashIndexTime << ","
                    << result.avgHashSingleIndexTime << ","
                    << result.avgHashSingleHierarchyIndexTime
                    << "\n";
            }

            // Comprehensive checks
            if (comprehensive_checks) {
                comprehensive_checks
                    << dbSize << ","
                    << result.realDataPercentage << ","

                    << result.avgMultiBloomChecks << ","
                    << result.avgMultiLeafBloomChecks << ","
                    << result.avgMultiNonLeafBloomChecks << ","
                    << result.avgMultiSSTChecks << ","

                    << result.avgSingleBloomChecks << ","
                    << result.avgSingleLeafBloomChecks << ","
                    << result.avgSingleNonLeafBloomChecks << ","
                    << result.avgSingleSSTChecks << ","

                    << result.avgRealMultiBloomChecks << ","
                    << result.avgRealMultiSSTChecks << ","
                    << result.avgFalseMultiBloomChecks << ","
                    << result.avgFalseMultiSSTChecks
                    << "\n";
            }

            // Per-column statistics
            if (per_column_stats) {
                per_column_stats
                    << dbSize << ","
                    << result.realDataPercentage << ","
                    << result.numColumns << ","

                    << result.avgMultiBloomChecksPerColumn << ","
                    << result.avgMultiLeafBloomChecksPerColumn << ","
                    << result.avgMultiNonLeafBloomChecksPerColumn << ","
                    << result.avgMultiSSTChecksPerColumn << ","

                    << result.avgSingleBloomChecksPerColumn << ","
                    << result.avgSingleLeafBloomChecksPerColumn << ","
                    << result.avgSingleNonLeafBloomChecksPerColumn << ","
                    << result.avgSingleSSTChecksPerColumn << ","

                    << result.avgRealMultiBloomChecksPerColumn << ","
                    << result.avgRealMultiSSTChecksPerColumn << ","
                    << result.avgFalseMultiBloomChecksPerColumn << ","
                    << result.avgFalseMultiSSTChecksPerColumn
                    << "\n";
            }
        }

        // --------------------------------------------------
        // Cleanup
        // --------------------------------------------------

        comprehensive_checks.close();
        per_column_stats.close();
        expJ1.close();

        dbManager.closeDB();

        spdlog::info(
            "ExpJ1: Finished configuration. "
            "Distribution={}, exponent={}, zipfRatio={}, cardinality={}",
            distributionName,
            config.exponent,
            config.zipfRatio,
            config.cardinality
        );
}

void runExpJ1(
    std::string baseDir,
    bool initMode,
    std::string sharedDbName,
    int defaultNumRecords,
    bool skipDbScan)
{
    std::vector<DistributionConfig> configurations;

    // 1. Rozkład mieszany: 80% uniform + 20% Zipf.
    {
        DistributionConfig config;
        config.type = DataDistribution::Mixed;
        config.exponent = 0.5;
        config.zipfRatio = 0.2;
        config.seed = 12345;
        configurations.push_back(config);
    }

    // 2. Rozkład Normal.
    {
        DistributionConfig config;
        config.type = DataDistribution::Normal;
        config.seed = 12345;
        configurations.push_back(config);
    }

    // 3. Rozkład Zipfian.
    {
        DistributionConfig config;
        config.type = DataDistribution::Zipfian;
        config.exponent = 0.5;
        config.seed = 12345;
        configurations.push_back(config);
    }

    const double cardinalityPercentage = 10.0;

    spdlog::info(
        "Starting ExpJ1: records={}, configurations={}, capacity={}%",
        defaultNumRecords,
        configurations.size(),
        cardinalityPercentage);

    size_t experimentNumber = 0;

    for (const auto& config : configurations) {
        ++experimentNumber;

        spdlog::info(
            "ExpJ1 experiment {}/{}: type={}, exponent={}, "
            "zipfRatio={}, capacity={}%",
            experimentNumber,
            configurations.size(),
            static_cast<int>(config.type),
            config.exponent,
            config.zipfRatio,
            cardinalityPercentage);

        runExpJ1ForConfig(
            baseDir,
            static_cast<size_t>(defaultNumRecords),
            config,
            cardinalityPercentage);
    }

    spdlog::info(
        "ExpJ1 completed. Total experiments: {}",
        experimentNumber);
}


/*
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

}*/