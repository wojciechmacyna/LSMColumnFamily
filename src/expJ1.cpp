
#include <vector>
#include <algorithm>
#include <string>
#include <filesystem>

#include "db_manager.hpp"
#include "expJ1.hpp"
#include "exp1.hpp"
#include "bloom_manager.hpp"

#include "experiment_initialization.hpp"
#include "comprehensive_query_analysis.hpp"
#include "csv_utils.hpp"



// Runs the J1 experiment for a specific data distribution configuration
// and cardinality percentage.
void runExpJ1ForConfig(
    const std::string& baseDir,
    size_t defaultNumRecords,
    const DistributionConfig& config,
    double capacityPercentage)
{
    const int dbSize = static_cast<int>(defaultNumRecords);

    // Calculate the cardinality based on the requested percentage
    // of the total number of records. Ensure that cardinality is at least 1.
    const size_t cardinality = std::max<size_t>(
        1,
        static_cast<size_t>(
            defaultNumRecords * capacityPercentage / 100.0));

    // Create a configuration copy and assign the calculated cardinality.
    DistributionConfig currentConfig = config;
    currentConfig.cardinality = cardinality;

    // Convert the distribution type into a string for logging and naming.
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

    // Generate a unique database name based on the experiment parameters.
    const std::string dbName =
        baseDir + "/expJ1_" +
        distributionName + "_" +
        std::to_string(dbSize) + "_" +
        std::to_string(capacityPercentage) + "_" +
        std::to_string(cardinality) + "_" +
        std::to_string(currentConfig.exponent) + "_" +
        std::to_string(currentConfig.zipfRatio);

    // Configure database initialization and data generation parameters.
    TestParams params{
        dbName,
        dbSize,
        5,
        1,
        20000,
        4'000'000,
        3
    };

    // Define the column families used in the experiment.
    const std::vector<std::string> columns{
        "phone", "mail", "address"
    };

    DBManager dbManager;
    BloomManager bloomManager;

    // Store the BloomTree hierarchy for each column family.
    std::map<std::string, BloomTree> hierarchies;

    // Store the SST files associated with each column family.
    std::map<std::string, std::vector<std::string>> columnSstFiles;

    // Initialize the database and build the required indexing structures.
    initialDB(
        dbManager,
        bloomManager,
        columns,
        params,
        hierarchies,
        columnSstFiles,
        currentConfig
    );

    // Execute the comprehensive query analysis for the current configuration.
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
    // Initialize CSV output files and write their headers.
    // --------------------------------------------------

    writeCsvHeader(
        "csv/expJ1.csv",
        "dbSize,distribution,exponent,zipfRatio,cardinality,"
        "realDataPercentage,"
        "avgRealMultiTime,avgRealSingleTime,"
        "avgRealHashTime,avgRealHashSingleTime,"
        "avgRealHashSingleHierarchyTime,"
        "avgRealHashMultiHierarchyIndexTime,"
        "avgFalseMultiTime,avgFalseSingleTime,"
        "avgFalseHashTime,avgFalseHashSingleTime,"
        "avgFalseHashSingleHierarchyTime,"
        "avgFalseHashMultiHierarchyIndexTime,"
        "avgHierarchicalMultiTime,avgHierarchicalSingleTime,"
        "avgHashIndexTime,avgHashSingleIndexTime,"
        "avgHashSingleHierarchyIndexTime,"
        "avgHashMultiHierarchyIndexTime"
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
    // Write the collected experiment results to CSV files.
    // --------------------------------------------------

    for (const auto& result : comprehensiveResults) {

        // Save query execution times for the different search methods.
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
                << result.avgRealHashMultiHierarchyIndexTime << ","
                << result.avgFalseDataMultiTime << ","
                << result.avgFalseDataSingleTime << ","
                << result.avgFalseHashIndexTime << ","
                << result.avgFalseHashSingleIndexTime << ","
                << result.avgFalseHashSingleHierachyIndexTime << ","
                << result.avgFalseHashMultiHierarchyIndexTime << ","
                << result.avgHierarchicalMultiTime << ","
                << result.avgHierarchicalSingleTime << ","
                << result.avgHashIndexTime << ","
                << result.avgHashSingleIndexTime << ","
                << result.avgHashSingleHierarchyIndexTime << ","
                << result.avgHashMultiHierarchyIndexTime
                << "\n";
        }

        // Save Bloom filter and SST lookup statistics for query analysis.
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

        // Save statistics normalized by the number of column families.
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
    // Close output files and release database resources.
    // --------------------------------------------------

    comprehensive_checks.close();
    per_column_stats.close();
    expJ1.close();

    dbManager.closeDB();

      // Remove the database directory after the experiment.
    std::error_code ec;
    std::filesystem::remove_all(dbName, ec);

    if (ec) {
        spdlog::error(
            "Failed to remove database directory '{}': {}",
            dbName,
            ec.message());
    }
    else {
        spdlog::info(
            "Database directory '{}' removed successfully.",
            dbName);
    }

    spdlog::info(
        "ExpJ1: Finished configuration. "
        "Distribution={}, exponent={}, zipfRatio={}, cardinality={}",
        distributionName,
        currentConfig.exponent,
        currentConfig.zipfRatio,
        currentConfig.cardinality
    );
}


// Runs the J1 experiment using multiple data distribution configurations.
void runExpJ1(
    std::string baseDir,
    bool initMode,
    std::string sharedDbName,
    int defaultNumRecords,
    bool skipDbScan)
{
    std::vector<DistributionConfig> configurations;

    // 1. Mixed distribution: 80% uniform and 20% Zipfian.
    {
        DistributionConfig config;
        config.type = DataDistribution::Mixed;
        config.exponent = 0.5;
        config.zipfRatio = 0.2;
        config.seed = 12345;
        configurations.push_back(config);
    }

    // 2. Normal distribution.
    {
        DistributionConfig config;
        config.type = DataDistribution::Normal;
        config.seed = 12345;
        configurations.push_back(config);
    }

    // 3. Zipfian distribution.

    {
        DistributionConfig config;
        config.type = DataDistribution::Zipfian;
        config.exponent = 0.1;
        config.seed = 12345;
        configurations.push_back(config);
    }


    {
        DistributionConfig config;
        config.type = DataDistribution::Zipfian;
        config.exponent = 0.4;
        config.seed = 12345;
        configurations.push_back(config);
    }

    {
        DistributionConfig config;
        config.type = DataDistribution::Zipfian;
        config.exponent = 0.8;
        config.seed = 12345;
        configurations.push_back(config);
    }

    // Define the percentage used to calculate the data cardinality.
    const double cardinalityPercentage = 10.0;

    spdlog::info(
        "Starting ExpJ1: records={}, configurations={}, capacity={}%",
        defaultNumRecords,
        configurations.size(),
        cardinalityPercentage
    );

    size_t experimentNumber = 0;

    // Execute the experiment once for each configured data distribution.
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
            cardinalityPercentage
        );

        runExpJ1ForConfig(
            baseDir,
            static_cast<size_t>(defaultNumRecords),
            config,
            cardinalityPercentage
        );
    }

    spdlog::info(
        "ExpJ1 completed. Total experiments: {}",
        experimentNumber
    );
}
