#include <vector>
#include <algorithm>
#include <string>
#include <filesystem>

#include "db_manager.hpp"
#include "expJ1.hpp"
#include "exp1.hpp"

void runExpJ1(std::string baseDir, bool initMode, std::string sharedDbName,
             int defaultNumRecords, bool skipDbScan){

        namespace fs = std::filesystem;
        DistributionConfig config;
        config.type = DataDistribution::Zipfian;
        config.exponent = 1.0;

///        const std::vector<double> capacityPercentages = {1.0, 5.0, 20.0, 50.0};

        const std::vector<double> capacityPercentages = {1.0};

        for (const double percentage : capacityPercentages)
        {
            config.cardinality = std::max<size_t>(1,static_cast<size_t>(defaultNumRecords * percentage / 100.0));
            fs::remove_all(sharedDbName);
            runExp1(baseDir, initMode, sharedDbName, defaultNumRecords, skipDbScan, config);
        };

}