
#include <string>
#include <filesystem>

#include "db_manager.hpp"
#include "expJ1.hpp"
#include "exp1.hpp"

void runExpJ1(std::string baseDir, bool initMode, std::string sharedDbName,
             int defaultNumRecords, bool skipDbScan){

        DistributionConfig config;
        config.type = DataDistribution::Zipfian;
        config.cardinality = 5000000;
        config.exponent = 1.0;

        namespace fs = std::filesystem;

        fs::remove_all(sharedDbName);

        runExp1(baseDir, initMode, sharedDbName, defaultNumRecords, skipDbScan, config);

}