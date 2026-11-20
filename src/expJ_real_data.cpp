#include <string>

#include "expJ_real_data.hpp"

#include "real_data_experiment.hpp"



void runExp_real_data(std::string baseDir, bool initMode, std::string sharedDbName, int defaultNumRecords, 
    bool skipDbScan){
        runRealDataExperiment(
            "data",
            "data/NYCTaxiDB",
            100'000,
            20);
    }