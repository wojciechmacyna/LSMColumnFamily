#pragma once

#include <string>

#include "db_manager.hpp"

void runExpJ2(std::string baseDir, bool initMode, std::string sharedDbName, int defaultNumRecords, 
    bool skipDbScan, const DistributionConfig& config = {});

