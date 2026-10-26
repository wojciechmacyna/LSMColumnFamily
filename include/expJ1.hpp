#pragma once

#include <string>

#include "db_manager.hpp"

void runExpJ1(std::string baseDir, bool initMode, std::string sharedDbName, int defaultNumRecords, 
    bool skipDbScan);