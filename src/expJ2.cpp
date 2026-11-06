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
#include "bloomTree.hpp"
#include "bloom_manager.hpp"
#include "db_manager.hpp"
#include "exp_utils.hpp"
#include "stopwatch.hpp"


extern void clearBloomFilterFiles(const std::string& dbDir);
extern boost::asio::thread_pool globalThreadPool;


void runExpJ2(std::string baseDir, bool initMode, std::string sharedDbName,
             int defaultNumRecords, bool skipDbScan, const DistributionConfig& config) {
  
  const std::vector<std::string> columns = {"phone", "mail", "address"};
  //const std::vector<int> dbSizes = {10'000'000, defaultNumRecords, 50'000'000, 500'000'000};
  const std::vector<int> dbSizes = {defaultNumRecords};

  // Run stundard queries            
  const int numStandardQueries = 10;

  // Run comprehensive analysis across different real data percentages
  const int numQueriesPerScenario = 100;

  DBManager dbManager;
  BloomManager bloomManager;
  StopWatch stopwatch;

  for (const auto& dbSize : dbSizes) {
    std::string dbName = (dbSize == defaultNumRecords)
                             ? sharedDbName
                             : baseDir + "/exp1_db_" + std::to_string(dbSize);
    TestParams params = {dbName, dbSize, 3, 1, 100000, 4'000'000, 3};
    spdlog::info("ExpBloomMetrics: Starting the experiment for the database: '{}'",
                 params.dbName);
    clearBloomFilterFiles(params.dbName);

    stopwatch.start();
    if (std::filesystem::exists(params.dbName)) {
      spdlog::info(
          "EXP1: Database '{}' already exists, skipping initialization.",
          params.dbName);
      dbManager.openDB(params.dbName, columns);
    } else {
      dbManager.openDB(params.dbName, columns);
      dbManager.insertRecords(params.numRecords, columns, config);
      try {
        dbManager.compactAllColumnFamilies(params.numRecords);
      } catch (const std::exception& e) {
        spdlog::error("Error '{}'", e.what());
        exit(1);
      }
    }
    stopwatch.stop();
    auto dbCreationTime = stopwatch.elapsedMicros();

    spdlog::info("Making scan SST Files");

    stopwatch.start();
    std::map<std::string, std::vector<std::string>> columnSstFiles =
        scanSstFilesAsync(columns, dbManager, params);
    std::map<std::string, BloomTree> hierarchies;

    spdlog::info("Build hierarchies");

    hierarchies = buildHierarchies(columnSstFiles, bloomManager, params);
    stopwatch.stop();
    auto bloomCreationTime = stopwatch.elapsedMicros();

    dbManager.closeDB();


    // Add additional records
    dbManager.openDB(params.dbName, columns, true);       
    spdlog::info("Inserting extra records.");

    const int numBatches = 1;
    const int batchSize = 5'000'000;

    for (int batch = 0; batch < numBatches; ++batch) {

    const int startRecord =
        params.numRecords + batch * batchSize + 1;

    const int endRecord =
        params.numRecords + (batch + 1) * batchSize;

    spdlog::info(
        "INSERTING BATCH {}: records [{}, {}]",
        batch + 1,
        startRecord,
        endRecord
    );

    dbManager.insertRecords(
        endRecord,
        columns,
        config,
        startRecord
    );

    try {
        dbManager.compactAllColumnFamilies(endRecord);
    }
    catch (const std::exception& e) {
        spdlog::error("Compaction error: {}", e.what());
        throw;
    }

    stopwatch.start();

    appendSstBatch(
        columns,
        dbManager,
        bloomManager,
        params,
        hierarchies
    );

    stopwatch.stop();

    spdlog::info(
        "BATCH {} PROCESSING TIME: {} µs",
        batch + 1,
        stopwatch.elapsedMicros()
    );
}

    dbManager.closeDB();
  }
}

