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

    size_t totalDiskBloomSize = 0;
    size_t totalMemoryBloomSize = 0;
    for (const auto& kv : hierarchies) {
      const BloomTree& tree = kv.second;
      totalDiskBloomSize += tree.diskSize();
      totalMemoryBloomSize += tree.memorySize();
    }

    dbManager.closeDB();


    // Add additional records
    dbManager.openDB(params.dbName, columns, true);       
    spdlog::info("Inserting extra records.");

    const int numAdditionalRecords = 200000;
    const int numInsertedRecords = params.numRecords;

//      int keyEnd = keyStart + numAdditionalRecords;
      
 //     int keyStart =   
 //     int numRecordsToInsert = numInsertedRecords + numAdditionalRecords;

      dbManager.insertRecords(params.numRecords+numAdditionalRecords, columns, config, params.numRecords+1);
      try {
        //dbManager.compactAllColumnFamilies(params.numRecords+numAdditionalRecords);
        dbManager.flushAllColumnFamilies();
      } catch (const std::exception& e) {
        spdlog::error("Error '{}'", e.what());
        exit(1);
      }      


    /*
    stopwatch.start();
    std::map<std::string, std::vector<std::string>> columnSstFilesInserted =
        scanSstFilesAsync(columns, dbManager, params, true);
    std::map<std::string, BloomTree> hierarchiesInserted;
    hierarchiesInserted = buildHierarchies(columnSstFilesInserted, bloomManager, params);
    stopwatch.stop();
*/


stopwatch.start();

auto columnSstFilesInserted =
    scanSstFilesAsync(columns, dbManager, params, true);

for (const auto& [column, files] : columnSstFilesInserted) {
    std::cout << "\nColumn Family: " << column << '\n';
    std::cout << "Number of SST files: " << files.size() << '\n';

    for (const auto& filename : files) {
        std::cout << "  " << filename << '\n';
          }
}

auto hierarchiesInserted =
    buildHierarchies(columnSstFilesInserted, bloomManager, params);

// Incremental update
for (auto& [column, insertedTree] : hierarchiesInserted)
{
    auto it = hierarchies.find(column);

    if (it == hierarchies.end()) {
        throw std::runtime_error(
            "Missing existing hierarchy for column: " + column
        );
    }

    it->second.appendLeaves(insertedTree);

    std::string error;
    if (!it->second.validateTree(error)) {
        throw std::runtime_error("Tree validation: " + error);
    }

    if (!it->second.validateBloomFilter(it->second.root, error)) {
        throw std::runtime_error("Bloom validation: " + error);
    }
}


for (const auto& [column, tree] : hierarchies)
{
    spdlog::info(
        "FINAL HIERARCHY: CF={}, leaves={}, root range=[{}, {}]",
        column,
        tree.leafNodes.size(),
        tree.root->startKey,
        tree.root->endKey
    );
}


stopwatch.stop();



/*

        dbManager.insertRecords(params.numRecords+numAdditionalRecords+numAdditionalRecords, columns, config, params.numRecords+numAdditionalRecords+1);
      try {
        dbManager.compactAllColumnFamilies(params.numRecords+numAdditionalRecords+numAdditionalRecords);
      } catch (const std::exception& e) {
        spdlog::error("Error '{}'", e.what());
        exit(1);
      }     
*/



    dbManager.closeDB();
  }
}

