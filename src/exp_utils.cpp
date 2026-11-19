#include "exp_utils.hpp"

#include <spdlog/spdlog.h>

#include <boost/asio/post.hpp>
#include <boost/asio/thread_pool.hpp>
#include <chrono>
#include <cmath>
#include <fstream>
#include <random>
#include <sstream>
#include <thread>
#include <filesystem>
#include <regex>

#include "algorithm.hpp"
#include "bloom_tree.hpp"
#include "bloom_manager.hpp"
#include "db_manager.hpp"
#include "stopwatch.hpp"
#include "sst_hash_index.hpp"
#include "stopwatch.hpp"

extern boost::asio::thread_pool globalThreadPool;
extern std::atomic<size_t> gBloomCheckCount;
extern std::atomic<size_t> gLeafBloomCheckCount;
extern std::atomic<size_t> gSSTCheckCount;

void clearBloomFilterFiles(const std::string& dbDir) {
  std::regex bloomFilePattern(R"(^\d+\.sst_[^_]+_[^_]+$)");
  std::error_code ec;

  for (auto const& entry : std::filesystem::directory_iterator(dbDir, ec)) {
    if (ec) {
      spdlog::error("Failed to iterate '{}': {}", dbDir, ec.message());
      return;
    }

    auto const& path = entry.path();
    auto filename = path.filename().string();

    if (entry.is_regular_file() &&
        std::regex_match(filename, bloomFilePattern)) {
      if (std::filesystem::remove(path, ec)) {
      } else {
        spdlog::error("Could not remove '{}': {}", path.string(), ec.message());
      }
    }
  }
}


void runNYCTaxiImport()
{
    const std::string dbName = "data/NYCTaxiDB";
    const std::string csvFile = "data/yellow_tripdata_2016-03.csv";

    const std::vector<std::string> columns = {
        "pickup_location",
        "dropoff_location",
        "payment_type",
        "passenger_count",
        "vendor_id",
        "ratecode_id"
    };

    DBManager dbManager;

    // Open database and create required Column Families.
    dbManager.openDB(dbName, columns, true);

    // Import first 10 million CSV records.
    dbManager.importNYCTaxiCSV(
        csvFile,
        10'000'000,
        10'000);

    dbManager.closeDB();
}



void initialDB(DBManager& dbManager, BloomManager& bloomManager,   const std::vector<std::string>& columns, 
  TestParams params, std::map<std::string, BloomTree>& hierarchies,  
  std::map<std::string, std::vector<std::string>> &columnSstFiles, const DistributionConfig config){

   spdlog::info("ExpBloomMetrics: Starting the experiment for the database: '{}'",
                 params.dbName);
    clearBloomFilterFiles(params.dbName);

    StopWatch stopwatch;

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

    stopwatch.start();
    columnSstFiles =
        scanSstFilesAsync(columns, dbManager, params);
    
    hierarchies = buildHierarchies(columnSstFiles, bloomManager, params);
    stopwatch.stop();
    auto bloomCreationTime = stopwatch.elapsedMicros();

    stopwatch.start();
    uint64_t hashIndexSize = buildHashIndexesAsync(columnSstFiles);
    stopwatch.stop();
    auto hashIndexCreationTime = stopwatch.elapsedMicros();

    size_t totalDiskBloomSize = 0;
    size_t totalMemoryBloomSize = 0;
    for (const auto& kv : hierarchies) {
      const BloomTree& tree = kv.second;
      totalDiskBloomSize += tree.diskSize();
      totalMemoryBloomSize += tree.memorySize();
    }

    writeCsvHeader(
      "csv/exp_1_bloom_metrics.csv",
      "numRecords,bloomTreeRatio,itemsPerPartition,bloomSize,numHashFunctions,"
      "singleHierarchyLeafs,bloomDiskSize,blomMemSize, hashIndexSize");

    std::ofstream out("csv/exp_1_bloom_metrics.csv", std::ios::app);
    if (!out) {
      spdlog::error(
          "ExpBloomMetrics: Nie udało się otworzyć pliku wynikowego!");
      return;
    }
    out << params.numRecords << "," << params.bloomTreeRatio << ","
        << params.itemsPerPartition << "," << params.bloomSize << ","
        << params.numHashFunctions << ","
        << hierarchies.at(columns[0]).leafNodes.size() << ","
        << totalDiskBloomSize << "," << totalMemoryBloomSize << "," << hashIndexSize <<  "\n";
    out.close();
    spdlog::info("ExpBloomMetrics: Experiment for database '{}' finished.",
                 params.dbName);


    /// Creation time metrics csv/exp_3_creation_time.csv
     writeCsvHeader("csv/exp_1_creation_time.csv",
                 "numRecords,dbCreationTime, bloomCreationTime, hashIndexCreationTime");

    std::ofstream outExp3("csv/exp_1_creation_time.csv", std::ios::app);
    if (!outExp3) {
      spdlog::error(
          "ExpBloomMetrics: Nie udało się otworzyć pliku wynikowego!");
      return;
    }
    outExp3 << params.numRecords << "," << dbCreationTime << ","
            << bloomCreationTime << "," << hashIndexCreationTime << "\n";
    outExp3.close();
}

uint64_t buildHashIndexesAsync(
    const std::map<std::string, std::vector<std::string>>& columnSstFiles) {

    // Diagnostic test: build hash indexes sequentially.
    boost::asio::thread_pool hashIndexPool(1);

    std::vector<std::future<uint64_t>> futures;

    for (const auto& [column, sstFiles] : columnSstFiles) {
        for (const auto& filename : sstFiles) {

            std::promise<uint64_t> promise;
            futures.push_back(promise.get_future());

            std::string indexPath = filename + "." + column + ".hidx";

            boost::asio::post(
                hashIndexPool,
                [filename, indexPath,
                 promise = std::move(promise)]() mutable {
                    try {
                        if (!SSTHashIndex::exists(indexPath)) {
                            SSTHashIndex::build(filename, indexPath, 4096);
                        }

                        uint64_t size =
                            std::filesystem::file_size(indexPath);

                        promise.set_value(size);

                    } catch (...) {
                        promise.set_exception(std::current_exception());
                    }
                });
        }
    }

    // Wait until all tasks have completed.
    hashIndexPool.join();

    uint64_t totalSize = 0;

    for (auto& future : futures) {
        totalSize += future.get();
    }

    return totalSize;
}



std::map<std::string, std::vector<std::string>> scanSstFilesAsync(
    const std::vector<std::string>& columns, DBManager& dbManager,
    const TestParams& params, const bool scanAddedFiles) {
  std::map<std::string, std::vector<std::string>> columnSstFiles;
  std::vector<std::future<std::pair<std::string, std::vector<std::string>>>>
      scanFutures;

  for (const auto& column : columns) {
    std::promise<std::pair<std::string, std::vector<std::string>>> scanPromise;

    scanFutures.push_back(scanPromise.get_future());

    boost::asio::post(globalThreadPool, [column, &dbManager, &params, scanAddedFiles,
                                         promise =
                                             std::move(scanPromise)]() mutable {
      if (scanAddedFiles==true) {
        auto sstFiles = dbManager.getChangedSSTFilesForColumn(params.dbName, column);
        promise.set_value(std::make_pair(column, std::move(sstFiles)));
      }             
      else{
        auto sstFiles = dbManager.scanSSTFilesForColumn(params.dbName, column);
        promise.set_value(std::make_pair(column, std::move(sstFiles)));
      }                            

    });
  }

  // Wait for all scanning to complete
  for (auto& fut : scanFutures) {
    auto [column, sstFiles] = fut.get();
    columnSstFiles[column] = std::move(sstFiles);
  }
  return columnSstFiles;
}

std::map<std::string, BloomTree> buildHierarchies(
    const std::map<std::string, std::vector<std::string>>& columnSstFiles,
    BloomManager& bloomManager, const TestParams& params) {
  std::map<std::string, BloomTree> hierarchies;
  for (const auto& [column, sstFiles] : columnSstFiles) {
    BloomTree hierarchy = bloomManager.createPartitionedHierarchy(
        sstFiles, params.itemsPerPartition, params.bloomSize,
        params.numHashFunctions, params.bloomTreeRatio);
    spdlog::info("Hierarchy built for column: {}", column);
    hierarchies.try_emplace(column, std::move(hierarchy));
  }
  return hierarchies;
}


std::map<std::string, std::vector<Node*>> buildLeaves(
    const std::map<std::string, std::vector<std::string>>& columnSstFiles,
    BloomManager& bloomManager,
    const TestParams& params)
{
    std::map<std::string, std::vector<Node*>> result;

    for (const auto& [column, sstFiles] : columnSstFiles) {
        result[column] = bloomManager.createPartitionedLeaves(
            sstFiles,
            params.itemsPerPartition,
            params.bloomSize,
            params.numHashFunctions
        );
    }

    return result;
}


void appendSstBatch(
    const std::vector<std::string>& columns,
    DBManager& dbManager,
    BloomManager& bloomManager,
    const TestParams& params,
    std::map<std::string, BloomTree>& hierarchies)
{
    spdlog::info("========== APPEND SST BATCH START ==========");

    // 1. Pobierz zmiany wynikające z compaction.
    std::map<std::string, SSTFileChanges> compactionChanges;

    for (const auto& column : columns) {

        auto changes =
            dbManager.getCompactionChangesForColumn(
                params.dbName, column
            );

        if (!changes.removedFiles.empty() ||
            !changes.addedFiles.empty()) {

            spdlog::info(
                "COMPACTION CHANGES: CF={}, removed={}, added={}",
                column,
                changes.removedFiles.size(),
                changes.addedFiles.size()
            );

            for (const auto& file : changes.removedFiles) {
                spdlog::info("  REMOVED: {}", file);
            }

            for (const auto& file : changes.addedFiles) {
                spdlog::info("  ADDED: {}", file);
            }
        }

        compactionChanges.emplace(column, std::move(changes));
    }

    // 2. Wykryj nowe pliki SST.
    auto columnSstFilesInserted =
        scanSstFilesAsync(columns, dbManager, params, true);

    for (const auto& [column, files] : columnSstFilesInserted) {

        spdlog::info(
            "NEW SST FILES: CF={}, count={}",
            column,
            files.size()
        );

        for (const auto& filename : files) {
            spdlog::info("  {}", filename);
        }
    }

    // 3. Zbuduj nowe liście Bloom.
    auto insertedLeaves = buildLeaves(
        columnSstFilesInserted,
        bloomManager,
        params
    );

    // 4. Dodaj liście do istniejących hierarchii.
    for (auto& [column, leaves] : insertedLeaves) {

        auto it = hierarchies.find(column);

        if (it == hierarchies.end()) {
            throw std::runtime_error(
                "Missing existing hierarchy for column: " + column
            );
        }

        it->second.appendLeaves(leaves);

        std::string error;

        if (!it->second.validateTree(error)) {
            throw std::runtime_error(
                "Tree validation failed for CF " +
                column + ": " + error
            );
        }

        if (!it->second.validateBloomFilter(it->second.root, error)) {
            throw std::runtime_error(
                "Bloom validation failed for CF " +
                column + ": " + error
            );
        }

        spdlog::info(
            "BATCH APPENDED: CF={}, leaves={}, root range=[{}, {}]",
            column,
            it->second.leafNodes.size(),
            it->second.root->startKey,
            it->second.root->endKey
        );
    }

    spdlog::info("========== APPEND SST BATCH END ==========");
}


void writeCsvHeader(const std::string& filename,
                    const std::string& headerLine) {
  std::ofstream out(filename, std::ios::app);  // Overwrite mode
  if (!out) {
    spdlog::error(
        "Utils: Nie udało się otworzyć pliku '{}' do zapisu nagłówka!",
        filename);
    exit(1);  // Consistent with how other header functions handle errors
  }
  out << headerLine << "\n";
  out.close();
}

double getProbabilityOfFalsePositive(size_t bloomSize, int numHashFunctions,
                                     size_t itemsPerPartition) {
  if (bloomSize == 0) {
    return 1.0;
  }
  double exponent =
      -static_cast<double>(numHashFunctions) * itemsPerPartition / bloomSize;
  double base = 1.0 - std::exp(exponent);
  return std::pow(base, numHashFunctions);
}

template <typename T>
TimingStatistics calculateNumericStatistics(const std::vector<T>& values) {
  if (values.empty()) {
    spdlog::warn(
        "calculateNumericStatistics called with empty vector. Returning zeroed "
        "statistics.");
    return TimingStatistics{};
  }

  std::vector<T> sorted_values = values;
  std::sort(sorted_values.begin(), sorted_values.end());

  TimingStatistics stats;
  stats.min = static_cast<long long>(sorted_values.front());
  stats.max = static_cast<long long>(sorted_values.back());

  if (sorted_values.size() % 2 == 0) {
    stats.median =
        static_cast<double>(sorted_values[sorted_values.size() / 2 - 1] +
                            sorted_values[sorted_values.size() / 2]) /
        2.0;
  } else {
    stats.median = static_cast<double>(sorted_values[sorted_values.size() / 2]);
  }

  stats.average = static_cast<double>(std::accumulate(
                      sorted_values.begin(), sorted_values.end(), 0LL)) /
                  sorted_values.size();
  return stats;
}

template <typename T>
CountStatistics calculateCountStatistics(const std::vector<T>& values) {
  if (values.empty()) {
    spdlog::warn(
        "calculateCountStatistics called with empty vector. Returning zeroed "
        "statistics.");
    return CountStatistics{};
  }

  std::vector<T> sorted_values = values;
  std::sort(sorted_values.begin(), sorted_values.end());

  CountStatistics stats;
  stats.min = static_cast<size_t>(sorted_values.front());
  stats.max = static_cast<size_t>(sorted_values.back());

  if (sorted_values.size() % 2 == 0) {
    stats.median =
        static_cast<double>(sorted_values[sorted_values.size() / 2 - 1] +
                            sorted_values[sorted_values.size() / 2]) /
        2.0;
  } else {
    stats.median = static_cast<double>(sorted_values[sorted_values.size() / 2]);
  }

  // Use 0ULL for size_t accumulation to avoid overflow issues with large counts
  // if T is smaller than size_t and to ensure the type of the sum is large
  // enough.
  unsigned long long sum = 0;
  for (const T& val : sorted_values) {
    sum += val;
  }
  stats.average = static_cast<double>(sum) / sorted_values.size();
  return stats;
}

AggregatedQueryTimings runStandardQueries(
    DBManager& dbManager, const std::map<std::string, BloomTree>& hierarchies,
    const std::vector<std::string>& columns, size_t dbSize, int numRuns,
    bool skipDbScan, const DistributionConfig& config, const std::map<std::string, std::vector<std::string>>*
    columnSstFiles) {
  AggregatedQueryTimings aggregated_timings;
  if (numRuns <= 0) {
    spdlog::warn(
        "runStandardQueries: numRuns is {} (<=0). Returning empty statistics.",
        numRuns);
    return aggregated_timings;
  }

  std::vector<long long> globalScanTimes, hierarchicalMultiTimes,
      hierarchicalSingleTimes, hashIndexTimeStats; 

  std::vector<size_t> multiCol_bloomChecks_vec;
  std::vector<size_t> multiCol_leafBloomChecks_vec;
  std::vector<size_t> multiCol_sstChecks_vec;
  std::vector<size_t> multiCol_nonLeafBloomChecks_vec;
  std::vector<size_t> singleCol_bloomChecks_vec;
  std::vector<size_t> singleCol_leafBloomChecks_vec;
  std::vector<size_t> singleCol_sstChecks_vec;
  std::vector<size_t> singleCol_nonLeafBloomChecks_vec;

  // Reserve space in vectors
  globalScanTimes.reserve(numRuns);
  hierarchicalMultiTimes.reserve(numRuns);
  hierarchicalSingleTimes.reserve(numRuns);
  hashIndexTimeStats.reserve(numRuns);

  multiCol_bloomChecks_vec.reserve(numRuns);
  multiCol_leafBloomChecks_vec.reserve(numRuns);
  multiCol_sstChecks_vec.reserve(numRuns);
  multiCol_nonLeafBloomChecks_vec.reserve(numRuns);
  singleCol_bloomChecks_vec.reserve(numRuns);
  singleCol_leafBloomChecks_vec.reserve(numRuns);
  singleCol_sstChecks_vec.reserve(numRuns);
  singleCol_nonLeafBloomChecks_vec.reserve(numRuns);

  // --- Setup for generating expected values ---
  // This part is outside the loop as queryTrees are constant for all runs.
  if (hierarchies.empty() || columns.empty()) {
    spdlog::warn(
        "runStandardQueries: Hierarchies map or columns vector is empty, "
        "skipping query execution.");
    return aggregated_timings;
  }

  std::vector<BloomTree> queryTrees;
  queryTrees.reserve(columns.size());
  for (const auto& column : columns) {
    auto it = hierarchies.find(column);
    if (it == hierarchies.end()) {
      spdlog::error(
          "runStandardQueries: Hierarchy for column '{}' not found. Skipping "
          "query execution.",
          column);
      return AggregatedQueryTimings{};
    }
    queryTrees.push_back(it->second);
  }

  if (queryTrees.empty()) {
    spdlog::error(
        "runStandardQueries: No query trees were prepared, possibly due to "
        "missing hierarchies. Skipping query execution.");
    return aggregated_timings;
  }

  std::random_device rd;
  std::mt19937 generator(rd());


 // Prev implementation
  //std::uniform_int_distribution<size_t> distribution(1, dbSize);


  // New implementation: Normal distribution takes from all values in db
  // Ziphian only from the inserted values (values in Ziphian can be repeated.)

  size_t cardinality=0;
  if (config.type == DataDistribution::Normal) {
    cardinality=dbSize;
  }
  else {
    cardinality =
      std::max<size_t>(1, std::min(config.cardinality, dbSize));
  }

  std::uniform_int_distribution<size_t> distribution(1, cardinality);
  auto zipfDist = zipfDistribution(config.exponent, cardinality, dbSize);

  StopWatch stopwatch;
  std::vector<std::string> currentExpectedValues;

  for (int i = 0; i < numRuns; ++i) {
    currentExpectedValues.clear();
    currentExpectedValues.reserve(columns.size());

    size_t existingId;

    if (config.type == DataDistribution::Normal) {
    existingId = distribution(generator);
    } else {
      existingId = zipfDist(generator) + 1;
    }

    //size_t currentId = distribution(generator);
    std::string currentExpectedValueSuffix =
        "_value" + std::to_string(existingId);

    spdlog::info("Run {}: Using expected value suffix: {}", i + 1,
                 currentExpectedValueSuffix);

    for (const auto& column : columns) {
      currentExpectedValues.push_back(column + currentExpectedValueSuffix);
    }

    // --- Global Scan Query ---
    long long globalScanTime = 0;
    if (!skipDbScan && i == 0) {
      stopwatch.start();
      [[maybe_unused]] std::vector<std::string> globalMatches =
          dbManager.scanForRecordsInColumns(columns, currentExpectedValues);
      stopwatch.stop();
      globalScanTime = stopwatch.elapsedMicros();
    } else {
      globalScanTime = 0;
    }
    globalScanTimes.push_back(globalScanTime);

    // --- Hash Index Scan ---
    long long hashIndexTime = 0;
    stopwatch.start();

    auto results = dbManager.findUsingHashIndexes(
    columns,
    currentExpectedValues,
    *columnSstFiles);

    stopwatch.stop();
    hashIndexTime = stopwatch.elapsedMicros();
    hashIndexTimeStats.push_back(hashIndexTime);


    // --- Hierarchical Multi-Column Query ---
    gBloomCheckCount = 0;
    gLeafBloomCheckCount = 0;
    gSSTCheckCount = 0;
    stopwatch.start();
    [[maybe_unused]] std::vector<std::string> hierarchicalMatches =
        multiColumnQueryHierarchical(queryTrees, columns, currentExpectedValues, "", "",
                                     dbManager);
    stopwatch.stop();
    hierarchicalMultiTimes.push_back(stopwatch.elapsedMicros());
    multiCol_bloomChecks_vec.push_back(gBloomCheckCount.load());
    multiCol_leafBloomChecks_vec.push_back(gLeafBloomCheckCount.load());
    multiCol_sstChecks_vec.push_back(gSSTCheckCount.load());
    multiCol_nonLeafBloomChecks_vec.push_back(gBloomCheckCount.load() - gLeafBloomCheckCount.load());

    // --- Hierarchical Single Column Query ---
    // Ensure queryTrees[0] is valid before dereferencing. Already checked by
    // queryTrees.empty()
    gBloomCheckCount = 0;
    gLeafBloomCheckCount = 0;
    gSSTCheckCount = 0;
    stopwatch.start();
    [[maybe_unused]] std::vector<std::string> singlehierarchyMatches =
        dbManager.findUsingSingleHierarchy(queryTrees[0], columns,
                                           currentExpectedValues);
    stopwatch.stop();
    hierarchicalSingleTimes.push_back(stopwatch.elapsedMicros());
    singleCol_bloomChecks_vec.push_back(gBloomCheckCount.load());
    singleCol_leafBloomChecks_vec.push_back(gLeafBloomCheckCount.load());
    singleCol_sstChecks_vec.push_back(gSSTCheckCount.load());
    singleCol_nonLeafBloomChecks_vec.push_back(gBloomCheckCount.load() - gLeafBloomCheckCount.load());

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  // Calculate statistics
  aggregated_timings.globalScanTimeStats =
      calculateNumericStatistics(globalScanTimes);
  aggregated_timings.hierarchicalMultiTimeStats =
      calculateNumericStatistics(hierarchicalMultiTimes);
  aggregated_timings.hierarchicalSingleTimeStats =
      calculateNumericStatistics(hierarchicalSingleTimes);
  aggregated_timings.hashIndexTimeStats =   
      calculateNumericStatistics(hashIndexTimeStats);  

  aggregated_timings.multiCol_bloomChecksStats =
      calculateCountStatistics(multiCol_bloomChecks_vec);
  aggregated_timings.multiCol_leafBloomChecksStats =
      calculateCountStatistics(multiCol_leafBloomChecks_vec);
  aggregated_timings.multiCol_sstChecksStats =
      calculateCountStatistics(multiCol_sstChecks_vec);
  aggregated_timings.multiCol_nonLeafBloomChecksStats =
      calculateCountStatistics(multiCol_nonLeafBloomChecks_vec);

  aggregated_timings.singleCol_bloomChecksStats =
      calculateCountStatistics(singleCol_bloomChecks_vec);
  aggregated_timings.singleCol_leafBloomChecksStats =
      calculateCountStatistics(singleCol_leafBloomChecks_vec);
  aggregated_timings.singleCol_sstChecksStats =
      calculateCountStatistics(singleCol_sstChecks_vec);
  aggregated_timings.singleCol_nonLeafBloomChecksStats =
      calculateCountStatistics(singleCol_nonLeafBloomChecks_vec);

  // Store number of columns for reference
  aggregated_timings.numColumns = columns.size();

  // Calculate per-column statistics (divide by column count)
  double numCols = static_cast<double>(columns.size());
  std::vector<double> multiCol_bloomChecksPerColumn_vec, multiCol_leafBloomChecksPerColumn_vec, 
                      multiCol_sstChecksPerColumn_vec, multiCol_nonLeafBloomChecksPerColumn_vec;
  std::vector<double> singleCol_bloomChecksPerColumn_vec, singleCol_leafBloomChecksPerColumn_vec, 
                      singleCol_sstChecksPerColumn_vec, singleCol_nonLeafBloomChecksPerColumn_vec;

  for (size_t i = 0; i < multiCol_bloomChecks_vec.size(); ++i) {
    multiCol_bloomChecksPerColumn_vec.push_back(static_cast<double>(multiCol_bloomChecks_vec[i]) / numCols);
    multiCol_leafBloomChecksPerColumn_vec.push_back(static_cast<double>(multiCol_leafBloomChecks_vec[i]) / numCols);
    multiCol_sstChecksPerColumn_vec.push_back(static_cast<double>(multiCol_sstChecks_vec[i]) / numCols);
    multiCol_nonLeafBloomChecksPerColumn_vec.push_back(static_cast<double>(multiCol_nonLeafBloomChecks_vec[i]) / numCols);

    singleCol_bloomChecksPerColumn_vec.push_back(static_cast<double>(singleCol_bloomChecks_vec[i]) );
    singleCol_leafBloomChecksPerColumn_vec.push_back(static_cast<double>(singleCol_leafBloomChecks_vec[i]) );
    singleCol_sstChecksPerColumn_vec.push_back(static_cast<double>(singleCol_sstChecks_vec[i]) );
    singleCol_nonLeafBloomChecksPerColumn_vec.push_back(static_cast<double>(singleCol_nonLeafBloomChecks_vec[i]) );
  }

  aggregated_timings.multiCol_bloomChecksPerColumnStats = 
      calculateNumericStatistics(multiCol_bloomChecksPerColumn_vec);
  aggregated_timings.multiCol_leafBloomChecksPerColumnStats = 
      calculateNumericStatistics(multiCol_leafBloomChecksPerColumn_vec);
  aggregated_timings.multiCol_sstChecksPerColumnStats = 
      calculateNumericStatistics(multiCol_sstChecksPerColumn_vec);
  aggregated_timings.multiCol_nonLeafBloomChecksPerColumnStats = 
      calculateNumericStatistics(multiCol_nonLeafBloomChecksPerColumn_vec);

  aggregated_timings.singleCol_bloomChecksPerColumnStats = 
      calculateNumericStatistics(singleCol_bloomChecksPerColumn_vec);
  aggregated_timings.singleCol_leafBloomChecksPerColumnStats = 
      calculateNumericStatistics(singleCol_leafBloomChecksPerColumn_vec);
  aggregated_timings.singleCol_sstChecksPerColumnStats = 
      calculateNumericStatistics(singleCol_sstChecksPerColumn_vec);
  aggregated_timings.singleCol_nonLeafBloomChecksPerColumnStats = 
      calculateNumericStatistics(singleCol_nonLeafBloomChecksPerColumn_vec);

  return aggregated_timings;
}

AggregatedQueryTimings runStandardQueriesWithTarget(
    DBManager& dbManager, const std::map<std::string, BloomTree>& hierarchies,
    const std::vector<std::string>& columns, size_t dbSize, int numRuns,
    bool skipDbScan, std::vector<std::string> currentExpectedValues) {
  AggregatedQueryTimings aggregated_timings;

  std::vector<long long> globalScanTimes, hierarchicalMultiTimes,
      hierarchicalSingleTimes;
  std::vector<size_t> multiCol_bloomChecks_vec;
  std::vector<size_t> multiCol_leafBloomChecks_vec;
  std::vector<size_t> multiCol_sstChecks_vec;
  std::vector<size_t> multiCol_nonLeafBloomChecks_vec;
  std::vector<size_t> singleCol_bloomChecks_vec;
  std::vector<size_t> singleCol_leafBloomChecks_vec;
  std::vector<size_t> singleCol_sstChecks_vec;
  std::vector<size_t> singleCol_nonLeafBloomChecks_vec;

  // Reserve space in vectors
  globalScanTimes.reserve(numRuns);
  hierarchicalMultiTimes.reserve(numRuns);
  hierarchicalSingleTimes.reserve(numRuns);
  multiCol_bloomChecks_vec.reserve(numRuns);
  multiCol_leafBloomChecks_vec.reserve(numRuns);
  multiCol_sstChecks_vec.reserve(numRuns);
  multiCol_nonLeafBloomChecks_vec.reserve(numRuns);
  singleCol_bloomChecks_vec.reserve(numRuns);
  singleCol_leafBloomChecks_vec.reserve(numRuns);
  singleCol_sstChecks_vec.reserve(numRuns);
  singleCol_nonLeafBloomChecks_vec.reserve(numRuns);

  std::vector<BloomTree> queryTrees;
  queryTrees.reserve(columns.size());
  for (const auto& column : columns) {
    auto it = hierarchies.find(column);
    if (it == hierarchies.end()) {
      spdlog::error(
          "runStandardQueries: Hierarchy for column '{}' not found. Skipping "
          "query execution.",
          column);
      return AggregatedQueryTimings{};
    }
    queryTrees.push_back(it->second);
  }

  if (queryTrees.empty()) {
    spdlog::error(
        "runStandardQueries: No query trees were prepared, possibly due to "
        "missing hierarchies. Skipping query execution.");
    return aggregated_timings;
  }

  StopWatch stopwatch;
  long long globalScanTime = 0;
  for (int i = 0; i < numRuns; ++i) {
    // --- Global Scan Query ---
    if (!skipDbScan && i == 0) {
      stopwatch.start();
      [[maybe_unused]] std::vector<std::string> globalMatches =
          dbManager.scanForRecordsInColumns(columns, currentExpectedValues);
      stopwatch.stop();
      globalScanTime = stopwatch.elapsedMicros();
    } else {
      globalScanTime = 0;
    }
    globalScanTimes.push_back(globalScanTime);

    // --- Hierarchical Multi-Column Query ---
    gBloomCheckCount = 0;
    gLeafBloomCheckCount = 0;
    gSSTCheckCount = 0;
    stopwatch.start();
    [[maybe_unused]] std::vector<std::string> hierarchicalMatches =
        multiColumnQueryHierarchical(queryTrees,columns,  currentExpectedValues, "", "",
                                     dbManager);
    stopwatch.stop();
    hierarchicalMultiTimes.push_back(stopwatch.elapsedMicros());
    multiCol_bloomChecks_vec.push_back(gBloomCheckCount.load());
    multiCol_leafBloomChecks_vec.push_back(gLeafBloomCheckCount.load());
    multiCol_sstChecks_vec.push_back(gSSTCheckCount.load());
    multiCol_nonLeafBloomChecks_vec.push_back(gBloomCheckCount.load() - gLeafBloomCheckCount.load());

    // --- Hierarchical Single Column Query ---
    // Ensure queryTrees[0] is valid before dereferencing. Already checked by
    // queryTrees.empty()
    gBloomCheckCount = 0;
    gLeafBloomCheckCount = 0;
    gSSTCheckCount = 0;
    stopwatch.start();
    [[maybe_unused]] std::vector<std::string> singlehierarchyMatches =
        dbManager.findUsingSingleHierarchy(queryTrees[0], columns,
                                           currentExpectedValues);
    stopwatch.stop();
    hierarchicalSingleTimes.push_back(stopwatch.elapsedMicros());
    singleCol_bloomChecks_vec.push_back(gBloomCheckCount.load());
    singleCol_leafBloomChecks_vec.push_back(gLeafBloomCheckCount.load());
    singleCol_sstChecks_vec.push_back(gSSTCheckCount.load());
    singleCol_nonLeafBloomChecks_vec.push_back(gBloomCheckCount.load() - gLeafBloomCheckCount.load());

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  // Calculate statistics
  aggregated_timings.globalScanTimeStats =
      calculateNumericStatistics(globalScanTimes);
  aggregated_timings.hierarchicalMultiTimeStats =
      calculateNumericStatistics(hierarchicalMultiTimes);
  aggregated_timings.hierarchicalSingleTimeStats =
      calculateNumericStatistics(hierarchicalSingleTimes);

  aggregated_timings.multiCol_bloomChecksStats =
      calculateCountStatistics(multiCol_bloomChecks_vec);
  aggregated_timings.multiCol_leafBloomChecksStats =
      calculateCountStatistics(multiCol_leafBloomChecks_vec);
  aggregated_timings.multiCol_sstChecksStats =
      calculateCountStatistics(multiCol_sstChecks_vec);
  aggregated_timings.multiCol_nonLeafBloomChecksStats =
      calculateCountStatistics(multiCol_nonLeafBloomChecks_vec);

  aggregated_timings.singleCol_bloomChecksStats =
      calculateCountStatistics(singleCol_bloomChecks_vec);
  aggregated_timings.singleCol_leafBloomChecksStats =
      calculateCountStatistics(singleCol_leafBloomChecks_vec);
  aggregated_timings.singleCol_sstChecksStats =
      calculateCountStatistics(singleCol_sstChecks_vec);
  aggregated_timings.singleCol_nonLeafBloomChecksStats =
      calculateCountStatistics(singleCol_nonLeafBloomChecks_vec);

  // Store number of columns for reference
  aggregated_timings.numColumns = columns.size();

  // Calculate per-column statistics (divide by column count)
  double numCols = static_cast<double>(columns.size());
  std::vector<double> multiCol_bloomChecksPerColumn_vec, multiCol_leafBloomChecksPerColumn_vec, 
                      multiCol_sstChecksPerColumn_vec, multiCol_nonLeafBloomChecksPerColumn_vec;
  std::vector<double> singleCol_bloomChecksPerColumn_vec, singleCol_leafBloomChecksPerColumn_vec, 
                      singleCol_sstChecksPerColumn_vec, singleCol_nonLeafBloomChecksPerColumn_vec;

  for (size_t i = 0; i < multiCol_bloomChecks_vec.size(); ++i) {
    multiCol_bloomChecksPerColumn_vec.push_back(static_cast<double>(multiCol_bloomChecks_vec[i]) / numCols);
    multiCol_leafBloomChecksPerColumn_vec.push_back(static_cast<double>(multiCol_leafBloomChecks_vec[i]) / numCols);
    multiCol_sstChecksPerColumn_vec.push_back(static_cast<double>(multiCol_sstChecks_vec[i]) / numCols);
    multiCol_nonLeafBloomChecksPerColumn_vec.push_back(static_cast<double>(multiCol_nonLeafBloomChecks_vec[i]) / numCols);

    singleCol_bloomChecksPerColumn_vec.push_back(static_cast<double>(singleCol_bloomChecks_vec[i]) );
    singleCol_leafBloomChecksPerColumn_vec.push_back(static_cast<double>(singleCol_leafBloomChecks_vec[i]) );
    singleCol_sstChecksPerColumn_vec.push_back(static_cast<double>(singleCol_sstChecks_vec[i]) );
    singleCol_nonLeafBloomChecksPerColumn_vec.push_back(static_cast<double>(singleCol_nonLeafBloomChecks_vec[i]) );
  }

  aggregated_timings.multiCol_bloomChecksPerColumnStats = 
      calculateNumericStatistics(multiCol_bloomChecksPerColumn_vec);
  aggregated_timings.multiCol_leafBloomChecksPerColumnStats = 
      calculateNumericStatistics(multiCol_leafBloomChecksPerColumn_vec);
  aggregated_timings.multiCol_sstChecksPerColumnStats = 
      calculateNumericStatistics(multiCol_sstChecksPerColumn_vec);
  aggregated_timings.multiCol_nonLeafBloomChecksPerColumnStats = 
      calculateNumericStatistics(multiCol_nonLeafBloomChecksPerColumn_vec);

  aggregated_timings.singleCol_bloomChecksPerColumnStats = 
      calculateNumericStatistics(singleCol_bloomChecksPerColumn_vec);
  aggregated_timings.singleCol_leafBloomChecksPerColumnStats = 
      calculateNumericStatistics(singleCol_leafBloomChecksPerColumn_vec);
  aggregated_timings.singleCol_sstChecksPerColumnStats = 
      calculateNumericStatistics(singleCol_sstChecksPerColumn_vec);
  aggregated_timings.singleCol_nonLeafBloomChecksPerColumnStats = 
      calculateNumericStatistics(singleCol_nonLeafBloomChecksPerColumn_vec);

  return aggregated_timings;
}

// Helper function to generate dynamic patterns based on column count
// Generates patterns where we gradually increase the number of existing values
// (t) from left to right: For 4 columns: [n,n,n,n], [t,n,n,n], [t,t,n,n],
// [t,t,t,n], [t,t,t,t]
std::vector<std::vector<bool>> generateDynamicPatterns(size_t numColumns) {
  std::vector<std::vector<bool>> patterns;

  for (size_t numExisting = 0; numExisting <= numColumns; ++numExisting) {
    std::vector<bool> pattern(numColumns, false);  // Start with all n (false)

    // Set first numExisting columns to true (existing values)
    for (size_t i = 0; i < numExisting; ++i) {
      pattern[i] = true;
    }

    patterns.push_back(pattern);
  }

  return patterns;
}

void testPatternGeneration() {
  spdlog::info("Testing pattern generation:");

  for (size_t numCols = 2; numCols <= 5; ++numCols) {
    spdlog::info("For {} columns:", numCols);
    auto patterns = generateDynamicPatterns(numCols);

    for (size_t i = 0; i < patterns.size(); ++i) {
      std::string patternStr = "[";
      for (size_t j = 0; j < patterns[i].size(); ++j) {
        patternStr += (patterns[i][j] ? "t" : "n");
        if (j < patterns[i].size() - 1) patternStr += ",";
      }
      patternStr += "]";

      size_t existingCount = 0;
      for (bool val : patterns[i]) {
        if (val) existingCount++;
      }
      double percentage = static_cast<double>(existingCount) / numCols * 100.0;

      spdlog::info("  Pattern {}: {} ({}% existing)", i, patternStr,
                   percentage);
    }
  }
}

std::vector<PatternQueryResult> runPatternQueriesWithCsvData(
    DBManager& dbManager, const std::map<std::string, BloomTree>& hierarchies,
    const std::vector<std::string>& columns, size_t dbSize, const DistributionConfig& config) {
  std::vector<PatternQueryResult> results;

  // Setup query trees
  if (hierarchies.empty() || columns.empty()) {
    spdlog::warn(
        "runPatternQueriesWithCsvData: Hierarchies map or "
        "columns vector is empty, skipping query execution.");
    return results;
  }

  std::vector<BloomTree> queryTrees;
  queryTrees.reserve(columns.size());
  for (const auto& column : columns) {
    auto it = hierarchies.find(column);
    if (it == hierarchies.end()) {
      spdlog::error(
          "runPatternQueriesWithCsvData: Hierarchy for column "
          "'{}' not found. Skipping query execution.",
          column);
      return results;
    }
    queryTrees.push_back(it->second);
  }

  if (queryTrees.empty()) {
    spdlog::error(
        "runPatternQueriesWithCsvData: No query trees were "
        "prepared, possibly due to missing hierarchies. Skipping query "
        "execution.");
    return results;
  }

  // Define non-existing record patterns
  // t = existing value, n = non-existing value
  std::vector<std::vector<bool>> patterns =
      generateDynamicPatterns(columns.size());

  // Log the generated patterns for debugging
  spdlog::info("Generated {} patterns for {} columns:", patterns.size(),
               columns.size());
  for (size_t i = 0; i < patterns.size(); ++i) {
    std::string patternStr = "[";
    for (size_t j = 0; j < patterns[i].size(); ++j) {
      patternStr += (patterns[i][j] ? "t" : "n");
      if (j < patterns[i].size() - 1) patternStr += ",";
    }
    patternStr += "]";
    spdlog::info("Pattern {}: {}", i, patternStr);
  }

  std::random_device rd;
  std::mt19937 generator(rd());
  
  // Previous implementation
  // std::uniform_int_distribution<size_t> distribution(1, dbSize);

  // New implementation: Normal distribution takes from all values in db
  // Ziphian only from the inserted values (values in Ziphian can be repeated.)

  size_t cardinality=0;
  if (config.type == DataDistribution::Normal) {
    cardinality=dbSize;
  }
  else {
    cardinality =
      std::max<size_t>(1, std::min(config.cardinality, dbSize));
  }

  std::uniform_int_distribution<size_t> distribution(1, cardinality);
 
  
  StopWatch stopwatch;
  std::vector<std::string> currentExpectedValues;
  results.reserve(patterns.size());

  for (int i = 0; i < patterns.size(); ++i) {
    currentExpectedValues.clear();
    currentExpectedValues.reserve(columns.size());

    const auto& pattern = patterns[i];

    size_t existingId = distribution(generator);

    std::string existingValueSuffix = "_value" + std::to_string(existingId);
    std::string nonExistingValueSuffix = "_wrong" + std::to_string(existingId);

    size_t existingCount = 0;
    for (size_t colIdx = 0; colIdx < columns.size() && colIdx < pattern.size();
         ++colIdx) {
      if (pattern[colIdx]) {
        existingCount++;
      }
    }

    double percentageExisting =
        static_cast<double>(existingCount) / columns.size() * 100.0;

    spdlog::info("Run: Using pattern index {} with {}% existing columns", i,
                 percentageExisting);

    // Generate values based on pattern
    for (size_t colIdx = 0; colIdx < columns.size() && colIdx < pattern.size();
         ++colIdx) {
      if (pattern[colIdx]) {
        // existing value
        currentExpectedValues.push_back(columns[colIdx] + existingValueSuffix);
      } else {
        // non-existing value
        currentExpectedValues.push_back(columns[colIdx] +
                                        nonExistingValueSuffix);
      }
    }

    PatternQueryResult result;
    result.percent = percentageExisting;

    // --- Hierarchical Multi-Column Query ---
    gBloomCheckCount = 0;
    gLeafBloomCheckCount = 0;
    gSSTCheckCount = 0;
    stopwatch.start();
    [[maybe_unused]] std::vector<std::string> hierarchicalMatches =
        multiColumnQueryHierarchical(queryTrees, columns, currentExpectedValues, "", "",
                                     dbManager);
    stopwatch.stop();
    result.hierarchicalMultiTime = stopwatch.elapsedMicros();
    result.multiCol_bloomChecks = gBloomCheckCount.load();
    result.multiCol_leafBloomChecks = gLeafBloomCheckCount.load();
    result.multiCol_sstChecks = gSSTCheckCount.load();

    // Calculate derived metrics
    result.multiCol_nonLeafBloomChecks = result.multiCol_bloomChecks - result.multiCol_leafBloomChecks;
    
    // Calculate per-column averages
    double numCols = static_cast<double>(columns.size());
    result.multiCol_bloomChecksPerColumn = static_cast<double>(result.multiCol_bloomChecks) / numCols;
    result.multiCol_leafBloomChecksPerColumn = static_cast<double>(result.multiCol_leafBloomChecks) / numCols;
    result.multiCol_sstChecksPerColumn = static_cast<double>(result.multiCol_sstChecks) / numCols;
    result.multiCol_nonLeafBloomChecksPerColumn = static_cast<double>(result.multiCol_nonLeafBloomChecks) / numCols;

    // --- Hierarchical Single Column Query ---
    gBloomCheckCount = 0;
    gLeafBloomCheckCount = 0;
    gSSTCheckCount = 0;
    stopwatch.start();
    [[maybe_unused]] std::vector<std::string> singlehierarchyMatches =
        dbManager.findUsingSingleHierarchy(queryTrees[0], columns,
                                           currentExpectedValues);
    stopwatch.stop();
    result.hierarchicalSingleTime = stopwatch.elapsedMicros();
    result.singleCol_bloomChecks = gBloomCheckCount.load();
    result.singleCol_leafBloomChecks = gLeafBloomCheckCount.load();
    result.singleCol_sstChecks = gSSTCheckCount.load();

    // Calculate derived metrics
    result.singleCol_nonLeafBloomChecks = result.singleCol_bloomChecks - result.singleCol_leafBloomChecks;
    
    // Calculate per-column averages
    result.singleCol_bloomChecksPerColumn = static_cast<double>(result.singleCol_bloomChecks) ;
    result.singleCol_leafBloomChecksPerColumn = static_cast<double>(result.singleCol_leafBloomChecks) ;
    result.singleCol_sstChecksPerColumn = static_cast<double>(result.singleCol_sstChecks) ;
    result.singleCol_nonLeafBloomChecksPerColumn = static_cast<double>(result.singleCol_nonLeafBloomChecks) ;

    results.push_back(result);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  return results;
}





std::vector<BloomTree> prepareQueryTrees(
    const std::map<std::string, BloomTree>& hierarchies,
    const std::vector<std::string>& columns)
{
    std::vector<BloomTree> queryTrees;

    if (hierarchies.empty() || columns.empty()) {
        spdlog::warn(
            "prepareQueryTrees: Hierarchies or columns are empty.");
        return queryTrees;
    }

    queryTrees.reserve(columns.size());

    for (const auto& column : columns) {
        auto it = hierarchies.find(column);

        if (it == hierarchies.end()) {
            spdlog::error(
                "prepareQueryTrees: Hierarchy for column '{}' not found.",
                column);
            return {};
        }

        queryTrees.push_back(it->second);
    }

    return queryTrees;
}



QueryGenerationContext createQueryGenerationContext(
    size_t numColumns,
    size_t dbSize,
    int numQueries,
    double realDataPercentage,
    const DistributionConfig& config)
{
    QueryGenerationContext context;

    const int numRealQueries = static_cast<int>(
        std::round(numQueries * realDataPercentage / 100.0));

    const int numFalseQueries = numQueries - numRealQueries;

    spdlog::info(
        "Running {} queries: {} real ({}%), {} false ({}%).",
        numQueries,
        numRealQueries,
        realDataPercentage,
        numFalseQueries,
        100.0 - realDataPercentage);

    context.patterns = generateDynamicPatterns(numColumns);

    spdlog::info(
        "Generated {} permutation patterns for {} columns.",
        context.patterns.size(),
        numColumns);

    context.isRealDataQuery.assign(numQueries, false);

    for (int i = 0; i < numRealQueries; ++i) {
        context.isRealDataQuery[i] = true;
    }

    std::mt19937_64 shuffleRng(config.seed);

    std::shuffle(
        context.isRealDataQuery.begin(),
        context.isRealDataQuery.end(),
        shuffleRng);

    const size_t cardinality =
        config.type == DataDistribution::Normal
            ? dbSize
            : std::max<size_t>(
                  1,
                  std::min(config.cardinality, dbSize));

    context.queryRng.seed(config.seed + 1);

    context.uniformDist =
        std::uniform_int_distribution<size_t>(1, cardinality);

    if (config.type == DataDistribution::Zipfian ||
        config.type == DataDistribution::Mixed) {

        std::vector<double> weights;
        weights.reserve(cardinality);

        double zipfWeightSum = 0.0;

        for (size_t rank = 1; rank <= cardinality; ++rank) {
            const double weight =
                1.0 / std::pow(
                    static_cast<double>(rank),
                    config.exponent);

            weights.push_back(weight);
            zipfWeightSum += weight;
        }

        if (config.type == DataDistribution::Mixed) {
            const double zipfRatio =
                std::clamp(config.zipfRatio, 0.0, 1.0);

            for (size_t i = 0; i < cardinality; ++i) {
                const double uniformProbability =
                    1.0 / static_cast<double>(cardinality);

                const double zipfProbability =
                    weights[i] / zipfWeightSum;

                weights[i] =
                    (1.0 - zipfRatio) * uniformProbability +
                    zipfRatio * zipfProbability;
            }
        }

        context.zipfDist =
            std::discrete_distribution<size_t>(
                weights.begin(),
                weights.end());
    }

    return context;
}


GeneratedQuery generateMixedQuery(
    int queryIdx,
    const std::vector<std::string>& columns,
    QueryGenerationContext& context,
    const DistributionConfig& config)
{
    GeneratedQuery query;

    query.isRealData = context.isRealDataQuery[queryIdx];

    size_t patternIndex = 0;

    if (query.isRealData) {
        patternIndex = context.patterns.size() - 1;
    } else {
        patternIndex =
            queryIdx % (context.patterns.size() - 1);
    }

    const auto& pattern = context.patterns[patternIndex];

    const size_t randomId =
        config.type == DataDistribution::Normal
            ? context.uniformDist(context.queryRng)
            : context.zipfDist(context.queryRng) + 1;

    const std::string existingSuffix =
        "_value" + std::to_string(randomId);

    const std::string nonExistingSuffix =
        "_wrong" + std::to_string(randomId);

    const size_t existingCount =
        std::count(pattern.begin(), pattern.end(), true);

    const double percentageExisting =
        static_cast<double>(existingCount) /
        static_cast<double>(columns.size()) * 100.0;

    if (query.isRealData) {
        spdlog::info(
            "Query {}: REAL data with ID {}.",
            queryIdx + 1,
            randomId);
    } else {
        spdlog::info(
            "Query {}: FALSE data (pattern {} - {}% existing) with ID {}.",
            queryIdx + 1,
            patternIndex,
            percentageExisting,
            randomId);
    }

    query.values.reserve(columns.size());

    for (size_t i = 0; i < columns.size() && i < pattern.size(); ++i) {
        const std::string& suffix =
            pattern[i] ? existingSuffix : nonExistingSuffix;

        query.values.push_back(columns[i] + suffix);
    }

    return query;
}



void runHashIndexQueries(
    DBManager& dbManager,
    const std::vector<std::string>& columns,
    const std::vector<std::string>& values,
    const std::map<std::string, std::vector<std::string>>* columnSstFiles,
    MixedQueryResult& result)
{
    if (columnSstFiles == nullptr) {
        spdlog::warn(
            "Hash Index queries skipped: columnSstFiles is null.");
        return;
    }

    StopWatch stopwatch;

    // Hash Index Multi-Column
    gHashIndexOpenCount = 0;
    gHashLookupCount = 0;
    gHashCandidateCount = 0;
    gHashVerificationCount = 0;

    stopwatch.start();

    [[maybe_unused]] auto multiResults =
        dbManager.findUsingHashIndexes(
            columns,
            values,
            *columnSstFiles);

    stopwatch.stop();

    result.hashIndexTime = stopwatch.elapsedMicros();
    result.hashIndexOpenCount = gHashIndexOpenCount.load();
    result.hashLookupCount = gHashLookupCount.load();
    result.hashCandidateCount = gHashCandidateCount.load();
    result.hashVerificationCount = gHashVerificationCount.load();

    // Hash Index Single-Column
    gHashIndexOpenCount = 0;
    gHashLookupCount = 0;
    gHashCandidateCount = 0;
    gHashVerificationCount = 0;

    stopwatch.start();

    [[maybe_unused]] auto singleResults =
        dbManager.findUsingHashIndexesSingleColumn(
            columns,
            values,
            *columnSstFiles);

    stopwatch.stop();

    result.hashSingleIndexTime = stopwatch.elapsedMicros();
    result.hashSingleIndexOpenCount = gHashIndexOpenCount.load();
    result.hashSingleLookupCount = gHashLookupCount.load();
    result.hashSingleCandidateCount = gHashCandidateCount.load();
    result.hashSingleVerificationCount =
        gHashVerificationCount.load();
}


  void runHashSingleHierarchyQuery(
    DBManager& dbManager,
    BloomTree& queryTree,
    const std::vector<std::string>& columns,
    const std::vector<std::string>& values,
    MixedQueryResult& result)
{
    StopWatch stopwatch;
    HashHierarchyTimings timings;

    stopwatch.start();

    [[maybe_unused]] auto matches =
        dbManager.findUsingSingleHierarchyAndHashIndexes(
            queryTree,
            columns,
            values,
            &timings);

    stopwatch.stop();

    result.hashSingleHierarchyIndexTime =
        stopwatch.elapsedMicros();

    spdlog::info(
        "HashSingleHierarchy: Bloom={} us, Open={} us, "
        "Lookup={} us, RangeFilter={} us, SortUnique={} us, "
        "RocksDBVerify={} us, Total={} us",
        timings.bloomTime,
        timings.openTime,
        timings.lookupTime,
        timings.rangeFilterTime,
        timings.sortUniqueTime,
        timings.rocksDBVerifyTime,
        timings.totalTime);
}

void runHierarchicalQueries(
    DBManager& dbManager,
    std::vector<BloomTree>& queryTrees,
    const std::vector<std::string>& columns,
    const std::vector<std::string>& values,
    MixedQueryResult& result)
{
    StopWatch stopwatch;

    // Hierarchical Multi-Column Query
    gBloomCheckCount = 0;
    gLeafBloomCheckCount = 0;
    gSSTCheckCount = 0;

    stopwatch.start();

    [[maybe_unused]] auto multiMatches =
        multiColumnQueryHierarchical(
            queryTrees,
            columns,
            values,
            "",
            "",
            dbManager);

    stopwatch.stop();

    result.hierarchicalMultiTime = stopwatch.elapsedMicros();

    result.multiCol_bloomChecks = gBloomCheckCount.load();
    result.multiCol_leafBloomChecks = gLeafBloomCheckCount.load();
    result.multiCol_sstChecks = gSSTCheckCount.load();

    result.multiCol_nonLeafBloomChecks =
        result.multiCol_bloomChecks -
        result.multiCol_leafBloomChecks;

    const double numCols = static_cast<double>(columns.size());

    result.multiCol_bloomChecksPerColumn =
        result.multiCol_bloomChecks / numCols;

    result.multiCol_leafBloomChecksPerColumn =
        result.multiCol_leafBloomChecks / numCols;

    result.multiCol_sstChecksPerColumn =
        result.multiCol_sstChecks / numCols;

    result.multiCol_nonLeafBloomChecksPerColumn =
        result.multiCol_nonLeafBloomChecks / numCols;

    // Hierarchical Single-Column Query
    gBloomCheckCount = 0;
    gLeafBloomCheckCount = 0;
    gSSTCheckCount = 0;

    stopwatch.start();

    [[maybe_unused]] auto singleMatches =
        dbManager.findUsingSingleHierarchy(
            queryTrees[0],
            columns,
            values);

    stopwatch.stop();

    result.hierarchicalSingleTime = stopwatch.elapsedMicros();

    result.singleCol_bloomChecks = gBloomCheckCount.load();
    result.singleCol_leafBloomChecks = gLeafBloomCheckCount.load();
    result.singleCol_sstChecks = gSSTCheckCount.load();

    result.singleCol_nonLeafBloomChecks =
        result.singleCol_bloomChecks -
        result.singleCol_leafBloomChecks;

    result.singleCol_bloomChecksPerColumn =
        static_cast<double>(result.singleCol_bloomChecks);

    result.singleCol_leafBloomChecksPerColumn =
        static_cast<double>(result.singleCol_leafBloomChecks);

    result.singleCol_sstChecksPerColumn =
        static_cast<double>(result.singleCol_sstChecks);

    result.singleCol_nonLeafBloomChecksPerColumn =
        static_cast<double>(result.singleCol_nonLeafBloomChecks);
}



std::vector<MixedQueryResult> runMixedQueriesWithCsvData(
    DBManager& dbManager,
    const std::map<std::string, BloomTree>& hierarchies,
    const std::vector<std::string>& columns,
    size_t dbSize,
    int numQueries,
    double realDataPercentage,
    const DistributionConfig& config,
    const std::map<std::string, std::vector<std::string>>* columnSstFiles)
{
    std::vector<MixedQueryResult> results;

    if (numQueries <= 0 || columns.empty() || dbSize == 0) {
        spdlog::warn(
            "runMixedQueriesWithCsvData: Invalid experiment parameters.");
        return results;
    }

    // Prepare query trees.
    auto queryTrees = prepareQueryTrees(hierarchies, columns);

    if (queryTrees.empty()) {
        return results;
    }

    // Initialize query generation.
    auto generationContext = createQueryGenerationContext(
        columns.size(),
        dbSize,
        numQueries,
        realDataPercentage,
        config);

    if (generationContext.patterns.size() < 2) {
        spdlog::error(
            "runMixedQueriesWithCsvData: At least two query patterns are required.");
        return results;
    }

    results.reserve(numQueries);

    // Execute queries.
    for (int queryIdx = 0; queryIdx < numQueries; ++queryIdx) {

        // Generate query.
        GeneratedQuery query = generateMixedQuery(
            queryIdx,
            columns,
            generationContext,
            config);

        MixedQueryResult result;
        result.queryIndex = queryIdx;
        result.isRealData = query.isRealData;

        // Execute search strategies.
        runHashIndexQueries(
            dbManager,
            columns,
            query.values,
            columnSstFiles,
            result);

        runHashSingleHierarchyQuery(
            dbManager,
            queryTrees[0],
            columns,
            query.values,
            result);

        runHierarchicalQueries(
            dbManager,
            queryTrees,
            columns,
            query.values,
            result);

        results.push_back(std::move(result));

        std::this_thread::sleep_for(
            std::chrono::milliseconds(10));
    }

    return results;
}


/*
std::vector<MixedQueryResult> runMixedQueriesWithCsvData(
    DBManager& dbManager, const std::map<std::string, BloomTree>& hierarchies,
    const std::vector<std::string>& columns, size_t dbSize, int numQueries, 
    double realDataPercentage, const DistributionConfig& config,
  const std::map<std::string, std::vector<std::string>>*
    columnSstFiles) {
  std::vector<MixedQueryResult> results;

  // Setup query trees
  if (hierarchies.empty() || columns.empty()) {
    spdlog::warn(
        "runMixedQueriesWithCsvData: Hierarchies map or "
        "columns vector is empty, skipping query execution.");
    return results;
  }

  std::vector<BloomTree> queryTrees;
  queryTrees.reserve(columns.size());
  for (const auto& column : columns) {
    auto it = hierarchies.find(column);
    if (it == hierarchies.end()) {
      spdlog::error(
          "runMixedQueriesWithCsvData: Hierarchy for column "
          "'{}' not found. Skipping query execution.",
          column);
      return results;
    }
    queryTrees.push_back(it->second);
  }

  if (queryTrees.empty()) {
    spdlog::error(
        "runMixedQueriesWithCsvData: No query trees were "
        "prepared, possibly due to missing hierarchies. Skipping query "
        "execution.");
    return results;
  }

  // Calculate how many queries should have real data
  int numRealQueries = static_cast<int>(std::round(numQueries * realDataPercentage / 100.0));
  int numFalseQueries = numQueries - numRealQueries;

  spdlog::info("runMixedQueriesWithCsvData: Running {} total queries: {} with real data ({}%), {} with false data ({}%)",
               numQueries, numRealQueries, realDataPercentage, numFalseQueries, 100.0 - realDataPercentage);

  // Generate permutation patterns for columns (focusing on cases where first column is true)
  std::vector<std::vector<bool>> patterns = generateDynamicPatterns(columns.size());
  
  spdlog::info("runMixedQueriesWithCsvData: Generated {} permutation patterns for {} columns", 
               patterns.size(), columns.size());

  std::vector<bool> isRealDataQuery(numQueries, false);
  for (int i = 0; i < numRealQueries; ++i) {
    isRealDataQuery[i] = true;
  }


  // Shuffle real/false query pattern deterministically.
std::mt19937_64 shuffleRng(config.seed);
std::shuffle(
    isRealDataQuery.begin(),
    isRealDataQuery.end(),
    shuffleRng);

// Determine cardinality.
size_t cardinality = 0;

if (config.type == DataDistribution::Normal) {
    cardinality = dbSize;
} else {
    cardinality = std::max<size_t>(
        1, std::min(config.cardinality, dbSize));
}

// Initialize query generators.
std::mt19937_64 queryRng(config.seed + 1);

std::discrete_distribution<size_t> zipfDist;
std::uniform_int_distribution<size_t> uniformDist(
    1, cardinality);

if (config.type == DataDistribution::Zipfian ||
    config.type == DataDistribution::Mixed) {

    std::vector<double> weights;
    weights.reserve(cardinality);

    double zipfWeightSum = 0.0;

    for (size_t rank = 1; rank <= cardinality; ++rank) {
        const double weight =
            1.0 / std::pow(
                static_cast<double>(rank),
                config.exponent);

        weights.push_back(weight);
        zipfWeightSum += weight;
    }

    if (config.type == DataDistribution::Mixed) {
        const double zipfRatio =
            std::clamp(config.zipfRatio, 0.0, 1.0);

        for (size_t i = 0; i < cardinality; ++i) {
            const double uniformProbability =
                1.0 / static_cast<double>(cardinality);

            const double zipfProbability =
                weights[i] / zipfWeightSum;

            weights[i] =
                (1.0 - zipfRatio) * uniformProbability +
                zipfRatio * zipfProbability;
        }
    }

    zipfDist = std::discrete_distribution<size_t>(
        weights.begin(), weights.end());
}


  StopWatch stopwatch;
  std::vector<std::string> currentExpectedValues;
  results.reserve(numQueries);

  for (int queryIdx = 0; queryIdx < numQueries; ++queryIdx) {
    currentExpectedValues.clear();
    currentExpectedValues.reserve(columns.size());

    bool useRealData = isRealDataQuery[queryIdx];
    std::vector<bool> pattern;
    
    if (useRealData) {
      // Use the last pattern (all true) for real data
      pattern = patterns.back();
    } else {
      // Use one of the permutation patterns (except the last one) for false data
      size_t patternIndex = queryIdx % (patterns.size() - 1);
      pattern = patterns[patternIndex];
    }
    

    size_t randomId;

    if (config.type == DataDistribution::Normal) {
        randomId = uniformDist(queryRng);
    } else {
        randomId = zipfDist(queryRng) + 1;
    }

    std::string existingValueSuffix = "_value" + std::to_string(randomId);
    std::string nonExistingValueSuffix = "_wrong" + std::to_string(randomId);

    // Count existing columns for logging
    size_t existingCount = 0;
    for (bool val : pattern) {
      if (val) existingCount++;
    }
    double percentageExisting = static_cast<double>(existingCount) / columns.size() * 100.0;

    if (useRealData) {
      spdlog::info("Query {}: Using REAL data (all true pattern) with ID {}", 
                   queryIdx + 1, randomId);
    } else {
      size_t patternIndex = queryIdx % (patterns.size() - 1);
      spdlog::info("Query {}: Using FALSE data (pattern {} - {}% existing) with ID {}", 
                   queryIdx + 1, patternIndex, percentageExisting, randomId);
    }

    // Generate values based on pattern
    for (size_t colIdx = 0; colIdx < columns.size() && colIdx < pattern.size(); ++colIdx) {
      if (pattern[colIdx]) {
        // existing value
        currentExpectedValues.push_back(columns[colIdx] + existingValueSuffix);
      } else {
        // non-existing value
        currentExpectedValues.push_back(columns[colIdx] + nonExistingValueSuffix);
      }
    }

    MixedQueryResult result;
    result.queryIndex = queryIdx;
    result.isRealData = useRealData;

    // --- Hash Index Scan (Multi-Column) ---

  gHashIndexOpenCount = 0;
  gHashLookupCount = 0;
  gHashCandidateCount = 0;
  gHashVerificationCount = 0;

  stopwatch.start();

  [[maybe_unused]] auto resultsData =
      dbManager.findUsingHashIndexes(
          columns,
          currentExpectedValues,
          *columnSstFiles);

  stopwatch.stop();

result.hashIndexTime = stopwatch.elapsedMicros();

result.hashIndexOpenCount = gHashIndexOpenCount.load();
result.hashLookupCount = gHashLookupCount.load();
result.hashCandidateCount = gHashCandidateCount.load();
result.hashVerificationCount = gHashVerificationCount.load();


// --- Hash Single Index Scan ---

gHashIndexOpenCount = 0;
gHashLookupCount = 0;
gHashCandidateCount = 0;
gHashVerificationCount = 0;

stopwatch.start();

[[maybe_unused]] auto resultsDataSingleHash =
    dbManager.findUsingHashIndexesSingleColumn(
        columns,
        currentExpectedValues,
        *columnSstFiles);

stopwatch.stop();

result.hashSingleIndexTime = stopwatch.elapsedMicros();

result.hashSingleIndexOpenCount = gHashIndexOpenCount.load();
result.hashSingleLookupCount = gHashLookupCount.load();
result.hashSingleCandidateCount = gHashCandidateCount.load();
result.hashSingleVerificationCount = gHashVerificationCount.load();


        // --- Hash Single Hierarchy Index Scan ---
  
    stopwatch.start();

    HashHierarchyTimings timings;

  auto resultsDataSingleHashHierarchy =
    dbManager.findUsingSingleHierarchyAndHashIndexes(queryTrees[0], columns, currentExpectedValues, &timings);
    stopwatch.stop();
    result.hashSingleHierarchyIndexTime = stopwatch.elapsedMicros();
  
  spdlog::info(
    "HashSingleHierarchy: Bloom={} us, Open={} us, "
    "Lookup={} us, RangeFilter={} us, SortUnique={} us, "
    "RocksDBVerify={} us, Total={} us",
    timings.bloomTime,
    timings.openTime,
    timings.lookupTime,
    timings.rangeFilterTime,
    timings.sortUniqueTime,
    timings.rocksDBVerifyTime,
    timings.totalTime);


    // --- Hierarchical Multi-Column Query ---
    gBloomCheckCount = 0;
    gLeafBloomCheckCount = 0;
    gSSTCheckCount = 0;
    stopwatch.start();
    [[maybe_unused]] std::vector<std::string> hierarchicalMatches =
        multiColumnQueryHierarchical(queryTrees, columns, currentExpectedValues, "", "",
                                     dbManager);
    stopwatch.stop();
    result.hierarchicalMultiTime = stopwatch.elapsedMicros();
    result.multiCol_bloomChecks = gBloomCheckCount.load();
    result.multiCol_leafBloomChecks = gLeafBloomCheckCount.load();
    result.multiCol_sstChecks = gSSTCheckCount.load();

    // Calculate derived metrics
    result.multiCol_nonLeafBloomChecks = result.multiCol_bloomChecks - result.multiCol_leafBloomChecks;
    
    // Calculate per-column averages
    double numCols = static_cast<double>(columns.size());
    result.multiCol_bloomChecksPerColumn = static_cast<double>(result.multiCol_bloomChecks) / numCols;
    result.multiCol_leafBloomChecksPerColumn = static_cast<double>(result.multiCol_leafBloomChecks) / numCols;
    result.multiCol_sstChecksPerColumn = static_cast<double>(result.multiCol_sstChecks) / numCols;
    result.multiCol_nonLeafBloomChecksPerColumn = static_cast<double>(result.multiCol_nonLeafBloomChecks) / numCols;

    // --- Hierarchical Single Column Query ---
    gBloomCheckCount = 0;
    gLeafBloomCheckCount = 0;
    gSSTCheckCount = 0;
    stopwatch.start();
    [[maybe_unused]] std::vector<std::string> singlehierarchyMatches =
        dbManager.findUsingSingleHierarchy(queryTrees[0], columns,
                                           currentExpectedValues);
    stopwatch.stop();
    result.hierarchicalSingleTime = stopwatch.elapsedMicros();
    result.singleCol_bloomChecks = gBloomCheckCount.load();
    result.singleCol_leafBloomChecks = gLeafBloomCheckCount.load();
    result.singleCol_sstChecks = gSSTCheckCount.load();

    // Calculate derived metrics
    result.singleCol_nonLeafBloomChecks = result.singleCol_bloomChecks - result.singleCol_leafBloomChecks;
    
    // Calculate per-column averages
    result.singleCol_bloomChecksPerColumn = static_cast<double>(result.singleCol_bloomChecks) ;
    result.singleCol_leafBloomChecksPerColumn = static_cast<double>(result.singleCol_leafBloomChecks) ;
    result.singleCol_sstChecksPerColumn = static_cast<double>(result.singleCol_sstChecks) ;
    result.singleCol_nonLeafBloomChecksPerColumn = static_cast<double>(result.singleCol_nonLeafBloomChecks) ;

    results.push_back(result);
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  return results;
}
*/

struct HashIndexCounters {
    size_t indexOpen = 0;
    size_t lookup = 0;
    size_t candidates = 0;
    size_t verification = 0;

    size_t singleIndexOpen = 0;
    size_t singleLookup = 0;
    size_t singleCandidates = 0;
    size_t singleVerification = 0;

    void add(const MixedQueryResult& result) {
        indexOpen += result.hashIndexOpenCount;
        lookup += result.hashLookupCount;
        candidates += result.hashCandidateCount;
        verification += result.hashVerificationCount;

        singleIndexOpen += result.hashSingleIndexOpenCount;
        singleLookup += result.hashSingleLookupCount;
        singleCandidates += result.hashSingleCandidateCount;
        singleVerification += result.hashSingleVerificationCount;
    }
};


std::vector<AccumulatedQueryMetrics> runComprehensiveQueryAnalysis(
    DBManager& dbManager,
    const std::map<std::string, BloomTree>& hierarchies,
    const std::vector<std::string>& columns,
    size_t dbSize,
    int numQueriesPerScenario,
    const DistributionConfig& config,
    const std::map<std::string, std::vector<std::string>>* columnSstFiles)
{
    std::vector<AccumulatedQueryMetrics> accumulatedResults;

    const std::vector<double> realDataPercentages = {
        0.0, 10.0, 20.0, 40.0, 60.0, 80.0, 100.0
    };

    spdlog::info(
        "Starting comprehensive analysis with {} queries per scenario",
        numQueriesPerScenario);

    for (double percentage : realDataPercentages) {

        spdlog::info("Running scenario with {}% real data", percentage);

        // --------------------------------------------------
        // 1. Execute queries
        // --------------------------------------------------

        std::vector<MixedQueryResult> results =
            runMixedQueriesWithCsvData(
                dbManager,
                hierarchies,
                columns,
                dbSize,
                numQueriesPerScenario,
                percentage,
                config,
                columnSstFiles);

        if (results.empty()) {
            spdlog::warn(
                "No results returned for {}% real data scenario",
                percentage);
            continue;
        }

        // --------------------------------------------------
        // 2. Initialize metrics
        // --------------------------------------------------

        AccumulatedQueryMetrics metrics;

        metrics.realDataPercentage = percentage;
        metrics.totalQueries = results.size();
        metrics.realQueries = 0;
        metrics.falseQueries = 0;
        metrics.numColumns = columns.size();

        // --------------------------------------------------
        // 3. Initialize time counters
        // --------------------------------------------------

        long long totalMultiTime = 0;
        long long totalSingleTime = 0;
        long long totalHashIndexTime = 0;
        long long totalHashSingleIndexTime = 0;
        long long totalHashSingleHierarchyIndexTime = 0;

        long long realMultiTimeSum = 0;
        long long realSingleTimeSum = 0;
        long long realHashIndexTimeSum = 0;
        long long realHashSingleIndexTimeSum = 0;
        long long realHashSingleHierarchyIndexTimeSum = 0;

        long long falseMultiTimeSum = 0;
        long long falseSingleTimeSum = 0;
        long long falseHashIndexTimeSum = 0;
        long long falseHashSingleIndexTimeSum = 0;
        long long falseHashSingleHierarchyIndexTimeSum = 0;

        // --------------------------------------------------
        // 4. Initialize BloomTree counters
        // --------------------------------------------------

        size_t totalMultiBloomChecks = 0;
        size_t totalMultiLeafBloomChecks = 0;
        size_t totalMultiSSTChecks = 0;
        size_t totalMultiNonLeafBloomChecks = 0;

        size_t totalSingleBloomChecks = 0;
        size_t totalSingleLeafBloomChecks = 0;
        size_t totalSingleSSTChecks = 0;
        size_t totalSingleNonLeafBloomChecks = 0;

        size_t realMultiBloomChecksSum = 0;
        size_t realMultiSSTChecksSum = 0;

        size_t falseMultiBloomChecksSum = 0;
        size_t falseMultiSSTChecksSum = 0;

        // --------------------------------------------------
        // 5. Initialize Hash Index counters
        // --------------------------------------------------

        HashIndexCounters totalHash;
        HashIndexCounters realHash;
        HashIndexCounters falseHash;

        // --------------------------------------------------
        // 6. Process query results
        // --------------------------------------------------

        for (const auto& result : results) {

            // Time measurements
            totalMultiTime += result.hierarchicalMultiTime;
            totalSingleTime += result.hierarchicalSingleTime;
            totalHashIndexTime += result.hashIndexTime;
            totalHashSingleIndexTime += result.hashSingleIndexTime;
            totalHashSingleHierarchyIndexTime +=
                result.hashSingleHierarchyIndexTime;

            // BloomTree measurements
            totalMultiBloomChecks += result.multiCol_bloomChecks;
            totalMultiLeafBloomChecks += result.multiCol_leafBloomChecks;
            totalMultiSSTChecks += result.multiCol_sstChecks;
            totalMultiNonLeafBloomChecks += result.multiCol_nonLeafBloomChecks;

            totalSingleBloomChecks += result.singleCol_bloomChecks;
            totalSingleLeafBloomChecks += result.singleCol_leafBloomChecks;
            totalSingleSSTChecks += result.singleCol_sstChecks;
            totalSingleNonLeafBloomChecks += result.singleCol_nonLeafBloomChecks;

            // Hash Index measurements
            totalHash.add(result);

            // Separate real and false data
            if (result.isRealData) {

                metrics.realQueries++;

                realMultiTimeSum += result.hierarchicalMultiTime;
                realSingleTimeSum += result.hierarchicalSingleTime;
                realHashIndexTimeSum += result.hashIndexTime;
                realHashSingleIndexTimeSum += result.hashSingleIndexTime;
                realHashSingleHierarchyIndexTimeSum +=
                    result.hashSingleHierarchyIndexTime;

                realMultiBloomChecksSum += result.multiCol_bloomChecks;
                realMultiSSTChecksSum += result.multiCol_sstChecks;

                realHash.add(result);
            }
            else {

                metrics.falseQueries++;

                falseMultiTimeSum += result.hierarchicalMultiTime;
                falseSingleTimeSum += result.hierarchicalSingleTime;
                falseHashIndexTimeSum += result.hashIndexTime;
                falseHashSingleIndexTimeSum += result.hashSingleIndexTime;
                falseHashSingleHierarchyIndexTimeSum +=
                    result.hashSingleHierarchyIndexTime;

                falseMultiBloomChecksSum += result.multiCol_bloomChecks;
                falseMultiSSTChecksSum += result.multiCol_sstChecks;

                falseHash.add(result);
            }
        }

        // --------------------------------------------------
        // 7. Calculate averages for all queries
        // --------------------------------------------------

        const double queryCount = static_cast<double>(metrics.totalQueries);

        metrics.avgHierarchicalMultiTime =
            totalMultiTime / queryCount;

        metrics.avgHierarchicalSingleTime =
            totalSingleTime / queryCount;

        metrics.avgHashIndexTime =
            totalHashIndexTime / queryCount;

        metrics.avgHashSingleIndexTime =
            totalHashSingleIndexTime / queryCount;

        metrics.avgHashSingleHierarchyIndexTime =
            totalHashSingleHierarchyIndexTime / queryCount;

        // BloomTree averages
        metrics.avgMultiBloomChecks =
            totalMultiBloomChecks / queryCount;

        metrics.avgMultiLeafBloomChecks =
            totalMultiLeafBloomChecks / queryCount;

        metrics.avgMultiSSTChecks =
            totalMultiSSTChecks / queryCount;

        metrics.avgMultiNonLeafBloomChecks =
            totalMultiNonLeafBloomChecks / queryCount;

        metrics.avgSingleBloomChecks =
            totalSingleBloomChecks / queryCount;

        metrics.avgSingleLeafBloomChecks =
            totalSingleLeafBloomChecks / queryCount;

        metrics.avgSingleSSTChecks =
            totalSingleSSTChecks / queryCount;

        metrics.avgSingleNonLeafBloomChecks =
            totalSingleNonLeafBloomChecks / queryCount;

        // Hash Index averages
        metrics.avgHashIndexOpenCount =
            totalHash.indexOpen / queryCount;

        metrics.avgHashLookupCount =
            totalHash.lookup / queryCount;

        metrics.avgHashCandidateCount =
            totalHash.candidates / queryCount;

        metrics.avgHashVerificationCount =
            totalHash.verification / queryCount;

        metrics.avgHashSingleIndexOpenCount =
            totalHash.singleIndexOpen / queryCount;

        metrics.avgHashSingleLookupCount =
            totalHash.singleLookup / queryCount;

        metrics.avgHashSingleCandidateCount =
            totalHash.singleCandidates / queryCount;

        metrics.avgHashSingleVerificationCount =
            totalHash.singleVerification / queryCount;

        // --------------------------------------------------
        // 8. Calculate per-column averages
        // --------------------------------------------------

        const double numCols =
            static_cast<double>(metrics.numColumns);

        if (numCols > 0) {

            metrics.avgMultiBloomChecksPerColumn =
                metrics.avgMultiBloomChecks / numCols;

            metrics.avgMultiLeafBloomChecksPerColumn =
                metrics.avgMultiLeafBloomChecks / numCols;

            metrics.avgMultiSSTChecksPerColumn =
                metrics.avgMultiSSTChecks / numCols;

            metrics.avgMultiNonLeafBloomChecksPerColumn =
                metrics.avgMultiNonLeafBloomChecks / numCols;
        }

        // Single-column hierarchy already represents one column.
        metrics.avgSingleBloomChecksPerColumn =
            metrics.avgSingleBloomChecks;

        metrics.avgSingleLeafBloomChecksPerColumn =
            metrics.avgSingleLeafBloomChecks;

        metrics.avgSingleSSTChecksPerColumn =
            metrics.avgSingleSSTChecks;

        metrics.avgSingleNonLeafBloomChecksPerColumn =
            metrics.avgSingleNonLeafBloomChecks;

        // --------------------------------------------------
        // 9. Calculate averages for real data queries
        // --------------------------------------------------

        if (metrics.realQueries > 0) {

            const double count =
                static_cast<double>(metrics.realQueries);

            metrics.avgRealDataMultiTime =
                realMultiTimeSum / count;

            metrics.avgRealDataSingleTime =
                realSingleTimeSum / count;

            metrics.avgRealHashIndexTime =
                realHashIndexTimeSum / count;

            metrics.avgRealHashSingleIndexTime =
                realHashSingleIndexTimeSum / count;

            metrics.avgRealHashSingleHierarchyIndexTime =
                realHashSingleHierarchyIndexTimeSum / count;

            metrics.avgRealMultiBloomChecks =
                realMultiBloomChecksSum / count;

            metrics.avgRealMultiSSTChecks =
                realMultiSSTChecksSum / count;

            metrics.avgRealMultiBloomChecksPerColumn =
                numCols > 0 ? metrics.avgRealMultiBloomChecks / numCols : 0.0;

            metrics.avgRealMultiSSTChecksPerColumn =
                numCols > 0 ? metrics.avgRealMultiSSTChecks / numCols : 0.0;

            // Hash Index - Multi-Column
            metrics.avgRealHashIndexOpenCount =
                realHash.indexOpen / count;

            metrics.avgRealHashLookupCount =
                realHash.lookup / count;

            metrics.avgRealHashCandidateCount =
                realHash.candidates / count;

            metrics.avgRealHashVerificationCount =
                realHash.verification / count;

            // Hash Index - Single-Column
            metrics.avgRealHashSingleIndexOpenCount =
                realHash.singleIndexOpen / count;

            metrics.avgRealHashSingleLookupCount =
                realHash.singleLookup / count;

            metrics.avgRealHashSingleCandidateCount =
                realHash.singleCandidates / count;

            metrics.avgRealHashSingleVerificationCount =
                realHash.singleVerification / count;
        }

        // --------------------------------------------------
        // 10. Calculate averages for false data queries
        // --------------------------------------------------

        if (metrics.falseQueries > 0) {

            const double count =
                static_cast<double>(metrics.falseQueries);

            metrics.avgFalseDataMultiTime =
                falseMultiTimeSum / count;

            metrics.avgFalseDataSingleTime =
                falseSingleTimeSum / count;

            metrics.avgFalseHashIndexTime =
                falseHashIndexTimeSum / count;

            metrics.avgFalseHashSingleIndexTime =
                falseHashSingleIndexTimeSum / count;

            metrics.avgFalseHashSingleHierachyIndexTime =
                falseHashSingleHierarchyIndexTimeSum / count;

            metrics.avgFalseMultiBloomChecks =
                falseMultiBloomChecksSum / count;

            metrics.avgFalseMultiSSTChecks =
                falseMultiSSTChecksSum / count;

            metrics.avgFalseMultiBloomChecksPerColumn =
                numCols > 0 ? metrics.avgFalseMultiBloomChecks / numCols : 0.0;

            metrics.avgFalseMultiSSTChecksPerColumn =
                numCols > 0 ? metrics.avgFalseMultiSSTChecks / numCols : 0.0;

            // Hash Index - Multi-Column
            metrics.avgFalseHashIndexOpenCount =
                falseHash.indexOpen / count;

            metrics.avgFalseHashLookupCount =
                falseHash.lookup / count;

            metrics.avgFalseHashCandidateCount =
                falseHash.candidates / count;

            metrics.avgFalseHashVerificationCount =
                falseHash.verification / count;

            // Hash Index - Single-Column
            metrics.avgFalseHashSingleIndexOpenCount =
                falseHash.singleIndexOpen / count;

            metrics.avgFalseHashSingleLookupCount =
                falseHash.singleLookup / count;

            metrics.avgFalseHashSingleCandidateCount =
                falseHash.singleCandidates / count;

            metrics.avgFalseHashSingleVerificationCount =
                falseHash.singleVerification / count;
        }

        // --------------------------------------------------
        // 11. Store scenario results
        // --------------------------------------------------

        accumulatedResults.push_back(metrics);

        spdlog::info(
            "Scenario {}% complete: {} real queries (avg: {:.2f} us), "
            "{} false queries (avg: {:.2f} us)",
            percentage,
            metrics.realQueries,
            metrics.avgRealDataMultiTime,
            metrics.falseQueries,
            metrics.avgFalseDataMultiTime);
    }

    spdlog::info(
        "Comprehensive analysis completed with {} scenarios",
        accumulatedResults.size());

    return accumulatedResults;
}
