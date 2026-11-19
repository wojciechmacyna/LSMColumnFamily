#include "db_manager.hpp"

#include <rocksdb/iterator.h>
#include <rocksdb/options.h>
#include <rocksdb/sst_file_manager.h>
#include <rocksdb/sst_file_reader.h>
#include <rocksdb/status.h>
#include <rocksdb/table_properties.h>
#include <spdlog/spdlog.h>

#include <boost/asio/post.hpp>
#include <boost/asio/thread_pool.hpp>
#include <filesystem>
#include <future>
#include <random>
#include <stdexcept>
#include <unordered_set>

#include <algorithm>
#include <cmath>

#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>
#include <chrono>

#include "algorithm.hpp"
#include "stopwatch.hpp"
#include "sst_hash_index.hpp"



extern boost::asio::thread_pool globalThreadPool;



std::vector<std::string>
DBManager::findUsingSingleHierarchyAndHashIndexes(
    BloomTree& hierarchy,
    const std::vector<std::string>& columns,
    const std::vector<std::string>& values,
    HashHierarchyTimings* timings)
{
    if (columns.size() != values.size() || columns.empty()) {
        throw std::runtime_error(
            "Number of columns and values must be equal and non-empty.");
    }

    using Clock = std::chrono::steady_clock;

    const auto elapsedMicros = [](const auto& start, const auto& end) {
        return static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::microseconds>(
                end - start).count());
    };

    StopWatch totalWatch;
    totalWatch.start();

    HashHierarchyTimings localTimings{};

    // --------------------------------------------------
    // 1. Wyszukanie kandydatów w BloomTree
    // --------------------------------------------------

    auto bloomStart = Clock::now();

    std::vector<const Node*> candidates =
        hierarchy.queryNodes(values[0], "", "");

    if (candidates.empty()) {

        auto bloomEnd = Clock::now();

        localTimings.bloomTime =
            elapsedMicros(bloomStart, bloomEnd);

        totalWatch.stop();

        localTimings.totalTime = totalWatch.elapsedMicros();

        if (timings != nullptr) {
            *timings = localTimings;
        }

        spdlog::info(
            "HashSingleHierarchy: no candidates. "
            "Bloom: {} us, Total: {} us",
            localTimings.bloomTime,
            localTimings.totalTime);

        return {};
    }

    // --------------------------------------------------
    // 2. Grupowanie liści według plików SST
    // --------------------------------------------------

    std::map<std::string, std::vector<SSTRange>> sstRanges;

    for (const Node* node : candidates) {
        sstRanges[node->filename].push_back({
            node->startKey,
            node->endKey
        });
    }

    auto bloomEnd = Clock::now();

    localTimings.bloomTime =
        elapsedMicros(bloomStart, bloomEnd);

    // --------------------------------------------------
    // 3. Otwieranie Hash Indexów i wyszukiwanie wartości
    // --------------------------------------------------

    std::vector<std::string> allKeys;

    uint64_t openedIndexes = 0;
    uint64_t failedIndexes = 0;
    uint64_t hashReturnedKeys = 0;

    for (const auto& [filename, ranges] : sstRanges) {

        const std::string indexPath =
            filename + "." + columns[0] + ".hidx";

        SSTHashIndex hashIndex;

        // T2: otwieranie indeksu

        auto openStart = Clock::now();

        if (!hashIndex.open(indexPath)) {

            auto openEnd = Clock::now();

            localTimings.openTime +=
                elapsedMicros(openStart, openEnd);

            ++failedIndexes;

            spdlog::warn(
                "Cannot open Hash Index: {}",
                indexPath);

            continue;
        }

        auto openEnd = Clock::now();

        localTimings.openTime +=
            elapsedMicros(openStart, openEnd);

        ++openedIndexes;

        std::vector<std::string> keys;

        // T3: wyszukiwanie wartości

        auto lookupStart = Clock::now();

        bool lookupSuccess =
            hashIndex.lookup(values[0], keys);

        auto lookupEnd = Clock::now();

        localTimings.lookupTime +=
            elapsedMicros(lookupStart, lookupEnd);

        if (!lookupSuccess) {

            spdlog::warn(
                "Hash Index lookup failed: {}",
                indexPath);

            continue;
        }

        hashReturnedKeys += keys.size();

        // --------------------------------------------------
        // 4. T4: Range filtering
        // --------------------------------------------------

        auto rangeStart = Clock::now();

        // Sortowanie zakresów według startKey.
        std::vector<SSTRange> sortedRanges = ranges;

        std::sort(
            sortedRanges.begin(),
            sortedRanges.end(),
            [](const SSTRange& a, const SSTRange& b) {
                return a.startKey < b.startKey;
            });

        // Scalanie nakładających się zakresów.
        std::vector<SSTRange> normalizedRanges;
        normalizedRanges.reserve(sortedRanges.size());

        for (const auto& range : sortedRanges) {

            if (normalizedRanges.empty() ||
                range.startKey > normalizedRanges.back().endKey) {

                normalizedRanges.push_back(range);
            }
            else if (range.endKey > normalizedRanges.back().endKey) {

                normalizedRanges.back().endKey = range.endKey;
            }
        }

        // Wyszukiwanie binarne zakresu dla każdego klucza.
        for (const auto& key : keys) {

            auto it = std::upper_bound(
                normalizedRanges.begin(),
                normalizedRanges.end(),
                key,
                [](const std::string& value, const SSTRange& range) {
                    return value < range.startKey;
                });

            if (it != normalizedRanges.begin()) {

                --it;

                if (key <= it->endKey) {
                    allKeys.push_back(key);
                }
            }
        }

        auto rangeEnd = Clock::now();

        localTimings.rangeFilterTime +=
            elapsedMicros(rangeStart, rangeEnd);
    }

    // --------------------------------------------------
    // 5. Sort + unique
    // --------------------------------------------------

    auto sortStart = Clock::now();

    std::sort(allKeys.begin(), allKeys.end());

    allKeys.erase(
        std::unique(allKeys.begin(), allKeys.end()),
        allKeys.end());

    auto sortEnd = Clock::now();

    localTimings.sortUniqueTime =
        elapsedMicros(sortStart, sortEnd);

    // --------------------------------------------------
    // 6. Przygotowanie Column Family handles
    // --------------------------------------------------

    std::vector<rocksdb::ColumnFamilyHandle*> cfHandles;
    cfHandles.reserve(columns.size());

    for (const auto& column : columns) {

        auto cfIt = cf_handles_.find(column);

        if (cfIt == cf_handles_.end()) {
            throw std::runtime_error(
                "Column Family not found: " + column);
        }

        cfHandles.push_back(cfIt->second.get());
    }

    rocksdb::ReadOptions readOptions;
    readOptions.fill_cache = false;

    // --------------------------------------------------
    // 7. Weryfikacja kluczy we wszystkich kolumnach
    // --------------------------------------------------

    auto verifyStart = Clock::now();

    std::vector<std::string> matchingKeys;

    for (const auto& key : allKeys) {

        bool matches = true;

        for (size_t i = 0; i < columns.size(); ++i) {

            std::string dbValue;

            auto status = db_->Get(
                readOptions,
                cfHandles[i],
                key,
                &dbValue);

            if (!status.ok() || dbValue != values[i]) {
                matches = false;
                break;
            }
        }

        if (matches) {
            matchingKeys.push_back(key);
        }
    }

    auto verifyEnd = Clock::now();

    localTimings.rocksDBVerifyTime =
        elapsedMicros(verifyStart, verifyEnd);

    // --------------------------------------------------
    // 8. Podsumowanie
    // --------------------------------------------------

    totalWatch.stop();

    localTimings.totalTime = totalWatch.elapsedMicros();

    if (timings != nullptr) {
        *timings = localTimings;
    }

    spdlog::info(
        "HashSingleHierarchy completed. "
        "Bloom candidates: {}, SST files: {}, "
        "Opened indexes: {}, Failed indexes: {}, "
        "Hash returned keys: {}, Unique keys: {}, "
        "Matching keys: {}, "
        "Bloom: {} us, Open: {} us, Lookup: {} us, "
        "RangeFilter: {} us, SortUnique: {} us, "
        "RocksDBVerify: {} us, Total: {} us",
        candidates.size(),
        sstRanges.size(),
        openedIndexes,
        failedIndexes,
        hashReturnedKeys,
        allKeys.size(),
        matchingKeys.size(),
        localTimings.bloomTime,
        localTimings.openTime,
        localTimings.lookupTime,
        localTimings.rangeFilterTime,
        localTimings.sortUniqueTime,
        localTimings.rocksDBVerifyTime,
        localTimings.totalTime);

    return matchingKeys;
}



std::vector<std::string> DBManager::findUsingHashIndexesSingleColumn(
    const std::vector<std::string>& columns,
    const std::vector<std::string>& values,
    const std::map<std::string, std::vector<std::string>>& columnSstFiles)
{
    if (columns.empty() || columns.size() != values.size()) {
        throw std::runtime_error(
            "Number of columns and values must be equal and non-empty.");
    }

    ++gHashIndexOpenCount;

    StopWatch sw;
    sw.start();

    // --------------------------------------------------
    // 1. Search Hash Indexes of the first column only
    // --------------------------------------------------

    const auto& firstColumn = columns[0];
    const auto& firstValue = values[0];

    auto filesIt = columnSstFiles.find(firstColumn);

    if (filesIt == columnSstFiles.end()) {
        throw std::runtime_error(
            "No SST files registered for column: " + firstColumn);
    }

    const auto& sstFiles = filesIt->second;

    spdlog::info(
        "Searching Hash Indexes for first column '{}', value '{}'. "
        "Number of SST files: {}",
        firstColumn,
        firstValue,
        sstFiles.size());

    // --------------------------------------------------
    // 2. Search all Hash Indexes of the first column
    // --------------------------------------------------

    std::unordered_set<std::string> uniqueKeys;

    for (const auto& sstFile : sstFiles) {

        const std::string indexPath =
            sstFile + "." + firstColumn + ".hidx";

        SSTHashIndex index;

        if (!index.open(indexPath)) {
            throw std::runtime_error(
                "Cannot open hash index: " + indexPath);
        }

        std::vector<std::string> keys;

        ++gHashLookupCount;

        if (!index.lookup(firstValue, keys)) {
            throw std::runtime_error(
                "Hash index lookup failed: " + indexPath);
        }

        uniqueKeys.insert(
            std::make_move_iterator(keys.begin()),
            std::make_move_iterator(keys.end()));
    }

    spdlog::info(
        "Number of unique candidate keys found: {}",
        uniqueKeys.size());

    // --------------------------------------------------
    // 3. Verify candidates against all queried columns
    // --------------------------------------------------

    std::vector<std::string> matchingKeys;

    for (const auto& key : uniqueKeys) {

        bool matches = true;

        for (size_t i = 0; i < columns.size(); ++i) {

            auto handleIt = cf_handles_.find(columns[i]);

            if (handleIt == cf_handles_.end()) {
                throw std::runtime_error(
                    "Unknown column family: " + columns[i]);
            }

            std::string currentValue;

            ++gHashVerificationCount;

            rocksdb::Status status = db_->Get(
                rocksdb::ReadOptions(),
                handleIt->second.get(),
                key,
                &currentValue);

            if (!status.ok() || currentValue != values[i]) {
                matches = false;
                break;
            }
        }

        if (matches) {
            matchingKeys.push_back(key);
        }
    }

    gHashCandidateCount = uniqueKeys.size();

    // --------------------------------------------------
    // 4. Sort results
    // --------------------------------------------------

    std::sort(
        matchingKeys.begin(),
        matchingKeys.end());

    // --------------------------------------------------
    // 5. Summary
    // --------------------------------------------------

    sw.stop();

    spdlog::info(
        "findUsingHashIndexesSingleColumn completed. "
        "First column: {}, SST files: {}, "
        "Unique keys: {}, Matching keys: {}, Time: {} us",
        firstColumn,
        sstFiles.size(),
        uniqueKeys.size(),
        matchingKeys.size(),
        sw.elapsedMicros());

    return matchingKeys;
}



std::vector<std::string> DBManager::findUsingHashIndexes(
    const std::vector<std::string>& columns,
    const std::vector<std::string>& values,
    const std::map<std::string, std::vector<std::string>>& columnSstFiles) {

    if (columns.empty() || columns.size() != values.size()) {
        return {};
    }

    // One candidate-key set per queried column.
    std::vector<std::unordered_set<std::string>> columnResults;
    columnResults.reserve(columns.size());

    // ---------------------------------------------------------
    // 1. Search ALL local hash indexes for every column.
    // ---------------------------------------------------------

    for (size_t i = 0; i < columns.size(); ++i) {

        const auto& column = columns[i];
        const auto& value = values[i];

        auto filesIt = columnSstFiles.find(column);

        if (filesIt == columnSstFiles.end()) {
            throw std::runtime_error(
                "No SST files registered for column: " + column);
        }

        std::unordered_set<std::string> matchingKeys;

        for (const auto& sstFile : filesIt->second) {

            const std::string indexPath =
                sstFile + "." + column + ".hidx";

            SSTHashIndex index;

            if (!index.open(indexPath)) {
                throw std::runtime_error(
                    "Cannot open hash index: " + indexPath);
            }

            ++gHashIndexOpenCount;

            std::vector<std::string> keys;

            ++gHashLookupCount;

            if (!index.lookup(value, keys)) {
                throw std::runtime_error(
                    "Hash index lookup failed: " + indexPath);
            }

            matchingKeys.insert(
                std::make_move_iterator(keys.begin()),
                std::make_move_iterator(keys.end()));
        }

        columnResults.push_back(std::move(matchingKeys));
    }

    // ---------------------------------------------------------
    // 2. Intersect candidate keys from all columns.
    // ---------------------------------------------------------

    // Start with the smallest candidate set.
    size_t smallestIndex = 0;

    for (size_t i = 1; i < columnResults.size(); ++i) {
        if (columnResults[i].size() <
            columnResults[smallestIndex].size()) {
            smallestIndex = i;
        }
    }

    std::vector<std::string> candidates;

    for (const auto& key : columnResults[smallestIndex]) {

        bool presentInAll = true;

        for (size_t i = 0; i < columnResults.size(); ++i) {

            if (i == smallestIndex) {
                continue;
            }

            if (columnResults[i].find(key) ==
                columnResults[i].end()) {
                presentInAll = false;
                break;
            }
        }

        if (presentInAll) {
            candidates.push_back(key);
        }
    }

    gHashCandidateCount = candidates.size();

    // ---------------------------------------------------------
    // 3. Verify candidates against current RocksDB data.
    // ---------------------------------------------------------

    std::vector<std::string> results;

    gHashVerificationCount = candidates.size();

    for (const auto& key : candidates) {

        bool matches = true;

        for (size_t i = 0; i < columns.size(); ++i) {

            auto handleIt = cf_handles_.find(columns[i]);

            if (handleIt == cf_handles_.end()) {
                throw std::runtime_error(
                    "Unknown column family: " + columns[i]);
            }

            std::string currentValue;

            rocksdb::Status status = db_->Get(
                rocksdb::ReadOptions(),
                handleIt->second.get(),
                key,
                &currentValue);

            if (!status.ok() || currentValue != values[i]) {
                matches = false;
                break;
            }
        }

        if (matches) {
            results.push_back(key);
        }
    }

    std::sort(results.begin(), results.end());

    return results;
}



void DBManager::flushAllColumnFamilies()
{
    if (!db_) {
        throw std::runtime_error("DB not open");
    }

    for (auto& kv : cf_handles_) {
        auto* handle = kv.second.get();

        rocksdb::FlushOptions options;
        options.wait = true;

        auto status = db_->Flush(options, handle);

        if (!status.ok()) {
            throw std::runtime_error(
                "Flush failed for CF '" + kv.first +
                "': " + status.ToString()
            );
        }

        spdlog::info(
            "Flush completed for CF '{}'",
            kv.first
        );
    }
}


void DBManager::compactAllColumnFamilies(size_t numRecords) {
  if (!db_) throw std::runtime_error("DB not open");
  rocksdb::CompactRangeOptions opts;

  auto s_enable_del = db_->EnableFileDeletions(true);
  if (!s_enable_del.ok()) {
      spdlog::warn("Failed to ensure file deletions are enabled: {}. DB size may grow.", s_enable_del.ToString());
  }

  for (auto& kv : cf_handles_) {
    auto* handle = kv.second.get();
    auto s_flush = db_->Flush(rocksdb::FlushOptions(), handle);
    if (!s_flush.ok()) {
        spdlog::error("Flush failed for CF '{}': {}. Skipping compaction for this CF.", kv.first, s_flush.ToString());
        continue;
    }

    rocksdb::Slice begin, end;
    std::string endKeyStr;

    if (numRecords > 0) {
      begin = rocksdb::Slice();
      const std::string index_str = std::to_string(numRecords);
      const std::string prefixedIndex =
          std::string(20 - index_str.size(), '0') + index_str;
      endKeyStr = "key" + prefixedIndex;
      end = rocksdb::Slice(endKeyStr);
      
      spdlog::info("Starting ranged compaction for CF '{}' up to key '{}'", kv.first, endKeyStr);
      auto status = db_->CompactRange(opts, handle, &begin, &end);
      if (!status.ok()) {
        spdlog::error("Ranged compaction failed for CF '{}': {}", kv.first, status.ToString());
      } else {
        spdlog::debug("Ranged compaction succeeded for CF '{}'", kv.first);
      }
    } else {
      spdlog::info("Starting full compaction for CF '{}'", kv.first);
      auto status = db_->CompactRange(opts, handle, nullptr, nullptr);
      if (!status.ok()) {
        spdlog::error("Full compaction failed for CF '{}': {}", kv.first, status.ToString());
      } else {
        spdlog::info("Full compaction succeeded for CF '{}'", kv.first);
      }
    }
  }

  spdlog::info("Waiting for all background compactions to finish across the DB...");
  rocksdb::WaitForCompactOptions wco;

  auto s_wait = db_->WaitForCompact(wco);
  if (!s_wait.ok()) {
    spdlog::error("Error occurred while waiting for all compactions to finish: {}", s_wait.ToString());
  } else {
    spdlog::info("All background compactions finished successfully.");
  }
}

void DBManager::openDB(const std::string& dbname,
                       std::vector<std::string> columns, const bool adding) {
  StopWatch sw;
  sw.start();

  if (db_) {
    spdlog::warn("DB already open, closing before reopening.");
    closeDB();
  }

  rocksdb::DBOptions dbOptions;
  dbOptions.create_if_missing = true;
  dbOptions.create_missing_column_families = true;

  if (adding==true){
    listener_ = std::make_shared<MyListener>();
    dbOptions.listeners.emplace_back(listener_);
    
}


  std::vector<std::string> cf_names = columns;
  cf_names.push_back("default");
  std::vector<rocksdb::ColumnFamilyDescriptor> cf_descriptors;
  for (const auto& name : cf_names) {
    cf_descriptors.emplace_back(name, rocksdb::ColumnFamilyOptions());
  }

  std::vector<rocksdb::ColumnFamilyHandle*> cf_handles_raw;
  rocksdb::DB* rawDbPtr = nullptr;

  auto status = rocksdb::DB::Open(dbOptions, dbname, cf_descriptors,
                                  &cf_handles_raw, &rawDbPtr);
  if (!status.ok()) {
    throw std::runtime_error("Failed to open DB with Column Families: " +
                             status.ToString());
  }

  db_.reset(rawDbPtr);

  // Store ColumnFamilyHandles in a map for easy access
  cf_handles_.clear();
  for (size_t i = 0; i < cf_names.size(); ++i) {
    cf_handles_[cf_names[i]].reset(cf_handles_raw[i]);
  }

  sw.stop();
  spdlog::critical("RocksDB opened at path: {} with CFs, took {} µs", dbname,
                   sw.elapsedMicros());
}


void DBManager::insertRecords(
    int numRecords,
    std::vector<std::string> columns,
    const DistributionConfig& config, int rangeFrom) {

  if (!db_) throw std::runtime_error("DB not open.");
  if (numRecords < 0)
    throw std::invalid_argument("numRecords cannot be negative.");

  StopWatch sw;
  sw.start();

 
const char* distributionName =
    config.type == DataDistribution::Zipfian ? "Zipfian" : "Normal";


  spdlog::info(
      "Inserting {} records across {} CFs using {} distribution...",
      numRecords, columns.size(), distributionName);

  // Initialize Zipfian / Mixed generator only when needed.
const double zipfExponent = config.exponent;
const double zipfRatio = std::clamp(config.zipfRatio, 0.0, 1.0);
const size_t maxZipfCardinality = config.cardinality;

std::mt19937_64 rng(config.seed);
std::vector<double> weights;
std::discrete_distribution<size_t> zipfDist;

if ((config.type == DataDistribution::Zipfian ||
     config.type == DataDistribution::Mixed) &&
    numRecords > 0) {

    const size_t cardinality = std::min(
        static_cast<size_t>(numRecords),
        maxZipfCardinality);

    if (cardinality > 0) {
        weights.reserve(cardinality);

        double zipfWeightSum = 0.0;

        // Calculate Zipf weights.
        for (size_t rank = 1; rank <= cardinality; ++rank) {
            const double weight =
                1.0 / std::pow(
                    static_cast<double>(rank),
                    zipfExponent);

            weights.push_back(weight);
            zipfWeightSum += weight;
        }

        // Normalize Zipf weights and combine with Uniform.
        if (config.type == DataDistribution::Mixed) {

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
}

  rocksdb::WriteBatch batch;

  for (int i = rangeFrom; i <= numRecords; ++i) {
    const std::string index = std::to_string(i);

    const std::string prefixedIndex =
        std::string(20 - index.size(), '0') + index;

    const std::string key = "key" + prefixedIndex;

    for (const auto& column : columns) {
      std::string value;

      if (config.type == DataDistribution::Zipfian ||
        config.type == DataDistribution::Mixed) {

        // Rank 1 is the most frequent value.
        const size_t rank = zipfDist(rng) + 1;

        value = column + "_value" + std::to_string(rank);
    } else {
        // Original behavior: every record has a distinct value.
        value = column + "_value" + index;
    }

      auto handle = cf_handles_.at(column).get();
      batch.Put(handle, key, value);
    }

    if (i % 1000000 == 0) {
      auto s = db_->Write(rocksdb::WriteOptions(), &batch);

      if (!s.ok())
        throw std::runtime_error(
            "Batch write failed: " + s.ToString());

      batch.Clear();
      spdlog::debug("Inserted {} records...", i);
    }
  }

  if (batch.Count() > 0) {
    auto s = db_->Write(rocksdb::WriteOptions(), &batch);

    if (!s.ok())
      throw std::runtime_error(
          "Final batch write failed: " + s.ToString());
  }

  for (const auto& column : columns) {
    auto handle = cf_handles_.at(column).get();
    auto s = db_->Flush(rocksdb::FlushOptions(), handle);

    if (!s.ok())
      throw std::runtime_error(
          "Flush failed: " + s.ToString());
  }

  sw.stop();

  spdlog::critical(
      "Inserted {} records across CFs using {} distribution in {} µs.",
      numRecords, distributionName, sw.elapsedMicros());
}


void DBManager::importNYCTaxiCSV(
    const std::string& filename,
    size_t maxRecords,
    size_t batchSize)
{
    if (!db_) {
        throw std::runtime_error(
            "Cannot import CSV: database is not open.");
    }

    if (batchSize == 0) {
        throw std::invalid_argument(
            "CSV batchSize must be greater than zero.");
    }

    StopWatch sw;
    sw.start();

    const std::vector<std::string> columns = {
        "pickup_location",
        "dropoff_location",
        "payment_type",
        "passenger_count",
        "vendor_id",
        "ratecode_id"
    };

    // Verify that all required Column Families exist.
    std::map<std::string, rocksdb::ColumnFamilyHandle*> handles;

    for (const auto& column : columns) {
        auto it = cf_handles_.find(column);

        if (it == cf_handles_.end() || !it->second) {
            throw std::runtime_error(
                "Missing Column Family: " + column);
        }

        handles[column] = it->second.get();
    }

    std::ifstream file(filename);

    if (!file.is_open()) {
        throw std::runtime_error(
            "Cannot open CSV file: " + filename);
    }

    // Read CSV header.
    std::string line;

    if (!std::getline(file, line)) {
        throw std::runtime_error("CSV file is empty.");
    }

    // Remove UTF-8 BOM if present.
    if (line.size() >= 3 &&
        static_cast<unsigned char>(line[0]) == 0xEF &&
        static_cast<unsigned char>(line[1]) == 0xBB &&
        static_cast<unsigned char>(line[2]) == 0xBF) {
        line.erase(0, 3);
    }

    std::vector<std::string> header;

    if (!parseCSVLine(line, header)) {
        throw std::runtime_error(
            "Invalid CSV header.");
    }

    // Map required attributes to CSV positions.
    const std::map<std::string, std::string> csvColumnNames = {
        {"pickup_location", "PULocationID"},
        {"dropoff_location", "DOLocationID"},
        {"payment_type", "payment_type"},
        {"passenger_count", "passenger_count"},
        {"vendor_id", "VendorID"},
        {"ratecode_id", "RatecodeID"}
    };

    std::map<std::string, size_t> positions;

    for (const auto& column : columns) {
        const std::string& csvName =
            csvColumnNames.at(column);

        auto it = std::find(
            header.begin(), header.end(), csvName);

        if (it == header.end()) {
            throw std::runtime_error(
                "CSV column not found: " + csvName);
        }

        positions[column] =
            static_cast<size_t>(
                std::distance(header.begin(), it));
    }

    spdlog::info(
        "Starting NYC Taxi CSV import: {}",
        filename);

    rocksdb::WriteBatch batch;

    size_t importedRecords = 0;
    size_t skippedRecords = 0;
    size_t processedRecords = 0;
    size_t batchRecords = 0;

    uint64_t recordIndex = 1;

    std::vector<std::string> fields;

    while (std::getline(file, line)) {

        if (maxRecords > 0 &&
            processedRecords >= maxRecords) {
            break;
        }

        ++processedRecords;

        if (!parseCSVLine(line, fields)) {
            ++skippedRecords;
            continue;
        }

        bool validRecord = true;

        for (const auto& column : columns) {
            const size_t position = positions.at(column);

            if (position >= fields.size() ||
                fields[position].empty()) {
                validRecord = false;
                break;
            }
        }

        if (!validRecord) {
            ++skippedRecords;
            continue;
        }

        const std::string key =
            makeRecordKey(recordIndex);

        for (const auto& column : columns) {
            batch.Put(
                handles.at(column),
                key,
                fields[positions.at(column)]);
        }

        ++recordIndex;
        ++importedRecords;
        ++batchRecords;

        if (batchRecords >= batchSize) {
            rocksdb::Status status =
                db_->Write(rocksdb::WriteOptions(), &batch);

            if (!status.ok()) {
                throw std::runtime_error(
                    "CSV batch write failed: " +
                    status.ToString());
            }

            batch.Clear();
            batchRecords = 0;
        }
    }

    // Write remaining records.
    if (batchRecords > 0) {
        rocksdb::Status status =
            db_->Write(rocksdb::WriteOptions(), &batch);

        if (!status.ok()) {
            throw std::runtime_error(
                "Final CSV batch write failed: " +
                status.ToString());
        }
    }

    file.close();

    // Flush all imported Column Families.
    for (const auto& column : columns) {
        rocksdb::FlushOptions flushOptions;
        flushOptions.wait = true;

        rocksdb::Status status =
            db_->Flush(
                flushOptions,
                handles.at(column));

        if (!status.ok()) {
            throw std::runtime_error(
                "Flush failed for Column Family " +
                column + ": " + status.ToString());
        }
    }

    sw.stop();

    spdlog::info(
        "NYC Taxi import completed. "
        "Imported: {}, skipped: {}, processed: {}, "
        "batch size: {}, time: {} us",
        importedRecords,
        skippedRecords,
        processedRecords,
        batchSize,
        sw.elapsedMicros());
}



void DBManager::insertRecordsWithSearchTargets(
    int numRecords, const std::vector<std::string>& columns,
    const std::unordered_set<int>& targetIndices) {
  if (!db_) throw std::runtime_error("DB not open.");

  StopWatch sw;
  sw.start();
  spdlog::info("Inserting {} records across {} CFs... with {} search targets",
               numRecords, columns.size(), targetIndices.size());

  rocksdb::WriteBatch batch;
  for (int i = 1; i <= numRecords; ++i) {
    bool isTarget = targetIndices.find(i) != targetIndices.end();

    // prefix index with 0s to ensure lexicographical order based on numRecords
    // size
    const std::string index = std::to_string(i);
    const std::string prefixedIndex =
        std::string(20 - index.size(), '0') + std::to_string(i);

    const std::string key = "key" + prefixedIndex;
    for (const auto& column : columns) {
      auto handle = cf_handles_.at(column).get();
      std::string value;

      if (isTarget) {
        value = column + "_target";
        spdlog::debug("Creating target value: {} for key: {}", value, key);
      } else {
        value = column + "_value" + index;
      }

      batch.Put(handle, key, value);
    }
    if (i % 1000000 == 0) {
      auto s = db_->Write(rocksdb::WriteOptions(), &batch);
      if (!s.ok())
        throw std::runtime_error("Batch write failed: " + s.ToString());
      batch.Clear();
      spdlog::debug("Inserted {} records...", i);
    }
  }

  if (batch.Count() > 0) {
    auto s = db_->Write(rocksdb::WriteOptions(), &batch);
    if (!s.ok())
      throw std::runtime_error("Final batch write failed: " + s.ToString());
  }

  for (const auto& column : columns) {
    auto handle = cf_handles_.at(column).get();
    auto s = db_->Flush(rocksdb::FlushOptions(), handle);
    if (!s.ok()) throw std::runtime_error("Flush failed: " + s.ToString());
  }

  sw.stop();
  spdlog::critical("Inserted {} records across CFs in {} µs.", numRecords,
                   sw.elapsedMicros());
}

std::vector<std::string> DBManager::scanSSTFilesForColumn(
    const std::string& dbname, const std::string& column) {
  if (!db_) throw std::runtime_error("DB not open.");
  if (cf_handles_.find(column) == cf_handles_.end())
    throw std::runtime_error("Unknown Column Family: " + column);

  rocksdb::ColumnFamilyMetaData meta;
  db_->GetColumnFamilyMetaData(cf_handles_[column].get(), &meta);

  std::vector<std::string> sst_files;
  for (const auto& level : meta.levels) {
    for (const auto& file : level.files) {
      sst_files.push_back(dbname + file.name);
    }
  }

  spdlog::info("Column {} has {} SST files.", column, sst_files.size());
  return sst_files;
}

bool DBManager::checkValueWithoutBloomFilters(const std::string& value) {
  StopWatch sw;
  sw.start();

  if (!db_) {
    throw std::runtime_error("Cannot check value: DB is not open.");
  }

  rocksdb::ReadOptions readOptions;
  readOptions.fill_cache = false;
  readOptions.verify_checksums = true;

  sw.stop();
  spdlog::critical("checkValueWithoutBloomFilters setup took {} µs.",
                   sw.elapsedMicros());

  sw.start();
  // Create an iterator to scan all K-V pairs in the open DB
  std::unique_ptr<rocksdb::Iterator> iter(db_->NewIterator(readOptions));
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    if (iter->value().ToString() == value) {
      sw.stop();
      spdlog::critical("checkValueWithoutBloomFilters took {} µs (found).",
                       sw.elapsedMicros());
      return true;
    }
  }
  sw.stop();
  spdlog::critical("checkValueWithoutBloomFilters took {} µs (not found).",
                   sw.elapsedMicros());
  return false;
}


std::vector<std::string> DBManager::getChangedSSTFilesForColumn(
    const std::string& dbname,
    const std::string& column)
{
    if (!db_) {
        throw std::runtime_error("DB not open.");
    }

    auto it = cf_handles_.find(column);
    if (it == cf_handles_.end()) {
        throw std::runtime_error("Unknown Column Family: " + column);
    }

    // Pobierz zdarzenia z listenera.
    auto changes = listener_->getSSTChanges(column);

    for (const auto& change : changes) {
    spdlog::info(
        "SST event: CF={}, path={}, created={}",
        change.cf_name,
        change.file_path,
        change.created
    );
}

    // Normalizacja ścieżek.
    auto normalizePath = [&](const std::string& path) {
    std::filesystem::path p(path);

    // Usuń początkowy "/" z nazwy pliku zwracanej przez RocksDB.
    if (p.is_absolute()) {
        p = p.relative_path();
    }

    // Jeżeli ścieżka zawiera tylko nazwę pliku,
    // dodaj katalog bazy danych.
    if (!p.has_parent_path()) {
        p = std::filesystem::path(dbname) / p;
    }

    return p.lexically_normal().string();
};

    // Zbierz ścieżki nowo utworzonych SST.
    std::unordered_set<std::string> createdFiles;

    for (const auto& change : changes) {
        if (change.created) {
            createdFiles.insert(normalizePath(change.file_path));
        }
    }

    rocksdb::ColumnFamilyMetaData meta;

    db_->GetColumnFamilyMetaData(
        it->second.get(),
        &meta
    );

    std::vector<std::string> sstFiles;


    // Wyświetlenie ścieżek z listenera
for (const auto& change : changes) {
    spdlog::info(
        "Listener path: [{}], created={}",
        change.file_path,
        change.created
    );
}

// Wyświetlenie ścieżek z metadanych RocksDB
for (const auto& level : meta.levels) {
    for (const auto& file : level.files) {
        std::string path = normalizePath(file.name);

        spdlog::info(
            "SST metadata: CF={}, file={}, smallest={}, largest={}",
            column,
            path,
            file.smallestkey,
            file.largestkey
        );

        if (createdFiles.contains(path)) {
            sstFiles.push_back(path);

            spdlog::info("NEW SST MATCHED: {}", path);
        }
    }
}


    spdlog::info(
        "Column {}: {} current SST files, {} created events, {} new SST files",
        column,
        [&]() {
            size_t count = 0;
            for (const auto& level : meta.levels) {
                count += level.files.size();
            }
            return count;
        }(),
        createdFiles.size(),
        sstFiles.size()
    );

    return sstFiles;
}



SSTFileChanges DBManager::getCompactionChangesForColumn(
        const std::string& dbname,
    const std::string& column)
{
    if (!db_) {
        throw std::runtime_error("DB not open.");
    }

    auto it = cf_handles_.find(column);

    if (it == cf_handles_.end()) {
        throw std::runtime_error(
            "Unknown Column Family: " + column
        );
    }

// Pobierz zdarzenia compaction dla wskazanego CF.
auto changes = listener_->getCompactionChanges(column);

// Normalizacja ścieżek.
auto normalizePath = [&dbname](const std::string& path) {
    std::filesystem::path p(path);

    if (p.is_absolute() && p.parent_path() == "/") {
        p = std::filesystem::path(dbname) / p.filename();
    }
    else if (!p.is_absolute() && !p.has_parent_path()) {
        p = std::filesystem::path(dbname) / p;
    }

    return p.lexically_normal().string();
};

SSTFileChanges result;

std::unordered_set<std::string> removedSet;
std::unordered_set<std::string> addedSet;

// Przetwarzanie zdarzeń compaction.
for (const auto& change : changes) {

    std::unordered_set<std::string> inputFiles;
    std::unordered_set<std::string> outputFiles;

    for (const auto& file : change.input_files) {
        inputFiles.insert(normalizePath(file));
    }

    for (const auto& file : change.output_files) {
        outputFiles.insert(normalizePath(file));
    }

    for (const auto& file : inputFiles) {
        if (!outputFiles.contains(file)) {
            removedSet.insert(file);
        }
    }

    for (const auto& file : outputFiles) {
        if (!inputFiles.contains(file)) {
            addedSet.insert(file);
        }
    }
}

result.removedFiles.assign(removedSet.begin(), removedSet.end());
result.addedFiles.assign(addedSet.begin(), addedSet.end());

return result;
}




rocksdb::Status DBManager::closeDB() {
  StopWatch sw;
  sw.start();

  if (db_) {
    cf_handles_.clear();  // Automatically deletes handles
    db_.reset();
    spdlog::debug("DB closed with Column Families.");
  }

  sw.stop();
  spdlog::critical("closeDB took {} µs.", sw.elapsedMicros());
  return rocksdb::Status::OK();
}

bool DBManager::ScanFileForValue(const std::string& filename,
                                 const std::string& value) {
  StopWatch sw;

  rocksdb::Options options;
  options.env = rocksdb::Env::Default();

  rocksdb::SstFileReader reader(options);
  rocksdb::Status status = reader.Open(filename);
  if (!status.ok()) {
    throw std::runtime_error("Failed to open SSTable: " + status.ToString());
  }

  rocksdb::ReadOptions readOptions;
  readOptions.fill_cache = false;
  readOptions.verify_checksums = true;

  auto iter =
      std::unique_ptr<rocksdb::Iterator>(reader.NewIterator(readOptions));

  sw.start();

  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    if (iter->value().ToString() == value) {
      sw.stop();
      spdlog::critical("ScanFileForValue({}) found value. Took {} µs.",
                       filename, sw.elapsedMicros());
      return true;
    }
  }

  sw.stop();
  spdlog::critical("ScanFileForValue({}) did not find value. Took {} µs.",
                   filename, sw.elapsedMicros());
  return false;
}

bool DBManager::noBloomcheckValueInColumn(const std::string& column,
                                          const std::string& value) {
  StopWatch sw;

  if (!db_) {
    throw std::runtime_error("DB not open.");
  }

  auto cf_it = cf_handles_.find(column);
  if (cf_it == cf_handles_.end()) {
    throw std::runtime_error("Column Family not found: " + column);
  }

  rocksdb::ReadOptions readOptions;
  readOptions.fill_cache = false;

  auto iter = std::unique_ptr<rocksdb::Iterator>(
      db_->NewIterator(readOptions, cf_it->second.get()));

  sw.start();
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    if (iter->value().ToString() == value) {
      sw.stop();
      // spdlog::info("Found '{}...' in column '{}' in {} µs.", value.substr(0,
      // 30), column, sw.elapsedMicros());
      return true;
    }
  }

  sw.stop();
  spdlog::info("Did NOT find '{}...' in column '{}' after {} µs.",
               value.substr(0, 30), column, sw.elapsedMicros());
  return false;
}

std::vector<std::string> DBManager::scanForRecordsInColumns(
    const std::vector<std::string>& columns,
    const std::vector<std::string>& values) {
  if (columns.size() != values.size() || columns.empty()) {
    throw std::runtime_error(
        "Number of columns and values must be equal and non-empty.");
  }

  StopWatch sw;
  sw.start();

  std::vector<std::string> matchingKeys;

  // Use the first column as the base for scanning.
  auto baseIt = cf_handles_.find(columns[0]);
  if (baseIt == cf_handles_.end()) {
    throw std::runtime_error("Column Family not found for base column: " +
                             columns[0]);
  }

  rocksdb::ReadOptions readOptions;
  readOptions.fill_cache = false;

  // Create an iterator for the base column and scan the entire key range.
  std::unique_ptr<rocksdb::Iterator> iter(
      db_->NewIterator(readOptions, baseIt->second.get()));
  iter->SeekToFirst();

  for (; iter->Valid(); iter->Next()) {
    std::string key = iter->key().ToString();
    bool allMatch = true;
    // For each column, get the value associated with the same key.
    for (size_t i = 0; i < columns.size(); ++i) {
      auto cfIt = cf_handles_.find(columns[i]);
      if (cfIt == cf_handles_.end()) {
        allMatch = false;
        break;
      }
      std::string candidateValue;
      auto status =
          db_->Get(readOptions, cfIt->second.get(), key, &candidateValue);
      if (!status.ok() || candidateValue != values[i]) {
        allMatch = false;
        break;
      }
    }
    if (allMatch) {
      matchingKeys.push_back(key);
    }
  }

  sw.stop();
  spdlog::info(
      "Scanned entire DB for {} columns in {} µs, found {} matching keys.",
      columns.size(), sw.elapsedMicros(), matchingKeys.size());

  return matchingKeys;
}

std::vector<std::string> DBManager::scanFileForKeysWithValue(
    const std::string& filename, const std::string& value,
    const std::string& rangeStart, const std::string& rangeEnd) {
  std::vector<std::string> matchingKeys;
  rocksdb::Options options;
  options.env = rocksdb::Env::Default();

  rocksdb::SstFileReader reader(options);
  auto status = reader.Open(filename);
  if (!status.ok()) {
    spdlog::error("Failed to open SSTable '{}': {}", filename,
                  status.ToString());
    return {};
  }

  rocksdb::ReadOptions readOptions;
  readOptions.fill_cache = false;

  auto iter =
      std::unique_ptr<rocksdb::Iterator>(reader.NewIterator(readOptions));
  if (!rangeStart.empty()) {
    iter->Seek(rangeStart);
  } else {
    iter->SeekToFirst();
  }

  while (iter->Valid()) {
    std::string currentKey = iter->key().ToString();
    if (!rangeEnd.empty() && currentKey > rangeEnd) break;

    if (iter->value().ToString() == value) {
      matchingKeys.push_back(currentKey);
    }
    iter->Next();
  }

  return matchingKeys;
}

bool DBManager::findRecordInHierarchy(BloomTree& hierarchy,
                                      const std::string& value,
                                      const std::string& startKey,
                                      const std::string& endKey) {
  StopWatch sw;
  sw.start();

  auto candidates = hierarchy.query(value, startKey, endKey);
  if (candidates.empty()) {
    spdlog::info("No candidates found in the hierarchy for '{}'.", value);
    return false;
  }

  std::vector<std::future<bool>> futures;
  std::atomic<bool> found{false};

  for (const auto& candidate : candidates) {
    spdlog::info("Checking candidate: {} ", candidate);
    futures.emplace_back(
        std::async(std::launch::async, [this, &candidate, &value, &found]() {
          if (found.load()) {
            return false;
          }
          bool result = ScanFileForValue(candidate, value);
          if (result) {
            found.store(true);
          }
          return result;
        }));
  }

  while (!futures.empty()) {
    for (auto it = futures.begin(); it != futures.end();) {
      if (it->wait_for(std::chrono::milliseconds(0)) ==
          std::future_status::ready) {
        if (it->get()) {
          spdlog::debug("Value truly found in one of the files.");
          sw.stop();
          spdlog::critical("checkValueInHierarchy took {} µs.",
                           sw.elapsedMicros());
          return true;
        }
        it = futures.erase(it);
      } else {
        ++it;
      }
    }
  }

  sw.stop();
  spdlog::info("No matching record found for '{}' after {} µs.", value,
               sw.elapsedMicros());
  return false;
}


std::vector<std::string> DBManager::scanSSTForRanges(
    const std::string& filename,
    const std::string& value,
    const std::vector<SSTRange>& ranges)
{
    std::vector<std::string> matchingKeys;

    rocksdb::Options options;
    rocksdb::SstFileReader reader(options);

    auto status = reader.Open(filename);

    if (!status.ok()) {
        throw std::runtime_error(
            "Failed to open SSTable '" + filename +
            "': " + status.ToString());
    }

    rocksdb::ReadOptions readOptions;
    readOptions.fill_cache = false;

    std::unique_ptr<rocksdb::Iterator> iter(
        reader.NewIterator(readOptions));

    const rocksdb::Slice targetValue(value);

    for (const auto& range : ranges) {
        iter->Seek(range.startKey);

        while (iter->Valid()) {
            const rocksdb::Slice key = iter->key();

            if (options.comparator->Compare(
                    key, rocksdb::Slice(range.endKey)) > 0) {
                break;
            }

            if (iter->value().compare(targetValue) == 0) {
                matchingKeys.emplace_back(key.data(), key.size());
            }

            iter->Next();
        }

        if (!iter->status().ok()) {
            throw std::runtime_error(
                "Error scanning SSTable '" + filename +
                "': " + iter->status().ToString());
        }
    }

    return matchingKeys;
}


std::vector<std::string> DBManager::findUsingSingleHierarchy(
    BloomTree& hierarchy,
    const std::vector<std::string>& columns,
    const std::vector<std::string>& values)
{
    if (columns.size() != values.size() || columns.empty()) {
        throw std::runtime_error(
            "Number of columns and values must be equal and non-empty.");
    }

    StopWatch sw;
    sw.start();

    // --------------------------------------------------
    // 1. Wyszukanie kandydatów w BloomTree
    // --------------------------------------------------

    std::vector<const Node*> candidates =
        hierarchy.queryNodes(values[0], "", "");

    if (candidates.empty()) {
        spdlog::info(
            "No candidates found in the hierarchy for '{}'.",
            values[0]);

        return {};
    }

    spdlog::info(
        "Number of candidate leaves: {}",
        candidates.size());

    // --------------------------------------------------
    // 2. Grupowanie liści według plików SST
    // --------------------------------------------------

    std::map<std::string, std::vector<SSTRange>> sstRanges;

    for (const Node* node : candidates) {

        sstRanges[node->filename].push_back({
            node->startKey,
            node->endKey
        });
    }

    spdlog::info(
        "Number of unique SST files to scan: {}",
        sstRanges.size());

    // --------------------------------------------------
    // 3. Skanowanie każdego SST tylko raz
    // --------------------------------------------------

    std::vector<std::string> allKeys;

    for (const auto& [filename, ranges] : sstRanges) {

        ++gSSTCheckCount;

        auto keys = scanSSTForRanges(
            filename,
            values[0],
            ranges);

        allKeys.insert(
            allKeys.end(),
            keys.begin(),
            keys.end());
    }

    // --------------------------------------------------
    // 4. Usuwanie duplikatów
    // --------------------------------------------------

    std::sort(allKeys.begin(), allKeys.end());

    allKeys.erase(
        std::unique(allKeys.begin(), allKeys.end()),
        allKeys.end());

    spdlog::info(
        "Number of unique keys found in SST files: {}",
        allKeys.size());

    // --------------------------------------------------
    // 5. Weryfikacja kluczy we wszystkich kolumnach
    // --------------------------------------------------

    std::vector<std::string> matchingKeys;

    for (const auto& key : allKeys) {

        bool matches = true;

        for (size_t i = 0; i < columns.size(); ++i) {

            auto cfIt = cf_handles_.find(columns[i]);

            if (cfIt == cf_handles_.end()) {
                throw std::runtime_error(
                    "Column Family not found: " + columns[i]);
            }

            std::string dbValue;

            rocksdb::ReadOptions readOptions;

          auto status = db_->Get(
              readOptions,
              cfIt->second.get(),
              key,
              &dbValue);

            if (!status.ok() || dbValue != values[i]) {

                matches = false;
                break;
            }
        }

        if (matches) {
            matchingKeys.push_back(key);
        }
    }

    // --------------------------------------------------
// 6. Podsumowanie
// --------------------------------------------------

sw.stop();

spdlog::info(
    "findUsingSingleHierarchy completed. "
    "Candidates: {}, SST files: {}, "
    "Bloom checks: {}, Leaf Bloom checks: {}, "
    "SST checks: {}, Unique keys: {}, Matching keys: {}, "
    "Time: {} us",
    candidates.size(),
    sstRanges.size(),
    gBloomCheckCount,
    gLeafBloomCheckCount,
    gSSTCheckCount,
    allKeys.size(),
    matchingKeys.size(),
    sw.elapsedMicros());

return matchingKeys;
}

std::string DBManager::getValue(const std::string& column_family_name,
                                const std::string& key) {
  rocksdb::PinnableSlice value;
  rocksdb::ColumnFamilyHandle* cf_handle =
      getColumnFamilyHandle(column_family_name);
  if (!cf_handle) {
    throw std::runtime_error("Column family not found: " + column_family_name);
  }
  rocksdb::ReadOptions read_options;
  rocksdb::Status status =
      db_->Get(read_options, cf_handle, rocksdb::Slice(key), &value);
  if (status.ok()) {
    return value.ToString();
  }
  return "";  // Or throw an exception / return status
}

rocksdb::ColumnFamilyHandle* DBManager::getColumnFamilyHandle(
    const std::string& column_family_name) {
  auto it = cf_handles_.find(column_family_name);
  if (it != cf_handles_.end()) {
    return it->second.get();
  }
  return nullptr;  // Or throw an exception
}

rocksdb::Status DBManager::applyModifications(
    const std::vector<std::tuple<std::string, std::string, std::string>>&
        modifications, size_t numRecords) {
  if (!db_) return rocksdb::Status::InvalidArgument("DB not open");

  rocksdb::WriteOptions write_options;
  for (const auto& mod : modifications) {
    const std::string& key = std::get<0>(mod);
    const std::string& column_name = std::get<1>(mod);
    const std::string& value = std::get<2>(mod);

    auto handle = cf_handles_.at(column_name).get();
    if (!handle) {
      spdlog::error(
          "ApplyModifications: Column family '{}' not found for key '{}'. "
          "Skipping.",
          column_name, key);
      continue;
    }
    rocksdb::Status s = db_->Put(write_options, handle, key, value);
    if (!s.ok()) {
      spdlog::error(
          "ApplyModifications: Failed to Put key '{}' in column '{}': {}", key,
          column_name, s.ToString());
      return s;
    }
  }
  compactAllColumnFamilies(numRecords);
  return rocksdb::Status::OK();
}

rocksdb::Status DBManager::revertModifications(
    const std::vector<std::tuple<std::string, std::string, std::string>>&
        reversions, size_t numRecords) {
  if (!db_) return rocksdb::Status::InvalidArgument("DB not open");

  rocksdb::WriteOptions write_options;
  for (const auto& rev : reversions) {
    const std::string& key = std::get<0>(rev);
    const std::string& column_name = std::get<1>(rev);
    const std::string& value = std::get<2>(rev);

    auto handle = cf_handles_.at(column_name).get();
    if (!handle) {
      spdlog::error(
          "RevertModifications: Column family '{}' not found for key '{}'. "
          "Skipping.",
          column_name, key);
      continue;
    }
    rocksdb::Status s = db_->Put(write_options, handle, key, value);
    if (!s.ok()) {
      spdlog::error(
          "RevertModifications: Failed to Put key '{}' in column '{}': {}", key,
          column_name, s.ToString());
      return s;
    }
  }
  compactAllColumnFamilies(numRecords);
  return rocksdb::Status::OK();
}
