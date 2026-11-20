#include "experiment_initialization.hpp"

#include "sst_bloom_infrastructure.hpp"

#include <spdlog/spdlog.h>

#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <map>
#include <string>
#include <vector>

#include "bloom_manager.hpp"
#include "bloom_tree.hpp"
#include "db_manager.hpp"
#include "stopwatch.hpp"
#include "csv_utils.hpp"


// Initializes the database and builds all required index structures.
void initialDB(
    DBManager& dbManager,
    BloomManager& bloomManager,
    const std::vector<std::string>& columns,
    TestParams params,
    std::map<std::string, BloomTree>& hierarchies,
    std::map<std::string, std::vector<std::string>>& columnSstFiles,
    const DistributionConfig config) {

    spdlog::info(
        "ExpBloomMetrics: Starting the experiment for the database: '{}'",
        params.dbName);

    clearBloomFilterFiles(params.dbName);

    StopWatch stopwatch;

    // Measure database creation time.
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
            std::exit(1);
        }
    }

    stopwatch.stop();
    const auto dbCreationTime = stopwatch.elapsedMicros();

    // Measure BloomTree creation time.
    stopwatch.start();

    columnSstFiles =
        scanSstFilesAsync(columns, dbManager, params);

    hierarchies =
        buildHierarchies(columnSstFiles, bloomManager, params);

    stopwatch.stop();
    const auto bloomCreationTime = stopwatch.elapsedMicros();

    // Measure Hash Index creation time.
    stopwatch.start();

    const uint64_t hashIndexSize =
        buildHashIndexesAsync(columnSstFiles);

    stopwatch.stop();
    const auto hashIndexCreationTime = stopwatch.elapsedMicros();

    // Calculate total BloomTree sizes.
    size_t totalDiskBloomSize = 0;
    size_t totalMemoryBloomSize = 0;

    for (const auto& kv : hierarchies) {
        const BloomTree& tree = kv.second;

        totalDiskBloomSize += tree.diskSize();
        totalMemoryBloomSize += tree.memorySize();
    }

    // Save BloomTree and Hash Index size metrics.
    writeCsvHeader(
        "csv/exp_1_bloom_metrics.csv",
        "numRecords,bloomTreeRatio,itemsPerPartition,bloomSize,"
        "numHashFunctions,singleHierarchyLeafs,bloomDiskSize,"
        "blomMemSize, hashIndexSize");

    std::ofstream out(
        "csv/exp_1_bloom_metrics.csv",
        std::ios::app);

    if (!out) {
        spdlog::error(
            "ExpBloomMetrics: Nie udało się otworzyć pliku wynikowego!");
        return;
    }

    out << params.numRecords << ","
        << params.bloomTreeRatio << ","
        << params.itemsPerPartition << ","
        << params.bloomSize << ","
        << params.numHashFunctions << ","
        << hierarchies.at(columns[0]).leafNodes.size() << ","
        << totalDiskBloomSize << ","
        << totalMemoryBloomSize << ","
        << hashIndexSize << "\n";

    out.close();

    spdlog::info(
        "ExpBloomMetrics: Experiment for database '{}' finished.",
        params.dbName);

    // Save database and index creation time metrics.
    writeCsvHeader(
        "csv/exp_1_creation_time.csv",
        "numRecords,dbCreationTime, bloomCreationTime, "
        "hashIndexCreationTime");

    std::ofstream outExp3(
        "csv/exp_1_creation_time.csv",
        std::ios::app);

    if (!outExp3) {
        spdlog::error(
            "ExpBloomMetrics: Nie udało się otworzyć pliku wynikowego!");
        return;
    }

    outExp3 << params.numRecords << ","
            << dbCreationTime << ","
            << bloomCreationTime << ","
            << hashIndexCreationTime << "\n";

    outExp3.close();
}