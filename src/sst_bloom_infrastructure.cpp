#include "sst_bloom_infrastructure.hpp"

#include <boost/asio/post.hpp>
#include <boost/asio/thread_pool.hpp>
#include <spdlog/spdlog.h>

#include <filesystem>
#include <future>
#include <regex>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "sst_hash_index.hpp"

extern boost::asio::thread_pool globalThreadPool;

void clearBloomFilterFiles(const std::string& dbDir) {
    const std::regex bloomFilePattern(R"(^\d+\.sst_[^_]+_[^_]+$)");
    std::error_code ec;

    for (const auto& entry : std::filesystem::directory_iterator(dbDir, ec)) {
        if (ec) {
            spdlog::error("Failed to iterate '{}': {}", dbDir, ec.message());
            return;
        }

        const auto& path = entry.path();
        const std::string filename = path.filename().string();

        if (entry.is_regular_file() &&
            std::regex_match(filename, bloomFilePattern)) {
            if (!std::filesystem::remove(path, ec)) {
                spdlog::error(
                    "Could not remove '{}': {}",
                    path.string(),
                    ec.message());
            }
        }
    }
}

uint64_t buildHashIndexesAsync(
    const std::map<std::string, std::vector<std::string>>& columnSstFiles) {

    // Use a single worker to build indexes sequentially.
    boost::asio::thread_pool hashIndexPool(1);
    std::vector<std::future<uint64_t>> futures;

    for (const auto& [column, sstFiles] : columnSstFiles) {
        for (const auto& filename : sstFiles) {
            std::promise<uint64_t> promise;
            futures.push_back(promise.get_future());

            const std::string indexPath = filename + "." + column + ".hidx";

            boost::asio::post(
                hashIndexPool,
                [filename, indexPath, promise = std::move(promise)]() mutable {
                    try {
                        if (!SSTHashIndex::exists(indexPath)) {
                            SSTHashIndex::build(filename, indexPath, 4096);
                        }

                        const uint64_t size =
                            std::filesystem::file_size(indexPath);
                        promise.set_value(size);
                    } catch (...) {
                        promise.set_exception(std::current_exception());
                    }
                });
        }
    }

    // Wait for all index-building tasks to finish.
    hashIndexPool.join();

    uint64_t totalSize = 0;
    for (auto& future : futures) {
        totalSize += future.get();
    }

    return totalSize;
}

std::map<std::string, std::vector<std::string>> scanSstFilesAsync(
    const std::vector<std::string>& columns,
    DBManager& dbManager,
    const TestParams& params,
    const bool scanAddedFiles) {

    std::map<std::string, std::vector<std::string>> columnSstFiles;
    std::vector<std::future<std::pair<std::string, std::vector<std::string>>>>
        scanFutures;

    for (const auto& column : columns) {
        std::promise<std::pair<std::string, std::vector<std::string>>>
            scanPromise;
        scanFutures.push_back(scanPromise.get_future());

        boost::asio::post(
            globalThreadPool,
            [column, &dbManager, &params, scanAddedFiles,
             promise = std::move(scanPromise)]() mutable {
                if (scanAddedFiles) {
                    auto sstFiles =
                        dbManager.getChangedSSTFilesForColumn(
                            params.dbName, column);
                    promise.set_value(
                        std::make_pair(column, std::move(sstFiles)));
                } else {
                    auto sstFiles =
                        dbManager.scanSSTFilesForColumn(
                            params.dbName, column);
                    promise.set_value(
                        std::make_pair(column, std::move(sstFiles)));
                }
            });
    }

    // Collect the results after all column-family scans complete.
    for (auto& future : scanFutures) {
        auto [column, sstFiles] = future.get();
        columnSstFiles[column] = std::move(sstFiles);
    }

    return columnSstFiles;
}

std::map<std::string, BloomTree> buildHierarchies(
    const std::map<std::string, std::vector<std::string>>& columnSstFiles,
    BloomManager& bloomManager,
    const TestParams& params) {

    std::map<std::string, BloomTree> hierarchies;

    for (const auto& [column, sstFiles] : columnSstFiles) {
        BloomTree hierarchy = bloomManager.createPartitionedHierarchy(
            sstFiles,
            params.itemsPerPartition,
            params.bloomSize,
            params.numHashFunctions,
            params.bloomTreeRatio);

        spdlog::info("Hierarchy built for column: {}", column);
        hierarchies.try_emplace(column, std::move(hierarchy));
    }

    return hierarchies;
}

std::map<std::string, std::vector<Node*>> buildLeaves(
    const std::map<std::string, std::vector<std::string>>& columnSstFiles,
    BloomManager& bloomManager,
    const TestParams& params) {

    std::map<std::string, std::vector<Node*>> leavesByColumn;

    for (const auto& [column, sstFiles] : columnSstFiles) {
        leavesByColumn[column] = bloomManager.createPartitionedLeaves(
            sstFiles,
            params.itemsPerPartition,
            params.bloomSize,
            params.numHashFunctions);
    }

    return leavesByColumn;
}

void appendSstBatch(
    const std::vector<std::string>& columns,
    DBManager& dbManager,
    BloomManager& bloomManager,
    const TestParams& params,
    std::map<std::string, BloomTree>& hierarchies) {

    spdlog::info("========== APPEND SST BATCH START ==========");

    // 1. Retrieve changes produced by compaction.
    std::map<std::string, SSTFileChanges> compactionChanges;

    for (const auto& column : columns) {
        auto changes = dbManager.getCompactionChangesForColumn(
            params.dbName, column);

        if (!changes.removedFiles.empty() || !changes.addedFiles.empty()) {
            spdlog::info(
                "COMPACTION CHANGES: CF={}, removed={}, added={}",
                column,
                changes.removedFiles.size(),
                changes.addedFiles.size());

            for (const auto& file : changes.removedFiles) {
                spdlog::info("  REMOVED: {}", file);
            }

            for (const auto& file : changes.addedFiles) {
                spdlog::info("  ADDED: {}", file);
            }
        }

        compactionChanges.emplace(column, std::move(changes));
    }

    // 2. Detect newly added SST files.
    auto insertedSstFiles =
        scanSstFilesAsync(columns, dbManager, params, true);

    for (const auto& [column, files] : insertedSstFiles) {
        spdlog::info(
            "NEW SST FILES: CF={}, count={}",
            column,
            files.size());

        for (const auto& filename : files) {
            spdlog::info("  {}", filename);
        }
    }

    // 3. Build Bloom filter leaves for the newly discovered SST files.
    auto insertedLeaves = buildLeaves(
        insertedSstFiles,
        bloomManager,
        params);

    // 4. Append the new leaves to the existing hierarchies.
    for (auto& [column, leaves] : insertedLeaves) {
        auto hierarchyIt = hierarchies.find(column);

        if (hierarchyIt == hierarchies.end()) {
            throw std::runtime_error(
                "Missing existing hierarchy for column: " + column);
        }

        hierarchyIt->second.appendLeaves(leaves);

        std::string error;

        if (!hierarchyIt->second.validateTree(error)) {
            throw std::runtime_error(
                "Tree validation failed for CF " + column + ": " + error);
        }

        if (!hierarchyIt->second.validateBloomFilter(
                hierarchyIt->second.root, error)) {
            throw std::runtime_error(
                "Bloom validation failed for CF " + column + ": " + error);
        }

        spdlog::info(
            "BATCH APPENDED: CF={}, leaves={}, root range=[{}, {}]",
            column,
            hierarchyIt->second.leafNodes.size(),
            hierarchyIt->second.root->startKey,
            hierarchyIt->second.root->endKey);
    }

    spdlog::info("========== APPEND SST BATCH END ==========");
}
