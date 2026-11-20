#pragma once

#include <cstdint>
#include <map>
#include <string>
#include <vector>

#include "test_params.hpp"
#include "db_manager.hpp"
#include "bloom_manager.hpp"
#include "bloom_tree.hpp"

// Remove Bloom filter files generated for a database.
void clearBloomFilterFiles(const std::string& dbDir);

// Build hash indexes for all SST files and return their total on-disk size.
uint64_t buildHashIndexesAsync(
    const std::map<std::string, std::vector<std::string>>& columnSstFiles);

// Scan SST files for the requested column families.
std::map<std::string, std::vector<std::string>> scanSstFilesAsync(
    const std::vector<std::string>& columns,
    DBManager& dbManager,
    const TestParams& params,
    const bool scanInsertedFiles = false);

// Build one BloomTree hierarchy for each column family.
std::map<std::string, BloomTree> buildHierarchies(
    const std::map<std::string, std::vector<std::string>>& columnSstFiles,
    BloomManager& bloomManager,
    const TestParams& params);

// Build BloomTree leaf nodes for the supplied SST files.
std::map<std::string, std::vector<Node*>> buildLeaves(
    const std::map<std::string, std::vector<std::string>>& columnSstFiles,
    BloomManager& bloomManager,
    const TestParams& params);

// Append newly discovered SST files to the existing BloomTree hierarchies.
void appendSstBatch(
    const std::vector<std::string>& columns,
    DBManager& dbManager,
    BloomManager& bloomManager,
    const TestParams& params,
    std::map<std::string, BloomTree>& hierarchies);
