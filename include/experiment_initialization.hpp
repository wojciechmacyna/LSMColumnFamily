#pragma once

#include <map>
#include <string>
#include <vector>

#include "test_params.hpp"
#include "db_manager.hpp"
#include "bloom_manager.hpp"
#include "bloom_tree.hpp"

// Initializes the database, builds BloomTree and Hash Index structures,
// and records database and index creation metrics.
void initialDB(
    DBManager& dbManager,
    BloomManager& bloomManager,
    const std::vector<std::string>& columns,
    TestParams params,
    std::map<std::string, BloomTree>& hierarchies,
    std::map<std::string, std::vector<std::string>>& columnSstFiles,
    const DistributionConfig config);