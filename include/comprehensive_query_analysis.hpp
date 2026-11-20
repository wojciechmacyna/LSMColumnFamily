#pragma once

#include "experiment_types.hpp"
#include "db_manager.hpp"
#include "bloom_tree.hpp"
#include "test_params.hpp"

#include <cstddef>
#include <map>
#include <string>
#include <vector>

std::vector<AccumulatedQueryMetrics> runComprehensiveQueryAnalysis(
    DBManager& dbManager,
    const std::map<std::string, BloomTree>& hierarchies,
    const std::vector<std::string>& columns,
    size_t dbSize,
    int numQueriesPerScenario,
    const DistributionConfig& config = {},
    const std::map<std::string, std::vector<std::string>>* columnSstFiles = nullptr);