#pragma once

#include "experiment_types.hpp"
#include "db_manager.hpp"
#include "bloom_tree.hpp"
#include "test_params.hpp"

#include <cstddef>
#include <string>
#include <vector>

std::vector<PatternQueryResult> runPatternQueriesWithCsvData(
    DBManager& dbManager,
    const std::map<std::string, BloomTree>& hierarchies,
    const std::vector<std::string>& columns,
    size_t dbSize,
    const DistributionConfig& config = {});