#pragma once

#include "experiment_types.hpp"
#include "result_aggregation.hpp"
#include "real_data.hpp"
#include "db_manager.hpp"
#include "bloom_tree.hpp"

#include <cstddef>
#include <cstdint>
#include <map>
#include <string>
#include <vector>

/**
 * @brief Creates a query context from a sampled subset of database records.
 */
RealDataQueryContext createRealDataQueryContext(
    DBManager& dbManager,
    const std::vector<std::string>& columns,
    size_t sampleSize,
    uint64_t seed);

/**
 * @brief Loads and validates a query context from sampled database records.
 */
RealDataQueryContext loadRealDataQueryContext(
    DBManager& dbManager,
    const std::vector<std::string>& columns,
    size_t sampleSize,
    uint64_t seed);

/**
 * @brief Runs the NYC Taxi real-data experiment for several positive-query ratios.
 */
void runRealDataExperiment(
    const std::string& baseDir,
    const std::string& dbName,
    size_t sampleSize,
    int numQueries);

std::vector<MixedQueryResult> runMixedQueriesWithRealData(
    DBManager& dbManager,
    const std::map<std::string, BloomTree>& hierarchies,
    const std::vector<std::string>& columns,
    RealDataQueryContext& realDataContext,
    int numQueries,
    double positiveQueryPercentage,
    const std::map<std::string, std::vector<std::string>>* columnSstFiles);