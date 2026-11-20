#pragma once

#include "experiment_types.hpp"
#include "db_manager.hpp"
#include "bloom_tree.hpp"
#include "test_params.hpp"

#include <cstddef>
#include <map>
#include <string>
#include <vector>

/**
 * @brief Runs repeated scan, hash-index, and hierarchical queries and aggregates their metrics.
 *
 * @param dbManager Open database manager used to execute queries.
 * @param hierarchies BloomTree hierarchies indexed by column-family name.
 * @param columns Column families participating in each query.
 * @param dbSize Number of records used to configure expected-value generation.
 * @param numRuns Number of query repetitions.
 * @param skipDbScan Whether to skip the baseline database scan.
 * @param config Data distribution used to generate expected values.
 * @param columnSstFiles SST files indexed by column-family name.
 * @return Aggregated timing and BloomTree-check statistics.
 */
AggregatedQueryTimings runStandardQueries(
    DBManager& dbManager,
    const std::map<std::string, BloomTree>& hierarchies,
    const std::vector<std::string>& columns,
    size_t dbSize,
    int numRuns = 10,
    bool skipDbScan = false,
    const DistributionConfig& config = {},
    const std::map<std::string, std::vector<std::string>>* columnSstFiles = nullptr);

/**
 * @brief Runs repeated hierarchical queries against a caller-supplied target value set.
 */
AggregatedQueryTimings runStandardQueriesWithTarget(
    DBManager& dbManager,
    const std::map<std::string, BloomTree>& hierarchies,
    const std::vector<std::string>& columns,
    size_t dbSize,
    int numRuns,
    bool skipDbScan,
    std::vector<std::string> currentExpectedValues);

/**
 * @brief Generates and executes a mixture of positive and negative synthetic queries.
 */
std::vector<MixedQueryResult> runMixedQueriesWithCsvData(
    DBManager& dbManager,
    const std::map<std::string, BloomTree>& hierarchies,
    const std::vector<std::string>& columns,
    size_t dbSize,
    int numQueries,
    double realDataPercentage,
    const DistributionConfig& config = {},
    const std::map<std::string, std::vector<std::string>>* columnSstFiles = nullptr);