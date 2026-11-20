#pragma once

#include "experiment_types.hpp"
#include "bloom_tree.hpp"
#include "test_params.hpp"
#include "db_manager.hpp"

#include <cstddef>
#include <map>
#include <string>
#include <vector>

/**
 * @brief Generates progressively populated query patterns for the given columns.
 *
 * Each pattern marks a prefix of columns as existing and the remaining columns
 * as non-existing.
 */
std::vector<std::vector<bool>> generateDynamicPatterns(size_t numColumns);

/** @brief Logs the query patterns generated for several column counts. */
void testPatternGeneration();

/**
 * @brief Copies the BloomTree hierarchies corresponding to the requested columns.
 * @return The ordered query trees, or an empty vector if a hierarchy is missing.
 */
std::vector<BloomTree> prepareQueryTrees(
    const std::map<std::string, BloomTree>& hierarchies,
    const std::vector<std::string>& columns);

/**
 * @brief Initializes query patterns, real/false query flags, and random distributions.
 *
 * The configured seed controls both query-type shuffling and value generation.
 */
QueryGenerationContext createQueryGenerationContext(
    size_t numColumns,
    size_t dbSize,
    int numQueries,
    double realDataPercentage,
    const DistributionConfig& config);

/**
 * @brief Generates one mixed query using the prepared context and data distribution.
 */
GeneratedQuery generateMixedQuery(
    int queryIdx,
    const std::vector<std::string>& columns,
    QueryGenerationContext& context,
    const DistributionConfig& config);