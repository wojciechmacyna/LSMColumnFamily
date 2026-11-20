#include "query_generation.hpp"

#include <spdlog/spdlog.h>

#include <algorithm>
#include <cmath>
#include <random>
#include <string>
#include <vector>

std::vector<std::vector<bool>> generateDynamicPatterns(size_t numColumns) {
  std::vector<std::vector<bool>> patterns;

  for (size_t numExisting = 0; numExisting <= numColumns; ++numExisting) {
    std::vector<bool> pattern(numColumns, false);  // Start with all n (false)

    // Set first numExisting columns to true (existing values)
    for (size_t i = 0; i < numExisting; ++i) {
      pattern[i] = true;
    }

    patterns.push_back(pattern);
  }

  return patterns;
}

void testPatternGeneration() {
  spdlog::info("Testing pattern generation:");

  for (size_t numCols = 2; numCols <= 5; ++numCols) {
    spdlog::info("For {} columns:", numCols);
    auto patterns = generateDynamicPatterns(numCols);

    for (size_t i = 0; i < patterns.size(); ++i) {
      std::string patternStr = "[";
      for (size_t j = 0; j < patterns[i].size(); ++j) {
        patternStr += (patterns[i][j] ? "t" : "n");
        if (j < patterns[i].size() - 1) patternStr += ",";
      }
      patternStr += "]";

      size_t existingCount = 0;
      for (bool val : patterns[i]) {
        if (val) existingCount++;
      }
      double percentage = static_cast<double>(existingCount) / numCols * 100.0;

      spdlog::info("  Pattern {}: {} ({}% existing)", i, patternStr,
                   percentage);
    }
  }
}

std::vector<BloomTree> prepareQueryTrees(
    const std::map<std::string, BloomTree>& hierarchies,
    const std::vector<std::string>& columns)
{
    std::vector<BloomTree> queryTrees;

    if (hierarchies.empty() || columns.empty()) {
        spdlog::warn(
            "prepareQueryTrees: Hierarchies or columns are empty.");
        return queryTrees;
    }

    queryTrees.reserve(columns.size());

    for (const auto& column : columns) {
        auto it = hierarchies.find(column);

        if (it == hierarchies.end()) {
            spdlog::error(
                "prepareQueryTrees: Hierarchy for column '{}' not found.",
                column);
            return {};
        }

        queryTrees.push_back(it->second);
    }

    return queryTrees;
}

QueryGenerationContext createQueryGenerationContext(
    size_t numColumns,
    size_t dbSize,
    int numQueries,
    double realDataPercentage,
    const DistributionConfig& config)
{
    QueryGenerationContext context;

    const int numRealQueries = static_cast<int>(
        std::round(numQueries * realDataPercentage / 100.0));

    const int numFalseQueries = numQueries - numRealQueries;

    spdlog::info(
        "Running {} queries: {} real ({}%), {} false ({}%).",
        numQueries,
        numRealQueries,
        realDataPercentage,
        numFalseQueries,
        100.0 - realDataPercentage);

    context.patterns = generateDynamicPatterns(numColumns);

    spdlog::info(
        "Generated {} permutation patterns for {} columns.",
        context.patterns.size(),
        numColumns);

    context.isRealDataQuery.assign(numQueries, false);

    for (int i = 0; i < numRealQueries; ++i) {
        context.isRealDataQuery[i] = true;
    }

    std::mt19937_64 shuffleRng(config.seed);

    std::shuffle(
        context.isRealDataQuery.begin(),
        context.isRealDataQuery.end(),
        shuffleRng);

    const size_t cardinality =
        config.type == DataDistribution::Normal
            ? dbSize
            : std::max<size_t>(
                  1,
                  std::min(config.cardinality, dbSize));

    context.queryRng.seed(config.seed + 1);

    context.uniformDist =
        std::uniform_int_distribution<size_t>(1, cardinality);

    if (config.type == DataDistribution::Zipfian ||
        config.type == DataDistribution::Mixed) {

        std::vector<double> weights;
        weights.reserve(cardinality);

        double zipfWeightSum = 0.0;

        for (size_t rank = 1; rank <= cardinality; ++rank) {
            const double weight =
                1.0 / std::pow(
                    static_cast<double>(rank),
                    config.exponent);

            weights.push_back(weight);
            zipfWeightSum += weight;
        }

        if (config.type == DataDistribution::Mixed) {
            const double zipfRatio =
                std::clamp(config.zipfRatio, 0.0, 1.0);

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

        context.zipfDist =
            std::discrete_distribution<size_t>(
                weights.begin(),
                weights.end());
    }

    return context;
}

GeneratedQuery generateMixedQuery(
    int queryIdx,
    const std::vector<std::string>& columns,
    QueryGenerationContext& context,
    const DistributionConfig& config)
{
    GeneratedQuery query;

    query.isRealData = context.isRealDataQuery[queryIdx];

    size_t patternIndex = 0;

    if (query.isRealData) {
        patternIndex = context.patterns.size() - 1;
    } else {
        patternIndex =
            queryIdx % (context.patterns.size() - 1);
    }

    const auto& pattern = context.patterns[patternIndex];

    const size_t randomId =
        config.type == DataDistribution::Normal
            ? context.uniformDist(context.queryRng)
            : context.zipfDist(context.queryRng) + 1;

    const std::string existingSuffix =
        "_value" + std::to_string(randomId);

    const std::string nonExistingSuffix =
        "_wrong" + std::to_string(randomId);

    const size_t existingCount =
        std::count(pattern.begin(), pattern.end(), true);

    const double percentageExisting =
        static_cast<double>(existingCount) /
        static_cast<double>(columns.size()) * 100.0;

    if (query.isRealData) {
        spdlog::info(
            "Query {}: REAL data with ID {}.",
            queryIdx + 1,
            randomId);
    } else {
        spdlog::info(
            "Query {}: FALSE data (pattern {} - {}% existing) with ID {}.",
            queryIdx + 1,
            patternIndex,
            percentageExisting,
            randomId);
    }

    query.values.reserve(columns.size());

    for (size_t i = 0; i < columns.size() && i < pattern.size(); ++i) {
        const std::string& suffix =
            pattern[i] ? existingSuffix : nonExistingSuffix;

        query.values.push_back(columns[i] + suffix);
    }

    return query;
}
