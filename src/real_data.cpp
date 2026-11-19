#include "real_data.hpp"

#include "db_manager.hpp"

#include <algorithm>
#include <stdexcept>

#include <numeric>

GeneratedRealDataQuery generatePositiveRealDataQuery(
    RealDataQueryContext& context)
{
    if (context.positiveRecords.empty()) {
        throw std::runtime_error(
            "Cannot generate positive query: no sampled records");
    }

    std::uniform_int_distribution<size_t> distribution(
        0, context.positiveRecords.size() - 1);

    const auto& record =
        context.positiveRecords[distribution(context.queryRng)];

    GeneratedRealDataQuery query;
    query.values = record.values;
    query.isPositive = true;
    query.matchingColumns = query.values.size();

    return query;
}

bool generateNegativeRealDataQuery(
    DBManager& dbManager,
    const std::vector<std::string>& columns,
    RealDataQueryContext& context,
    GeneratedRealDataQuery& query,
    size_t changedColumns,
    size_t maxAttempts)
{
    if (context.positiveRecords.empty() ||
        context.distinctValues.empty()) {
        return false;
    }

    const size_t numColumns = context.distinctValues.size();

    if (changedColumns == 0 || changedColumns > numColumns) {
        return false;
    }

    if (context.positiveRecords.front().values.size() != numColumns) {
        throw std::runtime_error(
            "Inconsistent number of values in sampled records");
    }

    if (maxAttempts == 0) {
        return false;
    }

    std::uniform_int_distribution<size_t> recordDistribution(
        0, context.positiveRecords.size() - 1);

    std::vector<size_t> columnIndices(numColumns);
    std::iota(columnIndices.begin(), columnIndices.end(), 0);

    for (size_t attempt = 0; attempt < maxAttempts; ++attempt) {
        const auto& baseRecord =
            context.positiveRecords[
                recordDistribution(context.queryRng)];

        GeneratedRealDataQuery candidate;
        candidate.values = baseRecord.values;
        candidate.isPositive = false;
        candidate.matchingColumns = numColumns - changedColumns;

        // Select columns to modify.
        std::shuffle(
            columnIndices.begin(),
            columnIndices.end(),
            context.queryRng);

        bool validCandidate = true;

        for (size_t i = 0; i < changedColumns; ++i) {
            const size_t column = columnIndices[i];
            const auto& domain = context.distinctValues[column];

            if (domain.size() < 2) {
                validCandidate = false;
                break;
            }

            std::vector<size_t> alternatives;
            alternatives.reserve(domain.size());

            for (size_t j = 0; j < domain.size(); ++j) {
                if (domain[j] != baseRecord.values[column]) {
                    alternatives.push_back(j);
                }
            }

            if (alternatives.empty()) {
                validCandidate = false;
                break;
            }

            std::uniform_int_distribution<size_t> valueDistribution(
                0, alternatives.size() - 1);

            candidate.values[column] =
                domain[alternatives[valueDistribution(context.queryRng)]];
        }

        if (!validCandidate) {
            continue;
        }

        const uint64_t hash =
            hashRealDataCombination(candidate.values);

        // If the hash is absent, the combination is definitely absent.
        if (!std::binary_search(
                context.combinationHashes.begin(),
                context.combinationHashes.end(),
                hash)) {
            query = std::move(candidate);
            return true;
        }

        // Hash found: verify exact combination to handle collisions.
        const auto matchingKeys =
            dbManager.scanForRecordsInColumns(
                std::vector<std::string>{},
                candidate.values);

        if (matchingKeys.empty()) {
            query = std::move(candidate);
            return true;
        }
    }

    return false;
}