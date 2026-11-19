
#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include <random>
#include <cstddef>

class DBManager;

struct RealDataRecord {
    std::string key;
    std::vector<std::string> values;
};

struct RealDataScanResult {
    std::vector<RealDataRecord> sampledRecords;
    std::vector<uint64_t> combinationHashes;
    size_t recordsScanned = 0;
};

struct RealDataQueryContext {
    std::vector<RealDataRecord> positiveRecords;

    // Distinct values collected from the sampled records.
    std::vector<std::vector<std::string>> distinctValues;

    // Sorted hashes of all combinations in the database.
    std::vector<uint64_t> combinationHashes;

    std::mt19937_64 queryRng;

    size_t recordsScanned = 0;
};


struct GeneratedRealDataQuery {
    std::vector<std::string> values;
    bool isPositive = false;
    size_t matchingColumns = 0;
};

GeneratedRealDataQuery generatePositiveRealDataQuery(
    RealDataQueryContext& context);

bool generateNegativeRealDataQuery(
    DBManager& dbManager,
    const std::vector<std::string>& columns,
    RealDataQueryContext& context,
    GeneratedRealDataQuery& query,
    size_t changedColumns,
    size_t maxAttempts = 100);



// FNV-1a with length prefixes to distinguish different value boundaries.
inline uint64_t hashRealDataCombination(
    const std::vector<std::string>& values) {

    constexpr uint64_t offsetBasis = 14695981039346656037ULL;
    constexpr uint64_t prime = 1099511628211ULL;

    uint64_t hash = offsetBasis;

    for (const auto& value : values) {
        uint64_t length = value.size();

        for (int i = 0; i < 8; ++i) {
            hash ^= static_cast<uint8_t>(length & 0xFF);
            hash *= prime;
            length >>= 8;
        }

        for (unsigned char c : value) {
            hash ^= c;
            hash *= prime;
        }
    }

    return hash;
}
