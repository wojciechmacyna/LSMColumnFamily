#pragma once

#include <spdlog/spdlog.h>

#include <algorithm>
#include <atomic>
#include <boost/asio/post.hpp>
#include <boost/asio/thread_pool.hpp>
#include <functional>
#include <future>
#include <iostream>
#include <string>
#include <unordered_set>
#include <vector>
#include <random>
#include <algorithm>

#include "bloom_tree.hpp"
#include "db_manager.hpp"
#include "node.hpp"
#include "stopwatch.hpp"
#include "sst_hash_index.hpp"

extern boost::asio::thread_pool globalThreadPool;

/// Global counter of bloom‐filter lookups performed
inline std::atomic<size_t> gBloomCheckCount{0};
/// Global counter of leaf-node bloom-filter lookups performed
inline std::atomic<size_t> gLeafBloomCheckCount{0};
/// Global counter of SSTables checked
inline std::atomic<size_t> gSSTCheckCount{0};

inline std::atomic<size_t> gHashIndexOpenCount{0};
inline std::atomic<size_t> gHashLookupCount{0};
inline std::atomic<size_t> gHashCandidateCount{0};


inline std::atomic<size_t> gHashVerificationCount{0};

// Combination of nodes
struct Combo {
  std::vector<Node*> nodes;  // One node per column.
  std::string rangeStart;
  std::string rangeEnd;
};

inline std::vector<std::string> globalfinalMatches;


inline void normalizeSSTRanges(std::vector<SSTRange>& ranges)
{
    if (ranges.empty()) {
        return;
    }

    std::sort(
        ranges.begin(),
        ranges.end(),
        [](const SSTRange& a, const SSTRange& b) {
            return a.startKey < b.startKey;
        });

    std::vector<SSTRange> merged;
    merged.reserve(ranges.size());

    for (const auto& range : ranges) {

        if (merged.empty() ||
            range.startKey > merged.back().endKey) {

            merged.push_back(range);
        } else {
            merged.back().endKey = std::max(
                merged.back().endKey,
                range.endKey);
        }
    }

    ranges = std::move(merged);
}



inline std::vector<std::string> finalSstScanAndIntersect(
    const std::vector<Combo>& leafCombinations,
    const std::vector<std::string>& columns, 
    const std::vector<std::string>& values,
    DBManager& dbManager)
{
    const size_t n = values.size();

    if (n == 0) {
        return {};
    }

    // One map per column:
    // filename -> all relevant ranges in this SST file.
    std::vector<std::map<std::string, std::vector<SSTRange>>>
        columnSSTRanges(n);

    // --------------------------------------------------
    // 1. Collect ranges from all leaf combinations.
    // --------------------------------------------------

    for (const Combo& combo : leafCombinations) {

        if (combo.nodes.size() != n) {
            throw std::runtime_error(
                "Invalid leaf combination.");
        }

        for (size_t i = 0; i < n; ++i) {

            const Node* leaf = combo.nodes[i];

            const std::string scanStart =
                std::max(combo.rangeStart, leaf->startKey);

            const std::string scanEnd =
                std::min(combo.rangeEnd, leaf->endKey);

            if (scanStart > scanEnd) {
                continue;
            }

            columnSSTRanges[i][leaf->filename].push_back({
                scanStart,
                scanEnd
            });
        }
    }

    // --------------------------------------------------
    // 2. Normalize ranges for every SST file.
    // --------------------------------------------------

    for (size_t i = 0; i < n; ++i) {

        for (auto& [filename, ranges] : columnSSTRanges[i]) {
            normalizeSSTRanges(ranges);
        }
    }

    // --------------------------------------------------
    // 3. Scan unique SST files in parallel.
    // --------------------------------------------------

    std::vector<std::unordered_set<std::string>>
        columnKeySets(n);

    for (size_t i = 0; i < n; ++i) {

        using ScanResult = std::vector<std::string>;

        std::vector<std::future<ScanResult>> futures;

        futures.reserve(columnSSTRanges[i].size());

        for (const auto& [filename, ranges] :
             columnSSTRanges[i]) {

            auto promise =
                std::make_shared<std::promise<ScanResult>>();

            futures.push_back(promise->get_future());

            // One task per unique SST file in this column.
            ++gSSTCheckCount;

            boost::asio::post(
                globalThreadPool,
                [filename, ranges, value = values[i],
                 &dbManager, promise]() {

                    try {
                        auto keys = dbManager.scanSSTForRanges(
                            filename,
                            value,
                            ranges);

                        promise->set_value(std::move(keys));

                    } catch (...) {
                        promise->set_exception(
                            std::current_exception());
                    }
                });
        }

        // Collect results for the current column.
        for (auto& future : futures) {

            auto keys = future.get();

            for (auto& key : keys) {
                columnKeySets[i].insert(std::move(key));
            }
        }

        // No point scanning further columns if one is empty.
        if (columnKeySets[i].empty()) {
            return {};
        }
    }

    // --------------------------------------------------
    // 4. Intersect results from all columns.
    // --------------------------------------------------

    std::unordered_set<std::string> result =
        std::move(columnKeySets[0]);

    for (size_t i = 1; i < n; ++i) {

        std::unordered_set<std::string> intersection;

        intersection.reserve(
            std::min(result.size(), columnKeySets[i].size()));

        for (const auto& key : result) {

            if (columnKeySets[i].find(key) !=
                columnKeySets[i].end()) {

                intersection.insert(key);
            }
        }

        result = std::move(intersection);

        if (result.empty()) {
            break;
        }
    }
   

    // 5. Verify candidates against current RocksDB values.
    std::vector<std::string> verifiedResults;
    verifiedResults.reserve(result.size());

    for (const auto& key : result) {

    bool valid = true;

    for (size_t i = 0; i < n; ++i) {

        std::string currentValue =
            dbManager.getValue(columns[i], key);

        if (currentValue != values[i]) {
            valid = false;
            break;
        }
    }

    if (valid) {
        verifiedResults.push_back(key);
    }
}

return verifiedResults;
}



inline std::vector<std::string> finalSstHashIndexAndIntersect(
    const std::vector<Combo>& leafCombinations,
    const std::vector<std::string>& columns,
    const std::vector<std::string>& values,
    DBManager& dbManager)
{
    const size_t n = values.size();

    if (n == 0 || columns.size() != n) {
        return {};
    }

    // One map per column:
    // SST filename -> relevant key ranges.
    std::vector<std::map<std::string, std::vector<SSTRange>>>
        columnSSTRanges(n);

    // 1. Collect SST files and key ranges selected by BloomTree.
    for (const Combo& combo : leafCombinations) {
        if (combo.nodes.size() != n) {
            throw std::runtime_error("Invalid leaf combination.");
        }

        for (size_t i = 0; i < n; ++i) {
            const Node* leaf = combo.nodes[i];

            if (leaf == nullptr) {
                continue;
            }

            const std::string scanStart =
                std::max(combo.rangeStart, leaf->startKey);

            const std::string scanEnd =
                std::min(combo.rangeEnd, leaf->endKey);

            if (scanStart > scanEnd) {
                continue;
            }

            columnSSTRanges[i][leaf->filename].push_back({
                scanStart,
                scanEnd
            });
        }
    }

    // 2. Normalize ranges for each SST file.
    for (size_t i = 0; i < n; ++i) {
        for (auto& [filename, ranges] : columnSSTRanges[i]) {
            normalizeSSTRanges(ranges);
        }
    }

    // 3. Search only hash indexes belonging to selected SST files.
    std::vector<std::unordered_set<std::string>> columnKeySets(n);

    size_t totalPostingKeys = 0;
    size_t totalRangeChecks = 0;
    size_t totalSelectedKeys = 0;

    for (size_t i = 0; i < n; ++i) {
        for (const auto& [filename, ranges] : columnSSTRanges[i]) {

            const std::string indexPath =
                filename + "." + columns[i] + ".hidx";

            SSTHashIndex index;

            if (!index.open(indexPath)) {
                throw std::runtime_error(
                    "Cannot open hash index: " + indexPath);
            }

            ++gHashIndexOpenCount;
            ++gHashLookupCount;
            ++gSSTCheckCount;

            std::vector<std::string> keys;

            if (!index.lookup(values[i], keys)) {
                throw std::runtime_error(
                    "Hash index lookup failed: " + indexPath);
            }

            totalPostingKeys += keys.size();

            // Keep only keys belonging to BloomTree-selected ranges.
            for (const auto& key : keys) {
            for (const auto& range : ranges) {
                ++totalRangeChecks;

                if (key >= range.startKey && key <= range.endKey) {
                    if (columnKeySets[i].insert(key).second) {
                        ++totalSelectedKeys;
                    }
                    break;
                }
            }
        }
        }

        // No point searching further columns if one is empty.
        if (columnKeySets[i].empty()) {
            spdlog::info(
                "HashMulti diagnostics (early return): "
                "posting keys={}, range checks={}, selected keys={}, "
                "empty column index={}",
                totalPostingKeys,
                totalRangeChecks,
                totalSelectedKeys,
                i);

            gHashCandidateCount = 0;
            gHashVerificationCount = 0;
            return {};
}
    }

    // 4. Intersect candidate keys from all columns.
    std::unordered_set<std::string> result =
        std::move(columnKeySets[0]);

    for (size_t i = 1; i < n; ++i) {
        std::unordered_set<std::string> intersection;

        intersection.reserve(
            std::min(result.size(), columnKeySets[i].size()));

        for (const auto& key : result) {
            if (columnKeySets[i].find(key) !=
                columnKeySets[i].end()) {
                intersection.insert(key);
            }
        }

        result = std::move(intersection);

        if (result.empty()) {
            break;
        }
    }

    gHashCandidateCount = result.size();

    // 5. Verify candidates against current RocksDB values.
    std::vector<std::string> verifiedResults;
    verifiedResults.reserve(result.size());

    gHashVerificationCount = result.size();

    for (const auto& key : result) {
        bool valid = true;

        for (size_t i = 0; i < n; ++i) {
            const std::string currentValue =
                dbManager.getValue(columns[i], key);

            if (currentValue != values[i]) {
                valid = false;
                break;
            }
        }

        if (valid) {
            verifiedResults.push_back(key);
        }
    }

    std::sort(verifiedResults.begin(), verifiedResults.end());

    return verifiedResults;
}


inline void dfsMultiColumn(
    const std::vector<std::string>& values,
    Combo currentCombo,
    DBManager& dbManager,
    bool isInitialCall,
    std::vector<Combo>& leafCombinations)
{
    const size_t n = currentCombo.nodes.size();

    if (n != values.size()) {
        throw std::runtime_error(
            "Number of nodes and values must be equal.");
    }

    // 1. Check root Bloom Filters.

    if (isInitialCall) {
        for (size_t i = 0; i < n; ++i) {
            Node* node = currentCombo.nodes[i];

            ++gBloomCheckCount;

            if (node->isLeaf()) {
                ++gLeafBloomCheckCount;
            }

            if (!node->bloom.exists(values[i])) {
                return;
            }
    }
}

    // 2. Check common key range.
    if (currentCombo.rangeStart > currentCombo.rangeEnd) {
        return;
    }

    // 3. Check whether all selected nodes are leaves.
    const bool allLeaves = std::all_of(
        currentCombo.nodes.begin(),
        currentCombo.nodes.end(),
        [](const Node* node) {
            return node->isLeaf();
        });

    if (allLeaves) {
        leafCombinations.push_back(std::move(currentCombo));
        return;
    }

    // 4. Build candidate options with progressive range pruning.
    std::vector<std::vector<Node*>> candidateOptions(n);

    std::string tightStart = currentCombo.rangeStart;
    std::string tightEnd = currentCombo.rangeEnd;

    for (size_t i = 0; i < n; ++i) {
        Node* node = currentCombo.nodes[i];

        std::string colMin;
        std::string colMax;

        bool found = false;

        auto consider = [&](Node* candidate) {

            // Reject candidates outside the current common range.
            if (candidate->endKey < tightStart ||
                candidate->startKey > tightEnd) {
                return;
            }

            ++gBloomCheckCount;

            if (candidate->isLeaf()) {
                ++gLeafBloomCheckCount;
            }

            if (!candidate->bloom.exists(values[i])) {
                return;
            }

            candidateOptions[i].push_back(candidate);

            if (!found) {
                colMin = candidate->startKey;
                colMax = candidate->endKey;
                found = true;
            } else {
                colMin = std::min(colMin, candidate->startKey);
                colMax = std::max(colMax, candidate->endKey);
            }
        };

        if (!node->isLeaf()) {
            for (Node* child : node->children) {
                consider(child);
            }
        } else {
            consider(node);
        }

        if (!found) {
            return;
        }

        // Tighten the common range before processing the next column.
        if (i + 1 < n) {
            tightStart = std::max(tightStart, colMin);
            tightEnd = std::min(tightEnd, colMax);

            if (tightStart > tightEnd) {
                return;
            }
        }
    }

    // 5. Generate combinations using backtracking.
    std::function<void(
        size_t,
        std::vector<Node*>&,
        const std::string&,
        const std::string&)> backtrack;

    backtrack = [&](
        size_t idx,
        std::vector<Node*>& chosen,
        const std::string& curStart,
        const std::string& curEnd)
    {
        if (idx == n) {
            Combo next{
                chosen,
                curStart,
                curEnd
            };

            dfsMultiColumn(
                values,
                std::move(next),
                dbManager,
                false,
                leafCombinations);

            return;
        }

        for (Node* candidate : candidateOptions[idx]) {

            const std::string newStart =
                std::max(curStart, candidate->startKey);

            const std::string newEnd =
                std::min(curEnd, candidate->endKey);

            if (newStart <= newEnd) {
                chosen[idx] = candidate;

                backtrack(
                    idx + 1,
                    chosen,
                    newStart,
                    newEnd);
            }
        }
    };

    std::vector<Node*> chosen(n, nullptr);

    backtrack(
        0,
        chosen,
        currentCombo.rangeStart,
        currentCombo.rangeEnd);
}




// Multi-column hierarchical query interface.

inline std::vector<std::string> multiColumnQueryHierarchical(
    std::vector<BloomTree>& trees,
    const std::vector<std::string>& columns,
    const std::vector<std::string>& values,
    const std::string& globalStart,
    const std::string& globalEnd,
    DBManager& dbManager)

{
    StopWatch sw;
    sw.start();

    const size_t n = trees.size();


    if (n == 0 || n != values.size() || n != columns.size()) {
    std::cerr << "Error: Number of trees, columns and values must match and be non-empty.\n";
    sw.stop();
    return {};
}

    // Check roots
    for (const auto& tree : trees) {
        if (tree.root == nullptr) {
            sw.stop();
            return {};
        }
    }

    Combo start;
    start.nodes.resize(n);

    std::string s = globalStart.empty()
        ? trees[0].root->startKey
        : globalStart;

    std::string e = globalEnd.empty()
        ? trees[0].root->endKey
        : globalEnd;

    for (size_t i = 0; i < n; ++i) {
        start.nodes[i] = trees[i].root;

        s = std::max(s, trees[i].root->startKey);
        e = std::min(e, trees[i].root->endKey);
    }

    if (s > e) {
        sw.stop();
        return {};
    }

    start.rangeStart = s;
    start.rangeEnd = e;

    // Phase 1: Traverse BloomTree hierarchies.
    std::vector<Combo> leafCombinations;

    dfsMultiColumn(
        values,
        std::move(start),
        dbManager,
        true,
        leafCombinations);

    // Phase 2: Scan unique SST files and intersect results.
    std::vector<std::string> matches =
        finalSstScanAndIntersect(
            leafCombinations,
            columns,
            values,
            dbManager);

    sw.stop();

    spdlog::critical(
        "Multi-column query took {} µs, found matching {} keys.",
        sw.elapsedMicros(),
        matches.size());

    spdlog::info(
        "Bloom filters checked: {} (total), {} (leaves only), SSTables checked: {}",
        gBloomCheckCount.load(),
        gLeafBloomCheckCount.load(),
        gSSTCheckCount.load());

    return matches;
}




inline std::vector<std::string> multiColumnQueryHierarchicalHashIndex(
    std::vector<BloomTree>& trees,
    const std::vector<std::string>& columns,
    const std::vector<std::string>& values,
    const std::string& globalStart,
    const std::string& globalEnd,
    DBManager& dbManager)
{
    StopWatch sw;
    sw.start();

    const size_t n = trees.size();

    if (n == 0 || n != values.size() || n != columns.size()) {
        std::cerr
            << "Error: Number of trees, columns and values must match "
               "and be non-empty.\n";
        sw.stop();
        return {};
    }

    // Check roots.
    for (const auto& tree : trees) {
        if (tree.root == nullptr) {
            sw.stop();
            return {};
        }
    }

    Combo start;
    start.nodes.resize(n);

    std::string s = globalStart.empty()
        ? trees[0].root->startKey
        : globalStart;

    std::string e = globalEnd.empty()
        ? trees[0].root->endKey
        : globalEnd;

    for (size_t i = 0; i < n; ++i) {
        start.nodes[i] = trees[i].root;

        s = std::max(s, trees[i].root->startKey);
        e = std::min(e, trees[i].root->endKey);
    }

    if (s > e) {
        sw.stop();
        return {};
    }

    start.rangeStart = s;
    start.rangeEnd = e;

    // Phase 1: Traverse BloomTree hierarchies.
    std::vector<Combo> leafCombinations;

    dfsMultiColumn(
        values,
        std::move(start),
        dbManager,
        true,
        leafCombinations);

    // Phase 2: Search hash indexes belonging to selected SST files.
    std::vector<std::string> matches =
        finalSstHashIndexAndIntersect(
            leafCombinations,
            columns,
            values,
            dbManager);

    sw.stop();

    spdlog::critical(
        "Multi-column hierarchical hash-index query took {} us, "
        "found {} matching keys.",
        sw.elapsedMicros(),
        matches.size());

    spdlog::info(
        "Bloom filters checked: {} (total), {} (leaves only), "
        "SSTables checked: {}, hash indexes opened: {}, "
        "hash lookups: {}, candidates: {}, verifications: {}",
        gBloomCheckCount.load(),
        gLeafBloomCheckCount.load(),
        gSSTCheckCount.load(),
        gHashIndexOpenCount.load(),
        gHashLookupCount.load(),
        gHashCandidateCount.load(),
        gHashVerificationCount.load());

    return matches;
}



inline std::discrete_distribution<size_t> zipfDistribution (const double zipfExponent, const size_t maxZipfCardinality, const size_t numRecords){

  std::vector<double> weights;
  std::discrete_distribution<size_t> zipfDist;

  if (numRecords > 0) {
    const size_t cardinality = std::min(
        static_cast<size_t>(numRecords),
        maxZipfCardinality);

    weights.reserve(cardinality);

    for (size_t rank = 1; rank <= cardinality; ++rank) {
      weights.push_back(
          1.0 / std::pow(static_cast<double>(rank), zipfExponent));
    }

    zipfDist = std::discrete_distribution<size_t>(
        weights.begin(), weights.end());
  }

  return zipfDist;
}

inline std::discrete_distribution<size_t> mixedZipfDistribution(
    const double zipfExponent,
    const size_t maxZipfCardinality,
    const size_t numRecords,
    const double zipfRatio) {

    std::vector<double> weights;
    std::discrete_distribution<size_t> mixedDist;

    if (numRecords == 0 || maxZipfCardinality == 0)
        return mixedDist;

    const size_t cardinality = std::min(
        static_cast<size_t>(numRecords),
        maxZipfCardinality);

    if (cardinality == 0)
        return mixedDist;

    const double ratio = std::clamp(zipfRatio, 0.0, 1.0);

    std::vector<double> zipfWeights;
    zipfWeights.reserve(cardinality);

    double zipfWeightSum = 0.0;

    for (size_t rank = 1; rank <= cardinality; ++rank) {
        const double weight =
            1.0 / std::pow(
                static_cast<double>(rank),
                zipfExponent);

        zipfWeights.push_back(weight);
        zipfWeightSum += weight;
    }

    weights.reserve(cardinality);

    for (size_t rank = 0; rank < cardinality; ++rank) {

        const double uniformProbability =
            1.0 / static_cast<double>(cardinality);

        const double zipfProbability =
            zipfWeights[rank] / zipfWeightSum;

        weights.push_back(
            (1.0 - ratio) * uniformProbability +
            ratio * zipfProbability);
    }

    mixedDist = std::discrete_distribution<size_t>(
        weights.begin(), weights.end());

    return mixedDist;
}