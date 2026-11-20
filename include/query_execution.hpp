#pragma once

#include "experiment_types.hpp"
#include "db_manager.hpp"
#include "bloom_tree.hpp"

#include <map>
#include <string>
#include <vector>

// Executes hash-index queries across the specified column families.
void runHashIndexQueries(
    DBManager& dbManager,
    const std::vector<std::string>& columns,
    const std::vector<std::string>& values,
    const std::map<std::string, std::vector<std::string>>* columnSstFiles,
    MixedQueryResult& result);

// Executes a query using a single BloomTree hierarchy.
void runHashSingleHierarchyQuery(
    DBManager& dbManager,
    BloomTree& queryTree,
    const std::vector<std::string>& columns,
    const std::vector<std::string>& values,
    MixedQueryResult& result);

// Executes queries using multiple BloomTree hierarchies.
void runHierarchicalQueries(
    DBManager& dbManager,
    std::vector<BloomTree>& queryTrees,
    const std::vector<std::string>& columns,
    const std::vector<std::string>& values,
    MixedQueryResult& result);