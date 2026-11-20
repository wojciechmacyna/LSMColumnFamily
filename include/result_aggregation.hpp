#pragma once

#include "experiment_types.hpp"

#include <cstddef>
#include <string>
#include <vector>

/**
 * @brief Aggregates per-query measurements for real and synthetic data.
 *
 * Computes average execution times for positive, negative, and all queries.
 *
 * @param results Per-query measurements collected during the experiment.
 * @return Aggregated experiment results.
 */
RealDataExperimentResult aggregateMixedQueryResults(
    const std::vector<MixedQueryResult>& results);

/**
 * @brief Appends aggregated real-data experiment results to a CSV file.
 *
 * Creates the CSV header when the output file does not yet exist.
 *
 * @param filename Destination CSV file.
 * @param result Aggregated experiment results.
 * @param dbSize Number of database records scanned or represented.
 */
void saveRealDataExperimentResults(
    const std::string& filename,
    const RealDataExperimentResult& result,
    size_t dbSize);
