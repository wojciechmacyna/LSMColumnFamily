#include "result_aggregation.hpp"

#include <filesystem>
#include <fstream>
#include <stdexcept>

#include <spdlog/spdlog.h>

void saveRealDataExperimentResults(
    const std::string& filename,
    const RealDataExperimentResult& result,
    size_t dbSize)
{
    const bool fileExists = std::filesystem::exists(filename);

    std::ofstream csv(filename, std::ios::out | std::ios::app);

    if (!csv.is_open()) {
        throw std::runtime_error("Cannot open CSV file: " + filename);
    }

    if (!fileExists) {
        csv
            << "dbSize,realDataPercentage,"
            << "avgRealDataMultiTime,"
            << "avgRealDataSingleTime,"
            << "avgRealHashIndexTime,"
            << "avgRealHashSingleIndexTime,"
            << "avgRealHashSingleHierarchyIndexTime,"
            << "avgFalseDataMultiTime,"
            << "avgFalseDataSingleTime,"
            << "avgFalseHashIndexTime,"
            << "avgFalseHashSingleIndexTime,"
            << "avgFalseHashSingleHierarchyIndexTime,"
            << "avgHierarchicalMultiTime,"
            << "avgHierarchicalSingleTime,"
            << "avgHashIndexTime,"
            << "avgHashSingleIndexTime,"
            << "avgHashSingleHierarchyIndexTime\n";
    }

    csv
        << dbSize << ","
        << result.realDataPercentage << ","
        << result.avgRealDataMultiTime << ","
        << result.avgRealDataSingleTime << ","
        << result.avgRealHashIndexTime << ","
        << result.avgRealHashSingleIndexTime << ","
        << result.avgRealHashSingleHierarchyIndexTime << ","
        << result.avgFalseDataMultiTime << ","
        << result.avgFalseDataSingleTime << ","
        << result.avgFalseHashIndexTime << ","
        << result.avgFalseHashSingleIndexTime << ","
        << result.avgFalseHashSingleHierarchyIndexTime << ","
        << result.avgHierarchicalMultiTime << ","
        << result.avgHierarchicalSingleTime << ","
        << result.avgHashIndexTime << ","
        << result.avgHashSingleIndexTime << ","
        << result.avgHashSingleHierarchyIndexTime << "\n";

    csv.close();

    spdlog::info(
        "RealData: CSV results saved. Positive={}, Negative={}, Total={}",
        result.positiveQueries,
        result.negativeQueries,
        result.totalQueries);
}

RealDataExperimentResult aggregateMixedQueryResults(
    const std::vector<MixedQueryResult>& results)
{
    RealDataExperimentResult aggregated;

    double realMulti = 0.0;
    double realSingle = 0.0;
    double realHash = 0.0;
    double realHashSingle = 0.0;
    double realHashSingleHierarchy = 0.0;

    double falseMulti = 0.0;
    double falseSingle = 0.0;
    double falseHash = 0.0;
    double falseHashSingle = 0.0;
    double falseHashSingleHierarchy = 0.0;

    double totalMulti = 0.0;
    double totalSingle = 0.0;
    double totalHash = 0.0;
    double totalHashSingle = 0.0;
    double totalHashSingleHierarchy = 0.0;

    for (const auto& result : results) {
        ++aggregated.totalQueries;

        if (result.isRealData) {
            ++aggregated.positiveQueries;

            realMulti += result.hierarchicalMultiTime;
            realSingle += result.hierarchicalSingleTime;
            realHash += result.hashIndexTime;
            realHashSingle += result.hashSingleIndexTime;
            realHashSingleHierarchy += result.hashSingleHierarchyIndexTime;
        } else {
            ++aggregated.negativeQueries;

            falseMulti += result.hierarchicalMultiTime;
            falseSingle += result.hierarchicalSingleTime;
            falseHash += result.hashIndexTime;
            falseHashSingle += result.hashSingleIndexTime;
            falseHashSingleHierarchy += result.hashSingleHierarchyIndexTime;
        }

        totalMulti += result.hierarchicalMultiTime;
        totalSingle += result.hierarchicalSingleTime;
        totalHash += result.hashIndexTime;
        totalHashSingle += result.hashSingleIndexTime;
        totalHashSingleHierarchy += result.hashSingleHierarchyIndexTime;
    }

    if (aggregated.positiveQueries > 0) {
        aggregated.avgRealDataMultiTime =
            realMulti / aggregated.positiveQueries;
        aggregated.avgRealDataSingleTime =
            realSingle / aggregated.positiveQueries;
        aggregated.avgRealHashIndexTime =
            realHash / aggregated.positiveQueries;
        aggregated.avgRealHashSingleIndexTime =
            realHashSingle / aggregated.positiveQueries;
        aggregated.avgRealHashSingleHierarchyIndexTime =
            realHashSingleHierarchy / aggregated.positiveQueries;
    }

    if (aggregated.negativeQueries > 0) {
        aggregated.avgFalseDataMultiTime =
            falseMulti / aggregated.negativeQueries;
        aggregated.avgFalseDataSingleTime =
            falseSingle / aggregated.negativeQueries;
        aggregated.avgFalseHashIndexTime =
            falseHash / aggregated.negativeQueries;
        aggregated.avgFalseHashSingleIndexTime =
            falseHashSingle / aggregated.negativeQueries;
        aggregated.avgFalseHashSingleHierarchyIndexTime =
            falseHashSingleHierarchy / aggregated.negativeQueries;
    }

    if (aggregated.totalQueries > 0) {
        aggregated.avgHierarchicalMultiTime =
            totalMulti / aggregated.totalQueries;
        aggregated.avgHierarchicalSingleTime =
            totalSingle / aggregated.totalQueries;
        aggregated.avgHashIndexTime =
            totalHash / aggregated.totalQueries;
        aggregated.avgHashSingleIndexTime =
            totalHashSingle / aggregated.totalQueries;
        aggregated.avgHashSingleHierarchyIndexTime =
            totalHashSingleHierarchy / aggregated.totalQueries;

        aggregated.realDataPercentage =
            100.0 * aggregated.positiveQueries / aggregated.totalQueries;
    }

    return aggregated;
}
