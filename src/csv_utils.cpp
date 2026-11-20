#include "csv_utils.hpp"

#include <spdlog/spdlog.h>

#include <cstdlib>
#include <cmath>
#include <fstream>

void writeCsvHeader(
    const std::string& filename,
    const std::string& headerLine) {

    std::ofstream out(filename, std::ios::app);

    if (!out) {
        spdlog::error(
            "Utils: Failed to open file '{}' for writing the CSV header!",
            filename);
        std::exit(1);
    }

    out << headerLine << "\n";
    out.close();
}

double getProbabilityOfFalsePositive(
    size_t bloomSize,
    int numHashFunctions,
    size_t itemsPerPartition) {

    if (bloomSize == 0) {
        return 1.0;
    }

    const double exponent =
        -static_cast<double>(numHashFunctions) *
        itemsPerPartition / bloomSize;

    const double base = 1.0 - std::exp(exponent);

    return std::pow(base, numHashFunctions);
}