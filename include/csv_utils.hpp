#pragma once

#include <cstddef>
#include <string>

void writeCsvHeader(const std::string& filename,
                    const std::string& headerLine);

double getProbabilityOfFalsePositive(
    size_t bloomSize,
    int numHashFunctions,
    size_t itemsPerPartition);