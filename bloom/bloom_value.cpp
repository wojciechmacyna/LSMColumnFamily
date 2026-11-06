/* #include "bloom_value.hpp"
#include <iostream>

#include <chrono>
#include <cstdint>

#include <stdexcept>

#include "MurmurHash3.h"

// BloomFilter::BloomFilter(size_t expectedItems, double falsePositiveRate) {
//     double ln2 = std::log(2.0);
//     bitArraySize = static_cast<size_t>(-(expectedItems * std::log(falsePositiveRate)) / (ln2 * ln2));
//     numHashFunctions = static_cast<int>(std::round((bitArraySize / expectedItems) * ln2));
//     bitArray.resize(bitArraySize, false);
// }

BloomFilter::BloomFilter(size_t size, double numHashFunctions) : bitArraySize(size), numHashFunctions(numHashFunctions) {
    bitArray.resize(bitArraySize, false);
}

size_t BloomFilter::hash(const std::string& key, int seed) const {
    uint32_t hashOutput;
    MurmurHash3_x86_32(key.c_str(), key.size(), seed, &hashOutput);
    return static_cast<size_t>(hashOutput) % bitArraySize;
}

void BloomFilter::insert(const std::string& key) {
    for (int i = 0; i < numHashFunctions; ++i) {
        bitArray[hash(key, i)] = true;
    }
}

bool BloomFilter::exists(const std::string& key) const {
    for (int i = 0; i < numHashFunctions; ++i) {
        if (!bitArray[hash(key, i)]) {
            return false;
        }
    }
    return true;
}

void BloomFilter::merge(const BloomFilter& other) {
    if (bitArray.size() != other.bitArray.size()) {
        throw std::runtime_error("BloomFilter size mismatch during merge");
    }

    const auto start = std::chrono::steady_clock::now();

    for (size_t i = 0; i < bitArray.size(); ++i) {
        bitArray[i] |= other.bitArray[i];
    }

    const auto end = std::chrono::steady_clock::now();

    mergeTimeNs += std::chrono::duration_cast<std::chrono::nanoseconds>(
        end - start
    ).count();

    ++mergeCount;
    mergeBytes += bitArray.size();
}

void BloomFilter::saveToFile(const std::string& filename) const {
    std::ofstream file(filename, std::ios::binary);
    if (!file) throw std::runtime_error("Error opening file: " + filename);

    file.write(reinterpret_cast<const char*>(&bitArraySize), sizeof(bitArraySize));
    file.write(reinterpret_cast<const char*>(&numHashFunctions), sizeof(numHashFunctions));

    size_t byteSize = (bitArraySize + 7) / 8;
    std::vector<char> buffer(byteSize, 0);
    for (size_t i = 0; i < bitArraySize; ++i) {
        if (bitArray[i]) buffer[i / 8] |= (1 << (i % 8));
    }
    file.write(buffer.data(), buffer.size());
}

BloomFilter BloomFilter::loadFromFile(const std::string& filename) {
    std::ifstream file(filename, std::ios::binary);
    if (!file) throw std::runtime_error("Error opening file: " + filename);

    size_t bitArraySize;
    int numHashFunctions;
    file.read(reinterpret_cast<char*>(&bitArraySize), sizeof(bitArraySize));
    file.read(reinterpret_cast<char*>(&numHashFunctions), sizeof(numHashFunctions));

    BloomFilter filter(1, 0.01);  // Temporary dummy values
    filter.bitArraySize = bitArraySize;
    filter.numHashFunctions = numHashFunctions;
    filter.bitArray.resize(bitArraySize);

    size_t byteSize = (bitArraySize + 7) / 8;
    std::vector<char> buffer(byteSize);
    file.read(buffer.data(), buffer.size());

    for (size_t i = 0; i < bitArraySize; ++i) {
        filter.bitArray[i] = buffer[i / 8] & (1 << (i % 8));
    }

    return filter;
}*/


#include "bloom_value.hpp"

#include <iostream>
#include <fstream>
#include <cstdint>
#include <stdexcept>
#include <vector>
#include <string>

#include "MurmurHash3.h"

BloomFilter::BloomFilter(size_t size, double numHashFunctions)
    : bitArraySize(size),
      numHashFunctions(numHashFunctions)
{
    // Jeden element uint64_t przechowuje 64 bity.
    bitArray.resize((bitArraySize + 63) / 64, 0);
}

size_t BloomFilter::hash(const std::string& key, int seed) const
{
    if (bitArraySize == 0) {
        throw std::runtime_error("BloomFilter has zero size");
    }

    uint32_t hashOutput;
    MurmurHash3_x86_32(key.c_str(), key.size(), seed, &hashOutput);

    return static_cast<size_t>(hashOutput) % bitArraySize;
}

void BloomFilter::insert(const std::string& key)
{
    for (int i = 0; i < numHashFunctions; ++i) {
        const size_t index = hash(key, i);

        bitArray[index / 64] |=
            (uint64_t{1} << (index % 64));
    }
}

bool BloomFilter::exists(const std::string& key) const
{
    for (int i = 0; i < numHashFunctions; ++i) {
        const size_t index = hash(key, i);

        if ((bitArray[index / 64] &
             (uint64_t{1} << (index % 64))) == 0) {
            return false;
        }
    }

    return true;
}

void BloomFilter::merge(const BloomFilter& other)
{
    if (bitArraySize != other.bitArraySize ||
        bitArray.size() != other.bitArray.size()) {
        throw std::runtime_error(
            "BloomFilter size mismatch during merge"
        );
    }

    for (size_t i = 0; i < bitArray.size(); ++i) {
        bitArray[i] |= other.bitArray[i];
    }
}

void BloomFilter::saveToFile(const std::string& filename) const
{
    std::ofstream file(filename, std::ios::binary);

    if (!file) {
        throw std::runtime_error(
            "Error opening file: " + filename
        );
    }

    file.write(
        reinterpret_cast<const char*>(&bitArraySize),
        sizeof(bitArraySize)
    );

    file.write(
        reinterpret_cast<const char*>(&numHashFunctions),
        sizeof(numHashFunctions)
    );

    // Zachowujemy dotychczasowy format:
    // 8 bitów w jednym bajcie.
    const size_t byteSize = (bitArraySize + 7) / 8;

    std::vector<uint8_t> buffer(byteSize, 0);

    for (size_t i = 0; i < bitArraySize; ++i) {
        if (bitArray[i / 64] &
            (uint64_t{1} << (i % 64))) {

            buffer[i / 8] |=
                static_cast<uint8_t>(1u << (i % 8));
        }
    }

    file.write(
        reinterpret_cast<const char*>(buffer.data()),
        static_cast<std::streamsize>(buffer.size())
    );

    if (!file) {
        throw std::runtime_error(
            "Error writing BloomFilter: " + filename
        );
    }
}

BloomFilter BloomFilter::loadFromFile(
    const std::string& filename)
{
    std::ifstream file(filename, std::ios::binary);

    if (!file) {
        throw std::runtime_error(
            "Error opening file: " + filename
        );
    }

    size_t bitArraySize;
    double numHashFunctions;

    file.read(
        reinterpret_cast<char*>(&bitArraySize),
        sizeof(bitArraySize)
    );

    file.read(
        reinterpret_cast<char*>(&numHashFunctions),
        sizeof(numHashFunctions)
    );

    if (!file || bitArraySize == 0) {
        throw std::runtime_error(
            "Invalid BloomFilter header: " + filename
        );
    }

    BloomFilter filter(bitArraySize, numHashFunctions);

    const size_t byteSize = (bitArraySize + 7) / 8;

    std::vector<uint8_t> buffer(byteSize);

    file.read(
        reinterpret_cast<char*>(buffer.data()),
        static_cast<std::streamsize>(buffer.size())
    );

    if (!file) {
        throw std::runtime_error(
            "Error reading BloomFilter data: " + filename
        );
    }

    for (size_t i = 0; i < bitArraySize; ++i) {
        if (buffer[i / 8] & (1u << (i % 8))) {
            filter.bitArray[i / 64] |=
                (uint64_t{1} << (i % 64));
        }
    }

    return filter;
}

