
#include "bloom_filter.hpp"

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

