/*#include "bloom_value.hpp"
#include <iostream>

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
      std::cout << "bitArray.size() " << bitArray.size() << " other.bitArray.size() " << other.bitArray.size() << std::endl;

        throw std::runtime_error("BloomFilter size mismatch during merge");
    }
    for (size_t i = 0; i < bitArray.size(); ++i) {
        bitArray[i] = bitArray[i] | other.bitArray[i];
    }
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
}
    */


#include "bloom_value.hpp"

#include <fstream>
#include <iostream>
#include <stdexcept>
#include <vector>

#include "MurmurHash3.h"


struct BloomFilter::Impl {
    std::vector<bool> bitArray;
    int numHashFunctions;
    size_t bitArraySize;
};


BloomFilter::BloomFilter(
    size_t size,
    double numHashFunctions)
{
    impl_ = std::make_unique<Impl>();

    impl_->bitArraySize = size;
    impl_->numHashFunctions =
        static_cast<int>(numHashFunctions);

    impl_->bitArray.resize(
        impl_->bitArraySize,
        false);
}


BloomFilter::~BloomFilter() = default;


BloomFilter::BloomFilter(
    const BloomFilter& other)
    : impl_(
          std::make_unique<Impl>(
              *other.impl_))
{
}


BloomFilter&
BloomFilter::operator=(
    const BloomFilter& other)
{
    if (this != &other) {
        *impl_ = *other.impl_;
    }

    return *this;
}


BloomFilter::BloomFilter(
    BloomFilter&& other) noexcept = default;


BloomFilter&
BloomFilter::operator=(
    BloomFilter&& other) noexcept = default;


static size_t hashKey(
    const std::string& key,
    int seed,
    size_t bitArraySize)
{
    uint32_t hashOutput;

    MurmurHash3_x86_32(
        key.c_str(),
        key.size(),
        seed,
        &hashOutput);

    return static_cast<size_t>(
               hashOutput) %
           bitArraySize;
}


void BloomFilter::insert(
    const std::string& key)
{
    for (int i = 0;
         i < impl_->numHashFunctions;
         ++i)
    {
        impl_->bitArray[
            hashKey(
                key,
                i,
                impl_->bitArraySize)] = true;
    }
}


bool BloomFilter::exists(
    const std::string& key) const
{
    for (int i = 0;
         i < impl_->numHashFunctions;
         ++i)
    {
        if (!impl_->bitArray[
                hashKey(
                    key,
                    i,
                    impl_->bitArraySize)])
        {
            return false;
        }
    }

    return true;
}


void BloomFilter::merge(
    const BloomFilter& other)
{
    if (impl_->bitArray.size() !=
        other.impl_->bitArray.size())
    {
        std::cout
            << "bitArray.size() "
            << impl_->bitArray.size()
            << " other.bitArray.size() "
            << other.impl_->bitArray.size()
            << std::endl;

        throw std::runtime_error(
            "BloomFilter size mismatch "
            "during merge");
    }

    for (size_t i = 0;
         i < impl_->bitArray.size();
         ++i)
    {
        impl_->bitArray[i] =
            impl_->bitArray[i] |
            other.impl_->bitArray[i];
    }
}

size_t BloomFilter::memoryBytes() const {
    return impl_->bitArray.capacity() / CHAR_BIT;
}

void BloomFilter::saveToFile(
    const std::string& filename) const
{
    std::ofstream file(
        filename,
        std::ios::binary);

    if (!file)
        throw std::runtime_error(
            "Error opening file: " +
            filename);

    file.write(
        reinterpret_cast<const char*>(
            &impl_->bitArraySize),
        sizeof(impl_->bitArraySize));

    file.write(
        reinterpret_cast<const char*>(
            &impl_->numHashFunctions),
        sizeof(impl_->numHashFunctions));

    size_t byteSize =
        (impl_->bitArraySize + 7) / 8;

    std::vector<char> buffer(
        byteSize,
        0);

    for (size_t i = 0;
         i < impl_->bitArraySize;
         ++i)
    {
        if (impl_->bitArray[i])
            buffer[i / 8] |=
                (1 << (i % 8));
    }

    file.write(
        buffer.data(),
        buffer.size());
}


BloomFilter BloomFilter::loadFromFile(
    const std::string& filename)
{
    std::ifstream file(
        filename,
        std::ios::binary);

    if (!file)
        throw std::runtime_error(
            "Error opening file: " +
            filename);

    size_t bitArraySize;
    int numHashFunctions;

    file.read(
        reinterpret_cast<char*>(
            &bitArraySize),
        sizeof(bitArraySize));

    file.read(
        reinterpret_cast<char*>(
            &numHashFunctions),
        sizeof(numHashFunctions));

    BloomFilter filter(
        1,
        0.01);

    filter.impl_->bitArraySize =
        bitArraySize;

    filter.impl_->numHashFunctions =
        numHashFunctions;

    filter.impl_->bitArray.resize(
        bitArraySize);

    size_t byteSize =
        (bitArraySize + 7) / 8;

    std::vector<char> buffer(
        byteSize);

    file.read(
        buffer.data(),
        buffer.size());

    for (size_t i = 0;
         i < bitArraySize;
         ++i)
    {
        filter.impl_->bitArray[i] =
            buffer[i / 8] &
            (1 << (i % 8));
    }

    return filter;
}
