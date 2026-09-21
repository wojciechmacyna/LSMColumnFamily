#include "bloom_value.hpp"

#include <cstdint>
#include <fstream>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "MurmurHash3.h"

namespace {

constexpr size_t BLOCK_BYTES = 64;
constexpr size_t BLOCK_BITS  = 512;

constexpr uint32_t BLOCKED_BLOOM_MAGIC = 0x42424631; // "BBF1"


// ============================================================
// Convert Bloom filter size in bits to number of 512-bit blocks
// ============================================================

size_t bitsToBlocks(size_t bits)
{
    if (bits == 0)
        throw std::invalid_argument(
            "Bloom filter size must be > 0"
        );

    return (bits + BLOCK_BITS - 1) / BLOCK_BITS;
}

} // namespace


// ============================================================
// Implementation
// ============================================================

struct BloomFilter::Impl {

    size_t numBlocks;
    int numHashFunctions;
    std::vector<uint8_t> blocks;


    // --------------------------------------------------------
    // Constructor
    // --------------------------------------------------------

    Impl(
        size_t numBlocks_,
        int numHashFunctions_
    )
        : numBlocks(numBlocks_),
          numHashFunctions(numHashFunctions_),
          blocks(
              numBlocks_ * BLOCK_BYTES,
              0
          )
    {
        if (numBlocks == 0)
            throw std::invalid_argument(
                "numBlocks must be > 0"
            );

        if (numHashFunctions <= 0)
            throw std::invalid_argument(
                "numHashFunctions must be > 0"
            );

        if (numHashFunctions > 512)
            throw std::invalid_argument(
                "numHashFunctions must be <= 512"
            );
    }


    // --------------------------------------------------------
    // MurmurHash3
    // --------------------------------------------------------

    uint32_t hash(
        const std::string& key,
        uint32_t seed
    ) const
    {
        uint32_t result;

        MurmurHash3_x86_32(
            key.data(),
            static_cast<int>(key.size()),
            seed,
            &result
        );

        return result;
    }


    // --------------------------------------------------------
    // Block selection
    // --------------------------------------------------------

    size_t blockIndex(
        const std::string& key
    ) const
    {
        const uint32_t h =
            hash(
                key,
                0x12345678
            );

        return static_cast<size_t>(
            h % numBlocks
        );
    }


    // --------------------------------------------------------
    // Set bit
    // --------------------------------------------------------

    void setBit(
        size_t block,
        size_t bit
    )
    {
        const size_t byteOffset =
            block * BLOCK_BYTES +
            bit / 8;

        const uint8_t mask =
            static_cast<uint8_t>(
                1u << (bit % 8)
            );

        blocks[byteOffset] |= mask;
    }


    // --------------------------------------------------------
    // Get bit
    // --------------------------------------------------------

    bool getBit(
        size_t block,
        size_t bit
    ) const
    {
        const size_t byteOffset =
            block * BLOCK_BYTES +
            bit / 8;

        const uint8_t mask =
            static_cast<uint8_t>(
                1u << (bit % 8)
            );

        return (
            blocks[byteOffset] &
            mask
        ) != 0;
    }
};


// ============================================================
// Constructor
//
// IMPORTANT:
// `size` has the same meaning as in the standard BloomFilter:
// it is the number of BITS.
//
// The blocked implementation converts it to the number of
// 512-bit (64-byte) blocks.
// ============================================================

BloomFilter::BloomFilter(
    size_t size,
    double numHashFunctions
)
    : impl_(
          std::make_unique<Impl>(
              bitsToBlocks(size),
              static_cast<int>(numHashFunctions)
          )
      )
{
}


// ============================================================
// Destructor
// ============================================================

BloomFilter::~BloomFilter() = default;


// ============================================================
// Copy constructor
// ============================================================

BloomFilter::BloomFilter(
    const BloomFilter& other
)
    : impl_(
          std::make_unique<Impl>(
              *other.impl_
          )
      )
{
}


// ============================================================
// Copy assignment
// ============================================================

BloomFilter&
BloomFilter::operator=(
    const BloomFilter& other
)
{
    if (this != &other) {

        impl_ = std::make_unique<Impl>(
            *other.impl_
        );
    }

    return *this;
}


// ============================================================
// Move constructor
// ============================================================

BloomFilter::BloomFilter(
    BloomFilter&& other
) noexcept = default;


// ============================================================
// Move assignment
// ============================================================

BloomFilter&
BloomFilter::operator=(
    BloomFilter&& other
) noexcept = default;


// ============================================================
// Insert
// ============================================================

void
BloomFilter::insert(
    const std::string& key
)
{
    const size_t block =
        impl_->blockIndex(key);

    /*
     * Select different bit positions inside the same
     * 512-bit block using different MurmurHash3 seeds.
     */

    for (int i = 0;
         i < impl_->numHashFunctions;
         ++i)
    {
        const uint32_t h =
            impl_->hash(
                key,
                0x9E3779B9u +
                static_cast<uint32_t>(i)
            );

        const size_t bit =
            static_cast<size_t>(h) &
            (BLOCK_BITS - 1);

        impl_->setBit(
            block,
            bit
        );
    }
}


// ============================================================
// Exists
// ============================================================

bool
BloomFilter::exists(
    const std::string& key
) const
{
    const size_t block =
        impl_->blockIndex(key);

    for (int i = 0;
         i < impl_->numHashFunctions;
         ++i)
    {
        const uint32_t h =
            impl_->hash(
                key,
                0x9E3779B9u +
                static_cast<uint32_t>(i)
            );

        const size_t bit =
            static_cast<size_t>(h) &
            (BLOCK_BITS - 1);

        if (!impl_->getBit(
                block,
                bit
            ))
        {
            return false;
        }
    }

    return true;
}


// ============================================================
// Merge
// ============================================================

void
BloomFilter::merge(
    const BloomFilter& other
)
{
    if (impl_->numBlocks !=
        other.impl_->numBlocks)
    {
        throw std::runtime_error(
            "BlockedBloomFilter numBlocks mismatch during merge"
        );
    }

    if (impl_->numHashFunctions !=
        other.impl_->numHashFunctions)
    {
        throw std::runtime_error(
            "BlockedBloomFilter numHashFunctions mismatch during merge"
        );
    }

    for (size_t i = 0;
         i < impl_->blocks.size();
         ++i)
    {
        impl_->blocks[i] |=
            other.impl_->blocks[i];
    }
}


// ============================================================
// Save
// ============================================================

void
BloomFilter::saveToFile(
    const std::string& filename
) const
{
    std::ofstream file(
        filename,
        std::ios::binary
    );

    if (!file)
        throw std::runtime_error(
            "Error opening file: " + filename
        );


    // --------------------------------------------------------
    // Magic
    // --------------------------------------------------------

    const uint32_t magic =
        BLOCKED_BLOOM_MAGIC;

    file.write(
        reinterpret_cast<const char*>(&magic),
        sizeof(magic)
    );


    // --------------------------------------------------------
    // Number of blocks
    // --------------------------------------------------------

    file.write(
        reinterpret_cast<const char*>(
            &impl_->numBlocks
        ),
        sizeof(impl_->numBlocks)
    );


    // --------------------------------------------------------
    // Number of hash functions
    // --------------------------------------------------------

    file.write(
        reinterpret_cast<const char*>(
            &impl_->numHashFunctions
        ),
        sizeof(impl_->numHashFunctions)
    );


    // --------------------------------------------------------
    // Raw block data
    // --------------------------------------------------------

    file.write(
        reinterpret_cast<const char*>(
            impl_->blocks.data()
        ),
        static_cast<std::streamsize>(
            impl_->blocks.size()
        )
    );


    if (!file)
        throw std::runtime_error(
            "Error writing Bloom filter"
        );
}


// ============================================================
// Load
// ============================================================

BloomFilter BloomFilter::loadFromFile(const std::string& filename)
{
    std::ifstream in(filename, std::ios::binary);
    if (!in) {
        throw std::runtime_error(
            "Cannot open Bloom filter file: " + filename
        );
    }

    uint32_t magic;
    size_t numBlocks;
    int numHashFunctions;

    in.read(
        reinterpret_cast<char*>(&magic),
        sizeof(magic)
    );

    if (!in || magic != BLOCKED_BLOOM_MAGIC) {
        throw std::runtime_error(
            "Invalid blocked Bloom filter file: " + filename
        );
    }

    in.read(
        reinterpret_cast<char*>(&numBlocks),
        sizeof(numBlocks)
    );

    in.read(
        reinterpret_cast<char*>(&numHashFunctions),
        sizeof(numHashFunctions)
    );

    if (!in) {
        throw std::runtime_error(
            "Corrupted blocked Bloom filter header: " + filename
        );
    }

    if (numBlocks == 0) {
        throw std::runtime_error(
            "Invalid number of blocks in Bloom filter file"
        );
    }

    if (numHashFunctions <= 0 ||
        numHashFunctions > static_cast<int>(BLOCK_BITS)) {
        throw std::runtime_error(
            "Invalid number of hash functions in Bloom filter file"
        );
    }

    /*
     * Public constructor expects SIZE IN BITS,
     * whereas the file stores NUMBER OF BLOCKS.
     */
    BloomFilter filter(
        numBlocks * BLOCK_BITS,
        static_cast<double>(numHashFunctions)
    );

    /*
     * Replace the newly allocated zeroed filter with
     * the exact structure described by the file.
     */
    filter.impl_ = std::make_unique<Impl>(
        numBlocks,
        numHashFunctions
    );

    in.read(
        reinterpret_cast<char*>(filter.impl_->blocks.data()),
        static_cast<std::streamsize>(filter.impl_->blocks.size())
    );

    if (!in) {
        throw std::runtime_error(
            "Corrupted blocked Bloom filter data: " + filename
        );
    }

    return filter;
}


// ============================================================
// Memory usage
// ============================================================

size_t
BloomFilter::memoryBytes() const
{
    return impl_->blocks.size();
}