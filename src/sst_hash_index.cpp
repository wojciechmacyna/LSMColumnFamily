#include "sst_hash_index.hpp"

#include <rocksdb/options.h>
#include <rocksdb/sst_file_reader.h>

#include <algorithm>
#include <array>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <unordered_map>
#include <utility>

namespace {

constexpr std::array<char, 8> MAGIC = {
    'S', 'S', 'T', 'H', 'I', 'D', 'X', '1'
};

constexpr uint32_t FORMAT_VERSION = 2;

template <typename T>
void writeValue(
    std::ostream& out,
    const T& value,
    const std::string& field = "unknown") {

    out.write(
        reinterpret_cast<const char*>(&value),
        sizeof(T));

    if (!out) {
        throw std::runtime_error(
            "Cannot write hash index field: " + field +
            ", bytes: " + std::to_string(sizeof(T)));
    }
}

template <typename T>
void readValue(std::istream& in, T& value) {
    in.read(
        reinterpret_cast<char*>(&value),
        sizeof(T));

    if (!in) {
        throw std::runtime_error("Cannot read hash index");
    }
}

void writeString(std::ostream& out, const std::string& value) {
    if (value.size() > std::numeric_limits<uint32_t>::max()) {
        throw std::runtime_error("String is too long");
    }

    uint32_t length = static_cast<uint32_t>(value.size());

    writeValue(out, length);

    out.write(value.data(), length);

    if (!out) {
        throw std::runtime_error("Cannot write string");
    }
}

std::string readString(std::istream& in) {
    uint32_t length = 0;
    readValue(in, length);

    std::string value(length, '\0');

    in.read(value.data(), length);

    if (!in) {
        throw std::runtime_error("Cannot read string");
    }

    return value;
}

struct DiskEntry {
    uint64_t hash = 0;
    uint64_t postingOffset = 0;
    uint64_t postingCount = 0;
    std::string value;
};

}  // namespace

uint64_t SSTHashIndex::hashValue(const std::string& value) {

    // FNV-1a 64-bit.
    constexpr uint64_t offsetBasis = 14695981039346656037ULL;
    constexpr uint64_t prime = 1099511628211ULL;

    uint64_t hash = offsetBasis;

    for (unsigned char c : value) {
        hash ^= c;
        hash *= prime;
    }

    return hash;
}

bool SSTHashIndex::exists(const std::string& indexPath) {
    return std::filesystem::exists(indexPath);
}


void SSTHashIndex::build(
    const std::string& sstPath,
    const std::string& indexPath,
    uint64_t bucketCount) {

    if (bucketCount == 0) {
        throw std::invalid_argument("bucketCount cannot be zero");
    }

    std::cout << "Building hash index: "
              << indexPath << std::endl;

    // ---------------------------------------------------------
    // 1. Read SST and aggregate keys by value.
    // ---------------------------------------------------------

    std::unordered_map<
        std::string,
        std::vector<std::string>> valueToKeys;

    rocksdb::Options options;
    rocksdb::SstFileReader reader(options);

    rocksdb::Status status = reader.Open(sstPath);

    if (!status.ok()) {
        throw std::runtime_error(
            "Cannot open SST file: " + status.ToString());
    }

    rocksdb::ReadOptions readOptions;

    std::unique_ptr<rocksdb::Iterator> iterator(
        reader.NewIterator(readOptions));

    for (iterator->SeekToFirst();
         iterator->Valid();
         iterator->Next()) {

        std::string key = iterator->key().ToString();
        std::string value = iterator->value().ToString();

        valueToKeys[value].push_back(std::move(key));
    }

    status = iterator->status();

    if (!status.ok()) {
        throw std::runtime_error(
            "SST iteration failed: " + status.ToString());
    }

    // ---------------------------------------------------------
    // 2. Create hash buckets.
    // ---------------------------------------------------------

    std::vector<std::vector<Entry>> bucketEntries(bucketCount);

    uint64_t totalEntries = 0;

    for (auto& [value, keys] : valueToKeys) {

        uint64_t hash = hashValue(value);
        uint64_t bucketId = hash % bucketCount;

        std::sort(keys.begin(), keys.end());

        keys.erase(
            std::unique(keys.begin(), keys.end()),
            keys.end());

        Entry entry;

        entry.hash = hash;
        entry.value = std::move(value);
        entry.keys = std::move(keys);

        bucketEntries[bucketId].push_back(std::move(entry));

        ++totalEntries;
    }

    valueToKeys.clear();
    valueToKeys.rehash(0);

    // ---------------------------------------------------------
    // 3. Write posting lists to a temporary file.
    // ---------------------------------------------------------

    const std::string postingTempPath =
        indexPath + ".postings.tmp";

    std::ofstream postingFile(
        postingTempPath,
        std::ios::binary | std::ios::trunc);

    if (!postingFile) {
        throw std::runtime_error(
            "Cannot create temporary postings file");
    }

    std::vector<std::vector<DiskEntry>> diskEntries(bucketCount);

    for (uint64_t bucketId = 0;
         bucketId < bucketCount;
         ++bucketId) {

        auto& entries = bucketEntries[bucketId];
        auto& diskBucket = diskEntries[bucketId];

        diskBucket.reserve(entries.size());

        for (const auto& entry : entries) {

            DiskEntry diskEntry;

            diskEntry.hash = entry.hash;
            diskEntry.value = entry.value;
            diskEntry.postingCount = entry.keys.size();

            const std::streampos postingPosition =
                postingFile.tellp();

            if (postingPosition == std::streampos(-1)) {
                throw std::runtime_error(
                    "Cannot get posting file position");
            }

            diskEntry.postingOffset =
                static_cast<uint64_t>(postingPosition);

            for (const auto& key : entry.keys) {
                writeString(postingFile, key);
            }

            diskBucket.push_back(std::move(diskEntry));
        }
    }

    postingFile.flush();

    if (!postingFile) {
        throw std::runtime_error(
            "Cannot flush temporary postings file");
    }

    postingFile.close();

    // ---------------------------------------------------------
    // 4. Write final index file.
    // ---------------------------------------------------------

    const std::string indexTempPath =
        indexPath + ".tmp";

    std::ofstream indexFile(
        indexTempPath,
        std::ios::binary | std::ios::trunc);

    if (!indexFile) {
        throw std::runtime_error(
            "Cannot create hash index file");
    }

    // Header.
    indexFile.write(MAGIC.data(), MAGIC.size());

    if (!indexFile) {
        throw std::runtime_error(
            "Cannot write hash index magic");
    }

    writeValue(indexFile, FORMAT_VERSION, "FORMAT_VERSION");
    writeValue(indexFile, bucketCount, "bucketCount");
    writeValue(indexFile, totalEntries, "totalEntries");

    // Reserve postingStart in header.
    const std::streampos postingStartPosition =
        indexFile.tellp();

    if (postingStartPosition == std::streampos(-1)) {
        throw std::runtime_error(
            "Cannot get postingStart header position");
    }

    const uint64_t postingStartPlaceholder = 0;

    writeValue(
        indexFile,
        postingStartPlaceholder,
        "postingStart placeholder");

    // Reserve bucket directory.
    const std::streampos directoryPosition =
        indexFile.tellp();

    if (directoryPosition == std::streampos(-1)) {
        throw std::runtime_error(
            "Cannot get directory position");
    }

    const uint64_t directoryStart =
        static_cast<uint64_t>(directoryPosition);

    std::vector<BucketInfo> directory(bucketCount);

    for (uint64_t i = 0; i < bucketCount; ++i) {
        writeValue(indexFile, directory[i].offset, "directory.offset");
        writeValue(indexFile, directory[i].count, "directory.count");
    }

    // Write bucket entries.
    for (uint64_t bucketId = 0;
         bucketId < bucketCount;
         ++bucketId) {

        const std::streampos bucketPosition =
            indexFile.tellp();

        if (bucketPosition == std::streampos(-1)) {
            throw std::runtime_error(
                "Cannot get bucket position");
        }

        directory[bucketId].offset =
            static_cast<uint64_t>(bucketPosition);

        directory[bucketId].count =
            diskEntries[bucketId].size();

        for (const auto& entry : diskEntries[bucketId]) {

            writeValue(indexFile, entry.hash, "entry.hash");

            const uint32_t valueLength =
                static_cast<uint32_t>(entry.value.size());

            writeValue(indexFile, valueLength, "valueLength");
            writeValue(indexFile, entry.postingOffset, "postingOffset");
            writeValue(indexFile, entry.postingCount, "postingCount");

            indexFile.write(
                entry.value.data(),
                static_cast<std::streamsize>(valueLength));

            if (!indexFile) {
                throw std::runtime_error(
                    "Cannot write hash entry value");
            }
        }
    }

    // ---------------------------------------------------------
    // 5. Write postingStart into header and append postings.
    // ---------------------------------------------------------

    const std::streampos metadataEnd =
        indexFile.tellp();

    if (metadataEnd == std::streampos(-1)) {
        throw std::runtime_error(
            "Cannot get metadata end position");
    }

    const uint64_t postingStart =
        static_cast<uint64_t>(metadataEnd);

    // Open temporary postings file.
    std::ifstream postingInput(
        postingTempPath,
        std::ios::binary);

    if (!postingInput) {
        throw std::runtime_error(
            "Cannot open temporary postings file");
    }

    // Update postingStart in header.
    indexFile.seekp(postingStartPosition);

    if (!indexFile) {
        throw std::runtime_error(
            "Cannot seek to postingStart position");
    }

    writeValue(indexFile, postingStart, "postingStart");

    // Return to the end of metadata.
    indexFile.seekp(metadataEnd);

    if (!indexFile) {
        throw std::runtime_error(
            "Cannot seek back to metadata end");
    }

    // Append posting lists.
    indexFile << postingInput.rdbuf();

    if (!indexFile) {
        throw std::runtime_error(
            "Cannot append posting lists");
    }

    postingInput.close();

    // ---------------------------------------------------------
    // 6. Rewrite bucket directory with actual offsets.
    // ---------------------------------------------------------

    indexFile.seekp(
        static_cast<std::streamoff>(directoryStart));

    if (!indexFile) {
        throw std::runtime_error(
            "Cannot seek to directory position");
    }

    for (const auto& bucket : directory) {

        writeValue(indexFile, bucket.offset, "bucket.offset");
        writeValue(indexFile, bucket.count, "bucket.count");
    }

    // ---------------------------------------------------------
    // 7. Finalize index file.
    // ---------------------------------------------------------

    indexFile.flush();

    if (!indexFile) {
        throw std::runtime_error(
            "Cannot finalize hash index");
    }

    indexFile.close();

    if (indexFile.fail()) {
        throw std::runtime_error(
            "Cannot close hash index file");
    }

    // ---------------------------------------------------------
    // 8. Replace old index only after successful construction.
    // ---------------------------------------------------------

    std::error_code ec;

    std::filesystem::remove(indexPath, ec);

    ec.clear();

    std::filesystem::rename(
        indexTempPath,
        indexPath,
        ec);

    if (ec) {
        throw std::runtime_error(
            "Cannot rename hash index file: " + ec.message());
    }

    std::filesystem::remove(postingTempPath, ec);

    std::cout << "Hash index built successfully: "
              << indexPath
              << ", entries: " << totalEntries
              << ", buckets: " << bucketCount
              << ", posting start: " << postingStart
              << std::endl;
}


bool SSTHashIndex::open(const std::string& indexPath) {

    std::ifstream file(indexPath, std::ios::binary);

    if (!file) {
        return false;
    }

    std::array<char, 8> magic{};

    file.read(magic.data(), magic.size());

    if (!file || magic != MAGIC) {
        return false;
    }

    uint32_t version = 0;
    uint64_t bucketCount = 0;

    readValue(file, version);
    readValue(file, bucketCount);
    readValue(file, entryCount_);
    readValue(file, postingStart_);

    if (!file ||
        version != FORMAT_VERSION ||
        bucketCount == 0) {
        return false;
    }

    buckets_.clear();
    buckets_.resize(bucketCount);

    for (auto& bucket : buckets_) {
        readValue(file, bucket.offset);
        readValue(file, bucket.count);

        if (!file) {
            buckets_.clear();
            return false;
        }
    }

    // Validate posting region.
    file.seekg(0, std::ios::end);

    if (!file) {
        buckets_.clear();
        return false;
    }

    const uint64_t fileSize =
        static_cast<uint64_t>(file.tellg());

    const uint64_t directoryEnd =
        8 + sizeof(uint32_t) +
        sizeof(uint64_t) * 3 +
        bucketCount * sizeof(uint64_t) * 2;

    if (postingStart_ < directoryEnd ||
        postingStart_ > fileSize) {
        buckets_.clear();
        return false;
    }

    for (const auto& bucket : buckets_) {

        if (bucket.count == 0) {
            continue;
        }

        if (bucket.offset < directoryEnd ||
            bucket.offset >= postingStart_) {
            buckets_.clear();
            return false;
        }
    }

    indexPath_ = indexPath;
    opened_ = true;

    return true;
}


bool SSTHashIndex::lookup(
    const std::string& value,
    std::vector<std::string>& keys) const {

    keys.clear();

    if (!opened_ || buckets_.empty()) {
        return false;
    }

    const uint64_t hash = hashValue(value);

    const uint64_t bucketId = hash % buckets_.size();

    const auto& bucket = buckets_[bucketId];

    if (bucket.count == 0) {
        return true;
    }

    std::ifstream file(indexPath_, std::ios::binary);

    if (!file) {
        return false;
    }

    file.seekg(bucket.offset);

    for (uint64_t i = 0; i < bucket.count; ++i) {

        uint64_t storedHash = 0;
        uint32_t valueLength = 0;
        uint64_t postingOffset = 0;
        uint64_t postingCount = 0;

        readValue(file, storedHash);
        readValue(file, valueLength);
        readValue(file, postingOffset);
        readValue(file, postingCount);

        std::string storedValue(valueLength, '\0');

        file.read(storedValue.data(), valueLength);

        if (!file) {
            return false;
        }

        if (storedHash == hash && storedValue == value) {

            file.seekg(
                postingStart_ + postingOffset);

            keys.reserve(postingCount);

            for (uint64_t j = 0; j < postingCount; ++j) {
                keys.push_back(readString(file));
            }

            return true;
        }
    }

    // No matching value in this bucket.
    return true;
}