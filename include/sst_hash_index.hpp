#pragma once

#include <cstdint>
#include <string>
#include <vector>

class SSTHashIndex {
   private:
    struct BucketInfo {
        uint64_t offset = 0;
        uint64_t count = 0;
    };

    struct Entry {
        uint64_t hash = 0;
        std::string value;
        std::vector<std::string> keys;
    };

    std::string indexPath_;
    std::vector<BucketInfo> buckets_;
    uint64_t entryCount_ = 0;
    uint64_t postingStart_ = 0;
    bool opened_ = false;

    static uint64_t hashValue(const std::string& value);

   public:
    SSTHashIndex() = default;

    // Buduje indeks na podstawie pliku SST.
    static void build(
        const std::string& sstPath,
        const std::string& indexPath,
        uint64_t bucketCount = 4096);

    // Otwiera istniejący indeks.
    bool open(const std::string& indexPath);

    // Wyszukuje klucze dla podanej wartości.
    bool lookup(
        const std::string& value,
        std::vector<std::string>& keys) const;

    static bool exists(const std::string& indexPath);

    uint64_t entryCount() const {
        return entryCount_;
    }

    bool isOpen() const {
        return opened_;
    }
};