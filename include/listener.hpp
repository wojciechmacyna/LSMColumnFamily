#include <rocksdb/db.h>
#include <rocksdb/listener.h>
#include <rocksdb/options.h>

#include <iostream>
#include <memory>

struct SSTChange {
    std::string file_path;
    std::string cf_name;
    bool created;
};

class MyListener : public rocksdb::EventListener {
private:
    std::vector<SSTChange> changes_;
    std::mutex mtx_;

public:

    void OnTableFileCreated(
        const rocksdb::TableFileCreationInfo& info
    ) override {

        std::lock_guard<std::mutex> lock(mtx_);

        changes_.push_back({
            info.file_path,
            info.cf_name,
            true
        });
    }

    void OnTableFileDeleted(
        const rocksdb::TableFileDeletionInfo& info
    ) override {

        std::lock_guard<std::mutex> lock(mtx_);

        changes_.push_back({
            info.file_path,
            info.db_name, // UWAGA: nie używaj, jeśli nie ma tego pola
            false
        });
    }

    std::vector<SSTChange> getChanges(
        const std::string& column
    ) {

        std::lock_guard<std::mutex> lock(mtx_);

        std::vector<SSTChange> result;

        auto it = changes_.begin();

        while (it != changes_.end()) {

            if (it->cf_name == column) {
                result.push_back(*it);
                it = changes_.erase(it);
            } else {
                ++it;
            }
        }

        spdlog::info(
        "getChanges: column={}, returned={}",
        column, result.size()
    );


        return result;
    }
};
