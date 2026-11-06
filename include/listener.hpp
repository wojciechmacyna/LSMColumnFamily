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

struct CompactionChange {
    std::string cf_name;
    int job_id;

    std::vector<std::string> input_files;
    std::vector<std::string> output_files;
};

class MyListener : public rocksdb::EventListener {
private:
        std::vector<SSTChange> sstChanges_;
        std::vector<CompactionChange> compactionChanges_;
        std::mutex mtx_;

public:
void OnTableFileCreated(
    const rocksdb::TableFileCreationInfo& info) override
{
    std::lock_guard<std::mutex> lock(mtx_);

    sstChanges_.push_back({
        info.file_path,
        info.cf_name,
        true
    });
}

    void OnCompactionCompleted(
        rocksdb::DB* db,
        const rocksdb::CompactionJobInfo& ci
    ) override {

        if (!ci.status.ok()) {
            spdlog::error(
                "Compaction failed: CF={}, job={}",
                ci.cf_name,
                ci.job_id
            );
            return;
        }

        CompactionChange change;

        change.cf_name = ci.cf_name;
        change.job_id = ci.job_id;

        // Kopiujemy dane, ponieważ ci przestaje być
        // ważne po zakończeniu callbacku.
        change.input_files = ci.input_files;
        change.output_files = ci.output_files;

        {
            std::lock_guard<std::mutex> lock(mtx_);
            compactionChanges_.push_back(std::move(change));
        }

        spdlog::info(
            "Compaction completed: CF={}, job={}, inputs={}, outputs={}",
            ci.cf_name,
            ci.job_id,
            ci.input_files.size(),
            ci.output_files.size()
        );

        for (const auto& file : ci.input_files) {
            spdlog::info("COMPACTION INPUT: {}", file);
        }

        for (const auto& file : ci.output_files) {
            spdlog::info("COMPACTION OUTPUT: {}", file);
        }
    }

    std::vector<CompactionChange> getChanges(
    const std::string& column)
{
    std::lock_guard<std::mutex> lock(mtx_);

    std::vector<CompactionChange> result;

    auto it = compactionChanges_.begin();

    while (it != compactionChanges_.end()) {
        if (it->cf_name == column) {
            result.push_back(std::move(*it));
            it = compactionChanges_.erase(it);
        } else {
            ++it;
        }
    }

    return result;
}

    std::vector<SSTChange> getSSTChanges(const std::string& column)
{
    std::lock_guard<std::mutex> lock(mtx_);

    std::vector<SSTChange> result;

    auto it = sstChanges_.begin();
    while (it != sstChanges_.end()) {
        if (it->cf_name == column) {
            result.push_back(std::move(*it));
            it = sstChanges_.erase(it);
        } else {
            ++it;
        }
    }

    return result;
}

std::vector<CompactionChange> getCompactionChanges(
    const std::string& column)
{
    std::lock_guard<std::mutex> lock(mtx_);

    std::vector<CompactionChange> result;

    auto it = compactionChanges_.begin();
    while (it != compactionChanges_.end()) {
        if (it->cf_name == column) {
            result.push_back(std::move(*it));
            it = compactionChanges_.erase(it);
        } else {
            ++it;
        }
    }

    return result;
}
};
