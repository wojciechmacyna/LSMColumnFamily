#ifndef BLOOM_MANAGER_HPP
#define BLOOM_MANAGER_HPP

#include <string>
#include <vector>
#include <boost/asio/thread_pool.hpp>
#include <memory>

#include "bloomTree.hpp"

extern boost::asio::thread_pool globalThreadPool;

class MyListener;


class BloomManager {
   private:
    std::shared_ptr<MyListener> listener_;
   public:
    BloomTree createPartitionedHierarchy(const std::vector<std::string>& sstFiles,
                                         size_t partitionSize,
                                         size_t bloomSize,
                                         int numHashFunctions,
                                         int branchingRatio);

   std::vector<Node*> createPartitionedLeaves(
            const std::vector<std::string>& sstFiles,
            size_t partitionSize,
            size_t bloomSize,
            int numHashFunctions
);

   private:
    std::vector<Node*> processSSTFile(const std::string& sstFile,
                                      size_t partitionSize,
                                      size_t bloomSize,
                                      int numHashFunctions);
};

#endif  // BLOOM_MANAGER_HPP
