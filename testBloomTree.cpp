#include "bloom/bloomTree.hpp"
#include "bloom/bloom_value.hpp"

#include <iostream>
#include <string>

#include <atomic>
#include <cstddef>

#include <iomanip>
#include <sstream>
#include <string>

std::string makeKey(size_t i) {
    std::ostringstream oss;
    oss << "key" << std::setw(20) << std::setfill('0') << i;
    return oss.str();
}


std::atomic<size_t> gBloomCheckCount{0};
std::atomic<size_t> gLeafBloomCheckCount{0};

int main() {

    const int ratio = 3;
    const size_t bloomSize = 10000;
    const int numHashFunctions = 7;

    BloomTree tree(ratio, bloomSize, numHashFunctions);

    for (int i = 1; i <= 100; ++i) {

        BloomFilter bf(bloomSize, numHashFunctions);

        std::string key = makeKey(i);

        tree.insertLeaf(
            std::move(bf),
            "sst_" + std::to_string(i),
            key,
            key
        );

       // std::cout << "\nAfter inserting leaf " << i << "\n";
      //  tree.print();

        std::string validationError;

        if (!tree.validateTree(validationError)) {
            spdlog::error(
                "Tree validation FAILED: {}",
                validationError
            );

            tree.print();
            return 1;
        }

        spdlog::info("Tree validation OK");

    }

    return 0;
}