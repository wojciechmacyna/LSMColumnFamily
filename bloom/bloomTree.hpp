#pragma once
#include <memory>
#include <vector>

#include "node.hpp"

class BloomTree {
   public:
    Node* root = nullptr;

   private:
    int ratio;
    size_t bloomSize;
    int numHashFunctions;

    // for future use
    //  size_t expectedItems;
    //  double bloomFalsePositiveRate;

    void buildLevel(std::vector<Node*>& nodes);
    void search(Node* node, const std::string& value,
                const std::string& qStart, const std::string& qEnd,
                std::vector<std::string>& results) const;

    void searchNodes(Node* node, const std::string& value,
                     const std::string& qStart, const std::string& qEnd,
                     std::vector<const Node*>& results) const;

   public:
    // for future use
    //   BloomTree(int branchingRatio, size_t expectedItems, double bloomFalsePositiveRate)
    //       : ratio(branchingRatio),
    //       expectedItems(expectedItems),
    //         bloomFalsePositiveRate(bloomFalsePositiveRate) {}
    BloomTree(int branchingRatio, size_t bloomSize, int numHashFunctions)
        : ratio(branchingRatio),
          bloomSize(bloomSize),
          numHashFunctions(numHashFunctions) {}

    std::vector<Node*> leafNodes;

    void insertIntoNode(Node* parent, Node* child);
   void updateAncestors(Node* node, const BloomFilter& delta);
    Node* createParent(Node* child);

    bool validateBloomFilter(const Node* node, std::string& error) const;

    void insertLeaf(
        BloomFilter&& bloom,
        const std::string& filename,
        const std::string& startKey,
        const std::string& endKey);

    void addLeafNode(BloomFilter&& bv, const std::string& file,
                     const std::string& start, const std::string& end);

    void appendLeaves(BloomTree& other);
    void appendLeavesBatch(BloomTree& other);

    void buildTree();

    std::vector<std::string> query(const std::string& value,
                                   const std::string& qStart,
                                   const std::string& qEnd) const;

    std::vector<const Node*> queryNodes(const std::string& value,
                                        const std::string& qStart,
                                        const std::string& qEnd) const;

    size_t memorySize() const;
    size_t diskSize() const;

    void print() const {
        root->print();
    }

    bool validateTree(std::string& error) const;
};
