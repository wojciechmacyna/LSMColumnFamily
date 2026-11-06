#include "bloomTree.hpp"

#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <climits>
#include <cstdio>
#include <filesystem>
#include <iostream>
#include <stdexcept>

#include <functional>
#include <unordered_set>
#include <algorithm>

extern std::atomic<size_t> gBloomCheckCount;  // declared in algorithm.hpp
extern std::atomic<size_t> gLeafBloomCheckCount;  // declared in algorithm.hpp

void BloomTree::addLeafNode(BloomFilter&& bv, const std::string& file,
                            const std::string& start, const std::string& end) {
    leafNodes.push_back(new Node(std::move(bv), file, start, end));
}


bool BloomTree::validateBloomFilter(
    const Node* node,
    std::string& error) const
{
    if (!node || node->isLeaf()) {
        return true;
    }

    BloomFilter expected(
        node->bloom.bitArraySize,
        node->bloom.numHashFunctions
    );

    for (const Node* child : node->children) {
        if (!child) {
            error = "Null child";
            return false;
        }

        if (child->bloom.bitArraySize != expected.bitArraySize ||
            child->bloom.numHashFunctions != expected.numHashFunctions) {
            error = "Incompatible Bloom filters";
            return false;
        }

        expected.merge(child->bloom);
    }

    if (node->bloom.bitArray != expected.bitArray) {
        error = "Parent Bloom filter differs from OR of child filters";
        return false;
    }

    for (const Node* child : node->children) {
        if (!validateBloomFilter(child, error)) {
            return false;
        }
    }

    return true;
}


bool BloomTree::validateTree(std::string& error) const
{
    error.clear();

    if (!root) {
        if (!leafNodes.empty()) {
            error = "Tree is empty, but leafNodes is not empty";
            return false;
        }
        return true;
    }

    if (root->parent != nullptr) {
        error = "Root has a parent";
        return false;
    }

    size_t expectedLeafDepth = 0;
    bool firstLeaf = true;

    std::unordered_set<const Node*> visited;
    std::unordered_set<const Node*> foundLeaves;

    std::function<bool(const Node*, size_t)> validate =
        [&](const Node* node, size_t depth) -> bool {

        if (!node) {
            error = "Null child pointer";
            return false;
        }

        // Wykrywanie cykli i wielokrotnego podłączenia węzła.
        if (!visited.insert(node).second) {
            error = "Cycle or duplicated node detected";
            return false;
        }

        // Liść.
        if (node->isLeaf()) {
            if (firstLeaf) {
                expectedLeafDepth = depth;
                firstLeaf = false;
            } else if (depth != expectedLeafDepth) {
                error = "Leaves are not at the same depth";
                return false;
            }

            if (node->startKey > node->endKey) {
                error = "Invalid leaf key range";
                return false;
            }

            foundLeaves.insert(node);
            return true;
        }

        // Węzeł wewnętrzny.
        if (node->children.size() > static_cast<size_t>(ratio)) {
            error = "Node exceeds branching ratio";
            return false;
        }

        if (node->children.empty()) {
            error = "Internal node has no children";
            return false;
        }

        for (size_t i = 0; i < node->children.size(); ++i) {
            const Node* child = node->children[i];

            if (!child) {
                error = "Null child pointer";
                return false;
            }

            if (child->parent != node) {
                error = "Incorrect parent pointer";
                return false;
            }

            if (i > 0 &&
                node->children[i - 1]->endKey >= child->startKey) {
                error = "Children are not ordered or ranges overlap";
                return false;
            }

            if (!validate(child, depth + 1)) {
                return false;
            }
        }

        // Zakres węzła musi odpowiadać zakresowi jego dzieci.
        if (node->startKey != node->children.front()->startKey) {
            error = "Incorrect startKey in internal node";
            return false;
        }

        if (node->endKey != node->children.back()->endKey) {
            error = "Incorrect endKey in internal node";
            return false;
        }

        return true;
    };

    if (!validate(root, 0)) {
        return false;
    }

    // Sprawdzenie zgodności leafNodes z rzeczywistymi liśćmi.
    if (foundLeaves.size() != leafNodes.size()) {
        error = "leafNodes size does not match tree leaf count";
        return false;
    }

    std::unordered_set<const Node*> listedLeaves;

    for (const Node* leaf : leafNodes) {
        if (!leaf || !leaf->isLeaf()) {
            error = "leafNodes contains an invalid node";
            return false;
        }

        if (!listedLeaves.insert(leaf).second) {
            error = "Duplicate leaf in leafNodes";
            return false;
        }

        if (foundLeaves.find(leaf) == foundLeaves.end()) {
            error = "leafNodes contains a node not present in the tree";
            return false;
        }
    }

    return true;
}



// Previous
/*void BloomTree::buildLevel(std::vector<Node*>& nodes) {
    if (nodes.size() == 1) {
        root = std::move(nodes.front());
        return;
    }

    std::vector<Node*> parentLevel;

    for (size_t i = 0; i < nodes.size(); i += ratio) {
        size_t end = std::min(i + ratio, nodes.size());

        Node* parent = new Node(BloomFilter(bloomSize, numHashFunctions), "Memory",
                                nodes[i]->startKey, nodes[end - 1]->endKey);

        for (size_t j = i; j < end; ++j) {
            if (parent->startKey > nodes[j]->startKey) {
                parent->startKey = nodes[j]->startKey;
            }
            if (parent->endKey < nodes[j]->endKey) {
                parent->endKey = nodes[j]->endKey;
            }
            parent->bloom.merge(nodes[j]->bloom);
            parent->children.push_back(std::move(nodes[j]));
        }

        parentLevel.push_back(parent);
    }

    buildLevel(parentLevel);
}
*/



void BloomTree::buildLevel(std::vector<Node*>& nodes) {
    if (nodes.empty()) {
        root = nullptr;
        return;
    }

    if (nodes.size() == 1) {
        root = nodes.front();
        root->parent = nullptr;
        return;
    }

    std::vector<Node*> parentLevel;

    for (size_t i = 0; i < nodes.size(); i += ratio) {
        size_t end = std::min(i + ratio, nodes.size());

        Node* parent = new Node(
            BloomFilter(bloomSize, numHashFunctions),
            "Memory",
            nodes[i]->startKey,
            nodes[end - 1]->endKey
        );

        for (size_t j = i; j < end; ++j) {
            Node* child = nodes[j];

            parent->bloom.merge(child->bloom);
            parent->children.push_back(child);

            child->parent = parent;

            if (child->startKey < parent->startKey) {
                parent->startKey = child->startKey;
            }

            if (child->endKey > parent->endKey) {
                parent->endKey = child->endKey;
            }
        }

        parentLevel.push_back(parent);
    }

    buildLevel(parentLevel);
}



void BloomTree::buildTree() {
    buildLevel(leafNodes);
    for (Node* node : leafNodes) {
        node->bloom.saveToFile(node->filename + "_" + node->startKey + "_" + node->endKey);
    }
}

void BloomTree::appendLeavesBatch(BloomTree& other)
{
    // 1. Pobierz wszystkie nowe liście.
    // 2. Wstaw je do struktury w jednym przebiegu.
    // 3. Zaktualizuj filtry węzłów nadrzędnych.
    // 4. Zaktualizuj zakresy węzłów.
    // 5. Zaktualizuj leafNodes.
}

void BloomTree::appendLeaves(const std::vector<Node*>& newLeaves)
{
    spdlog::info(
        "appendLeaves: target leaves={}, new leaves={}",
        leafNodes.size(),
        newLeaves.size()
    );

    for (Node* node : newLeaves) {

        if (!node) {
            throw std::runtime_error("Null leaf in new leaves");
        }

        insertLeaf(
            std::move(node->bloom),
            node->filename,
            node->startKey,
            node->endKey
        );

        // Zapisz filtr nowo utworzonego liścia.
        Node* insertedLeaf = leafNodes.back();

        insertedLeaf->bloom.saveToFile(
            insertedLeaf->filename + "_" +
            insertedLeaf->startKey + "_" +
            insertedLeaf->endKey
        );

        delete node;
    }
}


void BloomTree::insertLeaf(
    BloomFilter&& bloom,
    const std::string& filename,
    const std::string& startKey,
    const std::string& endKey)
{
    if (startKey > endKey) {
        throw std::invalid_argument("Invalid SST key range");
    }

    if (root && startKey <= root->endKey) {
        spdlog::error(
            "APPEND ERROR: SST={}, newStart={}, newEnd={}, "
            "treeStart={}, treeEnd={}",
            filename,
            startKey,
            endKey,
            root->startKey,
            root->endKey
        );

        throw std::invalid_argument(
            "New SST range must be after the existing tree range");
    }

    spdlog::info(
        "APPEND OK: SST={}, newStart={}, newEnd={}, treeEnd={}",
        filename,
        startKey,
        endKey,
        root ? root->endKey : "EMPTY"
    );

    auto t1 = std::chrono::steady_clock::now();


    Node* leaf = new Node(
        std::move(bloom),
        filename,
        startKey,
        endKey
    );

    auto t2 = std::chrono::steady_clock::now();

    leafNodes.push_back(leaf);

    auto t3 = std::chrono::steady_clock::now();

    if (!root) {
        root = leaf;
        return;
    }

    if (root->isLeaf()) {
        Node* newRoot = createParent(root);
        root = newRoot;

        insertIntoNode(root, leaf);
        return;
    }

    Node* node = root;

    while (!node->children.front()->isLeaf()) {
        node = node->children.back();
    }

    auto t4 = std::chrono::steady_clock::now();

    insertIntoNode(node, leaf);
    auto t5 = std::chrono::steady_clock::now();


    auto ms = [](auto a, auto b) {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
            b - a
        ).count();
    };

    spdlog::info(
        "insertLeaf timing: Node={} ms, vector={} ms, traversal={} ms, "
        "insertIntoNode={} ms",
        ms(t1, t2),
        ms(t2, t3),
        ms(t3, t4),
        ms(t4, t5)
    );
}


Node* BloomTree::createParent(Node* child)
{
    auto* parent = new Node(
        BloomFilter(bloomSize, numHashFunctions),
        "Memory",
        child->startKey,
        child->endKey
    );

    parent->children.push_back(child);
    child->parent = parent;

    parent->bloom.merge(child->bloom);

    return parent;
}

void BloomTree::insertIntoNode(Node* parent, Node* child)
{
    if (!parent || !child) {
        throw std::invalid_argument("Null node in insertIntoNode");
    }

    child->parent = parent;

    // Węzeł ma jeszcze wolne miejsce.
if (parent->children.size() < static_cast<size_t>(ratio)) {

    parent->children.push_back(child);

    // Aktualizacja filtra bezpośredniego rodzica.
    parent->bloom.merge(child->bloom);

    // Aktualizacja zakresu rodzica.
    parent->startKey = parent->children.front()->startKey;
    parent->endKey = parent->children.back()->endKey;

    // Propagacja nowych bitów do wszystkich przodków.
    updateAncestors(parent->parent, child->bloom);

    return;
}

    // Węzeł jest pełny. Przygotowujemy ratio + 1 dzieci.
    std::vector<Node*> allChildren = parent->children;
    allChildren.push_back(child);

    const size_t total = allChildren.size();
    const size_t leftCount = total / 2;

    // Zachowujemy istniejący węzeł jako lewą część splitu.
    parent->children.clear();

    parent->bloom = BloomFilter(bloomSize, numHashFunctions);

    for (size_t i = 0; i < leftCount; ++i) {
        Node* current = allChildren[i];

        parent->children.push_back(current);
        current->parent = parent;

        parent->bloom.merge(current->bloom);
    }

    parent->startKey = parent->children.front()->startKey;
    parent->endKey = parent->children.back()->endKey;

    // Tworzymy prawą część splitu.
    Node* rightNode = new Node(
        BloomFilter(bloomSize, numHashFunctions),
        "Memory",
        allChildren[leftCount]->startKey,
        allChildren.back()->endKey
    );

    for (size_t i = leftCount; i < total; ++i) {
        Node* current = allChildren[i];

        rightNode->children.push_back(current);
        current->parent = rightNode;

        rightNode->bloom.merge(current->bloom);
    }

    rightNode->startKey = rightNode->children.front()->startKey;
    rightNode->endKey = rightNode->children.back()->endKey;

    // Zapamiętujemy rodzica przed ewentualnym splitem wyżej.
    Node* grandParent = parent->parent;

    if (!grandParent) {
        // Przepełnił się root. Tworzymy nowy poziom.
        Node* newRoot = new Node(
            BloomFilter(bloomSize, numHashFunctions),
            "Memory",
            parent->startKey,
            rightNode->endKey
        );

        newRoot->children.push_back(parent);
        newRoot->children.push_back(rightNode);

        parent->parent = newRoot;
        rightNode->parent = newRoot;

        newRoot->bloom.merge(parent->bloom);
        newRoot->bloom.merge(rightNode->bloom);

        root = newRoot;
        return;
    }

    // Wstawiamy prawą część splitu do rodzica.
    // Jeżeli rodzic jest pełny, split zostanie wykonany rekurencyjnie.
    insertIntoNode(grandParent, rightNode);
}


void BloomTree::updateAncestors(
    Node* node,
    const BloomFilter& delta)
{
    while (node) {

        // Propagujemy nowe bity do filtra przodka.
        node->bloom.merge(delta);

        // Aktualizujemy zakres kluczy.
        if (!node->children.empty()) {
            node->startKey = node->children.front()->startKey;
            node->endKey = node->children.back()->endKey;
        }

        node = node->parent;
    }
}

void BloomTree::search(Node* node, const std::string& value,
                       const std::string& qStart, const std::string& qEnd,
                       std::vector<std::string>& results) const {
    if (!node) return;

    bool overlaps =
        (qEnd.empty() || node->startKey <= qEnd) &&
        (qStart.empty() || node->endKey >= qStart);

    if (overlaps) {
        ++gBloomCheckCount;
        
        // Track leaf bloom filter checks
        if (node->filename != "Memory") {
            ++gLeafBloomCheckCount;
        }
        
        if (node->bloom.exists(value)) {
            if (node->filename != "Memory") {
                results.push_back(node->filename);
            } else {
                for (Node* child : node->children) {
                    search(child, value, qStart, qEnd, results);
                }
            }
        }
    }
}

std::vector<std::string> BloomTree::query(const std::string& value,
                                          const std::string& qStart,
                                          const std::string& qEnd) const {
    std::vector<std::string> results;
    search(root, value, qStart, qEnd, results);
    return results;
}

// search that returns nodes
void BloomTree::searchNodes(Node* node, const std::string& value,
                            const std::string& qStart, const std::string& qEnd,
                            std::vector<const Node*>& results) const {
    if (!node) return;

    bool overlaps =
        (qEnd.empty() || node->startKey <= qEnd) &&
        (qStart.empty() || node->endKey >= qStart);

    if (overlaps) {
        ++gBloomCheckCount;
        
        // Track leaf bloom filter checks
        if (node->filename != "Memory") {
            ++gLeafBloomCheckCount;
        }
        
        if (node->bloom.exists(value)) {
            if (node->children.empty()) {
                results.push_back(node);
            } else {
                for (Node* child : node->children) {
                    searchNodes(child, value, qStart, qEnd, results);
                }
            }
        }
    }
}

// query where return type is vector of nodes
std::vector<const Node*> BloomTree::queryNodes(const std::string& value,
                                               const std::string& qStart,
                                               const std::string& qEnd) const {
    std::vector<const Node*> results;
    searchNodes(root, value, qStart, qEnd, results);
    return results;
}

static size_t computeNodeMemory(const Node* node) {
    if (!node) return 0;
    size_t mem = 0;

    mem += node->bloom.bitArray.capacity() / CHAR_BIT;
    mem += sizeof(node->bloom.bitArray);

    for (const Node* child : node->children) {
        mem += computeNodeMemory(child);
    }
    return mem;
}

static size_t computeBloomFilterDiskSize(const BloomFilter& bf) {
    char tmpName[] = "/tmp/bloomXXXXXX";
    int fd = mkstemp(tmpName);
    if (fd == -1) {
        throw std::runtime_error("mkstemp failed");
    }
    close(fd);

    bf.saveToFile(std::string(tmpName));

    size_t size = std::filesystem::file_size(tmpName);

    std::remove(tmpName);

    return size;
}

size_t BloomTree::memorySize() const {
    size_t total = 0;
    std::vector<const Node*> stack;
    stack.push_back(root);
    while (!stack.empty()) {
        const Node* node = stack.back();
        stack.pop_back();
        if (node->filename == "Memory") {
            total += computeBloomFilterDiskSize(node->bloom);
            for (const Node* child : node->children) {
                stack.push_back(child);
            }
        }
    }
    return total;
}

size_t BloomTree::diskSize() const {
    size_t total = 0;
    for (const Node* leaf : leafNodes) {
        if (leaf->filename != "Memory") {
            total += computeBloomFilterDiskSize(leaf->bloom);
        }
    }
    return total;
}