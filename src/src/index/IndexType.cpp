#include "index/IndexType.hpp"

namespace dbs {
namespace index {

void IndexValue::print() const {
    for (size_t i = 0; i < this->key.size(); ++i) {
        std::cout << this->key[i];
        if (i != this->key.size() - 1) {
            std::cout << ", ";
        }
    }
    std::cout << std::endl;
}

// Default constructor for BPlusTreeInternalChild
BPlusTreeInternalChild::BPlusTreeInternalChild() {
    pageId = -1;
    maxKey.clear();
}

// Constructor for BPlusTreeInternalChild with pageId and key count
BPlusTreeInternalChild::BPlusTreeInternalChild(int pageId, int keyCount) {
    this->pageId = pageId;
    maxKey.assign(keyCount, INT_MIN);
}

// Constructor for BPlusTreeInternalChild with pageId and maximum keys
BPlusTreeInternalChild::BPlusTreeInternalChild(int pageId, std::vector<int> maxKeys) {
    this->pageId = pageId;
    this->maxKey = std::move(maxKeys);
}

// Constructor for BPlusTreeInternalNode with previous and next page IDs
BPlusTreeInternalNode::BPlusTreeInternalNode(int prevPageId, int nextPageId) {
    this->prevPageId = prevPageId;
    this->nextPageId = nextPageId;
    children.clear();
}

// Default constructor for BPlusTreeInternalNode
BPlusTreeInternalNode::BPlusTreeInternalNode() {
    prevPageId = -1;
    nextPageId = -1;
    children.clear();
}

// Constructor for BPlusTreeLeafChild with pageId, slotId, and key count
BPlusTreeLeafChild::BPlusTreeLeafChild(int pageId, int slotId, int keyCount) {
    this->pageId = pageId;
    this->slotId = slotId;
    key.assign(keyCount, INT_MIN);
}


// // Check for exact match between two BPlusTreeLeafChild objects
bool BPlusTreeLeafChild::exactMatch(const BPlusTreeLeafChild& other) const {
    for (size_t i = 0; i < this->key.size(); ++i) {
        if (this->key[i] != other.key[i]) return false;
    }
    return this->pageId == other.pageId && this->slotId == other.slotId;
}


// Constructor for BPlusTreeLeafChild from an IndexValue
BPlusTreeLeafChild::BPlusTreeLeafChild(IndexValue indexValue) {
    this->pageId = indexValue.pageId;
    this->slotId = indexValue.slotId;
    this->key = std::move(indexValue.key);
}

// Default constructor for BPlusTreeLeafNode
BPlusTreeLeafNode::BPlusTreeLeafNode() {
    prevPageId = -1;
    nextPageId = -1;
    children.clear();
}

// Constructor for BPlusTreeLeafNode with previous and next page IDs
BPlusTreeLeafNode::BPlusTreeLeafNode(int prevPageId, int nextPageId) {
    this->prevPageId = prevPageId;
    this->nextPageId = nextPageId;
    children.clear();
}

// Default constructor for IndexValue
IndexValue::IndexValue() {
    pageId = -1;
    slotId = -1;
    key.clear();
}

// Constructor for IndexValue with pageId, slotId, and key values
IndexValue::IndexValue(int pageId, int slotId, std::vector<int> keyValues) {
    this->pageId = pageId;
    this->slotId = slotId;
    this->key = std::move(keyValues);
}

// Constructor for IndexValue with pageId, slotId, and key count
IndexValue::IndexValue(int pageId, int slotId, int keyCount) {
    this->pageId = pageId;
    this->slotId = slotId;
    key.assign(keyCount, INT_MIN);
}

// Converts a list of IndexValues to RecordLocation format
void indexValuesToRecordLocations(
    const std::vector<IndexValue>& indexValues,
    std::vector<record::RecordLocation>& recordLocations) {
    recordLocations.clear();
    for (const auto& indexValue : indexValues) {
        record::RecordLocation location;
        location.pageId = indexValue.pageId;
        location.slotId = indexValue.slotId;
        recordLocations.push_back(std::move(location));
    }
}

}  // namespace index
}  // namespace dbs
