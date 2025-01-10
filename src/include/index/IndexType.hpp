#pragma once

#include <vector>
#include <list> 
#include <climits>
#include <iostream>


#include "common/Config.hpp"
#include "record/DataType.hpp"

namespace dbs {
namespace index {

// Represents the value used for indexing
struct IndexValue {
    int pageId;  // ID of the page containing the data
    int slotId;  // Slot ID within the page
    std::vector<int> key;  // Key used for indexing

    // Default constructor, initializes with default values
    IndexValue();

    // Constructor with pageId, slotId, and key size
    IndexValue(int pageId, int slotId, int keyCount);

    // Constructor with pageId, slotId, and key values
    IndexValue(int pageId, int slotId, std::vector<int> keyValues);

    // Comparison operator for indexing (based on the key)
    bool operator<(const IndexValue& other){
        for (size_t i = 0; i < key.size(); ++i) {
            if (this->key[i] < other.key[i]) return true;
            if (this->key[i] > other.key[i]) return false;
        }
    return false;
    }

    bool operator>(const IndexValue& other){
        for (size_t i = 0; i < key.size(); ++i) {
            if (this->key[i] > other.key[i]) return true;
            if (this->key[i] < other.key[i]) return false;
        }
    return false;
    }


    // Prints the key values of the index
    void print() const;
};

// Converts a list of IndexValues to RecordLocation format
void indexValuesToRecordLocations(
    const std::vector<IndexValue>& indexValues,
    std::vector<record::RecordLocation>& recordLocations);

struct BPlusTreeInternalChild {
    std::vector<int> maxKey;  // Maximum key for the internal node
    int pageId;  // ID of the page

    // Default constructor
    BPlusTreeInternalChild();

    // Constructor with pageId and key count
    BPlusTreeInternalChild(int pageId, int keyCount);

    // Constructor with pageId and maximum keys
    BPlusTreeInternalChild(int pageId, std::vector<int> maxKeys);
};

struct BPlusTreeInternalNode {
    int prevPageId, nextPageId;  // IDs of previous and next pages
    std::list<BPlusTreeInternalChild> children;  // List of child nodes

    // Default constructor
    BPlusTreeInternalNode();

    // Constructor with previous and next page IDs
    BPlusTreeInternalNode(int prevPageId, int nextPageId);
};

struct BPlusTreeLeafChild {
    std::vector<int> key;  // Key for the leaf node
    int pageId, slotId;  // Page and slot IDs

    // Constructor from an IndexValue
    BPlusTreeLeafChild(IndexValue indexValue);

    // Constructor with pageId, slotId, and key count
    BPlusTreeLeafChild(int pageId, int slotId, int keyCount);

    // Comparison operators
    bool operator<(const BPlusTreeLeafChild& other) const {
        for (size_t i = 0; i < this->key.size(); ++i) {
            if (this->key[i] < other.key[i]) return true;
            if (this->key[i] > other.key[i]) return false;
        }
        return false;
    }

    // Comparison operator with an internal child
    bool operator<(const BPlusTreeInternalChild& other) const {
        for (size_t i = 0; i < this->key.size(); ++i) {
            if (this->key[i] < other.maxKey[i]) {
                return true;
            }
            if (this->key[i] > other.maxKey[i]) {
                return false;
            }
        }
        return false;
    }
    // compare with a leaf child 
    bool operator>(const BPlusTreeLeafChild& other) const {
        for (size_t i = 0; i < this->key.size(); ++i) {
            if (this->key[i] > other.key[i]) return true;
            if (this->key[i] < other.key[i]) return false;
        }
        return false;
    }

    bool operator<=(const BPlusTreeLeafChild& other) const {
        for (size_t i = 0; i < this->key.size(); ++i) {
            if (this->key[i] < other.key[i]) return true;
            if (this->key[i] > other.key[i]) return false;
        }
        return true;
    }

    // Comparison with a internal child
    bool operator<=(const BPlusTreeInternalChild& other) const {
        for (size_t i = 0; i < this->key.size(); ++i) {
            if (this->key[i] < other.maxKey[i]) {
                return true;
            }
            if (this->key[i] > other.maxKey[i]){
                return false;
            }
        }
        return true;
    }

    bool operator>=(const BPlusTreeLeafChild& other) const {
        for (size_t i = 0; i < this->key.size(); ++i) {
            if (this->key[i] > other.key[i]) return true;
            if (this->key[i] < other.key[i]) return false;
        }
        return true;
    }

    bool operator==(const BPlusTreeLeafChild& other) const {
        for (size_t i = 0; i < this->key.size(); ++i) {
            if (this->key[i] != other.key[i]) return false;
        }
        return true;
    }
    // Exact match between leaf nodes
    bool exactMatch(const BPlusTreeLeafChild& other) const;
};

struct BPlusTreeLeafNode {
    int prevPageId, nextPageId;  // IDs of previous and next pages
    std::list<BPlusTreeLeafChild> children;  // List of child leaf nodes

    // Default constructor
    BPlusTreeLeafNode();

    // Constructor with previous and next page IDs
    BPlusTreeLeafNode(int prevPageId, int nextPageId);
};

}  // namespace index
}  // namespace dbs
