#pragma once

#include "common/Config.hpp"
#include "utils/LinkList.hpp"
#include <type_traits>

namespace dbs {
namespace utils {

// Base class for hash items
class HashItem {
public:
    // Pure virtual function to compute hash for derived classes
    virtual int computeHash() const = 0;
    
    virtual ~HashItem() {}
};

// Derived class for storing two integer keys and a value
class HashItemTwoInt : public HashItem {
public:
    int primaryKey, secondaryKey;  // Two integer keys
    int value;  // The value associated with the keys
    
    // Default constructor
    HashItemTwoInt() : primaryKey(0), secondaryKey(0), value(-1) {}
    
    // Constructor with keys and value
    HashItemTwoInt(int pKey, int sKey, int val) : primaryKey(pKey), secondaryKey(sKey), value(val) {}
    
    // Constructor with keys, default value is -1
    HashItemTwoInt(int pKey, int sKey) : primaryKey(pKey), secondaryKey(sKey), value(-1) {}
    
    // Function to compute the hash based on the keys
    int computeHash() const override {
        return (primaryKey * HASH_PRIME_1 + secondaryKey * HASH_PRIME_2) % HASH_MOD;
    }

    // Comparison operator for equality check
    friend bool operator==(const HashItemTwoInt& a, const HashItemTwoInt& b) {
        return a.primaryKey == b.primaryKey && a.secondaryKey == b.secondaryKey;
    }
};

template <typename T>
class HashMap {
public:
    // Constructor: Initializes the hash map with an array of linked lists
    HashMap() {
        hashTable = new LinkedList<T>[HASH_MOD];
    }

    // Destructor: Cleans up the dynamically allocated memory
    ~HashMap() {
        delete[] hashTable;
    }

    // Inserts a new item into the hash map
    void insertItem(const T& item) {
        int hashIndex = item.computeHash();
        hashTable[hashIndex].insertAtHead(item);
    }

    // Finds an item in the hash map
    T findItem(const T& targetItem) {
        int hashIndex = targetItem.computeHash();
        LinkedListNode<T>* node = hashTable[hashIndex].findNode(targetItem);
        
        if (node == nullptr)
            return T();  // Return default-constructed object if not found
        
        return node->value;  // Return the value of the found node
    }

    // Deletes an item from the hash map
    bool deleteItem(const T& targetItem) {
        int hashIndex = targetItem.computeHash();
        LinkedListNode<T>* node = hashTable[hashIndex].findNode(targetItem);
        
        if (node != nullptr) {
            hashTable[hashIndex].deleteNode(node);
            return true;
        }
        
        return false;  // Return false if item not found
    }

private:
    LinkedList<T>* hashTable;  // Array of linked lists to handle collisions
};

}  // namespace utils
}  // namespace dbs
