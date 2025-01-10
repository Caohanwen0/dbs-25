#pragma once

#include "utils/LinkList.hpp"

namespace dbs {
namespace fs {

class FindReplace{
public:
    FindReplace(int capacity_);
    ~FindReplace();
    int find();
    void free(int index);
    void access(int index);

private:
    /**
     * @brief   
     * 
     * @param capacity_ 
     * @return true 
     * @return false 
     */
    bool updateCapacity(int capacity_);
    utils::LinkedList<int> *list;
    utils::LinkedListNode<int> **node;
    int capacity;
};

}  // namespace fs
}  // namespace dbs