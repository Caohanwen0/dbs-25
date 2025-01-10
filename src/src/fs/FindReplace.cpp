#include "fs/FindReplace.hpp"

namespace dbs {
namespace fs {


bool FindReplace::updateCapacity(int capacity_) {
    if (capacity_ < capacity) {
        return false;
    }
    utils::LinkedList<int> *newList = new utils::LinkedList<int>();
    utils::LinkedListNode<int> **newNode = new utils::LinkedListNode<int>*[capacity_];
    for (int i = 0; i < capacity; i++) {
        newList->insertAtHead(i);
        newNode[i] = newList->getHeadNode();
    }
    delete list;
    delete[] node;
    list = newList;
    node = newNode;
    capacity = capacity_;
    return true;
}


FindReplace::FindReplace(int capacity_) {
    capacity = capacity_;  // 设置容量
    list = new utils::LinkedList<int>();
    node = new utils::LinkedListNode<int>*[capacity];
    for (int i = 0; i < capacity; i++) {
        list->insertAtHead(i);
        node[i] = list->getHeadNode();
    }
}

FindReplace::~FindReplace() {
    delete list;
    for (int i = 0; i < capacity; i++){
        node[i] = nullptr;
    }
    delete[] node;
}

int FindReplace::find() {
    int index = list->getTailNode()->value;
    list->deleteNode(node[index]);
    node[index] = nullptr;
    list->insertAtHead(index);
    node[index] = list->getHeadNode();
    return index;
}

void FindReplace::access(int index) {
    list->deleteNode(node[index]);
    node[index] = nullptr;
    list->insertAtHead(index);
    node[index] = list->getHeadNode();
}

void FindReplace::free(int index) {
    list->deleteNode(node[index]);
    node[index] = nullptr;
    list->insertAtTail(index);
    node[index] = list->getTailNode();
}

}  // namespace fs
}  // namespace dbs