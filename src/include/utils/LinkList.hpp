#pragma once

namespace dbs {
namespace utils {

/**
 * @brief A node in the doubly linked list.
 * Each node stores data of type T and pointers to both its next and previous nodes.
 */
template <typename T>
struct LinkedListNode {
    T value;  // Data stored in the node.
    LinkedListNode<T>* nextNode;  // Pointer to the next node in the list.
    LinkedListNode<T>* prevNode;  // Pointer to the previous node in the list.

    // Default constructor initializes next and prev pointers to nullptr.
    LinkedListNode() : nextNode(nullptr), prevNode(nullptr) {}

    // Constructor that initializes node with a given value.
    LinkedListNode(T nodeData) : value(nodeData), nextNode(nullptr), prevNode(nullptr) {}

    // Destructor sets next and prev pointers to nullptr before deletion.
    ~LinkedListNode() {
        nextNode = nullptr;
        prevNode = nullptr;
    }

    /**
     * @brief Deletes the current node from the list, adjusting links of neighboring nodes.
     */
    void deleteNode() {
        if (prevNode != nullptr) {
            prevNode->nextNode = nextNode;
        }
        if (nextNode != nullptr) {
            nextNode->prevNode = prevNode;
        }
        delete this;
    }
};

/**
 * @brief A doubly linked list that allows insertion at both ends, searching, and deletion of nodes.
 */
template <typename T>
class LinkedList {
   public:
    // Constructor initializes an empty list with head and tail pointers set to nullptr.
    LinkedList() : headNode(nullptr), tailNode(nullptr) {}

    // Destructor deletes all nodes from the list to free up memory
    ~LinkedList() {
        LinkedListNode<T>* currentNode = headNode;
        while (currentNode != nullptr) {
            LinkedListNode<T>* nextNode = currentNode->nextNode;
            delete currentNode;
            currentNode = nextNode;
        }
        headNode = tailNode = nullptr;
    }

    /**
     * @brief Inserts a new node with the given value at the beginning of the list.
     * @param nodeValue The value to be stored in the new node.
     */
    void insertAtHead(T nodeValue) {
        LinkedListNode<T>* newNode = new LinkedListNode<T>(nodeValue);
        if (headNode == nullptr) {
            headNode = tailNode = newNode;  // If list is empty, new node becomes both head and tail.
        } 
        else {
            newNode->nextNode = headNode;
            headNode->prevNode = newNode;
            headNode = newNode;  // Update head pointer to the new node.
        }
    }

    /**
     * @brief Inserts a new node with the given value at the end of the list.
     * @param nodeValue The value to be stored in the new node.
     */
    void insertAtTail(T nodeValue) {
        LinkedListNode<T>* newNode = new LinkedListNode<T>(nodeValue);
        if (tailNode == nullptr) {
            headNode = tailNode = newNode;  // If list is empty, new node becomes both head and tail.
        } 
        else {
            tailNode->nextNode = newNode;
            newNode->prevNode = tailNode;
            tailNode = newNode;  // Update tail pointer to the new node.
        }
    }

    /**
     * @brief Retrieves the first node (head) of the list.
     * @return Pointer to the head node.
     */
    LinkedListNode<T>* getHeadNode() const { return headNode; }

    /**
     * @brief Retrieves the last node (tail) of the list.
     * @return Pointer to the tail node.
     */
    LinkedListNode<T>* getTailNode() const { return tailNode; }

    /**
     * @brief Searches for a node containing the given value.
     * @param nodeValue The value to search for in the list.
     * @return Pointer to the node containing the value, or nullptr if not found.
     */
    LinkedListNode<T>* findNode(T nodeValue) {
        LinkedListNode<T>* currentNode = headNode;
        while (currentNode != nullptr) {
            if (currentNode->value == nodeValue) {
                return currentNode;  // Node found, return it.
            }
            currentNode = currentNode->nextNode;
        }
        return nullptr;  // Value not found, return nullptr.
    }

    /**
     * @brief Deletes the specified node from the list.
     * @param nodeToDelete Pointer to the node to be deleted.
     */
    void deleteNode(LinkedListNode<T>* nodeToDelete) {
        if (nodeToDelete == headNode) {
            headNode = nodeToDelete->nextNode;  // Move head pointer to next node if deleting head.
        }
        if (nodeToDelete == tailNode) {
            tailNode = nodeToDelete->prevNode;  // Move tail pointer to previous node if deleting tail.
        }
        nodeToDelete->deleteNode();  // Call deleteNode method of the node to remove it.
    }

   private:
    LinkedListNode<T>* headNode;  // Pointer to the first node in the list.
    LinkedListNode<T>* tailNode;  // Pointer to the last node in the list.
};

}  // namespace utils
}  // namespace dbs
