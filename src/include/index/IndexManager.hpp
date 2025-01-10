#pragma once

#include "common/Config.hpp"
#include "fs/BufPageManager.hpp"
#include "fs/FileManager.hpp"
#include "index/IndexType.hpp"
#include "record/DataType.hpp"
#include "utils/BitOperations.hpp"

namespace dbs {
namespace index {

class IndexManager {
   public:
    /**
     * @brief Constructor
     * @param fm_ File manager
     * @param bpm_ Buffer page manager
     */
    IndexManager(fs::FileManager* fm_, fs::BufPageManager* bpm_);

    /**
     * @brief Destructor
     */
    ~IndexManager();

    /**
     * @brief Create an index file
     * @param file_path Path of the index file
     * @param index_key_num Number of indexed columns (excluding page ID and slot ID)
     */
    void initializeIndexFile(const char* file_path, int index_key_num);

    /**
     * @brief Insert an index
     * @param file_path Index file path
     * @param index_value Index value
     * @return true on success, false on failure (key count mismatch)
     */
    bool insertIndex(const char* file_path, const IndexValue& index_value);\

    /**
     * @brief Delete an index (deletes the first match if multiple exist)
     * @param file_path Index file path
     * @param index_value Index value
     * @param exact_match true: match page/slot ID, false: match only keys
     * @return true on success, false on failure (key count mismatch or not found)
     */
    bool deleteIndex(const char* file_path, const IndexValue& index_value,\
                     bool exact_match);

    /**
     * @brief Search for an exact index match
     * @param file_path Index file path
     * @param search_value Search value
     * @param search_results Returned record locations
     * @return true if found, false on failure (key count mismatch)
     */
    bool searchIndex(const char* file_path, const IndexValue& search_value,\
                     std::vector<IndexValue>& search_results);

    /**
     * @brief Search for an index within a range (inclusive)
     * @param file_path Index file path
     * @param search_value_low Lower bound
     * @param search_value_high Upper bound
     * @param search_results Returned record locations
     * @return true if found, false on failure (key count mismatch)
     */
    bool searchIndexInRanges(const char* file_path,
                    const IndexValue& search_value_low,
                    const IndexValue& search_value_high,
                    std::vector<IndexValue>& search_results);

    /**
     * @brief Delete an index file
     * @param file_path Index file path
     * @return true on success, false on failure
     */
    bool deleteIndexFile(const char* file_path);

    /**
     * @brief Close all open files
     */
    void closeAllCurrentFile();

   private:
    /**
     * @brief Write a B+ tree internal node to a buffer
     * @param b Buffer
     * @param node Internal node
     * @param index_key_num Number of keys
     */
    void writeBPlusTreeInternalNode2Page(BufType b,
                        const BPlusTreeInternalNode& node,
                        int index_key_num);

    /**
     * @brief Write a B+ tree leaf node to a buffer
     * @param b Buffer
     * @param node Leaf node
     * @param index_key_num Number of keys
     */
    void writeBPlusTreeLeafNode2Page(BufType b, const BPlusTreeLeafNode& node,
                                     int index_key_num);

    /**
     * @brief Read a B+ tree internal node from a buffer
     * @param b Buffer
     * @param node Internal node
     * @param index_key_num Number of keys
     */
    void readBPlusTreeInternalNodeFromPage(BufType b,
                                           BPlusTreeInternalNode& node,
                                           int index_key_num);

    /**
     * @brief Read a B+ tree leaf node from a buffer
     * @param b Buffer
     * @param node Leaf node
     * @param index_key_num Number of keys
     */
    void readBPlusTreeLeafNodeFromPage(BufType b, BPlusTreeLeafNode& node,
                                       int index_key_num);

    /**
     * @brief Create an empty bitmap page
     * @param file_id File ID
     * @param page_id Page ID
     */
    void createEmptyBitMapPage(int file_id, int page_id);

    /**
     * @brief Set a specific bit in a bitmap page
     * @param file_id File ID
     * @param base_page_id Base page ID
     * @param insert_pos Position to set
     * @param insert_val Value to set
     * @return true on success, false on failure (bitmap too short)
     */
    bool setBitMapPage(int file_id, int base_page_id, int insert_pos,
                       bool insert_val);

    /**
     * @brief Find the first unused page and optionally mark it as used
     * @param file_id File ID
     * @param set Mark as used if true
     * @return Page ID
     */
    int getFirstEmptyPageId(int file_id, bool set);

    /**
     * @brief Get the B+ tree M value for a given key count
     * @param index_key_num Key count
     * @return M value
     */
    int getBPlusTreeM(int index_key_num);

    /**
     * @brief Get the record length for a B+ tree node
     * @param index_key_num Key count
     * @return Record length in bytes
     */
    int getBPlusTreeItemLength(int index_key_num);

    /**
     * @brief Open a file with caching
     * @param file_path File path
     * @return File ID
     */
    int openFile(const char* file_path);

    void closeFirstFile();
    void closeFileIfOpen(const char* file_path);

    /**
     * @brief Insert into a B+ tree node
     * @param file_id File ID
     * @param page_id Page ID
     * @param insert_item Item to insert
     * @param index_key_num Key count
     * @param b_plus_tree_m B+ tree M value
     */
    void insertNode(int file_id, int page_id,
                    const BPlusTreeLeafChild& insert_item, int index_key_num,
                    int b_plus_tree_m);

    /**
     * @brief Find the leaf node for a value
     * @param file_id File ID
     * @param page_id Page ID
     * @param search_value Value to search
     * @param leaf_result Leaf node result
     * @return true if found, false otherwise
     */
    bool searchLeafNode(int file_id, int page_id,
                      const BPlusTreeLeafChild& search_value, int index_key_num,
                      BPlusTreeLeafNode& leaf_result);

    bool deleteNode(int file_id, int page_id,
                    const BPlusTreeLeafChild& delete_value, bool exact_match,
                    int index_key_num, int b_plus_tree_m);

    /**
     * @brief Check for overflow in an internal node
     * @param file_id File ID
     * @param page_id Page ID
     * @param node Internal node
     * @param insert_pos Insertion position
     * @param index_key_num Key count
     * @param b_plus_tree_m B+ tree M value
     * @return true if overflow occurs
     */
    bool internalNodeOverflow_(
        int file_id, int page_id, BPlusTreeInternalNode& node,
        const std::list<BPlusTreeInternalChild>::iterator& insert_pos,
        int index_key_num, int b_plus_tree_m);

    /**
     * @brief Handle leaf node overflow
     */
    void leafNodeOverflow_(int file_id, int page_id,
                           BPlusTreeLeafNode& node, int index_key_num,
                           int b_plus_tree_m);

    /**
     * @brief Split an internal node
     */
    void splitInternalNode(BPlusTreeInternalNode& origin_node,
                           BPlusTreeInternalNode& new_node,
                           int origin_node_page_id, int new_node_page_id,
                           int index_key_num, int b_plus_tree_m);

    /**
     * @brief Split a leaf node
     */
    void splitLeafNode(BPlusTreeLeafNode& origin_node,
                       BPlusTreeLeafNode& new_node, int origin_node_page_id,
                       int new_node_page_id, int index_key_num,
                       int b_plus_tree_m);

    /**
     * @brief Update the prev pointer of a node
     */
    void setPrevPageID(int file_id, int page_id, int prev_page_id);

    /**
     * @brief Update the next pointer of a node
     */
    void setNextPageID(int file_id, int page_id, int next_page_id);

    /**
     * @brief Update slot and page IDs for a leaf node child
     */
    void setLeafChildSlotIDAndPageID(int file_id, int page_id, int child_id,
                                     int item_page_id, int item_slot_id,
                                     int index_key_num);

    /**
     * @brief Search forward for values in range
     */
    void searchForward(int file_id, const BPlusTreeLeafNode& base_node,
                       int index_key_num,
                       const BPlusTreeLeafChild& search_value_low,
                       const BPlusTreeLeafChild& search_value_high,
                       std::vector<IndexValue>& search_results);

    fs::FileManager* fm;
    fs::BufPageManager* bpm;

    std::vector<char*> current_opening_file_paths;
    std::vector<int> current_opening_file_ids;
    const int cacheCapacity = 10;

    BPlusTreeInternalChild tmp_tree_internal_child[2];
    bool tmp_tree_underflow = false;
};

}  // namespace index
}  // namespace dbs
