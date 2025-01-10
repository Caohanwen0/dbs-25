#include "index/IndexManager.hpp"

namespace dbs {
namespace index {

IndexManager::IndexManager(fs::FileManager* fm_, fs::BufPageManager* bpm_) {
    fm = fm_;
    bpm = bpm_;
    current_opening_file_paths.clear();
    current_opening_file_ids.clear();
}

IndexManager::~IndexManager() {
    closeAllCurrentFile();
    fm = nullptr;
    bpm = nullptr;
}

void IndexManager::closeAllCurrentFile() {
    bpm->closeManager();
    for (auto& file_path : current_opening_file_paths) {
        delete[] file_path;
        file_path = nullptr;
    }
    current_opening_file_paths.clear();
    for (auto& file_id : current_opening_file_ids) {
        fm->closeFile(file_id);
    }
    current_opening_file_ids.clear();
}

void IndexManager::closeFileIfOpen(const char* file_path) {
    int current_opening_file_num = current_opening_file_paths.size();
    for (int i = 0; i < current_opening_file_num; i++) {
        if (strcmp(current_opening_file_paths[i], file_path) == 0) {
            int file_id = current_opening_file_ids[i];
            bpm->closeManager();
            fm->closeFile(file_id);
            delete[] current_opening_file_paths[i];
            current_opening_file_paths[i] = nullptr;
            current_opening_file_paths.erase(
                current_opening_file_paths.begin() + i);
            current_opening_file_ids.erase(current_opening_file_ids.begin() +
                                           i);
            return;
        }
    }
}

void IndexManager::closeFirstFile() {
    if (current_opening_file_ids.size() == 0) return;
    int file_id = current_opening_file_ids.front();
    current_opening_file_ids.erase(current_opening_file_ids.begin());
    bpm->closeManager();
    fm->closeFile(file_id);
    delete[] current_opening_file_paths.front();
    current_opening_file_paths.front() = nullptr;
    current_opening_file_paths.erase(current_opening_file_paths.begin());
}

int IndexManager::openFile(const char* file_path) {
    int current_opening_file_num = current_opening_file_paths.size();
    for (int i = 0; i < current_opening_file_num; i++) {
        if (strcmp(current_opening_file_paths[i], file_path) == 0) {
            return current_opening_file_ids[i];
        }
    }
    if (current_opening_file_num == cacheCapacity) {
        closeFirstFile();
    }
    current_opening_file_paths.push_back(new char[strlen(file_path) + 1]);
    strcpy(current_opening_file_paths.back(), file_path);
    int file_id = fm->openFile(file_path);
    current_opening_file_ids.push_back(file_id);
    return file_id;
}

void IndexManager::createEmptyBitMapPage(int file_id, int pageId) {
    BufType b;
    int index;

    b = bpm->getPage(file_id, pageId, index);
    memset(b, 0, PAGE_SIZE_BY_BYTE);
    b[BUF_PER_PAGE - 1] = -1;  // 目前没有下一页
    bpm->markPageDirty(index);
}

bool IndexManager::setBitMapPage(int file_id, int base_pageId, int insert_pos,
                                 bool insert_val) {
    if (base_pageId == -1) {
        return false;
    }
    BufType b;
    int index;
    b = bpm->getPage(file_id, base_pageId, index);
    if (insert_pos < INDEX_BITMAP_PAGE_BYTE_LEN * BIT_PER_BYTE) {
        // 在本页
        utils::setBitInBuffer(b, insert_pos, insert_val);
        bpm->markPageDirty(index);
        return true;
    }
    // 递归向后查找
    bpm->accessPage(index);
    return setBitMapPage(file_id, b[BUF_PER_PAGE - 1],
                         insert_pos - INDEX_BITMAP_PAGE_BYTE_LEN * BIT_PER_BYTE,
                         insert_val);
}

int IndexManager::getFirstEmptyPageId(int file_id, bool set) {
    BufType b;
    int index;
    int offset = 0;
    int base_pageId = 1;
    while (base_pageId != -1) {
        b = bpm->getPage(file_id, base_pageId, index);
        bpm->accessPage(index);
        for (int i = 0; i < BUF_PER_PAGE - 1; i++) {
            if (b[i] != 0xffffffff) {
                int j = utils::findFirstZeroBit(b[i]);
                if (set) {
                    utils::setBitInNumber(b[i], j, true);
                    bpm->markPageDirty(index);
                }
                return (i << LOG_BIT_PER_BUF) + j + offset;
            }
        }
        offset += (INDEX_BITMAP_PAGE_BYTE_LEN << LOG_BIT_PER_BYTE);
        base_pageId = b[BUF_PER_PAGE - 1];
    }
    b[BUF_PER_PAGE - 1] = offset;
    bpm->markPageDirty(index);
    createEmptyBitMapPage(file_id, offset);
    setBitMapPage(file_id, offset, 0, true);
    if (set) setBitMapPage(file_id, offset, 1, true);  // 第一个bitmap页
    return offset + 1;
}

int IndexManager::getBPlusTreeM(int index_key_num) {
    return (PAGE_SIZE_BY_BYTE - INDEX_HEADER_BYTE_LEN) /
               getBPlusTreeItemLength(index_key_num) -
           1;
}

int IndexManager::getBPlusTreeItemLength(int index_key_num) {
    return (index_key_num + 2) << LOG_BYTE_PER_BUF;
}

void IndexManager::writeBPlusTreeInternalNode2Page(
    BufType b, const BPlusTreeInternalNode& node, int index_key_num) {
    b[0] = node.prevPageId ;     // 上一个同级节点页号
    b[1] = node.nextPageId;     // 下一个同级节点页号
    b[2] = node.children.size();  // 子节点数量
    b[3] = 0;                     // 是否是叶节点
    int bufferCur = INDEX_HEADER_BYTE_LEN >> LOG_BYTE_PER_BUF;
    for (auto& child : node.children) {
        b[bufferCur] = child.pageId;
        ++ bufferCur;
        for (int i = 0; i < index_key_num; i++) {
            b[bufferCur] = child.maxKey[i];
            ++ bufferCur;
        }
    }
}

void IndexManager::readBPlusTreeInternalNodeFromPage(
    BufType b, BPlusTreeInternalNode& node, int index_key_num) {
    node.prevPageId  = b[0];  // 上一个同级节点页号
    node.nextPageId = b[1];  // 下一个同级节点页号
    int children_num = b[2];   // 子节点数量
    assert(b[3] == 0);         // 是否是叶节点
    node.children.clear();
    int buf_offset = INDEX_HEADER_BYTE_LEN >> LOG_BYTE_PER_BUF;
    for (int i = 0; i < children_num; i++) {
        BPlusTreeInternalChild child(b[buf_offset++], index_key_num);
        for (int j = 0; j < index_key_num; j++) {
            child.maxKey[j] = b[buf_offset++];
        }
        node.children.push_back(child);
    }
}

void IndexManager::writeBPlusTreeLeafNode2Page(BufType b,
                                               const BPlusTreeLeafNode& node,
                                               int index_key_num) {
    b[0] = node.prevPageId ;     // 上一个同级节点页号
    b[1] = node.nextPageId;     // 下一个同级节点页号
    b[2] = node.children.size();  // 子节点数量
    b[3] = 1;                     // 是否是叶节点
    int buf_offset = INDEX_HEADER_BYTE_LEN >> LOG_BYTE_PER_BUF;
    for (auto& child : node.children) {
        b[buf_offset++] = child.pageId;
        b[buf_offset++] = child.slotId;
        for (int i = 0; i < index_key_num; i++) {
            b[buf_offset++] = child.key[i];
        }
    }
}

void IndexManager::readBPlusTreeLeafNodeFromPage(BufType b,
                                                 BPlusTreeLeafNode& node,
                                                 int index_key_num) {
    node.prevPageId  = b[0];  // 上一个同级节点页号
    node.nextPageId = b[1];  // 下一个同级节点页号
    int children_num = b[2];   // 子节点数量
    assert(b[3] == 1);         // 是否是叶节点
    node.children.clear();
    int buf_offset = INDEX_HEADER_BYTE_LEN >> LOG_BYTE_PER_BUF;
    for (int i = 0; i < children_num; i++) {
        BPlusTreeLeafChild child(b[buf_offset], b[buf_offset + 1],
                                 index_key_num);
        buf_offset += 2;
        for (int j = 0; j < index_key_num; j++) {
            child.key[j] = b[buf_offset++];
        }
        node.children.push_back(child);
    }
}

void IndexManager::setPrevPageID(int file_id, int pageId, int prevPageId ) {
    BufType b;
    int index;
    b = bpm->getPage(file_id, pageId, index);
    b[0] = prevPageId ;
    bpm->markPageDirty(index);
}

void IndexManager::setNextPageID(int file_id, int pageId, int nextPageId) {
    BufType b;
    int index;
    b = bpm->getPage(file_id, pageId, index);
    b[1] = nextPageId;
    bpm->markPageDirty(index);
}

void IndexManager::setLeafChildSlotIDAndPageID(int file_id, int pageId,
                                               int child_id, int itemPageId,
                                               int itemSlotId,
                                               int index_key_num) {
    BufType b;
    int index;
    b = bpm->getPage(file_id, pageId, index);
    int buf_offset = INDEX_HEADER_BYTE_LEN >> LOG_BYTE_PER_BUF;
    buf_offset += child_id * (2 + index_key_num);
    b[buf_offset++] = itemPageId;
    b[buf_offset++] = itemSlotId;
    bpm->markPageDirty(index);
}

void IndexManager::initializeIndexFile(const char* file_path,
                                       int index_key_num) {
    assert(index_key_num > 0);
    closeFileIfOpen(file_path);
    if (fm->doesFileExist(file_path)) {
        assert(fm->deleteFile(file_path));
    }
    assert(fm->createFile(file_path));
    int file_id = openFile(file_path);
    assert(file_id != -1);

    BufType b;
    int index;

    b = bpm->getPage(file_id, 0, index);
    b[0] = index_key_num;  // index key num
    bpm->markPageDirty(index);

    // 第1页开始是使用页面的bitmap
    createEmptyBitMapPage(file_id, 1);
    setBitMapPage(file_id, 1, 0, true);  // 首页
    setBitMapPage(file_id, 1, 1, true);  // 第一个bitmap页

    // 创建根节点和叶节点
    int root_pageId = getFirstEmptyPageId(file_id, true);
    int leaf_pageId = getFirstEmptyPageId(file_id, true);

    b = bpm->getPage(file_id, 0, index);
    b[1] = root_pageId;
    bpm->markPageDirty(index);

    // 初始化叶节点
    BPlusTreeLeafNode leaf_node(-1, -1);
    BPlusTreeInternalNode root_node(-1, -1);
    root_node.children.push_back(
        BPlusTreeInternalChild(leaf_pageId, index_key_num));

    b = bpm->getPage(file_id, leaf_pageId, index);
    writeBPlusTreeLeafNode2Page(b, leaf_node, index_key_num);
    bpm->markPageDirty(index);

    b = bpm->getPage(file_id, root_pageId, index);
    writeBPlusTreeInternalNode2Page(b, root_node, index_key_num);
    bpm->markPageDirty(index);
}

bool IndexManager::insertIndex(const char* file_path,
                               const IndexValue& index_value) {
    // open file
    int file_id = openFile(file_path);
    assert(file_id != -1);

    // meta info
    BufType b;
    int index;
    b = bpm->getPage(file_id, 0, index);
    int index_key_num = b[0];
    int root_pageId = b[1];
    bpm->accessPage(index);
    int m = getBPlusTreeM(index_key_num);

    // check validity
    if (index_value.key.size() != (size_t)index_key_num) return false;
    if (root_pageId == -1) return false;

    // insert
    BPlusTreeLeafChild leaf_child(index_value);
    insertNode(file_id, root_pageId, leaf_child, index_key_num, m);

    // 处理根节点上溢
    if (tmp_tree_internal_child[0].pageId != -1) {
        int new_root_pageId = getFirstEmptyPageId(file_id, true);
        BPlusTreeInternalNode new_root(-1, -1);
        new_root.children.push_back(tmp_tree_internal_child[0]);
        new_root.children.push_back(tmp_tree_internal_child[1]);

        b = bpm->getPage(file_id, new_root_pageId, index);
        writeBPlusTreeInternalNode2Page(b, new_root, index_key_num);
        bpm->markPageDirty(index);

        b = bpm->getPage(file_id, 0, index);
        b[1] = new_root_pageId;
        bpm->markPageDirty(index);
    }

    // 返回
    return true;
}

bool IndexManager::internalNodeOverflow_(
    int file_id, int pageId, BPlusTreeInternalNode& node,
    const std::list<dbs::index::BPlusTreeInternalChild>::iterator& insert_pos,
    int index_key_num, int b_plus_tree_m) {
    // 子节点是否上溢
    if (tmp_tree_internal_child[0].pageId == -1) {
        return false;  // 当前节点不需要更新
    }

    // 因为子节点上溢了，更新当前节点
    *insert_pos = tmp_tree_internal_child[0];
    node.children.insert(std::next(insert_pos), tmp_tree_internal_child[1]);

    // 当前节点是否上溢
    if (node.children.size() <= (size_t)b_plus_tree_m) {
        // 当前节点没有上溢
        tmp_tree_internal_child[0].pageId = -1;
        tmp_tree_internal_child[1].pageId = -1;
    } else {
        // 当前节点上溢

        // 一分为二，插入新节点
        int new_node_pageId = getFirstEmptyPageId(file_id, true);
        BPlusTreeInternalNode new_node;
        splitInternalNode(node, new_node, pageId, new_node_pageId,
                          index_key_num, b_plus_tree_m);

        // 当前节点的下一个同级页面，修改指针
        if (new_node.nextPageId != -1) {
            setPrevPageID(file_id, new_node.nextPageId, new_node_pageId);
        }

        // 新节点写入文件
        BufType b;
        int index;
        b = bpm->getPage(file_id, new_node_pageId, index);
        writeBPlusTreeInternalNode2Page(b, new_node, index_key_num);
        bpm->markPageDirty(index);
    }
    return true;  // 当前节点需要更新
}

void IndexManager::splitInternalNode(BPlusTreeInternalNode& origin_node,
                                     BPlusTreeInternalNode& new_node,
                                     int origin_node_pageId,
                                     int new_node_pageId, int index_key_num,
                                     int b_plus_tree_m) {
    // 链表连接
    new_node.prevPageId  = origin_node_pageId;
    new_node.nextPageId = origin_node.nextPageId;
    origin_node.nextPageId = new_node_pageId;

    // 切分children
    auto middle =
        std::next(origin_node.children.begin(), (b_plus_tree_m + 1) / 2);
    new_node.children.splice(new_node.children.begin(), origin_node.children,
                             middle, origin_node.children.end());

    // 设置tmp_tree_internal_child
    tmp_tree_internal_child[0] = BPlusTreeInternalChild(
        origin_node_pageId, origin_node.children.back().maxKey);
    tmp_tree_internal_child[1] = BPlusTreeInternalChild(
        new_node_pageId, new_node.children.back().maxKey);

    return;
}

void IndexManager::leafNodeOverflow_(int file_id, int pageId,
                                          BPlusTreeLeafNode& node,
                                          int index_key_num,
                                          int b_plus_tree_m) {
    if (node.children.size() <= (size_t)b_plus_tree_m) {
        // 当前节点没有上溢
        tmp_tree_internal_child[0].pageId = -1;
        tmp_tree_internal_child[1].pageId = -1;
        return;
    }
    // 当前节点上溢

    // 一分为二，插入新节点
    int new_node_pageId = getFirstEmptyPageId(file_id, true);
    BPlusTreeLeafNode new_node;
    splitLeafNode(node, new_node, pageId, new_node_pageId, index_key_num,
                  b_plus_tree_m);

    // 当前页面的下一个同级页面，修改指针
    if (new_node.nextPageId != -1) {
        setPrevPageID(file_id, new_node.nextPageId, new_node_pageId);
    }

    // 新节点写入文件
    BufType b;
    int index;
    b = bpm->getPage(file_id, new_node_pageId, index);
    writeBPlusTreeLeafNode2Page(b, new_node, index_key_num);
    bpm->markPageDirty(index);
}

void IndexManager::splitLeafNode(BPlusTreeLeafNode& origin_node,
                                 BPlusTreeLeafNode& new_node,
                                 int origin_node_pageId, int new_node_pageId,
                                 int index_key_num, int b_plus_tree_m) {
    // 链表连接
    new_node.prevPageId  = origin_node_pageId;
    new_node.nextPageId = origin_node.nextPageId;
    origin_node.nextPageId = new_node_pageId;

    // 切分children
    auto middle =
        std::next(origin_node.children.begin(), (b_plus_tree_m + 1) / 2);
    new_node.children.splice(new_node.children.begin(), origin_node.children,
                             middle, origin_node.children.end());

    // 设置tmp_tree_internal_child
    tmp_tree_internal_child[0] = BPlusTreeInternalChild(
        origin_node_pageId, origin_node.children.back().key);
    tmp_tree_internal_child[1] =
        BPlusTreeInternalChild(new_node_pageId, new_node.children.back().key);

    return;
}

void IndexManager::insertNode(int file_id, int pageId,
                              const BPlusTreeLeafChild& insert_item,
                              int index_key_num, int b_plus_tree_m) {
    BufType b;
    int index;
    b = bpm->getPage(file_id, pageId, index);
    bool is_leaf = b[3];
    if (is_leaf) {
        // 如果是叶节点

        // 读取节点
        BPlusTreeLeafNode node;
        readBPlusTreeLeafNodeFromPage(b, node, index_key_num);
        bpm->accessPage(index);

        // 插入
        auto child_itr = node.children.begin();
        for (; child_itr != node.children.end(); child_itr++) {
            if (insert_item <= *child_itr) {
                node.children.insert(child_itr, insert_item);
                break;
            }
        }
        if (child_itr == node.children.end()) {
            node.children.push_back(insert_item);
        }

        // 检查overflow
        leafNodeOverflow_(file_id, pageId, node, index_key_num,
                               b_plus_tree_m);

        // 写回
        b = bpm->getPage(file_id, pageId, index);
        writeBPlusTreeLeafNode2Page(b, node, index_key_num);
        bpm->markPageDirty(index);
        return;
    } else {
        // 不是叶节点

        // 读取节点
        BPlusTreeInternalNode node;
        readBPlusTreeInternalNodeFromPage(b, node, index_key_num);
        bpm->accessPage(index);

        // 插入
        for (auto child_itr = node.children.begin();
             child_itr != node.children.end(); child_itr++) {
            if (insert_item <= *child_itr) {
                // 被插入节点小于max，递归插入
                insertNode(file_id, child_itr->pageId, insert_item,
                           index_key_num, b_plus_tree_m);

                // 处理overflow
                if (internalNodeOverflow_(file_id, pageId, node,
                                               child_itr, index_key_num,
                                               b_plus_tree_m)) {
                    // 如有必要，写回
                    b = bpm->getPage(file_id, pageId, index);
                    writeBPlusTreeInternalNode2Page(b, node, index_key_num);
                    bpm->markPageDirty(index);
                }
                return;
            }
        }
        // 被插入节点大于max，更新max
        node.children.back().maxKey = insert_item.key;

        // 插入
        insertNode(file_id, node.children.back().pageId, insert_item,
                   index_key_num, b_plus_tree_m);

        // 处理overflow
        internalNodeOverflow_(file_id, pageId, node,
                                   std::prev(node.children.end()),
                                   index_key_num, b_plus_tree_m);

        // 写回
        b = bpm->getPage(file_id, pageId, index);
        writeBPlusTreeInternalNode2Page(b, node, index_key_num);
        bpm->markPageDirty(index);
    }
}

bool IndexManager::searchIndex(const char* file_path,
                               const IndexValue& search_value,
                               std::vector<IndexValue>& search_results) {
    return searchIndexInRanges(file_path, search_value, search_value,
                               search_results);
}

bool IndexManager::searchIndexInRanges(
    const char* file_path, const IndexValue& search_value_low,
    const IndexValue& search_value_high,
    std::vector<IndexValue>& search_results) {
    search_results.clear();

    // open file
    int file_id = openFile(file_path);
    assert(file_id != -1);

    // meta info
    BufType b;
    int index;
    b = bpm->getPage(file_id, 0, index);
    int index_key_num = b[0];
    int root_pageId = b[1];
    bpm->accessPage(index);

    // check validity
    if (search_value_low.key.size() != (size_t)index_key_num) return false;
    if (search_value_high.key.size() != (size_t)index_key_num) return false;

    BPlusTreeLeafNode leaf_result;
    if (!searchLeafNode(file_id, root_pageId, BPlusTreeLeafChild(search_value_low),
                    index_key_num, leaf_result)) {
        return true;
    }

    searchForward(file_id, leaf_result, index_key_num,
                  BPlusTreeLeafChild(search_value_low),
                  BPlusTreeLeafChild(search_value_high), search_results);

    return true;
}

// Finds the leaf node in the BPlusTree that contains the search_value.
// Recursively searches through internal nodes until it finds the correct leaf node.
bool IndexManager::searchLeafNode(int file_id, int pageId,
                              const BPlusTreeLeafChild& search_value,
                              int index_key_num,
                              BPlusTreeLeafNode& leaf_result) {
    BufType b;
    int index;
    b = bpm->getPage(file_id, pageId, index);
    bool is_leaf = b[3];  // Check if the current node is a leaf.
    if (is_leaf) {
        // If it's a leaf node, read the leaf node and search for the value.
        BPlusTreeLeafNode node;
        readBPlusTreeLeafNodeFromPage(b, node, index_key_num);
        bpm->accessPage(index);

        // If search_value is less than or equal to the last child, return the leaf node.
        if (node.children.size() > 0 && search_value <= node.children.back()) {
            leaf_result = node;
            return true;
        }

        // Not found
        return false;
    } else {
        // If it's not a leaf node, recurse into the appropriate child node.
        BPlusTreeInternalNode node;
        readBPlusTreeInternalNodeFromPage(b, node, index_key_num);
        bpm->accessPage(index);

        // Search through the internal node's children for the appropriate child to search.
        for (auto& child : node.children) {
            if (search_value <= child) {
                return searchLeafNode(file_id, child.pageId, search_value,
                                  index_key_num, leaf_result);
            }
        }

        // Not found
        return false;
    }
}


void IndexManager::searchForward(int file_id,
                                 const BPlusTreeLeafNode& base_node,
                                 int index_key_num,
                                 const BPlusTreeLeafChild& search_value_low,
                                 const BPlusTreeLeafChild& search_value_high,
                                 std::vector<IndexValue>& search_results) {
    assert(base_node.children.size() > 0); // Ensure the base node is not empty.

    // Search through children of base_node for values between search_value_low and search_value_high.
    for (auto itr = base_node.children.begin(); itr != base_node.children.end(); itr++) {
        if (search_value_low <= *itr && *itr <= search_value_high) {
            search_results.push_back(
                IndexValue(itr->pageId, itr->slotId, itr->key));
        } else if (search_value_high < *itr) {
            return;  // Stop if we have passed the search range.
        }
    }

    // If the search range is not fully covered, continue searching on the next page.
    if (base_node.nextPageId == -1) return;
    BufType b;
    int index;
    b = bpm->getPage(file_id, base_node.nextPageId, index);
    BPlusTreeLeafNode next_node;
    readBPlusTreeLeafNodeFromPage(b, next_node, index_key_num);
    bpm->accessPage(index);

    // Recursive call to search the next page.
    searchForward(file_id, next_node, index_key_num, search_value_low,
                  search_value_high, search_results);
}

// Deletes an index entry from the BPlusTree corresponding to the given index_value.
bool IndexManager::deleteIndex(const char* file_path,
                               const IndexValue& index_value,
                               bool exactMatch) {
    int file_id = openFile(file_path);
    assert(file_id != -1);

    // Retrieve meta information about the tree.
    BufType b;
    int index;
    b = bpm->getPage(file_id, 0, index);
    int index_key_num = b[0];
    int root_pageId = b[1];
    bpm->accessPage(index);
    int m = getBPlusTreeM(index_key_num);

    // Check for validity of the index_value.
    if (index_value.key.size() != (size_t)index_key_num) return false;

    // Delegate deletion to the appropriate node deletion function.
    return deleteNode(file_id, root_pageId, BPlusTreeLeafChild(index_value),
                      exactMatch, index_key_num, m);
}


bool IndexManager::deleteNode(int file_id, int pageId,
                              const BPlusTreeLeafChild& delete_value,
                              bool exactMatch, int index_key_num,
                              int b_plus_tree_m) {
    BufType b;
    int index;
    b = bpm->getPage(file_id, pageId, index);
    bool is_leaf = b[3];

    if (is_leaf) {
        // 如果是叶节点

        // 读取节点
        BPlusTreeLeafNode node;
        readBPlusTreeLeafNodeFromPage(b, node, index_key_num);
        bpm->accessPage(index);

        bool found = false;
        bool update_max_val = false;
        for (auto itr = node.children.begin(); itr != node.children.end();
             itr++) {
            if (delete_value == *itr) {
                if (exactMatch && !delete_value.exactMatch(*itr)) {
                    bool found_exact = false;
                    for (auto exact_itr = itr; exact_itr != node.children.end();
                         exact_itr++) {
                        if (delete_value.exactMatch(*exact_itr)) {
                            (*exact_itr).slotId = (*itr).slotId;
                            (*exact_itr).pageId = (*itr).pageId;
                            found_exact = true;
                            break;
                        }
                    }
                    if (!found_exact) {
                        BPlusTreeLeafNode exact_node = node;
                        int exact_pageId = pageId, child_itemSlotId = 0;
                        std::list<BPlusTreeLeafChild>::iterator exact_itr;
                        while (!found_exact) {
                            if (exact_node.nextPageId == -1) {
                                break;
                            }
                            exact_pageId = exact_node.nextPageId;
                            b = bpm->getPage(file_id, exact_node.nextPageId,
                                             index);
                            readBPlusTreeLeafNodeFromPage(b, exact_node,
                                                          index_key_num);
                            bpm->accessPage(index);
                            bool failed = false;
                            child_itemSlotId = 0;
                            for (exact_itr = exact_node.children.begin();
                                 exact_itr != exact_node.children.end();
                                 exact_itr++, child_itemSlotId++) {
                                if (delete_value.exactMatch(*exact_itr)) {
                                    found_exact = true;
                                    break;
                                } else if (delete_value < *exact_itr) {
                                    failed = true;
                                    break;
                                }
                            }
                            if (found_exact) break;
                            if (failed) break;
                        }
                        if (!found_exact) {
                            // 没找到
                            return false;
                        }

                        // 写回
                        setLeafChildSlotIDAndPageID(
                            file_id, exact_pageId, child_itemSlotId,
                            itr->pageId, itr->slotId, index_key_num);
                    }
                }
                // 删掉找到的位置
                found = true;
                update_max_val = (std::next(itr) == node.children.end());
                node.children.erase(itr);
                break;
            } else if (delete_value < *itr) {
                break;
            }
        }
        if (!found) {
            // 没找到
            return false;
        }
        tmp_tree_underflow = false;
        if (node.nextPageId != -1 &&
            (int)node.children.size() < (b_plus_tree_m + 1) / 2) {
            // 出现下溢
            b = bpm->getPage(file_id, node.nextPageId, index);
            BPlusTreeLeafNode next_node;
            readBPlusTreeLeafNodeFromPage(b, next_node, index_key_num);
            bpm->accessPage(index);

            if ((int)next_node.children.size() + (int)node.children.size() <=
                b_plus_tree_m) {
                // 合并节点
                tmp_tree_underflow = true;
                update_max_val = false;
                next_node.children.splice(next_node.children.begin(),
                                          node.children);

                // 修改链表
                next_node.prevPageId  = node.prevPageId ;
                if (next_node.prevPageId  != -1) {
                    setNextPageID(file_id, next_node.prevPageId ,
                                  node.nextPageId);
                }

                // next node 写回
                b = bpm->getPage(file_id, node.nextPageId, index);
                writeBPlusTreeLeafNode2Page(b, next_node, index_key_num);
                bpm->markPageDirty(index);

                // 删除节点
                setBitMapPage(file_id, 1, pageId, false);
            } else {
                // 借一个节点
                node.children.push_back(next_node.children.front());
                next_node.children.pop_front();
                update_max_val = true;

                // next node 写回
                b = bpm->getPage(file_id, node.nextPageId, index);
                writeBPlusTreeLeafNode2Page(b, next_node, index_key_num);
                bpm->markPageDirty(index);
            }
        }

        if (node.nextPageId == -1 && node.children.size() == 0 &&
            node.prevPageId  != -1) {
            tmp_tree_underflow = true;
            update_max_val = false;

            setNextPageID(file_id, node.prevPageId , -1);

            setBitMapPage(file_id, 1, pageId, false);
        }
        if (update_max_val) {
            // 更新max值
            if (node.children.size() == 0) {
                tmp_tree_internal_child[0].maxKey.clear();
                for (int i = 0; i < index_key_num; i++)
                    tmp_tree_internal_child[0].maxKey.push_back(INT_MIN);
            } else {
                tmp_tree_internal_child[0].maxKey = node.children.back().key;
            }
            tmp_tree_internal_child[0].pageId = pageId;
        } else {
            tmp_tree_internal_child[0].pageId = -1;
        }

        // 写回
        if (!tmp_tree_underflow) {
            // underflow的情况不用写回
            b = bpm->getPage(file_id, pageId, index);
            writeBPlusTreeLeafNode2Page(b, node, index_key_num);
            bpm->markPageDirty(index);
        }

        return true;
    } else {
        // 不是叶节点

        // 读取节点
        BPlusTreeInternalNode node;
        readBPlusTreeInternalNodeFromPage(b, node, index_key_num);
        bpm->accessPage(index);

        // 搜索
        for (auto itr = node.children.begin(); itr != node.children.end();
             itr++) {
            if (delete_value <= *itr) {
                if (!deleteNode(file_id, (*itr).pageId, delete_value,
                                exactMatch, index_key_num, b_plus_tree_m)) {
                    // 没找到
                    return false;
                }
                if (tmp_tree_underflow) {
                    // 子节点踢出去
                    tmp_tree_underflow = false;
                    bool update_max_val =
                        (std::next(itr) == node.children.end());
                    node.children.erase(itr);
                    if ((int)node.children.size() < (b_plus_tree_m + 1) / 2 &&
                        node.nextPageId != -1) {
                        // 出现下溢
                        b = bpm->getPage(file_id, node.nextPageId, index);
                        BPlusTreeInternalNode next_node;
                        readBPlusTreeInternalNodeFromPage(b, next_node,
                                                          index_key_num);
                        bpm->accessPage(index);

                        if ((int)next_node.children.size() +
                                (int)node.children.size() <=
                            b_plus_tree_m) {
                            // 合并节点
                            tmp_tree_underflow = true;
                            update_max_val = false;
                            next_node.children.splice(
                                next_node.children.begin(), node.children);

                            // 修改链表
                            next_node.prevPageId  = node.prevPageId ;
                            if (next_node.prevPageId  != -1) {
                                setNextPageID(file_id, next_node.prevPageId ,
                                              node.nextPageId);
                            }

                            // next node 写回
                            b = bpm->getPage(file_id, node.nextPageId, index);
                            writeBPlusTreeInternalNode2Page(b, next_node,
                                                            index_key_num);
                            bpm->markPageDirty(index);

                            // 删除节点
                            setBitMapPage(file_id, 1, pageId, false);
                        } else {
                            node.children.push_back(next_node.children.front());
                            next_node.children.pop_front();
                            update_max_val = true;

                            // next node 写回
                            b = bpm->getPage(file_id, node.nextPageId, index);
                            writeBPlusTreeInternalNode2Page(b, next_node,
                                                            index_key_num);
                            bpm->markPageDirty(index);
                        }
                    }
                    if (node.nextPageId == -1 && node.children.size() == 0 &&
                        node.prevPageId  != -1) {
                        tmp_tree_underflow = true;
                        update_max_val = false;

                        setNextPageID(file_id, node.prevPageId , -1);

                        setBitMapPage(file_id, 1, pageId, false);
                    }
                    if (update_max_val) {
                        // 更新max值
                        tmp_tree_internal_child[0].maxKey =
                            node.children.back().maxKey;
                        tmp_tree_internal_child[0].pageId = pageId;
                    } else {
                        tmp_tree_internal_child[0].pageId = -1;
                    }

                    if (!tmp_tree_underflow) {
                        // 写回
                        b = bpm->getPage(file_id, pageId, index);
                        writeBPlusTreeInternalNode2Page(b, node, index_key_num);
                        bpm->markPageDirty(index);
                    }
                } else if (tmp_tree_internal_child[0].pageId != -1) {
                    // 更新max值
                    (*itr).maxKey = tmp_tree_internal_child[0].maxKey;
                    if (std::next(itr) == node.children.end()) {
                        // 更新max值
                        tmp_tree_internal_child[0].maxKey =
                            node.children.back().maxKey;
                        tmp_tree_internal_child[0].pageId = pageId;
                    } else {
                        tmp_tree_internal_child[0].pageId = -1;
                    }

                    // 写回
                    b = bpm->getPage(file_id, pageId, index);
                    writeBPlusTreeInternalNode2Page(b, node, index_key_num);
                    bpm->markPageDirty(index);
                }
                return true;
            }
        }

        // 没找到
        return false;
    }
}

bool IndexManager::deleteIndexFile(const char* file_path) {
    // 检查文件是否存在
    closeFileIfOpen(file_path);
    return fm->deleteFile(file_path);
}

}  // namespace index
}  // namespace dbs