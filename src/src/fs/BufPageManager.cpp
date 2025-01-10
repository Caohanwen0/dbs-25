#include "fs/BufPageManager.hpp"

namespace dbs {
namespace fs {

BufPageManager::BufPageManager(FileManager* fileMgr) {
    fileManager = fileMgr;
    pageReplacementStrategy = new FindReplace(CACHE_CAPACITY);
    dirtyPageTracker = new utils::BitMap(CACHE_CAPACITY, false);
    pageBuffers = new BufType[CACHE_CAPACITY];
    for (int i = 0; i < CACHE_CAPACITY; ++i) {
        pageBuffers[i] = nullptr;
    }
    pageHashMap = new utils::HashMap<utils::HashItemTwoInt>();
    pageLocations = new PageLocation[CACHE_CAPACITY];
    lastAccessedPageIndex = -1;
}

BufPageManager::~BufPageManager() {
    fileManager = nullptr;
    delete pageReplacementStrategy;
    delete dirtyPageTracker;
    delete[] pageBuffers;
    delete pageHashMap;
    delete[] pageLocations;
}

BufType BufPageManager::allocateMemory() {
    return new unsigned int[(PAGE_SIZE_BY_BYTE >> 2)];
}

BufType BufPageManager::loadPage(int fileID, int pageID, int& pageIndex) {
    BufType buffer;
    pageIndex = pageReplacementStrategy->find();
    buffer = pageBuffers[pageIndex];

    if (buffer == nullptr) {
        buffer = allocateMemory();
        pageBuffers[pageIndex] = buffer;
    } else {
        if (dirtyPageTracker->getBit(pageIndex)) {
            fileManager->writePage(pageLocations[pageIndex].fileID,
                                   pageLocations[pageIndex].pageID, buffer, 0);
            dirtyPageTracker->setBit(pageIndex, false);
        }
        assert(pageHashMap->deleteItem(pageLocations[pageIndex].toHashItem()));
    }

    pageHashMap->insertItem(utils::HashItemTwoInt(fileID, pageID, pageIndex));
    pageLocations[pageIndex] = PageLocation(fileID, pageID);
    return buffer;
}

void BufPageManager::accessPage(int pageIndex) {
    if (pageIndex != lastAccessedPageIndex) {
        pageReplacementStrategy->access(pageIndex);
        lastAccessedPageIndex = pageIndex;
    }
}

BufType BufPageManager::getPage(int fileID, int pageID, int& pageIndex) {
    utils::HashItemTwoInt searchItem(fileID, pageID);
    searchItem = pageHashMap->findItem(searchItem);
    pageIndex = searchItem.value;

    if (pageIndex != -1) {
        accessPage(pageIndex);
        return pageBuffers[pageIndex];
    } else {
        BufType buffer = loadPage(fileID, pageID, pageIndex);
        fileManager->readPage(fileID, pageID, buffer, 0);
        return buffer;
    }
}

void BufPageManager::markPageDirty(int pageIndex) {
    dirtyPageTracker->setBit(pageIndex, true);
    accessPage(pageIndex);
}

void BufPageManager::flushPageToDisk(int pageIndex) {
    if (dirtyPageTracker->getBit(pageIndex)) {
        fileManager->writePage(pageLocations[pageIndex].fileID,
                               pageLocations[pageIndex].pageID, pageBuffers[pageIndex], 0);
        dirtyPageTracker->setBit(pageIndex, false);
    }
}

void BufPageManager::releasePage(int pageIndex) {
    flushPageToDisk(pageIndex);
    pageReplacementStrategy->free(pageIndex);
    pageHashMap->deleteItem(pageLocations[pageIndex].toHashItem());
}

void BufPageManager::closeManager() {
    for (int i = 0; i < CACHE_CAPACITY; ++i) {
        if (pageBuffers[i] != nullptr) {
            releasePage(i);
            delete[] pageBuffers[i];
            pageBuffers[i] = nullptr;
        }
    }
}

}  // namespace fs
}  // namespace dbs
