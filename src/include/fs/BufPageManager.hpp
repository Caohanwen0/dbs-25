#pragma once

#include "fs/FileManager.hpp"
#include "fs/FindReplace.hpp"
#include "utils/BitMap.hpp"
#include "utils/HashMap.hpp"

namespace dbs {
namespace fs {

struct PageLocation {
    int fileID, pageID;

    PageLocation() : fileID(-1), pageID(-1) {}
    PageLocation(int fileID_, int pageID_) : fileID(fileID_), pageID(pageID_) {}

    utils::HashItemTwoInt toHashItem() const {
        return utils::HashItemTwoInt(fileID, pageID);
    }
};

class BufPageManager {
public:
    BufPageManager(FileManager* fileMgr);
    ~BufPageManager();

    /**
     * @brief Retrieves a page from the buffer.
     *
     * @param fileID Identifier for the file
     * @param pageID Identifier for the page
     * @param pageIndex Index of the page in the buffer
     * @return BufType Buffer representation of the page
     */
    BufType getPage(int fileID, int pageID, int& pageIndex);

    /**
     * @brief Marks a page as accessed (should be called after accessing a page).
     *
     * @param pageIndex Index of the accessed page
     */
    void accessPage(int pageIndex);

    /**
     * @brief Marks a page as dirty (should be called after modifying a page).
     *
     * @param pageIndex Index of the modified page
     */
    void markPageDirty(int pageIndex);

    /**
     * @brief Closes all pages and releases resources.
     *        Must be called when exiting the program.
     */
    void closeManager();

private:
    BufType allocateMemory();
    BufType loadPage(int fileID, int pageID, int& pageIndex);
    void releasePage(int pageIndex);
    void flushPageToDisk(int pageIndex);

    FindReplace* pageReplacementStrategy;
    utils::BitMap* dirtyPageTracker;
    BufType* pageBuffers;
    FileManager* fileManager;
    utils::HashMap<utils::HashItemTwoInt>* pageHashMap;
    PageLocation* pageLocations;
    int lastAccessedPageIndex;
};

}  // namespace fs
}  // namespace dbs
