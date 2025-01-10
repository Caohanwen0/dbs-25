#pragma once

#include <iostream>
#include <map>
#include <cstdio>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>

#include "common/Config.hpp"


namespace dbs {
namespace fs {

class FileManager {
public:
    /**
     * @brief Writes 8KB of data (2048 4-byte integers) from the buffer to a file's specified page.
     *
     * @param fileID Identifier for the file
     * @param pageID Identifier for the page in the file
     * @param buffer Pointer to the data buffer
     * @param offset Byte offset from the buffer
     * @return true if the write operation is successful, false otherwise
     */
    bool writePage(int fileID, int pageID, BufType buffer, int offset);

    /**
     * @brief Reads 8KB of data (2048 4-byte integers) from a file's specified page into the buffer.
     *
     * @param fileID Identifier for the file
     * @param pageID Identifier for the page in the file
     * @param buffer Pointer to the data buffer
     * @param offset Byte offset from the buffer
     * @return true if the read operation is successful, false otherwise
     */
    bool readPage(int fileID, int pageID, BufType buffer, int offset);

    /**
     * @brief Closes the specified file.
     *
     * @param fileID Identifier for the file
     */
    void closeFile(int fileID);

    /**
     * @brief Creates a new file with the specified name.
     *
     * @param fileName Name of the file to create
     * @return true if the file creation is successful, false otherwise
     */
    bool createFile(const char* fileName);

    /**
     * @brief Opens an existing file with the specified name.
     *
     * @param fileName Name of the file to open
     * @return File ID if successful, -1 otherwise
     */
    int openFile(const char* fileName);

    /**
     * @brief Checks if a file with the specified name exists.
     *
     * @param fileName Name of the file to check
     * @return true if the file exists, false otherwise
     */
    bool doesFileExist(const char* fileName);

    /**
     * @brief Deletes a file with the specified name.
     *
     * @param fileName Name of the file to delete
     * @return true if the deletion is successful, false otherwise
     */
    bool deleteFile(const char* fileName);

    /**
     * @brief Checks if a folder exists at the specified path.
     *
     * @param folderPath Path to the folder
     * @return true if the folder exists, false otherwise
     */
    bool doesFolderExist(const char* folderPath);

    /**
     * @brief Creates a folder at the specified path.
     *
     * @param folderPath Path to the folder
     * @return true if the folder creation is successful, false otherwise
     */
    bool createFolder(const char* folderPath);

    /**
     * @brief Deletes a folder at the specified path.
     *
     * @param folderPath Path to the folder
     * @return true if the deletion is successful, false otherwise
     */
    bool deleteFolder(const char* folderPath);

private:
    std::map<int, int> openFiles;  // Maps fileID to file descriptor
    uint nextFileID = 0;  // Counter for generating file IDs
};

}  // namespace fs
}  // namespace dbs
