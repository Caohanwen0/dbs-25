#include "fs/FileManager.hpp"
#include "common/Color.hpp"

namespace dbs {
namespace fs {

// Write data to a specific page in a file with an optional offset within the page
bool FileManager::writePage(int fileID, int pageID, BufType buffer, int offset) {
    int fileDesc = openFiles[fileID];  // Get the file descriptor from the open files map
    off_t fileOffset = static_cast<off_t>(pageID) << PAGE_SIZE_IDX;  // Calculate the byte offset for the page
    off_t error = lseek(fileDesc, fileOffset, SEEK_SET);  // Seek to the correct position in the file
    if (error != fileOffset) return false;  // If seeking fails, return false

    BufType dataBuffer = buffer + offset;  // Adjust the buffer by the offset
    error = write(fileDesc, static_cast<void*>(dataBuffer), PAGE_SIZE_BY_BYTE);  // Write data to the file
    return error != -1;  // Return true if write is successful, false otherwise
}

// Read data from a specific page in a file with an optional offset within the page
bool FileManager::readPage(int fileID, int pageID, BufType buffer, int offset) {
    int fileDesc = openFiles[fileID];  // Get the file descriptor from the open files map
    off_t fileOffset = static_cast<off_t>(pageID) << PAGE_SIZE_IDX;  // Calculate the byte offset for the page
    off_t error = lseek(fileDesc, fileOffset, SEEK_SET);  // Seek to the correct position in the file
    if (error != fileOffset) return false;  // If seeking fails, return false

    BufType dataBuffer = buffer + offset;  // Adjust the buffer by the offset
    error = read(fileDesc, static_cast<void*>(dataBuffer), PAGE_SIZE_BY_BYTE);  // Read data from the file
    return error != -1;  // Return true if read is successful, false otherwise
}

// Close the file given its ID and remove it from the open files map
void FileManager::closeFile(int fileID) {
    close(openFiles[fileID]);  // Close the file using its descriptor
    openFiles.erase(fileID);  // Remove the file from the open files map
}

// Create a new file with the specified file name
bool FileManager::createFile(const char* fileName) {
    FILE* file = fopen(fileName, "a+");  // Open file for reading and appending
    if (file == nullptr) {  // If file creation fails
        std::cerr << Color::FAIL << "DB failed to create file: " << fileName << Color::ENDC << std::endl;  // Print error message
        return false;  // Return false
    }
    fclose(file);  // Close the file if created successfully
    return true;  // Return true
}

// Open an existing file by its name and return its file ID
int FileManager::openFile(const char* fileName) {
    int fileID = nextFileID++;  // Assign the next available file ID
    int fileDesc = open(fileName, O_RDWR);  // Open the file with read/write permissions
    if (fileDesc == -1) {  // If file opening fails
        std::cerr << Color::FAIL << "DB failed to open file: " << fileName << Color::ENDC << std::endl;  // Print error message
        return -1;  // Return -1 to indicate failure
    }
    openFiles[fileID] = fileDesc;  // Store the file descriptor in the open files map
    return fileID;  // Return the file ID
}

// Check if a file exists by its name
bool FileManager::doesFileExist(const char* fileName) {
    int fileDesc = open(fileName, O_RDWR);  // Try to open the file with read/write permissions
    if (fileDesc == -1) return false;  // If the file does not exist, return false
    close(fileDesc);  // Close the file if it exists
    return true;  // Return true if file exists
}

// Delete a file given its name
bool FileManager::deleteFile(const char* fileName) {
    if (remove(fileName) != 0) {  // Try to remove the file
        std::cerr << Color::FAIL << "DB failed to delete file: " << fileName << Color::ENDC << std::endl;  // Print error message if deletion fails
        return false;  // Return false
    }
    return true;  // Return true if deletion is successful
}

// Check if a folder exists by its path
bool FileManager::doesFolderExist(const char* folderPath) {
    struct stat folderInfo;  // Stat structure to store information about the folder
    return stat(folderPath, &folderInfo) == 0 && S_ISDIR(folderInfo.st_mode);  // Return true if folder exists
}

// Create a new folder with the specified path
bool FileManager::createFolder(const char* folderPath) {
    if (mkdir(folderPath, 0777) == 0) {  // Create folder with full read/write/execute permissions
        return true;  // Return true if folder creation is successful
    }
    std::cerr << Color::FAIL << "DB failed to create folder: " << folderPath << Color::ENDC << std::endl;  // Print error message if creation fails
    return false;  // Return false
}

// Delete a folder and its contents recursively by its path
bool FileManager::deleteFolder(const char* folderPath) {
    DIR* dir = opendir(folderPath);  // Open the directory for reading
    struct dirent* entry;  // Directory entry structure to hold each file/directory name

    if (dir == nullptr) {  // If folder opening fails
        std::cerr << Color::FAIL << "DB failed to delete folder: " << folderPath << Color::ENDC << std::endl;  // Print error message
        return false;  // Return false
    }

    // Iterate through all entries in the folder
    while ((entry = readdir(dir)) != nullptr) {
        std::string entryName = entry->d_name;  // Get the name of the entry
        if (entryName != "." && entryName != "..") {  // Skip '.' and '..' entries
            std::string fullPath = std::string(folderPath) + "/" + entryName;  // Build the full path of the entry
            struct stat statBuf;  // Stat structure to store file information
            if (lstat(fullPath.c_str(), &statBuf) == -1) {  // Get file info
                closedir(dir);  // Close the directory
                std::cerr << Color::FAIL << "DB failed to delete folder: " << folderPath << Color::ENDC << std::endl;  // Print error message
                return false;  // Return false if there was an error
            }

            if (S_ISDIR(statBuf.st_mode)) {  // If the entry is a directory
                if (!deleteFolder(fullPath.c_str())) {  // Recursively delete the folder
                    closedir(dir);  // Close the directory
                    return false;  // Return false if folder deletion fails
                }
            } 
            else {  // If the entry is a file
                if (unlink(fullPath.c_str()) == -1) {  // Delete the file
                    closedir(dir);  // Close the directory
                    std::cerr << Color::FAIL << "DB failed to delete folder: " << folderPath << Color::ENDC << std::endl;  // Print error message
                    return false;  // Return false if file deletion fails
                }
            }
        }
    }
    closedir(dir);  // Close the directory

    if (rmdir(folderPath) == -1) {  // Remove the empty folder
        std::cerr << Color::FAIL << "DB failed to delete folder: " << folderPath << Color::ENDC << std::endl;  // Print error message if removal fails
        return false;  // Return false if folder removal fails
    }

    return true;  // Return true if the folder is successfully deleted
}

}  // namespace fs
}  // namespace dbs
