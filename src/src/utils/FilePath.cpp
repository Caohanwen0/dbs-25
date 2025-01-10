#include "utils/FilePath.hpp"

namespace dbs {
namespace utils {

void integerToString(int number, char** resultStr) {
    // Calculate the length of the integer when converted to a string.
    int length = snprintf(nullptr, 0, "%d", number);
    
    // Allocate memory for the result string (+1 for the null terminator).
    *resultStr = new char[length + 1];
    
    // Convert the integer to a string and store it in resultStr.
    sprintf(*resultStr, "%d", number);
}

void joinPaths(const char* basePath, const char* relativePath, char** resultPath) {
    // Get the lengths of both paths.
    int baseLength = strlen(basePath);
    int relativeLength = strlen(relativePath);
    
    // Allocate memory for the concatenated path (+2 for the '/' separator and null terminator).
    *resultPath = new char[baseLength + relativeLength + 2];
    
    // Concatenate the base path, separator '/', and the relative path.
    strcpy(*resultPath, basePath);
    strcat(*resultPath, "/");
    strcat(*resultPath, relativePath);
}

void joinStrings(const char* firstString, const char* secondString, char** resultString) {
    // Get the lengths of both strings.
    int firstLength = strlen(firstString);
    int secondLength = strlen(secondString);
    
    // Allocate memory for the concatenated string (+1 for the null terminator).
    *resultString = new char[firstLength + secondLength + 1];
    
    // Concatenate the two strings.
    strcpy(*resultString, firstString);
    strcat(*resultString, secondString);
}

}  // namespace utils
}  // namespace dbs
