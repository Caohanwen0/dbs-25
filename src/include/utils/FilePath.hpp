#pragma once

#include <vector>
#include <string>
#include <cstring>
#include <fstream>
#include <iostream>

namespace dbs {
namespace utils {

/**
 * @brief Converts an integer to a string representation.
 * 
 * @param number The integer to be converted.
 * @param resultStr A pointer to the result string that will hold the string representation of the integer.
 */
void integerToString(int number, char** resultStr);

/**
 * @brief Concatenates two file paths into one.
 * 
 * @param basePath The base directory path.
 * @param relativePath The file or directory name to append to the base path.
 * @param resultPath A pointer to the resulting concatenated path string.
 */
void joinPaths(const char* basePath, const char* relativePath, char** resultPath);

/**
 * @brief Concatenates two strings into one.
 * 
 * @param firstString The first string to concatenate.
 * @param secondString The second string to concatenate.
 * @param resultString A pointer to the resulting concatenated string.
 */
void joinStrings(const char* firstString, const char* secondString, char** resultString);

}  // namespace utils
}  // namespace dbs
