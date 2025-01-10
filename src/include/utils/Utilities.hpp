#pragma once

#include <string>
#include <filesystem>

namespace dbs {
namespace utils {

/**
 * @brief A function to obtain the folder name through relative or direct path
 * @param a relative or absolute path for folder
 */

std::string getFolderName(std::string folderPath);

}
}
