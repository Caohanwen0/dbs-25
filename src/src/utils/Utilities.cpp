#include "utils/Utilities.hpp"

namespace dbs {
namespace utils {

std::string getFolderName(std::string folderPath){
    std::filesystem::path path(folderPath);
    return path.filename().string();
}

}
}