// Color.hpp
#pragma once

#include <string>

// colors for stdout and stderr
namespace Color {
    const std::string HEADER = "\033[95m";  // Purple
    const std::string OKBLUE = "\033[94m";  // Blue
    const std::string OKCYAN = "\033[96m";  // Cyan
    const std::string OKGREEN = "\033[92m";  // Green
    const std::string WARNING = "\033[93m";  // Yellow
    const std::string FAIL = "\033[91m";  // Red
    const std::string PINK = "\033[35m";  // Pink (Magenta)
    const std::string ENDC = "\033[0m";  // Reset
    const std::string BOLD = "\033[1m";  // Bold
    const std::string UNDERLINE = "\033[4m";  // Underline
}
