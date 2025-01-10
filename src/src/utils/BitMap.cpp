#include "utils/BitMap.hpp"

namespace dbs {
namespace utils {

BitMap::BitMap(int mapCapacity, bool initialValue) {
    bitCapacity = mapCapacity;  // in bit
    mapSize = (mapCapacity >> BIT_MAP_BIAS);  // Calculate how many words are needed.
    bitData = new uint[mapSize];              // Allocate memory for the bit map.

    // Initialize the bit map based on the initial value.
    if (initialValue == true)
        std::memset(bitData, 0xff, sizeof(uint) * mapSize);  // Set all bits to 1.
    else
        std::memset(bitData, 0, sizeof(uint) * mapSize);      // Set all bits to 0.
}

BitMap::~BitMap() {
    delete[] bitData;  // Free the allocated memory when the bit map is destroyed.
}

void BitMap::setBit(int bitIndex, bool bitValue) {
    assert(bitIndex < bitCapacity);  // Ensure the index is within valid range.
    
    int wordPos, bitPos;
    computeBitPositions(bitIndex, wordPos, bitPos);  // Calculate the word and bit positions.

    uint mask = (1 << bitPos);  // Create a mask for the target bit.
    uint word = bitData[wordPos] & (~mask);  // Clear the bit at the target position.

    if (bitValue)
        word |= mask;  // Set the bit if `bitValue` is true.

    bitData[wordPos] = word;  // Store the modified word back into the array.
}

bool BitMap::getBit(int bitIndex) {
    assert(bitIndex < bitCapacity);  // Ensure the index is within valid range.
    
    int wordPos, bitPos;
    computeBitPositions(bitIndex, wordPos, bitPos);  // Calculate the word and bit positions.

    uint mask = (1 << bitPos);  // Create a mask for the target bit.
    return (bitData[wordPos] & mask) != 0;  // Return true if the bit is set, false otherwise.
}

}  // namespace utils
}  // namespace dbs
