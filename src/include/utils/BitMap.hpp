#pragma once
#include <cassert>
#include <cstring>
#include <sstream>

#include "common/Config.hpp"

namespace dbs {
namespace utils {

class BitMap {
public:
    /**
     * @brief Constructs a new BitMap object with a specified capacity and initial value.
     * 
     * @param mapCapacity The total number of bits in the bit map.
     * @param initialValue The value to initialize the bits with (either `true` or `false`).
     */
    BitMap(int mapCapacity, bool initialValue);

    /**
     * @brief Destroys the BitMap object and releases its resources.
     */
    ~BitMap();

    /**
     * @brief Sets the bit at the specified index to a given value.
     * 
     * @param bitIndex The index of the bit to modify.
     * @param bitValue The value to set at the given index (`true` or `false`).
     */
    void setBit(int bitIndex, bool bitValue);

    /**
     * @brief Retrieves the value of the bit at the specified index.
     * 
     * @param bitIndex The index of the bit to retrieve.
     * @return `true` if the bit is set to 1, `false` if it is set to 0.
     */
    bool getBit(int bitIndex);

private:
    /**
     * @brief Helper function to calculate the word position and bit position for a given bit index.
     * 
     * @param bitIndex The bit index for which the positions are calculated.
     * @param wordPos The word position (output).
     * @param bitPos The bit position within the word (output).
     */
    static void computeBitPositions(int bitIndex, int& wordPos, int& bitPos) {
        wordPos = (bitIndex >> BIT_MAP_BIAS);   // Calculate the word position based on the bias.
        bitPos = bitIndex - (wordPos << BIT_MAP_BIAS);  // Calculate the exact bit position within the word.
    }

    uint* bitData;      // Array to hold the bit map data.
    int bitCapacity;    // The total number of bits in the bit map.
    int mapSize;        // The number of uints required to store the bit map.
};

}  // namespace utils
}  // namespace dbs
