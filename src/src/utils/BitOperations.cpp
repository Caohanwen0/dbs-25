#include "utils/BitOperations.hpp"

namespace dbs {
namespace utils {

unsigned int intToBit32(int input) {
    unsigned int result;
    std::memcpy(&result, &input, sizeof(int));
    return result;
}

int bit32ToInt(unsigned int bit32) {
    int result;
    std::memcpy(&result, &bit32, sizeof(int));
    return result;
}

void floatToBit32(double floatValue, unsigned int &firstWord, unsigned int &secondWord) {
    // Store the floating-point value into two 32-bit unsigned integers
    unsigned int *intArray = reinterpret_cast<unsigned int *>(&floatValue);
    firstWord = intArray[0];
    secondWord = intArray[1];
}

double bit32ToFloat(unsigned int firstWord, unsigned int secondWord) {
    // Reconstruct the floating-point value from two 32-bit unsigned integers
    unsigned int intArray[2] = {firstWord, secondWord};
    double floatValue = *reinterpret_cast<double *>(intArray);
    return floatValue;
}

bool getBitFromNumber(unsigned int bit32, int position) {
    assert(position >= 0 && position < BIT_PER_BUF);
    return (bit32 >> position) & 1;
}

bool getBitFromBuffer(BufType buffer, int position) {
    assert(position >= 0 && position < BIT_PER_PAGE);
    int offset = position >> LOG_BIT_PER_BUF;
    int bias = position & BIT_PER_BUF_MASK;
    return getBitFromNumber(buffer[offset], bias);
}

void setBitInNumber(unsigned int &bit32, int position, bool value) {
    assert(position >= 0 && position < BIT_PER_BUF);
    if (value) {
        bit32 |= (1 << position);
    } 
    else {
        bit32 &= ~(1 << position);
    }
}

void setBitInBuffer(BufType buffer, int position, bool value) {
    assert(position >= 0 && position < BIT_PER_PAGE);
    int offset = position >> LOG_BIT_PER_BUF;
    int bias = position & BIT_PER_BUF_MASK;
    setBitInNumber(buffer[offset], bias, value);
}

unsigned int getTwoBytes(unsigned int bit32, int bytePosition) {
    assert(bytePosition == 0 || bytePosition == 1);
    return (bit32 >> (bytePosition << 4)) & 0xffff;
}

void setTwoBytes(unsigned int &bit32, int bytePosition, unsigned int value) {
    assert(bytePosition == 0 || bytePosition == 1);
    bit32 &= ~(0xffff << (bytePosition << 4));
    bit32 |= (value << (bytePosition << 4));
}

char getByte(unsigned int bit32, int bytePosition) {
    assert(bytePosition == 0 || bytePosition == 1 || bytePosition == 2 || bytePosition == 3);
    return (bit32 >> (bytePosition << 3)) & 0xff;
}

void setByte(unsigned int &bit32, int bytePosition, char value) {
    assert(bytePosition == 0 || bytePosition == 1 || bytePosition == 2 || bytePosition == 3);
    bit32 &= ~(0xff << (bytePosition << 3));
    bit32 |= (value << (bytePosition << 3));
}

int findFirstZeroBit(unsigned int bit32) {
    for (int i = 0; i < BIT_PER_BUF; i++) {
        if (!getBitFromNumber(bit32, i)) {
            return i;
        }
    }
    return -1;  // No zero bit found
}

}  // namespace utils
}  // namespace dbs
