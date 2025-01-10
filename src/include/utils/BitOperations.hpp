#pragma once

#include <cassert>
#include <cstring>

#include "common/Config.hpp"

namespace dbs {
namespace utils {

/**
 * @brief Converts an integer to a 32-bit unsigned integer representation.
 * 
 * @param input Integer to convert
 * @return Corresponding 32-bit unsigned integer
 */
unsigned int intToBit32(int input);

/**
 * @brief Converts a 32-bit unsigned integer back to a signed integer.
 *
 * @param bit32 32-bit unsigned integer to convert
 * @return Corresponding signed integer
 */
int bit32ToInt(unsigned int bit32);

/**
 * @brief Converts a floating-point number to two 32-bit unsigned integers.
 *
 * @param floatValue The floating-point value to convert
 * @param firstWord First 32-bit unsigned integer result
 * @param secondWord Second 32-bit unsigned integer result
 */
void floatToBit32(double floatValue, unsigned int &firstWord, unsigned int &secondWord);

/**
 * @brief Converts two 32-bit unsigned integers back to a floating-point number.
 *
 * @param firstWord First 32-bit unsigned integer
 * @param secondWord Second 32-bit unsigned integer
 * @return Corresponding floating-point number
 */
double bit32ToFloat(unsigned int firstWord, unsigned int secondWord);

/**
 * @brief Retrieves the bit at a specific position from a 32-bit integer.
 *
 * @param bit32 The 32-bit integer
 * @param position The bit position to retrieve (0-31)
 * @return `true` if the bit is 1, `false` if the bit is 0
 */
bool getBitFromNumber(unsigned int bit32, int position);

/**
 * @brief Retrieves the bit at a specific position from a buffer.
 *
 * @param buffer The buffer to retrieve from
 * @param position The bit position to retrieve
 * @return `true` if the bit is 1, `false` if the bit is 0
 */
bool getBitFromBuffer(BufType buffer, int position);

/**
 * @brief Sets a bit at a specific position in a 32-bit integer.
 *
 * @param bit32 The 32-bit integer to modify
 * @param position The bit position to modify (0-31)
 * @param value The value to set (true/false)
 */
void setBitInNumber(unsigned int &bit32, int position, bool value);

/**
 * @brief Sets a bit at a specific position in a buffer.
 *
 * @param buffer The buffer to modify
 * @param position The bit position to modify
 * @param value The value to set (true/false)
 */
void setBitInBuffer(BufType buffer, int position, bool value);

/**
 * @brief Retrieves the first two bytes from a 32-bit integer.
 *
 * @param bit32 The 32-bit integer
 * @param bytePosition The byte position (0 or 1)
 * @return The requested 2-byte value
 */
unsigned int getTwoBytes(unsigned int bit32, int bytePosition);

/**
 * @brief Sets the first two bytes of a 32-bit integer.
 *
 * @param bit32 The 32-bit integer to modify
 * @param bytePosition The byte position (0 or 1)
 * @param value The 2-byte value to set
 */
void setTwoBytes(unsigned int &bit32, int bytePosition, unsigned int value);

/**
 * @brief Retrieves a specific byte from a 32-bit integer.
 *
 * @param bit32 The 32-bit integer
 * @param bytePosition The byte position (0 to 3)
 * @return The requested byte value
 */
char getByte(unsigned int bit32, int bytePosition);

/**
 * @brief Sets a specific byte in a 32-bit integer.
 *
 * @param bit32 The 32-bit integer to modify
 * @param bytePosition The byte position (0 to 3)
 * @param value The byte value to set
 */
void setByte(unsigned int &bit32, int bytePosition, char value);

/**
 * @brief Finds the position of the first zero bit in a 32-bit integer.
 *
 * @param bit32 The 32-bit integer
 * @return The position of the first zero bit, or -1 if none found
 */
int findFirstZeroBit(unsigned int bit32);

}  // namespace utils
}  // namespace dbs
