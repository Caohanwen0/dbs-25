#pragma once

#include <cstring>
#include <iomanip>
#include <iostream>
#include <map>
#include <string>
#include <vector>
#include <sstream>
#include <variant>

namespace dbs {
namespace record {

// Enum to define the various types of data.
enum DataTypeIdentifier {
    INT,
    FLOAT,
    VARCHAR, // Variable length character string
    DATE,
    ANY,
};

/**
 * @brief Retrieves the size of a specific data type in bytes.
 * For VARCHAR, the size is the size of a single character.
 * 
 * @param dataType The data type whose size is to be determined.
 * @return int Size in bytes of the given data type.
 */
int getDataTypeSize(DataTypeIdentifier dataType);

/**
 * @brief Struct to represent a Date value (year, month, and day).
 */
struct DateValue {
    int year, month, day;
    DateValue();
    DateValue(int year_, int month_, int day_);
};

/**
 * @brief Struct to represent any value of a specific data type.
 */
struct DataValue {
    DataTypeIdentifier dataType;
    bool isNull;
    // std::variant<int, double, std::string, DateValue> value;
    struct {
        int intValue;
        double floatValue;
        std::string charValue;
        DateValue dateValue;
    } value;
    void print() const;
    DataValue();
    DataValue(DataTypeIdentifier dataType_, bool isNull_);
    DataValue(DataTypeIdentifier dataType_, bool isNull_, int intValue_);
    DataValue(DataTypeIdentifier dataType_, bool isNull_, double floatValue_);
    DataValue(DataTypeIdentifier dataType_, bool isNull_, const std::string& stringValue_);
    DataValue(DataTypeIdentifier dataType_, bool isNull_, DateValue dateValue_);
    bool isEqual(const DataValue& other) const;
    std::string toString() const;
    
    friend bool operator<=(const DataValue& lhs, const DataValue& rhs){
        return !(lhs > rhs);
    }
    friend bool operator>(const DataValue& lhs, const DataValue& rhs){
        return !(lhs < rhs) && !(lhs == rhs);
    }
    friend bool operator>=(const DataValue& lhs, const DataValue& rhs){
        return !(lhs < rhs);
    }
    friend bool operator<(const DataValue& lhs, const DataValue& rhs){
        if (lhs.isNull) return true;
        switch (lhs.dataType){
            case DataTypeIdentifier::INT:
                return lhs.value.intValue < rhs.value.intValue;
            case DataTypeIdentifier::FLOAT:

                return lhs.value.floatValue < rhs.value.floatValue;
            case DataTypeIdentifier::VARCHAR:

                return lhs.value.charValue < rhs.value.charValue;
            case DataTypeIdentifier::DATE:
                return lhs.value.dateValue.year < rhs.value.dateValue.year ||
                       (lhs.value.dateValue.year == rhs.value.dateValue.year &&
                        lhs.value.dateValue.month < rhs.value.dateValue.month) ||
                       (lhs.value.dateValue.year == rhs.value.dateValue.year &&
                        lhs.value.dateValue.month == rhs.value.dateValue.month &&
                        lhs.value.dateValue.day < rhs.value.dateValue.day);
    
        }
        return false;
    }
    friend bool operator==(const DataValue& lhs, const DataValue& rhs){

        if (lhs.dataType != rhs.dataType || lhs.isNull != rhs.isNull) return false;
        if (lhs.isNull && rhs.isNull) return true;
        switch (lhs.dataType) {
            case DataTypeIdentifier::INT:
                return lhs.value.intValue == rhs.value.intValue;
            case DataTypeIdentifier::FLOAT:
                return lhs.value.floatValue == rhs.value.floatValue;
            case DataTypeIdentifier::VARCHAR:
                return lhs.value.charValue == rhs.value.charValue;
            case DataTypeIdentifier::DATE:
                return lhs.value.dateValue.year == rhs.value.dateValue.year &&
                       lhs.value.dateValue.month == rhs.value.dateValue.month &&
                       lhs.value.dateValue.day == rhs.value.dateValue.day;
            default:
                return false;
        }
    }
};

/**
 * @brief Struct to represent a collection of DataValues and their corresponding column IDs.
 */
struct DataItem {
    std::vector<DataValue> values;
    std::vector<int> columnIds;
    unsigned int dataId;

    bool isEqual(const DataItem& other) const;
    friend bool operator<(const DataItem& lhs, const DataItem& rhs){
        for (size_t i = 0; i < lhs.values.size(); i++) {
            if (lhs.values[i] < rhs.values[i]) {
                return true;
            } 
            else if (lhs.values[i] > rhs.values[i]) {
                return false;
            }
        }
        return false;
    }
};

/**
 * @brief Struct to represent a default value for a column.
 */
struct DefaultValue {
    bool hasDefaultValue;
    DataValue value;
    DefaultValue();
    bool isEqual(const DefaultValue& other) const;
};

/**
 * @brief Struct to represent a column type (data type, length, uniqueness, etc.).
 */
struct ColumnType {
    DataTypeIdentifier dataType;
    int varcharLength;
    int varcharSpace;  // in bytes
    bool isNotNull;
    bool isUnique;
    DefaultValue defaultValue;
    std::string columnName;
    int columnId;

    ColumnType();
    ColumnType(DataTypeIdentifier dataType_, int varcharLength_, int varcharSpace_,
               bool isNotNull_, bool isUnique_, DefaultValue defaultValue_,
               std::string columnName_);
    
    bool isEqual(const ColumnType& other) const;
    std::string typeAsString() const;
};

/**
 * @brief Struct to represent the location of a record (page and slot IDs).
 */
struct RecordLocation {
    int pageId;
    int slotId;
};

/**
 * @brief Checks if a DataItem exactly matches the given column types.
 */
bool exactMatch(const std::vector<ColumnType>& columnTypes, const DataItem& dataItem);

}  // namespace record
}  // namespace dbs
