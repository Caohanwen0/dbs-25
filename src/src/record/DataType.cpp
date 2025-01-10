#include <variant>
#include <string>
#include <iostream>
#include <sstream>
#include <iomanip>

#include "record/DataType.hpp"

namespace dbs {
namespace record {

// Function to print the value of DataValue
void DataValue::print() const {
    if (isNull) {
        std::cout << "NULL";
    } 
    else {
        switch (dataType) {
            case DataTypeIdentifier::INT:
                std::cout << value.intValue;
                break;
            case DataTypeIdentifier::FLOAT:
                std::cout << std::fixed << std::setprecision(2) << value.floatValue;
                break;
            case DataTypeIdentifier::VARCHAR:
                std::cout << value.charValue;
                break;
            case DataTypeIdentifier::DATE:
                std::cout << value.dateValue.year << "-"
                          << value.dateValue.month << "-"
                          << value.dateValue.day;
                break;

        }
    }
}

// Function to get the size of a data type
int getDataTypeSize(DataTypeIdentifier dataType) {
    switch (dataType) {
        case DataTypeIdentifier::INT:
            return 4;
        case DataTypeIdentifier::FLOAT:
            return 8;
        case DataTypeIdentifier::VARCHAR:
            return 1;
        case DataTypeIdentifier::DATE:
            return 4;
        default:
            return 0;
    }
}

// DataValue constructors
DataValue::DataValue() : isNull(true), dataType(DataTypeIdentifier::INT) {
    value.intValue = 0;
}

DataValue::DataValue(DataTypeIdentifier dataType_, bool isNull_)
    : isNull(isNull_), dataType(dataType_){
    switch (dataType) {
        case DataTypeIdentifier::INT:
            value.intValue = 0;
            break;
        case DataTypeIdentifier::FLOAT:
            value.floatValue = 0.0;
            break;
        case DataTypeIdentifier::VARCHAR:
            value.charValue = "";
            break;
        case DataTypeIdentifier::DATE:
            value.dateValue = DateValue(0, 0, 0);
            break;
        default:
            break;
    }
}

DataValue::DataValue(DataTypeIdentifier dataType_, bool isNull_, int intValue_)
    : isNull(isNull_), dataType(dataType_){
        value.intValue = intValue_; //  int
    }

DataValue::DataValue(DataTypeIdentifier dataType_, bool isNull_, double floatValue_)
    : isNull(isNull_), dataType(dataType_){
        value.floatValue = floatValue_; // float
    }

DataValue::DataValue(DataTypeIdentifier dataType_, bool isNull_, const std::string & stringValue_)
    : isNull(isNull_), dataType(dataType_){
        value.charValue = stringValue_; // varchar
    }

DataValue::DataValue(DataTypeIdentifier dataType_, bool isNull_, DateValue dateValue_)
    : isNull(isNull_), dataType(dataType_){
        value.dateValue = dateValue_; // time
    }

DefaultValue::DefaultValue() {
    hasDefaultValue = false;
}

ColumnType::ColumnType() {
    dataType = DataTypeIdentifier::INT;
    varcharLength = 0;
    varcharSpace = 0;
    isNotNull = false;
    isUnique = false;
    defaultValue = DefaultValue();
    columnName = "";
    columnId = -1;
}

ColumnType::ColumnType(DataTypeIdentifier dataType_, int varcharLength_, int varcharSpace_,
               bool isNotNull_, bool isUnique_, DefaultValue defaultValue_,
               std::string columnName_){
    dataType = dataType_;
    varcharLength = varcharLength_;
    varcharSpace = varcharSpace_;
    isNotNull = isNotNull_;
    isUnique = isUnique_;
    defaultValue = defaultValue_;
    columnName = columnName_;
    columnId = -1;
}

bool ColumnType::isEqual(const ColumnType& other) const {
    if (dataType != other.dataType || varcharLength != other.varcharLength ||
        varcharSpace != other.varcharSpace || isNotNull != other.isNotNull ||
        isUnique != other.isUnique || !defaultValue.isEqual(other.defaultValue) ||
        columnName != other.columnName) {
        return false;
    }
    return true;
}

// Method to check equality of two DataValue objects
bool DataValue::isEqual(const DataValue& other) const {
    // Check if the data types are the same
    if (dataType != other.dataType || isNull != other.isNull) {
        return false;
    }
    if (!isNull) {
        switch (dataType) {
            case DataTypeIdentifier::INT:
                if (value.intValue != other.value.intValue) return false;
                break;
            case DataTypeIdentifier::FLOAT:
                if (value.floatValue != other.value.floatValue) return false;
                break;
            case DataTypeIdentifier::VARCHAR:
                if (value.charValue != other.value.charValue) return false;
                break;
            case DataTypeIdentifier::DATE:
                if (value.dateValue.year != other.value.dateValue.year) return false;
                if (value.dateValue.month != other.value.dateValue.month) return false;
                if (value.dateValue.day != other.value.dateValue.day) return false;
                break;
            default:
                break;
        }
    }
    return true;

}

std::string ColumnType::typeAsString() const {
    switch (dataType) {
        case DataTypeIdentifier::INT:
            return "INT";
        case DataTypeIdentifier::FLOAT:
            return "FLOAT";
        case DataTypeIdentifier::VARCHAR:
            return "VARCHAR(" + std::to_string(varcharLength) + ")";
        case DataTypeIdentifier::DATE:
            return "DATE";
        default:
            return "";
    }
}


// Conversion of DataType to a VARCHAR representation
std::string DataValue::toString() const {

    if (isNull){
        return "NULL";
    }
    else {
        switch (dataType) {
            case DataTypeIdentifier::INT:
                return std::to_string(value.intValue);
            case DataTypeIdentifier::FLOAT:
                return std::to_string(value.floatValue);
            case DataTypeIdentifier::VARCHAR:
                return value.charValue;
            case DataTypeIdentifier::DATE:
                return std::to_string(value.dateValue.year) + "-" +
                       std::to_string(value.dateValue.month) + "-" +
                       std::to_string(value.dateValue.day);
            default:
                return "";
        }
    }
}   


bool DefaultValue::isEqual(const DefaultValue& other) const{
    if (hasDefaultValue != other.hasDefaultValue) {
        return false;
    }
    if (hasDefaultValue) {
        return value.isEqual(other.value);
    }
    return true;
}

bool exactMatch(const std::vector<ColumnType>& columnTypes, const DataItem& dataItem){
    if (columnTypes.size() != dataItem.values.size()) {
        return false;
    }
    for (size_t i = 0; i < columnTypes.size(); i++) {
        if (columnTypes[i].dataType != dataItem.values[i].dataType) {
            return false;
        }
        if (dataItem.values[i].isNull) {
            if (columnTypes[i].isNotNull) {
                return false;
            }
        } else {
            switch (columnTypes[i].dataType) {
                case DataTypeIdentifier::VARCHAR:
                    if (dataItem.values[i].value.charValue.size() >
                        (size_t)columnTypes[i].varcharLength) {
                        return false;
                    }
                    break;
                default:
                    break;
            }
        }
    }
    return true;
}

bool DataItem::isEqual(const DataItem& other) const {
    if (values.size() != other.values.size()) return false;
    std::map<int, DataValue> value_map;
    for (size_t i = 0; i < values.size(); i++) {
        value_map[columnIds[i]] = values[i];
    }
    for (size_t i = 0; i < other.values.size(); i++) {
        if (!value_map.count(other.columnIds[i])) return false;
        if (!value_map[other.columnIds[i]].isEqual(other.values[i])) return false;
    }
    return true;
}


// DateValue constructors
DateValue::DateValue() : year(0), month(0), day(0) {}

DateValue::DateValue(int year_, int month_, int day_)
    : year(year_), month(month_), day(day_) {}

}  // namespace record
}  // namespace dbs
