#pragma once

#include <climits>
#include <vector>

#include "common/Config.hpp"
#include "record/DataType.hpp"

namespace dbs {
namespace system {

// static 的columnType初始化放在SystemManager的构造函数中

static std::vector<record::ColumnType> GlobalDatabaseInfoColumnType;
static std::vector<record::ColumnType> DatabaseTableInfoColumnType;
static std::vector<record::ColumnType> TablePrimaryKeyInfoColumnType;
static std::vector<record::ColumnType> TableForeignKeyInfoColumnType;
static std::vector<record::ColumnType> TableDominateInfoColumnType;
static std::vector<record::ColumnType> TableIndexInfoColumnType;

struct ForeignKeyInputInfo {
    std::vector<std::string> fKColumnNames;
    std::string reference_table_name;
    std::vector<std::string> reference_column_names;

    ForeignKeyInputInfo();
    ForeignKeyInputInfo(
        const std::vector<std::string>& fKColumnNames_,
        const std::string& reference_table_name_,
        const std::vector<std::string>& reference_column_names_);
};

struct ForeignKeyInfo {
    std::vector<int> foreign_key_columnIds;
    int reference_table_id;
    std::vector<int> reference_columnIds;
    std::string name;
    ForeignKeyInfo();
    ForeignKeyInfo(const std::vector<int>& foreign_key_columnIds_,
                   int reference_table_id_,
                   const std::vector<int>& reference_columnIds_,
                   std::string name_);
    friend bool operator==(const ForeignKeyInfo& a, const ForeignKeyInfo& b) {
        if (a.reference_table_id != b.reference_table_id) {
            return false;
        }
        if (a.foreign_key_columnIds.size() !=
            b.foreign_key_columnIds.size()) {
            return false;
        }
        for (int i = 0; i < a.foreign_key_columnIds.size(); i++) {
            int found_idx = -1;
            for (int j = 0; j < b.foreign_key_columnIds.size(); j++) {
                if (a.foreign_key_columnIds[i] ==
                    b.foreign_key_columnIds[j]) {
                    found_idx = j;
                    break;
                }
            }
            if (found_idx == -1) {
                return false;
            }
            if (a.reference_columnIds[i] !=
                b.reference_columnIds[found_idx]) {
                return false;
            }
        }
        return true;
    }
};

enum ConstraintType { EQ, NEQ, GT, GEQ, LT, LEQ };

struct SearchConstraint {
    int columnId;  // 是对哪一列的约束
    record::DataTypeIdentifier
        dataType;  // 约束的数据类型 （也就是对应列的数据类型）
    std::vector<ConstraintType> constraintTypes;  // vector是为了方便后续操作；使用的时候push一个元素就好，约束对应上述类型
                                                   // EQ, NEQ, GT, GEQ, LT, LEQ
    std::vector<record::DataValue>
        constraintValues;  // vector是为了方便后续操作；使用的时候push一个元素就好，约束对应的值
    void print() const;
};

bool mergeConstraints(std::vector<SearchConstraint>& constraints);

bool validConstraint(const SearchConstraint& constraint,
                     const record::DataItem& item);

void filterConstraints(
    const std::vector<SearchConstraint>& constraints,
    const std::vector<record::DataItem>& data_items,
    const std::vector<record::RecordLocation>& record_locations,
    std::vector<record::DataItem>& result,
    std::vector<record::RecordLocation>& record_location_results);

}  // namespace system
}  // namespace dbs