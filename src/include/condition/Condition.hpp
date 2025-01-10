#pragma once

#include <iostream>
#include <limits>
#include <set>
#include <vector>

#include "common/Config.hpp"
#include "system/SystemColumns.hpp"
#include "system/SystemManager.hpp"

namespace dbs {
namespace condition {

enum class Operator { EQ, NEQ, LT, LEQ, GT, GEQ, IS, UNDEFINED };
enum class VariableType { INT, FLOAT, STRING, DATE, NULL_OR_NOT, TABLE_COLUMN };

struct NullType {};

struct Condition {
    std::string table_name;
    std::string column_name;
    Operator op;
    VariableType type;

    int int_val;
    double float_val;
    std::string string_val;
    std::string table_name_other, column_name_other;
    bool is_null;
    record::DateValue date_val;
    Condition();
};

struct IndexCondition {
    int table_id, columnId;
    int int_lower_bound, int_upper_bound, int_val;
    double float_lower_bound, float_upper_bound, float_val;
    std::string str_val;
    int table_id_other, columnIdOther;
    bool is_null;

    Operator op;
    VariableType type;

    IndexCondition();
    IndexCondition(int table_id_, int columnId_, Condition& condition,
                   system::SystemManager* sm);
    /**
     * @brief Updates the upper and lower bounds of the current condition.
     * @param table_id_ The table ID of the new condition.
     * @param columnId_ The column ID of the new condition.
     * @param condition The new condition.
     * @return true if the condition was updated successfully.
     */
    bool update(int table_id_, int columnId_, Condition& condition);

    /**
     * @brief Converts the index condition to a search constraint.
     * @return The corresponding search constraint.
     */
    system::SearchConstraint toSearchConstraint() const;
};

/**
 * @brief Updates all index conditions based on the given list of conditions.
 * @param index_conditions The list of existing index conditions.
 * @param conditions The list of conditions to update the index conditions.
 * @param sm The system manager used to handle table metadata.
 * @param table_names The list of table names to consider when resolving conditions.
 * @return true if the index conditions were updated successfully.
 */
bool updateAllIndexConditions(std::vector<IndexCondition>& index_conditions,
                               std::vector<Condition>& conditions,
                               system::SystemManager* sm,
                               std::vector<std::string> table_names);

}  // namespace condition
}  // namespace dbs
