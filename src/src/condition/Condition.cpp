#include "condition/Condition.hpp"
#include "common/Color.hpp"

namespace dbs {
namespace condition {

/**
 * Converts the current index condition to a search constraint.
 * @return The corresponding search constraint.
 */
system::SearchConstraint IndexCondition::toSearchConstraint() const {
    system::SearchConstraint constraint;
    constraint.columnId = columnId;
    
    // Handle different variable types and corresponding operators
    if (type == VariableType::INT) {
        constraint.dataType = record::DataTypeIdentifier::INT;
        if (op == Operator::EQ) {
            constraint.constraintTypes.push_back(system::ConstraintType::EQ);
            constraint.constraintValues.push_back(record::DataValue(record::DataTypeIdentifier::INT, false, int_val));
        } else if (op == Operator::NEQ) {
            constraint.constraintTypes.push_back(system::ConstraintType::NEQ);
            constraint.constraintValues.push_back(record::DataValue(record::DataTypeIdentifier::INT, false, int_val));
        } else {
            constraint.constraintTypes.push_back(system::ConstraintType::LEQ);
            constraint.constraintValues.push_back(record::DataValue(record::DataTypeIdentifier::INT, false, int_upper_bound));
            constraint.constraintTypes.push_back(system::ConstraintType::GEQ);
            constraint.constraintValues.push_back(record::DataValue(record::DataTypeIdentifier::INT, false, int_lower_bound));
        }
    } else if (type == VariableType::FLOAT) {
        constraint.dataType = record::DataTypeIdentifier::FLOAT;
        if (op == Operator::EQ) {
            constraint.constraintTypes.push_back(system::ConstraintType::EQ);
            constraint.constraintValues.push_back(record::DataValue(record::DataTypeIdentifier::FLOAT, false, float_val));
        } else if (op == Operator::NEQ) {
            constraint.constraintTypes.push_back(system::ConstraintType::NEQ);
            constraint.constraintValues.push_back(record::DataValue(record::DataTypeIdentifier::FLOAT, false, float_val));
        } else if (op == Operator::LT) {
            constraint.constraintTypes.push_back(system::ConstraintType::LT);
            constraint.constraintValues.push_back(record::DataValue(record::DataTypeIdentifier::FLOAT, false, float_upper_bound));
        } else if (op == Operator::LEQ) {
            constraint.constraintTypes.push_back(system::ConstraintType::LEQ);
            constraint.constraintValues.push_back(record::DataValue(record::DataTypeIdentifier::FLOAT, false, float_upper_bound));
        } else if (op == Operator::GT) {
            constraint.constraintTypes.push_back(system::ConstraintType::GT);
            constraint.constraintValues.push_back(record::DataValue(record::DataTypeIdentifier::FLOAT, false, float_lower_bound));
        } else if (op == Operator::GEQ) {
            constraint.constraintTypes.push_back(system::ConstraintType::GEQ);
            constraint.constraintValues.push_back(record::DataValue(record::DataTypeIdentifier::FLOAT, false, float_lower_bound));
        }
    } else if (type == VariableType::STRING) {
        constraint.dataType = record::DataTypeIdentifier::VARCHAR;
        if (op == Operator::EQ) {
            constraint.constraintTypes.push_back(system::ConstraintType::EQ);
            constraint.constraintValues.push_back(record::DataValue(record::DataTypeIdentifier::VARCHAR, false, str_val));
        } else if (op == Operator::NEQ) {
            constraint.constraintTypes.push_back(system::ConstraintType::NEQ);
            constraint.constraintValues.push_back(record::DataValue(record::DataTypeIdentifier::VARCHAR, false, str_val));
        } else if (op == Operator::LT) {
            constraint.constraintTypes.push_back(system::ConstraintType::LT);
            constraint.constraintValues.push_back(record::DataValue(record::DataTypeIdentifier::VARCHAR, false, str_val));
        } else if (op == Operator::LEQ) {
            constraint.constraintTypes.push_back(system::ConstraintType::LEQ);
            constraint.constraintValues.push_back(record::DataValue(record::DataTypeIdentifier::VARCHAR, false, str_val));
        } else if (op == Operator::GT) {
            constraint.constraintTypes.push_back(system::ConstraintType::GT);
            constraint.constraintValues.push_back(record::DataValue(record::DataTypeIdentifier::VARCHAR, false, str_val));
        } else if (op == Operator::GEQ) {
            constraint.constraintTypes.push_back(system::ConstraintType::GEQ);
            constraint.constraintValues.push_back(record::DataValue(record::DataTypeIdentifier::VARCHAR, false, str_val));
        }
    } else if (type == VariableType::NULL_OR_NOT) {
        constraint.dataType = record::DataTypeIdentifier::VARCHAR;
        if (is_null) {
            constraint.constraintTypes.push_back(system::ConstraintType::EQ);
            constraint.constraintValues.push_back(record::DataValue(record::DataTypeIdentifier::VARCHAR, true, 0));
        } else {
            constraint.constraintTypes.push_back(system::ConstraintType::NEQ);
            constraint.constraintValues.push_back(record::DataValue(record::DataTypeIdentifier::VARCHAR, true, 0));
        }
    }
    return constraint;
}

/**
 * Default constructor for Condition. Initializes all members with default values.
 */
Condition::Condition() {
    table_name = "";
    column_name = "";
    op = Operator::UNDEFINED;
    type = VariableType::INT;
    int_val = -1;
    float_val = -1;
    string_val = "";
    table_name_other = "";
    column_name_other = "";
    is_null = false;
}

/**
 * Default constructor for IndexCondition.
 * Initializes all values with sensible default values.
 */
IndexCondition::IndexCondition() {
    table_id = -1;
    columnId = -1;
    op = Operator::UNDEFINED;
    type = VariableType::INT;
    int_lower_bound = INT_MIN;
    int_upper_bound = INT_MAX;
    int_val = -1;
    float_lower_bound = std::numeric_limits<double>::min();
    float_upper_bound = std::numeric_limits<double>::max();
    float_val = -1;
    str_val = "";
    table_id_other = -1;
    columnIdOther = -1;
    is_null = false;
}

/**
 * Constructor to create an IndexCondition from a Condition.
 * @param table_id_ The table ID associated with the index condition.
 * @param columnId_ The column ID associated with the index condition.
 * @param condition The condition to be converted into an index condition.
 * @param sm The system manager responsible for handling the table's metadata.
 */
IndexCondition::IndexCondition(int table_id_, int columnId_,
                               Condition& condition,
                               system::SystemManager* sm) {
    table_id = table_id_;
    columnId = columnId_;
    op = condition.op;
    type = condition.type;

    int_lower_bound = INT_MIN;
    int_upper_bound = INT_MAX;
    int_val = -1;
    float_lower_bound = std::numeric_limits<double>::min();
    float_upper_bound = std::numeric_limits<double>::max();
    float_val = -1;
    str_val = "";
    table_id_other = -1;
    columnIdOther = -1;
    is_null = false;

    // Process based on the variable type (INT, FLOAT, STRING, etc.)
    if (type == VariableType::INT) {
        switch (op) {
            case Operator::EQ:
                int_val = condition.int_val;
                break;
            case Operator::NEQ:
                int_val = condition.int_val;
                break;
            case Operator::LT:
                int_upper_bound = condition.int_val - 1;
                break;
            case Operator::LEQ:
                int_upper_bound = condition.int_val;
                break;
            case Operator::GT:
                int_lower_bound = condition.int_val + 1;
                break;
            case Operator::GEQ:
                int_lower_bound = condition.int_val;
                break;
            default:
                break;
        }
    } else if (type == VariableType::FLOAT) {
        switch (op) {
            case Operator::EQ:
                float_val = condition.float_val;
                break;
            case Operator::NEQ:
                float_val = condition.float_val;
                break;
            case Operator::LT:
                float_upper_bound = condition.float_val;
                break;
            case Operator::LEQ:
                float_upper_bound = condition.float_val;
                break;
            case Operator::GT:
                float_lower_bound = condition.float_val;
                break;
            case Operator::GEQ:
                float_lower_bound = condition.float_val;
                break;
            default:
                break;
        }
    } else if (type == VariableType::STRING) {
        str_val = condition.string_val;
    } else if (type == VariableType::TABLE_COLUMN) {
        // Resolve table and column IDs for references to other tables
        table_id_other = sm->getTableId(condition.table_name_other.c_str());

        std::vector<record::ColumnType> column_types;
        if (table_id_other == -1) {
            std::cout << Color::FAIL << "!ERROR!" << Color::ENDC << std::endl;
            std::cout << "Table " << condition.table_name_other
                    << " does not exist" << std::endl;
        }
        sm->getTableColumnTypes(table_id_other, column_types);
        for (int i = 0; i < column_types.size(); i++) {
            if (column_types[i].columnName == condition.column_name_other) {
                columnIdOther = column_types[i].columnId;
                break;
            }
        }
        if (columnIdOther == -1) {
            std::cout << Color::FAIL << "!ERROR!" << Color::ENDC << std::endl;
            std::cout << "Column " << condition.column_name_other
                      << " does not exist" << std::endl;
        }
    } else if (type == VariableType::NULL_OR_NOT) {
        is_null = condition.is_null;
    }
}

/**
 * Updates the index condition with a new condition.
 * @param table_id_ The table ID of the new condition.
 * @param columnId_ The column ID of the new condition.
 * @param condition The new condition to be used for updating.
 * @return true if the condition was updated successfully, false otherwise.
 */
bool IndexCondition::update(int table_id_, int columnId_,
                            Condition& condition) {
    if (table_id != table_id_ || columnId != columnId_) return false;
    if (type == VariableType::STRING || type == VariableType::TABLE_COLUMN ||
        type == VariableType::FLOAT || type == VariableType::NULL_OR_NOT ||
        type != condition.type)
        return false;

    // Update bounds based on the new condition's operator
    switch (condition.op) {
        case Operator::LT:
            if (type == VariableType::INT) {
                if (condition.int_val - 1 < int_upper_bound) {
                    int_upper_bound = condition.int_val - 1;
                }
            } else if (type == VariableType::FLOAT) {
                if (condition.float_val < float_upper_bound) {
                    float_upper_bound = condition.float_val;
                }
            }
            break;
        case Operator::LEQ:
            if (type == VariableType::INT) {
                if (condition.int_val < int_upper_bound) {
                    int_upper_bound = condition.int_val;
                }
            } else if (type == VariableType::FLOAT) {
                if (condition.float_val < float_upper_bound) {
                    float_upper_bound = condition.float_val;
                }
            }
            break;
        case Operator::GT:
            if (type == VariableType::INT) {
                if (condition.int_val + 1 > int_lower_bound) {
                    int_lower_bound = condition.int_val + 1;
                }
            } else if (type == VariableType::FLOAT) {
                if (condition.float_val > float_lower_bound) {
                    float_lower_bound = condition.float_val;
                }
            }
            break;
        case Operator::GEQ:
            if (type == VariableType::INT) {
                if (condition.int_val > int_lower_bound) {
                    int_lower_bound = condition.int_val;
                }
            } else if (type == VariableType::FLOAT) {
                if (condition.float_val > float_lower_bound) {
                    float_lower_bound = condition.float_val;
                }
            }
            break;
    }
    return true;
}

/**
 * Updates all index conditions based on a list of conditions.
 * @param index_conditions The list of existing index conditions.
 * @param conditions The list of new conditions to update the index conditions.
 * @param sm The system manager used to access table metadata.
 * @param table_names The list of table names that may contain the conditions.
 * @return true if the index conditions were updated successfully.
 */
bool updateAllIndexConditions(std::vector<IndexCondition>& index_conditions,
                               std::vector<Condition>& conditions,
                               system::SystemManager* sm,
                               std::vector<std::string> table_names) {
    bool updated = false;
    std::string selected_table_name = "";

    for (auto& condition : conditions) {
        // Get table name
        if (condition.table_name == "") {
            if (table_names.size() == 1) {
                selected_table_name = table_names[0];
            } else {
                selected_table_name = sm->findTableNameOfColumnName(condition.column_name, table_names);
            }
            if (selected_table_name == "") {
                std::cout << Color::FAIL << "!ERROR!" << Color::ENDC << std::endl;
                std::cout << "No table contains column " << condition.column_name << std::endl;
                return false;
            }
        } else {
            selected_table_name = condition.table_name;
        }

        // Get table ID
        int table_id = sm->getTableId(selected_table_name.c_str());
        if (table_id == -1) {
            std::cout << Color::FAIL << "!ERROR!" << Color::ENDC << std::endl;
            std::cout << "Table " << selected_table_name << " does not exist" << std::endl;
            return false;
        }

        // Get column ID
        int columnId;
        std::vector<record::ColumnType> column_types;
        sm->getTableColumnTypes(table_id, column_types);
        for (int i = 0; i < column_types.size(); i++) {
            if (column_types[i].columnName == condition.column_name) {
                columnId = column_types[i].columnId;
                break;
            }
        }
        if (columnId == -1) {
            std::cout << Color::FAIL << "!ERROR!" << Color::ENDC << std::endl;
            std::cout << "Column " << condition.column_name << " does not exist" << std::endl;
            return false;
        }

        // Update index condition
        for (auto& prev_condition : index_conditions) {
            if (prev_condition.table_id == table_id && prev_condition.columnId == columnId) {
                if (prev_condition.update(table_id, columnId, condition)) {
                    updated = true;
                    break;
                }
            }
        }
        if (!updated)
            index_conditions.push_back(IndexCondition(table_id, columnId, condition, sm));
        updated = false;
    }

    return true;
}

}  // namespace condition
}  // namespace dbs
