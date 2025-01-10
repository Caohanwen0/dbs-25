#include "system/SystemColumns.hpp"
#include "common/Color.hpp"

namespace dbs {
namespace system {

ForeignKeyInputInfo::ForeignKeyInputInfo() {
    fKColumnNames = std::vector<std::string>();
    reference_table_name = "";
    reference_column_names = std::vector<std::string>();
}

ForeignKeyInputInfo::ForeignKeyInputInfo(
    const std::vector<std::string> & fKColumnNames_,
    const std::string & reference_table_name_,
    const std::vector<std::string> & reference_column_names_) {
    fKColumnNames = fKColumnNames_;
    reference_table_name = reference_table_name_;
    reference_column_names = reference_column_names_;
}

ForeignKeyInfo::ForeignKeyInfo() {
    foreign_key_columnIds = std::vector<int>();
    reference_table_id = -1;
    reference_columnIds = std::vector<int>();
    name = "";
}

ForeignKeyInfo::ForeignKeyInfo(const std::vector<int>& foreign_key_columnIds_,
                               int reference_table_id_,
                               const std::vector<int>& reference_columnIds_,
                               std::string name_) {
    foreign_key_columnIds = foreign_key_columnIds_;
    reference_table_id = reference_table_id_;
    reference_columnIds = reference_columnIds_;
    name = name_;
}

bool mergeConstraints(std::vector<SearchConstraint>& constraints) {
    int num = constraints.size();
    for (int i = 0; i < num; i++) {
        for (int j = i + 1; j < num; j++) {
            if (constraints[i].columnId == constraints[j].columnId) {
                constraints[i].constraintTypes.insert(
                    constraints[i].constraintTypes.end(),
                    constraints[j].constraintTypes.begin(),
                    constraints[j].constraintTypes.end());
                constraints[i].constraintValues.insert(
                    constraints[i].constraintValues.end(),
                    constraints[j].constraintValues.begin(),
                    constraints[j].constraintValues.end());
                constraints.erase(constraints.begin() + j);
                num--;
                j--;
            }
        }
    }
    bool valid = true;
    for (auto& constraint : constraints) {
        if (constraint.dataType == record::DataTypeIdentifier::INT) {
            int num = constraint.constraintTypes.size();
            record::DataValue lower_bound(record::DataTypeIdentifier::INT, true,
                                          INT_MIN);
            record::DataValue upper_bound(record::DataTypeIdentifier::INT, true,
                                          INT_MAX);
            std::vector<record::DataValue> neq_val;
            std::vector<record::DataValue> null_val;
            std::vector<ConstraintType> null_type;
            for (int i = 0; i < num; i++) {
                if (constraint.constraintValues[i].isNull) {
                    null_val.push_back(constraint.constraintValues[i]);
                    null_type.push_back(constraint.constraintTypes[i]);
                    continue;
                }
                if (constraint.constraintTypes[i] == ConstraintType::EQ) {
                    lower_bound =
                        std::max(lower_bound, constraint.constraintValues[i]);
                    upper_bound =
                        std::min(upper_bound, constraint.constraintValues[i]);
                } else if (constraint.constraintTypes[i] ==
                           ConstraintType::NEQ) {
                    neq_val.push_back(constraint.constraintValues[i]);
                } else if (constraint.constraintTypes[i] ==
                           ConstraintType::GT) {
                    lower_bound = std::max(
                        lower_bound,
                        record::DataValue(
                            record::DataTypeIdentifier::INT, false,
                            constraint.constraintValues[i].value.intValue + 1));

                } else if (constraint.constraintTypes[i] ==
                           ConstraintType::GEQ) {
                    lower_bound =
                        std::max(lower_bound, constraint.constraintValues[i]);
                } else if (constraint.constraintTypes[i] ==
                           ConstraintType::LT) {
                    upper_bound = std::min(
                        upper_bound,
                        record::DataValue(
                            record::DataTypeIdentifier::INT, false,
                            constraint.constraintValues[i].value.intValue -
                                1));
                } else if (constraint.constraintTypes[i] ==
                           ConstraintType::LEQ) {
                    upper_bound =
                        std::min(upper_bound, constraint.constraintValues[i]);
                }
            }
            constraint.constraintValues.clear();
            constraint.constraintTypes.clear();
            for (int i = 0; i < null_type.size(); i++) {
                constraint.constraintValues.push_back(null_val[i]);
                constraint.constraintTypes.push_back(null_type[i]);
            }
            if (!lower_bound.isNull && !upper_bound.isNull &&
                upper_bound < lower_bound) {
                valid = false;
                break;
            } else {
                if (!lower_bound.isNull) {
                    constraint.constraintValues.push_back(lower_bound);
                    constraint.constraintTypes.push_back(ConstraintType::GEQ);
                }
                if (!upper_bound.isNull) {
                    constraint.constraintValues.push_back(upper_bound);
                    constraint.constraintTypes.push_back(ConstraintType::LEQ);
                }
                for (auto& val : neq_val) {
                    if (!lower_bound.isNull && val < lower_bound) {
                        continue;
                    }
                    if (!upper_bound.isNull && upper_bound < val) {
                        continue;
                    }
                    constraint.constraintValues.push_back(val);
                    constraint.constraintTypes.push_back(ConstraintType::NEQ);
                }
            }
        } else {
            record::DataValue lower_bound(record::DataTypeIdentifier::FLOAT, true,
                                          -FLOAT_MAX);
            record::DataValue upper_bound(record::DataTypeIdentifier::FLOAT, true,
                                          FLOAT_MAX);
            if (constraint.dataType == record::DataTypeIdentifier::FLOAT) {
                lower_bound = record::DataValue(record::DataTypeIdentifier::FLOAT,
                                                true, -FLOAT_MAX);
                upper_bound = record::DataValue(record::DataTypeIdentifier::FLOAT,
                                                true, FLOAT_MAX);
            } else if (constraint.dataType == record::DataTypeIdentifier::VARCHAR) {
                lower_bound =
                    record::DataValue(record::DataTypeIdentifier::VARCHAR, true, "");
                upper_bound =
                    record::DataValue(record::DataTypeIdentifier::VARCHAR, true, "");
            } else if (constraint.dataType == record::DataTypeIdentifier::DATE) {
                lower_bound =
                    record::DataValue(record::DataTypeIdentifier::DATE, true,
                                      record::DateValue(0, 0, 0));
                upper_bound =
                    record::DataValue(record::DataTypeIdentifier::DATE, true,
                                      record::DateValue(9999, 99, 99));
            }
            ConstraintType lower_bound_type, upper_bound_type;
            std::vector<record::DataValue> neq_val;
            std::vector<record::DataValue> null_val;
            std::vector<ConstraintType> null_type;
            int num = constraint.constraintTypes.size();
            for (int i = 0; i < num; i++) {
                if (constraint.constraintValues[i].isNull) {
                    null_val.push_back(constraint.constraintValues[i]);
                    null_type.push_back(constraint.constraintTypes[i]);
                    continue;
                }
                if (constraint.constraintTypes[i] == ConstraintType::EQ) {
                    if (lower_bound < constraint.constraintValues[i] ||
                        lower_bound.isNull) {
                        lower_bound = constraint.constraintValues[i];
                        lower_bound_type = ConstraintType::GEQ;
                    }
                    if (constraint.constraintValues[i] < upper_bound ||
                        upper_bound.isNull) {
                        upper_bound = constraint.constraintValues[i];
                        upper_bound_type = ConstraintType::LEQ;
                    }
                } else if (constraint.constraintTypes[i] ==
                           ConstraintType::NEQ) {
                    neq_val.push_back(constraint.constraintValues[i]);
                } else if (constraint.constraintTypes[i] ==
                           ConstraintType::GT) {
                    if (lower_bound <= constraint.constraintValues[i] ||
                        lower_bound.isNull) {
                        lower_bound = constraint.constraintValues[i];
                        lower_bound_type = ConstraintType::GT;
                    }
                } else if (constraint.constraintTypes[i] ==
                           ConstraintType::GEQ) {
                    if (lower_bound < constraint.constraintValues[i] ||
                        lower_bound.isNull) {
                        lower_bound = constraint.constraintValues[i];
                        lower_bound_type = ConstraintType::GEQ;
                    }
                } else if (constraint.constraintTypes[i] ==
                           ConstraintType::LT) {
                    if (constraint.constraintValues[i] <= upper_bound ||
                        upper_bound.isNull) {
                        upper_bound = constraint.constraintValues[i];
                        upper_bound_type = ConstraintType::LT;
                    }
                } else if (constraint.constraintTypes[i] ==
                           ConstraintType::LEQ) {
                    if (constraint.constraintValues[i] < upper_bound ||
                        upper_bound.isNull) {
                        upper_bound = constraint.constraintValues[i];
                        upper_bound_type = ConstraintType::LEQ;
                    }
                }
            }
            constraint.constraintValues.clear();
            constraint.constraintTypes.clear();
            for (int i = 0; i < null_type.size(); i++) {
                constraint.constraintValues.push_back(null_val[i]);
                constraint.constraintTypes.push_back(null_type[i]);
            }
            if (!lower_bound.isNull && !upper_bound.isNull &&
                (upper_bound < lower_bound ||
                 (upper_bound == lower_bound &&
                  !(lower_bound_type == ConstraintType::GEQ &&
                    upper_bound_type == ConstraintType::LEQ)))) {
                valid = false;
                break;
            } else {
                if (!lower_bound.isNull) {
                    constraint.constraintValues.push_back(lower_bound);
                    constraint.constraintTypes.push_back(lower_bound_type);
                }
                if (!upper_bound.isNull) {
                    constraint.constraintValues.push_back(upper_bound);
                    constraint.constraintTypes.push_back(upper_bound_type);
                }
                for (auto& val : neq_val) {
                    if (!lower_bound.isNull && val < lower_bound ||
                        (val == lower_bound &&
                         lower_bound_type == ConstraintType::GT)) {
                        continue;
                    }
                    if (!upper_bound.isNull && upper_bound < val ||
                        (val == upper_bound &&
                         upper_bound_type == ConstraintType::LT)) {
                        continue;
                    }
                    constraint.constraintValues.push_back(val);
                    constraint.constraintTypes.push_back(ConstraintType::NEQ);
                }
            }
        }
    }
    return valid;
}

bool validConstraint(const SearchConstraint& constraint,
                     const record::DataItem& item) {
    for (int i = 0; i < item.columnIds.size(); i++) {
        if (constraint.columnId == item.columnIds[i]) {
            for (int j = 0; j < constraint.constraintTypes.size(); j++) {
                if (constraint.constraintValues[j].isNull) {
                    if (constraint.constraintTypes[j] == ConstraintType::EQ) {
                        if (!item.values[i].isNull) {
                            return false;
                        }
                    } else if (constraint.constraintTypes[j] ==
                               ConstraintType::NEQ) {
                        if (item.values[i].isNull) {
                            return false;
                        }
                    }
                    continue;
                }
                if (constraint.constraintTypes[j] == ConstraintType::EQ) {
                    if (!(item.values[i] ==constraint.constraintValues[j])) {
                        return false;
                    }
                } 
                else if (constraint.constraintTypes[j] ==
                           ConstraintType::NEQ) {
                    if (item.values[i] ==
                        constraint.constraintValues[j]) {
                        return false;
                    }
                } else if (constraint.constraintTypes[j] ==
                           ConstraintType::GT) {
                    if (item.values[i] <=
                            constraint.constraintValues[j] ||
                        item.values[i].isNull) {
                        return false;
                    }
                } else if (constraint.constraintTypes[j] ==
                           ConstraintType::GEQ) {
                    if (item.values[i] < constraint.constraintValues[j] ||
                        item.values[i].isNull) {
                        return false;
                    }
                } else if (constraint.constraintTypes[j] ==
                           ConstraintType::LT) {
                    if (item.values[i] >=
                            constraint.constraintValues[j] ||
                        item.values[i].isNull) {
                        return false;
                    }
                } else if (constraint.constraintTypes[j] ==
                           ConstraintType::LEQ) {
                    if (item.values[i] > constraint.constraintValues[j] ||
                        item.values[i].isNull) {
                        return false;
                    }
                }
            }
        }
    }
    return true;
}

void filterConstraints(
    const std::vector<SearchConstraint>& constraints,
    const std::vector<record::DataItem>& data_items,
    const std::vector<record::RecordLocation>& record_locations,
    std::vector<record::DataItem>& result,
    std::vector<record::RecordLocation>& record_location_results) {
    result.clear();
    record_location_results.clear();
    for (int i = 0; i < data_items.size(); i++) {
        auto& data_item = data_items[i];
        auto& location = record_locations[i];
        bool valid = true;
        for (auto& constraint : constraints) {
            if (!validConstraint(constraint, data_item)) {
                valid = false;
                break;
            }
        }
        if (valid) {
            result.push_back(data_item);
            record_location_results.push_back(location);
        }
    }
}

void SearchConstraint::print() const {
    // print the search constraint
    // for debugging
    std::cerr << "COLUMN ID:      " << columnId << std::endl;
    std::cerr << "DATA TYPE:       " << dataType << std::endl;
    std::cerr << "CONSTRAINT TYPE: ";
    for (auto& constraint_type : constraintTypes) {
        switch (constraint_type) {
            case ConstraintType::EQ:
                std::cerr <<"EQ  ";
                break;
            case ConstraintType::NEQ:
                std::cerr  <<"NEQ " ;
                break;
            case ConstraintType::GT:
                std::cerr <<"GT  " ;
                break;
            case ConstraintType::GEQ:
                std::cerr  <<"GEQ ";
                break;
            case ConstraintType::LT:
                std::cerr <<"LT  " ;
                break;
            case ConstraintType::LEQ:
                std::cerr <<"LEQ " ;
                break;
        }
    }
    std::cerr << std::endl;
    std::cerr <<"CONSTRAINT VALUES: ";
    for (auto& constraint_value : constraintValues) {
        constraint_value.print();
        std::cerr <<" ";
    }
    std::cerr << std::endl;
}

}  // namespace system
}  // namespace dbs