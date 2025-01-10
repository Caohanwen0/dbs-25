#include "system/SystemManager.hpp"

namespace dbs {
namespace system {

SystemManager::SystemManager(fs::FileManager* fm_, record::RecordManager* rm_,
                             index::IndexManager* im_)
    : fm(fm_), rm(rm_), im(im_) {
    currentDatabaseId = -1;
    currentDatabaseName = "";

    GlobalDatabaseInfoColumnType.clear();
    GlobalDatabaseInfoColumnType.push_back(
        record::ColumnType(record::DataTypeIdentifier::VARCHAR, 255, 0, true, true,
                           record::DefaultValue(), "DATABASES"));

    DatabaseTableInfoColumnType.clear();
    DatabaseTableInfoColumnType.push_back(
        record::ColumnType(record::DataTypeIdentifier::VARCHAR, 255, 0, true, true,
                           record::DefaultValue(), "TABLES"));

    TablePrimaryKeyInfoColumnType.clear();
    TablePrimaryKeyInfoColumnType.push_back(
        record::ColumnType(record::DataTypeIdentifier::INT, 0, 0, true, true,
                           record::DefaultValue(), "PRIMARY_KEY_IDS"));

    TableForeignKeyInfoColumnType.clear();
    TableForeignKeyInfoColumnType.push_back(record::ColumnType(
        record::DataTypeIdentifier::INT, 0, 0, true, false, record::DefaultValue(),
        "FOREIGN_KEY_REFERENCE_TABLE_ID"));
    for (int i = 0; i < FOREIGN_KEY_MAX_NUM; i++) {
        TableForeignKeyInfoColumnType.push_back(
            record::ColumnType(record::DataTypeIdentifier::INT, 0, 0, false, false,
                               record::DefaultValue(),
                               "FOREIGN_KEY_columnId_" + std::to_string(i)));
    }
    for (int i = 0; i < FOREIGN_KEY_MAX_NUM; i++) {
        TableForeignKeyInfoColumnType.push_back(record::ColumnType(
            record::DataTypeIdentifier::INT, 0, 0, false, false,
            record::DefaultValue(),
            "FOREIGN_KEY_REFERENCE_columnId_" + std::to_string(i)));
    }
    TableForeignKeyInfoColumnType.push_back(record::ColumnType(
        record::DataTypeIdentifier::VARCHAR, 255, 0, false, false,
        record::DefaultValue(), "FOREIGN_KEY_REFERENCE_TABLE_NAME"));

    TableDominateInfoColumnType.clear();
    TableDominateInfoColumnType.push_back(
        record::ColumnType(record::DataTypeIdentifier::INT, 0, 0, true, false,
                           record::DefaultValue(), "DOMINATE_TABLE_ID"));

    TableIndexInfoColumnType.clear();
    for (int i = 0; i < INDEX_KEY_MAX_NUM; i++) {
        TableIndexInfoColumnType.push_back(record::ColumnType(
            record::DataTypeIdentifier::INT, 0, 0, false, false,
            record::DefaultValue(), "INDEX_columnId_" + std::to_string(i)));
    }
    TableIndexInfoColumnType.push_back(
        record::ColumnType(record::DataTypeIdentifier::VARCHAR, 255, 0, false, false,
                           record::DefaultValue(), "INDEX_NAME"));
}

SystemManager::~SystemManager() {
    fm = nullptr;
    rm = nullptr;
    im = nullptr;
}

void SystemManager::cleanSystem() {
    rm->cleanAllCurrentColumnTypes();
    rm->closeAllCurrentFile();
    im->closeAllCurrentFile();
    if (fm->doesFolderExist(DATABASE_PATH)) {
        assert(fm->deleteFolder(DATABASE_PATH));
    }
    initializeSystem();
}

void SystemManager::initializeSystem() {
    rm->cleanAllCurrentColumnTypes();
    rm->closeAllCurrentFile();
    im->closeAllCurrentFile();
    if (!fm->doesFolderExist(DATABASE_PATH)) {
        fm->createFolder(DATABASE_PATH);
        fm->createFolder(DATABASE_BASE_PATH);
        fm->createFolder(DATABASE_GLOBAL_PATH);
        rm->initializeRecordFile(DATABASE_GLOBAL_RECORD_PATH,
                                 GlobalDatabaseInfoColumnType);
    }
}

bool SystemManager::createDatabase(const char* database_name) {
    // check if database exists
    auto id = getDatabaseId(database_name);
    if (getDatabaseId(database_name) != -1) {
        return false;
        std::cout << "!ERROR" << std::endl;
        std::cout << "Database " << database_name << " already exists"
                  << std::endl;
    }

    // insert a new record into global database
    std::vector<record::ColumnType> column_types;
    rm->getColumnTypes(DATABASE_GLOBAL_RECORD_PATH, column_types);
    assert(column_types.size() == 1);

    record::DataItem data_item;
    data_item.values.push_back(
        record::DataValue(record::DataTypeIdentifier::VARCHAR, false, database_name));
    data_item.columnIds.push_back(column_types[0].columnId);
    auto location = rm->insertRecord(DATABASE_GLOBAL_RECORD_PATH, data_item);
    assert(location.pageId != -1 && location.slotId != -1);

    // get record to updaate data_id
    assert(rm->getRecord(DATABASE_GLOBAL_RECORD_PATH, location, data_item));

    // create database folder
    char* database_path = nullptr;
    getDatabasePath(data_item.dataId, &database_path);
    assert(fm->createFolder(database_path));

    // create alltable.txt
    char* database_info_path = nullptr;
    utils::joinPaths(database_path, ALL_TABLE_FILE_NAME, &database_info_path);
    rm->initializeRecordFile(database_info_path, DatabaseTableInfoColumnType);

    delete[] database_path;
    delete[] database_info_path;

    return true;
}

bool SystemManager::dropDatabase(const char* database_name) {
    // check if database exists
    int database_id = getDatabaseId(database_name);
    if (database_id == -1) {
        std::cout << "!ERROR" << std::endl;
        std::cout << "Database " << database_name << " does not exist"
                  << std::endl;
        return false;
    }

    if (database_id == currentDatabaseId) {
        currentDatabaseId = -1;
        currentDatabaseName = "";
    }

    im->closeAllCurrentFile();
    rm->closeAllCurrentFile();
    rm->cleanAllCurrentColumnTypes();

    char* database_path = nullptr;
    getDatabasePath(database_id, &database_path);
    assert(fm->deleteFolder(database_path));

    delete[] database_path;

    // delete database record
    std::vector<record::DataItem> data_items;
    std::vector<record::RecordLocation> record_locations;
    rm->getAllRecords(DATABASE_GLOBAL_RECORD_PATH, data_items,
                      record_locations);

    for (int i = 0; i < data_items.size(); i++) {
        if (data_items[i].dataId == database_id) {
            rm->deleteRecord(DATABASE_GLOBAL_RECORD_PATH, record_locations[i]);
            return true;
        }
    }

    std::cout << "!ERROR" << std::endl;
    std::cout << "Database " << database_name << " does not exist" << std::endl;
    return false;
}

int SystemManager::getDatabaseId(const char* database_name) {
    std::vector<record::DataItem> data_items;
    std::vector<record::RecordLocation> record_locations;
    rm->getAllRecords(DATABASE_GLOBAL_RECORD_PATH, data_items,
                      record_locations);

    // searchRowsInTable for the database name
    for (auto& item : data_items) {
        if (strcmp(item.values[0].value.charValue.c_str(),
                   database_name) == 0) {
            return item.dataId;
        }
    }
    return -1;
}


bool SystemManager::deleteForeignKey(const char* table_name,
                                     std::string foreign_key_name) {
    // check db id
    if (currentDatabaseId == -1) {
        std::cout << "!ERROR" << std::endl;
        std::cout << "No database selected" << std::endl;
        return false;
    }

    int table_id = getTableId(table_name);

    // check if table exists
    if (table_id == -1) {
        std::cout << "!ERROR" << std::endl;
        std::cout << "Table " << table_name << " does not exist" << std::endl;
        return false;
    }

    // get fk key path
    char* table_path = nullptr;
    getTableRecordPath(currentDatabaseId, table_id, &table_path);
    char* foreign_key_path = nullptr;
    utils::joinPaths(table_path, FOREIGN_KEY_FILE_NAME, &foreign_key_path);
    // get all record from fk key file
    std::vector<record::DataItem> data_items;
    std::vector<record::RecordLocation> record_locations;
    rm->getAllRecords(foreign_key_path, data_items, record_locations);

    for (int i = 0; i < data_items.size(); i++) {
        auto& data_item = data_items[i];
        std::string name =
            data_item.values[FOREIGN_KEY_MAX_NUM * 2 + 1].value.charValue;
        if (name != foreign_key_name) {
            continue;
        }
        auto& record_location = record_locations[i];
        // delete record
        rm->deleteRecord(foreign_key_path, record_location);
        // delete dominates
        char* reference_table_path = nullptr;
        getTableRecordPath(currentDatabaseId,
                           data_item.values[0].value.intValue,
                           &reference_table_path);
        char* reference_table_dominate_path = nullptr;
        utils::joinPaths(reference_table_path, DOMINATE_FILE_NAME,
                          &reference_table_dominate_path);
        // get all record from reference dominate
        std::vector<record::DataItem> reference_table_data_items;
        std::vector<record::RecordLocation> reference_table_record_locations;
        rm->getAllRecords(reference_table_dominate_path,
                          reference_table_data_items,
                          reference_table_record_locations);
        for (int j = 0; j < reference_table_data_items.size(); j++) {
            auto& reference_table_data_item = reference_table_data_items[j];
            if (reference_table_data_item.values[0].value.intValue ==
                table_id) {
                rm->deleteRecord(reference_table_dominate_path,
                                 reference_table_record_locations[j]);
                break;
            }
        }
        delete[] reference_table_path;
        delete[] reference_table_dominate_path;
        delete[] table_path;
        delete[] foreign_key_path;

        return true;
    }
    std::cout << "!ERROR" << std::endl;
    std::cout << "Foreign key " << foreign_key_name << " does not exist"
              << std::endl;
    delete[] table_path;
    delete[] foreign_key_path;
    return false;
}

bool SystemManager::addForeignKey(const char* table_name,
                                  const ForeignKeyInfo& new_foreign_key) {
    // check db id
    if (currentDatabaseId == -1) {
        std::cout << "!ERROR" << std::endl;
        std::cout << "No database selected" << std::endl;
        return false;
    }

    int table_id = getTableId(table_name);

    // check if table exists
    if (table_id == -1) {
        std::cout << "!ERROR" << std::endl;
        std::cout << "Table " << table_name << " does not exist" << std::endl;
        return false;
    }

    // get current fk key
    std::vector<ForeignKeyInfo> foreign_key_infos;
    getTableForeignKeys(table_id, foreign_key_infos);

    // check if already exist
    for (auto& fk_info : foreign_key_infos) {
        if (fk_info == new_foreign_key) {
            std::cout << "!ERROR" << std::endl;
            std::cout << "foreign" << std::endl;
            return false;
        }
    }

    // get all record
    char* table_path = nullptr;
    getTableRecordPath(currentDatabaseId, table_id, &table_path);
    char* record_path = nullptr;
    utils::joinPaths(table_path, RECORD_FILE_NAME, &record_path);
    std::vector<record::ColumnType> column_types;
    rm->getColumnTypes(record_path, column_types);
    std::vector<record::DataItem> data_items;
    std::vector<record::RecordLocation> record_locations;
    rm->getAllRecords(record_path, data_items, record_locations);

    delete[] record_path;

    // check all record satisfy new fk constraint
    for (auto& data_item : data_items) {
        std::vector<SearchConstraint> constraints;
        for (int fk_i = 0; fk_i < new_foreign_key.foreign_key_columnIds.size();
             fk_i++) {
            auto& foreign_key_columnId =
                new_foreign_key.foreign_key_columnIds[fk_i];
            auto& reference_columnId =
                new_foreign_key.reference_columnIds[fk_i];

            SearchConstraint constraint;
            constraint.columnId = reference_columnId;
            constraint.constraintTypes.push_back(ConstraintType::EQ);
            constraint.dataType = record::DataTypeIdentifier::INT;
            for (int i = 0; i < data_item.values.size(); i++) {
                if (data_item.columnIds[i] == foreign_key_columnId) {
                    if (data_item.values[i].isNull) {
                        std::cout << "!ERROR" << std::endl;
                        std::cout << "foreign" << std::endl;
                        delete[] table_path;
                        return false;
                    }
                    if (data_item.values[i].dataType !=
                        record::DataTypeIdentifier::INT) {
                        std::cout << "!ERROR" << std::endl;
                        std::cout << "foreign" << std::endl;
                        delete[] table_path;
                        return false;
                    }
                    constraint.constraintValues.push_back(
                        data_item.values[i]);
                    break;
                }
            }
            constraints.push_back(constraint);
        }

        std::vector<record::DataItem> result_datas;
        std::vector<record::ColumnType> result_column_types;
        std::vector<record::RecordLocation> record_locations;
        searchRowsInTable(new_foreign_key.reference_table_id, constraints, result_datas,
               result_column_types, record_locations, -1);
        if (result_datas.size() == 0) {
            std::cout << "!ERROR" << std::endl;
            std::cout << "foreign" << std::endl;
            delete[] table_path;
            return false;
        }
    }

    // insert a new record into foreign key
    char* foreign_key_path = nullptr;
    utils::joinPaths(table_path, FOREIGN_KEY_FILE_NAME, &foreign_key_path);

    delete[] table_path;
    std::vector<record::ColumnType> foreign_key_column_types;
    rm->getColumnTypes(foreign_key_path, foreign_key_column_types);
    record::DataItem data_item;
    data_item.values.push_back(record::DataValue(
        record::DataTypeIdentifier::INT, false, new_foreign_key.reference_table_id));
    data_item.columnIds.push_back(foreign_key_column_types[0].columnId);
    for (int i = 0; i < FOREIGN_KEY_MAX_NUM; i++) {
        if (i < new_foreign_key.foreign_key_columnIds.size()) {
            data_item.values.push_back(
                record::DataValue(record::DataTypeIdentifier::INT, false,
                                  new_foreign_key.foreign_key_columnIds[i]));
            data_item.columnIds.push_back(
                foreign_key_column_types[i + 1].columnId);
        } else {
            data_item.values.push_back(
                record::DataValue(record::DataTypeIdentifier::INT, true));
            data_item.columnIds.push_back(
                foreign_key_column_types[i + 1].columnId);
        }
    }
    for (int i = 0; i < FOREIGN_KEY_MAX_NUM; i++) {
        if (i < new_foreign_key.reference_columnIds.size()) {
            data_item.values.push_back(
                record::DataValue(record::DataTypeIdentifier::INT, false,
                                  new_foreign_key.reference_columnIds[i]));
            data_item.columnIds.push_back(
                foreign_key_column_types[i + 1 + FOREIGN_KEY_MAX_NUM]
                    .columnId);
        } else {
            data_item.values.push_back(
                record::DataValue(record::DataTypeIdentifier::INT, true));
            data_item.columnIds.push_back(
                foreign_key_column_types[i + 1 + FOREIGN_KEY_MAX_NUM]
                    .columnId);
        }
    }
    data_item.values.push_back(record::DataValue(
        record::DataTypeIdentifier::VARCHAR, false, new_foreign_key.name));
    data_item.columnIds.push_back(
        foreign_key_column_types[FOREIGN_KEY_MAX_NUM * 2 + 1].columnId);
    auto location = rm->insertRecord(foreign_key_path, data_item);
    assert(location.pageId != -1 && location.slotId != -1);

    delete[] foreign_key_path;

    //  add dominates for refernce table
    char* reference_table_path = nullptr;
    getTableRecordPath(currentDatabaseId, new_foreign_key.reference_table_id,
                       &reference_table_path);
    char* reference_table_dominate_path = nullptr;
    utils::joinPaths(reference_table_path, DOMINATE_FILE_NAME,
                      &reference_table_dominate_path);
    std::vector<record::ColumnType> reference_table_dominate_column_types;
    rm->getColumnTypes(reference_table_dominate_path,
                       reference_table_dominate_column_types);
    record::DataItem reference_table_data_item;
    reference_table_data_item.values.push_back(
        record::DataValue(record::DataTypeIdentifier::INT, false, table_id));
    reference_table_data_item.columnIds.push_back(
        reference_table_dominate_column_types[0].columnId);
    rm->insertRecord(reference_table_dominate_path, reference_table_data_item);

    delete[] reference_table_path;
    delete[] reference_table_dominate_path;

    return true;
}

bool SystemManager::createTable(
    const char* table_name, std::vector<record::ColumnType>& column_types,
    const std::vector<std::string>& primary_keys,
    const std::vector<ForeignKeyInputInfo>& foreign_keys) {
    // check db id
    if (currentDatabaseId == -1) {
        std::cout << "!ERROR" << std::endl;
        std::cout << "No database selected" << std::endl;
        return false;
    }

    // check if table exists
    if (getTableId(table_name) != -1) {
        std::cout << "!ERROR" << std::endl;
        std::cout << "Table " << table_name << " already exists" << std::endl;
        return false;
    }

    // check column type has different name
    std::set<std::string> column_names;
    int column_names_idx = 0;
    for (auto& column_type : column_types) {
        if (column_names.find(column_type.columnName) != column_names.end()) {
            std::cout << "!ERROR" << std::endl;
            std::cout << "Multiple columns have the same name" << std::endl;
            return false;
        }
        column_names.insert(column_type.columnName);
        column_type.columnId = column_names_idx++;
    }

    // check primary key
    std::vector<int> primary_key_ids;
    for (auto& primary_key : primary_keys) {
        int find_idx = -1;
        for (int i = 0; i < column_types.size(); i++) {
            if (column_types[i].columnName== primary_key) {
                find_idx = i;
                break;
            }
        }
        if (find_idx == -1) {
            std::cout << "!ERROR" << std::endl;
            std::cout << "Primary key " << primary_key << " does not exist"
                      << std::endl;
            return false;
        }
        if (column_types[find_idx].dataType != record::DataTypeIdentifier::INT) {
            std::cout << "!ERROR" << std::endl;
            std::cout << "Primary key " << primary_key << " is not of type INT"
                      << std::endl;
            return false;
        }
        column_types[find_idx].isNotNull = true;
        primary_key_ids.push_back(find_idx);
    }

    // check foreign key
    std::vector<ForeignKeyInfo> foreign_key_infos;
    for (auto& foreign_key : foreign_keys) {
        int reference_table_id =
            getTableId(foreign_key.reference_table_name.c_str());
        if (reference_table_id == -1) {
            std::cout << "!ERROR" << std::endl;
            std::cout << "Foreign key reference table "
                      << foreign_key.reference_table_name << " does not exist"
                      << std::endl;
            return false;
        }

        std::vector<record::ColumnType> reference_table_column_types;
        getTableColumnTypes(reference_table_id, reference_table_column_types);

        std::vector<int> reference_columnIds;
        for (auto& column_name : foreign_key.reference_column_names) {
            int find_idx = -1;
            for (int i = 0; i < reference_table_column_types.size(); i++) {
                if (reference_table_column_types[i].columnName==
                    column_name) {
                    find_idx = i;
                    break;
                }
            }
            if (find_idx == -1) {
                std::cout << "!ERROR" << std::endl;
                std::cout << "Foreign key reference column " << column_name
                          << " does not exist" << std::endl;
                return false;
            }
            reference_columnIds.push_back(
                reference_table_column_types[find_idx].columnId);
        }

        std::set<int> reference_table_primary_keys;
        getTablePrimaryKeys(reference_table_id, reference_table_primary_keys);

        // check if reference_table_primary_keys == refrence_columnIds
        if (reference_table_primary_keys.size() !=
            reference_columnIds.size()) {
            std::cout << "!ERROR" << std::endl;
            std::cout << "Foreign key reference column does not match"
                      << std::endl;
            return false;
        }
        for (auto& reference_table_primary_key : reference_table_primary_keys) {
            bool find = false;
            for (size_t i = 0; i < reference_columnIds.size(); i++) {
                if (reference_table_primary_key == reference_columnIds[i]) {
                    find = true;
                    break;
                }
            }
            if (!find) {
                std::cout << "!ERROR" << std::endl;
                std::cout << "Foreign key reference column does not "
                             "match"
                          << std::endl;
                return false;
            }
        }

        std::vector<int> foreign_key_columnIds;
        for (auto& column_name : foreign_key.fKColumnNames) {
            int find_idx = -1;
            for (int i = 0; i < column_types.size(); i++) {
                if (column_types[i].columnName== column_name) {
                    find_idx = i;
                    break;
                }
            }
            if (find_idx == -1) {
                std::cout << "!ERROR" << std::endl;
                std::cout << "Foreign key column " << column_name
                          << " does not exist" << std::endl;
                return false;
            }
            foreign_key_columnIds.push_back(find_idx);
        }

        if (foreign_key_columnIds.size() != reference_columnIds.size()) {
            std::cout << "!ERROR" << std::endl;
            std::cout << "Foreign key column does not match" << std::endl;
            return false;
        }

        foreign_key_infos.push_back(ForeignKeyInfo(foreign_key_columnIds,
                                                   reference_table_id,
                                                   reference_columnIds, ""));
    }

    // get all table path
    char* db_base_path = nullptr;
    getDatabasePath(currentDatabaseId, &db_base_path);
    char* all_table_path = nullptr;
    utils::joinPaths(db_base_path, ALL_TABLE_FILE_NAME, &all_table_path);

    delete[] db_base_path;

    // get column info from all table
    std::vector<record::ColumnType> all_table_column_types;
    rm->getColumnTypes(all_table_path, all_table_column_types);

    // insert a new record into all table
    record::DataItem data_item;
    data_item.values.push_back(
        record::DataValue(record::DataTypeIdentifier::VARCHAR, false, table_name));
    data_item.columnIds.push_back(all_table_column_types[0].columnId);
    auto location = rm->insertRecord(all_table_path, data_item);

    // get record to updateRows data_id
    assert(rm->getRecord(all_table_path, location, data_item));
    int table_id = data_item.dataId;

    delete[] all_table_path;

    // create table folder
    char* table_path = nullptr;
    getTableRecordPath(currentDatabaseId, table_id, &table_path);
    assert(fm->createFolder(table_path));

    // Record path
    char* record_path = nullptr;
    utils::joinPaths(table_path, RECORD_FILE_NAME, &record_path);
    rm->initializeRecordFile(record_path, column_types);
    delete[] record_path;

    // primary key path
    char* primary_key_path = nullptr;
    utils::joinPaths(table_path, PRIMARY_KEY_FILE_NAME, &primary_key_path);
    rm->initializeRecordFile(primary_key_path, TablePrimaryKeyInfoColumnType);
    std::vector<record::ColumnType> primary_key_table_column_types;
    rm->getColumnTypes(primary_key_path, primary_key_table_column_types);
    for (auto& primary_key_id : primary_key_ids) {
        record::DataItem data_item;
        data_item.values.push_back(record::DataValue(
            record::DataTypeIdentifier::INT, false, primary_key_id));
        data_item.columnIds.push_back(
            primary_key_table_column_types[0].columnId);
        rm->insertRecord(primary_key_path, data_item);
    }
    delete[] primary_key_path;

    // foreign key path
    char* foreign_key_path = nullptr;
    utils::joinPaths(table_path, FOREIGN_KEY_FILE_NAME, &foreign_key_path);
    rm->initializeRecordFile(foreign_key_path, TableForeignKeyInfoColumnType);
    std::vector<record::ColumnType> foreign_key_table_column_types;
    rm->getColumnTypes(foreign_key_path, foreign_key_table_column_types);
    for (auto& foreign_key_info : foreign_key_infos) {
        record::DataItem data_item;
        data_item.values.push_back(
            record::DataValue(record::DataTypeIdentifier::INT, false,
                              foreign_key_info.reference_table_id));
        data_item.columnIds.push_back(
            foreign_key_table_column_types[0].columnId);
        for (int i = 0; i < FOREIGN_KEY_MAX_NUM; i++) {
            if (i < foreign_key_info.foreign_key_columnIds.size()) {
                data_item.values.push_back(record::DataValue(
                    record::DataTypeIdentifier::INT, false,
                    foreign_key_info.foreign_key_columnIds[i]));
                data_item.columnIds.push_back(
                    foreign_key_table_column_types[i + 1].columnId);
            } else {
                data_item.values.push_back(
                    record::DataValue(record::DataTypeIdentifier::INT, true));
                data_item.columnIds.push_back(
                    foreign_key_table_column_types[i + 1].columnId);
            }
        }
        for (int i = 0; i < FOREIGN_KEY_MAX_NUM; i++) {
            if (i < foreign_key_info.reference_columnIds.size()) {
                data_item.values.push_back(record::DataValue(
                    record::DataTypeIdentifier::INT, false,
                    foreign_key_info.reference_columnIds[i]));
                data_item.columnIds.push_back(
                    foreign_key_table_column_types[i + 1 + FOREIGN_KEY_MAX_NUM]
                        .columnId);
            } else {
                data_item.values.push_back(
                    record::DataValue(record::DataTypeIdentifier::INT, true));
                data_item.columnIds.push_back(
                    foreign_key_table_column_types[i + 1 + FOREIGN_KEY_MAX_NUM]
                        .columnId);
            }
        }
        data_item.values.push_back(record::DataValue(
            record::DataTypeIdentifier::VARCHAR, true, foreign_key_info.name));
        data_item.columnIds.push_back(
            foreign_key_table_column_types[FOREIGN_KEY_MAX_NUM * 2 + 1]
                .columnId);
        rm->insertRecord(foreign_key_path, data_item);
    }
    delete[] foreign_key_path;

    // dominate path
    char* dominate_path = nullptr;
    utils::joinPaths(table_path, DOMINATE_FILE_NAME, &dominate_path);
    rm->initializeRecordFile(dominate_path, TableDominateInfoColumnType);
    delete[] dominate_path;

    // change dominates for other tables
    for (auto& foreign_key_info : foreign_key_infos) {
        char* reference_table_path = nullptr;
        getTableRecordPath(currentDatabaseId,
                           foreign_key_info.reference_table_id,
                           &reference_table_path);
        char* reference_table_dominate_path = nullptr;
        utils::joinPaths(reference_table_path, DOMINATE_FILE_NAME,
                          &reference_table_dominate_path);
        std::vector<record::ColumnType> reference_table_dominate_column_types;
        rm->getColumnTypes(reference_table_dominate_path,
                           reference_table_dominate_column_types);
        record::DataItem data_item;
        data_item.values.push_back(
            record::DataValue(record::DataTypeIdentifier::INT, false, table_id));
        data_item.columnIds.push_back(
            reference_table_dominate_column_types[0].columnId);
        rm->insertRecord(reference_table_dominate_path, data_item);
        delete[] reference_table_path;
        delete[] reference_table_dominate_path;
    }

    // index info path
    char* index_info_path = nullptr;
    utils::joinPaths(table_path, INDEX_INFO_FILE_NAME, &index_info_path);
    rm->initializeRecordFile(index_info_path, TableIndexInfoColumnType);

    // index folder
    char* index_folder_path = nullptr;
    utils::joinPaths(table_path, INDEX_FOLDER_NAME, &index_folder_path);
    fm->createFolder(index_folder_path);


    if (primary_key_ids.size() > 0)
        _createIndex (index_info_path, index_folder_path, primary_key_ids,
                    table_id, "");
    for (auto foreign_key_info : foreign_key_infos) {
        // if (foreign_key_info.foreign_key_columnIds.size() > 1)
        _createIndex (index_info_path, index_folder_path,
                    foreign_key_info.foreign_key_columnIds, table_id, "");
    }

    // delete path
    delete[] index_info_path;
    delete[] index_folder_path;
    delete[] table_path;

    return true;
}

bool SystemManager::addIndex(const char* table_name,
                             const std::string& index_name,
                             const std::vector<int>& columnIds,
                             bool check_unique) {
    if (currentDatabaseId == -1) {
        std::cout << "!ERROR" << std::endl;
        std::cout << "No database selected" << std::endl;
    }

    int table_id = getTableId(table_name);
    if (table_id == -1) {
        std::cout << "!ERROR" << std::endl;
        std::cout << "Table " << table_name << " does not exist" << std::endl;
        return false;
    }

    // get index info path
    char* table_path = nullptr;
    getTableRecordPath(currentDatabaseId, table_id, &table_path);
    char* index_info_path = nullptr;
    utils::joinPaths(table_path, INDEX_INFO_FILE_NAME, &index_info_path);

    // read all from index info
    std::vector<record::DataItem> data_items;
    std::vector<record::RecordLocation> record_locations;
    rm->getAllRecords(index_info_path, data_items, record_locations);

    for (int i = 0; i < data_items.size(); i++) {
        auto& data_item = data_items[i];
        auto& origin_index_name = data_item.values[INDEX_KEY_MAX_NUM];
        if (!origin_index_name.isNull &&
            origin_index_name.value.charValue == index_name) {
            std::cout << "!ERROR" << std::endl;
            std::cout << "Index already exists" << std::endl;
            delete[] table_path;
            delete[] index_info_path;
            return false;
        }
    }

    if (!check_unique) {
        for (int i = 0; i < data_items.size(); i++) {
            auto& data_item = data_items[i];
            std::vector<int> index_id;
            for (int i = 0; i < INDEX_KEY_MAX_NUM; i++) {
                if (data_item.values[i].isNull == false) {
                    index_id.push_back(
                        data_item.values[i].value.intValue);
                }
            }
            auto& origin_index_name = data_item.values[INDEX_KEY_MAX_NUM];
            bool same = true;
            if (index_id.size() != columnIds.size()) {
                same = false;
            } else {
                for (int i = 0; i < index_id.size(); i++) {
                    if (index_id[i] != columnIds[i]) {
                        same = false;
                        break;
                    }
                }
            }
            if (same) {
                if (origin_index_name.isNull) {
                    data_item.values[INDEX_KEY_MAX_NUM].isNull = false;
                    data_item.values[INDEX_KEY_MAX_NUM].value.charValue =
                        index_name;
                    data_item.values[INDEX_KEY_MAX_NUM].dataType =
                        record::DataTypeIdentifier::VARCHAR;
                    rm->updateRecord(index_info_path, record_locations[i],
                                     data_item);
                    delete[] table_path;
                    delete[] index_info_path;
                    return true;
                } else {
                    std::cout << "!ERROR" << std::endl;
                    std::cout << "Index " << index_name << " already exists"
                              << std::endl;
                    delete[] table_path;
                    delete[] index_info_path;
                    return false;
                }
            }
        }
    }

    // get record path of table_id
    char* record_path = nullptr;
    utils::joinPaths(table_path, RECORD_FILE_NAME, &record_path);
    std::vector<record::ColumnType> record_column_types;
    rm->getColumnTypes(record_path, record_column_types);
    // assert all columnIds are INT
    for (auto& columnId : columnIds) {
        bool found = false;
        for (auto& column_type : record_column_types) {
            if (column_type.columnId == columnId) {
                if (column_type.dataType != record::DataTypeIdentifier::INT) {
                    std::cout << "!ERROR" << std::endl;
                    std::cout << "Index column is not of type INT" << std::endl;
                    delete[] table_path;
                    delete[] index_info_path;
                    delete[] record_path;
                    return false;
                }
                found = true;
                break;
            }
        }
        if (!found) {
            std::cout << "!ERROR" << std::endl;
            std::cout << "Index column does not exist" << std::endl;
            delete[] table_path;
            delete[] index_info_path;
            delete[] record_path;
            return false;
        }
    }

    // create index
    int index_id = _createIndex (index_info_path, table_path, columnIds,
                               table_id, index_name);

    // insert into index
    char* index_file_path = nullptr;
    getIndexRecordPath(currentDatabaseId, table_id, index_id,
                       &index_file_path);
    std::vector<record::ColumnType> index_column_types;
    rm->getColumnTypes(index_file_path, index_column_types);

    // get all data from record file
    rm->getAllRecords(record_path, data_items, record_locations);

    // insert into index
    for (int i = 0; i < data_items.size(); i++) {
        auto& data_item = data_items[i];
        auto& location = record_locations[i];
        index::IndexValue insert_item(location.pageId, location.slotId, 0);
        for (int j = 0; j < columnIds.size(); j++) {
            int columnId = columnIds[j];
            for (int k = 0; k < data_item.values.size(); k++) {
                if (data_item.columnIds[k] == columnId) {
                    insert_item.key.push_back(
                        data_item.values[k].value.intValue);
                    break;
                }
            }
        }
        if (check_unique) {
            std::vector<index::IndexValue> search_results;
            im->searchIndex(index_file_path, insert_item, search_results);
            if (search_results.size() > 0) {
                std::cout << "!ERROR" << std::endl;
                std::cout << "duplicate" << std::endl;
                dropIndex(table_name, index_name);
                delete[] table_path;
                delete[] index_info_path;
                delete[] record_path;
                delete[] index_file_path;
                return false;
            }
        }
        im->insertIndex(index_file_path, insert_item);
    }
    delete[] index_file_path;
    delete[] record_path;
    delete[] table_path;
    delete[] index_info_path;
    return true;
}

bool SystemManager::dropIndex(const char* table_name,
                              const std::string& index_name) {
    if (currentDatabaseId == -1) {
        std::cout << "!ERROR" << std::endl;
        std::cout << "No database selected" << std::endl;
    }

    int table_id = getTableId(table_name);
    if (table_id == -1) {
        std::cout << "!ERROR" << std::endl;
        std::cout << "Table " << table_name << " does not exist" << std::endl;
        return false;
    }

    // get index info path
    char* table_path = nullptr;
    getTableRecordPath(currentDatabaseId, table_id, &table_path);
    char* index_info_path = nullptr;
    utils::joinPaths(table_path, INDEX_INFO_FILE_NAME, &index_info_path);

    // read all from index info
    std::vector<record::DataItem> data_items;
    std::vector<record::RecordLocation> record_locations;
    rm->getAllRecords(index_info_path, data_items, record_locations);

    for (int i = 0; i < data_items.size(); i++) {
        auto& data_item = data_items[i];
        auto& origin_index_name = data_item.values[INDEX_KEY_MAX_NUM];
        if (!origin_index_name.isNull &&
            origin_index_name.value.charValue == index_name) {
            // delete index
            rm->deleteRecord(index_info_path, record_locations[i]);
            // delete index file
            char* index_file_path = nullptr;
            getIndexRecordPath(currentDatabaseId, table_id, data_item.dataId,
                               &index_file_path);
            im->deleteIndexFile(index_file_path);
            delete[] table_path;
            delete[] index_info_path;
            return true;
        }
    }
    std::string index_name_with_unique = index_name + UNIQUE_SUFFIX;
    for (int i = 0; i < data_items.size(); i++) {
        auto& data_item = data_items[i];
        auto& origin_index_name = data_item.values[INDEX_KEY_MAX_NUM];
        if (!origin_index_name.isNull &&
            origin_index_name.value.charValue == index_name_with_unique) {
            // delete index
            rm->deleteRecord(index_info_path, record_locations[i]);
            // delete index file
            char* index_file_path = nullptr;
            getIndexRecordPath(currentDatabaseId, table_id, data_item.dataId,
                               &index_file_path);
            im->deleteIndexFile(index_file_path);
            delete[] table_path;
            delete[] index_info_path;
            return true;
        }
    }

    std::cout << "!ERROR" << std::endl;
    std::cout << "Index " << index_name << " does not exist" << std::endl;
    delete[] table_path;
    delete[] index_info_path;
    return false;
}

int SystemManager::_createIndex (const char* index_info_path,
                               const char* index_folder_path,
                               std::vector<int> index_ids, int table_id,
                               std::string name) {
    std::vector<record::ColumnType> index_info_column_types;
    rm->getColumnTypes(index_info_path, index_info_column_types);
    record::DataItem data_item;
    for (int i = 0; i <= INDEX_KEY_MAX_NUM; i++) {
        data_item.values.push_back(
            record::DataValue(record::DataTypeIdentifier::INT, true));
        data_item.columnIds.push_back(index_info_column_types[i].columnId);
    }

    for (int i = 0; i < index_ids.size(); i++) {
        data_item.values[i].isNull = false;
        data_item.values[i].value.intValue = index_ids[i];
    }
    if (name == "") {
        // null
        data_item.values[INDEX_KEY_MAX_NUM].isNull = true;
        data_item.values[INDEX_KEY_MAX_NUM].dataType =
            record::DataTypeIdentifier::VARCHAR;
    } else {
        data_item.values[INDEX_KEY_MAX_NUM].isNull = false;
        data_item.values[INDEX_KEY_MAX_NUM].value.charValue = name;
        data_item.values[INDEX_KEY_MAX_NUM].dataType =
            record::DataTypeIdentifier::VARCHAR;
    }
    auto location = rm->insertRecord(index_info_path, data_item);
    assert(location.pageId != -1);
    // updateRows data_item
    assert(rm->getRecord(index_info_path, location, data_item));

    // create index file
    char* index_file_path = nullptr;
    getIndexRecordPath(currentDatabaseId, table_id, data_item.dataId,
                       &index_file_path);
    im->initializeIndexFile(index_file_path, index_ids.size());

    // delete path
    delete[] index_file_path;
    return data_item.dataId;
}

bool SystemManager::insertIntoTable(const char* table_name,
                                    std::vector<record::DataItem>& data_items) {
    // check db id
    if (currentDatabaseId == -1) {
        std::cout << "!ERROR" << std::endl;
        std::cout << "No database selected" << std::endl;
        return false;
    }

    // get table id
    int table_id = getTableId(table_name);
    if (table_id == -1) {
        std::cout << "!ERROR" << std::endl;
        std::cout << "Table " << table_name << " does not exist" << std::endl;
        return false;
    }

    // get table path
    char* table_path = nullptr;
    getTableRecordPath(currentDatabaseId, table_id, &table_path);

    // get record path
    char* record_path = nullptr;
    utils::joinPaths(table_path, RECORD_FILE_NAME, &record_path);

    // get column types
    std::vector<record::ColumnType> column_types;
    rm->getColumnTypes(record_path, column_types);

    // get primary keys
    std::set<int> primary_keys;
    getTablePrimaryKeys(table_id, primary_keys);

    // get foreign keys
    std::vector<ForeignKeyInfo> foreign_key_infos;
    getTableForeignKeys(table_id, foreign_key_infos);

    // record insert val primary keys
    std::vector<std::vector<int>> insert_val_primary_keys;

    // check data_items
    for (auto& data_item : data_items) {
        if (data_item.values.size() != column_types.size()) {
            std::cout << "!ERROR" << std::endl;
            std::cout << "Data item size does not match column size"
                      << std::endl;
            return false;
        }
        data_item.columnIds.clear();
        for (int i = 0; i < data_item.values.size(); i++) {
            if (data_item.values[i].isNull) {
                if (column_types[i].isNotNull) {
                    std::cout << "!ERROR" << std::endl;
                    std::cout << "null" << std::endl;
                    return false;
                }
                data_item.values[i].dataType = column_types[i].dataType;
            }
            if (data_item.values[i].dataType !=
                column_types[i].dataType) {
                std::cout << "!ERROR" << std::endl;
                std::cout << "Data item type does not match column type"
                          << std::endl;
                return false;
            }
            data_item.columnIds.push_back(column_types[i].columnId);
        }

        // upd 以下检查主键是否唯一
        if (primary_keys.size() > 0) {
            // upd 设置查找主键的constraint
            std::vector<SearchConstraint> primary_constraints;
            std::vector<int> insert_val_primary_key;
            for (auto& primary_key : primary_keys) {
                SearchConstraint constraint;
                constraint.columnId = primary_key;
                constraint.constraintTypes.push_back(ConstraintType::EQ);
                constraint.dataType = record::DataTypeIdentifier::INT;
                for (int i = 0; i < data_item.values.size(); i++) {
                    if (data_item.columnIds[i] == primary_key) {
                        constraint.constraintValues.push_back(
                            data_item.values[i]);
                        insert_val_primary_key.push_back(
                            data_item.values[i].value.intValue);
                        if (data_item.values[i].isNull) {
                            std::cout << "!ERROR" << std::endl;
                            std::cout << "null" << std::endl;
                            return false;
                        }
                    }
                }
                primary_constraints.push_back(constraint);
            }

            // upd 检查是否重复（insert val之间的primary key)
            for (int i = 0; i < insert_val_primary_keys.size(); i++) {
                bool same = true;
                for (int j = 0; j < insert_val_primary_keys[i].size(); j++) {
                    if (insert_val_primary_keys[i][j] !=
                        insert_val_primary_key[j]) {
                        same = false;
                        break;
                    }
                }
                if (same) {
                    std::cout << "!ERROR" << std::endl;
                    std::cout << "duplicate primary keys" << std::endl;
                    return false;
                }
            }
            insert_val_primary_keys.push_back(insert_val_primary_key);

            // upd 检查是否重复
            std::vector<record::DataItem> result_datas;
            std::vector<record::ColumnType> result_column_types;
            std::vector<record::RecordLocation> record_locations;
            searchRowsInTable(table_id, primary_constraints, result_datas,
                   result_column_types, record_locations, -1);
            if (result_datas.size() > 0) {
                std::cout << "!ERROR" << std::endl;
                std::cout << "duplicate" << std::endl;
                return false;
            }
        }

        // upd 以下检查外键是否唯一
        for (auto& foreign_key_info : foreign_key_infos) {
            std::vector<record::DataValue> foreign_key_values;
            std::vector<SearchConstraint> constraints;
            for (int fk_i = 0;
                 fk_i < foreign_key_info.foreign_key_columnIds.size();
                 fk_i++) {
                auto& foreign_key_columnId =
                    foreign_key_info.foreign_key_columnIds[fk_i];
                auto& reference_columnId =
                    foreign_key_info.reference_columnIds[fk_i];

                SearchConstraint constraint;
                constraint.columnId = reference_columnId;
                constraint.constraintTypes.push_back(ConstraintType::EQ);
                constraint.dataType = record::DataTypeIdentifier::INT;
                bool isNull = false;
                for (int i = 0; i < data_item.values.size(); i++) {
                    if (data_item.columnIds[i] == foreign_key_columnId) {
                        if (data_item.values[i].isNull) {
                            isNull = true;
                            break;
                        }
                        foreign_key_values.push_back(data_item.values[i]);
                        constraint.constraintValues.push_back(
                            data_item.values[i]);
                        break;
                    }
                }
                if (!isNull) constraints.push_back(constraint);
            }

            std::vector<record::DataItem> result_datas;
            std::vector<record::ColumnType> result_column_types;
            std::vector<record::RecordLocation> record_locations;
            searchRowsInTable(foreign_key_info.reference_table_id, constraints,
                   result_datas, result_column_types, record_locations, -1);
            if (result_datas.size() == 0) {
                std::cout << "!ERROR" << std::endl;
                std::cout << "foreign key does not exist" << std::endl;
                return false;
            }
        }
    }

    // TODO unique 检查未实现
    std::vector<SearchConstraint> constraints;
    std::vector<record::DataItem> result_datas;
    std::vector<record::RecordLocation> record_locations;

    std::vector<std::pair<int, std::vector<int>>> all_index;
    std::vector<std::string> index_names;
    getAllIndex(currentDatabaseId, table_id, all_index, index_names);

    rm->getColumnTypes(record_path, column_types);

    for (auto& data_item : data_items) {
        // check unique
        for (auto& column : column_types) {
            if (!column.isUnique) continue;
            std::vector<SearchConstraint> constraints;
            for (int i = 0; i < data_item.columnIds.size(); i++) {
                if (data_item.columnIds[i] == column.columnId) {
                    SearchConstraint constraint;
                    constraint.columnId = column.columnId;
                    constraint.constraintTypes.push_back(ConstraintType::EQ);
                    constraint.dataType = column.dataType;
                    constraint.constraintValues.push_back(
                        data_item.values[i]);
                    constraints.push_back(constraint);
                    break;
                }
            }
            searchRowsInTable(table_id, constraints, result_datas, column_types,
                   record_locations, -1);
            if (result_datas.size() > 0) {
                std::cout << "!ERROR" << std::endl;
                std::cout << "duplicate" << std::endl;
                return false;
            }
        }

        auto location = rm->insertRecord(record_path, data_item);

        for (auto& index : all_index) {
            char* index_file_path = nullptr;
            getIndexRecordPath(currentDatabaseId, table_id, index.first,
                               &index_file_path);
            index::IndexValue index_value(location.pageId, location.slotId,
                                          0);
            for (auto& columnId : index.second) {
                for (int i = 0; i < data_item.columnIds.size(); i++) {
                    if (data_item.columnIds[i] == columnId) {
                        if (data_item.values[i].isNull) {
                            index_value.key.push_back(INT_MIN);
                        } else {
                            index_value.key.push_back(
                                data_item.values[i].value.intValue);
                        }
                        break;
                    }
                }
            }
            assert(im->insertIndex(index_file_path, index_value));
            delete[] index_file_path;
        }
    }
    std::cout << "rows" << std::endl;
    std::cout << data_items.size() << std::endl;
    // delete path
    delete[] table_path;
    delete[] record_path;

    return true;
}

void SystemManager::fillInDataTypeField(
    std::vector<std::tuple<std::string, record::DataValue>>& update_info,
    int table_id, std::vector<std::string>& column_names,
    record::DataItem& data_item) {
    char* table_path = nullptr;
    getTableRecordPath(currentDatabaseId, table_id, &table_path);
    char* record_path = nullptr;
    utils::joinPaths(table_path, RECORD_FILE_NAME, &record_path);

    std::vector<record::ColumnType> column_types;
    rm->getColumnTypes(record_path, column_types);

    for (auto& info : update_info) {
        std::string& table_name = std::get<0>(info);
        record::DataValue& data_value = std::get<1>(info);

        if (data_value.dataType == record::DataTypeIdentifier::ANY) {
            for (auto& column_type : column_types) {
                if (column_type.columnName== table_name) {
                    data_value.dataType = column_type.dataType;
                    break;
                }
            }
        }
        assert(data_value.dataType != record::DataTypeIdentifier::ANY);

        column_names.push_back(std::get<0>(info));
        data_item.values.push_back(std::get<1>(info));
    }
}

void SystemManager::fillInDataTypeField(
    std::vector<SearchConstraint>& constraints, int table_id) {
    // find DataType of constraint columns
    // get table path
    char* table_path = nullptr;
    getTableRecordPath(currentDatabaseId, table_id, &table_path);
    char* record_path = nullptr;
    utils::joinPaths(table_path, RECORD_FILE_NAME, &record_path);

    std::vector<record::ColumnType> column_types;
    rm->getColumnTypes(record_path, column_types);

    for (auto& constraint : constraints) {
        if (constraint.dataType == record::DataTypeIdentifier::ANY) {
            for (auto& column_type : column_types) {
                if (column_type.columnId == constraint.columnId) {
                    constraint.dataType = column_type.dataType;
                    break;
                }
            }
        }
        assert(constraint.dataType != record::DataTypeIdentifier::ANY);
    }
}

std::string SystemManager::findTableNameOfColumnName(
    const std::string& column_name, std::vector<std::string>& table_names) {
    std::string selected_table_name = "";
    for (auto& table_name : table_names) {
        int table_id = getTableId(table_name.c_str());
        if (table_id == -1) {
            std::cout << "!ERROR" << std::endl;
            std::cout << "Table " << table_name << " does not exist"
                      << std::endl;
            return "";
        }

        std::vector<record::ColumnType> column_types;
        getTableColumnTypes(table_id, column_types);
        for (int i = 0; i < column_types.size(); i++) {
            if (column_types[i].columnName == column_name) {
                if (selected_table_name != "") {
                    std::cout << "!ERROR" << std::endl;
                    std::cout << "Ambiguous column name" << std::endl;
                    return "";
                }
                selected_table_name = table_name;
            }
        }
    }
    return selected_table_name;
}

bool SystemManager::getColumnID(int table_id,
                                const std::vector<std::string>& column_names,
                                std::vector<int>& columnIds) {
    // get table path
    char* table_path = nullptr;
    getTableRecordPath(currentDatabaseId, table_id, &table_path);
    char* record_path = nullptr;
    utils::joinPaths(table_path, RECORD_FILE_NAME, &record_path);

    // get column types
    std::vector<record::ColumnType> column_types;
    rm->getColumnTypes(record_path, column_types);

    // get column id
    for (auto& column_name : column_names) {
        bool find = false;
        for (auto& column_type : column_types) {
            if (column_type.columnName== column_name) {
                columnIds.push_back(column_type.columnId);
                find = true;
                break;
            }
        }
        if (!find) {
            std::cout << "!ERROR" << std::endl;
            std::cout << "Column " << column_name << " does not exist"
                      << std::endl;
            return false;
        }
    }

    delete[] table_path;
    delete[] record_path;
    return true;
}

bool SystemManager::updateRows(int table_id,
                           std::vector<SearchConstraint>& constraints,
                           const record::DataItem& update_data) {
    // searchRowsInTable those satisfy constraints
    std::vector<record::DataItem> result_datas;
    std::vector<record::ColumnType> column_types;
    std::vector<record::RecordLocation> record_location_results;
    if (!searchRowsInTable(table_id, constraints, result_datas, column_types,
                record_location_results, -1)) {
        return false;
    }

    // get primary key id
    std::set<int> primary_keys;
    getTablePrimaryKeys(table_id, primary_keys);

    // get foriegn key id
    std::vector<ForeignKeyInfo> foreign_key_infos;
    getTableForeignKeys(table_id, foreign_key_infos);

    // get dominate table id
    std::vector<int> dominate_table_ids;
    getTableDominate(table_id, dominate_table_ids);

    std::vector<bool> change_foreign_key(foreign_key_infos.size(), false);
    for (int i = 0; i < foreign_key_infos.size(); i++) {
        auto& foreign_key_info = foreign_key_infos[i];
        for (auto& foreign_key_columnId :
             foreign_key_info.foreign_key_columnIds) {
            bool found = false;
            for (auto& data_columnId : update_data.columnIds) {
                if (foreign_key_columnId == data_columnId) {
                    change_foreign_key[i] = true;
                    found = true;
                    break;
                }
            }
            if (found) break;
        }
    }

    // get all index
    std::vector<std::pair<int, std::vector<int>>> all_index;
    std::vector<std::string> index_names;
    getAllIndex(currentDatabaseId, table_id, all_index, index_names);

    std::vector<bool> change_index;
    for (int i = 0; i < all_index.size(); i++) {
        change_index.push_back(false);
        for (auto& index_id : all_index[i].second) {
            for (auto& data_columnId : update_data.columnIds) {
                if (index_id == data_columnId) {
                    change_index[i] = true;
                    break;
                }
            }
        }
    }

    // get record path
    char* table_path = nullptr;
    getTableRecordPath(currentDatabaseId, table_id, &table_path);
    char* record_path = nullptr;
    utils::joinPaths(table_path, RECORD_FILE_NAME, &record_path);

    for (int i = 0; i < result_datas.size(); i++) {
        auto& result_data = result_datas[i];
        auto& record_location_result = record_location_results[i];

        bool change_primary_key = false;
        dbs::record::DataItem new_data = result_data;
        for (int j = 0; j < update_data.columnIds.size(); j++) {
            bool find = false;
            for (int k = 0; k < new_data.columnIds.size(); k++) {
                if (update_data.columnIds[j] == new_data.columnIds[k]) {
                    if (primary_keys.find(update_data.columnIds[j]) !=
                            primary_keys.end() &&
                        !(new_data.values[k] ==
                          update_data.values[j])) {
                        change_primary_key = true;
                    }
                    new_data.values[k] = update_data.values[j];
                    find = true;
                    break;
                }
            }
            if (!find) {
                std::cout << "!ERROR" << std::endl;
                std::cout << "Update column does not exist" << std::endl;
                delete[] table_path;
                delete[] record_path;
                return false;
            }
        }

        // upd 检查new data的unique
        for (auto& column : column_types) {
            if (column.isUnique == false) continue;
            std::vector<SearchConstraint> constraints;
            bool equal_old = false;
            for (int ii = 0; ii < new_data.values.size(); ii++) {
                if (new_data.columnIds[ii] == column.columnId) {
                    SearchConstraint constraint;
                    constraint.columnId = column.columnId;
                    constraint.constraintTypes.push_back(ConstraintType::EQ);
                    constraint.dataType = new_data.values[ii].dataType;
                    constraint.constraintValues.push_back(
                        new_data.values[ii]);
                    constraints.push_back(constraint);
                    if (new_data.values[ii] ==
                        result_data.values[ii]) {
                        equal_old = true;
                    }
                    break;
                }
            }
            if (equal_old) continue;
            std::vector<record::DataItem> result_datas;
            std::vector<record::ColumnType> result_column_types;
            std::vector<record::RecordLocation> record_locations;
            searchRowsInTable(table_id, constraints, result_datas, result_column_types,
                   record_locations, -1);
            if (result_datas.size() > 0) {
                std::cout << "!ERROR" << std::endl;
                std::cout << "duplicate" << std::endl;
                delete[] table_path;
                delete[] record_path;
                return false;
            }
        }

        // upd 检查修改涉及到了主键 & 和该主键被其他表的外键引用的情况
        if (change_primary_key) {
            // 主键
            std::vector<SearchConstraint> primary_constraints;
            for (auto& primary_key : primary_keys) {
                SearchConstraint constraint;
                constraint.columnId = primary_key;
                constraint.constraintTypes.push_back(ConstraintType::EQ);
                constraint.dataType = record::DataTypeIdentifier::INT;
                for (int ii = 0; ii < new_data.values.size(); ii++) {
                    if (new_data.columnIds[ii] == primary_key) {
                        constraint.constraintValues.push_back(
                            new_data.values[ii]);
                        if (new_data.values[ii].isNull) {
                            std::cout << "!ERROR" << std::endl;
                            std::cout << "null" << std::endl;
                            delete[] table_path;
                            delete[] record_path;
                            return false;
                        }
                    }
                }
                primary_constraints.push_back(constraint);
            }
            std::vector<record::DataItem> result_datas;
            std::vector<record::ColumnType> result_column_types;
            std::vector<record::RecordLocation> record_locations;
            searchRowsInTable(table_id, primary_constraints, result_datas,
                   result_column_types, record_locations, -1);
            if (result_datas.size() > 0 &&
                !(result_datas.size() == 1 &&
                  result_datas[0].dataId == result_data.dataId)) {
                std::cout << "!ERROR" << std::endl;
                std::cout << "Primary key already exists" << std::endl;
                delete[] table_path;
                delete[] record_path;
                return false;
            }

            // 被外键
            for (auto& dominate_table_id : dominate_table_ids) {
                // get dominate table foreign key
                std::vector<ForeignKeyInfo> dominate_foreign_key_infos;
                getTableForeignKeys(dominate_table_id,
                                    dominate_foreign_key_infos);

                for (auto& dominate_foreign_key_info :
                     dominate_foreign_key_infos) {
                    if (dominate_foreign_key_info.reference_table_id ==
                        table_id) {
                        std::vector<SearchConstraint> dominate_constraints;
                        for (int dominate_i = 0;
                             dominate_i < dominate_foreign_key_info
                                              .foreign_key_columnIds.size();
                             dominate_i++) {
                            auto& dominate_foreign_key_columnId =
                                dominate_foreign_key_info
                                    .foreign_key_columnIds[dominate_i];
                            auto& dominate_reference_columnId =
                                dominate_foreign_key_info
                                    .reference_columnIds[dominate_i];

                            SearchConstraint constraint;
                            constraint.columnId =
                                dominate_foreign_key_columnId;
                            constraint.constraintTypes.push_back(
                                ConstraintType::EQ);
                            constraint.dataType = record::DataTypeIdentifier::INT;
                            for (int ii = 0;
                                 ii < result_data.values.size(); ii++) {
                                if (result_data.columnIds[ii] ==
                                    dominate_reference_columnId) {
                                    constraint.constraintValues.push_back(
                                        result_data.values[ii]);
                                    break;
                                }
                            }
                            dominate_constraints.push_back(constraint);
                        }
                        std::vector<record::DataItem> result_datas;
                        std::vector<record::ColumnType> result_column_types;
                        std::vector<record::RecordLocation> record_locations;
                        searchRowsInTable(dominate_table_id, dominate_constraints,
                               result_datas, result_column_types,
                               record_locations, -1);
                        if (result_datas.size() > 0) {
                            std::cout << "!ERROR" << std::endl;
                            std::cout << "break foreign key rules" << std::endl;
                            delete[] table_path;
                            delete[] record_path;
                            return false;
                        }
                    }
                }
            }
        }

        // upd 检查null 约束
        for (auto& column : column_types) {
            if (column.isNotNull) {
                for (int ii = 0; ii < new_data.values.size(); ii++) {
                    if (new_data.columnIds[ii] == column.columnId) {
                        if (new_data.values[ii].isNull) {
                            std::cout << "!ERROR" << std::endl;
                            std::cout << "null" << std::endl;
                            delete[] table_path;
                            delete[] record_path;
                            return false;
                        }
                        break;
                    }
                }
            }
        }

        // upd 检查外键
        for (int ii = 0; ii < foreign_key_infos.size(); ii++) {
            if (change_foreign_key[ii]) {
                auto& foreign_key_info = foreign_key_infos[ii];
                std::vector<SearchConstraint> foreign_key_constraints;
                for (int foreign_key_i = 0;
                     foreign_key_i <
                     foreign_key_info.foreign_key_columnIds.size();
                     foreign_key_i++) {
                    auto& foreign_key_columnId =
                        foreign_key_info.foreign_key_columnIds[foreign_key_i];
                    auto& reference_columnId =
                        foreign_key_info.reference_columnIds[foreign_key_i];

                    SearchConstraint constraint;
                    constraint.columnId = reference_columnId;
                    constraint.constraintTypes.push_back(ConstraintType::EQ);
                    constraint.dataType = record::DataTypeIdentifier::INT;
                    bool foreign_key_isNull = false;
                    for (int ii = 0; ii < new_data.values.size(); ii++) {
                        if (new_data.columnIds[ii] == foreign_key_columnId) {
                            if (new_data.values[ii].isNull) {
                                foreign_key_isNull = true;
                            }
                            constraint.constraintValues.push_back(
                                new_data.values[ii]);
                            break;
                        }
                    }
                    if (!foreign_key_isNull)
                        foreign_key_constraints.push_back(constraint);
                }
                std::vector<record::DataItem> result_datas;
                std::vector<record::ColumnType> result_column_types;
                std::vector<record::RecordLocation> record_locations;
                searchRowsInTable(foreign_key_info.reference_table_id,
                       foreign_key_constraints, result_datas,
                       result_column_types, record_locations, -1);
                if (result_datas.size() == 0) {
                    std::cout << "!ERROR" << std::endl;
                    std::cout << "foreign key does not exist" << std::endl;
                    delete[] table_path;
                    delete[] record_path;
                    return false;
                }
            }
        }

        // updateRows record
        if (!rm->updateRecord(record_path, record_location_result, new_data)) {
            std::cout << "!ERROR" << std::endl;
            std::cout << "Update record failed" << std::endl;
            delete[] table_path;
            delete[] record_path;
            return false;
        }

        // delete index affected, insert again
        for (int i = 0; i < all_index.size(); i++) {
            if (!change_index[i]) continue;
            char* index_file_path = nullptr;
            getIndexRecordPath(currentDatabaseId, table_id,
                               all_index[i].first, &index_file_path);

            index::IndexValue old_index_value(record_location_result.pageId,
                                              record_location_result.slotId,
                                              0);
            for (auto& columnId : all_index[i].second) {
                for (int i = 0; i < result_data.columnIds.size(); i++) {
                    if (result_data.columnIds[i] == columnId) {
                        if (result_data.values[i].isNull) {
                            old_index_value.key.push_back(INT_MIN);
                        } else {
                            old_index_value.key.push_back(
                                result_data.values[i].value.intValue);
                        }
                        break;
                    }
                }
            }
            if (!im->deleteIndex(index_file_path, old_index_value, true)) {
                std::cout << "!ERROR" << std::endl;
                std::cout << "Delete index failed" << std::endl;
                delete[] table_path;
                delete[] record_path;
                delete[] index_file_path;
                return false;
            }

            index::IndexValue new_index_value(record_location_result.pageId,
                                              record_location_result.slotId,
                                              0);
            for (auto& columnId : all_index[i].second) {
                for (int i = 0; i < new_data.columnIds.size(); i++) {
                    if (new_data.columnIds[i] == columnId) {
                        if (new_data.values[i].isNull) {
                            new_index_value.key.push_back(INT_MIN);
                        } else {
                            new_index_value.key.push_back(
                                new_data.values[i].value.intValue);
                        }
                        break;
                    }
                }
            }
            if (!im->insertIndex(index_file_path, new_index_value)) {
                std::cout << "!ERROR" << std::endl;
                std::cout << "Insert index failed" << std::endl;
                delete[] table_path;
                delete[] record_path;
                delete[] index_file_path;
                return false;
            }
            delete[] index_file_path;
        }
    }
    delete[] table_path;
    delete[] record_path;
    std::cout << "rows" << std::endl;
    std::cout << result_datas.size() << std::endl;
    return true;
}

bool SystemManager::deleteRowsFromTable(
    int table_id, std::vector<SearchConstraint>& constraints) {
    // searchRowsInTable those satisfy constraints
    std::vector<record::DataItem> result_datas;
    std::vector<record::ColumnType> column_types;
    std::vector<record::RecordLocation> record_location_results;
    if (!searchRowsInTable(table_id, constraints, result_datas, column_types,
                record_location_results, -1)) {
        return false;
    }

    // get primary key id
    std::set<int> primary_keys;
    getTablePrimaryKeys(table_id, primary_keys);

    // get dominate table id
    std::vector<int> dominate_table_ids;
    getTableDominate(table_id, dominate_table_ids);

    // get all index
    std::vector<std::pair<int, std::vector<int>>> all_index;
    std::vector<std::string> index_names;
    getAllIndex(currentDatabaseId, table_id, all_index, index_names);

    // get record path
    char* table_path = nullptr;
    getTableRecordPath(currentDatabaseId, table_id, &table_path);
    char* record_path = nullptr;
    utils::joinPaths(table_path, RECORD_FILE_NAME, &record_path);

    for (int i = 0; i < result_datas.size(); i++) {
        auto& result_data = result_datas[i];
        auto& record_location_result = record_location_results[i];

        // check 被外键
        for (auto& dominate_table_id : dominate_table_ids) {
            // get dominate table foreign key
            std::vector<ForeignKeyInfo> dominate_foreign_key_infos;
            getTableForeignKeys(dominate_table_id, dominate_foreign_key_infos);

            for (auto& dominate_foreign_key_info : dominate_foreign_key_infos) {
                if (dominate_foreign_key_info.reference_table_id == table_id) {
                    std::vector<SearchConstraint> dominate_constraints;
                    for (int dominate_i = 0;
                         dominate_i < dominate_foreign_key_info
                                          .foreign_key_columnIds.size();
                         dominate_i++) {
                        auto& dominate_foreign_key_columnId =
                            dominate_foreign_key_info
                                .foreign_key_columnIds[dominate_i];
                        auto& dominate_reference_columnId =
                            dominate_foreign_key_info
                                .reference_columnIds[dominate_i];

                        SearchConstraint constraint;
                        constraint.columnId = dominate_foreign_key_columnId;
                        constraint.dataType = record::DataTypeIdentifier::INT;
                        constraint.constraintTypes.push_back(
                            ConstraintType::EQ);
                        for (int ii = 0; ii < result_data.values.size();
                             ii++) {
                            if (result_data.columnIds[ii] ==
                                dominate_reference_columnId) {
                                constraint.constraintValues.push_back(
                                    result_data.values[ii]);
                                break;
                            }
                        }
                        dominate_constraints.push_back(constraint);
                    }
                    std::vector<record::DataItem> result_datas;
                    std::vector<record::ColumnType> result_column_types;
                    std::vector<record::RecordLocation> record_locations;
                    searchRowsInTable(dominate_table_id, dominate_constraints,
                           result_datas, result_column_types, record_locations,
                           -1);
                    if (result_datas.size() > 0) {
                        std::cout << "!ERROR" << std::endl;
                        std::cout << "break foreign key rules" << std::endl;
                        delete[] table_path;
                        delete[] record_path;
                        return false;
                    }
                }
            }
        }

        // delete record
        if (!rm->deleteRecord(record_path, record_location_result)) {
            std::cout << "!ERROR" << std::endl;
            std::cout << "Delete record failed" << std::endl;
            delete[] table_path;
            delete[] record_path;
            return false;
        }

        // delete all index
        for (int i = 0; i < all_index.size(); i++) {
            char* index_file_path = nullptr;
            getIndexRecordPath(currentDatabaseId, table_id,
                               all_index[i].first, &index_file_path);

            index::IndexValue old_index_value(record_location_result.pageId,
                                              record_location_result.slotId,
                                              0);
            for (auto& columnId : all_index[i].second) {
                for (int i = 0; i < result_data.columnIds.size(); i++) {
                    if (result_data.columnIds[i] == columnId) {
                        if (result_data.values[i].isNull) {
                            old_index_value.key.push_back(INT_MIN);
                        } else {
                            old_index_value.key.push_back(
                                result_data.values[i].value.intValue);
                        }
                        break;
                    }
                }
            }
            if (!im->deleteIndex(index_file_path, old_index_value, true)) {
                std::cout << "!ERROR" << std::endl;
                std::cout << "Delete index failed" << std::endl;
                delete[] table_path;
                delete[] record_path;
                delete[] index_file_path;
                return false;
            }
            delete[] index_file_path;
        }
    }

    delete[] table_path;
    delete[] record_path;
    std::cout << "rows" << std::endl;
    std::cout << result_datas.size() << std::endl;
    return true;
}

bool SystemManager::dropPrimaryKey(int table_id) {
    // get table path
    char* table_path = nullptr;
    getTableRecordPath(currentDatabaseId, table_id, &table_path);

    // get table primary key path
    char* primary_key_path = nullptr;
    utils::joinPaths(table_path, PRIMARY_KEY_FILE_NAME, &primary_key_path);

    // check if primary key exists in primary key file
    std::vector<record::RecordLocation> locations;
    std::vector<record::DataItem> primary_keys;
    rm->getAllRecords(primary_key_path, primary_keys, locations);

    if (primary_keys.size() == 0) {
        std::cout << "!ERROR" << std::endl;
        std::cout << "Table " << table_id << " does not have primary key"
                  << std::endl;
        return false;
    }

    for (auto& location : locations) {
        assert(rm->deleteRecord(primary_key_path, location));
    }

    return true;
}

bool SystemManager::addPrimaryKey(int table_id,
                                  std::vector<int> primary_key_ids) {
    // get table path
    char* table_path = nullptr;
    getTableRecordPath(currentDatabaseId, table_id, &table_path);

    // get table primary key path
    char* primary_key_path = nullptr;
    utils::joinPaths(table_path, PRIMARY_KEY_FILE_NAME, &primary_key_path);

    // check if primary key exists in primary key file
    std::vector<record::DataItem> data_items;
    std::vector<record::RecordLocation> record_locations;
    rm->getAllRecords(primary_key_path, data_items, record_locations);
    if (data_items.size() != 0) {
        std::cout << "!ERROR" << std::endl;
        std::cout << "Can only have one primary key" << std::endl;
        return false;
    }

    // get data items of private key
    std::vector<record::DataItem> table_data_items;
    char* record_path = nullptr;
    utils::joinPaths(table_path, RECORD_FILE_NAME, &record_path);
    rm->getAllRecords(record_path, table_data_items, record_locations);

    std::vector<record::DataItem> primary_key_values;
    for (auto& table_data_item : table_data_items) {
        record::DataItem primary_key_value;
        for (int i = 0; i < table_data_item.values.size(); i++) {
            for (auto& primary_key_id : primary_key_ids) {
                if (table_data_item.columnIds[i] == primary_key_id) {
                    primary_key_value.values.push_back(
                        table_data_item.values[i]);
                    primary_key_value.columnIds.push_back(
                        table_data_item.columnIds[i]);
                    break;
                }
            }
        }
        primary_key_values.push_back(primary_key_value);
    }

    // check if primary key values are unique
    std::set<record::DataItem> primary_key_values_set(
        primary_key_values.begin(), primary_key_values.end());
    if (primary_key_values_set.size() != primary_key_values.size()) {
        std::cout << "!ERROR" << std::endl;
        std::cout << "duplicated value" << std::endl;
        return false;
    }

    // insert primary key
    std::vector<record::ColumnType> primary_key_table_column_types;
    rm->getColumnTypes(primary_key_path, primary_key_table_column_types);

    for (auto& primary_key_id : primary_key_ids) {
        record::DataItem data_item;
        data_item.values.push_back(record::DataValue(
            record::DataTypeIdentifier::INT, false, primary_key_id));
        data_item.columnIds.push_back(
            primary_key_table_column_types[0].columnId);
        record::RecordLocation location =
            rm->insertRecord(primary_key_path, data_item);
        if (location.pageId == -1) {
            throw std::runtime_error("insert primary key failed");
        }
    }

    delete[] table_path;
    delete[] primary_key_path;

    return true;
}

bool SystemManager::addUnique(const char* table_name,
                              const std::string& unique_name,
                              const std::vector<int>& columnIds) {
    if (currentDatabaseId == -1) {
        std::cout << "!ERROR" << std::endl;
        std::cout << "No database selected" << std::endl;
        return false;
    }

    int table_id = getTableId(table_name);
    if (table_id == -1) {
        std::cout << "!ERROR" << std::endl;
        std::cout << "Table " << table_name << " does not exist" << std::endl;
        return false;
    }
    std::string index_name = unique_name + UNIQUE_SUFFIX;
    if (!addIndex(table_name, index_name, columnIds, true)) return false;

    // check if current item is unique
    char* table_path = nullptr;
    getTableRecordPath(currentDatabaseId, table_id, &table_path);
    char* record_path = nullptr;
    utils::joinPaths(table_path, RECORD_FILE_NAME, &record_path);

    if (columnIds.size() != 1) {
        std::cout << "!ERROR" << std::endl;
        std::cout << "Can only have one unique column" << std::endl;
        return false;
    }

    rm->updateColumnUnique(record_path, columnIds[0], true);

    std::vector<record::ColumnType> column_types;
    rm->getColumnTypes(record_path, column_types);

    assert(column_types[columnIds[0]].isUnique);

    return true;
}

bool SystemManager::searchAndSave(int tableId,
                                  std::vector<record::ColumnType>& columnTypes,
                                  std::vector<SearchConstraint>& constraints,
                                  std::string& savePath, int& totalNum) {
    columnTypes.clear();

    if (currentDatabaseId == -1) {
        std::cout << "!ERROR" << std::endl;
        std::cout << "No database selected" << std::endl;
        return false;
    }

    std::string filePath = DATABASE_PATH;
    filePath.push_back('/');
    filePath += TMP_FILE_PREFIX;
    filePath += std::to_string(tmpFileIdx++);

    savePath = DATABASE_PATH;
    savePath.push_back('/');
    savePath += TMP_FILE_PREFIX;
    savePath += std::to_string(tmpFileIdx++);

    // Delete existing temporary file if exists and create a new one
    if (fm->doesFileExist(filePath.c_str())) {
        fm->deleteFile(filePath.c_str());
    }
    fm->createFile(filePath.c_str());

    // Get column types from the table
    getTableColumnTypes(tableId, columnTypes);

    bool hasItems = mergeConstraints(constraints);

    // Initialize record file for saving
    rm->initializeRecordFile(savePath.c_str(), columnTypes);

    // Verify constraint column types
    std::vector<int> intConstraintsWithRange;
    for (auto& constraint : constraints) {
        bool found = false;
        for (auto& columnType : columnTypes) {
            if (constraint.columnId == columnType.columnId) {
                if (constraint.dataType != columnType.dataType) {
                    std::cout << "!ERROR" << std::endl;
                    std::cout << "Constraint data type does not match column "
                                 "type" << std::endl;
                    std::cout << "Constraint data type: " << int(constraint.dataType)
                              << " column type: " << int(columnType.dataType)
                              << std::endl;
                    return false;
                }
                found = true;
                break;
            }
        }
        if (!found) {
            std::cout << "!ERROR" << std::endl;
            std::cout << "Constraint column id does not exist" << std::endl;
            return false;
        }
        if (constraint.dataType == record::DataTypeIdentifier::INT &&
            constraint.constraintTypes.size() > 0 &&
            constraint.constraintTypes[0] != ConstraintType::NEQ) {
            intConstraintsWithRange.push_back(constraint.columnId);
        }
    }

    if (!hasItems) return true;

    // Get all available indexes
    std::vector<std::pair<int, std::vector<int>>> allIndexes;
    std::vector<std::string> indexNames;
    getAllIndex(currentDatabaseId, tableId, allIndexes, indexNames);

    // Determine the most suitable index to use
    int chosenIndex = -1, overlapCount = 0;
    std::vector<int> indexValues;
    for (auto& index : allIndexes) {
        int indexId = index.first;
        auto& indexColumns = index.second;
        int overlap = 0;
        for (auto& indexColumnId : indexColumns) {
            bool found = false;
            for (auto& intConstraintWithRangeId : intConstraintsWithRange) {
                if (indexColumnId == intConstraintWithRangeId) {
                    overlap++;
                    found = true;
                    break;
                }
            }
            if (!found) break;
        }
        if (overlap > overlapCount) {
            overlapCount = overlap;
            chosenIndex = indexId;
            indexValues = indexColumns;
        }
    }

    // If no suitable index was found, fetch all records and save
    if (chosenIndex == -1) {
        char* tablePath = nullptr;
        getTableRecordPath(currentDatabaseId, tableId, &tablePath);
        char* recordPath = nullptr;
        utils::joinPaths(tablePath, RECORD_FILE_NAME, &recordPath);

        totalNum = rm->getAllRecordWithConstraintSaveFile(
            recordPath, filePath.c_str(), constraints);

        delete[] tablePath;
        delete[] recordPath;

        rm->insertRecordsToEmptyRecord(savePath.c_str(), filePath.c_str(), ",", false);
        return true;
    } else {
        index::IndexValue indexRangeLow, indexRangeHigh;
        for (int i = 0; i < overlapCount; i++) {
            int lowRange = INT_MIN, highRange = INT_MAX;
            for (auto& constraint : constraints) {
                if (constraint.columnId == indexValues[i]) {
                    for (int j = 0; j < constraint.constraintTypes.size(); j++) {
                        if (constraint.constraintTypes[j] == ConstraintType::LEQ) {
                            highRange = std::min(highRange, constraint.constraintValues[j].value.intValue);
                        } else if (constraint.constraintTypes[j] == ConstraintType::GEQ) {
                            lowRange = std::max(lowRange, constraint.constraintValues[j].value.intValue);
                        }
                    }
                }
            }
            indexRangeLow.key.push_back(lowRange);
            indexRangeHigh.key.push_back(highRange);
        }
        for (int i = overlapCount; i < indexValues.size(); i++) {
            indexRangeLow.key.push_back(INT_MIN);
            indexRangeHigh.key.push_back(INT_MAX);
        }

        // Get index file path and search the index
        char* indexFilePath = nullptr;
        getIndexRecordPath(currentDatabaseId, tableId, chosenIndex, &indexFilePath);

        std::vector<index::IndexValue> indexResults;
        im->searchIndexInRanges(indexFilePath, indexRangeLow, indexRangeHigh, indexResults);
        delete[] indexFilePath;

        // Get record file path
        char* tablePath = nullptr;
        getTableRecordPath(currentDatabaseId, tableId, &tablePath);
        char* recordPath = nullptr;
        utils::joinPaths(tablePath, RECORD_FILE_NAME, &recordPath);

        // Fetch the records matching the index range
        std::vector<record::RecordLocation> recordLocations;
        index::indexValuesToRecordLocations(indexResults, recordLocations);
        std::vector<record::DataItem> dataItems;
        rm->getRecords(recordPath, recordLocations, dataItems);

        delete[] tablePath;
        delete[] recordPath;

        // Write the filtered results to a file
        int count = 0;
        std::ofstream outputFile(filePath);
        if (!outputFile.is_open()) {
            std::cerr << "Error opening file!" << std::endl;
            return false;
        }
        for (auto& dataItem : dataItems) {
            bool isValid = true;
            for (auto& constraint : constraints) {
                if (!validConstraint(constraint, dataItem)) {
                    isValid = false;
                    break;
                }
            }
            if (isValid) {
                for (int i = 0; i < dataItem.values.size() - 1; i++) {
                    outputFile << dataItem.values[i].toString() << ",";
                }
                outputFile << dataItem.values.back().toString();
                outputFile << std::endl;
                count++;
            }
        }
        outputFile.close();
        totalNum = count;

        rm->insertRecordsToEmptyRecord(savePath.c_str(), filePath.c_str(), ",", false);
        return true;
    }
}

bool SystemManager::searchRowsInTable(
    int tableId, std::vector<SearchConstraint>& constraints,
    std::vector<record::DataItem>& resultDatas,
    std::vector<record::ColumnType>& columnTypes,
    std::vector<record::RecordLocation>& recordLocationResults, int sortBy) {
    if (currentDatabaseId == -1) {
        std::cout << "!ERROR" << std::endl;
        std::cout << "No database selected" << std::endl;
        return false;
    }

    resultDatas.clear();
    columnTypes.clear();
    recordLocationResults.clear();

    // Get column types for the table
    getTableColumnTypes(tableId, columnTypes);

    bool hasItems = mergeConstraints(constraints);

    // Verify constraint column types
    std::vector<int> intConstraintsWithRange;
    for (auto& constraint : constraints) {
        bool found = false;
        for (auto& columnType : columnTypes) {
            if (constraint.columnId == columnType.columnId) {
                if (constraint.dataType != columnType.dataType) {
                    std::cout << "!ERROR" << std::endl;
                    std::cout << "Constraint data type does not match column "
                                 "type" << std::endl;
                    std::cout << "Constraint data type: " << int(constraint.dataType)
                              << " column type: " << int(columnType.dataType)
                              << std::endl;
                    return false;
                }
                found = true;
                break;
            }
        }
        if (!found) {
            std::cout << "!ERROR" << std::endl;
            std::cout << "Constraint column id does not exist" << std::endl;
            return false;
        }
        if (constraint.dataType == record::DataTypeIdentifier::INT && \
            constraint.constraintTypes.size() > 0 && constraint.constraintTypes[0] != ConstraintType::NEQ) {
            intConstraintsWithRange.push_back(constraint.columnId); // Save the column id for range search
        }
    }

    if (!hasItems) return true;

    // Get all indexes for the table
    std::vector<std::pair<int, std::vector<int>>> allIndexes;
    std::vector<std::string> indexNames;
    getAllIndex(currentDatabaseId, tableId, allIndexes, indexNames);

    // Choose the best index to use
    int chosenIndex = -1, overlapCount = 0;
    std::vector<int> indexValues;
    for (auto& index : allIndexes) {
        int indexId = index.first;
        auto& indexColumns = index.second;
        int overlap = 0;
        for (auto& indexColumnId : indexColumns) {
            bool found = false;
            for (auto& intConstraintWithRangeId : intConstraintsWithRange) {
                if (indexColumnId == intConstraintWithRangeId) {
                    overlap++;
                    found = true;
                    break;
                }
            }
            if (!found) break;
        }
        if (overlap > overlapCount) {
            overlapCount = overlap;
            chosenIndex = indexId;
            indexValues = indexColumns;
        }
    }

    if (chosenIndex == -1) {
        // If no index is found, just scan the whole table
        char* tablePath = nullptr;
        getTableRecordPath(currentDatabaseId, tableId, &tablePath);
        char* recordPath = nullptr;
        utils::joinPaths(tablePath, RECORD_FILE_NAME, &recordPath);

        rm->getAllRecordWithConstraint(recordPath, resultDatas, recordLocationResults, constraints);

        delete[] tablePath;
        delete[] recordPath;
    } else {
        // Handle index-based search
        index::IndexValue indexRangeLow, indexRangeHigh;
        for (int i = 0; i < overlapCount; i++) {
            int lowRange = INT_MIN, highRange = INT_MAX;
            for (auto& constraint : constraints) {
                if (constraint.columnId == indexValues[i]) {
                    for (int j = 0; j < constraint.constraintTypes.size(); j++) {
                        if (constraint.constraintTypes[j] == ConstraintType::LEQ) {
                            highRange = std::min(highRange, constraint.constraintValues[j].value.intValue);
                        } else if (constraint.constraintTypes[j] == ConstraintType::GEQ) {
                            lowRange = std::max(lowRange, constraint.constraintValues[j].value.intValue);
                        }
                    }
                }
            }
            indexRangeLow.key.push_back(lowRange);
            indexRangeHigh.key.push_back(highRange);
        }
        for (int i = overlapCount; i < indexValues.size(); i++) {
            indexRangeLow.key.push_back(INT_MIN);
            indexRangeHigh.key.push_back(INT_MAX);
        }

        // Get the index file path
        char* indexFilePath = nullptr;
        getIndexRecordPath(currentDatabaseId, tableId, chosenIndex, &indexFilePath);

        std::vector<index::IndexValue> indexResults;
        im->searchIndexInRanges(indexFilePath, indexRangeLow, indexRangeHigh, indexResults);
        delete[] indexFilePath;

        // Get the record file path
        char* tablePath = nullptr;
        getTableRecordPath(currentDatabaseId, tableId, &tablePath);
        char* recordPath = nullptr;
        utils::joinPaths(tablePath, RECORD_FILE_NAME, &recordPath);

        // Fetch the records matching the index range
        std::vector<record::RecordLocation> recordLocations;
        index::indexValuesToRecordLocations(indexResults, recordLocations);
        rm->getRecords(recordPath, recordLocations, resultDatas);

        delete[] tablePath;
        delete[] recordPath;
    }

    return true;
}

          
bool SystemManager::dropTable(const char* table_name) {
    // check db id
    if (currentDatabaseId == -1) {
        std::cout << "!ERROR" << std::endl;
        std::cout << "No database selected" << std::endl;
        return false;
    }

    // get table id
    int table_id = getTableId(table_name);
    if (table_id == -1) {
        std::cout << "!ERROR" << std::endl;
        std::cout << "Table " << table_name << " does not exist" << std::endl;
        return false;
    }

    // get table path
    char* table_path = nullptr;
    getTableRecordPath(currentDatabaseId, table_id, &table_path);

    // get dominate record
    char* dominate_path = nullptr;
    utils::joinPaths(table_path, DOMINATE_FILE_NAME, &dominate_path);
    std::vector<record::DataItem> data_items;
    std::vector<record::RecordLocation> record_locations;
    rm->getAllRecords(dominate_path, data_items, record_locations);
    delete[] dominate_path;

    if (data_items.size() != 0) {
        std::cout << "!ERROR" << std::endl;
        std::cout << "Table " << table_name << " is referenced by other table"
                  << std::endl;
        delete[] table_path;
        return false;
    }

    std::vector<ForeignKeyInfo> foreign_key_infos;
    getTableForeignKeys(table_id, foreign_key_infos);

    // delete dominates for other tables
    for (auto& foreign_key_info : foreign_key_infos) {
        char* reference_table_path = nullptr;
        getTableRecordPath(currentDatabaseId,
                           foreign_key_info.reference_table_id,
                           &reference_table_path);
        char* reference_table_dominate_path = nullptr;
        utils::joinPaths(reference_table_path, DOMINATE_FILE_NAME,
                          &reference_table_dominate_path);
        // get all record in reference_table_dominate_path
        std::vector<record::DataItem> data_items;
        std::vector<record::RecordLocation> record_locations;
        rm->getAllRecords(reference_table_dominate_path, data_items,
                          record_locations);
        // delete record in reference_table_dominate_path
        for (int i = 0; i < data_items.size(); i++) {
            if (data_items[i].values[0].value.intValue == table_id) {
                rm->deleteRecord(reference_table_dominate_path,
                                 record_locations[i]);
            }
        }
        delete[] reference_table_path;
        delete[] reference_table_dominate_path;
    }

    im->closeAllCurrentFile();
    rm->closeAllCurrentFile();
    rm->cleanAllCurrentColumnTypes();

    // delete table folder
    assert(fm->deleteFolder(table_path));
    delete[] table_path;

    // delete table record
    char* db_base_path = nullptr;
    getDatabasePath(currentDatabaseId, &db_base_path);
    char* all_table_path = nullptr;
    utils::joinPaths(db_base_path, ALL_TABLE_FILE_NAME, &all_table_path);

    rm->getAllRecords(all_table_path, data_items, record_locations);
    for (int i = 0; i < data_items.size(); i++) {
        if (data_items[i].dataId == table_id) {
            rm->deleteRecord(all_table_path, record_locations[i]);
            break;
        }
    }

    delete[] db_base_path;
    delete[] all_table_path;

    return true;
}

bool SystemManager::getTableColumnTypes(
    int table_id, std::vector<record::ColumnType>& column_types) {
    char* table_path = nullptr;
    getTableRecordPath(currentDatabaseId, table_id, &table_path);
    char* record_path = nullptr;
    utils::joinPaths(table_path, RECORD_FILE_NAME, &record_path);
    rm->getColumnTypes(record_path, column_types);
    delete[] table_path;
    delete[] record_path;
    return true;
}

bool SystemManager::getTablePrimaryKeys(int table_id,
                                        std::set<int>& primary_keys) {
    char* table_path = nullptr;
    getTableRecordPath(currentDatabaseId, table_id, &table_path);
    char* primary_key_path = nullptr;
    utils::joinPaths(table_path, PRIMARY_KEY_FILE_NAME, &primary_key_path);
    std::vector<record::DataItem> data_items;
    std::vector<record::RecordLocation> record_locations;
    rm->getAllRecords(primary_key_path, data_items, record_locations);
    primary_keys.clear();
    for (auto& data_item : data_items) {
        primary_keys.insert(data_item.values[0].value.intValue);
    }
    delete[] table_path;
    delete[] primary_key_path;
    return true;
}

bool SystemManager::getTableDominate(int table_id,
                                     std::vector<int>& dominate_table_ids) {
    char* table_path = nullptr;
    getTableRecordPath(currentDatabaseId, table_id, &table_path);
    char* dominate_path = nullptr;
    utils::joinPaths(table_path, DOMINATE_FILE_NAME, &dominate_path);
    std::vector<record::DataItem> data_items;
    std::vector<record::RecordLocation> record_locations;
    rm->getAllRecords(dominate_path, data_items, record_locations);
    dominate_table_ids.clear();
    for (auto& data_item : data_items) {
        dominate_table_ids.push_back(data_item.values[0].value.intValue);
    }
    delete[] table_path;
    delete[] dominate_path;
    return true;
}


bool SystemManager::getTableForeignKeys(
    int tableId, std::vector<ForeignKeyInfo>& foreignKeys) {
    // Get the path for the table
    char* tablePath = nullptr;
    getTableRecordPath(currentDatabaseId, tableId, &tablePath);

    // Get the path for foreign keys related to this table
    char* foreignKeyPath = nullptr;
    utils::joinPaths(tablePath, FOREIGN_KEY_FILE_NAME, &foreignKeyPath);

    // Fetch all records from the foreign key path
    std::vector<record::DataItem> dataItems;
    std::vector<record::RecordLocation> recordLocations;
    rm->getAllRecords(foreignKeyPath, dataItems, recordLocations);

    foreignKeys.clear();
    for (auto& dataItem : dataItems) {
        std::vector<int> foreignKeyColumnIds;
        for (int i = 0; i < FOREIGN_KEY_MAX_NUM; i++) {
            if (!dataItem.values[i + 1].isNull) {
                foreignKeyColumnIds.push_back(
                    dataItem.values[i + 1].value.intValue);
            }
        }

        std::vector<int> referenceColumnIds;
        for (int i = 0; i < FOREIGN_KEY_MAX_NUM; i++) {
            if (!dataItem.values[i + 1 + FOREIGN_KEY_MAX_NUM].isNull) {
                referenceColumnIds.push_back(
                    dataItem.values[i + 1 + FOREIGN_KEY_MAX_NUM]
                        .value.intValue);
            }
        }

        foreignKeys.push_back(ForeignKeyInfo(
            foreignKeyColumnIds, dataItem.values[0].value.intValue,
            referenceColumnIds,
            dataItem.values[FOREIGN_KEY_MAX_NUM * 2 + 1]
                .value.charValue));
    }

    // Clean up dynamically allocated memory
    delete[] tablePath;
    delete[] foreignKeyPath;

    return true;
}

void SystemManager::getAllDatabase( std::vector<record::DataItem>& databaseNames,\
    std::vector<record::ColumnType>& columnTypes) {
    databaseNames.clear();
    columnTypes.clear();

    // Get all database records
    std::vector<record::RecordLocation> recordLocations;
    rm->getAllRecords(DATABASE_GLOBAL_RECORD_PATH, databaseNames, recordLocations);

    // Get the column types for the database records
    rm->getColumnTypes(DATABASE_GLOBAL_RECORD_PATH, columnTypes);
}


bool SystemManager::getAllTable(std::vector<record::DataItem>& table_names,
                                std::vector<record::ColumnType>& column_types) {
    table_names.clear();
    column_types.clear();

    // check db id
    if (currentDatabaseId == -1) {
        std::cout << "!ERROR" << std::endl;
        std::cout << "No database selected" << std::endl;
        return false;
    }

    // get all table path
    char* db_base_path = nullptr;
    getDatabasePath(currentDatabaseId, &db_base_path);
    char* all_table_path = nullptr;
    utils::joinPaths(db_base_path, ALL_TABLE_FILE_NAME, &all_table_path);

    delete[] db_base_path;

    // get all table record
    table_names.clear();
    std::vector<record::RecordLocation> record_locations;
    rm->getAllRecords(all_table_path, table_names, record_locations);
    rm->getColumnTypes(all_table_path, column_types);

    delete[] all_table_path;

    return true;
}

bool SystemManager::describeTable(
    const char* table_name, std::vector<record::DataItem>& table_info,
    std::vector<record::ColumnType>& column_types,
    std::vector<std::string>& primary_keys,
    std::vector<ForeignKeyInputInfo>& foreign_keys,
    std::vector<std::vector<std::string>>& index,
    std::vector<std::vector<std::string>>& unique) {
    table_info.clear();
    column_types.clear();

    // check db id
    if (currentDatabaseId == -1) {
        std::cout << "!ERROR" << std::endl;
        std::cout << "No database selected" << std::endl;
        return false;
    }

    // get table id
    int table_id = getTableId(table_name);
    if (table_id == -1) {
        std::cout << "!ERROR" << std::endl;
        std::cout << "Table " << table_name << " does not exist" << std::endl;
        return false;
    }

    // get table path
    char* table_path = nullptr;
    getTableRecordPath(currentDatabaseId, table_id, &table_path);

    // get record path
    char* record_path = nullptr;
    utils::joinPaths(table_path, RECORD_FILE_NAME, &record_path);

    // get record
    std::vector<record::ColumnType> all_column_types;
    rm->getColumnTypes(record_path, all_column_types);

    delete[] record_path;
    delete[] table_path;

    // set column_types
    column_types.push_back(record::ColumnType(record::DataTypeIdentifier::VARCHAR,
                                              255, 0, true, true,
                                              record::DefaultValue(), "Field"));
    column_types.push_back(record::ColumnType(record::DataTypeIdentifier::VARCHAR,
                                              255, 0, true, false,
                                              record::DefaultValue(), "Type"));
    column_types.push_back(record::ColumnType(record::DataTypeIdentifier::VARCHAR,
                                              255, 0, true, false,
                                              record::DefaultValue(), "Null"));
    column_types.push_back(
        record::ColumnType(record::DataTypeIdentifier::VARCHAR, 255, 0, false, false,
                           record::DefaultValue(), "Default"));
    for (int i = 0; i < 4; i++) column_types[i].columnId = i;

    // set column_values
    for (auto& all_column_type : all_column_types) {
        record::DataItem data_item;
        data_item.values.push_back(record::DataValue(
            record::DataTypeIdentifier::VARCHAR, false, all_column_type.columnName));
        data_item.columnIds.push_back(column_types[0].columnId);
        data_item.values.push_back(
            record::DataValue(record::DataTypeIdentifier::VARCHAR, false,
                              all_column_type.typeAsString()));
        data_item.columnIds.push_back(column_types[1].columnId);
        data_item.values.push_back(
            record::DataValue(record::DataTypeIdentifier::VARCHAR, false,
                              all_column_type.isNotNull ? "NO" : "YES"));
        data_item.columnIds.push_back(column_types[2].columnId);
        data_item.values.push_back(record::DataValue(
            record::DataTypeIdentifier::VARCHAR, false,
            all_column_type.defaultValue.hasDefaultValue ? all_column_type.defaultValue.value.toString() : "NULL"));
        data_item.columnIds.push_back(column_types[3].columnId);
        table_info.push_back(data_item);
    }

    // set primary_keys
    std::set<int> primary_key_ids;
    getTablePrimaryKeys(table_id, primary_key_ids);
    for (auto& primary_key_id : primary_key_ids) {
        primary_keys.push_back(all_column_types[primary_key_id].columnName);
    }

    // set foreign_keys
    std::vector<ForeignKeyInfo> foreign_key_infos;
    getTableForeignKeys(table_id, foreign_key_infos);

    std::vector<ForeignKeyInputInfo> foreign_key_input_infos;
    for (auto& foreign_key_info : foreign_key_infos) {
        // get foreign key column names
        std::vector<std::string> fKColumnNames;
        for (auto& foreign_key_columnId :
             foreign_key_info.foreign_key_columnIds) {
            fKColumnNames.push_back(
                all_column_types[foreign_key_columnId].columnName);
        }

        // get reference table name
        char* db_base_path = nullptr;
        getDatabasePath(currentDatabaseId, &db_base_path);
        char* all_table_path = nullptr;
        utils::joinPaths(db_base_path, ALL_TABLE_FILE_NAME, &all_table_path);

        std::vector<record::DataItem> data_items;
        std::vector<record::RecordLocation> record_locations;
        rm->getAllRecords(all_table_path, data_items, record_locations);

        delete[] db_base_path;
        delete[] all_table_path;

        std::string reference_table_name;
        for (auto& data_item : data_items) {
            if (data_item.dataId == foreign_key_info.reference_table_id) {
                reference_table_name =
                    data_item.values[0].value.charValue;
                break;
            }
        }

        // get record of the reference table
        char* reference_table_path = nullptr;
        getTableRecordPath(currentDatabaseId,
                           foreign_key_info.reference_table_id,
                           &reference_table_path);
        char* reference_table_record_path = nullptr;
        utils::joinPaths(reference_table_path, RECORD_FILE_NAME,
                          &reference_table_record_path);

        std::vector<record::ColumnType> reference_table_column_types;
        rm->getColumnTypes(reference_table_record_path,
                           reference_table_column_types);

        delete[] reference_table_path;
        delete[] reference_table_record_path;

        // get reference column names
        std::vector<std::string> reference_column_names;
        for (auto& reference_columnId :
             foreign_key_info.reference_columnIds) {
            reference_column_names.push_back(
                reference_table_column_types[reference_columnId].columnName);
        }

        foreign_keys.push_back(ForeignKeyInputInfo(fKColumnNames,
                                                   reference_table_name,
                                                   reference_column_names));
    }

    // get all index and unique
    std::vector<std::pair<int, std::vector<int>>> all_index;
    std::vector<std::string> index_names;
    getAllIndex(currentDatabaseId, table_id, all_index, index_names);

    index.clear();
    unique.clear();
    for (int i = 0; i < all_index.size(); i++) {
        auto& single_index = all_index[i];
        auto& index_name = index_names[i];
        if (index_name == "") continue;

        if (index_name.find(UNIQUE_SUFFIX) != std::string::npos) {
            std::vector<std::string> unique_column_names;
            for (auto& index_columnId : single_index.second) {
                for (auto& column_type : all_column_types) {
                    if (column_type.columnId == index_columnId) {
                        unique_column_names.push_back(column_type.columnName);
                        break;
                    }
                }
            }
            unique.push_back(unique_column_names);
            continue;
        } else {
            std::vector<std::string> index_column_names;
            for (auto& index_columnId : single_index.second) {
                for (auto& column_type : all_column_types) {
                    if (column_type.columnId == index_columnId) {
                        index_column_names.push_back(column_type.columnName);
                        break;
                    }
                }
            }
            index.push_back(index_column_names);
        }
    }
    return true;
}

bool SystemManager::loadTableFromFile(const char* table_name,
                                      const char* file_path,
                                      const char* delimeter) {
    if (currentDatabaseId == -1) {
        std::cout << "!ERROR" << std::endl;
        std::cout << "No database selected" << std::endl;
        return false;
    }

    // get table id
    int table_id = getTableId(table_name);
    if (table_id == -1) {
        std::cout << "!ERROR" << std::endl;
        std::cout << "Table " << table_name << " does not exist" << std::endl;
        return false;
    }

    // get record path
    char* table_path = nullptr;
    getTableRecordPath(currentDatabaseId, table_id, &table_path);
    char* record_path = nullptr;
    utils::joinPaths(table_path, RECORD_FILE_NAME, &record_path);

    int data_item_per_page =
        rm->insertRecordsToEmptyRecord(record_path, file_path, delimeter, true);
    if (data_item_per_page == -1) {
        return false;
    }
    std::vector<record::ColumnType> column_types;
    rm->getColumnTypes(record_path, column_types);

    std::vector<std::pair<int, std::vector<int>>> all_index;
    std::vector<std::string> index_names;
    getAllIndex(currentDatabaseId, table_id, all_index, index_names);

    std::ifstream csv_file(file_path);
    if (!csv_file.is_open()) {
        std::cerr << "Failed to open file " << file_path << std::endl;
        return false;
    }

    std::string line;
    int pageId = 1, slotId = 0;
    while (getline(csv_file, line)) {
        if (slotId == data_item_per_page) {
            pageId++;
            slotId = 0;
        }
        std::vector<std::string> csv_row;
        std::string item;
        for (int i = 0; i < line.length(); i++) {
            if (line[i] == delimeter[0]) {
                csv_row.push_back(item);
                item = "";
            } else {
                item += line[i];
            }
        }
        csv_row.push_back(item);

        record::DataItem data_item;
        int csv_row_idx = 0;
        for (auto& column : column_types) {
            data_item.columnIds.push_back(column.columnId);
            record::DateValue date_value;
            std::istringstream iss;
            double floatValue;
            if (column.dataType == record::DataTypeIdentifier::FLOAT) {
                std::istringstream iss(csv_row[csv_row_idx++]);
                iss >> floatValue;
            }
            switch (column.dataType) {
                case record::DataTypeIdentifier::INT:
                    data_item.values.push_back(
                        record::DataValue(record::DataTypeIdentifier::INT, false,
                                          std::stoi(csv_row[csv_row_idx++])));
                    break;
                case record::DataTypeIdentifier::FLOAT:
                    data_item.values.push_back(record::DataValue(
                        record::DataTypeIdentifier::FLOAT, false, floatValue));
                    break;
                case record::DataTypeIdentifier::VARCHAR:
                    data_item.values.push_back(
                        record::DataValue(record::DataTypeIdentifier::VARCHAR, false,
                                          csv_row[csv_row_idx++]));
                    break;
                case record::DataTypeIdentifier::DATE:
                    date_value.year = std::stoi(csv_row[csv_row_idx].substr(
                        0, csv_row[csv_row_idx].find('-')));
                    date_value.month = std::stoi(csv_row[csv_row_idx].substr(
                        csv_row[csv_row_idx].find('-') + 1,
                        csv_row[csv_row_idx].rfind('-') -
                            csv_row[csv_row_idx].find('-') - 1));
                    date_value.day = std::stoi(csv_row[csv_row_idx].substr(
                        csv_row[csv_row_idx].rfind('-') + 1));
                    csv_row_idx++;
                    data_item.values.push_back(record::DataValue(
                        record::DataTypeIdentifier::DATE, false, date_value));
                    break;
            }
        }

        for (auto& index : all_index) {
            char* index_file_path = nullptr;
            getIndexRecordPath(currentDatabaseId, table_id, index.first,
                               &index_file_path);
            index::IndexValue index_value(pageId, slotId, 0);
            for (auto& index_id : index.second) {
                if (data_item.values[index_id].isNull == false)
                    index_value.key.push_back(
                        data_item.values[index_id].value.intValue);
                else
                    index_value.key.push_back(INT_MIN);
            }
            assert(im->insertIndex(index_file_path, index_value));
            delete[] index_file_path;
        }
        slotId++;
    }

    delete[] table_path;
    delete[] record_path;
    return true;
}

void SystemManager::getAllIndex(
    int database_id, int table_id,
    std::vector<std::pair<int, std::vector<int>>>& index_ids,
    std::vector<std::string>& index_names) {
    index_ids.clear();
    // get index info path
    char* table_path = nullptr;
    getTableRecordPath(database_id, table_id, &table_path);
    char* index_info_path = nullptr;
    utils::joinPaths(table_path, INDEX_INFO_FILE_NAME, &index_info_path);

    // read all from index info
    std::vector<record::DataItem> data_items;
    std::vector<record::RecordLocation> record_locations;
    rm->getAllRecords(index_info_path, data_items, record_locations);

    for (auto& data_item : data_items) {
        std::vector<int> index_id;
        for (int i = 0; i < INDEX_KEY_MAX_NUM; i++) {
            if (data_item.values[i].isNull == false) {
                index_id.push_back(data_item.values[i].value.intValue);
            }
        }
        index_ids.push_back(std::make_pair(data_item.dataId, index_id));
        if (data_item.values[INDEX_KEY_MAX_NUM].isNull == false)
            index_names.push_back(
                data_item.values[INDEX_KEY_MAX_NUM].value.charValue);
        else
            index_names.push_back("");
    }
    delete[] table_path;
    delete[] index_info_path;
}

bool SystemManager::setActiveDatabase(const char* database_name) {
    // check if database exists
    int database_id = getDatabaseId(database_name);
    if (database_id == -1) {
        std::cout << "!ERROR" << std::endl;
        std::cout << "Database " << database_name << " does not exist"
                  << std::endl;
        return false;
    }
    currentDatabaseId = database_id;
    // update the current database name
    currentDatabaseName = database_name;
    return true;
}

int SystemManager::getTableId(const char* table_name) {
    // check db id
    if (currentDatabaseId == -1) return -1;

    // get all table path
    char* db_base_path = nullptr;
    getDatabasePath(currentDatabaseId, &db_base_path);
    char* all_table_path = nullptr;
    utils::joinPaths(db_base_path, ALL_TABLE_FILE_NAME, &all_table_path);

    // get all table record
    std::vector<record::DataItem> data_items;
    std::vector<record::RecordLocation> record_locations;
    rm->getAllRecords(all_table_path, data_items, record_locations);

    delete[] db_base_path;
    delete[] all_table_path;

    // searchRowsInTable for the table name
    for (int i = 0; i < data_items.size(); i++) {
        if (strcmp(data_items[i].values[0].value.charValue.c_str(),
                   table_name) == 0) {
            return data_items[i].dataId;
        }
    }
    return -1;
}

void SystemManager::getDatabasePath(int database_id, char** result) const {
    char* folder_name = nullptr;
    char* database_id_str = nullptr;
    
    utils::integerToString(database_id, & database_id_str);
    utils::joinStrings(DATABSE_FOLDER_PREFIX, database_id_str, &folder_name);
    utils::joinPaths(DATABASE_BASE_PATH, folder_name, result);

    delete[] folder_name;
    delete[] database_id_str;
}

void SystemManager::getTableRecordPath(int database_id, int table_id,
                                       char** result) {
    char* database_path = nullptr;
    getDatabasePath(database_id, &database_path);
    char* table_id_str = nullptr;
    utils::integerToString(table_id, &table_id_str);
    char* folder_name = nullptr;
    utils::joinStrings(TABLE_FOLDER_PREFIX, table_id_str, &folder_name);
    utils::joinPaths(database_path, folder_name, result);
    delete[] database_path;
    delete[] table_id_str;
    delete[] folder_name;
}

void SystemManager::getIndexRecordPath(int database_id, int table_id,
                                       int index_id, char** result) {
    char* table_path = nullptr;
    getTableRecordPath(database_id, table_id, &table_path);
    char* index_id_str = nullptr;
    utils::integerToString(index_id, &index_id_str);
    char* file_name = nullptr;
    utils::joinStrings(INDEX_FILE_PREFIX, index_id_str, &file_name);
    char* folder_path = nullptr;
    utils::joinPaths(table_path, INDEX_FOLDER_NAME, &folder_path);
    utils::joinPaths(folder_path, file_name, result);
    delete[] table_path;
    delete[] index_id_str;
    delete[] file_name;
    delete[] folder_path;
}

int SystemManager::getActiveDatabaseId() const { 
    return currentDatabaseId; 
}

std::string SystemManager::getDatabaseName(int databaseId){
    // get database name with ID.
    // dont use filename; it's not the same as database name.
    std::string databaseName;
    std::vector<record::DataItem> databaseNames; 
    std::vector<record::ColumnType> columnTypes;
    getAllDatabase(databaseNames, columnTypes);
    for (auto& database : databaseNames) {
        if (database.dataId == databaseId) {
            databaseName = database.values[0].value.charValue;
            break;
        }
    }
    return databaseName;

}

std::string SystemManager::getActiveDatabaseName() const {
    return currentDatabaseName;
}

}  // namespace system
}  // namespace dbs