#pragma once

// Standard library imports
#include <fstream>  // File operations
#include <string>  // String operations
#include <set>      // To handle sets of unique elements
#include <vector>   // To handle dynamic arrays (vectors)
#include <sstream>  // For stringstream operations  
#include <assert.h> // For assertions and debugging

// Includes from the project's common modules
#include "common/Config.hpp"        // Common configurations
#include "fs/BufPageManager.hpp"    // Buffer page manager for file system
#include "fs/FileManager.hpp"       // File system management
#include "index/IndexManager.hpp"  // Index management
#include "record/DataType.hpp"     // Defines different data types
#include "record/RecordManager.hpp" // Record management
#include "system/SystemColumns.hpp" // System columns and their definitions
#include "utils/Utilities.hpp"    // for database name retrivel

namespace dbs {
namespace system {

// SystemManager class is the main system controller responsible for handling 
// database operations, including creation, deletion, and management.
class SystemManager {
   public:
    /**
     * @brief Constructs the SystemManager object
     *
     * @param fm_ A pointer to the FileManager instance
     * @param bpm_ A pointer to the BufferPageManager instance
     * @param rm_ A pointer to the RecordManager instance
     * @param im_ A pointer to the IndexManager instance
     */
    SystemManager(fs::FileManager* fm_, record::RecordManager* rm_,
                  index::IndexManager* im_);

    /**
     * @brief Destructor of SystemManager to handle cleanup
     */
    ~SystemManager();

    /**
     * @brief Initializes the system, setting up necessary structures if data
     * is absent, or does nothing if data already exists.
     */
    void initializeSystem();

    /**
     * @brief Cleans the system by removing all data
     */
    void cleanSystem();

    /**
     * @brief Creates a new database with the given name
     *
     * @param database_name The name of the database to create
     * @return true if the database was created successfully, false otherwise
     */
    bool createDatabase(const char* database_name);

    /**
     * @brief Drops (deletes) a database with the given name
     *
     * @param database_name The name of the database to drop
     * @return true if the database was dropped successfully, false otherwise
     */
    bool dropDatabase(const char* database_name);

    /**
     * @brief Retrieves the unique identifier (ID) for a database
     *
     * @param database_name The name of the database
     * @return The unique database ID, or -1 if the database does not exist
     */
    int getDatabaseId(const char* database_name);

    /**
     * @brief Retrieves the name of a database given its unique ID
     *
     * @param database_id The unique ID of the database
     * @return The name of the database, or an empty string if it does not exist
     */
    std::string getDatabaseName(int database_id);
    /**
     * @brief Creates a new table in the current database
     *
     * @param table_name The name of the table to create
     * @param column_types A vector of column types for the table
     * @param primary_keys A vector of primary key column names
     * @param foreign_keys A vector of foreign key definitions
     * @return true if the table was created successfully, false otherwise
     */
    bool createTable(const char* table_name,
                     std::vector<record::ColumnType>& column_types,
                     const std::vector<std::string>& primary_keys,
                     const std::vector<ForeignKeyInputInfo>& foreign_keys);

    // Methods for managing foreign keys in tables
    bool addForeignKey(const char* table_name,
                       const ForeignKeyInfo& new_foreign_key);
    bool deleteForeignKey(const char* table_name, std::string foreign_key_name);

    /**
     * @brief Retrieves the column types for a given table
     *
     * @param table_id The unique ID of the table
     * @param column_types The vector to store the column types
     * @return true if the column types were retrieved, false otherwise
     */
    bool getTableColumnTypes(int table_id,
                             std::vector<record::ColumnType>& column_types);

    // Method to get primary keys for a table
    bool getTablePrimaryKeys(int table_id, std::set<int>& primary_keys);

    bool getTableForeignKeys(int table_id,
                             std::vector<ForeignKeyInfo>& foreign_keys);

    bool getTableDominate(int table_id, std::vector<int>& dominate_table_ids);

    /**
     * @brief Creates an index on a table's specified columns
     *
     * @param table_name The name of the table
     * @param index_name The name for the new index
     * @param columnIds The list of column indices to index
     * @param check_unique Whether the index should enforce uniqueness
     * @return true if the index was created successfully, false otherwise
     */
    bool addIndex(const char* table_name, const std::string& index_name,
                  const std::vector<int>& columnIds, bool check_unique);

    // Method to drop (delete) an existing index
    bool dropIndex(const char* table_name, const std::string& index_name);

    /**
     * @brief Drops a table from the database
     *
     * @param table_name The name of the table to drop
     * @return true if the table was dropped, false if it failed
     */
    bool dropTable(const char* table_name);

    /**
     * @brief Retrieves the unique table ID for a given table
     *
     * @param table_name The name of the table
     * @return The unique table ID, or -1 if the table does not exist
     */
    int getTableId(const char* table_name);

    /**
     * @brief Retrieves all databases in the system
     *
     * @param database_names A vector to store the names of all databases
     */
    void getAllDatabase(std::vector<record::DataItem>& database_names,
                        std::vector<record::ColumnType>& column_types);

    /**
     * @brief Retrieves all tables in the current database
     *
     * @param table_names A vector to store the names of all tables
     */
    bool getAllTable(std::vector<record::DataItem>& table_names,
                     std::vector<record::ColumnType>& column_types);

    /**
     * @brief Describes the structure of a table (Field, Type, Nullability, Default)
     *
     * @param table_name The name of the table to describe
     * @param table_info A vector to store the table structure information
     * @param column_types A vector to store column types
     * @return true if the table was described successfully, false otherwise
     */
    bool describeTable(const char* table_name,
                       std::vector<record::DataItem>& table_info,
                       std::vector<record::ColumnType>& column_types,
                       std::vector<std::string>& primary_keys,
                       std::vector<ForeignKeyInputInfo>& foreign_keys,
                       std::vector<std::vector<std::string>>& index,
                       std::vector<std::vector<std::string>>& unique);

    bool insertIntoTable(const char* table_name,
                         std::vector<record::DataItem>& data_items);

    /**
     * @brief Retrieves the file system path of the database
     * e.g., /data/base/DB1
     *
     * @param database_id The unique ID of the database
     * @param result A pointer to store the resulting path
     */
    void getDatabasePath(int database_id, char** result) const;

    /**
     * @brief Retrieves the file path for a table's records
     * e.g., /data/base/DB1/TB1
     *
     * @param database_id The ID of the database
     * @param table_id The ID of the table
     * @param result A pointer to store the resulting path
     */
    void getTableRecordPath(int database_id, int table_id, char** result);

    /**
     * @brief Retrieves the path for the index file associated with a table
     * e.g., /data/base/DB1/TB1/IndexFiles/INDEX1
     *
     * @param database_id The database ID
     * @param table_id The table ID
     * @param index_id The index ID
     * @param result A pointer to store the resulting path
     */
    void getIndexRecordPath(int database_id, int table_id, int index_id,
                            char** result);

    /**
     * @brief Switches to the specified database
     *
     * @param database_name The name of the database to use
     * @return true if the database switch was successful, false otherwise
     */
    bool setActiveDatabase(const char* database_name);

    /**
     * @brief Loads table data from a file
     *
     * @param table_name The name of the table to load data into
     * @param file_path The path of the file to load
     * @param delimeter The delimiter used in the file
     * @return true if the table was loaded successfully, false otherwise
     */
    bool loadTableFromFile(const char* table_name, const char* file_path,
                           const char* delimeter);

    /**
     * @brief Get the Active Database Id object
     * 
     * @return int 
     */
    int getActiveDatabaseId() const;

    std::string getActiveDatabaseName() const;

    /**
     * @brief Get the Active Database Name object
     * 
     * @return std::string 
     */
    bool deleteRowsFromTable(int table_id,
                         std::vector<SearchConstraint>& constraints);

    /**
     * @brief Get the Active Database Name object
     * 
     * @return std::string 
     */
    bool addPrimaryKey(int table_id, std::vector<int> columnIds);

    bool dropPrimaryKey(int table);

    bool addUnique(const char* table_name, const std::string& unique_name,
                   const std::vector<int>& columns);

    /**
     * @brief Drops a unique constraint from a table
     *
     * @param table_name The name of the table
     * @param unique_name The name of the unique constraint
     * @return true if the unique constraint was dropped, false otherwise
     */
    bool searchRowsInTable(int table_id, std::vector<SearchConstraint>& constraints,
                std::vector<record::DataItem>& result_datas,
                std::vector<record::ColumnType>& column_types,
                std::vector<record::RecordLocation>& record_location_results,
                int sort_by);
    /**
     * @brief Drops a unique constraint from a table
     *
     * @param table_name The name of the table
     * @param unique_name The name of the unique constraint
     * @return true if the unique constraint was dropped, false otherwise
     */
    bool searchAndSave(int table_id,
                       std::vector<record::ColumnType>& column_types,
                       std::vector<SearchConstraint>& constraints,
                       std::string& save_path, int& total_num);

    /**
     * @brief Drops a unique constraint from a table
     *
     * @param table_name The name of the table
     * @param unique_name The name of the unique constraint
     * @return true if the unique constraint was dropped, false otherwise
     */
    bool updateRows(int table_id, std::vector<SearchConstraint>& constraints,
                const record::DataItem& update_data);

    /**
     * @brief   
     * 
     * @param update_info 
     * @param table_id 
     * @param column_names 
     * @param data_item 
     */
    void fillInDataTypeField(
        std::vector<std::tuple<std::string, record::DataValue>>& update_info,
        int table_id, std::vector<std::string>& column_names,
        record::DataItem& data_item);

    /**
     * @brief   
     * 
     * @param constraints 
     * @param table_id 
     */
    void fillInDataTypeField(std::vector<SearchConstraint>& constraints,
                             int table_id);
    /**
     * @brief   
     * 
     * @param column_name 
     * @param table_names 
     * @return std::string 
     */
    std::string findTableNameOfColumnName(
        const std::string& column_name, std::vector<std::string>& table_names);

    bool getColumnID(int table_id, const std::vector<std::string>& column_names,
                     std::vector<int>& columnIds);

   private:
    int _createIndex (const char* index_info_path, const char* index_folder_path,
                    std::vector<int> index_ids, int table_id, std::string name);

    /**
     * @brief Retrieves all indices associated with a table
     *
     * @param database_id The ID of the database
     * @param table_id The ID of the table
     * @param index_ids A vector to store the index information
     * @param index_names A vector to store the names of the indices
     */
    void getAllIndex(int database_id, int table_id,
                     std::vector<std::pair<int, std::vector<int>>>& index_ids,
                     std::vector<std::string>& index_names);

    fs::FileManager* fm;   // Pointer to the FileManager instance
    record::RecordManager* rm;  // Pointer to the RecordManager instance
    index::IndexManager* im;  // Pointer to the IndexManager instance

    int currentDatabaseId;  // ID of the current active database
    std::string currentDatabaseName;  // Name of the current active database
};

}  // namespace system
}  // namespace dbs
