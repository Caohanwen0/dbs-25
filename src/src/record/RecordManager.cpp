#include "record/RecordManager.hpp"

namespace dbs {
namespace record {

RecordManager::RecordManager(fs::FileManager* fm_, fs::BufPageManager* bpm_) {
    fm = fm_;
    bpm = bpm_;
    current_opening_file_paths.clear();
    current_opening_file_ids.clear();
    current_column_types_file_paths.clear();
    current_column_types.clear();
}

RecordManager::~RecordManager() {
    cleanAllCurrentColumnTypes();
    closeAllCurrentFile();
    fm = nullptr;
    bpm = nullptr;
}

void RecordManager::closeAllCurrentFile() {
    bpm->closeManager();
    for (auto& file_path : current_opening_file_paths) {
        delete[] file_path;
        file_path = nullptr;
    }
    current_opening_file_paths.clear();
    for (auto& file_id : current_opening_file_ids) {
        fm->closeFile(file_id);
    }
    current_opening_file_ids.clear();
}

void RecordManager::closeFileIfExist(const char* file_path) {
    int current_opening_file_num = current_opening_file_paths.size();
    for (int i = 0; i < current_opening_file_num; i++) {
        if (strcmp(current_opening_file_paths[i], file_path) == 0) {
            int file_id = current_opening_file_ids[i];
            bpm->closeManager();
            fm->closeFile(file_id);
            delete[] current_opening_file_paths[i];
            current_opening_file_paths[i] = nullptr;
            current_opening_file_paths.erase(
                current_opening_file_paths.begin() + i);
            current_opening_file_ids.erase(current_opening_file_ids.begin() +
                                           i);
            return;
        }
    }
}

void RecordManager::closeFirstFile() {
    if (current_opening_file_ids.size() == 0) return;
    bpm->closeManager();
    int file_id = current_opening_file_ids.front();
    current_opening_file_ids.erase(current_opening_file_ids.begin());
    fm->closeFile(file_id);
    delete[] current_opening_file_paths.front();
    current_opening_file_paths.front() = nullptr;
    current_opening_file_paths.erase(current_opening_file_paths.begin());
}

void RecordManager::cleanAllCurrentColumnTypes() {
    for (auto& file_path : current_column_types_file_paths) {
        delete[] file_path;
        file_path = nullptr;
    }
    current_column_types_file_paths.clear();
    current_column_types.clear();
}

void RecordManager::cleanFirstColumnTypes() {
    if (current_column_types_file_paths.size() == 0) return;
    delete[] current_column_types_file_paths.front();
    current_column_types_file_paths.erase(
        current_column_types_file_paths.begin());
    current_column_types.erase(current_column_types.begin());
}

void RecordManager::cleanColumnTypesIfExist(const char* file_path) {
    int current_column_types_num = current_column_types_file_paths.size();
    for (int i = 0; i < current_column_types_num; i++) {
        if (strcmp(current_column_types_file_paths[i], file_path) == 0) {
            delete[] current_column_types_file_paths[i];
            current_column_types_file_paths[i] = nullptr;
            current_column_types_file_paths.erase(
                current_column_types_file_paths.begin() + i);
            current_column_types.erase(current_column_types.begin() + i);
            return;
        }
    }
}

int RecordManager::openFile(const char* file_path) {
    int current_opening_file_num = current_opening_file_paths.size();
    for (int i = 0; i < current_opening_file_num; i++) {
        if (strcmp(current_opening_file_paths[i], file_path) == 0) {
            return current_opening_file_ids[i];
        }
    }
    if (current_opening_file_num == cacheCapacity) {
        closeFirstFile();
    }
    current_opening_file_paths.push_back(new char[strlen(file_path) + 1]);
    strcpy(current_opening_file_paths.back(), file_path);
    int file_id = fm->openFile(file_path);
    current_opening_file_ids.push_back(file_id);
    return file_id;
}

void RecordManager::initializeRecordFile(
    const char* file_path, const std::vector<ColumnType>& column_types) {
    closeFileIfExist(file_path);
    cleanColumnTypesIfExist(file_path);
    if (fm->doesFileExist(file_path)) {
        assert(fm->deleteFile(file_path));
    }
    assert(fm->createFile(file_path));
    int file_id = openFile(file_path);
    assert(file_id != -1);
    int index;
    BufType b;
    b = bpm->getPage(file_id, 0, index);
    for (int i = 0; i < RECORD_META_DATA_HEAD / BYTE_PER_BUF; i++) b[i] = 0;
    b[7] = (column_types.size() + BIT_PER_BUF - 1) / BIT_PER_BUF + 1;
    for (auto& column : column_types) {
        utils::setBitInBuffer(b, b[4], true);
        unsigned int columnId = b[4]++;

        int start_buf_position =
            (columnId * RECORD_META_DATA_LENGTH + RECORD_META_DATA_HEAD) /
            BYTE_PER_BUF;
        int columnName_length = column.columnName.size();
        b[start_buf_position] = columnId;
        utils::setByte(b[start_buf_position + 1], 0, column.dataType);
        utils::setByte(b[start_buf_position + 1], 1, columnName_length);
        if (column.dataType == VARCHAR) {
            utils::setTwoBytes(b[start_buf_position + 1], 1,
                             column.varcharLength);
            int varcharSpace = column.varcharLength * 2;
            if (varcharSpace % BYTE_PER_BUF == 0) {
                varcharSpace += 2;
            }
            b[start_buf_position + 2] = varcharSpace;
        }
        start_buf_position += 3;
        int buf_position = start_buf_position, byte_position = 0;
        for (int i = 0; i < columnName_length; i++) {
            if (byte_position == BYTE_PER_BUF) {
                buf_position++;
                byte_position = 0;
            }
            utils::setByte(b[buf_position], byte_position++,
                            column.columnName[i]);
        }
        start_buf_position += 8;
        auto& defaultValue = column.defaultValue.value;
        int defaultValue_varchar_len = 0;
        utils::setBitInNumber(b[start_buf_position], 0, column.isNotNull);
        utils::setBitInNumber(b[start_buf_position], 1,
                             column.defaultValue.hasDefaultValue);
        utils::setBitInNumber(b[start_buf_position], 2, defaultValue.isNull);
        utils::setBitInNumber(b[start_buf_position], 3, column.isUnique);
        if (column.defaultValue.hasDefaultValue && !defaultValue.isNull &&
            column.dataType == VARCHAR) {
            defaultValue_varchar_len = defaultValue.value.charValue.size();
            utils::setTwoBytes(b[start_buf_position], 1,
                             defaultValue_varchar_len);
        }
        start_buf_position += 1;
        if (column.defaultValue.hasDefaultValue && !defaultValue.isNull) {
            switch (column.dataType) {
                case INT:
                    b[start_buf_position] =
                        utils::intToBit32(defaultValue.value.intValue);
                    break;
                case FLOAT:
                    utils::floatToBit32(defaultValue.value.floatValue,
                                       b[start_buf_position],
                                       b[start_buf_position + 1]);
                    break;
                case VARCHAR:
                    buf_position = start_buf_position, byte_position = 0;
                    for (int i = 0; i < defaultValue_varchar_len; i++) {
                        if (byte_position == BYTE_PER_BUF) {
                            buf_position++;
                            byte_position = 0;
                        }
                        utils::setByte(b[buf_position], byte_position++,
                                        defaultValue.value.charValue[i]);
                    }
                    break;
                case DATE:
                    utils::setTwoBytes(b[start_buf_position], 0,
                                     defaultValue.value.dateValue.year);
                    utils::setByte(b[start_buf_position], 2,
                                    defaultValue.value.dateValue.month);
                    utils::setByte(b[start_buf_position], 3,
                                    defaultValue.value.dateValue.day);
                    break;
            }
        }
    }
    bpm->markPageDirty(index);
}

void RecordManager::updateColumnUnique(const char* file_path, int columnId,
                                       bool unique) {
    cleanAllCurrentColumnTypes();
    int file_id = openFile(file_path);
    assert(file_id != -1);
    int index;
    BufType b;
    b = bpm->getPage(file_id, 0, index);
    int column_num = b[4];
    bpm->accessPage(index);
    if (columnId >= column_num) return;
    int start_buf_position =
        (columnId * RECORD_META_DATA_LENGTH + RECORD_META_DATA_HEAD) /
        BYTE_PER_BUF;
    utils::setBitInNumber(b[start_buf_position + 11], 3, unique);
    bpm->markPageDirty(index);
}

void RecordManager::getColumnTypes(const char* file_path,
                                   std::vector<ColumnType>& column_types) {
    column_types.clear();
    int current_column_types_num = current_column_types_file_paths.size();
    for (int i = 0; i < current_column_types_num; i++) {
        if (strcmp(current_column_types_file_paths[i], file_path) == 0) {
            for (auto& column_type : current_column_types[i]) {
                column_types.push_back(column_type);
            }
            return;
        }
    }
    if (current_column_types_num == columnCacheCapacity) {
        cleanFirstColumnTypes();
    }
    current_column_types_file_paths.push_back(new char[strlen(file_path) + 1]);
    strcpy(current_column_types_file_paths.back(), file_path);
    int file_id = openFile(file_path);
    assert(file_id != -1);
    int index;
    BufType b;
    b = bpm->getPage(file_id, 0, index);
    for (int i = 0; i < MAX_COLUMN_NUM; i++) {
        if (!utils::getBitFromBuffer(b, i)) continue;
        int start_buf_position =
            (i * RECORD_META_DATA_LENGTH + RECORD_META_DATA_HEAD) /
            BYTE_PER_BUF;
        ColumnType column_type;
        column_type.columnId = b[start_buf_position];
        column_type.dataType =
            (DataTypeIdentifier)utils::getByte(b[start_buf_position + 1], 0);
        int columnName_length = utils::getByte(b[start_buf_position + 1], 1);
        column_type.varcharLength =
            utils::getTwoBytes(b[start_buf_position + 1], 1);
        column_type.varcharSpace = b[start_buf_position + 2];
        start_buf_position += 3;
        int buf_position = start_buf_position, byte_position = 0;
        column_type.columnName = "";
        for (int i = 0; i < columnName_length; i++) {
            if (byte_position == BYTE_PER_BUF) {
                buf_position++;
                byte_position = 0;
            }
            column_type.columnName.push_back(
                utils::getByte(b[buf_position], byte_position++));
        }
        start_buf_position += 8;
        column_type.isNotNull =
            utils::getBitFromNumber(b[start_buf_position], 0);
        column_type.defaultValue.hasDefaultValue =
            utils::getBitFromNumber(b[start_buf_position], 1);
        column_type.defaultValue.value.isNull =
            utils::getBitFromNumber(b[start_buf_position], 2);
        column_type.isUnique =
            utils::getBitFromNumber(b[start_buf_position], 3);
        int defaultValue_varchar_len =
            utils::getTwoBytes(b[start_buf_position], 1);
        column_type.defaultValue.value.dataType = column_type.dataType;
        if (column_type.defaultValue.hasDefaultValue &&
            !column_type.defaultValue.value.isNull) {
            start_buf_position++;
            switch (column_type.dataType) {
                case INT:
                    column_type.defaultValue.value.value.intValue =
                        utils::bit32ToInt(b[start_buf_position]);
                    break;
                case FLOAT:
                    column_type.defaultValue.value.value.floatValue =
                        utils::bit32ToFloat(b[start_buf_position],
                                           b[start_buf_position + 1]);
                    break;
                case VARCHAR:
                    buf_position = start_buf_position, byte_position = 0;
                    column_type.defaultValue.value.value.charValue = "";
                    for (int i = 0; i < defaultValue_varchar_len; i++) {
                        if (byte_position == BYTE_PER_BUF) {
                            buf_position++;
                            byte_position = 0;
                        }
                        column_type.defaultValue.value.value.charValue
                            .push_back(utils::getByte(b[buf_position],
                                                       byte_position++));
                    }
                    break;
                case DATE:
                    column_type.defaultValue.value.value.dateValue.year =
                        utils::getTwoBytes(b[start_buf_position], 0);

                    column_type.defaultValue.value.value.dateValue.month =
                        utils::getByte(b[start_buf_position], 2);
                    //
                    column_type.defaultValue.value.value.dateValue.day =
                        utils::getByte(b[start_buf_position], 3);
                    break;
            }
        }
        column_types.push_back(column_type);
    }
    bpm->accessPage(index);
    current_column_types.push_back(std::vector<ColumnType>());
    for (auto& column_type : column_types) {
        current_column_types.back().push_back(column_type);
    }
}

int RecordManager::insertRecordsToEmptyRecord(const char* file_path,
                                              const char* csv_path,
                                              const char* delimeter,
                                              bool output) {
    std::ifstream csv_file(csv_path);
    if (!csv_file.is_open()) {
        std::cerr << "Failed to open file " << csv_path << std::endl;
        return -1;
    }

    int file_id = openFile(file_path);
    assert(file_id != -1);

    std::vector<ColumnType> column_types;
    getColumnTypes(file_path, column_types);

    BufType b;
    int index;
    b = bpm->getPage(file_id, 0, index);
    int null_bitmap_buf_size = b[7];
    bpm->markPageDirty(index);
    int data_item_length =
        dataItemLength(column_types, null_bitmap_buf_size * BYTE_PER_BUF);
    int data_item_per_page = std::min(
        (PAGE_SIZE_BY_BYTE - RECORD_PAGE_HEADER) / data_item_length, MAX_ITEM_PER_PAGE);

    int pageId = 1, slotId = 0, record_id = 0;
    b = bpm->getPage(file_id, pageId, index);
    memset(b, 0, BUF_PER_PAGE * BYTE_PER_BUF);
    bpm->markPageDirty(index);

    std::string line;
    while (getline(csv_file, line)) {
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
        if (csv_row.size() != column_types.size()) {
            std::cout << "!ERROR" << std::endl;
            std::cout << "CSV file format error" << std::endl;
            return -1;
        }
        for (auto& column : column_types) {
            data_item.columnIds.push_back(column.columnId);
            record::DateValue dateValue;
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
                    dateValue.year = std::stoi(csv_row[csv_row_idx].substr(
                        0, csv_row[csv_row_idx].find('-')));
                    dateValue.month = std::stoi(csv_row[csv_row_idx].substr(
                        csv_row[csv_row_idx].find('-') + 1,
                        csv_row[csv_row_idx].rfind('-') -
                            csv_row[csv_row_idx].find('-') - 1));
                    dateValue.day = std::stoi(csv_row[csv_row_idx].substr(
                        csv_row[csv_row_idx].rfind('-') + 1));
                    csv_row_idx++;
                    data_item.values.push_back(record::DataValue(
                        record::DataTypeIdentifier::DATE, false, dateValue));
                    break;
            }
        }

        if (slotId == data_item_per_page) {
            pageId++;
            slotId = 0;
            b = bpm->getPage(file_id, pageId, index);
            memset(b, 0, BUF_PER_PAGE * BYTE_PER_BUF);
            bpm->markPageDirty(index);
        }
        setSlotItem(b, slotId, data_item_length, null_bitmap_buf_size,
                    record_id++, data_item, column_types);
        slotId++;
    }
    csv_file.close();
    if (output) {
        std::cout << "rows" << std::endl;
        std::cout << record_id << std::endl;
    }

    b = bpm->getPage(file_id, 0, index);
    b[5] = pageId;
    b[6] = record_id;
    bpm->markPageDirty(index);
    return data_item_per_page;
}

RecordLocation RecordManager::insertRecord(const char* file_path,
                                           DataItem data_item) {
    int file_id = openFile(file_path);
    assert(file_id != -1);

    std::vector<ColumnType> column_types;
    getColumnTypes(file_path, column_types);
    sortDataItem(column_types, data_item);
    if (!exactMatch(column_types, data_item)) {
        return RecordLocation{-1, -1};
    }

    BufType b;
    int index;
    b = bpm->getPage(file_id, 0, index);
    int page_num = b[5];
    int record_id = b[6];
    int null_bitmap_buf_size = b[7];
    bpm->accessPage(index);
    int data_item_length =
        dataItemLength(column_types, null_bitmap_buf_size * BYTE_PER_BUF);
    int data_item_per_page = std::min(
        (PAGE_SIZE_BY_BYTE - RECORD_PAGE_HEADER) / data_item_length, MAX_ITEM_PER_PAGE);

    for (int pageId = 1; pageId <= page_num; pageId++) {
        b = bpm->getPage(file_id, pageId, index);
        for (int slotId = 0; slotId < data_item_per_page; slotId++) {
            if (!utils::getBitFromBuffer(b, slotId)) {
                setSlotItem(b, slotId, data_item_length, null_bitmap_buf_size,
                            record_id, data_item, column_types);
                bpm->markPageDirty(index);
                b = bpm->getPage(file_id, 0, index);
                b[6]++;
                bpm->markPageDirty(index);
                return RecordLocation{pageId, slotId};
            }
            bpm->accessPage(index);
        }
    }
    b = bpm->getPage(file_id, page_num + 1, index);
    for (int i = 0; i < RECORD_PAGE_HEADER / BYTE_PER_BUF; i++) b[i] = 0;
    setSlotItem(b, 0, data_item_length, null_bitmap_buf_size, record_id,
                data_item, column_types);
    bpm->markPageDirty(index);
    b = bpm->getPage(file_id, 0, index);
    b[5]++;
    b[6]++;
    bpm->markPageDirty(index);
    return RecordLocation{page_num + 1, 0};
}

bool RecordManager::deleteRecord(const char* file_path,
                                 const RecordLocation& record_location) {
    int file_id = openFile(file_path);
    assert(file_id != -1);
    int index;
    BufType b;
    b = bpm->getPage(file_id, record_location.pageId, index);
    utils::setBitInBuffer(b, record_location.slotId, false);
    bpm->markPageDirty(index);
    return true;
}

bool RecordManager::getRecord(const char* file_path,
                              const RecordLocation& record_location,
                              DataItem& data_item) {
    int file_id = openFile(file_path);
    assert(file_id != -1);
    std::vector<ColumnType> column_types;
    getColumnTypes(file_path, column_types);
    int index;
    BufType b;
    b = bpm->getPage(file_id, 0, index);
    int null_bitmap_buf_size = b[7];

    bpm->accessPage(index);

    b = bpm->getPage(file_id, record_location.pageId, index);
    bpm->accessPage(index);
    if (!utils::getBitFromBuffer(b, record_location.slotId)) return false;
    data_item = getSlotItem(
        b, record_location.slotId,
        dataItemLength(column_types, null_bitmap_buf_size * BYTE_PER_BUF),
        null_bitmap_buf_size, column_types);
    return true;
}

bool RecordManager::getRecords(
    const char* file_path, const std::vector<RecordLocation>& record_locations,
    std::vector<DataItem>& data_items) {
    data_items.clear();
    int file_id = openFile(file_path);
    assert(file_id != -1);
    int index;
    BufType b;

    b = bpm->getPage(file_id, 0, index);
    int null_bitmap_buf_size = b[7];

    std::vector<ColumnType> column_types;
    getColumnTypes(file_path, column_types);

    for (auto& record_location : record_locations) {
        b = bpm->getPage(file_id, record_location.pageId, index);
        bpm->accessPage(index);
        if (!utils::getBitFromBuffer(b, record_location.slotId)) return false;
        data_items.push_back(getSlotItem(
            b, record_location.slotId,
            dataItemLength(column_types, null_bitmap_buf_size * BYTE_PER_BUF),
            null_bitmap_buf_size, column_types));
    }
    return true;
}

bool RecordManager::updateRecord(const char* file_path,
                                 const RecordLocation& record_location,
                                 const DataItem& data_item) {
    DataItem original_data_item, original_data_item_save;
    if (!getRecord(file_path, record_location, original_data_item)) {
        return false;
    }
    original_data_item_save = original_data_item;
    size_t update_column_num = data_item.values.size();
    for (size_t i = 0, j; i < update_column_num; i++) {
        auto& update_columnId = data_item.columnIds[i];
        auto& update_data_value = data_item.values[i];
        for (j = 0; j < original_data_item.columnIds.size(); j++) {
            if (original_data_item.columnIds[j] == update_columnId) {
                original_data_item.values[j] = update_data_value;
                break;
            }
        }
        if (j == original_data_item.columnIds.size()) {
            return false;
        }
    }
    if (!deleteRecord(file_path, record_location)) {
        return false;
    }
    int file_id = openFile(file_path);
    assert(file_id != -1);
    std::vector<ColumnType> column_types;
    getColumnTypes(file_path, column_types);

    BufType b;
    int index;
    b = bpm->getPage(file_id, 0, index);
    int null_bitmap_buf_size = b[7];
    bpm->accessPage(index);
    int data_item_length =
        dataItemLength(column_types, null_bitmap_buf_size * BYTE_PER_BUF);

    b = bpm->getPage(file_id, record_location.pageId, index);

    sortDataItem(column_types, original_data_item);
    if (!exactMatch(column_types, original_data_item)) {
        setSlotItem(b, record_location.slotId, data_item_length,
                    null_bitmap_buf_size, original_data_item.dataId,
                    original_data_item_save, column_types);
        bpm->markPageDirty(index);
        return false;
    }

    setSlotItem(b, record_location.slotId, data_item_length,
                null_bitmap_buf_size, original_data_item.dataId,
                original_data_item, column_types);
    bpm->markPageDirty(index);
    return true;
}

void RecordManager::setSlotItem(BufType b, int slotId, int slot_length,
                                int null_bitmap_buf_size, int record_id,
                                const DataItem& data_item,
                                const std::vector<ColumnType>& column_types) {
    utils::setBitInBuffer(b, slotId, true);
    int start_buf_position =
        (RECORD_PAGE_HEADER + slotId * slot_length) / BYTE_PER_BUF;
    b[start_buf_position] = record_id;
    int column_num = column_types.size(), buf_position = start_buf_position;
    int buf_offset = 0;
    start_buf_position++;
    for (int columnId = 0; columnId < column_num; columnId ++) {
        if (columnId % BIT_PER_BUF == 0) {
            buf_position++;
        }
        utils::setBitInNumber(b[buf_position], columnId & BIT_PER_BUF_MASK, data_item.values[columnId].isNull);
    }
    start_buf_position += null_bitmap_buf_size;
    for (int columnId = 0; columnId < column_num; columnId++) {
        auto& data_value = data_item.values[columnId];
        auto& column_type = column_types[columnId];
        int column_byte_width = 0;
        switch (column_type.dataType) {
            case INT:
            case FLOAT:
            case DATE:
                column_byte_width = getDataTypeSize(column_type.dataType);
                break;
            case VARCHAR:
                column_byte_width = column_type.varcharSpace + 2;
                break;
        }
        int varcharLength = 0;
        if (!data_value.isNull) {
            switch (column_type.dataType) {
                case INT:
                    b[start_buf_position] =
                        utils::intToBit32(data_value.value.intValue);
                    break;
                case FLOAT:
                    utils::floatToBit32(data_value.value.floatValue,
                                       b[start_buf_position],
                                       b[start_buf_position + 1]);
                    break;
                case VARCHAR:
                    varcharLength = data_value.value.charValue.size();
                    utils::setTwoBytes(b[start_buf_position], 0, varcharLength);
                    buf_position = start_buf_position, buf_offset = 2;
                    for (int i = 0; i < varcharLength; i++) {
                        if (buf_offset == BYTE_PER_BUF) {
                            buf_position++;
                            buf_offset = 0;
                        }
                        utils::setByte(b[buf_position], buf_offset++,
                                        data_value.value.charValue[i]);
                    }
                    break;
                case DATE:
                    utils::setTwoBytes(b[start_buf_position], 0,
                                     data_value.value.dateValue.year);
                    utils::setByte(b[start_buf_position], 2,
                                    data_value.value.dateValue.month);
                    utils::setByte(b[start_buf_position], 3,
                                    data_value.value.dateValue.day);
                    break;
            }
        }
        start_buf_position += column_byte_width / BYTE_PER_BUF;
    }
}

DataItem RecordManager::getSlotItem(
    BufType b, int slotId, int slot_length, int null_bitmap_buf_size,
    const std::vector<ColumnType>& column_types) {
    DataItem data_item;
    int start_buf_position =
        (RECORD_PAGE_HEADER + slotId * slot_length) / BYTE_PER_BUF;
    data_item.dataId = b[start_buf_position];
    int column_num = column_types.size(), buf_position = start_buf_position;
    int buf_offset = 0;
    start_buf_position++;
    for (int columnId = 0; columnId < column_num; columnId++) {
        if (columnId % BIT_PER_BUF == 0) buf_position++;
        bool isNull =
            utils::getBitFromNumber(b[buf_position], columnId & BIT_PER_BUF_MASK);
        data_item.values.push_back(
            DataValue{column_types[columnId].dataType, isNull});
        data_item.columnIds.push_back(column_types[columnId].columnId);
    }
    start_buf_position += null_bitmap_buf_size;
    for (int columnId = 0; columnId < column_num; columnId++) {
        auto& column_type = column_types[columnId];
        int column_byte_width = 0;
        data_item.values[columnId].dataType = column_type.dataType;
        switch (column_type.dataType) {
            case INT:
            case FLOAT:
            case DATE:
                column_byte_width = getDataTypeSize(column_type.dataType);
                break;
            case VARCHAR:
                column_byte_width = column_type.varcharSpace + 2;
                break;
        }
        if (!data_item.values[columnId].isNull) {
            int varcharLength = 0;
            switch (column_type.dataType) {
                case INT:
                    data_item.values[columnId].value.intValue =
                        utils::bit32ToInt(b[start_buf_position]);
                    break;
                case FLOAT:
                    data_item.values[columnId].value.floatValue =
                        utils::bit32ToFloat(b[start_buf_position],
                                           b[start_buf_position + 1]);
                    break;
                case VARCHAR:
                    varcharLength = utils::getTwoBytes(b[start_buf_position], 0);
                    data_item.values[columnId].value.charValue = "";
                    buf_position = start_buf_position, buf_offset = 2;
                    for (int i = 0; i < varcharLength; i++) {
                        if (buf_offset == BYTE_PER_BUF) {
                            buf_position++;
                            buf_offset = 0;
                        }
                        data_item.values[columnId]
                            .value.charValue.push_back(
                                utils::getByte(b[buf_position], buf_offset++));
                    }
                    break;
                case DATE:
                    data_item.values[columnId].value.dateValue.year =
                        utils::getTwoBytes(b[start_buf_position], 0);

                    data_item.values[columnId].value.dateValue.month =
                        utils::getByte(b[start_buf_position], 2);
                    data_item.values[columnId].value.dateValue.day =
                        utils::getByte(b[start_buf_position], 3);
                    break;
            }
        }
        start_buf_position += column_byte_width / BYTE_PER_BUF;
    }
    return data_item;
}

int RecordManager::dataItemLength(const std::vector<ColumnType>& column_types,
                                  int null_bitmap_size) {
    // return byte length
    int length = 4 + null_bitmap_size, tmp_length;
    for (auto& column_type : column_types) {
        switch (column_type.dataType) {
            case INT:
            case FLOAT:
            case DATE:
                length += getDataTypeSize(column_type.dataType);
                break;
            case VARCHAR:
                tmp_length =
                    2 + column_type.varcharSpace * getDataTypeSize(VARCHAR);
                tmp_length += tmp_length % 4 == 0 ? 0 : 4 - tmp_length % 4;
                length += tmp_length;
                break;
        }
    }
    length *= 2;
    return length;
}

bool RecordManager::deleteRecordFile(const char* file_path) {
    closeFileIfExist(file_path);
    cleanColumnTypesIfExist(file_path);
    if (!fm->doesFileExist(file_path)) return false;
    return fm->deleteFile(file_path);
}

void RecordManager::sortDataItem(const std::vector<ColumnType>& column_types,
                                 DataItem& data_item) {
    std::vector<DataValue> sorted_data_values;
    std::vector<int> sorted_columnIds;
    for (auto& column_type : column_types) {
        for (size_t i = 0; i < data_item.columnIds.size(); i++) {
            if (data_item.columnIds[i] == column_type.columnId) {
                sorted_data_values.push_back(data_item.values[i]);
                sorted_columnIds.push_back(data_item.columnIds[i]);
                break;
            }
        }
    }
    data_item.values = sorted_data_values;
    data_item.columnIds = sorted_columnIds;
}

int RecordManager::getTotalPageNum(const char* file_path) {
    int file_id = openFile(file_path);
    assert(file_id != -1);
    BufType b;
    int index;
    b = bpm->getPage(file_id, 0, index);
    int page_num = b[5];
    bpm->accessPage(index);
    return page_num;
}

void RecordManager::getRecordsInPageRange(const char* file_path,
                                          std::vector<DataItem>& data_items,
                                          int low_page, int upper_page) {
    data_items.clear();
    int file_id = openFile(file_path);
    assert(file_id != -1);

    std::vector<ColumnType> column_types;
    getColumnTypes(file_path, column_types);

    BufType b;
    int index;
    b = bpm->getPage(file_id, 0, index);
    int page_num = b[5];
    int null_bitmap_buf_size = b[7];
    bpm->accessPage(index);
    int data_item_length =
        dataItemLength(column_types, null_bitmap_buf_size * BYTE_PER_BUF);
    int data_item_per_page = std::min(
        (PAGE_SIZE_BY_BYTE - RECORD_PAGE_HEADER) / data_item_length, MAX_ITEM_PER_PAGE);

    for (int pageId = low_page; pageId < upper_page; pageId++) {
        b = bpm->getPage(file_id, pageId, index);
        bpm->accessPage(index);
        for (int slotId = 0; slotId < data_item_per_page; slotId++) {
            if (!utils::getBitFromBuffer(b, slotId)) continue;
            data_items.push_back(
                getSlotItem(b, slotId,
                            dataItemLength(column_types,
                                           null_bitmap_buf_size * BYTE_PER_BUF),
                            null_bitmap_buf_size, column_types));
        }
    }
}

void RecordManager::getAllRecords(
    const char* file_path, std::vector<DataItem>& data_items,
    std::vector<RecordLocation>& record_locations) {
    data_items.clear();
    record_locations.clear();
    int file_id = openFile(file_path);
    assert(file_id != -1);

    std::vector<ColumnType> column_types;
    getColumnTypes(file_path, column_types);

    BufType b;
    int index;
    b = bpm->getPage(file_id, 0, index);
    int page_num = b[5];
    int null_bitmap_buf_size = b[7];
    bpm->accessPage(index);
    int data_item_length =
        dataItemLength(column_types, null_bitmap_buf_size * BYTE_PER_BUF);
    int data_item_per_page = std::min(
        (PAGE_SIZE_BY_BYTE - RECORD_PAGE_HEADER) / data_item_length, MAX_ITEM_PER_PAGE);

    for (int pageId = 1; pageId <= page_num; pageId++) {
        b = bpm->getPage(file_id, pageId, index);
        bpm->accessPage(index);
        for (int slotId = 0; slotId < data_item_per_page; slotId++) {
            if (!utils::getBitFromBuffer(b, slotId)) continue;
            data_items.push_back(
                getSlotItem(b, slotId,
                            dataItemLength(column_types,
                                           null_bitmap_buf_size * BYTE_PER_BUF),
                            null_bitmap_buf_size, column_types));
            record_locations.push_back(RecordLocation{pageId, slotId});
        }
    }
}

int RecordManager::getAllRecordWithConstraintSaveFile(
    const char* file_path, const char* file_path_save,
    const std::vector<system::SearchConstraint>& constraints) {
    int file_id = openFile(file_path);
    assert(file_id != -1);

    std::vector<ColumnType> column_types;
    getColumnTypes(file_path, column_types);

    std::ofstream outputFile(file_path_save);
    if (!outputFile.is_open()) {
        std::cerr << "Error opening file!" << std::endl;
        return 0;
    }

    int cnt = 0;
    BufType b;
    int index;
    b = bpm->getPage(file_id, 0, index);
    int page_num = b[5];
    int null_bitmap_buf_size = b[7];
    bpm->accessPage(index);
    int data_item_length =
        dataItemLength(column_types, null_bitmap_buf_size * BYTE_PER_BUF);
    int data_item_per_page = std::min(
        (PAGE_SIZE_BY_BYTE - RECORD_PAGE_HEADER) / data_item_length, MAX_ITEM_PER_PAGE);

    for (int pageId = 1; pageId <= page_num; pageId++) {
        b = bpm->getPage(file_id, pageId, index);
        bpm->accessPage(index);
        for (int slotId = 0; slotId < data_item_per_page; slotId++) {
            if (!utils::getBitFromBuffer(b, slotId)) continue;
            auto data_item =
                getSlotItem(b, slotId,
                            dataItemLength(column_types,
                                           null_bitmap_buf_size * BYTE_PER_BUF),
                            null_bitmap_buf_size, column_types);
            bool valid = true;
            for (auto& constraint : constraints) {
                if (!system::validConstraint(constraint, data_item)) {
                    valid = false;
                    break;
                }
            }
            if (valid) {
                for (int i = 0; i < data_item.values.size() - 1; i++) {
                    outputFile << data_item.values[i].toString() << ",";
                }
                outputFile << data_item.values.back().toString();
                outputFile << std::endl;
                cnt++;
            }
        }
    }
    outputFile.close();
    return cnt;
}

void RecordManager::getAllRecordWithConstraint(
    const char* file_path, std::vector<DataItem>& data_items,
    std::vector<RecordLocation>& record_locations,
    const std::vector<system::SearchConstraint>& constraints) {
    data_items.clear();
    record_locations.clear();
    int file_id = openFile(file_path);
    assert(file_id != -1);

    std::vector<ColumnType> column_types;
    getColumnTypes(file_path, column_types);

    BufType b;
    int index;
    b = bpm->getPage(file_id, 0, index);
    int page_num = b[5];
    int null_bitmap_buf_size = b[7];
    bpm->accessPage(index);
    int data_item_length =
        dataItemLength(column_types, null_bitmap_buf_size * BYTE_PER_BUF);
    int data_item_per_page = std::min(
        (PAGE_SIZE_BY_BYTE - RECORD_PAGE_HEADER) / data_item_length, MAX_ITEM_PER_PAGE);

    for (int pageId = 1; pageId <= page_num; pageId++) {
        b = bpm->getPage(file_id, pageId, index);
        bpm->accessPage(index);
        for (int slotId = 0; slotId < data_item_per_page; slotId++) {
            if (!utils::getBitFromBuffer(b, slotId)) continue;
            auto data_item =
                getSlotItem(b, slotId,
                            dataItemLength(column_types,
                                           null_bitmap_buf_size * BYTE_PER_BUF),
                            null_bitmap_buf_size, column_types);
            bool valid = true;
            for (auto& constraint : constraints) {
                if (!system::validConstraint(constraint, data_item)) {
                    valid = false;
                    break;
                }
            }
            if (valid) {
                data_items.push_back(data_item);
                record_locations.push_back(RecordLocation{pageId, slotId});
            }
        }
    }
}
}  // namespace record
}  // namespace dbs