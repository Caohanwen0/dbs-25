#pragma once  // 使头文件只被包含一次，避免重复包含

// 类型定义
typedef unsigned int uint;  // 定义 uint 为无符号整型
typedef unsigned char uchar;  // 定义 uchar 为无符号字符型
typedef unsigned int* BufType;  // 定义 BufType 为无符号整型指针

// 常用常量
#define BIT_PER_BUF 32  // 每个缓冲区包含 32 位
#define LOG_BIT_PER_BUF 5    // 2 ^ 5 = 32，表示 BIT_PER_BUF 的对数
#define BIT_PER_BUF_MASK 31  // 32 - 1，用于掩码，获取最低 5 位
#define BIT_PER_BYTE 8  // 每字节 8 位
#define LOG_BIT_PER_BYTE 3  // 2 ^ 3 = 8，表示 BIT_PER_BYTE 的对数

#define BYTE_PER_BUF 4  // 每个缓冲区包含 4 字节
#define LOG_BYTE_PER_BUF 2   // 2 ^ 2 = 4，表示 BYTE_PER_BUF 的对数
#define BYTE_PER_BUF_MASK 3  // 4 - 1，用于掩码，获取最低 2 位

#define FLOAT_MAX float(1e50)  // 浮点数最大值，设置为 1e50

// 位图相关常量
#define BIT_MAP_BIAS 5  // 位图的偏移量，用于位图操作

// 文件管理相关常量
#define PAGE_SIZE_BY_BYTE 8192  // 页面大小，单位为字节
#define BUF_PER_PAGE 2048  // 每个页面的缓冲区数量
#define BIT_PER_PAGE 65536  // 每个页面包含 65536 位
#define PAGE_SIZE_IDX 13  // 页面大小的对数，2^13 = 8192

// 哈希相关常量
#define HASH_MOD 6007  // 哈希表的模数
#define HASH_PRIME_1 53  // 哈希表的第一个质数
#define HASH_PRIME_2 89  // 哈希表的第二个质数

// 缓存相关常量
#define CACHE_CAPACITY 6000  // 缓存容量，单位为条目

// 记录管理相关常量
#define RECORD_META_DATA_LENGTH 80  // 记录的元数据长度，单位为字节
#define RECORD_META_DATA_HEAD 32    // 记录的元数据头部长度，单位为字节
#define MAX_COLUMN_NUM 102  // 最大列数
#define RECORD_PAGE_HEADER 64  // 记录页面头部长度，单位为字节
#define MAX_ITEM_PER_PAGE 512  // 每页最多条目数

// 索引管理相关常量
#define INDEX_HEADER_BYTE_LEN 16         // 索引头部字节长度，单位为字节
#define INDEX_BITMAP_PAGE_BYTE_LEN 8188  // 索引位图页面字节长度，单位为字节

// 数据路径相关常量
#define DATABASE_PATH "./data"  // 数据库路径
#define DATABASE_BASE_PATH "./data/base"  // 数据库基础路径
#define DATABASE_GLOBAL_PATH "./data/global"  // 数据库全局路径
#define DATABASE_GLOBAL_RECORD_PATH "./data/global/ALLDatabase"  // 全局数据库记录路径
#define DATABSE_FOLDER_PREFIX "DB"  // 数据库文件夹前缀
#define TABLE_FOLDER_PREFIX "TB"  // 表文件夹前缀
#define INDEX_FOLDER_NAME "IndexFiles"  // 索引文件夹名称
#define INDEX_FILE_PREFIX "INDEX"  // 索引文件前缀
#define ALL_TABLE_FILE_NAME "ALLTable"  // 所有表文件名
#define RECORD_FILE_NAME "Record"  // 记录文件名
#define PRIMARY_KEY_FILE_NAME "PrimaryKey"  // 主键文件名
#define FOREIGN_KEY_FILE_NAME "ForeignKey"  // 外键文件名
#define DOMINATE_FILE_NAME "Dominate"  // 主导文件名
#define INDEX_INFO_FILE_NAME "IndexInfo"  // 索引信息文件名

#define UNIQUE_SUFFIX "_UNIQUE_SUFFIX_"  // 唯一后缀标识

#define FOREIGN_KEY_MAX_NUM 10  // 外键最大数量
#define INDEX_KEY_MAX_NUM 10  // 索引键最大数量

#define JOIN_TABLE_ID_MULTIPLY 88  // 联接表ID乘数，用于联接操作

#define TMP_FILE_PREFIX "TMP"  // 临时文件前缀

#define BLOCK_PAGE_NUM 8  // 每个块包含 8 个页面

// 临时文件索引，用于生成不同的临时文件名
static int tmpFileIdx = 0;  // 初始化临时文件索引为 0

