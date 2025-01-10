#include <cstring>
#include <fstream>
#include <iostream>

#include "common/Color.hpp"
#include "fs/BufPageManager.hpp"
#include "fs/FileManager.hpp"
#include "index/IndexManager.hpp"
#include "parser/Parser.hpp"
#include "record/RecordManager.hpp"
#include "system/SystemManager.hpp"

int main(int argc, char *argv[]) {
    bool batchMode = false;
    bool init = false;
    std::string file_path = "";
    std::string table_name = "";
    std::string initDatabaseName = "";
    for (int i = 1; i < argc; i++) {
        auto param = std::string(argv[i]);
        if (param == "--init") { // initialization
            init = true;
        } 
        else if (param == "--batch" || param == "-b") { // -b, --batch：用于批处理模式启动
            batchMode = true;
        } 
        else if (param == "--file" || param == "-f") {  // -f <path>, --file <path>：用于从路径为 <path> 的文件导入数据，需要配合 -t 或 --table 使用，导入完成后 DBMS 应该以状态码 0 结束进程
            file_path = std::string(argv[++i]);
        } 
        else if (param == "--table" || param == "-t") { // -t <table>, --table <table>：用于指定导入数据的时的目标数据表，需要配合 -f 或 --file 使用
            table_name = std::string(argv[++i]);
        } 
        else if (param == "--database" || param == "-d") { //-d <db>, --database <db>：用于指定启动时使用的数据库，
        // 相当于已经执行了 USE <db>
            initDatabaseName = std::string(argv[++i]);
        } 
        else {
            std::cout  << "Unknown param: " << param << std::endl;
            i++;
            return -1;
        }
    }

    if (init) {
        dbs::fs::FileManager *fm = new dbs::fs::FileManager();
        dbs::fs::BufPageManager *bpm = new dbs::fs::BufPageManager(fm);
        dbs::record::RecordManager *rm =
            new dbs::record::RecordManager(fm, bpm);
        dbs::index::IndexManager *im = new dbs::index::IndexManager(fm, bpm);
        dbs::system::SystemManager *sm =
            new dbs::system::SystemManager(fm, rm, im);
        sm->cleanSystem();
        delete sm;
        delete im;
        delete rm;
        delete bpm;
        delete fm;
        return 0;
    }
    dbs::fs::FileManager *fm = new dbs::fs::FileManager();
    dbs::fs::BufPageManager *bpm = new dbs::fs::BufPageManager(fm);
    dbs::record::RecordManager *rm = new dbs::record::RecordManager(fm, bpm);
    dbs::index::IndexManager *im = new dbs::index::IndexManager(fm, bpm);
    dbs::system::SystemManager *sm = new dbs::system::SystemManager(fm, rm, im);
    dbs::parser::Parser *parser = new dbs::parser::Parser(rm, im, sm);
    sm->initializeSystem();
    if (initDatabaseName != "") {
        sm->setActiveDatabase(initDatabaseName.c_str());
    }
    if (table_name != "" && file_path != "") {
        std::string input = "LOAD DATA INFILE '" + file_path + "' INTO TABLE " +
                            table_name + " FIELDS TERMINATED BY ',';\n";
        auto result = parser->parse(input.c_str());
        std::cout << Color::OKGREEN << "@ " << (result ? "Success" : "Fail") << Color::ENDC << std::endl;
    }
    if (batchMode) {
        // while 输入一行
        parser->setOutputMode(false);
        char input[1000];
        while (std::cin.getline(input, 1000)) {
            if (strcmp(input, "exit") == 0) {
                break;
            }
            auto result = parser->parse(input);
            // print type of result
            std::cout << "@ " << (result ? "success" : "fail") << std::endl; //dont use color here it will be slow excruciatingly slow
        }
    } 
    else { // interactive mode
        // while 输入一行 string
        parser->setOutputMode(true);
        std::string input;
        std::cout << Color::PINK <<"Welcome to mySQL" << Color::ENDC << std::endl;
        std::cout << Color::PINK << "mySQL ("<< sm->getActiveDatabaseName() << ") >> " << Color::ENDC << std::flush;
        //std::cout << "mySQL>> " << std::flush;
        while (std::getline(std::cin, input, '\n')) {
            if (input == "exit" || input == "quit" || input == "q") {
                break;
            }
            while (input.find(";") == std::string::npos) {
                std::cout << "    -> " << std::flush;
                std::string input_continue;
                std::getline(std::cin, input_continue, '\n');
                input = input + input_continue;
            }
            auto result = parser->parse(input);
            // update thee active database  
            bpm->closeManager(); 
            std::cout << Color::PINK << "mySQL ("<< sm->getActiveDatabaseName() << ") >> " << Color::ENDC << std::flush;
            // std::cout << "mySQL>> " << std::flush;
        }
    }

    delete parser;
    delete sm;
    delete im;
    delete rm;
    delete bpm;
    delete fm;

    return 0;
}