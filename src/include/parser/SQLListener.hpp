
// Generated from SQL.g4 by ANTLR 4.13.1

#pragma once


#include "antlr4-runtime.h"
#include "parser/SQLParser.hpp"


/**
 * This interface defines an abstract listener for a parse tree produced by  antlr4::SQLParser.
 */
class  SQLListener : public antlr4::tree::ParseTreeListener {
public:

  virtual void enterProgram( antlr4::SQLParser::ProgramContext *ctx) = 0;
  virtual void exitProgram( antlr4::SQLParser::ProgramContext *ctx) = 0;

  virtual void enterStatement( antlr4::SQLParser::StatementContext *ctx) = 0;
  virtual void exitStatement( antlr4::SQLParser::StatementContext *ctx) = 0;

  virtual void enterCreate_db( antlr4::SQLParser::Create_dbContext *ctx) = 0;
  virtual void exitCreate_db( antlr4::SQLParser::Create_dbContext *ctx) = 0;

  virtual void enterDrop_db( antlr4::SQLParser::Drop_dbContext *ctx) = 0;
  virtual void exitDrop_db( antlr4::SQLParser::Drop_dbContext *ctx) = 0;

  virtual void enterShow_dbs( antlr4::SQLParser::Show_dbsContext *ctx) = 0;
  virtual void exitShow_dbs( antlr4::SQLParser::Show_dbsContext *ctx) = 0;

  virtual void enterUse_db( antlr4::SQLParser::Use_dbContext *ctx) = 0;
  virtual void exitUse_db( antlr4::SQLParser::Use_dbContext *ctx) = 0;

  virtual void enterShow_tables( antlr4::SQLParser::Show_tablesContext *ctx) = 0;
  virtual void exitShow_tables( antlr4::SQLParser::Show_tablesContext *ctx) = 0;

  virtual void enterShow_indexes( antlr4::SQLParser::Show_indexesContext *ctx) = 0;
  virtual void exitShow_indexes( antlr4::SQLParser::Show_indexesContext *ctx) = 0;

  virtual void enterLoad_data( antlr4::SQLParser:: Load_tableContext *ctx) = 0;
  virtual void exitLoad_data( antlr4::SQLParser:: Load_tableContext *ctx) = 0;

  virtual void enterDump_data( antlr4::SQLParser::Drop_dbContext *ctx) = 0;
  virtual void exitDump_data( antlr4::SQLParser::Drop_dbContext *ctx) = 0;

  virtual void enterCreate_table( antlr4::SQLParser::Create_tableContext *ctx) = 0;
  virtual void exitCreate_table( antlr4::SQLParser::Create_tableContext *ctx) = 0;

  virtual void enterDrop_table( antlr4::SQLParser::Drop_tableContext *ctx) = 0;
  virtual void exitDrop_table( antlr4::SQLParser::Drop_tableContext *ctx) = 0;

  virtual void enterDescribe_table( antlr4::SQLParser::Describe_tableContext *ctx) = 0;
  virtual void exitDescribe_table( antlr4::SQLParser::Describe_tableContext *ctx) = 0;

  virtual void enterInsert_into_table( antlr4::SQLParser::Insert_into_tableContext *ctx) = 0;
  virtual void exitInsert_into_table( antlr4::SQLParser::Insert_into_tableContext *ctx) = 0;

  virtual void enterDelete_from_table( antlr4::SQLParser::Delete_from_tableContext *ctx) = 0;
  virtual void exitDelete_from_table( antlr4::SQLParser::Delete_from_tableContext *ctx) = 0;

  virtual void enterUpdate_table( antlr4::SQLParser::Update_tableContext *ctx) = 0;
  virtual void exitUpdate_table( antlr4::SQLParser::Update_tableContext *ctx) = 0;

  virtual void enterSelect_table_( antlr4::SQLParser::Select_table_Context *ctx) = 0;
  virtual void exitSelect_table_( antlr4::SQLParser::Select_table_Context *ctx) = 0;

  virtual void enterSelect_table( antlr4::SQLParser::Select_tableContext *ctx) = 0;
  virtual void exitSelect_table( antlr4::SQLParser::Select_tableContext *ctx) = 0;

  virtual void enterAlter_add_index( antlr4::SQLParser::Alter_add_indexContext *ctx) = 0;
  virtual void exitAlter_add_index( antlr4::SQLParser::Alter_add_indexContext *ctx) = 0;

  virtual void enterAlter_drop_index( antlr4::SQLParser::Alter_drop_indexContext *ctx) = 0;
  virtual void exitAlter_drop_index( antlr4::SQLParser::Alter_drop_indexContext *ctx) = 0;

  virtual void enterAlter_table_drop_pk( antlr4::SQLParser::Alter_table_drop_pkContext *ctx) = 0;
  virtual void exitAlter_table_drop_pk( antlr4::SQLParser::Alter_table_drop_pkContext *ctx) = 0;

  virtual void enterAlter_table_drop_foreign_key( antlr4::SQLParser::Alter_table_drop_foreign_keyContext *ctx) = 0;
  virtual void exitAlter_table_drop_foreign_key( antlr4::SQLParser::Alter_table_drop_foreign_keyContext *ctx) = 0;

  virtual void enterAlter_table_add_pk( antlr4::SQLParser::Alter_table_add_pkContext *ctx) = 0;
  virtual void exitAlter_table_add_pk( antlr4::SQLParser::Alter_table_add_pkContext *ctx) = 0;

  virtual void enterAlter_table_add_foreign_key( antlr4::SQLParser::Alter_table_add_foreign_keyContext *ctx) = 0;
  virtual void exitAlter_table_add_foreign_key( antlr4::SQLParser::Alter_table_add_foreign_keyContext *ctx) = 0;

  virtual void enterAlter_table_add_unique( antlr4::SQLParser::Alter_table_add_uniqueContext *ctx) = 0;
  virtual void exitAlter_table_add_unique( antlr4::SQLParser::Alter_table_add_uniqueContext *ctx) = 0;

  virtual void enterField_list( antlr4::SQLParser::Field_listContext *ctx) = 0;
  virtual void exitField_list( antlr4::SQLParser::Field_listContext *ctx) = 0;

  virtual void enterNormal_field( antlr4::SQLParser::Normal_fieldContext *ctx) = 0;
  virtual void exitNormal_field( antlr4::SQLParser::Normal_fieldContext *ctx) = 0;

  virtual void enterPrimary_key_field( antlr4::SQLParser::Primary_key_fieldContext *ctx) = 0;
  virtual void exitPrimary_key_field( antlr4::SQLParser::Primary_key_fieldContext *ctx) = 0;

  virtual void enterForeign_key_field( antlr4::SQLParser::Foreign_key_fieldContext *ctx) = 0;
  virtual void exitForeign_key_field( antlr4::SQLParser::Foreign_key_fieldContext *ctx) = 0;

  virtual void enterType_( antlr4::SQLParser::Type_Context *ctx) = 0;
  virtual void exitType_( antlr4::SQLParser::Type_Context *ctx) = 0;

  virtual void enterValue_lists( antlr4::SQLParser::Value_listsContext *ctx) = 0;
  virtual void exitValue_lists( antlr4::SQLParser::Value_listsContext *ctx) = 0;

  virtual void enterValue_list( antlr4::SQLParser::Value_listContext *ctx) = 0;
  virtual void exitValue_list( antlr4::SQLParser::Value_listContext *ctx) = 0;

  virtual void enterValue( antlr4::SQLParser::ValueContext *ctx) = 0;
  virtual void exitValue( antlr4::SQLParser::ValueContext *ctx) = 0;

  virtual void enterWhere_and_clause( antlr4::SQLParser::Where_and_clauseContext *ctx) = 0;
  virtual void exitWhere_and_clause( antlr4::SQLParser::Where_and_clauseContext *ctx) = 0;

  virtual void enterWhere_operator_expression( antlr4::SQLParser::Where_operator_expressionContext *ctx) = 0;
  virtual void exitWhere_operator_expression( antlr4::SQLParser::Where_operator_expressionContext *ctx) = 0;

  virtual void enterWhere_operator_select( antlr4::SQLParser::Where_operator_selectContext *ctx) = 0;
  virtual void exitWhere_operator_select( antlr4::SQLParser::Where_operator_selectContext *ctx) = 0;

  virtual void enterWhere_null( antlr4::SQLParser::Where_nullContext *ctx) = 0;
  virtual void exitWhere_null( antlr4::SQLParser::Where_nullContext *ctx) = 0;

  virtual void enterWhere_in_list( antlr4::SQLParser::Where_in_listContext *ctx) = 0;
  virtual void exitWhere_in_list( antlr4::SQLParser::Where_in_listContext *ctx) = 0;

  virtual void enterWhere_in_select( antlr4::SQLParser::Where_in_selectContext *ctx) = 0;
  virtual void exitWhere_in_select( antlr4::SQLParser::Where_in_selectContext *ctx) = 0;

  virtual void enterWhere_like_string( antlr4::SQLParser::Where_like_stringContext *ctx) = 0;
  virtual void exitWhere_like_string( antlr4::SQLParser::Where_like_stringContext *ctx) = 0;

  virtual void enterColumn( antlr4::SQLParser::ColumnContext *ctx) = 0;
  virtual void exitColumn( antlr4::SQLParser::ColumnContext *ctx) = 0;

  virtual void enterExpression( antlr4::SQLParser::ExpressionContext *ctx) = 0;
  virtual void exitExpression( antlr4::SQLParser::ExpressionContext *ctx) = 0;

  virtual void enterSet_clause( antlr4::SQLParser::Set_clauseContext *ctx) = 0;
  virtual void exitSet_clause( antlr4::SQLParser::Set_clauseContext *ctx) = 0;

  virtual void enterSelectors( antlr4::SQLParser::SelectorsContext *ctx) = 0;
  virtual void exitSelectors( antlr4::SQLParser::SelectorsContext *ctx) = 0;

  virtual void enterSelector( antlr4::SQLParser::SelectorContext *ctx) = 0;
  virtual void exitSelector( antlr4::SQLParser::SelectorContext *ctx) = 0;

  virtual void enterIdentifiers( antlr4::SQLParser::IdentifiersContext *ctx) = 0;
  virtual void exitIdentifiers( antlr4::SQLParser::IdentifiersContext *ctx) = 0;

  virtual void enterOperator_( antlr4::SQLParser::Operator_Context *ctx) = 0;
  virtual void exitOperator_( antlr4::SQLParser::Operator_Context *ctx) = 0;

  virtual void enterAggregator( antlr4::SQLParser::AggregatorContext *ctx) = 0;
  virtual void exitAggregator( antlr4::SQLParser::AggregatorContext *ctx) = 0;


};

