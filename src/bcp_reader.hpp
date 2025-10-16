#pragma once
#include "duckdb.hpp"
#include "bcp_format_utils.hpp"
#include <vector>
#include <string>

struct BCPCol;

struct BCPTableFunctionData;
struct BCPTableGlobalState;

duckdb::unique_ptr<duckdb::FunctionData> BCPTableBind(duckdb::ClientContext &context,
                                                      duckdb::TableFunctionBindInput &input,
                                                      duckdb::vector<duckdb::LogicalType> &return_types,
                                                      duckdb::vector<duckdb::string> &names);

duckdb::unique_ptr<duckdb::FunctionData> BCPReadBind(duckdb::ClientContext &context,
                                                     duckdb::CopyFromFunctionBindInput &info,
                                                     duckdb::vector<duckdb::string> &expected_names,
                                                     duckdb::vector<duckdb::LogicalType> &expected_types);

void BCPTableRead(duckdb::ClientContext &context, duckdb::TableFunctionInput &input, duckdb::DataChunk &output);

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> BCPTableInit(duckdb::ClientContext &context,
                                                                  duckdb::TableFunctionInitInput &input);
