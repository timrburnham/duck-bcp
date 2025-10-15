#pragma once
#include <string>
#include <vector>
#include "bcp_writer.hpp"
#include "duckdb/common/types.hpp"

// Parse a non-XML BCP .fmt file (version 8.0+)
// Returns empty vector on error or if not found
std::vector<BCPTargetCol> ParseBCPFmtFile(const std::string &fmt_path);

// Map BCP file format type to DuckDB LogicalType
duckdb::LogicalType MapBCPTypeToDuckDBType(const std::string &bcp_type, int length = 0);

// Map DuckDB LogicalType to BCP file format type string
std::string MapDuckDBTypeToBCPType(const duckdb::LogicalType &type);
