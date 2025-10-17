#pragma once
#include <string>
#include <vector>
#include "duckdb/common/types.hpp"

struct BCPCol {
	std::string name;
	std::string sql_type;
	std::string collation;
	int prefix = 0;
	int length = 0;
};

// Parse a non-XML BCP .fmt file (version 8.0+)
// Returns empty vector on error or if not found
std::vector<BCPCol> ParseBCPFmtFile(const std::string &fmt_path);

// Map BCP file format type to DuckDB LogicalType
duckdb::LogicalType MapBCPTypeToDuckDBType(const std::string &bcp_type, int length = 0);

// Map DuckDB LogicalType to BCP file format type string
std::string MapDuckDBTypeToBCPType(const duckdb::LogicalType &type);

// Map BCP file format type to number of prefix bytes, assuming nullability
int MapBCPTypeToPrefixBytes(const std::string &bcp_type);
