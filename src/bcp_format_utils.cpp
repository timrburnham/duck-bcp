#include "bcp_format_utils.hpp"
#include <fstream>
#include <sstream>
#include "duckdb/common/string_util.hpp"

using namespace duckdb;

std::vector<BCPTargetCol> ParseBCPFmtFile(const std::string &fmt_path) {
	std::vector<BCPTargetCol> cols;
	std::ifstream f(fmt_path);
	if (!f.is_open())
		return cols;
	std::string line;
	int header_lines = 2; // skip first 2 lines (version, col count)
	while (header_lines-- && std::getline(f, line)) {
	}
	while (std::getline(f, line)) {
		std::istringstream iss(line);
		int colid, prefixlen, fieldlen, colnum, nullable = 1;
		std::string type, term, colname;
		if (!(iss >> colid >> type >> prefixlen >> fieldlen >> term >> colnum >> colname))
			continue;
		// Try to read nullable (optional, SQL 2012+)
		if (!(iss >> nullable))
			nullable = 1;
		BCPTargetCol col;
		col.name = colname;
		col.sql_type = type;
		col.length = fieldlen;
		col.nullable = (nullable != 0);
		cols.push_back(col);
	}
	return cols;
}

duckdb::LogicalType MapBCPTypeToDuckDBType(const std::string &bcp_type, int length) {
	if (bcp_type == "SQLBIT") return LogicalType::BOOLEAN;
	if (bcp_type == "SQLTINYINT") return LogicalType::TINYINT;
	if (bcp_type == "SQLSMALLINT") return LogicalType::SMALLINT;
	if (bcp_type == "SQLINT") return LogicalType::INTEGER;
	if (bcp_type == "SQLBIGINT") return LogicalType::BIGINT;
	if (bcp_type == "SQLFLT4") return LogicalType::FLOAT;
	if (bcp_type == "SQLFLT8") return LogicalType::DOUBLE;
	if (bcp_type == "SQLDECIMAL" || bcp_type == "SQLNUMERIC") return LogicalType::DECIMAL(18, 0); // TODO: parse precision/scale
	if (bcp_type == "SQLCHAR" || bcp_type == "SQLVARCHAR") return LogicalType::VARCHAR;
	if (bcp_type == "SQLNCHAR" || bcp_type == "SQLNVARCHAR") return LogicalType::VARCHAR;
	if (bcp_type == "SQLVARBINARY" || bcp_type == "SQLBINARY" || bcp_type == "SQLIMAGE") return LogicalType::BLOB;
	if (bcp_type == "SQLDATE") return LogicalType::DATE;
	if (bcp_type == "SQLTIME") return LogicalType::TIME;
	if (bcp_type == "SQLDATETIME2" || bcp_type == "SQLDATETIME" || bcp_type == "SQLDATETIM4") return LogicalType::TIMESTAMP;
	if (bcp_type == "SQLUNIQUEID") return LogicalType::UUID;
	return LogicalType::VARCHAR;
}

std::string MapDuckDBTypeToBCPType(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
		return "SQLBIT";
	case LogicalTypeId::TINYINT:
		return "SQLTINYINT";
	case LogicalTypeId::SMALLINT:
		return "SQLSMALLINT";
	case LogicalTypeId::INTEGER:
		return "SQLINT";
	case LogicalTypeId::BIGINT:
		return "SQLBIGINT";
	case LogicalTypeId::UTINYINT:
		return "SQLTINYINT";
	case LogicalTypeId::USMALLINT:
		return "SQLSMALLINT";
	case LogicalTypeId::UINTEGER:
		return "SQLINT";
	case LogicalTypeId::UBIGINT:
		return "SQLBIGINT";
	case LogicalTypeId::FLOAT:
		return "SQLFLT4";
	case LogicalTypeId::DOUBLE:
		return "SQLFLT8";
	case LogicalTypeId::DECIMAL:
		return "SQLDECIMAL";
	case LogicalTypeId::VARCHAR:
		return "SQLCHAR";
	case LogicalTypeId::BLOB:
		return "SQLVARBINARY";
	case LogicalTypeId::DATE:
		return "SQLDATE";
	case LogicalTypeId::TIME:
		return "SQLTIME";
	case LogicalTypeId::TIMESTAMP:
		return "SQLDATETIME2";
	case LogicalTypeId::UUID:
		return "SQLUNIQUEID";
	default:
		return "SQLCHAR";
	}
}
