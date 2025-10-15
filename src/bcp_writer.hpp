#pragma once
#include "duckdb.hpp"
#include <string>
#include <memory>
#include <fstream>

struct BCPTargetCol {
	std::string name;
	std::string sql_type;
	int length = 0;
	bool nullable;
};
struct BCPWriter {
	BCPWriter(const std::string &data_path, const std::vector<BCPTargetCol> &cols, bool unicode_native);

	void WriteChunk(duckdb::DataChunk &chunk, const std::vector<BCPTargetCol> &cols);
	void Finish();

private:
	std::ofstream out_;
	std::vector<BCPTargetCol> cols_;
	bool unicode_native_;

	// helpers
	void WriteNullFixedPrefix(size_t prefix_len);
	void WriteIntLE64(int64_t v, int nbytes);
	void WriteFloatLE(float v);
	void WriteDoubleLE(double v);
	void WriteGUID(const duckdb::hugeint_t &uuid_hi_lo);
	void WriteDecimal(const duckdb::Value &v, int precision, int scale);
	void WriteVarcharBytes(const duckdb::string_t &s, int maxlen);  // varchar with 2-byte prefix
	void WriteNVarcharUTF16(const duckdb::string_t &s, int maxlen); // nvarchar with 2-byte prefix (UTF-16LE)
	void WriteVarbinary(const duckdb::string_t &s);                 // 1-byte or 4-byte prefix (<=255 -> 1 byte)
	void WriteDate(const duckdb::date_t &d);                        // 3 bytes
	void WriteTime(const duckdb::dtime_t &t, int precision);        // 3..5 bytes
	void WriteDateTime2(const duckdb::timestamp_t &ts, int precision);
	void WriteDateTimeLegacy(const duckdb::timestamp_t &ts); // 8 bytes (days + 1/300s)
	void WriteSmallDateTime(const duckdb::timestamp_t &ts);  // 4 bytes (days + minutes)
};
