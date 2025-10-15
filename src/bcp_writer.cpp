#include "bcp_writer.hpp"
#include "duckdb/common/serializer/buffered_file_writer.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/vector.hpp"
#include "utf8proc_wrapper.hpp"

#include <nlohmann/json.hpp>
#include <codecvt>
#include <locale>

using namespace duckdb;
using std::string;

static inline int ParsePrecision(const string &sql_type, int def = 18) {
	auto lp = sql_type.find('(');
	auto rp = sql_type.find(')');
	if (lp == string::npos || rp == string::npos)
		return def;
	auto csv = sql_type.substr(lp + 1, rp - lp - 1);
	auto comma = csv.find(',');
	if (comma == string::npos)
		return stoi(csv);
	return stoi(csv.substr(0, comma));
}
static inline int ParseScale(const string &sql_type, int def = 0) {
	auto lp = sql_type.find('(');
	auto rp = sql_type.find(')');
	if (lp == string::npos || rp == string::npos)
		return def;
	auto csv = sql_type.substr(lp + 1, rp - lp - 1);
	auto comma = csv.find(',');
	if (comma == string::npos)
		return def;
	return stoi(csv.substr(comma + 1));
}
static inline int ParseDT2Prec(const string &sql_type, int def = 6) {
	// datetime2(p)
	return ParsePrecision(sql_type, def);
}

BCPWriter::BCPWriter(const string &data_path, const std::vector<BCPTargetCol> &cols, bool unicode_native)
    : cols_(cols), unicode_native_(unicode_native) {
	out_.open(data_path, std::ios::binary);
	if (!out_)
		throw IOException("Unable to open BCP output: " + data_path);
}

void BCPWriter::Finish() {
	out_.flush();
	out_.close();
}

void BCPWriter::WriteIntLE64(int64_t v, int nbytes) {
	for (int i = 0; i < nbytes; i++)
		out_.put((char)((uint64_t)v >> (8 * i)));
}
void BCPWriter::WriteFloatLE(float v) {
	static_assert(sizeof(float) == 4, "float size");
	uint32_t u;
	memcpy(&u, &v, 4);
	WriteIntLE64(u, 4);
}
void BCPWriter::WriteDoubleLE(double v) {
	static_assert(sizeof(double) == 8, "double size");
	uint64_t u;
	memcpy(&u, &v, 8);
	WriteIntLE64((int64_t)u, 8);
}
void BCPWriter::WriteNullFixedPrefix(size_t prefix_len) {
	// NULL is represented by prefix length containing -1
	// table shows at least 1 byte needed; we'll fill all prefix bytes with 0xFF for -1 two's complement
	for (size_t i = 0; i < prefix_len; i++)
		out_.put((char)0xFF);
}

void BCPWriter::WriteVarcharBytes(const string_t &s, int maxlen) {
	// prefix 2 bytes length, -1 => NULL
	auto len = s.GetSize();
	if (maxlen > 0 && len > (size_t)maxlen)
		len = maxlen;
	uint16_t le = (uint16_t)len;
	out_.put((char)(le & 0xFF));
	out_.put((char)((le >> 8) & 0xFF));
	out_.write(s.GetData(), len);
}

void BCPWriter::WriteNVarcharUTF16(const string_t &s, int maxlen) {
	// UTF-16LE, prefix is number of BYTES (SQL Server expects pairs)
	std::u16string u16;
	{
		// quick UTF8->UTF16LE conversion
		auto u8 = std::string(s.GetData(), s.GetSize());
		// Use Utf8Proc API to validate/inspect string (no-op replacement for previous utf8proc usage)
		Utf8Proc::IsValid(u8.c_str(), u8.size());
		std::wstring_convert<std::codecvt_utf8_utf16<char16_t>, char16_t> cvt;
		u16 = cvt.from_bytes(u8);
	}
	if (maxlen > 0 && (int)u16.size() > maxlen)
		u16.resize(maxlen);
	uint32_t bytes = (uint32_t)(u16.size() * 2);
	// prefix 2 bytes (varchar/nvarchar)
	uint16_t le = (uint16_t)bytes;
	out_.put((char)(le & 0xFF));
	out_.put((char)((le >> 8) & 0xFF));
	out_.write((const char *)u16.data(), bytes);
}

void BCPWriter::WriteVarbinary(const string_t &s) {
	auto len = s.GetSize();
	if (len <= 0xFF) {
		out_.put((char)len); // 1-byte prefix
	} else {
		// for simplicity: 4-byte prefix for larger payloads
		WriteIntLE64((int32_t)len, 4);
	}
	out_.write(s.GetData(), len);
}

void BCPWriter::WriteGUID(const hugeint_t &uuid) {
	// DuckDB stores UUID as 128-bit; here we write 16 bytes as-is (LE groups align with SQL Server's uniqueidentifier)
	// (Note: exact byte order of uniqueidentifier vs textual GUID display is implementation-defined; for BCP native it
	// writes raw GUID) Store low then high 64 bits as little-endian bytes This simplistic approach works with SQL
	// Server uniqueidentifier BCP native.
	WriteIntLE64((int64_t)uuid.lower, 8);
	WriteIntLE64((int64_t)uuid.upper, 8);
}

void BCPWriter::WriteDate(const date_t &d) {
	// SQL Server 'date' stores days since 0001-01-01 in 3 bytes (LE). We can convert via DuckDB helper.
	int32_t days = Date::EpochDays(d); // days since 1970-01-01 in DuckDB
	// Convert epoch: SQL Server's 'date' epoch is 0001-01-01; DuckDB's is 1970-01-01: add offset
	// 1970-01-01 is 719162 days after 0001-01-01
	int32_t ss_days = days + 719162;
	// write 3 bytes LE
	out_.put((char)(ss_days & 0xFF));
	out_.put((char)((ss_days >> 8) & 0xFF));
	out_.put((char)((ss_days >> 16) & 0xFF));
}

void BCPWriter::WriteTime(const dtime_t &t, int precision) {
	// time(p): integral count of 10^-n seconds (100ns at p=7)
	// DuckDB stores microseconds. Convert to 100ns ticks:
	int64_t micros = t.micros;
	// 1 microsecond = 10 * 100ns units
	int64_t ticks100ns = micros * 10;
	// SQL Server stores variable-sized little-endian:
	// p 0-2 => 3 bytes, p 3-4 => 4 bytes, p 5-7 => 5 bytes
	int size = (precision <= 2) ? 3 : (precision <= 4 ? 4 : 5);
	for (int i = 0; i < size; i++)
		out_.put((char)((uint64_t)ticks100ns >> (8 * i)));
}

void BCPWriter::WriteDateTime2(const timestamp_t &ts, int precision) {
	// datetime2: date (days since 0001-01-01) + time (ticks of 100ns), little-endian, size varies by precision
	date_t d = Timestamp::GetDate(ts);
	dtime_t t = Timestamp::GetTime(ts);
	WriteTime(t, precision);
	WriteDate(d);
}

void BCPWriter::WriteDateTimeLegacy(const timestamp_t &ts) {
	// datetime: 8 bytes = days since 1900-01-01 (4) + ticks of 1/300 second since midnight (4)
	date_t d = Timestamp::GetDate(ts);
	int32_t days1970 = Date::EpochDays(d);
	int32_t days_since_1900 = days1970 + (719162 - 693596); // 1970-01-01−>0001 + adjust to 1900-01-01
	if (days_since_1900 < -53690)
		days_since_1900 = -53690; // clamp
	dtime_t tt = Timestamp::GetTime(ts);
	int64_t micros = tt.micros;
	int32_t ticks300 = (int32_t)llround((double)micros / (1000000.0 / 300.0));
	WriteIntLE64(days_since_1900, 4);
	WriteIntLE64(ticks300, 4);
}

void BCPWriter::WriteSmallDateTime(const timestamp_t &ts) {
	// smalldatetime: days since 1900-01-01 (2 bytes) + minutes since midnight (2 bytes)
	date_t d = Timestamp::GetDate(ts);
	int32_t days1970 = Date::EpochDays(d);
	int32_t days_since_1900 = days1970 + (719162 - 693596);
	WriteIntLE64(days_since_1900 & 0xFFFF, 2);
	dtime_t t = Timestamp::GetTime(ts);
	int32_t minutes = (int32_t)(t.micros / (60 * 1000000));
	WriteIntLE64(minutes & 0xFFFF, 2);
}

static inline bool IsNullable(const BCPTargetCol &c) {
	return c.nullable;
}

// dispatch per SQL Server type string
void BCPWriter::WriteChunk(DataChunk &chunk, const std::vector<BCPTargetCol> &cols) {
	chunk.Flatten();
	auto n = chunk.size();
	for (idx_t r = 0; r < n; r++) {
		for (idx_t c = 0; c < std::min(chunk.ColumnCount(), cols.size()); c++) {
			auto &vc = chunk.data[c];
			auto &tc = cols[c];

			if (vc.GetType().id() == LogicalTypeId::SQLNULL || FlatVector::IsNull(vc, r)) {
				// NULL handling by BCP type
				auto bcp_type = tc.sql_type;
				if (bcp_type == "SQLCHAR" || bcp_type == "SQLNCHAR" || bcp_type == "SQLVARCHAR" ||
				    bcp_type == "SQLNVARCHAR") {
					WriteNullFixedPrefix(2);
				} else if (bcp_type == "SQLVARBINARY" || bcp_type == "SQLBINARY" || bcp_type == "SQLIMAGE") {
					WriteNullFixedPrefix(1);
				} else {
					WriteNullFixedPrefix(1);
				}
				continue;
			}

			// Non-null: encode based on BCP type
			auto bcp_type = tc.sql_type;
			if (bcp_type == "SQLBIT") {
				if (IsNullable(tc))
					out_.put((char)1);
				out_.put((char)(BooleanValue::Get(vc.GetValue(r)) ? 1 : 0));
			} else if (bcp_type == "SQLTINYINT") {
				if (IsNullable(tc))
					out_.put((char)1);
				auto v = (uint8_t)IntegerValue::Get(vc.GetValue(r));
				out_.put((char)v);
			} else if (bcp_type == "SQLSMALLINT") {
				if (IsNullable(tc))
					out_.put((char)2);
				WriteIntLE64((int16_t)IntegerValue::Get(vc.GetValue(r)), 2);
			} else if (bcp_type == "SQLINT") {
				if (IsNullable(tc))
					out_.put((char)4);
				WriteIntLE64((int32_t)IntegerValue::Get(vc.GetValue(r)), 4);
			} else if (bcp_type == "SQLBIGINT") {
				if (IsNullable(tc))
					out_.put((char)8);
				WriteIntLE64((int64_t)IntegerValue::Get(vc.GetValue(r)), 8);
			} else if (bcp_type == "SQLDECIMAL" || bcp_type == "SQLNUMERIC") {
				// TODO: parse precision/scale if needed
				if (IsNullable(tc))
					out_.put((char)1);
				WriteDecimal(vc.GetValue(r), 18, 0); // default p,s
			} else if (bcp_type == "SQLFLT4") {
				if (IsNullable(tc))
					out_.put((char)4);
				WriteFloatLE((float)FloatValue::Get(vc.GetValue(r)));
			} else if (bcp_type == "SQLFLT8") {
				if (IsNullable(tc))
					out_.put((char)8);
				WriteDoubleLE((double)DoubleValue::Get(vc.GetValue(r)));
			} else if (bcp_type == "SQLUNIQUEID") {
				if (IsNullable(tc))
					out_.put((char)16);
				// value in DuckDB UUID is stored as VARCHAR; adapt if you keep UUID logical type
				auto s = StringValue::Get(vc.GetValue(r));
				throw NotImplementedException("UUID parser omitted for brevity.");
			} else if (bcp_type == "SQLVARBINARY" || bcp_type == "SQLBINARY" || bcp_type == "SQLIMAGE") {
				auto s = StringValue::Get(vc.GetValue(r));
				string_t st(s);
				WriteVarbinary(st);
			} else if (bcp_type == "SQLNCHAR" || bcp_type == "SQLNVARCHAR") {
				auto s = StringValue::Get(vc.GetValue(r));
				string_t st(s);
				WriteNVarcharUTF16(st, tc.length);
			} else if (bcp_type == "SQLCHAR" || bcp_type == "SQLVARCHAR") {
				auto s = StringValue::Get(vc.GetValue(r));
				string_t st(s);
				WriteVarcharBytes(st, tc.length);
			} else if (bcp_type == "SQLDATE") {
				if (IsNullable(tc))
					out_.put((char)3);
				WriteDate(DateValue::Get(vc.GetValue(r)));
			} else if (bcp_type == "SQLTIME") {
				int p = 7; // default precision
				if (IsNullable(tc))
					out_.put((char)5);
				WriteTime(TimeValue::Get(vc.GetValue(r)), p);
			} else if (bcp_type == "SQLDATETIME2") {
				int p = 6; // default precision
				if (IsNullable(tc))
					out_.put((char)8);
				WriteDateTime2(TimestampValue::Get(vc.GetValue(r)), p);
			} else if (bcp_type == "SQLDATETIME") {
				if (IsNullable(tc))
					out_.put((char)8);
				WriteDateTimeLegacy(TimestampValue::Get(vc.GetValue(r)));
			} else if (bcp_type == "SQLDATETIM4") {
				if (IsNullable(tc))
					out_.put((char)4);
				WriteSmallDateTime(TimestampValue::Get(vc.GetValue(r)));
			} else {
				throw NotImplementedException("Unsupported BCP type in BCP writer: " + bcp_type);
			}
		}
	}
}

void BCPWriter::WriteDecimal(const Value &v, int precision, int scale) {
	// For native DECIMAL, BCP writes:
	// [length (1 byte)] [sign (1 byte)] [packed digits little-endian base-100...]
	// Implementing a portable encoder is possible but verbose; for brevity we use DuckDB to string then pack.
	// Simplified: convert to scaled integer, then pack every 2 decimal digits per byte (LSB-first).
	auto s = v.ToString();
	bool neg = (s.size() && s[0] == '-');
	// remove sign and decimal point, pad to even digits
	string digits;
	for (auto ch : s)
		if (isdigit((unsigned char)ch))
			digits.push_back(ch);
	// ensure scale digits present
	// ... omitted: robust normalization to exact (p,s)
	if (digits.size() % 2)
		digits = "0" + digits;
	uint8_t len = (uint8_t)(1 + digits.size() / 2); // sign + bytes
	out_.put((char)len);
	out_.put((char)(neg ? 0x00 : 0x01));
	// pack two digits per byte, little-endian => write from least significant pair first
	for (int i = (int)digits.size() - 2; i >= 0; i -= 2) {
		uint8_t b = (uint8_t)((digits[i] - '0') + 10 * (digits[i + 1] - '0'));
		out_.put((char)b);
	}
}
