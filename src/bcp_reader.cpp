#include "duckdb.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/function/copy_function.hpp"             // CopyFunction API
#include "duckdb/parser/tableref/table_function_ref.hpp" // TableFunctionRef definition
#include "duckdb/common/string_util.hpp"                 // StringUtil helpers
#include "duckdb/catalog/catalog.hpp"                    // Catalog::GetSystemCatalog
#include "duckdb/parser/parsed_data/create_copy_function_info.hpp"
#include "duckdb/execution/execution_context.hpp" // ExecutionContext for sink signature
#include "duckdb/function/copy_function.hpp"      // CopyFunctionInput
#include "bcp_writer.hpp"                         // BCPWriter
#include "bcp_format_utils.hpp"                   // shared BCP format helpers

#include <fstream>
#include <memory>
#include <cstring>

using namespace duckdb;

struct BCPTableFunctionData : public TableFunctionData {
	std::string path;
	std::vector<BCPCol> cols;
	idx_t row_idx = 0;
};

struct BCPTableGlobalState : public GlobalTableFunctionState {
	std::unique_ptr<std::ifstream> file;
	std::vector<char> row_buffer;
	bool eof = false;
	idx_t row_idx = 0;
};

unique_ptr<FunctionData> BCPTableBind(ClientContext &context, TableFunctionBindInput &input,
                                      duckdb::vector<duckdb::LogicalType> &return_types,
                                      duckdb::vector<duckdb::string> &names) {
	std::string fmt_path;
	auto it = input.named_parameters.find("FORMAT_FILE");
	if (it != input.named_parameters.end()) {
		fmt_path = it->second.ToString();
	}
	if (fmt_path.empty()) {
		throw InvalidInputException("FORMAT_FILE option is required for BCP import");
	}
	auto fmt_cols = ParseBCPFmtFile(fmt_path);
	if (fmt_cols.empty()) {
		throw InvalidInputException("Failed to parse BCP format file: " + fmt_path);
	}
	names.clear();
	return_types.clear();
	for (auto &col : fmt_cols) {
		names.push_back(col.name);
		return_types.push_back(MapBCPTypeToDuckDBType(col.sql_type, col.length));
	}
	auto bind = make_uniq<BCPTableFunctionData>();
	bind->path = input.inputs[0].ToString();
	bind->cols = fmt_cols;
	return std::move(bind);
}

unique_ptr<FunctionData> BCPReadBind(ClientContext &context, CopyFromFunctionBindInput &info,
                                     duckdb::vector<duckdb::string> &expected_names,
                                     duckdb::vector<duckdb::LogicalType> &expected_types) {
	auto inputs = duckdb::vector<duckdb::Value> {duckdb::Value(info.info.file_path)};
	auto &options = info.info.options;
	duckdb::named_parameter_map_t named_parameters;
	for (auto &kv : options) {
		named_parameters[kv.first] = kv.second.back();
	}
	duckdb::vector<duckdb::LogicalType> dummy_types;
	duckdb::vector<duckdb::string> dummy_names;
	duckdb::TableFunction dummy_function;
	duckdb::optional_ptr<duckdb::TableFunctionInfo> info_ptr = nullptr;
	duckdb::optional_ptr<duckdb::Binder> binder_ptr = nullptr;
	static duckdb::TableFunctionRef dummy_ref;
	duckdb::TableFunctionBindInput tf_input(inputs, named_parameters, dummy_types, dummy_names, info_ptr, binder_ptr,
	                                        dummy_function, dummy_ref);
	duckdb::vector<duckdb::LogicalType> return_types;
	duckdb::vector<duckdb::string> names;
	auto bind_data = BCPTableBind(context, tf_input, return_types, names);
	expected_names = names;
	expected_types = return_types;
	return bind_data;
}

void BCPTableRead(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	const auto &data = input.bind_data->Cast<BCPTableFunctionData>();
	auto &state = input.global_state->Cast<BCPTableGlobalState>();
	const idx_t max_rows = STANDARD_VECTOR_SIZE;
	if (!state.file) {
		state.file = duckdb::make_uniq<std::ifstream>(data.path, std::ios::binary);
		if (!state.file->is_open()) {
			throw IOException("Could not open BCP file: " + data.path);
		}
	}
	if (state.eof) {
		output.SetCardinality(0);
		return;
	}
	idx_t rows_read = 0;
	while (rows_read < max_rows && state.file && !state.file->eof()) {
		std::vector<duckdb::Value> row_values;
		bool row_eof = false;
		for (auto &col : data.cols) {
			bool is_null = false;
			int actual_len = col.length;
			std::vector<char> prefix_buf;
			if (col.prefix > 0) {
				prefix_buf.resize(col.prefix);
				state.file->read(prefix_buf.data(), col.prefix);
				if (state.file->gcount() != (std::streamsize)col.prefix) {
					row_eof = true;
					break;
				}
				if (col.prefix == 1) {
					uint8_t len = (uint8_t)prefix_buf[0];
					if (len == 0xFF) {
						is_null = true;
					} else {
						actual_len = len;
					}
				} else if (col.prefix == 2) {
					uint16_t len = (uint8_t)prefix_buf[0] | ((uint8_t)prefix_buf[1] << 8);
					if (len == 0xFFFF) {
						is_null = true;
					} else {
						actual_len = len;
					}
				} else if (col.prefix == 4) {
					uint32_t len = (uint8_t)prefix_buf[0] | ((uint8_t)prefix_buf[1] << 8) |
					               ((uint8_t)prefix_buf[2] << 16) | ((uint8_t)prefix_buf[3] << 24);
					if (len == 0xFFFFFFFF) {
						is_null = true;
					} else {
						actual_len = len;
					}
				}
			}
			std::vector<char> value_buf;
			if (!is_null) {
				value_buf.resize(actual_len);
				state.file->read(value_buf.data(), actual_len);
				if (state.file->gcount() != (std::streamsize)actual_len) {
					row_eof = true;
					break;
				}
			}
			duckdb::Value v;
			const char *value_data = value_buf.data();
			if (is_null) {
				v = duckdb::Value();
			} else if (col.sql_type == "SQLBIT") {
				v = duckdb::Value::BOOLEAN(value_data[0] != 0);
			} else if (col.sql_type == "SQLTINYINT") {
				v = duckdb::Value::TINYINT((int8_t)value_data[0]);
			} else if (col.sql_type == "SQLSMALLINT") {
				int16_t val = (uint8_t)value_data[0] | ((uint8_t)value_data[1] << 8);
				v = duckdb::Value::SMALLINT(val);
			} else if (col.sql_type == "SQLINT") {
				int32_t val = (uint8_t)value_data[0] | ((uint8_t)value_data[1] << 8) | ((uint8_t)value_data[2] << 16) |
				              ((uint8_t)value_data[3] << 24);
				v = duckdb::Value::INTEGER(val);
			} else if (col.sql_type == "SQLBIGINT") {
				int64_t val = 0;
				for (int i = 0; i < 8; i++)
					val |= ((int64_t)(uint8_t)value_data[i]) << (8 * i);
				v = duckdb::Value::BIGINT(val);
			} else if (col.sql_type == "SQLFLT4") {
				float f;
				memcpy(&f, value_data, 4);
				v = duckdb::Value::FLOAT(f);
			} else if (col.sql_type == "SQLFLT8") {
				double d;
				memcpy(&d, value_data, 8);
				v = duckdb::Value::DOUBLE(d);
			} else if (col.sql_type == "SQLCHAR" || col.sql_type == "SQLVARCHAR") {
				std::string s(value_data, actual_len);
				s.erase(s.find_last_not_of(" \0") + 1);
				v = duckdb::Value(s);
			} else if (col.sql_type == "SQLNCHAR" || col.sql_type == "SQLNVARCHAR") {
				std::string s;
				for (int i = 0; i + 1 < actual_len; i += 2) {
					uint16_t code_unit = ((uint8_t)value_data[i]) | ((uint8_t)value_data[i + 1] << 8);
					if (code_unit == 0)
						break;
					if (code_unit < 0x80) {
						s.push_back((char)code_unit);
					} else if (code_unit < 0x800) {
						s.push_back((char)(0xC0 | (code_unit >> 6)));
						s.push_back((char)(0x80 | (code_unit & 0x3F)));
					} else {
						s.push_back((char)(0xE0 | (code_unit >> 12)));
						s.push_back((char)(0x80 | ((code_unit >> 6) & 0x3F)));
						s.push_back((char)(0x80 | (code_unit & 0x3F)));
					}
				}
				s.erase(s.find_last_not_of(" \0") + 1);
				v = duckdb::Value(s);
			} else if (col.sql_type == "SQLVARBINARY" || col.sql_type == "SQLBINARY" || col.sql_type == "SQLIMAGE") {
				v = duckdb::Value::BLOB(reinterpret_cast<const duckdb::data_t *>(value_data), actual_len);
			} else if (col.sql_type == "SQLDATE") {
				int32_t days = (uint8_t)value_data[0] | ((uint8_t)value_data[1] << 8) | ((uint8_t)value_data[2] << 16);
				days -= 719162;
				v = duckdb::Value::DATE(duckdb::date_t(days));
			} else if (col.sql_type == "SQLTIME") {
				int64_t ticks = 0;
				for (int i = 0; i < 5; i++)
					ticks |= ((int64_t)(uint8_t)value_data[i]) << (8 * i);
				int64_t micros = ticks / 10;
				v = duckdb::Value::TIME(duckdb::dtime_t(micros));
			} else if (col.sql_type == "SQLDATETIME2") {
				int64_t ticks = 0;
				for (int i = 0; i < 5; i++)
					ticks |= ((int64_t)(uint8_t)value_data[i]) << (8 * i);
				int32_t days = (uint8_t)value_data[5] | ((uint8_t)value_data[6] << 8) | ((uint8_t)value_data[7] << 16);
				int64_t micros = ticks / 10;
				days -= 719162;
				duckdb::date_t d(days);
				duckdb::dtime_t t(micros);
				v = duckdb::Value::TIMESTAMP(duckdb::Timestamp::FromDatetime(d, t));
			} else if (col.sql_type == "SQLDATETIME") {
				int32_t days = (uint8_t)value_data[0] | ((uint8_t)value_data[1] << 8) | ((uint8_t)value_data[2] << 16) |
				               ((uint8_t)value_data[3] << 24);
				int32_t ticks = (uint8_t)value_data[4] | ((uint8_t)value_data[5] << 8) |
				                ((uint8_t)value_data[6] << 16) | ((uint8_t)value_data[7] << 24);
				int32_t days1970 = days - (719162 - 693596);
				double micros = ticks * (1000000.0 / 300.0);
				v = duckdb::Value::TIMESTAMP(
				    duckdb::Timestamp::FromDatetime(duckdb::date_t(days1970), duckdb::dtime_t((int64_t)micros)));
			} else if (col.sql_type == "SQLDATETIM4") {
				int32_t days = (uint8_t)value_data[0] | ((uint8_t)value_data[1] << 8);
				int32_t minutes = (uint8_t)value_data[2] | ((uint8_t)value_data[3] << 8);
				int32_t days1970 = days - (719162 - 693596);
				int64_t micros = minutes * 60 * 1000000LL;
				v = duckdb::Value::TIMESTAMP(
				    duckdb::Timestamp::FromDatetime(duckdb::date_t(days1970), duckdb::dtime_t(micros)));
			} else if (col.sql_type == "SQLUNIQUEID") {
				if (actual_len == 16) {
					duckdb::hugeint_t uuid;
					uuid.lower = 0;
					uuid.upper = 0;
					for (int i = 0; i < 8; i++)
						uuid.lower |= ((uint64_t)(uint8_t)value_data[i]) << (8 * i);
					for (int i = 0; i < 8; i++)
						uuid.upper |= ((uint64_t)(uint8_t)value_data[8 + i]) << (8 * i);
					v = duckdb::Value::UUID(uuid);
				} else {
					v = duckdb::Value();
				}
			} else {
				std::string s(value_data, actual_len);
				s.erase(s.find_last_not_of(" \0") + 1);
				v = duckdb::Value(s);
			}
			row_values.push_back(v);
		}
		if (row_eof) {
			state.eof = true;
			break;
		}
		for (idx_t col_idx = 0; col_idx < row_values.size(); col_idx++) {
			output.SetValue(col_idx, rows_read, row_values[col_idx]);
		}
		rows_read++;
	}
	output.SetCardinality(rows_read);
	if (rows_read == 0) {
		state.eof = true;
	}
}

unique_ptr<GlobalTableFunctionState> BCPTableInit(ClientContext &context, TableFunctionInitInput &input) {
	return duckdb::make_uniq<BCPTableGlobalState>();
}
