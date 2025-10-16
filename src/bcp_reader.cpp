#include "duckdb.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/function/copy_function.hpp"             // CopyFunction API
#include "duckdb/parser/tableref/table_function_ref.hpp" // TableFunctionRef definition
#include "duckdb/common/string_util.hpp"                 // StringUtil helpers
#include "duckdb/catalog/catalog.hpp"                    // Catalog::GetSystemCatalog
#include "duckdb/parser/parsed_data/create_copy_function_info.hpp"
#include "duckdb/execution/execution_context.hpp" // ExecutionContext for sink signature
#include "duckdb/function/copy_function.hpp"      // CopyFunctionInput
#include "bcp_writer.hpp"                         // <-- defines BCPTargetCol & BCPWriter
#include "bcp_format_utils.hpp"                   // shared BCP format helpers

#include <fstream>
#include <memory>
#include <cstring>

using namespace duckdb;

// extern "C" {

struct BCPTableFunctionData : public TableFunctionData {
	std::string path;
	std::vector<BCPTargetCol> cols;
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
		size_t row_len = 0;
		for (auto &col : data.cols)
			row_len += col.length;
		if (row_len == 0)
			break;
		state.row_buffer.resize(row_len);
		state.file->read(state.row_buffer.data(), row_len);
		std::streamsize n = state.file->gcount();
		if (n != (std::streamsize)row_len) {
			state.eof = true;
			break;
		}
		size_t offset = 0;
		for (idx_t col_idx = 0; col_idx < data.cols.size(); col_idx++) {
			auto &col = data.cols[col_idx];
			std::string val(state.row_buffer.data() + offset, col.length);
			val.erase(val.find_last_not_of(" \0") + 1);
			output.SetValue(col_idx, rows_read, Value(val));
			offset += col.length;
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

// } // extern "C"
