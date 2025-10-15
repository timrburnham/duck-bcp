#include "duckdb.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/function/copy_function.hpp" // CopyFunction API
#include "duckdb/common/string_util.hpp"     // StringUtil helpers
#include "duckdb/catalog/catalog.hpp"        // Catalog::GetSystemCatalog
#include "duckdb/parser/parsed_data/create_copy_function_info.hpp"
#include "duckdb/execution/execution_context.hpp" // ExecutionContext for sink signature
#include "duckdb/function/copy_function.hpp"      // CopyFunctionInput
#include "bcp_writer.hpp"                         // <-- defines BCPTargetCol & BCPWriter
#include "bcp_format_utils.hpp"                   // shared BCP format helpers

#include <string>
#include <vector>
#include <memory>
#include <fstream>
#include <sstream>

using namespace duckdb;

// ---------- Options helpers (DuckDB v1.4+: options = map<string, vector<Value>>) ----------
using options_map_t = duckdb::case_insensitive_map_t<duckdb::vector<duckdb::Value>>;


// ...existing code...

// Forward declarations for helpers
static std::string GetOption(const options_map_t &opts, const std::string &k, const std::string &def = "");
// Removed BCPFmtCol; use BCPTargetCol everywhere
// static std::vector<BCPTargetCol> ParseBCPFmtFile(const std::string &fmt_path);

// --- BCP TableFunction for COPY FROM ---
struct BCPTableFunctionData : public TableFunctionData {
	std::string path;
	std::vector<BCPTargetCol> cols;
	idx_t row_idx = 0;
	// Add any state needed for reading
};

static void BCPTableRead(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	// TODO: Implement reading BCP file row by row, decode fields, and fill output chunk
	// Use input.bind_data to get file path and column info
	// Use output.SetCardinality() and output.SetValue(col, row, value)
	output.SetCardinality(0); // no rows (skeleton)
}

static unique_ptr<FunctionData> BCPTableBind(ClientContext &context, TableFunctionBindInput &input,
								 vector<LogicalType> &return_types, vector<string> &names) {
	// Expect FORMAT_FILE option
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
	bind->path = input.inputs[0].ToString(); // first parameter is file path
	bind->cols = fmt_cols;
	return std::move(bind);
}

static unique_ptr<GlobalTableFunctionState> BCPTableInit(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<BCPTableFunctionData>();
	// Open file, prepare state, etc. (not implemented)
	return make_uniq<GlobalTableFunctionState>();
}





namespace {
// Trivial local sink state for DuckDB COPY API
static unique_ptr<LocalFunctionData> BCPWriteLocalSink(ExecutionContext &, FunctionData &) {
	return make_uniq<LocalFunctionData>();
}

// ---------- FunctionData for binding ----------
struct BCPBindData : public FunctionData {
	std::string path;               // output .bcp file path
	std::vector<BCPTargetCol> cols; // must match BCPWriter::WriteChunk signature
	bool unicode_native = false;    // write NVARCHAR (for bcp -N) for char types

	unique_ptr<FunctionData> Copy() const override {
		auto res = make_uniq<BCPBindData>();
		res->path = path;
		res->cols = cols;
		res->unicode_native = unicode_native;
		return std::move(res);
	}
	bool Equals(const FunctionData &other_p) const override {
		auto &o = (const BCPBindData &)other_p;
		return path == o.path && cols.size() == o.cols.size() && unicode_native == o.unicode_native;
	}
};

static const duckdb::Value *FindLastOption(const options_map_t &opts, const std::string &k) {
	auto it = opts.find(k);
	if (it == opts.end() || it->second.empty())
		return nullptr;
	return &it->second.back();
}

static std::string GetOption(const options_map_t &opts, const std::string &k, const std::string &def = "") {
	auto v = FindLastOption(opts, k);
	if (!v)
		return def;
	auto s = v->ToString();
	return s.empty() ? def : s;
}

// ...existing code...

// Restore GetBoolOption helper
static bool GetBoolOption(const options_map_t &opts, const std::string &k, bool def = false) {
	auto v = FindLastOption(opts, k);
	if (!v)
		return def;
	auto s = StringUtil::Lower(v->ToString());
	if (s == "true" || s == "1" || s == "yes" || s == "on")
		return true;
	if (s == "false" || s == "0" || s == "no" || s == "off")
		return false;
	return def;
}

// ---------- Bind: generate BCPTargetCol from DuckDB output ----------
static unique_ptr<FunctionData> BCPWriteBind(ClientContext & /*context*/, CopyFunctionBindInput &input,
                                        const duckdb::vector<std::string> &names,
                                        const duckdb::vector<LogicalType> &sql_types) {
	const auto &ci = input.info; // CopyInfo
	auto bind = make_uniq<BCPBindData>();
	bind->path = ci.file_path;

	// Check for .fmt file option
	auto fmt_path = GetOption(ci.options, "FORMAT_FILE", "");
	std::vector<BCPTargetCol> fmt_cols;
	if (!fmt_path.empty()) {
		fmt_cols = ParseBCPFmtFile(fmt_path);
	}

	if (!fmt_cols.empty() && fmt_cols.size() == names.size()) {
		// Use .fmt file metadata (already BCP types)
		for (idx_t i = 0; i < names.size(); i++) {
			BCPTargetCol col;
			col.name = names[i];
			col.sql_type = fmt_cols[i].sql_type;
			col.length = fmt_cols[i].length;
			col.nullable = fmt_cols[i].nullable;
			bind->cols.push_back(std::move(col));
		}
	} else {
		// Fallback: map from DuckDB types to BCP types
		for (idx_t i = 0; i < names.size(); i++) {
			BCPTargetCol col;
			col.name = names[i];
			col.sql_type = MapDuckDBTypeToBCPType(sql_types[i]);
			col.nullable = true; // Could inspect nullability from DuckDB if needed
			bind->cols.push_back(std::move(col));
		}
	}

	bind->unicode_native = GetBoolOption(ci.options, "UNICODE_NATIVE", false);
	return std::move(bind);
}

// ---------- Global writer state ----------
struct BCPGlobalState : public GlobalFunctionData {
	unique_ptr<BCPWriter> writer;
	std::vector<BCPTargetCol> cols; // std::vector to match BCPWriter::WriteChunk
	bool initialized = false;
};

// Initialize writer once per COPY command
static unique_ptr<GlobalFunctionData> BCPWriteInitGlobal(ClientContext & /*context*/, FunctionData &bind_p,
                                                    const std::string & /*file_path*/) {
	auto &bind = (BCPBindData &)bind_p;
	auto gs = make_uniq<BCPGlobalState>();
	gs->cols = bind.cols;
	gs->writer = make_uniq<BCPWriter>(bind.path, bind.cols, bind.unicode_native);
	gs->initialized = true;
	return std::move(gs);
}

// Stream chunks into writer (new signature: ExecutionContext&)
static void BCPWriteSink(ExecutionContext & /*context*/, FunctionData &bind_p, GlobalFunctionData &gstate_p,
                    LocalFunctionData & /*lstate*/, DataChunk &input) {
	auto &bind = (BCPBindData &)bind_p;
	auto &gs = (BCPGlobalState &)gstate_p;
	gs.writer->WriteChunk(input, bind.cols); // now types match exactly
}

// Finalize writer (flush, emit XML format if requested)
static void BCPWriteFinish(ClientContext & /*context*/, FunctionData & /*bind_p*/, GlobalFunctionData &gstate_p) {
	auto &gs = (BCPGlobalState &)gstate_p;
	gs.writer->Finish();
}

} // namespace

// ---------- Extension entry points ----------
extern "C" {


DUCKDB_EXTENSION_API void bcp_init(duckdb::DatabaseInstance &db) {
	CopyFunction fun("bcp");
	fun.extension = "bcp";

	// Bind & pipeline hooks for COPY ... TO
	fun.copy_to_bind = BCPWriteBind;
	fun.copy_to_initialize_global = BCPWriteInitGlobal;
	fun.copy_to_sink = BCPWriteSink;
	fun.copy_to_finalize = BCPWriteFinish;
	fun.copy_to_initialize_local = BCPWriteLocalSink;

	// Bind & pipeline hooks for COPY ... FROM (TableFunction approach)
	fun.copy_from_function = TableFunction(
		{LogicalType::VARCHAR}, // arguments: file path
		BCPTableRead,           // main function
		BCPTableBind,           // bind function
		BCPTableInit            // init_global function
	);
	fun.copy_from_function.named_parameters["FORMAT_FILE"] = LogicalType::VARCHAR;

	CreateCopyFunctionInfo info(std::move(fun));
	auto &catalog = Catalog::GetSystemCatalog(db);
	auto transaction = CatalogTransaction::GetSystemTransaction(db);
	catalog.CreateCopyFunction(transaction, info);
}

DUCKDB_EXTENSION_API const char *bcp_version() {
	return DuckDB::LibraryVersion();
}

// Generic DuckDB extension symbols
DUCKDB_EXTENSION_API void duckdb_extension_init(duckdb::DatabaseInstance &db) {
	bcp_init(db);
}

DUCKDB_EXTENSION_API const char *duckdb_extension_version() {
	return DuckDB::LibraryVersion();
}

} // extern "C"
