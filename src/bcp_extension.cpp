#include "duckdb.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/function/copy_function.hpp"             // CopyFunction API
#include "duckdb/parser/tableref/table_function_ref.hpp" // TableFunctionRef definition
#include "duckdb/common/string_util.hpp"                 // StringUtil helpers
#include "duckdb/catalog/catalog.hpp"                    // Catalog::GetSystemCatalog
#include "duckdb/parser/parsed_data/create_copy_function_info.hpp"
#include "duckdb/execution/execution_context.hpp" // ExecutionContext for sink signature
#include "duckdb/function/copy_function.hpp"      // CopyFunctionInput
#include "bcp_format_utils.hpp"                   // shared BCP format helpers
#include "bcp_writer.hpp"                         // BCPWriter
#include "bcp_reader.hpp"                         // BCP reader logic

#include <string>
#include <vector>
#include <memory>
#include <fstream>
#include <sstream>

using namespace duckdb;

// ---------- Options helpers (DuckDB v1.4+: options = map<string, vector<Value>>) ----------
using options_map_t = duckdb::case_insensitive_map_t<duckdb::vector<duckdb::Value>>;

namespace {
// Trivial local sink state for DuckDB COPY API
static unique_ptr<LocalFunctionData> BCPWriteLocalSink(ExecutionContext &, FunctionData &) {
	return make_uniq<LocalFunctionData>();
}

// ---------- FunctionData for binding ----------
struct BCPBindData : public FunctionData {
	std::string path;               // output .bcp file path
	std::vector<BCPCol> cols; // must match BCPWriter::WriteChunk signature
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

// ---------- Bind: generate BCPCol from DuckDB output ----------
static unique_ptr<FunctionData> BCPWriteBind(ClientContext & /*context*/, CopyFunctionBindInput &input,
                                             const duckdb::vector<std::string> &names,
                                             const duckdb::vector<LogicalType> &sql_types) {
	const auto &ci = input.info; // CopyInfo
	auto bind = make_uniq<BCPBindData>();
	bind->path = ci.file_path;

	// Check for .fmt file option
	auto fmt_path = GetOption(ci.options, "FORMAT_FILE", "");
	std::vector<BCPCol> fmt_cols;
	if (!fmt_path.empty()) {
		fmt_cols = ParseBCPFmtFile(fmt_path);
	}

	if (!fmt_cols.empty() && fmt_cols.size() == names.size()) {
		// Use .fmt file metadata (already BCP types)
		for (idx_t i = 0; i < names.size(); i++) {
			BCPCol col;
			col.name = names[i];
			col.sql_type = fmt_cols[i].sql_type;
			col.length = fmt_cols[i].length;
			col.nullable = fmt_cols[i].nullable;
			bind->cols.push_back(std::move(col));
		}
	} else {
		// Fallback: map from DuckDB types to BCP types
		for (idx_t i = 0; i < names.size(); i++) {
			BCPCol col;
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
	std::vector<BCPCol> cols; // std::vector to match BCPWriter::WriteChunk
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

	// Bind & pipeline hooks for COPY ... FROM (core API and TableFunction)
	fun.copy_from_bind = BCPReadBind;
	fun.copy_from_function = TableFunction({LogicalType::VARCHAR}, // arguments: file path
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
