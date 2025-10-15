// bcp_extension.hpp
// Minimal static-extension shim for bcp extension

#pragma once

#include "duckdb.hpp"

namespace duckdb {

// Forward-declare the C entrypoint provided in src/bcp_extension.cpp
extern "C" {
DUCKDB_EXTENSION_API void bcp_init(duckdb::DatabaseInstance &db);
}

class BcpExtension : public Extension {
public:
	void Load(ExtensionLoader &loader) override {
		// Call the legacy C++ init that registers the copy function into the system catalog
		bcp_init(loader.GetDatabaseInstance());
	}

	std::string Name() override {
		return std::string("bcp");
	}

	std::string Version() const override {
		return std::string(DuckDB::LibraryVersion());
	}
};

} // namespace duckdb
