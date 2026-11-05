#define DUCKDB_EXTENSION_MAIN

#include "hudi_extension.hpp"

#include "hudi_metadata.hpp"
#include "hudi_scan.hpp"
#include "hudi_write.hpp"

#include "duckdb/main/config.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

static void LoadInternal(ExtensionLoader &loader) {
	loader.RegisterFunction(HudiReadFunction());
	loader.RegisterFunction(HudiWriteFunction());
	loader.RegisterFunction(HudiCreateFunction());
	loader.RegisterFunction(HudiDeleteFunction());
	for (auto &function : HudiMetadataFunctions()) {
		loader.RegisterFunction(function);
	}
	for (auto &function : HudiScalarFunctions()) {
		loader.RegisterFunction(function);
	}
	auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
	config.AddExtensionOption("hudi_row_estimate",
	                          "Report the kernel's row count of the planned file slices as the estimated "
	                          "cardinality of a read_hudi scan (the count is size-based unless the metadata "
	                          "table records it)",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(false));
}

void HudiExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string HudiExtension::Name() {
	return "hudi";
}

std::string HudiExtension::Version() const {
#ifdef EXT_VERSION_HUDI
	return EXT_VERSION_HUDI;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(hudi, loader) {
	duckdb::LoadInternal(loader);
}
}
