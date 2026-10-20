#pragma once

#include "duckdb.hpp"

namespace duckdb {

// hudi_info(uri) and hudi_files(uri).
vector<ScalarFunction> HudiScalarFunctions();
// hudi_mdt(uri), hudi_commits(uri) and hudi_stats(uri).
vector<TableFunction> HudiMetadataFunctions();

} // namespace duckdb
