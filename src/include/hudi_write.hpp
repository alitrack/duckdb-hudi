// Write side of the extension: hudi_write (table-in-table-out function that
// takes a subquery and commits it as one Hudi append), hudi_create and
// hudi_delete (single-row table functions).
#pragma once

#include "duckdb.hpp"

namespace duckdb {

TableFunction HudiWriteFunction();
TableFunction HudiCreateFunction();
TableFunction HudiDeleteFunction();

} // namespace duckdb
