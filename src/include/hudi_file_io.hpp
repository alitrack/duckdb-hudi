#pragma once

#include "duckdb.hpp"
#include "hudi_ffi.hpp"

namespace duckdb {

// Whether read_hudi reads the table at `uri` through DuckDB's file system:
// `hudi_duckdb_file_system` is on, `hudi_options` sets no storage option, the
// scheme is s3, s3a, gs, gcs, az or abfss with its extension loaded, and a
// DuckDB secret matches `uri`. The undocumented `hudi_duckdb_file_system_local`
// also routes local paths, for tests.
bool HudiUseDuckDBFileSystem(ClientContext &context, const string &uri, const string &hudi_options);

// Opens the table at `uri`, reading its files through DuckDB's caching file
// system on `context`'s connection. Listing stays with hudi-rs.
shared_ptr<HudiTableHandle> HudiOpenWithDuckDBFileSystem(ClientContext &context, const string &uri,
                                                         const string &hudi_options);

} // namespace duckdb
