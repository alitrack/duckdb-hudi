// Converts the batches of one Arrow stream produced by the Rust library into
// DuckDB vectors, one vector's worth of rows at a time, with DuckDB's own Arrow
// reader. Shared by read_hudi and the metadata table functions.
#pragma once

#include "duckdb.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/function/table/arrow.hpp"

namespace duckdb {

class HudiArrowReader {
public:
	// `types` are the DuckDB types every opened stream must map to, column by
	// column; a stream may carry them in any Arrow encoding (a string as a
	// view or a dictionary, a decimal in 64 bits, for example).
	HudiArrowReader(ClientContext &context, vector<LogicalType> types);

	// Takes ownership of a stream the Rust library wrote into `raw` and reads
	// its schema, which drives the conversion of its batches.
	void Open(unique_ptr<ArrowArrayStreamWrapper> stream);
	void Close();
	bool IsOpen() const {
		return stream != nullptr;
	}

	// Converts up to STANDARD_VECTOR_SIZE rows of the stream into `converted`,
	// whose columns are the stream's columns. Returns the row count, 0 once the
	// stream is exhausted.
	idx_t Next(DataChunk &converted);

	idx_t batches_read = 0;

private:
	vector<LogicalType> types;
	unique_ptr<ArrowArrayStreamWrapper> stream;
	ArrowTableSchema stream_schema;
	ArrowScanLocalState state;
};

} // namespace duckdb
