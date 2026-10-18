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
	// `column_ids` are the table column ids of the stream's columns, in stream
	// order: the reader looks their Arrow types up in the table schema.
	HudiArrowReader(ClientContext &context, vector<column_t> column_ids);

	// Takes ownership of a stream the Rust library wrote into `raw`.
	void Open(unique_ptr<ArrowArrayStreamWrapper> stream);
	void Close();
	bool IsOpen() const {
		return stream != nullptr;
	}

	// Converts up to STANDARD_VECTOR_SIZE rows of the stream into `converted`,
	// whose columns are the stream's columns. Returns the row count, 0 once the
	// stream is exhausted.
	idx_t Next(const arrow_column_map_t &arrow_columns, DataChunk &converted);

	idx_t batches_read = 0;

private:
	unique_ptr<ArrowArrayStreamWrapper> stream;
	ArrowScanLocalState state;
};

} // namespace duckdb
