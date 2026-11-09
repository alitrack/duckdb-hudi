//! Rust side of the DuckDB Apache Hudi extension.
//!
//! `kernel` drives the hudi-rs kernel: it opens tables, plans reads (file slices
//! after partition and file pruning with the pushed-down filters), streams each
//! slice's merged rows, and builds the metadata functions' rows. `ffi` is the C
//! ABI the C++ extension calls; Arrow data crosses it as C Data Interface
//! structs, so the C++ side converts batches with DuckDB's own Arrow reader.
//! `duckdb_io` is an object store that reads a table's files through DuckDB's
//! file system, and with it DuckDB's external file cache.

pub mod duckdb_io;
pub mod ffi;
pub mod kernel;
