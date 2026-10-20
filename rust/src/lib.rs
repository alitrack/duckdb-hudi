//! Rust side of the DuckDB Apache Hudi extension.
//!
//! `kernel` drives the hudi-rs kernel: it opens tables, plans reads (file slices
//! after partition and file pruning with the pushed-down filters), streams each
//! slice's merged rows, and builds the metadata functions' rows. `ffi` is the C
//! ABI the C++ extension calls; Arrow data crosses it as C Data Interface
//! structs, so the C++ side converts batches with DuckDB's own Arrow reader.

pub mod ffi;
pub mod kernel;
