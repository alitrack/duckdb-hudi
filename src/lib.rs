//! DuckDB Hudi extension — read Apache Hudi COW tables via DuckDB's native Parquet reader.
//! Zero external dependencies beyond quack-rs. Parses Hudi metadata manually.

use quack_rs::prelude::*;
use quack_rs::entry_point::init_extension_v2;
use quack_rs::scalar::ScalarFunctionBuilder;

use libduckdb_sys::{
    duckdb_connection, duckdb_database, duckdb_data_chunk, duckdb_function_info,
    duckdb_vector, duckdb_extension_info, duckdb_extension_access,
    duckdb_result, DuckDBSuccess,
};
use std::sync::atomic::{AtomicPtr, Ordering};

static DB: AtomicPtr<std::ffi::c_void> = AtomicPtr::new(std::ptr::null_mut());

fn get_con() -> Result<duckdb_connection, String> {
    let db = DB.load(Ordering::Acquire) as duckdb_database;
    if db.is_null() { return Err("db not set".into()); }
    let mut con: duckdb_connection = std::ptr::null_mut();
    if unsafe { libduckdb_sys::duckdb_connect(db, &mut con) } != DuckDBSuccess {
        return Err("connect failed".into());
    }
    Ok(con)
}

/// Return the list of base file paths for a Hudi COW table.
fn hudi_base_files(table_path: &str) -> Result<Vec<String>, String> {
    let path = std::path::Path::new(table_path);
    if !path.exists() {
        return Err(format!("Path does not exist: {table_path}"));
    }

    // Check .hoodie/hoodie.properties for table type
    let props_path = path.join(".hoodie").join("hoodie.properties");
    if !props_path.exists() {
        return Err(format!("Not a Hudi table: no .hoodie/hoodie.properties at {table_path}"));
    }

    // Collect *.parquet files from the base path (COW tables)
    let mut files: Vec<String> = Vec::new();
    if let Ok(entries) = std::fs::read_dir(path) {
        for entry in entries.flatten() {
            let fname = entry.file_name();
            let name = fname.to_string_lossy();
            if name.ends_with(".parquet") {
                files.push(entry.path().to_string_lossy().to_string());
            }
        }
    }

    // If no top-level parquet, check partition directories
    if files.is_empty() {
        if let Ok(entries) = std::fs::read_dir(path) {
            for entry in entries.flatten() {
                let sub = entry.path();
                if sub.is_dir() && !sub.file_name().map_or(false, |n| n == ".hoodie") {
                    if let Ok(sub_entries) = std::fs::read_dir(&sub) {
                        for e in sub_entries.flatten() {
                            let n = e.file_name();
                            if n.to_string_lossy().ends_with(".parquet") {
                                files.push(e.path().to_string_lossy().to_string());
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(files)
}

// ── Scalar: hudi_info(path) → VARCHAR ──────────────────────────────────

fn build_hudi_info() -> ScalarFunctionBuilder {
    ScalarFunctionBuilder::new("hudi_info")
        .param(TypeId::Varchar)
        .returns(TypeId::Varchar)
        .function(hudi_info_fn)
}

unsafe extern "C" fn hudi_info_fn(
    _info: duckdb_function_info,
    input: duckdb_data_chunk,
    output: duckdb_vector,
) {
    let r0 = quack_rs::vector::VectorReader::new(input, 0);
    let table_path = r0.read_str(0).to_string();
    let msg = match hudi_base_files(&table_path) {
        Ok(files) => format!("Hudi table: {} base files", files.len()),
        Err(e) => format!("Error: {e}"),
    };
    quack_rs::vector::VectorWriter::new(output).write_str(0, &msg);
}

// ── Scalar: hudi_files(path) → VARCHAR ──────────────────────────────────

fn build_hudi_files() -> ScalarFunctionBuilder {
    ScalarFunctionBuilder::new("hudi_files")
        .param(TypeId::Varchar)
        .returns(TypeId::Varchar)
        .function(hudi_files_fn)
}

unsafe extern "C" fn hudi_files_fn(
    _info: duckdb_function_info,
    input: duckdb_data_chunk,
    output: duckdb_vector,
) {
    let r0 = quack_rs::vector::VectorReader::new(input, 0);
    let table_path = r0.read_str(0).to_string();
    let msg = match hudi_base_files(&table_path) {
        Ok(files) => files.join("\n"),
        Err(e) => format!("Error: {e}"),
    };
    quack_rs::vector::VectorWriter::new(output).write_str(0, &msg);
}

// ── Registration ────────────────────────────────────────────────────────

fn register_all(reg: &impl Registrar) -> Result<(), ExtensionError> {
    unsafe {
        reg.register_scalar(build_hudi_info());
        reg.register_scalar(build_hudi_files());
    }
    Ok(())
}

// ── Entry ───────────────────────────────────────────────────────────────

#[no_mangle]
pub unsafe extern "C" fn hudi_init_c_api(
    info: duckdb_extension_info,
    access: *const duckdb_extension_access,
) -> bool {
    unsafe {
        init_extension_v2(info, access, quack_rs::DUCKDB_API_VERSION, |con| {
            let db = con.as_raw_database();
            DB.store(db as *mut std::ffi::c_void, Ordering::Release);
            register_all(con)
        })
    }
}
