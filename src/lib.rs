//! DuckDB Hudi extension — read Apache Hudi COW tables with full timeline parsing.
//! Parses .hoodie metadata to discover committed Parquet files, then uses DuckDB's
//! native Parquet reader via duckdb_query().

use quack_rs::prelude::*;
use quack_rs::entry_point::init_extension_v2;
use quack_rs::scalar::ScalarFunctionBuilder;

use libduckdb_sys::{
    duckdb_connection, duckdb_database, duckdb_data_chunk, duckdb_function_info,
    duckdb_vector, duckdb_extension_info, duckdb_extension_access,
    duckdb_result, DuckDBSuccess,
};
use std::sync::atomic::{AtomicPtr, Ordering};

static DB_HANDLE: AtomicPtr<std::ffi::c_void> = AtomicPtr::new(std::ptr::null_mut());

fn get_con() -> Result<duckdb_connection, String> {
    let db = DB_HANDLE.load(Ordering::Acquire) as duckdb_database;
    if db.is_null() { return Err("db not set".into()); }
    let mut con: duckdb_connection = std::ptr::null_mut();
    if unsafe { libduckdb_sys::duckdb_connect(db, &mut con) } != DuckDBSuccess {
        return Err("connect failed".into());
    }
    Ok(con)
}

/// Parsed Hudi table metadata
struct HudiTable {
    /// Absolute paths to committed base Parquet files (latest version per file group)
    committed_files: Vec<String>,
    /// Hudi table type: "COPY_ON_WRITE" or "MERGE_ON_READ"
    table_type: String,
    /// Total partition count
    partition_count: usize,
    /// Total commit count in timeline
    commit_count: usize,
}

/// Parse the Hudi timeline and return committed base file paths.
fn get_hudi_committed_files(base_path: &str) -> Result<HudiTable, String> {
    let path = std::path::Path::new(base_path);
    let hoodie_dir = path.join(".hoodie");

    if !hoodie_dir.exists() {
        return Err(format!("Not a Hudi table: no .hoodie directory at {base_path}"));
    }

    // 1. Read hoodie.properties
    let props_path = hoodie_dir.join("hoodie.properties");
    let mut table_type = String::from("COPY_ON_WRITE");
    if let Ok(props) = std::fs::read_to_string(&props_path) {
        for line in props.lines() {
            if line.starts_with("hoodie.table.type=") {
                table_type = line.split('=').nth(1).unwrap_or("COPY_ON_WRITE").to_string();
            }
        }
    }

    if table_type == "MERGE_ON_READ" {
        return Err(format!(
            "MERGE_ON_READ tables not yet supported (found at {base_path}). Only COW tables are supported."
        ));
    }

    // 2. Parse timeline: collect all .commit files in reverse chronological order
    let timeline_dir = hoodie_dir;
    let mut commits: Vec<(String, String)> = Vec::new(); // (instant, filename)

    if let Ok(entries) = std::fs::read_dir(&timeline_dir) {
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().to_string();
            if !name.ends_with(".commit") && !name.ends_with(".deltacommit") {
                continue;
            }
            // Extract instant from filename: YYYYMMDDHHmmssSSS.commit
            let instant = name.trim_end_matches(".commit")
                             .trim_end_matches(".deltacommit")
                             .to_string();
            commits.push((instant, name));
        }
    }

    // Sort by instant descending (newest first)
    commits.sort_by(|a, b| b.0.cmp(&a.0));
    let commit_count = commits.len();

    if commit_count == 0 {
        return Err("No commits found in timeline".into());
    }

    // 3. For COW tables: read the latest commit metadata to get active file paths.
    let latest_commit_file = timeline_dir.join(&commits[0].1);
    let content = std::fs::read_to_string(&latest_commit_file)
        .map_err(|e| format!("Cannot read commit file: {e}"))?;

    let mut committed_files: Vec<String> = Vec::new();
    let mut seen_file_groups = std::collections::HashSet::new();

    // Parse commit JSON to extract file paths.
    if let Ok(commit_json) = serde_json::from_str::<serde_json::Value>(&content) {
        // Try multiple metadata locations in the commit structure
        let metadata = commit_json
            .get("extraMetadata")
            .or_else(|| commit_json.get("commitMetadata"))
            .or_else(|| commit_json.get("metadata"));

        if let Some(meta) = metadata {
            if let Some(files) = meta.get("partitionToWriteStats").or_else(|| meta.get("fileStats")) {
                if let Some(map) = files.as_object() {
                    for (_partition, stats_list) in map {
                        if let Some(stats_array) = stats_list.as_array() {
                            for stat in stats_array {
                                let file_name = stat.get("fileName")
                                    .or_else(|| stat.get("fileName"))
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("");
                                if file_name.is_empty() { continue; }

                                // Extract file group from Hudi naming convention:
                                // <file_id>_<write_token>_<instant>.parquet
                                let file_id = file_name.split('_').next().unwrap_or(file_name);

                                // Keep only the latest version per file group
                                if !seen_file_groups.contains(file_id) {
                                    seen_file_groups.insert(file_id.to_string());
                                    let full_path = std::path::Path::new(base_path)
                                        .join(file_name)
                                        .to_string_lossy()
                                        .to_string();
                                    committed_files.push(full_path);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    // 4. Fallback: if metadata parsing failed, scan for .parquet files and
    //    deduplicate by file group preserving latest commit.
    if committed_files.is_empty() {
        let mut all_parquet: Vec<(String, String)> = Vec::new(); // (file_id, full_path)
        collect_parquet_files_recursive(path, &mut all_parquet)?;

        // Sort by file name (instant is in the name), keep latest
        all_parquet.sort_by(|a, b| b.0.cmp(&a.0));
        for (file_id, full_path) in &all_parquet {
            let base_id = file_id.split('_').next().unwrap_or(file_id);
            if !seen_file_groups.contains(base_id) {
                seen_file_groups.insert(base_id.to_string());
                committed_files.push(full_path.clone());
            }
        }
    }

    Ok(HudiTable {
        committed_files,
        table_type,
        partition_count: seen_file_groups.len(),
        commit_count,
    })
}

/// Recursively collect .parquet files from a directory tree.
fn collect_parquet_files_recursive(
    dir: &std::path::Path,
    result: &mut Vec<(String, String)>,
) -> Result<(), String> {
    if !dir.is_dir() { return Ok(()); }
    for entry in std::fs::read_dir(dir).map_err(|e| e.to_string())? {
        let entry = entry.map_err(|e| e.to_string())?;
        let p = entry.path();
        if p.is_dir() {
            // Skip .hoodie directory
            if p.file_name().map_or(false, |n| n == ".hoodie") {
                continue;
            }
            collect_parquet_files_recursive(&p, result)?;
        } else if p.extension().map_or(false, |ext| ext == "parquet") {
            let name = p.file_name().unwrap_or_default().to_string_lossy().to_string();
            let full = p.to_string_lossy().to_string();
            result.push((name, full));
        }
    }
    Ok(())
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
    let msg = match get_hudi_committed_files(&table_path) {
        Ok(t) => format!(
            "Hudi table: {} files, {} commits, {} file groups, type={}",
            t.committed_files.len(), t.commit_count, t.partition_count, t.table_type
        ),
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
    let msg = match get_hudi_committed_files(&table_path) {
        Ok(t) => t.committed_files.join("\n"),
        Err(e) => format!("Error: {e}"),
    };
    quack_rs::vector::VectorWriter::new(output).write_str(0, &msg);
}

// ── Scalar: hudi_scan_sql(path) → VARCHAR ───────────────────────────────

fn build_hudi_scan_sql() -> ScalarFunctionBuilder {
    ScalarFunctionBuilder::new("hudi_scan_sql")
        .param(TypeId::Varchar)
        .returns(TypeId::Varchar)
        .function(hudi_scan_sql_fn)
}

unsafe extern "C" fn hudi_scan_sql_fn(
    _info: duckdb_function_info,
    input: duckdb_data_chunk,
    output: duckdb_vector,
) {
    let r0 = quack_rs::vector::VectorReader::new(input, 0);
    let table_path = r0.read_str(0).to_string();
    let msg = match get_hudi_committed_files(&table_path) {
        Ok(t) => {
            if t.committed_files.is_empty() {
                "No committed files found".to_string()
            } else {
                let file_list: Vec<String> = t.committed_files.iter()
                    .map(|f| format!("'{}'", f.replace('\'', "''")))
                    .collect();
                format!(
                    "SELECT * FROM read_parquet([{}])",
                    file_list.join(", ")
                )
            }
        }
        Err(e) => format!("Error: {e}"),
    };
    quack_rs::vector::VectorWriter::new(output).write_str(0, &msg);
}

// ── Registration ────────────────────────────────────────────────────────

fn register_all(reg: &impl Registrar) -> Result<(), ExtensionError> {
    unsafe {
        reg.register_scalar(build_hudi_info());
        reg.register_scalar(build_hudi_files());
        reg.register_scalar(build_hudi_scan_sql());
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
            DB_HANDLE.store(db as *mut std::ffi::c_void, Ordering::Release);
            register_all(con)
        })
    }
}
