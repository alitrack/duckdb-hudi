//! DuckDB Apache Hudi extension — P0: read Hudi COW/MOR tables via the hudi-rs 0.5.0 kernel.
//!
//! # Kernel
//! The `hudi` crate (hudi-rs) parses the timeline, discovers file slices, resolves the
//! table schema, and performs COW/MOR merging natively in Rust. This extension only
//! drives the kernel (one `block_on` in `bind`) and bridges its Arrow output into
//! DuckDB; it contains no timeline/parquet logic of its own.
//!
//! # Data path (all Arrow→DuckDB conversion happens in `bind`)
//! Each non-empty hudi `RecordBatch` (arrow 58):
//!   1. wrapped as a struct `ArrayRef` and exported over the Arrow C Data Interface
//!      (`arrow::ffi::export_array_into_raw`) into `FFI_ArrowArray`/`FFI_ArrowSchema`
//!      — the same `#[repr(C)]` layout libduckdb-sys declares (pointer cast);
//!   2. imported on a process-lifetime *global* `OwnedConnection` (opened inside the
//!      entry point, kept alive in a `static`) via the raw C-API
//!      `duckdb_schema_from_arrow` + `duckdb_data_chunk_from_arrow`;
//!   3. in `scan`, each imported chunk's column vectors are aliased into DuckDB's
//!      `output` chunk with `duckdb_vector_reference_vector` (no payload copy);
//!   4. the imported chunks stay alive in the bind data (shared via `Arc`) until the
//!      last scan state drops, then are destroyed on that thread.
//!
//! A loadable C-API (v1.2.0) extension cannot recover the query's own
//! `duckdb_connection` in `scan` (`duckdb_client_context_get_connection` is absent from
//! the v1.2.0 table), so chunks are imported on the global connection and *referenced*
//! into `output` rather than filled in place. The database handle quack hands the
//! entry point is only valid during the load (DuckDB drops `load_state.database_data`
//! after it), so a deferred `duckdb_connect` on it would dereference a dangling
//! pointer — hence the `OwnedConnection` opened inside the closure.
//!
//! # Result schema (derived, not hand-mapped)
//! Declared column types come from a real import: `duckdb_vector_get_column_type` on
//! the first imported chunk's vectors returns a *borrowed* `duckdb_logical_type` per
//! column, and `duckdb_bind_add_result_column` *copies* that type into the bind
//! (verified against the DuckDB v1.5.4 C-API source: `return_types.push_back(*logical_type)`).
//! The borrowed handle is passed as-is and never freed — the v1.2.0 loadable table has
//! no `duckdb_copy_logical_type`, and wrapping the handle in an owning type would
//! double-free it. Any Arrow type the kernel can emit (decimal, timestamp with tz,
//! list/map/struct, interval, …) therefore flows through unchanged.

use std::sync::Arc;
use std::sync::atomic::{AtomicPtr, Ordering};
use std::sync::OnceLock;

use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow::record_batch::RecordBatch;
use hudi::table::{QueryType, ReadOptions, Table};
use libduckdb_sys::{
    duckdb_bind_info, duckdb_data_chunk, duckdb_function_info, duckdb_init_info, duckdb_vector,
    idx_t,
};
use quack_rs::entry_point::init_extension_v2;
use quack_rs::prelude::*;
use quack_rs::table::{
    BindInfo, FfiBindData, FfiInitData, InitInfo, TableFunctionBuilder,
};

/// Shared multi-thread tokio runtime driving the async hudi-rs kernel.
static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();

fn runtime() -> &'static tokio::runtime::Runtime {
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .expect("failed to build tokio runtime")
    })
}

/// A process-lifetime global `duckdb_connection` (raw pointer) for the Arrow C Data
/// Interface import. The matching [`OwnedConnection`] is kept alive in
/// [`GLOBAL_OWNED`] so this pointer never dangles; it is published only after the
/// connection is opened.
///
/// Why not `duckdb_connect(as_raw_database())` later: the database handle quack hands
/// the entry point comes from `access.get_database` and is only valid *during* the
/// load (DuckDB drops `load_state.database_data` afterwards). A `duckdb_connect` on
/// that handle at query time dereferences a dangling pointer and segfaults. An
/// `OwnedConnection` opened inside the registration closure owns its
/// `ClientContext` shared_ptr and stays valid for the process lifetime.
static GLOBAL_CON_RAW: AtomicPtr<std::ffi::c_void> = AtomicPtr::new(std::ptr::null_mut());

/// Keeps the global connection(s) alive (drops = disconnect) for the process
/// lifetime. A `Vec` — never shrunk — because a re-LOAD must NOT disconnect the
/// previous connection: chunks imported on it may still be referenced by a
/// query in flight, and dropping the `OwnedConnection` would free that
/// connection's context (and the imported buffers) out from under it. Process
/// lifetime is bounded, so pinning every loaded connection is both safe and
/// leak-free for practical purposes.
static GLOBAL_OWNED: std::sync::Mutex<Vec<quack_rs::query::OwnedConnection>> =
    std::sync::Mutex::new(Vec::new());

/// Open (once) and return a global `duckdb_connection` usable for the Arrow import.
fn global_connection() -> Result<libduckdb_sys::duckdb_connection, String> {
    let con = GLOBAL_CON_RAW.load(Ordering::Acquire) as libduckdb_sys::duckdb_connection;
    if con.is_null() {
        return Err("read_hudi: global connection not initialised".into());
    }
    Ok(con)
}


/// Read a `duckdb_error_data` message, then free the error data.
unsafe fn take_error_message(mut err: libduckdb_sys::duckdb_error_data) -> String {
    if err.is_null() {
        return String::new();
    }
    let ptr = libduckdb_sys::duckdb_error_data_message(err);
    let msg = if ptr.is_null() {
        "unknown error".to_string()
    } else {
        std::ffi::CStr::from_ptr(ptr).to_string_lossy().into_owned()
    };
    libduckdb_sys::duckdb_destroy_error_data(&mut err);
    msg
}

/// Wrap a raw chunk pointer in a `Send` newtype. `duckdb_data_chunk` is `*mut c_void`
/// (not `Send`); we own these chunks and destroy them on the thread that drops the
/// bind data (after every scan state has been torn down), so `Send` is sound.
#[derive(Clone)]
struct SendChunk(std::ptr::NonNull<std::ffi::c_void>);
unsafe impl Send for SendChunk {}
// Shared read-only across scan threads (DuckDB only reads the chunk's vectors); the
// sole mutation is the destroy in `HudiBindData::drop`, after every scan state is gone.
unsafe impl Sync for SendChunk {}

/// Import one hudi `RecordBatch` into a `duckdb_data_chunk` on the global
/// connection, over the Arrow C Data Interface. The chunk is owned by the caller.
unsafe fn import_batch(
    con: libduckdb_sys::duckdb_connection,
    batch: &RecordBatch,
) -> Result<libduckdb_sys::duckdb_data_chunk, String> {
    let struct_ref: arrow::array::ArrayRef = std::sync::Arc::new(arrow::array::StructArray::new(
        batch.schema().fields().clone(),
        batch.columns().to_vec(),
        None,
    ));
    // `export_array_into_raw` is the deprecated path in arrow 58 (the owned
    // `FFI_ArrowArray::new` / `FFI_ArrowSchema::try_from` constructors replace it),
    // but it is the form verified end-to-end for every Arrow type this kernel can
    // emit (nested list/map/struct included), so it is kept deliberately.
    #[allow(deprecated)]
    let mut ffi_array = FFI_ArrowArray::empty();
    #[allow(deprecated)]
    let mut ffi_schema = FFI_ArrowSchema::empty();
    #[allow(deprecated)]
    let export = arrow::ffi::export_array_into_raw(
        struct_ref,
        std::ptr::addr_of_mut!(ffi_array),
        std::ptr::addr_of_mut!(ffi_schema),
    );
    export.map_err(|e| format!("arrow FFI export failed: {e}"))?;

    let sch_ptr = std::ptr::addr_of_mut!(ffi_schema) as *mut libduckdb_sys::ArrowSchema;
    let mut converted: libduckdb_sys::duckdb_arrow_converted_schema = std::ptr::null_mut();
    let err = libduckdb_sys::duckdb_schema_from_arrow(con, sch_ptr, &mut converted);
    if !err.is_null() {
        let msg = take_error_message(err);
        return Err(format!("schema_from_arrow failed: {msg}"));
    }
    let arr_ptr = std::ptr::addr_of_mut!(ffi_array) as *mut libduckdb_sys::ArrowArray;
    let mut chunk: libduckdb_sys::duckdb_data_chunk = std::ptr::null_mut();
    let err = libduckdb_sys::duckdb_data_chunk_from_arrow(con, arr_ptr, converted, &mut chunk);
    libduckdb_sys::duckdb_destroy_arrow_converted_schema(&mut converted);
    if !err.is_null() {
        let msg = take_error_message(err);
        return Err(format!("data_chunk_from_arrow failed: {msg}"));
    }
    if chunk.is_null() {
        return Err("data_chunk_from_arrow produced no chunk".into());
    }
    Ok(chunk)
}

/// A Hudi table read, fully materialised in bind: the kernel's record batches plus the
/// Arrow-imported DuckDB chunks they were bridged into (kept alive for `scan`).
#[derive(Clone)]
struct HudiBindData {
    batches: Vec<RecordBatch>,
    chunks: Vec<SendChunk>,
}

impl std::fmt::Debug for HudiBindData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HudiBindData")
            .field("batches", &self.batches.len())
            .field("chunks", &self.chunks.len())
            .finish()
    }
}

/// Parsed `read_hudi` arguments (positional + named), used to build the kernel's
/// `ReadOptions` and to open the table with any cloud/storage credentials.
/// Public so integration tests (a separate crate) can construct the argument set
/// and drive [`hudi_read_batches`] / [`read_options_for`] directly.
#[derive(Clone, Debug)]
pub struct HudiQueryArgs {
    /// `as_of =>` commit timestamp: time-travel to the latest commit <= this instant.
    pub as_of: Option<String>,
    /// `start =>` + `end =>` (both given): incremental read of commits in
    /// (`start`, `end`]. At least one of them implies `QueryType::Incremental`.
    pub start: Option<String>,
    /// Incremental upper bound (inclusive).
    pub end: Option<String>,
    /// `hudi_option =>` the raw `k=v; k2=v2` string. Parsed into `(key, value)`
    /// pairs inside [`read_hudi_table`] (cloud credentials `hoodie.s3…` /
    /// `hoodie.gcs…` / `hoodie.aws.region`, read tuning like
    /// `hoodie.file_group_reader.version`). Forwarded to both the table open and
    /// the read.
    pub hudi_options: Option<String>,
}

impl Default for HudiQueryArgs {
    fn default() -> Self {
        Self {
            as_of: None,
            start: None,
            end: None,
            hudi_options: None,
        }
    }
}

/// Build the hudi-rs [`ReadOptions`] from the parsed query arguments (the same
/// logic `read_hudi_table` applies), so the mapping `as_of`/`start`/`end` →
/// `QueryType` + timestamps is testable in isolation. Returns an error if a
/// `hudi_option` entry is malformed.
pub fn read_options_for(args: &HudiQueryArgs) -> Result<ReadOptions, String> {
    let mut options = ReadOptions::new();
    for (k, v) in parse_hudi_option(args)? {
        options = options.with_hudi_option(k, v);
    }
    match (&args.start, &args.end) {
        (Some(start), Some(end)) => {
            options = options
                .with_query_type(QueryType::Incremental)
                .with_start_timestamp(start)
                .with_end_timestamp(end);
        }
        (Some(start), None) => {
            options = options
                .with_query_type(QueryType::Incremental)
                .with_start_timestamp(start);
        }
        (None, Some(end)) => {
            options = options
                .with_query_type(QueryType::Incremental)
                .with_end_timestamp(end);
        }
        (None, None) => {
            if let Some(as_of) = &args.as_of {
                options = options
                    .with_query_type(QueryType::Snapshot)
                    .with_as_of_timestamp(as_of);
            }
        }
    }
    Ok(options)
}

/// Parse a `hudi_option` `k=v; k2=v2` string into `(key, value)` pairs.
/// Returns an error naming the first malformed entry (one without `=`).
pub fn parse_hudi_option(args: &HudiQueryArgs) -> Result<Vec<(String, String)>, String> {
    match &args.hudi_options {
        Some(raw) => raw
            .split(';')
            .map(|kv| kv.trim())
            .filter(|kv| !kv.is_empty())
            .map(|kv| {
                let (k, v) = kv
                    .split_once('=')
                    .ok_or_else(|| format!("hudi_option entry {kv:?} has no '=' separator"))?;
                Ok((k.trim().to_string(), v.trim().to_string()))
            })
            .collect(),
        None => Ok(Vec::new()),
    }
}

/// Read a Hudi table's rows via the kernel using the parsed query arguments.
/// Returns the (possibly zero) non-empty `RecordBatch`es, exactly as
/// `read_hudi_table` would produce them before bridging into DuckDB — no global
/// connection is required, so this is unit-testable against local fixtures.
pub fn hudi_read_batches(uri: &str, args: &HudiQueryArgs) -> Result<Vec<RecordBatch>, String> {
    let hudi_options = parse_hudi_option(args)?;
    let open_opts = hudi_options.clone();
    let options = read_options_for(args)?;
    runtime().block_on(async {
        let table = if open_opts.is_empty() {
            Table::new(uri)
                .await
                .map_err(|e| format!("failed to open Hudi table at {uri}: {e}"))?
        } else {
            Table::new_with_options(uri, open_opts.into_iter())
                .await
                .map_err(|e| format!("failed to open Hudi table at {uri}: {e}"))?
        };
        let batches = table
            .read(&options)
            .await
            .map_err(|e| format!("Hudi read failed: {e}"))?;
        Ok(batches.into_iter().filter(|b| b.num_rows() > 0).collect())
    })
}

/// Read the Hudi table at `uri` via the hudi-rs kernel (single `block_on`) and import
/// every non-empty batch into a DuckDB chunk on the global connection. Used by the
/// `read_hudi` table function's `bind`; the kernel-only test path uses
/// [`hudi_read_batches`] instead (no global connection needed).
fn read_hudi_table(uri: &str, args: &HudiQueryArgs) -> Result<HudiBindData, String> {
    // Parse the `hudi_option` k=v list (`k=v; k2=v2`).
    let hudi_options: Vec<(String, String)> = match &args.hudi_options {
        Some(raw) => raw
            .split(';')
            .map(|kv| kv.trim())
            .filter(|kv| !kv.is_empty())
            .map(|kv| {
                let (k, v) = kv
                    .split_once('=')
                    .ok_or_else(|| format!("hudi_option entry {kv:?} has no '=' separator"))?;
                Ok((k.trim().to_string(), v.trim().to_string()))
            })
            .collect::<Result<Vec<_>, String>>()?,
        None => Vec::new(),
    };

    let open_opts = hudi_options.clone();
    let read_opts: Vec<(String, String)> = hudi_options.clone();

    runtime().block_on(async {
        // Open the table, forwarding any storage-credential options the user supplied.
        let table = if open_opts.is_empty() {
            Table::new(uri)
                .await
                .map_err(|e| format!("failed to open Hudi table at {uri}: {e}"))?
        } else {
            Table::new_with_options(uri, open_opts.into_iter())
                .await
                .map_err(|e| format!("failed to open Hudi table at {uri}: {e}"))?
        };

        // Build the kernel read options from the query arguments.
        let mut options = ReadOptions::new();
        for (k, v) in &read_opts {
            options = options.with_hudi_option(k, v);
        }
        match (&args.start, &args.end) {
            (Some(start), Some(end)) => {
                options = options
                    .with_query_type(QueryType::Incremental)
                    .with_start_timestamp(start)
                    .with_end_timestamp(end);
            }
            (Some(start), None) => {
                // Incremental from `start` up to the latest commit.
                options = options
                    .with_query_type(QueryType::Incremental)
                    .with_start_timestamp(start);
            }
            (None, Some(end)) => {
                options = options
                    .with_query_type(QueryType::Incremental)
                    .with_end_timestamp(end);
            }
            (None, None) => {
                if let Some(as_of) = &args.as_of {
                    options = options
                        .with_query_type(QueryType::Snapshot)
                        .with_as_of_timestamp(as_of);
                }
                // else: default latest snapshot — nothing to set.
            }
        }

        let batches = table
            .read(&options)
            .await
            .map_err(|e| format!("Hudi read failed: {e}"))?;
        let mut batches: Vec<RecordBatch> =
            batches.into_iter().filter(|b| b.num_rows() > 0).collect();

        let con = global_connection()?;
        let mut chunks = Vec::with_capacity(batches.len().max(1));

        if batches.is_empty() {
            // A legitimate zero-row read (an incremental window with no new commits,
            // or an empty table) still needs a schema for `bind` to declare.
            // Synthesise an empty batch from the table's latest schema so the result
            // schema is correct even when no rows come back.
            let schema = table
                .get_schema()
                .await
                .map_err(|e| format!("failed to read Hudi schema at {uri}: {e}"))?;
            let arrow_schema = std::sync::Arc::new(schema);
            let ncols = arrow_schema.fields().len();
            let cols: Vec<arrow::array::ArrayRef> = (0..ncols)
                .map(|i| {
                    arrow::array::new_empty_array(&arrow_schema.fields()[i].data_type())
                })
                .collect();
            let empty = arrow::record_batch::RecordBatch::try_new(arrow_schema, cols)
                .map_err(|e| format!("failed to build empty result batch: {e}"))?;
            let chunk = unsafe { import_batch(con, &empty) }?;
            let ptr = std::ptr::NonNull::new(chunk as *mut std::ffi::c_void)
                .ok_or("data_chunk_from_arrow returned a null chunk")?;
            chunks.push(SendChunk(ptr));
            batches.push(empty);
        } else {
            for b in &batches {
                let chunk = unsafe { import_batch(con, b) }?;
                let ptr = std::ptr::NonNull::new(chunk as *mut std::ffi::c_void)
                    .ok_or("data_chunk_from_arrow returned a null chunk")?;
                chunks.push(SendChunk(ptr));
            }
        }

        Ok(HudiBindData { batches, chunks })
    })
}

// ── read_hudi(uri) — table function ─────────────────────────────────────────

/// Read a `quack_rs::Value` named-parameter as an `Option<String>`. `None` when the
/// parameter was not supplied (null handle) or is SQL `NULL`.
fn named_param_string(bind_info: &BindInfo, name: &str) -> Option<String> {
    let v = unsafe { bind_info.get_named_parameter_value(name) };
    if v.is_null() {
        return None;
    }
    if v.is_sql_null() {
        return None;
    }
    v.as_str().ok()
}

/// bind: read the table once via the hudi-rs kernel (honouring `as_of` / `start` /
/// `end` / `hudi_option`), import the batches, declare the result schema from the
/// real import, stash everything for init/scan.
unsafe extern "C" fn hudi_bind(info: duckdb_bind_info) {
    let bind_info = BindInfo::new(info);
    let uri = match bind_info.get_parameter_value(0).as_str() {
        Ok(s) => s,
        Err(_) => {
            bind_info.set_error("read_hudi: the table path (uri) argument is required or not a string");
            return;
        }
    };
    if uri.is_empty() {
        bind_info.set_error("read_hudi: the table path (uri) is required");
        return;
    }

    let as_of = named_param_string(&bind_info, "as_of");
    let start = named_param_string(&bind_info, "start");
    let end = named_param_string(&bind_info, "end");
    let hudi_options = named_param_string(&bind_info, "hudi_option");

    // Both `as_of` and an incremental window are mutually exclusive query types.
    if as_of.is_some() && (start.is_some() || end.is_some()) {
        bind_info.set_error(
            "read_hudi: `as_of` (time-travel snapshot) and `start`/`end` (incremental) are mutually exclusive",
        );
        return;
    }

    let args = HudiQueryArgs {
        as_of,
        start,
        end,
        hudi_options,
    };

    let bind_data = match read_hudi_table(&uri, &args) {
        Ok(d) => d,
        Err(e) => {
            bind_info.set_error(&e);
            return;
        }
    };

    // Declare the schema from the first imported chunk: per-column *borrowed*
    // logical-type handles (valid while `chunks` is alive), which
    // `duckdb_bind_add_result_column` copies into the bind.
    let first_chunk = bind_data.chunks[0].0.as_ptr() as libduckdb_sys::duckdb_data_chunk;
    let ncols = unsafe { libduckdb_sys::duckdb_data_chunk_get_column_count(first_chunk) } as usize;
    let names: Vec<String> = bind_data.batches[0]
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    let cnames: Vec<std::ffi::CString> =
        match names.iter().map(|s| std::ffi::CString::new(s.as_str())).collect() {
            Ok(v) => v,
            Err(_) => {
                bind_info.set_error("read_hudi: a column name is not valid UTF-8");
                return;
            }
        };
    for i in 0..ncols {
        let vec = unsafe { libduckdb_sys::duckdb_data_chunk_get_vector(first_chunk, i as idx_t) };
        // SAFETY: `vec` is a live vector of `first_chunk`; v1.5.4's
        // `duckdb_vector_get_column_type` returns a FRESH owned handle
        // (`new LogicalType(v->GetType())` in the DuckDB source), and
        // `duckdb_bind_add_result_column` copies it — so we destroy our handle.
        let ty = unsafe { libduckdb_sys::duckdb_vector_get_column_type(vec) };
        unsafe {
            libduckdb_sys::duckdb_bind_add_result_column(info, cnames[i].as_ptr(), ty);
            let mut ty2 = ty;
            libduckdb_sys::duckdb_destroy_logical_type(&mut ty2);
        }
    }

    FfiBindData::<Arc<HudiBindData>>::set(info, Arc::new(bind_data));
}

/// Per-execution scan state, stored as the GLOBAL init data (read in `scan` via
/// `FfiInitData::get(function_info)` — the v1.2.0 loadable C API exposes no
/// `duckdb_init_get_init_data`, so `local_init` cannot read it; the state is
/// therefore created in `init` and read from `scan`, exactly like hello-ext's
/// single-scan-thread pattern).
///
/// `next_batch` claims chunks across scanner threads (exactly once each); `bind`
/// keeps the imported chunks alive for the whole query.
struct HudiGlobalState {
    next_batch: std::sync::atomic::AtomicUsize,
    bind: Arc<HudiBindData>,
}

unsafe impl Send for HudiGlobalState {}
unsafe impl Sync for HudiGlobalState {}

/// init (global, once per execution): publish the shared cursor + bind data to scan.
unsafe extern "C" fn hudi_init(info: duckdb_init_info) {
    let Some(bind) = FfiBindData::<Arc<HudiBindData>>::get_from_init(info) else {
        InitInfo::new(info).set_error("read_hudi: missing bind data");
        return;
    };
    FfiInitData::<HudiGlobalState>::set(
        info,
        HudiGlobalState {
            next_batch: std::sync::atomic::AtomicUsize::new(0),
            bind: (*bind).clone(),
        },
    );
}

/// scan: claim the next chunk via the shared cursor (exactly-once across threads),
/// alias its vectors into `output`; signal end-of-stream (size 0) once exhausted.
/// The imported chunks stay alive inside the bind data (global init data), so the
/// aliased vectors remain valid for the whole query.
unsafe extern "C" fn hudi_scan(info: duckdb_function_info, output: duckdb_data_chunk) {
    let Some(state) = FfiInitData::<HudiGlobalState>::get_mut(info) else {
        libduckdb_sys::duckdb_data_chunk_set_size(output, 0);
        return;
    };
    let bind = state.bind.clone();
    let chunks = bind.chunks.len();

    // Claim a chunk index atomically; skip empty chunks.
    loop {
        let claimed = state
            .next_batch
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        if claimed >= chunks {
            libduckdb_sys::duckdb_data_chunk_set_size(output, 0);
            return;
        }
        let sc = &bind.chunks[claimed];
        let src = sc.0.as_ptr() as libduckdb_sys::duckdb_data_chunk;
        let n = libduckdb_sys::duckdb_data_chunk_get_size(src);
        if n == 0 {
            continue;
        }
        let ncols = libduckdb_sys::duckdb_data_chunk_get_column_count(src) as idx_t;
        for col in 0..ncols {
            let out_vec = libduckdb_sys::duckdb_data_chunk_get_vector(output, col);
            let src_vec = libduckdb_sys::duckdb_data_chunk_get_vector(src, col);
            libduckdb_sys::duckdb_vector_reference_vector(out_vec, src_vec);
        }
        libduckdb_sys::duckdb_data_chunk_set_size(output, n);
        return;
    }
}

/// Destroy the imported chunks once the last reference to the bind data goes away
/// (after every scan state of every execution using it has been torn down).
impl Drop for HudiBindData {
    fn drop(&mut self) {
        for sc in self.chunks.drain(..) {
            let mut raw = sc.0.as_ptr() as libduckdb_sys::duckdb_data_chunk;
            if !raw.is_null() {
                unsafe {
                    libduckdb_sys::duckdb_destroy_data_chunk(&mut raw);
                }
            }
        }
    }
}

// ── hudi_info(path) / hudi_files(path) — scalar metadata functions ──────────
// Rebuilt on the kernel (DESIGN P2): table type from `table_type()`, base file
// format + commits from the resolved table config/timeline, files from
// `get_file_slices()`. Signature matches the original (single `path` arg).

/// One file slice rendered for `hudi_files` output.
fn file_slice_line(uri: &str, fs: &hudi::file_group::file_slice::FileSlice) -> String {
    let base = fs
        .base_file
        .as_ref()
        .map(|b| {
            let partition = fs.partition_path.trim_end_matches('/');
            if partition.is_empty() {
                b.file_name()
            } else {
                format!("{partition}/{}", b.file_name())
            }
        })
        .unwrap_or_else(|| format!("(log-only, file_id={})", fs.file_id()));
    let logs = fs.log_files.len();
    let kind = match (fs.base_file.is_some(), fs.has_log_file()) {
        (true, true) => "MOR",
        (true, false) => "COW",
        (false, _) => "LOG",
    };
    let size = fs.total_size_bytes();
    format!("{uri}/{base}\t{kind}\t{size}\t{logs} log")
}

/// Collect a table's metadata via the kernel (single `block_on`).
fn hudi_table_metadata(uri: &str) -> Result<(String, Vec<String>), String> {
    runtime().block_on(async {
        let table = Table::new(uri)
            .await
            .map_err(|e| format!("failed to open Hudi table at {uri}: {e}"))?;

        let timeline = table.get_timeline();
        let latest = timeline
            .get_latest_commit_timestamp()
            .unwrap_or_else(|_| "unknown".into());
        let n_commits = timeline.completed_commits.len();

        let slices = table
            .get_file_slices(&hudi::table::ReadOptions::new())
            .await
            .map_err(|e| format!("failed to read Hudi file slices at {uri}: {e}"))?;
        let files = slices
            .iter()
            .map(|fs| file_slice_line(uri, fs))
            .collect::<Vec<_>>();
        let n_files = files.len();

        // Table type + base file format from the resolved table config (falling
        // back to the first base file's extension when the config key is absent).
        let table_type = table.table_type();
        let base_format = table
            .hudi_options()
            .get("hoodie.table.base.file.format")
            .cloned()
            .or_else(|| {
                slices
                    .iter()
                    .find_map(|fs| fs.base_file.as_ref().map(|b| b.extension.clone()))
            })
            .unwrap_or_else(|| "?".into());

        let info = format!(
            "Hudi table: type={table_type} base_format={base_format} \
             latest_commit={latest} commits={n_commits} files={n_files}"
        );
        Ok((info, files))
    })
}

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
    let reader = quack_rs::vector::VectorReader::new(input, 0);
    let uri = reader.read_str(0).to_string();
    let msg = match hudi_table_metadata(&uri) {
        Ok((info, _)) => info,
        Err(e) => format!("Error: {e}"),
    };
    quack_rs::vector::VectorWriter::new(output).write_str(0, &msg);
}

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
    let reader = quack_rs::vector::VectorReader::new(input, 0);
    let uri = reader.read_str(0).to_string();
    let msg = match hudi_table_metadata(&uri) {
        Ok((_, files)) => files.join("\n"),
        Err(e) => format!("Error: {e}"),
    };
    quack_rs::vector::VectorWriter::new(output).write_str(0, &msg);
}

// ── Registration ────────────────────────────────────────────────────────────

fn register_all(reg: &impl Registrar) -> Result<(), ExtensionError> {
    unsafe {
        reg.register_table(
            TableFunctionBuilder::new("read_hudi")
                .param(quack_rs::types::TypeId::Varchar)
                .named_param("as_of", quack_rs::types::TypeId::Varchar)
                .named_param("start", quack_rs::types::TypeId::Varchar)
                .named_param("end", quack_rs::types::TypeId::Varchar)
                .named_param("hudi_option", quack_rs::types::TypeId::Varchar)
                .bind(hudi_bind)
                .init(hudi_init)
                .scan(hudi_scan),
        )?;
        reg.register_scalar(build_hudi_info())?;
        reg.register_scalar(build_hudi_files())?;
    }
    Ok(())
}

// ── Entry ───────────────────────────────────────────────────────────────────

#[no_mangle]
pub unsafe extern "C" fn hudi_init_c_api(
    info: libduckdb_sys::duckdb_extension_info,
    access: *const libduckdb_sys::duckdb_extension_access,
) -> bool {
    unsafe {
        init_extension_v2(info, access, quack_rs::DUCKDB_API_VERSION, |con| {
            // Open a process-lifetime global connection *inside* the registration
            // closure: `con`'s database handle is only valid during the load, but an
            // `OwnedConnection` owns its `ClientContext` shared_ptr and survives it.
            // `bind` (at query time) imports Arrow data on this connection, so the
            // connection must outlive the load — quack disconnects `con` after this
            // closure returns, which is exactly why `con` itself cannot be reused.
            let owned = con.open_connection()?;
            let raw = owned.as_raw() as *mut std::ffi::c_void;
            // Publish the raw pointer, then pin the OwnedConnection. Re-LOADs
            // APPEND to GLOBAL_OWNED and never drop the previous connection:
            // imported chunks may still be referenced by in-flight queries.
            GLOBAL_CON_RAW.store(raw, Ordering::Release);
            GLOBAL_OWNED.lock().unwrap().push(owned);
            register_all(con)
        })
    }
}

#[cfg(test)]
mod tests {
    //! P5: unit + integration tests for the kernel read path that backs
    //! `read_hudi`. The `#[ignore]` tests need the hudi-rs sample-table fixtures
    //! materialised (see `scripts/materialize.sh`); they skip cleanly otherwise.

    use super::{hudi_read_batches, parse_hudi_option, read_options_for, HudiQueryArgs};

    fn total_rows(batches: &[arrow::record_batch::RecordBatch]) -> u64 {
        batches.iter().map(|b| b.num_rows() as u64).sum()
    }

    fn fixture_ready(p: &str) -> bool {
        std::path::Path::new(&format!("{p}/.hoodie/hoodie.properties")).exists()
    }

    // ── hudi_option parsing (pure, no fixtures) ────────────────────────────

    #[test]
    fn hudi_option_parsing_trims_and_pairs() {
        let args = HudiQueryArgs {
            hudi_options: Some(" a = 1 ;b=2; c=3 ".into()),
            ..Default::default()
        };
        let got: Vec<(String, String)> =
            parse_hudi_option(&args).unwrap().into_iter().map(|(k, v)| (k, v)).collect();
        assert_eq!(
            got,
            vec![("a".into(), "1".into()), ("b".into(), "2".into()), ("c".into(), "3".into())]
        );
    }

    #[test]
    fn hudi_option_none_is_empty() {
        assert!(parse_hudi_option(&HudiQueryArgs::default()).unwrap().is_empty());
    }

    // ── query-argument → ReadOptions mapping (pure, no fixtures) ───────────

    #[test]
    fn query_type_selection_does_not_panic() {
        let _ = read_options_for(&HudiQueryArgs::default()); // snapshot
        let _ = read_options_for(&HudiQueryArgs {
            as_of: Some("x".into()),
            ..Default::default()
        }); // time-travel snapshot
        let _ = read_options_for(&HudiQueryArgs {
            start: Some("a".into()),
            end: Some("b".into()),
            ..Default::default()
        }); // incremental window
        let _ = read_options_for(&HudiQueryArgs {
            start: Some("a".into()),
            ..Default::default()
        }); // incremental from start
        let _ = read_options_for(&HudiQueryArgs {
            end: Some("b".into()),
            ..Default::default()
        }); // incremental to end
    }

    // ── kernel read against local fixtures (require scripts/materialize.sh) ─

    #[test]
    #[ignore = "needs materialised fixtures"]
    fn cow_snapshot_rowcount() {
        let p = "/tmp/hudi-sample/cow/v6_nonpartitioned";
        if !fixture_ready(p) {
            eprintln!("cow fixture not materialised; skipping");
            return;
        }
        let batches = hudi_read_batches(p, &HudiQueryArgs::default()).expect("read cow");
        assert_eq!(total_rows(&batches), 4, "v6 COW nonpartitioned = 4 rows");
    }

    #[test]
    #[ignore = "needs materialised fixtures"]
    fn mor_merge_rowcount() {
        let p = "/tmp/hudi-sample/mor/v6_nonpartitioned";
        if !fixture_ready(p) {
            eprintln!("mor fixture not materialised; skipping");
            return;
        }
        let batches = hudi_read_batches(p, &HudiQueryArgs::default()).expect("read mor");
        assert_eq!(total_rows(&batches), 4, "v6 MOR nonpartitioned merges base+log to 4 rows");
    }

    #[test]
    #[ignore = "needs materialised fixtures"]
    fn mor_time_travel() {
        let p = "/tmp/hudi-sample/mor/v6_nonpartitioned";
        if !fixture_ready(p) {
            return;
        }
        let first = hudi_read_batches(
            p,
            &HudiQueryArgs {
                as_of: Some("20250117083125837".into()),
                ..Default::default()
            },
        )
        .expect("as_of first");
        assert_eq!(total_rows(&first), 3, "as_of first commit = 3 rows");
        let latest = hudi_read_batches(
            p,
            &HudiQueryArgs {
                as_of: Some("20250117083136333".into()),
                ..Default::default()
            },
        )
        .expect("as_of latest");
        assert_eq!(total_rows(&latest), 4, "as_of latest commit = 4 rows");
    }

    #[test]
    #[ignore = "needs materialised fixtures"]
    fn mor_incremental_window() {
        let p = "/tmp/hudi-sample/mor/v6_nonpartitioned";
        if !fixture_ready(p) {
            return;
        }
        let incr = hudi_read_batches(
            p,
            &HudiQueryArgs {
                start: Some("20250117083125837".into()),
                end: Some("20250117083136333".into()),
                ..Default::default()
            },
        )
        .expect("incremental");
        assert_eq!(total_rows(&incr), 2, "incremental (commit1,commit2] = 2 rows");
        let empty = hudi_read_batches(
            p,
            &HudiQueryArgs {
                start: Some("20250117083136333".into()),
                end: Some("20250117083136333".into()),
                ..Default::default()
            },
        )
        .expect("empty incremental");
        assert_eq!(total_rows(&empty), 0, "empty incremental window = 0 rows");
    }
}
