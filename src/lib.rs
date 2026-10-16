//! DuckDB Apache Hudi extension — P0: read Hudi COW/MOR tables via the hudi-rs 0.5.0 kernel.
//!
//! # Kernel
//! The `hudi` crate (hudi-rs) parses the timeline, discovers file slices, resolves the
//! table schema, and performs COW/MOR merging natively in Rust. This extension only
//! drives the kernel and bridges its Arrow output into DuckDB; it contains no
//! timeline/parquet logic of its own.
//!
//! # Data path
//! `bind` opens the table, resolves the query's file slices and declares the table
//! schema; no rows are read there. `init` maps DuckDB's projection onto the table's
//! columns and allows one scanner thread per file slice. Each scanner thread claims
//! slices in turn and streams each one through `read_file_slice_stream`, reading
//! only the projected columns, in batches of one DuckDB vector. Each batch is:
//!   1. wrapped as a struct `ArrayRef` and exported over the Arrow C Data Interface
//!      (`arrow::ffi::export_array_into_raw`) into `FFI_ArrowArray`/`FFI_ArrowSchema`
//!      — the same `#[repr(C)]` layout libduckdb-sys declares (pointer cast);
//!   2. imported on a process-lifetime *global* `OwnedConnection` (opened inside the
//!      entry point, kept alive in a `static`) via the raw C-API
//!      `duckdb_schema_from_arrow` + `duckdb_data_chunk_from_arrow`;
//!   3. aliased into DuckDB's `output` chunk with `duckdb_vector_reference_vector`
//!      (no payload copy). The referenced vectors share the imported Arrow buffers,
//!      so the imported chunk itself is released on the thread's next call.
//!
//! The metadata table functions (`hudi_mdt`, `hudi_commits`, `hudi_stats`) still
//! build their small results in `bind` and keep the imported chunks there.
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
//! Declared column types come from a real import: `read_hudi` imports an empty batch
//! of the table schema, the metadata functions their first chunk. For each column,
//! `duckdb_vector_get_column_type` returns a fresh `duckdb_logical_type`, which
//! `duckdb_bind_add_result_column` copies into the bind, so the handle is destroyed
//! after. Any Arrow type the kernel can emit (decimal, timestamp with tz,
//! list/map/struct, interval, …) therefore flows through unchanged.

use std::sync::Arc;
use std::sync::atomic::{AtomicPtr, Ordering};
use std::sync::OnceLock;

use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow::record_batch::RecordBatch;
use arrow::datatypes::{Field, Schema, SchemaRef};
use futures::stream::{BoxStream, StreamExt};
use hudi::file_group::file_slice::FileSlice;
use hudi::file_group::reader::FileGroupReader;
use hudi::table::{QueryType, ReadOptions, Table};
use libduckdb_sys::{
    duckdb_bind_info, duckdb_data_chunk, duckdb_function_info, duckdb_init_info, duckdb_vector,
    idx_t,
};
use quack_rs::entry_point::init_extension_v2;
use quack_rs::prelude::*;
use quack_rs::table::{
    BindInfo, FfiBindData, FfiInitData, FfiLocalInitData, FunctionInfo, InitInfo,
    TableFunctionBuilder,
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

/// Import `batch` as one chunk per DuckDB vector's worth of rows, appending them to
/// `chunks`. A scan emits one imported chunk per call, and an output chunk larger
/// than a vector overruns the buffers of every operator downstream, while the
/// kernel returns batches of any size.
fn import_batch_in_vectors(
    con: libduckdb_sys::duckdb_connection,
    batch: &RecordBatch,
    chunks: &mut Vec<SendChunk>,
) -> Result<(), String> {
    let max_rows = unsafe { libduckdb_sys::duckdb_vector_size() } as usize;
    let mut offset = 0;
    // A zero-row batch still becomes one chunk: binds declare their result schema
    // from the first chunk.
    loop {
        let len = max_rows.min(batch.num_rows() - offset);
        let chunk = unsafe { import_batch(con, &batch.slice(offset, len)) }?;
        let ptr = std::ptr::NonNull::new(chunk as *mut std::ffi::c_void)
            .ok_or("data_chunk_from_arrow returned a null chunk")?;
        chunks.push(SendChunk(ptr));
        offset += len;
        if offset >= batch.num_rows() {
            return Ok(());
        }
    }
}

/// A metadata table function's result, built in bind: its record batches plus the
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
    /// pairs by [`parse_hudi_option`] (cloud credentials `hoodie.s3…` /
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

/// Build the hudi-rs [`ReadOptions`] from the parsed query arguments, as
/// `read_hudi` plans its scan with them, so the mapping `as_of`/`start`/`end` →
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

/// Read a Hudi table's rows via the kernel's `Table::read` using the parsed query
/// arguments, returning the (possibly zero) non-empty `RecordBatch`es. No global
/// connection is required, so the tests use it as a reference for the fixtures.
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

/// A planned `read_hudi` scan, resolved in `bind`: the file slices the query reads
/// and a reader that carries its timestamps. No rows are read until `scan`, where
/// each scanner thread streams whole file slices on its own.
struct HudiScanBind {
    reader: FileGroupReader,
    slices: Vec<FileSlice>,
    /// The declared result schema, every field nullable.
    schema: SchemaRef,
}

/// Open the table at `uri` and resolve the slices and result schema of the read
/// that `args` describes.
fn plan_hudi_scan(uri: &str, args: &HudiQueryArgs) -> Result<HudiScanBind, String> {
    let open_opts = parse_hudi_option(args)?;
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
        let slices = table
            .get_file_slices(&options)
            .await
            .map_err(|e| format!("failed to plan Hudi read at {uri}: {e}"))?;
        // The reader resolves the snapshot or incremental bounds from `options`
        // and applies the commit-time filter of an incremental read per batch.
        let reader = table
            .create_file_group_reader_with_options(
                Some(&options),
                std::iter::empty::<(&str, &str)>(),
            )
            .await
            .map_err(|e| format!("failed to create Hudi reader at {uri}: {e}"))?;
        // Slices carry the meta fields exactly when the table populates them.
        let populates_meta_fields = table
            .hudi_options()
            .get("hoodie.populate.meta.fields")
            .is_none_or(|v| !v.eq_ignore_ascii_case("false"));
        let schema = if populates_meta_fields {
            table.get_schema_with_meta_fields().await
        } else {
            table.get_schema().await
        }
        .map_err(|e| format!("failed to read Hudi schema at {uri}: {e}"))?;
        let fields: Vec<Field> = schema
            .fields()
            .iter()
            .map(|f| f.as_ref().clone().with_nullable(true))
            .collect();
        Ok(HudiScanBind {
            reader,
            slices,
            schema: Arc::new(Schema::new(fields)),
        })
    })
}

/// Shape a kernel batch as `schema`: columns picked by name, in its order, cast
/// where the file's type differs from the declared one.
fn align_batch(batch: &RecordBatch, schema: &SchemaRef) -> Result<RecordBatch, String> {
    let columns = schema
        .fields()
        .iter()
        .map(|field| {
            let column = batch.column_by_name(field.name()).ok_or_else(|| {
                format!("read_hudi: a file slice has no column {:?}", field.name())
            })?;
            if column.data_type() == field.data_type() {
                Ok(column.clone())
            } else {
                arrow_cast::cast(column, field.data_type())
                    .map_err(|e| format!("read_hudi: cannot cast column {:?}: {e}", field.name()))
            }
        })
        .collect::<Result<Vec<_>, String>>()?;
    let options = arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
    RecordBatch::try_new_with_options(schema.clone(), columns, &options)
        .map_err(|e| format!("read_hudi: {e}"))
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

/// bind: plan the read (honouring `as_of` / `start` / `end` / `hudi_option`) and
/// declare the table's columns. Rows are read in `scan`.
unsafe extern "C" fn hudi_bind(info: duckdb_bind_info) {
    if let Err(msg) = catch_panic(|| hudi_bind_body(info)) {
        BindInfo::new(info).set_error(&msg);
    }
}

unsafe fn hudi_bind_body(info: duckdb_bind_info) {
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

    let plan = match plan_hudi_scan(&uri, &args) {
        Ok(p) => p,
        Err(e) => {
            bind_info.set_error(&e);
            return;
        }
    };

    // DuckDB's own conversion of the schema gives the declared types, so they are
    // exactly the types the scan's imports produce.
    let con = match global_connection() {
        Ok(c) => c,
        Err(e) => {
            bind_info.set_error(&e);
            return;
        }
    };
    let empty = RecordBatch::new_empty(plan.schema.clone());
    let chunk = match unsafe { import_batch(con, &empty) } {
        Ok(c) => c,
        Err(e) => {
            bind_info.set_error(&e);
            return;
        }
    };
    let chunk_guard = OwnedChunk(chunk);
    for (i, field) in plan.schema.fields().iter().enumerate() {
        let Ok(name) = std::ffi::CString::new(field.name().as_str()) else {
            bind_info.set_error("read_hudi: a column name contains a NUL byte");
            return;
        };
        let vec = unsafe { libduckdb_sys::duckdb_data_chunk_get_vector(chunk_guard.0, i as idx_t) };
        // SAFETY: `duckdb_vector_get_column_type` returns a fresh owned handle and
        // `duckdb_bind_add_result_column` copies it, so ours is destroyed here.
        let mut ty = unsafe { libduckdb_sys::duckdb_vector_get_column_type(vec) };
        unsafe {
            libduckdb_sys::duckdb_bind_add_result_column(info, name.as_ptr(), ty);
            libduckdb_sys::duckdb_destroy_logical_type(&mut ty);
        }
    }

    let rows: i64 = plan
        .slices
        .iter()
        .filter_map(|s| s.base_file.as_ref()?.file_metadata.as_ref())
        .map(|m| m.num_records.max(0))
        .sum();
    if rows > 0 {
        bind_info.set_cardinality(rows as u64, false);
    }

    FfiBindData::<Arc<HudiScanBind>>::set(info, Arc::new(plan));
}

/// Run a callback body, turning a panic into an error message: a panic unwinding
/// out of an `extern "C"` callback aborts the host process. One way to panic here
/// is `block_on` on a thread that is already inside a tokio runtime, which is the
/// case when an application drives DuckDB from its own runtime's workers.
fn catch_panic(body: impl FnOnce()) -> Result<(), String> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(body)).map_err(|payload| {
        let detail = payload
            .downcast_ref::<&str>()
            .map(|s| s.to_string())
            .or_else(|| payload.downcast_ref::<String>().cloned())
            .unwrap_or_else(|| "unknown panic".to_string());
        format!("read_hudi: internal error: {detail}")
    })
}

/// A chunk this extension imported and still owns; destroyed on drop.
struct OwnedChunk(libduckdb_sys::duckdb_data_chunk);

// SAFETY: the chunk is only touched by the thread holding the owning state.
unsafe impl Send for OwnedChunk {}

impl Drop for OwnedChunk {
    fn drop(&mut self) {
        if !self.0.is_null() {
            unsafe { libduckdb_sys::duckdb_destroy_data_chunk(&mut self.0) };
        }
    }
}

/// Per-execution state shared by every scanner thread.
struct HudiScanGlobal {
    bind: Arc<HudiScanBind>,
    /// Claims slices across threads, each exactly once.
    next_slice: std::sync::atomic::AtomicUsize,
    /// The projection and batch size each slice is read with.
    slice_options: ReadOptions,
    /// The projected columns the table declares, which batches are aligned to.
    projected: SchemaRef,
    /// For each output column, its position in `projected`; `None` for a column
    /// the table does not declare, such as the row id `count(*)` asks for.
    output_columns: Vec<Option<usize>>,
}

/// Per-thread scan state: the slice being streamed and the batch being emitted.
struct HudiScanLocal {
    stream: Option<BoxStream<'static, hudi::error::Result<RecordBatch>>>,
    batch: Option<RecordBatch>,
    offset: usize,
    /// The chunk the last output references, kept until the next call.
    current: Option<OwnedChunk>,
}

/// init (global): map DuckDB's projection onto the table's columns and allow one
/// scanner per file slice.
unsafe extern "C" fn hudi_scan_init(info: duckdb_init_info) {
    if let Err(msg) = catch_panic(|| hudi_scan_init_body(info)) {
        InitInfo::new(info).set_error(&msg);
    }
}

unsafe fn hudi_scan_init_body(info: duckdb_init_info) {
    let init_info = InitInfo::new(info);
    let Some(bind) = FfiBindData::<Arc<HudiScanBind>>::get_from_init(info) else {
        init_info.set_error("read_hudi: missing bind data");
        return;
    };
    let schema = &bind.schema;
    let mut projected_fields = Vec::new();
    let mut output_columns = Vec::new();
    for i in 0..init_info.projected_column_count() {
        match init_info.projected_column_index(i) {
            Some(col) if col < schema.fields().len() => {
                output_columns.push(Some(projected_fields.len()));
                projected_fields.push(schema.field(col).clone());
            }
            _ => output_columns.push(None),
        }
    }
    let projected = Arc::new(Schema::new(projected_fields));
    // DuckDB projects at least one declared column even for `count(*)` (a C API
    // table function has no row id), so this fallback is defensive: with nothing
    // declared requested, the kernel still reads one column to count rows.
    let kernel_columns: Vec<String> = if projected.fields().is_empty() {
        schema.fields().first().map(|f| f.name().clone()).into_iter().collect()
    } else {
        projected.fields().iter().map(|f| f.name().clone()).collect()
    };
    let slice_options = match ReadOptions::new().with_batch_size(vector_size()) {
        Ok(o) => o.with_projection(kernel_columns),
        Err(e) => {
            init_info.set_error(&format!("read_hudi: {e}"));
            return;
        }
    };
    init_info.set_max_threads(bind.slices.len().max(1) as u64);
    FfiInitData::<HudiScanGlobal>::set(
        info,
        HudiScanGlobal {
            bind: (*bind).clone(),
            next_slice: std::sync::atomic::AtomicUsize::new(0),
            slice_options,
            projected,
            output_columns,
        },
    );
}

/// init (per thread): an idle scanner.
unsafe extern "C" fn hudi_scan_local_init(info: duckdb_init_info) {
    if let Err(msg) = catch_panic(|| hudi_scan_local_init_body(info)) {
        InitInfo::new(info).set_error(&msg);
    }
}

unsafe fn hudi_scan_local_init_body(info: duckdb_init_info) {
    FfiLocalInitData::<HudiScanLocal>::set(
        info,
        HudiScanLocal {
            stream: None,
            batch: None,
            offset: 0,
            current: None,
        },
    );
}

/// The scan's chunk size: the kernel is asked for batches of this many rows, and
/// any larger batch is emitted in pieces of it.
fn vector_size() -> usize {
    unsafe { libduckdb_sys::duckdb_vector_size() as usize }
}

/// scan: emit up to one vector of the current batch, pulling the next batch from
/// this thread's slice stream, and the next unclaimed slice once that ends.
unsafe extern "C" fn hudi_scan_scan(info: duckdb_function_info, output: duckdb_data_chunk) {
    if let Err(msg) = catch_panic(|| hudi_scan_scan_body(info, output)) {
        libduckdb_sys::duckdb_data_chunk_set_size(output, 0);
        FunctionInfo::new(info).set_error(&msg);
    }
}

unsafe fn hudi_scan_scan_body(info: duckdb_function_info, output: duckdb_data_chunk) {
    let function_info = FunctionInfo::new(info);
    let (Some(global), Some(local)) = (
        FfiInitData::<HudiScanGlobal>::get(info),
        FfiLocalInitData::<HudiScanLocal>::get_mut(info),
    ) else {
        function_info.set_error("read_hudi: missing scan state");
        return;
    };
    // The previous output has been consumed; DuckDB's vectors keep their own
    // references to anything still in use.
    local.current = None;
    match next_output(global, local, output) {
        Ok(rows) => libduckdb_sys::duckdb_data_chunk_set_size(output, rows as idx_t),
        Err(e) => function_info.set_error(&e),
    }
}

/// Fill `output` with the next rows of this thread's share of the scan, returning
/// how many; 0 once every slice has been claimed and read.
unsafe fn next_output(
    global: &HudiScanGlobal,
    local: &mut HudiScanLocal,
    output: duckdb_data_chunk,
) -> Result<usize, String> {
    loop {
        if let Some(batch) = &local.batch {
            if local.offset < batch.num_rows() {
                let len = vector_size().min(batch.num_rows() - local.offset);
                let piece = batch.slice(local.offset, len);
                local.offset += len;
                if global.projected.fields().is_empty() {
                    return Ok(len);
                }
                let chunk = OwnedChunk(import_batch(global_connection()?, &piece)?);
                for (out_col, src_col) in global.output_columns.iter().enumerate() {
                    if let Some(src_col) = src_col {
                        let out_vec = libduckdb_sys::duckdb_data_chunk_get_vector(output, out_col as idx_t);
                        let src_vec = libduckdb_sys::duckdb_data_chunk_get_vector(chunk.0, *src_col as idx_t);
                        libduckdb_sys::duckdb_vector_reference_vector(out_vec, src_vec);
                    }
                }
                local.current = Some(chunk);
                return Ok(len);
            }
            local.batch = None;
        }
        if let Some(stream) = &mut local.stream {
            match runtime().block_on(stream.next()) {
                Some(Ok(batch)) => {
                    local.batch = Some(align_batch(&batch, &global.projected)?);
                    local.offset = 0;
                }
                Some(Err(e)) => return Err(format!("Hudi read failed: {e}")),
                None => local.stream = None,
            }
            continue;
        }
        let claimed = global
            .next_slice
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let Some(slice) = global.bind.slices.get(claimed) else {
            return Ok(0);
        };
        let stream = runtime()
            .block_on(global.bind.reader.read_file_slice_stream(slice, &global.slice_options))
            .map_err(|e| format!("Hudi read failed: {e}"))?;
        local.stream = Some(stream);
    }
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

// ── hudi_mdt(uri) / hudi_commits(uri) — index + timeline table functions ──
// "More Hudi indexes under DuckDB SQL" (T1): expose the metadata table (where all
// Hudi indexes live — files/column_stats/bloom_filters/record_index/
// partition_stats/secondary_index_*) and the commit timeline as queryable rows.
// Both build their RecordBatches in bind, bridge them onto the global connection,
// declare the schema from the first chunk, and let the shared `hudi_init` /
// `hudi_scan` walk the chunks. Only the *bind* (how the batches are produced)
// differs.

/// Bridge already-produced record batches into a [`HudiBindData`]: import each onto
/// the global connection over the Arrow C Data Interface, in pieces of at most one
/// vector.
fn bridge_batches(batches: Vec<RecordBatch>) -> Result<HudiBindData, String> {
    let con = global_connection()?;
    let mut chunks = Vec::with_capacity(batches.len().max(1));
    for b in &batches {
        import_batch_in_vectors(con, b, &mut chunks)?;
    }
    Ok(HudiBindData { batches, chunks })
}

/// Declare the result schema for a bind from `bind_data`: per-column logical types
/// from the first chunk, copied into the bind by `duckdb_bind_add_result_column`.
/// Column names come from the Arrow schema of the first batch. Shared by the
/// metadata table function binds.
fn declare_bind_schema(
    info: duckdb_bind_info,
    bind_info: &BindInfo,
    bind_data: &HudiBindData,
    label: &str,
) {
    let first_chunk = bind_data.chunks[0].0.as_ptr() as libduckdb_sys::duckdb_data_chunk;
    let ncols = unsafe { libduckdb_sys::duckdb_data_chunk_get_column_count(first_chunk) } as usize;
    let names: Vec<String> = bind_data.batches[0]
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    let cnames: Result<Vec<std::ffi::CString>, _> =
        names.iter().map(|s| std::ffi::CString::new(s.as_str())).collect();
    let Ok(cnames) = cnames else {
        bind_info.set_error(&format!("{label}: a column name is not valid UTF-8"));
        return;
    };
    for i in 0..ncols {
        let vec = unsafe { libduckdb_sys::duckdb_data_chunk_get_vector(first_chunk, i as idx_t) };
        // SAFETY: same contract as hudi_bind — `duckdb_vector_get_column_type`
        // returns a FRESH owned handle; `duckdb_bind_add_result_column` copies it.
        let ty = unsafe { libduckdb_sys::duckdb_vector_get_column_type(vec) };
        unsafe {
            libduckdb_sys::duckdb_bind_add_result_column(info, cnames[i].as_ptr(), ty);
            let mut ty2 = ty;
            libduckdb_sys::duckdb_destroy_logical_type(&mut ty2);
        }
    }
}

/// `hudi_mdt(uri)`: the table's **`files` index** as rows — one row per data file
/// (`partition_path`, `file_name`, `size`, `is_deleted`). Hudi's `files` index
/// stores, per partition, a map `file_name -> {size, isDeleted}`; this reads that
/// partition through the public kernel API (`read_metadata_table_files_partition_arrow`,
/// the same path the kernel uses for MDT-backed file discovery) and decodes the
/// map into flat rows, so the index content is queryable in SQL:
/// `SELECT partition_path, file_name, size FROM hudi_mdt('...') WHERE is_deleted = false`.
///
/// Why a flat decoder and not a raw dump: every Hudi MDT record's payload is an
/// Avro **union-of-records** (files/column_stats/bloom/record_index/secondary),
/// which (a) hudi-rs 0.5.0's `avro_to_arrow` cannot decode through the public
/// `Table::read()` path (unimplemented `AvroSchema::Ref`), and (b) DuckDB's
/// `schema_from_arrow` cannot import dense Arrow unions (`Unsupported Arrow Type
/// "d" Union`). Decoding to primitives avoids both. The other index partitions
/// (`record_index`, `bloom_filters`, `column_stats`, `partition_stats`,
/// `secondary_index_*`) need a small hudi-rs visibility PR (`partition_reader` is
/// `pub(crate)` today) plus per-index decoders — tracked as T2.
///
/// Errors cleanly when the table has no metadata table enabled.
fn hudi_mdt_batches(uri: &str) -> Result<Vec<RecordBatch>, String> {
    use arrow::array::{Array, BooleanArray, Int64Array, MapArray, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    runtime().block_on(async {
        let table = Table::new(uri)
            .await
            .map_err(|e| format!("failed to open Hudi table at {uri}: {e}"))?;
        if !table.is_metadata_table_enabled() {
            return Err(format!(
                "hudi_mdt: table at {uri} has no metadata table enabled \
                 (no MDT partitions configured); hudi_mdt needs hoodie.metadata.enable / a \
                 files MDT partition"
            ));
        }
        // `&[]` = every key = the whole `files` partition. One RecordBatch carrying
        // the HoodieMetadataRecord schema (a nullable `filesystemMetadata` map per row).
        let batch = table
            .read_metadata_table_files_partition_arrow(&[])
            .await
            .map_err(|e| format!("Hudi metadata-table (files partition) read failed: {e}"))?;

        // Decode the `filesystemMetadata` map into flat per-file rows.
        let mut partition_path: Vec<String> = Vec::new();
        let mut file_name: Vec<String> = Vec::new();
        let mut size: Vec<i64> = Vec::new();
        let mut is_deleted: Vec<bool> = Vec::new();

        let Some(key_col) = batch.column_by_name("key") else {
            return Err("hudi_mdt: files-partition record is missing the `key` (partition path) column".into());
        };
        let Some(fs_col) = batch.column_by_name("filesystemMetadata") else {
            return Err("hudi_mdt: files-partition record is missing the `filesystemMetadata` column".into());
        };
        let keys = key_col.as_any().downcast_ref::<StringArray>()
            .ok_or("hudi_mdt: `key` column is not a string array")?;
        let maps = fs_col.as_any().downcast_ref::<MapArray>()
            .ok_or("hudi_mdt: `filesystemMetadata` column is not a map array")?;

        for i in 0..batch.num_rows() {
            if maps.is_null(i) || keys.is_null(i) {
                continue;
            }
            let part = keys.value(i).to_string();
            // The map value is a struct { size: Int64, isDeleted: Bool }; MapArray::value
            // returns that {key, value} struct as a StructArray.
            let entry = maps.value(i);
            let entry_keys = entry.column(0).as_any().downcast_ref::<StringArray>()
                .ok_or("hudi_mdt: files-map keys are not strings")?;
            let entry_vals = entry.column(1)
                .as_any().downcast_ref::<arrow::array::StructArray>()
                .ok_or("hudi_mdt: files-map values are not a struct")?;
            let v_size = entry_vals.column(0).as_any().downcast_ref::<Int64Array>()
                .ok_or("hudi_mdt: files-map `size` is not Int64")?;
            let v_del = entry_vals.column(1).as_any().downcast_ref::<BooleanArray>()
                .ok_or("hudi_mdt: files-map `isDeleted` is not Boolean")?;
            for j in 0..entry.len() {
                partition_path.push(part.clone());
                file_name.push(entry_keys.value(j).to_string());
                size.push(v_size.value(j));
                is_deleted.push(v_del.value(j));
            }
        }

        let schema = Schema::new(vec![
            Field::new("partition_path", DataType::Utf8, false),
            Field::new("file_name", DataType::Utf8, false),
            Field::new("size", DataType::Int64, false),
            Field::new("is_deleted", DataType::Boolean, false),
        ]);
        let cols: Vec<arrow::array::ArrayRef> = vec![
            Arc::new(StringArray::from(partition_path)),
            Arc::new(StringArray::from(file_name)),
            Arc::new(Int64Array::from(size)),
            Arc::new(BooleanArray::from(is_deleted)),
        ];
        let flat = RecordBatch::try_new(Arc::new(schema), cols)
            .map_err(|e| format!("failed to build hudi_mdt (files index) batch: {e}"))?;
        Ok(vec![flat])
    })
}

/// `hudi_commits(uri)`: the table's commit timeline as rows (one per completed
/// commit): `timestamp`, `action` (commit/deltacommit/replacecommit/…),
/// `completion` (v8+ completion instant, NULL otherwise), `epoch_millis`.
fn hudi_commits_batches(uri: &str) -> Result<Vec<RecordBatch>, String> {
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    runtime().block_on(async {
        let table = Table::new(uri)
            .await
            .map_err(|e| format!("failed to open Hudi table at {uri}: {e}"))?;
        let commits = table.get_timeline().completed_commits.clone();

        let n = commits.len();
        let ts: Vec<String> = commits.iter().map(|c| c.timestamp.clone()).collect();
        let action: Vec<String> = commits.iter().map(|c| c.action.as_ref().to_string()).collect();
        let completion: Vec<Option<String>> =
            commits.iter().map(|c| c.completion_timestamp.clone()).collect();
        let epoch: Vec<i64> = commits.iter().map(|c| c.epoch_millis).collect();

        let schema = Schema::new(vec![
            Field::new("timestamp", DataType::Utf8, false),
            Field::new("action", DataType::Utf8, false),
            Field::new("completion", DataType::Utf8, true),
            Field::new("epoch_millis", DataType::Int64, false),
        ]);
        let cols: Vec<arrow::array::ArrayRef> = vec![
            Arc::new(StringArray::from(ts)),
            Arc::new(StringArray::from(action)),
            Arc::new(StringArray::from(completion)),
            Arc::new(Int64Array::from(epoch)),
        ];
        let batch = RecordBatch::try_new(Arc::new(schema), cols)
            .map_err(|e| format!("failed to build hudi_commits batch: {e}"))?;
        Ok(if n == 0 { vec![] } else { vec![batch] })
    })
}

/// bind for `hudi_mdt`: produce the files-partition batch (the `files` index),
/// bridge it (a zero-row batch still carries the schema), declare the schema.
unsafe extern "C" fn hudi_mdt_bind(info: duckdb_bind_info) {
    let bind_info = BindInfo::new(info);
    let uri = match bind_info.get_parameter_value(0).as_str() {
        Ok(s) => s,
        Err(_) => {
            bind_info.set_error("hudi_mdt: the table path (uri) argument is required or not a string");
            return;
        }
    };
    if uri.is_empty() {
        bind_info.set_error("hudi_mdt: the table path (uri) is required");
        return;
    }
    let batches = match hudi_mdt_batches(&uri) {
        Ok(b) => b,
        Err(e) => {
            bind_info.set_error(&e);
            return;
        }
    };
    let bind_data = match bridge_batches(batches) {
        Ok(d) => d,
        Err(e) => {
            bind_info.set_error(&e);
            return;
        }
    };
    declare_bind_schema(info, &bind_info, &bind_data, "hudi_mdt");
    FfiBindData::<Arc<HudiBindData>>::set(info, Arc::new(bind_data));
}

/// bind for `hudi_commits`: produce the timeline batch (empty → zero-row schema).
unsafe extern "C" fn hudi_commits_bind(info: duckdb_bind_info) {
    let bind_info = BindInfo::new(info);
    let uri = match bind_info.get_parameter_value(0).as_str() {
        Ok(s) => s,
        Err(_) => {
            bind_info.set_error("hudi_commits: the table path (uri) argument is required or not a string");
            return;
        }
    };
    if uri.is_empty() {
        bind_info.set_error("hudi_commits: the table path (uri) is required");
        return;
    }
    let batches = match hudi_commits_batches(&uri) {
        Ok(b) => b,
        Err(e) => {
            bind_info.set_error(&e);
            return;
        }
    };
    let batches = if batches.is_empty() { hudi_commits_empty_batch() } else { batches };
    let bind_data = match bridge_batches(batches) {
        Ok(d) => d,
        Err(e) => {
            bind_info.set_error(&e);
            return;
        }
    };
    declare_bind_schema(info, &bind_info, &bind_data, "hudi_commits");
    FfiBindData::<Arc<HudiBindData>>::set(info, Arc::new(bind_data));
}

/// A zero-row batch with the fixed hudi_commits schema (empty timeline).
fn hudi_commits_empty_batch() -> Vec<RecordBatch> {
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    let schema = Schema::new(vec![
        Field::new("timestamp", DataType::Utf8, false),
        Field::new("action", DataType::Utf8, false),
        Field::new("completion", DataType::Utf8, true),
        Field::new("epoch_millis", DataType::Int64, false),
    ]);
    let cols: Vec<arrow::array::ArrayRef> = vec![
        Arc::new(StringArray::from(Vec::<String>::new())),
        Arc::new(StringArray::from(Vec::<String>::new())),
        Arc::new(StringArray::from(Vec::<Option<String>>::new())),
        Arc::new(Int64Array::from(Vec::<i64>::new())),
    ];
    match RecordBatch::try_new(Arc::new(schema), cols) {
        Ok(b) => vec![b],
        Err(_) => Vec::new(),
    }
}

// ── hudi_stats(uri) — the column_stats index under DuckDB SQL ──────────────
// Per data file × column: file-level min/max + row count, exactly the statistics
// Hudi's file pruner consults to skip files. Sourced from the Parquet footer and
// handed to hudi-rs's `StatisticsContainer::from_parquet_metadata` — the same code
// path the kernel's pruning uses — so the values are the kernel's, not a second
// implementation. Files are fetched via the table's public `Storage::get_file_data`
// (honours object-store options); the footer is parsed from the resident bytes.

/// `BoxFuture<'a, parquet::Result<Bytes>>` spelled without the `futures` crate.
type ParquetSuffixFuture<'a> = std::pin::Pin<
    Box<dyn std::future::Future<Output = parquet::errors::Result<bytes::Bytes>> + Send + 'a>,
>;

/// In-memory byte source for a parquet footer fetch. `hudi::storage::StorageReader`
/// is private to hudi-core (and the orphan rule would bar the impl on it anyway),
/// so each file is pulled via the public `Storage::get_file_data` (which honours the
/// table's object-store options) and the footer is sliced out of the resident bytes.
/// Whole-file fetch, not a ranged one — there is no public range-read on `Storage`
/// today; for the stats use case (parquet data files) that is the cost of correctness.
/// `load_via_suffix_and_finish` then parses just the footer from the tail.
struct FooterSource {
    bytes: bytes::Bytes,
}

impl parquet::arrow::async_reader::MetadataFetch for &mut FooterSource {
    fn fetch(&mut self, range: std::ops::Range<u64>) -> ParquetSuffixFuture<'_> {
        let len = self.bytes.len() as u64;
        let start = range.start.min(len) as usize;
        let end = range.end.min(len) as usize;
        let slice = self.bytes.slice(start..end);
        Box::pin(async move { Ok(slice) })
    }
}

impl parquet::arrow::async_reader::MetadataSuffixFetch for &mut FooterSource {
    fn fetch_suffix(&mut self, suffix: usize) -> ParquetSuffixFuture<'_> {
        let slice = self.bytes.slice(self.bytes.len().saturating_sub(suffix)..);
        Box::pin(async move { Ok(slice) })
    }
}

/// One parquet footer's file-level column stats, as flat rows.
fn footer_stats_rows(
    file: &str,
    container: &hudi::statistics::StatisticsContainer,
) -> Vec<(String, String, String, Option<String>, Option<String>, i64)> {
    let num_rows = container.num_rows.unwrap_or(0);
    let mut names: Vec<&String> = container.columns.keys().collect();
    names.sort();
    names
        .into_iter()
        .map(|name| {
            let stats = &container.columns[name];
            let min = stats
                .min_value
                .as_ref()
                .map(|a| arrow_cast::display::array_value_to_string(a.as_ref(), 0).unwrap_or_default());
            let max = stats
                .max_value
                .as_ref()
                .map(|a| arrow_cast::display::array_value_to_string(a.as_ref(), 0).unwrap_or_default());
            (
                file.to_string(),
                name.clone(),
                format!("{}", stats.data_type),
                min,
                max,
                num_rows,
            )
        })
        .collect()
}

/// `hudi_stats(uri)`: one row per (data file, column) with the file-level min/max
/// from the parquet footer. Files without a parquet base (log-only slices,
/// HFile/Lance base files) are skipped.
fn hudi_stats_batches(uri: &str) -> Result<Vec<RecordBatch>, String> {
    runtime().block_on(async {
        use hudi::storage::Storage;
        use parquet::arrow::async_reader::MetadataSuffixFetch;
        use parquet::file::metadata::ParquetMetaDataReader;

        let table = Table::new(uri)
            .await
            .map_err(|e| format!("failed to open Hudi table at {uri}: {e}"))?;
        let schema = table
            .get_schema()
            .await
            .map_err(|e| format!("failed to resolve schema at {uri}: {e}"))?;
        let slices = table
            .get_file_slices(&hudi::table::ReadOptions::new())
            .await
            .map_err(|e| format!("failed to list file slices at {uri}: {e}"))?;
        let storage = Storage::new(Arc::new(table.storage_options()), Arc::clone(&table.hudi_configs))
            .map_err(|e| format!("failed to open storage for {uri}: {e}"))?;

        let mut rows: Vec<(String, String, String, Option<String>, Option<String>, i64)> = Vec::new();
        for fs in &slices {
            let Some(base) = fs.base_file.as_ref() else {
                continue;
            };
            if !base.extension.eq_ignore_ascii_case("parquet") {
                continue;
            }
            let rel = fs
                .base_file_relative_path()
                .map_err(|e| format!("no base file path for {uri}: {e}"))?;
            let Some(rel) = rel else {
                continue;
            };
            let bytes = storage
                .get_file_data(&rel)
                .await
                .map_err(|e| format!("failed to read {rel} at {uri}: {e}"))?;
            let mut src = FooterSource { bytes };
            let meta = ParquetMetaDataReader::new()
                .load_via_suffix_and_finish(&mut src)
                .await
                .map_err(|e| format!("failed to read parquet footer {rel} at {uri}: {e}"))?;
            let container =
                hudi::statistics::StatisticsContainer::from_parquet_metadata(&meta, &schema);
            rows.extend(footer_stats_rows(&rel, &container));
        }

        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("file", arrow::datatypes::DataType::Utf8, false),
            arrow::datatypes::Field::new("column", arrow::datatypes::DataType::Utf8, false),
            arrow::datatypes::Field::new("data_type", arrow::datatypes::DataType::Utf8, false),
            arrow::datatypes::Field::new("min_value", arrow::datatypes::DataType::Utf8, true),
            arrow::datatypes::Field::new("max_value", arrow::datatypes::DataType::Utf8, true),
            arrow::datatypes::Field::new("num_rows", arrow::datatypes::DataType::Int64, false),
        ]));
        use arrow::array::{Int64Array, StringArray};
        let cols: Vec<arrow::array::ArrayRef> = vec![
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.0.as_str()).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.1.as_str()).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.2.as_str()).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.3.as_deref()).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.4.as_deref()).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.5).collect::<Vec<_>>(),
            )),
        ];
        RecordBatch::try_new(schema, cols)
            .map(|b| vec![b])
            .map_err(|e| format!("failed to build hudi_stats batch: {e}"))
    })
}

/// bind for `hudi_stats`: produce the column-stats batch (a zero-row batch still
/// carries the schema for empty tables), bridge, declare schema.
unsafe extern "C" fn hudi_stats_bind(info: duckdb_bind_info) {
    let bind_info = BindInfo::new(info);
    let uri = match bind_info.get_parameter_value(0).as_str() {
        Ok(s) => s,
        Err(_) => {
            bind_info.set_error("hudi_stats: the table path (uri) argument is required or not a string");
            return;
        }
    };
    if uri.is_empty() {
        bind_info.set_error("hudi_stats: the table path (uri) is required");
        return;
    }
    let batches = match hudi_stats_batches(&uri) {
        Ok(b) => b,
        Err(e) => {
            bind_info.set_error(&e);
            return;
        }
    };
    let bind_data = match bridge_batches(batches) {
        Ok(d) => d,
        Err(e) => {
            bind_info.set_error(&e);
            return;
        }
    };
    declare_bind_schema(info, &bind_info, &bind_data, "hudi_stats");
    FfiBindData::<Arc<HudiBindData>>::set(info, Arc::new(bind_data));
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
                .projection_pushdown(true)
                .bind(hudi_bind)
                .init(hudi_scan_init)
                .local_init(hudi_scan_local_init)
                .scan(hudi_scan_scan),
        )?;
        reg.register_table(
            TableFunctionBuilder::new("hudi_mdt")
                .param(quack_rs::types::TypeId::Varchar)
                .bind(hudi_mdt_bind)
                .init(hudi_init)
                .scan(hudi_scan),
        )?;
        reg.register_table(
            TableFunctionBuilder::new("hudi_commits")
                .param(quack_rs::types::TypeId::Varchar)
                .bind(hudi_commits_bind)
                .init(hudi_init)
                .scan(hudi_scan),
        )?;
        reg.register_table(
            TableFunctionBuilder::new("hudi_stats")
                .param(quack_rs::types::TypeId::Varchar)
                .bind(hudi_stats_bind)
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

    use super::{
        align_batch, hudi_read_batches, parse_hudi_option, plan_hudi_scan, read_options_for,
        runtime, HudiQueryArgs,
    };
    use arrow::array::{Array, Float64Array, Int32Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use futures::StreamExt;
    use std::sync::Arc;

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

    // ── batch alignment (pure, no fixtures) ────────────────────────────────

    #[test]
    fn align_batch_reorders_and_casts_by_name() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("a", DataType::Int32, false),
                Field::new("b", DataType::Float64, false),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(Float64Array::from(vec![0.5, 1.5])),
            ],
        )
        .unwrap();
        let target = Arc::new(Schema::new(vec![
            Field::new("b", DataType::Float64, true),
            Field::new("a", DataType::Int64, true),
        ]));
        let aligned = align_batch(&batch, &target).unwrap();
        assert_eq!(aligned.schema(), target);
        let a = aligned.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(a.values(), &[1, 2]);
    }

    #[test]
    fn align_batch_rejects_a_missing_column() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)])),
            vec![Arc::new(Int32Array::from(vec![1]))],
        )
        .unwrap();
        let target = Arc::new(Schema::new(vec![Field::new("z", DataType::Int32, true)]));
        let err = align_batch(&batch, &target).unwrap_err();
        assert!(err.contains("\"z\""), "{err}");
    }

    // ── the scan's kernel path against the in-repo fixture ─────────────────

    /// A zipped `test/fixture/<name>.zip` inflated into its own temporary
    /// directory, removed on drop.
    struct InflatedFixture {
        dir: std::path::PathBuf,
        table: String,
    }

    impl InflatedFixture {
        fn new(name: &str) -> Self {
            let archive = format!("{}/test/fixture/{name}.zip", env!("CARGO_MANIFEST_DIR"));
            let dir = std::env::temp_dir().join(format!("duckdb-hudi-{name}-{}", std::process::id()));
            let _ = std::fs::remove_dir_all(&dir);
            let status = std::process::Command::new("unzip")
                .args(["-q", &archive, "-d"])
                .arg(&dir)
                .status()
                .expect("run unzip");
            assert!(status.success(), "unzip {archive} failed: {status}");
            let table = dir.join(name).to_string_lossy().into_owned();
            Self { dir, table }
        }
    }

    impl Drop for InflatedFixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }


    /// Plans the 5000-row single-file fixture the way `read_hudi` does and streams
    /// its slices with a projection, checking the batches stay within the requested
    /// size and carry the same rows `Table::read` returns.
    #[test]
    fn planned_scan_streams_projected_batches() {
        let fixture = InflatedFixture::new("cow_single_file_5000");
        let p = fixture.table.as_str();
        let plan = plan_hudi_scan(p, &HudiQueryArgs::default()).expect("plan");
        assert_eq!(plan.slices.len(), 1);
        assert!(plan.schema.field_with_name("_hoodie_commit_time").is_ok());
        let projected = Arc::new(Schema::new(vec![
            plan.schema.field_with_name("price").unwrap().clone(),
            plan.schema.field_with_name("id").unwrap().clone(),
        ]));
        let options = super::ReadOptions::new()
            .with_batch_size(2048)
            .unwrap()
            .with_projection(["price", "id"]);
        let mut rows = 0;
        let mut id_sum = 0i64;
        for slice in &plan.slices {
            let mut stream = runtime()
                .block_on(plan.reader.read_file_slice_stream(slice, &options))
                .expect("stream");
            while let Some(batch) = runtime().block_on(stream.next()) {
                let batch = align_batch(&batch.expect("batch"), &projected).expect("align");
                assert!(batch.num_rows() <= 2048, "batch of {} rows", batch.num_rows());
                let ids = batch.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
                id_sum += ids.iter().flatten().sum::<i64>();
                rows += batch.num_rows();
            }
        }
        let reference = hudi_read_batches(p, &HudiQueryArgs::default()).expect("read");
        assert_eq!(rows as u64, total_rows(&reference));
        assert_eq!(id_sum, 12_502_500);
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
