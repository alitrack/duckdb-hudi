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
    duckdb_bind_info, duckdb_data_chunk, duckdb_function_info, duckdb_init_info, idx_t,
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

/// Keeps the global connection alive (drops = disconnect) for the process lifetime.
static GLOBAL_OWNED: std::sync::Mutex<Option<quack_rs::query::OwnedConnection>> =
    std::sync::Mutex::new(None);

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
struct HudiQueryArgs {
    /// `as_of =>` commit timestamp: time-travel to the latest commit <= this instant.
    as_of: Option<String>,
    /// `start =>` + `end =>` (both given): incremental read of commits in
    /// (`start`, `end`]. At least one of them implies `QueryType::Incremental`.
    start: Option<String>,
    end: Option<String>,
    /// `hudi_option =>` the raw `k=v; k2=v2` string. Parsed into `(key, value)`
    /// pairs inside `read_hudi_table` (cloud credentials `hoodie.s3…` /
    /// `hoodie.gcs…` / `hoodie.aws.region`, read tuning like
    /// `hoodie.file_group_reader.version`). Forwarded to both the table open and
    /// the read.
    hudi_options: Option<String>,
}

/// Read the Hudi table at `uri` via the hudi-rs kernel (single `block_on`) and import
/// every non-empty batch into a DuckDB chunk on the global connection.
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
        )
    }
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
            // Publish the raw pointer, then pin the OwnedConnection (process lifetime,
            // Drop = disconnect). A re-LOAD overwrites both; the dropped value is the
            // freshly-opened one (harmless: it is never used).
            GLOBAL_CON_RAW.store(raw, Ordering::Release);
            *GLOBAL_OWNED.lock().unwrap() = Some(owned);
            register_all(con)
        })
    }
}
