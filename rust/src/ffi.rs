//! The C ABI the C++ extension calls. Conventions: every function that can fail
//! returns 0 on success and non-zero on failure, with the message available from
//! `hudi_last_error()` on the same thread until the next call; every handle has
//! an explicit free; Arrow data crosses as C Data Interface structs; a panic in
//! the kernel is caught and reported as an error, never unwound into DuckDB.
//!
//! Every `unsafe extern "C"` function takes raw pointers from C++: a handle
//! this library returned and has not freed, NUL-terminated strings, arrays of
//! the stated length, and an out pointer to writable memory of the right type.
#![allow(clippy::missing_safety_doc)]

use std::cell::RefCell;
use std::ffi::{c_char, CStr, CString};
use std::sync::Arc;

use arrow::array::RecordBatchReader;
use arrow::datatypes::{Schema, SchemaRef};
use arrow::error::ArrowError;
use arrow::ffi::FFI_ArrowSchema;
use arrow::ffi_stream::FFI_ArrowArrayStream;
use arrow::record_batch::{RecordBatch, RecordBatchIterator};
use futures::stream::{BoxStream, StreamExt};

use crate::kernel::{self, FilterSpec, HudiQueryArgs, OpenedTable, ScanPlan};

thread_local! {
    static LAST_ERROR: RefCell<Option<CString>> = const { RefCell::new(None) };
}

fn set_last_error(message: String) {
    let c = CString::new(message.replace('\0', "\\0"))
        .unwrap_or_else(|_| CString::new("hudi: error message not representable").unwrap());
    LAST_ERROR.with(|e| *e.borrow_mut() = Some(c));
}

/// The message of the last failed call on this thread, or null. Valid until
/// the next call into this library on the same thread.
#[no_mangle]
pub extern "C" fn hudi_last_error() -> *const c_char {
    LAST_ERROR.with(|e| e.borrow().as_ref().map_or(std::ptr::null(), |c| c.as_ptr()))
}

fn panic_message(payload: &Box<dyn std::any::Any + Send>) -> String {
    payload
        .downcast_ref::<&str>()
        .map(|s| s.to_string())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "unknown panic".to_string())
}

/// Run `body` under a panic guard, mapping its outcome to the C return code.
fn guarded(body: impl FnOnce() -> Result<(), String>) -> i32 {
    let outcome =
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(body)).unwrap_or_else(|payload| {
            Err(format!("hudi: internal error: {}", panic_message(&payload)))
        });
    match outcome {
        Ok(()) => {
            LAST_ERROR.with(|e| *e.borrow_mut() = None);
            0
        }
        Err(message) => {
            set_last_error(message);
            1
        }
    }
}

unsafe fn required_str<'a>(ptr: *const c_char, what: &str) -> Result<&'a str, String> {
    if ptr.is_null() {
        return Err(format!("hudi: {what} is null"));
    }
    CStr::from_ptr(ptr)
        .to_str()
        .map_err(|e| format!("hudi: {what} is not valid UTF-8: {e}"))
}

/// `from_raw_parts` without the null pointer a zero-length C array may carry.
unsafe fn optional_slice<'a, T>(ptr: *const T, len: usize) -> &'a [T] {
    if len == 0 || ptr.is_null() {
        &[]
    } else {
        std::slice::from_raw_parts(ptr, len)
    }
}

unsafe fn optional_string(ptr: *const c_char, what: &str) -> Result<Option<String>, String> {
    if ptr.is_null() {
        Ok(None)
    } else {
        required_str(ptr, what).map(|s| Some(s.to_string()))
    }
}

/// An opened table, its declared read schema, and the uri it was opened from.
pub struct HudiTable {
    uri: String,
    opened: OpenedTable,
    schema: SchemaRef,
}

/// A planned scan over one table.
pub struct HudiPlan {
    plan: ScanPlan,
    schema: SchemaRef,
}

#[allow(dead_code)]
fn assert_handles_are_thread_safe() {
    fn check<T: Send + Sync>() {}
    check::<HudiTable>();
    check::<HudiPlan>();
}

/// Open the Hudi table at `uri`. `hudi_options` is the `k=v; k2=v2` string of
/// the `hudi_option` parameter, or null.
#[no_mangle]
pub unsafe extern "C" fn hudi_table_open(
    uri: *const c_char,
    hudi_options: *const c_char,
    out: *mut *mut HudiTable,
) -> i32 {
    guarded(|| {
        if out.is_null() {
            return Err("hudi_table_open: out is null".into());
        }
        let uri = required_str(uri, "table uri")?.to_string();
        let hudi_options = optional_string(hudi_options, "hudi_option")?;
        let opened = kernel::open_table(&uri, hudi_options)?;
        let schema =
            kernel::read_schema(&opened.table).map_err(|e| format!("{e} (table {uri})"))?;
        *out = Box::into_raw(Box::new(HudiTable {
            uri,
            opened,
            schema,
        }));
        Ok(())
    })
}

#[no_mangle]
pub unsafe extern "C" fn hudi_table_free(table: *mut HudiTable) {
    if !table.is_null() {
        drop(Box::from_raw(table));
    }
}

/// Export the table's read schema into `out`, an `ArrowSchema`.
#[no_mangle]
pub unsafe extern "C" fn hudi_table_schema(
    table: *const HudiTable,
    out: *mut std::ffi::c_void,
) -> i32 {
    guarded(|| {
        let table = table.as_ref().ok_or("hudi_table_schema: table is null")?;
        if out.is_null() {
            return Err("hudi_table_schema: out is null".into());
        }
        let exported = FFI_ArrowSchema::try_from(table.schema.as_ref())
            .map_err(|e| format!("hudi: cannot export schema: {e}"))?;
        std::ptr::write(out as *mut FFI_ArrowSchema, exported);
        Ok(())
    })
}

/// `kind` 0: the `hudi_info` summary line; 1: the `hudi_files` listing, one
/// slice per line. Free the result with `hudi_string_free`.
#[no_mangle]
pub unsafe extern "C" fn hudi_table_describe(
    table: *const HudiTable,
    kind: i32,
    out: *mut *mut c_char,
) -> i32 {
    guarded(|| {
        let table = table.as_ref().ok_or("hudi_table_describe: table is null")?;
        if out.is_null() {
            return Err("hudi_table_describe: out is null".into());
        }
        let (info, files) = kernel::table_metadata(&table.uri, &table.opened.table)?;
        let text = match kind {
            0 => info,
            1 => files.join("\n"),
            other => return Err(format!("hudi_table_describe: unknown kind {other}")),
        };
        let c = CString::new(text.replace('\0', "\\0")).map_err(|e| format!("hudi: {e}"))?;
        *out = c.into_raw();
        Ok(())
    })
}

#[no_mangle]
pub unsafe extern "C" fn hudi_string_free(s: *mut c_char) {
    if !s.is_null() {
        drop(CString::from_raw(s));
    }
}

/// Hand `reader` to the caller as an `ArrowArrayStream` written into `out`.
unsafe fn export_stream(out: *mut std::ffi::c_void, reader: Box<dyn RecordBatchReader + Send>) {
    std::ptr::write(
        out as *mut FFI_ArrowArrayStream,
        FFI_ArrowArrayStream::new(reader),
    );
}

fn batches_reader(
    batches: Vec<RecordBatch>,
    schema: SchemaRef,
) -> Box<dyn RecordBatchReader + Send> {
    Box::new(RecordBatchIterator::new(
        batches.into_iter().map(Ok),
        schema,
    ))
}

/// `kind` 0: `hudi_mdt` (the files index); 1: `hudi_commits` (the timeline);
/// 2: `hudi_stats` (per file column statistics). Writes an `ArrowArrayStream`
/// into `out`; its schema is the function's result schema even when there are
/// no rows.
#[no_mangle]
pub unsafe extern "C" fn hudi_table_metadata_stream(
    table: *const HudiTable,
    kind: i32,
    out: *mut std::ffi::c_void,
) -> i32 {
    guarded(|| {
        let table = table
            .as_ref()
            .ok_or("hudi_table_metadata_stream: table is null")?;
        if out.is_null() {
            return Err("hudi_table_metadata_stream: out is null".into());
        }
        let batches = match kind {
            0 => kernel::mdt_batches(&table.opened.table)?,
            1 => kernel::commits_batches(&table.opened.table)?,
            2 => kernel::stats_batches(&table.opened.table)?,
            other => return Err(format!("hudi_table_metadata_stream: unknown kind {other}")),
        };
        let schema = batches
            .first()
            .map(|b| b.schema())
            .ok_or("hudi: metadata function produced no schema")?;
        export_stream(out, batches_reader(batches, schema));
        Ok(())
    })
}

/// The `read_hudi` named parameters, each null when absent.
#[repr(C)]
pub struct HudiReadArgs {
    pub as_of: *const c_char,
    pub start: *const c_char,
    pub end: *const c_char,
}

/// One pushed-down predicate: `column op values`, every value rendered as text.
#[repr(C)]
pub struct HudiFilter {
    pub column: *const c_char,
    pub op: *const c_char,
    pub values: *const *const c_char,
    pub n_values: usize,
}

/// What a plan kept after pruning.
#[repr(C)]
pub struct HudiPlanInfo {
    pub slice_count: usize,
    pub partition_count: usize,
    /// Rows in the kept base files, or -1 when a slice does not report a count.
    pub row_estimate: i64,
}

unsafe fn filter_specs(filters: *const HudiFilter, n: usize) -> Result<Vec<FilterSpec>, String> {
    if n == 0 {
        return Ok(Vec::new());
    }
    if filters.is_null() {
        return Err("hudi_plan_create: filters is null".into());
    }
    std::slice::from_raw_parts(filters, n)
        .iter()
        .map(|f| {
            let column = required_str(f.column, "filter column")?.to_string();
            let op = required_str(f.op, "filter operator")?.to_string();
            if f.n_values > 0 && f.values.is_null() {
                return Err("hudi_plan_create: filter values is null".into());
            }
            let values = optional_slice(f.values, f.n_values)
                .iter()
                .map(|v| required_str(*v, "filter value").map(str::to_string))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(FilterSpec { column, op, values })
        })
        .collect()
}

/// Plan the read `args` describes, pruning partitions and files with `filters`.
#[no_mangle]
pub unsafe extern "C" fn hudi_plan_create(
    table: *const HudiTable,
    args: *const HudiReadArgs,
    filters: *const HudiFilter,
    n_filters: usize,
    out: *mut *mut HudiPlan,
) -> i32 {
    guarded(|| {
        let table = table.as_ref().ok_or("hudi_plan_create: table is null")?;
        let args = args.as_ref().ok_or("hudi_plan_create: args is null")?;
        if out.is_null() {
            return Err("hudi_plan_create: out is null".into());
        }
        let query = HudiQueryArgs {
            as_of: optional_string(args.as_of, "as_of")?,
            start: optional_string(args.start, "start")?,
            end: optional_string(args.end, "end")?,
            hudi_options: None,
        };
        let filters = filter_specs(filters, n_filters)?;
        let plan = kernel::plan_scan(&table.opened, &query, filters)
            .map_err(|e| format!("{e} (table {})", table.uri))?;
        *out = Box::into_raw(Box::new(HudiPlan {
            plan,
            schema: Arc::clone(&table.schema),
        }));
        Ok(())
    })
}

#[no_mangle]
pub unsafe extern "C" fn hudi_plan_free(plan: *mut HudiPlan) {
    if !plan.is_null() {
        drop(Box::from_raw(plan));
    }
}

#[no_mangle]
pub unsafe extern "C" fn hudi_plan_info(plan: *const HudiPlan, out: *mut HudiPlanInfo) -> i32 {
    guarded(|| {
        let plan = plan.as_ref().ok_or("hudi_plan_info: plan is null")?;
        let out = out.as_mut().ok_or("hudi_plan_info: out is null")?;
        out.slice_count = plan.plan.slices.len();
        out.partition_count = plan.plan.partition_count();
        out.row_estimate = plan.plan.row_estimate().unwrap_or(-1);
        Ok(())
    })
}

#[derive(Debug)]
struct ReadError(String);

impl std::fmt::Display for ReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ReadError {}

fn read_error(message: String) -> ArrowError {
    ArrowError::ExternalError(Box::new(ReadError(message)))
}

/// One slice's merged rows as a synchronous batch reader. Every batch is
/// shaped to the projected columns in order; the types are the declared ones,
/// except that a string column keeps the encoding (view or dictionary) the
/// kernel read it with. The first batch is pulled when the reader is created,
/// so the stream's schema reports those encodings before any batch is handed
/// out, and later batches are cast to the same schema.
struct SliceBatchReader {
    stream: BoxStream<'static, hudi::error::Result<RecordBatch>>,
    schema: SchemaRef,
    pending: Option<Result<RecordBatch, ArrowError>>,
}

impl SliceBatchReader {
    fn new(
        mut stream: BoxStream<'static, hudi::error::Result<RecordBatch>>,
        projected: SchemaRef,
    ) -> Self {
        let first = Self::pull(&mut stream, &projected, true);
        let schema = match &first {
            Some(Ok(batch)) => batch.schema(),
            _ => projected,
        };
        Self {
            stream,
            schema,
            pending: first,
        }
    }

    fn pull(
        stream: &mut BoxStream<'static, hudi::error::Result<RecordBatch>>,
        schema: &SchemaRef,
        keep_string_encodings: bool,
    ) -> Option<Result<RecordBatch, ArrowError>> {
        // Arrow's stream callbacks carry no panic guard of their own, and a
        // panic unwinding through them would abort the host process.
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            match kernel::runtime().block_on(stream.next()) {
                Some(Ok(batch)) => Some(
                    kernel::align_batch_with(&batch, schema, keep_string_encodings)
                        .map_err(read_error),
                ),
                Some(Err(e)) => Some(Err(read_error(format!("Hudi read failed: {e}")))),
                None => None,
            }
        }));
        match outcome {
            Ok(item) => item,
            Err(payload) => Some(Err(read_error(format!(
                "hudi: internal error: {}",
                panic_message(&payload)
            )))),
        }
    }
}

impl Iterator for SliceBatchReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(first) = self.pending.take() {
            return Some(first);
        }
        Self::pull(&mut self.stream, &self.schema, false)
    }
}

impl RecordBatchReader for SliceBatchReader {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

/// Open the stream of slice `slice_index`'s rows, projected to `columns` (in
/// that order) and masked by the plan's filters, in batches of about
/// `batch_size` rows. Writes an `ArrowArrayStream` into `out`.
#[no_mangle]
pub unsafe extern "C" fn hudi_plan_slice_stream(
    plan: *const HudiPlan,
    slice_index: usize,
    columns: *const *const c_char,
    n_columns: usize,
    batch_size: usize,
    out: *mut std::ffi::c_void,
) -> i32 {
    guarded(|| {
        let plan = plan
            .as_ref()
            .ok_or("hudi_plan_slice_stream: plan is null")?;
        if out.is_null() {
            return Err("hudi_plan_slice_stream: out is null".into());
        }
        let slice =
            plan.plan.slices.get(slice_index).ok_or_else(|| {
                format!("hudi_plan_slice_stream: slice {slice_index} out of range")
            })?;
        if n_columns > 0 && columns.is_null() {
            return Err("hudi_plan_slice_stream: columns is null".into());
        }
        let columns = optional_slice(columns, n_columns)
            .iter()
            .map(|c| required_str(*c, "column name").map(str::to_string))
            .collect::<Result<Vec<_>, _>>()?;
        let fields = columns
            .iter()
            .map(|name| {
                plan.schema
                    .field_with_name(name)
                    .cloned()
                    .map_err(|_| format!("read_hudi: no column {name:?} in the table schema"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let projected = Arc::new(Schema::new(fields));
        let options = kernel::slice_read_options(&plan.plan, &columns, batch_size)?;
        let stream = kernel::open_slice_stream(&plan.plan, slice, &options)?;
        export_stream(out, Box::new(SliceBatchReader::new(stream, projected)));
        Ok(())
    })
}
