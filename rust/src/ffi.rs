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
use arrow::array::StructArray;
use arrow::datatypes::{Schema, SchemaRef};
use arrow::error::ArrowError;
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
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
/// Writes take `&mut` through a per-handle mutex: the kernel's write methods
/// mutate the table's timeline in place, and concurrent reads of the same
/// handle must not observe a half-reloaded timeline.
pub struct HudiTable {
    uri: String,
    opened: std::sync::Mutex<OpenedTable>,
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
            opened: std::sync::Mutex::new(opened),
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
        let opened = table
            .opened
            .lock()
            .map_err(|e| format!("hudi_table_describe: table handle poisoned: {e}"))?;
        let (info, files) = kernel::table_metadata(&table.uri, &opened.table)?;
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
        let opened = table
            .opened
            .lock()
            .map_err(|e| format!("hudi_table_metadata_stream: table handle poisoned: {e}"))?;
        let batches = match kind {
            0 => kernel::mdt_batches(&opened.table)?,
            1 => kernel::commits_batches(&opened.table)?,
            2 => kernel::stats_batches(&opened.table)?,
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
        let opened = table
            .opened
            .lock()
            .map_err(|e| format!("hudi_plan_create: table handle poisoned: {e}"))?;
        let plan = kernel::plan_scan(&opened, &query, filters)
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
/// except that a column keeps an encoding the kernel read it with when it
/// carries the same values (a string as a view or dictionary, a decimal in 64
/// bits). The first batch is pulled when the reader is created,
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
        keep_encodings: bool,
    ) -> Option<Result<RecordBatch, ArrowError>> {
        // Arrow's stream callbacks carry no panic guard of their own, and a
        // panic unwinding through them would abort the host process.
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            match kernel::runtime().block_on(stream.next()) {
                Some(Ok(batch)) => Some(
                    kernel::align_batch_with(&batch, schema, keep_encodings).map_err(read_error),
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

/// Create a new Hudi table at `uri`. `args` carries the table's identity and
/// layout (every field null when unset); `hudi_options` is the `k=v; k2=v2`
/// string. Returns a table handle usable for writes and reads.
#[no_mangle]
pub unsafe extern "C" fn hudi_table_create(
    uri: *const c_char,
    args: *const HudiCreateArgs,
    hudi_options: *const c_char,
    out: *mut *mut HudiTable,
) -> i32 {
    guarded(|| {
        if out.is_null() {
            return Err("hudi_table_create: out is null".into());
        }
        let uri = required_str(uri, "table uri")?.to_string();
        let args = args.as_ref().ok_or("hudi_table_create: args is null")?;
        let create = kernel::HudiCreateArgs {
            table_name: optional_string(args.table_name, "table_name")?,
            table_type: optional_string(args.table_type, "table_type")?,
            record_key: optional_string(args.record_key, "record_key")?,
            partition_fields: optional_string(args.partition_fields, "partition_fields")?,
            ordering_fields: optional_string(args.ordering_fields, "ordering_fields")?,
            populate_meta_fields: args.populate_meta_fields != 0,
            record_index: args.record_index != 0,
            metadata_table: args.metadata_table != 0,
        };
        let hudi_options = optional_string(hudi_options, "hudi_option")?;
        let opened = kernel::write_create_table(&uri, hudi_options, &create)?;
        // A fresh table has no commits yet; the read schema is unknown until
        // the first append. `hudi_table_append` refreshes it. Reads of an
        // empty table return the zero-row schema declared there.
        let schema: SchemaRef = Arc::new(Schema::new(Vec::<arrow::datatypes::Field>::new()));
        *out = Box::into_raw(Box::new(HudiTable {
            uri,
            opened: std::sync::Mutex::new(opened),
            schema,
        }));
        Ok(())
    })
}

/// The `hudi_create` named parameters, each null when absent.
#[repr(C)]
pub struct HudiCreateArgs {
    pub table_name: *const c_char,
    pub table_type: *const c_char,
    pub record_key: *const c_char,
    pub partition_fields: *const c_char,
    pub ordering_fields: *const c_char,
    pub populate_meta_fields: i32,
    pub record_index: i32,
    pub metadata_table: i32,
}

/// Append the rows of `stream` (an Arrow C Data Interface stream, moved into
/// this call) to the table as one commit. The stream's schema is the table's
/// data schema in column names. Refreshes the handle's read schema to the
/// first batch's. Free the result with `hudi_string_free`.
#[no_mangle]
pub unsafe extern "C" fn hudi_table_append(
    table: *mut HudiTable,
    stream: *mut FFI_ArrowArrayStream,
    out: *mut *mut c_char,
) -> i32 {
    guarded(|| {
        let table = table.as_mut().ok_or("hudi_table_append: table is null")?;
        if stream.is_null() {
            return Err("hudi_table_append: stream is null".into());
        }
        if out.is_null() {
            return Err("hudi_table_append: out is null".into());
        }
        // Move the stream in; ownership crosses the boundary here.
        let ffi_stream = std::ptr::read(stream);
        let reader = arrow::ffi_stream::ArrowArrayStreamReader::try_new(ffi_stream)
            .map_err(|e| format!("hudi_table_append: cannot import stream: {e}"))?;
        let batches: Vec<RecordBatch> = reader
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| format!("hudi_table_append: stream read failed: {e}"))?;
        if batches.is_empty() {
            return Err("hudi_table_append: stream carried no rows".into());
        }
        let first_schema = Arc::clone(&batches[0].schema());
        let opened = &mut *table
            .opened
            .lock()
            .map_err(|e| format!("hudi_table_append: table handle poisoned: {e}"))?;
        let outcome = kernel::write_append_table(opened, batches)?;
        table.schema = first_schema;
        let text = render_outcome(&outcome);
        let c = CString::new(text).map_err(|e| format!("hudi: {e}"))?;
        *out = c.into_raw();
        Ok(())
    })
}

/// Delete the rows matching one `column op value` predicate (the same
/// operators `read_hudi` takes: `=`, `!=`, `<`, `<=`, `>`, `>=`, `IN`,
/// `NOT IN`). Free the result with `hudi_string_free`.
#[no_mangle]
pub unsafe extern "C" fn hudi_table_delete(
    table: *mut HudiTable,
    column: *const c_char,
    op: *const c_char,
    values: *const *const c_char,
    n_values: usize,
    out: *mut *mut c_char,
) -> i32 {
    guarded(|| {
        let table = table.as_mut().ok_or("hudi_table_delete: table is null")?;
        if out.is_null() {
            return Err("hudi_table_delete: out is null".into());
        }
        let column = required_str(column, "delete column")?.to_string();
        let op = required_str(op, "delete operator")?.to_string();
        if n_values > 0 && values.is_null() {
            return Err("hudi_table_delete: values is null".into());
        }
        let values = optional_slice(values, n_values)
            .iter()
            .map(|v| required_str(*v, "delete value").map(str::to_string))
            .collect::<Result<Vec<_>, _>>()?;
        let multi = op.eq_ignore_ascii_case("IN") || op.eq_ignore_ascii_case("NOT IN");
        if multi && values.is_empty() {
            return Err("hudi_table_delete: IN/NOT IN need at least one value".into());
        }
        // Quote the values unless the column is a bare numeric type: a record
        // key on an Int64 column takes `id = 2`, not `id = '2'`, and hudi's
        // record-key delete route type-checks the literal.
        let quoted = {
            let dt = table
                .schema
                .field_with_name(&column)
                .map(|f| f.data_type())
                .ok()
                .cloned();
            match dt {
                Some(dt) => !value_needs_quotes(&dt),
                None => true,
            }
        };
        let render = |v: &str| -> String {
            if quoted {
                v.to_string()
            } else {
                quote_value(v)
            }
        };
        let predicate = if values.len() == 1 && !multi {
            format!("{column} {op} {}", render(&values[0]))
        } else {
            format!(
                "{column} {op} ({})",
                values.iter().map(|v| render(v)).collect::<Vec<_>>().join(", ")
            )
        };
        let opened = &mut *table
            .opened
            .lock()
            .map_err(|e| format!("hudi_table_delete: table handle poisoned: {e}"))?;
        let outcome = kernel::write_delete_table(opened, &predicate)?;
        let text = render_outcome(&outcome);
        let c = CString::new(text).map_err(|e| format!("hudi: {e}"))?;
        *out = c.into_raw();
        Ok(())
    })
}

/// Append one `RecordBatch` exported into `array` (an Arrow C Data Interface
/// array, moved into this call; the matching `schema` is only read) to the
/// table as one commit. This is the entry point for the C++ `COPY ... TO
/// (FORMAT hudi)` sink, which accumulates the query's `DataChunk`s with
/// DuckDB's `ArrowAppender` and hands over its `Finalize()` result. Free the
/// result with `hudi_string_free`.
#[no_mangle]
pub unsafe extern "C" fn hudi_table_append_array(
    table: *mut HudiTable,
    array: *mut FFI_ArrowArray,
    schema: *const FFI_ArrowSchema,
    out: *mut *mut c_char,
) -> i32 {
    guarded(|| {
        let table = table.as_mut().ok_or("hudi_table_append_array: table is null")?;
        if array.is_null() {
            return Err("hudi_table_append_array: array is null".into());
        }
        if schema.is_null() {
            return Err("hudi_table_append_array: schema is null".into());
        }
        if out.is_null() {
            return Err("hudi_table_append_array: out is null".into());
        }
        let schema = &*schema;
        // Import the C array: ownership of the buffers crosses the boundary
        // here; `from_ffi` releases it exactly once when the ArrayData drops.
        let data = arrow::ffi::from_ffi(std::ptr::read(array), schema)
            .map_err(|e| format!("hudi_table_append_array: cannot import array: {e}"))?;
        let struct_array = StructArray::from(data);
        let batch = RecordBatch::from(struct_array);
        if batch.num_rows() == 0 {
            return Err("hudi_table_append_array: the array carried no rows".into());
        }
        let first_schema = Arc::clone(&batch.schema());
        let opened = &mut *table
            .opened
            .lock()
            .map_err(|e| format!("hudi_table_append_array: table handle poisoned: {e}"))?;
        let outcome = kernel::write_append_table(opened, vec![batch])?;
        table.schema = first_schema;
        let text = render_outcome(&outcome);
        let c = CString::new(text).map_err(|e| format!("hudi: {e}"))?;
        *out = c.into_raw();
        Ok(())
    })
}

/// Delete the rows matching `predicate`, a fully rendered `column op value(s)`
/// clause (the operators and quoting the C++ side already applies for
/// `read_hudi` filter pushdown). Free the result with `hudi_string_free`.
#[no_mangle]
pub unsafe extern "C" fn hudi_table_delete_predicate(
    table: *mut HudiTable,
    predicate: *const c_char,
    out: *mut *mut c_char,
) -> i32 {
    guarded(|| {
        let table = table.as_mut().ok_or("hudi_table_delete_predicate: table is null")?;
        if out.is_null() {
            return Err("hudi_table_delete_predicate: out is null".into());
        }
        let predicate = required_str(predicate, "delete predicate")?.to_string();
        let opened = &mut *table
            .opened
            .lock()
            .map_err(|e| format!("hudi_table_delete_predicate: table handle poisoned: {e}"))?;
        let outcome = kernel::write_delete_table(opened, &predicate)?;
        let text = render_outcome(&outcome);
        let c = CString::new(text).map_err(|e| format!("hudi: {e}"))?;
        *out = c.into_raw();
        Ok(())
    })
}

/// Render a write outcome as `commit=<instant|none>; writes=N; updates=N;
/// inserts=N; deletes=N` for SQL display.
fn render_outcome(o: &kernel::WriteOutcome) -> String {
    format!(
        "commit={}; writes={}; updates={}; inserts={}; deletes={}",
        o.completion_time.as_deref().unwrap_or("none"),
        o.num_writes,
        o.num_updates,
        o.num_inserts,
        o.num_deletes
    )
}

/// A single-quoted SQL text literal for a filter value.
fn quote_value(v: &str) -> String {
    format!("'{}'", v.replace('\'', "''"))
}

/// Whether values for a column of this type must be single-quoted in a filter
/// predicate. Numeric and temporal-numeric columns (the common record keys)
/// stay unquoted; strings, booleans and textual renderings of time/decimal do
/// not.
fn value_needs_quotes(dt: &arrow::datatypes::DataType) -> bool {
    use arrow::datatypes::DataType::*;
    matches!(
        dt,
        Utf8 | LargeUtf8 | Utf8View
            | Boolean
            | Date32
            | Date64
            | Timestamp(_, _)
            | Time32(_)
            | Time64(_)
            | Duration(_)
            | Interval(_)
            | Decimal128(_, _)
            | Decimal256(_, _)
            | Binary
            | LargeBinary
            | BinaryView
    )
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

#[cfg(test)]
mod ffi_write_tests {
    use super::*;
    use arrow::array::{ArrayRef, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field};

    fn c_str(s: &str) -> (CString, *const c_char) {
        let c = CString::new(s).unwrap();
        let ptr = c.as_ptr();
        (c, ptr)
    }

    /// Fetch the last error message (set by `guarded` on a failed call).
    fn last_error() -> String {
        let p = hudi_last_error();
        if p.is_null() {
            "<no last error>".to_string()
        } else {
            unsafe { std::ffi::CStr::from_ptr(p) }
                .to_string_lossy()
                .into_owned()
        }
    }

    /// Plan a read on `handle` with no pushed-down filters.
    unsafe fn plan_read(handle: *mut HudiTable) -> (*mut HudiPlan, usize) {
        let args = HudiReadArgs {
            as_of: std::ptr::null(),
            start: std::ptr::null(),
            end: std::ptr::null(),
        };
        let mut plan: *mut HudiPlan = std::ptr::null_mut();
        let rc = hudi_plan_create(handle, &args, std::ptr::null(), 0, &mut plan);
        assert_eq!(rc, 0, "hudi_plan_create failed");
        let mut info = HudiPlanInfo {
            slice_count: 0,
            partition_count: 0,
            row_estimate: 0,
        };
        assert_eq!(hudi_plan_info(plan, &mut info), 0, "hudi_plan_info failed");
        (plan, info.slice_count)
    }

    /// Read slice 0 of `plan` projected to `columns` (or all when empty) and
    /// collect its rows.
    unsafe fn read_plan_rows(plan: *mut HudiPlan, columns: &[*const c_char]) -> Vec<RecordBatch> {
        let mut stream_out: FFI_ArrowArrayStream = std::mem::zeroed();
        let rc = hudi_plan_slice_stream(
            plan,
            0,
            columns.as_ptr(),
            columns.len(),
            1024,
            &mut stream_out as *mut FFI_ArrowArrayStream as *mut std::ffi::c_void,
        );
        assert_eq!(rc, 0, "hudi_plan_slice_stream failed");
        let reader = arrow::ffi_stream::ArrowArrayStreamReader::try_new(stream_out)
            .expect("import read stream");
        reader
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .expect("read rows")
    }

    /// The full FFI write path as C++ will call it: `hudi_table_create` →
    /// export a `RecordBatch` into an `FFI_ArrowArrayStream` (exactly what
    /// the C++ side builds) → `hudi_table_append` → `hudi_plan_create` +
    /// `hudi_plan_slice_stream` to read the rows back → `hudi_table_delete`
    /// → read back again.
    #[test]
    fn ffi_create_append_roundtrip() {
        let dir = std::env::temp_dir().join(format!("duckdb_hudi_ffi_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let uri = dir.to_str().unwrap();

        // hudi_table_create
        let mut handle: *mut HudiTable = std::ptr::null_mut();
        let c_uri = c_str(uri);
        let c_record_key = c_str("id");
        let args = HudiCreateArgs {
            table_name: std::ptr::null(),
            table_type: std::ptr::null(),
            record_key: c_record_key.1,
            partition_fields: std::ptr::null(),
            ordering_fields: std::ptr::null(),
            populate_meta_fields: 1,
            record_index: 0,
            metadata_table: 0,
        };
        let rc =
            unsafe { hudi_table_create(c_uri.0.as_ptr(), &args, std::ptr::null(), &mut handle) };
        assert_eq!(rc, 0, "hudi_table_create failed: {}", last_error());
        assert!(!handle.is_null());
        drop(c_record_key);

        // Build the batch the C++ side would send: id BIGINT, name VARCHAR.
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1_i64, 2, 3])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("a"), Some("b"), None])) as ArrayRef,
            ],
        )
        .unwrap();

        // Export it as an Arrow C stream (what C++ does via ArrowArrayStream
        // + ArrowArrayStreamOptions callbacks). `ManuallyDrop`: ownership moves
        // into `hudi_table_append`, which reads and releases it; the caller
        // must not free it again (the C++ side follows the same contract).
        let reader = Box::new(RecordBatchIterator::new(
            vec![Ok(batch)],
            Arc::clone(&schema),
        ));
        let mut stream = std::mem::ManuallyDrop::new(FFI_ArrowArrayStream::new(reader));

        // hudi_table_append (takes ownership of the stream)
        let mut result: *mut c_char = std::ptr::null_mut();
        let rc = unsafe { hudi_table_append(handle, &mut *stream, &mut result) };
        assert_eq!(rc, 0, "hudi_table_append failed: {}", last_error());
        let outcome = unsafe { CStr::from_ptr(result) }
            .to_string_lossy()
            .to_string();
        unsafe { hudi_string_free(result) };
        assert!(outcome.starts_with("commit="), "outcome: {outcome}");
        assert!(outcome.contains("writes="), "outcome: {outcome}");

        // Read the rows back through the read path.
        let (plan, n_slices) = unsafe { plan_read(handle) };
        assert_eq!(n_slices, 1, "expected one slice");
        let col_id = c_str("id");
        let col_name = c_str("name");
        let cols = [col_id.1, col_name.1];
        let batches = unsafe { read_plan_rows(plan, &cols) };
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 3);
        let ids: Vec<i64> = batches
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect();
        assert_eq!(ids, vec![1, 2, 3]);
        let names: Vec<Option<&str>> = batches
            .iter()
            .flat_map(|b| {
                b.column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(names, vec![Some("a"), Some("b"), None]);

        // hudi_table_delete id = 2
        let c_col = c_str("id");
        let c_op = c_str("=");
        let c_val = c_str("2");
        let vals = [c_val.1];
        let mut result: *mut c_char = std::ptr::null_mut();
        let rc = unsafe {
            hudi_table_delete(
                handle,
                c_col.0.as_ptr(),
                c_op.0.as_ptr(),
                vals.as_ptr(),
                1,
                &mut result,
            )
        };
        assert_eq!(rc, 0, "hudi_table_delete failed: {}", last_error());
        let outcome = unsafe { CStr::from_ptr(result) }
            .to_string_lossy()
            .to_string();
        unsafe { hudi_string_free(result) };
        assert!(outcome.contains("deletes="), "outcome: {outcome}");

        // Read back: two rows remain.
        let (plan2, _) = unsafe { plan_read(handle) };
        let batches = unsafe { read_plan_rows(plan2, &[]) };
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 2);

        unsafe {
            hudi_plan_free(plan);
            hudi_plan_free(plan2);
            hudi_table_free(handle);
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The `hudi_table_append_array` path the C++ `COPY ... TO (FORMAT hudi)`
    /// sink takes: `hudi_table_create` → export a `RecordBatch` with
    /// `arrow::ffi::to_ffi` (an `FFI_ArrowArray` + `FFI_ArrowSchema`, exactly
    /// what DuckDB's `ArrowAppender::Finalize()` + `ToArrowSchema` produce) →
    /// `hudi_table_append_array` (which moves the array in) → read the rows
    /// back.
    #[test]
    fn ffi_create_append_array_roundtrip() {
        let dir = std::env::temp_dir().join(format!(
            "duckdb_hudi_ffi_array_{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let uri = dir.to_str().unwrap();

        // hudi_table_create
        let mut handle: *mut HudiTable = std::ptr::null_mut();
        let c_uri = c_str(uri);
        let c_record_key = c_str("id");
        let args = HudiCreateArgs {
            table_name: std::ptr::null(),
            table_type: std::ptr::null(),
            record_key: c_record_key.1,
            partition_fields: std::ptr::null(),
            ordering_fields: std::ptr::null(),
            populate_meta_fields: 1,
            record_index: 0,
            metadata_table: 0,
        };
        let rc = unsafe { hudi_table_create(c_uri.0.as_ptr(), &args, std::ptr::null(), &mut handle) };
        assert_eq!(rc, 0, "hudi_table_create failed: {}", last_error());
        assert!(!handle.is_null());
        drop(c_record_key);

        // Build the batch C++ would send: id BIGINT, name VARCHAR.
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("name", DataType::Utf8, true),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![10_i64, 20, 30])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("x"), None, Some("z")])) as ArrayRef,
            ],
        )
        .unwrap();

        // Export the batch to the C Data Interface (buffer clones are
        // self-owned, so exporting `batch` is safe; the array's release moves
        // ownership into `hudi_table_append_array`, the schema is only read).
        use arrow::array::Array as _;
        let struct_array = StructArray::from(batch);
        let (c_array, c_schema) = arrow::ffi::to_ffi(&struct_array.to_data()).expect("to_ffi");
        let mut c_array = std::mem::ManuallyDrop::new(c_array);

        // hudi_table_append_array (moves the array in)
        let mut result: *mut c_char = std::ptr::null_mut();
        let rc =
            unsafe { hudi_table_append_array(handle, &mut *c_array, &c_schema, &mut result) };
        assert_eq!(rc, 0, "hudi_table_append_array failed: {}", last_error());
        let outcome = unsafe { CStr::from_ptr(result) }
            .to_string_lossy()
            .to_string();
        unsafe { hudi_string_free(result) };
        assert!(outcome.starts_with("commit="), "outcome: {outcome}");
        assert!(outcome.contains("writes=3"), "outcome: {outcome}");

        // Read the rows back.
        let (plan, n_slices) = unsafe { plan_read(handle) };
        assert_eq!(n_slices, 1, "expected one slice");
        let col_id = c_str("id");
        let col_name = c_str("name");
        let cols = [col_id.1, col_name.1];
        let batches = unsafe { read_plan_rows(plan, &cols) };
        let ids: Vec<i64> = batches
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect();
        assert_eq!(ids, vec![10, 20, 30]);

        unsafe {
            hudi_plan_free(plan);
            hudi_table_free(handle);
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
}
