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
use std::sync::{Arc, OnceLock};

use arrow::array::RecordBatchReader;
use arrow::datatypes::{Schema, SchemaRef};
use arrow::error::ArrowError;
use arrow::ffi::FFI_ArrowSchema;
use arrow::ffi_stream::FFI_ArrowArrayStream;
use arrow::record_batch::{RecordBatch, RecordBatchIterator};
use futures::future::{AbortHandle, Aborted};
use futures::stream::{BoxStream, StreamExt};
use futures::FutureExt;
use hudi::table::ReadOptions;

use crate::kernel::{self, FilterSpec, HudiQueryArgs, OpenedTable, ScanPlan, TimelineMarker};

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
    // Built by the first freshness check, so only a cached table pays for it.
    marker: OnceLock<TimelineMarker>,
}

/// A planned scan over one table.
pub struct HudiPlan {
    // Shared with the slices opening in the background, which may outlive
    // the handle.
    plan: Arc<ScanPlan>,
    schema: SchemaRef,
}

#[allow(dead_code)]
fn assert_handles_are_thread_safe() {
    fn check<T: Send + Sync>() {}
    check::<HudiTable>();
    check::<HudiPlan>();
    check::<HudiSliceOpen>();
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
            marker: OnceLock::new(),
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

/// Write 1 into `out` when the table still matches its timeline in storage,
/// 0 when the timeline changed and the table has to be opened again.
#[no_mangle]
pub unsafe extern "C" fn hudi_table_is_current(table: *const HudiTable, out: *mut i32) -> i32 {
    guarded(|| {
        let table = table
            .as_ref()
            .ok_or("hudi_table_is_current: table is null")?;
        let out = out.as_mut().ok_or("hudi_table_is_current: out is null")?;
        let marker = match table.marker.get() {
            Some(marker) => marker,
            None => {
                let marker = TimelineMarker::new(&table.opened.table)?;
                table.marker.get_or_init(|| marker)
            }
        };
        let current = marker
            .is_current(&table.opened.table)
            .map_err(|e| format!("{e} (table {})", table.uri))?;
        *out = i32::from(current);
        Ok(())
    })
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
            plan: Arc::new(plan),
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

type SliceStream = BoxStream<'static, hudi::error::Result<RecordBatch>>;

/// One slice's merged rows as a synchronous batch reader. Every batch is
/// shaped to the projected columns in order; the types are the declared ones,
/// except that a column keeps an encoding the kernel read it with when it
/// carries the same values (a string as a view or dictionary, a decimal in 64
/// bits). The first batch is pulled when the reader is created,
/// so the stream's schema reports those encodings before any batch is handed
/// out, and later batches are cast to the same schema.
struct SliceBatchReader {
    stream: SliceStream,
    schema: SchemaRef,
    pending: Option<Result<RecordBatch, ArrowError>>,
}

impl SliceBatchReader {
    /// Open slice `slice_index` of `plan`, which `slice_request` checked is in
    /// range, and pull its first batch.
    async fn open(
        plan: Arc<ScanPlan>,
        slice_index: usize,
        options: ReadOptions,
        projected: SchemaRef,
    ) -> Result<Self, String> {
        let slice = &plan.slices[slice_index];
        let stream = kernel::open_slice_stream(&plan, slice, &options).await?;
        Ok(Self::from_stream(stream, projected).await)
    }

    async fn from_stream(mut stream: SliceStream, projected: SchemaRef) -> Self {
        let first = match std::panic::AssertUnwindSafe(stream.next())
            .catch_unwind()
            .await
        {
            Ok(item) => item.map(|item| Self::shape(item, &projected, true)),
            Err(payload) => Some(Err(internal_read_error(&payload))),
        };
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

    fn shape(
        item: hudi::error::Result<RecordBatch>,
        schema: &SchemaRef,
        keep_encodings: bool,
    ) -> Result<RecordBatch, ArrowError> {
        match item {
            Ok(batch) => {
                kernel::align_batch_with(&batch, schema, keep_encodings).map_err(read_error)
            }
            Err(e) => Err(read_error(format!("Hudi read failed: {e}"))),
        }
    }

    fn pull(
        stream: &mut SliceStream,
        schema: &SchemaRef,
        keep_encodings: bool,
    ) -> Option<Result<RecordBatch, ArrowError>> {
        // Arrow's stream callbacks carry no panic guard of their own, and a
        // panic unwinding through them would abort the host process.
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            kernel::runtime()
                .block_on(stream.next())
                .map(|item| Self::shape(item, schema, keep_encodings))
        }));
        match outcome {
            Ok(item) => item,
            Err(payload) => Some(Err(internal_read_error(&payload))),
        }
    }
}

fn internal_read_error(payload: &Box<dyn std::any::Any + Send>) -> ArrowError {
    read_error(format!("hudi: internal error: {}", panic_message(payload)))
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

/// The projected schema and read options of a stream over slice `slice_index`
/// of `plan`; `what` names the calling function in errors.
unsafe fn slice_request(
    plan: &HudiPlan,
    slice_index: usize,
    columns: *const *const c_char,
    n_columns: usize,
    batch_size: usize,
    what: &str,
) -> Result<(SchemaRef, ReadOptions), String> {
    if slice_index >= plan.plan.slices.len() {
        return Err(format!("{what}: slice {slice_index} out of range"));
    }
    if n_columns > 0 && columns.is_null() {
        return Err(format!("{what}: columns is null"));
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
    let options = kernel::slice_read_options(&plan.plan, &columns, batch_size)?;
    Ok((Arc::new(Schema::new(fields)), options))
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
        const WHAT: &str = "hudi_plan_slice_stream";
        let plan = plan
            .as_ref()
            .ok_or("hudi_plan_slice_stream: plan is null")?;
        if out.is_null() {
            return Err("hudi_plan_slice_stream: out is null".into());
        }
        let (projected, options) =
            slice_request(plan, slice_index, columns, n_columns, batch_size, WHAT)?;
        let reader = kernel::runtime().block_on(SliceBatchReader::open(
            Arc::clone(&plan.plan),
            slice_index,
            options,
            projected,
        ))?;
        export_stream(out, Box::new(reader));
        Ok(())
    })
}

type SliceOpenOutcome = Result<Result<SliceBatchReader, String>, Aborted>;

/// A slice opening in the background: its stream built and its first batch
/// pulled, as `hudi_plan_slice_stream` does before it returns. Dropping it
/// stops the open at its next await point and drops whatever it produced.
pub struct HudiSliceOpen {
    task: tokio::task::JoinHandle<SliceOpenOutcome>,
    abort: AbortHandle,
}

impl HudiSliceOpen {
    fn spawn(
        open: impl std::future::Future<Output = Result<SliceBatchReader, String>> + Send + 'static,
    ) -> Self {
        let (open, abort) = futures::future::abortable(open);
        // A blocking thread of its own, not a runtime worker: the open decodes
        // and merges inside its polls, and hudi-core hands none of that off,
        // so on the few workers it would cap the scan's parallelism and delay
        // the I/O wakeups of the slices other threads are reading.
        let runtime = kernel::runtime().handle().clone();
        let task = kernel::runtime().spawn_blocking(move || runtime.block_on(open));
        Self { task, abort }
    }

    /// Wait for the open to finish and return its reader or its error.
    fn take(mut self) -> Result<SliceBatchReader, String> {
        match kernel::runtime().block_on(&mut self.task) {
            Ok(Ok(outcome)) => outcome,
            Ok(Err(Aborted)) => Err("hudi: opening a slice was aborted".into()),
            Err(e) if e.is_panic() => Err(format!(
                "hudi: internal error: {}",
                panic_message(&e.into_panic())
            )),
            Err(e) => Err(format!("hudi: opening a slice did not finish: {e}")),
        }
    }
}

impl Drop for HudiSliceOpen {
    fn drop(&mut self) {
        // The task drops its result itself once nothing waits for it.
        self.abort.abort();
        self.task.abort();
    }
}

/// Start opening slice `slice_index` as `hudi_plan_slice_stream` would, in the
/// background, and write the handle into `out`. The handle holds what it
/// needs of the plan, so the plan may be freed first. Pass it to
/// `hudi_slice_open_take`, or to `hudi_slice_open_free` if it is not read.
#[no_mangle]
pub unsafe extern "C" fn hudi_plan_slice_open_start(
    plan: *const HudiPlan,
    slice_index: usize,
    columns: *const *const c_char,
    n_columns: usize,
    batch_size: usize,
    out: *mut *mut HudiSliceOpen,
) -> i32 {
    guarded(|| {
        const WHAT: &str = "hudi_plan_slice_open_start";
        let plan = plan
            .as_ref()
            .ok_or("hudi_plan_slice_open_start: plan is null")?;
        if out.is_null() {
            return Err("hudi_plan_slice_open_start: out is null".into());
        }
        let (projected, options) =
            slice_request(plan, slice_index, columns, n_columns, batch_size, WHAT)?;
        let open = HudiSliceOpen::spawn(SliceBatchReader::open(
            Arc::clone(&plan.plan),
            slice_index,
            options,
            projected,
        ));
        *out = Box::into_raw(Box::new(open));
        Ok(())
    })
}

/// Wait for `open` to finish and write its `ArrowArrayStream` into `out`, or
/// fail with the error the open hit. Consumes `open` either way.
#[no_mangle]
pub unsafe extern "C" fn hudi_slice_open_take(
    open: *mut HudiSliceOpen,
    out: *mut std::ffi::c_void,
) -> i32 {
    guarded(|| {
        if open.is_null() {
            return Err("hudi_slice_open_take: open is null".into());
        }
        let open = Box::from_raw(open);
        if out.is_null() {
            return Err("hudi_slice_open_take: out is null".into());
        }
        let reader = open.take()?;
        export_stream(out, Box::new(reader));
        Ok(())
    })
}

/// Abort and free a handle that is not going to be taken.
#[no_mangle]
pub unsafe extern "C" fn hudi_slice_open_free(open: *mut HudiSliceOpen) {
    if !open.is_null() {
        drop(Box::from_raw(open));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kernel::tests::{fixture, ScratchTable};
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field};
    use arrow::ffi_stream::ArrowArrayStreamReader;
    use std::sync::mpsc;
    use std::time::{Duration, Instant};

    const WAIT: Duration = Duration::from_secs(30);

    fn plan_for(uri: &str) -> Box<HudiPlan> {
        let opened = kernel::open_table(uri, None).expect("open");
        let schema = kernel::read_schema(&opened.table).expect("schema");
        let plan = kernel::plan_scan(&opened, &HudiQueryArgs::default(), Vec::new()).expect("plan");
        Box::new(HudiPlan {
            plan: Arc::new(plan),
            schema,
        })
    }

    /// The column the tests project, held with the pointer array C++ passes.
    struct Columns {
        _names: Vec<CString>,
        pointers: Vec<*const c_char>,
    }

    fn columns() -> Columns {
        let names = vec![CString::new("_hoodie_record_key").unwrap()];
        let pointers = names.iter().map(|c| c.as_ptr()).collect();
        Columns {
            _names: names,
            pointers,
        }
    }

    fn last_error() -> String {
        let message = hudi_last_error();
        assert!(!message.is_null(), "a failed call left no error");
        unsafe { CStr::from_ptr(message) }
            .to_string_lossy()
            .into_owned()
    }

    fn start(plan: &HudiPlan, slice: usize, columns: &Columns) -> *mut HudiSliceOpen {
        let mut open = std::ptr::null_mut();
        let rc = unsafe {
            hudi_plan_slice_open_start(
                plan,
                slice,
                columns.pointers.as_ptr(),
                columns.pointers.len(),
                1024,
                &mut open,
            )
        };
        assert_eq!(rc, 0, "{}", last_error());
        open
    }

    /// Reads `stream` to the end: its row count, or the first error.
    fn read_rows(stream: FFI_ArrowArrayStream) -> Result<usize, String> {
        let reader = ArrowArrayStreamReader::try_new(stream).map_err(|e| e.to_string())?;
        let mut rows = 0;
        for batch in reader {
            rows += batch.map_err(|e| e.to_string())?.num_rows();
        }
        Ok(rows)
    }

    fn take_rows(open: *mut HudiSliceOpen) -> Result<usize, String> {
        let mut stream = FFI_ArrowArrayStream::empty();
        let out = &mut stream as *mut FFI_ArrowArrayStream as *mut std::ffi::c_void;
        if unsafe { hudi_slice_open_take(open, out) } != 0 {
            return Err(last_error());
        }
        read_rows(stream)
    }

    /// The same slice through the blocking `hudi_plan_slice_stream`.
    fn blocking_rows(plan: &HudiPlan, slice: usize, columns: &Columns) -> Result<usize, String> {
        let mut stream = FFI_ArrowArrayStream::empty();
        let out = &mut stream as *mut FFI_ArrowArrayStream as *mut std::ffi::c_void;
        let rc = unsafe {
            hudi_plan_slice_stream(
                plan,
                slice,
                columns.pointers.as_ptr(),
                columns.pointers.len(),
                1024,
                out,
            )
        };
        if rc != 0 {
            return Err(last_error());
        }
        read_rows(stream)
    }

    fn wait_until_finished(open: *mut HudiSliceOpen) {
        let deadline = Instant::now() + WAIT;
        while !unsafe { &*open }.task.is_finished() {
            assert!(Instant::now() < deadline, "the open did not finish");
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    fn spawned(
        open: impl std::future::Future<Output = Result<SliceBatchReader, String>> + Send + 'static,
    ) -> *mut HudiSliceOpen {
        Box::into_raw(Box::new(HudiSliceOpen::spawn(open)))
    }

    #[test]
    fn take_after_ready_reads_what_the_blocking_open_reads() {
        let uri = fixture("mor_v9_mdt_simple/v9_txns_simple_meta");
        let plan = plan_for(&uri);
        let columns = columns();
        let slices = plan.plan.slices.len();
        assert!(slices > 1, "the fixture has {slices} slices");
        let mut total = 0;
        for slice in 0..slices {
            let open = start(&plan, slice, &columns);
            wait_until_finished(open);
            let rows = take_rows(open).expect("take");
            assert_eq!(rows, blocking_rows(&plan, slice, &columns).unwrap());
            total += rows;
        }
        let reference = kernel::hudi_read_batches(&uri, &HudiQueryArgs::default()).expect("read");
        let expected: usize = reference.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total, expected);

        // The handle keeps what it needs of the plan.
        let expected = blocking_rows(&plan, 0, &columns).unwrap();
        let open = start(&plan, 0, &columns);
        unsafe { hudi_plan_free(Box::into_raw(plan)) };
        assert_eq!(take_rows(open), Ok(expected));
    }

    #[test]
    fn take_before_ready_waits_for_the_open() {
        let (schema, batch) = one_batch();
        let (release, released) = tokio::sync::oneshot::channel::<()>();
        let open = spawned(async move {
            released.await.expect("release");
            let items: Vec<hudi::error::Result<RecordBatch>> = vec![Ok(batch)];
            Ok(SliceBatchReader::from_stream(futures::stream::iter(items).boxed(), schema).await)
        });
        std::thread::sleep(Duration::from_millis(50));
        assert!(!unsafe { &*open }.task.is_finished());
        let releaser = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(100));
            release.send(()).unwrap();
        });
        assert_eq!(take_rows(open), Ok(3));
        releaser.join().unwrap();

        // The same through a real slice, taken straight after it starts.
        let plan = plan_for(&fixture("cow_v6_nonpartitioned"));
        let columns = columns();
        let open = start(&plan, 0, &columns);
        assert_eq!(take_rows(open), Ok(4));
    }

    /// Sends once when dropped.
    struct OnDrop(mpsc::Sender<()>);

    impl Drop for OnDrop {
        fn drop(&mut self) {
            let _ = self.0.send(());
        }
    }

    fn one_batch() -> (SchemaRef, RecordBatch) {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        (schema, batch)
    }

    #[test]
    fn free_without_take_stops_the_open_and_drops_its_result() {
        // Waiting at an await point: the open is dropped there.
        let (started_tx, started) = mpsc::channel();
        let (dropped_tx, dropped) = mpsc::channel();
        let open = spawned(async move {
            let _guard = OnDrop(dropped_tx);
            started_tx.send(()).unwrap();
            std::future::pending::<()>().await;
            Err("never resumed".to_string())
        });
        started.recv_timeout(WAIT).expect("the open did not start");
        unsafe { hudi_slice_open_free(open) };
        dropped
            .recv_timeout(WAIT)
            .expect("freeing the handle did not stop the open");

        // In a step that does not yield: the step finishes, and the open stops
        // at the next await point instead of resuming.
        let (started_tx, started) = mpsc::channel();
        let (dropped_tx, dropped) = mpsc::channel();
        let step_done = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let resumed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let (step_flag, resumed_flag) = (Arc::clone(&step_done), Arc::clone(&resumed));
        let open = spawned(async move {
            let _guard = OnDrop(dropped_tx);
            started_tx.send(()).unwrap();
            std::thread::sleep(Duration::from_millis(200));
            step_flag.store(true, std::sync::atomic::Ordering::SeqCst);
            tokio::task::yield_now().await;
            resumed_flag.store(true, std::sync::atomic::Ordering::SeqCst);
            Err("resumed".to_string())
        });
        started.recv_timeout(WAIT).expect("the open did not start");
        unsafe { hudi_slice_open_free(open) };
        dropped
            .recv_timeout(WAIT)
            .expect("freeing the handle did not stop the open");
        assert!(step_done.load(std::sync::atomic::Ordering::SeqCst));
        assert!(!resumed.load(std::sync::atomic::Ordering::SeqCst));

        // Finished but never taken: its reader, first batch included, is dropped.
        let (schema, batch) = one_batch();
        let (dropped_tx, dropped) = mpsc::channel();
        let open = spawned(async move {
            let guard = OnDrop(dropped_tx);
            let items: Vec<hudi::error::Result<RecordBatch>> = vec![Ok(batch)];
            let stream = futures::stream::iter(items)
                .map(move |item| {
                    let _ = &guard;
                    item
                })
                .boxed();
            Ok(SliceBatchReader::from_stream(stream, schema).await)
        });
        wait_until_finished(open);
        assert!(
            dropped.try_recv().is_err(),
            "the reader went before the free"
        );
        unsafe { hudi_slice_open_free(open) };
        dropped
            .recv_timeout(WAIT)
            .expect("freeing the handle did not drop the finished reader");

        // A real slice, freed while opening and after it finished.
        let plan = plan_for(&fixture("cow_v6_nonpartitioned"));
        let columns = columns();
        unsafe { hudi_slice_open_free(start(&plan, 0, &columns)) };
        let open = start(&plan, 0, &columns);
        wait_until_finished(open);
        unsafe { hudi_slice_open_free(open) };
        unsafe { hudi_slice_open_free(std::ptr::null_mut()) };
    }

    #[test]
    fn background_opens_run_off_the_runtime_workers() {
        // More opens than the runtime has workers, each holding its thread
        // until all of them run at once: on the workers they could not.
        const OPENS: usize = 8;
        let arrived = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let opens: Vec<_> = (0..OPENS)
            .map(|_| {
                let arrived = Arc::clone(&arrived);
                spawned(async move {
                    arrived.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    let deadline = Instant::now() + Duration::from_secs(10);
                    while arrived.load(std::sync::atomic::Ordering::SeqCst) < OPENS {
                        if Instant::now() > deadline {
                            return Err("not all opens ran at once".to_string());
                        }
                        std::thread::sleep(Duration::from_millis(1));
                    }
                    Err("all opens ran at once".to_string())
                })
            })
            .collect();
        for open in opens {
            assert_eq!(take_rows(open), Err("all opens ran at once".to_string()));
        }
    }

    fn explode() -> Result<SliceBatchReader, String> {
        panic!("kaboom")
    }

    #[test]
    fn error_in_background_open_surfaces_at_take() {
        let open = spawned(async { Err("boom".to_string()) });
        assert_eq!(take_rows(open), Err("boom".to_string()));

        let open = spawned(async { explode() });
        assert_eq!(
            take_rows(open),
            Err("hudi: internal error: kaboom".to_string())
        );

        // A base file gone after planning fails the background open the way it
        // fails the blocking one.
        let scratch = ScratchTable::new("slice-open-missing-file");
        let plan = plan_for(&scratch.uri());
        for entry in std::fs::read_dir(&scratch.table).expect("read dir") {
            let path = entry.expect("dir entry").path();
            if path.extension().is_some_and(|e| e == "parquet") {
                std::fs::remove_file(path).expect("remove base file");
            }
        }
        let columns = columns();
        let blocking = blocking_rows(&plan, 0, &columns).expect_err("blocking open");
        assert!(blocking.contains("Hudi read failed"), "{blocking}");
        let open = start(&plan, 0, &columns);
        assert_eq!(take_rows(open), Err(blocking));
    }

    #[test]
    fn start_rejects_a_slice_out_of_range() {
        let plan = plan_for(&fixture("cow_v6_nonpartitioned"));
        let columns = columns();
        let mut open = std::ptr::null_mut();
        let rc = unsafe {
            hudi_plan_slice_open_start(
                &*plan,
                1,
                columns.pointers.as_ptr(),
                columns.pointers.len(),
                1024,
                &mut open,
            )
        };
        assert_eq!(rc, 1);
        assert!(open.is_null());
        assert!(last_error().contains("out of range"), "{}", last_error());
    }
}
