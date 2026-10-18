//! The Hudi side of the extension: opening a table, planning a read (file
//! slices after partition and file pruning), streaming one slice's merged rows,
//! and the metadata table functions. Everything here is plain Rust over the
//! hudi-rs 0.5.0 kernel; `ffi` exposes it over a C ABI.

use std::sync::Arc;
use std::sync::OnceLock;

use arrow::datatypes::{Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use futures::stream::BoxStream;
use hudi::expr::filter::Filter;
use hudi::file_group::file_slice::FileSlice;
use hudi::file_group::reader::FileGroupReader;
use hudi::table::{QueryType, ReadOptions, Table};

/// Shared multi-thread tokio runtime driving the async hudi-rs kernel.
static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();

pub fn runtime() -> &'static tokio::runtime::Runtime {
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .expect("failed to build tokio runtime")
    })
}

/// Parsed `read_hudi` arguments (positional + named), used to build the kernel's
/// `ReadOptions` and to open the table with any cloud/storage credentials.
#[derive(Clone, Debug, Default)]
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

/// One pushed-down predicate, as the extension hands it to the kernel: a column
/// name, an operator token (`=`, `!=`, `<`, `<=`, `>`, `>=`, `IN`, `NOT IN`) and
/// its literal values rendered as strings, which the kernel casts to the
/// column's Arrow type.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FilterSpec {
    pub column: String,
    pub op: String,
    pub values: Vec<String>,
}

impl FilterSpec {
    fn to_kernel(&self) -> Result<Filter, String> {
        let op = self
            .op
            .parse::<hudi::expr::ExprOperator>()
            .map_err(|e| format!("read_hudi: unsupported filter operator {:?}: {e}", self.op))?;
        Filter::new(self.column.clone(), op, self.values.clone())
            .map_err(|e| format!("read_hudi: invalid filter on {:?}: {e}", self.column))
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

/// An opened table together with the `hudi_option` pairs it was opened with,
/// which every read of it is also configured with.
pub struct OpenedTable {
    pub table: Table,
    pub hudi_options: Option<String>,
}

/// Open the table at `uri` with the `hudi_option` pairs (cloud credentials, read
/// tuning) applied.
pub fn open_table(uri: &str, hudi_options: Option<String>) -> Result<OpenedTable, String> {
    let args = HudiQueryArgs {
        hudi_options,
        ..Default::default()
    };
    let open_opts = parse_hudi_option(&args)?;
    let table = runtime()
        .block_on(async {
            if open_opts.is_empty() {
                Table::new(uri).await
            } else {
                Table::new_with_options(uri, open_opts).await
            }
        })
        .map_err(|e| format!("failed to open Hudi table at {uri}: {e}"))?;
    Ok(OpenedTable {
        table,
        hudi_options: args.hudi_options,
    })
}

/// The schema `read_hudi` declares for a table: the data columns, preceded by
/// the Hudi meta fields when the table populates them, every field nullable.
pub fn read_schema(table: &Table) -> Result<SchemaRef, String> {
    runtime().block_on(async {
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
        .map_err(|e| format!("failed to read Hudi schema: {e}"))?;
        let fields: Vec<Field> = schema
            .fields()
            .iter()
            .map(|f| f.as_ref().clone().with_nullable(true))
            .collect();
        Ok(Arc::new(Schema::new(fields)))
    })
}

/// Read a Hudi table's rows via the kernel's `Table::read` using the parsed query
/// arguments, returning the (possibly zero) non-empty `RecordBatch`es. The tests
/// use it as the reference for the fixtures.
pub fn hudi_read_batches(uri: &str, args: &HudiQueryArgs) -> Result<Vec<RecordBatch>, String> {
    let opened = open_table(uri, args.hudi_options.clone())?;
    let options = read_options_for(args)?;
    runtime().block_on(async {
        let batches = opened
            .table
            .read(&options)
            .await
            .map_err(|e| format!("Hudi read failed: {e}"))?;
        Ok(batches.into_iter().filter(|b| b.num_rows() > 0).collect())
    })
}

/// A planned `read_hudi` scan: the file slices the query reads after partition
/// and file pruning, and a reader that carries its timestamps. No rows are read
/// until a slice is streamed.
pub struct ScanPlan {
    pub reader: FileGroupReader,
    pub slices: Vec<FileSlice>,
    /// The filters the slices were pruned with; every slice stream masks its
    /// rows with them too.
    pub filters: Vec<FilterSpec>,
}

impl ScanPlan {
    /// Distinct partition paths among the kept slices.
    pub fn partition_count(&self) -> usize {
        let mut paths: Vec<&str> = self
            .slices
            .iter()
            .map(|s| s.partition_path.as_str())
            .collect();
        paths.sort_unstable();
        paths.dedup();
        paths.len()
    }

    /// The kernel's row count of the kept slices' base files when every one
    /// reports one, `None` otherwise. The kernel estimates the count from the
    /// file size unless the metadata table records it, so it can be far off for
    /// small files.
    pub fn row_estimate(&self) -> Option<i64> {
        let mut total = 0i64;
        for slice in &self.slices {
            let n = slice
                .base_file
                .as_ref()?
                .file_metadata
                .as_ref()?
                .num_records;
            total += n.max(0);
        }
        Some(total)
    }
}

/// Resolve the slices and reader of the read that `args` and `filters` describe.
pub fn plan_scan(
    opened: &OpenedTable,
    args: &HudiQueryArgs,
    filters: Vec<FilterSpec>,
) -> Result<ScanPlan, String> {
    let args = HudiQueryArgs {
        hudi_options: opened.hudi_options.clone(),
        ..args.clone()
    };
    let mut options = read_options_for(&args)?;
    // Set directly rather than through `with_filters`, which takes one literal
    // per filter and re-splits it on commas.
    options.filters = filters
        .iter()
        .map(FilterSpec::to_kernel)
        .collect::<Result<Vec<_>, _>>()?;
    let table = &opened.table;
    runtime().block_on(async {
        let slices = table
            .get_file_slices(&options)
            .await
            .map_err(|e| format!("failed to plan Hudi read: {e}"))?;
        // The reader resolves the snapshot or incremental bounds from `options`
        // and applies the commit-time filter of an incremental read per batch.
        let reader = table
            .create_file_group_reader_with_options(
                Some(&options),
                std::iter::empty::<(&str, &str)>(),
            )
            .await
            .map_err(|e| format!("failed to create Hudi reader: {e}"))?;
        Ok(ScanPlan {
            reader,
            slices,
            filters,
        })
    })
}

/// The read options one slice is streamed with: the plan's filters as a row
/// mask, the projection, and the batch size.
pub fn slice_read_options(
    plan: &ScanPlan,
    columns: &[String],
    batch_size: usize,
) -> Result<ReadOptions, String> {
    let mut options = ReadOptions::new()
        .with_batch_size(batch_size)
        .map_err(|e| format!("read_hudi: {e}"))?
        .with_projection(columns.iter().cloned());
    options.filters = plan
        .filters
        .iter()
        .map(FilterSpec::to_kernel)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(options)
}

/// Open the stream of one slice's merged rows.
pub fn open_slice_stream(
    plan: &ScanPlan,
    slice: &FileSlice,
    options: &ReadOptions,
) -> Result<BoxStream<'static, hudi::error::Result<RecordBatch>>, String> {
    runtime()
        .block_on(plan.reader.read_file_slice_stream(slice, options))
        .map_err(|e| format!("Hudi read failed: {e}"))
}

/// Shape a kernel batch as `schema`: columns picked by name, in its order, cast
/// where the file's type differs from the declared one.
pub fn align_batch(batch: &RecordBatch, schema: &SchemaRef) -> Result<RecordBatch, String> {
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
    let options =
        arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
    RecordBatch::try_new_with_options(schema.clone(), columns, &options)
        .map_err(|e| format!("read_hudi: {e}"))
}

// ── hudi_info(path) / hudi_files(path) ───────────────────────────────────────

/// One file slice rendered for `hudi_files` output.
fn file_slice_line(uri: &str, fs: &FileSlice) -> String {
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

/// Collect a table's summary line and file list via the kernel.
pub fn table_metadata(uri: &str, table: &Table) -> Result<(String, Vec<String>), String> {
    runtime().block_on(async {
        let timeline = table.get_timeline();
        let latest = timeline
            .get_latest_commit_timestamp()
            .unwrap_or_else(|_| "unknown".into());
        let n_commits = timeline.completed_commits.len();

        let slices = table
            .get_file_slices(&ReadOptions::new())
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

// ── hudi_mdt(uri) / hudi_commits(uri) / hudi_stats(uri) ─────────────────────

/// `hudi_mdt(uri)`: the table's `files` index as rows, one per data file
/// (`partition_path`, `file_name`, `size`, `is_deleted`). Hudi's `files` index
/// stores, per partition, a map `file_name -> {size, isDeleted}`; this reads that
/// partition through the public kernel API and decodes the map into flat rows.
///
/// Decoded rather than dumped raw because every MDT record's payload is an Avro
/// union of records, which neither hudi-rs 0.5.0's `avro_to_arrow` nor DuckDB's
/// Arrow import can take as is.
///
/// Errors cleanly when the table has no metadata table enabled.
pub fn mdt_batches(table: &Table) -> Result<Vec<RecordBatch>, String> {
    use arrow::array::{Array, BooleanArray, Int64Array, MapArray, StringArray};
    use arrow::datatypes::DataType;

    runtime().block_on(async {
        if !table.is_metadata_table_enabled() {
            return Err("hudi_mdt: table has no metadata table enabled (hoodie.metadata.enable=false or absent)".into());
        }
        // `&[]` = every key = the whole `files` partition: one batch carrying the
        // HoodieMetadataRecord schema (a nullable `filesystemMetadata` map per row).
        let batch = table
            .read_metadata_table_files_partition_arrow(&[])
            .await
            .map_err(|e| format!("Hudi metadata-table (files partition) read failed: {e}"))?;
        let raw = [batch];

        let mut partition_path: Vec<String> = Vec::new();
        let mut file_name: Vec<String> = Vec::new();
        let mut size: Vec<i64> = Vec::new();
        let mut is_deleted: Vec<bool> = Vec::new();

        for batch in &raw {
            let Some(key_col) = batch.column_by_name("key") else {
                return Err("hudi_mdt: files-partition record is missing the `key` column".into());
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
pub fn commits_batches(table: &Table) -> Result<Vec<RecordBatch>, String> {
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::DataType;

    let commits = table.get_timeline().completed_commits.clone();

    let ts: Vec<String> = commits.iter().map(|c| c.timestamp.clone()).collect();
    let action: Vec<String> = commits
        .iter()
        .map(|c| c.action.as_ref().to_string())
        .collect();
    let completion: Vec<Option<String>> = commits
        .iter()
        .map(|c| c.completion_timestamp.clone())
        .collect();
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
    Ok(vec![batch])
}

/// `BoxFuture<'a, parquet::Result<Bytes>>` spelled without the `futures` crate.
type ParquetSuffixFuture<'a> = std::pin::Pin<
    Box<dyn std::future::Future<Output = parquet::errors::Result<bytes::Bytes>> + Send + 'a>,
>;

/// In-memory byte source for a parquet footer fetch. `hudi::storage::StorageReader`
/// is private to hudi-core, so each file is pulled via the public
/// `Storage::get_file_data` (which honours the table's object-store options)
/// and the footer is sliced out of the resident bytes.
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

/// One `hudi_stats` row: file, column, data type, min, max, row count.
type StatsRow = (String, String, String, Option<String>, Option<String>, i64);

/// One parquet footer's file-level column stats, as flat rows.
fn footer_stats_rows(
    file: &str,
    container: &hudi::statistics::StatisticsContainer,
) -> Vec<StatsRow> {
    let num_rows = container.num_rows.unwrap_or(0);
    let mut names: Vec<&String> = container.columns.keys().collect();
    names.sort();
    names
        .into_iter()
        .map(|name| {
            let stats = &container.columns[name];
            let min = stats.min_value.as_ref().map(|a| {
                arrow_cast::display::array_value_to_string(a.as_ref(), 0).unwrap_or_default()
            });
            let max = stats.max_value.as_ref().map(|a| {
                arrow_cast::display::array_value_to_string(a.as_ref(), 0).unwrap_or_default()
            });
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

/// `hudi_stats(uri)`: one row per (data file, column) with the file-level
/// min/max from the parquet footer, exactly the statistics the kernel's file
/// pruner consults. Files without a parquet base (log-only slices, HFile/Lance
/// base files) are skipped.
pub fn stats_batches(table: &Table) -> Result<Vec<RecordBatch>, String> {
    runtime().block_on(async {
        use hudi::storage::Storage;
        use parquet::file::metadata::ParquetMetaDataReader;

        let schema = table
            .get_schema()
            .await
            .map_err(|e| format!("failed to resolve schema: {e}"))?;
        let slices = table
            .get_file_slices(&ReadOptions::new())
            .await
            .map_err(|e| format!("failed to list file slices: {e}"))?;
        let storage = Storage::new(
            Arc::new(table.storage_options()),
            Arc::clone(&table.hudi_configs),
        )
        .map_err(|e| format!("failed to open storage: {e}"))?;

        let mut rows: Vec<StatsRow> = Vec::new();
        for fs in &slices {
            let Some(base) = fs.base_file.as_ref() else {
                continue;
            };
            if !base.extension.eq_ignore_ascii_case("parquet") {
                continue;
            }
            let rel = fs
                .base_file_relative_path()
                .map_err(|e| format!("no base file path: {e}"))?;
            let Some(rel) = rel else {
                continue;
            };
            let bytes = storage
                .get_file_data(&rel)
                .await
                .map_err(|e| format!("failed to read {rel}: {e}"))?;
            let mut src = FooterSource { bytes };
            let meta = ParquetMetaDataReader::new()
                .load_via_suffix_and_finish(&mut src)
                .await
                .map_err(|e| format!("failed to read parquet footer {rel}: {e}"))?;
            let container =
                hudi::statistics::StatisticsContainer::from_parquet_metadata(&meta, &schema);
            rows.extend(footer_stats_rows(&rel, &container));
        }

        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::DataType;
        let schema = Arc::new(Schema::new(vec![
            Field::new("file", DataType::Utf8, false),
            Field::new("column", DataType::Utf8, false),
            Field::new("data_type", DataType::Utf8, false),
            Field::new("min_value", DataType::Utf8, true),
            Field::new("max_value", DataType::Utf8, true),
            Field::new("num_rows", DataType::Int64, false),
        ]));
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Float64Array, Int32Array, Int64Array};
    use arrow::datatypes::DataType;
    use futures::StreamExt;

    /// An in-repo fixture's absolute path; the kernel rejects a `..` segment.
    fn fixture(name: &str) -> String {
        std::fs::canonicalize(format!(
            "{}/../test/fixture/{name}",
            env!("CARGO_MANIFEST_DIR")
        ))
        .expect("fixture path")
        .to_string_lossy()
        .into_owned()
    }

    fn total_rows(batches: &[RecordBatch]) -> u64 {
        batches.iter().map(|b| b.num_rows() as u64).sum()
    }

    #[test]
    fn hudi_option_parsing_trims_and_pairs() {
        let args = HudiQueryArgs {
            hudi_options: Some(" a = 1 ;b=2; c=3 ".into()),
            ..Default::default()
        };
        let got = parse_hudi_option(&args).unwrap();
        assert_eq!(
            got,
            vec![
                ("a".into(), "1".into()),
                ("b".into(), "2".into()),
                ("c".into(), "3".into())
            ]
        );
    }

    #[test]
    fn hudi_option_none_is_empty() {
        assert!(parse_hudi_option(&HudiQueryArgs::default())
            .unwrap()
            .is_empty());
    }

    #[test]
    fn hudi_option_without_separator_is_an_error() {
        let args = HudiQueryArgs {
            hudi_options: Some("a=1; nope".into()),
            ..Default::default()
        };
        let err = parse_hudi_option(&args).unwrap_err();
        assert!(err.contains("nope"), "{err}");
    }

    #[test]
    fn query_type_selection() {
        let snapshot = read_options_for(&HudiQueryArgs::default()).unwrap();
        assert_eq!(snapshot.query_type().unwrap(), QueryType::Snapshot);
        let travel = read_options_for(&HudiQueryArgs {
            as_of: Some("x".into()),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(travel.as_of_timestamp(), Some("x"));
        let window = read_options_for(&HudiQueryArgs {
            start: Some("a".into()),
            end: Some("b".into()),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(window.query_type().unwrap(), QueryType::Incremental);
        assert_eq!(window.start_timestamp(), Some("a"));
        assert_eq!(window.end_timestamp(), Some("b"));
        let open_ended = read_options_for(&HudiQueryArgs {
            start: Some("a".into()),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(open_ended.query_type().unwrap(), QueryType::Incremental);
    }

    #[test]
    fn filter_spec_maps_every_operator() {
        for op in ["=", "!=", "<", "<=", ">", ">=", "IN", "NOT IN"] {
            let spec = FilterSpec {
                column: "c".into(),
                op: op.into(),
                values: vec!["1".into(), "2".into()],
            };
            let multi = op == "IN" || op == "NOT IN";
            let single = FilterSpec {
                values: vec!["1".into()],
                ..spec.clone()
            };
            assert!(single.to_kernel().is_ok(), "{op}");
            assert_eq!(spec.to_kernel().is_ok(), multi, "{op} with two values");
        }
        let bad = FilterSpec {
            column: "c".into(),
            op: "LIKE".into(),
            values: vec!["x".into()],
        };
        assert!(bad.to_kernel().is_err());
    }

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
        let a = aligned
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
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

    /// Plans the in-repo COW fixture the way `read_hudi` does and streams its
    /// slices with a projection and a filter, checking the rows against the
    /// kernel's own `Table::read`.
    #[test]
    fn planned_scan_streams_projected_filtered_batches() {
        let p = fixture("cow_v6_nonpartitioned");
        let opened = open_table(&p, None).expect("open");
        let schema = read_schema(&opened.table).expect("schema");
        assert!(schema.field_with_name("_hoodie_commit_time").is_ok());

        let plan = plan_scan(&opened, &HudiQueryArgs::default(), Vec::new()).expect("plan");
        assert_eq!(plan.slices.len(), 1);
        assert_eq!(plan.partition_count(), 1);
        // The kernel estimates a file's rows from its size, so only presence is checked.
        assert!(plan.row_estimate().is_some());

        let filtered = plan_scan(
            &opened,
            &HudiQueryArgs::default(),
            vec![FilterSpec {
                column: "intField".into(),
                op: ">".into(),
                values: vec!["20000".into()],
            }],
        )
        .expect("plan");
        let columns = vec!["intField".to_string(), "id".to_string()];
        let projected = Arc::new(Schema::new(vec![
            schema.field_with_name("intField").unwrap().clone(),
            schema.field_with_name("id").unwrap().clone(),
        ]));
        let options = slice_read_options(&filtered, &columns, 2).expect("options");
        let mut rows = 0;
        let mut sum = 0i64;
        for slice in &filtered.slices {
            let mut stream = open_slice_stream(&filtered, slice, &options).expect("stream");
            while let Some(batch) = runtime().block_on(stream.next()) {
                let batch = align_batch(&batch.expect("batch"), &projected).expect("align");
                assert!(batch.num_rows() <= 2, "batch of {} rows", batch.num_rows());
                let ints = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                sum += ints.iter().flatten().map(i64::from).sum::<i64>();
                rows += batch.num_rows();
            }
        }
        let reference = hudi_read_batches(&p, &HudiQueryArgs::default()).expect("read");
        assert_eq!(total_rows(&reference), 4);
        assert_eq!(rows, 3, "one of the four rows has intField <= 20000");
        assert_eq!(sum, 120_000 - 15_000);
    }

    #[test]
    fn metadata_batches_for_the_fixtures() {
        let cow = open_table(&fixture("cow_v6_nonpartitioned"), None).expect("open");
        let (info, files) = table_metadata("x", &cow.table).expect("meta");
        assert!(info.contains("type=COPY_ON_WRITE"), "{info}");
        assert_eq!(files.len(), 1);
        let commits = commits_batches(&cow.table).expect("commits");
        assert_eq!(total_rows(&commits), 2);
        let stats = stats_batches(&cow.table).expect("stats");
        assert_eq!(total_rows(&stats), 16);
        assert!(mdt_batches(&cow.table)
            .unwrap_err()
            .contains("no metadata table"));

        let mdt =
            open_table(&fixture("mor_v9_mdt_simple/v9_txns_simple_meta"), None).expect("open");
        let files = mdt_batches(&mdt.table).expect("mdt");
        assert!(total_rows(&files) >= 17);
    }
}
