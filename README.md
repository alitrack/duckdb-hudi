# duckdb-hudi

Read [Apache Hudi](https://hudi.apache.org/) tables in DuckDB through the
[hudi-rs](https://github.com/apache/hudi-rs) kernel.

The extension is a C++ DuckDB extension (`src/`) over a Rust static library
(`rust/`). The Rust side drives hudi-rs: it parses the timeline, plans the file
slices of a read after partition and file pruning, merges copy-on-write and
merge-on-read slices, and hands each slice's rows over the Arrow C Data
Interface. The C++ side registers the functions, translates the filters DuckDB
pushes into a scan into hudi-rs predicates, and converts Arrow batches with
DuckDB's own Arrow reader. Neither side carries timeline, Parquet or merge
logic of its own; the kernel is the source of truth.

## Query surface

```sql
-- Latest snapshot (default).
SELECT * FROM read_hudi('/path/to/hudi_table');

-- Time-travel to the latest commit <= `as_of` (an instant like 20250117083125837).
SELECT * FROM read_hudi('/path/to/table', as_of => '20250117083125837');

-- Incremental: rows changed in (start, end]. `end` is a reserved word in DuckDB,
-- so quote it as a named parameter.
SELECT * FROM read_hudi('/path/to/table',
    start => '20250117083125837',
    "end" => '20250117083136333');

-- A lone `start` reads the changes from `start` up to the latest commit.
SELECT * FROM read_hudi('/path/to/table', start => '20250117083125837');

-- Cloud / storage credentials and reader tuning, passed through to the kernel:
--   hudi_option => 'k=v; k2=v2'
SELECT * FROM read_hudi('s3://bucket/table',
    hudi_option => 'hoodie.aws.region=us-west-2; hoodie.s3.access_key_id=AK…; hoodie.s3.secret_access_key=***');
```

`as_of` (time-travel snapshot) and `start`/`end` (incremental) are mutually
exclusive — the kernel treats them as different query types, and `read_hudi`
rejects combining them.

| Named param   | Type      | Meaning                                                            |
|---------------|-----------|--------------------------------------------------------------------|
| `as_of`       | VARCHAR   | Time-travel to the latest commit `<=` this instant.                |
| `start`       | VARCHAR   | Incremental lower bound (exclusive). Pair with `end` for a window. |
| `end`         | VARCHAR   | Incremental upper bound (inclusive). Reserved word — quote it.     |
| `hudi_option` | VARCHAR   | `k=v; k2=v2` passthrough of Hudi reader config (creds, tuning).    |

The result schema is the kernel's Arrow schema mapped by DuckDB's Arrow reader,
so every Arrow type the kernel can emit (decimal, timestamp-with-tz,
list/map/struct, interval, binary, …) flows through unchanged. A local path may
be relative to the working directory.

## Filter pushdown

A `WHERE` on a `read_hudi` scan prunes partitions and file slices before any
data is read, and the kernel masks the remaining rows, so a selective query
touches only the files it needs. `EXPLAIN` shows what the kernel received and
what survived pruning:

```
READ_HUDI
  Table: /data/orders
  Query: snapshot
  Hudi Filters: region = us AND order_date >= 2024-01-01
  Partitions: 1 / 12
  File Slices: 3 / 40
```

Pushed predicates are `column op literal` (`=`, `!=`, `<`, `<=`, `>`, `>=`),
`IN`, `NOT IN`, `BETWEEN` and `AND` of these, on columns of type BOOLEAN,
TINYINT to BIGINT, VARCHAR, DATE and DECIMAL. When DuckDB compares a column
through a cast it added (an INTEGER column against a BIGINT literal, a DATE
column against a TIMESTAMP), the literal is pushed only if it converts to the
column's type exactly. Other predicates (floats, timestamps, `OR`, `IS NULL`,
`LIKE`, functions, nested fields) are evaluated by DuckDB alone. Every pushed
filter stays in DuckDB's plan as well, so a predicate the kernel cannot use
only costs pruning, never rows. Time travel and incremental reads prune the
same way; data-column filters on an incremental read mask rows only.

`SET hudi_row_estimate = true` reports the kernel's row count of the planned
file slices as the scan's estimated cardinality. It is off by default because
hudi-rs 0.5.0 estimates that count from file sizes unless the metadata table
records it.

## Supported tables

Verified end-to-end on DuckDB 1.5.4 (unsigned, `LOAD`) against the hudi-rs
sample tables:

- **Storage formats**: Parquet, Avro, and Lance base files.
- **Table types**: Copy-on-Write and Merge-on-Read (real base+log merge).
- **Layouts**: partitioned (hivestyle / time-based / non-hivestyle) and
  non-partitioned, v6/v8/v9.
- **Timeline**: empty tables (0 rows, correct schema), rollbacks, and transactional
  tables.

A zero-row read (empty table, or an incremental window with no new commits)
returns the table's schema with no rows rather than erroring.

## Metadata functions

Two scalar functions report table structure, all sourced from the kernel (no
hand-rolled timeline parsing):

```sql
-- One-line summary: table type, base file format, latest commit, counts.
SELECT hudi_info('/path/to/table');
-- → Hudi table: type=MERGE_ON_READ base_format=parquet latest_commit=2025… commits=2 files=2

-- One line per file slice: full path, COW/MOR/LOG kind, size, log-file count.
SELECT hudi_files('/path/to/table');
```

`hudi_files` distinguishes each slice: `COW` (base only), `MOR` (base + log),
and `LOG` (log-only slices a slice written straight to log files).

## Hudi indexes & timeline

Two table functions expose Hudi's index / timeline data as queryable rows — the
first step in "more Hudi indexes under DuckDB SQL". Both are kernel-sourced (no
hand-rolled parsing) and bridge through the same Arrow path as `read_hudi`.

```sql
-- The commit timeline, one row per completed commit.
SELECT timestamp, action, completion, epoch_millis
FROM hudi_commits('/path/to/table');
-- → 20250117083125837 | commit | NULL | 1737131485837

-- The table's `files` index: one row per data file (partition, name, size, tombstone).
SELECT partition_path, file_name, size, is_deleted
FROM hudi_mdt('/path/to/table')
WHERE is_deleted = false;
```

`hudi_mdt` decodes the metadata table's `files` index (the per-file map Hudi
maintains for file discovery) into flat rows, so the index content is usable in
SQL. Only the `files` index is exposed in this release: the other index
partitions (`record_index`, `bloom_filters`, `column_stats`, `partition_stats`,
`secondary_index_*`) are stored as an Avro union-of-records that (a) hudi-rs 0.5.0's
`avro_to_arrow` cannot yet decode through the public read path (`AvroSchema::Ref`
unimplemented) and (b) DuckDB cannot import as a dense Arrow union. Exposing them
needs a small hudi-rs visibility PR plus per-index decoders — see
[DESIGN.md](DESIGN.md). `hudi_mdt` reports a clean error on tables without a
metadata table enabled.

`hudi_stats` exposes the **column statistics** index a different way: instead of
reading the (union-of-records) metadata table, it reads each parquet file's footer
and hands it to the kernel's own `StatisticsContainer::from_parquet_metadata` — the
same code path the kernel's file pruner uses — so the min/max values are the
kernel's, not a second implementation. One row per (data file, column):

```sql
-- Per-file column stats: the index Hudi uses to skip whole files.
SELECT file, column, data_type, min_value, max_value, num_rows
FROM hudi_stats('/path/to/table')
WHERE column = 'txn_date';
```

The min/max are rendered as strings (via Arrow's display) so the column stays
queryable across heterogeneous types; `WHERE min_value < ...` works on the
string form for fixed-width numerics and ISO dates.

## Testing

- **Rust unit tests** (`rust/src/kernel.rs`): `hudi_option` parsing, the
  `as_of`/`start`/`end` to query-type mapping, filter mapping, batch alignment,
  and a planned, filtered, projected scan of the in-repo fixtures checked
  against the kernel's own `Table::read`.

  ```sh
  cd rust && cargo test
  ```

- **sqllogictests** (`test/sql/`): the query surface, the metadata functions,
  and filter pushdown (pruned results equal unpruned ones on partitioned COW,
  MOR and hive-style tables, NULL semantics per operator, EXPLAIN output).
  The partitioned tables live as zip archives under `test/fixture`, each with
  a note on how it was written; `make test_release` inflates them first.

  ```sh
  make test_release
  ```

- **SQL gate** (`scripts/gate.sh`): drives the loadable extension through a
  DuckDB CLI with known-good values.

  ```sh
  scripts/gate.sh build/release/extension/hudi/hudi.duckdb_extension build/release/duckdb
  ```

## Building

The build follows DuckDB's extension template: CMake builds DuckDB from the
`duckdb` submodule (pinned to v1.5.6) together with the extension, and runs
`cargo` for the Rust library. `hudi-rs` transitively pulls Lance, whose build
script needs `protoc`; the build uses `$PROTOC`, else one on `PATH`, else
downloads a pinned release under `third_party/protoc`.

```sh
git submodule update --init
GEN=ninja make release
# -> build/release/extension/hudi/hudi.duckdb_extension (loadable)
# -> build/release/duckdb (CLI with the extension statically linked)
```

Load the artifact in an **unsigned** DuckDB of the same version:

```sh
duckdb -unsigned -c "LOAD '/abs/path/hudi.duckdb_extension'; SELECT * FROM read_hudi('/path/to/table');"
```

`hudi` is built with `default-features = false`, dropping `spill-rocksdb` (the
MOR merge spill layer). COW reads and typical MOR merges need no spill; re-enable
the feature for very large MOR merges that would otherwise spill.

### Windows

Builds on Windows (MSVC + Ninja, `x86_64-pc-windows-msvc` Rust target). Two
caveats, both kernel-side and tracked at
[apache/hudi-rs#825](https://github.com/apache/hudi-rs/issues/825):

- **Local tables must be addressed as `file://` URIs.** The kernel's storage
  layer rejects bare Windows paths (`Unable to recognise URL "d:\..."`):

  ```sql
  SELECT * FROM read_hudi('file:///D:/data/orders_hudi/');
  ```

- **Two-level hive-style partitioned tables currently return no rows on
  Windows** (partition discovery fails; the same table reads correctly on
  Linux/macOS). Single-level and non-partitioned tables, including filter
  pushdown, work. Related: upstream
  [apache/hudi-rs#445](https://github.com/apache/hudi-rs/issues/445) (Windows
  path separator handling).

## Design & status

See [DESIGN.md](DESIGN.md) for the kernel-based design and its phase plan. The
query surface (`as_of`, `start`/`end`, `hudi_option`), the metadata functions
(`hudi_info` / `hudi_files` / `hudi_commits` / `hudi_mdt` / `hudi_stats`) and
filter pushdown are complete and verified; cloud credentials pass through
`hudi_option`.
