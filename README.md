# duckdb-hudi

Read [Apache Hudi](https://hudi.apache.org/) tables in DuckDB, natively in Rust,
through the [hudi-rs](https://github.com/apache/hudi-rs) kernel.

`read_hudi(uri, …)` drives the hudi-rs kernel — which parses the timeline,
discovers file slices, resolves the schema, and performs COW/MOR merging — and
bridges its Arrow output into DuckDB over the Arrow C Data Interface. This
extension owns **no** timeline / Parquet / merge logic of its own; the kernel is
the source of truth.

## Query surface

`read_hudi(uri, …)` drives the hudi-rs kernel — which parses the timeline,
discovers file slices, resolves the schema, and performs COW/MOR merging — and
bridges its Arrow output into DuckDB over the Arrow C Data Interface. This
extension owns **no** timeline / Parquet / merge logic of its own; the kernel is
the source of truth.

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

The result schema is **derived from a real import**, not hand-mapped, so every
Arrow type the kernel can emit (decimal, timestamp-with-tz, list/map/struct,
interval, binary, …) flows through unchanged.

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

Five table functions expose Hudi's index / timeline data as queryable rows —
"more Hudi indexes under DuckDB SQL". All are kernel-sourced (no hand-rolled
parsing) and bridge through the same Arrow path as `read_hudi`:

| Function                | Exposes                    | hudi-rs dependency            |
|-------------------------|----------------------------|-------------------------------|
| `hudi_commits(uri)`     | commit timeline            | crates.io (0.5.0)             |
| `hudi_mdt(uri)`         | `files` index              | crates.io (0.5.0)             |
| `hudi_stats(uri)`       | `column_stats` (footer)    | crates.io (0.5.0)             |
| `hudi_record_index(uri)`| `record_index` index       | [#819](https://github.com/apache/hudi-rs/pull/819) |
| `hudi_locate(uri, key)` | record-key point lookup    | [#819](https://github.com/apache/hudi-rs/pull/819) |
| `hudi_bloom(uri)`       | `bloom_filters` index      | [#819](https://github.com/apache/hudi-rs/pull/819) |

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
SQL. It reports a clean error on tables without a metadata table enabled.

The `column_stats` index is exposed a different way as `hudi_stats`: instead of
reading the (union-of-records) metadata table, it reads each parquet file's
footer and hands it to the kernel's own `StatisticsContainer::from_parquet_metadata` — the
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

### Record index & bloom filters (`hudi_record_index` / `hudi_locate` / `hudi_bloom`)

These three ride hudi-rs PR [#819](https://github.com/apache/hudi-rs/pull/819)
(`Table::read_metadata_table_partition_arrow`), not yet on crates.io — the
`hudi` dependency in this branch is pinned to that PR's commit. They read their
index partition straight through the kernel's metadata-table reader and decode
the struct payload to flat rows (DuckDB cannot import the dense unions the full
MDT schema carries; the payloads of these two indexes are plain structs, so no
union is involved).

```sql
-- Every indexed record key → which data file it lives in, at which position.
SELECT record_key, partition_name, file_id, file_index, position, instant_time
FROM hudi_record_index('/path/to/table');

-- Point lookup: the index's answer for one record key. One row on a hit,
-- zero rows for a key the table never wrote — not an error. The record key is
-- passed as-is; the kernel stores bare keys and shards on a Java string hash
-- internally, so there is no client-side hashing to get right.
SELECT * FROM hudi_locate('/path/to/table', 'TXN-011');

-- The bloom-filter index: one row per record key with the filter's
-- type/instant and byte size. Zero rows when the index is not populated.
SELECT record_key, filter_type, timestamp, filter_size, is_deleted
FROM hudi_bloom('/path/to/table');
```

Notes:

- `hudi_locate` is a targeted read: the kernel routes the key to exactly one
  index shard and opens only that file group, so it does not scan the whole
  index partition.
- Index partitions stored as log-only file groups with no readable records
  (Hudi's all-tombstone log case) surface a clean "no schema available for
  merge output" error — a kernel reader limitation tracked upstream.
- A table without a metadata table enabled gets the same clean error as
  `hudi_mdt`.

## Testing

- **Unit + kernel tests** (in `src/lib.rs`, `#[cfg(test)]`): `hudi_option`
  parsing, the `as_of`/`start`/`end` → `QueryType` mapping, and — against local
  fixtures — COW/MOR row counts, time-travel, and incremental windows. Run:

  ```sh
  ./scripts/materialize.sh      # extract the hudi-rs sample tables once
  cargo test --lib              # pure tests (always)
  cargo test --lib -- --ignored # + fixture tests
  ```

- **SQL gate**: build, stamp, and drive the real unsigned DuckDB 1.5.4 with
  `SELECT … FROM read_hudi(…)` / `hudi_info(…)` (see *Building* below). This is
  the authoritative end-to-end check, since `bind` imports on the global
  connection established only inside the C-API entry point.

## Building

`hudi-rs` transitively pulls Lance → `prost-build`, so `protoc` is a hard
build prerequisite:

```sh
brew install protobuf        # or your platform's equivalent
export PROTOC=/opt/homebrew/bin/protoc
cargo build --release        # -> target/release/libhudi.dylib
```

Then stamp the loadable-extension metadata footer (the C-API version, `v1.2.0`,
and the target platform — not the SQL version):

```sh
python3 extension-ci-tools/scripts/append_extension_metadata.py \
    -l target/release/libhudi.dylib -n hudi \
    -p osx_arm64 -dv v1.2.0 -ev 0.2.0 \
    -o hudi.duckdb_extension
```

Load in an **unsigned** build of DuckDB 1.5.4:

```sh
duckdb -unsigned -c "LOAD '/abs/path/hudi.duckdb_extension'; SELECT * FROM read_hudi('/path/to/table');"
```

`hudi` is built with `default-features = false`, dropping `spill-rocksdb` (the
MOR merge spill layer). COW reads and typical MOR merges need no spill; re-enable
the feature for very large MOR merges that would otherwise spill.

## Design & status

See [DESIGN.md](DESIGN.md) for the architecture, the P0→P6 phase plan, and the
known risks. P0 (read COW/MOR via the kernel), the P1/P3 query surface
(`as_of`, `start`/`end`, `hudi_option`), and P2 metadata (`hudi_info` /
`hudi_files` / `hudi_commits` / `hudi_mdt` / `hudi_stats`) are complete and
verified. On this branch the T2 index surface — `hudi_record_index`,
`hudi_locate`, `hudi_bloom` — is implemented and gate-verified (47 assertions)
against the hudi-rs [#819](https://github.com/apache/hudi-rs/pull/819) branch;
it merges to master when #819 lands and releases. P4 (cloud credential wiring)
and the push-down wiring remain.
