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
known risks. P0 (read COW/MOR via the kernel) and the P1/P3 query surface
(`as_of`, `start`/`end`, `hudi_option`) are complete and verified; P2
(`hudi_info`/`hudi_files`/`hudi_commits` metadata functions) and P4 (cloud
credential wiring) remain.
