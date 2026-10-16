#!/usr/bin/env bash
# DuckDB Hudi extension SQL gate.
#
# Drives the built extension (unsigned) against the in-repo COW fixture and
# asserts known-good values across the full query surface: snapshot, as_of
# time-travel, incremental windows, zero-row synthesis, metadata functions,
# error paths, re-LOAD, and parallel scan.
#
# Usage:
#   scripts/gate.sh <extension.duckdb_extension> [duckdb-bin] [fixture-dir]
#
#   extension: path to the .duckdb_extension artifact (from `make release`)
#   duckdb-bin: a DuckDB >= 1.5.4 CLI (default: `duckdb` on PATH)
#   fixture-dir: a Hudi table dir (default: test/fixture/cow_v6_nonpartitioned)
#
# Exit 0 iff every assertion passes.

set -euo pipefail

EXT="${1:?usage: gate.sh <extension.duckdb_extension> [duckdb-bin] [fixture-dir]}"
DB="${2:-duckdb}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
FIX="${3:-$SCRIPT_DIR/../test/fixture/cow_v6_nonpartitioned}"
# Metadata-table (MDT) fixture: a table with `hoodie.metadata.enable` + the files
# index populated, so hudi_mdt (files index) can be exercised end-to-end.
MDTFIX="${4:-$SCRIPT_DIR/../test/fixture/mor_v9_mdt_simple/v9_txns_simple_meta}"

# The hudi-rs storage layer rejects any literal ".." in the table path
# ("illegal character sequence '..'"), so canonicalise both paths to absolute
# form without ".." before they reach the kernel / LOAD.
canon() {
  local p="$1"
  # A directory: `cd && pwd -P` lets the shell collapse any '..'.
  if [ -d "$p" ]; then
    ( cd "$p" && pwd -P )
    return $?
  fi
  # A file: canonicalise its directory, re-attach the basename.
  local dir base
  dir="$(dirname "$p")"
  base="$(basename "$p")"
  case "$dir" in
    ''|'/') dir='/' ;;
    *) if [ -d "$dir" ]; then dir="$( cd "$dir" && pwd -P )"; fi ;;
  esac
  case "$dir" in
    /*) printf '%s/%s' "$dir" "$base" ;;
    *)  printf '%s/%s/%s' "$PWD" "$dir" "$base" ;;
  esac
}
EXT="$(canon "$EXT")"
FIX="$(canon "$FIX")"
MDTFIX="$(canon "$MDTFIX")"

[ -f "$EXT" ] || { echo "FATAL: extension not found: $EXT" >&2; exit 2; }
[ -f "$FIX/.hoodie/hoodie.properties" ] || { echo "FATAL: fixture not found: $FIX" >&2; exit 2; }

# The COW fixture's two commits (first, latest) — commit timestamps used to
# exercise time-travel and the incremental window.
C1="20240418173550988"
C2="20240418173551906"

# Run one SQL statement with the extension loaded, CSV output, last line.
csv() { "$DB" -unsigned -csv -c "LOAD '$EXT'; $1" 2>/dev/null | tail -1 | tr -d '\r'; }
# Same but keep stderr (for error-path assertions); always succeeds.
raw() { "$DB" -unsigned -c "LOAD '$EXT'; $1" 2>&1 || true; }

PASS=0; FAIL=0
ok()  { PASS=$((PASS+1)); printf '  ok   %-26s = %s\n' "$1" "$2"; }
bad() { FAIL=$((FAIL+1)); printf '  FAIL %-26s got=%q want=%q\n' "$1" "$2" "$3"; }
eq()  { if [ "$2" = "$3" ]; then ok "$1" "$2"; else bad "$1" "$2" "$3"; fi; }
has() { if printf '%s' "$2" | grep -qF -- "$3"; then ok "$1" "contains:$3"; else bad "$1" "$2" "contains:$3"; fi; }

echo "== read_hudi query surface =="
eq "snapshot rows"        "$(csv "SELECT count(*) FROM read_hudi('$FIX')")" 4
eq "snapshot sum"         "$(csv "SELECT sum(\"intField\") FROM read_hudi('$FIX')")" 120000
eq "as_of first commit"   "$(csv "SELECT count(*) FROM read_hudi('$FIX', as_of => '$C1')")" 3
eq "as_of sum first"      "$(csv "SELECT sum(\"intField\") FROM read_hudi('$FIX', as_of => '$C1')")" 75000
eq "as_of latest"         "$(csv "SELECT count(*) FROM read_hudi('$FIX', as_of => '$C2')")" 4
eq "incr window rows"     "$(csv "SELECT count(*) FROM read_hudi('$FIX', start => '$C1', \"end\" => '$C2')")" 2
eq "incr window sum"      "$(csv "SELECT sum(\"intField\") FROM read_hudi('$FIX', start => '$C1', \"end\" => '$C2')")" 60000
eq "incr empty window"    "$(csv "SELECT count(*) FROM read_hudi('$FIX', start => '$C2', \"end\" => '$C2')")" 0

echo "== metadata functions =="
eq  "hudi_info NULL"      "$(csv "SELECT hudi_info(NULL::VARCHAR)")" "NULL"
has "hudi_info summary"   "$(csv "SELECT hudi_info('$FIX')")" "type=COPY_ON_WRITE"
has "hudi_info format"    "$(csv "SELECT hudi_info('$FIX')")" "base_format=parquet"
has "hudi_info commits"   "$(csv "SELECT hudi_info('$FIX')")" "commits=2"
has "hudi_files nonempty" "$(raw "SELECT hudi_files('$FIX')")" "parquet"
eq  "hudi_files line count" "$(csv "SELECT count(*) FROM (SELECT regexp_split_to_table(hudi_files('$FIX'), E'\n') t) WHERE t <> ''")" 1

echo "== error paths =="
has "mutex error"         "$(raw "SELECT * FROM read_hudi('$FIX', as_of => 'x', start => 'y')")" "mutually exclusive"
has "bad-path error"      "$(raw "SELECT * FROM read_hudi('/nonexistent/no/such/table')")" "failed to open"
has "null-uri error"      "$(raw "SELECT * FROM read_hudi(NULL::VARCHAR)")" "required"

echo "== re-LOAD + parallel scan =="
eq "double LOAD a"   "$("$DB" -unsigned -csv -c "LOAD '$EXT'; SELECT count(*) FROM read_hudi('$FIX'); LOAD '$EXT'; SELECT count(*) FROM read_hudi('$FIX')" 2>/dev/null | grep -oE '^[0-9]+$' | paste -sd, -)" "4,4"
eq "threads=16 rows" "$("$DB" -unsigned -csv -c "SET threads=16; LOAD '$EXT'; SELECT count(*) FROM read_hudi('$FIX')" 2>/dev/null | tail -1 | tr -d '\r')" 4
eq "threads=16 sum"  "$("$DB" -unsigned -csv -c "SET threads=16; LOAD '$EXT'; SELECT sum(\"intField\") FROM read_hudi('$FIX')" 2>/dev/null | tail -1 | tr -d '\r')" 120000

echo "== hudi_commits (timeline) =="
eq  "commits rows (COW)"   "$(csv "SELECT count(*) FROM hudi_commits('$FIX')")" 2
has "commits action"       "$(csv "SELECT action FROM hudi_commits('$FIX') ORDER BY timestamp LIMIT 1")" "commit"
has "commits ts first"     "$(csv "SELECT timestamp FROM hudi_commits('$FIX') ORDER BY timestamp LIMIT 1")" "$C1"

echo "== hudi_mdt (files index) =="
if [ -f "$MDTFIX/.hoodie/hoodie.properties" ] && [ -d "$MDTFIX/.hoodie/metadata/files" ]; then
  eq  "mdt data-file rows"   "$(csv "SELECT count(*) FROM hudi_mdt('$MDTFIX') WHERE partition_path <> '__all_partitions__' AND is_deleted = false")" 17
  # at least one real file row (name ends .parquet, size > 0)
  has "mdt real file name"   "$(csv "SELECT file_name FROM hudi_mdt('$MDTFIX') WHERE is_deleted = false AND size > 0 LIMIT 1")" ".parquet"
  has "mdt 3 partitions"     "$(csv "SELECT count(DISTINCT partition_path) FROM hudi_mdt('$MDTFIX') WHERE partition_path <> '__all_partitions__'")" 3
  has "mdt size sums"        "$(csv "SELECT sum(size) FROM hudi_mdt('$MDTFIX') WHERE partition_path = 'region=us' AND is_deleted = false")" 2629276
  has "mdt disabled-path err" "$(raw "SELECT * FROM hudi_mdt('$FIX')")" "no metadata table enabled"
else
  echo "  (MDT fixture not present — skipping hudi_mdt assertions)"
fi

echo "== hudi_stats (column_stats index) =="
# One row per (data file, column), from the parquet footer via the kernel's own
# StatisticsContainer. COW fixture: single file, 16 columns; cross-checked values.
eq  "stats rows (COW)"       "$(csv "SELECT count(*) FROM hudi_stats('$FIX')")" 16
eq  "stats files (COW)"      "$(csv "SELECT count(DISTINCT file) FROM hudi_stats('$FIX')")" 1
eq  "stats cols (COW)"       "$(csv "SELECT count(DISTINCT \"column\") FROM hudi_stats('$FIX')")" 16
has "stats date min"         "$(csv "SELECT min_value FROM hudi_stats('$FIX') WHERE \"column\" = 'dateField'")" "2023-04-01"
has "stats date max"         "$(csv "SELECT max_value FROM hudi_stats('$FIX') WHERE \"column\" = 'dateField'")" "2023-04-04"
has "stats int min"          "$(csv "SELECT min_value FROM hudi_stats('$FIX') WHERE \"column\" = 'id'")" "1"
has "stats int max"          "$(csv "SELECT max_value FROM hudi_stats('$FIX') WHERE \"column\" = 'id'")" "4"
if [ -f "$MDTFIX/.hoodie/hoodie.properties" ]; then
  eq  "stats rows (MDT)"     "$(csv "SELECT count(*) FROM hudi_stats('$MDTFIX')")" 65
  eq  "stats files (MDT)"    "$(csv "SELECT count(DISTINCT file) FROM hudi_stats('$MDTFIX')")" 5
  eq  "stats cols (MDT)"     "$(csv "SELECT count(DISTINCT \"column\") FROM hudi_stats('$MDTFIX')")" 13
fi

echo
echo "== gate result: $PASS passed, $FAIL failed =="
[ "$FAIL" -eq 0 ] || exit 1
echo "ALL PASS"
