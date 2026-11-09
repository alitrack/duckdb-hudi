#!/usr/bin/env bash
# Checks that read_hudi reads a remote table through DuckDB's external file
# cache (hudi_duckdb_file_system).
#
# Usage:
#   scripts/duckdb_file_cache_check.sh <s3 table uri> [extension] [duckdb-bin]
#
#   s3 table uri: a Hudi table, e.g. s3://example-bucket/path/to/table
#   extension:    the built .duckdb_extension
#                 (default: build/release/extension/hudi/hudi.duckdb_extension)
#   duckdb-bin:   a DuckDB CLI of the version the extension was built for
#                 (default: build/release/duckdb, else `duckdb` on PATH)
#
# Credentials: DuckDB reads through a credential_chain S3 secret, and the Hudi
# kernel lists through the AWS_* environment (or the instance profile), so the
# box needs read access to the table either way. Set AWS_REGION when the bucket
# is not in the default region. HUDI_OPTION, when set, is passed to read_hudi
# as `hudi_option`; it must hold only `hoodie.*` keys, since a storage option
# keeps the table on the kernel's own I/O.
#
# It checks, each in a fresh DuckDB process:
#   1. read_hudi returns the same rows with the setting on and off, and the
#      table opened with the setting on is marked as read through DuckDB.
#   2. With the setting on, a second identical query issues (almost) no GET
#      bytes: DuckDB's HTTP log counts the GETs and their content-length.
#   3. With enable_external_file_cache = false, a repeat of the query
#      downloads again.
#
# Exit 0 iff every check passes.

set -euo pipefail

URI="${1:?usage: duckdb_file_cache_check.sh <s3 table uri> [extension] [duckdb-bin]}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$SCRIPT_DIR/.."
EXT="${2:-$ROOT/build/release/extension/hudi/hudi.duckdb_extension}"
if [ -n "${3:-}" ]; then
  DB="$3"
elif [ -x "$ROOT/build/release/duckdb" ]; then
  DB="$ROOT/build/release/duckdb"
else
  DB=duckdb
fi
[ -f "$EXT" ] || { echo "FATAL: extension not found: $EXT" >&2; exit 2; }

# Fraction of the first query's GET bytes the cached repeat may still download
# (validation and footer reads of files the cache does not hold).
CACHED_MAX_PCT="${CACHED_MAX_PCT:-5}"
# Fraction of the first query's GET bytes the uncached repeat must download.
UNCACHED_MIN_PCT="${UNCACHED_MIN_PCT:-50}"

ARGS="'$URI'"
if [ -n "${HUDI_OPTION:-}" ]; then
  ARGS="$ARGS, hudi_option => '$HUDI_OPTION'"
fi
# Every result line starts with a tag, so the lines other statements print
# (CREATE SECRET reports success) are told apart.
query() { echo "SELECT '$1', count(*) || ',' || sum(hash(t)::HUGEINT) FROM read_hudi($ARGS) t;"; }

PRELUDE="LOAD httpfs; LOAD '$EXT';
CREATE SECRET (TYPE s3, PROVIDER credential_chain${AWS_REGION:+, REGION '$AWS_REGION'});"

# GET requests and bytes in DuckDB's HTTP log since the last truncate, as
# `requests,bytes`.
gets() {
  echo "SELECT '$1', count(*) || ',' || coalesce(sum(TRY_CAST(response.headers['content-length'] AS BIGINT)), 0)
FROM duckdb_logs_parsed('HTTP') WHERE request.type = 'GET';"
}

# Run a script; print its result lines as `tag|value`, without headers.
run() { "$DB" -unsigned -list -separator '|' -noheader -c "$1" | tr -d '\r'; }
# The value of the line tagged $2 in output $1.
tagged() { printf '%s\n' "$1" | sed -n "s/^$2|//p" | head -1; }

PASS=0; FAIL=0
ok()  { PASS=$((PASS+1)); printf '  ok   %s: %s\n' "$1" "$2"; }
bad() { FAIL=$((FAIL+1)); printf '  FAIL %s: %s\n' "$1" "$2"; }

echo "== 1. same rows with hudi_duckdb_file_system on and off =="
off="$(tagged "$(run "$PRELUDE SET hudi_duckdb_file_system = false; $(query rows)")" rows)"
on_out="$(run "$PRELUDE $(query rows) SELECT 'marked', duckdb_file_system FROM hudi_table_cache();")"
on="$(tagged "$on_out" rows)"
marked="$(tagged "$on_out" marked)"
echo "  off: rows,hash = $off"
echo "  on:  rows,hash = $on"
if [ -n "$off" ] && [ "$off" = "$on" ]; then ok "rows" "equal"; else bad "rows" "differ"; fi
if [ "$marked" = "true" ]; then
  ok "read through DuckDB" "hudi_table_cache().duckdb_file_system = true"
else
  bad "read through DuckDB" "hudi_table_cache().duckdb_file_system = '$marked' (is httpfs loadable?)"
fi

echo "== 2./3. external file cache =="
out="$(run "$PRELUDE
CALL enable_logging('HTTP', storage = 'memory');
$(query rows1)
$(gets gets1)
CALL truncate_duckdb_logs();
$(query rows2)
$(gets gets2)
SELECT 'cached', count(*) || ',' || coalesce(sum(nr_bytes), 0) FROM duckdb_external_file_cache();
CALL truncate_duckdb_logs();
SET enable_external_file_cache = false;
$(query rows3)
$(gets gets3)")"
first_rows="$(tagged "$out" rows1)"
first="$(tagged "$out" gets1)"
second_rows="$(tagged "$out" rows2)"
second="$(tagged "$out" gets2)"
cached="$(tagged "$out" cached)"
third_rows="$(tagged "$out" rows3)"
third="$(tagged "$out" gets3)"
echo "  first query:          GETs,bytes = $first"
echo "  repeat, cache on:     GETs,bytes = $second"
echo "  cache contents:       blocks,bytes = $cached"
echo "  repeat, cache off:    GETs,bytes = $third"

first_bytes="${first#*,}"; second_bytes="${second#*,}"; third_bytes="${third#*,}"
if [ "$first_rows" = "$on" ] && [ "$second_rows" = "$on" ] && [ "$third_rows" = "$on" ]; then
  ok "rows" "every query returned $on"
else
  bad "rows" "first=$first_rows second=$second_rows third=$third_rows want=$on"
fi
if [ "${first_bytes:-0}" -gt 0 ]; then
  ok "first query downloads" "$first_bytes bytes"
else
  bad "first query downloads" "no GET bytes logged; DuckDB did not serve the reads"
fi
if [ "${first_bytes:-0}" -gt 0 ] && [ $((second_bytes * 100)) -le $((first_bytes * CACHED_MAX_PCT)) ]; then
  ok "cached repeat" "$second_bytes bytes <= ${CACHED_MAX_PCT}% of $first_bytes"
else
  bad "cached repeat" "$second_bytes bytes, more than ${CACHED_MAX_PCT}% of $first_bytes"
fi
if [ "${first_bytes:-0}" -gt 0 ] && [ $((third_bytes * 100)) -ge $((first_bytes * UNCACHED_MIN_PCT)) ]; then
  ok "uncached repeat" "$third_bytes bytes >= ${UNCACHED_MIN_PCT}% of $first_bytes"
else
  bad "uncached repeat" "$third_bytes bytes, less than ${UNCACHED_MIN_PCT}% of $first_bytes"
fi

echo
echo "passed $PASS, failed $FAIL"
[ "$FAIL" -eq 0 ]
