#!/usr/bin/env bash
# A commit made between two queries of one session is visible to the second,
# with read_hudi's table cache on.
#
# Copies the two-commit COW fixture with its latest commit's completed instant
# held back (the commit is inflight: its data file is written, not yet
# visible), reads it, then completes the commit from inside the same CLI
# session with `.shell` and reads it again.
#
# Usage:
#   scripts/table_cache_freshness.sh [duckdb-bin]
#
#   duckdb-bin: a DuckDB CLI with the extension linked in (default: build/release/duckdb)
#
# Exit 0 iff the session sees the commit.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DB="${1:-$ROOT/build/release/duckdb}"
[ -x "$DB" ] || { echo "FATAL: duckdb CLI not found: $DB" >&2; exit 2; }

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
TMP="$(cd "$TMP" && pwd -P)"
cp -R "$ROOT/test/fixture/cow_v6_nonpartitioned" "$TMP/table"
INSTANT=".hoodie/20240418173551906.commit"
mv "$TMP/table/$INSTANT" "$TMP/held.commit"

got="$("$DB" -csv -noheader 2>&1 <<SQL
SELECT 'before', count(*), sum("intField") FROM read_hudi('$TMP/table');
SELECT 'repeat', count(*), sum("intField") FROM read_hudi('$TMP/table');
.shell mv '$TMP/held.commit' '$TMP/table/$INSTANT'
SELECT 'after', count(*), sum("intField") FROM read_hudi('$TMP/table');
SELECT 'as_of', count(*), sum("intField") FROM read_hudi('$TMP/table', as_of => '20240418173550988');
SELECT 'cache', opens, hits FROM hudi_table_cache();
SQL
)"

want="before,3,75000
repeat,3,75000
after,4,120000
as_of,3,75000
cache,2,2"

if [ "$got" = "$want" ]; then
  echo "ok: a commit between two queries is visible to the second"
else
  echo "FAIL: table cache freshness"
  echo "--- want"
  echo "$want"
  echo "--- got"
  echo "$got"
  exit 1
fi
