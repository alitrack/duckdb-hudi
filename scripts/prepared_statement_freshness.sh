#!/usr/bin/env bash
# A prepared read_hudi statement reads the table as of each execution.
#
# Copies the two-commit COW fixture with its latest commit's completed instant
# held back (the commit is inflight: its data file is written, not yet
# visible), prepares and executes a read, then from inside the same CLI
# session completes the commit and deletes the file slice it replaced, as a
# clean would, and executes the prepared read again.
#
# Usage:
#   scripts/prepared_statement_freshness.sh [duckdb-bin]
#
#   duckdb-bin: a DuckDB CLI with the extension linked in (default: build/release/duckdb)
#
# Exit 0 iff the second execution sees the commit.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DB="${1:-$ROOT/build/release/duckdb}"
[ -x "$DB" ] || { echo "FATAL: duckdb CLI not found: $DB" >&2; exit 2; }

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
TMP="$(cd "$TMP" && pwd -P)"
cp -R "$ROOT/test/fixture/cow_v6_nonpartitioned" "$TMP/table"
INSTANT=".hoodie/20240418173551906.commit"
REPLACED="a079bdb3-731c-4894-b855-abfcd6921007-0_0-182-253_20240418173550988.parquet"
mv "$TMP/table/$INSTANT" "$TMP/held.commit"

got="$("$DB" -csv -noheader 2>&1 <<SQL
PREPARE scan AS SELECT count(*), sum("intField") FROM read_hudi('$TMP/table');
PREPARE commits AS SELECT count(*) FROM hudi_commits('$TMP/table');
EXECUTE scan;
EXECUTE commits;
.shell mv '$TMP/held.commit' '$TMP/table/$INSTANT'
.shell rm '$TMP/table/$REPLACED'
EXECUTE scan;
EXECUTE commits;
SQL
)" || true

want="3,75000
1
4,120000
2"

if [ "$got" = "$want" ]; then
  echo "ok: a prepared statement sees a commit made between two executions"
else
  echo "FAIL: prepared statement freshness"
  echo "--- want"
  echo "$want"
  echo "--- got"
  echo "$got"
  exit 1
fi
