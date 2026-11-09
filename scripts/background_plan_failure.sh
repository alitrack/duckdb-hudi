#!/usr/bin/env bash
# A background plan that fails in one statement does not fail a later one.
#
# Copies a partitioned COW fixture and makes one partition unreadable, so
# planning a read of the table fails. Runs a statement that plans in the
# background (a filtered read, which fails, and a PREPARE of an unfiltered
# read, which takes no plan), then makes the partition readable again from
# inside the same CLI session with `.shell` and reads the whole table. The
# later reads must succeed, as storage is healthy by then.
#
# Usage:
#   scripts/background_plan_failure.sh [duckdb-bin]
#
#   duckdb-bin: a DuckDB CLI with the extension linked in (default: build/release/duckdb)
#
# Exit 0 iff every read after the partition is restored succeeds.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DB="${1:-$ROOT/build/release/duckdb}"
[ -x "$DB" ] || { echo "FATAL: duckdb CLI not found: $DB" >&2; exit 2; }

TMP="$(mktemp -d)"
trap 'chmod -R u+rwx "$TMP"; rm -rf "$TMP"' EXIT
TMP="$(cd "$TMP" && pwd -P)"
LOCKED="byteField=30/shortField=100"
for t in filtered prepared; do
  cp -R "$ROOT/test/fixture/cow_v6_complexkeygen_hivestyle" "$TMP/$t"
done

got="$("$DB" -csv -noheader 2>&1 <<SQL
.shell chmod 000 '$TMP/filtered/$LOCKED'
SELECT 'filtered', count(*) FROM read_hudi('$TMP/filtered') WHERE "byteField" = 20;
.shell chmod 755 '$TMP/filtered/$LOCKED'
SELECT 'filtered then all', count(*) FROM read_hudi('$TMP/filtered');
.shell chmod 000 '$TMP/prepared/$LOCKED'
PREPARE whole AS SELECT 'prepared then all', count(*) FROM read_hudi('$TMP/prepared');
.shell chmod 755 '$TMP/prepared/$LOCKED'
SELECT 'prepared then all', count(*) FROM read_hudi('$TMP/prepared');
EXECUTE whole;
SQL
)" || true
# The path in a planning error differs per run.
got="$(printf '%s\n' "$got" | sed 's/.*Permission denied.*/planning failed: permission denied/')"

want="planning failed: permission denied
filtered then all,4
prepared then all,4
prepared then all,4"

if [ "$got" = "$want" ]; then
  echo "ok: a failed background plan does not fail a later statement"
else
  echo "FAIL: background plan failure"
  echo "--- want"
  echo "$want"
  echo "--- got"
  echo "$got"
  exit 1
fi
