#!/usr/bin/env bash
# Materialise the hudi-rs sample-table fixtures that the in-crate tests and the
# real-DuckDB gate read from. Set HUDI_RS to point at a hudi-rs checkout
# (defaults to ~/ws/hudi-rs), or HUDI_SAMPLE to an already-extracted copy.
set -euo pipefail

HUDI_RS="${HUDI_RS:-$HOME/ws/hudi-rs}"
SAMPLE="$HUDI_RS/crates/test/data/sample_table"
OUT="${OUT:-/tmp/hudi-sample}"

need() { [ -d "$SAMPLE" ] || { echo "sample tables not found under $SAMPLE — set HUDI_RS" >&2; exit 1; }; }
# Fetch a lazily-stored blob from the sparse clone, then unzip it under OUT.
pull() { # $1 = zip relpath under $SAMPLE, $2 = out subdir
  local zip="$SAMPLE/$1" dest="$OUT/$2"
  if [ -f "$dest/.hoodie/hoodie.properties" ] || [ -f "$dest/v6_nonpartitioned/.hoodie/hoodie.properties" ]; then
    return 0
  fi
  [ -s "$zip" ] || ( cd "$HUDI_RS" && git checkout HEAD -- "$1" )
  mkdir -p "$dest"
  unzip -oq "$zip" -d "$dest"
}

need
echo "→ COW v6 nonpartitioned (the P0 gate table)"
pull "cow/v6_nonpartitioned.zip" "cow"
echo "→ MOR v6 nonpartitioned (base + log merge, time-travel, incremental)"
pull "mor/parquet/v6_nonpartitioned.zip" "mor"
echo "done. Fixtures under $OUT:"
find "$OUT" -maxdepth 4 -name hoodie.properties 2>/dev/null
