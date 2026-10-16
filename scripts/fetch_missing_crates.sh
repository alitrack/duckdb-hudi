#!/bin/bash
# Repeatedly run cargo offline build; each time it reports one missing crate,
# fetch that .crate straight from static.crates.io into the rsproxy cache.
CACHE=~/.cargo/registry/cache/rsproxy.cn-e3de039b2554c837
cd /mnt/d/wsl2/duckdb-hudi || exit 1
for i in $(seq 1 500); do
  out=$(cargo build --release --offline 2>&1)
  line=$(printf '%s\n' "$out" | grep -a "failed to download" | head -1)
  if [ -z "$line" ]; then
    echo "=== ALL DEPS PRESENT; build output: ==="
    printf '%s\n' "$out" | grep -aE "^error|Compiling hudi |Compiling duckdb-hudi|Finished" | head -8
    break
  fi
  name=$(printf '%s\n' "$line" | awk '{print $5}' | tr -cd 'a-zA-Z0-9._-')
  ver=$(printf '%s\n' "$line" | awk '{print $6}' | sed 's/^v//' | tr -cd '0-9.')
  namever="${name}-${ver}"
  if [ -z "$namever" ] || [ "$namever" = "-" ]; then
    echo "unexpected error line: $line"
    break
  fi
  if [ -s "$CACHE/${namever}.crate" ]; then
    echo "already cached $namever"; break
  fi
  echo "fetching $namever ..."
  if curl -sSL --max-time 30 "https://static.crates.io/crates/${name}/${namever}.crate" -o "$CACHE/${namever}.crate" && [ -s "$CACHE/${namever}.crate" ]; then
    echo "  OK $(du -h "$CACHE/${namever}.crate" | cut -f1)"
  else
    echo "  fetch FAILED for $namever"
    rm -f "$CACHE/${namever}.crate"
  fi
done
echo "=== done ==="
