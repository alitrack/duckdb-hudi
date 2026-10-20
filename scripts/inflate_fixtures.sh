#!/usr/bin/env bash
# Inflate every zipped fixture under test/fixture next to its archive, so the
# sqllogictests can read it as a table directory. Each archive holds its table
# directory at the root (see the .md next to it). Idempotent: an inflated
# fixture is left alone.
set -euo pipefail

FIXTURES="$(cd "$(dirname "$0")/../test/fixture" && pwd)"
for zip in "$FIXTURES"/*.zip; do
  name="$(basename "$zip" .zip)"
  if [ -f "$FIXTURES/$name/.hoodie/hoodie.properties" ]; then
    continue
  fi
  unzip -q -o "$zip" -d "$FIXTURES"
  echo "inflated $name"
done
