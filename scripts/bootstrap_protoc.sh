#!/usr/bin/env bash
# Self-bootstrap a self-contained `protoc` for the hudi -> Lance -> prost-build
# chain.
#
# prost-build (pulled in transitively by the hudi kernel via Lance) needs a
# `protoc` binary on the build host. DuckDB's community build images do not
# ship one, so we make the extension self-contained and reproducible: download
# a pinned, self-contained prebuilt protoc release (it bundles its own
# include/ well-known protos and needs no system libraries) and vendor it under
# third_party/. The build then points prost-build at it via PROTOC /
# PROTOC_INCLUDE. Idempotent: no-ops when the vendored binary already works.
#
#   $1 = absolute path the protoc binary must end up at (e.g. .../third_party/protoc/bin/protoc)
#   $2 = vendoring root the zip is extracted into (e.g. .../third_party/protoc)
set -euo pipefail

PROTOC_BIN="$1"
ROOT="$2"
VERSION=26.1

# Idempotency: if a working protoc is already vendored, do nothing.
if [ -x "$PROTOC_BIN" ] && "$PROTOC_BIN" --version >/dev/null 2>&1; then
  echo "[bootstrap-protoc] already present: $PROTOC_BIN"
  exit 0
fi

OS="$(uname -s)"
ARCH="$(uname -m)"
case "$OS" in
  Darwin)
    if [ "$ARCH" = "arm64" ]; then A=osx-aarch_64; else A=osx-x86_64; fi
    ;;
  Linux)
    if [ "$ARCH" = "aarch64" ] || [ "$ARCH" = "arm64" ]; then A=linux-aarch_64; else A=linux-x86_64; fi
    ;;
  MINGW*|MSYS*|CYGWIN*)
    A=win64
    ;;
  *)
    echo "[bootstrap-protoc] unsupported OS: $OS" >&2
    exit 1
    ;;
esac

URL="https://github.com/protocolbuffers/protobuf/releases/download/v${VERSION}/protoc-${VERSION}-${A}.zip"
echo "[bootstrap-protoc] downloading $URL"
mkdir -p "$ROOT"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
curl -fsSL --retry 3 "$URL" -o "$TMP/protoc.zip"

# Extract the zip (bin/protoc + include/) with whatever the minimal image has.
if command -v unzip >/dev/null 2>&1; then
  unzip -oq "$TMP/protoc.zip" -d "$ROOT"
elif command -v python3 >/dev/null 2>&1; then
  python3 -c "import zipfile,sys; zipfile.ZipFile(sys.argv[1]).extractall(sys.argv[2])" "$TMP/protoc.zip" "$ROOT"
elif command -v busybox >/dev/null 2>&1; then
  busybox unzip -oq "$TMP/protoc.zip" -d "$ROOT"
else
  echo "[bootstrap-protoc] no unzip/python3/busybox available to extract $TMP/protoc.zip" >&2
  exit 1
fi

chmod +x "$ROOT/bin/protoc" 2>/dev/null || true
"$PROTOC_BIN" --version
echo "[bootstrap-protoc] ready: $PROTOC_BIN (includes: $ROOT/include)"
