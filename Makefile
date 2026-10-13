EXTENSION_NAME=hudi
TARGET_DUCKDB_VERSION=v1.2.0
USE_UNSTABLE_C_API=0
SKIP_TESTS=1

# COW read + the in-repo COW fixture need no spill; the kernel is built with
# `default-features = false` (no spill-rocksdb), so a stock build suffices.
# Re-enable `spill-rocksdb` (and a larger CI matrix) for large MOR merges.

all: release

# --- Vendored protoc (self-contained build) ---
# The hudi kernel pulls in Lance, whose build.rs runs `prost-build` and so
# needs a `protoc` binary on the build host. DuckDB's community build images
# do not ship one, so we vendor a pinned prebuilt protoc (or reuse a system
# protoc when present) and point prost-build at it via PROTOC. This keeps the
# community build working without depending on the host image.
PROTODIR := $(abspath third_party/protoc)
ifeq ($(OS),Windows_NT)
PROTOC ?= $(PROTODIR)/bin/protoc.exe
else
PROTOC ?= $(PROTODIR)/bin/protoc
endif
export PROTOC
PROTOC_INCLUDE := $(PROTODIR)/include
export PROTOC_INCLUDE

.PHONY: ensure_protoc
ensure_protoc:
	@bash scripts/bootstrap_protoc.sh "$(PROTOC)" "$(PROTODIR)"

# Guarantee protoc exists before cargo compiles the hudi/Lance tree. Declared
# before the includes so it lands first in the (merged) prerequisite list of
# the library build targets defined by rust.Makefile.
build_extension_library_release: ensure_protoc
build_extension_library_debug: ensure_protoc

# The community extension CI invokes `make test_release` / `make test_debug`,
# which the C-API Rust template (base.Makefile) does not define directly — it
# only defines test_extension_release/debug. Alias them. With SKIP_TESTS=1
# (this extension's test suite is validated by our own CI SQL gate, not run in
# the community build) these resolve to the tests_skipped NOP, so the community
# build completes without a missing-target error.
test_release: test_extension_release
test_debug: test_extension_debug

include extension-ci-tools/makefiles/c_api_extensions/base.Makefile
include extension-ci-tools/makefiles/c_api_extensions/rust.Makefile

configure: venv platform extension_version
debug: build_extension_library_debug build_extension_with_metadata_debug
release: build_extension_library_release build_extension_with_metadata_release
test: test_debug
test_debug: test_extension_debug
clean: clean_build clean_rust
clean_all: clean_configure clean
