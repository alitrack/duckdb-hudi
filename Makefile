EXTENSION_NAME=hudi
TARGET_DUCKDB_VERSION=v1.2.0
USE_UNSTABLE_C_API=0
SKIP_TESTS=1

# COW read + the in-repo COW fixture need no spill; the kernel is built with
# `default-features = false` (no spill-rocksdb), so a stock build suffices.
# Re-enable `spill-rocksdb` (and a larger CI matrix) for large MOR merges.

all: release

include extension-ci-tools/makefiles/c_api_extensions/base.Makefile
include extension-ci-tools/makefiles/c_api_extensions/rust.Makefile

configure: venv platform extension_version
debug: build_extension_library_debug build_extension_with_metadata_debug
release: build_extension_library_release build_extension_with_metadata_release
test: test_debug
test_debug: test_extension_debug
clean: clean_build clean_rust
clean_all: clean_configure clean
