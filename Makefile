PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=hudi
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Windows: Rust's MSVC target links the static CRT (libcmt). The hudi/Lance
# tree pulls C deps (zstd-sys, etc.) compiled via CMake, which on MSVC default
# to the dynamic CRT; mixing the two leaves unresolved symbols. Force the
# CMake-built C tree to the matching static runtime.
ifeq ($(OS),Windows_NT)
export CMAKE_MSVC_RUNTIME_LIBRARY := MultiThreaded
endif

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile
