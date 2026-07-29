.PHONY: all configure debug release test clean clean_all

PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

EXTENSION_NAME=hudi
USE_UNSTABLE_C_API=0
TARGET_DUCKDB_VERSION=v1.2.0
SKIP_TESTS=1

all: release

include extension-ci-tools/makefiles/c_api_extensions/base.Makefile
include extension-ci-tools/makefiles/c_api_extensions/rust.Makefile
