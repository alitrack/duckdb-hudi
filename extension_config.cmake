# This file is included by DuckDB's build system. It specifies which extension to load

# Extension from this repo
duckdb_extension_load(hudi
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    LOAD_TESTS
)
# DuckDB 2.0 links an extension into the shell and unittest only when listed here.
if(COMMAND duckdb_extension_statically_link)
    duckdb_extension_statically_link(hudi)
endif()
