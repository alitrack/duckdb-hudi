cmake_minimum_required(VERSION 3.14...3.29)

set(TARGET_NAME hudi)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})

set(CMAKE_CXX_STANDARD "17" CACHE STRING "C++ standard to enforce")
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include_directories(src/include)

set(EXTENSION_SOURCES
    src/hudi_extension.cpp
    src/hudi_arrow_stream.cpp
    src/hudi_filter.cpp
    src/hudi_metadata.cpp
    src/hudi_scan.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# System libraries the Rust dependency tree (object_store TLS, tokio, ...) needs.
if(APPLE)
  if(NOT DEFINED CMAKE_OSX_DEPLOYMENT_TARGET OR CMAKE_OSX_DEPLOYMENT_TARGET
                                                STREQUAL "")
    set(CMAKE_OSX_DEPLOYMENT_TARGET
        13.3
        CACHE STRING "Minimum OS X deployment version" FORCE)
  endif()
  set(PLATFORM_LIBS "-framework CoreFoundation" "-framework IOKit"
                    "-framework SystemConfiguration" "-framework Security")
elseif(UNIX)
  set(PLATFORM_LIBS m c)
elseif(WIN32)
  set(PLATFORM_LIBS
      ws2_32
      userenv
      advapi32
      crypt32
      ntdll
      bcrypt
      ncrypt
      secur32)
endif()

# ---------------------------------------------------------------------------
# Rust static library (rust/): hudi-rs planning and reading behind a C ABI.
# ---------------------------------------------------------------------------
set(RUST_PLATFORM_TARGET "")

set(DUCKDB_PLATFORM "$ENV{DUCKDB_PLATFORM}")
if(DUCKDB_PLATFORM STREQUAL "linux_amd64_musl")
  set(RUST_PLATFORM_TARGET "x86_64-unknown-linux-musl")
elseif(DUCKDB_PLATFORM STREQUAL "linux_arm64_musl")
  set(RUST_PLATFORM_TARGET "aarch64-unknown-linux-musl")
elseif(DUCKDB_PLATFORM STREQUAL "windows_amd64_mingw")
  set(RUST_PLATFORM_TARGET "x86_64-pc-windows-gnu")
endif()

if(RUST_PLATFORM_TARGET STREQUAL "")
  if("${OS_NAME}" STREQUAL "linux")
    if("${OS_ARCH}" STREQUAL "arm64")
      set(RUST_PLATFORM_TARGET "aarch64-unknown-linux-gnu")
    else()
      set(RUST_PLATFORM_TARGET "x86_64-unknown-linux-gnu")
    endif()
  elseif("${OS_NAME}" STREQUAL "osx")
    if("${OSX_BUILD_ARCH}" STREQUAL "arm64")
      set(RUST_PLATFORM_TARGET "aarch64-apple-darwin")
    elseif("${OSX_BUILD_ARCH}" STREQUAL "x86_64")
      set(RUST_PLATFORM_TARGET "x86_64-apple-darwin")
    elseif("${OS_ARCH}" STREQUAL "arm64")
      set(RUST_PLATFORM_TARGET "aarch64-apple-darwin")
    else()
      set(RUST_PLATFORM_TARGET "x86_64-apple-darwin")
    endif()
  elseif("${OS_NAME}" STREQUAL "windows")
    if("${OS_ARCH}" STREQUAL "arm64")
      set(RUST_PLATFORM_TARGET "aarch64-pc-windows-msvc")
    else()
      set(RUST_PLATFORM_TARGET "x86_64-pc-windows-msvc")
    endif()
  endif()
endif()

if(RUST_PLATFORM_TARGET STREQUAL "")
  if(APPLE)
    if(CMAKE_HOST_SYSTEM_PROCESSOR MATCHES "arm64|aarch64")
      set(RUST_PLATFORM_TARGET "aarch64-apple-darwin")
    else()
      set(RUST_PLATFORM_TARGET "x86_64-apple-darwin")
    endif()
  elseif(UNIX)
    if(CMAKE_HOST_SYSTEM_PROCESSOR MATCHES "arm64|aarch64")
      set(RUST_PLATFORM_TARGET "aarch64-unknown-linux-gnu")
    else()
      set(RUST_PLATFORM_TARGET "x86_64-unknown-linux-gnu")
    endif()
  elseif(WIN32)
    set(RUST_PLATFORM_TARGET "x86_64-pc-windows-msvc")
  endif()
endif()

if(WIN32)
  if(RUST_PLATFORM_TARGET MATCHES "windows-gnu")
    set(RUST_STATICLIB_NAME "libhudi_duckdb_ffi.a")
  else()
    set(RUST_STATICLIB_NAME "hudi_duckdb_ffi.lib")
  endif()
else()
  set(RUST_STATICLIB_NAME "libhudi_duckdb_ffi.a")
endif()

set(RUST_CRATE_DIR "${CMAKE_CURRENT_LIST_DIR}/rust")
set(RUST_TARGET_DIR "${CMAKE_BINARY_DIR}/cargo")
set(RUST_DEBUG_LIB
    "${RUST_TARGET_DIR}/${RUST_PLATFORM_TARGET}/debug/${RUST_STATICLIB_NAME}")
set(RUST_RELEASE_LIB
    "${RUST_TARGET_DIR}/${RUST_PLATFORM_TARGET}/release/${RUST_STATICLIB_NAME}")

file(GLOB_RECURSE RUST_SOURCES CONFIGURE_DEPENDS "${RUST_CRATE_DIR}/src/*.rs")
set(RUST_FFI_DEPENDS ${RUST_CRATE_DIR}/Cargo.toml ${RUST_CRATE_DIR}/Cargo.lock
                     ${RUST_SOURCES})

set(RUST_CARGO_ENV CARGO_TARGET_DIR=${RUST_TARGET_DIR}
                   MACOSX_DEPLOYMENT_TARGET=${CMAKE_OSX_DEPLOYMENT_TARGET})

# The hudi crate pulls in Lance, whose build script runs prost-build and needs
# a protoc binary. Use PROTOC from the environment, else one on PATH, else a
# pinned prebuilt release vendored under third_party/protoc.
set(HUDI_PROTOC "$ENV{PROTOC}")
if(HUDI_PROTOC STREQUAL "")
  find_program(HUDI_PROTOC_ON_PATH protoc)
  if(HUDI_PROTOC_ON_PATH)
    set(HUDI_PROTOC "${HUDI_PROTOC_ON_PATH}")
  endif()
endif()
if(HUDI_PROTOC STREQUAL "")
  set(HUDI_PROTOC_ROOT "${CMAKE_CURRENT_LIST_DIR}/third_party/protoc")
  if(WIN32)
    set(HUDI_PROTOC "${HUDI_PROTOC_ROOT}/bin/protoc.exe")
  else()
    set(HUDI_PROTOC "${HUDI_PROTOC_ROOT}/bin/protoc")
  endif()
  execute_process(
    COMMAND bash "${CMAKE_CURRENT_LIST_DIR}/scripts/bootstrap_protoc.sh"
            "${HUDI_PROTOC}" "${HUDI_PROTOC_ROOT}"
    RESULT_VARIABLE HUDI_PROTOC_BOOTSTRAP_RESULT)
  if(NOT HUDI_PROTOC_BOOTSTRAP_RESULT EQUAL 0)
    message(FATAL_ERROR "could not bootstrap protoc for the hudi Rust build")
  endif()
  list(APPEND RUST_CARGO_ENV "PROTOC_INCLUDE=${HUDI_PROTOC_ROOT}/include")
endif()
list(APPEND RUST_CARGO_ENV "PROTOC=${HUDI_PROTOC}")

# Only build the active configuration's Rust artifact (both at once is slow and
# memory hungry in CI).
if(CMAKE_CONFIGURATION_TYPES)
  add_custom_command(
    OUTPUT "${RUST_DEBUG_LIB}" "${RUST_RELEASE_LIB}"
    COMMAND ${CMAKE_COMMAND} -E env ${RUST_CARGO_ENV} cargo build --locked
            --target=${RUST_PLATFORM_TARGET}
    COMMAND ${CMAKE_COMMAND} -E env ${RUST_CARGO_ENV} cargo build --locked --release
            --target=${RUST_PLATFORM_TARGET}
    WORKING_DIRECTORY ${RUST_CRATE_DIR}
    DEPENDS ${RUST_FFI_DEPENDS}
    VERBATIM)
  add_custom_target(hudi_duckdb_ffi_build DEPENDS "${RUST_DEBUG_LIB}"
                                                  "${RUST_RELEASE_LIB}")
else()
  if(NOT DEFINED CMAKE_BUILD_TYPE OR CMAKE_BUILD_TYPE STREQUAL "")
    set(CMAKE_BUILD_TYPE Release)
  endif()
  if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    add_custom_command(
      OUTPUT "${RUST_DEBUG_LIB}"
      COMMAND ${CMAKE_COMMAND} -E env ${RUST_CARGO_ENV} cargo build --locked
              --target=${RUST_PLATFORM_TARGET}
      WORKING_DIRECTORY ${RUST_CRATE_DIR}
      DEPENDS ${RUST_FFI_DEPENDS}
      VERBATIM)
    add_custom_target(hudi_duckdb_ffi_build DEPENDS "${RUST_DEBUG_LIB}")
  else()
    add_custom_command(
      OUTPUT "${RUST_RELEASE_LIB}"
      COMMAND ${CMAKE_COMMAND} -E env ${RUST_CARGO_ENV} cargo build --locked --release
              --target=${RUST_PLATFORM_TARGET}
      WORKING_DIRECTORY ${RUST_CRATE_DIR}
      DEPENDS ${RUST_FFI_DEPENDS}
      VERBATIM)
    add_custom_target(hudi_duckdb_ffi_build DEPENDS "${RUST_RELEASE_LIB}")
  endif()
endif()

target_link_libraries(${EXTENSION_NAME} debug "${RUST_DEBUG_LIB}" optimized
                      "${RUST_RELEASE_LIB}" ${PLATFORM_LIBS})
add_dependencies(${EXTENSION_NAME} hudi_duckdb_ffi_build)

target_link_libraries(${LOADABLE_EXTENSION_NAME} debug "${RUST_DEBUG_LIB}"
                      optimized "${RUST_RELEASE_LIB}" ${PLATFORM_LIBS})
add_dependencies(${LOADABLE_EXTENSION_NAME} hudi_duckdb_ffi_build)

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
