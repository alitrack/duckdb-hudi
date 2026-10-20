// RAII wrappers around the C ABI of the Rust library (hudi_ffi.h).
#pragma once

#include "duckdb.hpp"
#include "hudi_ffi.h"

namespace duckdb {

// The message of the last failed call into the Rust library on this thread.
string HudiLastError();

// Throws the exception DuckDB reports for a failed call, prefixed with `what`.
[[noreturn]] void ThrowHudiError(const string &what);

// The table location the kernel is given: a URL as is, a local path made
// absolute against the working directory with `.` and `..` segments resolved.
string HudiResolveUri(ClientContext &context, const string &uri);

struct HudiTableHandle {
	explicit HudiTableHandle(HudiTable *ptr_p) : ptr(ptr_p) {
	}
	~HudiTableHandle() {
		hudi_table_free(ptr);
	}
	HudiTableHandle(const HudiTableHandle &) = delete;
	HudiTableHandle &operator=(const HudiTableHandle &) = delete;

	// Opens the table at `uri`; `hudi_options` is the raw `hudi_option` string
	// or empty.
	static shared_ptr<HudiTableHandle> Open(const string &uri, const string &hudi_options);

	HudiTable *ptr;
};

struct HudiPlanHandle {
	explicit HudiPlanHandle(HudiPlan *ptr_p) : ptr(ptr_p) {
	}
	~HudiPlanHandle() {
		hudi_plan_free(ptr);
	}
	HudiPlanHandle(const HudiPlanHandle &) = delete;
	HudiPlanHandle &operator=(const HudiPlanHandle &) = delete;

	HudiPlanInfo Info() const;

	HudiPlan *ptr;
};

} // namespace duckdb
