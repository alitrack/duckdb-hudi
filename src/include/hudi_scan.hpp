#pragma once

#include "duckdb.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "hudi_ffi.hpp"

namespace duckdb {

// One predicate pushed to the kernel, see HudiFilter in hudi_ffi.h.
struct HudiFilterSpec {
	string column;
	string op;
	vector<string> values;

	string ToString() const;
	bool operator==(const HudiFilterSpec &other) const {
		return column == other.column && op == other.op && values == other.values;
	}
};

struct HudiScanBindData : public TableFunctionData {
	string uri;
	// The raw `hudi_option` string, empty when absent.
	string hudi_options;
	// The `as_of` / `start` / `end` parameters and whether each was given.
	bool has_as_of = false;
	bool has_start = false;
	bool has_end = false;
	string as_of;
	string start;
	string end;

	shared_ptr<HudiTableHandle> table;
	ArrowTableSchema arrow_table;
	vector<string> names;
	vector<LogicalType> types;

	// The predicates pushdown_complex_filter translated; every one of them also
	// stays in DuckDB's plan.
	vector<HudiFilterSpec> filters;

	// Returns the plan for the current filters, planning on first use and
	// again when the filters changed since the last plan.
	shared_ptr<HudiPlanHandle> EnsurePlan();
	// The slice and partition counts of the unfiltered read, for EXPLAIN.
	HudiPlanInfo Totals();

private:
	mutex lock;
	shared_ptr<HudiPlanHandle> plan;
	vector<HudiFilterSpec> planned_filters;
	bool has_totals = false;
	HudiPlanInfo totals {};
};

// Translates the filters DuckDB pushes to a scan into HudiFilterSpecs on the
// bind data (hudi_filter.cpp).
void HudiPushdownComplexFilter(ClientContext &context, LogicalGet &get, FunctionData *bind_data,
                               vector<unique_ptr<Expression>> &filters);

TableFunction HudiReadFunction();

} // namespace duckdb
