#include "hudi_scan.hpp"

#include "hudi_arrow_reader.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/table_column.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/profiler/profiling_node.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/storage/statistics/node_statistics.hpp"

namespace duckdb {

// Rows requested per kernel batch. A batch is handed out one vector at a time,
// so a larger batch amortises the per-batch cost of the parquet reader without
// changing what DuckDB sees.
static constexpr idx_t HUDI_SCAN_BATCH_ROWS = 65536;

string HudiFilterSpec::ToString() const {
	if (values.size() == 1 && op != "IN" && op != "NOT IN") {
		return column + " " + op + " " + values[0];
	}
	return column + " " + op + " (" + StringUtil::Join(values, ", ") + ")";
}

static vector<HudiFilter> ToFfiFilters(const vector<HudiFilterSpec> &filters, vector<vector<const char *>> &values) {
	vector<HudiFilter> result;
	values.clear();
	values.reserve(filters.size());
	for (auto &filter : filters) {
		values.emplace_back();
		for (auto &value : filter.values) {
			values.back().push_back(value.c_str());
		}
		HudiFilter ffi {};
		ffi.column = filter.column.c_str();
		ffi.op = filter.op.c_str();
		ffi.values = values.back().data();
		ffi.n_values = values.back().size();
		result.push_back(ffi);
	}
	return result;
}

static shared_ptr<HudiPlanHandle> CreatePlan(const HudiScanBindData &bind, const vector<HudiFilterSpec> &filters) {
	HudiReadArgs args {};
	args.as_of = bind.has_as_of ? bind.as_of.c_str() : nullptr;
	args.start = bind.has_start ? bind.start.c_str() : nullptr;
	args.end = bind.has_end ? bind.end.c_str() : nullptr;
	vector<vector<const char *>> value_pointers;
	auto ffi_filters = ToFfiFilters(filters, value_pointers);
	HudiPlan *plan = nullptr;
	if (hudi_plan_create(bind.table->ptr, &args, ffi_filters.data(), ffi_filters.size(), &plan) != 0) {
		ThrowHudiError("read_hudi");
	}
	return make_shared_ptr<HudiPlanHandle>(plan);
}

shared_ptr<HudiPlanHandle> HudiScanBindData::EnsurePlan() {
	lock_guard<mutex> guard(lock);
	if (!plan || planned_filters != filters) {
		plan = CreatePlan(*this, filters);
		planned_filters = filters;
	}
	return plan;
}

HudiPlanInfo HudiScanBindData::Totals() {
	lock_guard<mutex> guard(lock);
	if (!has_totals) {
		if (plan && planned_filters.empty()) {
			totals = plan->Info();
		} else {
			totals = CreatePlan(*this, vector<HudiFilterSpec>())->Info();
		}
		has_totals = true;
	}
	return totals;
}

//===--------------------------------------------------------------------===//
// Bind
//===--------------------------------------------------------------------===//

static bool NamedParameter(const TableFunctionBindInput &input, const string &name, string &value) {
	auto entry = input.named_parameters.find(Identifier(name));
	if (entry == input.named_parameters.end() || entry->second.IsNull()) {
		return false;
	}
	value = StringValue::Get(entry->second);
	return true;
}

static unique_ptr<FunctionData> HudiScanBind(ClientContext &context, TableFunctionBindInput &input,
                                             vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto result = make_uniq<HudiScanBindData>();
	if (input.inputs.empty() || input.inputs[0].IsNull()) {
		throw BinderException("read_hudi: the table path (uri) is required");
	}
	result->uri = StringValue::Get(input.inputs[0]);
	if (result->uri.empty()) {
		throw BinderException("read_hudi: the table path (uri) is required");
	}
	result->uri = HudiResolveUri(context, result->uri);
	result->has_as_of = NamedParameter(input, "as_of", result->as_of);
	result->has_start = NamedParameter(input, "start", result->start);
	result->has_end = NamedParameter(input, "end", result->end);
	NamedParameter(input, "hudi_option", result->hudi_options);
	if (result->has_as_of && (result->has_start || result->has_end)) {
		throw BinderException(
		    "read_hudi: `as_of` (time-travel snapshot) and `start`/`end` (incremental) are mutually exclusive");
	}

	result->table = HudiTableHandle::Open(result->uri, result->hudi_options);

	ArrowSchemaWrapper schema;
	if (hudi_table_schema(result->table->ptr, &schema.arrow_schema) != 0) {
		ThrowHudiError("read_hudi");
	}
	if (schema.arrow_schema.n_children < 1) {
		throw InvalidInputException("read_hudi: the table at %s has no columns", result->uri);
	}
	ArrowTableFunction::PopulateArrowTableSchema(context, result->arrow_table, schema.arrow_schema);
	result->names = result->arrow_table.GetNames();
	result->types = result->arrow_table.GetTypes();
	for (auto &name : result->names) {
		names.emplace_back(name);
	}
	return_types = result->types;
	return std::move(result);
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//

struct HudiScanGlobalState : public GlobalTableFunctionState {
	shared_ptr<HudiPlanHandle> plan;
	idx_t slice_count = 0;
	atomic<idx_t> next_slice {0};

	// The columns every slice stream is asked for, in stream order.
	vector<column_t> stream_column_ids;
	vector<string> stream_columns;
	vector<LogicalType> stream_types;
	// For each output column, its index among the stream columns, or
	// DConstants::INVALID_INDEX for a column the stream does not carry.
	vector<idx_t> output_to_stream;


	idx_t MaxThreads() const override {
		return MaxValue<idx_t>(slice_count, 1);
	}
};

struct HudiScanLocalState : public LocalTableFunctionState {
	HudiScanLocalState(ClientContext &context, vector<LogicalType> types) : reader(context, std::move(types)) {
	}

	HudiArrowReader reader;
	DataChunk converted;
	idx_t rows_read = 0;
};

static idx_t NarrowestColumn(const vector<LogicalType> &types) {
	idx_t best = 0;
	idx_t best_width = NumericLimits<idx_t>::Maximum();
	for (idx_t i = 0; i < types.size(); i++) {
		auto physical = types[i].InternalType();
		if (!TypeIsConstantSize(physical)) {
			continue;
		}
		auto width = GetTypeIdSize(physical);
		if (width < best_width) {
			best = i;
			best_width = width;
		}
	}
	return best;
}

static unique_ptr<GlobalTableFunctionState> HudiScanInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind = input.bind_data->CastNoConst<HudiScanBindData>();
	auto result = make_uniq<HudiScanGlobalState>();
	result->plan = bind.EnsurePlan();
	result->slice_count = result->plan->Info().slice_count;

	// The scan reads the projected table columns; a virtual column (row id, or
	// the empty column a count(*) projects) is filled in by the scan itself.
	vector<idx_t> output_columns;
	if (input.projection_ids.empty()) {
		for (idx_t i = 0; i < input.column_ids.size(); i++) {
			output_columns.push_back(i);
		}
	} else {
		output_columns = input.projection_ids;
	}
	unordered_map<column_t, idx_t> stream_index;
	for (auto column_position : output_columns) {
		auto column_id = input.column_ids[column_position];
		if (IsVirtualColumn(column_id) || column_id >= bind.names.size()) {
			result->output_to_stream.push_back(DConstants::INVALID_INDEX);
			continue;
		}
		auto entry = stream_index.find(column_id);
		if (entry == stream_index.end()) {
			entry = stream_index.emplace(column_id, result->stream_columns.size()).first;
			result->stream_column_ids.push_back(column_id);
			result->stream_columns.push_back(bind.names[column_id]);
			result->stream_types.push_back(bind.types[column_id]);
		}
		result->output_to_stream.push_back(entry->second);
	}
	if (result->stream_columns.empty()) {
		// Nothing but virtual columns: rows still have to be counted, so read
		// the cheapest column, the narrowest fixed-width one.
		auto column_id = NarrowestColumn(bind.types);
		result->stream_column_ids.push_back(column_id);
		result->stream_columns.push_back(bind.names[column_id]);
		result->stream_types.push_back(bind.types[column_id]);
	}
	return std::move(result);
}

static unique_ptr<LocalTableFunctionState> HudiScanInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                             GlobalTableFunctionState *global_state_p) {
	auto &global_state = global_state_p->Cast<HudiScanGlobalState>();
	auto result = make_uniq<HudiScanLocalState>(context.client, global_state.stream_types);
	result->converted.Initialize(context.client, global_state.stream_types);
	return std::move(result);
}

static bool OpenNextSlice(HudiScanGlobalState &global_state, HudiScanLocalState &local_state) {
	auto slice = global_state.next_slice.fetch_add(1);
	if (slice >= global_state.slice_count) {
		return false;
	}
	vector<const char *> columns;
	for (auto &column : global_state.stream_columns) {
		columns.push_back(column.c_str());
	}
	auto stream = make_uniq<ArrowArrayStreamWrapper>();
	if (hudi_plan_slice_stream(global_state.plan->ptr, slice, columns.data(), columns.size(), HUDI_SCAN_BATCH_ROWS,
	                           &stream->arrow_array_stream) != 0) {
		ThrowHudiError("read_hudi");
	}
	local_state.reader.Open(std::move(stream));
	return true;
}

static void HudiScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &global_state = data.global_state->Cast<HudiScanGlobalState>();
	auto &local_state = data.local_state->Cast<HudiScanLocalState>();

	idx_t count = 0;
	while (count == 0) {
		if (!local_state.reader.IsOpen() && !OpenNextSlice(global_state, local_state)) {
			return;
		}
		count = local_state.reader.Next(local_state.converted);
		if (count == 0) {
			local_state.reader.Close();
		}
	}
	local_state.rows_read += count;
	for (idx_t i = 0; i < output.ColumnCount(); i++) {
		auto stream_index = global_state.output_to_stream[i];
		if (stream_index == DConstants::INVALID_INDEX) {
			output.data[i].Reference(Value(output.data[i].GetType()), count_t(count));
		} else {
			output.data[i].Reference(local_state.converted.data[stream_index]);
		}
	}
	output.CheckCardinality(count);
}

//===--------------------------------------------------------------------===//
// EXPLAIN and statistics
//===--------------------------------------------------------------------===//

static string QueryDescription(const HudiScanBindData &bind) {
	if (bind.has_start || bind.has_end) {
		return "incremental (" + (bind.has_start ? bind.start : string("earliest")) + ", " +
		       (bind.has_end ? bind.end : string("latest")) + "]";
	}
	if (bind.has_as_of) {
		return "snapshot as of " + bind.as_of;
	}
	return "snapshot";
}

static InsertionOrderPreservingMap<string> HudiScanToString(TableFunctionToStringInput &input) {
	InsertionOrderPreservingMap<string> result;
	if (!input.bind_data) {
		return result;
	}
	auto &bind = input.bind_data->CastNoConst<HudiScanBindData>();
	result["Table"] = bind.uri;
	result["Query"] = QueryDescription(bind);
	vector<string> filters;
	for (auto &filter : bind.filters) {
		filters.push_back(filter.ToString());
	}
	result["Hudi Filters"] = filters.empty() ? "none" : StringUtil::Join(filters, " AND ");
	auto kept = bind.EnsurePlan()->Info();
	auto totals = bind.Totals();
	result["Partitions"] = to_string(kept.partition_count) + " / " + to_string(totals.partition_count);
	result["File Slices"] = to_string(kept.slice_count) + " / " + to_string(totals.slice_count);
	return result;
}

// Called as each thread finishes the scan, and the profiler keeps the extra
// info of whichever thread reports last, so it only carries a count that is
// final by then: every slice is claimed before any thread finishes. Rows are
// per thread, which the profiler sums.
static void HudiScanGetMetrics(TableFunctionGetMetricsInput &input) {
	if (!input.global_state) {
		return;
	}
	auto &global_state = input.global_state->Cast<HudiScanGlobalState>();
	if (input.local_state) {
		input.operator_metrics.rows_scanned = input.local_state->Cast<HudiScanLocalState>().rows_read;
	}
	auto claimed = MinValue<idx_t>(global_state.next_slice.load(), global_state.slice_count);
	input.operator_metrics.AddExtraInfo("Slices Read", to_string(claimed));
}

static unique_ptr<NodeStatistics> HudiScanCardinality(ClientContext &context, const FunctionData *bind_data_p) {
	Value setting;
	if (context.TryGetCurrentSetting("hudi_row_estimate", setting) && !BooleanValue::Get(setting)) {
		return nullptr;
	}
	auto &bind = bind_data_p->CastNoConst<HudiScanBindData>();
	auto info = bind.EnsurePlan()->Info();
	if (info.row_estimate < 0) {
		return nullptr;
	}
	auto rows = NumericCast<idx_t>(info.row_estimate);
	return make_uniq<NodeStatistics>(rows, rows);
}

// The empty column lets a count(*) project nothing, so the scan reads only
// the narrowest column to count rows instead of whatever column comes first.
static virtual_column_map_t HudiVirtualColumns(ClientContext &context, optional_ptr<FunctionData> bind_data) {
	virtual_column_map_t result;
	result.insert(make_pair(COLUMN_IDENTIFIER_EMPTY, TableColumn("", LogicalType::BOOLEAN)));
	return result;
}

TableFunction HudiReadFunction() {
	TableFunction function("read_hudi", {LogicalType::VARCHAR}, HudiScan, HudiScanBind, HudiScanInitGlobal,
	                       HudiScanInitLocal);
	// Options, unlike keyword parameters, cast their values, so an instant may
	// be passed unquoted.
	function.GetSignature().WithTypedKwargs("options", [](TypedKwargs &options) {
		options.Add("as_of", LogicalType::VARCHAR)
		    .Add("start", LogicalType::VARCHAR)
		    .Add("end", LogicalType::VARCHAR)
		    .Add("hudi_option", LogicalType::VARCHAR);
	});
	function.projection_pushdown = true;
	function.pushdown_complex_filter = HudiPushdownComplexFilter;
	function.to_string = HudiScanToString;
	function.get_metrics = HudiScanGetMetrics;
	function.cardinality = HudiScanCardinality;
	function.get_virtual_columns = HudiVirtualColumns;
	return function;
}

} // namespace duckdb
