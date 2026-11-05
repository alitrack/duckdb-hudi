// The write side of the extension, mirroring read_hudi's structure.
//
//   hudi_write(uri, (SELECT ...), record_key => 'id', ...)
//       A table-in-table-out function (the same mechanism as summary/unnest):
//       the subquery's rows are appended to the Hudi table as one commit.
//       When the table does not exist yet it is created first. Returns one
//       row: (commit, written_rows, status).
//
//   hudi_create(uri, record_key => 'id', ...)
//       Creates the table (or reports that it already exists); one row.
//
//   hudi_delete(uri, column, op, value)
//       Deletes the matching rows as one commit; one row.

#include "hudi_write.hpp"

#include "hudi_ffi.hpp"

#include "duckdb/common/arrow/arrow_appender.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Shared helpers
//===--------------------------------------------------------------------===//

struct HudiCreateOptions {
	string uri;
	string table_type;
	string record_key;
	string partition_fields;
	string ordering_fields;
	string hudi_options;
	bool populate_meta_fields = true; // record-key deletes need the meta fields
	bool record_index = false;
	bool metadata_table = false;

	static HudiCreateOptions FromBind(const string &function, TableFunctionBindInput &input) {
		HudiCreateOptions result;
		if (input.inputs.empty() || input.inputs[0].IsNull()) {
			throw BinderException("%s: the table path (uri) is required", function.c_str());
		}
		result.uri = StringValue::Get(input.inputs[0]);
		if (result.uri.empty()) {
			throw BinderException("%s: the table path (uri) is required", function.c_str());
		}
		auto named = [&input](const string &name) -> optional_ptr<Value> {
			auto entry = input.named_parameters.find(name);
			if (entry == input.named_parameters.end() || entry->second.IsNull()) {
				return nullptr;
			}
			return &entry->second;
		};
		if (auto v = named("table_type")) {
			result.table_type = StringValue::Get(*v);
		}
		if (auto v = named("record_key")) {
			result.record_key = StringValue::Get(*v);
		}
		if (auto v = named("partition_fields")) {
			result.partition_fields = StringValue::Get(*v);
		}
		if (auto v = named("ordering_fields")) {
			result.ordering_fields = StringValue::Get(*v);
		}
		if (auto v = named("hudi_option")) {
			result.hudi_options = StringValue::Get(*v);
		}
		if (auto v = named("populate_meta_fields")) {
			result.populate_meta_fields = BooleanValue::Get(*v);
		}
		if (auto v = named("record_index")) {
			result.record_index = BooleanValue::Get(*v);
		}
		if (auto v = named("metadata_table")) {
			result.metadata_table = BooleanValue::Get(*v);
		}
		return result;
	}

	// Fill a C-ABI HudiCreateArgs from these options.
	HudiCreateArgs ToFfiArgs() const {
		HudiCreateArgs args {};
		args.table_name = nullptr;
		args.table_type = table_type.empty() ? nullptr : table_type.c_str();
		args.record_key = record_key.empty() ? nullptr : record_key.c_str();
		args.partition_fields = partition_fields.empty() ? nullptr : partition_fields.c_str();
		args.ordering_fields = ordering_fields.empty() ? nullptr : ordering_fields.c_str();
		args.populate_meta_fields = populate_meta_fields ? 1 : 0;
		args.record_index = record_index ? 1 : 0;
		args.metadata_table = metadata_table ? 1 : 0;
		return args;
	}
};

// Open the table at uri; when it is not a Hudi table yet, create it with the
// given options. When the path exists but is not a readable Hudi table (e.g. a
// table created earlier with hudi_create and never committed: hudi-rs derives
// the table schema from commits), fail with a clear message instead of trying
// to create on top of it.
static shared_ptr<HudiTableHandle> HudiOpenOrCreate(ClientContext &context, const HudiCreateOptions &options) {
	auto uri = HudiResolveUri(context, options.uri);
	const char *hudi_options = options.hudi_options.empty() ? nullptr : options.hudi_options.c_str();

	HudiTable *table = nullptr;
	if (hudi_table_open(uri.c_str(), hudi_options, &table) == 0) {
		return make_shared_ptr<HudiTableHandle>(table);
	}
	auto open_error = HudiLastError();

	HudiCreateArgs args = options.ToFfiArgs();
	if (hudi_table_create(uri.c_str(), &args, hudi_options, &table) != 0) {
		auto create_error = HudiLastError();
		if (create_error.find("already exists") != string::npos) {
			throw IOException(
			    "cannot write Hudi table at %s: the table exists but cannot be opened, so it cannot be appended "
			    "to either (%s). If it was created with hudi_create and has no commits yet, write its first "
			    "data in the same session the table was created in.",
			    uri.c_str(), open_error.c_str());
		}
		throw IOException("cannot open or create Hudi table at %s: %s (existing table would not open: %s)",
		                  uri.c_str(), create_error.c_str(), open_error.c_str());
	}
	return make_shared_ptr<HudiTableHandle>(table);
}

// Parse "commit=<instant|none>; writes=N; updates=N; inserts=N; deletes=N"
// into the summary values. A write's count falls back to its `writes=` field,
// a delete's to `deletes=`.
static void ParseOutcome(const string &outcome, string &commit, int64_t &count, const char *delete_field) {
	commit = "none";
	count = 0;
	auto semi = outcome.find(';');
	auto head = semi == string::npos ? outcome : outcome.substr(0, semi);
	auto eq = head.find('=');
	if (head.rfind("commit=", 0) == 0 && eq != string::npos) {
		commit = head.substr(eq + 1);
	}
	auto extract = [outcome](const char *field) -> int64_t {
		auto pos = outcome.find(field);
		if (pos == string::npos) {
			return 0;
		}
		pos += strlen(field);
		try {
			return stoll(outcome.substr(pos));
		} catch (...) {
			return 0;
		}
	};
	count = delete_field ? extract(delete_field) : extract("writes=");
}

//===--------------------------------------------------------------------===//
// hudi_write(uri, (SELECT ...)) — table-in-table-out
//===--------------------------------------------------------------------===//

struct HudiWriteBindData : public TableFunctionData {
	HudiCreateOptions options;
	// The subquery's columns, in order.
	vector<string> input_names;
	vector<LogicalType> input_types;
};

struct HudiWriteGlobalState : public GlobalTableFunctionState {
	shared_ptr<HudiTableHandle> table;
};

struct HudiWriteLocalState : public LocalTableFunctionState {
	HudiWriteLocalState(ClientContext &client, const vector<LogicalType> &types, const vector<string> &names)
	    : types_p(types), names_p(names) {
		auto properties = client.GetClientProperties();
		arrow_types.clear();
		appender = make_uniq<ArrowAppender>(types, STANDARD_VECTOR_SIZE, properties, arrow_types);
	}

	vector<LogicalType> types_p;
	vector<string> names_p;
	// Empty extension-type map for the appender (no Arrow extension types in
	// a DuckDB->Hudi write).
	unordered_map<idx_t, const shared_ptr<ArrowTypeExtensionData>> arrow_types;
	unique_ptr<ArrowAppender> appender;
	idx_t rows = 0;
};

static unique_ptr<FunctionData> HudiWriteBind(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<HudiWriteBindData>();
	result->options = HudiCreateOptions::FromBind("hudi_write", input);

	// The subquery argument is exposed through the input table types/names
	// (the same way summary() sees its columns).
	if (input.input_table_types.empty()) {
		throw BinderException(
		    "hudi_write: a subquery argument is required, e.g. hudi_write('path', (SELECT id, name FROM ...))");
	}
	if (input.input_table_names.size() != input.input_table_types.size()) {
		throw BinderException("hudi_write: the subquery's columns could not be resolved");
	}
	result->input_types = input.input_table_types;
	result->input_names = input.input_table_names;

	// One summary row: (commit, written_rows, status).
	return_types = {LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::VARCHAR};
	names = {"commit", "written_rows", "status"};
	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> HudiWriteInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	// The table is opened/created lazily in the final callback, only when there
	// are actually rows to write — an empty subquery must not create the table.
	return make_uniq<HudiWriteGlobalState>();
}

static unique_ptr<LocalTableFunctionState> HudiWriteInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                              GlobalTableFunctionState *global_state_p) {
	auto &bind = input.bind_data->Cast<HudiWriteBindData>();
	auto result = make_uniq<HudiWriteLocalState>(context.client, bind.input_types, bind.input_names);
	return std::move(result);
}

// Per-chunk: accumulate the subquery's rows into the appender.
static OperatorResultType HudiWriteInOut(ExecutionContext &context, TableFunctionInput &data, DataChunk &input,
                                         DataChunk &output) {
	auto &local_state = data.local_state->Cast<HudiWriteLocalState>();
	if (input.size() > 0) {
		local_state.appender->Append(input, 0, input.size(), input.size());
		local_state.rows += input.size();
	}
	// Consume the whole subquery; the commit happens in the final callback.
	return OperatorResultType::NEED_MORE_INPUT;
}

// Final: commit the accumulated rows as one Hudi commit and emit the row.
static OperatorFinalizeResultType HudiWriteInOutFinal(ExecutionContext &context, TableFunctionInput &data,
                                                      DataChunk &output) {
	auto &global_state = data.global_state->Cast<HudiWriteGlobalState>();
	auto &local_state = data.local_state->Cast<HudiWriteLocalState>();

	string commit;
	int64_t written = 0;
	string status;

	if (local_state.rows == 0) {
		commit = "none";
		status = "nothing to write";
	} else {
		// Open or create the table now — only when there is data to write, so
		// an empty subquery leaves no trace on the filesystem.
		auto &bind = data.bind_data->Cast<HudiWriteBindData>();
		if (!global_state.table) {
			global_state.table = HudiOpenOrCreate(context.client, bind.options);
		}

		// Finalize the appender: it yields an ArrowArray whose release
		// callback owns the exported buffers.
		ArrowArray array = local_state.appender->Finalize();
		local_state.appender.reset();

		// Build the matching ArrowSchema (self-owning: its release callback
		// frees the exported names); keep it alive until after the call.
		ArrowSchema schema;
		schema.Init();
		auto properties = context.client.GetClientProperties();
		ArrowConverter::ToArrowSchema(&schema, local_state.types_p, local_state.names_p, properties);

		// Hand both to Rust: the array's ownership moves into
		// hudi_table_append_array, which releases it exactly once, success or
		// failure; the schema is only read.
		char *outcome_text = nullptr;
		if (hudi_table_append_array(global_state.table->ptr, &array, &schema, &outcome_text) != 0) {
			if (schema.release) {
				schema.release(&schema);
			}
			throw IOException("hudi_write: Hudi append failed: %s", HudiLastError().c_str());
		}
		if (schema.release) {
			schema.release(&schema);
		}
		string outcome(outcome_text);
		hudi_string_free(outcome_text);
		status = outcome;
		ParseOutcome(outcome, commit, written, nullptr);
	}

	output.SetCardinality(1);
	output.SetValue(0, 0, Value(commit));
	output.SetValue(1, 0, Value::BIGINT(written));
	output.SetValue(2, 0, Value(status));
	return OperatorFinalizeResultType::FINISHED;
}

TableFunction HudiWriteFunction() {
	TableFunction function("hudi_write", {LogicalType::VARCHAR, LogicalType::TABLE}, nullptr, HudiWriteBind);
	function.init_global = HudiWriteInitGlobal;
	function.init_local = HudiWriteInitLocal;
	function.in_out_function = HudiWriteInOut;
	function.in_out_function_final = HudiWriteInOutFinal;
	function.named_parameters["table_type"] = LogicalType::VARCHAR;
	function.named_parameters["record_key"] = LogicalType::VARCHAR;
	function.named_parameters["partition_fields"] = LogicalType::VARCHAR;
	function.named_parameters["ordering_fields"] = LogicalType::VARCHAR;
	function.named_parameters["hudi_option"] = LogicalType::VARCHAR;
	function.named_parameters["populate_meta_fields"] = LogicalType::BOOLEAN;
	function.named_parameters["record_index"] = LogicalType::BOOLEAN;
	function.named_parameters["metadata_table"] = LogicalType::BOOLEAN;
	return function;
}

//===--------------------------------------------------------------------===//
// hudi_create(uri, ...) — one row: (status)
//===--------------------------------------------------------------------===//

struct HudiCreateBindData : public TableFunctionData {
	HudiCreateOptions options;
};

struct HudiCreateLocalState : public LocalTableFunctionState {
	bool done = false;
};

static unique_ptr<FunctionData> HudiCreateBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<HudiCreateBindData>();
	result->options = HudiCreateOptions::FromBind("hudi_create", input);
	return_types = {LogicalType::VARCHAR};
	names = {"status"};
	return std::move(result);
}

static void HudiCreateScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &local_state = data.local_state->Cast<HudiCreateLocalState>();
	if (local_state.done) {
		output.SetCardinality(0);
		return;
	}
	local_state.done = true;
	auto &bind = data.bind_data->CastNoConst<HudiCreateBindData>();
	auto uri = HudiResolveUri(context, bind.options.uri);
	const char *hudi_options = bind.options.hudi_options.empty() ? nullptr : bind.options.hudi_options.c_str();

	HudiTable *table = nullptr;
	string status;
	if (hudi_table_open(uri.c_str(), hudi_options, &table) == 0) {
		status = "exists";
	} else {
		HudiCreateArgs args = bind.options.ToFfiArgs();
		if (hudi_table_create(uri.c_str(), &args, hudi_options, &table) != 0) {
			throw IOException("hudi_create: %s", HudiLastError().c_str());
		}
		status = "created";
	}
	HudiTableHandle handle(table);
	output.SetCardinality(1);
	output.SetValue(0, 0, Value(status));
}

static unique_ptr<LocalTableFunctionState> HudiCreateInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                               GlobalTableFunctionState *global_state_p) {
	return make_uniq<HudiCreateLocalState>();
}

TableFunction HudiCreateFunction() {
	TableFunction function("hudi_create", {LogicalType::VARCHAR}, HudiCreateScan, HudiCreateBind, nullptr,
	                       HudiCreateInitLocal);
	function.named_parameters["table_type"] = LogicalType::VARCHAR;
	function.named_parameters["record_key"] = LogicalType::VARCHAR;
	function.named_parameters["partition_fields"] = LogicalType::VARCHAR;
	function.named_parameters["ordering_fields"] = LogicalType::VARCHAR;
	function.named_parameters["hudi_option"] = LogicalType::VARCHAR;
	function.named_parameters["populate_meta_fields"] = LogicalType::BOOLEAN;
	function.named_parameters["record_index"] = LogicalType::BOOLEAN;
	function.named_parameters["metadata_table"] = LogicalType::BOOLEAN;
	return function;
}

//===--------------------------------------------------------------------===//
// hudi_delete(uri, column, op, value) — one row: (commit, deleted_rows, status)
//===--------------------------------------------------------------------===//

struct HudiDeleteBindData : public TableFunctionData {
	string uri;
	string hudi_options;
	string column;
	string op;
	string value;
};

struct HudiDeleteLocalState : public LocalTableFunctionState {
	bool done = false;
};

static unique_ptr<FunctionData> HudiDeleteBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<HudiDeleteBindData>();
	if (input.inputs.size() != 4) {
		throw BinderException("hudi_delete(uri, column, op, value) requires exactly four arguments");
	}
	for (idx_t i = 0; i < 4; i++) {
		if (input.inputs[i].IsNull()) {
			throw BinderException("hudi_delete: argument %d must not be NULL", i + 1);
		}
	}
	result->uri = StringValue::Get(input.inputs[0]);
	result->column = StringValue::Get(input.inputs[1]);
	result->op = StringUtil::Upper(StringValue::Get(input.inputs[2]));
	result->value = StringValue::Get(input.inputs[3]);
	auto named = input.named_parameters.find("hudi_option");
	if (named != input.named_parameters.end() && !named->second.IsNull()) {
		result->hudi_options = StringValue::Get(named->second);
	}
	if (result->op != "=" && result->op != "!=" && result->op != "<" && result->op != "<=" && result->op != ">" &&
	    result->op != ">=") {
		throw BinderException("hudi_delete: unsupported operator %s (use one of =, !=, <, <=, >, >=)",
		                      result->op.c_str());
	}
	return_types = {LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::VARCHAR};
	names = {"commit", "deleted_rows", "status"};
	return std::move(result);
}

static void HudiDeleteScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &local_state = data.local_state->Cast<HudiDeleteLocalState>();
	if (local_state.done) {
		output.SetCardinality(0);
		return;
	}
	local_state.done = true;
	auto &bind = data.bind_data->CastNoConst<HudiDeleteBindData>();
	auto uri = HudiResolveUri(context, bind.uri);

	HudiTable *table = nullptr;
	const char *hudi_options = bind.hudi_options.empty() ? nullptr : bind.hudi_options.c_str();
	if (hudi_table_open(uri.c_str(), hudi_options, &table) != 0) {
		throw IOException("hudi_delete: failed to open Hudi table at %s: %s", uri.c_str(), HudiLastError().c_str());
	}
	HudiTableHandle handle(table);

	const char *value_ptr = bind.value.c_str();
	char *outcome_text = nullptr;
	if (hudi_table_delete(handle.ptr, bind.column.c_str(), bind.op.c_str(), &value_ptr, 1, &outcome_text) != 0) {
		throw IOException("hudi_delete: %s", HudiLastError().c_str());
	}
	string outcome(outcome_text);
	hudi_string_free(outcome_text);

	string commit;
	int64_t deleted = 0;
	ParseOutcome(outcome, commit, deleted, "deletes=");

	output.SetCardinality(1);
	output.SetValue(0, 0, Value(commit));
	output.SetValue(1, 0, Value::BIGINT(deleted));
	output.SetValue(2, 0, Value(outcome));
}

static unique_ptr<LocalTableFunctionState> HudiDeleteInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                               GlobalTableFunctionState *global_state_p) {
	return make_uniq<HudiDeleteLocalState>();
}

TableFunction HudiDeleteFunction() {
	TableFunction function("hudi_delete", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                                       LogicalType::VARCHAR},
	                       HudiDeleteScan, HudiDeleteBind, nullptr, HudiDeleteInitLocal);
	function.named_parameters["hudi_option"] = LogicalType::VARCHAR;
	return function;
}

} // namespace duckdb
