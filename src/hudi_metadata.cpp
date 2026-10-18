#include "hudi_metadata.hpp"

#include "hudi_arrow_reader.hpp"
#include "hudi_ffi.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/function/table/arrow.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// hudi_info(uri) / hudi_files(uri)
//===--------------------------------------------------------------------===//

// A failure is returned as the text "Error: ..." rather than thrown, so a
// listing over many tables reports each one.
static string Describe(ClientContext &context, const string &uri_p, int kind) {
	auto uri = HudiResolveUri(context, uri_p);
	HudiTable *table = nullptr;
	if (hudi_table_open(uri.c_str(), nullptr, &table) != 0) {
		return "Error: " + HudiLastError();
	}
	HudiTableHandle handle(table);
	char *text = nullptr;
	if (hudi_table_describe(handle.ptr, kind, &text) != 0) {
		return "Error: " + HudiLastError();
	}
	string result(text);
	hudi_string_free(text);
	return result;
}

template <int KIND>
static void HudiDescribeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	UnaryExecutor::Execute<string_t, string_t>(args.data[0], result, args.size(), [&](string_t uri) {
		return StringVector::AddString(result, Describe(context, uri.GetString(), KIND));
	});
}

vector<ScalarFunction> HudiScalarFunctions() {
	vector<ScalarFunction> result;
	result.emplace_back("hudi_info", vector<LogicalType> {LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                    HudiDescribeFunction<0>);
	result.emplace_back("hudi_files", vector<LogicalType> {LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                    HudiDescribeFunction<1>);
	return result;
}

//===--------------------------------------------------------------------===//
// hudi_mdt(uri) / hudi_commits(uri) / hudi_stats(uri)
//===--------------------------------------------------------------------===//

struct HudiMetadataBindData : public TableFunctionData {
	string function_name;
	string uri;
	int kind = 0;
	shared_ptr<HudiTableHandle> table;
	ArrowTableSchema arrow_table;
	vector<LogicalType> types;

	// The stream bind opened to learn the schema, handed to the first scan so
	// the rows are not produced twice.
	mutex lock;
	unique_ptr<ArrowArrayStreamWrapper> pending_stream;

	unique_ptr<ArrowArrayStreamWrapper> OpenStream() {
		auto stream = make_uniq<ArrowArrayStreamWrapper>();
		if (hudi_table_metadata_stream(table->ptr, kind, &stream->arrow_array_stream) != 0) {
			ThrowHudiError(function_name);
		}
		return stream;
	}

	unique_ptr<ArrowArrayStreamWrapper> TakeStream() {
		lock_guard<mutex> guard(lock);
		if (pending_stream) {
			return std::move(pending_stream);
		}
		return OpenStream();
	}
};

struct HudiMetadataGlobalState : public GlobalTableFunctionState {
	unique_ptr<ArrowArrayStreamWrapper> stream;
	idx_t MaxThreads() const override {
		return 1;
	}
};

struct HudiMetadataLocalState : public LocalTableFunctionState {
	HudiMetadataLocalState(ClientContext &context, vector<column_t> column_ids) : reader(context, std::move(column_ids)) {
	}
	HudiArrowReader reader;
	DataChunk converted;
};

template <int KIND>
static unique_ptr<FunctionData> HudiMetadataBind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<HudiMetadataBindData>();
	result->function_name = input.table_function.name;
	result->kind = KIND;
	if (input.inputs.empty() || input.inputs[0].IsNull()) {
		throw BinderException("%s: the table path (uri) is required", result->function_name);
	}
	result->uri = StringValue::Get(input.inputs[0]);
	if (result->uri.empty()) {
		throw BinderException("%s: the table path (uri) is required", result->function_name);
	}
	result->uri = HudiResolveUri(context, result->uri);
	HudiTable *table = nullptr;
	if (hudi_table_open(result->uri.c_str(), nullptr, &table) != 0) {
		ThrowHudiError(result->function_name);
	}
	result->table = make_shared_ptr<HudiTableHandle>(table);

	auto stream = result->OpenStream();
	ArrowSchemaWrapper schema;
	stream->GetSchema(schema);
	ArrowTableFunction::PopulateArrowTableSchema(context, result->arrow_table, schema.arrow_schema);
	names = result->arrow_table.GetNames();
	return_types = result->arrow_table.GetTypes();
	result->types = return_types;
	result->pending_stream = std::move(stream);
	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> HudiMetadataInitGlobal(ClientContext &context,
                                                                   TableFunctionInitInput &input) {
	auto &bind = input.bind_data->CastNoConst<HudiMetadataBindData>();
	auto result = make_uniq<HudiMetadataGlobalState>();
	result->stream = bind.TakeStream();
	return std::move(result);
}

static unique_ptr<LocalTableFunctionState> HudiMetadataInitLocal(ExecutionContext &context,
                                                                 TableFunctionInitInput &input,
                                                                 GlobalTableFunctionState *global_state_p) {
	auto &bind = input.bind_data->Cast<HudiMetadataBindData>();
	auto &global_state = global_state_p->Cast<HudiMetadataGlobalState>();
	vector<column_t> all_columns;
	for (idx_t i = 0; i < bind.types.size(); i++) {
		all_columns.push_back(i);
	}
	auto result = make_uniq<HudiMetadataLocalState>(context.client, std::move(all_columns));
	result->converted.Initialize(context.client, bind.types);
	if (global_state.stream) {
		result->reader.Open(std::move(global_state.stream));
	}
	return std::move(result);
}

static void HudiMetadataScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind = data.bind_data->Cast<HudiMetadataBindData>();
	auto &local_state = data.local_state->Cast<HudiMetadataLocalState>();
	auto count = local_state.reader.Next(bind.arrow_table.GetColumns(), local_state.converted);
	output.SetCardinality(count);
	if (count == 0) {
		return;
	}
	for (idx_t i = 0; i < output.ColumnCount(); i++) {
		output.data[i].Reference(local_state.converted.data[i]);
	}
}

template <int KIND>
static TableFunction MetadataFunction(const string &name) {
	TableFunction function(name, {LogicalType::VARCHAR}, HudiMetadataScan, HudiMetadataBind<KIND>,
	                       HudiMetadataInitGlobal, HudiMetadataInitLocal);
	return function;
}

vector<TableFunction> HudiMetadataFunctions() {
	vector<TableFunction> result;
	result.push_back(MetadataFunction<0>("hudi_mdt"));
	result.push_back(MetadataFunction<1>("hudi_commits"));
	result.push_back(MetadataFunction<2>("hudi_stats"));
	return result;
}

} // namespace duckdb
