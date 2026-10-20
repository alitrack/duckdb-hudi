#include "hudi_arrow_reader.hpp"
#include "hudi_ffi.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

string HudiLastError() {
	auto message = hudi_last_error();
	return message ? string(message) : string("unknown error");
}

void ThrowHudiError(const string &what) {
	auto message = HudiLastError();
	if (message.find("failed to open") != string::npos || message.find("read failed") != string::npos) {
		throw IOException("%s: %s", what, message);
	}
	throw InvalidInputException("%s: %s", what, message);
}

string HudiResolveUri(ClientContext &context, const string &uri) {
	if (uri.find("://") != string::npos) {
		return uri;
	}
	auto &fs = FileSystem::GetFileSystem(context);
	auto path = fs.ExpandPath(uri);
	if (!fs.IsPathAbsolute(path)) {
		path = fs.JoinPath(fs.GetWorkingDirectory(), path);
	}
	// The kernel rejects a `..` segment, so resolve them lexically.
	auto separator = fs.PathSeparator(path);
	vector<string> resolved;
	for (auto &segment : StringUtil::Split(path, separator)) {
		if (segment.empty() || segment == ".") {
			continue;
		}
		if (segment == "..") {
			if (!resolved.empty()) {
				resolved.pop_back();
			}
			continue;
		}
		resolved.push_back(segment);
	}
	string prefix = StringUtil::StartsWith(path, separator) ? separator : string();
	return prefix + StringUtil::Join(resolved, separator);
}

shared_ptr<HudiTableHandle> HudiTableHandle::Open(const string &uri, const string &hudi_options) {
	HudiTable *table = nullptr;
	if (hudi_table_open(uri.c_str(), hudi_options.empty() ? nullptr : hudi_options.c_str(), &table) != 0) {
		ThrowHudiError("read_hudi");
	}
	return make_shared_ptr<HudiTableHandle>(table);
}

HudiPlanInfo HudiPlanHandle::Info() const {
	HudiPlanInfo info {};
	if (hudi_plan_info(ptr, &info) != 0) {
		ThrowHudiError("read_hudi");
	}
	return info;
}

HudiArrowReader::HudiArrowReader(ClientContext &context, vector<LogicalType> types_p)
    : types(std::move(types_p)), state(make_uniq<ArrowArrayWrapper>(), context) {
	for (idx_t i = 0; i < types.size(); i++) {
		state.column_ids.push_back(i);
	}
}

void HudiArrowReader::Open(unique_ptr<ArrowArrayStreamWrapper> stream_p) {
	Close();
	// A cached dictionary belongs to the stream that produced it.
	state.array_states.clear();
	ArrowSchemaWrapper schema;
	stream_p->GetSchema(schema);
	stream_schema = ArrowTableSchema();
	ArrowTableFunction::PopulateArrowTableSchema(state.context, stream_schema, schema.arrow_schema);
	if (stream_schema.GetTypes() != types) {
		throw InternalException("hudi: a stream's schema does not match the declared columns");
	}
	stream = std::move(stream_p);
}

void HudiArrowReader::Close() {
	stream.reset();
	state.Reset();
	state.chunk = make_shared_ptr<ArrowArrayWrapper>();
}

idx_t HudiArrowReader::Next(DataChunk &converted) {
	if (!stream) {
		return 0;
	}
	while (state.chunk_offset >= NumericCast<idx_t>(state.chunk->arrow_array.length)) {
		state.Reset();
		state.chunk = stream->GetNextChunk();
		if (!state.chunk->arrow_array.release) {
			return 0;
		}
		batches_read++;
	}
	auto remaining = NumericCast<idx_t>(state.chunk->arrow_array.length) - state.chunk_offset;
	auto count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, remaining);
	converted.Reset();
	converted.SetCardinality(count);
	ArrowTableFunction::ArrowToDuckDB(state, stream_schema.GetColumns(), converted);
	state.chunk_offset += count;
	return count;
}

} // namespace duckdb
