#include "hudi_file_io.hpp"

#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/storage/external_file_cache/caching_file_system.hpp"

#include <cerrno>

namespace duckdb {

struct HudiDuckDBFileIo {
	// Weak, so a table kept past its connection fails its reads instead of
	// reaching a destroyed context.
	weak_ptr<ClientContext> context;
};

static void SetError(HudiReadSink *sink, const string &message) {
	hudi_read_sink_set_error(sink, message.c_str(), message.size());
}

// NOT_FOUND only for a missing file: an HTTP 404 (httpfs puts the status in
// the extra info) or a local ENOENT. Access errors, server errors and timeouts
// stay errors.
static int FailureStatus(const ErrorData &error) {
	auto &info = error.ExtraInfo();
	auto status = info.find("status_code");
	if (status != info.end()) {
		return status->second == "404" ? HUDI_IO_NOT_FOUND : HUDI_IO_ERROR;
	}
	auto error_number = info.find("errno");
	if (error_number != info.end() && error_number->second == std::to_string(ENOENT)) {
		return HUDI_IO_NOT_FOUND;
	}
	return HUDI_IO_ERROR;
}

static int HudiDuckDBRead(void *ctx, const char *uri_p, HudiReadSink *sink) {
	try {
		auto context = static_cast<HudiDuckDBFileIo *>(ctx)->context.lock();
		if (!context) {
			SetError(sink, "the connection that opened the table is closed");
			return HUDI_IO_ERROR;
		}
		if (context->IsInterrupted()) {
			SetError(sink, "the query was interrupted");
			return HUDI_IO_ERROR;
		}
		OpenFileInfo info(uri_p);
		// The timeline and table config change in place, so their cached blocks
		// are always validated, whatever validate_external_file_cache says.
		if (info.path.find("/.hoodie/") != string::npos) {
			info.extended_info = make_shared_ptr<ExtendedOpenFileInfo>();
			info.extended_info->options["validate_external_file_cache"] = Value::BOOLEAN(true);
		}
		auto fs = CachingFileSystem::Get(*context);
		auto flags = FileFlags::FILE_FLAGS_READ;
		flags.SetRequestSizing(RequestSizing::BY_READER);
		auto handle = fs.OpenFile(QueryContext(*context), info, flags);
		auto size = handle->GetFileSize();
		auto last_modified = handle->GetLastModifiedTime();
		auto version = handle->GetVersionTag();
		hudi_read_sink_set_version(sink, version.c_str(), version.size());
		auto last_modified_us = last_modified.IsFinite() ? Timestamp::GetEpochMicroSeconds(last_modified) : 0;
		uint64_t offset = 0;
		size_t len = 0;
		uint8_t *buf = nullptr;
		if (hudi_read_sink_set_info(sink, size, last_modified_us, &offset, &len, &buf) == 1) {
			// One read for the whole range, so DuckDB fetches its blocks
			// concurrently; hudi-rs bounds the size of a single read above this.
			handle->Read(len, offset).CopyTo(buf, len);
		}
		return HUDI_IO_OK;
	} catch (std::exception &ex) {
		ErrorData error(ex);
		SetError(sink, error.Message());
		return FailureStatus(error);
	} catch (...) {
		SetError(sink, "unknown error");
		return HUDI_IO_ERROR;
	}
}

static void HudiDuckDBRelease(void *ctx) {
	delete static_cast<HudiDuckDBFileIo *>(ctx);
}

static bool BooleanSetting(ClientContext &context, const char *name, bool default_value) {
	Value value;
	if (!context.TryGetCurrentSetting(name, value) || value.IsNull()) {
		return default_value;
	}
	return BooleanValue::Get(value);
}

// The DuckDB secret type that authenticates `uri`, or null for a scheme that
// is not routed through DuckDB. http(s):// is left out: DuckDB reads it without
// a secret, so routing it would change whose credentials a read uses without
// the user saying so, and an https:// URI can name an Azure container that the
// kernel and DuckDB map to object paths differently.
static const char *SecretTypeOf(const string &uri) {
	static const char *const SCHEMES[][2] = {{"s3://", "s3"},   {"s3a://", "s3"},   {"gs://", "gcs"},
	                                         {"gcs://", "gcs"}, {"az://", "azure"}, {"abfss://", "azure"}};
	for (auto &scheme : SCHEMES) {
		if (StringUtil::StartsWith(uri, scheme[0])) {
			return scheme[1];
		}
	}
	return nullptr;
}

// Whether `hudi_options` (`k=v; k2=v2`) sets a storage option: any key that is
// not a `hoodie.` config, as the kernel splits them.
static bool HasStorageOptions(const string &hudi_options) {
	for (auto &entry : StringUtil::Split(hudi_options, ';')) {
		auto key = entry.substr(0, entry.find('='));
		StringUtil::Trim(key);
		if (!key.empty() && !StringUtil::StartsWith(key, "hoodie.")) {
			return true;
		}
	}
	return false;
}

bool HudiUseDuckDBFileSystem(ClientContext &context, const string &uri, const string &hudi_options) {
	if (!BooleanSetting(context, "hudi_duckdb_file_system", true) || HasStorageOptions(hudi_options)) {
		return false;
	}
	string extension;
	if (!FileSystem::IsRemoteFile(uri, extension)) {
		return BooleanSetting(context, "hudi_duckdb_file_system_local", false);
	}
	auto secret_type = SecretTypeOf(uri);
	if (!secret_type || !context.db->ExtensionIsLoaded(extension)) {
		return false;
	}
	try {
		auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
		return SecretManager::Get(context).LookupSecret(transaction, uri, secret_type).HasMatch();
	} catch (...) {
		return false;
	}
}

shared_ptr<HudiTableHandle> HudiOpenWithDuckDBFileSystem(ClientContext &context, const string &uri,
                                                         const string &hudi_options) {
	HudiFileIo io {new HudiDuckDBFileIo {context.shared_from_this()}, HudiDuckDBRead, HudiDuckDBRelease};
	HudiTable *table = nullptr;
	// The table owns `io.ctx` from here on, whether or not the open succeeds.
	if (hudi_table_open_with_io(uri.c_str(), hudi_options.empty() ? nullptr : hudi_options.c_str(), &io, &table) !=
	    0) {
		ThrowHudiError("read_hudi");
	}
	return make_shared_ptr<HudiTableHandle>(table);
}

} // namespace duckdb
