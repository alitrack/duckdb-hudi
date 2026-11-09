#include "hudi_table_cache.hpp"

#include "hudi_file_io.hpp"

#include "duckdb/main/client_context.hpp"

#include <algorithm>

namespace duckdb {

static constexpr const char *HUDI_TABLE_CACHE_STATE = "hudi_table_cache";
static constexpr idx_t HUDI_CACHED_TABLES = 32;
static constexpr idx_t HUDI_CACHED_PLANS_PER_TABLE = 8;
static constexpr idx_t HUDI_CACHED_SLICES_PER_TABLE = 100000;

shared_ptr<HudiPlanHandle> HudiTableEntry::FindPlan(const string &key) {
	for (auto it = plans.begin(); it != plans.end(); ++it) {
		if (it->key == key) {
			plans.splice(plans.begin(), plans, it);
			return plans.front().plan;
		}
	}
	return nullptr;
}

shared_ptr<HudiPlanHandle> HudiTableEntry::GetPlan(const string &key,
                                                   const std::function<shared_ptr<HudiPlanHandle>()> &create) {
	{
		lock_guard<mutex> guard(lock);
		auto plan = FindPlan(key);
		if (plan) {
			plan_hits++;
			return plan;
		}
	}
	auto plan = create();
	auto slices = plan->Info().slice_count;
	lock_guard<mutex> guard(lock);
	auto kept = FindPlan(key);
	if (kept) {
		return kept;
	}
	plans.push_front(KeptPlan {key, plan, slices});
	kept_slices += slices;
	while (!plans.empty() &&
	       (plans.size() > HUDI_CACHED_PLANS_PER_TABLE || kept_slices > HUDI_CACHED_SLICES_PER_TABLE)) {
		kept_slices -= plans.back().slices;
		plans.pop_back();
	}
	return plan;
}

idx_t HudiTableEntry::PlanCount() {
	lock_guard<mutex> guard(lock);
	return plans.size();
}

idx_t HudiTableEntry::PlanHits() {
	lock_guard<mutex> guard(lock);
	return plan_hits;
}

static shared_ptr<HudiTableHandle> OpenTable(ClientContext &context, const string &uri, const string &hudi_options,
                                             bool duckdb_file_system) {
	if (duckdb_file_system) {
		return HudiOpenWithDuckDBFileSystem(context, uri, hudi_options);
	}
	return HudiTableHandle::Open(uri, hudi_options);
}

shared_ptr<HudiTableEntry> HudiTableCache::Get(ClientContext &context, const string &uri,
                                               const string &hudi_options) {
	auto duckdb_file_system = HudiUseDuckDBFileSystem(context, uri, hudi_options);
	Value enabled;
	if (context.TryGetCurrentSetting("hudi_table_cache", enabled) && !BooleanValue::Get(enabled)) {
		context.registered_state->Remove(HUDI_TABLE_CACHE_STATE);
		return make_shared_ptr<HudiTableEntry>(OpenTable(context, uri, hudi_options, duckdb_file_system));
	}
	auto cache = context.registered_state->GetOrCreate<HudiTableCache>(HUDI_TABLE_CACHE_STATE);
	return cache->GetEntry(context, uri, hudi_options, duckdb_file_system);
}

shared_ptr<HudiTableEntry> HudiTableCache::GetEntry(ClientContext &context, const string &uri,
                                                    const string &hudi_options, bool duckdb_file_system) {
	// A table read through DuckDB's file system is kept apart from one read
	// through hudi-rs's, so switching the setting reopens it.
	auto key = to_string(uri.size()) + ":" + uri + (duckdb_file_system ? "D" : "H") + hudi_options;
	shared_ptr<HudiTableEntry> cached;
	{
		lock_guard<mutex> guard(lock);
		auto slot = slots.find(key);
		if (slot != slots.end()) {
			cached = slot->second.entry;
		}
	}
	// Storage is reached outside the lock, so a bind of another table does not
	// wait on it.
	if (cached && cached->handle->IsCurrent()) {
		lock_guard<mutex> guard(lock);
		auto slot = slots.find(key);
		if (slot != slots.end() && slot->second.entry == cached) {
			slot->second.hits++;
			slot->second.last_used = ++clock;
		}
		return cached;
	}

	shared_ptr<HudiTableEntry> opened;
	try {
		opened = make_shared_ptr<HudiTableEntry>(OpenTable(context, uri, hudi_options, duckdb_file_system));
	} catch (...) {
		lock_guard<mutex> guard(lock);
		auto slot = slots.find(key);
		if (slot != slots.end() && slot->second.entry == cached) {
			slots.erase(slot);
		}
		throw;
	}

	lock_guard<mutex> guard(lock);
	auto &slot = slots[key];
	slot.uri = uri;
	slot.duckdb_file_system = duckdb_file_system;
	slot.entry = opened;
	slot.opens++;
	slot.last_used = ++clock;
	if (slots.size() > HUDI_CACHED_TABLES) {
		auto oldest = slots.begin();
		for (auto it = slots.begin(); it != slots.end(); ++it) {
			if (it->second.last_used < oldest->second.last_used) {
				oldest = it;
			}
		}
		slots.erase(oldest);
	}
	return opened;
}

vector<HudiTableCache::EntryInfo> HudiTableCache::Entries() {
	vector<shared_ptr<HudiTableEntry>> entries;
	vector<EntryInfo> result;
	{
		lock_guard<mutex> guard(lock);
		for (auto &slot : slots) {
			result.push_back(
			    EntryInfo {slot.second.uri, slot.second.duckdb_file_system, slot.second.opens, slot.second.hits, 0, 0});
			entries.push_back(slot.second.entry);
		}
	}
	for (idx_t i = 0; i < result.size(); i++) {
		result[i].plans = entries[i]->PlanCount();
		result[i].plan_hits = entries[i]->PlanHits();
	}
	std::sort(result.begin(), result.end(), [](const EntryInfo &a, const EntryInfo &b) {
		return a.uri < b.uri || (a.uri == b.uri && a.duckdb_file_system < b.duckdb_file_system);
	});
	return result;
}

//===--------------------------------------------------------------------===//
// hudi_table_cache()
//===--------------------------------------------------------------------===//

struct HudiTableCacheScanState : public GlobalTableFunctionState {
	vector<HudiTableCache::EntryInfo> entries;
	idx_t offset = 0;
};

static unique_ptr<FunctionData> HudiTableCacheBind(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<Identifier> &names) {
	names.emplace_back("uri");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("duckdb_file_system");
	return_types.emplace_back(LogicalType::BOOLEAN);
	for (auto name : {"opens", "hits", "plans", "plan_hits"}) {
		names.emplace_back(name);
		return_types.emplace_back(LogicalType::BIGINT);
	}
	return nullptr;
}

static unique_ptr<GlobalTableFunctionState> HudiTableCacheInit(ClientContext &context,
                                                               TableFunctionInitInput &input) {
	auto result = make_uniq<HudiTableCacheScanState>();
	auto cache = context.registered_state->Get<HudiTableCache>(HUDI_TABLE_CACHE_STATE);
	if (cache) {
		result->entries = cache->Entries();
	}
	return std::move(result);
}

static void HudiTableCacheScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<HudiTableCacheScanState>();
	idx_t count = 0;
	while (state.offset < state.entries.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = state.entries[state.offset++];
		output.data[0].Append(Value(entry.uri));
		output.data[1].Append(Value::BOOLEAN(entry.duckdb_file_system));
		output.data[2].Append(Value::BIGINT(NumericCast<int64_t>(entry.opens)));
		output.data[3].Append(Value::BIGINT(NumericCast<int64_t>(entry.hits)));
		output.data[4].Append(Value::BIGINT(NumericCast<int64_t>(entry.plans)));
		output.data[5].Append(Value::BIGINT(NumericCast<int64_t>(entry.plan_hits)));
		count++;
	}
}

TableFunction HudiTableCacheFunction() {
	return TableFunction("hudi_table_cache", {}, HudiTableCacheScan, HudiTableCacheBind, HudiTableCacheInit);
}

} // namespace duckdb
