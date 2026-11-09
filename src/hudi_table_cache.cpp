#include "hudi_table_cache.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"

#include <algorithm>

namespace duckdb {

static constexpr const char *HUDI_TABLE_CACHE_STATE = "hudi_table_cache";
static constexpr idx_t HUDI_CACHED_TABLES = 32;
static constexpr idx_t HUDI_CACHED_PLANS_PER_TABLE = 8;
static constexpr idx_t HUDI_CACHED_SLICES_PER_TABLE = 100000;

HudiTableEntry::KeptPlan *HudiTableEntry::FindPlan(const string &key) {
	for (auto it = plans.begin(); it != plans.end(); ++it) {
		if (it->key == key) {
			plans.splice(plans.begin(), plans, it);
			return &plans.front();
		}
	}
	return nullptr;
}

void HudiTableEntry::EvictPlans() {
	while (!plans.empty() &&
	       (plans.size() > HUDI_CACHED_PLANS_PER_TABLE || kept_slices > HUDI_CACHED_SLICES_PER_TABLE)) {
		kept_slices -= plans.back().slices;
		plans.pop_back();
	}
}

shared_ptr<HudiPlanHandle> HudiTableEntry::Keep(const string &key, shared_ptr<HudiPlanHandle> plan) {
	auto slices = plan->Info().slice_count;
	lock_guard<mutex> guard(lock);
	auto kept = FindPlan(key);
	if (kept && kept->plan) {
		return kept->plan;
	}
	if (kept) {
		kept->plan = plan;
		kept->pending = HudiPlanFuture();
		kept->slices = slices;
	} else {
		plans.push_front(KeptPlan {key, plan, HudiPlanFuture(), slices});
	}
	kept_slices += slices;
	EvictPlans();
	return plan;
}

void HudiTableEntry::DropPending(const string &key) {
	lock_guard<mutex> guard(lock);
	plans.remove_if([&](const KeptPlan &kept) { return kept.key == key && !kept.plan; });
}

shared_ptr<HudiPlanHandle> HudiTableEntry::GetPlan(const string &key,
                                                   const std::function<shared_ptr<HudiPlanHandle>()> &create) {
	HudiPlanFuture pending;
	{
		lock_guard<mutex> guard(lock);
		auto kept = FindPlan(key);
		if (kept && kept->plan) {
			plan_hits++;
			return kept->plan;
		}
		if (kept) {
			pending = kept->pending;
		}
	}
	if (pending.valid()) {
		shared_ptr<HudiPlanHandle> plan;
		try {
			plan = pending.get();
		} catch (...) {
			// A background failure may have been transient or another
			// statement's, so this statement plans for itself and reports its
			// own error, if any.
			DropPending(key);
		}
		if (plan) {
			{
				lock_guard<mutex> guard(lock);
				plan_hits++;
			}
			return Keep(key, std::move(plan));
		}
	}
	return Keep(key, create());
}

HudiPlanFuture HudiTableEntry::StartPlan(const string &key, const std::function<HudiPlanFuture()> &start) {
	lock_guard<mutex> guard(lock);
	if (FindPlan(key)) {
		return HudiPlanFuture();
	}
	auto pending = start();
	if (!pending.valid()) {
		return pending;
	}
	plans.push_front(KeptPlan {key, nullptr, pending, 0});
	EvictPlans();
	return pending;
}

void HudiTableEntry::FinishPlan(const string &key, const HudiPlanFuture &pending) {
	shared_ptr<HudiPlanHandle> plan;
	try {
		plan = pending.get();
	} catch (...) {
		DropPending(key);
		return;
	}
	auto slices = plan->Info().slice_count;
	lock_guard<mutex> guard(lock);
	// Taken plans are kept already, and an evicted one stays evicted.
	for (auto &kept : plans) {
		if (kept.key == key && !kept.plan) {
			kept.plan = std::move(plan);
			kept.pending = HudiPlanFuture();
			kept.slices = slices;
			kept_slices += slices;
			EvictPlans();
			return;
		}
	}
}

idx_t HudiTableEntry::PlanCount() {
	lock_guard<mutex> guard(lock);
	return plans.size();
}

idx_t HudiTableEntry::RunningPlanCount() {
	lock_guard<mutex> guard(lock);
	idx_t count = 0;
	for (auto &kept : plans) {
		if (!kept.plan) {
			count++;
		}
	}
	return count;
}

idx_t HudiTableEntry::PlanHits() {
	lock_guard<mutex> guard(lock);
	return plan_hits;
}

// The running statement's query number, or MAXIMUM_QUERY_ID outside a statement
// (a relation bound ahead of its execution).
static idx_t ActiveQuery(ClientContext &context) {
	return context.transaction.HasActiveTransaction() ? context.transaction.GetActiveQuery() : MAXIMUM_QUERY_ID;
}

shared_ptr<HudiTableEntry> HudiTableCache::Get(ClientContext &context, const string &uri,
                                               const string &hudi_options) {
	Value enabled;
	auto keep = !context.TryGetCurrentSetting("hudi_table_cache", enabled) || BooleanValue::Get(enabled);
	// Outside a statement nothing is pinned.
	auto query = ActiveQuery(context);
	auto cache = context.registered_state->GetOrCreate<HudiTableCache>(HUDI_TABLE_CACHE_STATE);
	return cache->GetEntry(uri, hudi_options, keep, query);
}

void HudiTableCache::StartPlan(ClientContext &context, const shared_ptr<HudiTableEntry> &entry, const string &key,
                               const std::function<HudiPlanFuture()> &start) {
	// Outside a statement no end would wait for the plan.
	if (ActiveQuery(context) == MAXIMUM_QUERY_ID) {
		return;
	}
	auto pending = entry->StartPlan(key, start);
	if (!pending.valid()) {
		return;
	}
	auto cache = context.registered_state->GetOrCreate<HudiTableCache>(HUDI_TABLE_CACHE_STATE);
	lock_guard<mutex> guard(cache->lock);
	cache->started.push_back(StartedPlan {entry, key, std::move(pending)});
}

void HudiTableCache::QueryEnd(ClientContext &context) {
	vector<StartedPlan> to_finish;
	{
		lock_guard<mutex> guard(lock);
		pinned.clear();
		pinned_query = MAXIMUM_QUERY_ID;
		to_finish = std::move(started);
		started.clear();
	}
	// A plan nobody took (PREPARE, a statement that failed before its scan
	// started, a pushdown pass whose filters a later pass replaced) still reads
	// storage, so it finishes here, in its statement.
	for (auto &plan : to_finish) {
		try {
			plan.entry->FinishPlan(plan.key, plan.pending);
		} catch (...) {
		}
	}
}

// Spellings of one location that hudi-rs opens as the same table share a key:
// with or without a trailing `/`, and `file://` before an absolute local path.
static string KeyUri(const string &uri) {
	auto result = StringUtil::StartsWith(uri, "file:///") ? uri.substr(7) : uri;
	while (result.size() > 1 && result.back() == '/') {
		result.pop_back();
	}
	return result;
}

shared_ptr<HudiTableEntry> HudiTableCache::GetEntry(const string &uri, const string &hudi_options, bool keep,
                                                    idx_t query) {
	auto key_uri = KeyUri(uri);
	auto key = to_string(key_uri.size()) + ":" + key_uri + hudi_options;
	{
		lock_guard<mutex> guard(lock);
		if (!keep) {
			slots.clear();
		}
		// The query number changes per statement, also between the statements
		// one statement expands into.
		if (query != pinned_query) {
			pinned.clear();
			pinned_query = query;
		}
		auto pin = pinned.find(key);
		if (pin != pinned.end()) {
			return pin->second;
		}
	}
	auto entry = keep ? GetKept(key, uri, hudi_options)
	                  : make_shared_ptr<HudiTableEntry>(HudiTableHandle::Open(uri, hudi_options));
	if (query != MAXIMUM_QUERY_ID) {
		lock_guard<mutex> guard(lock);
		if (query == pinned_query) {
			pinned.emplace(key, entry);
		}
	}
	return entry;
}

shared_ptr<HudiTableEntry> HudiTableCache::GetKept(const string &key, const string &uri, const string &hudi_options) {
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
		opened = make_shared_ptr<HudiTableEntry>(HudiTableHandle::Open(uri, hudi_options));
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
			result.push_back(EntryInfo {slot.second.uri, slot.second.opens, slot.second.hits, 0, 0, 0});
			entries.push_back(slot.second.entry);
		}
	}
	for (idx_t i = 0; i < result.size(); i++) {
		result[i].plans = entries[i]->PlanCount();
		result[i].plan_hits = entries[i]->PlanHits();
		result[i].running_plans = entries[i]->RunningPlanCount();
	}
	std::sort(result.begin(), result.end(),
	          [](const EntryInfo &a, const EntryInfo &b) { return a.uri < b.uri; });
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
	for (auto name : {"opens", "hits", "plans", "plan_hits", "running_plans"}) {
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
		output.data[1].Append(Value::BIGINT(NumericCast<int64_t>(entry.opens)));
		output.data[2].Append(Value::BIGINT(NumericCast<int64_t>(entry.hits)));
		output.data[3].Append(Value::BIGINT(NumericCast<int64_t>(entry.plans)));
		output.data[4].Append(Value::BIGINT(NumericCast<int64_t>(entry.plan_hits)));
		output.data[5].Append(Value::BIGINT(NumericCast<int64_t>(entry.running_plans)));
		count++;
	}
}

TableFunction HudiTableCacheFunction() {
	return TableFunction("hudi_table_cache", {}, HudiTableCacheScan, HudiTableCacheBind, HudiTableCacheInit);
}

} // namespace duckdb
