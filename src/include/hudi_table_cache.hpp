#pragma once

#include "duckdb.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/main/client_context_state.hpp"
#include "hudi_ffi.hpp"

#include <functional>
#include <future>
#include <list>

namespace duckdb {

// A plan being made in the background: the first get() waits for it, and
// throws what planning in the foreground would have thrown.
using HudiPlanFuture = std::shared_future<shared_ptr<HudiPlanHandle>>;

// An opened table and the plans made over it. A plan depends only on the
// table's state and on its key (the query type, timestamps and filters), so it
// is reused for as long as the table is.
class HudiTableEntry {
public:
	explicit HudiTableEntry(shared_ptr<HudiTableHandle> handle_p) : handle(std::move(handle_p)) {
	}

	// The plan kept for `key`, waiting for it if it is still being made, else
	// the one `create` makes, which is then kept. A kept plan that failed in
	// the background is dropped and `create` plans again.
	shared_ptr<HudiPlanHandle> GetPlan(const string &key, const std::function<shared_ptr<HudiPlanHandle>()> &create);
	// Keeps the plan `start` begins making for `key` unless one is kept
	// already, and returns it; `start` returns an invalid future when it could
	// not begin.
	HudiPlanFuture StartPlan(const string &key, const std::function<HudiPlanFuture()> &start);
	// Waits for `pending`, a plan StartPlan began for `key`, and keeps it as a
	// finished plan, or drops it if it failed.
	void FinishPlan(const string &key, const HudiPlanFuture &pending);
	idx_t PlanCount();
	// The kept plans still being made.
	idx_t RunningPlanCount();
	idx_t PlanHits();

	const shared_ptr<HudiTableHandle> handle;

private:
	struct KeptPlan {
		string key;
		// Null while the plan is being made, and `pending` yields it.
		shared_ptr<HudiPlanHandle> plan;
		HudiPlanFuture pending;
		// Zero until the plan is made.
		idx_t slices;
	};

	KeptPlan *FindPlan(const string &key);
	void DropPending(const string &key);
	shared_ptr<HudiPlanHandle> Keep(const string &key, shared_ptr<HudiPlanHandle> plan);
	void EvictPlans();

	mutex lock;
	// Most recently used first.
	std::list<KeptPlan> plans;
	idx_t kept_slices = 0;
	idx_t plan_hits = 0;
};

// The tables read_hudi opened in one session, keyed by uri and `hudi_option`.
class HudiTableCache : public ClientContextState {
public:
	// The table at `uri` opened with `hudi_options`. The first read of a table
	// in a statement takes the session's entry while its timeline is unchanged,
	// else a newly opened one, which the session keeps; every later read of it
	// in that statement gets the same entry, so one statement sees one snapshot
	// of each table. With `hudi_table_cache` off, the session drops what it
	// kept and the table is opened for the statement alone.
	static shared_ptr<HudiTableEntry> Get(ClientContext &context, const string &uri, const string &hudi_options);
	// Starts the plan for `key` in the background (see
	// HudiTableEntry::StartPlan) when a statement is running; the statement's
	// end waits for it, so no plan outlives the statement that started it.
	static void StartPlan(ClientContext &context, const shared_ptr<HudiTableEntry> &entry, const string &key,
	                      const std::function<HudiPlanFuture()> &start);

	void QueryEnd(ClientContext &context) override;

	struct EntryInfo {
		string uri;
		idx_t opens;
		idx_t hits;
		idx_t plans;
		idx_t plan_hits;
		idx_t running_plans;
	};
	vector<EntryInfo> Entries();

private:
	struct Slot {
		string uri;
		shared_ptr<HudiTableEntry> entry;
		idx_t opens = 0;
		idx_t hits = 0;
		idx_t last_used = 0;
	};

	shared_ptr<HudiTableEntry> GetEntry(const string &uri, const string &hudi_options, bool keep, idx_t query);
	shared_ptr<HudiTableEntry> GetKept(const string &key, const string &uri, const string &hudi_options);

	mutex lock;
	unordered_map<string, Slot> slots;
	idx_t clock = 0;
	// The entries the running statement has read, by key, and that statement's
	// query number.
	unordered_map<string, shared_ptr<HudiTableEntry>> pinned;
	idx_t pinned_query = MAXIMUM_QUERY_ID;
	struct StartedPlan {
		shared_ptr<HudiTableEntry> entry;
		string key;
		HudiPlanFuture pending;
	};
	// The background plans the running statement started.
	vector<StartedPlan> started;
};

// hudi_table_cache(): one row per table the session keeps.
TableFunction HudiTableCacheFunction();

} // namespace duckdb
