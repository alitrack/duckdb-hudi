#pragma once

#include "duckdb.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/main/client_context_state.hpp"
#include "hudi_ffi.hpp"

#include <functional>
#include <list>

namespace duckdb {

// An opened table and the plans made over it. A plan depends only on the
// table's state and on its key (the query type, timestamps and filters), so it
// is reused for as long as the table is.
class HudiTableEntry {
public:
	explicit HudiTableEntry(shared_ptr<HudiTableHandle> handle_p) : handle(std::move(handle_p)) {
	}

	// The plan kept for `key`, else the one `create` makes, which is then kept.
	shared_ptr<HudiPlanHandle> GetPlan(const string &key, const std::function<shared_ptr<HudiPlanHandle>()> &create);
	idx_t PlanCount();
	idx_t PlanHits();

	const shared_ptr<HudiTableHandle> handle;

private:
	shared_ptr<HudiPlanHandle> FindPlan(const string &key);

	mutex lock;
	struct KeptPlan {
		string key;
		shared_ptr<HudiPlanHandle> plan;
		idx_t slices;
	};
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

	void QueryEnd(ClientContext &context) override;

	struct EntryInfo {
		string uri;
		idx_t opens;
		idx_t hits;
		idx_t plans;
		idx_t plan_hits;
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
};

// hudi_table_cache(): one row per table the session keeps.
TableFunction HudiTableCacheFunction();

} // namespace duckdb
