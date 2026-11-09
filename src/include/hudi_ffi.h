// C ABI of the Rust library (rust/src/ffi.rs). Every function that can fail
// returns 0 on success and non-zero on failure; the message is available from
// hudi_last_error() on the same thread until the next call into the library.
#pragma once

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

struct ArrowSchema;
struct ArrowArrayStream;

typedef struct HudiTable HudiTable;
typedef struct HudiPlan HudiPlan;
typedef struct HudiPendingPlan HudiPendingPlan;

// The read_hudi named parameters, each NULL when absent.
typedef struct HudiReadArgs {
	const char *as_of;
	const char *start;
	const char *end;
} HudiReadArgs;

// One pushed-down predicate: column op values, every value rendered as text.
// op is one of "=", "!=", "<", "<=", ">", ">=", "IN", "NOT IN".
typedef struct HudiFilter {
	const char *column;
	const char *op;
	const char *const *values;
	size_t n_values;
} HudiFilter;

// What a plan kept after pruning.
typedef struct HudiPlanInfo {
	size_t slice_count;
	size_t partition_count;
	// Rows in the kept base files, or -1 when a slice does not report a count.
	int64_t row_estimate;
} HudiPlanInfo;

const char *hudi_last_error(void);

int hudi_table_open(const char *uri, const char *hudi_options, HudiTable **out);
void hudi_table_free(HudiTable *table);
// Writes 1 into out when the table still matches its timeline in storage, 0
// when the timeline changed and the table has to be opened again.
int hudi_table_is_current(const HudiTable *table, int *out);
// Exports the table's read schema into out.
int hudi_table_schema(const HudiTable *table, struct ArrowSchema *out);
// kind 0: the hudi_info summary line; 1: the hudi_files listing. Free the
// result with hudi_string_free.
int hudi_table_describe(const HudiTable *table, int kind, char **out);
void hudi_string_free(char *s);
// kind 0: hudi_mdt; 1: hudi_commits; 2: hudi_stats. Writes a stream whose
// schema is the function's result schema even when there are no rows.
int hudi_table_metadata_stream(const HudiTable *table, int kind, struct ArrowArrayStream *out);

int hudi_plan_create(const HudiTable *table, const HudiReadArgs *args, const HudiFilter *filters, size_t n_filters,
                     HudiPlan **out);
void hudi_plan_free(HudiPlan *plan);
// Starts hudi_plan_create in the background. The handle does not depend on the
// table staying alive. Pass it to hudi_pending_plan_take, or to
// hudi_pending_plan_free if the plan is not needed.
int hudi_plan_create_start(const HudiTable *table, const HudiReadArgs *args, const HudiFilter *filters,
                           size_t n_filters, HudiPendingPlan **out);
// Waits for the plan and writes it into out, or fails with the error
// hudi_plan_create would have reported. Consumes pending either way.
int hudi_pending_plan_take(HudiPendingPlan *pending, HudiPlan **out);
// Frees a handle that is not going to be taken.
void hudi_pending_plan_free(HudiPendingPlan *pending);
int hudi_plan_info(const HudiPlan *plan, HudiPlanInfo *out);
// Streams slice slice_index projected to columns (in that order) and masked by
// the plan's filters, in batches of about batch_size rows.
int hudi_plan_slice_stream(const HudiPlan *plan, size_t slice_index, const char *const *columns, size_t n_columns,
                           size_t batch_size, struct ArrowArrayStream *out);

#ifdef __cplusplus
}
#endif
