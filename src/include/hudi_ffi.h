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

// File I/O the table reads its files through, supplied by the engine.
// `read` reads the file at `uri`: it reports the size and modification time
// with hudi_read_sink_set_info, which says which bytes to copy and where, and
// returns HUDI_IO_OK, HUDI_IO_NOT_FOUND, or HUDI_IO_ERROR after
// hudi_read_sink_set_error. It runs on threads the engine does not own,
// concurrently. `release` frees `ctx` once nothing uses it, from any thread.
typedef struct HudiReadSink HudiReadSink;

#define HUDI_IO_OK 0
#define HUDI_IO_NOT_FOUND 1
#define HUDI_IO_ERROR 2

typedef struct HudiFileIo {
	void *ctx;
	int (*read)(void *ctx, const char *uri, HudiReadSink *sink);
	void (*release)(void *ctx);
} HudiFileIo;

// Returns 1 and sets `offset`, `len` and `buf` when `len` bytes of the file
// starting at `offset` must be copied into `buf`; 0 when no bytes are wanted.
// `buf` is uninitialized: `read` returns HUDI_IO_OK only after writing all of it.
int hudi_read_sink_set_info(HudiReadSink *sink, uint64_t size, int64_t last_modified_us, uint64_t *offset,
                            size_t *len, uint8_t **buf);
void hudi_read_sink_set_version(HudiReadSink *sink, const char *tag, size_t len);
void hudi_read_sink_set_error(HudiReadSink *sink, const char *message, size_t len);

const char *hudi_last_error(void);

int hudi_table_open(const char *uri, const char *hudi_options, HudiTable **out);
// Takes ownership of `io->ctx` whether or not the open succeeds.
int hudi_table_open_with_io(const char *uri, const char *hudi_options, const HudiFileIo *io, HudiTable **out);
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
int hudi_plan_info(const HudiPlan *plan, HudiPlanInfo *out);
// Streams slice slice_index projected to columns (in that order) and masked by
// the plan's filters, in batches of about batch_size rows.
int hudi_plan_slice_stream(const HudiPlan *plan, size_t slice_index, const char *const *columns, size_t n_columns,
                           size_t batch_size, struct ArrowArrayStream *out);

#ifdef __cplusplus
}
#endif
