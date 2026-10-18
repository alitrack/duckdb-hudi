## Create script

Structure of table:
- Merge-on-read table, table version 6, metadata table disabled, parquet log blocks
  (`hoodie.table.log.file.format = PARQUET`), Spark record merger
- Partitioned by `byteField` with the simple key generator, non-hive-style partition
  paths (`10/`, `20/`, `30/`), record key `id`, precombine field `longField`
- Two commits: an insert of three rows, then an upsert that updates `id = 1` (a log file
  in partition `10`) and inserts `id = 4` (a new base file in partition `30`)
- Same columns and rows as `cow_v6_simplekeygen_nonhivestyle`, so a merged read must
  return the same four rows

The table is `mor/parquet/v6_simplekeygen_nonhivestyle` of the hudi-rs sample tables
(`crates/test/data/sample_table`), whose `.sql` file there has the full statements. Zipped
with the `mor_v6_simplekeygen_nonhivestyle/` directory at the archive root, leaving out the
`.crc` files.

`scripts/inflate_fixtures.sh` inflates the archive next to it before the sqllogictests run.
Values the tests rely on, besides the rows shared with the copy-on-write twin:

| Query | Value |
| --- | --- |
| `hudi_info` | `type=MERGE_ON_READ`, 3 file slices |
| `hudi_files` | one `MOR` slice with 1 log file (partition `10`), two `COW` slices |
| `isActive` of `id = 1` after the merge | false |
