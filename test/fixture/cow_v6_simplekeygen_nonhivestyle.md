## Create script

Structure of table:
- Copy-on-write table, table version 6, metadata table disabled
- Partitioned by `byteField` with the simple key generator, non-hive-style partition
  paths (`10/`, `20/`, `30/`), record key `id`, precombine field `longField`
- Two commits: an insert of three rows, then an upsert that updates `id = 1` and inserts
  `id = 4`, so partition `10` holds two base files and the other partitions one each
- Columns of every primitive type plus an array of structs, a map of structs and a
  nested struct; no NULLs

The table is `cow/v6_simplekeygen_nonhivestyle` of the hudi-rs sample tables
(`crates/test/data/sample_table`), whose `.sql` file next to the archive there has the full
`CREATE TABLE` and `INSERT` statements. Zipped with the `cow_v6_simplekeygen_nonhivestyle/`
directory at the archive root, leaving out the `.crc` files.

`scripts/inflate_fixtures.sh` inflates the archive next to it before the sqllogictests run.
`test/sql/pushdown.test` compares pruned reads against an unpruned copy of the table; the
values it relies on:

| Query | Value |
| --- | --- |
| `count(*)` | 4 |
| `id` per partition | `10`: 1, 3; `20`: 2; `30`: 4 |
| `name`, `isActive` by `id` | Alice false, Bob false, Carol true, Diana true |
| `intField` by `id` | 15000, 25000, 35000, 45000 |
| `dateField` by `id` | 2023-04-01 to 2023-04-04 |
| commits | 20240418172802262, 20240418172804498 |
