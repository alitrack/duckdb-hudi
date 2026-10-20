## Create script

Structure of table:
- Copy-on-write table, table version 6, metadata table disabled
- Partitioned by `byteField` and `shortField` with the complex key generator, hive-style
  partition paths (`byteField=10/shortField=300/` and so on), record key `id,name`,
  precombine field `longField`
- Two commits: an insert of three rows, then an upsert that updates `id = 1` and inserts
  `id = 4`
- Same rows as `cow_v6_simplekeygen_nonhivestyle` apart from the second partition column

The table is `cow/v6_complexkeygen_hivestyle` of the hudi-rs sample tables
(`crates/test/data/sample_table`), whose `.sql` file there has the full statements. Zipped
with the `cow_v6_complexkeygen_hivestyle/` directory at the archive root, leaving out the
`.crc` files.

`scripts/inflate_fixtures.sh` inflates the archive next to it before the sqllogictests run.
`test/sql/pushdown.test` reads it with the same predicates as the simple-key tables and
expects the same rows (`count(*)` 4; `byteField = 10` gives `id` 1 and 3; `byteField IN
(10, 30)` keeps 2 of 3 partitions).
