## Create script

Structure of table:
- COW non-partitioned table, metadata table disabled
- One commit writing one base file of 5000 rows, more than one DuckDB vector (2048 rows)
- No log files

Written with Spark 3.5.5 and Hudi 1.1.1 (`hudi-spark3.5-bundle_2.12`), then zipped with the
`cow_single_file_5000/` directory at the archive root, leaving out the `.crc` files:

```sql
CREATE TABLE cow_5000 (id BIGINT, price DOUBLE, qty INT, name STRING)
USING hudi
LOCATION 'file:///tmp/cow_single_file_5000'
TBLPROPERTIES (type = 'cow', primaryKey = 'id', 'hoodie.metadata.enable' = 'false');

SET hoodie.metadata.enable=false;
SET hoodie.spark.sql.insert.into.operation=bulk_insert;
SET hoodie.bulkinsert.shuffle.parallelism=1;

INSERT INTO cow_5000
SELECT /*+ COALESCE(1) */ id, CAST(id AS DOUBLE) * 1.5 AS price, CAST(id % 50 AS INT) AS qty,
       concat('name-', id) AS name
FROM range(1, 5001);
```

`scripts/gate.sh` inflates the archive into a temporary directory for each run and deletes it
afterwards. The values it expects are what `read_parquet` returns over the base file:

| Query | Value |
| --- | --- |
| `count(*)` | 5000 |
| `sum(id)` | 12502500 |
| `max(price * (1 - qty / 100.0))` | 7500.0 |
| `sum(length(name))` | 43893 |
