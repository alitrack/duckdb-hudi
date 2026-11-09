"""Two reads of one table in one statement see one snapshot of it.

Copies the two-commit COW fixture with its latest commit's completed instant
held back, then runs statements that read the table twice, where the second
read's path goes through `complete`, a Python UDF that completes the commit.
DuckDB folds a table function's arguments while binding it, so the commit lands
after the first read is bound and before the second is. Both reads must count
the snapshot without the commit, and the next statement must see it. Runs with
read_hudi's table cache on and off.

Usage:
  python scripts/statement_snapshot.py [extension]

  extension: the loadable extension (default:
             build/release/extension/hudi/hudi.duckdb_extension). Needs the
             `duckdb` Python package of the same DuckDB version, and `numpy`
             for its Python UDFs.

Exit 0 iff every statement sees one snapshot.
"""
import os
import shutil
import sys
import tempfile

import duckdb

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
EXTENSION = os.path.abspath(
    sys.argv[1] if len(sys.argv) > 1 else os.path.join(ROOT, "build/release/extension/hudi/hudi.duckdb_extension")
)
FIXTURE = os.path.join(ROOT, "test/fixture/cow_v6_nonpartitioned")
INSTANT = ".hoodie/20240418173551906.commit"

CASES = [
    (
        "two subqueries",
        "SELECT (SELECT count(*) FROM read_hudi('{t}')), (SELECT count(*) FROM read_hudi(complete('{t}')))",
        [(3, 3)],
    ),
    (
        "self anti-join",
        "SELECT count(*) FROM read_hudi('{t}') a RIGHT JOIN read_hudi(complete('{t}')) b USING (id) "
        "WHERE a.id IS NULL",
        [(0,)],
    ),
    (
        "cte referenced twice",
        "WITH x AS NOT MATERIALIZED (SELECT id FROM read_hudi('{t}')) "
        "SELECT a.n, m.n, b.n FROM (SELECT count(*) n FROM x) a, "
        "(SELECT count(*) n FROM read_hudi(complete('{t}'))) m, (SELECT count(*) n FROM x) b",
        [(3, 3, 3)],
    ),
]


def run(name, sql, want, cache):
    tmp = os.path.realpath(tempfile.mkdtemp())
    try:
        table = os.path.join(tmp, "table")
        shutil.copytree(FIXTURE, table)
        held = os.path.join(tmp, "held.commit")
        shutil.move(os.path.join(table, INSTANT), held)

        def complete(path):
            if os.path.exists(held):
                shutil.move(held, os.path.join(table, INSTANT))
            return path

        con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
        con.execute(f"LOAD '{EXTENSION}'")
        con.create_function("complete", complete, ["VARCHAR"], "VARCHAR")
        con.execute(f"SET hudi_table_cache = {cache}")
        got = con.execute(sql.format(t=table)).fetchall()
        after = con.execute(f"SELECT count(*) FROM read_hudi('{table}')").fetchall()
        con.close()
        ok = got == want and after == [(4,)]
        print(f"{'ok' if ok else 'FAIL'}: {name}, cache {cache}: got {got}, want {want}; next statement {after}")
        return ok
    finally:
        shutil.rmtree(tmp)


results = [run(name, sql, want, cache) for cache in ("true", "false") for name, sql, want in CASES]
sys.exit(0 if all(results) else 1)
