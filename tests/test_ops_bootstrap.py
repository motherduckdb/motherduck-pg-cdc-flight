"""The first report makes a fresh operations database readable before compaction."""

from types import SimpleNamespace

import duckdb
import pytest
from test_compaction import Statements
from test_dive_queries import every

from cdc_flight.ops import Ops
from cdc_flight.ops_ingest import report_preflight

EXPECTED = {
    "ingest_status",
    "ingest_runs",
    "preflight_status",
    "apply_status",
    "apply_runs",
    "backfill_status",
    "backfill_runs",
    "compaction_status",
    "audit",
    "sources",
}
CONFIG = SimpleNamespace(database="memory", source_id="fresh")


@pytest.fixture
def fresh_ops(tmp_path):
    path = tmp_path / "ops.duckdb"
    with duckdb.connect() as con:
        con.execute(f"ATTACH '{path}' AS cdc_ops")
        yield con, path


def tables(con):
    return {
        r[0]
        for r in con.execute(
            "SELECT table_name FROM duckdb_tables() WHERE database_name='cdc_ops'"
        ).fetchall()
    }


def test_first_preflight_report_creates_ten_tables_before_compaction(fresh_ops):
    con, path = fresh_ops
    recorded = Statements(con)
    assert report_preflight(recorded, CONFIG, "first", "ready") == "written"
    assert tables(con) == EXPECTED
    for table in EXPECTED:
        assert con.execute(f"SELECT count(*) FROM cdc_ops.main.{table}").fetchone()[0] == (
            1 if table == "preflight_status" else 0
        )
    ddl = [s for s in recorded.seen if s.startswith(("CREATE", "ALTER", "DROP"))]
    assert len(ddl) == 10
    assert all(s.startswith("CREATE TABLE IF NOT EXISTS") for s in ddl)
    # Repeated reports are idempotent and issue no DDL once the tables exist.
    recorded.seen.clear()
    assert report_preflight(recorded, CONFIG, "second", "ready") == "written"
    assert not [s for s in recorded.seen if s.startswith(("CREATE", "ALTER", "DROP"))]
    con.execute("DETACH cdc_ops")
    assert every(path)  # Every all/source query variant binds before compaction.


def test_report_schema_bootstrap_has_no_status_rows_and_all_dive_queries_bind(fresh_ops):
    con, path = fresh_ops
    # Exercise the same report guard/schema path without inserting a status row.
    assert Ops(con, CONFIG.database, CONFIG.source_id).report({}, lambda: None) == "written"
    assert tables(con) == EXPECTED
    for table in EXPECTED:
        assert con.execute(f"SELECT count(*) FROM cdc_ops.main.{table}").fetchone()[0] == 0
    con.execute("DETACH cdc_ops")
    assert every(path)


def test_bootstrap_preserves_unrelated_table_and_caller_column_healing(fresh_ops):
    con, _ = fresh_ops
    con.execute("CREATE TABLE cdc_ops.main.compaction_status (source_id VARCHAR, extra VARCHAR)")
    con.execute("INSERT INTO cdc_ops.main.compaction_status VALUES ('other','keep')")
    con.execute(
        "CREATE TABLE cdc_ops.main.preflight_status (source_id VARCHAR PRIMARY KEY, extra VARCHAR)"
    )
    con.execute("INSERT INTO cdc_ops.main.preflight_status VALUES ('other','keep')")
    recorded = Statements(con)
    for run in ("first", "again"):
        assert report_preflight(recorded, CONFIG, run, "ready") == "written"
    assert con.execute("DESCRIBE cdc_ops.main.compaction_status").fetchall() == [
        ("source_id", "VARCHAR", "YES", None, None, None),
        ("extra", "VARCHAR", "YES", None, None, None),
    ]
    assert con.execute("SELECT * FROM cdc_ops.main.compaction_status").fetchall() == [
        ("other", "keep")
    ]
    assert con.execute(
        "SELECT extra FROM cdc_ops.main.preflight_status WHERE source_id='other'"
    ).fetchone() == ("keep",)
    alters = [s for s in recorded.seen if s.startswith("ALTER")]
    assert alters and all("preflight_status ADD COLUMN IF NOT EXISTS" in s for s in alters)
    assert not [s for s in recorded.seen if s.startswith("DROP")]


def test_bootstrap_ddl_failure_is_best_effort_and_does_not_write(fresh_ops):
    con, _ = fresh_ops

    class FailsCreate(Statements):
        def execute(self, statement, parameters=None):
            if statement.startswith("CREATE TABLE"):
                raise RuntimeError("bootstrap failure")
            return super().execute(statement, parameters)

    assert report_preflight(FailsCreate(con), CONFIG, "failed", "ready") == "failed:RuntimeError"
    assert not tables(con)


def test_remaining_budget_skips_bootstrap(fresh_ops):
    con, _ = fresh_ops
    assert report_preflight(con, CONFIG, "late", "ready", remaining=0) == "skipped:time"
    assert not tables(con)
