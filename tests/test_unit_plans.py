"""The grouped writer is set based; metadata plans do not print their payloads."""

from dataclasses import replace

from test_ingest_log2 import build, log2  # noqa: F401 (fixture)

from cdc_flight.format2 import Chunk
from cdc_flight.unit_store import CHUNK_TYPES, insert_statement, metadata_parameter


class Recording:
    def __init__(self, con):
        self.con, self.statements, self.snapshot = con, [], None
        self.frame_query = None

    def execute(self, sql, parameters=None):
        self.statements.append(sql)
        if sql.startswith("WITH candidates"):
            self.snapshot = (sql, parameters)
        if sql.startswith("WITH physical"):
            self.frame_query = (sql, parameters)
        return self.con.execute(sql, parameters) if parameters else self.con.execute(sql)

    def __getattr__(self, name):
        return getattr(self.con, name)


def test_warm_grouped_append_statement_budget_is_independent_of_members(
    log2,  # noqa: F811
):
    log2.append(build(log2), partition_day="20261106")
    recovery = log2.recovery()
    from test_spool2 import REL, transaction

    items = build(log2, messages=[REL] + transaction(102) + transaction(103), recovery=recovery)
    recording = Recording(log2.con)
    log2.con = recording
    assert log2.append(items, partition_day="20261106") == 104
    sql = recording.statements
    assert sql[0] == "BEGIN" and sql[-1] == "COMMIT"
    assert len(sql) == 9, sql  # existing descriptors; the optional insert makes 10
    assert sum(statement.startswith("WITH candidates") for statement in sql) == 1
    assert not any("RETURNING" in statement for statement in sql)
    assert "guard=uuid()" in sql[1] and "expires_at=current_timestamp" in sql[1]
    assert sql[3].startswith("INSERT INTO") and "BY NAME" in sql[3]


def _plan(log, sql, params):
    return "\n".join(str(row[-1]) for row in log.con.execute("EXPLAIN " + sql, params).fetchall())


def test_bulk_chunk_projection_plan_does_not_expand_with_input(
    log2,  # noqa: F811
):
    from cdc_flight.ingest_log2 import _chunk_row

    chunk = next(c for c in build(log2) if isinstance(c, Chunk))
    sql = insert_statement("chunks_v2", CHUNK_TYPES).replace("{s}", log2.prefix)
    plans = []
    for n in (1, 100, 2200):
        rows = [_chunk_row(replace(chunk, unit_start_lsn=90 + i), "20261106") for i in range(n)]
        plans.append(_plan(log2, sql, [metadata_parameter(rows, CHUNK_TYPES)]))
    assert max(map(len, plans)) - min(map(len, plans)) <= 1024


def test_combined_retry_projection_plan_does_not_expand_with_candidates(
    log2,  # noqa: F811
):
    chunk = next(c for c in build(log2) if isinstance(c, Chunk))
    log2.recovery()
    recording = Recording(log2.con)
    log2.con = recording
    plans = []
    for n in (1, 100, 2200):
        chunks = [replace(chunk, unit_start_lsn=90 + i) for i in range(n)]
        log2._snapshot(chunks, [c.unit_start_lsn for c in chunks])
        sql, params = recording.snapshot
        plans.append(_plan(log2, sql, params))
    assert max(map(len, plans)) - min(map(len, plans)) <= 1024
