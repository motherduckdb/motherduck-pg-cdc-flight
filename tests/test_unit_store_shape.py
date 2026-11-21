"""UnitReader.frames returns frames as result rows (one statement, a leading checks row).
This pins it against the earlier shape, one list(STRUCT) value, kept here as text: the
two shapes go through different DuckDB-to-Python conversions, so every field's value
AND type must match on a page that covers every record kind, a multi-oid TRUNCATE,
NULL and empty nested values, NULL in each nullable column, and the integer extremes.
"""

from dataclasses import replace

import pgstream
import pytest
from test_format2_protocol import binding, config
from test_spool2 import REL, relation_for

from cdc_flight import unit_store
from cdc_flight.format2 import COMMIT, EVENT, UnitConfig
from cdc_flight.ingest_log2 import IngestConfig2, IngestLog2
from cdc_flight.spool2 import Spool

UNIT = UnitConfig(max_commits=2, chunk_frames=3)
DAY = "20261108"
U64, I64, U32 = (1 << 64) - 1, (1 << 63) - 1, (1 << 32) - 1
KEYED = ("record_kind", "unit_start_lsn", "frame_ordinal", "source_epoch")


def txn(lsn, *changes):
    return [pgstream.begin(lsn, -5, 7), *changes, pgstream.commit(lsn, lsn + 1, -4)]


STREAM = (
    [REL, relation_for(102)]
    + txn(100, pgstream.insert(101, ("1", "a")), pgstream.insert(102, ("2", None)))
    + txn(110, pgstream.update(101, ("1", pgstream.UNCHANGED), ("1", None), "O"))
    + txn(120, pgstream.update(101, ("1", "k"), ("1", None), "K"))
    + txn(130, pgstream.delete(101, ("1", None), "O"))
    + txn(140, pgstream.truncate((101, 102), options=3))
    + txn(150, pgstream.update(101, ("1", "new")))
)


@pytest.fixture
def page(stores):
    s, _ = stores
    base = replace(config(), database=s.database.strip('"'))
    cfg = IngestConfig2(**base.__dict__, text_binding=binding(), unit_config=UNIT)
    log = IngestLog2(s.con, cfg)
    log.initialize()
    tables = [
        dict(oid=101, schema="public", name="items", keys=["id"]),
        dict(oid=102, schema="public", name="items102", keys=["id"]),
    ]
    log.register(("system", 1), 90, tables)
    log.acquire("writer")
    try:
        spool = Spool(
            cfg,
            {101: ("id",), 102: ("id",)},
            900,
            "marker",
            binding=cfg.text_binding,
            start_lsn=90,
            unit_config=UNIT,
            clock=lambda: 0,
        )
        items = []
        for wire in STREAM:
            items.extend(spool.feed(wire))
        items.extend(spool.close())
        high = log.append(items, partition_day=DAY)
    finally:
        log.release()
    frames = "{s}frames_v2_" + DAY
    # The extremes and NULL/empty cases, written in place. frames() checks counts and
    # positions, never these values, so the page stays readable. Rows are picked by
    # their stored position, which the ORDER BY of both shapes keeps.
    first_event = log.execute(
        f"SELECT unit_start_lsn,frame_ordinal FROM {frames} WHERE record_kind={EVENT} "
        "ORDER BY unit_start_lsn,frame_ordinal LIMIT 1"
    ).fetchone()
    first_commit = log.execute(
        f"SELECT unit_start_lsn,frame_ordinal FROM {frames} WHERE record_kind={COMMIT} "
        "ORDER BY unit_start_lsn,frame_ordinal LIMIT 1"
    ).fetchone()
    where = "WHERE unit_start_lsn=? AND frame_ordinal=?"
    log.execute(
        f"UPDATE {frames} SET commit_lsn={U64},event_ordinal={U64},commit_ts_us={I64},"
        f"txid={U32},wire_length={U64},truncate_oids=[],"
        "old={'values':NULL::VARCHAR[],'kinds':[]::UTINYINT[]},"
        "new_key={'values':[NULL,'',repeat('x',5000)],'kinds':[1,3,3]},"
        f"schema_name=NULL,table_name='',key_layout_hash=NULL {where}",
        list(first_event),
    )
    log.execute(
        f"UPDATE {frames} SET end_lsn={U64},end_ts_us=-{I64}-1,transaction_digest='' {where}",
        list(first_commit),
    )
    # A second COMMIT row: NULL in every column but the four the read keys on.
    second_commit = log.execute(
        f"SELECT unit_start_lsn,frame_ordinal FROM {frames} WHERE record_kind={COMMIT} "
        "ORDER BY unit_start_lsn,frame_ordinal LIMIT 1 OFFSET 1"
    ).fetchone()
    columns = [
        c
        for (c,) in log.execute(
            "SELECT column_name FROM duckdb_columns() WHERE database_name=? AND schema_name=? "
            "AND table_name=?",
            [log.config.database, log.config.source_id, "frames_v2_" + DAY],
        ).fetchall()
        if c not in KEYED
    ]
    log.execute(
        f"UPDATE {frames} SET " + ",".join(f"{c}=NULL" for c in columns) + f" {where}",
        list(second_commit),
    )
    reader = log.reader()
    return log, reader, reader.units(1, 90, high), high


def old_frames(log, units, relation_id, epoch=1):
    """The pre-change frame query: one list(STRUCT) value, verbatim."""
    days = sorted({day for unit in units for day in unit.days})
    low, end = units[0].start, units[-1].end
    branches = [
        f"SELECT * FROM {{s}}frames_v2_{day} WHERE source_epoch={epoch} "
        f"AND unit_start_lsn>={low} AND unit_start_lsn<{end}"
        for day in days
    ]
    predicate, parameters = "", []
    if relation_id is not None:
        predicate = " WHERE record_kind=2 OR relation_id=? OR list_contains(truncate_oids,?)"
        parameters = [relation_id, relation_id]
    rows = log.execute(
        "WITH physical AS MATERIALIZED (" + " UNION ALL ".join(branches) + ") "
        "SELECT (SELECT list(f ORDER BY unit_start_lsn,frame_ordinal) FROM physical f"
        + predicate
        + ")",
        parameters,
    ).fetchone()[0]
    return rows or []


def same_typed(a, b, path="frame"):
    assert type(a) is type(b), (path, type(a), type(b))
    if isinstance(a, dict):
        assert list(a) == list(b), path
        for key in a:
            same_typed(a[key], b[key], f"{path}.{key}")
    elif isinstance(a, list):
        assert len(a) == len(b), path
        for i, (x, y) in enumerate(zip(a, b)):
            same_typed(x, y, f"{path}[{i}]")
    else:
        assert a == b, path


@pytest.mark.parametrize("fetch_rows", [8192, 2])  # 2: every read spans many fetches
@pytest.mark.parametrize("relation_id", [None, 101, 102, 999])
def test_frame_rows_equal_the_list_struct_shape_value_for_value_and_type(
    page, relation_id, fetch_rows, monkeypatch
):
    monkeypatch.setattr(unit_store, "FETCH_ROWS", fetch_rows)
    log, reader, units, high = page
    new = reader.frames(units, relation_id, high=high)
    old = old_frames(log, units, relation_id)
    assert new == old
    same_typed(new, old)
    if relation_id is None:
        # The fixture really covers what it claims.
        kinds = {f["record_kind"] for f in new}
        assert kinds == {EVENT, COMMIT}
        assert any(f["truncate_oids"] == [101, 102] for f in new)
        assert any(f["truncate_oids"] == [] for f in new)
        assert any(f["truncate_oids"] is None for f in new)
        assert any(f["old"] is None for f in new) and any(f["old"] is not None for f in new)
        assert any(f["old"] is not None and f["old"]["values"] is None for f in new)
        assert max(f["commit_lsn"] or 0 for f in new) == U64
        assert max(f["end_lsn"] or 0 for f in new) == U64
        assert min(f["end_ts_us"] or 0 for f in new) == -I64 - 1
        assert len(new[0]) == 25
        for field in new[0]:
            assert any(f[field] is None for f in new) or field in KEYED, field
    if relation_id == 999:
        assert {f["record_kind"] for f in new} == {COMMIT}  # commits always pass the filter


def test_frames_is_still_one_statement(page):
    log, reader, units, high = page
    seen = []
    execute = reader.execute

    def counting(statement, parameters=None):
        seen.append(statement)
        return execute(statement, parameters)

    reader.execute = counting
    reader.frames(units, high=high)
    assert len(seen) == 1 and seen[0].startswith("WITH physical AS MATERIALIZED")
