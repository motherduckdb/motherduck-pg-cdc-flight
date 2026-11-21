"""Metadata transport precision and payload identity on the format-2 change log.

Ledger rows (chunks_v2, units_v2, descriptors_v2) travel as one JSON parameter checked by
metadata_parameter before any SQL; frames travel as one Arrow upload per day. Format 1's
bounded consolidation copies have no format-2 counterpart (a chunk is already bounded by
the unit configuration's chunk_bytes/chunk_frames: test_spool2), so that claim retires.
"""

from dataclasses import replace
from decimal import Decimal

import duckdb
import pyarrow as pa
import pytest
from test_phase1 import config, frames, log, spool_all, transaction  # noqa: F401 (fixtures)

from cdc_flight.format2 import FRAME_SCHEMA, Chunk, UnitComplete
from cdc_flight.unit_store import CHUNK_TYPES, DESCRIPTOR_TYPES, UNIT_TYPES

pytestmark = pytest.mark.parametrize("stores", ["local"], indirect=True)

TEXT = 'quote" slash\\ 🦆 null'
NULLABLE_UNIT = {
    "end_lsn",
    "digest",
    "first_commit",
    "last_commit",
    "tail_commit",
    "last_end",
    "tail_txid",
    "tail_timestamp",
    "abandoned_into_epoch",
    "abandoned_reason",
    "abandoned_frame_end",
}


def chunk_row(**changes):
    row = dict(
        id=TEXT,
        epoch=2**32 - 1,
        unit_start_lsn=2**64 - 1,
        first_frame=2**64 - 1,
        n=0,
        event_count=0,
        member_count=0,
        bytes=0,
        digest=TEXT,
        chain_digest=TEXT,
        partition_day="20261105",
        config_revision=1,
        mapping_revision=1,
        descriptor_keys="[]",
    )
    return dict(row, **changes)


def unit_row(**changes):
    row = {name: 1 if dtype != "VARCHAR" else "d" for name, dtype in UNIT_TYPES.items()}
    row.update(complete=False, **{name: None for name in NULLABLE_UNIT})
    return dict(row, **changes)


def descriptor_row(**changes):
    row = {name: "v" for name in DESCRIPTOR_TYPES}
    row.update(epoch=1, relation_id=1)
    return dict(row, **changes)


def guarded_insert(log, table, types, rows, nullable=()):  # noqa: F811
    with log.transaction():
        log.guard()
        log._insert(table, types, rows, nullable=nullable)


def test_metadata_integer_string_and_null_fidelity(log):  # noqa: F811
    guarded_insert(log, "chunks_v2", CHUNK_TYPES, [chunk_row()])
    descriptor = {name: TEXT for name in DESCRIPTOR_TYPES}
    descriptor.update(epoch=2**32 - 1, relation_id=2**32 - 1, descriptor='{"null":null}')
    guarded_insert(log, "descriptors_v2", DESCRIPTOR_TYPES, [descriptor])
    unit = unit_row(epoch=1, unit_start_lsn=2**63 + 1)
    guarded_insert(log, "units_v2", UNIT_TYPES, [unit], NULLABLE_UNIT)
    for table, types, row in (
        ("chunks_v2", CHUNK_TYPES, chunk_row()),
        ("descriptors_v2", DESCRIPTOR_TYPES, descriptor),
        ("units_v2", UNIT_TYPES, unit),
    ):
        columns = ",".join(types)
        assert log.execute(f"SELECT {columns} FROM {{s}}{table}").fetchone() == tuple(row.values())
    # Out of range: refused by the parameter check, before any SQL runs.
    for column in ("unit_start_lsn", "first_frame", "n"):
        for value in (-1, 2**64):
            with pytest.raises(ValueError, match="metadata range"):
                guarded_insert(log, "chunks_v2", CHUNK_TYPES, [chunk_row(**{column: value})])
    with pytest.raises(ValueError, match="metadata range"):
        guarded_insert(log, "chunks_v2", CHUNK_TYPES, [chunk_row(epoch=2**32)])
    assert log.execute("SELECT count(*) FROM {s}chunks_v2").fetchone() == (1,)


@pytest.mark.parametrize(
    "field,value",
    [
        ("unit_start_lsn", float(2**53 + 1)),
        ("count", True),
        ("count", "7"),
        ("count", Decimal(7)),
        ("digest", 7),
        ("chain_digest", None),
    ],
)
def test_bad_chunk_metadata_fails_before_guard(log, config, tmp_path, monkeypatch, field, value):  # noqa: F811
    items = spool_all(config, tmp_path, transaction(1))
    assert isinstance(items[0], Chunk)
    items[0] = replace(items[0], **{field: value})
    monkeypatch.setattr(log, "guard", lambda: pytest.fail("malformed chunk reached guard"))
    with pytest.raises((ValueError, TypeError)):
        log.append(items)
    assert log.execute("SELECT count(*) FROM {s}chunks_v2").fetchone() == (0,)


@pytest.mark.parametrize(
    "field,value", [("end_lsn", float(2**53 + 1)), ("member_count", True), ("digest", 7)]
)
def test_bad_commit_metadata_fails_before_guard(log, config, tmp_path, monkeypatch, field, value):  # noqa: F811
    items = spool_all(config, tmp_path, transaction(1))
    assert isinstance(items[-1], UnitComplete)
    items[-1] = replace(items[-1], **{field: value})
    monkeypatch.setattr(log, "guard", lambda: pytest.fail("malformed terminal reached guard"))
    with pytest.raises((ValueError, TypeError)):
        log.append(items)
    assert log.execute("SELECT count(*) FROM {s}chunks_v2").fetchone() == (0,)


def test_lone_surrogate_is_rejected_by_both_transports(log):  # noqa: F811
    invalid = "\ud800"
    with pytest.raises(RuntimeError, match="Unable to cast Python instance"):
        log.execute("SELECT unnest(?::VARCHAR[])", [[invalid]])
    with pytest.raises((duckdb.Error, ValueError, UnicodeError)):
        guarded_insert(
            log, "descriptors_v2", DESCRIPTOR_TYPES, [descriptor_row(descriptor=invalid)]
        )
    assert log.execute("SELECT count(*) FROM {s}descriptors_v2").fetchone() == (0,)


@pytest.mark.parametrize("change", ["missing", "extra", "null", "bool"])
def test_metadata_shape_and_complete_null_checks(log, change):  # noqa: F811
    row = unit_row(epoch=1, unit_start_lsn=100)
    if change == "missing":
        del row["frame_count"]
    elif change == "extra":
        row["unknown"] = 0
    elif change == "null":
        row["frame_count"] = None  # not nullable
    else:
        row["complete"] = 1
    with pytest.raises(ValueError, match="metadata"):
        guarded_insert(log, "units_v2", UNIT_TYPES, [row], NULLABLE_UNIT)
    assert log.execute("SELECT count(*) FROM {s}units_v2").fetchone() == (0,)


def frame_rows(log):  # noqa: F811
    return log.execute(
        f"SELECT * FROM ({frames(log)}) ORDER BY source_epoch,unit_start_lsn,frame_ordinal"
    ).fetchall()


def test_prefix_replay_keeps_raw_rows_and_each_chunk_identity(log, config, tmp_path):  # noqa: F811
    items = spool_all(config, tmp_path, transaction(10, commit=2**63 + 9))
    chunks = [item for item in items if isinstance(item, Chunk)]
    assert len(chunks) > 2
    log.append(chunks[:2], partition_day="20261104")
    prefix = frame_rows(log)
    log.append(items, partition_day="20261105")
    rows = frame_rows(log)
    expected = pa.concat_tables([c.table for c in chunks]).to_pylist()
    assert rows == [tuple(row.values()) for row in expected]
    assert len(rows) == len({row[1:4] for row in rows}) == sum(c.count for c in chunks)
    assert rows[: len(prefix)] == prefix
    # The durable prefix replay is checked before a later completed replay.
    log.append(items, partition_day="20261105")
    assert frame_rows(log) == rows
    assert log.execute("SELECT id,digest,n FROM {s}chunks_v2 ORDER BY first_frame").fetchall() == [
        (c.id, c.digest, c.count) for c in chunks
    ]
    assert log.execute("SELECT frame_count,digest,complete FROM {s}units_v2").fetchone() == (
        items[-1].frame_count,
        items[-1].digest,
        True,
    )
    assert log.checkpoint() == items[-1].end_lsn


def units(config, tmp_path, commits):  # noqa: F811
    """One unit per one-insert transaction, each spooled from the previous END."""
    items, start = [], 0
    for commit in commits:
        items.extend(spool_all(config, tmp_path, transaction(1, commit=commit), start_lsn=start))
        start = commit + 1
    return items


def test_one_frame_upload_per_day_and_retry(log, config, tmp_path, monkeypatch):  # noqa: F811
    """Several units' chunks go up in one Arrow upload per day, with the frame schema
    and its metadata; the chunks' own tables are never modified; a failed insert
    unregisters its view and the retry stores every frame once."""
    items = units(config, tmp_path, (100, 200, 300))
    chunks = [item for item in items if isinstance(item, Chunk)]
    original = [c.table for c in chunks]
    uploaded = []
    register, execute = log.con.register, log.execute

    class RecordingConnection:
        def __getattr__(self, name):
            return getattr(connection, name)

        def register(self, name, table):
            if name.startswith("cdc_v2_") and name != "cdc_v2_empty":
                uploaded.append(table)
            return register(name, table)

    connection = log.con
    monkeypatch.setattr(log, "con", RecordingConnection())

    def fail_insert(sql, params=None):
        if sql.startswith("INSERT INTO {s}frames_v2_"):
            raise RuntimeError("upload failed")
        return execute(sql, params)

    monkeypatch.setattr(log, "execute", fail_insert)
    with pytest.raises(RuntimeError, match="upload failed"):
        log.append(items, partition_day="20261105")
    monkeypatch.setattr(log, "execute", execute)
    log.append(items, partition_day="20261105")
    assert len(uploaded) == 2  # the failed attempt and the retry: one upload each
    for table in uploaded:
        assert table.schema.equals(FRAME_SCHEMA, check_metadata=True)
        assert table.equals(pa.concat_tables(original))
    assert all(c.table is before for c, before in zip(chunks, original, strict=True))
    assert len(frame_rows(log)) == sum(c.count for c in chunks)
    assert log.execute("SELECT count(*) FROM {s}chunks_v2").fetchone() == (len(chunks),)
    # Finally-unregister runs even on the failed insertion.
    assert log.con.execute(
        "SELECT count(*) FROM duckdb_views() WHERE view_name LIKE 'cdc_v2_%'"
    ).fetchone() == (0,)


@pytest.mark.parametrize("target", ["chunks_v2", "units_v2", "descriptors_v2", "snapshot"])
def test_metadata_plans_do_not_expand_with_batch_size(log, config, tmp_path, monkeypatch, target):  # noqa: F811
    lengths = []
    execute = log.execute

    class PlanCaptured(Exception):
        pass

    def recording(sql, params=None):
        prefix = "WITH candidates AS" if target == "snapshot" else f"INSERT INTO {{s}}{target} "
        if sql.startswith(prefix):
            plan = execute("EXPLAIN " + sql, params).fetchall()
            lengths.append(sum(len(row[1]) for row in plan))
            # Keep table statistics unchanged between the two input sizes.
            raise PlanCaptured
        return execute(sql, params)

    makers = {
        "chunks_v2": (CHUNK_TYPES, lambda i: chunk_row(id=str(i), first_frame=i), ()),
        "units_v2": (UNIT_TYPES, lambda i: unit_row(epoch=1, unit_start_lsn=i), NULLABLE_UNIT),
        "descriptors_v2": (
            DESCRIPTOR_TYPES,
            lambda i: descriptor_row(relation_id=i, version=str(i)),
            (),
        ),
    }
    monkeypatch.setattr(log, "execute", recording)
    for count in (10, 1000):
        if target == "snapshot":
            items = units(config, tmp_path, range(100, 100 * (count + 1), 100))
            with pytest.raises(PlanCaptured):
                log.append(items, partition_day="20261105")
        else:
            types, make, nullable = makers[target]
            with pytest.raises(PlanCaptured):
                log._insert(target, types, [make(i) for i in range(count)], nullable=nullable)
    assert len(lengths) == 2
    # Cardinality labels may grow; folded input values must not grow the plan.
    assert lengths[1] - lengths[0] <= 1024, lengths
