import json
import os
import random
import struct
import threading
import time as clock
import uuid
from dataclasses import replace
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal

import conftest
import duckdb
import psycopg2
import pyarrow as pa
import pytest
from psycopg2 import sql
from psycopg2.extras import Json

from cdc_flight.apply import ApplyError, FenceLost
from cdc_flight.apply_run import ApplyRun
from cdc_flight.bootstrap import bootstrap_log
from cdc_flight.config import ApplyConfig, IngestConfig
from cdc_flight.format2 import Binding, UnitConfig
from cdc_flight.ingest import run_once
from cdc_flight.ingest_log2 import IngestConfig2, IngestLog2
from cdc_flight.mirror import (
    Blocked,
    Column,
    cast_sql,
    descriptor_columns,
    duck_type,
    fold,
    reconcile,
)
from cdc_flight.pgoutput import Cell, Event, Relation, normalize
from cdc_flight.preflight import PreflightError, check, query
from cdc_flight.source import Source
from cdc_flight.spool2 import Spool as Spool2

UNCHANGED = object()
TEXT, INT4, BOOL = 25, 23, 16


def numeric(precision, scale):
    return ((precision << 16) | scale) + 4


# ----------------------------------------------------------------------------
# Synthetic pgoutput wire messages


def tuple_data(values):
    out = struct.pack("!H", len(values))
    for value in values:
        if value is None:
            out += b"n"
        elif value is UNCHANGED:
            out += b"u"
        else:
            data = value.encode()
            out += b"t" + struct.pack("!I", len(data)) + data
    return out


def relation_message(oid, name, columns, identity="f"):
    out = b"R" + struct.pack("!I", oid) + b"public\0" + name.encode() + b"\0" + identity.encode()
    out += struct.pack("!H", len(columns))
    for flags, column, type_oid, typmod in columns:
        out += (
            struct.pack("!B", flags)
            + column.encode()
            + b"\0"
            + struct.pack("!Ii", type_oid, typmod)
        )
    return out


class Wire:
    """Builds committed transactions with increasing LSNs."""

    def __init__(self):
        self.lsn = self.end = 1000

    def txn(self, *messages, adjacent=False):
        """adjacent: the commit record starts exactly at the previous end LSN, as
        when another session's data WAL preceded the previous commit."""
        self.lsn = self.end if adjacent else self.lsn + 100
        commit = self.lsn
        self.end = commit + 10
        return (
            [b"B" + struct.pack("!QqI", commit, 0, 1)]
            + list(messages)
            + [b"C\0" + struct.pack("!QQq", commit, self.end, 0)]
        )

    @staticmethod
    def insert(oid, values):
        return b"I" + struct.pack("!I", oid) + b"N" + tuple_data(values)

    @staticmethod
    def update(oid, new, old=None, kind="O"):
        old_part = b"" if old is None else kind.encode() + tuple_data(old)
        return b"U" + struct.pack("!I", oid) + old_part + b"N" + tuple_data(new)

    @staticmethod
    def delete(oid, old, kind="O"):
        return b"D" + struct.pack("!I", oid) + kind.encode() + tuple_data(old)

    @staticmethod
    def truncate(*oids):
        return (
            b"T" + struct.pack("!IB", len(oids), 0) + b"".join(struct.pack("!I", o) for o in oids)
        )


ITEMS = [
    (1, "id", INT4, -1),
    (1, "value", TEXT, -1),
    (1, "flag", BOOL, -1),
    (1, "amount", 1700, numeric(10, 2)),
]
OTHER = [(1, "id", INT4, -1), (0, "flag", BOOL, -1), (0, "blob", TEXT, -1)]
SIMPLE = [(1, "id", INT4, -1), (1, "value", TEXT, -1)]
TABLES = {
    1: ("items", ["id"]),
    2: ("other", ["id"]),
    3: ("third", ["id"]),
    4: ("fourth", ["id"]),
}


def harness_binding(source_id):
    """A complete format-2 text binding for a synthetic source (test_format2_protocol's
    settings), so IngestLog2 accepts the fixture without a PostgreSQL session."""
    from test_format2_protocol import SETTINGS

    return Binding(source_id, SETTINGS, (("plpgsql", "1.0"),))


class Harness:
    """A synthetic format-2 source: the real IngestLog2, the real Spool2 and the public
    (format-2) ApplyRun. One commit per unit by default (max_commits=1), so every unit
    end is a commit end and checkpoints read as they did per transaction; tests of
    multi-commit units pass max_commits."""

    def __init__(self, stores, tmp_path, *, max_commits=1, **apply_options):
        s, other = stores
        self.store, self.other_store = s, other
        self.tmp_path = tmp_path
        database, mirror = s.database.strip('"'), s.mirror_database.strip('"')
        names = tuple(f"public.{name}" for name, _ in TABLES.values())
        base = IngestConfig(
            "phase2",
            "slot",
            "pub",
            names,
            database=database,
            chunk_bytes=1024,
            batch_bytes=4096,
            max_event_bytes=100000,
            allow_local=True,
        )
        self.ingest_config = IngestConfig2(
            **base.__dict__,
            text_binding=harness_binding("phase2"),
            unit_config=UnitConfig(max_commits=max_commits, chunk_bytes=1024),
        )
        self.log = IngestLog2(s.con, self.ingest_config)
        self.log.initialize()
        self.log.register(
            ("system", 1),
            0,
            [
                dict(oid=oid, schema="public", name=name, keys=keys, empty=oid != 4)
                for oid, (name, keys) in TABLES.items()
            ],
        )
        self.config = ApplyConfig(
            "phase2", names, database=database, mirror_database=mirror, **apply_options
        )
        self.apply = ApplyRun(s.con, self.config)
        self.apply.initialize()
        for name in names:
            # "fourth" was not empty after slot creation; a log-only start is explicit.
            self.apply.start_from_log(name, accept_incomplete=name == "public.fourth")
        # Stands in for ingest, which stamps the registered epoch.
        self.spool = Spool2(
            replace(self.ingest_config, epoch=1),
            {oid: tuple(k) for oid, (_, k) in TABLES.items()},
            999,
            "m",
            binding=self.ingest_config.text_binding,
            start_lsn=0,
            unit_config=self.ingest_config.unit_config,
            clock=lambda: 0,
        )
        self.wire = Wire()

    def ingest(self, *transactions, day=None):
        """Append these transactions (each a list of pgoutput messages); `day` places them
        on that partition day (the clock's day otherwise). Returns their commit LSNs."""
        items = []
        for messages in transactions:
            for message in messages:
                items.extend(self.spool.feed(message))
        self.log.acquire("test")
        try:
            self.log.append(items, partition_day=day)
            # Synthetic sources have fixed keys: confirm them through the checkpoint,
            # as ingest does from the catalog after every run.
            self.log.record_keys(
                {oid: list(keys) for oid, (_, keys) in TABLES.items()}, self.log.checkpoint(), 0
            )
        finally:
            self.log.release()
        return [t[0] for t in transactions]

    def rows(self, table, columns="* EXCLUDE (_cdc_updated_at)"):
        return sorted(
            self.store.con.execute(
                f"SELECT {columns} FROM {self.apply.mirror}.public.{table} ORDER BY id"
            ).fetchall()
        )

    def state(self, table):
        relation_id, _ = self.apply.registered("public." + table)
        return self.store.con.execute(
            f"SELECT state,applied_through_lsn,blocked_commit_lsn,detail FROM {self.apply.mirror}._cdc.apply_state WHERE relation_id=?",
            [relation_id],
        ).fetchone()

    def run(self, table, **options):
        return self.apply.apply_table("public." + table, "run", **options)


# ----------------------------------------------------------------------------
# Pure unit tests


def test_type_mapping_and_casts():
    assert duck_type(23, -1) == "BIGINT" and duck_type(21, -1) == "BIGINT"
    assert duck_type(1700, numeric(10, 2)) == "DECIMAL(10,2)"
    assert duck_type(1700, -1) == "VARCHAR"
    assert duck_type(1700, numeric(60, 2)) == "VARCHAR"
    assert duck_type(3802, -1) == "JSON" and duck_type(17, -1) == "BLOB"
    assert duck_type(1007, -1) == "VARCHAR"  # int[] keeps its exact text form
    con = duckdb.connect()
    cases = [
        ("t", "BOOLEAN", True),
        ("-12.50", "DECIMAL(10,2)", __import__("decimal").Decimal("-12.50")),
        ("NaN", "DOUBLE", None),
        ("\\x00ff", "BLOB", b"\x00\xff"),
        ("2024-02-29 23:59:59.123456", "TIMESTAMP", None),
    ]
    for text, dtype, expected in cases:
        value = con.execute(
            f"SELECT {cast_sql('x', dtype)} FROM (SELECT ?::VARCHAR AS x)", [text]
        ).fetchone()[0]
        if expected is not None:
            assert value == expected
    with pytest.raises(duckdb.Error):
        con.execute(
            f"SELECT {cast_sql('x', 'BLOB')} FROM (SELECT ?::VARCHAR AS x)", ["abc"]
        ).fetchone()
    with pytest.raises(duckdb.ConversionException):
        con.execute(
            f"SELECT {cast_sql('x', 'BOOLEAN')} FROM (SELECT ?::VARCHAR AS x)", ["maybe"]
        ).fetchone()


def descriptor(columns, identity="f"):
    return json.dumps(dict(oid=1, namespace="public", name="t", identity=identity, columns=columns))


def test_reconcile_additive_and_blocked_transitions():
    base = descriptor(ITEMS)
    columns = reconcile(None, base, ("id",))
    assert [c.name for c in columns] == ["id", "value", "flag", "amount"]
    assert reconcile(columns, base, ("id",)) == columns
    added = reconcile(columns, descriptor(ITEMS + [(1, "extra", TEXT, -1)]), ("id",))
    assert [c.name for c in added][-1] == "extra"
    # text -> varchar(n) keeps the VARCHAR mirror type but may have changed values (a
    # USING rewrite looks the same): it holds.
    narrowed = [(1, "id", INT4, -1), (1, "value", 1043, 104), *ITEMS[2:]]
    with pytest.raises(Blocked, match="changed type"):
        reconcile(columns, descriptor(narrowed), ("id",))
    # A varchar widening keeps every value: kept, as the column's current source type.
    as_varchar = [Column(c.name, c.dtype, 1043, 104) if c.name == "value" else c for c in columns]
    widened = [(1, "id", INT4, -1), (1, "value", 1043, 204), *ITEMS[2:]]
    kept = reconcile(as_varchar, descriptor(widened), ("id",))
    assert [(c.name, c.dtype) for c in kept] == [(c.name, c.dtype) for c in columns]
    assert next(c for c in kept if c.name == "value").typmod == 204
    for change, reason in [
        (ITEMS[:3], "dropped or renamed"),
        ([*ITEMS[:3], (1, "amount", 1700, numeric(12, 2))], "changed type"),
        ([*ITEMS, (1, "_cdc_x", TEXT, -1)], "reserved prefix"),
    ]:
        with pytest.raises(Blocked, match=reason):
            reconcile(columns, descriptor(change), ("id",))
    with pytest.raises(Blocked, match="registered key"):
        reconcile(None, descriptor([(0, "id", INT4, -1), (1, "value", TEXT, -1)], "d"), ("id",))
    with pytest.raises(Blocked, match="NOTHING"):
        reconcile(None, descriptor(ITEMS, "n"), ("id",))
    assert [c.dtype for c in descriptor_columns(base)] == [
        "BIGINT",
        "VARCHAR",
        "BOOLEAN",
        "DECIMAL(10,2)",
    ]


def cells(names, values):
    return {
        name: {"kind": "unchanged", "value": None}
        if value is UNCHANGED
        else {"kind": "null", "value": None}
        if value is None
        else {"kind": "value", "value": value}
        for name, value in zip(names, values)
    }


def event(op, commit, new=None, old=None, names=("id", "value", "note"), full_old=True):
    return dict(
        op=op,
        full_old=full_old,
        commit_lsn=commit,
        commit_ts_us=0,
        payload=dict(
            new=None if new is None else cells(names, new),
            old=None if old is None else cells(names, old),
        ),
    )


def test_fold_matches_reference_and_model_on_random_full_identity_workload():
    """Small key space: deletes are reinserted, keys move into freed keys, and
    swap cycles run inside one transaction."""
    rng = random.Random(7)
    names = ("id", "value", "note")
    relation = Relation(1, "public", "items", "f", tuple((1, n, TEXT, -1) for n in names))

    def as_cells(row):
        return tuple(
            Cell("unchanged") if v is UNCHANGED else Cell("null") if v is None else Cell("value", v)
            for v in row
        )

    for trial in range(60):
        model, events, references = {}, [], []

        def emit(op, commit, new, old):
            events.append(event(op, commit, new, old))
            references.append(
                Event(
                    "s",
                    1,
                    commit,
                    commit + 1,
                    len(references),
                    relation,
                    op,
                    None if old is None else as_cells(old),
                    None if new is None else as_cells(new),
                )
            )

        def update(commit, key, new_key):
            old = model.pop(key)
            value = old[1] if rng.random() < 0.5 else rng.choice(["d", "e"])
            note = rng.choice([None, "p", "q"])
            model[new_key] = (new_key, value, note)
            # Unchanged TOAST only when the value did not change.
            wire_value = UNCHANGED if value == old[1] and rng.random() < 0.7 else value
            emit("update", commit, (new_key, wire_value, note), old)

        for commit in range(1, 40):
            for _ in range(rng.randint(1, 4)):
                free = [str(k) for k in range(1, 9) if str(k) not in model]
                choice = rng.random()
                if free and (not model or choice < 0.3):
                    key = rng.choice(free)
                    model[key] = (key, rng.choice(["a", "b", "c" * 50]), rng.choice([None, "x"]))
                    emit("insert", commit, model[key], None)
                elif choice < 0.65:
                    key = rng.choice(sorted(model))
                    moved = free and rng.random() < 0.3
                    update(commit, key, rng.choice(free) if moved else key)
                elif choice < 0.8 and len(model) >= 2 and free:
                    a, b = rng.sample(sorted(model), 2)
                    spare = rng.choice(free)
                    update(commit, a, spare)
                    update(commit, b, a)
                    update(commit, spare, b)
                else:
                    key = rng.choice(sorted(model))
                    emit("delete", commit, None, model.pop(key))
        images = fold({}, events, ("id",), list(names), float_keys=frozenset())
        folded = {key: image.values for key, image in images.items() if image.op == "upsert"}
        assert all(image.complete for image in images.values() if image.op == "upsert")
        reference = normalize({}, references, ("id",))
        truth = {(k,): dict(zip(names, row)) for k, row in model.items()}
        assert folded == reference == truth, trial


def test_fold_blocks_on_unknowable_changes():
    names = ["id", "value", "note"]
    # A TOASTed key unchanged on the new image is recovered from the full old image.
    images = fold(
        {},
        [event("update", 1, (UNCHANGED, "v", None), ("1", "v", "n"))],
        ("id",),
        names,
        float_keys=frozenset(),
    )
    assert images[("1",)].values == {"id": "1", "value": "v", "note": None}
    # Without an old image the key cannot be located.
    with pytest.raises(Blocked, match="unchanged"):
        fold(
            {}, [event("update", 2, (UNCHANGED, "v", None))], ("id",), names, float_keys=frozenset()
        )
    # An absent identity column (dropped from the relation) is never guessed.
    with pytest.raises(Blocked, match="absent"):
        fold(
            {},
            [event("insert", 3, ("v", None), names=("value", "note"))],
            ("id",),
            names,
            float_keys=frozenset(),
        )
    # A key-only old tuple's NULL placeholders are not row values.
    images = fold(
        {},
        [event("update", 4, ("1", UNCHANGED, "n"), ("1", None, None), full_old=False)],
        ("id",),
        names,
        float_keys=frozenset(),
    )
    assert images[("1",)].values == {"id": "1", "note": "n"} and not images[("1",)].complete
    # Key change whose unchanged column has no baseline cannot move the row.
    with pytest.raises(Blocked, match="key change") as info:
        fold(
            {},
            [event("update", 4, ("2", UNCHANGED, None), ("1", None, None), full_old=False)],
            ("id",),
            names,
            float_keys=frozenset(),
        )
    assert info.value.commit_lsn == 4
    with pytest.raises(Blocked) as info:
        fold(
            {},
            [dict(op="truncate", commit_lsn=5, commit_ts_us=0, payload={})],
            ("id",),
            names,
            float_keys=frozenset(),
        )
    assert info.value.state == "needs_resync"
    # Partial image of an unseen row is kept incomplete for the MERGE to resolve.
    images = fold(
        {},
        [event("update", 6, ("9", UNCHANGED, "n"), None)],
        ("id",),
        names,
        float_keys=frozenset(),
    )
    assert not images[("9",)].complete and "value" not in images[("9",)].values


def test_apply_config_validation():
    with pytest.raises(ValueError):
        ApplyConfig("s", ("public.t",), database="same", mirror_database="same")
    with pytest.raises(ValueError):
        ApplyConfig("s", ("t",))
    with pytest.raises(ValueError, match="lease"):
        ApplyConfig("s", ("public.t",), runtime_seconds=900, lease_seconds=900)
    assert ApplyConfig("src", ("public.t",)).mirror_database == "src_mirror"


# ----------------------------------------------------------------------------
# Apply against the durable Phase 1 log (local DuckDB and real MotherDuck)


def test_apply_fold_schema_faults_and_resume(stores, tmp_path):
    h = Harness(stores, tmp_path)
    w = h.wire
    rel = relation_message(1, "items", ITEMS)
    h.ingest(
        w.txn(rel, *[w.insert(1, (str(i), f"v{i}", "t", f"{i}.50")) for i in range(1, 6)]),
        w.txn(
            w.update(1, ("1", "v1x", "f", "1.50"), ("1", "v1", "t", "1.50")),
            w.update(1, ("20", "v2", "t", "2.50"), ("2", "v2", "t", "2.50")),
            w.delete(1, ("3", "v3", "t", "3.50")),
        ),
        w.txn(w.update(1, ("4", UNCHANGED, "f", None), ("4", "v4", "t", "4.50"))),
    )
    result = h.run("items")
    assert result.status == "caught_up" and result.transactions == 3 and result.events == 9
    assert h.rows("items", "id,value,flag,amount::VARCHAR") == [
        (1, "v1x", False, "1.50"),
        (4, "v4", False, None),
        (5, "v5", True, "5.50"),
        (20, "v2", True, "2.50"),
    ]
    assert result.boundary == h.log.checkpoint() == h.state("items")[1]
    # Quiet tables still advance to the durable ingest boundary.
    assert h.run("other").boundary == h.log.checkpoint()

    # Additive schema change mid-stream: column appended, older rows NULL.
    extended = ITEMS + [(1, "extra", TEXT, -1)]
    h.ingest(
        w.txn(relation_message(1, "items", extended), w.insert(1, ("6", "v6", "t", "6.00", "e6"))),
        w.txn(w.update(1, ("1", "v1x", "f", "1.50", "e1"), ("1", "v1x", "f", "1.50", None))),
    )
    assert h.run("items").status == "caught_up"
    assert h.rows("items", "id,extra") == [(1, "e1"), (4, None), (5, None), (6, "e6"), (20, None)]

    # Crash inside the MERGE transaction: nothing changes; rerun is identical.
    h.ingest(w.txn(w.update(1, ("5", "v5y", "t", "5.50", None), ("5", "v5", "t", "5.50", None))))
    before = h.state("items")

    def crash(stage):
        if stage == "after_merge":
            raise RuntimeError("killed mid-merge")

    with pytest.raises(RuntimeError, match="mid-merge"):
        h.run("items", fault=crash)
    assert h.state("items") == before
    assert ("v5",) in h.rows("items", "value")

    def lost(stage):
        if stage == "after_commit":
            raise RuntimeError("lost response")

    with pytest.raises(RuntimeError, match="lost response"):
        h.run("items", fault=lost)
    snapshot = h.rows("items")
    again = h.run("items")
    assert again.status == "caught_up" and again.events == 0 and h.rows("items") == snapshot
    assert ("v5y",) in h.rows("items", "value")

    # Unsupported type change: the earlier transaction applies, the table holds.
    keep, blocked = h.ingest(
        w.txn(w.insert(1, ("7", "v7", "t", "7.00", None))),
        w.txn(
            relation_message(
                1, "items", [*extended[:3], (1, "amount", 1700, numeric(12, 2)), extended[4]]
            ),
            w.insert(1, ("8", "v8", "t", "8.00", None)),
        ),
    )
    result = h.run("items")
    assert result.status == "needs_action" and "changed type" in result.detail
    ids = [r[0] for r in h.rows("items", "id")]
    assert 7 in ids and 8 not in ids
    state = h.state("items")
    assert state[0] == "needs_action" and state[2] == struct.unpack("!Q", blocked[1:9])[0]
    assert state[1] == struct.unpack("!Q", keep[1:9])[0] + 10
    # Held tables are not touched by later runs.
    assert h.run("items").status == "needs_action" and h.state("items") == state


def test_apply_partial_images_cast_failure_truncate_and_fencing(stores, tmp_path):
    h = Harness(stores, tmp_path, max_batch_events=2)
    w = h.wire
    other = relation_message(2, "other", OTHER, identity="d")
    third = relation_message(3, "third", SIMPLE)
    fourth = relation_message(4, "fourth", OTHER, identity="d")
    h.ingest(
        w.txn(other, w.insert(2, ("1", "t", "x")), w.insert(2, ("2", "t", "y"))),
        w.txn(w.update(2, ("1", "f", UNCHANGED))),  # default identity: no old image
        w.txn(third, w.insert(3, ("1", "a")), w.insert(3, ("2", "b")), w.insert(3, ("3", "c"))),
        w.txn(w.delete(3, ("2", "b"))),
        w.txn(fourth, w.insert(4, ("1", "t", "x"))),
    )
    result = h.run("other")
    # Batch cap of two events: several MERGE transactions, same final state.
    assert result.status == "caught_up" and result.transactions == 2
    assert h.rows("other", "id,flag,blob") == [(1, False, "x"), (2, True, "y")]
    assert h.run("third").status == "caught_up"
    assert h.rows("third", "id,value") == [(1, "a"), (3, "c")]
    assert h.run("fourth").status == "caught_up"

    # A partial image of a row the mirror lacks has no baseline: hold, apply nothing.
    h.ingest(w.txn(w.update(4, ("9", "t", UNCHANGED))))
    before = h.rows("fourth")
    result = h.run("fourth")
    assert result.status == "needs_action" and "full image" in result.detail
    assert h.rows("fourth") == before

    # A value the mirror type rejects is a poison transaction, never coerced. The
    # clean transactions before it apply; the hold names the exact transaction.
    good, poison, _ = h.ingest(
        w.txn(w.insert(2, ("3", "t", "z"))),
        w.txn(w.insert(2, ("4", "maybe", "z"))),
        w.txn(w.insert(2, ("5", "t", "z"))),
    )
    h.apply.config = replace(h.config, max_batch_events=100)
    result = h.run("other")
    assert result.status == "needs_action" and "ConversionException" in result.detail
    state = h.state("other")
    assert state[2] == struct.unpack("!Q", poison[1:9])[0]
    assert state[1] == struct.unpack("!Q", good[1:9])[0] + 10
    assert h.rows("other", "id") == [(1,), (2,), (3,)]

    # TRUNCATE moves the table to needs_resync after applying earlier transactions.
    h.ingest(w.txn(w.insert(3, ("4", "d"))), w.txn(w.truncate(3)))
    result = h.run("third")
    assert result.status == "needs_resync"
    assert h.rows("third", "id") == [(1,), (3,), (4,)]

    # Overlapping runs: the second reports busy; a stale worker cannot commit.
    relation_id, _ = h.apply.registered("public.items")
    token = h.apply.acquire(relation_id, "first")
    assert h.run("items").status == "busy"
    h.store.con.execute(
        f"UPDATE {h.apply.mirror}._cdc.apply_state SET expires_at=current_timestamp-INTERVAL 1 SECOND WHERE relation_id=?",
        [relation_id],
    )
    h.apply.acquire(relation_id, "second")
    current = h.apply.state(relation_id)
    with pytest.raises(FenceLost):
        h.apply.hold(relation_id, token, current, Blocked("needs_action", "stale", code="poison"))
    with pytest.raises(ApplyError, match="already has a generation"):
        h.apply.start_from_log("public.items")
    # A log-only start needs evidence the table was empty after its slot existed.
    h.store.con.execute(
        f"DELETE FROM {h.apply.mirror}._cdc.apply_state WHERE relation_id=4; "
        f"DELETE FROM {h.apply.mirror}._cdc.generations WHERE relation_id=4"
    )
    with pytest.raises(ApplyError, match="not observed empty"):
        h.apply.start_from_log("public.fourth")
    h.apply.start_from_log("public.fourth", accept_incomplete=True)
    assert h.store.con.execute(
        f"SELECT mode FROM {h.apply.mirror}._cdc.generations WHERE relation_id=4"
    ).fetchall() == [("log_incomplete",)]


def test_adjacent_commit_lsn_is_applied(stores, tmp_path):
    """A commit record that starts at the previous transaction's end LSN is routine
    under concurrent sessions; it must not fall between checkpoints."""
    h = Harness(stores, tmp_path, max_batch_events=1)
    w = h.wire
    rel = relation_message(1, "items", ITEMS)
    h.ingest(w.txn(rel, w.insert(1, ("1", "a", "t", "1.00"))))
    assert h.run("items").status == "caught_up"
    first_end = h.state("items")[1]
    second = w.txn(w.insert(1, ("2", "b", "t", "2.00")), adjacent=True)
    third = w.txn(w.insert(1, ("3", "c", "t", "3.00")), adjacent=True)
    assert struct.unpack("!Q", second[0][1:9])[0] == first_end
    h.ingest(second, third)
    # Batch cap of one event: each adjacent transaction is its own batch boundary.
    result = h.run("items")
    assert result.status == "caught_up" and result.events == 2
    assert h.rows("items", "id") == [(1,), (2,), (3,)]
    # A schema hold cut at an adjacent commit keeps the transaction before it.
    h.ingest(
        w.txn(w.insert(1, ("4", "d", "t", "4.00")), adjacent=True),
        w.txn(
            relation_message(1, "items", [*ITEMS[:3], (1, "amount", TEXT, -1)]),
            w.insert(1, ("5", "e", "t", "x")),
            adjacent=True,
        ),
    )
    assert h.run("items").status == "needs_action"
    assert h.rows("items", "id") == [(1,), (2,), (3,), (4,)]


# ----------------------------------------------------------------------------
# Phase 2 gate on real PostgreSQL: source EXCEPT ALL mirror at quiescent markers


@pytest.fixture
def pg_phase2(stores):
    dsn = os.getenv("CDC_TEST_PG_DSN")
    if not dsn:
        pytest.skip("set CDC_TEST_PG_DSN for real PostgreSQL probes")
    s, _ = stores
    with Source(dsn) as source:
        schema = source.schema
        source.execute(
            "CREATE TABLE typed (tenant int, code text, b boolean, i2 smallint, i8 bigint, "
            "f8 double precision, f4 real, n numeric(12,3), nu numeric, d date, t time, "
            "ts timestamp, tz timestamptz, j jsonb, bin bytea, u uuid, arr int[], iv interval, "
            "PRIMARY KEY (tenant, code))"
        )
        source.execute("ALTER TABLE typed REPLICA IDENTITY FULL")
        source.execute("CREATE TABLE plain (id bigint PRIMARY KEY, qty int, ok boolean)")
        tables = tuple(f"{schema}.{name}" for name in ("items", "typed", "plain"))
        ingest = IngestConfig(
            "phase2",
            "p2_" + schema,
            "p2_" + schema,
            tables,
            heartbeat=schema + ".pulse",
            database=s.database.strip('"'),
            dsn=dsn,
            allow_local=True,
            chunk_bytes=4096,
            batch_bytes=32768,
            flush_seconds=0.1,
            runtime_seconds=60,
        )
        apply = ApplyConfig(
            "phase2",
            tables,
            database=s.database.strip('"'),
            mirror_database=s.mirror_database.strip('"'),
            max_batch_events=50,
        )
        try:
            yield source, ingest, apply
        finally:
            conftest.wait_slot_released(source.con, ingest.slot)
            query(
                source.con,
                "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
                "WHERE slot_name=%s",
                [ingest.slot],
            )
            query(
                source.con,
                sql.SQL("DROP PUBLICATION IF EXISTS {}").format(sql.Identifier(ingest.publication)),
            )


def assert_mirror_equals_source(apply, source, table):
    relation_id, _ = apply.registered(table)
    current = apply.state(relation_id)
    columns = current["columns"]
    schema, name = table.split(".")
    rows = source.execute(
        sql.SQL("SELECT {} FROM {}").format(
            sql.SQL(",").join(sql.SQL("{}::text").format(sql.Identifier(c.name)) for c in columns),
            sql.Identifier(schema, name),
        )
    )
    arrow = pa.table(
        {f"c{i}": pa.array([row[i] for row in rows], pa.string()) for i in range(len(columns))}
    )
    con = apply.con
    con.register("pg_rows", arrow)
    try:
        source_sql = (
            "SELECT "
            + ",".join(f"{cast_sql(f'c{i}', c.dtype)} AS c{i}" for i, c in enumerate(columns))
            + " FROM pg_rows"
        )
        mirror_sql = (
            "SELECT "
            + ",".join(f'"{c.name}" AS c{i}' for i, c in enumerate(columns))
            + f" FROM {current['target']}"
        )
        missing = con.execute(
            f"SELECT count(*) FROM ({source_sql} EXCEPT ALL {mirror_sql})"
        ).fetchone()[0]
        extra = con.execute(
            f"SELECT count(*) FROM ({mirror_sql} EXCEPT ALL {source_sql})"
        ).fetchone()[0]
        assert (missing, extra) == (0, 0), table
        count = con.execute(f"SELECT count(*) FROM {current['target']}").fetchone()[0]
        assert count == len(rows), table
    finally:
        con.unregister("pg_rows")


class Session:
    """An extra PostgreSQL session so transactions interleave in the WAL."""

    def __init__(self, dsn, schema):
        self.con = psycopg2.connect(dsn)
        self.con.autocommit = True
        self.execute(sql.SQL("SET search_path TO {}").format(sql.Identifier(schema)))

    def execute(self, statement, args=None):
        with self.con.cursor() as cur:
            cur.execute(statement, args)
            return cur.fetchall() if cur.description else None

    def close(self):
        self.con.close()


POOLS = {
    "items": [k for k in range(1, 41)],
    "typed": [(t, f"c{k}") for t in (1, 2) for k in range(1, 16)],
    "plain": [k for k in range(1, 31)],
}
KEY_SQL = {"items": "id=%s", "typed": "tenant=%s AND code=%s", "plain": "id=%s"}
KEY_SET = {"items": "id=%s", "typed": "tenant=%s,code=%s", "plain": "id=%s"}
TYPED_COLUMNS = "b,i2,i8,f8,f4,n,nu,d,t,ts,tz,j,bin,u,arr,iv"


def key_args(key):
    return list(key) if isinstance(key, tuple) else [key]


class Workload:
    """Seeded workload over small key spaces, so keys are deleted and reinserted,
    moved into freed keys and swapped in cycles. Transactions interleave across
    sessions: B stays open while A transactions commit, and C (typed only) can
    span a whole ingest run. Keys an open transaction touched are claimed so the
    single-threaded driver never waits on its own row locks."""

    def __init__(self, source, seed):
        self.source, self.rng = source, random.Random(seed)
        self.b = Session(source._dsn, source.schema)
        self.c = Session(source._dsn, source.schema)
        self.rows = {table: set() for table in POOLS}
        self.claimed = set()
        self.items_extra = self.plain_score = False
        self.b_claims = None
        self.c_claims = None

    def close(self):
        self.b.close()
        self.c.close()

    def toast(self):
        # Random hex is incompressible enough to be stored out of line.
        return "".join(f"{self.rng.getrandbits(64):016x}" for _ in range(300))

    def typed_values(self):
        r = self.rng
        return [
            r.choice([True, False, None]),
            r.randint(-32768, 32767),
            r.randint(-(2**63), 2**63 - 1),
            r.choice([0.1, -1e300, float("inf"), float("nan"), 3.141592653589793, None]),
            r.choice([1.5, -0.25, 3.4e38]),
            Decimal(r.randint(-(10**11), 10**11)) / 1000,
            r.choice(["123456789012345678901234567890.123456789", "-0.5", "NaN"]),
            date(r.randint(1, 9999), r.randint(1, 12), r.randint(1, 28)),
            time(r.randint(0, 23), r.randint(0, 59), r.randint(0, 59), r.randint(0, 999999)),
            datetime(2024, r.randint(1, 12), 3, 4, 5, 6, r.randint(0, 999999)),
            datetime(2024, 1, 2, 3, 4, 5, r.randint(0, 999999), tzinfo=timezone.utc),
            Json({"k": r.randint(0, 9), "nested": [1, None, "x"], "s": "é"}),
            bytes(r.getrandbits(8) for _ in range(r.randint(0, 20))),
            str(uuid.UUID(int=r.getrandbits(128))),
            [r.randint(0, 5), None],
            timedelta(days=r.randint(0, 9), microseconds=r.randint(0, 999)),
        ]

    def free(self, table):
        return [
            k for k in POOLS[table] if k not in self.rows[table] and (table, k) not in self.claimed
        ]

    def available(self, table):
        return sorted(k for k in self.rows[table] if (table, k) not in self.claimed)

    def claim(self, holder, table, *keys):
        if holder is not None:
            for key in keys:
                holder.add((table, key))
                self.claimed.add((table, key))

    def insert(self, x, table, key):
        r = self.rng
        if table == "items":
            x(
                "INSERT INTO items (id,value,note) VALUES (%s,%s,%s)",
                [key, self.toast() if r.random() < 0.4 else "small", r.choice([None, "n"])],
            )
        elif table == "typed":
            x(
                f"INSERT INTO typed (tenant,code,{TYPED_COLUMNS}) VALUES (%s,%s,{','.join(['%s'] * 16)})",
                [*key, *self.typed_values()],
            )
        else:
            x(
                "INSERT INTO plain (id,qty,ok) VALUES (%s,%s,%s)",
                [key, r.randint(0, 100), r.choice([True, None])],
            )

    def update(self, x, table, key):
        r = self.rng
        where = f" WHERE {KEY_SQL[table]}"
        if table == "items":
            choice = r.random()
            if choice < 0.4:
                x("UPDATE items SET note=%s" + where, [r.choice([None, "u", "v"]), key])
            elif choice < 0.6 and self.items_extra:
                x("UPDATE items SET extra=%s" + where, [r.choice(["e", None]), key])
            else:
                x("UPDATE items SET value=%s" + where, [self.toast(), key])
        elif table == "typed":
            x(
                f"UPDATE typed SET ({TYPED_COLUMNS})=({','.join(['%s'] * 16)})" + where,
                [*self.typed_values(), *key],
            )
        else:
            assignment = "qty=qty+1" + (",score=%s" if self.plain_score else "")
            args = [r.randint(0, 9)] if self.plain_score else []
            x(f"UPDATE plain SET {assignment}" + where, [*args, key])

    def move(self, x, table, old, new):
        extra = ",b=NOT b" if table == "typed" else ""
        x(
            f"UPDATE {table} SET {KEY_SET[table]}{extra} WHERE {KEY_SQL[table]}",
            [*key_args(new), *key_args(old)],
        )
        self.rows[table].remove(old)
        self.rows[table].add(new)

    def statement(self, x, holder=None, tables=("items", "typed", "plain")):
        r = self.rng
        table = r.choice(tables)
        free, available = self.free(table), self.available(table)
        choice = r.random()
        if free and (not available or choice < 0.35):
            key = r.choice(free)
            self.claim(holder, table, key)
            self.insert(x, table, key)
            self.rows[table].add(key)
        elif not available:
            return
        elif choice < 0.6:
            key = r.choice(available)
            self.claim(holder, table, key)
            self.update(x, table, key)
        elif choice < 0.75 and free:
            old, new = r.choice(available), r.choice(free)
            self.claim(holder, table, old, new)
            self.move(x, table, old, new)
        elif choice < 0.85 and free and len(available) >= 2:
            # Swap cycle inside one transaction: a -> spare, b -> a, spare -> b.
            a, b = r.sample(available, 2)
            spare = r.choice(free)
            self.claim(holder, table, a, b, spare)
            self.move(x, table, a, spare)
            self.move(x, table, b, a)
            self.move(x, table, spare, b)
        else:
            key = r.choice(available)
            self.claim(holder, table, key)
            x(f"DELETE FROM {table} WHERE {KEY_SQL[table]}", key_args(key))
            self.rows[table].remove(key)

    def commit(self, session, holder):
        session.execute("COMMIT")
        self.claimed -= holder

    def start_long(self):
        """C: an open transaction on typed that spans the next ingest run."""
        self.c_claims = set()
        self.c.execute("BEGIN")
        for _ in range(3):
            self.statement(self.c.execute, self.c_claims, tables=("typed",))

    def finish_long(self):
        """Commit C with no further writes. Run first in a round, its commit record
        follows the drained marker's commit, so its commit LSN equals the apply
        checkpoint (barring unrelated server WAL)."""
        if self.c_claims is not None:
            self.commit(self.c, self.c_claims)
            self.c_claims = None

    def transactions(self, count):
        r = self.rng
        for _ in range(count):
            if self.b_claims is None and r.random() < 0.4:
                self.b_claims = set()
                self.b.execute("BEGIN")
                for _ in range(r.randint(1, 3)):
                    self.statement(self.b.execute, self.b_claims)
            # A: multi-table transaction, possibly touching one key repeatedly.
            self.source.execute("BEGIN")
            for _ in range(r.randint(1, 6)):
                self.statement(self.source.execute)
            self.source.execute("COMMIT")
            if self.b_claims is not None and r.random() < 0.5:
                self.statement(self.b.execute, self.b_claims)
                self.commit(self.b, self.b_claims)
                self.b_claims = None
        if self.b_claims is not None:
            self.commit(self.b, self.b_claims)
            self.b_claims = None


def commit_right_after(execute, waiting, lock_key, body):
    """Run `body` in a transaction whose commit record is followed immediately by the
    commit of `waiting`'s open transaction, with no WAL in between (barring other
    sessions in the cluster), so that commit's LSN equals this transaction's end LSN.

    `execute(text, args)` runs a statement on the first session. `waiting` (a Session
    with an open transaction) has already been sent one query, `lock K; COMMIT`, and
    waits on advisory lock K, which writes no WAL. The first transaction takes K
    before `body`; its commit releases K, and `waiting` commits server-side straight
    away: the window is one backend wakeup. Returns `body`'s result."""
    (pid,) = waiting.execute("SELECT pg_backend_pid()")[0]
    errors = []

    def commit_when_unlocked():
        try:
            waiting.execute("SELECT pg_advisory_xact_lock(%s); COMMIT", [lock_key])
        except Exception as exc:  # reported on the main thread below
            errors.append(exc)

    execute("BEGIN")
    execute("SELECT pg_advisory_xact_lock(%s)", [lock_key])
    waiter = threading.Thread(target=commit_when_unlocked, daemon=True)
    waiter.start()
    deadline = clock.monotonic() + 30
    while not execute(
        "SELECT 1 FROM pg_locks WHERE pid=%s AND locktype='advisory' AND NOT granted", [pid]
    ):
        assert clock.monotonic() < deadline and not errors, ("never waited on K", errors)
        clock.sleep(0.01)
    result = body()
    execute("COMMIT")
    waiter.join(30)
    assert not waiter.is_alive() and not errors, ("did not commit", errors)
    return result


def frame_rows(log, columns, where):
    """`columns` of every format-2 frame matching `where`, across all frame days."""
    days = [d for (d,) in log.execute("SELECT DISTINCT partition_day FROM {s}chunks_v2").fetchall()]
    union = " UNION ALL ".join(
        f"SELECT {columns} FROM {{s}}frames_v2_{day} WHERE {where}" for day in days
    )
    return log.execute(union).fetchall() if days else []


def adjacent_commits(log):
    """Count transactions whose commit LSN equals the previous transaction's end, from the
    format-2 COMMIT frames (record_kind 2) of every frame day, in log order."""
    days = [d for (d,) in log.execute("SELECT DISTINCT partition_day FROM {s}chunks_v2").fetchall()]
    union = " UNION ALL ".join(
        f"SELECT unit_start_lsn,frame_ordinal,commit_lsn,end_lsn FROM {{s}}frames_v2_{day} "
        "WHERE record_kind=2"
        for day in days
    )
    return log.execute(
        "SELECT count(*) FROM (SELECT commit_lsn, lag(end_lsn) OVER "
        f"(ORDER BY unit_start_lsn, frame_ordinal) AS prev FROM ({union})) WHERE commit_lsn=prev"
    ).fetchone()[0]


@pytest.mark.postgres
def test_phase2_gate_source_equals_mirror_under_seeded_workload(stores, pg_phase2, request):
    source, ingest_config, apply_config = pg_phase2
    log, info = bootstrap_log(stores[0].con, ingest_config, create_objects=True)
    apply = ApplyRun(stores[0].con, apply_config)
    apply.initialize()
    for table in apply_config.tables:
        apply.start_from_log(table)
    workload = Workload(source, seed=2026)
    # Runs before the source fixture drops its schema, even on failure: open
    # transactions in the extra sessions would otherwise block the DROP.
    request.addfinalizer(workload.close)

    def crash(stage):
        if stage == "after_merge":
            raise RuntimeError("apply killed mid-merge")

    applied_boundaries = []
    for round_number in range(6):
        workload.finish_long()
        if round_number == 2:
            source.execute("ALTER TABLE items ADD COLUMN extra text")
            workload.items_extra = True
        if round_number == 4:
            source.execute("ALTER TABLE plain ADD COLUMN score int")
            workload.plain_score = True
        workload.transactions(12)
        if round_number in (1, 3):
            # Open across the ingest run, apply and comparison; commits next round.
            workload.start_long()
        assert run_once(ingest_config, log).status == "drained"
        if round_number == 3:
            with pytest.raises(RuntimeError, match="mid-merge"):
                apply.run(fault=crash)
        results = apply.run()
        assert [r.status for r in results] == ["caught_up"] * 3, results
        assert {r.boundary for r in results} == {log.checkpoint()}
        applied_boundaries.append(log.checkpoint())
        # Quiescent source at the shared boundary: every published state must match.
        for table in apply_config.tables:
            assert_mirror_equals_source(apply, source, table)
    workload.finish_long()

    def starts_at(boundaries):
        # Transactions (format 2's COMMIT frames) whose commit LSN is a stored checkpoint.
        commits = {c for (c,) in frame_rows(log, "commit_lsn", "record_kind=2")}
        return len(commits & set(boundaries))

    # Construct the case where an exclusive lower bound loses data: a transaction
    # whose commit LSN equals a stored apply checkpoint. C writes before the drain,
    # and its commit record must be the very next WAL record after the drain's marker
    # commit. Any WAL in between (other sessions, or other xdist workers in the
    # cluster) breaks the adjacency, so C's COMMIT must not wait for a client:
    # - ingest's marker upsert runs in a transaction that first takes advisory lock K;
    # - C, still open, has already been sent one query, `lock K; COMMIT`, and waits on
    #   K. Advisory locks write no WAL.
    # When the marker transaction commits, it releases K, and C commits server-side
    # straight away. The window is one backend wakeup. C stays outside that ingest
    # run, which stops at the marker. Retry a few times, and fail below if it never
    # forms.
    import zlib

    import cdc_flight.ingest as ingest_module

    real_query = ingest_module.query
    commit_c = {"armed": False}
    lock_key = zlib.crc32(source.schema.encode())

    def query_then_commit_c(con, statement, params=None):
        text = statement.as_string(con) if hasattr(statement, "as_string") else statement
        if not (commit_c["armed"] and "(id,marker)" in text):
            return real_query(con, statement, params)
        commit_c["armed"] = False
        rows = commit_right_after(
            lambda text, args=None: real_query(con, text, args),
            workload.c,
            lock_key,
            lambda: real_query(con, statement, params),
        )
        workload.claimed -= workload.c_claims
        workload.c_claims = None
        return rows

    request.getfixturevalue("monkeypatch").setattr(ingest_module, "query", query_then_commit_c)
    # Attempts are cheap locally (about 0.3 s) and dear on MotherDuck (about 40 s).
    attempts = conftest.md_size(stores, local=20, motherduck=5)
    for _attempt in range(attempts):
        if starts_at(applied_boundaries):
            break
        workload.start_long()
        commit_c["armed"] = True
        assert run_once(ingest_config, log).status == "drained"
        assert not commit_c["armed"], "C was not committed at ingest's marker"
        workload.finish_long()
        results = apply.run()
        assert [r.status for r in results] == ["caught_up"] * 3, results
        applied_boundaries.append(log.checkpoint())
        assert run_once(ingest_config, log).status == "drained"
        results = apply.run()
        assert [r.status for r in results] == ["caught_up"] * 3, results
        for table in apply_config.tables:
            assert_mirror_equals_source(apply, source, table)
    assert starts_at(applied_boundaries) > 0, (
        f"no commit landed on an apply checkpoint in {attempts} attempts: concurrent WAL in "
        "the cluster kept breaking the adjacency. That's a test precondition, not an apply bug"
    )
    # The interleaved sessions produce adjacent commit records.
    assert adjacent_commits(log) > 0
    schema = source.schema

    # Unsupported type change: plain holds after its earlier transaction applies.
    source.execute("INSERT INTO plain (id,qty,ok) VALUES (-1,1,true)")
    source.execute("ALTER TABLE plain ALTER COLUMN ok TYPE int USING ok::int")
    source.execute("INSERT INTO plain (id,qty,ok) VALUES (-2,2,7)")
    workload.plain_score = False
    workload.transactions(0)
    assert run_once(ingest_config, log).status == "drained"
    results = {r.table: r for r in apply.run()}
    assert results[f"{schema}.plain"].status == "needs_action"
    relation_id, _ = apply.registered(f"{schema}.plain")
    target = apply.state(relation_id)["target"]
    ids = {row[0] for row in apply.con.execute(f"SELECT id FROM {target}").fetchall()}
    # Format 2 holds before the WHOLE unit holding the type change: row -1 is applied
    # only when its commit closed an earlier unit than the blocking commit's.
    unit_of = {
        key["values"][0]: start
        for start, key in frame_rows(
            log,
            "unit_start_lsn,new_key",
            f"record_kind=1 AND relation_id={relation_id} AND new_key IS NOT NULL",
        )
    }
    held_at = apply.con.execute(
        f"SELECT blocked_unit_start_lsn,applied_through_lsn FROM {apply.mirror}._cdc.apply_state "
        "WHERE relation_id=?",
        [relation_id],
    ).fetchone()
    assert held_at[0] == held_at[1] == unit_of["-2"]
    assert -2 not in ids and ((-1 in ids) == (unit_of["-1"] < unit_of["-2"]))
    for table in ("items", "typed"):
        assert results[f"{schema}.{table}"].status == "caught_up"
        assert_mirror_equals_source(apply, source, f"{schema}.{table}")

    # TRUNCATE is never applied as row deletes; the table needs a new generation.
    source.execute("TRUNCATE items")
    assert run_once(ingest_config, log).status == "drained"
    results = {r.table: r for r in apply.run()}
    assert results[f"{schema}.items"].status == "needs_resync"
    assert results[f"{schema}.typed"].status == "caught_up"


def float_bits(value):
    return None if value is None else struct.pack("!d", value)


@pytest.mark.postgres
def test_type_fidelity_against_native_values_and_poison_cut(stores, pg_phase2):
    """Independent of apply's casts: mirror values are compared with psycopg2's
    native Python values (or PostgreSQL canonical text where Python cannot
    represent the value), with a non-UTC walsender TimeZone."""
    source, base_ingest, base_apply = pg_phase2
    schema = source.schema
    table = f"{schema}.edge"
    source.execute(
        "CREATE TABLE edge (id int PRIMARY KEY, f8 double precision, f4 real, i8 bigint, "
        "i2 smallint, n numeric(12,3), nu numeric, ts timestamp, tz timestamptz, d date, "
        "t time, bin bytea, j jsonb, js json, b boolean, u uuid)"
    )
    source.execute("ALTER TABLE edge REPLICA IDENTITY FULL")
    dsn = base_ingest.dsn + " options='-c TimeZone=Asia/Kathmandu'"
    ingest_config = replace(
        base_ingest,
        source_id="fidelity",
        slot="pf_" + schema,
        publication="pf_" + schema,
        tables=(table,),
        heartbeat=schema + ".pulse_f",
        dsn=dsn,
    )
    # Output formats that apply cannot parse exactly fail preflight on both sessions.
    for option, match in (
        ("-c extra_float_digits=0", "extra_float_digits"),
        ("-c DateStyle=SQL,DMY", "DateStyle"),
        ("-c bytea_output=escape", "bytea_output"),
    ):
        # Ingest pins the text-output settings on every connection, so a role or DSN option
        # can no longer change them: preflight passes the format check and stops at the
        # missing objects instead (the option was overridden, never trusted).
        with pytest.raises(PreflightError) as refused:
            check(replace(ingest_config, dsn=base_ingest.dsn + f" options='{option}'"))
        assert match not in str(refused.value) and "missing" in str(refused.value), option
    try:
        log, info = bootstrap_log(stores[0].con, ingest_config, create_objects=True)
        assert [t["empty"] for t in info.tables if t["name"] == "edge"] == [True]
        apply = ApplyRun(stores[0].con, replace(base_apply, source_id="fidelity", tables=(table,)))
        apply.initialize()
        apply.start_from_log(table)
        rows = [
            (
                1,
                -0.0,
                -0.0,
                2**63 - 1,
                -32768,
                Decimal("999999999.999"),
                "-123456789012345678901234567890.000000000001",
                datetime(2024, 2, 29, 23, 59, 59, 999999),
                datetime(2024, 6, 1, 12, 0, 0, 1, tzinfo=timezone.utc),
                "infinity",
                "24:00:00",
                b"\x00\x01\xff\\",
                Json({"s": 'é\t"\\ \U0001f600', "n": 1.5e300}),
                '{"a" : [1, 2]}',
                True,
                "00000000-0000-0000-0000-000000000001",
            ),
            (
                2,
                float("nan"),
                float("inf"),
                -(2**63),
                32767,
                Decimal("-0.001"),
                "NaN",
                datetime(1, 1, 1),
                datetime(1970, 1, 1, tzinfo=timezone.utc),
                "0001-01-01",
                "00:00:00",
                b"",
                Json([]),
                "null",
                False,
                "ffffffff-ffff-ffff-ffff-ffffffffffff",
            ),
            (
                3,
                1e-308,
                3.4028235e38,
                0,
                0,
                Decimal("0.000"),
                "1e-1000",
                datetime(9999, 12, 31, 23, 59, 59, 999999),
                datetime(2024, 3, 10, 2, 30, tzinfo=timezone(timedelta(hours=-8))),
                "2024-02-29",
                "12:34:56.789012",
                bytes(range(256)),
                Json({"k": None}),
                "[]",
                None,
                None,
            ),
            (
                4,
                5e-324,
                -1.17549435e-38,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            ),
            (
                5,
                float("-inf"),
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            ),
        ]
        source.execute("BEGIN")
        for row in rows:
            source.execute(f"INSERT INTO edge VALUES ({','.join(['%s'] * 16)})", list(row))
        source.execute("COMMIT")
        assert run_once(ingest_config, log).status == "drained"
        payloads = " ".join(str(new) for (new,) in frame_rows(log, "new", "record_kind=1"))
        # Ingest pins TimeZone=UTC on the replication session (the format-2 binding): the
        # walsender renders UTC whatever the role's or the session's own setting says.
        assert "+05:45" not in payloads and "+00" in payloads
        [result] = apply.run()
        assert result.status == "caught_up"
        relation_id, _ = apply.registered(table)
        target = apply.state(relation_id)["target"]
        mirror = apply.con.execute(
            "SELECT id,f8,f4,i8,i2,n,nu,ts,epoch_us(tz),d::VARCHAR,t::VARCHAR,bin,j::VARCHAR,"
            f"js::VARCHAR,b,u FROM {target} ORDER BY id"
        ).fetchall()
        native = source.execute(
            "SELECT id,f8,f4,i8,i2,n,nu::text,ts,(extract(epoch FROM tz)*1000000)::bigint,"
            "d::text,t::text,bin,j,js::text,b,u::text FROM edge ORDER BY id"
        )
        assert len(mirror) == len(native) == len(rows)
        for got, want in zip(mirror, native):
            got, want = list(got), list(want)
            assert float_bits(got[1]) == float_bits(want[1]), (got[0], "f8")
            assert float_bits(got[2]) == float_bits(want[2]), (got[0], "f4")
            want[11] = None if want[11] is None else bytes(want[11])
            got[12] = None if got[12] is None else json.loads(got[12])
            assert got[3:] == want[3:], got[0]
        # Poison: NaN fits numeric(12,3) in PostgreSQL but not DECIMAL(12,3).
        for statement in (
            "INSERT INTO edge (id,n) VALUES (10,1)",
            "INSERT INTO edge (id,n) VALUES (11,'NaN')",
            "INSERT INTO edge (id,n) VALUES (12,2)",
        ):
            source.execute(statement)
        assert run_once(ingest_config, log).status == "drained"
        [result] = apply.run()
        assert result.status == "needs_action" and "ConversionException" in result.detail
        blocked, unit, applied = apply.con.execute(
            "SELECT blocked_commit_lsn,blocked_unit_start_lsn,applied_through_lsn "
            f"FROM {apply.mirror}._cdc.apply_state WHERE relation_id=?",
            [relation_id],
        ).fetchone()
        commit_of = {
            key["values"][0]: (commit, start)
            for commit, start, key in frame_rows(
                log,
                "commit_lsn,unit_start_lsn,new_key",
                f"record_kind=1 AND relation_id={relation_id} AND new_key IS NOT NULL",
            )
        }
        nan_commit, nan_unit = commit_of["11"]
        # The hold is before the poison's WHOLE unit; it names that unit's first commit for
        # this table (Apply's whole-unit bisect can't split a unit; the commit is a
        # diagnosis). Row 10 is applied only when its commit closed an earlier unit.
        first_in_unit = min(c for c, start in commit_of.values() if start == nan_unit)
        assert (blocked, unit, applied) == (first_in_unit, nan_unit, nan_unit)
        assert first_in_unit <= nan_commit
        ids = [r[0] for r in apply.con.execute(f"SELECT id FROM {target} ORDER BY id").fetchall()]
        ten_applied = commit_of["10"][1] < nan_unit
        assert ids == [1, 2, 3, 4, 5] + ([10] if ten_applied else [])
    finally:
        conftest.wait_slot_released(source.con, ingest_config.slot)
        query(
            source.con,
            "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name=%s",
            [ingest_config.slot],
        )
        query(
            source.con,
            sql.SQL("DROP PUBLICATION IF EXISTS {}").format(
                sql.Identifier(ingest_config.publication)
            ),
        )


def test_reverted_or_stale_key_observation_stops_apply_at_the_cut(stores, tmp_path):
    """A key change is a new version even if a later observation shows the old key
    again (A, B, A, or a stale view of A after B was recorded): apply on an A-keyed
    mirror must stop at the first cut and hold."""
    h = Harness(stores, tmp_path)
    w = h.wire
    rel = relation_message(1, "items", ITEMS)
    h.ingest(w.txn(rel, w.insert(1, ("1", "a", "t", "1.00"))))
    assert h.run("items").status == "caught_up"
    cut = h.log.checkpoint()
    # Under key B = (value), two rows may share id 2.
    h.log.acquire("test")
    try:
        h.log.record_keys({1: ["value"]}, cut, cut + 1)
    finally:
        h.log.release()
    messages = w.txn(w.insert(1, ("2", "b", "t", "2.00")), w.insert(1, ("2", "c", "t", "2.00")))
    items = []
    for message in messages:
        items.extend(h.spool.feed(message))
    h.log.acquire("test")
    try:
        h.log.append(items)
        # A stale (or reverted) observation of key A after the change to B.
        h.log.record_keys({1: ["id"]}, h.log.checkpoint(), 0)
    finally:
        h.log.release()
    assert h.log.execute(
        "SELECT key_version,catalog_key FROM {s}tables WHERE relation_id=1"
    ).fetchone() == (2, '["id"]')
    result = h.run("items")
    assert result.status == "needs_resync" and "key changed" in result.detail
    assert h.state("items")[1] == cut
    assert h.rows("items", "id,value") == [(1, "a")]
    # A log-only start is refused once the key has changed.
    h.store.con.execute(f"DELETE FROM {h.apply.mirror}._cdc.apply_state WHERE relation_id=1")
    with pytest.raises(ApplyError, match="key changed since capture"):
        h.apply.start_from_log("public.items")
