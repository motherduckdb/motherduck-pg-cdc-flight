"""Phase 3 compaction core: dropping sealed days prunes the ledger rows nothing can
read again, in the drop's transaction, and the compaction run stops safely.

Local DuckDB and MotherDuck (the `stores` fixture), on the Phase 2 harness.
"""

import re
from dataclasses import replace
from pathlib import Path

import conftest
import duckdb
import pytest
from audit_points import audited
from test_phase2 import ITEMS, SIMPLE, TABLES, Harness, relation_message
from test_retention import drop, ingest, status

from cdc_flight.compact import CompactConfig, run_compaction
from cdc_flight.retention import FloorAboveCheckpoint, Retention, table_owner
from cdc_flight.runtime import open_log

SRC = Path(__file__).resolve().parents[1] / "src" / "cdc_flight"


def apply_all(h):
    for name, _ in TABLES.values():
        assert h.run(name).status == "caught_up"


def ledger(h):
    """The format-2 ledger: (chunk rows, unit rows)."""
    return h.log.execute(
        "SELECT (SELECT count(*) FROM {s}chunks_v2),(SELECT count(*) FROM {s}units_v2)"
    ).fetchone()


def batch_days(h):
    return sorted(
        d for (d,) in h.log.execute("SELECT DISTINCT partition_day FROM {s}chunks_v2").fetchall()
    )


def drop_and_prune(retention, day):
    """One day's drop (guarded) and then the prune (lease only), as a run does them."""
    token = retention.acquire_compaction("test")
    try:
        dropped = retention.drop_partition(day, token)
        return dropped, retention.prune(token)
    finally:
        retention.release_compaction(token)


def config(h, **options):
    # The harness's mirror is attached under its own name: expiry never attaches.
    return CompactConfig(
        h.config.source_id,
        database=h.config.database,
        mirror_database=h.config.mirror_database,
        **options,
    )


def days_log(h, days):
    """One transaction per day on `items` (the first carries the Relation)."""
    w = h.wire
    rel = relation_message(1, "items", ITEMS)
    for i, day in enumerate(days):
        first = (rel,) if i == 0 else ()
        ingest(h, day, w.txn(*first, w.insert(1, (str(i), "v", "t", "1.00"))))
    apply_all(h)


@pytest.mark.md_gate
def test_compaction_drops_days_and_keeps_the_ledger_bounded(stores, tmp_path):
    h = Harness(stores, tmp_path)
    count = conftest.md_size(stores, local=5, motherduck=3)
    days = [f"2026010{i}" for i in range(1, count + 1)]
    assert len(days) >= 3  # at least two sealed days and one active day
    days_log(h, days)
    assert ledger(h) == (count, count)
    before = audited(h)
    result = run_compaction(config(h), h.store.con)
    audited(h, before)  # local only: the MotherDuck placement is test_retention's
    # Every sealed day below every pin goes; the active day stays.
    assert result.status == "done" and result.days == days[:-1]
    assert result.kept_day == days[-1] and result.kept_reason == "not_sealed"
    assert (result.chunks, result.units) == (count - 1, count - 1)
    assert ledger(h) == (1, 1) and batch_days(h) == [days[-1]]
    # The days' maxima are non-decreasing, which is why the run stops at the first kept day.
    highs = h.log.execute(
        "SELECT max(epoch),max(max_unit) FROM {s}frame_partitions_v2 "
        "GROUP BY partition_day ORDER BY partition_day"
    ).fetchall()
    assert highs == sorted(highs)
    # More days: the ledger stays bounded at the retained window.
    w = h.wire
    for day in ("20260106", "20260107"):
        ingest(h, day, w.txn(w.insert(1, (day, "v", "t", "1.00"))))
    apply_all(h)
    run_compaction(config(h), h.store.con)
    assert ledger(h) == (1, 1)
    rows = h.log.execute(
        "SELECT status,days_dropped,chunks_pruned,units_pruned,kept_reason "
        "FROM {s}compactions ORDER BY finished_at"
    ).fetchall()
    assert rows == [
        ("done", count - 1, count - 1, count - 1, "not_sealed"),
        ("done", 2, 2, 2, "not_sealed"),
    ]
    assert h.run("items").status == "caught_up"


def split(h, *parts):
    """One transaction's items, cut into `parts` (sizes; the rest is the last part)."""
    w = h.wire
    rel = relation_message(3, "third", SIMPLE)
    messages = w.txn(rel, *[w.insert(3, (str(i), "x" * 300)) for i in range(12)])
    items = [i for m in messages for i in h.spool.feed(m)]
    out, start = [], 0
    for size in parts:
        out.append(items[start : start + size])
        start += size
    out.append(items[start:])
    return out


def append(h, items, day):
    h.log.acquire("test")
    try:
        h.log.append(items, partition_day=day)
    finally:
        h.log.release()


@pytest.mark.md_gate
def test_a_transaction_across_midnight_keeps_its_manifest_until_both_days_drop(stores, tmp_path):
    h = Harness(stores, tmp_path)
    first, middle, rest = split(h, 2, 1)
    assert len(rest) >= 2  # chunks and the unit's terminal
    append(h, first, "20260101")
    append(h, middle, "20260102")  # seals day 1 while the transaction is still open
    retention = h.apply.retention
    # Its commit is above the checkpoint, so above every pin: day 1 stays.
    kept = drop(retention, "20260101")
    assert not kept and kept.reason == "pinned"
    append(h, rest, "20260102")
    w = h.wire
    ingest(h, "20260103", w.txn(w.insert(3, ("100", "z"))))
    apply_all(h)
    assert drop_and_prune(retention, "20260101")[0]
    # Day 2 still holds chunks of that unit: its units_v2 row stays.
    start = first[0].unit_start_lsn
    unit = "SELECT count(*) FROM {s}units_v2 WHERE unit_start_lsn=?"
    assert h.log.execute(unit, [start]).fetchone()[0] == 1
    assert drop_and_prune(retention, "20260102")[0]
    assert h.log.execute(unit, [start]).fetchone()[0] == 0
    assert h.run("third").status == "caught_up"


@pytest.mark.md_gate
def test_pruning_is_by_epoch_and_lsn_and_takes_chunkless_commits(stores, tmp_path):
    """A drop prunes exactly the dropped day's chunks and the units ending at or below
    the floor; later units stay. Format 2 has no chunkless commit (every member commit
    is a COMMIT frame inside a chunk: test_spool2), and a later epoch restarting at
    lower LSNs is test_retention2::test_a_new_epoch_can_reuse_a_dropped_day_without_
    resurrecting_old_rows."""
    h = Harness(stores, tmp_path)
    days_log(h, ["20260101", "20260102", "20260103"])
    floor_day_end = h.log.execute(
        "SELECT max(end_lsn) FROM {s}units_v2 u JOIN {s}chunks_v2 c USING(epoch,unit_start_lsn) "
        "WHERE c.partition_day='20260101'"
    ).fetchone()[0]
    dropped, (chunks, units) = drop_and_prune(h.apply.retention, "20260101")
    assert dropped and (chunks, units) == (1, 1)
    assert h.apply.retention.floor() == (1, floor_day_end)
    rows = h.log.execute("SELECT epoch,end_lsn FROM {s}units_v2 ORDER BY ALL").fetchall()
    assert len(rows) == 2 and all(end > floor_day_end for _, end in rows)


@pytest.mark.md_gate
def test_a_published_table_without_its_pin_stops_compaction(stores, tmp_path):
    h = Harness(stores, tmp_path)
    days_log(h, ["20260101", "20260102"])
    rel = h.apply.registered("public.third")[0]
    assert h.log.execute("SELECT count(*) FROM {s}tables WHERE pin_state='pinned'").fetchone()[
        0
    ] == len(TABLES)
    h.log.execute("DELETE FROM {s}retention_pins WHERE owner=?", [table_owner(rel)])
    result = run_compaction(config(h), h.store.con)
    assert result.status == "held" and result.held_relation == rel and result.days == []
    assert status(h, "20260101") == [("sealed",)]
    # A removed table is exempt.
    h.log.execute("UPDATE {s}tables SET held_reason='removed' WHERE relation_id=?", [rel])
    assert run_compaction(config(h), h.store.con).days == ["20260101"]


@pytest.mark.md_gate
def test_an_abandoned_first_generation_leaves_the_table_exempt(stores, tmp_path):
    """pin_state is set with the table pin (at transfer), never at registration."""
    h = Harness(stores, tmp_path)
    days_log(h, ["20260101", "20260102"])
    retention = h.apply.retention
    h.log.execute(
        "INSERT INTO {s}tables (relation_id,schema_name,table_name,state) "
        "VALUES (99,'public','late','snapshotting')"
    )
    retention.register_generation(99, "first", 1, h.log.checkpoint())
    retention.release("generation:first", "first")  # abandoned before publishing
    assert (
        h.log.execute("SELECT pin_state FROM {s}tables WHERE relation_id=99").fetchone()[0] is None
    )
    assert run_compaction(config(h), h.store.con).days == ["20260101"]


@pytest.mark.md_gate
def test_pin_state_is_set_on_every_owning_transfer_and_backfilled_on_upgrade(stores, tmp_path):
    h = Harness(stores, tmp_path)
    retention = h.apply.retention
    rel = h.apply.registered("public.items")[0]
    pin = retention.pin(table_owner(rel))

    def state():
        return h.log.execute(
            "SELECT pin_state FROM {s}tables WHERE relation_id=?", [rel]
        ).fetchone()[0]

    assert state() == "pinned"
    h.log.execute("UPDATE {s}tables SET pin_state=NULL WHERE relation_id=?", [rel])
    # An idempotent rerun of the transfer (the pin is already owned) still marks it.
    retention.register_generation(rel, pin["generation"], pin["epoch"], pin["lsn"])
    assert retention.transfer(rel, pin["generation"], pin["epoch"], pin["lsn"])
    assert state() == "pinned"
    # (An upgrade from a store without the column is a format-1 store upgrade: such a
    # store is refused, runtime.open_log / Retention2.initialize.)


@pytest.mark.md_gate
def test_the_floor_never_passes_the_checkpoint(stores, tmp_path):
    h = Harness(stores, tmp_path)
    days_log(h, ["20260101", "20260102"])
    retention = h.apply.retention
    before = ledger(h)
    # Pins above the checkpoint can't happen; force it, and the drop refuses.
    h.log.execute("UPDATE {s}source SET confirmed_lsn=1 WHERE id=1")
    with pytest.raises(FloorAboveCheckpoint):
        drop(retention, "20260101")
    assert status(h, "20260101") == [("sealed",)] and ledger(h) == before
    assert retention.floor() is None
    # A run records it as an invariant failure, distinct from a crash.
    with pytest.raises(FloorAboveCheckpoint):
        run_compaction(config(h), h.store.con)
    assert h.log.execute("SELECT status,error FROM {s}compactions").fetchall() == [
        ("invariant", "FloorAboveCheckpoint")
    ]


@pytest.mark.md_gate
def test_compaction_runs_stop_safely(stores, tmp_path):
    h = Harness(stores, tmp_path)
    days_log(h, ["20260101", "20260102", "20260103"])
    retention = h.apply.retention
    # Busy while another run holds the lease.
    token = retention.acquire_compaction("other")
    assert run_compaction(config(h), h.store.con).status == "busy"
    retention.release_compaction(token)

    # The runtime cap stops between days.
    # Time runs out once the first day is dropped.
    def clock():
        return 1000 if status(h, "20260101") == [("dropped",)] else 0

    result = run_compaction(config(h, runtime_seconds=60), h.store.con, clock=clock)
    assert result.status == "capped" and result.days == ["20260101"]
    # Out of time, the prune stops between days too; the next run finishes it.
    assert result.chunks == 0 and ledger(h)[0] == 3
    # A failure inside a day rolls that day back, keeps earlier days, releases the lease.
    days_log(h, ["20260104"])

    def boom(stage):
        raise RuntimeError("injected")

    with pytest.raises(RuntimeError, match="injected"):
        run_compaction(config(h), h.store.con, fault=boom)
    assert status(h, "20260102") == [("sealed",)]
    retention.release_compaction(retention.acquire_compaction("after"))
    statuses = [
        s
        for (s,) in h.log.execute(
            "SELECT status FROM {s}compactions ORDER BY finished_at"
        ).fetchall()
    ]
    assert statuses == ["busy", "capped", "failed"]


@pytest.mark.md_gate
def test_a_replay_above_the_floor_still_dedups(stores, tmp_path):
    """The proposal's acceptance case: after a drop and prune, an ingest crash after
    commit and before feedback replays its batch, and it is stored once."""
    h = Harness(stores, tmp_path)
    days_log(h, ["20260101"])
    w = h.wire
    items = [i for m in w.txn(w.insert(1, ("9", "v", "t", "1.00"))) for i in h.spool.feed(m)]
    append(h, items, "20260102")
    apply_all(h)
    ingest(h, "20260103", w.txn(w.insert(1, ("10", "v", "t", "1.00"))))
    apply_all(h)
    assert run_compaction(config(h), h.store.con).days == ["20260101", "20260102"]
    checkpoint, counts = h.log.checkpoint(), ledger(h)
    # The replay of the last durable batch (its items are above the floor).
    last = [i for m in w.txn(w.insert(1, ("11", "v", "t", "1.00"))) for i in h.spool.feed(m)]
    append(h, last, "20260103")
    append(h, last, "20260103")
    assert ledger(h) == (counts[0] + 1, counts[1] + 1)
    assert h.log.checkpoint() > checkpoint
    frames = h.log.execute(
        "SELECT count(*),count(DISTINCT (source_epoch,unit_start_lsn,frame_ordinal)) "
        "FROM {s}frames_v2_20260103"
    ).fetchone()
    assert frames[0] == frames[1]


@pytest.mark.md_gate
def test_a_day_boundary_append_during_a_drop(stores, tmp_path):
    """Ingest's first append of a new day creates its table and seals the previous
    day while a drop's transaction is open. Format 2: the drop's source guard wins, so
    the append aborts retryably (or the compaction records the day busy); the retried
    append and the next run then take it (test_retention2::test_drop_guard_winning_
    aborts_append_without_changing_the_ingest_lease)."""
    h = Harness(stores, tmp_path)
    days_log(h, ["20260101", "20260102"])
    w = h.wire
    late = [i for m in w.txn(w.insert(1, ("50", "v", "t", "1.00"))) for i in h.spool.feed(m)]
    other_log = open_log(h.other_store.con, h.ingest_config)
    done, appended = [], []

    def concurrent(stage):
        if stage == "after_drop" and not done:
            done.append(1)
            other_log.acquire("other")
            try:
                other_log.append(late, partition_day="20260103")
                appended.append(1)
            except duckdb.TransactionException:
                pass  # the drop's guard won: ingest retries this append
            finally:
                other_log.release()

    result = run_compaction(config(h), h.store.con, fault=concurrent)
    assert done
    assert result.status in ("done", "busy"), result
    if not appended:
        append(h, late, "20260103")  # ingest's retry
    if result.status == "busy":
        assert result.kept_reason == "conflict"
        result = run_compaction(config(h), h.store.con)
    apply_all(h)
    assert status(h, "20260101") == [("dropped",)]
    assert (
        h.log.execute("SELECT count(*) FROM {s}frames_v2_20260103 WHERE record_kind=1").fetchone()[
            0
        ]
        == 1
    )


# The writer list ---------------------------------------------------------

WRITE = re.compile(
    r"(?:INSERT INTO|UPDATE|DELETE FROM)\s+\{s\}(chunks_v2|units_v2)\b"
    r"|_insert\(\s*\"(chunks_v2|units_v2)\""
)


def ledger_writes(text):
    """Writes to the ledger tables, with SQL joined across adjacent string literals."""
    joined = re.sub(r"\"\s*\n\s*(?:f|rf|r)?\"", "", text)
    return [m.group(0) for m in WRITE.finditer(joined)]


def test_only_append_and_the_drop_write_the_ledger():
    writers = {p.name for p in SRC.glob("*.py") if ledger_writes(p.read_text())}
    assert writers == {"ingest_log2.py", "retention2.py"}
    # The check sees a write split over two literals (the mutant it must catch).
    mutant = 'con.execute(\n    "UPDATE "\n    "{s}units_v2 SET frame_count=0"\n)'
    assert ledger_writes(mutant)


def test_compaction_config_is_validated():
    with pytest.raises(ValueError):
        CompactConfig("Bad-Name")
    with pytest.raises(ValueError):
        CompactConfig("src", runtime_seconds=900)
    with pytest.raises(ValueError):
        CompactConfig("src", database="x;drop")


@pytest.mark.md_gate
def test_pin_writes_wait_only_on_the_short_drop_never_on_the_prune(stores, tmp_path):
    """The drop holds the shared guard (pin registration serializes with it); the
    prune holds only the compaction lease, so a backfill start or a publication's
    transfer during a long prune goes through."""
    h = Harness(stores, tmp_path)
    days_log(h, ["20260101", "20260102", "20260103"])
    theirs = Retention(h.other_store.con, h.config.database, h.config.source_id)
    rel = h.apply.registered("public.third")[0]
    seen = {}

    def concurrent(stage):
        if stage == "after_drop" and "drop" not in seen:
            try:
                theirs.register_generation(rel, "during_drop", 1, h.log.checkpoint())
                seen["drop"] = "ok"
            except duckdb.TransactionException:
                seen["drop"] = "conflict"
        if stage == "prune_chunks" and "prune" not in seen:
            theirs.register_generation(rel, "during_prune", 1, h.log.checkpoint())
            assert theirs.transfer(rel, "during_prune", 1, h.log.checkpoint())
            seen["prune"] = "ok"

    result = run_compaction(config(h), h.store.con, fault=concurrent)
    assert result.days == ["20260101", "20260102"] and result.chunks == 2
    assert seen == {"drop": "conflict", "prune": "ok"}
    assert theirs.pin(table_owner(rel))["generation"] == "during_prune"


@pytest.mark.md_gate
def test_a_crash_after_the_drop_leaves_rows_the_next_run_prunes(stores, tmp_path):
    h = Harness(stores, tmp_path)
    days_log(h, ["20260101", "20260102", "20260103"])

    def crash(stage):
        if stage == "prune_chunks":
            raise RuntimeError("crash in the prune")

    with pytest.raises(RuntimeError, match="crash in the prune"):
        run_compaction(config(h), h.store.con, fault=crash)
    # Both days dropped (extra ledger rows only), and the floor is committed.
    assert status(h, "20260101") == status(h, "20260102") == [("dropped",)]
    assert ledger(h) == (3, 3)
    result = run_compaction(config(h), h.store.con)
    assert result.days == [] and (result.chunks, result.units) == (2, 2)
    assert ledger(h) == (1, 1)


class Statements:
    """A connection that records every statement it runs."""

    def __init__(self, con):
        self.con, self.seen = con, []

    def execute(self, statement, parameters=None):
        self.seen.append(statement)
        return self.con.execute(statement, parameters or [])

    def __getattr__(self, name):
        return getattr(self.con, name)


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_a_local_store_never_attaches_motherduck(stores, tmp_path, monkeypatch):
    """Expiry needs the mirror: on a local store it's attached by its owner, never
    fetched from MotherDuck (which, without a token, would open a browser login)."""
    monkeypatch.delenv("motherduck_token", raising=False)
    monkeypatch.setenv("MOTHERDUCK_TOKEN", "set-but-must-not-be-used")
    h = Harness(stores, tmp_path)
    days_log(h, ["20260101", "20260102"])
    con = Statements(h.store.con)
    with pytest.raises(RuntimeError, match="not attached to this local store"):
        run_compaction(replace(config(h), mirror_database="elsewhere_mirror"), con)
    assert not [s for s in con.seen if "md:" in s]
    # With the harness's mirror (attached), the run judges holders and attaches nothing.
    con = Statements(h.store.con)
    assert run_compaction(config(h), con).status == "done"
    assert not [s for s in con.seen if "md:" in s]
