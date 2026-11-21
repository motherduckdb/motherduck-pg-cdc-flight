"""Retention pins and floor (DESIGN Protocol 4) on local DuckDB and MotherDuck."""

import duckdb
import pytest
from audit_points import audited
from test_phase2 import ITEMS, SIMPLE, TABLES, Harness, relation_message

from cdc_flight.ingest_common import SourceMismatch
from cdc_flight.retention import (
    HistoryDropped,
    Retention,
    generation_owner,
    table_owner,
)


def ingest(h, day, *transactions):
    """Append these transactions on `day` (one unit each: the Harness's max_commits=1);
    returns the checkpoint, the last complete unit END."""
    h.ingest(*transactions, day=day)
    return h.log.checkpoint()


def relation(h, name):
    return h.apply.registered("public." + name)[0]


def table_pin(h, name):
    pin = h.apply.retention.pin(table_owner(relation(h, name)))
    return None if pin is None else (pin["epoch"], pin["lsn"])


def generation_pins(h):
    return sorted(o for o in h.apply.retention.pins() if o.startswith("generation:"))


def drop(retention, day, fault=None):
    token = retention.acquire_compaction("test")
    try:
        return retention.drop_partition(day, token, fault=fault)
    finally:
        retention.release_compaction(token)


def status(h, day):
    return h.log.execute(
        "SELECT DISTINCT status FROM {s}frame_partitions_v2 WHERE partition_day=?", [day]
    ).fetchall()


def unit_ends(h, count, day="20260101"):
    """`count` one-insert transactions on `items`, one unit each; their unit ENDs, the
    only positions (besides an epoch start) a pin may name."""
    w, ends = h.wire, []
    for i in range(1, count + 1):
        first = [relation_message(1, "items", ITEMS)] if i == 1 else []
        ends.append(ingest(h, day, w.txn(*first, w.insert(1, (str(i), "a", "t", "1")))))
    return ends


def other_retention(h):
    return Retention(
        h.other_store.con, h.config.database, h.config.source_id, h.config.lease_seconds
    )


@pytest.mark.md_gate
def test_pins_advance_after_commit_quiet_tables_and_floor(stores, tmp_path):
    h = Harness(stores, tmp_path)
    w = h.wire
    # start_from_log registers a generation pin, then transfers it to the table.
    assert generation_pins(h) == []
    assert all(table_pin(h, name) == (1, 0) for name, _ in TABLES.values())

    rel = relation_message(1, "items", ITEMS)
    first = ingest(h, "20260101", w.txn(rel, w.insert(1, ("1", "a", "t", "1.00"))))
    second = ingest(h, "20260102", w.txn(w.insert(1, ("2", "b", "t", "2.00"))))
    assert status(h, "20260101") == [("sealed",)]
    # S5's one MotherDuck placement (within the 29-case budget): a pair around the
    # first drop; the second, before any prune, also checks the floor's shape (A3b).
    before = audited(h, remote=True)
    retention = h.apply.retention
    assert not drop(retention, "20260101")

    # A crash after the mirror commit, before the pin advance: extra history.
    def lost(stage):
        if stage == "after_commit":
            raise RuntimeError("lost response")

    with pytest.raises(RuntimeError, match="lost response"):
        h.run("items", fault=lost)
    assert h.state("items")[1] == second
    assert table_pin(h, "items") == (1, 0)
    assert not drop(retention, "20260101")
    # The rerun applies nothing and advances the pin.
    result = h.run("items")
    assert result.status == "caught_up" and result.events == 0
    assert table_pin(h, "items") == (1, second)

    # The other tables are quiet, but still pin the log until they run.
    assert not drop(retention, "20260101")
    for name in ("other", "third", "fourth"):
        assert h.run(name).status == "caught_up"
        assert table_pin(h, name) == (1, second)
    # The floor is strict: a pin exactly at day one's unit END still holds that day.
    retention.register_generation(1, "at_end", 1, first)
    assert not drop(retention, "20260101")
    retention.release(generation_owner("at_end"))
    assert drop(retention, "20260101")
    assert status(h, "20260101") == [("dropped",)]
    assert retention.floor() == (1, first)
    audited(h, before, shape=True, remote=True)
    # The active day is never dropped.
    assert not drop(retention, "20260102")

    # A later day seals the active one; a dropped day stays dropped, and dropping
    # it again is refused rather than failing.
    ingest(h, "20260103", w.txn(w.insert(1, ("3", "c", "t", "3.00"))))
    assert status(h, "20260101") == [("dropped",)]
    assert status(h, "20260102") == [("sealed",)]
    assert not drop(retention, "20260101")

    # A pin at or below the floor would claim dropped units; above it, pins sit at a
    # complete unit END (or an epoch start), never inside a unit.
    with pytest.raises(HistoryDropped):
        retention.register_generation(1, "at_floor", 1, first)
    with pytest.raises(HistoryDropped):
        retention.register_generation(1, "below", 1, first - 1)
    with pytest.raises(SourceMismatch, match="complete unit end"):
        retention.register_generation(1, "inside", 1, first + 1)
    retention.register_generation(1, "above", 1, second)
    assert retention.pin(generation_owner("above"))["lsn"] == second

    # Apply still works across the dropped day: its reads start above the floor.
    assert h.run("items").status == "caught_up"
    assert h.rows("items", "id") == [(1,), (2,), (3,)]


@pytest.mark.md_gate
def test_drop_fails_closed_without_pins_and_crash_rolls_back(stores, tmp_path):
    h = Harness(stores, tmp_path)
    w = h.wire
    ingest(h, "20260101", w.txn(relation_message(3, "third", SIMPLE), w.insert(3, ("1", "a"))))
    ingest(h, "20260102", w.txn(w.insert(3, ("2", "b"))))
    for name, _ in TABLES.values():
        assert h.run(name).status == "caught_up"
    retention = h.apply.retention

    def crash(stage):
        raise RuntimeError(stage)

    with pytest.raises(RuntimeError, match="after_drop"):
        drop(retention, "20260101", fault=crash)
    assert status(h, "20260101") == [("sealed",)] and retention.floor() is None
    # Metadata alone would miss a committed DROP followed by a lost response.
    # Another connection must still read the physical log table after rollback.
    assert h.other_store.con.execute(
        f"SELECT count(*) FROM {h.log.prefix}frames_v2_20260101 WHERE record_kind=1"
    ).fetchone() == (1,)  # its one row change (EVENT), beside the COMMIT frame
    # With no pins at all, nothing is dropped.
    for name, _ in TABLES.values():
        retention.release(table_owner(relation(h, name)))
    assert retention.pins() == {}
    assert not drop(retention, "20260101")


@pytest.mark.md_gate
def test_missing_pin_fails_closed_and_interrupted_transfer_completes(stores, tmp_path, monkeypatch):
    h = Harness(stores, tmp_path)
    w = h.wire
    ingest(
        h,
        "20260101",
        w.txn(relation_message(1, "items", ITEMS), w.insert(1, ("1", "a", "t", "1.00"))),
    )

    # A missing table pin: apply reads nothing and holds the table.
    h.apply.retention.release(table_owner(relation(h, "items")))
    result = h.run("items")
    assert result.status == "needs_resync" and "retention pin" in result.detail
    assert h.state("items")[:2] == ("needs_resync", 0)

    # A log-only start whose transfer failed after the mirror commit: only the
    # generation pin exists, and apply completes the transfer before reading.
    third = relation(h, "third")
    h.store.con.execute(
        f"DELETE FROM {h.apply.mirror}._cdc.apply_state WHERE relation_id={third}; "
        f"DELETE FROM {h.apply.mirror}._cdc.generations WHERE relation_id={third}"
    )
    h.apply.retention.release(table_owner(third))

    def broken(*args, **kwargs):
        raise RuntimeError("transfer lost")

    with monkeypatch.context() as patch:
        patch.setattr(h.apply.retention, "transfer", broken)
        generation = h.apply.start_from_log("public.third")
    assert table_pin(h, "third") is None
    assert generation_pins(h) == [generation_owner(generation)]
    ingest(h, "20260102", w.txn(relation_message(3, "third", SIMPLE), w.insert(3, ("1", "x"))))
    assert h.run("third").status == "caught_up"
    pin = h.apply.retention.pin(table_owner(third))
    assert pin["generation"] == generation and pin["lsn"] == h.log.checkpoint()
    assert generation_pins(h) == []

    # Completion is refused (fail closed) once the floor has passed the checkpoint.
    other = relation(h, "other")
    h.store.con.execute(
        f"DELETE FROM {h.apply.mirror}._cdc.apply_state WHERE relation_id={other}; "
        f"DELETE FROM {h.apply.mirror}._cdc.generations WHERE relation_id={other}"
    )
    h.apply.retention.release(table_owner(other))
    with monkeypatch.context() as patch:
        patch.setattr(h.apply.retention, "transfer", broken)
        h.apply.start_from_log("public.other")
    h.log.execute("UPDATE {s}retention SET floor_epoch=1,floor_lsn=0")
    result = h.run("other")
    assert result.status == "needs_resync" and "floor" in result.detail
    assert table_pin(h, "other") is None


def test_stale_generation_cannot_raise_pin_and_transfer_releases_leaked_pins(stores, tmp_path):
    h = Harness(stores, tmp_path)
    retention = h.apply.retention
    items = relation(h, "items")
    owner = table_owner(items)
    ends = unit_ends(h, 5)
    current = retention.pin(owner)
    # A stale worker of another generation updates nothing; another epoch, which has no
    # recorded start, is refused outright.
    assert not retention.advance(owner, "replaced_generation", 1, ends[4])
    with pytest.raises(SourceMismatch, match="epoch start"):
        retention.advance(owner, current["generation"], 2, ends[4])
    assert retention.pin(owner) == current
    assert retention.advance(owner, current["generation"], 1, ends[2])
    assert retention.advance(owner, current["generation"], 1, ends[1])  # never lowers
    assert retention.pin(owner)["lsn"] == ends[2]

    # The table pin still names an earlier generation while the current one's pin
    # exists (an interrupted transfer after a resync): apply completes the transfer
    # rather than reading under the old generation's pin.
    retention.register_generation(items, current["generation"], 1, 0)
    h.log.execute(
        "UPDATE {s}retention_pins SET generation_id='previous',pinned_lsn=0 WHERE owner=?",
        [owner],
    )
    assert h.run("items").status == "caught_up"
    assert retention.pin(owner)["generation"] == current["generation"]
    assert generation_pins(h) == []

    # A replaced generation's pin left by an interrupted transfer, an active
    # backfill's pin, and a just-registered start (mirror commit in flight).
    for generation in ("leaked", "active", "young"):
        retention.register_generation(items, generation, 1, ends[0])
    h.log.execute(
        "UPDATE {s}retention_pins SET created_at=created_at-INTERVAL 1 DAY "
        "WHERE generation_id IN ('leaked','active')"
    )
    retention.register_generation(items, "next", 1, ends[1])
    retention.transfer(items, "next", 1, ends[2], keep="active")
    assert generation_pins(h) == [generation_owner("active"), generation_owner("young")]
    pin = retention.pin(owner)
    assert (pin["generation"], pin["lsn"]) == ("next", ends[2])
    # Idempotent, and never lowers a pin it already transferred.
    retention.advance(owner, "next", 1, ends[4])
    retention.transfer(items, "next", 1, ends[2], keep="active")
    assert retention.pin(owner)["lsn"] == ends[4]


def test_post_read_floor_check_holds_the_table(stores, tmp_path, monkeypatch):
    """Format 2 checks the floor inside the page read's own statement (units()), so a
    drop that commits before the read holds the table and applies nothing. The
    in-statement check itself: test_retention2::test_cut_validation_includes_floor_and_
    epoch_start_in_one_statement."""
    h = Harness(stores, tmp_path)
    w = h.wire
    ingest(
        h,
        "20260101",
        w.txn(relation_message(1, "items", ITEMS), w.insert(1, ("1", "a", "t", "1.00"))),
    )
    read = h.apply.reader.units

    def raced(*args, **kwargs):
        # A drop committed just before this read (simulated by its floor raise).
        h.log.execute("UPDATE {s}retention SET floor_epoch=1,floor_lsn=0")
        return read(*args, **kwargs)

    monkeypatch.setattr(h.apply.reader, "units", raced)
    result = h.run("items")
    assert result.status == "needs_resync" and "dropped" in result.detail
    assert h.state("items")[1] == 0


def test_pin_writes_of_two_consumers_and_compaction_serialize(stores, tmp_path):
    h = Harness(stores, tmp_path)
    ends = unit_ends(h, 3)
    mine, theirs = h.apply.retention, other_retention(h)
    items, other = relation(h, "items"), relation(h, "other")
    generation = mine.pin(table_owner(other))["generation"]
    token = theirs.acquire_compaction("test")
    # A registration holds the shared guard (uncommitted) on one connection.
    con = h.store.con
    con.execute("BEGIN")
    try:
        mine.execute("UPDATE {s}retention SET guard=uuid()::VARCHAR WHERE id=1")
        mine.execute(
            "INSERT INTO {s}retention_pins (owner,kind,relation_id,generation_id,source_epoch,"
            "pinned_lsn,created_at,updated_at) VALUES ('generation:g1','generation',?, 'g1',1,0,"
            "current_timestamp,current_timestamp)",
            [items],
        )
        # The other table's unguarded advance commits concurrently.
        assert theirs.advance(table_owner(other), generation, 1, ends[0])
        # A drop, or another guarded pin write, conflicts and is retried, then
        # reported; it never commits past an in-flight registration.
        with pytest.raises(duckdb.TransactionException):
            theirs.drop_partition("20260101", token)
        with pytest.raises(duckdb.TransactionException):
            theirs.transfer(other, "g2", 1, ends[1])
    finally:
        con.execute("COMMIT")
    theirs.release_compaction(token)
    assert mine.pin(table_owner(other))["lsn"] == ends[0]
    assert mine.pin(generation_owner("g1")) is not None
    # Once the registration commits, the guarded writes go through.
    theirs.register_generation(other, "g2", 1, ends[1])
    assert theirs.transfer(other, "g2", 1, ends[1])
    assert mine.pin(table_owner(other))["generation"] == "g2"


def test_ambiguous_log_only_start_keeps_its_pin(stores, tmp_path, monkeypatch):
    """A lost response after the mirror commit must not release the pin the
    committed generation needs; apply completes the transfer."""
    h = Harness(stores, tmp_path)
    w = h.wire
    third = relation(h, "third")
    h.store.con.execute(
        f"DELETE FROM {h.apply.mirror}._cdc.apply_state WHERE relation_id={third}; "
        f"DELETE FROM {h.apply.mirror}._cdc.generations WHERE relation_id={third}"
    )
    h.apply.retention.release(table_owner(third))
    real = h.apply._start_from_log

    def lost(*args):
        real(*args)
        raise ConnectionError("commit response lost")

    with monkeypatch.context() as patch:
        patch.setattr(h.apply, "_start_from_log", lost)
        with pytest.raises(ConnectionError):
            h.apply.start_from_log("public.third")
    assert len(generation_pins(h)) == 1
    ingest(h, "20260101", w.txn(relation_message(3, "third", SIMPLE), w.insert(3, ("1", "x"))))
    assert h.run("third").status == "caught_up"
    assert generation_pins(h) == [] and table_pin(h, "third") == (1, h.log.checkpoint())
    # A refusal inside the transaction rolled back, so its pin is released.
    with pytest.raises(Exception, match="already has a generation"):
        h.apply.start_from_log("public.third")
    assert generation_pins(h) == []


def test_delayed_transfer_of_a_replaced_generation_changes_nothing(stores, tmp_path):
    h = Harness(stores, tmp_path)
    w = h.wire
    items = relation(h, "items")
    owner = table_owner(items)
    before = h.apply.retention.pin(owner)
    # Its generation pin is gone (released by the transfer that replaced it).
    assert not h.apply.retention.transfer(items, "replaced", 1, 0)
    assert h.apply.retention.pin(owner) == before
    ingest(
        h, "20260101", w.txn(relation_message(1, "items", ITEMS), w.insert(1, ("1", "a", "t", "1")))
    )
    assert h.run("items").status == "caught_up"


def test_initialize_is_catalog_checked_and_idempotent(stores, tmp_path):
    h = Harness(stores, tmp_path)
    retention = h.apply.retention
    # Already initialized by the harness: nothing left to create.
    assert retention.statements(retention.catalog()) == []
    retention.initialize()
    assert retention.floor() is None
    # A store without retention tables gets both, plus the guard row, in one go.
    h.log.execute("DROP TABLE {s}retention_pins; DROP TABLE {s}retention")
    assert len(retention.statements(retention.catalog())) == 3
    retention.initialize()
    assert retention.statements(retention.catalog()) == []
    assert retention.floor() is None and retention.pins() == {}


def test_initialize_retries_once_after_a_concurrent_initializer():
    """A conflicting concurrent initializer: the first transaction fails with a
    write-write conflict, the retry re-reads the catalog and finishes. Only one retry,
    and only for a conflict.

    The conflict is injected. Locally DuckDB treats another connection's uncommitted
    CREATE TABLE as existing for IF NOT EXISTS, and Retention never creates the schema
    (the race the ingest-log test stages), so a real race can't be staged here.
    """
    con = duckdb.connect()
    con.execute('CREATE SCHEMA "memory"."rt"')
    # A format-2 store's protocol row: Retention2 initializes only there.
    con.execute(
        'CREATE TABLE "memory"."rt".protocol_v2 (id INTEGER, format_version INTEGER, min_reader INTEGER)'
    )
    con.execute('INSERT INTO "memory"."rt".protocol_v2 VALUES (1, 2, 2)')

    def make(failures):
        retention = Retention(con, "memory", "rt")
        real_execute, reads = retention.execute, []
        real_catalog = retention.catalog

        def execute(statement, parameters=None):
            if failures and statement.startswith("CREATE TABLE"):
                raise failures.pop(0)
            return real_execute(statement, parameters)

        def catalog():
            reads.append(1)
            return real_catalog()

        retention.execute, retention.catalog = execute, catalog
        return retention, reads

    conflict = duckdb.TransactionException("Catalog write-write conflict on create")
    with pytest.raises(duckdb.TransactionException, match="conflict"):
        make([conflict, conflict])[0].initialize()  # a second conflict is reported
    with pytest.raises(duckdb.TransactionException, match="other"):
        make([duckdb.TransactionException("other")])[0].initialize()  # not a conflict
    retention, reads = make([conflict])
    retention.initialize()
    # One catalog read for the format-2 check, then the first attempt and its one retry.
    assert len(reads) == 3
    assert retention.statements(retention.catalog()) == []
    assert con.execute('SELECT count(*) FROM "memory"."rt".retention').fetchone() == (1,)
