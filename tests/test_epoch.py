"""Source epoch transitions: the metadata side (IngestLog.begin_epoch) and consumers.

The PostgreSQL side (epoch.begin_epoch: slot recreation, skip) is in
tests/test_epoch_pg.py.
"""

from dataclasses import replace

import pytest
from audit_points import audited
from test_phase2 import ITEMS, TABLES, Harness, Wire, relation_message

from cdc_flight.apply import ApplyError
from cdc_flight.config import ApplyConfig, IngestConfig
from cdc_flight.ingest_log import FenceLost, IngestLog, SourceMismatch
from cdc_flight.retention import HistoryDropped, generation_owner
from cdc_flight.spool2 import Spool


def feed(spool, *transactions):
    return [item for messages in transactions for m in messages for item in spool.feed(m)]


def append(h, spool, day, *transactions):
    items = feed(spool, *transactions)
    h.log.acquire("test")
    try:
        h.log.append(items, partition_day=day)
        h.log.record_keys(
            {oid: list(keys) for oid, (_, keys) in TABLES.items()}, h.log.checkpoint(), 0
        )
    finally:
        h.log.release()
    return h.log.checkpoint()


def spool_for(h, epoch):
    return Spool(
        replace(h.ingest_config, epoch=epoch),
        {oid: tuple(k) for oid, (_, k) in TABLES.items()},
        999,
        "m",
        binding=h.ingest_config.text_binding,
        unit_config=h.ingest_config.unit_config,
        start_lsn=h.log.checkpoint(),
        clock=lambda: 0,
    )


def transition(h, epoch, start, reason="slot_lost", slot="slot_e2", **details):
    h.log.acquire("transition")
    try:
        return h.log.begin_epoch(epoch, start, reason, slot, details=details)
    finally:
        h.log.release()


def one(h, sql, params=None):
    return h.log.execute(sql, params).fetchone()


def test_begin_epoch_records_seals_versions_and_is_idempotent(stores, tmp_path):
    h = Harness(stores, tmp_path)
    w = h.wire
    rel = relation_message(1, "items", ITEMS)
    old = append(h, h.spool, "20260101", w.txn(rel, w.insert(1, ("1", "a", "t", "1.00"))))
    assert h.run("items").status == "caught_up"

    # The new slot's history starts below the old checkpoint.
    start = old - 500
    assert transition(h, 2, start, timeline_id=1, abandoned_from=old)
    assert one(h, "SELECT epoch,confirmed_lsn,slot_name,open_commit_lsn FROM {s}source") == (
        2,
        start,
        "slot_e2",
        None,
    )
    assert one(
        h,
        "SELECT previous_epoch,start_lsn,previous_confirmed_lsn,reason,slot_name,"
        "previous_slot_name,timeline_id,abandoned_from FROM {s}source_epochs WHERE epoch=2",
    ) == (1, start, old, "slot_lost", "slot_e2", "slot", 1, old)
    assert h.log.execute("SELECT DISTINCT epoch,status FROM {s}frame_partitions_v2").fetchall() == [
        (1, "sealed")
    ]
    # A key version per table in the new epoch, unconfirmed until ingest checks it.
    rows = h.log.execute(
        "SELECT t.relation_id,t.key_version,t.key_epoch,t.key_checked_lsn,t.state,"
        "k.epoch,k.new_from_lsn,k.cut_lsn FROM {s}tables t JOIN {s}key_changes k "
        "ON k.relation_id=t.relation_id AND k.version=t.key_version ORDER BY 1"
    ).fetchall()
    assert len(rows) == len(TABLES)
    for _, version, key_epoch, checked, state, epoch, new_from, cut in rows:
        assert (version, key_epoch, checked, state, epoch, new_from) == (
            1,
            2,
            None,
            "needs_resync",
            2,
            start,
        )
        assert cut == start

    # A retry of the same transition (a lost response) changes nothing.
    assert not transition(h, 2, start, timeline_id=1, abandoned_from=old)
    with pytest.raises(SourceMismatch, match="already began differently"):
        transition(h, 2, start + 1)
    with pytest.raises(SourceMismatch, match="must follow"):
        transition(h, 4, start, slot="slot_e4")
    with pytest.raises(ValueError, match="reason"):
        transition(h, 3, start, reason="other")
    assert one(h, "SELECT count(*) FROM {s}source_epochs") == (2,)

    # Every mirror of the old history holds; apply never reads the new epoch for it.
    result = h.run("items")
    assert result.status == "needs_resync"
    assert h.state("items")[3] == "source epoch changed"
    assert h.rows("items", "id") == [(1,)]


def test_new_epoch_appends_at_lower_lsns_and_same_identities(stores, tmp_path):
    h = Harness(stores, tmp_path)
    w = h.wire
    rel = relation_message(1, "items", ITEMS)
    first = w.txn(rel, w.insert(1, ("1", "a", "t", "1.00")))
    old = append(h, h.spool, "20260101", first)
    transition(h, 2, 900)
    # Epoch 2's first commit has the same (commit_lsn, ordinal) as epoch 1's.
    again = Wire()
    new = append(
        h, spool_for(h, 2), "20260102", again.txn(rel, again.insert(1, ("9", "z", "t", "9.00")))
    )
    assert new == old  # same LSNs, a different epoch
    assert h.log.execute("SELECT epoch,last_commit FROM {s}units_v2 ORDER BY 1").fetchall() == [
        (1, old - 10),
        (2, old - 10),
    ]
    # Old-epoch items are refused once the epoch moved.
    stale_items = feed(h.spool, w.txn(w.insert(1, ("2", "b", "t", "2.00"))))
    h.log.acquire("stale")
    try:
        with pytest.raises(SourceMismatch, match="batch epoch"):
            h.log.append(stale_items, partition_day="20260102")
    finally:
        h.log.release()
    # Keys confirmed by ingest under the new epoch.
    assert one(h, "SELECT key_epoch,key_checked_lsn FROM {s}tables WHERE relation_id=1") == (
        2,
        new,
    )


def test_stale_worker_of_the_old_epoch_is_fenced(stores, tmp_path):
    h = Harness(stores, tmp_path)
    w = h.wire
    rel = relation_message(1, "items", ITEMS)
    append(h, h.spool, "20260101", w.txn(rel, w.insert(1, ("1", "a", "t", "1.00"))))
    stale = IngestLog(h.other_store.con, h.ingest_config)
    stale.acquire("stale")
    assert stale.epoch == 1 and stale.slot_name == "slot"
    # Its lease lapses (a long pause); the transition takes the lease over.
    h.log.execute("UPDATE {s}source SET expires_at=current_timestamp-INTERVAL 1 SECOND")
    transition(h, 2, 500)
    items = feed(h.spool, w.txn(w.insert(1, ("2", "b", "t", "2.00"))))
    with pytest.raises(FenceLost):
        stale.append(items, partition_day="20260101")
    with pytest.raises(FenceLost):
        stale.renew()
    assert one(h, "SELECT epoch,confirmed_lsn FROM {s}source") == (2, 500)


def test_configured_epoch_is_an_expectation(stores, tmp_path):
    h = Harness(stores, tmp_path)
    transition(h, 2, 500)
    expecting_one = IngestLog(h.store.con, replace(h.ingest_config, epoch=1))
    with pytest.raises(SourceMismatch, match="configured epoch"):
        expecting_one.acquire("run")
    assert one(h, "SELECT holder FROM {s}source") == (None,)  # the lease was released
    with pytest.raises(SourceMismatch, match="configured epoch"):
        expecting_one.register(("system", 1), 0, [])
    # Registration is compared without the epoch, and never adopts config's.
    IngestLog(h.store.con, h.ingest_config).register(("system", 1), 0, [])
    following = IngestLog(h.store.con, replace(h.ingest_config, epoch=2))
    following.acquire("run")
    following.release()
    apply = type(h.apply)(h.store.con, replace(h.config, epoch=1))
    with pytest.raises(ApplyError, match="configured epoch"):
        apply.source_position()
    assert type(h.apply)(h.store.con, h.config).source_position()[0] == 2
    assert ApplyConfig("s", ("a.b",)).epoch is None
    # A new source starts at epoch 1: a configured later epoch is refused, not adopted.
    fresh = IngestLog(
        h.store.con,
        replace(
            h.ingest_config,
            source_id="fresh",
            epoch=3,
            text_binding=replace(h.ingest_config.text_binding, source_id="fresh"),
        ),
    )
    fresh.initialize()
    with pytest.raises(SourceMismatch, match="starts at epoch 1"):
        fresh.register(("system", 1), 0, [])
    assert fresh.execute("SELECT count(*) FROM {s}source").fetchone() == (0,)


def test_log_only_start_is_refused_after_the_first_epoch(stores, tmp_path):
    h = Harness(stores, tmp_path)
    h.store.con.execute(f"DELETE FROM {h.apply.mirror}._cdc.apply_state WHERE relation_id=1")
    transition(h, 2, 500)
    with pytest.raises(ApplyError, match="backfill"):
        h.apply.start_from_log("public.items")


def test_old_epoch_history_drops_once_pins_move_and_new_pins_stay_above_the_floor(stores, tmp_path):
    """(epoch, LSN) order: a new-epoch pin below an old-epoch drop is above the floor."""
    h = Harness(stores, tmp_path)
    w = h.wire
    rel = relation_message(1, "items", ITEMS)
    old = append(h, h.spool, "20260101", w.txn(rel, w.insert(1, ("1", "a", "t", "1.00"))))
    for name, _ in TABLES.values():
        assert h.run(name).status == "caught_up"
    before = audited(h)
    # A commit no mirror applied before the transition: the old pins hold it.
    old = append(h, h.spool, "20260101", w.txn(w.insert(1, ("2", "b", "t", "2.00"))))
    transition(h, 2, 500)
    retention = h.apply.retention
    token = retention.acquire_compaction("test")
    try:
        assert not retention.drop_partition("20260101", token)  # old pins hold it
    finally:
        retention.release_compaction(token)
    # A resync of every table moves its pin to the new epoch (as publication does).
    for oid in TABLES:
        generation = f"resync{oid}"
        retention.register_generation(oid, generation, 2, 500)
        assert retention.transfer(oid, generation, 2, 500)
    token = retention.acquire_compaction("test")
    try:
        assert retention.drop_partition("20260101", token)
    finally:
        retention.release_compaction(token)
    assert retention.floor() == (1, old)
    # Far below the old floor's LSN, but in a later epoch.
    retention.register_generation(1, "low", 2, 500)
    assert retention.pin(generation_owner("low"))["lsn"] == 500
    with pytest.raises(HistoryDropped):
        retention.register_generation(1, "old", 1, old)
    # Across the slot_lost epoch (a lower LSN), pins transferred to new generations
    # (lower pins) and the drop: every position, the floor and the leases still hold.
    audited(h, before)


def test_legacy_epoch_catalog_refuses_without_an_implicit_upgrade(stores):
    from reference.legacy_store import create

    from cdc_flight.runtime import open_log

    store, _ = stores
    config = IngestConfig(
        "upgrade", "slot", "pub", ("public.t",), database=store.database.strip('"')
    )
    create(store.con, config)
    before = store.con.execute(
        "SELECT table_name,column_name,data_type FROM duckdb_columns() ORDER BY ALL"
    ).fetchall()
    with pytest.raises(SourceMismatch, match="fresh store"):
        open_log(store.con, config)
    assert (
        store.con.execute(
            "SELECT table_name,column_name,data_type FROM duckdb_columns() ORDER BY ALL"
        ).fetchall()
        == before
    )


def test_transition_day_drops_only_after_the_new_epoch_seals_it(stores, tmp_path):
    h = Harness(stores, tmp_path)
    w = h.wire
    rel = relation_message(1, "items", ITEMS)
    append(h, h.spool, "20260101", w.txn(rel, w.insert(1, ("1", "a", "t", "1.00"))))
    transition(h, 2, 500)
    new, spool = Wire(), spool_for(h, 2)
    append(h, spool, "20260101", new.txn(rel, new.insert(1, ("2", "b", "t", "2.00"))))
    retention = h.apply.retention
    for oid in TABLES:
        retention.register_generation(oid, f"g{oid}", 2, 500)
        assert retention.transfer(oid, f"g{oid}", 2, new.end)

    def drop():
        token = retention.acquire_compaction("test")
        try:
            return retention.drop_partition("20260101", token)
        finally:
            retention.release_compaction(token)

    # Epoch 1's row was sealed by the transition; epoch 2's is still active.
    assert sorted(
        h.log.execute(
            "SELECT epoch,status FROM {s}frame_partitions_v2 WHERE partition_day='20260101'"
        ).fetchall()
    ) == [(1, "sealed"), (2, "active")]
    assert not drop()
    later = append(h, spool, "20260102", new.txn(new.insert(1, ("3", "c", "t", "3.00"))))
    for oid in TABLES:
        retention.advance(f"table:{oid}", f"g{oid}", 2, later)
    assert drop()


def test_transition_whose_lease_lapsed_writes_nothing(stores, tmp_path):
    """begin_epoch's own guard: the PG step outlived the lease and ingest took over."""
    h = Harness(stores, tmp_path)
    w = h.wire
    rel = relation_message(1, "items", ITEMS)
    old = append(h, h.spool, "20260101", w.txn(rel, w.insert(1, ("1", "a", "t", "1.00"))))
    h.log.acquire("transition")
    h.log.execute("UPDATE {s}source SET expires_at=current_timestamp-INTERVAL 1 SECOND")
    ingest = IngestLog(h.other_store.con, h.ingest_config)
    ingest.acquire("ingest")
    with pytest.raises(FenceLost):
        h.log.begin_epoch(2, 500, "slot_lost", "slot_e2")
    assert one(h, "SELECT epoch,confirmed_lsn,slot_name FROM {s}source") == (1, old, "slot")
    assert one(h, "SELECT count(*) FROM {s}source_epochs") == (1,)
    assert h.log.execute("SELECT DISTINCT status FROM {s}frame_partitions_v2").fetchall() == [
        ("active",)
    ]
    ingest.release()
    assert transition(h, 2, 500)


def test_new_epoch_command_parses_and_ignores_the_configured_epoch(capsys):
    from types import SimpleNamespace

    from cdc_flight import cli, epoch

    calls = []

    class Log:
        def __init__(self, con, config):
            self.con, self.config = con, config

        def checkpoint(self):
            return 0x1_0000_0010

    def begin(config, log, new_epoch, reason, *, to_lsn):
        calls.append((config.epoch, new_epoch, reason, to_lsn))
        return epoch.EpochResult("begun", new_epoch, "slot", to_lsn or 5)

    fake = SimpleNamespace(begin_epoch=begin, lsn_text=epoch.lsn_text)
    config = IngestConfig("s", "slot", "pub", ("a.b",), epoch=1)
    args = SimpleNamespace(epoch=2, reason="skip", to_lsn="1/20")
    cli.new_epoch_main(args, config, Log(None, config), fake)
    assert calls == [(None, 2, "skip", 0x1_0000_0020)]
    assert "skipping WAL [1/10, 1/20)" in capsys.readouterr().out
    with pytest.raises(SystemExit, match="X/Y"):
        cli.new_epoch_main(
            SimpleNamespace(epoch=2, reason="skip", to_lsn="bad"), config, Log(None, config), fake
        )
    with pytest.raises(SystemExit, match="only for --reason skip"):
        cli.new_epoch_main(
            SimpleNamespace(epoch=2, reason="slot_lost", to_lsn="1/20"),
            config,
            Log(None, config),
            fake,
        )
    cli.new_epoch_main(
        SimpleNamespace(epoch=2, reason="slot_lost", to_lsn=None), config, Log(None, config), fake
    )
    assert "slot_e2" in capsys.readouterr().out
