# ruff: noqa: F811 (the imported pair fixture is used as a parameter)
"""Replica capture against a real standby (compose profile `replica`): keyed backfill
finality from the standby, the bounded recovery-conflict retry,
and the per-table append refusal. Opt-in like tests/test_replica_capture.py."""

import time
from dataclasses import replace

import psycopg2
import pytest
from test_phase2 import assert_mirror_equals_source
from test_replica_capture import (  # noqa: F401 (the pair fixture)
    Snapshots,
    config,
    connect,
    dba_setup,
    heartbeat_job,
    pair,
    query,
    replayed,
)

from cdc_flight.apply import ApplyError
from cdc_flight.apply_run import ApplyRun
from cdc_flight.backfill import Backfill
from cdc_flight.bootstrap import bootstrap_log
from cdc_flight.config import BackfillConfig
from cdc_flight.ingest import run_once

pytestmark = pytest.mark.replica

ROWS = 240


class Primary:
    """The primary as `assert_mirror_equals_source`'s source: the truth the mirror must
    equal once the standby has replayed it."""

    def __init__(self, con):
        self.con = con

    def execute(self, statement, args=None):
        return query(self.con, statement, args)


def beat(pair):
    """An ingest fault hook: the DBA job's next upsert once the run has read B."""

    def fault(stage):
        if stage == "after_start":
            heartbeat_job(pair)

    return fault


def ingest_once(pair, cfg, log):
    replayed(pair)
    return run_once(cfg, log, fault=beat(pair))


@pytest.fixture
def standby_backfill(pair, stores):
    """A bootstrapped external source with ROWS rows in its table, and a Backfill on the
    standby. Yields (cfg, log, backfill)."""
    dba_setup(pair)
    query(
        pair["primary"],
        f"INSERT INTO {pair['table']} SELECT i,'v'||i FROM generate_series(1,%s) i",
        [ROWS],
    )
    replayed(pair)
    with Snapshots(pair):
        log, _ = bootstrap_log(
            stores[0].con, config(pair, stores, runtime_seconds=60), create_objects=True
        )
    cfg = log.config
    bcfg = BackfillConfig(
        cfg.source_id,
        cfg.tables,
        database=cfg.database,
        mirror_database=stores[0].mirror_database.strip('"'),
        heartbeat=cfg.heartbeat,
        heartbeat_writer="external",
        chunk_rows=50,
        marker_wait_seconds=0,
        allow_local=True,
        dsn=cfg.dsn,
    )
    backfill = Backfill(stores[0].con, bcfg)
    backfill.initialize()
    try:
        yield cfg, log, backfill
    finally:
        backfill.close()


def drive(pair, backfill, table, cfg, log, *, between=None, limit=200):
    for _ in range(limit):
        result = backfill.run_table(table, max_chunks=1)
        if result.status in ("failed", "published"):
            return result
        if between:
            between(result)
        if result.status in ("waiting_past_replay", "waiting_for_ingest", "catching_up"):
            assert ingest_once(pair, cfg, log).status == "drained"
    raise AssertionError("backfill did not finish")


def test_a_keyed_backfill_from_the_standby_is_exact_with_a_transaction_across_b(
    pair, stores, standby_backfill
):
    """Rule U for keyed backfill: the per-chunk bound B (no marker row written), H the
    first unit past it; a primary transaction open across the scan and DML between
    chunks land exactly once; the generation records the external mode."""
    cfg, log, backfill = standby_backfill
    table = pair["table"]
    assert ingest_once(pair, cfg, log).status == "drained"
    long = psycopg2.connect(pair["dsns"]["primary"])
    query(long, f"UPDATE {table} SET v='long' WHERE id=1")  # open across the scan
    state = {"chunks": 0, "committed": False}

    def between(result):
        if result.status == "snapshotting":
            state["chunks"] += 1
            query(
                pair["primary"],
                f"UPDATE {table} SET v='during' WHERE id=%s",
                [ROWS - state["chunks"]],
            )
            query(
                pair["primary"], f"INSERT INTO {table} VALUES (%s,'new')", [1000 + state["chunks"]]
            )
        elif not state["committed"]:
            long.commit()  # commits after B: catch-up or apply carries it
            state["committed"] = True

    try:
        result = drive(pair, backfill, table, cfg, log, between=between)
    finally:
        long.rollback() if not state["committed"] else None
        long.close()
    assert result.status == "published", result
    gen = backfill.generation(result.generation)
    assert gen["finality_mode"] == "external"
    assert gen["marker"] is None  # no marker: the standby can't be written
    assert gen["wal_before_marker"] == gen["replay_bound"] > gen["pin_lsn"]
    assert gen["marker_lsn"] > gen["wal_before_marker"]  # H: strictly past B
    assert query(pair["primary"], f"SELECT id FROM {pair['heartbeat']} ORDER BY id") == [
        (f"external:{pair['source']}",)
    ]  # nothing of backfill's in the heartbeat table
    # Apply from H; the mirror equals the primary once replayed and ingested.
    assert ingest_once(pair, cfg, log).status == "drained"
    apply = ApplyRun(stores[0].con, replace(backfill.config, tables=(table,)))
    assert apply.run()[0].status == "caught_up"
    assert_mirror_equals_source(apply, Primary(pair["primary"]), table)


def test_a_recovery_conflict_between_fetches_is_retried_on_a_new_session(
    pair, stores, standby_backfill, monkeypatch
):
    """An ACCESS EXCLUSIVE lock replayed while the chunk's cursor sits idle in
    its transaction terminates the session (FATAL 40001); the run
    reopens it, resumes at the persisted key with half the rows, and completes."""
    cfg, log, first = standby_backfill
    table = pair["table"]
    backfill = Backfill(stores[0].con, replace(first.config, chunk_rows=200))
    query(pair["standby"], "ALTER SYSTEM SET max_standby_streaming_delay='100ms'")
    query(pair["standby"], "SELECT pg_reload_conf()")
    try:
        assert ingest_once(pair, cfg, log).status == "drained"
        assert backfill.run_table(table, max_chunks=1).status == "snapshotting"
        fetch, fired = Backfill._fetch_chunk, {"done": False}

        def idle_then_conflict(self, con, statement, params):
            rows = fetch(self, con, statement, params)
            if not fired["done"]:
                fired["done"] = True
                # The session is idle in its transaction (holding AccessShareLock) while
                # the primary's ACCESS EXCLUSIVE lock replays.
                # A transaction (LOCK needs one): its AccessExclusiveLock is WAL-logged and
                # replayed on the standby, which waits max_standby_streaming_delay for our
                # idle session's AccessShareLock, then terminates it.
                locker = psycopg2.connect(pair["dsns"]["primary"])
                with locker.cursor() as cur:
                    cur.execute(f"LOCK TABLE {table} IN ACCESS EXCLUSIVE MODE")
                # The lock's WAL record reaches the standby only once flushed: any commit.
                heartbeat_job(pair)
                time.sleep(2)  # past max_standby_streaming_delay: the conflict fires
                locker.commit()
                locker.close()
            return rows

        monkeypatch.setattr(Backfill, "_fetch_chunk", idle_then_conflict)
        before = backfill.generation(
            backfill.active(backfill.registered(table)[0])["generation_id"]
        )
        result = backfill.run_table(table, max_chunks=3)
        assert fired["done"]
        assert backfill._conflicts >= 1 and backfill._chunk_rows == 100  # halved once
        after = backfill.generation(result.generation)
        assert (
            after["chunks"] > before["chunks"] and after["replay_bound"] >= before["replay_bound"]
        )
        monkeypatch.undo()
        assert drive(pair, backfill, table, cfg, log).status == "published"
    finally:
        backfill.close()
        query(pair["standby"], "ALTER SYSTEM RESET max_standby_streaming_delay")
        query(pair["standby"], "SELECT pg_reload_conf()")


def test_a_backfill_config_that_disagrees_with_the_recorded_mode_is_refused(
    pair, stores, standby_backfill
):
    cfg, log, backfill = standby_backfill
    wrong = Backfill(stores[0].con, replace(backfill.config, heartbeat_writer="self"))
    try:
        with pytest.raises(ApplyError, match="registered as external"):
            wrong.run_table(pair["table"])
    finally:
        wrong.close()
    assert backfill.active(backfill.registered(pair["table"])[0]) is None


# Append snapshot and epoch on a standby --------------------------------------------------


def append_table(pair, stores, cfg, log, standby_backfill_, *, rows=6, **config):
    """An append (keyless) table on the primary, published, ingested, added in append mode
    through the standby; a Backfill for it. Returns (table, snap, apply)."""
    from test_keyless_add import declare_all

    from cdc_flight.membership import add_table

    _, _, backfill = standby_backfill_
    schema = pair["schema"]
    table = f"{schema}.events"
    query(pair["primary"], f"CREATE TABLE {table} (msg text, n int)")
    query(pair["primary"], f"ALTER TABLE {table} REPLICA IDENTITY FULL")
    query(pair["primary"], f"ALTER PUBLICATION {pair['publication']} ADD TABLE {table}")
    query(
        pair["primary"],
        f"INSERT INTO {table} SELECT 'm'||i, i FROM generate_series(1,%s) i",
        [rows],
    )
    assert ingest_once(pair, cfg, log).status == "drained"
    declare_all(log)
    log.forget()
    apply = ApplyRun(stores[0].con, replace(backfill.config, tables=(table,)))
    added = add_table(log, apply, pair["standby"], pair["publication"], table, mode="append")
    assert added.status in ("added", "adopted"), added
    snap = Backfill(
        stores[0].con,
        replace(backfill.config, publication=pair["publication"], **config),
    )
    snap.initialize()
    return table, snap, apply


def mirror_rows(apply, table):
    schema, name = table.split(".")
    return sorted(
        apply.con.execute(f'SELECT msg,n FROM {apply.mirror}."{schema}"."{name}"').fetchall()
    )


def primary_rows(pair, table):
    return sorted(query(pair["primary"], f"SELECT msg,n FROM {table}"))


def drive_append(pair, snap, table, cfg, log, limit=40):
    for _ in range(limit):
        result = snap.run_table(table)
        if result.status in ("failed", "published", "refused"):
            return result
        assert ingest_once(pair, cfg, log).status == "drained"
    raise AssertionError("append snapshot did not finish")


def test_an_append_snapshot_from_the_standby_is_exact_and_retries_a_slot_wait(
    pair, stores, standby_backfill
):
    """An open primary transaction holds the exported slot's
    creation past its bound twice (`waiting_for_primary`, nothing written, still
    retryable); once it ends, one copy; B >= C; H > B; no marker; exact after apply."""
    cfg, log, _ = standby_backfill
    table, snap, apply = append_table(pair, stores, cfg, log, standby_backfill, slot_wait_seconds=1)
    try:
        holder = psycopg2.connect(pair["dsns"]["primary"])
        query(holder, f"INSERT INTO {table} VALUES ('open', 0)")  # open: blocks the slot
        try:
            for _ in range(2):
                result = snap.run_table(table)
                assert result.status == "waiting_for_primary", result
                assert "pg_log_standby_snapshot()" in result.detail
                gen = snap.generation(result.generation)
                assert gen["state"] == "snapshotting" and not gen["copy_started"]
                assert gen["rows"] == 0
        finally:
            holder.commit()
            holder.close()
        query(pair["primary"], f"INSERT INTO {table} VALUES ('after', 99)")
        with Snapshots(pair):
            result = drive_append(pair, snap, table, cfg, log)
        assert result.status == "published", result
        gen = snap.generation(result.generation)
        assert gen["finality_mode"] == "external" and gen["marker"] is None
        assert (
            gen["wal_before_marker"] >= gen["snapshot_lsn"]
            and gen["marker_lsn"] > gen["wal_before_marker"]
        )
        assert ingest_once(pair, cfg, log).status == "drained"
        assert apply.run()[0].status == "caught_up"
        assert mirror_rows(apply, table) == primary_rows(pair, table)  # each row exactly once
    finally:
        snap.close()


def test_a_recovery_conflict_during_the_append_copy_fails_the_generation(
    pair, stores, standby_backfill
):
    """The exported snapshot can't be re-imported, so a conflict that ends the
    import session mid-copy (idle in its transaction between batches) fails the generation
    with the reason; nothing half-copied is published."""
    cfg, log, _ = standby_backfill
    table, snap, apply = append_table(
        pair, stores, cfg, log, standby_backfill, rows=8, chunk_rows=2
    )
    query(pair["standby"], "ALTER SYSTEM SET max_standby_streaming_delay='100ms'")
    query(pair["standby"], "SELECT pg_reload_conf()")
    fired = {"done": False}

    def conflict(stage):
        if stage == "after_chunk_commit" and not fired["done"]:
            fired["done"] = True
            locker = psycopg2.connect(pair["dsns"]["primary"])
            with locker.cursor() as cur:
                cur.execute(f"LOCK TABLE {table} IN ACCESS EXCLUSIVE MODE")
            heartbeat_job(pair)  # flushes the lock's WAL record to the standby
            time.sleep(2)
            locker.commit()
            locker.close()

    try:
        with Snapshots(pair):
            result = snap.run_table(table, fault=conflict)
        assert fired["done"]
        assert result.status == "failed", result
        assert "recovery conflict or a lost connection" in result.detail
    finally:
        query(pair["standby"], "ALTER SYSTEM RESET max_standby_streaming_delay")
        query(pair["standby"], "SELECT pg_reload_conf()")
        snap.close()


def test_epoch_on_a_standby_recovers_a_lost_slot_and_clamps_a_skip_to_replay(
    pair, stores, standby_backfill
):
    """Recover-slot recreates the slot on the standby under the bounded wait
    (a slot missing or invalidated is `lost`; spike item 4: a conflict-invalidated slot
    reads wal_status 'lost'); skip-to-lsn clamps to the replay position."""
    from cdc_flight.epoch import begin_epoch

    cfg, log, _ = standby_backfill
    assert ingest_once(pair, cfg, log).status == "drained"
    # A lost slot: dropped on the standby (as an invalidated one would be recreated).
    for _ in range(100):
        if not query(
            pair["standby"],
            "SELECT 1 FROM pg_replication_slots WHERE slot_name=%s AND active",
            [log.slot_name],
        ):
            break
        time.sleep(0.05)
    query(pair["standby"], "SELECT pg_drop_replication_slot(%s)", [log.slot_name])
    with Snapshots(pair):
        recovered = begin_epoch(cfg, log, 2, "slot_lost")
    assert recovered.status == "begun", recovered
    assert query(
        pair["standby"], "SELECT 1 FROM pg_replication_slots WHERE slot_name=%s", [recovered.slot]
    )
    assert ingest_once(pair, cfg, log).status == "drained"
    # Skip far past the replay position: clamped to it. New WAL first, so the replay
    # position is past the durable prefix ingest just reached.
    heartbeat_job(pair)
    replayed(pair)
    (replay,) = query(pair["standby"], "SELECT pg_last_wal_replay_lsn()-'0/0'::pg_lsn")[0]
    target = int(replay) + 64 * 1024 * 1024
    skipped = begin_epoch(cfg, log, 3, "skip", to_lsn=target)
    assert skipped.status == "begun" and skipped.clamped, skipped
    (after,) = query(pair["standby"], "SELECT pg_last_wal_replay_lsn()-'0/0'::pg_lsn")[0]
    assert skipped.start_lsn <= int(after) < target
    assert ingest_once(pair, cfg, log).status == "drained"
