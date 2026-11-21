"""Phase 2 gate, resync after slot loss: exit criterion (b) (dev-docs/test-map.md).

Mirrors are built by backfill under the seeded workload. Then the slot is lost with
committed changes it never delivered, the workload keeps writing while there is no slot,
and a transaction stays open across the new slot's creation. An operator begins epoch 2;
every table holds `needs_resync` until a new-epoch generation rebuilds it under load. The
run ends at a quiescent marker with source EXCEPT ALL mirror empty for every table.
"""

import random
import threading
import time
from dataclasses import replace

import conftest
import pytest
from test_gate_backfill import Driver, converged
from test_phase2 import (  # noqa: F401 (pg_phase2 is a fixture)
    KEY_SQL,
    POOLS,
    Workload,
    assert_mirror_equals_source,
    key_args,
    pg_phase2,
)

from cdc_flight.apply_run import ApplyRun
from cdc_flight.backfill import Backfill
from cdc_flight.bootstrap import bootstrap_log
from cdc_flight.config import BackfillConfig
from cdc_flight.epoch import begin_epoch
from cdc_flight.ingest import run_once
from cdc_flight.preflight import PreflightError, query
from cdc_flight.retention import table_owner


def change_every_table(workload, x):
    """An update, a delete and an insert on every table; returns what changed."""
    changed = []
    for table in POOLS:
        available, free = workload.available(table), workload.free(table)
        workload.update(x, table, available[0])
        changed.append((table, "update", available[0]))
        x(f"DELETE FROM {table} WHERE {KEY_SQL[table]}", key_args(available[1]))
        workload.rows[table].remove(available[1])
        changed.append((table, "delete", available[1]))
        # A full key pool re-inserts the deleted key (still an insert after a delete).
        key = free[0] if free else available[1]
        workload.insert(x, table, key)
        workload.rows[table].add(key)
        changed.append((table, "insert", key))
    return changed


def in_mirror(apply, schema, table, key):
    relation_id, _ = apply.registered(f"{schema}.{table}")
    current = apply.state(relation_id)
    where = KEY_SQL[table].replace("%s", "?")
    return apply.con.execute(
        f"SELECT count(*) FROM {current['target']} WHERE {where}", key_args(key)
    ).fetchone()[0]


def backfill_all(drive, tables, between, *, touch=True):
    """Backfill every table round-robin, one chunk per step, under the workload. Returns
    {table: generation}, comparing each table right after its publication."""
    published = {}
    for _ in range(200):
        if len(published) == len(tables):
            return published
        if touch:
            for table in tables:
                if table not in published:
                    drive.touch_scanned(table)
        drive.workload.transactions(between)
        drive.ingest()
        for table in tables:
            if table in published:
                continue
            result = drive.step(table)
            assert result.status != "failed", result
            if result.status == "published":
                published[table] = result.generation
                converged(drive, table)
    raise AssertionError(f"backfill did not finish: {published}")


@pytest.mark.postgres
def test_phase2_gate_resync_after_slot_loss_under_seeded_workload(stores, pg_phase2, request):  # noqa: F811
    source, ingest_config, apply_config = pg_phase2
    s, _ = stores
    schema = source.schema
    workload = Workload(source, seed=2028)
    request.addfinalizer(workload.close)
    rng = random.Random(2028)
    for table, pool in POOLS.items():
        for key in rng.sample(pool, len(pool) // 2):
            workload.insert(source.execute, table, key)
            workload.rows[table].add(key)
    log, info = bootstrap_log(s.con, ingest_config, create_objects=True)
    ingest_config = log.config
    config = BackfillConfig(
        apply_config.source_id,
        apply_config.tables,
        database=apply_config.database,
        mirror_database=apply_config.mirror_database,
        heartbeat=ingest_config.heartbeat,
        chunk_rows=4,
        marker_wait_seconds=0,
        max_batch_events=50,
        allow_local=True,
        dsn=ingest_config.dsn,
    )
    backfill = Backfill(s.con, config)
    backfill.initialize()
    request.addfinalizer(backfill.close)
    apply = ApplyRun(s.con, apply_config)
    drive = Driver(stores, source, workload, ingest_config, log, backfill, apply)
    between = conftest.md_size(stores, local=3, motherduck=2)
    new_slot = ingest_config.slot + "_e2"
    request.addfinalizer(
        lambda: query(
            source.con,
            "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
            "WHERE slot_name=%s AND NOT active",
            [new_slot],
        )
    )

    # Epoch 1: mirrors built by backfill (large chunks: only the resync below is held
    # to 3 chunks), then one live round.
    initial = Backfill(s.con, replace(config, chunk_rows=1000))
    request.addfinalizer(initial.close)
    backfill_all(
        Driver(stores, source, workload, ingest_config, log, initial, apply),
        apply_config.tables,
        between,
        touch=False,
    )
    workload.transactions(6)
    drive.ingest()
    assert [r.status for r in apply.run()] == ["caught_up"] * 3
    for table in apply_config.tables:
        assert_mirror_equals_source(apply, source, table)

    # The slot is lost while ingest lags: committed changes it never delivered...
    undelivered = change_every_table(workload, source.execute)
    workload.transactions(4)
    # ...a transaction that stays open across the new slot's creation...
    workload.start_long()
    conftest.wait_slot_released(source.con, ingest_config.slot)
    query(source.con, "SELECT pg_drop_replication_slot(%s)", [ingest_config.slot])
    # ...and writes while there is no slot at all.
    gap = change_every_table(workload, source.execute)
    workload.transactions(4)
    with pytest.raises(PreflightError, match="missing"):
        run_once(ingest_config, log)

    # new-epoch waits for the open transaction: slot creation needs a consistent point.
    outcome = {}

    def transition():
        try:
            outcome["result"] = begin_epoch(ingest_config, log, 2, "slot_lost")
        except Exception as exc:  # reported below
            outcome["error"] = exc

    (c_xid,) = workload.c.execute("SELECT txid_current_if_assigned()::text")[0]
    assert c_xid is not None  # C has written, so it holds a transaction id
    worker = threading.Thread(target=transition)
    worker.start()
    # Wait until this transition's slot creation waits on C itself (other workers'
    # slot creations may be waiting too). Slot creation waits for every transaction
    # open in the cluster, so allow for a busy cluster under -n 4.
    deadline = time.monotonic() + 300
    while not query(
        source.con,
        "SELECT 1 FROM pg_locks l JOIN pg_stat_activity a ON a.pid=l.pid "
        "WHERE l.locktype='transactionid' AND l.transactionid::text=%s AND NOT l.granted "
        "AND a.query ILIKE '%%pg_create_logical_replication_slot%%'",
        [c_xid],
    ):
        assert time.monotonic() < deadline and worker.is_alive(), outcome
        time.sleep(0.05)
    assert "result" not in outcome  # still waiting on C
    workload.finish_long()  # C commits from its own session
    worker.join(300)
    assert outcome.get("result") and outcome["result"].status == "begun", outcome
    assert (outcome["result"].slot, log.position()[:2]) == (new_slot, (2, new_slot))

    # Epoch 2: ingest follows the new slot; every mirror of the old history holds.
    drive.ingest()
    results = apply.run()
    assert [r.status for r in results] == ["needs_resync"] * 3, results

    # Resync every table under load; each is compared right after it publishes.
    published = backfill_all(drive, apply_config.tables, between)
    for table, generation in published.items():
        gen = backfill.generation(generation)
        assert gen["epoch"] == 2 and gen["chunks"] >= 3, (table, gen)
        relation_id, _ = apply.registered(table)
        pin = apply.retention.pin(table_owner(relation_id))
        assert (pin["generation"], pin["epoch"]) == (generation, 2)
    # No pin still holds epoch 1, whose partitions the transition sealed.
    assert all(epoch == 2 for epoch, _ in apply.retention.pins().values())
    assert log.execute(
        "SELECT DISTINCT status FROM {s}frame_partitions_v2 WHERE epoch=1"
    ).fetchall() == [("sealed",)]

    # Live rounds in epoch 2.
    for _ in range(conftest.md_size(stores, local=2, motherduck=1)):
        workload.transactions(6)
        drive.ingest()
        assert [r.status for r in apply.run()] == ["caught_up"] * 3

    # Quiescent marker: workload stopped (no open B or C), ingest drained to its
    # marker, apply caught up; then source EXCEPT ALL mirror is empty everywhere.
    assert workload.b_claims is None and workload.c_claims is None and not workload.claimed
    result = run_once(ingest_config, log)
    assert result.status == "drained"
    final = apply.run()
    assert [r.status for r in final] == ["caught_up"] * 3
    assert {r.boundary for r in final} == {result.boundary}
    for table in apply_config.tables:
        assert_mirror_equals_source(apply, source, table)
    # The changes lost to the log are there: deletes absent, the rest present.
    for table, op, key in undelivered + gap:
        if workload.rows[table] and (key in workload.rows[table]) == (op != "delete"):
            assert in_mirror(apply, schema, table, key) == (0 if op == "delete" else 1)
