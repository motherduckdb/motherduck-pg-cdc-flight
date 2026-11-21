"""Phase 2 gate, backfill path: every table backfilled under the seeded workload.

The log-only gate (test_phase2.py) starts mirrors from empty tables. Here every table
already has rows when capture begins, so each mirror is built by a backfill
generation while the same multi-session workload keeps changing the source: during
every snapshot chunk, between chunks, and across the end marker.
"""

import json
import random
from collections import Counter

import conftest
import pytest
from test_phase2 import (  # noqa: F401 (pg_phase2 is a fixture)
    KEY_SQL,
    POOLS,
    Session,
    Workload,
    assert_mirror_equals_source,
    key_args,
    pg_phase2,
)

from cdc_flight.apply import ApplyError
from cdc_flight.apply_run import ApplyRun
from cdc_flight.backfill import Backfill
from cdc_flight.bootstrap import bootstrap_log
from cdc_flight.config import BackfillConfig
from cdc_flight.ingest import run_once
from cdc_flight.retention import generation_owner, table_owner


def key_value(table, value):
    """A key as Workload holds it, from a generation's JSON chunk_key (text values)."""
    return (int(value[0]), value[1]) if table == "typed" else int(value[0])


def scanned(table, key, chunk_key):
    """Whether `key` is in the range a generation has already scanned. Python's order
    matches PostgreSQL's here because typed codes are `c1`..`c15` (a lowercase letter
    and digits), which sort alike under C and en_US collations; revisit this if the
    pool changes."""
    return chunk_key is not None and key <= key_value(table, json.loads(chunk_key))


def converged(drive, table):
    """Right after publication, before the workload moves on: catch the mirror up to
    the durable boundary and compare. Later changes could otherwise repair a stale
    row (the key spaces are small), hiding a lost catch-up change."""
    drive.ingest()
    assert drive.apply.apply_table(table, "check").status == "caught_up"
    assert_mirror_equals_source(drive.apply, drive.source, table)


class Driver:
    def __init__(self, stores, source, workload, ingest_config, log, backfill, apply):
        self.stores, self.source, self.workload = stores, source, workload
        self.ingest_config, self.log, self.backfill, self.apply = (
            ingest_config,
            log,
            backfill,
            apply,
        )
        # Per table: changes that only catch-up can deliver, made while its generation
        # had scanned past their key.
        self.after_scan = {t: Counter() for t in apply.config.tables}
        self.b_across_chunk = False
        self.c_across_marker = False

    def generation(self, table):
        return self.backfill.active(self.backfill.registered(table)[0])

    def ingest(self):
        assert run_once(self.ingest_config, self.log).status == "drained"

    def touch_scanned(self, table):
        """Update, delete and insert keys this table's generation has already read."""
        gen = self.generation(table)
        if gen is None or gen["chunk_key"] is None or gen["scan_done"]:
            return
        short = table.split(".")[1]
        w, x = self.workload, self.source.execute
        done = [k for k in w.available(short) if scanned(short, k, gen["chunk_key"])]
        free = [k for k in w.free(short) if scanned(short, k, gen["chunk_key"])]
        if len(done) >= 2:
            if short == "items":
                # Leaves the TOASTed value unchanged: a partial image under FULL identity.
                x("UPDATE items SET note=%s WHERE id=%s", ["after_scan", done[0]])
            else:
                w.update(x, short, done[0])
            self.after_scan[table]["update"] += 1
            x(f"DELETE FROM {short} WHERE {KEY_SQL[short]}", key_args(done[1]))
            w.rows[short].remove(done[1])
            self.after_scan[table]["delete"] += 1
        if free:
            w.insert(x, short, free[0])
            w.rows[short].add(free[0])
            self.after_scan[table]["insert"] += 1

    def step(self, table, fault=None):
        """One bounded backfill step, with B open across it."""
        w = self.workload
        w.b_claims = set()
        w.b.execute("BEGIN")
        w.statement(w.b.execute, w.b_claims)
        before = self.generation(table)
        try:
            result = self.backfill.run_table(table, max_chunks=1, fault=fault)
        finally:
            w.commit(w.b, w.b_claims)
            w.b_claims = None
        after = self.generation(table)
        if before and after and after["chunks"] > before["chunks"]:
            self.b_across_chunk = True
        if result.status == "waiting_for_marker" and w.c_claims is not None:
            self.c_across_marker = True
        return result


def crash(stage):
    def fault(at):
        if at == stage:
            raise RuntimeError(stage)

    return fault


@pytest.mark.postgres
def test_phase2_gate_backfill_every_table_under_seeded_workload(stores, pg_phase2, request):  # noqa: F811
    source, ingest_config, apply_config = pg_phase2
    s, _ = stores
    workload = Workload(source, seed=2027)
    request.addfinalizer(workload.close)

    # Every table has rows before the slot exists: no log-only start is exact.
    rng = random.Random(2027)
    for table, pool in POOLS.items():
        for key in rng.sample(pool, len(pool) // 2):
            workload.insert(source.execute, table, key)
            workload.rows[table].add(key)
    log, _ = bootstrap_log(s.con, ingest_config, create_objects=True)
    ingest_config = log.config  # the recorded IngestConfig2 (binding, units)
    config = BackfillConfig(
        apply_config.source_id,
        apply_config.tables,
        database=apply_config.database,
        mirror_database=apply_config.mirror_database,
        heartbeat=ingest_config.heartbeat,
        # At least 3 chunks per table at every size (15–20 seeded rows each; asserted).
        chunk_rows=conftest.md_size(stores, local=4, motherduck=6),
        marker_wait_seconds=0,
        max_batch_events=50,
        allow_local=True,
        dsn=ingest_config.dsn,
    )
    backfill = Backfill(s.con, config)
    backfill.initialize()
    request.addfinalizer(backfill.close)
    apply = ApplyRun(s.con, apply_config)
    retention = apply.retention
    for table in apply_config.tables:
        with pytest.raises(ApplyError, match="not observed empty"):
            apply.start_from_log(table)
    items, typed, plain = apply_config.tables
    drive = Driver(stores, source, workload, ingest_config, log, backfill, apply)

    # Backfill all three, round-robin one chunk at a time, under load.
    armed = {items: ["after_chunk_insert", "after_chunk_commit"], typed: ["after_publish_commit"]}
    fired, published = [], {}
    between = conftest.md_size(stores, local=3, motherduck=2)
    for step in range(200):
        if len(published) == 3:
            break
        for table in apply_config.tables:
            if table not in published:
                drive.touch_scanned(table)
        workload.transactions(between)
        if step == 1:
            workload.start_long()  # C: open across ingest runs and an end marker
        elif workload.c_claims is not None and drive.c_across_marker:
            workload.finish_long()
        # Changes reach the log between one chunk and the next (a late L loses them).
        drive.ingest()
        for table in apply_config.tables:
            if table in published:
                continue
            gen = drive.generation(table)
            fault = None
            stage = (armed.get(table) or [None])[0]
            if stage and (stage == "after_publish_commit" or (gen and gen["chunks"] >= 1)):
                fault = crash(stage)
            try:
                result = drive.step(table, fault)
            except RuntimeError as exc:
                assert str(exc) == stage
                fired.append(armed[table].pop(0))
                if stage == "after_publish_commit":
                    # Published, but the pin transfer never ran: the generation pin
                    # remains and the table pin is not yet the new generation's.
                    relation_id, _ = apply.registered(table)
                    generation = apply.state(relation_id)["generation"]
                    assert retention.pin(generation_owner(generation)) is not None
                    pin = retention.pin(table_owner(relation_id))
                    assert pin is None or pin["generation"] != generation
                    # Apply's read precondition completes it.
                    assert apply.apply_table(table, "after-crash").status == "caught_up"
                    pin = retention.pin(table_owner(relation_id))
                    assert pin["generation"] == generation
                    published[table] = generation
                    converged(drive, table)
                continue
            assert result.status != "failed", result
            if result.status == "published":
                published[table] = result.generation
                converged(drive, table)
    assert len(published) == 3, published
    assert fired == ["after_chunk_insert", "after_chunk_commit", "after_publish_commit"]
    workload.finish_long()

    # The structure the gate depends on, so a resize can't silently drop a case.
    for table, generation in published.items():
        assert backfill.generation(generation)["chunks"] >= 3, table
        counts = drive.after_scan[table]
        assert counts["update"] >= 1 and counts["delete"] >= 1 and counts["insert"] >= 1, (
            table,
            counts,
        )
        relation_id, _ = apply.registered(table)
        pin = retention.pin(table_owner(relation_id))
        assert pin["generation"] == generation
        assert pin["lsn"] >= backfill.generation(generation)["marker_lsn"]
    assert drive.b_across_chunk and drive.c_across_marker
    assert not [o for o in retention.pins() if o.startswith("generation:")]

    # Live apply rounds, as in the log-only gate, with a column added after publication.
    rounds = conftest.md_size(stores, local=3, motherduck=1)
    for round_number in range(rounds):
        if round_number == min(1, rounds - 1):
            source.execute("ALTER TABLE items ADD COLUMN extra text")
            workload.items_extra = True
        workload.transactions(8)
        drive.ingest()
        results = apply.run()
        assert [r.status for r in results] == ["caught_up"] * 3, results
        assert {r.boundary for r in results} == {log.checkpoint()}
        for table in apply_config.tables:
            assert_mirror_equals_source(apply, source, table)

    # A resync of a live table under load replaces its mirror.
    relation_id, _ = apply.registered(plain)
    old = apply.state(relation_id)["generation"]
    drive.after_scan[plain].clear()
    for _ in range(100):
        drive.touch_scanned(plain)
        workload.transactions(between)
        drive.ingest()
        result = drive.step(plain)
        assert result.status != "failed", result
        if result.status == "published":
            break
    assert result.status == "published"
    converged(drive, plain)
    new = result.generation
    assert backfill.generation(old)["state"] == "replaced"
    assert retention.pin(table_owner(relation_id))["generation"] == new
    assert generation_owner(old) not in retention.pins()
    assert generation_owner(new) not in retention.pins()
    assert drive.after_scan[plain]["update"] >= 1

    workload.transactions(8)
    drive.ingest()
    results = apply.run()
    assert [r.status for r in results] == ["caught_up"] * 3, results
    for table in apply_config.tables:
        assert_mirror_equals_source(apply, source, table)


@pytest.mark.postgres
@pytest.mark.parametrize("stores", ["local"], indirect=True)
@pytest.mark.parametrize("fold", ["sql", "python"])
def test_catch_up_folds_the_unit_starting_at_each_batch_boundary(
    stores,
    pg_phase2,  # noqa: F811
    request,
    drain_only_units,
    fold,
):
    """Catch-up's own lower bound (`catchup_lsn`) is inclusive: the unit that starts
    exactly at the previous catch-up batch's end is folded.

    Format 2: a catch-up batch is a page of whole complete units, and its checkpoint is
    the page's last unit end. Every ingest run closes its unit when it drains, so one
    update of an already-scanned key per run gives one unit per update after L; one
    event per batch makes every page one unit, starting at the previous checkpoint (the
    unit chain is contiguous). An exclusive bound skips (or refuses) that unit, and
    the updates are increments, so the mirror would differ.

    The drain must be the only unit boundary. Otherwise a slow read closes a unit early
    on its age (`max_delay_ms`) or member count, and on PostgreSQL 13/14, where pgoutput
    sends other sessions' transactions as empty members, that unit can hold no event, so
    one page spans two units. `drain_only_units` makes both limits unreachable."""
    source, ingest_config, apply_config = pg_phase2
    s, _ = stores
    plain = apply_config.tables[2]
    source.execute("INSERT INTO plain (id,qty,ok) SELECT i,0,true FROM generate_series(1,8) i")
    log, _ = bootstrap_log(s.con, ingest_config, create_objects=True)
    assert log.config.unit_config.max_delay_ms == 600_000
    ingest_config = log.config
    config = BackfillConfig(
        apply_config.source_id,
        apply_config.tables,
        database=apply_config.database,
        mirror_database=apply_config.mirror_database,
        heartbeat=ingest_config.heartbeat,
        chunk_rows=4,
        marker_wait_seconds=0,
        max_batch_events=1,
        allow_local=True,
        dsn=ingest_config.dsn,
    )
    backfill = Backfill(s.con, config, fold=fold)
    backfill.initialize()
    request.addfinalizer(backfill.close)
    apply = ApplyRun(s.con, apply_config)

    assert backfill.run_table(plain, max_chunks=1).status == "snapshotting"
    generation_id = backfill.active(backfill.registered(plain)[0])["generation_id"]
    low = backfill.generation(generation_id)["pin_lsn"]
    # Keys 1..4 are scanned (chunk_rows=4): only catch-up can deliver these updates.
    for key in (1, 2, 3, 1, 3):
        source.execute("UPDATE plain SET qty=qty+1 WHERE id=%s", [key])
        assert run_once(ingest_config, log).status == "drained"
    units = log.execute(
        "SELECT unit_start_lsn,end_lsn FROM {s}units_v2 WHERE complete AND unit_start_lsn>=? "
        "ORDER BY unit_start_lsn",
        [low],
    ).fetchall()
    # The precondition: one unit per run after L, each starting at the previous one's end.
    assert len(units) == 5, units
    assert all(b[0] == a[1] for a, b in zip(units, units[1:]))
    # Each catch-up batch: its inclusive lower bound and the boundary it checkpoints.
    # Either fold's read: the SQL plan (a batch it takes), else the Python batch.
    batches, real, real_plan = [], backfill._catch_up_batch, backfill._catch_up_plan

    def recorded(gen, current, low_bound, high, **kw):
        chosen, boundary, columns, images = real(gen, current, low_bound, high, **kw)
        batches.append((low_bound, boundary))
        return chosen, boundary, columns, images

    def planned(gen, current, low_bound, high):
        batch, read = real_plan(gen, current, low_bound, high)
        if batch is not None:
            batches.append((low_bound, batch.page[-1].end))
        return batch, read

    backfill._catch_up_batch, backfill._catch_up_plan = recorded, planned
    for _ in range(40):
        result = backfill.run_table(plain, max_chunks=1)
        assert result.status != "failed", result
        if result.status == "published":
            break
        if result.status in ("waiting_for_marker", "waiting_for_ingest", "catching_up"):
            assert run_once(ingest_config, log).status == "drained"
    assert result.status == "published"
    ends = {
        end
        for (end,) in log.execute(
            "SELECT end_lsn FROM {s}units_v2 WHERE complete AND unit_start_lsn>=?", [low]
        ).fetchall()
    }
    # One unit per batch: each starts at the previous batch's checkpoint (inclusive),
    # and each checkpoint is a unit end, across every unit with an update.
    assert batches[0][0] == low and all(b[1] in ends for b in batches), batches
    assert all(b[0] == a[1] for a, b in zip(batches, batches[1:])), batches
    assert {start for start, _ in units} <= {b[0] for b in batches}, (units, batches)
    # Every catch-up batch took the chosen fold (none fell back).
    assert backfill.fold_stats == ({"sql": len(batches)} if fold == "sql" else {})
    assert run_once(ingest_config, log).status == "drained"
    assert apply.apply_table(plain, "check").status == "caught_up"
    assert_mirror_equals_source(apply, source, plain)
