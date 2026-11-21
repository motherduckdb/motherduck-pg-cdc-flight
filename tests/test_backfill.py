"""Phase 2 slice 2: generation-based backfill on real PostgreSQL."""

import json
import os
import random
from dataclasses import replace

import conftest
import psycopg2
import pytest
from psycopg2 import sql
from test_phase2 import assert_mirror_equals_source

from cdc_flight.apply import ApplyError, FenceLost
from cdc_flight.apply_run import ApplyRun
from cdc_flight.backfill import Backfill
from cdc_flight.bootstrap import bootstrap_log
from cdc_flight.config import BackfillConfig, IngestConfig
from cdc_flight.ingest import run_once
from cdc_flight.preflight import query
from cdc_flight.retention import generation_owner, table_owner
from cdc_flight.source import Source


class Session:
    def __init__(self, dsn, schema):
        self.con = psycopg2.connect(dsn)
        self.con.autocommit = True
        self.execute(sql.SQL("SET search_path TO {}").format(sql.Identifier(schema)))
        # A self-inflicted lock wait must fail the test rather than hang it.
        self.execute("SET lock_timeout='20s'")

    def execute(self, statement, args=None):
        with self.con.cursor() as cur:
            cur.execute(statement, args)
            return cur.fetchall() if cur.description else None

    def close(self):
        self.con.close()


def big_rows(stores):
    """Rows in `big`: fewer on MotherDuck, where every chunk costs round trips."""
    return conftest.md_size(stores, local=300, motherduck=180)


@pytest.fixture
def populated(stores, request):
    dsn = os.getenv("CDC_TEST_PG_DSN")
    if not dsn:
        pytest.skip("set CDC_TEST_PG_DSN for real PostgreSQL probes")
    s, _ = stores
    with Source(dsn) as source:
        schema = source.schema
        source.execute("SET lock_timeout='20s'")
        source.execute("CREATE TABLE big (id int PRIMARY KEY, v text, note text)")
        source.execute("ALTER TABLE big REPLICA IDENTITY FULL")
        source.execute(
            "INSERT INTO big SELECT i, CASE WHEN i %% 7 = 0 THEN repeat(md5(i::text), 80) "
            "ELSE 'v' || i END, NULL FROM generate_series(1, %s) i",
            [big_rows(stores)],
        )
        source.execute("CREATE TABLE pairs (a int, b text, v int, PRIMARY KEY (a, b))")
        source.execute("ALTER TABLE pairs REPLICA IDENTITY FULL")
        source.execute("INSERT INTO pairs SELECT i % 5, 'k' || i, i FROM generate_series(1, 120) i")
        tables = (f"{schema}.big", f"{schema}.pairs")
        ingest = IngestConfig(
            "backfill",
            "pb_" + schema,
            "pb_" + schema,
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
        config = BackfillConfig(
            "backfill",
            tables,
            database=s.database.strip('"'),
            mirror_database=s.mirror_database.strip('"'),
            heartbeat=ingest.heartbeat,
            chunk_rows=40,
            # Small enough that chunks with TOASTed rows stop on bytes, not rows.
            max_chunk_bytes=8_000,
            marker_wait_seconds=0,
            max_batch_events=50,
            allow_local=True,
            dsn=dsn,
        )
        other = Session(dsn, schema)
        backfills = []
        try:
            log, info = bootstrap_log(s.con, ingest, create_objects=True)
            ingest = log.config  # the IngestConfig2 recorded at bootstrap (binding, units)
            backfill = Backfill(s.con, config)
            backfill.initialize()
            backfills.append(backfill)
            yield source, other, ingest, log, backfill
        finally:
            for b in backfills:
                b.close()
            other.close()
            conftest.wait_slot_released(source.con, ingest.slot)
            query(
                source.con,
                "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
                "WHERE (slot_name=%s OR slot_name LIKE %s) AND NOT active",
                # Epoch tests create <slot>_e<n>: a failure before their own cleanup
                # would otherwise leak WAL.
                [ingest.slot, ingest.slot + "\\_e%"],
            )
            query(
                source.con,
                sql.SQL("DROP PUBLICATION IF EXISTS {}").format(sql.Identifier(ingest.publication)),
            )


class BigWorkload:
    """Writes that land inside, ahead of and behind the chunk being read."""

    def __init__(self, source, seed, rows=300):
        self.source, self.rng = source, random.Random(seed)
        self.keys = set(range(1, rows + 1))
        # Free keys for inserts and key moves: half as many again as the starting rows.
        self.key_space = rows * 3 // 2
        self.extra = False
        # Keys touched by a transaction that is still open in another session.
        self.held = set()

    def free(self):
        return [
            k for k in range(1, self.key_space + 1) if k not in self.keys and k not in self.held
        ]

    def step(self, x, hold=False):
        r = self.rng
        key = r.choice(sorted(self.keys - self.held))
        choice = r.random()
        before = set(self.keys)
        if choice < 0.25:
            x("UPDATE big SET note=%s WHERE id=%s", [r.choice(["n", None]), key])
        elif choice < 0.35:
            x("UPDATE big SET v=%s WHERE id=%s", [r.randbytes(3000).hex(), key])
        elif choice < 0.5 and self.extra:
            x("UPDATE big SET extra=%s WHERE id=%s", [r.choice(["e", None]), key])
        elif choice < 0.65:
            # Moves rows between the scanned and unscanned key ranges.
            new = r.choice(self.free())
            x("UPDATE big SET id=%s WHERE id=%s", [new, key])
            self.keys.remove(key)
            self.keys.add(new)
        elif choice < 0.8:
            x("DELETE FROM big WHERE id=%s", [key])
            self.keys.remove(key)
        else:
            new = r.choice(self.free())
            x("INSERT INTO big (id,v,note) VALUES (%s,%s,'i')", [new, f"v{new}"])
            self.keys.add(new)
        if hold:
            self.held |= {key} | (self.keys ^ before)

    def transaction(self, x, count=3):
        x("BEGIN")
        for _ in range(count):
            self.step(x)
        x("COMMIT")


def drive(backfill, table, ingest, log, *, until="published", limit=200, between=None):
    for _ in range(limit):
        result = backfill.run_table(table, max_chunks=1)
        if result.status == until or result.status in ("failed", "published"):
            return result
        if between:
            between(result)
        if result.status in ("waiting_for_marker", "waiting_for_ingest", "catching_up"):
            assert run_once(ingest, log).status == "drained"
    raise AssertionError("backfill did not finish")


@pytest.mark.postgres
def test_resync_of_a_held_table_clears_both_blocked_positions(stores, populated):
    """A keyed resync publishes the table live with no blocked commit AND no blocked
    unit start: ops_apply and table_actions read blocked_unit_start_lsn, so a stale
    one would describe a hold the new generation never had."""
    source, _, ingest, log, backfill = populated
    pairs = f"{source.schema}.pairs"
    relation_id, _ = backfill.registered(pairs)
    apply = ApplyRun(stores[0].con, replace(backfill.config, tables=(pairs,)))
    apply.start_from_log(pairs, accept_incomplete=True)
    state = f"{backfill.mirror}._cdc.apply_state"
    stores[0].con.execute(
        f"UPDATE {state} SET state='needs_resync',detail='held',blocked_commit_lsn=200,"
        "blocked_unit_start_lsn=100,held_since=current_timestamp WHERE relation_id=?",
        [relation_id],
    )
    assert drive(backfill, pairs, ingest, log).status == "published"
    row = (
        stores[0]
        .con.execute(
            f"SELECT state,detail,blocked_commit_lsn,blocked_unit_start_lsn,held_since "
            f"FROM {state} WHERE relation_id=?",
            [relation_id],
        )
        .fetchone()
    )
    assert row == ("live", None, None, None, None), row


@pytest.mark.postgres
def test_backfill_under_load_crashes_and_publication(stores, populated):
    source, other, ingest, log, backfill = populated
    schema = source.schema
    big = f"{schema}.big"
    apply = ApplyRun(stores[0].con, backfill.config)
    with pytest.raises(ApplyError, match="not observed empty"):
        apply.start_from_log(big)
    workload = BigWorkload(source, seed=11, rows=big_rows(stores))

    # Chunk 1, then a crash inside the chunk transaction (rolled back) and one
    # after its commit (kept); the resumed scan neither skips nor duplicates.
    assert backfill.run_table(big, max_chunks=1).status == "snapshotting"

    def crash(at):
        def fault(stage):
            if stage == at:
                raise RuntimeError(at)

        return fault

    gen = backfill.active(backfill.registered(big)[0])
    with pytest.raises(RuntimeError, match="after_chunk_insert"):
        backfill.run_table(big, max_chunks=1, fault=crash("after_chunk_insert"))
    assert backfill.generation(gen["generation_id"])["chunks"] == 1
    with pytest.raises(RuntimeError, match="after_chunk_commit"):
        backfill.run_table(big, max_chunks=1, fault=crash("after_chunk_commit"))
    assert backfill.generation(gen["generation_id"])["chunks"] == 2

    # A transaction opened before a chunk read and committed after it.
    open_txn = {"active": False}

    def between(result):
        if result.status != "snapshotting":
            if open_txn["active"]:
                other.execute("COMMIT")
                open_txn["active"] = False
                workload.held.clear()
            if not workload.extra:
                # Additive DDL during catch-up reaches staging through the log.
                source.execute("ALTER TABLE big ADD COLUMN extra text")
                workload.extra = True
            workload.transaction(source.execute)
            return
        if open_txn["active"]:
            workload.step(other.execute, hold=True)
            other.execute("COMMIT")
            open_txn["active"] = False
            workload.held.clear()
        else:
            other.execute("BEGIN")
            workload.step(other.execute, hold=True)
            open_txn["active"] = True
        for _ in range(2):
            workload.transaction(source.execute)
        if random.Random(len(workload.keys)).random() < 0.5:
            assert run_once(ingest, log).status == "drained"

    result = drive(backfill, big, ingest, log, between=between)
    if open_txn["active"]:
        other.execute("COMMIT")
    assert result.status == "published", result
    assert result.chunks > 5
    published = backfill.generation(result.generation)
    assert published["state"] == "published" and published["catchup_lsn"] == result.boundary
    assert apply.state(backfill.registered(big)[0])["applied"] == result.boundary

    # Apply continues from H; the mirror equals the source at a quiescent boundary.
    for _ in range(3):
        workload.transaction(source.execute)
    assert run_once(ingest, log).status == "drained"
    [big_result, pairs_result] = ApplyRun(
        stores[0].con, replace(backfill.config, tables=(big,))
    ).run() + [None]
    assert big_result.status == "caught_up"
    assert_mirror_equals_source(apply, source, big)
    staging = (
        stores[0]
        .con.execute(
            "SELECT count(*) FROM duckdb_tables() WHERE database_name=? AND table_name LIKE 'big__gen_%'",
            [backfill.config.mirror_database],
        )
        .fetchone()[0]
    )
    assert staging == 0


@pytest.mark.postgres
def test_resync_replaces_stale_mirror_and_fences_old_writers(stores, populated):
    source, _, ingest, log, backfill = populated
    pairs = f"{source.schema}.pairs"
    relation_id, _ = backfill.registered(pairs)
    apply = ApplyRun(stores[0].con, replace(backfill.config, tables=(pairs,)))
    # A log-only mirror that knowingly lacks the pre-existing rows.
    apply.start_from_log(pairs, accept_incomplete=True)
    source.execute("UPDATE pairs SET v=v+1000 WHERE a=1")
    source.execute("INSERT INTO pairs VALUES (9,'new',1)")
    assert run_once(ingest, log).status == "drained"
    assert apply.run()[0].status == "caught_up"
    old = apply.state(relation_id)
    assert stores[0].con.execute(f"SELECT count(*) FROM {old['target']}").fetchone()[0] < 121

    # Abandon a generation mid-scan: staging is dropped and a new one can start.
    first = backfill.run_table(pairs, max_chunks=1)
    assert first.status == "snapshotting"
    backfill.abandon(pairs)
    assert backfill.generation(first.generation)["state"] == "abandoned"
    assert backfill.active(relation_id) is None
    # Abandon releases its lease, as run_table does: no holder is left.
    assert backfill.generation(first.generation)["holder"] is None

    # A stale worker of the abandoned generation cannot write, even with the
    # generation's current token: the guard requires an active state.
    current_token = (
        stores[0]
        .con.execute(
            f"SELECT token FROM {backfill.mirror}._cdc.generations WHERE generation_id=?",
            [first.generation],
        )
        .fetchone()[0]
    )
    with pytest.raises(FenceLost):
        with backfill.transaction():
            backfill.guard_generation(first.generation, current_token)

    # Resync while the source keeps changing; apply keeps running on the old mirror.
    def between(result):
        source.execute("UPDATE pairs SET v=v+1 WHERE a=2 AND b IN ('k2','k7')")
        source.execute("DELETE FROM pairs WHERE a=3 AND b='k3'")
        apply.run()

    result = drive(backfill, pairs, ingest, log, until="waiting_for_marker", between=between)
    assert result.status == "waiting_for_marker"
    resync = result.generation
    assert run_once(ingest, log).status == "drained"
    # Publication waits for the table's apply lease; nothing changes meanwhile.
    token = apply.acquire(relation_id, "held")
    result = backfill.run_table(pairs, start=False)
    assert result.status == "busy_publish" and result.generation == resync
    assert apply.state(relation_id)["generation"] == old["generation"]
    apply.release(relation_id, token)

    def crash(at):
        def fault(stage):
            if stage == at:
                raise RuntimeError(at)

        return fault

    # A failure after the RENAME rolls publication back entirely.
    with pytest.raises(RuntimeError, match="after_rename"):
        backfill.run_table(pairs, fault=crash("after_rename"))
    assert backfill.active(relation_id)["state"] == "catching_up"
    assert apply.state(relation_id)["generation"] == old["generation"]
    # A lost response after commit: publication happened exactly once, and a
    # resume-only retry does not start another generation.
    with pytest.raises(RuntimeError, match="after_publish_commit"):
        backfill.run_table(pairs, fault=crash("after_publish_commit"))
    assert backfill.active(relation_id) is None
    assert backfill.run_table(pairs, start=False).status == "idle"
    result = backfill.result(pairs, resync, "published")
    assert apply.state(relation_id)["generation"] == resync
    assert backfill.generation(resync)["state"] == "published"
    assert backfill.generation(old["generation"])["state"] == "replaced"
    new = apply.state(relation_id)
    assert new["generation"] == result.generation and new["state"] == "live"
    # The replaced generation's apply checkpoint can no longer be written.
    with pytest.raises(FenceLost):
        token = apply.acquire(relation_id, "stale")
        try:
            with apply.transaction():
                apply._set_state(
                    relation_id,
                    token,
                    old,
                    boundary=old["applied"],
                    columns=old["columns"],
                    state="live",
                    detail=None,
                    commit=None,
                    code=None,
                )
        finally:
            apply.release(relation_id, token)
    source.execute("UPDATE pairs SET v=-1 WHERE a=4")
    assert run_once(ingest, log).status == "drained"
    assert apply.run()[0].status == "caught_up"
    assert_mirror_equals_source(apply, source, pairs)


@pytest.mark.postgres
def test_truncate_during_backfill_fails_the_generation(stores, populated):
    source, _, ingest, log, backfill = populated
    big = f"{source.schema}.big"
    relation_id, _ = backfill.registered(big)
    assert backfill.run_table(big, max_chunks=2).status == "snapshotting"
    source.execute("TRUNCATE big")
    result = drive(backfill, big, ingest, log)
    assert result.status == "failed" and "TRUNCATE" in result.detail
    assert backfill.active(relation_id) is None
    assert ApplyRun(stores[0].con, backfill.config).state(relation_id) is None
    staging = (
        stores[0]
        .con.execute(
            "SELECT count(*) FROM duckdb_tables() WHERE database_name=? AND table_name LIKE 'big__gen_%'",
            [backfill.config.mirror_database],
        )
        .fetchone()[0]
    )
    assert staging == 0


def staging_tables(stores, backfill, name):
    return (
        stores[0]
        .con.execute(
            "SELECT count(*) FROM duckdb_tables() WHERE database_name=? AND table_name LIKE ?",
            [backfill.config.mirror_database, name + "__gen_%"],
        )
        .fetchone()[0]
    )


@pytest.mark.postgres
def test_schema_drift_mid_snapshot_fails_then_retry_publishes(stores, populated):
    source, _, ingest, log, backfill = populated
    big = f"{source.schema}.big"
    relation_id, _ = backfill.registered(big)
    first = backfill.run_table(big, max_chunks=1)
    assert first.status == "snapshotting"
    source.execute("ALTER TABLE big DROP COLUMN note")
    result = backfill.run_table(big, max_chunks=1)
    assert result.status == "failed" and "column 'note'" in result.detail
    assert backfill.active(relation_id) is None and staging_tables(stores, backfill, "big") == 0
    # Once ingest has passed the DDL, a new generation builds without the column.
    source.execute("UPDATE big SET v='after' WHERE id=5")
    assert run_once(ingest, log).status == "drained"
    result = drive(backfill, big, ingest, log)
    assert result.status == "published" and result.generation != first.generation
    assert result.chunks > 5  # the retry still scans several chunks at big_rows on MotherDuck
    apply = ApplyRun(stores[0].con, replace(backfill.config, tables=(big,)))
    source.execute("DELETE FROM big WHERE id=6")
    assert run_once(ingest, log).status == "drained"
    assert apply.run()[0].status == "caught_up"
    assert_mirror_equals_source(apply, source, big)


@pytest.mark.postgres
def test_key_change_on_full_identity_holds_apply_then_resync_rekeys(stores, populated):
    """REPLICA IDENTITY FULL Relation messages flag every column, so only ingest's
    catalog key tracking can see a primary-key change."""
    source, _, ingest, log, backfill = populated
    pairs = f"{source.schema}.pairs"
    relation_id, _ = backfill.registered(pairs)
    apply = ApplyRun(stores[0].con, replace(backfill.config, tables=(pairs,)))
    assert drive(backfill, pairs, ingest, log).status == "published"
    source.execute("UPDATE pairs SET v=v+1 WHERE a=0")
    assert run_once(ingest, log).status == "drained"
    assert apply.run()[0].status == "caught_up"
    assert_mirror_equals_source(apply, source, pairs)
    before = (
        stores[0]
        .con.execute(f"SELECT count(*) FROM {apply.state(relation_id)['target']}")
        .fetchone()[0]
    )

    # The key widens, so rows duplicating the old key become legal.
    source.execute("ALTER TABLE pairs DROP CONSTRAINT pairs_pkey")
    source.execute("ALTER TABLE pairs ADD PRIMARY KEY (a, b, v)")
    # Not yet recorded by ingest: a backfill on the new key is refused.
    with pytest.raises(ApplyError, match="run ingest"):
        backfill.run_table(pairs)
    source.execute("INSERT INTO pairs SELECT a, b, v + 1000 FROM pairs WHERE a=1")
    assert run_once(ingest, log).status == "drained"
    result = apply.run()[0]
    assert result.status == "needs_resync" and "key changed" in result.detail
    target = apply.state(relation_id)["target"]
    assert stores[0].con.execute(f"SELECT count(*) FROM {target}").fetchone()[0] == before

    # A resync rebuilds the mirror on the new key; apply then continues.
    result = drive(backfill, pairs, ingest, log)
    assert result.status == "published"
    assert apply.state(relation_id)["keys"] == ("a", "b", "v")
    source.execute("INSERT INTO pairs SELECT a, b, v + 2000 FROM pairs WHERE a=2")
    assert run_once(ingest, log).status == "drained"
    assert apply.run()[0].status == "caught_up"
    assert_mirror_equals_source(apply, source, pairs)


@pytest.mark.postgres
def test_key_change_and_revert_is_still_a_key_change(stores, populated):
    """A -> B -> A: the mirror's key equals the catalog key again, but B-era
    changes were made under another key. Apply must hold, not fold them on A."""
    source, _, ingest, log, backfill = populated
    pairs = f"{source.schema}.pairs"
    relation_id, _ = backfill.registered(pairs)
    apply = ApplyRun(stores[0].con, replace(backfill.config, tables=(pairs,)))
    assert drive(backfill, pairs, ingest, log).status == "published"
    assert apply.state(relation_id)["key_version"] == 0
    source.execute("ALTER TABLE pairs DROP CONSTRAINT pairs_pkey")
    source.execute("ALTER TABLE pairs ADD PRIMARY KEY (a, b, v)")
    original = source.execute("SELECT v FROM pairs WHERE a=1 AND b='k1'")[0][0]
    source.execute("INSERT INTO pairs VALUES (1,'k1',999)")
    source.execute("DELETE FROM pairs WHERE a=1 AND b='k1' AND v=%s", [original])
    assert run_once(ingest, log).status == "drained"
    source.execute("ALTER TABLE pairs DROP CONSTRAINT pairs_pkey")
    source.execute("ALTER TABLE pairs ADD PRIMARY KEY (a, b)")
    assert run_once(ingest, log).status == "drained"
    version, key = log.execute(
        "SELECT key_version,catalog_key FROM {s}tables WHERE relation_id=?", [relation_id]
    ).fetchone()
    assert version == 2 and json.loads(key) == ["a", "b"]  # compact JSON on format 2
    result = apply.run()[0]
    assert result.status == "needs_resync", result
    # A resync under the current version re-establishes an exact mirror.
    result = drive(backfill, pairs, ingest, log)
    assert result.status == "published"
    assert apply.state(relation_id)["key_version"] == 2
    source.execute("UPDATE pairs SET v=v+1 WHERE a=3")
    assert run_once(ingest, log).status == "drained"
    assert apply.run()[0].status == "caught_up"
    assert_mirror_equals_source(apply, source, pairs)


def generation_pins(backfill, relation_id):
    return sorted(g for g, _ in backfill.retention.generation_pins(relation_id))


@pytest.mark.postgres
@pytest.mark.md_gate
def test_generation_pins_register_release_and_sweep(stores, populated):
    source, _, ingest, log, backfill = populated
    pairs = f"{source.schema}.pairs"
    relation_id, _ = backfill.registered(pairs)
    retention = backfill.retention

    def crash(at):
        def fault(stage):
            if stage == at:
                raise RuntimeError(at)

        return fault

    # The pin commits before the generation: a crash between leaves an orphan pin
    # (extra history) and no generation.
    with pytest.raises(RuntimeError, match="after_pin"):
        backfill.run_table(pairs, max_chunks=1, fault=crash("after_pin"))
    assert backfill.active(relation_id) is None
    [orphan] = generation_pins(backfill, relation_id)
    # Within the lease TTL it may be a start in flight, so it is kept.
    first = backfill.run_table(pairs, max_chunks=1)
    assert first.status == "snapshotting"
    assert generation_pins(backfill, relation_id) == sorted([orphan, first.generation])
    gen = backfill.generation(first.generation)
    pin = retention.pin(generation_owner(first.generation))
    assert (pin["epoch"], pin["lsn"]) == (gen["epoch"], gen["pin_lsn"])

    # Abandonment releases the pin after its mirror commit; a crash between keeps it.
    with pytest.raises(RuntimeError, match="after_end_commit"):
        backfill.abandon(pairs, fault=crash("after_end_commit"))
    assert backfill.generation(first.generation)["state"] == "abandoned"
    assert first.generation in generation_pins(backfill, relation_id)
    # The next start sweeps the abandoned generation's pin and the expired orphan.
    log.execute(
        "UPDATE {s}retention_pins SET created_at=created_at-INTERVAL 1 DAY WHERE generation_id=?",
        [orphan],
    )
    second = backfill.run_table(pairs, max_chunks=1)
    assert generation_pins(backfill, relation_id) == [second.generation]

    # A missing generation pin fails the generation (closed) before the next read.
    retention.release(generation_owner(second.generation))
    failed = backfill.run_table(pairs, max_chunks=1)
    assert failed.status == "failed" and "retention pin" in failed.detail
    assert staging_tables(stores, backfill, "pairs") == 0

    # Failure releases the pin.
    third = backfill.run_table(pairs, max_chunks=1)
    source.execute("TRUNCATE pairs")
    assert drive(backfill, pairs, ingest, log).status == "failed"
    assert backfill.generation(third.generation)["state"] == "failed"
    assert generation_pins(backfill, relation_id) == []


@pytest.mark.postgres
@pytest.mark.md_gate
def test_publication_transfers_pin_and_replaced_pins_do_not_leak(stores, populated):
    source, _, ingest, log, backfill = populated
    pairs = f"{source.schema}.pairs"
    relation_id, _ = backfill.registered(pairs)
    retention = backfill.retention
    apply = ApplyRun(stores[0].con, replace(backfill.config, tables=(pairs,)))
    owner = table_owner(relation_id)

    def crash(at):
        def fault(stage):
            if stage == at:
                raise RuntimeError(at)

        return fault

    def publish(fault):
        result = drive(backfill, pairs, ingest, log, until="waiting_for_marker")
        assert result.status == "waiting_for_marker"
        assert run_once(ingest, log).status == "drained"
        with pytest.raises(RuntimeError):
            backfill.run_table(pairs, start=False, fault=fault)
        return result.generation

    # A failure after the RENAME rolls back: the pins are unchanged.
    g1 = publish(crash("after_rename"))
    assert retention.pin(owner) is None and generation_pins(backfill, relation_id) == [g1]
    # A lost response after the publication commit: the transfer did not run, and
    # the generation pin at L still protects everything apply needs.
    with pytest.raises(RuntimeError, match="after_publish_commit"):
        backfill.run_table(pairs, start=False, fault=crash("after_publish_commit"))
    assert apply.state(relation_id)["generation"] == g1
    assert retention.pin(owner) is None and generation_pins(backfill, relation_id) == [g1]

    # The table is held, so apply never completes that transfer. A resync then
    # publishes G2: its transfer and sweep leave no generation pins behind.
    stores[0].con.execute(
        f"UPDATE {backfill.mirror}._cdc.apply_state SET state='needs_action' WHERE relation_id=?",
        [relation_id],
    )
    assert apply.run()[0].status == "needs_action"
    source.execute("UPDATE pairs SET v=v+1 WHERE a=1")
    # G1 is published and current: its pin is the table's only protection, so a
    # sweep at G2's start must keep it.
    assert backfill.run_table(pairs, max_chunks=1).status == "snapshotting"
    assert g1 in generation_pins(backfill, relation_id)
    g2 = drive(backfill, pairs, ingest, log).generation
    assert backfill.generation(g1)["state"] == "replaced"
    assert generation_pins(backfill, relation_id) == []
    pin = retention.pin(owner)
    assert pin["generation"] == g2 and pin["lsn"] == backfill.generation(g2)["marker_lsn"]

    # Apply from H advances the transferred pin; nothing pins the log below it.
    source.execute("DELETE FROM pairs WHERE a=2")
    assert run_once(ingest, log).status == "drained"
    assert apply.run()[0].status == "caught_up"
    assert retention.pin(owner)["lsn"] == apply.state(relation_id)["applied"]
    assert sorted(retention.pins().values())[0][1] > backfill.generation(g1)["pin_lsn"]
    assert_mirror_equals_source(apply, source, pairs)


@pytest.mark.postgres
@pytest.mark.md_gate
def test_interrupted_transfer_is_completed_by_apply(stores, populated):
    source, _, ingest, log, backfill = populated
    big = f"{source.schema}.big"
    relation_id, _ = backfill.registered(big)
    apply = ApplyRun(stores[0].con, replace(backfill.config, tables=(big,)))
    result = drive(backfill, big, ingest, log, until="waiting_for_marker")
    assert run_once(ingest, log).status == "drained"

    def lost(stage):
        if stage == "after_publish_commit":
            raise RuntimeError(stage)

    with pytest.raises(RuntimeError):
        backfill.run_table(big, start=False, fault=lost)
    assert backfill.retention.pin(table_owner(relation_id)) is None
    source.execute("UPDATE big SET note='after' WHERE id<10")
    assert run_once(ingest, log).status == "drained"
    assert apply.run()[0].status == "caught_up"
    pin = backfill.retention.pin(table_owner(relation_id))
    assert pin["generation"] == result.generation
    assert pin["lsn"] == apply.state(relation_id)["applied"]
    assert generation_pins(backfill, relation_id) == []
    assert_mirror_equals_source(apply, source, big)


@pytest.mark.postgres
def test_ambiguous_backfill_start_keeps_its_pin(stores, populated, monkeypatch):
    source, _, ingest, log, backfill = populated
    pairs = f"{source.schema}.pairs"
    relation_id, _ = backfill.registered(pairs)
    real = backfill._start

    def lost(*args):
        real(*args)
        raise ConnectionError("commit response lost")

    with monkeypatch.context() as patch:
        patch.setattr(backfill, "_start", lost)
        with pytest.raises(ConnectionError):
            backfill.run_table(pairs, max_chunks=1)
    gen = backfill.active(relation_id)
    assert generation_pins(backfill, relation_id) == [gen["generation_id"]]
    # The committed generation resumes under its pin, and publishes.
    result = backfill.run_table(pairs, max_chunks=1, start=False)
    assert result.status == "snapshotting" and result.generation == gen["generation_id"]
    assert drive(backfill, pairs, ingest, log).status == "published"


class StaleCatalog:
    """A connection whose client catalog never lists tables (duckdb_tables() is
    empty), as a stale MotherDuck catalog can be. Every other statement runs."""

    def __init__(self, con):
        self.con = con

    def execute(self, statement, parameters=None):
        if "duckdb_tables()" in statement:
            return self.con.execute("SELECT 0 WHERE false UNION ALL SELECT 0")
        return self.con.execute(statement, parameters or [])

    def __getattr__(self, name):
        return getattr(self.con, name)


@pytest.mark.postgres
@pytest.mark.parametrize(
    "stores,stale",
    [("local", False), ("local", True), ("motherduck", True)],
    indirect=["stores"],
    ids=["local-fresh_catalog", "local-stale_catalog", "motherduck-stale_catalog"],
)
def test_publish_never_replaces_a_table_without_apply_state(stores, populated, stale):
    """Another client created a table at the mirror's name. Publication fails the
    generation and keeps that table, also when this client's catalog misses it."""
    source, _, ingest, log, backfill = populated
    pairs = f"{source.schema}.pairs"
    target = f'"{backfill.config.mirror_database}"."{source.schema}"."pairs"'
    other = stores[1].con
    other.execute(
        f'CREATE SCHEMA IF NOT EXISTS "{backfill.config.mirror_database}"."{source.schema}"'
    )
    other.execute(f"CREATE TABLE {target} (mine VARCHAR)")
    other.execute(f"INSERT INTO {target} VALUES ('keep me')")
    if stale:
        backfill = Backfill(StaleCatalog(stores[0].con), backfill.config)
    result = drive(backfill, pairs, ingest, log)
    assert result.status == "failed" and "refusing to replace" in result.detail
    assert other.execute(f"SELECT * FROM {target}").fetchall() == [("keep me",)]
    relation_id, _ = backfill.registered(pairs)
    assert backfill.state(relation_id) is None
    # The failure leaves no history pinned: no generation pin, and no table pin.
    pins = backfill.retention.pins()
    assert not any(owner.startswith("generation:") for owner in pins), pins
    assert table_owner(relation_id) not in pins
