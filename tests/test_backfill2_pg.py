"""Format 2 (active since the format-2 cutover): one backfill generation end to end on a local
PostgreSQL, through the real run_once with IngestLog2, Backfill2 and Apply2.

L is read and pinned before the snapshot; writes land during the snapshot and the
catch-up; H is the end of the complete unit holding the marker; publish hands Apply2
that unit end, and Apply2 continues on the chain until the mirror equals the source.
"""

import os
import random
from dataclasses import replace

import conftest
import pytest
from psycopg2 import sql
from test_backfill import BigWorkload, Session, big_rows
from test_ingest_pipeline import replication_binding
from test_phase2 import assert_mirror_equals_source

from cdc_flight import format2 as f2
from cdc_flight.apply2 import Apply2
from cdc_flight.backfill2 import Backfill2
from cdc_flight.config import BackfillConfig, IngestConfig
from cdc_flight.ingest import run_once
from cdc_flight.ingest_log2 import IngestConfig2, IngestLog2
from cdc_flight.preflight import check, query
from cdc_flight.retention2 import Retention2
from cdc_flight.source import Source


@pytest.fixture
def populated2(stores):
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
        tables = (f"{schema}.big",)
        ingest = IngestConfig(
            "backfill2",
            "pb2_" + schema,
            "pb2_" + schema,
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
            "backfill2",
            tables,
            database=s.database.strip('"'),
            mirror_database=s.mirror_database.strip('"'),
            heartbeat=ingest.heartbeat,
            chunk_rows=40,
            max_chunk_bytes=8_000,
            marker_wait_seconds=0,
            max_batch_events=50,
            allow_local=True,
            dsn=dsn,
        )
        other = Session(dsn, schema)
        backfills = []
        try:
            info = check(ingest, create_objects=True)
            binding, major = replication_binding(ingest)
            ingest2 = IngestConfig2(
                **ingest.__dict__,
                text_binding=binding,
                server_major=major,
                unit_config=f2.UnitConfig(max_commits=3, max_delay_ms=600_000),
            )
            log = IngestLog2(s.con, ingest2)
            log.initialize()
            log.register(info.identity, info.initial_lsn, info.tables)

            def retention():
                return Retention2(s.con, config.database, config.source_id, config.lease_seconds)

            backfill = Backfill2(s.con, config, retention=retention())
            backfill.initialize()
            backfills.append(backfill)
            yield source, other, ingest2, log, backfill, retention
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


def complete_ends(log):
    return {e for (e,) in log.execute("SELECT end_lsn FROM {s}units_v2 WHERE complete").fetchall()}


@pytest.mark.postgres
def test_a_v2_generation_end_to_end_then_apply_continues(stores, populated2):
    source, other, ingest, log, backfill, retention = populated2
    big = f"{source.schema}.big"
    workload = BigWorkload(source, seed=29, rows=big_rows(stores))
    # Some committed history before the generation: L is a complete unit end.
    for _ in range(4):
        workload.transaction(source.execute)
    assert run_once(ingest, log).status == "drained"
    before = log.checkpoint()

    assert backfill.run_table(big, max_chunks=1).status == "snapshotting"
    gen = backfill.active(backfill.registered(big)[0])
    # L: read and pinned before the snapshot work, at the durable checkpoint.
    assert gen["pin_lsn"] == before and before in complete_ends(log)

    statuses = []
    for _ in range(200):
        result = backfill.run_table(big, max_chunks=1)
        statuses.append(result.status)
        if result.status in ("published", "failed"):
            break
        # Writes during the snapshot and the catch-up, captured in units.
        workload.transaction(source.execute)
        if random.Random(len(statuses)).random() < 0.5 or result.status != "snapshotting":
            assert run_once(ingest, log).status == "drained"
    assert result.status == "published", (result, statuses[-5:])
    assert "snapshotting" in statuses and result.chunks > 3

    published = backfill.generation(result.generation)
    high = published["marker_lsn"]
    # H is a complete unit end at or after the marker, and catch-up reached it.
    assert high in complete_ends(log) and published["catchup_lsn"] == high == result.boundary
    apply = Apply2(stores[0].con, replace(backfill.config, tables=(big,)), retention=retention())
    relation_id = backfill.registered(big)[0]
    assert apply.state(relation_id)["applied"] == high
    # Publication transferred the generation's pin to the table at (epoch, H).
    pin = log.execute(
        "SELECT source_epoch,pinned_lsn FROM {s}retention_pins WHERE owner=?",
        [f"table:{relation_id}"],
    ).fetchone()
    assert pin == (published["epoch"], high)

    # Apply2 continues on the chain from H; the mirror equals the source.
    for _ in range(4):
        workload.transaction(source.execute)
    assert run_once(ingest, log).status == "drained"
    [outcome] = apply.run()
    assert outcome.status == "caught_up", outcome
    assert outcome.boundary == log.checkpoint() and outcome.boundary in complete_ends(log)
    assert_mirror_equals_source(apply, source, big)
