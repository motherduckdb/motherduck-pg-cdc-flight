"""The source DDL matrix (docs/tables.md, "Source DDL"), end to end on real PostgreSQL: what
apply does at each source schema change, and that a resync generation then makes the mirror equal the
source (part of the Phase 2 exit criterion (c)).

One table per case, seeded before capture, mirrored by backfill. ADD COLUMN, primary
key changes and TRUNCATE during a backfill are proven elsewhere (the gates and
test_backfill.py); this file covers the rest of the matrix, and observes what
renaming or dropping a configured table does.
"""

import os

import conftest
import pytest
from psycopg2 import sql
from test_backfill import drive
from test_phase2 import assert_mirror_equals_source

from cdc_flight.apply_run import ApplyRun
from cdc_flight.backfill import Backfill
from cdc_flight.bootstrap import bootstrap_log
from cdc_flight.config import BackfillConfig, IngestConfig
from cdc_flight.ingest import run_once
from cdc_flight.preflight import query
from cdc_flight.retention import table_owner
from cdc_flight.source import Source

CASES = ("t_default", "t_type", "t_rename", "t_drop", "t_trunc")


@pytest.fixture
def schema_source(stores, request, drain_only_units):
    dsn = os.getenv("CDC_TEST_PG_DSN")
    if not dsn:
        pytest.skip("set CDC_TEST_PG_DSN for real PostgreSQL probes")
    s, _ = stores
    with Source(dsn) as source:
        schema = source.schema
        source.execute("SET lock_timeout='20s'")
        for name in CASES:
            source.execute(
                sql.SQL("CREATE TABLE {} (id int PRIMARY KEY, a text, n int)").format(
                    sql.Identifier(name)
                )
            )
            source.execute(
                sql.SQL("ALTER TABLE {} REPLICA IDENTITY FULL").format(sql.Identifier(name))
            )
            source.execute(
                sql.SQL("INSERT INTO {} SELECT i,'a'||i,i FROM generate_series(1,8) i").format(
                    sql.Identifier(name)
                )
            )
        tables = tuple(f"{schema}.{name}" for name in CASES)
        ingest = IngestConfig(
            "schema",
            "ps_" + schema,
            "ps_" + schema,
            tables,
            heartbeat=schema + ".pulse",
            database=s.database.strip('"'),
            dsn=dsn,
            allow_local=True,
            flush_seconds=0.1,
            runtime_seconds=60,
        )
        config = BackfillConfig(
            "schema",
            tables,
            database=s.database.strip('"'),
            mirror_database=s.mirror_database.strip('"'),
            heartbeat=ingest.heartbeat,
            chunk_rows=conftest.md_size(stores, local=3, motherduck=20),
            marker_wait_seconds=0,
            allow_local=True,
            dsn=dsn,
        )
        backfill = None
        try:
            log, _ = bootstrap_log(s.con, ingest, create_objects=True)
            ingest = log.config  # the IngestConfig2 recorded at bootstrap (binding, units)
            backfill = Backfill(s.con, config)
            backfill.initialize()
            yield source, ingest, log, backfill, ApplyRun(s.con, config)
        finally:
            if backfill is not None:
                backfill.close()
            conftest.wait_slot_released(source.con, ingest.slot)
            query(
                source.con,
                "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
                "WHERE (slot_name=%s OR slot_name LIKE %s) AND NOT active",
                [ingest.slot, ingest.slot + "\\_e%"],
            )
            query(
                source.con,
                sql.SQL("DROP PUBLICATION IF EXISTS {}").format(sql.Identifier(ingest.publication)),
            )


def wal(source):
    return int(query(source.con, "SELECT pg_current_wal_lsn()-'0/0'::pg_lsn")[0][0])


def state(apply, table):
    relation_id, _ = apply.registered(table)
    return apply.state(relation_id), relation_id


def resync(source, ingest, log, backfill, apply, table):
    """A new generation replaces the mirror; then the mirror equals the source and the
    retention pin belongs to it."""
    # A catalog hold's change is observed at ingest's end read, so a resync waits for
    # the next ingest run to pass it (as production's schedule does).
    run_once(ingest, log)
    result = drive(backfill, table, ingest, log)
    assert result.status == "published", result
    assert run_once(ingest, log).status == "drained"
    assert apply.apply_table(table, "after-resync").status == "caught_up"
    assert_mirror_equals_source(apply, source, table)
    current, relation_id = state(apply, table)
    assert current["generation"] == result.generation
    pin = apply.retention.pin(table_owner(relation_id))
    assert pin["generation"] == result.generation
    assert not [o for o in apply.retention.pins() if o.startswith("generation:")]


@pytest.mark.postgres
def test_schema_cases_hold_where_designed_and_resync_restores_equality(stores, schema_source):
    source, ingest, log, backfill, apply = schema_source
    schema = source.schema
    tables = {name: f"{schema}.{name}" for name in CASES}
    for table in tables.values():
        assert drive(backfill, table, ingest, log).status == "published"
    x = source.execute

    # ADD COLUMN ... DEFAULT on existing rows: no WAL for the fill (the existing rows read
    # a stored default: atthasmissing). Ingest's catalog check holds the table at the
    # last verified cut, needs_resync, so no row is mirrored without
    # it; resync below restores equality.
    x("ALTER TABLE t_default ADD COLUMN d text DEFAULT 'filled'")
    x("UPDATE t_default SET a='updated' WHERE id=1")
    x("INSERT INTO t_default VALUES (100,'new',100)")
    # ALTER COLUMN TYPE: the transaction after holds, with its whole unit; the one
    # before applies when it lies in an earlier unit (format 2 holds whole units).
    before = {"t_type": wal(source)}
    x("INSERT INTO t_type VALUES (100,'before',100)")
    x("ALTER TABLE t_type ALTER COLUMN n TYPE text USING n::text")
    type_from = wal(source)
    x("INSERT INTO t_type VALUES (101,'after','101')")
    type_to = wal(source)
    x("INSERT INTO t_type VALUES (102,'later','102')")
    # RENAME and DROP COLUMN inside one transaction that writes rows on both sides of
    # the DDL: the whole transaction holds, the one before it applies.
    for name, ddl in (
        ("t_rename", "ALTER TABLE t_rename RENAME COLUMN a TO renamed"),
        ("t_drop", "ALTER TABLE t_drop DROP COLUMN a"),
    ):
        before[name] = wal(source)
        x(sql.SQL("INSERT INTO {} VALUES (100,'before',100)").format(sql.Identifier(name)))
        x("BEGIN")
        x(sql.SQL("INSERT INTO {} (id,n) VALUES (101,101)").format(sql.Identifier(name)))
        x(ddl)
        x(sql.SQL("INSERT INTO {} (id,n) VALUES (102,102)").format(sql.Identifier(name)))
        x("COMMIT")
    # TRUNCATE on a live mirror.
    x("TRUNCATE t_trunc")
    x("INSERT INTO t_trunc VALUES (100,'after',100)")

    assert run_once(ingest, log).status == "drained"
    statuses = {name: apply.apply_table(t, "ddl").status for name, t in tables.items()}
    assert statuses == {
        "t_default": "needs_resync",
        "t_type": "needs_action",
        "t_rename": "needs_action",
        "t_drop": "needs_action",
        "t_trunc": "needs_resync",
    }, statuses

    mirror = {name: state(apply, t)[0]["target"] for name, t in tables.items()}
    con = apply.con
    (detail,) = con.execute(
        f"SELECT detail FROM {apply.mirror}._cdc.apply_state WHERE relation_id=?",
        [state(apply, tables["t_default"])[1]],
    ).fetchone()
    assert "added with a default" in detail, detail  # the catalog reason, not "key changed"
    # Held at the cut before the ALTER: nothing of the new column was applied.
    columns = [r[0] for r in con.execute(f"DESCRIBE {mirror['t_default']}").fetchall()]
    assert "d" not in columns
    assert con.execute(f"SELECT max(id) FROM {mirror['t_default']}").fetchone() == (8,)
    held, relation_id = state(apply, tables["t_type"])
    (blocked,) = con.execute(
        f"SELECT blocked_commit_lsn FROM {apply.mirror}._cdc.apply_state WHERE relation_id=?",
        [relation_id],
    ).fetchone()
    # The blocked commit is the first transaction after the ALTER (the diagnosis); the
    # table holds before that commit's whole unit, and nothing from it on applied.
    assert type_from <= blocked < type_to and held["applied"] <= type_from
    for name in ("t_type", "t_rename", "t_drop"):
        # The run's one unit (drain_only_units: it closes only at the drain) holds the
        # DDL rounds with the row before the DDL: nothing of that unit applied (its
        # start is the checkpoint). The applied-prefix case at one commit per unit is
        # test_apply2::test_a_blocking_commit_holds_before_its_whole_unit.
        held, relation_id = state(apply, tables[name])
        (unit_start,) = con.execute(
            f"SELECT blocked_unit_start_lsn FROM {apply.mirror}._cdc.apply_state "
            "WHERE relation_id=?",
            [relation_id],
        ).fetchone()
        assert held["state"] == "needs_action" and held["applied"] == unit_start, name
        assert unit_start <= before[name], name
        assert con.execute(f"SELECT max(id) FROM {mirror[name]}").fetchone() == (8,), name

    for table in tables.values():
        resync(source, ingest, log, backfill, apply, table)
