"""Version-specific preflight behavior for the PostgreSQL 13/16/18 matrix."""

from dataclasses import replace

import conftest
import pytest
from psycopg2 import sql

from cdc_flight.config import IngestConfig
from cdc_flight.preflight import PreflightError, check, query
from cdc_flight.source import Source

pytestmark = pytest.mark.postgres


@pytest.fixture
def source(pg_dsn):
    with Source(pg_dsn) as source:
        source.config = IngestConfig(
            "matrix",
            "pm_" + source.schema,
            "pm_" + source.schema,
            (source.schema + ".items",),
            heartbeat=source.schema + ".pulse",
            dsn=pg_dsn,
            allow_local=True,
        )
        try:
            yield source
        finally:
            conftest.wait_slot_released(source.con, source.config.slot)
            query(
                source.con,
                "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
                "WHERE slot_name=%s",
                [source.config.slot],
            )
            source.execute(
                sql.SQL("DROP PUBLICATION IF EXISTS {}").format(
                    sql.Identifier(source.config.publication)
                )
            )


def create_publication(source, options=""):
    source.execute(
        sql.SQL(
            "CREATE PUBLICATION {} FOR TABLE {}, {} WITH (publish_via_partition_root=true{})"
        ).format(
            sql.Identifier(source.config.publication),
            sql.Identifier(source.schema, "items"),
            sql.Identifier(source.schema, "pulse"),
            sql.SQL(options),
        )
    )


def create_pulse(source):
    source.execute("CREATE TABLE pulse (id text PRIMARY KEY, marker text NOT NULL)")


def test_repeat_preflight_over_partitioned_root(source):
    # Before PG15 pg_publication_tables could list leaves; via the root it lists the root.
    source.execute("CREATE TABLE parts (id int PRIMARY KEY, value text) PARTITION BY RANGE(id)")
    source.execute("CREATE TABLE parts_a PARTITION OF parts FOR VALUES FROM (0) TO (100)")
    source.execute("CREATE TABLE parts_b PARTITION OF parts FOR VALUES FROM (100) TO (200)")
    for table in ("parts", "parts_a", "parts_b"):
        source.execute(f"ALTER TABLE {table} REPLICA IDENTITY FULL")
    source.config = replace(source.config, tables=(source.schema + ".parts",))
    first = check(source.config, create_objects=True)
    second = check(source.config)
    assert first.identity == second.identity
    assert [t["name"] for t in second.tables] == ["parts", "pulse"]


def test_schema_publication_is_rejected(source, pg_version):
    if pg_version < 150000:
        pytest.skip("FOR TABLES IN SCHEMA does not exist before PostgreSQL 15")
    create_pulse(source)
    source.execute(
        sql.SQL(
            "CREATE PUBLICATION {} FOR TABLES IN SCHEMA {} WITH (publish_via_partition_root=true)"
        ).format(sql.Identifier(source.config.publication), sql.Identifier(source.schema))
    )
    with pytest.raises(PreflightError, match="not schemas"):
        check(source.config, create_objects=True)


def test_generated_columns_are_not_published(source, pg_version):
    """pgoutput omits generated columns by default on every version, matching the
    ordinary-column set that backfill and the TOAST check read."""
    source.execute("ALTER TABLE items ADD COLUMN doubled int GENERATED ALWAYS AS (id*2) STORED")
    if pg_version >= 180000:
        source.execute(
            "ALTER TABLE items ADD COLUMN tripled int GENERATED ALWAYS AS (id*3) VIRTUAL"
        )
    source.execute("INSERT INTO items VALUES (1, 'a', 'b')")
    # PostgreSQL 13 and 14 also send empty transactions for the DDL; 15+ skip them.
    (events,) = [events for _, events in source.transactions() if events]
    assert [c[1] for c in events[0].relation.columns] == ["id", "value", "note"]
    create_pulse(source)
    create_publication(source)
    assert check(source.config, create_objects=True).tables[0]["name"] == "items"


def test_publish_generated_columns_is_rejected(source, pg_version):
    if pg_version < 180000:
        pytest.skip("publish_generated_columns does not exist before PostgreSQL 18")
    source.execute("ALTER TABLE items ADD COLUMN doubled int GENERATED ALWAYS AS (id*2) STORED")
    create_pulse(source)
    create_publication(source, ", publish_generated_columns=stored")
    with pytest.raises(PreflightError, match="publish_generated_columns"):
        check(source.config, create_objects=True)
