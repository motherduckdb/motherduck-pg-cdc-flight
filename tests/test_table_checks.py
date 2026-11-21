"""Every per-table preflight refusal holds only that table (the follow-up to the table
holds): the rest of the source keeps running and the slot keeps advancing. When the
DBA fixes the table, its hold clears and a new key version makes apply hold
`needs_resync` (changes logged meanwhile may carry incomplete images), so a backfill
rebuilds the mirror. Format 2: the version's cut is the table's last verified unit end
before the hold (its key_checked_lsn froze while held), so apply may first finish the
units verified before the hold, never one logged during it. Source-wide refusals still
stop the source.
"""

import pytest
from psycopg2 import sql
from test_backfill import drive
from test_phase2 import assert_mirror_equals_source
from test_schema_cases import schema_source  # noqa: F401 (fixture)

from cdc_flight import preflight
from cdc_flight.apply import ApplyError
from cdc_flight.ingest import run_once
from cdc_flight.preflight import PreflightError, TableInvalid, query


def slot_position(source, ingest):
    return int(
        query(
            source.con,
            "SELECT confirmed_flush_lsn-'0/0'::pg_lsn FROM pg_replication_slots WHERE slot_name=%s",
            [ingest.slot],
        )[0][0]
    )


def ingest_once(ingest, log, **options):
    result = run_once(ingest, log, **options)
    assert result.status == "drained", result
    return result


def held(result, table):
    return next((h for h in result.held if h["table"] == table), None)


def recovers(source, ingest, log, backfill, apply, table):
    """After the DBA's fix: the hold clears, apply holds needs_resync at once, and a
    backfill of the new key version makes the mirror equal the source."""
    relation_id, _ = apply.registered(table)
    applied = apply.state(relation_id)["applied"]
    result = ingest_once(ingest, log)
    assert held(result, table) is None, result.held
    (cut,) = log.execute(
        "SELECT cut_lsn FROM {s}key_changes WHERE relation_id=? ORDER BY version DESC LIMIT 1",
        [relation_id],
    ).fetchone()
    assert apply.apply_table(table, "after-fix").status == "needs_resync"
    # Held at the frozen cut: units verified before the hold may apply, nothing
    # logged during the hold or since.
    assert applied <= apply.state(relation_id)["applied"] == cut < result.boundary
    assert drive(backfill, table, ingest, log).status == "published"
    ingest_once(ingest, log)
    assert apply.apply_table(table, "after-resync").status == "caught_up"
    assert_mirror_equals_source(apply, source, table)


@pytest.mark.postgres
def test_replica_identity_lost_mid_run_holds_alone_and_forces_a_resync(stores, schema_source):  # noqa: F811
    """The DBA resets REPLICA IDENTITY during a run and updates a row: that run logs an
    image without its unchanged TOAST-capable values and confirms keys through its end.
    Apply may even apply it before the next run holds the table, so the cleared hold's
    cut is 0: needs_resync at once."""
    source, ingest, log, backfill, apply = schema_source
    schema = source.schema
    table, other = f"{schema}.t_default", f"{schema}.t_drop"
    for t in (table, other):
        assert drive(backfill, t, ingest, log).status == "published"
    changed = []

    def reset(stage):
        if stage == "after_append" and not changed:
            changed.append(1)
            source.execute("ALTER TABLE t_default REPLICA IDENTITY DEFAULT")
            source.execute("UPDATE t_default SET n=n+1 WHERE id=1")

    source.execute("INSERT INTO t_drop VALUES (900,'a batch',900)")
    ingest_once(ingest, log, fault=reset)
    assert changed
    apply.apply_table(table, "between")  # it may apply the incomplete image: allowed
    before = slot_position(source, ingest)
    source.execute("INSERT INTO t_drop VALUES (901,'others continue',901)")
    result = ingest_once(ingest, log)
    hold = held(result, table)
    assert hold["reason"] == "identity_not_full", result.held
    assert slot_position(source, ingest) == result.boundary > before
    assert apply.apply_table(other, "run").status == "caught_up"
    assert apply.apply_table(table, "run").status == "source_missing"
    with pytest.raises(ApplyError, match="identity_not_full"):
        backfill.start(table)
    source.execute("ALTER TABLE t_default REPLICA IDENTITY FULL")
    recovers(source, ingest, log, backfill, apply, table)


@pytest.mark.postgres
def test_a_dropped_primary_key_holds_alone_until_restored(stores, schema_source):  # noqa: F811
    source, ingest, log, backfill, apply = schema_source
    table = f"{source.schema}.t_type"
    assert drive(backfill, table, ingest, log).status == "published"
    source.execute("ALTER TABLE t_type DROP CONSTRAINT t_type_pkey")
    source.execute("UPDATE t_type SET a='keyless' WHERE id=2")
    result = ingest_once(ingest, log)
    assert held(result, table)["reason"] == "no_key"
    assert apply.apply_table(table, "run").status == "source_missing"
    source.execute("ALTER TABLE t_type ADD PRIMARY KEY (id)")
    recovers(source, ingest, log, backfill, apply, table)


@pytest.mark.postgres
def test_a_publication_row_filter_on_one_table_holds_it(stores, schema_source):  # noqa: F811
    source, ingest, log, backfill, apply = schema_source
    if int(query(source.con, "SHOW server_version_num")[0][0]) < 150000:
        pytest.skip("row filters need PostgreSQL 15")
    table = f"{source.schema}.t_rename"
    assert drive(backfill, table, ingest, log).status == "published"
    publication = sql.Identifier(ingest.publication)
    source.execute(sql.SQL("ALTER PUBLICATION {} DROP TABLE t_rename").format(publication))
    source.execute(
        sql.SQL("ALTER PUBLICATION {} ADD TABLE t_rename WHERE (n > 3)").format(publication)
    )
    result = ingest_once(ingest, log)
    hold = held(result, table)
    assert hold["reason"] == "publication_filter" and "row filter" in hold["detail"]
    assert "n > 3" not in hold["detail"]  # the kind, never the SQL
    source.execute(sql.SQL("ALTER PUBLICATION {} DROP TABLE t_rename").format(publication))
    source.execute(sql.SQL("ALTER PUBLICATION {} ADD TABLE t_rename").format(publication))
    recovers(source, ingest, log, backfill, apply, table)


@pytest.mark.postgres
def test_source_wide_refusals_still_stop_the_source(stores, schema_source):  # noqa: F811
    source, ingest, log, backfill, apply = schema_source
    publication = sql.Identifier(ingest.publication)
    pulse = sql.Identifier(*ingest.heartbeat.split("."))
    source.execute(sql.SQL("ALTER PUBLICATION {} DROP TABLE {}").format(publication, pulse))
    with pytest.raises(PreflightError, match="heartbeat"):
        run_once(ingest, log)
    source.execute(sql.SQL("ALTER PUBLICATION {} ADD TABLE {}").format(publication, pulse))
    ingest_once(ingest, log)


@pytest.mark.parametrize(
    "found, keys, leaves, reason",
    [
        ((1, "v", "f", True), ["id"], [], "invalid_kind"),
        ((1, "r", "f", False), ["id"], [], "no_select"),
        ((1, "r", "n", True), ["id"], [], "no_key"),
        ((1, "r", "f", True), None, [], "no_key"),
        ((1, "p", "f", True), ["id"], [(2, "s.leaf", "d", True, True)], "leaf_invalid"),
        ((1, "p", "f", True), ["id"], [(2, "s.leaf", "f", False, False)], "leaf_invalid"),
    ],
)
def test_every_per_table_check_raises_a_held_reason(monkeypatch, found, keys, leaves, reason):
    """The checks a superuser test connection can't stage on a real server."""

    def fake_query(con, statement, params=None):
        if "pg_partition_tree" in statement:
            return leaves
        if "indisunique" in statement:
            return []  # no_key's advice: no unique index qualifies
        if "current_user" in statement:
            return [("cdc",)]  # no_select's GRANT names the role
        return [(0,)]  # no TOAST-capable columns

    monkeypatch.setattr(preflight, "query", fake_query)
    monkeypatch.setattr(preflight, "catalog_key", lambda con, oid: keys)
    with pytest.raises(TableInvalid) as raised:
        preflight.validate_table(None, "s.t", found)
    assert raised.value.reason == reason


@pytest.mark.postgres
def test_a_generation_running_when_its_table_is_held_fails_before_publishing(
    stores,
    schema_source,  # noqa: F811
):
    """Start refuses a held table, but a generation already running must not catch up
    or publish a mirror from suspect changes: it fails, and a new one starts after the
    fix."""
    source, ingest, log, backfill, apply = schema_source
    table = f"{source.schema}.t_default"
    assert backfill.run_table(table, max_chunks=1).status != "published"
    source.execute("ALTER TABLE t_default REPLICA IDENTITY DEFAULT")
    source.execute("UPDATE t_default SET n=n+1 WHERE id=1")
    assert held(ingest_once(ingest, log), table)["reason"] == "identity_not_full"
    result = drive(backfill, table, ingest, log)
    assert result.status == "failed" and "identity_not_full" in result.detail, result
    assert apply.apply_table(table, "run").status == "source_missing"
    source.execute("ALTER TABLE t_default REPLICA IDENTITY FULL")
    ingest_once(ingest, log)
    assert drive(backfill, table, ingest, log).status == "published"
    ingest_once(ingest, log)
    assert apply.apply_table(table, "run").status == "caught_up"
    assert_mirror_equals_source(apply, source, table)
