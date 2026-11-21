"""A configured table that disappears holds alone; operators remove or add tables.

The DDL matrix rows (docs/tables.md, "Source DDL") for RENAME TABLE and DROP TABLE, end to end on real PostgreSQL:
the rest of the source keeps running (the slot keeps advancing), the missing table holds
with a durable reason, and `cdc-flight-tables` (membership.py) removes it, relabels a
rename, or adopts a recreated table whose new generation replaces the old mirror.
"""

import json
from dataclasses import replace

import pytest
from psycopg2 import sql
from test_backfill import drive
from test_phase2 import assert_mirror_equals_source
from test_schema_cases import schema_source  # noqa: F401 (fixture)

from cdc_flight import explain
from cdc_flight.apply import ApplyError, TableResult
from cdc_flight.cli import apply_failed
from cdc_flight.ingest import run_once
from cdc_flight.ingest_log import IngestLog
from cdc_flight.membership import add_table, remove_table
from cdc_flight.preflight import PreflightError, query
from cdc_flight.retention import TableHeld, table_owner


def slot_position(source, ingest):
    return int(
        query(
            source.con,
            "SELECT confirmed_flush_lsn-'0/0'::pg_lsn FROM pg_replication_slots WHERE slot_name=%s",
            [ingest.slot],
        )[0][0]
    )


def held_row(log, relation_id):
    return log.execute(
        # Timestamps as text: fetching TIMESTAMPTZ would need pytz; ISO text orders.
        "SELECT held_reason,held_detail,held_since::VARCHAR,held_checked_at::VARCHAR,"
        "retired_by FROM {s}tables "
        "WHERE relation_id=?",
        [relation_id],
    ).fetchone()


def ingest_once(ingest, log):
    result = run_once(ingest, log)
    assert result.status == "drained", result
    return result


def add_to_publication(source, ingest, table):
    source.execute(
        sql.SQL("ALTER PUBLICATION {} ADD TABLE {}").format(
            sql.Identifier(ingest.publication), sql.Identifier(table)
        )
    )


def recreate(source, table):
    source.execute(
        sql.SQL("CREATE TABLE {} (id int PRIMARY KEY, a text, n int)").format(sql.Identifier(table))
    )
    source.execute(sql.SQL("ALTER TABLE {} REPLICA IDENTITY FULL").format(sql.Identifier(table)))


@pytest.mark.postgres
def test_a_dropped_table_holds_alone_and_remove_table_releases_it(stores, schema_source):  # noqa: F811
    source, ingest, log, backfill, apply = schema_source
    schema = source.schema
    gone, other = f"{schema}.t_trunc", f"{schema}.t_default"
    for table in (gone, other):
        assert drive(backfill, table, ingest, log).status == "published"
    relation_id, _ = apply.registered(gone)
    before = slot_position(source, ingest)

    source.execute("DROP TABLE t_trunc")
    source.execute("INSERT INTO t_default VALUES (300,'still captured',300)")
    first = ingest_once(ingest, log)
    # The rest of the source keeps running, and the slot keeps advancing.
    assert slot_position(source, ingest) == first.boundary > before
    assert first.held == [dict(table=gone, reason="dropped", detail=f"{gone} was dropped")]
    assert apply.apply_table(other, "run").status == "caught_up"
    assert_mirror_equals_source(apply, source, other)
    # The hold is durable: first seen stays, last checked moves.
    reason, detail, since, checked, _ = held_row(log, relation_id)
    second = ingest_once(ingest, log)
    assert second.held == first.held
    again = held_row(log, relation_id)
    assert (again[0], again[2]) == (reason, since) and again[3] >= checked
    # Apply reports it (not a failure); backfill refuses it.
    result = apply.apply_table(gone, "run")
    assert result.status == "source_missing" and "dropped" in result.detail
    assert not apply_failed([result])
    assert apply_failed([TableResult(other, "needs_action", 0)])
    assert apply_failed([TableResult(other, "needs_resync", 0)])
    with pytest.raises(ApplyError, match="dropped"):
        backfill.start(gone)
    # The pin registration itself refuses it, under the guard remove-table takes.
    with pytest.raises(TableHeld):
        apply.retention.register_generation(relation_id, "racing", 1, first.boundary)
    # Its pin still holds the log until the operator removes it.
    assert apply.retention.pin(table_owner(relation_id)) is not None

    removed = remove_table(log, apply, gone)
    assert removed.status == "removed", removed
    assert apply.retention.pin(table_owner(relation_id)) is None
    assert held_row(log, relation_id)[0] == "removed"
    assert remove_table(log, apply, gone).status == "already"
    # A stale CDC_TABLES still listing it is ignored and reported.
    third = ingest_once(ingest, log)
    assert third.stale == [gone] and third.held == []
    without = replace(ingest, tables=tuple(t for t in ingest.tables if t != gone))
    assert run_once(without, IngestLog(log.con, without)).status == "drained"


@pytest.mark.postgres
def test_remove_table_is_refused_while_a_generation_is_active_and_force_logs_wal(
    stores,
    schema_source,  # noqa: F811
):
    source, ingest, log, backfill, apply = schema_source
    table = f"{source.schema}.t_drop"
    assert drive(backfill, table, ingest, log).status == "published"
    relation_id, _ = apply.registered(table)
    # Present: only with --force.
    assert remove_table(log, apply, table).status == "refused"
    # A generation in flight must be abandoned first.
    assert backfill.run_table(table, max_chunks=1).status != "published"
    assert backfill.active(relation_id) is not None  # a generation in flight
    refused = remove_table(log, apply, table, force=True)
    assert refused.status == "refused" and "abandon" in refused.detail
    backfill.abandon(table)
    # Changes still in the WAL for the removed table: logged, never applied, no error.
    source.execute("INSERT INTO t_drop VALUES (400,'in the WAL',400)")
    assert remove_table(log, apply, table, force=True).status == "removed"
    assert apply.retention.pin(table_owner(relation_id)) is None
    ingest_once(ingest, log)
    assert apply.apply_table(table, "run").status == "source_missing"


def test_there_is_one_lease_busy_class():
    """ingest_log and apply re-export one class, so no caller can catch the wrong one
    (found under load: membership caught ingest_log's around apply.acquire)."""
    from cdc_flight import apply as apply_module
    from cdc_flight import config, ingest_log

    assert apply_module.LeaseBusy is ingest_log.LeaseBusy is config.LeaseBusy


@pytest.mark.postgres
def test_a_remove_or_relabel_racing_an_apply_run_is_busy_and_writes_nothing(stores, schema_source):  # noqa: F811
    """remove_table's fence and add_table's relabel take the table's apply lease. When an
    apply run holds it, both return busy (they used to raise apply.LeaseBusy), and
    nothing is written: the holds, the pin and the mirror's name are unchanged."""
    source, ingest, log, backfill, apply = schema_source
    schema = source.schema
    gone, old, new = f"{schema}.t_trunc", f"{schema}.t_rename", f"{schema}.t_renamed"
    for table in (gone, old):
        assert drive(backfill, table, ingest, log).status == "published"
    gone_id, _ = apply.registered(gone)
    rename_id, _ = apply.registered(old)
    source.execute("DROP TABLE t_trunc")
    source.execute("ALTER TABLE t_rename RENAME TO t_renamed")
    ingest_once(ingest, log)

    def snapshot():
        return (
            held_row(log, gone_id)[0],
            held_row(log, rename_id)[0],
            apply.state(rename_id)["target"],
            apply.retention.pin(table_owner(gone_id)) is not None,
        )

    before = snapshot()
    assert before[:2] == ("dropped", "renamed") and before[3]
    for relation_id, act in (
        (gone_id, lambda: remove_table(log, apply, gone)),
        (rename_id, lambda: add_table(log, apply, source.con, ingest.publication, new)),
    ):
        token = apply.acquire(relation_id, "a racing apply run")
        try:
            result = act()
            assert result.status == "busy" and "apply run holds" in result.detail, result
        finally:
            apply.release(relation_id, token)
    assert snapshot() == before
    # Once that apply run is gone, both go through.
    assert remove_table(log, apply, gone).status == "removed"
    assert add_table(log, apply, source.con, ingest.publication, new).status == "relabelled"


@pytest.mark.postgres
def test_a_rename_holds_then_relabels_in_place_without_a_resync(stores, schema_source):  # noqa: F811
    source, ingest, log, backfill, apply = schema_source
    schema = source.schema
    old, new = f"{schema}.t_rename", f"{schema}.t_renamed"
    assert drive(backfill, old, ingest, log).status == "published"
    relation_id, _ = apply.registered(old)
    generation = apply.state(relation_id)["generation"]
    source.execute("ALTER TABLE t_rename RENAME TO t_renamed")
    source.execute("INSERT INTO t_renamed VALUES (500,'during the hold',500)")
    first = ingest_once(ingest, log)
    assert first.held[0]["reason"] == "renamed" and new in first.held[0]["detail"]
    assert apply.apply_table(old, "run").status == "source_missing"
    # add-table under the new name: same OID, so a relabel. A name collision in the
    # mirror is refused first, and a crash between the two steps is finished by a retry.
    apply.con.execute(f"CREATE TABLE {apply.mirror}.{schema}.t_renamed (x int)")
    assert add_table(log, apply, source.con, ingest.publication, new).status == "refused"
    apply.con.execute(f"DROP TABLE {apply.mirror}.{schema}.t_renamed")
    real = log.transaction
    calls = []

    def crash_once():
        calls.append(1)
        if len(calls) == 1:
            raise RuntimeError("lost before the registry step")
        return real()

    log.transaction = crash_once
    with pytest.raises(RuntimeError, match="lost before"):
        add_table(log, apply, source.con, ingest.publication, new)
    log.transaction = real
    assert apply.state(relation_id)["target"].endswith('"t_renamed"')  # mirror renamed
    result = add_table(log, apply, source.con, ingest.publication, new)
    assert result.status == "relabelled", result
    # Apply resumes from its checkpoint under the new name: no new generation.
    assert apply.apply_table(new, "run").status == "caught_up"
    assert apply.state(relation_id)["generation"] == generation
    assert_mirror_equals_source(apply, source, new)
    # A config still naming the old table: ignored and reported.
    assert ingest_once(ingest, log).stale == [old]


@pytest.mark.postgres
def test_a_key_change_during_a_rename_hold_is_still_tracked(stores, schema_source):  # noqa: F811
    source, ingest, log, backfill, apply = schema_source
    schema = source.schema
    old, new = f"{schema}.t_rename", f"{schema}.t_renamed"
    assert drive(backfill, old, ingest, log).status == "published"
    source.execute("ALTER TABLE t_rename RENAME TO t_renamed")
    ingest_once(ingest, log)
    source.execute("ALTER TABLE t_renamed DROP CONSTRAINT t_rename_pkey")
    source.execute("ALTER TABLE t_renamed ADD PRIMARY KEY (id, n)")
    source.execute("INSERT INTO t_renamed VALUES (600,'new key',600)")
    ingest_once(ingest, log)
    assert add_table(log, apply, source.con, ingest.publication, new).status == "relabelled"
    assert apply.apply_table(new, "run").status == "needs_resync"


@pytest.mark.postgres
def test_a_rename_reverted_clears_the_hold(stores, schema_source):  # noqa: F811
    source, ingest, log, backfill, apply = schema_source
    table = f"{source.schema}.t_rename"
    assert drive(backfill, table, ingest, log).status == "published"
    source.execute("ALTER TABLE t_rename RENAME TO t_tmp")
    assert ingest_once(ingest, log).held
    source.execute("ALTER TABLE t_tmp RENAME TO t_rename")
    source.execute("INSERT INTO t_rename VALUES (700,'back',700)")
    assert ingest_once(ingest, log).held == []
    assert apply.apply_table(table, "run").status == "caught_up"
    assert_mirror_equals_source(apply, source, table)


@pytest.mark.postgres
def test_a_recreated_table_is_adopted_and_replaces_the_old_mirror(stores, schema_source):  # noqa: F811
    source, ingest, log, backfill, apply = schema_source
    table = f"{source.schema}.t_type"
    assert drive(backfill, table, ingest, log).status == "published"
    old_id, _ = apply.registered(table)
    source.execute("DROP TABLE t_type")
    recreate(source, "t_type")
    source.execute("INSERT INTO t_type SELECT i,'new'||i,i FROM generate_series(1,5) i")
    # Not yet in the publication: held `replaced`; the source keeps running.
    assert ingest_once(ingest, log).held[0]["reason"] == "replaced"
    refused = add_table(log, apply, source.con, ingest.publication, table)
    assert refused.status == "refused" and "ALTER PUBLICATION" in refused.detail
    # The DBA publishes it: registered unmanaged (logged, not applied), never refused.
    add_to_publication(source, ingest, "t_type")
    source.execute("INSERT INTO t_type VALUES (6,'published',6)")
    result = ingest_once(ingest, log)
    assert result.unmanaged == [table]
    assert add_table(log, apply, source.con, ingest.publication, table).status == "adopted"
    new_id, _ = apply.registered(table)
    assert new_id != old_id and held_row(log, old_id)[4] == new_id  # retired_by
    assert apply.retention.pin(table_owner(old_id)) is None
    # The old mirror stays queryable until the new generation publishes over it.
    assert apply.apply_table(table, "run").status == "awaiting_generation"
    old_target = apply.state(old_id)["target"]
    assert apply.con.execute(f"SELECT count(*) FROM {old_target}").fetchone()[0] > 0
    assert drive(backfill, table, ingest, log).status == "published"
    ingest_once(ingest, log)
    assert apply.apply_table(table, "run").status == "caught_up"
    assert apply.state(old_id) is None
    assert_mirror_equals_source(apply, source, table)


@pytest.mark.postgres
def test_publish_over_a_retired_mirror_waits_for_its_apply_lease(stores, schema_source):  # noqa: F811
    source, ingest, log, backfill, apply = schema_source
    table = f"{source.schema}.t_type"
    assert drive(backfill, table, ingest, log).status == "published"
    old_id, _ = apply.registered(table)
    source.execute("DROP TABLE t_type")
    recreate(source, "t_type")
    add_to_publication(source, ingest, "t_type")
    ingest_once(ingest, log)
    assert add_table(log, apply, source.con, ingest.publication, table).status == "adopted"
    # An apply run of the retired relation holds its lease: publish must not replace.
    token = apply.acquire(old_id, "someone")
    try:
        result = drive(backfill, table, ingest, log, until="busy_publish")
        assert result.status == "busy_publish", result
        assert apply.state(old_id) is not None
    finally:
        apply.release(old_id, token)
    assert drive(backfill, table, ingest, log).status == "published"


@pytest.mark.postgres
def test_a_new_table_is_added_and_an_unregistered_one_is_refused(stores, schema_source):  # noqa: F811
    source, ingest, log, backfill, apply = schema_source
    table = f"{source.schema}.t_new"
    recreate(source, "t_new")
    source.execute("INSERT INTO t_new SELECT i,'x',i FROM generate_series(1,4) i")
    ingest_once(ingest, log)
    # Declared in CDC_TABLES before anyone added it: refused, pointing at add-table.
    declared = replace(ingest, tables=(*ingest.tables, table))
    with pytest.raises(PreflightError, match="run add-table"):
        run_once(declared, IngestLog(log.con, declared))
    add_to_publication(source, ingest, "t_new")
    assert ingest_once(ingest, log).unmanaged == [table]
    assert add_table(log, apply, source.con, ingest.publication, table).status == "adopted"
    assert ingest_once(ingest, log).undeclared == [table]
    with pytest.raises(ApplyError, match="not observed empty|backfill"):
        apply.start_from_log(table)
    assert drive(backfill, table, ingest, log).status == "published"
    ingest_once(declared, IngestLog(log.con, declared))
    assert apply.apply_table(table, "run").status == "caught_up"
    assert_mirror_equals_source(apply, source, table)


@pytest.mark.postgres
def test_a_table_dropped_during_a_run_is_not_a_key_change(stores, schema_source):  # noqa: F811
    source, ingest, log, backfill, apply = schema_source
    table = f"{source.schema}.t_trunc"
    relation_id, _ = apply.registered(table)
    version = log.execute(
        "SELECT key_version FROM {s}tables WHERE relation_id=?", [relation_id]
    ).fetchone()

    def drop(stage):
        # After the first durable batch, before the run's end-of-run key read.
        if stage == "after_append" and not dropped:
            dropped.append(1)
            source.execute("DROP TABLE t_trunc")

    dropped = []
    source.execute("INSERT INTO t_default VALUES (800,'a batch',800)")
    assert run_once(ingest, log, fault=drop).status == "drained"
    assert dropped
    assert (
        log.execute(
            "SELECT key_version FROM {s}tables WHERE relation_id=?", [relation_id]
        ).fetchone()
        == version
    )
    assert ingest_once(ingest, log).held[0]["reason"] == "dropped"


def test_a_binding_registered_with_a_table_list_still_matches(stores, tmp_path):
    """Stores registered before the registry was the truth kept the tables in their
    binding: they compare equal on everything else."""
    from test_format2_protocol import binding

    from cdc_flight.config import IngestConfig
    from cdc_flight.ingest_log2 import IngestConfig2

    s, _ = stores
    base = IngestConfig(
        "bound", "slot", "pub", ("public.a",), database=s.database.strip('"'), allow_local=True
    )
    config = IngestConfig2(**base.__dict__, text_binding=replace(binding(), source_id="bound"))
    log = IngestLog(s.con, config)
    log.initialize()
    tables = [dict(oid=7, schema="public", name="a", keys=["id"])]
    log.register(("system", 1), 0, tables)
    legacy = json.loads(config.binding())
    legacy["tables"] = ["public.a", "public.gone"]
    log.execute("UPDATE {s}source SET binding=?", [json.dumps(legacy, sort_keys=True)])
    log.forget()  # a new run must check the stored identity, not cached arguments
    log.register(("system", 1), 0, tables)
    assert log.registered_slot() == "slot" and log.checkpoint() == 0


def test_held_tables_are_reported_in_the_ingest_chart(capsys):
    from types import SimpleNamespace

    from cdc_flight.flightlog import Run

    log = SimpleNamespace(registry=lambda: [dict(relation_id=9, schema_name="s", table_name="t")])
    explain.tables_held(Run(), log, SimpleNamespace(held={9: ("dropped", "s.t was dropped")}))
    out = capsys.readouterr().out
    assert "@flight-run held ok branch=held rows=1" in out
    assert "@flight-log held s.t dropped" in out


@pytest.mark.postgres
def test_an_adopted_table_published_keyless_is_mirrored_by_its_later_key(stores, schema_source):  # noqa: F811
    """An unmanaged table registers the key preflight saw then (none): its logged
    events carry that projection. Apply folds by the generation's key from the images,
    so an adoption after ADD PRIMARY KEY mirrors exactly, including later updates and
    deletes."""
    source, ingest, log, backfill, apply = schema_source
    table = f"{source.schema}.t_keyless"
    source.execute("CREATE TABLE t_keyless (id int NOT NULL, a text, n int)")
    source.execute("ALTER TABLE t_keyless REPLICA IDENTITY FULL")
    source.execute("INSERT INTO t_keyless SELECT i,'k'||i,i FROM generate_series(1,6) i")
    add_to_publication(source, ingest, "t_keyless")
    source.execute("UPDATE t_keyless SET a='logged keyless' WHERE id=2")
    assert ingest_once(ingest, log).unmanaged == [table]
    relation = next(r for r in log.registry() if r["table_name"] == "t_keyless")
    assert json.loads(relation["key_columns"]) == []
    oid = relation["relation_id"]
    descriptors = log.execute(
        "SELECT version,key_hash,keys,descriptor FROM {s}descriptors_v2 WHERE relation_id=? ORDER BY ALL",
        [oid],
    ).fetchall()
    assert descriptors and all(json.loads(d[2]) == [] for d in descriptors)
    source.execute("ALTER TABLE t_keyless ADD PRIMARY KEY (id)")
    boundary = log.checkpoint()
    assert add_table(log, apply, source.con, ingest.publication, table).status == "adopted"
    # A drained v2 adoption installs the new projection for the NEXT spool. Old
    # descriptors stay immutable and lie before the new generation's unit cut L.
    assert log.key_columns()[oid] == ("id",)
    assert (
        log.execute(
            "SELECT version,key_hash,keys,descriptor FROM {s}descriptors_v2 WHERE relation_id=? ORDER BY ALL",
            [oid],
        ).fetchall()
        == descriptors
    )
    generation = backfill.start(table)
    assert generation["pin_lsn"] == boundary
    assert generation["key_columns"] == ("id",)
    ingest_once(ingest, log)
    assert drive(backfill, table, ingest, log).status == "published"
    source.execute("UPDATE t_keyless SET a='after adoption', n=n+10 WHERE id IN (1,3)")
    source.execute("DELETE FROM t_keyless WHERE id=4")
    source.execute("INSERT INTO t_keyless VALUES (7,'new',7)")
    ingest_once(ingest, log)
    assert apply.apply_table(table, "run").status == "caught_up"
    assert_mirror_equals_source(apply, source, table)


@pytest.mark.postgres
def test_a_reactivated_table_with_a_changed_key_is_mirrored_by_it(stores, schema_source):  # noqa: F811
    source, ingest, log, backfill, apply = schema_source
    table = f"{source.schema}.t_default"
    assert drive(backfill, table, ingest, log).status == "published"
    assert remove_table(log, apply, table, force=True).status == "removed"
    source.execute("ALTER TABLE t_default DROP CONSTRAINT t_default_pkey")
    source.execute("ALTER TABLE t_default ADD PRIMARY KEY (id, n)")
    source.execute("UPDATE t_default SET a='while removed' WHERE id=2")
    ingest_once(ingest, log)
    assert add_table(log, apply, source.con, ingest.publication, table).status == "reactivated"
    assert drive(backfill, table, ingest, log).status == "published"
    source.execute("UPDATE t_default SET a='after' WHERE id=1")
    source.execute("UPDATE t_default SET n=n+100 WHERE id=3")  # a key move under (id, n)
    source.execute("DELETE FROM t_default WHERE id=5")
    ingest_once(ingest, log)
    assert apply.apply_table(table, "run").status == "caught_up"
    assert_mirror_equals_source(apply, source, table)


@pytest.mark.postgres
def test_a_failed_publish_over_a_retired_mirror_frees_its_lease_at_once(stores, schema_source):  # noqa: F811
    source, ingest, log, backfill, apply = schema_source
    table = f"{source.schema}.t_type"
    assert drive(backfill, table, ingest, log).status == "published"
    source.execute("DROP TABLE t_type")
    recreate(source, "t_type")
    add_to_publication(source, ingest, "t_type")
    ingest_once(ingest, log)
    assert add_table(log, apply, source.con, ingest.publication, table).status == "adopted"

    def crash(stage):
        if stage == "after_rename":
            raise RuntimeError("killed before the publish commit")

    for _ in range(20):
        try:
            result = backfill.run_table(table, fault=crash)
        except RuntimeError:
            break
        if result.status in ("waiting_for_marker", "waiting_for_ingest"):
            ingest_once(ingest, log)
    else:
        raise AssertionError("never reached the publish")
    # The retry publishes now: the retired relation's lease was released, not leaked.
    assert backfill.run_table(table).status == "published"


@pytest.mark.postgres
def test_a_stale_classification_does_not_re_hold_a_relabelled_table(stores, schema_source):  # noqa: F811
    from cdc_flight.preflight import check

    source, ingest, log, backfill, apply = schema_source
    schema = source.schema
    old, new = f"{schema}.t_rename", f"{schema}.t_renamed"
    assert drive(backfill, old, ingest, log).status == "published"
    source.execute("ALTER TABLE t_rename RENAME TO t_renamed")
    ingest_once(ingest, log)
    # A run classifies before its lease, from the registry as it was...
    stale = check(ingest, registry=log.registry())
    assert next(iter(stale.held.values()))[0] == "renamed"
    # ...an operator relabels meanwhile...
    assert add_table(log, apply, source.con, ingest.publication, new).status == "relabelled"
    # ...and the run's hold write doesn't match the renamed row.
    assert run_once(ingest, log, info=stale).status == "drained"
    assert apply.apply_table(new, "run").status == "caught_up"
