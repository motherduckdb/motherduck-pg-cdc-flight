"""The registry is the table set (Phase 3, U4): CDC_TABLES is optional.

Unset, apply covers every registered table (not removed, not unmanaged), backfill takes
any registered table, and ingest captures the registry without a declaration check. Set,
everything is as before, and apply reports registered tables the list leaves out. The
first registration still needs CDC_TABLES, and a registered source's provisioning DDL
lists the registry's tables, never an empty CDC_TABLES.
"""

import os
import subprocess
import sys
from dataclasses import replace

import conftest
import pytest
from psycopg2 import sql
from test_backfill import drive
from test_phase2 import assert_mirror_equals_source
from test_schema_cases import schema_source  # noqa: F401 (fixture)
from test_table_hold import add_to_publication, ingest_once, recreate

from cdc_flight import cli, table_set
from cdc_flight.apply_run import ApplyRun
from cdc_flight.backfill import Backfill
from cdc_flight.config import ApplyConfig, IngestConfig, configured_tables
from cdc_flight.ingest_log import IngestLog
from cdc_flight.membership import add_table, remove_table
from cdc_flight.preflight import PreflightError, check, query

LOCAL = pytest.mark.parametrize("stores", ["local"], indirect=True)

# config and CLI input (no I/O) ----------------------------------------------------


def test_an_empty_table_list_means_unset_and_bad_lists_are_still_refused():
    assert ApplyConfig("s", ()).tables == ()
    assert IngestConfig("s", "slot", "pub", ()).tables == ()
    with pytest.raises(ValueError, match="unique"):
        ApplyConfig("s", ("public.a", "public.a"))
    with pytest.raises(ValueError, match="schema-qualified"):
        ApplyConfig("s", ("a",))
    with pytest.raises(ValueError, match="heartbeat"):
        IngestConfig("s", "slot", "pub", ("public.md_cdc_heartbeat",))


@pytest.mark.parametrize("value,expected", [(None, ()), ("", ()), ('["a.b"]', ("a.b",))])
def test_cdc_tables_unset_or_blank_reads_as_no_list(monkeypatch, value, expected):
    if value is None:
        monkeypatch.delenv("CDC_TABLES", raising=False)
    else:
        monkeypatch.setenv("CDC_TABLES", value)
    assert configured_tables() == expected


def test_an_explicit_empty_list_never_widens_to_every_table(monkeypatch):
    # Every entry point (cli.main, cdc-flight-tables) calls required() before config.
    monkeypatch.setenv("CDC_TABLES", "[]")
    monkeypatch.setenv("CDC_SOURCE_ID", "s")
    with pytest.raises(SystemExit, match="CDC_TABLES must be a non-empty JSON list"):
        cli.required()


def test_required_accepts_no_cdc_tables(monkeypatch):
    monkeypatch.setenv("CDC_SOURCE_ID", "s")
    monkeypatch.delenv("CDC_TABLES", raising=False)
    cli.required()
    monkeypatch.delenv("CDC_SOURCE_ID")
    with pytest.raises(SystemExit, match="CDC_SOURCE_ID is required"):
        cli.required()


def run_cli(tmp_path, command, *args, **env):
    environ = {k: v for k, v in os.environ.items() if not k.startswith("CDC_")}
    environ.update(MOTHERDUCK_TOKEN="unused", CDC_SOURCE_ID="s", CDC_PG_DSN="unused", **env)
    return subprocess.run(
        [sys.executable, "-m", "cdc_flight.cli", command, *args],
        env=environ,
        capture_output=True,
        text=True,
        cwd=tmp_path,
    )


@pytest.mark.parametrize(
    "entry",
    [
        ("-m", "cdc_flight.cli", "apply"),
        ("-m", "cdc_flight.cli", "backfill"),
        # cdc-flight-tables (its console script runs tables_cli.main)
        ("-c", "from cdc_flight.tables_cli import main; main()", "add", "--table", "public.a"),
    ],
    ids=["cdc-flight apply", "cdc-flight backfill", "cdc-flight-tables add"],
)
def test_every_entry_point_refuses_an_explicit_empty_cdc_tables(tmp_path, entry):
    """configured_tables() reads [] as unset, so the refusal at each entry point is what
    keeps an emptied list from meaning every table."""
    environ = {k: v for k, v in os.environ.items() if not k.startswith("CDC_")}
    environ.update(MOTHERDUCK_TOKEN="unused", CDC_SOURCE_ID="s", CDC_PG_DSN="unused")
    environ.update(CDC_TABLES="[]", CDC_BACKFILL_TABLE="public.a")
    out = subprocess.run(
        [sys.executable, *entry], env=environ, capture_output=True, text=True, cwd=tmp_path
    )
    assert out.returncode == 1, out.stderr
    assert "CDC_TABLES must be a non-empty JSON list" in out.stderr
    assert "Traceback" not in out.stderr


@pytest.mark.parametrize("table", ["", "orders", "a.b.c"])
def test_a_malformed_backfill_table_fails_naming_the_key_before_connecting(tmp_path, table):
    out = run_cli(tmp_path, "backfill", CDC_BACKFILL_TABLE=table)
    assert out.returncode == 1, out.stderr
    assert 'CDC_BACKFILL_TABLE must be a "schema.table" name' in out.stderr
    assert "Traceback" not in out.stderr


@pytest.mark.parametrize("args", [(), ("--table", "orders")])
def test_a_malformed_start_from_log_table_fails_before_connecting(tmp_path, args):
    out = run_cli(tmp_path, "start-from-log", *args)
    assert out.returncode == 1, out.stderr
    assert '--table must be a "schema.table" name' in out.stderr
    assert "Traceback" not in out.stderr


def test_a_set_cdc_tables_still_restricts_backfill(tmp_path):
    out = run_cli(tmp_path, "backfill", CDC_TABLES='["public.a"]', CDC_BACKFILL_TABLE="public.b")
    assert out.returncode == 1 and "backfill table must be one of CDC_TABLES" in out.stderr


# PostgreSQL + local store ----------------------------------------------------------


def unset(config):
    return replace(config, tables=())


@LOCAL
@pytest.mark.postgres
def test_the_first_registration_needs_cdc_tables(stores, schema_source):  # noqa: F811
    source, ingest, *_ = schema_source
    fresh = replace(
        unset(ingest), slot="ps_first_" + source.schema, publication="pf_" + source.schema
    )
    try:
        with pytest.raises(PreflightError, match="CDC_TABLES is required to register"):
            check(fresh, create_objects=True)
        # Refused before provisioning anything.
        assert not query(
            source.con, "SELECT 1 FROM pg_replication_slots WHERE slot_name=%s", [fresh.slot]
        )
        assert not query(
            source.con, "SELECT 1 FROM pg_publication WHERE pubname=%s", [fresh.publication]
        )
    finally:
        # A regression that provisions must not leave a slot retaining WAL (the
        # fixture's teardown drops only its own slot and publication).
        query(
            source.con,
            "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
            "WHERE slot_name=%s",
            [fresh.slot],
        )
        query(
            source.con,
            sql.SQL("DROP PUBLICATION IF EXISTS {}").format(sql.Identifier(fresh.publication)),
        )


@LOCAL
@pytest.mark.postgres
def test_unset_ingest_captures_the_registry_without_declaration_reports(stores, schema_source):  # noqa: F811
    source, ingest, log, backfill, apply = schema_source
    s = source.schema
    gone, omitted = f"{s}.t_trunc", f"{s}.t_type"
    assert remove_table(log, apply, gone, force=True).status == "removed"
    # A declaration with a stale name (removed) that omits an active table.
    declared = replace(ingest, tables=tuple(t for t in ingest.tables if t != omitted))
    result = ingest_once(declared, IngestLog(log.con, declared))
    assert (result.stale, result.undeclared) == ([gone], [omitted])
    free = unset(ingest)
    source.execute("INSERT INTO t_type VALUES (100,'x',100)")
    result = ingest_once(free, IngestLog(log.con, free))
    assert (result.stale, result.undeclared, result.held) == ([], [], [])
    assert result.transactions >= 1


@LOCAL
@pytest.mark.postgres
def test_unset_apply_covers_the_registry_and_a_set_list_reports_what_it_omits(
    stores,
    schema_source,  # noqa: F811
):
    source, ingest, log, backfill, apply = schema_source
    s = source.schema
    for table in ("t_default", "t_trunc", "t_drop"):
        assert drive(backfill, f"{s}.{table}", ingest, log).status == "published"
    # removed (forced), unmanaged (published, never added), and held (dropped).
    assert remove_table(log, apply, f"{s}.t_rename", force=True).status == "removed"
    recreate(source, "t_extra")
    add_to_publication(source, ingest, "t_extra")
    source.execute("DROP TABLE t_drop")
    ingest_once(ingest, log)

    free = ApplyRun(log.con, unset(apply.config))
    statuses = {r.table: r.status for r in table_set.run(free)}
    assert list(statuses) == sorted(statuses)
    assert statuses == {
        f"{s}.t_default": "caught_up",
        f"{s}.t_drop": "source_missing",
        f"{s}.t_trunc": "caught_up",
        f"{s}.t_type": "awaiting_generation",
    }

    # Set: exactly the list, plus a warning per registered, unheld table it leaves out.
    only = ApplyRun(log.con, replace(apply.config, tables=(f"{s}.t_default",)))
    results = table_set.run(only)
    assert [(r.table, r.status) for r in results] == [
        (f"{s}.t_default", "caught_up"),
        (f"{s}.t_trunc", "not_configured"),
        (f"{s}.t_type", "not_configured"),
    ]
    assert "pin holds the log" in results[1].detail
    assert not cli.apply_failed(results)

    # A drop/recreate adopted by add-table: a removed row and an active row, one name.
    recreate(source, "t_drop")
    add_to_publication(source, ingest, "t_drop")
    ingest_once(ingest, log)
    assert add_table(log, free, source.con, ingest.publication, f"{s}.t_drop").status in (
        "adopted",
        "added",
    )
    names = [name for name, _ in table_set.registered(free)]
    assert names.count(f"{s}.t_drop") == 1
    assert f"{s}.t_rename" not in names and f"{s}.t_extra" not in names
    assert {r.table: r.status for r in table_set.run(free)}[f"{s}.t_drop"] == "awaiting_generation"


@LOCAL
@pytest.mark.postgres
def test_an_added_table_is_backfilled_and_applied_without_editing_cdc_tables(
    stores,
    schema_source,  # noqa: F811
):
    """The U4 acceptance: add-table, backfill and apply, with CDC_TABLES unset on all."""
    source, ingest, log, backfill, apply = schema_source
    s = source.schema
    free_ingest = unset(ingest)
    free_log = IngestLog(log.con, free_ingest)
    free_backfill = Backfill(log.con, unset(backfill.config))
    free_backfill.initialize()
    free_apply = ApplyRun(log.con, unset(apply.config))
    try:
        assert drive(free_backfill, f"{s}.t_default", free_ingest, free_log).status == "published"
        recreate(source, "t_new")
        source.execute("INSERT INTO t_new SELECT i,'n'||i,i FROM generate_series(1,5) i")
        add_to_publication(source, ingest, "t_new")
        ingest_once(free_ingest, free_log)
        added = add_table(free_log, free_apply, source.con, ingest.publication, f"{s}.t_new")
        assert added.status == "adopted", added
        assert not any("CDC_TABLES" in step for step in added.next_steps), added
        assert drive(free_backfill, f"{s}.t_new", free_ingest, free_log).status == "published"
        source.execute("INSERT INTO t_new VALUES (6,'later',6)")
        ingest_once(free_ingest, free_log)
        statuses = {r.table: r.status for r in table_set.run(free_apply)}
        assert statuses[f"{s}.t_new"] == statuses[f"{s}.t_default"] == "caught_up"
        assert_mirror_equals_source(free_apply, source, f"{s}.t_new")
        # The same apply with the old list: t_new is left out, and said so.
        old = ApplyRun(log.con, apply.config)
        assert {r.table: r.status for r in table_set.run(old)}[f"{s}.t_new"] == "not_configured"
    finally:
        free_backfill.close()


@LOCAL
@pytest.mark.postgres
def test_membership_hints_mention_cdc_tables_only_when_it_is_set(stores, schema_source):  # noqa: F811
    source, ingest, log, backfill, apply = schema_source
    s = source.schema
    declared = remove_table(log, apply, f"{s}.t_type", force=True)
    assert any("CDC_TABLES" in step for step in declared.next_steps)
    free_log = IngestLog(log.con, unset(ingest))
    free = remove_table(free_log, apply, f"{s}.t_rename", force=True)
    assert free.status == "removed"
    assert not any("CDC_TABLES" in step for step in free.next_steps)


@LOCAL
@pytest.mark.postgres
def test_the_heartbeat_is_never_added_as_a_table(stores, schema_source):  # noqa: F811
    source, ingest, log, backfill, apply = schema_source
    result = add_table(log, apply, source.con, ingest.publication, ingest.heartbeat)
    assert result.status == "refused" and "heartbeat" in result.detail
    assert ingest.heartbeat not in [name for name, _ in table_set.registered(apply)]


def publication_tables(source, publication):
    return sorted(
        f"{schema}.{name}"
        for schema, name in query(
            source.con,
            "SELECT schemaname,tablename FROM pg_publication_tables WHERE pubname=%s",
            [publication],
        )
    )


@LOCAL
@pytest.mark.postgres
def test_a_registered_source_with_cdc_tables_unset_reports_and_provisions_from_the_registry(
    stores,
    schema_source,  # noqa: F811
):
    """The missing-objects error and --create-objects use the registry's
    captured tables, so a lost slot or dropped publication is reported as such."""
    source, ingest, log, backfill, apply = schema_source
    s = source.schema
    free = unset(ingest)
    # A renamed table is held but still exists: it must stay in a recreated publication
    # under its current name, or its relabelled mirror misses changes.
    source.execute("ALTER TABLE t_type RENAME TO t_type2")
    result = ingest_once(free, IngestLog(log.con, free))
    assert [h["reason"] for h in result.held] == ["renamed"]
    published = publication_tables(source, ingest.publication)
    assert f"{s}.t_type2" in published
    conftest.wait_slot_released(source.con, ingest.slot)
    query(source.con, "SELECT pg_drop_replication_slot(%s)", [ingest.slot])
    with pytest.raises(PreflightError) as lost:
        check(free, registry=log.registry())
    # Only the slot is missing: preflight creates it; no DBA statement is needed.
    assert "missing replication slot" in str(lost.value)
    assert "CDC_CREATE_OBJECTS=1" in str(lost.value) and "PUBLICATION" not in str(lost.value)

    query(source.con, sql.SQL("DROP PUBLICATION {}").format(sql.Identifier(ingest.publication)))
    with pytest.raises(PreflightError) as dropped:
        check(free, registry=log.registry())
    message = str(dropped.value)
    assert "missing publication, replication slot" in message
    for table in published:
        schema, name = table.split(".")
        assert f'"{schema}"."{name}"' in message

    check(free, create_objects=True, registry=log.registry())
    assert publication_tables(source, ingest.publication) == published
