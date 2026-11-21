"""Keyless capture, phase 1a: add-table's append mode is checked fully and then refused.

Append (insert-only) capture of a keyless table comes in a later release. This one
checks an append add in the order the owner fixes things and refuses it at the end
("not yet supported"):

- a key, read live from PostgreSQL → add it keyed;
- a unique index that qualifies as a replica identity → capture it keyed, with the
  statement (smallest index first);
- partitioned, no SELECT, identity not FULL → refused; the FULL statement comes before
  the publication's ADD TABLE (published without FULL, the table's UPDATE and
  DELETE fail on the source);
- not in the publication → the DBA's statement;
- a registry row that is managed or held (only unmanaged, removed or absent ones qualify).

Preflight's `no_key` message carries the same advice."""

import pytest
from test_action_flight import Trigger
from test_membership_flight import dry, go
from test_schema_cases import schema_source  # noqa: F401,E402 (fixture)
from test_table_hold import add_to_publication, held_row, ingest_once  # noqa: E402

from cdc_flight import action_base as af
from cdc_flight import membership_flight as mf
from cdc_flight import table_actions as ta
from cdc_flight.membership import add_table, remove_table
from cdc_flight.preflight import (
    TableInvalid,
    identity_statements,
    qualifying_unique_indexes,
    resolve,
    sql_name,
    validate_append,
    validate_table,
)
from cdc_flight.releases import RELEASE_STAMPED, declare_not_deployed

PG = pytest.mark.postgres


# Inputs, before any connection ----------------------------------------------------------


def test_the_flight_checks_mode_and_ignore_deletes_before_connecting():
    base = {"CDC_ACTION": "add", "CDC_TABLE": "public.t", "CDC_DRY_RUN": "1"}
    assert af.parse({**base, "CDC_MODE": "append"}, mf.MEMBERSHIP).values == {"mode": "append"}
    both = af.parse({**base, "CDC_MODE": "append", "CDC_IGNORE_DELETES": "1"}, mf.MEMBERSHIP)
    assert both.values == {"mode": "append", "ignore_deletes": True}
    # The defaults hash as unset: a keyed add's preview is the same as before.
    assert (
        af.parse({**base, "CDC_MODE": "keyed", "CDC_IGNORE_DELETES": "0"}, mf.MEMBERSHIP).values
        == {}
    )
    for bad, message in (
        ({"CDC_MODE": "history"}, "CDC_MODE must be one of keyed, append"),
        ({"CDC_IGNORE_DELETES": "yes"}, "CDC_IGNORE_DELETES must be 0 or 1"),
        ({"CDC_IGNORE_DELETES": "1"}, "only for CDC_MODE=append"),
        ({"CDC_MODE": "keyed", "CDC_IGNORE_DELETES": "1"}, "only for CDC_MODE=append"),
    ):
        with pytest.raises(af.InputError, match=message):
            af.parse({**base, **bad}, mf.MEMBERSHIP)
    remove = {"CDC_ACTION": "remove", "CDC_TABLE": "public.t", "CDC_DRY_RUN": "1"}
    with pytest.raises(af.InputError, match="CDC_MODE is not used by remove"):
        af.parse({**remove, "CDC_MODE": "append"}, mf.MEMBERSHIP)


def test_the_cli_checks_mode_and_ignore_deletes_before_connecting(monkeypatch):
    from cdc_flight import tables_cli

    for argv, message in (
        (["remove", "--table", "s.t", "--mode", "append"], "only for add"),
        (["add", "--table", "s.t", "--ignore-deletes"], "only for --mode append"),
    ):
        monkeypatch.setattr("sys.argv", ["cdc-flight-tables", *argv])
        with pytest.raises(SystemExit, match=message):
            tables_cli.main()


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_release_status_and_the_not_deployed_declaration(stores):
    """D5: stamped, declared not deployed, or missing; an unknown command and a stamped one
    are refused; a later Flight run replaces a declaration."""
    from test_apply2 import Env

    from cdc_flight.releases import ReleaseError, record_release, release_status

    log = Env(stores).log
    assert set(release_status(log.execute).values()) == {"missing"}
    with pytest.raises(ReleaseError, match="must be one of"):
        declare_not_deployed(log.execute, "ingest", "t")
    record_release(log.execute, "apply", {"MOTHERDUCK_FLIGHT_ID": "f1"})
    with pytest.raises(ReleaseError, match="deployed"):
        declare_not_deployed(log.execute, "apply", "t")
    declare_not_deployed(log.execute, "quarantine", "t")
    status = release_status(log.execute)
    assert (status["apply"], status["quarantine"], status["backfill"]) == (
        "stamped",
        "not deployed",
        "missing",
    )
    assert record_release(log.execute, "quarantine", {"MOTHERDUCK_FLIGHT_ID": "f2"})
    assert release_status(log.execute)["quarantine"] == "stamped"
    by = log.execute("SELECT not_deployed_by FROM {s}releases WHERE command='quarantine'")
    assert by.fetchone()[0] is None


def test_the_releases_cli_checks_its_inputs_before_connecting(monkeypatch):
    from cdc_flight import tables_cli

    for argv, message in (
        (["releases", "--table", "s.t"], "not used by releases"),
        (["add"], "--table is required"),
        (["add", "--table", "s.t", "--not-deployed", "apply"], "only for releases"),
    ):
        monkeypatch.setattr("sys.argv", ["cdc-flight-tables", *argv])
        with pytest.raises(SystemExit, match=message):
            tables_cli.main()


def test_owner_statements_quote_only_when_needed():
    assert sql_name("orders") == "orders"
    assert sql_name("Orders") == '"Orders"'
    assert sql_name('a"b') == '"a""b"'
    assert identity_statements("s.log", ["Log_uq"]) == [
        'ALTER TABLE s.log REPLICA IDENTITY USING INDEX "Log_uq";'
    ]


# On PostgreSQL ----------------------------------------------------------------------------


def _oid(source, name):
    return resolve(source.con, source.schema, name)


@PG
def test_only_indexes_postgres_accepts_as_replica_identity_qualify(schema_source):  # noqa: F811
    source = schema_source[0]
    source.execute(
        "CREATE TABLE t_idx (a int NOT NULL, b int NOT NULL, c int, d int NOT NULL, e int NOT NULL)"
    )
    source.execute("CREATE UNIQUE INDEX t_idx_c ON t_idx (c)")  # a nullable column
    source.execute("CREATE UNIQUE INDEX t_idx_partial ON t_idx (a) WHERE a > 0")
    source.execute("CREATE UNIQUE INDEX t_idx_expr ON t_idx (abs(e))")
    source.execute("ALTER TABLE t_idx ADD CONSTRAINT t_idx_deferred UNIQUE (d) DEFERRABLE")
    source.execute("CREATE INDEX t_idx_plain ON t_idx (a)")  # not unique
    source.execute("CREATE TABLE t_inc (a int NOT NULL, b int)")
    source.execute("CREATE UNIQUE INDEX t_inc_a ON t_inc (a) INCLUDE (b)")
    # Only key columns must be NOT NULL (an INCLUDE column may be nullable), as PostgreSQL
    # itself checks: the hint is offered and accepted.
    included = _oid(source, "t_inc")[0]
    assert qualifying_unique_indexes(source.con, included) == ["t_inc_a"]
    source.execute(identity_statements(f"{source.schema}.t_inc", ["t_inc_a"])[0])
    found = _oid(source, "t_idx")
    assert qualifying_unique_indexes(source.con, found[0]) == []
    source.execute("CREATE UNIQUE INDEX t_idx_ab ON t_idx (a, b)")
    source.execute("CREATE UNIQUE INDEX t_idx_b ON t_idx (b)")
    schema = source.schema
    # Fewest columns first; each is accepted by PostgreSQL itself.
    assert qualifying_unique_indexes(source.con, found[0]) == ["t_idx_b", "t_idx_ab"]
    with pytest.raises(TableInvalid, match="capture it keyed") as raised:
        validate_table(source.con, f"{schema}.t_idx", found)
    assert raised.value.reason == "no_key"
    assert "REPLICA IDENTITY USING INDEX t_idx_b;" in str(raised.value)
    for statement in identity_statements(f"{schema}.t_idx", ["t_idx_b"]):
        source.execute(statement)
    assert validate_table(source.con, f"{schema}.t_idx", _oid(source, "t_idx"))["keys"] == ["b"]


@PG
def test_preflight_no_key_advice(schema_source):  # noqa: F811
    source = schema_source[0]
    schema = source.schema
    source.execute("CREATE TABLE t_plain (a int, b text)")
    with pytest.raises(TableInvalid) as raised:
        validate_table(source.con, f"{schema}.t_plain", _oid(source, "t_plain"))
    text = str(raised.value)
    assert raised.value.reason == "no_key"
    assert f"ALTER TABLE {schema}.t_plain REPLICA IDENTITY FULL;" in text
    assert text.index("REPLICA IDENTITY FULL") < text.index("joins the publication")
    source.execute("ALTER TABLE t_plain REPLICA IDENTITY FULL")
    with pytest.raises(TableInvalid, match="insert-only: add it in append mode"):
        validate_table(source.con, f"{schema}.t_plain", _oid(source, "t_plain"))
    source.execute("CREATE TABLE t_part (a int, b text) PARTITION BY RANGE (a)")
    with pytest.raises(TableInvalid, match="keyless partitioned table can't be captured"):
        validate_table(source.con, f"{schema}.t_part", _oid(source, "t_part"))
    with pytest.raises(TableInvalid) as raised:
        validate_append(source.con, f"{schema}.t_part", _oid(source, "t_part"))
    assert raised.value.reason == "append_partitioned"
    info = validate_append(source.con, f"{schema}.t_plain", _oid(source, "t_plain"))
    assert info["keys"] == () and info["append"] is True


def declare_all(log):
    for command in RELEASE_STAMPED:
        declare_not_deployed(log.execute, command, "test")


@PG
def test_append_capture_end_to_end(schema_source):  # noqa: F811
    """The normal path: FULL, then publish, then ingest registers it unmanaged,
    then add (the version stamps first, D5), then start-from-log at S, then ingest and
    apply. Then: identity DEFAULT holds it append_identity
    without a resync, FULL clears it, and a primary key added later changes nothing: no key
    version, no key_changes row, keyless frames, the mirror exact."""
    source, ingest, log, _, apply = schema_source
    schema, pub = source.schema, ingest.publication
    table = f"{schema}.t_log"
    p = log.prefix

    def add(**kw):
        log.forget()
        return add_table(log, apply, source.con, pub, table, mode="append", **kw)

    def registry():
        return log.execute(
            "SELECT held_reason,capture_mode,key_columns,catalog_key,coalesce(key_version,0),"
            "ignore_deletes FROM {s}tables WHERE table_name='t_log'"
        ).fetchone()

    def mirror():
        return apply.con.execute(
            f"SELECT msg FROM {apply.mirror}.{schema}.t_log ORDER BY msg NULLS FIRST"
        ).fetchall()

    source.execute("CREATE TABLE t_log (at timestamptz DEFAULT now(), msg text)")
    first = add()
    assert first.status == "refused" and "needs REPLICA IDENTITY FULL" in first.detail
    # Unpublished: FULL first, then the publication (M1b).
    full, publish = first.detail.index("REPLICA IDENTITY FULL;"), first.detail.index("ADD TABLE")
    assert full < publish
    source.execute("ALTER TABLE t_log REPLICA IDENTITY FULL")
    assert "not in the publication" in add().detail
    add_to_publication(source, ingest, "t_log")
    source.execute("INSERT INTO t_log (msg) VALUES ('before S')")
    assert table in ingest_once(ingest, log).unmanaged
    # D5: no Flight ran here, so every command is missing; the refusal names each.
    stamps = add()
    assert stamps.status == "refused" and "table-membership: missing" in stamps.detail
    assert "--not-deployed" in stamps.detail
    declare_all(log)
    assert add(dry_run=True).status == "would"
    preview = dry(schema_source, action="add", table=table, mode="append")
    trigger = Trigger()
    out = go(
        schema_source, trigger=trigger, action="add", table=table, mode="append", preview_id=preview
    )
    assert (out["status"], out["result"]) == ("done", "adopted"), out
    assert "start-from-log" in out["next_steps"][0]
    assert trigger.calls == []  # no backfill for an append table
    assert registry() == (None, "append", "[]", None, 0, False)
    assert add().status == "already"
    done = ta.start_from_log(apply, table, run_id="s", actor="t")
    assert done.status == "done", done
    source.execute("INSERT INTO t_log (msg) VALUES ('dup'),('dup'),(NULL)")
    ingest_once(ingest, log)
    assert apply.apply_table(table, "a1").status == "caught_up"
    assert mirror() == [(None,), ("dup",), ("dup",)]  # 'before S' is not captured
    # Identity DEFAULT: ingest holds the registry row; inserts keep coming.
    source.execute("ALTER TABLE t_log REPLICA IDENTITY DEFAULT")
    source.execute("INSERT INTO t_log (msg) VALUES ('during')")
    ingest_once(ingest, log)
    assert registry()[0] == "append_identity"
    assert apply.apply_table(table, "a2").status == "source_missing"
    source.execute("ALTER TABLE t_log REPLICA IDENTITY FULL")
    log.forget()
    ingest_once(ingest, log)
    assert registry() == (None, "append", "[]", None, 0, False)  # cleared: no key version
    assert apply.apply_table(table, "a3").status == "caught_up"
    assert ("during",) in mirror()
    # A primary key added later: still append, still keyless.
    source.execute("ALTER TABLE t_log ADD COLUMN id bigserial PRIMARY KEY")
    source.execute("INSERT INTO t_log (msg) VALUES ('keyed now')")
    log.forget()
    ingest_once(ingest, log)
    source.execute("INSERT INTO t_log (msg) VALUES ('keyed now')")
    log.forget()
    ingest_once(ingest, log)
    assert registry() == (None, "append", "[]", None, 0, False)
    oid = log.execute("SELECT relation_id FROM {s}tables WHERE table_name='t_log'").fetchone()[0]
    assert not log.execute(
        "SELECT count(*) FROM {s}key_changes WHERE relation_id=?", [oid]
    ).fetchone()[0]
    assert {
        k
        for (k,) in log.execute(
            "SELECT DISTINCT keys FROM {s}descriptors_v2 WHERE relation_id=?", [oid]
        ).fetchall()
    } == {"[]"}
    assert apply.apply_table(table, "a4").status == "caught_up"
    assert mirror().count(("keyed now",)) == 2 and len(mirror()) == 6
    # The repro: an UPDATE holds it; remove, add again, start from the log again.
    source.execute("UPDATE t_log SET msg='changed' WHERE msg='during'")
    ingest_once(ingest, log)
    held = apply.apply_table(table, "a5")
    assert held.status == "needs_action" and "append_update" in held.detail
    assert remove_table(log, apply, table, force=True).status == "removed"
    keyed = add()  # it has a primary key now: D4
    assert keyed.status == "refused" and "add it keyed" in keyed.detail
    source.execute("ALTER TABLE t_log DROP CONSTRAINT t_log_pkey")
    log.forget()
    assert add().status == "reactivated"
    restarted = ta.start_from_log(apply, table, run_id="s2", actor="t")
    assert restarted.status == "done", restarted
    assert restarted.observed["kept_mirror"].startswith(f"{schema}.t_log__gen_")
    source.execute("INSERT INTO t_log (msg) VALUES ('after restart')")
    ingest_once(ingest, log)
    assert apply.apply_table(table, "a6").status == "caught_up"
    assert mirror() == [("after restart",)]
    assert p


@PG
def test_append_add_refuses_keys_unique_indexes_and_registered_rows(schema_source):  # noqa: F811
    source, ingest, log, _, apply = schema_source
    schema, pub = source.schema, ingest.publication

    def add(name, **kw):
        log.forget()
        return add_table(log, apply, source.con, pub, f"{schema}.{name}", **kw)

    # D4, read live: t_default is captured keyed (its registry key is irrelevant).
    keyed = add("t_default", mode="append")
    assert keyed.status == "refused" and "add it keyed, not in append mode" in keyed.detail
    source.execute("CREATE TABLE t_uq (id int NOT NULL, a text)")
    source.execute("CREATE UNIQUE INDEX t_uq_id ON t_uq (id)")
    source.execute("ALTER TABLE t_uq REPLICA IDENTITY FULL")
    unique = add("t_uq", mode="append")
    assert unique.status == "refused" and "capture it keyed instead" in unique.detail
    assert "REPLICA IDENTITY USING INDEX t_uq_id;" in unique.detail
    source.execute("CREATE TABLE t_part (a int, b text) PARTITION BY RANGE (a)")
    part = add("t_part", mode="append")
    assert part.status == "refused" and "partitioned" in part.detail
    published_plain = "t_type"  # captured keyed, then its key dropped: a managed row
    source.execute(f"ALTER TABLE {published_plain} DROP CONSTRAINT {published_plain}_pkey")
    managed = add(published_plain, mode="append")
    assert managed.status == "refused" and "captured keyed" in managed.detail
    # Ingest holds it no_key; the advice for a captured table is to restore its key.
    ingest_once(ingest, log)
    oid = _oid(source, published_plain)[0]
    reason, detail = held_row(log, oid)[:2]
    assert reason == "no_key" and "restore its key, or remove it" in detail
    assert "joins the publication" not in detail
    # Removed, then keyless with FULL: it qualifies, and this release still refuses.
    assert remove_table(log, apply, f"{schema}.{published_plain}", force=True).status == "removed"
    log.forget()
    assert held_row(log, oid)[0] == "removed"
    stamps = add(published_plain, mode="append")
    assert stamps.status == "refused" and "missing" in stamps.detail  # D5 last
    for bad, message in (
        (dict(mode="history"), "mode must be one of"),
        (dict(ignore_deletes=True), "ignore_deletes is only for append mode"),
    ):
        result = add("t_uq", **bad)
        assert result.status == "refused" and message in result.detail
