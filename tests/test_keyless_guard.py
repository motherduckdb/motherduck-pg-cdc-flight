"""The keyless-capture guard release (keyless design, phase 0).

A table registered for append capture (`tables.capture_mode`, written by a later
release), or a table meant to be applied with no effective key, never reaches a keyed
path here: apply reports it and writes nothing, start-from-log, backfill and the table
actions refuse it, add-table refuses to adopt it, and a page shared with keyed tables
plans them exactly as before. Unmanaged keyless publication members (key_columns '[]')
are unchanged. Each Flight that runs a keyed path records its guard."""

import importlib.util
import json
from pathlib import Path

import duckdb
import pgstream
import pytest
import test_apply2
from test_apply2 import OID, OTHER, OTHER_TABLE, TABLE, Env
from test_schema_cases import schema_source  # noqa: F401 (fixture)
from test_table_hold import add_to_publication, held_row, ingest_once

from cdc_flight import sql_fold
from cdc_flight import table_actions as ta
from cdc_flight.apply import ApplyError
from cdc_flight.apply2 import Apply2
from cdc_flight.ingest_common import append_refusal
from cdc_flight.membership import add_table, remove_table
from cdc_flight.releases import (
    KEYLESS_GUARD,
    RELEASE_STAMPED,
    record_release,
)
from cdc_flight.store import Store, ident

LOCAL = pytest.mark.parametrize("stores", ["local"], indirect=True)

# Every other Flight command, and why it needs no stamp (the test below checks the two cover all; moved
# here from retention_common, which every command imports).
RELEASE_UNSTAMPED = {
    "preflight": "a pre-guard preflight classifies a keyless table no_key and holds it",
    "ingest": "a pre-guard ingest observes no catalog key and holds the table no_key; "
    "clearing that hold forces a key version and a resync",
    "compact": "key-agnostic: expiry only holds a mirror needs_resync and expires pins",
    "epoch": "key-agnostic: holds mirrors and moves the source epoch",
    "delete-source": "key-agnostic: retires the whole source",
}


def other(key, value):
    return pgstream.insert(OTHER, (key, value))


def ins(key, value):
    return pgstream.insert(OID, (key, value))


def mark(env, *, mode="future", keys="[]", catalog=None, held=None):
    """A capture mode this release doesn't know (append was that mode until phase 1b
    added it), registered for public.others."""
    env.log.execute(
        "UPDATE {s}tables SET capture_mode=?,key_columns=?,catalog_key=coalesce(?,catalog_key),"
        "held_reason=? WHERE relation_id=?",
        [mode, keys, catalog, held, OTHER],
    )


@pytest.fixture(params=["sql", "python"])
def fold(request):
    return request.param


def test_the_refusal_rule():
    keyed = '["id"]'
    assert append_refusal("s.t", None, keyed) is None
    assert append_refusal("s.t", None, "[]", keyed) is None  # the catalog key is effective
    assert "future capture" in append_refusal("s.t", "future", keyed)
    assert "no registered key" in append_refusal("s.t", None, "[]")
    assert "no registered key" in append_refusal("s.t", None, "[]", "[]", "dropped")
    # Ingest found no key for a moment (catalog_key 'null'): key_columns still holds.
    assert append_refusal("s.t", None, keyed, "null") is None
    assert append_refusal("s.t", None, keyed, "null", "no_key") is None
    assert append_refusal("s.t", None, keyed, None, "no_key") is None
    assert "no registered key" in append_refusal("s.t", None, "[]", "null")
    # A keyless publication member nobody added, or a removed row: never applied.
    assert append_refusal("s.t", None, "[]", "[]", "unmanaged") is None
    assert append_refusal("s.t", None, "[]", None, "removed") is None
    assert "future capture" in append_refusal("s.t", "future", "[]", None, "unmanaged")
    assert "future capture" in append_refusal("s.t", "future", "[]", append_ok=True)
    # Append (phase 1b): append-aware paths take it; every keyed path still refuses it.
    assert append_refusal("s.t", "append", "[]", append_ok=True) is None
    assert "insert-only" in append_refusal("s.t", "append", "[]")


def mixed_run(stores, monkeypatch, fold, *, guarded):
    """items and others written in the same transactions and units. Guarded: others is
    registered for append capture, so its frames carry an empty key (as that release's
    ingest writes them). Returns the env, the run's results and every batched plan's
    relations."""
    if guarded:
        real = test_apply2.Spool

        def keyless_others(config, keys, *args, **kwargs):
            return real(config, {**keys, OTHER: ()}, *args, **kwargs)

        monkeypatch.setattr(test_apply2, "Spool", keyless_others)
    env = Env(stores, fold=fold, max_commits=3)
    env.apply.start_from_log(TABLE)
    if guarded:
        mark(env)
    else:
        env.apply.start_from_log(OTHER_TABLE)
    plans = []
    real_plan = sql_fold.page_plan_sql

    def spy(frames, relations, only):
        plans.append((sorted(relations), sorted(only)))
        return real_plan(frames, relations, only)

    monkeypatch.setattr(sql_fold, "page_plan_sql", spy)
    for i in range(1, 8):
        env.commit(ins(str(i), f"v{i}"), other(str(i), f"o{i}"))
    env.commit(pgstream.update(OID, (str(2), "v2b"), None), other("9", "o9"))
    env.commit(ins("10", "v10"))
    results = {r.table: r for r in env.apply.run()}
    return env, results, plans


def local_stores(tmp_path, name):
    """A second pair of local stores (the `stores` fixture's shape), for a control run."""
    con = duckdb.connect(str(tmp_path / f"{name}.duckdb"))
    con.execute(f"ATTACH '{tmp_path / (name + '_mirror.duckdb')}' AS {ident(name + '_mirror')}")
    con.execute("SET TimeZone='UTC'")
    store = Store(con, name, name + "_mirror")
    store.initialize()
    return store, None


@LOCAL
def test_a_mixed_page_applies_the_keyed_table_as_before_and_reports_the_append_one(
    stores, monkeypatch, fold, tmp_path
):
    control_stores = local_stores(tmp_path, "control")
    control, expected, control_plans = mixed_run(control_stores, monkeypatch, fold, guarded=False)
    monkeypatch.undo()
    env, results, plans = mixed_run(stores, monkeypatch, fold, guarded=True)
    items = results[TABLE]
    assert (items.status, items.events, items.transactions) == (
        expected[TABLE].status,
        expected[TABLE].events,
        expected[TABLE].transactions,
    )
    assert env.mirror() == control.mirror() and env.mirror()["2"] == "v2b"
    refused = results[OTHER_TABLE]
    assert refused.status == "needs_action" and "future capture" in refused.detail
    assert env.mirror("others") == {}  # no mirror table, no apply_state, nothing written
    assert (
        env.store.con.execute(
            f"SELECT count(*) FROM {env.apply.mirror}._cdc.apply_state WHERE relation_id=?",
            [OTHER],
        ).fetchone()[0]
        == 0
    )
    # The append relation never enters a fold input; the keyed one plans as before.
    for relations, only in plans:
        assert OTHER not in {r[0] for r in relations} and OTHER not in only
    if fold == "sql":
        assert plans and any(OTHER in only for _, only in control_plans)
        keyed = [
            ([r for r in rel if r[0] == OID], [o for o in only if o == OID])
            for rel, only in control_plans
        ]
        assert [(r, o) for r, o in plans] == keyed


@LOCAL
@pytest.mark.parametrize(
    "registered",
    [dict(mode="future"), dict(mode=None, keys="[]", catalog="[]")],
    ids=["future", "keyless"],
)
def test_start_from_log_backfill_and_table_actions_refuse_it(stores, registered):
    from test_backfill2 import BEnv

    env = BEnv(stores)
    mark(env, **registered)
    message = "future capture" if registered["mode"] else "no registered key"
    with pytest.raises(ApplyError, match=message):
        env.apply.start_from_log(OTHER_TABLE)
    with pytest.raises(ApplyError, match=message):
        env.backfill.start(OTHER_TABLE)
    with pytest.raises(ApplyError, match=message):
        ta.observe(env.apply, OTHER_TABLE)
    paused = ta.pause(env.apply, OTHER_TABLE, run_id="p", actor="t")
    assert paused.status == "refused" and message in paused.detail, paused
    result = env.apply.apply_table(OTHER_TABLE, "run")
    assert result.status == "needs_action" and message in result.detail
    # The keyed table beside it is untouched by any of this.
    env.apply.start_from_log(TABLE)
    env.commit(ins("1", "a"))
    env.commit(ins("2", "b"))
    assert env.apply.apply_table(TABLE, "run").status == "caught_up"
    assert env.mirror() == {"1": "a", "2": "b"}


@LOCAL
def test_an_unmanaged_keyless_member_is_reported_as_before(stores):
    env = Env(stores)
    mark(env, mode=None, keys="[]", catalog="[]", held="unmanaged")
    # As on main: an unmanaged row is not a registered table to apply (BY_NAME).
    for call in (
        lambda: env.apply.apply_table(OTHER_TABLE, "run"),
        lambda: env.apply.registered(OTHER_TABLE),
    ):
        with pytest.raises(ApplyError, match="is not registered"):
            call()


@LOCAL
def test_a_keyed_table_whose_catalog_key_is_null_is_not_refused(stores):
    """Ingest writes catalog_key 'null' when it finds no key for a moment (a PK dropped
    between its reads); the registered key_columns stays effective, as on main."""
    env = Env(stores)
    env.apply.start_from_log(TABLE)
    env.commit(ins("1", "a"))
    env.commit(ins("2", "b"))
    env.log.execute("UPDATE {s}tables SET catalog_key='null' WHERE relation_id=?", [OID])
    assert env.apply.registered(TABLE)[0] == OID
    result = env.apply.apply_table(TABLE, "run")
    assert "registered key" not in (result.detail or ""), result


@LOCAL
def test_an_existing_store_gains_the_columns_and_the_releases_table(stores):
    env = Env(stores)
    con = env.store.con
    con.execute(f"ALTER TABLE {env.log.prefix}tables DROP COLUMN capture_mode")
    con.execute(f"ALTER TABLE {env.apply.mirror}._cdc.apply_state DROP COLUMN capture_mode")
    con.execute(f"DROP TABLE {env.log.prefix}releases")
    apply = Apply2(con, env.config, retention=env.retention())
    apply.initialize()
    assert con.execute(f"SELECT capture_mode FROM {env.log.prefix}tables").fetchall()
    con.execute(f"SELECT capture_mode FROM {env.apply.mirror}._cdc.apply_state")
    assert con.execute(f"SELECT count(*) FROM {env.log.prefix}releases").fetchone() == (0,)
    # A current store needs no DDL.
    assert apply.retention.statements(apply.retention.catalog()) == []


@LOCAL
def test_the_release_stamp_is_written_inside_a_flight_only_and_when_it_differs(stores):
    env = Env(stores)
    execute = env.apply.execute

    def stamps():
        return execute(
            "SELECT command,keyless_guard,flight_id,recorded_at::VARCHAR FROM {s}releases ORDER BY 1"
        ).fetchall()

    assert not record_release(execute, "apply", {})  # a local CLI run
    assert not record_release(execute, "compact", {"MOTHERDUCK_FLIGHT_ID": "f1"})
    assert stamps() == []
    assert record_release(execute, "apply", {"MOTHERDUCK_FLIGHT_ID": "f1"})
    (first,) = stamps()
    assert first[:3] == ("apply", KEYLESS_GUARD, "f1")
    assert not record_release(execute, "apply", {"MOTHERDUCK_FLIGHT_ID": "f1"})  # equal: no write
    assert stamps() == [first]  # unchanged: not rewritten
    record_release(execute, "apply", {"MOTHERDUCK_FLIGHT_ID": "f2"})
    assert stamps()[0][2] == "f2"
    # A failure is swallowed: the stamp is best effort.

    def broken(*args, **kwargs):
        raise RuntimeError("boom")

    assert not record_release(broken, "apply", {"MOTHERDUCK_FLIGHT_ID": "f1"})


def test_every_flight_command_stamps_or_says_why_not():
    path = Path(__file__).resolve().parents[1] / "scripts/build_flight.py"
    spec = importlib.util.spec_from_file_location("build_flight", path)
    build_flight = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(build_flight)
    assert not set(RELEASE_STAMPED) & set(RELEASE_UNSTAMPED)
    assert set(RELEASE_STAMPED) | set(RELEASE_UNSTAMPED) == set(build_flight.ENTRY)


# On PostgreSQL: a pre-guard ingest, add-table and remove-table --------------------------


@pytest.mark.postgres
def test_an_append_row_is_held_no_key_refused_by_add_and_still_removable(
    stores,
    schema_source,  # noqa: F811
):
    """A keyless FULL table registered for append capture (as that release's add would
    register it): this release's ingest observes no catalog key and holds it no_key,
    and add-table refuses to adopt it. Once the table has a key, clearing the hold
    records a new key version (a resync), as for any validation hold.
    Remove stays possible."""
    source, ingest, log, backfill, apply = schema_source
    table = f"{source.schema}.t_append"
    source.execute("CREATE TABLE t_append (id int NOT NULL, a text)")
    source.execute("ALTER TABLE t_append REPLICA IDENTITY FULL")
    add_to_publication(source, ingest, "t_append")
    assert ingest_once(ingest, log).unmanaged == [table]
    oid = next(r["relation_id"] for r in log.registry() if r["table_name"] == "t_append")
    log.execute(
        "UPDATE {s}tables SET capture_mode='future',held_reason=NULL,held_detail=NULL,"
        "state='active' WHERE relation_id=?",
        [oid],
    )
    log.forget()
    source.execute("INSERT INTO t_append VALUES (1,'a')")
    ingest_once(ingest, log)
    assert held_row(log, oid)[0] == "no_key"
    versions = log.execute(
        "SELECT count(*) FROM {s}key_changes WHERE relation_id=?", [oid]
    ).fetchone()[0]
    source.execute("ALTER TABLE t_append ADD PRIMARY KEY (id)")
    # With a key, add would adopt (re-key) the row in place: refused for an append row.
    result = add_table(log, apply, source.con, ingest.publication, table)
    assert result.status == "refused" and "future capture" in result.detail
    log.forget()
    ingest_once(ingest, log)
    assert held_row(log, oid)[0] is None
    cut = log.execute(
        "SELECT version,key_columns,cut_lsn FROM {s}key_changes WHERE relation_id=? "
        "ORDER BY version DESC LIMIT 1",
        [oid],
    ).fetchone()
    # A new key version: a mirror of this table holds needs_resync at its cut.
    assert cut is not None and cut[0] > versions and json.loads(cut[1]) == ["id"]
    removed = remove_table(log, apply, table, force=True)  # captured again: as for keyed
    assert removed.status == "removed", removed
    assert held_row(log, oid)[0] == "removed"
