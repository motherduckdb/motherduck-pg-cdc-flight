"""The table-membership Flight (membership_flight): add and remove a table.

Validation and the owner's settings need no store. Add and remove run on real
PostgreSQL (schema_source) with a local change log and mirror and a fake trigger. The
built stub runs in a subprocess on the Phase 2 harness, with its md: connections mapped
to local files. No real Flight is created.
"""

import json
import os
import subprocess
import sys
import uuid
from dataclasses import replace

import pytest
from flight_stub import BACKFILL_FLIGHT, assert_stub_ok, stub_env, stub_for
from test_action_flight import OWNER_ONLY, SHIM, FakeCon, Trigger
from test_backfill import drive
from test_explainer import branches, drift
from test_phase2 import Harness
from test_schema_cases import schema_source  # noqa: F401 (fixture)
from test_table_actions import LOCAL, caught_up_items
from test_table_hold import add_to_publication, ingest_once, recreate

from cdc_flight import action_flight as af
from cdc_flight import membership
from cdc_flight import membership_flight as mf
from cdc_flight.chain import FLIGHT_CONFIG
from cdc_flight.flightlog import Run
from cdc_flight.ingest_log import IngestLog, LeaseBusy

PG = pytest.mark.postgres


def owner_for(ingest, apply):
    return mf.Owner(
        ingest.source_id,
        BACKFILL_FLIGHT,
        slot=ingest.slot,
        publication=ingest.publication,
        heartbeat=ingest.heartbeat,
        database=ingest.database,
        mirror_database=apply.config.mirror_database,
    )


def go(fixture, *, trigger=None, run=None, run_id=None, wait=0, **env):
    """One table-membership run against schema_source: a fresh IngestLog per run, as a
    Flight has (the registry read is cached per instance)."""
    source, ingest, log, _, apply = fixture
    owner = owner_for(ingest, apply)
    fresh = IngestLog(log.con, replace(ingest, tables=()))
    assert mf.ensure_previews(fresh)  # main's attach step
    environ = {f"CDC_{k.upper()}": v for k, v in env.items()}
    return mf.perform(
        fresh,
        apply,
        lambda: source.con,
        af.parse(environ, mf.MEMBERSHIP),
        run_id=run_id or str(uuid.uuid4()),
        actor="flight:test",
        owner=owner,
        trigger=trigger or Trigger(),
        run=run,
        wait=wait,
    )


def dry(fixture, **env):
    out = go(fixture, dry_run="1", **env)
    assert out["status"] == "would", out
    return out["preview_id"]


def previews(fixture):
    _, ingest, log, _, _ = fixture
    mf.ensure_previews(IngestLog(log.con, ingest))
    return log.con.execute(
        f'SELECT id,action,used_at IS NOT NULL,used_by,actor FROM "{ingest.database}".'
        f'"{ingest.source_id}".previews ORDER BY created_at'
    ).fetchall()


def registry(fixture, table):
    source, ingest, log, _, _ = fixture
    schema, name = table.split(".")
    return log.con.execute(
        f'SELECT relation_id,held_reason,key_version FROM "{ingest.database}".'
        f'"{ingest.source_id}".tables WHERE schema_name=? AND table_name=? ORDER BY relation_id',
        [schema, name],
    ).fetchall()


def mirror_state(fixture, table):
    _, _, log, _, apply = fixture
    rel = apply.registered(table)[0]
    return log.con.execute(
        f"SELECT schema_name,table_name,detail,token,guard FROM {apply.mirror}._cdc.apply_state "
        "WHERE relation_id=?",
        [rel],
    ).fetchone()


# Validation (no connection) ---------------------------------------------------------

T = "public.items"
REFUSALS = [
    (dict(ACTION="pause", TABLE=T), "CDC_ACTION must be one of add, remove"),
    (dict(ACTION="add"), "CDC_TABLE is required for add"),
    (dict(ACTION="add", TABLE="public.items; DROP"), "CDC_TABLE must be"),
    (dict(ACTION="add", TABLE=T, FORCE="1"), "CDC_FORCE is not used by add"),
    (dict(ACTION="add", TABLE=T, CONFIRM=f"REMOVE {T}"), "CDC_CONFIRM is not used by add"),
    (dict(ACTION="remove", TABLE=T, FORCE="yes"), "CDC_FORCE must be 0 or 1"),
    (dict(ACTION="remove", TABLE=T, FORCE="1", DRY_RUN="1"), f'exactly "REMOVE {T}"'),
    (
        dict(ACTION="remove", TABLE=T, FORCE="1", CONFIRM=f"remove {T}", DRY_RUN="1"),
        f'exactly "REMOVE {T}"',
    ),
    (
        dict(ACTION="remove", TABLE=T, FORCE="1", CONFIRM=f"REMOVE {T} ", DRY_RUN="1"),
        f'exactly "REMOVE {T}"',
    ),
    (
        dict(ACTION="remove", TABLE=T, CONFIRM=f"REMOVE {T}", DRY_RUN="1"),
        "CDC_CONFIRM is only for a forced remove",
    ),
    (dict(ACTION="remove", TABLE=T), "CDC_PREVIEW_ID is required"),
]


@pytest.mark.parametrize("env, message", REFUSALS)
def test_a_bad_input_is_refused_before_connecting(env, message, monkeypatch, capsys):
    def no_connection(*args, **kwargs):
        raise AssertionError("connected before validating")

    monkeypatch.setattr(mf.duckdb, "connect", no_connection)
    owner = mf.Owner("probe", BACKFILL_FLIGHT)
    environ = {f"CDC_{k}": v for k, v in env.items()}
    assert mf.main(owner, environ) == 2
    out = capsys.readouterr().out
    result = json.loads(out.splitlines()[-1])
    assert result["status"] == "refused" and message in result["detail"], result
    assert "DROP" not in out and "yes" not in out
    assert not drift("table-membership", out, required=["validate", "invalid"])


def test_force_and_confirm_are_hashed_inputs_and_force_0_is_unset():
    forced = af.parse(
        {
            "CDC_ACTION": "remove",
            "CDC_TABLE": T,
            "CDC_FORCE": "1",
            "CDC_CONFIRM": f"REMOVE {T}",
            "CDC_DRY_RUN": "1",
        },
        mf.MEMBERSHIP,
    )
    assert forced.values == {"force": True, "confirm": f"REMOVE {T}"}
    plain = {"CDC_ACTION": "remove", "CDC_TABLE": T, "CDC_DRY_RUN": "1"}
    assert af.parse(plain, mf.MEMBERSHIP).values == {}
    assert af.parse({**plain, "CDC_FORCE": "0"}, mf.MEMBERSHIP).values == {}
    decided = {"row": None, "active_generation": None}
    assert af.preview_hash(forced, decided) != af.preview_hash(
        af.parse(plain, mf.MEMBERSHIP), decided
    )


def test_the_flight_declares_only_its_inputs_and_owner_settings_are_validated():
    keys = set(FLIGHT_CONFIG["table-membership"])
    assert keys == {
        "CDC_ACTION",
        "CDC_TABLE",
        "CDC_FORCE",
        "CDC_CONFIRM",
        "CDC_MODE",
        "CDC_IGNORE_DELETES",
        "CDC_DRY_RUN",
        "CDC_PREVIEW_ID",
        "CDC_REQUEST_ID",
    }
    pipeline = {
        k
        for c, ks in FLIGHT_CONFIG.items()
        if c not in ("table-action", "table-membership", "quarantine", "epoch", "delete-source")
        for k in ks
    }
    assert not keys & pipeline and not keys & OWNER_ONLY
    owner = mf.Owner("shop", BACKFILL_FLIGHT)
    assert (owner.slot, owner.publication, owner.mirror_database, owner.dsn_env) == (
        "md_cdc_shop",
        "md_cdc_shop_pub",
        "shop_mirror",
        "CDC_PG_DSN",
    )
    good = dict(source_id="shop", backfill_flight=BACKFILL_FLIGHT)
    for bad in (
        dict(good, dsn_env="MOTHERDUCK_TOKEN"),
        dict(good, dsn_env="MOTHERDUCK_X_DSN"),
        dict(good, dsn_env="CDC_PG"),
        dict(good, dsn_env="A-B_DSN"),
        dict(good, publication="Pub"),
        dict(good, heartbeat="pulse"),
        dict(good, backfill_flight="x"),
        dict(good, ops_database="cdc_internal"),
        dict(good, compact_flight=BACKFILL_FLIGHT),  # not this Flight's setting
    ):
        with pytest.raises(ValueError):
            mf.Owner.from_dict(bad)


def test_one_role_by_default_membership_reads_the_same_secret_as_ingest(monkeypatch):
    """One PostgreSQL role by default (operations.md): with no --dsn-env, table-membership
    reads the same secret variable as ingest and backfill do with no CDC_DSN_ENV."""
    from cdc_flight.config import BackfillConfig, IngestConfig

    monkeypatch.delenv("CDC_DSN_ENV", raising=False)
    monkeypatch.setenv("CDC_SOURCE_ID", "probe")
    monkeypatch.setenv("CDC_TABLES", '["public.t"]')
    default = mf.Owner(source_id="probe", backfill_flight=BACKFILL_FLIGHT).dsn_env
    monkeypatch.setenv(default, "host=one-role")
    assert IngestConfig.from_env().dsn == "host=one-role"
    assert BackfillConfig.from_env().dsn == "host=one-role"


def test_the_build_bakes_the_secret_name_never_the_secret(tmp_path):
    flags = ["--source", "probe", "--backfill-flight", BACKFILL_FLIGHT, "--dsn-env", "SHOP_DSN"]
    stub = stub_for(tmp_path, "table-membership", flags)
    line = [x for x in stub.splitlines() if x.startswith("OWNER = ")][0]
    owner = json.loads(eval(line.removeprefix("OWNER = ")))
    # Every field but heartbeat_writer, which a primary source's stub omits (replica
    # capture: `self` is baked as before the field existed).
    assert owner["dsn_env"] == "SHOP_DSN"
    assert set(owner) == {f.name for f in mf.fields(mf.Owner)} - {"heartbeat_writer"}
    for flags in (
        [
            "--source",
            "probe",
            "--backfill-flight",
            BACKFILL_FLIGHT,
            "--dsn-env",
            "MOTHERDUCK_TOKEN",
        ],
        [
            "--source",
            "probe",
            "--backfill-flight",
            BACKFILL_FLIGHT,
            "--compact-flight",
            BACKFILL_FLIGHT,
        ],
        ["--source", "probe"],
    ):
        with pytest.raises((TypeError, ValueError)):
            stub_for(tmp_path, "table-membership", flags)
    # table-action never takes the secret's name.
    with pytest.raises((TypeError, ValueError)):
        stub_for(
            tmp_path,
            "table-action",
            ["--source", "probe", "--backfill-flight", BACKFILL_FLIGHT]
            + ["--compact-flight", BACKFILL_FLIGHT, "--dsn-env", "SHOP_DSN"],
        )


# The bounded lease wait ----------------------------------------------------


class FakeLog:
    def __init__(self, busy_for, error=None):
        self.busy_for, self.attempts, self.released = busy_for, 0, 0
        self.error = error or LeaseBusy("held")

    def acquire(self, run_id):
        self.attempts += 1
        if self.attempts <= self.busy_for:
            raise self.error

    def forget(self):
        pass

    def release(self):
        self.released += 1


def leased(log, wait):
    clock = [0.0]

    def sleep(seconds):
        clock[0] += seconds

    result = membership._leased(
        log, lambda run_id: "acted", wait, poll=3, sleep=sleep, clock=lambda: clock[0]
    )
    return result, clock[0]


def test_the_flight_waits_for_the_ingest_lease_within_its_bound():
    log = FakeLog(busy_for=10)
    assert leased(log, 120) == ("acted", 30.0) and log.released == 1
    held = FakeLog(busy_for=10_000)
    result, waited = leased(held, 120)
    assert result is None and 120 <= waited <= 123 and held.released == 0
    once = FakeLog(busy_for=1)
    assert leased(once, 0) == (None, 0.0) and once.attempts == 1  # the CLI: one try


def test_an_acquire_conflict_while_waiting_is_busy_and_any_other_error_fails():
    """Round 1 S1: colliding with ingest's own acquire is a write-write conflict."""
    conflict = mf.duckdb.TransactionException("TransactionContext Error: write-write conflict")
    log = FakeLog(busy_for=3, error=conflict)
    assert leased(log, 120) == ("acted", 9.0)
    held = FakeLog(busy_for=10_000, error=conflict)
    assert leased(held, 120)[0] is None
    other = FakeLog(busy_for=1, error=mf.duckdb.TransactionException("something else"))
    with pytest.raises(mf.duckdb.TransactionException, match="something else"):
        leased(other, 120)


# add and remove on PostgreSQL ----------------------------------------------------------


@LOCAL
@PG
def test_add_previews_then_registers_and_starts_its_backfill(stores, schema_source):  # noqa: F811
    source, ingest, *_ = schema_source
    s = source.schema
    recreate(source, "t_new")
    add_to_publication(source, ingest, "t_new")
    table = f"{s}.t_new"
    out = go(schema_source, dry_run="1", action="add", table=table)
    assert out["status"] == "would" and "register" in out["detail"], out
    assert registry(schema_source, table) == []  # a dry run registers nothing
    trigger = Trigger()
    done = go(
        schema_source, trigger=trigger, action="add", table=table, preview_id=out["preview_id"]
    )
    assert (done["status"], done["result"]) == ("done", "added"), done
    assert trigger.calls == [
        (BACKFILL_FLIGHT, {"CDC_BACKFILL_TABLE": table, "CDC_BACKFILL_START": "1"})
    ]
    assert done["triggered"]["flight"] == "backfill"
    assert not any(step.startswith("run backfill") for step in done["next_steps"])
    assert [p[1:] for p in previews(schema_source)] == [
        ("add", True, done["run_id"], "flight:test")
    ]
    again = go(schema_source, action="add", table=table, preview_id=out["preview_id"])
    assert again["status"] == "already"


@LOCAL
@PG
def test_add_refuses_what_preflight_or_the_publication_refuses(stores, schema_source):  # noqa: F811
    source, ingest, *_ = schema_source
    s = source.schema
    recreate(source, "t_out")  # not in the publication
    out = go(schema_source, dry_run="1", action="add", table=f"{s}.t_out")
    assert out["status"] == "refused" and "ALTER PUBLICATION" in out["detail"]
    heartbeat = go(schema_source, dry_run="1", action="add", table=ingest.heartbeat)
    assert heartbeat["status"] == "refused" and "heartbeat" in heartbeat["detail"]
    missing = go(schema_source, dry_run="1", action="add", table=f"{s}.no_such")
    assert missing["status"] == "refused"
    assert previews(schema_source) == []


@LOCAL
@PG
def test_a_moved_holder_row_refuses_the_add_it_previewed(stores, schema_source):  # noqa: F811
    """The same-named holder's hold is part of the hash."""
    source, ingest, log, _, _ = schema_source
    s = source.schema
    table = f"{s}.t_drop"
    source.execute("DROP TABLE t_drop")
    ingest_once(ingest, log)
    recreate(source, "t_drop")
    add_to_publication(source, ingest, "t_drop")
    preview = dry(schema_source, action="add", table=table)
    ((holder, held, _),) = registry(schema_source, table)
    assert held == "dropped"
    log.con.execute(
        f'UPDATE "{ingest.database}"."{ingest.source_id}".tables SET held_reason=\'replaced\' '
        "WHERE relation_id=?",
        [holder],
    )
    before = registry(schema_source, table)
    trigger = Trigger()
    out = go(schema_source, trigger=trigger, action="add", table=table, preview_id=preview)
    assert out["status"] == "refused" and out["detail"] == af.NO_PREVIEW, out
    assert registry(schema_source, table) == before and trigger.calls == []
    assert not previews(schema_source)[0][2]


@LOCAL
@PG
def test_a_forced_remove_with_a_stale_preview_writes_nothing(stores, schema_source):  # noqa: F811
    """The preview is checked before the apply fence."""
    source, ingest, log, backfill, _ = schema_source
    table = f"{source.schema}.t_default"
    assert drive(backfill, table, ingest, log).status == "published"
    env = dict(action="remove", table=table, force="1", confirm=f"REMOVE {table}")
    fenced, registered = mirror_state(schema_source, table), registry(schema_source, table)
    other = f"{source.schema}.t_type"
    elsewhere = dry(schema_source, **dict(env, table=other, confirm=f"REMOVE {other}"))
    for preview in (str(uuid.uuid4()), elsewhere):
        out = go(schema_source, preview_id=preview, **env)
        assert out["status"] == "refused" and out["detail"] == af.NO_PREVIEW, out
        assert mirror_state(schema_source, table) == fenced
        assert registry(schema_source, table) == registered
    preview = dry(schema_source, **env)
    out = go(schema_source, preview_id=preview, **env)
    assert (out["status"], out["result"]) == ("done", "removed"), out
    assert registry(schema_source, table)[0][1] == "removed"
    assert mirror_state(schema_source, table)[2] == "source table removed"


@LOCAL
@PG
def test_a_relabel_with_a_stale_preview_leaves_the_mirror_alone(stores, schema_source):  # noqa: F811
    source, ingest, log, backfill, _ = schema_source
    s = source.schema
    assert drive(backfill, f"{s}.t_rename", ingest, log).status == "published"
    source.execute("ALTER TABLE t_rename RENAME TO t_renamed")
    ingest_once(ingest, log)
    table = f"{s}.t_renamed"
    preview = dry(schema_source, action="add", table=table)
    before = mirror_state(schema_source, f"{s}.t_rename")
    stale = go(schema_source, action="add", table=table, preview_id=str(uuid.uuid4()))
    assert stale["status"] == "refused" and stale["detail"] == af.NO_PREVIEW
    assert mirror_state(schema_source, f"{s}.t_rename") == before  # not renamed
    trigger = Trigger()
    out = go(schema_source, trigger=trigger, action="add", table=table, preview_id=preview)
    assert (out["status"], out["result"]) == ("done", "relabelled"), out
    assert trigger.calls == []  # its mirror stays exact: no backfill
    assert mirror_state(schema_source, table)[:2] == (s, "t_renamed")
    assert previews(schema_source)[-1][2]


@LOCAL
@PG
def test_a_failure_after_the_claim_rolls_it_back_with_the_registry(
    stores,
    schema_source,  # noqa: F811
    monkeypatch,
):
    source, ingest, *_ = schema_source
    table = f"{source.schema}.t_new"
    recreate(source, "t_new")
    add_to_publication(source, ingest, "t_new")
    preview = dry(schema_source, action="add", table=table)
    real = membership._add

    def broken(*args, **kwargs):
        real(*args, **kwargs)
        raise RuntimeError("fault after the claim and the registry write")

    monkeypatch.setattr(membership, "_add", broken)
    with pytest.raises(RuntimeError, match="fault after the claim"):
        go(schema_source, action="add", table=table, preview_id=preview)
    monkeypatch.undo()
    assert registry(schema_source, table) == [] and not previews(schema_source)[0][2]
    out = go(schema_source, action="add", table=table, preview_id=preview)
    assert out["result"] == "added", out


@LOCAL
@PG
def test_busy_after_the_wait_leaves_the_preview_unspent(stores, schema_source):  # noqa: F811
    source, ingest, log, *_ = schema_source
    table = f"{source.schema}.t_new"
    recreate(source, "t_new")
    add_to_publication(source, ingest, "t_new")
    preview = dry(schema_source, action="add", table=table)
    holder = IngestLog(log.con, ingest)
    holder.acquire("someone-else")
    try:
        out = go(schema_source, action="add", table=table, preview_id=preview, wait=0)
    finally:
        holder.release()
    assert out["status"] == "busy" and not previews(schema_source)[0][2]
    assert go(schema_source, action="add", table=table, preview_id=preview)["result"] == "added"


@LOCAL
@PG
def test_a_failed_backfill_start_after_an_add_points_to_resync(stores, schema_source, capsys):  # noqa: F811
    """The table is added; the way on is its backfill."""
    source, ingest, *_ = schema_source
    table = f"{source.schema}.t_new"
    recreate(source, "t_new")
    add_to_publication(source, ingest, "t_new")
    preview = dry(schema_source, action="add", table=table)
    capsys.readouterr()
    out = go(
        schema_source,
        trigger=Trigger(RuntimeError("no")),
        run=Run(),
        action="add",
        table=table,
        preview_id=preview,
    )
    assert out["status"] == "failed" and "was added" in out["detail"], out
    assert out["next_steps"] == [f"resync {table} (table-action)"]
    assert registry(schema_source, table) and previews(schema_source)[0][2]
    assert not drift("table-membership", capsys.readouterr().out, required=["trigger"])


@LOCAL
@PG
def test_remove_lines_match_the_chart_and_the_cli_agrees(stores, schema_source, capsys):  # noqa: F811
    source, ingest, log, _, apply = schema_source
    s = source.schema
    capsys.readouterr()
    taken = set()

    def lines(**env):
        run = Run()
        run.step("validate", "ok", branch="ok")
        out = go(schema_source, run=run, **env)
        text = capsys.readouterr().out
        assert not drift("table-membership", text, required=["validate", "decide"]), text
        taken.update(branches(text))
        return out

    table = f"{s}.t_trunc"
    live = lines(dry_run="1", action="remove", table=table)
    assert live["status"] == "refused" and "--force" in live["detail"]
    cli = membership.remove_table(IngestLog(log.con, ingest), apply, table, dry_run=True)
    assert (cli.status, cli.detail) == (live["result"], live["detail"])
    env = dict(action="remove", table=table, force="1", confirm=f"REMOVE {table}")
    preview = lines(dry_run="1", **env)["preview_id"]
    assert lines(preview_id=str(uuid.uuid4()), **env)["status"] == "refused"
    assert lines(preview_id=preview, **env)["status"] == "done"
    assert lines(preview_id=preview, **env)["status"] == "already"
    assert taken >= {
        ("decide", "would"),
        ("decide", "refused"),
        ("decide", "act"),
        ("outcome", "done"),
        ("outcome", "refused"),
        ("outcome", "already"),
    }


# The built stub, in a subprocess --------------------------------------------------------

SECRET_DSN = (
    "host=127.0.0.1 port=1 dbname=shopdb user=shopuser password=s3cr3t-pw connect_timeout=2"
)


@LOCAL
def test_the_stub_uses_only_baked_settings_and_never_prints_the_secret(stores, tmp_path):
    """Every key any Flight declares is set to an attacker value. remove never connects
    to PostgreSQL; add connects only through the baked secret name, and a refused
    connection prints no part of the DSN."""
    h = Harness(stores, tmp_path)
    caught_up_items(h)
    database, mirror = h.config.database, h.config.mirror_database
    files = {
        database: str(tmp_path / f"{database}.duckdb"),
        mirror: str(tmp_path / f"{mirror}.duckdb"),
    }
    for con in {id(s.con): s.con for s in stores}.values():
        con.close()
    flags = ["--source", "phase2", "--database", database, "--mirror", mirror]
    flags += ["--slot", "slot", "--publication", "pub", "--dsn-env", "SHOP_DSN"]
    flags += ["--backfill-flight", BACKFILL_FLIGHT]
    flight_py = tmp_path / "flight.py"
    flight_py.write_text(stub_for(tmp_path, "table-membership", flags))
    shim, log = tmp_path / "shim.py", tmp_path / "shim.log"
    shim.write_text(
        SHIM.replace(
            "duckdb.connect = connect\n",
            "duckdb.connect = connect\n"
            "import psycopg2\n"
            "real_pg = psycopg2.connect\n"
            "def pg_connect(*a, **k):\n"
            "    LOG.write(json.dumps(['pg', k.get('dbname'), k.get('port')]) + '\\n')\n"
            "    LOG.flush()\n"
            "    return real_pg(*a, **k)\n"
            "psycopg2.connect = pg_connect\n",
        )
    )
    attacker = {k: "evil" for c in FLIGHT_CONFIG.values() for k in c}
    attacker.update(CDC_DSN_ENV="MOTHERDUCK_TOKEN", CDC_DATABASE="evil_db", CDC_PG_DSN="evil")

    def flight(**inputs):
        env = {
            "PATH": os.environ["PATH"],
            "MOTHERDUCK_TOKEN": "local-run-placeholder-not-a-motherduck-token",
            "MOTHERDUCK_FLIGHT_RUN_ID": str(uuid.uuid4()),
            "SHOP_DSN": SECRET_DSN,
            "SHIM_FILES": json.dumps(files),
            "SHIM_LOG": str(log),
            **stub_env(tmp_path),
            **attacker,
            **{k: "" for k in FLIGHT_CONFIG["table-membership"]},
            **{f"CDC_{k.upper()}": v for k, v in inputs.items()},
        }
        result = subprocess.run(
            [sys.executable, str(shim), str(flight_py)],
            capture_output=True,
            text=True,
            cwd=tmp_path,
            env=env,
        )
        assert_stub_ok(result.stdout)
        out = [line for line in result.stdout.splitlines() if line.startswith("{")]
        return result, json.loads(out[-1]) if out else None, env["MOTHERDUCK_FLIGHT_RUN_ID"]

    remove = dict(action="remove", table="public.items", force="1", confirm="REMOVE public.items")
    result, would, run_id = flight(dry_run="1", **remove)
    assert result.returncode == 0 and would["status"] == "would", result.stderr[-2000:]
    result, done, _ = flight(preview_id=run_id, **remove)
    assert result.returncode == 0 and done["result"] == "removed", result.stderr[-2000:]
    result, _, _ = flight(dry_run="1", action="add", table="public.other")
    assert result.returncode == 1  # the refused PostgreSQL connection fails the run
    events = [json.loads(line) for line in log.read_text().splitlines()]
    assert {e[1] for e in events if e[0] == "connect"} == {f"md:{mirror}?attach_mode=single"}
    assert {e[1] for e in events if e[0] == "attach"} <= {database, "cdc_ops"}
    assert [e for e in events if e[0] == "pg"] == [["pg", "shopdb", "1"]]  # add only
    assert not any("evil" in json.dumps(e) for e in events)
    lines = [x for x in result.stdout.splitlines() if x.startswith("@flight-run")]
    assert lines and not any(part in x for x in lines for part in ("shopuser", "shopdb", "port"))
    for part in ("s3cr3t-pw", SECRET_DSN):
        assert part not in result.stdout and part not in result.stderr


def test_finish_audits_membership_runs_with_its_owner():
    calls = []

    def audit(*args, **kwargs):
        calls.append((args, kwargs))
        return True

    con = FakeCon()
    environ = {
        "CDC_ACTION": "remove",
        "CDC_TABLE": T,
        "CDC_DRY_RUN": "1",
        "CDC_REQUEST_ID": "dive-1",
    }
    inputs = af.parse(environ, mf.MEMBERSHIP)
    owner = mf.Owner("shop", BACKFILL_FLIGHT, ops_database="ops")
    af.finish(con, owner, inputs, dict(status="done", run_id="r"), Run(), audit=audit)
    ((args, kwargs),) = calls
    assert args[1:5] == ("cdc_internal", "shop", "r", "remove") and kwargs == {
        "database": "ops",
        "started_by": None,
        "tls": None,  # finish passes it through; membership's main passes pg_ca.fields()
        "request_id": "dive-1",
    }
    assert con.statements == ['CREATE SNAPSHOT OF "ops"']


@LOCAL
def test_membership_main_records_who_started_the_run(stores, tmp_path, monkeypatch, capsys):
    """Membership's main wires whois into the actor (previews.actor, written in
    the action's transaction) and into finish's audit copy."""
    import re

    from test_quarantine_flight import WHOIS_CALL, runs_table

    h = Harness(stores, tmp_path)
    caught_up_items(h)
    database, mirror = h.config.database, h.config.mirror_database
    files = {
        database: str(tmp_path / f"{database}.duckdb"),
        mirror: str(tmp_path / f"{mirror}.duckdb"),
    }
    for con in {id(s.con): s.con for s in stores}.values():
        con.close()
    real_connect, environ = mf.duckdb.connect, {}

    class Con:
        def __init__(self, con):
            self.con = con

        def execute(self, statement, parameters=None):
            attach = re.fullmatch(r"ATTACH IF NOT EXISTS 'md:(\w+)' AS \"(\w+)\"", statement)
            if attach:
                statement = (
                    f"ATTACH IF NOT EXISTS '{files[attach.group(1)]}' AS \"{attach.group(2)}\""
                )
            if WHOIS_CALL in statement:
                sql = runs_table(
                    self.con, environ["MOTHERDUCK_FLIGHT_ID"], environ["MOTHERDUCK_FLIGHT_RUN_ID"]
                )
                return self.con.execute(sql, parameters)
            return self.con.execute(statement, parameters or [])

        def __getattr__(self, name):
            return getattr(self.con, name)

    def connect(address, *args, **kwargs):
        name = re.fullmatch(r"md:(\w+)\?attach_mode=single", address).group(1)
        return Con(real_connect(files[name]))

    finished = []
    monkeypatch.setattr(mf.duckdb, "connect", connect)
    monkeypatch.setattr(mf, "finish", lambda *a, **k: finished.append(k.get("started_by")))
    owner = mf.Owner(
        "phase2",
        BACKFILL_FLIGHT,
        slot="slot",
        publication="pub",
        database=database,
        mirror_database=mirror,
    )
    remove = {
        "CDC_ACTION": "remove",
        "CDC_TABLE": "public.items",
        "CDC_FORCE": "1",
        "CDC_CONFIRM": "REMOVE public.items",
    }
    environ.update(
        MOTHERDUCK_FLIGHT_ID=str(uuid.uuid4()), MOTHERDUCK_FLIGHT_RUN_ID=str(uuid.uuid4())
    )
    assert mf.main(owner, dict(environ, **remove, CDC_DRY_RUN="1")) == 0
    preview = environ["MOTHERDUCK_FLIGHT_RUN_ID"]
    environ["MOTHERDUCK_FLIGHT_RUN_ID"] = str(uuid.uuid4())
    assert mf.main(owner, dict(environ, **remove, CDC_PREVIEW_ID=preview)) == 0
    assert "@flight-run whois ok" in capsys.readouterr().out
    assert finished == ["alice", "alice"]
    con = real_connect(files[database])
    used, actor = con.execute("SELECT used_at IS NOT NULL, actor FROM phase2.previews").fetchone()
    assert used and actor.endswith(" (started by alice)"), actor
