"""The quarantine Flight (quarantine_flight) and the run-starter lookup (whois).

Local DuckDB on the Phase 2 harness (its poison transaction); the built stub runs in
a subprocess with its md: connections mapped to local files. No real Flight is created.
"""

import json
import os
import subprocess
import sys
import time
import uuid

import duckdb
import pytest
from flight_stub import assert_stub_ok, stub_env, stub_for
from test_action_flight import OWNER_ONLY, SHIM, Trigger
from test_explainer import branches, drift
from test_phase2 import Harness
from test_table_actions import LOCAL, OTHER_T, caught_up_items, poisoned, row

from cdc_flight import action_flight as af
from cdc_flight import quarantine_flight as qf
from cdc_flight import table_actions
from cdc_flight.chain import FLIGHT_CONFIG
from cdc_flight.config import lsn_text
from cdc_flight.flightlog import Run


def owner(h):
    return qf.Owner(
        h.config.source_id,
        database=h.config.database,
        mirror_database=h.config.mirror_database,
    )


def blocked_unit(h, table=OTHER_T):
    """(epoch, start, end) of the whole unit the table is held before."""
    _, observed = table_actions.observe(h.apply, table)
    return observed["source_epoch"], observed["unit_start_lsn"], observed["unit_end_lsn"]


def env_for(h, epoch=1, table=OTHER_T, **extra):
    """The quarantine inputs for the observed blocked unit: its exact UNIT literal.
    Computed before acting (once quarantined, no unit is blocked)."""
    unit_epoch, start, end = blocked_unit(h, table)
    return dict(
        action="quarantine",
        table=table,
        expected_epoch=str(epoch),
        confirm=af.unit_literal(table, unit_epoch, start, end),
        **extra,
    )


def go(h, run=None, actor="flight:test", **env):
    environ = {f"CDC_{k.upper()}": v for k, v in env.items()}
    return af.perform(
        h.apply,
        af.parse(environ, qf.QUARANTINE),
        run_id=str(uuid.uuid4()),
        actor=actor,
        owner=owner(h),
        trigger=Trigger(),
        run=run,
    )


def quarantine_rows(h):
    return h.store.con.execute(
        f"SELECT commit_lsn,actor,reason FROM {h.apply.mirror}._cdc.quarantine"
    ).fetchall()


def preview_used(h):
    return h.store.con.execute(
        f"SELECT used_at IS NOT NULL,actor FROM {h.apply.mirror}._cdc.previews"
    ).fetchall()


# Validation and keys -----------------------------------------------------------------

T = OTHER_T
GOOD = dict(
    ACTION="quarantine",
    TABLE=T,
    EXPECTED_EPOCH="1",
    CONFIRM=f"QUARANTINE {T} UNIT 1 0/5A..0/6A",
    DRY_RUN="1",
)
REFUSALS = [
    (dict(GOOD, ACTION="pause"), "CDC_ACTION must be one of quarantine"),
    (dict(GOOD, CONFIRM=""), "CDC_CONFIRM is required"),
    (dict(GOOD, EXPECTED_EPOCH="0"), "CDC_EXPECTED_EPOCH must be"),
    (dict(GOOD, EXPECTED_EPOCH="1; drop"), "CDC_EXPECTED_EPOCH must be"),
    (dict(GOOD, TABLE="public.other'--"), "CDC_TABLE must be"),
    (dict(GOOD, CONFIRM="x" * 201), "CDC_CONFIRM is too long"),
    (dict(GOOD, DRY_RUN=""), "CDC_PREVIEW_ID is required"),
]


@pytest.mark.parametrize("env, message", REFUSALS)
def test_a_bad_input_is_refused_before_connecting(env, message, monkeypatch, capsys):
    def no_connection(*args, **kwargs):
        raise AssertionError("connected before validating")

    monkeypatch.setattr(af.duckdb, "connect", no_connection)
    environ = {f"CDC_{k}": v for k, v in env.items()}
    assert qf.main(qf.Owner("probe"), environ) == 2
    out = capsys.readouterr().out
    result = json.loads(out.splitlines()[-1])
    assert result["status"] == "refused" and message in result["detail"], result
    for bad in ("1; drop", "'--", "zz"):
        assert bad not in out
    assert not drift("quarantine", out, required=["validate", "invalid"])


def test_the_flight_declares_only_its_inputs_and_the_owner_is_validated():
    keys = set(FLIGHT_CONFIG["quarantine"])
    assert keys == {
        "CDC_ACTION",
        "CDC_TABLE",
        "CDC_EXPECTED_EPOCH",
        "CDC_CONFIRM",
        "CDC_DRY_RUN",
        "CDC_PREVIEW_ID",
        "CDC_REQUEST_ID",
    }
    actions = ("table-action", "table-membership", "quarantine", "epoch", "delete-source")
    pipeline = {k for c, ks in FLIGHT_CONFIG.items() if c not in actions for k in ks}
    assert not keys & pipeline and not keys & OWNER_ONLY
    assert qf.Owner("shop").mirror_database == "shop_mirror"
    for bad in (
        dict(source_id="Shop"),
        dict(source_id="shop", ops_database="cdc_internal"),
        dict(source_id="shop", backfill_flight=str(uuid.uuid4())),  # not this Flight's
    ):
        with pytest.raises((ValueError, TypeError)):
            qf.Owner.from_dict(bad)


def test_the_build_bakes_only_the_source_settings(tmp_path):
    stub = stub_for(tmp_path, "quarantine", ["--source", "probe"])
    line = [x for x in stub.splitlines() if x.startswith("OWNER = ")][0]
    assert json.loads(eval(line.removeprefix("OWNER = "))) == dict(
        source_id="probe",
        database="cdc_internal",
        mirror_database="probe_mirror",
        ops_database="cdc_ops",
    )
    with pytest.raises((TypeError, ValueError)):
        stub_for(tmp_path, "quarantine", ["--source", "probe", "--dsn-env", "CDC_PG_DSN"])


# The preview lifecycle (the poison transaction) -----------------------------------------


@LOCAL
def test_a_dry_run_then_the_matching_real_run_quarantines_the_poison_commit(stores, tmp_path):
    h = Harness(stores, tmp_path)
    caught_up_items(h)
    poison = poisoned(h)
    env = env_for(h)
    would = go(h, dry_run="1", **env)
    assert would["status"] == "would" and "may diverge until a resync" in would["detail"]
    assert would["confirm"] == env["confirm"]  # the literal the real run repeats
    assert row(h, OTHER_T)[0] == "needs_action" and quarantine_rows(h) == []
    done = go(h, preview_id=would["preview_id"], actor="flight:test (started by alice)", **env)
    assert done["status"] == "done" and done["next_steps"] == [f"resync {OTHER_T}"], done
    assert row(h, OTHER_T)[0] == "live"
    ((lsn, actor, reason),) = quarantine_rows(h)
    assert lsn == poison and actor == "flight:test (started by alice)" and "diverge" in reason
    assert preview_used(h) == [(True, "flight:test (started by alice)")]
    again = go(h, preview_id=would["preview_id"], **env)
    assert again["status"] == "already", again
    # The repeat's literal comes from the stored row's epoch; a differing
    # expected epoch is refused, never reported as already.
    stale = go(h, preview_id=would["preview_id"], **dict(env, expected_epoch="2"))
    assert stale["status"] == "refused" and "expected epoch 2" in stale["detail"], stale


@LOCAL
@pytest.mark.parametrize(
    "change",
    ["wrong confirm", "lowercase confirm", "stale epoch", "other lsn"],
)
def test_a_wrong_literal_or_stale_value_is_refused_and_writes_nothing(stores, tmp_path, change):
    h = Harness(stores, tmp_path)
    caught_up_items(h)
    poisoned(h)
    env = env_for(h, dry_run="1")
    literal = env["confirm"]
    if change == "wrong confirm":
        env["confirm"] = f"QUARANTINE {OTHER_T}"
    elif change == "lowercase confirm":
        env["confirm"] = env["confirm"].lower()
    elif change == "stale epoch":
        env["expected_epoch"] = "2"
    else:  # another unit: its end moved by one
        epoch, start, end = blocked_unit(h)
        env["confirm"] = af.unit_literal(OTHER_T, epoch, start, end + 1)
    env.pop("dry_run")
    out = go(h, preview_id=str(uuid.uuid4()), **env)  # the confirmation is checked on a real run
    assert out["status"] == "refused", out
    if "confirm" in change:
        assert literal in out["detail"]  # the literal to type
    assert quarantine_rows(h) == [] and row(h, OTHER_T)[0] == "needs_action"


@LOCAL
def test_a_moved_decision_field_refuses_and_rolls_back(stores, tmp_path):
    h = Harness(stores, tmp_path)
    caught_up_items(h)
    poisoned(h)
    env = env_for(h)
    preview = go(h, dry_run="1", **env)["preview_id"]
    rel = h.apply.registered(OTHER_T)[0]
    h.store.con.execute(
        f"UPDATE {h.apply.mirror}._cdc.apply_state SET generation_id='moved' WHERE relation_id=?",
        [rel],
    )
    out = go(h, preview_id=preview, **env)
    assert out["status"] == "refused" and out["detail"] == af.NO_PREVIEW, out
    assert quarantine_rows(h) == [] and preview_used(h) == [(False, None)]


@LOCAL
def test_quarantine_lines_match_the_chart(stores, tmp_path, capsys):
    h = Harness(stores, tmp_path)
    caught_up_items(h)
    poisoned(h)
    env = env_for(h)
    capsys.readouterr()
    taken = set()

    def lines(**env):
        run = Run()
        run.step("validate", "ok", branch="ok")
        out = go(h, run=run, **env)
        text = capsys.readouterr().out
        assert not drift("quarantine", text, required=["validate", "decide"]), text
        taken.update(branches(text))
        return out

    preview = lines(dry_run="1", **env)["preview_id"]
    assert lines(preview_id=str(uuid.uuid4()), **env)["status"] == "refused"
    assert lines(preview_id=preview, **env)["status"] == "done"
    assert lines(dry_run="1", **env)["status"] == "already"
    assert taken >= {
        ("decide", "would"),
        ("decide", "act"),
        ("decide", "already"),
        ("outcome", "done"),
        ("outcome", "refused"),
    }


# The run-starter lookup, on real DuckDB -----------------------------

FLIGHT_ENV = dict(
    MOTHERDUCK_FLIGHT_ID=str(uuid.uuid4()), MOTHERDUCK_FLIGHT_RUN_ID=str(uuid.uuid4())
)

WHOIS_CALL = 'MD_LIST_FLIGHT_RUNS(flight_id := ?::UUID, "limit" := 100)'


def runs_table(con, flight, run_id):
    """A stand-in for MD_LIST_FLIGHT_RUNS keyed on both ids: this run is 'alice'; another
    run of the same Flight ('mallory', listed first) and this run id under another
    Flight ('eve') must never be picked."""
    con.execute(
        "CREATE OR REPLACE TABLE whois_runs (flight_id VARCHAR, run_id UUID, started_by_name VARCHAR)"
    )
    con.execute(
        "INSERT INTO whois_runs (flight_id, run_id, started_by_name) VALUES "
        "(?, ?::UUID, 'mallory'), (?, ?::UUID, 'eve'), (?, ?::UUID, 'alice')",
        [flight, str(uuid.uuid4()), str(uuid.uuid4()), run_id, flight, run_id],
    )
    return af.WHOIS_SQL.replace(
        WHOIS_CALL, "(SELECT * FROM whois_runs WHERE flight_id=?::UUID::VARCHAR)"
    )


def whois_lines(con, environ, capsys, **kwargs):
    capsys.readouterr()
    name = af.whois(con, environ, Run(), **kwargs)
    return name, [x.split(" ts=")[0] for x in capsys.readouterr().out.splitlines()]


def test_whois_is_skipped_outside_a_flight(capsys):
    con = duckdb.connect()
    assert whois_lines(con, {}, capsys) == (None, ["@flight-run whois skipped"])


def test_whois_on_a_connection_without_the_function_is_none_and_visible(capsys):
    con = duckdb.connect()  # plain DuckDB: no MD_LIST_FLIGHT_RUNS
    name, lines = whois_lines(con, FLIGHT_ENV, capsys)
    assert name is None
    assert lines == ["@flight-run whois failed error=CatalogException"], lines
    assert con.execute("SELECT 1").fetchone() == (1,)


def test_a_slow_lookup_is_interrupted_and_the_connection_stays_usable(capsys):
    con = duckdb.connect()
    slow = (
        "SELECT count(*)::VARCHAR FROM range(200000000) a, range(200000000) b "
        "WHERE ?::VARCHAR IS NOT NULL AND ?::VARCHAR IS NOT NULL"
    )
    started = time.monotonic()
    name, lines = whois_lines(con, FLIGHT_ENV, capsys, seconds=0.5, sql=slow)
    assert name is None and time.monotonic() - started < 10
    assert lines == ["@flight-run whois failed error=InterruptException"], lines
    assert con.execute("SELECT 42").fetchone() == (42,)
    time.sleep(0.7)  # the cancelled timer never fires into a later query
    assert con.execute("SELECT 43").fetchone() == (43,)


def test_the_real_predicate_picks_this_runs_starter_only(capsys):
    """The real WHOIS_SQL text, with only the function swapped for a keyed table."""
    con = duckdb.connect()
    assert WHOIS_CALL in af.WHOIS_SQL
    sql = runs_table(
        con, FLIGHT_ENV["MOTHERDUCK_FLIGHT_ID"], FLIGHT_ENV["MOTHERDUCK_FLIGHT_RUN_ID"]
    )
    assert whois_lines(con, FLIGHT_ENV, capsys, sql=sql) == ("alice", ["@flight-run whois ok"])
    other = dict(FLIGHT_ENV, MOTHERDUCK_FLIGHT_RUN_ID=str(uuid.uuid4()))
    name, lines = whois_lines(con, other, capsys, sql=sql)
    assert name is None and lines == ["@flight-run whois failed error=NoRow"], lines


def test_the_timer_is_cancelled_before_the_action_runs(capsys):
    """A lost cancel would interrupt the action's statement after `seconds`."""
    con = duckdb.connect()
    stub = "SELECT 'alice' WHERE ?::VARCHAR IS NOT NULL AND ?::VARCHAR IS NOT NULL"
    assert whois_lines(con, FLIGHT_ENV, capsys, seconds=0.3, sql=stub)[0] == "alice"
    started = time.monotonic()
    total = con.execute(
        "SELECT sum(a.range*b.range) FROM range(30000) a, range(30000) b"
    ).fetchone()
    assert total[0] > 0 and time.monotonic() - started > 0.5  # it ran across the deadline


def test_a_found_name_is_returned_and_reaches_the_actor(capsys):
    con = duckdb.connect()
    stub = "SELECT 'alice' WHERE ?::VARCHAR IS NOT NULL AND ?::VARCHAR IS NOT NULL"
    assert whois_lines(con, FLIGHT_ENV, capsys, sql=stub) == ("alice", ["@flight-run whois ok"])
    assert af.actor_of(con, "alice").endswith(" (started by alice)")
    assert " (started by" not in af.actor_of(con, None)


# The built stub, in a subprocess ----------------------------------------------------------


@LOCAL
def test_the_stub_writes_who_started_it_in_the_quarantine_row(stores, tmp_path):
    """End to end on the real stub: MD_LIST_FLIGHT_RUNS answers 'alice' through the shim,
    and the name lands in the quarantine row written in the action's transaction. Every
    key any Flight declares is set to an attacker value; only the baked stores are used."""
    h = Harness(stores, tmp_path)
    caught_up_items(h)
    poisoned(h)
    env = env_for(h)  # read before the stores' connections close
    database, mirror = h.config.database, h.config.mirror_database
    files = {
        database: str(tmp_path / f"{database}.duckdb"),
        mirror: str(tmp_path / f"{mirror}.duckdb"),
    }
    for con in {id(s.con): s.con for s in stores}.values():
        con.close()
    flight_py = tmp_path / "flight.py"
    flags = ["--source", "phase2", "--database", database, "--mirror", mirror]
    flight_py.write_text(stub_for(tmp_path, "quarantine", flags))
    shim, log = tmp_path / "shim.py", tmp_path / "shim.log"
    shim.write_text(
        SHIM.replace(
            '        if "MD_RUN_FLIGHT" in statement:',
            '        if "MD_LIST_FLIGHT_RUNS" in statement:\n'
            '            LOG.write(json.dumps(["whois", parameters]) + "\\n")\n'
            "            self.con.execute(RUNS_DDL)\n"
            "            self.con.execute(RUNS_ROWS, RUNS_PARAMS)\n"
            "            return self.con.execute(statement.replace(WHOIS_CALL, RUNS_SUB), parameters)\n"
            '        if "MD_RUN_FLIGHT" in statement:',
        )
    )
    flight_id = str(uuid.uuid4())
    shim.write_text(
        shim.read_text().replace(
            "duckdb.connect = connect\n",
            "duckdb.connect = connect\n"
            f"WHOIS_CALL = {WHOIS_CALL!r}\n"
            "RUNS_SUB = '(SELECT * FROM temp.whois_runs WHERE flight_id=?::UUID::VARCHAR)'\n"
            "RUNS_DDL = 'CREATE OR REPLACE TEMP TABLE whois_runs "
            "(flight_id VARCHAR, run_id UUID, started_by_name VARCHAR)'\n"
            "RUNS_ROWS = ('INSERT INTO temp.whois_runs (flight_id, run_id, started_by_name) VALUES '\n"
            "    \"(?, ?::UUID, 'mallory'), (?, ?::UUID, 'eve'), (?, ?::UUID, 'alice')\")\n"
            "_F, _R = os.environ['MOTHERDUCK_FLIGHT_ID'], os.environ['MOTHERDUCK_FLIGHT_RUN_ID']\n"
            "RUNS_PARAMS = [_F, '00000000-0000-0000-0000-0000000000aa', "
            "'00000000-0000-0000-0000-0000000000bb', _R, _F, _R]\n",
        )
    )
    attacker = {k: "evil" for c in FLIGHT_CONFIG.values() for k in c}
    attacker.update(CDC_DATABASE="evil_db", CDC_MIRROR_DATABASE="evil_mirror")

    def flight(**inputs):
        run_id = str(uuid.uuid4())
        env = {
            "PATH": os.environ["PATH"],
            "MOTHERDUCK_TOKEN": "local-run-placeholder-not-a-motherduck-token",
            "MOTHERDUCK_FLIGHT_ID": flight_id,
            "MOTHERDUCK_FLIGHT_RUN_ID": run_id,
            "SHIM_FILES": json.dumps(files),
            "SHIM_LOG": str(log),
            **stub_env(tmp_path),
            **attacker,
            **{k: "" for k in FLIGHT_CONFIG["quarantine"]},
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
        out = [x for x in result.stdout.splitlines() if x.startswith("{")]
        assert out, result.stderr[-2000:]
        return result, json.loads(out[-1]), run_id

    result, would, run_id = flight(dry_run="1", **env)
    assert result.returncode == 0 and would["status"] == "would", result.stderr[-1500:]
    assert "@flight-run whois ok" in result.stdout
    result, done, _ = flight(preview_id=run_id, **env)
    assert result.returncode == 0 and done["status"] == "done", result.stderr[-1500:]
    events = [json.loads(x) for x in log.read_text().splitlines()]
    assert {e[1] for e in events if e[0] == "connect"} == {f"md:{mirror}?attach_mode=single"}
    assert not any("evil" in json.dumps(e) for e in events)
    con = duckdb.connect(files[mirror])
    actor = con.execute("SELECT actor FROM _cdc.quarantine").fetchone()[0]
    assert actor.endswith(" (started by alice)"), actor


# The audit copy carries who started the run ------------------------------------


def test_finish_passes_who_started_the_run_to_the_audit_copy():
    from test_action_flight import BACKFILL_FLIGHT, COMPACT_FLIGHT, FakeCon

    calls = []

    def audit(*args, **kwargs):
        calls.append(kwargs)
        return True

    inputs = af.parse({"CDC_ACTION": "compact-now", "CDC_DRY_RUN": "1"})
    owner_ = af.Owner("shop", BACKFILL_FLIGHT, COMPACT_FLIGHT, ops_database="ops")
    af.finish(
        FakeCon(),
        owner_,
        inputs,
        dict(status="would", run_id="r"),
        Run(),
        audit=audit,
        started_by="alice",
    )
    assert calls == [{"database": "ops", "started_by": "alice", "tls": None, "request_id": None}]


@LOCAL
def test_write_audit_stores_started_by_and_widens_an_older_audit_table(stores, tmp_path):
    from cdc_flight.ops import write_audit

    h = Harness(stores, tmp_path)
    con = stores[0].con
    con.execute(f"ATTACH IF NOT EXISTS '{tmp_path / 'cdc_ops.duckdb'}' AS cdc_ops")
    # An audit table from before started_by (the original shape): the write adds the column.
    con.execute(
        "CREATE TABLE cdc_ops.main.audit (source_id VARCHAR,run_id VARCHAR,written_at TIMESTAMPTZ,"
        "token_user VARCHAR,action VARCHAR,inputs VARCHAR,dry_run BOOLEAN,result VARCHAR)"
    )
    args = (
        h.config.database,
        h.config.source_id,
        "run-1",
        "quarantine",
        {"table": "t"},
        False,
        "ok",
    )
    assert write_audit(con, *args, started_by="alice")
    assert write_audit(con, *args[:2], "run-2", *args[3:])  # no starter known
    assert con.execute(
        "SELECT run_id,started_by FROM cdc_ops.main.audit ORDER BY run_id"
    ).fetchall() == [("run-1", "alice"), ("run-2", None)]


# The remote whole-unit quarantine node. The only format-2 destructive action
# path end to end through main(): a REAL poison (poisoned_unit_env), a dry run, a
# pre-COMMIT failure that lands nothing, the same preview then committing with a lost
# response, the rerun reporting already with one spend, and the next apply passing the
# unit END without its rows. Facts are read on a fresh instance after each outcome.

QUARANTINED = "public.others"


def _facts_sql(mirror):
    """One statement, one snapshot: the preview's spend, the quarantine row,
    apply_state and the runs audit, plus each table's fingerprint."""
    m = f'"{mirror}"._cdc.'

    # The action's lease (acquire, release) writes apply_state's lease columns outside
    # its transaction by design; the fingerprint is of the facts, not the lease.
    lease = " EXCLUDE (token,holder,expires_at,guard,updated_at)"

    def fp(table):
        cols = lease if table == "apply_state" else ""
        return (
            f"(SELECT [count(*), coalesce(sum(hash(t)),0)] FROM (SELECT *{cols} FROM {m}{table}) t)"
        )

    return (
        f"SELECT (SELECT count(*) FILTER (WHERE used_at IS NOT NULL) FROM {m}previews),"
        f"(SELECT list([unit_start_lsn,unit_end_lsn,commit_lsn] ORDER BY unit_start_lsn) "
        f"FROM {m}quarantine),"
        f"(SELECT [state,applied_through_lsn::VARCHAR,blocked_unit_start_lsn::VARCHAR] "
        f"FROM {m}apply_state WHERE schema_name='public' AND table_name='others'),"
        f"(SELECT count(*) FROM {m}runs),"
        + ",".join(fp(t) for t in ("previews", "quarantine", "apply_state", "runs"))
    )


PROBE = "s1_probe"  # attached on the run's connections only (S1's self-proof)
# The real connect, captured at import: the node patches duckdb.connect (the module
# attribute main() uses) for the run, and readers must never go through that patch.
REAL_CONNECT = duckdb.connect


class _Reader:
    """Facts read after each outcome. Remote: a NEW md: instance (a distinct
    connection string, so not the run's cached instance), opened and closed per read,
    and proven new: the run's connections attach PROBE, which a reader must not see.
    Local: the store's second connection (one process can't open a second instance)."""

    def __init__(self, stores, database, mirror):
        self.stores, self.database, self.mirror = stores, database, mirror
        self.reads = 0

    def read(self, work):
        """One read on a fresh instance: token fail-fast, PROBE absent, closed in finally."""
        self.reads += 1
        if not self.stores[0].remote:
            return work(self.stores[1].con)
        import os

        if not (os.getenv("MOTHERDUCK_TOKEN") or os.getenv("motherduck_token")):
            raise RuntimeError("MOTHERDUCK_TOKEN is not set: refusing an md: reader")
        con = REAL_CONNECT(f"md:{self.database}?attach_mode=single&dbinstance_inactivity_ttl=0s")
        try:
            names = {
                r[0] for r in con.execute("SELECT database_name FROM duckdb_databases()").fetchall()
            }
            assert PROBE not in names, "the reader shares the run's DuckDB instance"
            con.execute(f"ATTACH IF NOT EXISTS 'md:{self.mirror}' AS \"{self.mirror}\"")
            return work(con)
        finally:
            con.close()

    def facts(self):
        return self.read(lambda con: con.execute(_facts_sql(self.mirror)).fetchone())

    def log(self):
        from test_apply2 import log_snapshot

        return self.read(lambda con: log_snapshot(con, self.database, "f2"))


def _probed_connect(real):
    """Remote: main()'s md: connections, each with PROBE attached (an in-memory
    database visible only within that DuckDB instance)."""

    def connect(address, *args, **kwargs):
        con = real(address, *args, **kwargs)
        con.execute(f"ATTACH IF NOT EXISTS ':memory:' AS {PROBE}")
        return con

    return connect


class _LocalMain:
    """Local twin only: main()'s md: connections become cursors on the store's own
    connection (its mirror is already attached), so the same files are acted on."""

    def __init__(self, store):
        self.store = store

    def connect(self, address, *args, **kwargs):
        return _Cursor(self.store.con.cursor())


class _Cursor:
    def __init__(self, con):
        self.con = con

    def execute(self, statement, parameters=None):
        if statement.startswith("ATTACH IF NOT EXISTS 'md:"):
            return None  # the store's database and mirror are already attached
        return self.con.execute(statement, parameters or [])

    def __getattr__(self, name):
        return getattr(self.con, name)


@pytest.mark.md_gate
@pytest.mark.parametrize("stores", ["local", "motherduck"], indirect=True)
def test_whole_unit_quarantine_through_main(stores, monkeypatch, capsys):
    from test_apply2 import poisoned_unit_env

    env, unit, blocked = poisoned_unit_env(stores)
    start, end, digest, members = unit
    database, mirror = env.config.database, env.config.mirror_database
    ops_absent = "cdc_ops_absent_" + uuid.uuid4().hex[:8]  # S3: never created or touched
    owner = qf.Owner("f2", database=database, mirror_database=mirror, ops_database=ops_absent)
    if stores[0].remote:
        monkeypatch.setattr(af.duckdb, "connect", _probed_connect(REAL_CONNECT))
    else:
        monkeypatch.setattr(af.duckdb, "connect", _LocalMain(stores[0]).connect)
    reader = _Reader(stores, database, mirror)
    environ = dict(
        CDC_ACTION="quarantine",
        CDC_TABLE=QUARANTINED,
        CDC_EXPECTED_EPOCH="1",
        CDC_CONFIRM=af.unit_literal(QUARANTINED, 1, start, end),
    )  # S3: no MOTHERDUCK_FLIGHT_* variables, so whois is skipped

    def run(fault=None, **env_):
        code = qf.main(owner, dict(environ, **env_), fault=fault)
        lines = capsys.readouterr().out.strip().splitlines()
        assert "@flight-run whois skipped" in "\n".join(lines)
        return code, json.loads(lines[-1])

    log_before = reader.log()
    code, dry = run(CDC_DRY_RUN="1")
    assert code == 0 and dry["status"] == "would", dry
    preview = dry["preview_id"]
    if stores[0].remote:
        # Positive control: main()'s EXACT md: string reopens its cached
        # instance, where the probe IS attached; so the readers' "absent" is not vacuous.
        con = REAL_CONNECT(f"md:{mirror}?attach_mode=single")
        try:
            names = {
                r[0] for r in con.execute("SELECT database_name FROM duckdb_databases()").fetchall()
            }
            assert PROBE in names, "s1_probe never reached main()'s instance"
        finally:
            con.close()
    after_dry = reader.facts()
    assert after_dry[0] == 0 and after_dry[1] is None and after_dry[2][0] == "needs_action"

    # Step 3: three refusals on a real run, each writing nothing in either database.
    for name, overrides in (
        (
            "legacy commit literal",
            dict(CDC_CONFIRM=f"QUARANTINE {QUARANTINED} {lsn_text(blocked)}"),
        ),
        ("near miss", dict(CDC_CONFIRM=af.unit_literal(QUARANTINED, 1, start, end + 1))),
        ("case change", dict(CDC_CONFIRM=af.unit_literal(QUARANTINED, 1, start, end).lower())),
        ("wrong epoch", dict(CDC_EXPECTED_EPOCH="2")),
    ):
        code, refused = run(CDC_PREVIEW_ID=preview, **overrides)
        assert code == 2 and refused["status"] == "refused", (name, refused)
        assert reader.facts() == after_dry, name
    assert reader.log() == log_before

    def fail_at(stage):
        def fault(at):
            if at == stage:
                raise RuntimeError(f"injected at {stage}")

        return fault

    # Pre-COMMIT: every write of the one mirror transaction rolls back together.
    with pytest.raises(RuntimeError, match="before_action_commit"):
        run(fail_at("before_action_commit"), CDC_PREVIEW_ID=preview)
    capsys.readouterr()
    assert reader.facts() == after_dry  # nothing landed; the preview is unspent

    # The same preview commits; the response is lost after COMMIT.
    with pytest.raises(RuntimeError, match="after_action_commit"):
        run(fail_at("after_action_commit"), CDC_PREVIEW_ID=preview)
    capsys.readouterr()
    landed = reader.facts()
    assert landed[0] == 1  # spent exactly once
    assert landed[1] == [[start, end, blocked]]
    assert landed[2] == ["live", str(start), None]  # quarantine never moves the checkpoint
    assert landed[3] == after_dry[3] + 1  # one runs audit row

    # The rerun with the same inputs reports already and writes nothing.
    code, again = run(CDC_PREVIEW_ID=preview)
    assert code == 0 and again["status"] == "already", again
    assert reader.facts() == landed

    # The change log was never written (S1: read on a fresh instance).
    assert reader.log() == log_before

    # The next apply passes the unit END without any of its rows.
    result = env.apply.apply_table(QUARANTINED, "after")
    assert result.status == "caught_up" and result.boundary == end, result
    rows = env.store.con.execute(f'SELECT id FROM "{mirror}".public.others ORDER BY id').fetchall()
    assert rows == [("1",), ("2",)]  # unit 2 (commits 3 and 4) skipped whole

    # S3: the nonexistent operations database was never created.
    if stores[0].remote:
        assert not reader.read(
            lambda con: con.execute(
                "SELECT count(*) FROM md_all_databases() WHERE alias=?", [ops_absent]
            ).fetchone()[0]
        )
    print(f"quarantine node: {reader.reads} fresh-instance reads")  # S1 evidence
