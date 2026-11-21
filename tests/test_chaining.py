"""Flight chaining: ingest -> apply and backfill self-re-trigger, with fake triggers."""

import json
import os
import py_compile
import re
import subprocess
import sys
import uuid
from pathlib import Path

import pytest
from test_backfill import populated  # noqa: F401  (fixture)

from cdc_flight import chain as chain_mod
from cdc_flight.backfill import BackfillResult
from cdc_flight.chain import (
    CONTINUE,
    FLIGHT_CONFIG,
    ApplyChain,
    BackfillChain,
    ConfigError,
    after_backfill,
    after_ingest,
    blank_to_unset,
    run_flight,
)
from cdc_flight.config import ApplyConfig, BackfillConfig, IngestConfig
from cdc_flight.ingest import RunResult

ROOT = Path(__file__).resolve().parents[1]
APPLY_ID = str(uuid.uuid4())
SELF_ID = str(uuid.uuid4())


class Recorder:
    def __init__(self, error=None):
        self.calls, self.error = [], error

    def __call__(self, target, overrides):
        if self.error:
            raise self.error
        self.calls.append((target, dict(overrides)))
        return len(self.calls)


def ingest_result(status, boundary):
    return RunResult("run", status, boundary, 1, 1, False)


# Ingest -> apply ------------------------------------------------------------


@pytest.mark.parametrize("status", ["drained", "capped", "needs_action"])
def test_ingest_that_advanced_triggers_apply(status):
    trigger = Recorder()
    outcome = after_ingest(ingest_result(status, 200), 100, ApplyChain(APPLY_ID), trigger)
    assert outcome == "triggered"
    assert trigger.calls == [(APPLY_ID, {})]


@pytest.mark.parametrize(
    "status,boundary",
    [("drained", 100), ("capped", 100), ("needs_action", 100), ("busy", 100), ("busy", 200)],
)
def test_ingest_without_advance_or_busy_does_not_trigger(status, boundary):
    trigger = Recorder()
    outcome = after_ingest(ingest_result(status, boundary), 100, ApplyChain(APPLY_ID), trigger)
    assert outcome == "skipped" and trigger.calls == []


@pytest.mark.parametrize("chain", [ApplyChain(""), ApplyChain(APPLY_ID, enabled=False)])
def test_ingest_chain_disabled(chain):
    trigger = Recorder()
    assert after_ingest(ingest_result("drained", 200), 100, chain, trigger) == "disabled"
    assert trigger.calls == []


def test_failed_trigger_never_fails_ingest_and_hides_message(capsys):
    trigger = Recorder(error=RuntimeError("host=secret.example password=hunter2"))
    outcome = after_ingest(ingest_result("drained", 200), 100, ApplyChain(APPLY_ID), trigger)
    assert outcome == "trigger_failed"
    err = capsys.readouterr().err
    assert "RuntimeError" in err and "hunter2" not in err and "secret.example" not in err


def test_failed_ingest_run_never_reaches_the_trigger(capsys):
    """An exception from run_once propagates before after_ingest is called: the CLI
    (explain.ingest) calls after_ingest only with a returned RunResult."""
    from cdc_flight import explain
    from cdc_flight.flightlog import Run

    def run_once(config, log, *, info, fault):
        fault("after_lease")
        raise RuntimeError("source went away")

    def after_ingest(*args):
        raise AssertionError("after_ingest must not run")

    with pytest.raises(RuntimeError, match="went away"):
        explain.ingest(Run(), None, None, None, run_once, None, None, after_ingest, 0)
    # The node that was running when it failed is closed as failed (type only).
    assert "@flight-run slot failed error=RuntimeError" in capsys.readouterr().out


# Backfill self-re-trigger ---------------------------------------------------


def backfill_result(status, chunks=1, rows=10, boundary=0, generation="g1"):
    return BackfillResult("s.t", generation, status, chunks, rows, boundary)


def chain(**kw):
    return BackfillChain("s.t", self_flight_id=SELF_ID, delay_seconds=7, **kw)


@pytest.mark.parametrize("status", CONTINUE)
def test_each_continue_status_retriggers_resume_only(status):
    trigger, slept = Recorder(), []
    outcome, counts = after_backfill(backfill_result(status), chain(), trigger, sleep=slept.append)
    assert (outcome, counts) == ("triggered", dict(stalled_runs=0, waiting_runs=0))
    [(target, overrides)] = trigger.calls
    assert target == SELF_ID
    assert overrides == {
        "CDC_BACKFILL_TABLE": "s.t",
        "CDC_BACKFILL_START": "0",
        "CDC_BACKFILL_ABANDON": "",
        "CDC_BACKFILL_STALLED_RUNS": "0",
        "CDC_BACKFILL_WAITING_RUNS": "0",
        "CDC_BACKFILL_LAST_PROGRESS": f"g1:{status}:1:10:0:0",
    }
    # Waiting statuses back off so the chain doesn't outrun the ingest cron.
    assert slept == ([7] if status in chain_mod.WAITING else [])


@pytest.mark.parametrize("status", ["published", "failed", "idle", "busy"])
def test_terminal_statuses_stop(status):
    trigger = Recorder()
    assert after_backfill(backfill_result(status), chain(), trigger)[0] == "stopped"
    assert trigger.calls == []


def test_no_progress_counts_up_and_progress_resets():
    result = backfill_result("waiting_for_ingest", boundary=5)
    same = chain_mod.progress_signature(result)
    trigger = Recorder()
    sleep = [].append
    assert after_backfill(result, chain(last_progress=same, stalled_runs=3), trigger, sleep=sleep)[
        1
    ] == dict(stalled_runs=4, waiting_runs=1)
    moved = backfill_result("snapshotting", chunks=2)
    assert after_backfill(moved, chain(last_progress=same, stalled_runs=3), trigger, sleep=sleep)[
        1
    ] == dict(stalled_runs=0, waiting_runs=0)
    assert trigger.calls[0][1]["CDC_BACKFILL_STALLED_RUNS"] == "4"


def test_catching_up_counts_as_progress():
    result = backfill_result("catching_up", boundary=9)
    sig = chain_mod.progress_signature(result)
    state = chain(last_progress=sig, stalled_runs=9, waiting_runs=59)
    outcome, counts = after_backfill(result, state, Recorder())
    assert (outcome, counts) == ("triggered", dict(stalled_runs=0, waiting_runs=0))


def test_no_progress_bound_stops_visibly():
    result = backfill_result("busy_publish")
    sig = chain_mod.progress_signature(result)
    trigger = Recorder()
    bounded = chain(last_progress=sig, stalled_runs=4, max_stalled_runs=5)
    assert after_backfill(result, bounded, trigger) == (
        "stalled",
        dict(stalled_runs=5, waiting_runs=1),
    )
    assert trigger.calls == []


def test_backfill_trigger_error_and_missing_flight_id():
    result = backfill_result("snapshotting")
    assert after_backfill(result, chain(), Recorder(error=OSError("x")))[0] == "trigger_failed"
    no_id = BackfillChain("s.t", self_flight_id="")
    assert after_backfill(result, no_id, Recorder())[0] == "no_flight_id"
    off = BackfillChain("s.t", self_flight_id=SELF_ID, enabled=False)
    assert after_backfill(result, off, Recorder())[0] == "disabled"


def test_chain_loop_converges_without_a_second_generation():
    """Drive the loop through the config a re-triggered run would receive."""
    script = iter(
        [
            backfill_result("snapshotting", chunks=1),
            backfill_result("snapshotting", chunks=2),
            backfill_result("waiting_for_marker", chunks=2),
            backfill_result("waiting_for_ingest", chunks=2, boundary=50),
            backfill_result("catching_up", chunks=2, boundary=50),
            backfill_result("busy_publish", chunks=2, boundary=50),
            backfill_result("published", chunks=2, boundary=50),
        ]
    )
    environ = {"MOTHERDUCK_FLIGHT_ID": SELF_ID}
    starts = []
    for _ in range(20):
        current = BackfillChain.from_env("s.t", environ)
        starts.append(current.start)
        trigger = Recorder()
        outcome, _ = after_backfill(next(script), current, trigger, sleep=lambda s: None)
        if outcome != "triggered":
            break
        environ = {"MOTHERDUCK_FLIGHT_ID": SELF_ID, **trigger.calls[0][1]}
    assert outcome == "stopped"
    assert starts == [True] + [False] * 6


# Config ---------------------------------------------------------------------


def test_blank_config_means_unset(monkeypatch):
    environ = {"CDC_DATABASE": "", "CDC_SOURCE_ID": "s", "OTHER": "", "CDC_EPOCH": "2"}
    blank_to_unset(environ)
    assert environ == {"CDC_SOURCE_ID": "s", "OTHER": "", "CDC_EPOCH": "2"}
    monkeypatch.setattr(os, "environ", {"CDC_SOURCE_ID": "s", "CDC_TABLES": '["a.b"]'})
    os.environ.update(CDC_DATABASE="", CDC_MIRROR_DATABASE="", CDC_EPOCH="")
    blank_to_unset()
    config = ApplyConfig.from_env()
    assert (config.database, config.mirror_database, config.epoch) == (
        "cdc_internal",
        "s_mirror",
        None,  # unset: follow the registered epoch
    )


@pytest.mark.parametrize(
    "key,value",
    [
        ("CDC_BACKFILL_START", "yes"),
        ("CDC_BACKFILL_RETRIGGER", "true"),
        ("CDC_BACKFILL_MAX_STALLED_RUNS", "0"),
        ("CDC_BACKFILL_MAX_STALLED_RUNS", "101"),
        ("CDC_BACKFILL_MAX_STALLED_RUNS", "-1"),
        ("CDC_BACKFILL_RETRIGGER_DELAY_SECONDS", "1e3"),
        ("CDC_BACKFILL_STALLED_RUNS", "1; DROP"),
        ("CDC_BACKFILL_LAST_PROGRESS", "g1'; DROP TABLE x"),
        ("CDC_BACKFILL_LAST_PROGRESS", "a" * 257),
        ("MOTHERDUCK_FLIGHT_ID", "not-a-uuid"),
    ],
)
def test_bad_backfill_config_is_rejected_by_key_name(key, value):
    with pytest.raises(ConfigError, match=key) as info:
        BackfillChain.from_env("s.t", {key: value})
    assert value not in str(info.value) or value == "0"


def test_bad_apply_flight_id_and_flag():
    with pytest.raises(ConfigError, match="CDC_APPLY_FLIGHT_ID"):
        ApplyChain.from_env({"CDC_APPLY_FLIGHT_ID": "x"})
    with pytest.raises(ConfigError, match="CDC_CHAIN_APPLY"):
        ApplyChain.from_env({"CDC_CHAIN_APPLY": "on"})
    assert ApplyChain.from_env({}) == ApplyChain("", True)


def test_trigger_target_is_only_the_runtime_flight_id():
    """A run override must not redirect the owner-token trigger to another Flight."""
    other = str(uuid.uuid4())
    environ = {"MOTHERDUCK_FLIGHT_ID": SELF_ID, "CDC_BACKFILL_FLIGHT_ID": other}
    current = BackfillChain.from_env("s.t", environ)
    assert current.self_flight_id == SELF_ID
    assert "CDC_BACKFILL_FLIGHT_ID" not in FLIGHT_CONFIG["backfill"]
    assert "MOTHERDUCK_FLIGHT_ID" not in {k for v in FLIGHT_CONFIG.values() for k in v}
    trigger = Recorder()
    after_backfill(backfill_result("snapshotting"), current, trigger)
    assert trigger.calls[0][0] == SELF_ID


def test_ingest_progress_is_not_a_stall():
    """Waiting on an ingest that is draining a backlog: the backfill signature is
    unchanged, but the ingest checkpoint moved, so the stall count resets."""
    result = backfill_result("waiting_for_ingest", boundary=0)
    last = chain_mod.progress_signature(result, ingest_lsn=100)
    trigger = Recorder()
    state = chain(last_progress=last, stalled_runs=9, max_stalled_runs=10)
    assert after_backfill(result, state, trigger, ingest_lsn=150, sleep=lambda s: None) == (
        "triggered",
        dict(stalled_runs=0, waiting_runs=1),
    )
    assert trigger.calls[0][1]["CDC_BACKFILL_LAST_PROGRESS"].endswith(":150")
    # Neither moved: the bound stops the chain.
    assert after_backfill(result, state, Recorder(), ingest_lsn=100) == (
        "stalled",
        dict(stalled_runs=10, waiting_runs=1),
    )


@pytest.mark.parametrize("status", chain_mod.WAITING)
def test_waiting_bound_stops_when_only_ingest_moves(status):
    """Ingest healthy, generation stuck: the stall count keeps resetting, so the
    coarser waiting bound, which only generation progress resets, must end it."""
    result = backfill_result(status, boundary=5)
    environ = {"MOTHERDUCK_FLIGHT_ID": SELF_ID, "CDC_BACKFILL_MAX_WAITING_RUNS": "4"}
    outcomes = []
    for lsn in range(100, 110):
        current = BackfillChain.from_env("s.t", environ)
        trigger = Recorder()
        outcome, counts = after_backfill(
            result, current, trigger, ingest_lsn=lsn, sleep=lambda s: None
        )
        outcomes.append(outcome)
        if outcome != "triggered":
            break
        environ = {"MOTHERDUCK_FLIGHT_ID": SELF_ID, **trigger.calls[0][1]}
    # Run 1 is progress (no previous signature); runs 2..4 wait; run 5 hits 4.
    assert outcomes == ["triggered"] * 4 + ["waiting"]
    assert counts == dict(stalled_runs=0, waiting_runs=4)


def test_generation_progress_resets_the_waiting_bound():
    before = backfill_result("waiting_for_marker", boundary=0)
    last = chain_mod.progress_signature(before, ingest_lsn=1)
    after = backfill_result("waiting_for_ingest", boundary=0)
    state = chain(last_progress=last, waiting_runs=50, stalled_runs=3)
    assert after_backfill(after, state, Recorder(), ingest_lsn=1, sleep=lambda s: None) == (
        "triggered",
        dict(stalled_runs=0, waiting_runs=0),
    )


def test_operator_overrides_carry_into_every_rerun():
    environ = {
        "MOTHERDUCK_FLIGHT_ID": SELF_ID,
        "CDC_BACKFILL_CHUNK_ROWS": "1000",
        "CDC_RUNTIME_SECONDS": "300",
        "CDC_TABLES": '["s.t"]',
        "CDC_BACKFILL_ABANDON": "1",
        "CDC_PG_DSN": "secret-dsn",
        "UNRELATED": "x",
    }
    trigger = Recorder()
    after_backfill(backfill_result("snapshotting"), BackfillChain.from_env("s.t", environ), trigger)
    overrides = trigger.calls[0][1]
    assert overrides["CDC_BACKFILL_CHUNK_ROWS"] == "1000"
    assert overrides["CDC_RUNTIME_SECONDS"] == "300"
    assert overrides["CDC_TABLES"] == '["s.t"]'
    # Chain state is set by the loop; secrets and undeclared keys never travel.
    assert overrides["CDC_BACKFILL_ABANDON"] == "" and overrides["CDC_BACKFILL_START"] == "0"
    assert "CDC_PG_DSN" not in overrides and "UNRELATED" not in overrides
    assert set(overrides) <= set(FLIGHT_CONFIG["backfill"])


@pytest.mark.parametrize(
    "tables,message",
    [
        # CDC_TABLES is optional (unset: the registry); an explicit [] never is.
        ("[]", "non-empty JSON list"),
        ('"public.a"', "JSON list"),
        ("[1]", "JSON list"),
        ("{", "JSON list"),
    ],
)
def test_required_keys_and_table_shape(tmp_path, tables, message):
    env = dict(os.environ, MOTHERDUCK_TOKEN="unused", CDC_SOURCE_ID="s", CDC_TABLES=tables)
    out = subprocess.run(
        [sys.executable, "-m", "cdc_flight.cli", "apply"],
        env=env,
        capture_output=True,
        text=True,
        cwd=tmp_path,
    )
    assert out.returncode == 1 and message in out.stderr


def test_unknown_backfill_table_is_rejected(monkeypatch, tmp_path):
    env = dict(
        os.environ,
        MOTHERDUCK_TOKEN="unused",
        CDC_SOURCE_ID="s",
        CDC_TABLES='["public.a"]',
        CDC_PG_DSN="unused",
        CDC_BACKFILL_TABLE="public.b",
    )
    out = subprocess.run(
        [sys.executable, "-m", "cdc_flight.cli", "backfill"],
        env=env,
        capture_output=True,
        text=True,
        cwd=tmp_path,
    )
    assert out.returncode == 1
    assert "backfill table must be one of CDC_TABLES" in out.stderr


@pytest.mark.parametrize(
    "dsn_env", ["MOTHERDUCK_TOKEN", "HOME", "motherduck_DSN", "MOTHERDUCK_X_DSN", "a-b_DSN"]
)
@pytest.mark.parametrize("command", ["preflight", "ingest", "backfill"])
def test_dsn_env_cannot_point_at_other_variables(tmp_path, command, dsn_env):
    token = "dummy-not-a-real-token-" + uuid.uuid4().hex
    env = dict(
        os.environ,
        MOTHERDUCK_TOKEN=token,
        motherduck_DSN=token,
        MOTHERDUCK_X_DSN=token,
        CDC_SOURCE_ID="s",
        CDC_TABLES='["public.a"]',
        CDC_BACKFILL_TABLE="public.a",
        CDC_DSN_ENV=dsn_env,
    )
    out = subprocess.run(
        [sys.executable, "-m", "cdc_flight.cli", command],
        env=env,
        capture_output=True,
        text=True,
        cwd=tmp_path,
        timeout=60,
    )
    assert out.returncode == 1
    assert "CDC_DSN_ENV must name a variable ending in _DSN" in out.stderr
    assert token not in out.stdout + out.stderr


def test_malformed_dsn_secret_is_not_echoed(tmp_path):
    secret = "not-a-dsn-" + uuid.uuid4().hex
    env = dict(
        os.environ,
        MOTHERDUCK_TOKEN="unused",
        CDC_SOURCE_ID="s",
        CDC_TABLES='["public.a"]',
        cdc_pg_DSN=secret,
        CDC_DSN_ENV="cdc_pg_DSN",
    )
    out = subprocess.run(
        [sys.executable, "-m", "cdc_flight.cli", "ingest"],
        env=env,
        capture_output=True,
        text=True,
        cwd=tmp_path,
        timeout=60,
    )
    assert out.returncode == 1 and "invalid PostgreSQL DSN" in out.stderr
    assert secret not in out.stdout + out.stderr


def test_bad_chain_config_exits_before_connecting(tmp_path):
    env = dict(
        os.environ,
        MOTHERDUCK_TOKEN="unused",
        CDC_SOURCE_ID="s",
        CDC_TABLES='["public.a"]',
        CDC_PG_DSN="unused",
        CDC_BACKFILL_TABLE="public.a",
        CDC_BACKFILL_START="maybe",
    )
    out = subprocess.run(
        [sys.executable, "-m", "cdc_flight.cli", "backfill"],
        env=env,
        capture_output=True,
        text=True,
        cwd=tmp_path,
    )
    assert out.returncode == 1 and "CDC_BACKFILL_START must be 0 or 1" in out.stderr


# Trigger SQL ----------------------------------------------------------------


class FakeCon:
    def __init__(self):
        self.sql = self.params = None

    def execute(self, statement, params):
        self.sql, self.params = statement, params
        return self

    def fetchone(self):
        return (uuid.UUID(APPLY_ID), 7)


def test_run_flight_binds_everything_as_parameters():
    con = FakeCon()
    hostile = "x'); DROP TABLE t; --"
    assert run_flight(con, SELF_ID.upper(), {"CDC_BACKFILL_TABLE": hostile}) == (APPLY_ID, 7)
    assert "MD_RUN_FLIGHT" in con.sql and hostile not in con.sql and SELF_ID not in con.sql
    assert con.params == [SELF_ID, ["CDC_BACKFILL_TABLE"], [hostile]]
    with pytest.raises(ValueError):
        run_flight(con, "not-a-uuid")
    with pytest.raises(TypeError):
        run_flight(con, SELF_ID, {"K": 1})


# Bundles and declared config ------------------------------------------------


class Reads(dict):
    """Records every environment key a from_env reads."""

    def __init__(self, base):
        super().__init__(base)
        self.read = set()

    def get(self, key, default=None):
        self.read.add(key)
        return super().get(key, default)

    def __getitem__(self, key):
        self.read.add(key)
        return super().__getitem__(key)


@pytest.mark.parametrize(
    "command,factory",
    [
        ("ingest", IngestConfig.from_env),
        ("preflight", IngestConfig.from_env),
        ("apply", ApplyConfig.from_env),
        ("backfill", BackfillConfig.from_env),
        ("ingest", lambda: ApplyChain.from_env(os.environ)),
        ("backfill", lambda: BackfillChain.from_env("a.b", os.environ)),
    ],
)
def test_declared_config_covers_every_key_read(monkeypatch, command, factory):
    reads = Reads({"CDC_SOURCE_ID": "s", "CDC_TABLES": '["a.b"]', "CDC_PG_DSN": "x"})
    monkeypatch.setattr(os, "environ", reads)
    factory()
    # The secret's own variable and the runtime's reserved ids are not config.
    needed = {k for k in reads.read if k not in ("CDC_PG_DSN", "MOTHERDUCK_FLIGHT_ID")}
    assert needed <= set(FLIGHT_CONFIG[command])


def test_cli_reads_only_declared_keys():
    source = (ROOT / "src/cdc_flight/cli.py").read_text()
    for key in set(re.findall(r'"(CDC_[A-Z_]+)"', source)):
        assert any(key in keys for keys in FLIGHT_CONFIG.values()), key


@pytest.mark.parametrize("command", ["preflight", "ingest", "apply", "backfill"])
def test_stubs_build_compile_and_declare_the_config(tmp_path, command):
    from flight_stub import SHA, VERSION, build_flight

    output = tmp_path / f"{command}_flight.py"
    pg_ca = "rds-us-east-1" if command in build_flight.PG_COMMANDS else None
    build_flight.write(command, output, SHA, VERSION, pg_ca=pg_ca)
    py_compile.compile(str(output), doraise=True)
    assert f"COMMAND = {command!r}" in build_flight.literals(output.read_text())
    assert "from cdc_flight.cli import main" in output.read_text()
    template = json.loads(output.with_suffix(".config.json").read_text())
    assert list(template) == list(FLIGHT_CONFIG[command])
    assert set(template.values()) == {""}


# End to end: real ingest, backfill and apply on PG16, chained by fake triggers ------


@pytest.mark.postgres
def test_chained_backfill_then_ingest_to_apply(stores, populated):  # noqa: F811
    from test_phase2 import assert_mirror_equals_source

    from cdc_flight.apply_run import ApplyRun
    from cdc_flight.ingest import run_once

    source, _, ingest, log, backfill = populated
    big = f"{source.schema}.big"
    source.execute("UPDATE big SET note='before' WHERE id % 10 = 0")

    # Backfill: the operator's first run, then only what each run's re-trigger passes.
    environ = {"MOTHERDUCK_FLIGHT_ID": SELF_ID, "CDC_BACKFILL_MAX_STALLED_RUNS": "3"}
    starts, generations, statuses = [], set(), []
    for step in range(200):
        current = BackfillChain.from_env(big, environ)
        starts.append(current.start)
        result = backfill.run_table(big, max_chunks=2, start=current.start)
        statuses.append(result.status)
        generations.add(result.generation)
        if step % 3 == 0:
            source.execute("UPDATE big SET note=%s WHERE id=%s", [f"s{step}", step % 300 + 1])
        if result.status in ("waiting_for_marker", "waiting_for_ingest", "catching_up"):
            assert run_once(ingest, log).status == "drained"  # the ingest cron
        trigger = Recorder()
        outcome, _ = after_backfill(
            result,
            current,
            trigger,
            ingest_lsn=backfill.source_position()[1],
            sleep=lambda s: None,
        )
        if outcome != "triggered":
            break
        environ = {"MOTHERDUCK_FLIGHT_ID": SELF_ID, **trigger.calls[0][1]}
    assert (outcome, result.status) == ("stopped", "published"), statuses
    assert result.chunks > 5  # several chunks even at big_rows on MotherDuck
    assert starts[0] and not any(starts[1:])
    assert len(generations) == 1 and "snapshotting" in statuses

    # A resume-only rerun after publication is idle and starts nothing.
    assert backfill.run_table(big, start=False).status == "idle"

    # Ingest -> apply: chained only after an ingest run that advanced the checkpoint.
    apply = ApplyRun(stores[0].con, backfill.config)
    applied = []

    def trigger_apply(target, overrides):
        assert (target, overrides) == (APPLY_ID, {})
        applied.append(apply.run())
        return len(applied)

    source.execute("INSERT INTO big VALUES (100001, 'new', 'chained')")
    source.execute("DELETE FROM big WHERE id = 7")
    before = log.checkpoint()
    result = run_once(ingest, log)
    assert result.boundary > before
    assert after_ingest(result, before, ApplyChain(APPLY_ID), trigger_apply) == "triggered"
    [table_result] = [r for r in applied[0] if r.table == big]
    assert table_result.status == "caught_up" and table_result.events >= 2, table_result
    assert_mirror_equals_source(apply, source, big)
