"""Local checks of the test harness itself: transient retry in admin_ddl, md_gate
selection, the slot-release wait and the MotherDuck store template. The retry and
selection tests replace duckdb.connect, so they need nothing. The slot test needs
CDC_TEST_PG_DSN, and the template test needs CDC_TEST_MD=1."""

import os
import subprocess
import sys
import uuid
from pathlib import Path

import conftest
import duckdb
import pytest


class FakeConnection:
    def __init__(self, outcomes, calls):
        self.outcomes, self.calls = outcomes, calls

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, statement):
        self.calls.append(statement)
        outcome = self.outcomes.pop(0)
        if outcome is not None:
            raise outcome


@pytest.fixture
def fake_md(monkeypatch):
    monkeypatch.delenv(
        "MD_SUITE_RESOURCE_DIR", raising=False
    )  # fake allocations are not owned resources
    outcomes, calls, sleeps = [], [], []
    monkeypatch.setattr(conftest.duckdb, "connect", lambda _: FakeConnection(outcomes, calls))
    monkeypatch.setattr(conftest.time, "sleep", sleeps.append)
    return outcomes, calls, sleeps


@pytest.mark.parametrize("marker", conftest.TRANSIENT_MD_ERRORS)
def test_admin_ddl_retries_each_transient_error(fake_md, marker):
    outcomes, calls, sleeps = fake_md
    outcomes += [duckdb.Error(f"Request failed: ... ({marker}) ..."), None]
    conftest.admin_ddl("CREATE DATABASE IF NOT EXISTS x")
    assert len(calls) == 2 and sleeps == [1]


def test_admin_ddl_retries_observed_unavailable_message(fake_md):
    outcomes, calls, _ = fake_md
    observed = (
        "Request failed: Could not connect to MotherDuck. Please try again later or contact "
        "support@motherduck.com for help. (UNAVAILABLE, RPC 'CREATE_DATABASE', request id x)"
    )
    outcomes += [duckdb.Error(observed), duckdb.Error(observed), None]
    conftest.admin_ddl("CREATE DATABASE IF NOT EXISTS x")
    assert len(calls) == 3


@pytest.mark.parametrize(
    "message",
    [
        'Binder Error: Failed to create database: database with name "x" already exists',
        "Request failed: quota (RESOURCE_EXHAUSTED)",
        "Invalid Input Error: authentication failed",
        "Could not connect to MotherDuck (UNAUTHENTICATED, RPC 'CREATE_DATABASE')",
        "Request failed: (UNAVAILABLE) ... PERMISSION_DENIED",
    ],
)
def test_admin_ddl_raises_non_transient_errors_at_once(fake_md, message):
    outcomes, calls, sleeps = fake_md
    outcomes += [duckdb.Error(message), None]
    with pytest.raises(duckdb.Error, match="."):
        conftest.admin_ddl("CREATE DATABASE IF NOT EXISTS x")
    assert len(calls) == 1 and sleeps == []


def test_admin_ddl_gives_up_after_attempts(fake_md):
    outcomes, calls, sleeps = fake_md
    outcomes += [duckdb.Error("DEADLINE_EXCEEDED")] * 3
    with pytest.raises(duckdb.Error, match="DEADLINE_EXCEEDED"):
        conftest.admin_ddl("DROP DATABASE IF EXISTS x", attempts=3)
    assert len(calls) == 3 and sleeps == [1, 2]


DROPS = ['DROP DATABASE IF EXISTS "cdc_p0_x_mirror"', 'DROP DATABASE IF EXISTS "cdc_p0_x"']
INTERRUPTS = [
    KeyboardInterrupt("SIGTERM"),
    RuntimeError("Query interrupted"),
    duckdb.InterruptException("INTERRUPT Error: Interrupted!"),
]


@pytest.mark.parametrize("interrupt", INTERRUPTS, ids=["keyboard", "query", "duckdb"])
def test_drop_md_databases_retries_an_interrupted_drop_and_drops_the_rest(fake_md, interrupt):
    """A soft stop that lands in teardown interrupts one DROP, once. That DROP is retried,
    the other databases are still dropped, and the interrupt is raised at the end."""
    outcomes, calls, _ = fake_md
    outcomes += [interrupt, None, None]
    with pytest.raises(type(interrupt)):
        conftest.drop_md_databases(["cdc_p0_x", "cdc_p0_x_mirror"])
    assert calls == [DROPS[0], DROPS[0], DROPS[1]]


def test_drop_md_databases_retries_an_interrupt_only_once(fake_md):
    outcomes, calls, _ = fake_md
    outcomes += [KeyboardInterrupt("SIGTERM"), RuntimeError("Query interrupted"), None]
    with pytest.raises(KeyboardInterrupt):
        conftest.drop_md_databases(["cdc_p0_x", "cdc_p0_x_mirror"])
    assert calls == [DROPS[0], DROPS[0], DROPS[1]]


def test_drop_md_databases_raises_other_exceptions_at_once(fake_md):
    outcomes, calls, _ = fake_md
    outcomes += [RuntimeError("something else")]
    with pytest.raises(RuntimeError, match="something else"):
        conftest.drop_md_databases(["cdc_p0_x", "cdc_p0_x_mirror"])
    assert calls == [DROPS[0]]


TESTS_DIR = Path(conftest.__file__).parent


@pytest.mark.postgres
def test_wait_slot_released_waits_for_the_walsender():
    dsn = os.getenv("CDC_TEST_PG_DSN")
    if not dsn:
        pytest.skip("set CDC_TEST_PG_DSN for real PostgreSQL probes")
    import psycopg2
    from psycopg2.extras import LogicalReplicationConnection

    slot = "cdc_p0_" + uuid.uuid4().hex[:12]
    admin = psycopg2.connect(dsn)
    admin.autocommit = True
    with admin.cursor() as cur:
        cur.execute("SELECT pg_create_logical_replication_slot(%s, 'test_decoding')", [slot])
    replication = None
    try:
        replication = psycopg2.connect(dsn, connection_factory=LogicalReplicationConnection)
        replication.cursor().start_replication(slot_name=slot, decode=True)
        with pytest.raises(AssertionError, match="still held"):
            conftest.wait_slot_released(admin, slot, timeout=0.5)
        replication.close()
        conftest.wait_slot_released(admin, slot)
        with admin.cursor() as cur:
            cur.execute("SELECT pg_drop_replication_slot(%s)", [slot])
        conftest.wait_slot_released(admin, slot)  # a missing slot counts as released
    finally:
        try:
            if replication is not None and not replication.closed:
                replication.close()
            conftest.wait_slot_released(admin, slot, timeout=10)
            with admin.cursor() as cur:
                cur.execute(
                    "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
                    "WHERE slot_name=%s",
                    [slot],
                )
        finally:
            admin.close()


def md_catalog_and_contents(con, database):
    """Everything about one MotherDuck database that a test could observe, minus its name."""
    tables = con.execute(
        "SELECT schema_name, table_name, sql FROM duckdb_tables() WHERE database_name=? "
        "ORDER BY ALL",
        [database],
    ).fetchall()
    columns = con.execute(
        "SELECT schema_name, table_name, column_index, column_name, data_type, column_default, "
        "is_nullable, numeric_precision, numeric_scale FROM duckdb_columns() "
        "WHERE database_name=? ORDER BY ALL",
        [database],
    ).fetchall()
    constraints = con.execute(
        # constraint_index is a catalog-wide counter, not a property of the database.
        "SELECT schema_name, table_name, constraint_type, constraint_text, "
        "constraint_column_names::VARCHAR FROM duckdb_constraints() WHERE database_name=? "
        "ORDER BY ALL",
        [database],
    ).fetchall()
    others = con.execute(
        "SELECT 'sequence', sequence_name FROM duckdb_sequences() WHERE database_name=? "
        "UNION ALL SELECT 'view', view_name FROM duckdb_views() WHERE database_name=? "
        "AND NOT internal UNION ALL SELECT 'index', index_name FROM duckdb_indexes() "
        "WHERE database_name=? ORDER BY ALL",
        [database] * 3,
    ).fetchall()
    contents = {
        (schema, table): con.execute(
            f'SELECT * FROM "{database}"."{schema}"."{table}" ORDER BY ALL'
        ).fetchall()
        for schema, table, _ in tables
    }
    return tables, columns, constraints, others, contents


@pytest.mark.motherduck
def test_md_template_clone_equals_a_freshly_initialized_store(md_template):
    """MotherDuck stores are clones of md_template instead of running Store.initialize()
    per test. A clone must be indistinguishable from a fresh initialization. md_template
    skips without CDC_TEST_MD."""
    name = "cdc_p0_" + uuid.uuid4().hex[:12]
    clone = "cdc_p0_" + uuid.uuid4().hex[:12]
    created = []
    try:
        fresh = conftest.create_initialized_md_pair(name, created)
        for db, source in zip((clone, clone + "_mirror"), md_template):
            created.append(db)
            conftest.admin_ddl(
                f"CREATE DATABASE IF NOT EXISTS {conftest.ident(db)} FROM {conftest.ident(source)}"
            )
        # Attach each database explicitly, as `stores` does: a workspace connection
        # (`md:`) can miss databases created moments before it, most often under load.
        with duckdb.connect(f"md:{fresh[0]}?attach_mode=single") as con:
            for db in (fresh[1], clone, clone + "_mirror"):
                con.execute(f"ATTACH IF NOT EXISTS 'md:{db}' AS {conftest.ident(db)}")
            for fresh_db, clone_db in zip(fresh, (clone, clone + "_mirror")):
                expected = md_catalog_and_contents(con, fresh_db)
                assert expected[0], f"no tables in {fresh_db}"
                assert md_catalog_and_contents(con, clone_db) == expected
    finally:
        conftest.drop_md_databases(created)


TEMPLATE_PROBE_CONFTEST = """
import conftest_under_test as under_test


def touched(*args, **kwargs):
    raise AssertionError("touched MotherDuck")


under_test.md_token = under_test.admin_ddl = under_test.create_initialized_md_pair = touched
from conftest_under_test import md_template, stores  # noqa: E402,F401
"""

TEMPLATE_PROBE = """
import pytest

def test_template(md_template):
    pass


@pytest.mark.parametrize("stores", ["local", "motherduck"], indirect=True)
def test_stores(stores):
    pass
"""


@pytest.fixture
def template_probe(pytester):
    pytester.makepyfile(conftest_under_test=(TESTS_DIR / "conftest.py").read_text())
    for helper in ("md_policy.py", "md_resources.py", "md_allowlist.json", "ci_shard.py"):
        (pytester.path / helper).write_text((TESTS_DIR / helper).read_text())
    pytester.makeconftest(TEMPLATE_PROBE_CONFTEST)
    pytester.makepyfile(test_probe=TEMPLATE_PROBE)
    pytester.syspathinsert()
    return pytester


def test_md_template_skips_without_md_flag_and_never_touches_motherduck(
    template_probe, monkeypatch
):
    monkeypatch.delenv("CDC_TEST_MD", raising=False)
    rec = template_probe.inline_run("-p", "no:cacheprovider")
    outcomes = {r.nodeid.split("::")[1]: r.outcome for r in rec.getreports() if r.when == "setup"}
    assert outcomes == {
        "test_template": "skipped",
        "test_stores[local]": "passed",
        "test_stores[motherduck]": "skipped",
    }
    # The probe is live: with the flag set, the same fixtures do reach MotherDuck.
    monkeypatch.setenv("CDC_TEST_MD", "1")
    rec = template_probe.inline_run("-p", "no:cacheprovider", "-k", "template")
    [error] = [r for r in rec.getreports() if r.when == "setup" and r.failed]
    assert "touched MotherDuck" in str(error.longrepr)


SIGTERM_PROBE_CONFTEST = """
import os
import time
from pathlib import Path

import pytest
from conftest_under_test import pytest_configure, pytest_unconfigure  # noqa: F401
from xdist.workermanage import NodeManager

# An interrupted xdist controller gives its workers EXIT_TIMEOUT (10 s) and then kills
# them; real MotherDuck teardown can take longer. Scaled down against a 3 s teardown.
NodeManager.EXIT_TIMEOUT = 1


@pytest.fixture(scope="session")
def resource():
    yield
    # Slow, like dropping MotherDuck databases: the xdist controller must outlive it.
    time.sleep(3)
    Path("torn-down-" + os.getenv("PYTEST_XDIST_WORKER", "master")).write_text("ok")
"""

SIGTERM_PROBE = """
import os
import time
from pathlib import Path

import pytest


@pytest.mark.parametrize("n", [1, 2, 3, 4])
def test_slow(resource, n):
    Path(f"ready-{os.getpid()}-{n}").touch()
    try:
        time.sleep(120)
    except KeyboardInterrupt:
        if n != 1:
            raise
        # As DuckDB does: it turns the interrupt into duckdb.InterruptException, so this
        # test fails and the session would go on to the next 120 s test.
        raise ValueError("Interrupted!") from None
"""


DROP_PROBE_CONFTEST = """
import time
from pathlib import Path

import pytest
import conftest_under_test as under_test
from conftest_under_test import pytest_configure, pytest_unconfigure  # noqa: F401


class Connection:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, statement):
        with open("drops", "a") as f:
            f.write(statement + "\\n")
        if not Path("dropping").exists():
            Path("dropping").touch()
            time.sleep(60)  # a slow MotherDuck DROP; the stop lands here


under_test.duckdb.connect = lambda _: Connection()


@pytest.fixture(scope="session")
def created():
    yield
    under_test.drop_md_databases(["cdc_p0_x", "cdc_p0_x_mirror"])
"""


def test_a_stop_that_lands_in_teardown_still_drops_every_database(pytester):
    """The stop arrives while session teardown is inside a (fake) DROP: that DROP is
    retried and the next one still runs, so the stop leaks nothing."""
    import signal
    import subprocess
    import sys
    import time

    pytester.makepyfile(conftest_under_test=(TESTS_DIR / "conftest.py").read_text())
    for helper in ("md_policy.py", "md_resources.py", "md_allowlist.json", "ci_shard.py"):
        (pytester.path / helper).write_text((TESTS_DIR / helper).read_text())
    pytester.makeconftest(DROP_PROBE_CONFTEST)
    pytester.makepyfile(test_probe="def test_quick(created):\n    pass\n")
    env = {k: v for k, v in os.environ.items() if not k.startswith("PYTEST_XDIST_")}
    env["PYTHONPATH"] = str(pytester.path)
    run = subprocess.Popen(
        [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider", "-p", "no:xdist"],
        cwd=pytester.path,
        env=env,
        start_new_session=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    try:
        deadline = time.monotonic() + 60
        while not (pytester.path / "dropping").exists():
            assert run.poll() is None, run.stdout.read().decode()
            assert time.monotonic() < deadline, "teardown never reached the DROP"
            time.sleep(0.1)
        os.killpg(run.pid, signal.SIGTERM)
        output = run.communicate(timeout=30)[0].decode()
    finally:
        if run.poll() is None:
            os.killpg(run.pid, signal.SIGKILL)
            run.wait()
    drops = (pytester.path / "drops").read_text().splitlines()
    assert drops == [DROPS[0], DROPS[0], DROPS[1]], output
    assert run.returncode >= 0, (run.returncode, output)


@pytest.mark.parametrize("workers", [["-n", "2"], ["-p", "no:xdist"]], ids=["xdist", "serial"])
def test_sigterm_to_the_run_still_tears_down_session_fixtures(pytester, workers):
    """md-suite stops a run by sending SIGTERM to its process group, and `uv run` forwards
    another to pytest. Every pytest process, xdist workers included, must still run session
    teardown, which is where the MotherDuck databases are dropped."""
    import signal
    import subprocess
    import sys
    import time

    pytester.makepyfile(conftest_under_test=(TESTS_DIR / "conftest.py").read_text())
    for helper in ("md_policy.py", "md_resources.py", "md_allowlist.json", "ci_shard.py"):
        (pytester.path / helper).write_text((TESTS_DIR / helper).read_text())
    pytester.makeconftest(SIGTERM_PROBE_CONFTEST)
    pytester.makepyfile(test_probe=SIGTERM_PROBE)
    # Not this run's xdist identity: under -n, PYTEST_XDIST_WORKER would name the probe.
    env = {k: v for k, v in os.environ.items() if not k.startswith("PYTEST_XDIST_")}
    env["PYTHONPATH"] = str(pytester.path)
    run = subprocess.Popen(
        [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider", *workers],
        cwd=pytester.path,
        env=env,
        start_new_session=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    try:
        expected = 2 if workers[0] == "-n" else 1
        deadline = time.monotonic() + 60
        while len(list(pytester.path.glob("ready-*"))) < expected:
            assert run.poll() is None, run.stdout.read().decode()
            assert time.monotonic() < deadline, "the probe tests never started"
            time.sleep(0.1)
        started = time.monotonic()
        os.killpg(run.pid, signal.SIGTERM)
        # md-suite runs pytest under `uv run`, which forwards a second SIGTERM to pytest.
        time.sleep(0.2)
        os.kill(run.pid, signal.SIGTERM)
        output = run.communicate(timeout=60)[0].decode()
    finally:
        if run.poll() is None:
            os.killpg(run.pid, signal.SIGKILL)
            run.wait()
    assert time.monotonic() - started < 30, output
    names = {"-n": {"torn-down-gw0", "torn-down-gw1"}, "-p": {"torn-down-master"}}
    assert {p.name for p in pytester.path.glob("torn-down-*")} == names[workers[0]], output
    # pytest's interrupted status: no process was killed by the signal itself (-15).
    assert run.returncode == pytest.ExitCode.INTERRUPTED, (run.returncode, output)


# The local-run token placeholder ---------------------------------------------------


@pytest.mark.skipif(os.getenv("CDC_TEST_MD") == "1", reason="local runs only")
def test_a_local_run_has_only_the_placeholder_token_and_children_inherit_it():
    assert os.environ["MOTHERDUCK_TOKEN"] == conftest.LOCAL_RUN_TOKEN
    assert os.environ["motherduck_token"] == conftest.LOCAL_RUN_TOKEN
    out = subprocess.run(
        [sys.executable, "-c", "import os; print(os.environ['MOTHERDUCK_TOKEN'])"],
        capture_output=True,
        text=True,
        check=True,
    )
    assert out.stdout.strip() == conftest.LOCAL_RUN_TOKEN


@pytest.mark.parametrize("md,kept", [("", False), ("0", False), ("1", True)])
def test_importing_conftest_replaces_a_shell_token_unless_md_runs_are_on(md, kept):
    """Set at import, so it precedes every fixture, plugin and child process. No
    inherited environment variable can switch it off."""
    shell = "a-real-looking-token-from-the-shell"
    env = {
        **{k: v for k, v in os.environ.items() if not k.startswith(("CDC_", "PYTEST_"))},
        "MOTHERDUCK_TOKEN": shell,
        "motherduck_token": shell,
        "CDC_TEST_TOKEN_MODE": "md",  # the first fix's variable: must have no effect
        "CDC_TEST_MD": md,
        "PYTHONPATH": str(Path(conftest.__file__).parent),
    }
    out = subprocess.run(
        [
            sys.executable,
            "-c",
            "import os, conftest; print(os.environ['MOTHERDUCK_TOKEN'], os.environ['motherduck_token'])",
        ],
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    expected = shell if kept else conftest.LOCAL_RUN_TOKEN
    assert out.stdout.split() == [expected, expected]


def load_conftest_copy(tmp_path, monkeypatch):
    """Import a copy of conftest.py in this process, as an in-process pytester run does
    (it re-executes the module-level token guard)."""
    import importlib.util

    copy = tmp_path / "conftest_copy_for_token_test.py"
    copy.write_text((TESTS_DIR / "conftest.py").read_text())
    spec = importlib.util.spec_from_file_location(copy.stem, copy)
    module = importlib.util.module_from_spec(spec)
    monkeypatch.syspath_prepend(str(tmp_path))
    spec.loader.exec_module(module)
    return module


def test_a_later_conftest_import_without_the_md_flag_keeps_an_md_runs_token(tmp_path, monkeypatch):
    """The first import in a process decides the token mode. A MotherDuck run's worker
    that loads a copy of conftest with CDC_TEST_MD unset (in-process pytester) keeps its
    real token; before the fix the copy replaced it with the placeholder."""
    real = "a-real-token-of-an-md-run"
    monkeypatch.setattr(sys, "_cdc_test_token_mode", "md", raising=False)
    monkeypatch.setenv("MOTHERDUCK_TOKEN", real)
    monkeypatch.setenv("motherduck_token", real)
    monkeypatch.delenv("CDC_TEST_MD", raising=False)
    load_conftest_copy(tmp_path, monkeypatch)
    assert (os.environ["MOTHERDUCK_TOKEN"], os.environ["motherduck_token"]) == (real, real)


def test_after_the_pytester_md_flag_case_this_worker_still_reaches_motherduck(
    template_probe, monkeypatch
):
    """The in-process pytester case restores this worker's token for the next test.
    Real connections are exercised only by allowlisted cases, not this unit check.
    """
    md = os.getenv("CDC_TEST_MD") == "1"
    if md:
        conftest.md_token()
    before = os.environ["MOTHERDUCK_TOKEN"]
    test_md_template_skips_without_md_flag_and_never_touches_motherduck(template_probe, monkeypatch)
    monkeypatch.undo()  # back to this run's own environment, as the next test sees it
    assert os.environ["MOTHERDUCK_TOKEN"] == before
    if not md:
        assert before == conftest.LOCAL_RUN_TOKEN
        return
