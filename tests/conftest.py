import contextlib
import os
import signal
import sys
import threading
import time
import uuid
from pathlib import Path

import ci_shard
import duckdb
import pytest
from dotenv import load_dotenv

from cdc_flight.store import Store, ident

pytest_plugins = ["pytester"]
PREVIOUS_SIGTERM = pytest.StashKey()


def server_version(dsn):
    import psycopg2

    con = psycopg2.connect(dsn)
    try:
        with con.cursor() as cur:
            cur.execute("SHOW server_version_num")
            return int(cur.fetchone()[0])
    finally:
        con.close()


SIGTERM_REPEAT_GRACE = 2.0  # seconds


class SoftStop:
    """Turn the first SIGTERM into a stop that still runs session teardown, which drops the
    MotherDuck databases and Postgres objects that fixtures created. By default SIGTERM
    kills the process without any teardown.

    A worker, or a run without xdist, raises KeyboardInterrupt as Ctrl-C would. An xdist
    controller keeps running: its workers' interrupts stop the session, and if it exited
    first, each worker's channel would close and execnet would end the worker mid-teardown.

    One stop can deliver SIGTERM twice: md-suite signals the process group, and `uv run`
    also forwards its own SIGTERM to pytest. Repeats within SIGTERM_REPEAT_GRACE are that
    same stop. A later SIGTERM kills the process at once.

    The interrupt alone isn't enough: DuckDB checks for signals during a query and turns
    the KeyboardInterrupt into duckdb.InterruptException, an ordinary error that fails one
    test while the session goes on. So the stop also sets session.shouldstop, which ends
    the run after the current test either way."""

    def __init__(self, controller):
        self.controller = controller
        self.first = None
        self.session = None

    def pytest_sessionstart(self, session):
        self.session = session

    def __call__(self, signum, frame):
        now = time.monotonic()
        if self.first is None:
            self.first = now
            if self.session is not None:
                self.session.shouldstop = "SIGTERM"
            if not self.controller:
                raise KeyboardInterrupt("SIGTERM")
        elif now - self.first > SIGTERM_REPEAT_GRACE:
            signal.signal(signal.SIGTERM, signal.SIG_DFL)
            os.kill(os.getpid(), signal.SIGTERM)


def pytest_configure(config):
    configure(config)
    ci_shard.configure(config)
    if threading.current_thread() is not threading.main_thread():
        return
    controller = not hasattr(config, "workerinput") and getattr(config.option, "dist", "no") != "no"
    soft_stop = SoftStop(controller)
    config.pluginmanager.register(soft_stop, "cdc-soft-stop")
    config.stash[PREVIOUS_SIGTERM] = signal.signal(signal.SIGTERM, soft_stop)


def pytest_unconfigure(config):
    unconfigure(config)
    if PREVIOUS_SIGTERM in config.stash:
        signal.signal(signal.SIGTERM, config.stash[PREVIOUS_SIGTERM])


def pytest_sessionstart(session):
    """With CDC_TEST_PG_MAJOR set (CI), prove the DSN reaches that server version, so a
    wrong DSN cannot turn every Postgres test into a skip."""
    timing_event("preflight_start", session.config)
    if session.config.getoption("md_suite", default=False):
        md_token()
    expected = os.getenv("CDC_TEST_PG_MAJOR")
    if not expected:
        timing_event("preflight_done", session.config)
        return
    dsn = os.getenv("CDC_TEST_PG_DSN")
    if not dsn:
        raise pytest.UsageError("CDC_TEST_PG_MAJOR requires CDC_TEST_PG_DSN")
    actual = server_version(dsn) // 10000
    if actual != int(expected):
        raise pytest.UsageError(f"CDC_TEST_PG_DSN is PostgreSQL {actual}, expected {expected}")
    timing_event("preflight_done", session.config)


@pytest.fixture
def pg_dsn():
    dsn = os.getenv("CDC_TEST_PG_DSN")
    if not dsn:
        pytest.skip("set CDC_TEST_PG_DSN for real PostgreSQL probes")
    return dsn


@pytest.fixture
def pg_version(pg_dsn):
    return server_version(pg_dsn)


@contextlib.contextmanager
def private_database(pg_dsn, prefix):
    """A fresh database of its own on the test server (yields its DSN), dropped after.

    Teardown: DROP DATABASE refuses while any slot exists in it, and FORCE doesn't
    remove slots. A walsender can still show its slot active for a moment after being
    terminated, so terminate until none is active, then drop every slot, then the
    database, and check nothing leaked."""
    import psycopg2
    from psycopg2.extensions import make_dsn

    name = f"{prefix}_{uuid.uuid4().hex[:12]}"
    admin = psycopg2.connect(pg_dsn)
    admin.autocommit = True
    try:
        with admin.cursor() as cur:
            cur.execute(f"CREATE DATABASE {name} TEMPLATE template1")
        try:
            yield make_dsn(pg_dsn, dbname=name)
        finally:
            with admin.cursor() as cur:
                for _ in range(50):
                    cur.execute(
                        "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots "
                        "WHERE database=%s AND active",
                        [name],
                    )
                    if not cur.fetchall():
                        break
                    time.sleep(0.1)
                cur.execute(
                    "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
                    "WHERE database=%s AND NOT active",
                    [name],
                )
                cur.execute(f"DROP DATABASE IF EXISTS {name} WITH (FORCE)")
                cur.execute(
                    "SELECT (SELECT count(*) FROM pg_database WHERE datname=%s)"
                    " + (SELECT count(*) FROM pg_replication_slots WHERE database=%s)",
                    [name, name],
                )
                assert cur.fetchone()[0] == 0, f"private database {name} leaked"
    finally:
        admin.close()


@pytest.fixture
def private_pg_database(pg_dsn, monkeypatch):
    """Point CDC_TEST_PG_DSN at a fresh database of its own, for the rest of this test.

    Logical decoding reads only its slot's database, so no other test's transactions
    reach this test's slot (on PostgreSQL 13/14 they otherwise arrive as empty members,
    at any point in the stream). List it before fixtures that read the DSN, such as
    pg_phase1: pytest sets fixtures up in argument order and tears them down in reverse,
    so the slot is dropped before the database."""
    with private_database(pg_dsn, "cdc_p0_priv") as dsn:
        monkeypatch.setenv("CDC_TEST_PG_DSN", dsn)
        yield dsn


@pytest.fixture
def drain_only_units(monkeypatch):
    """Logs bootstrapped in this test close a unit only when an ingest run drains.

    By default a unit also closes at 100 members or once open 250 ms (UnitConfig). On
    PostgreSQL 13/14 pgoutput sends other sessions' transactions as empty members, so
    under parallel tests on a slow runner those limits fall at arbitrary points. A test
    that asserts where units start or end pins them out of reach."""
    from cdc_flight import bootstrap
    from cdc_flight.format2 import UnitConfig

    monkeypatch.setattr(
        bootstrap,
        "UnitConfig",
        lambda **kw: UnitConfig(**kw, max_commits=1_000_000, max_delay_ms=600_000),
    )


# Backend selection is local by default. Only the md_policy.BUDGET allowlisted remote nodes
# are part of routine validation; md_gate is historical evidence metadata only.
from md_policy import (  # noqa: E402,F401
    configure,
    pytest_addoption,
    pytest_collection_finish,
    pytest_collection_modifyitems,
    pytest_collectreport,
    pytest_generate_tests,
    pytest_runtest_logreport,
    pytest_runtest_protocol,
    pytest_sessionfinish,
    remote,
    timing_event,
    unconfigure,
)
from md_resources import owned_cleanup, record_resource  # noqa: E402

# Substrings of a MotherDuck error that mark it as transient (seen in suite logs as
# timeouts and connection failures). Anything else, such as binder, catalog, auth or
# RESOURCE_EXHAUSTED (a possible real quota), is raised immediately.
TRANSIENT_MD_ERRORS = (
    "DEADLINE_EXCEEDED",
    "UNAVAILABLE",
    "Could not connect to MotherDuck",
    "the MotherDuck servers took too long to respond",
)


# Auth failures are never transient, even when the text also says it couldn't connect;
# retrying a bad token would only burn ~30 s of backoff.
NEVER_TRANSIENT_MD_ERRORS = ("UNAUTHENTICATED", "PERMISSION_DENIED")


def wait_slot_released(con, slot, timeout=30.0):
    """Wait until no walsender holds the replication slot (or it is gone).

    A client closes its replication connection before PostgreSQL's walsender has
    released the slot, so dropping or advancing it straight after an ingest run can
    fail with ObjectInUse when the server is busy.
    """
    deadline = time.monotonic() + timeout
    while True:
        with con.cursor() as cur:
            cur.execute("SELECT active_pid FROM pg_replication_slots WHERE slot_name=%s", [slot])
            row = cur.fetchone()
        if row is None or row[0] is None:
            return
        if time.monotonic() > deadline:
            raise AssertionError(f"slot {slot} still held by pid {row[0]} after {timeout} s")
        time.sleep(0.1)


def is_transient_md_error(exc):
    text = str(exc)
    if any(marker in text for marker in NEVER_TRANSIENT_MD_ERRORS):
        return False
    return any(marker in text for marker in TRANSIENT_MD_ERRORS)


def admin_ddl(statement, attempts=5):
    """Fixture-only MotherDuck admin DDL with retry on transient errors. A timed-out
    request may still have succeeded, so statements must be idempotent (IF [NOT]
    EXISTS on names unique to this test)."""
    import re

    allocation = re.match(r'CREATE DATABASE IF NOT EXISTS "([a-zA-Z0-9_]+)"', statement)
    if allocation:
        record_resource("motherduck", allocation[1], "allocated")
    for attempt in range(attempts):
        try:
            removed = re.match(r'DROP DATABASE IF EXISTS "([a-zA-Z0-9_]+)"', statement)
            with owned_cleanup(removed[1] if removed else None):
                with duckdb.connect("md:") as admin:
                    admin.execute(statement)
            removed = re.match(r'DROP DATABASE IF EXISTS "([a-zA-Z0-9_]+)"', statement)
            if removed:
                record_resource("motherduck", removed[1], "released")
            return
        except duckdb.Error as exc:
            if not is_transient_md_error(exc) or attempt == attempts - 1:
                raise
            time.sleep(2**attempt)


def md_size(stores, *, local, motherduck):
    """A workload size that's smaller on MotherDuck, where each cycle costs round trips.
    Local runs (and CI) keep the full size. Every MotherDuck-only reduction goes through
    here, so `grep md_size` lists them all."""
    return motherduck if stores[0].remote else local


# A local run (CDC_TEST_MD unset) must never reach MotherDuck. If some code path opens
# md: anyway, DuckDB with no token may launch a browser login; with this explicit invalid
# placeholder it fails fast with an authentication error instead. Set for the whole
# session, at import, so every child process (CLI subprocesses, actors, inner pytest
# runs) inherits it, and overriding any real token from the shell. It covers pytest
# runs only: a script run outside pytest must check its own token.
# The first import in a process decides, and records it on `sys` (process-local, never
# inherited, so nothing in a later run's environment can switch the guard off): a later
# import (a test that loads a copy of this file in-process with CDC_TEST_MD unset) must
# not replace a MotherDuck run's real token, which monkeypatch could not restore.
LOCAL_RUN_TOKEN = "local-run-placeholder-not-a-motherduck-token"
if not hasattr(sys, "_cdc_test_token_mode"):
    sys._cdc_test_token_mode = "md" if os.getenv("CDC_TEST_MD") == "1" else "local"
if sys._cdc_test_token_mode == "local":
    os.environ["MOTHERDUCK_TOKEN"] = os.environ["motherduck_token"] = LOCAL_RUN_TOKEN


def md_token():
    load_dotenv(Path(__file__).resolve().parents[1] / ".env", override=False)
    token = os.getenv("MOTHERDUCK_TOKEN") or os.getenv("motherduck_token")
    if not token:
        pytest.fail("MotherDuck probes require a token in .env; browser login is disabled")
    os.environ["MOTHERDUCK_TOKEN"] = token


def is_interrupt(exc):
    """The soft stop's KeyboardInterrupt (SoftStop), in any of the forms it reaches us in:
    as itself, or turned by DuckDB, mid-query, into InterruptException or
    `RuntimeError: Query interrupted`."""
    if isinstance(exc, (KeyboardInterrupt, duckdb.InterruptException)):
        return True
    return isinstance(exc, RuntimeError) and "Query interrupted" in str(exc)


def drop_md_databases(names):
    """Drop every name, even after a failure, then raise the first failure.

    A soft stop can land in the middle of this teardown. Its interrupt arrives once, so
    the interrupted DROP is retried once (it may or may not have run; IF EXISTS makes
    that safe) and the rest still get dropped. The interrupt is raised at the end."""
    failures = []
    for db in reversed(names):
        for retry in (False, True):
            try:
                admin_ddl(f"DROP DATABASE IF EXISTS {ident(db)}")
            except BaseException as exc:
                if not (isinstance(exc, duckdb.Error) or is_interrupt(exc)):
                    raise
                failures.append(exc)
                if is_interrupt(exc) and not retry:
                    continue
            break
    if failures:
        raise failures[0]


def create_initialized_md_pair(name, created):
    """Create name and name_mirror on MotherDuck and run Store.initialize() on them."""
    mirror = name + "_mirror"
    for db in (name, mirror):
        # Record first: a timed-out create may exist and must be dropped.
        created.append(db)
        admin_ddl(f"CREATE DATABASE IF NOT EXISTS {ident(db)}")
    with duckdb.connect(f"md:{name}?attach_mode=single") as con:
        con.execute(f"ATTACH IF NOT EXISTS 'md:{mirror}' AS {ident(mirror)}")
        con.execute("SET TimeZone='UTC'")
        Store(con, name, mirror).initialize()
    return name, mirror


@pytest.fixture(scope="session")
def md_template():
    """An initialized store pair that each MotherDuck `stores` clones, one per process
    (so one per xdist worker). Store.initialize() costs about 17 MotherDuck round trips,
    and cloning the pair costs about one. tests/test_conftest.py checks that a clone
    equals a freshly initialized pair."""
    # Skip before touching MotherDuck: local and CI runs have no CDC_TEST_MD (or token).
    if os.getenv("CDC_TEST_MD") != "1":
        pytest.skip("set CDC_TEST_MD=1 to run real MotherDuck probes")
    md_token()
    created = []
    try:
        yield create_initialized_md_pair("cdc_p0_tmpl_" + uuid.uuid4().hex[:12], created)
    finally:
        drop_md_databases(created)


@pytest.fixture
def stores(request, tmp_path):
    remote = getattr(request, "param", "local") == "motherduck"
    if remote and os.getenv("CDC_TEST_MD") != "1":
        pytest.skip("set CDC_TEST_MD=1 to run real MotherDuck probes")
    name = "cdc_p0_" + uuid.uuid4().hex[:12]
    mirror = name + "_mirror"
    if remote:
        md_token()
    connections, created, result = [], [], []
    try:
        if remote:
            template = request.getfixturevalue("md_template")
            for db, source in zip((name, mirror), template):
                # Record first: a timed-out create may exist and must be dropped.
                created.append(db)
                admin_ddl(f"CREATE DATABASE IF NOT EXISTS {ident(db)} FROM {ident(source)}")
            address = f"md:{name}?attach_mode=single"
            attachment = "md:" + mirror
        else:
            address = str(tmp_path / (name + ".duckdb"))
            attachment = str(tmp_path / (mirror + ".duckdb"))
        for _ in range(2):
            con = duckdb.connect(address)
            connections.append(con)
            con.execute(f"ATTACH IF NOT EXISTS '{attachment}' AS {ident(mirror)}")
            con.execute("SET TimeZone='UTC'")
            store = Store(con, name, mirror)
            store.address, store.attachment, store.remote = address, attachment, remote
            result.append(store)
        if not remote:
            result[0].initialize()  # MotherDuck stores are clones of md_template
        yield tuple(result)
    finally:
        for store in reversed(result):
            store.release_all()
        for con in reversed(connections):
            con.close()
        drop_md_databases(created)


@pytest.fixture(scope="session", autouse=True)
def track_source_namespaces():
    """Record unique PG namespaces before allocation, without recording credentials.

    Configured slot/publication names carry this source's unique namespace, including
    epoch suffixes. The registry supports owner-scoped reconciliation after a kill;
    it does not authorize automatic cleanup of ambiguous resources.
    """
    from cdc_flight.source import Source

    enter, leave = Source.__enter__, Source.__exit__

    def tracked_enter(source):
        record_resource("postgres_namespace", source.schema, "allocated")
        return enter(source)

    def tracked_leave(source, *args):
        result = leave(source, *args)
        record_resource("postgres_namespace", source.schema, "released")
        return result

    Source.__enter__, Source.__exit__ = tracked_enter, tracked_leave
    try:
        yield
    finally:
        Source.__enter__, Source.__exit__ = enter, leave


@pytest.fixture(autouse=True)
def isolate_nested_harness_environment(request, monkeypatch):
    """Nested pytest/pytester probes own no part of the real suite's journals.

    These harness-only tests intentionally create failed, interrupted and skipped
    child runs. Their own explicitly requested temporary results files still work.
    The real template-clone check in test_conftest.py remains a remote case.
    """
    meta_files = {"test_conftest.py", "test_md_policy.py", "test_md_suite.py"}
    if remote(request.node):
        return
    if request.node.path.name in meta_files or "pytester" in request.fixturenames:
        for name in list(os.environ):
            if name.startswith("MD_SUITE_"):
                monkeypatch.delenv(name, raising=False)
        monkeypatch.delenv(ci_shard.ENV, raising=False)  # a nested run is never a CI shard
