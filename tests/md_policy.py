"""The fixed remote regression budget. No environment flag restores a larger matrix."""

import json
import os
import signal
import time
from functools import lru_cache
from pathlib import Path

import duckdb
import md_resources
import pytest

POLICY = Path(__file__).with_name("md_allowlist.json")
BUDGET = 29  # the approved remote budget: md_allowlist.json holds exactly this many
PASSED = set()
CONFIG = None
ORIGINAL_CONNECT = None


def timing_event(phase, config, **fields):
    directory = os.getenv("MD_SUITE_EVENTS_DIR")
    if not directory or not config.getoption("md_suite", default=False):
        return
    start = float(os.environ["MD_SUITE_STARTED"])
    role = getattr(config, "workerinput", {}).get("workerid", "controller")
    row = {"phase": phase, "role": role, "elapsed": round(time.monotonic() - start, 3), **fields}
    with (Path(directory) / f"{os.getpid()}.jsonl").open("a") as out:
        out.write(json.dumps(row) + "\n")


def result_line(text):
    path = os.getenv("MD_SUITE_RESULTS")
    if path and CONFIG is not None and not hasattr(CONFIG, "workerinput"):
        with open(path, "a") as out:
            out.write(text + "\n")


def failure_reason(report):
    crash = getattr(report.longrepr, "reprcrash", None)
    text = crash.message if crash is not None else str(report.longrepr or "")
    return " ".join(text.split())[:300]


def configure(config):
    global CONFIG, ORIGINAL_CONNECT
    CONFIG = config
    PASSED.clear()
    config._md_busy_seconds = 0.0
    config._md_remote_seconds = 0.0
    result_line("# md-suite results: FAILED/ERROR ids as they happened (short-summary format)")
    result_line(f"# start {time.strftime('%Y-%m-%d %H:%M:%S')} pid {os.getpid()}")
    timing_event("configure", config)
    if not config.getoption("md_suite", default=False):
        return
    ORIGINAL_CONNECT = duckdb.connect

    def checked(database=":memory:", *args, **kwargs):
        if str(database).startswith("md:") and not (
            md_resources.ACTIVE_REMOTE or md_resources.CLEANING
        ):
            raise AssertionError("unbudgeted MotherDuck connection outside an allowlisted case")
        return ORIGINAL_CONNECT(database, *args, **kwargs)

    duckdb.connect = checked


def unconfigure(config):
    global CONFIG, ORIGINAL_CONNECT
    if ORIGINAL_CONNECT is not None:
        duckdb.connect = ORIGINAL_CONNECT
        ORIGINAL_CONNECT = None
    CONFIG = None


@lru_cache
def allowlist():
    rows = json.loads(POLICY.read_text())
    nodes = {row["nodeid"]: row for row in rows}
    if len(rows) != BUDGET or len(nodes) != BUDGET or any(not r.get("reason") for r in rows):
        raise pytest.UsageError(
            f"md_allowlist.json must contain exactly {BUDGET} unique, justified nodes"
        )
    return nodes


def explicit_node(config):
    """Only a single fully named diagnostic may add a non-allowlisted remote case."""
    named = [a for a in config.args if "::" in a]
    if len(named) != 1:
        return None
    path, _, name = named[0].partition("::")
    try:
        path = (config.invocation_params.dir / path).resolve().relative_to(config.rootpath)
    except ValueError:
        return None
    return f"{path.as_posix()}::{name}"


def pytest_addoption(parser):
    parser.addoption(
        "--md-suite", action="store_true", help=f"require the complete {BUDGET}-case MD budget"
    )


@pytest.hookimpl(tryfirst=True)
def pytest_generate_tests(metafunc):
    if "stores" not in metafunc.fixturenames:
        return
    # Explicit local parametrizations and the two mixed secondary-axis tables own
    # their own cases. All new ordinary stores tests otherwise default to local.
    for mark in metafunc.definition.iter_markers("parametrize"):
        names = mark.args[0]
        names = names.split(",") if isinstance(names, str) else names
        if "stores" in [n.strip() for n in names]:
            return
    function = metafunc.definition.nodeid
    selected = any(n.startswith(function + "[") for n in allowlist())
    diagnostic = explicit_node(metafunc.config)
    diagnostic = (diagnostic or "").startswith(function + "[motherduck")
    params = ["local", "motherduck"] if selected or diagnostic else ["local"]
    metafunc.parametrize("stores", params, indirect=True)


def remote(item):
    spec = getattr(item, "callspec", None)
    return (spec is not None and spec.params.get("stores") == "motherduck") or (
        item.get_closest_marker("motherduck") is not None
    )


def pytest_collection_modifyitems(config, items):
    allowed, diagnostic = allowlist(), explicit_node(config)
    removed = []
    for item in list(items):
        if remote(item) and item.nodeid not in allowed and item.nodeid != diagnostic:
            items.remove(item)
            removed.append(item)
    if removed:
        config.hook.pytest_deselected(items=removed)
    # Ordinary local cases and complete-table variants retain their relative order.
    # Start the longest remote cases first so a large case cannot strand one worker.
    items.sort(key=lambda i: -allowed.get(i.nodeid, {}).get("observed_seconds", 0))


def pytest_collection_finish(session):
    timing_event("collection_finish", session.config, items=len(session.items))
    if not session.config.getoption("md_suite"):
        return
    if os.getenv("CDC_TEST_MD") != "1" or not os.getenv("CDC_TEST_PG_DSN"):
        raise pytest.UsageError("md-suite requires CDC_TEST_MD=1 and CDC_TEST_PG_DSN")
    # Under xdist every worker collects. The controller does not own collected items.
    if not session.items and not hasattr(session.config, "workerinput"):
        return
    actual = {i.nodeid for i in session.items if remote(i)}
    if actual != set(allowlist()):
        raise pytest.UsageError(
            f"md-suite remote collection differs: missing={set(allowlist()) - actual}, extra={actual - set(allowlist())}"
        )


def pytest_runtest_logreport(report):
    if CONFIG is not None:
        CONFIG._md_busy_seconds += report.duration
        if report.nodeid in allowlist():
            CONFIG._md_remote_seconds += report.duration
            timing_event(
                "remote_report",
                CONFIG,
                nodeid=report.nodeid,
                stage=report.when,
                duration=round(report.duration, 4),
                outcome=report.outcome,
            )
    if report.when == "call" and report.passed and report.nodeid in allowlist():
        PASSED.add(report.nodeid)
    if report.failed:
        status = "FAILED" if report.when == "call" else "ERROR"
        result_line(f"{status} {report.nodeid} - {failure_reason(report)}")


def pytest_collectreport(report):
    if report.failed:
        result_line(f"ERROR {report.nodeid or '(collection)'} - {failure_reason(report)}")


def pytest_sessionfinish(session, exitstatus):
    if session.config.getoption("md_suite") and not hasattr(session.config, "workerinput"):
        missing = set(allowlist()) - PASSED
        if missing and exitstatus == 0:
            session.exitstatus = 1
            reporter = session.config.pluginmanager.get_plugin("terminalreporter")
            if reporter:
                reporter.write_line(
                    f"md-suite FAILED: mandatory remote cases did not pass: {sorted(missing)}"
                )

    result_line(f"# end {time.strftime('%Y-%m-%d %H:%M:%S')} exitstatus {int(session.exitstatus)}")
    timing_event(
        "session_finish",
        session.config,
        remote_passed=len(PASSED),
        busy_seconds=round(getattr(session.config, "_md_busy_seconds", 0), 3),
        remote_seconds=round(getattr(session.config, "_md_remote_seconds", 0), 3),
        exitstatus=int(session.exitstatus),
    )


@pytest.hookimpl(wrapper=True, tryfirst=True)
def pytest_runtest_protocol(item, nextitem):
    if not item.config.getoption("md_suite") or not remote(item):
        md_resources.ACTIVE_REMOTE = False
        return (yield)
    # A timed-out worker cannot restart/retry a remote test: the runner disables
    # xdist worker restarts and stops on the first failure. Its journal survives.
    md_resources.ACTIVE_REMOTE = True
    old = signal.signal(signal.SIGALRM, lambda *args: os._exit(124))
    previous = signal.setitimer(signal.ITIMER_REAL, 150)
    try:
        return (yield)
    finally:
        signal.setitimer(signal.ITIMER_REAL, *previous)
        signal.signal(signal.SIGALRM, old)
        md_resources.ACTIVE_REMOTE = False
