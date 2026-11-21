"""Real process groups exercise success, stuck calls, and stuck teardown cheaply."""

import importlib.util
import os
import sys
import time
from pathlib import Path

import md_policy
import pytest
from test_md_policy import policy_probe  # noqa: F401

spec = importlib.util.spec_from_file_location(
    "md_suite", Path(__file__).resolve().parents[1] / "scripts/md_suite.py"
)
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)


def run(script, tmp_path):
    log = tmp_path / "child.log"
    start = time.monotonic()
    with log.open("w", buffering=1) as output:
        code, cancelled = runner.supervise(
            [sys.executable, "-c", script],
            env=os.environ.copy(),
            output=output,
            started=start,
            soft=0.4,
            force=0.7,
            limit=1.0,
        )
    return code, cancelled, time.monotonic() - start, log.read_text()


def test_success_is_green_and_a_failed_child_is_not(tmp_path):
    assert run("pass", tmp_path)[:2] == (0, False)
    assert run("raise SystemExit(3)", tmp_path)[:2] == (3, False)


@pytest.mark.parametrize(
    "script",
    [
        "import signal,time; signal.signal(signal.SIGTERM, signal.SIG_IGN); time.sleep(30)",
        "import signal,time; signal.signal(signal.SIGTERM, lambda *a: time.sleep(30)); time.sleep(30)",
        # The worker survives its controller; the outer supervisor still owns its group.
        'import subprocess,sys; subprocess.Popen([sys.executable,"-c","import time; time.sleep(30)"])',
    ],
)
def test_watchdog_bounds_a_stuck_call_or_teardown_or_orphan(script, tmp_path):
    code, cancelled, elapsed, text = run(script, tmp_path)
    assert code == 124 and cancelled
    assert elapsed < 2
    assert "stop" in text


def test_resource_journal_retains_only_unreleased_names(tmp_path):
    (tmp_path / "1.jsonl").write_text(
        "\n".join(
            [
                '{"kind":"motherduck","name":"owned","event":"allocated"}',
                '{"kind":"motherduck","name":"owned","event":"released"}',
                '{"kind":"postgres_namespace","name":"own_schema","event":"allocated"}',
            ]
        )
    )
    assert runner.pending_resources(tmp_path) == [
        {"kind": "postgres_namespace", "name": "own_schema"}
    ]


def test_a_deadline_is_failure_even_when_the_child_exits_zero_on_term(tmp_path):
    script = "import signal,sys,time; signal.signal(signal.SIGTERM, lambda *a: sys.exit(0)); time.sleep(30)"
    assert run(script, tmp_path)[:2] == (124, True)


def test_a_truncated_allocation_record_cannot_be_green(tmp_path):
    (tmp_path / "1.jsonl").write_text('{"kind":"motherduck",')
    assert runner.pending_resources(tmp_path) == [
        {"kind": "incomplete_journal", "name": "1.jsonl:0"}
    ]


def test_runner_distributes_the_remote_budget_across_four_workers(policy_probe, monkeypatch):  # noqa: F811
    monkeypatch.setenv("CDC_TEST_MD", "1")
    monkeypatch.setenv("CDC_TEST_PG_DSN", "fake-unused-by-these-fixtures")
    path = policy_probe.path / "test_cases.py"
    text = path.read_text().replace(
        "import pytest", "import pytest,os,time\nfrom pathlib import Path", 1
    )
    text = text.replace(
        "(stores): pass",
        '(stores):\n if stores == "motherduck":\n  with open("md-workers", "a") as out: out.write(os.environ["PYTEST_XDIST_WORKER"] + "\\n")\n  time.sleep(0.02)',
    )
    # Reproduce the original pathology: with 450+ items xdist's default initial
    # chunk is about28, enough to strand the entire sorted remote budget on gw0.
    text += "\n" + "\n".join(f"def test_local_extra_{n}(): pass" for n in range(400))
    path.write_text(text)
    args = runner.pytest_command(sys.executable)[3:]
    result = policy_probe.runpytest_subprocess(*args)
    result.assert_outcomes(passed=2 * md_policy.BUDGET + 2 + 400)
    workers = (policy_probe.path / "md-workers").read_text().splitlines()
    assert len(workers) == md_policy.BUDGET and set(workers) == {"gw0", "gw1", "gw2", "gw3"}
