"""Selection is a fixed budget, including exact secondary-parameter IDs."""

import json
import subprocess
import sys
from pathlib import Path

import md_policy
import pytest


@pytest.fixture
def policy_probe(pytester, monkeypatch):
    monkeypatch.delenv("MD_SUITE_EVENTS_DIR", raising=False)
    monkeypatch.delenv("MD_SUITE_RESULTS", raising=False)
    monkeypatch.delenv("MD_SUITE_RESOURCE_DIR", raising=False)
    monkeypatch.delenv("CDC_TEST_MD", raising=False)
    monkeypatch.delenv("CDC_TEST_PG_DSN", raising=False)
    source = Path(md_policy.__file__).read_text()
    pytester.makepyfile(md_policy_under_test=source)
    pytester.makeconftest(
        "from md_policy_under_test import *\ndef pytest_configure(config): configure(config)\ndef pytest_unconfigure(config): unconfigure(config)\n"
    )
    pytester.makepyfile(
        md_resources=Path(md_policy.__file__).with_name("md_resources.py").read_text()
    )
    rows = [
        {
            "nodeid": f"test_cases.py::test_case_{n}[motherduck]",
            "reason": "budget",
            "observed_seconds": n,
        }
        for n in range(md_policy.BUDGET)
    ]
    (pytester.path / "md_allowlist.json").write_text(json.dumps(rows))
    pytester.makepyfile(
        test_cases="""import pytest
@pytest.fixture
def stores(request):
    return request.param
"""
        + "\n".join(f"def test_case_{n}(stores): pass" for n in range(md_policy.BUDGET + 2))
    )
    return pytester


def test_fixed_budget_and_new_cases_default_to_local(policy_probe):
    result = policy_probe.runpytest_subprocess("-q")
    result.assert_outcomes(passed=2 * md_policy.BUDGET + 2)  # every local, exactly BUDGET remote


def test_old_environment_flags_cannot_restore_the_matrix(policy_probe, monkeypatch):
    monkeypatch.setenv("CDC_TEST_MD_FULL", "1")
    monkeypatch.setenv("CDC_TEST_MD_GATE", "1")
    result = policy_probe.runpytest_subprocess("-q")
    result.assert_outcomes(passed=2 * md_policy.BUDGET + 2)


def test_one_explicit_remote_diagnostic_is_available(policy_probe):
    result = policy_probe.runpytest_subprocess(
        "-q", f"test_cases.py::test_case_{md_policy.BUDGET}[motherduck]"
    )
    result.assert_outcomes(passed=1)


def test_removing_an_allowlist_entry_fails_collection(policy_probe):
    path = policy_probe.path / "md_allowlist.json"
    path.write_text(json.dumps(json.loads(path.read_text())[:-1]))
    result = policy_probe.runpytest_subprocess("--collect-only", "-q")
    assert result.ret != 0
    assert f"exactly {md_policy.BUDGET}" in result.stdout.str() + result.stderr.str()


def test_renaming_a_mandatory_test_fails_strict_collection(policy_probe, monkeypatch):
    monkeypatch.setenv("CDC_TEST_MD", "1")
    monkeypatch.setenv("CDC_TEST_PG_DSN", "fake-unused-by-these-fixtures")
    path = policy_probe.path / "test_cases.py"
    path.write_text(path.read_text().replace("test_case_19(", "test_renamed("))
    result = policy_probe.runpytest_subprocess("--collect-only", "--md-suite", "-q")
    assert result.ret != 0
    assert "missing=" in result.stdout.str() + result.stderr.str()


def test_a_skipped_mandatory_remote_case_is_not_green(policy_probe, monkeypatch):
    monkeypatch.setenv("CDC_TEST_MD", "1")
    monkeypatch.setenv("CDC_TEST_PG_DSN", "fake-unused-by-these-fixtures")
    path = policy_probe.path / "test_cases.py"
    path.write_text(
        path.read_text().replace(
            "def test_case_19(stores): pass",
            'def test_case_19(stores): pytest.skip("not completed")',
        )
    )
    result = policy_probe.runpytest_subprocess("--md-suite", "-q", "-n", "2")
    assert result.ret != 0
    assert "mandatory remote cases did not pass" in result.stdout.str()


def test_real_collection_has_only_the_exact_allowlisted_remote_nodes():
    root = Path(__file__).resolve().parents[1]
    script = """import json,pytest
class Check:
 def pytest_collection_finish(self,session):
  nodes=[i.nodeid for i in session.items if getattr(getattr(i,'callspec',None),'params',{}).get('stores')=='motherduck' or i.get_closest_marker('motherduck') is not None]
  print('NODES='+json.dumps(sorted(nodes)))
pytest.main(['--collect-only','-q','-p','no:cacheprovider'],plugins=[Check()])
"""
    result = subprocess.run(
        [sys.executable, "-c", script], cwd=root, text=True, capture_output=True, timeout=30
    )
    assert result.returncode == 0, result.stderr
    nodes = json.loads(
        next(
            line.split("=", 1)[1]
            for line in result.stdout.splitlines()
            if line.startswith("NODES=")
        )
    )
    assert nodes == sorted(md_policy.allowlist())


def test_an_allowlist_entry_requires_its_justification(policy_probe):
    path = policy_probe.path / "md_allowlist.json"
    rows = json.loads(path.read_text())
    rows[0]["reason"] = ""
    path.write_text(json.dumps(rows))
    result = policy_probe.runpytest_subprocess("--collect-only", "-q")
    assert result.ret != 0
    assert "justified" in result.stdout.str() + result.stderr.str()


def test_a_function_without_a_remote_param_does_not_widen_collection(policy_probe):
    result = policy_probe.runpytest_subprocess("-q", f"test_cases.py::test_case_{md_policy.BUDGET}")
    result.assert_outcomes(passed=1)


def test_unmarked_real_md_connections_are_refused_before_the_sdk(policy_probe, monkeypatch):
    monkeypatch.setenv("CDC_TEST_MD", "1")
    monkeypatch.setenv("CDC_TEST_PG_DSN", "fake-unused-by-these-fixtures")
    path = policy_probe.path / "test_cases.py"
    path.write_text(
        path.read_text() + '\ndef test_unmarked():\n import duckdb\n duckdb.connect("md:")\n'
    )
    result = policy_probe.runpytest_subprocess("--md-suite", "-q")
    assert result.ret != 0
    assert "unbudgeted MotherDuck connection" in result.stdout.str()


def test_only_the_controller_writes_failed_ids(policy_probe, monkeypatch):
    results = policy_probe.path / "results"
    monkeypatch.setenv("MD_SUITE_RESULTS", str(results))
    path = policy_probe.path / "test_cases.py"
    path.write_text(path.read_text() + "\ndef test_deliberate_failure(): assert False\n")
    result = policy_probe.runpytest_subprocess("-q", "-n", "2")
    assert result.ret != 0
    lines = results.read_text().splitlines()
    failures = [line for line in lines if line.startswith("FAILED ")]
    assert len(failures) == 1 and failures[0].startswith(
        "FAILED test_cases.py::test_deliberate_failure - "
    )
    assert len([line for line in lines if line.startswith("# start ")]) == 1
    assert len([line for line in lines if line.startswith("# end ")]) == 1
