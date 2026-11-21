"""CI shards (tests/ci_shard.py): every collected test runs in exactly one shard."""

import os
import subprocess
import sys
from pathlib import Path

import ci_shard
import pytest

ROOT = Path(__file__).resolve().parents[1]
FILES = sorted(f"tests/{p.name}" for p in (ROOT / "tests").glob("test_*.py"))


@pytest.mark.parametrize("count", range(1, 7))
def test_every_real_test_file_lands_in_exactly_one_shard(count):
    weights = ci_shard.load_weights()
    shard_of = ci_shard.assign(FILES, count, weights)
    assert set(shard_of) == set(FILES)
    assert set(shard_of.values()) <= set(range(1, count + 1))
    # The same files in any order give the same assignment: every shard agrees.
    assert ci_shard.assign(list(reversed(FILES)), count, weights) == shard_of


def test_an_unknown_file_still_gets_a_shard_and_a_missing_table_is_balanced_by_count():
    files = ["tests/test_a.py", "tests/test_new.py", "tests/test_b.py"]
    shard_of = ci_shard.assign(files, 2, {"tests/test_a.py": 10.0, "tests/test_b.py": 1.0})
    assert shard_of == {"tests/test_a.py": 1, "tests/test_b.py": 2, "tests/test_new.py": 2}
    assert sorted(ci_shard.assign(files, 3, {}).values()) == [1, 2, 3]


@pytest.mark.parametrize("spec", ["", "0/4", "5/4", "1/0", "x", "1/4/2", " 1/4"])
def test_a_malformed_shard_spec_is_a_usage_error(spec):
    with pytest.raises(pytest.UsageError):
        ci_shard.parse(spec)


def collected(shard=None):
    env = {k: v for k, v in os.environ.items() if k != ci_shard.ENV}
    if shard:
        env[ci_shard.ENV] = shard
    proc = subprocess.run(
        [sys.executable, "-m", "pytest", "--collect-only", "-q", "-p", "no:cacheprovider"],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0, proc.stdout[-2000:] + proc.stderr[-2000:]
    return [line for line in proc.stdout.splitlines() if "::" in line]


def test_the_shards_of_the_real_collection_are_disjoint_and_cover_it():
    whole = collected()
    shards = [collected(f"{i}/4") for i in range(1, 5)]
    assert all(shards), [len(s) for s in shards]
    union = [node for shard in shards for node in shard]
    assert len(union) == len(set(union)), "a test landed in two shards"
    assert set(union) == set(whole)
    # Whole files only: no file is split between shards.
    files = [{node.split("::")[0] for node in shard} for shard in shards]
    assert sum(len(f) for f in files) == len(set().union(*files))


def test_weights_are_mean_seconds_per_file_over_the_logs(tmp_path):
    one, two = tmp_path / "one.log", tmp_path / "two.log"
    one.write_text(
        "2.00s call     tests/test_a.py::test_x[local]\n"
        "1.00s setup    tests/test_a.py::test_y\n"
        "2026-11-08T00:00:00Z 4.00s teardown tests/test_b.py::test_z\n"
    )
    two.write_text("1.00s call     tests/test_a.py::test_x[local]\n")
    assert ci_shard.weights_from_logs([one, two]) == {
        "tests/test_a.py": 2.0,
        "tests/test_b.py": 2.0,
    }
