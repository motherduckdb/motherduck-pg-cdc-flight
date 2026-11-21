"""CI shards: `CDC_TEST_SHARD=i/n` keeps only shard i of n (1-based) of the collected tests.

The unit is the test file, so a module's tests and fixtures stay together. Files are
assigned greedily, heaviest first, to the lightest shard, using the per-file seconds in
`ci_weights.json`. A file missing from the table gets the median weight. Every shard
computes the same assignment from the same collected file list, so each test runs in
exactly one shard. A stale table only unbalances the shards; it never drops a test.
Without `CDC_TEST_SHARD` nothing is registered.

The weights are worker-seconds under CI's sharded `-n 2` jobs. To regenerate them,
concatenate each major's four shard job logs (each prints `--durations=0`) into one
file per major, then (`dev-docs/testing.md`):

    python tests/ci_shard.py weights pg13.log pg16.log pg18.log > tests/ci_weights.json
"""

import json
import os
import re
import statistics
import sys
from collections import defaultdict
from pathlib import Path

import pytest

WEIGHTS = Path(__file__).with_name("ci_weights.json")
ENV = "CDC_TEST_SHARD"


def parse(spec):
    match = re.fullmatch(r"(\d+)/(\d+)", spec or "")
    if not match:
        raise pytest.UsageError(f"{ENV} must be i/n with 1 <= i <= n, got {spec!r}")
    index, count = int(match[1]), int(match[2])
    if not 1 <= index <= count:
        raise pytest.UsageError(f"{ENV} must be i/n with 1 <= i <= n, got {spec!r}")
    return index, count


def load_weights(path=WEIGHTS):
    return json.loads(path.read_text()) if path.exists() else {}


def assign(files, count, weights):
    """{file: shard index (1-based)}: heaviest first onto the lightest shard. Ties break
    by name and by lowest shard index, so the result depends only on the inputs."""
    files = sorted(set(files))
    known = [weights[f] for f in files if f in weights]
    default = statistics.median(known) if known else 1.0
    load = [0.0] * count
    shard_of = {}
    for name in sorted(files, key=lambda f: (-weights.get(f, default), f)):
        index = min(range(count), key=lambda i: (load[i], i))
        load[index] += weights.get(name, default)
        shard_of[name] = index + 1
    return shard_of


def file_of(item):
    return item.nodeid.split("::", 1)[0]


class ShardPlugin:
    def __init__(self, index, count):
        self.index, self.count = index, count

    @pytest.hookimpl(trylast=True)
    def pytest_collection_modifyitems(self, config, items):
        shard_of = assign({file_of(i) for i in items}, self.count, load_weights())
        keep = [i for i in items if shard_of[file_of(i)] == self.index]
        drop = [i for i in items if shard_of[file_of(i)] != self.index]
        if drop:
            config.hook.pytest_deselected(items=drop)
            items[:] = keep

    def pytest_report_header(self, config):
        return f"ci shard {self.index}/{self.count}"


def configure(config):
    spec = os.getenv(ENV)
    if spec:
        config.pluginmanager.register(ShardPlugin(*parse(spec)), "cdc-ci-shard")


def weights_from_logs(paths):
    """Mean per-file seconds (setup + call + teardown) over the given --durations logs."""
    per_log = []
    for path in paths:
        totals = defaultdict(float)
        for line in Path(path).read_text(errors="replace").splitlines():
            match = re.search(r"([\d.]+)s (?:call|setup|teardown)\s+(tests/\S+?\.py)::", line)
            if match:
                totals[match[2]] += float(match[1])
        per_log.append(totals)
    files = sorted(set().union(*per_log))
    return {f: round(sum(t.get(f, 0.0) for t in per_log) / len(per_log), 1) for f in files}


if __name__ == "__main__":
    if len(sys.argv) < 3 or sys.argv[1] != "weights":
        sys.exit("usage: python tests/ci_shard.py weights <durations log>...")
    json.dump(weights_from_logs(sys.argv[2:]), sys.stdout, indent=1, sort_keys=True)
    sys.stdout.write("\n")
