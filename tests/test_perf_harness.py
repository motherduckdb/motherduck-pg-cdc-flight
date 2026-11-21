"""The format A/B harness (scripts/perf/ab.py): metric plumbing, oracle, schedule,
tree isolation, and one tiny local-store rep per workload through the f2 arm."""

import importlib.util
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import conftest
import duckdb
import pyarrow as pa
import pytest

ROOT = Path(__file__).resolve().parents[1]
AB_PATH = ROOT / "scripts" / "perf" / "ab.py"
_spec = importlib.util.spec_from_file_location("perf_ab", AB_PATH)
ab = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(ab)


def run_python(code, **env):
    return subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        cwd=ROOT,
        env={**os.environ, **env},
        timeout=120,
    )


PER_STEP_RSS = ("peak_rss_ingest_mb", "peak_rss_apply_mb", "peak_rss_oracle_mb")


@pytest.mark.skipif(sys.platform != "linux", reason="per-step peaks use /proc clear_refs")
def test_per_step_peaks_are_their_own_and_never_lower_the_whole_peak(monkeypatch):
    monkeypatch.setattr(ab, "STEP", ["start"])
    monkeypatch.setattr(ab, "STEP_PEAKS", {})
    monkeypatch.setattr(ab, "_HWM_RESET", [None])
    ab.step("big")
    block = bytearray(200 * 1024 * 1024)
    block[::4096] = b"1" * len(block[::4096])  # touch every page: it's resident
    del block
    ab.step("small")
    steps = ab.peak_rss_by_step_kb()
    assert steps["big"] - steps["small"] > 150 * 1024, steps  # the reset split them
    assert ab.peak_rss_kb() >= steps["big"]  # the whole-rep peak still includes "big"


# Isolation: the harness never imports the product ------------------------------------


def test_harness_import_leaves_the_product_unimported():
    proc = run_python(
        "import sys; sys.path.insert(0, 'scripts/perf'); import ab; "
        "assert 'cdc_flight' not in sys.modules, 'harness imported the product'"
    )
    assert proc.returncode == 0, proc.stderr[-400:]


def make_tree(tmp_path, arm="f2", *, digest=None):
    tree = tmp_path / arm
    (tree / "src" / "cdc_flight").mkdir(parents=True)
    (tree / "src" / "cdc_flight" / "__init__.py").write_text("")
    manifest = dict(arm=arm, commit="0" * 40, tree_sha256=digest or ab.tree_digest(tree))
    (tree / "manifest.json").write_text(json.dumps(manifest))
    return tree


def load_in_child(arm, tree):
    return run_python(
        "import sys; sys.path.insert(0, 'scripts/perf'); import ab; "
        f"ab.load_arm({arm!r}, {str(tree)!r})"
    )


def test_load_arm_refuses_a_changed_tree(tmp_path):
    tree = make_tree(tmp_path, digest="f" * 64)
    proc = load_in_child("f2", tree)
    assert proc.returncode != 0 and "tree_digest_mismatch" in proc.stderr


def test_load_arm_refuses_a_tree_of_another_arm(tmp_path):
    tree = make_tree(tmp_path, arm="f1")
    proc = load_in_child("f2", tree)
    assert proc.returncode != 0 and "tree_arm_mismatch" in proc.stderr


def test_load_arm_names_the_branch_for_the_f1_adapter(tmp_path):
    if (AB_PATH.parent / "arm_f1.py").exists():
        pytest.skip("the f1 adapter is present (perf-f1-arm)")
    tree = make_tree(tmp_path, arm="f1")
    proc = load_in_child("f1", tree)
    assert proc.returncode != 0 and "perf-f1-arm" in proc.stderr


def test_load_arm_refuses_once_the_product_is_imported(tmp_path):
    import cdc_flight  # noqa: F401  (this pytest process already holds a product)

    with pytest.raises(RuntimeError, match="product_imported_before_tree"):
        ab.load_arm("f2", make_tree(tmp_path))


def test_load_arm_refuses_a_product_from_outside_the_tree(tmp_path):
    # The tree has no package, so `cdc_flight` resolves elsewhere (the working tree).
    tree = tmp_path / "f2"
    (tree / "src").mkdir(parents=True)
    (tree / "src" / "README").write_text("no package here")
    manifest = dict(arm="f2", commit="0" * 40, tree_sha256=ab.tree_digest(tree))
    (tree / "manifest.json").write_text(json.dumps(manifest))
    proc = load_in_child("f2", tree)
    assert proc.returncode != 0
    assert "product_not_from_tree" in proc.stderr or "ModuleNotFoundError" in proc.stderr


# Statement kinds and Arrow bytes ---------------------------------------------------


@pytest.mark.parametrize(
    ("sql", "kind"),
    [
        ("COMMIT", "commit"),
        ('INSERT INTO "db"."s".frames_v2_20261107 SELECT * FROM x', "insert:frames_v2_d"),
        ('INSERT INTO "db"."s".changes_3 SELECT 1', "insert:changes_d"),
        ('UPDATE "db"."s".source SET a=1', "update:source"),
        ('CREATE TABLE "db"."s".frames_v2_20261107 AS SELECT 1', "create:frames_v2_d"),
        ("SELECT 1", "select:?"),
    ],
)
def test_sql_kind(sql, kind):
    assert ab.sql_kind(sql) == kind


def test_proxy_counts_arrow_bytes_on_every_path():
    con = duckdb.connect()
    proxy = ab.TimingProxy(con)
    table = pa.table({"a": list(range(1000))})
    size = table.nbytes
    proxy.register("t1", table)
    proxy.from_arrow(table)
    reader = pa.RecordBatchReader.from_batches(table.schema, table.to_batches())
    proxy.register("t2", reader)  # read once and registered as a table
    assert proxy.execute("SELECT count(*) FROM t2").fetchone()[0] == 1000
    summary = proxy.summary()
    assert summary["arrow_bytes_by_path"] == dict(
        register=2 * size, from_arrow=size, parameter=0, replacement=0
    )
    assert summary["statements"]["register"][0] == 2
    assert summary["statements"]["select:t2"][0] == 1


def test_proxy_counts_replacement_scan_objects():
    class Fake:
        def execute(self, sql, *args):
            return None

    proxy = ab.TimingProxy(Fake())
    scanned = pa.table({"a": [1, 2, 3]})
    unrelated = pa.table({"b": [1]})
    proxy.execute("SELECT count(*) FROM scanned")
    assert proxy.summary()["arrow_bytes_by_path"]["replacement"] == scanned.nbytes
    del unrelated


def test_proxy_counts_arrow_parameters():
    class Fake:
        def execute(self, sql, *args):
            return None

    proxy = ab.TimingProxy(Fake())
    batch = pa.record_batch({"a": [1, 2, 3]})
    proxy.execute("SELECT ?", [batch, 1])
    assert proxy.summary()["arrow_bytes_by_path"]["parameter"] == batch.nbytes


def test_hooks_are_exclusive_when_nested():
    class Owner:
        def outer(self):
            self.inner()

        def inner(self):
            sum(range(200_000))

    hooks = ab.Hooks()
    hooks.install(Owner, "outer", "outer")
    hooks.install(Owner, "inner", "inner")
    for _ in range(3):
        Owner().outer()
    totals = hooks.summary()
    assert totals["outer"][0] == totals["inner"][0] == 3
    assert totals["outer"][1] < totals["inner"][1]  # the nested call's time is the inner's


# Wire: the default-route interface only ----------------------------------------------

ROUTES = """Iface\tDestination\tGateway\tFlags\tRefCnt\tUse\tMetric\tMask\tMTU\tWindow\tIRTT
ens5\t00000000\t0100A8C0\t0003\t0\t0\t100\t00000000\t0\t0\t0
ens5\t0000A8C0\t00000000\t0001\t0\t0\t100\t00FFFFFF\t0\t0\t0
docker0\t000011AC\t00000000\t0001\t0\t0\t0\t0000FFFF\t0\t0\t0
"""
NETDEV = """Inter-|   Receive |  Transmit
 face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets
    lo: 500 5 0 0 0 0 0 0 600 6 0 0 0 0 0 0
  ens5: 1000 10 0 0 0 0 0 0 2000 20 0 0 0 0 0 0
docker0: 7000 70 0 0 0 0 0 0 8000 80 0 0 0 0 0 0
"""


def test_default_interface_and_routes():
    assert ab.default_interface(ROUTES) == "ens5"
    assert ab.route_interface(ROUTES, "172.17.0.2") == ("docker0", False)
    assert ab.route_interface(ROUTES, "192.168.0.9") == ("ens5", False)
    assert ab.route_interface(ROUTES, "10.1.2.3") == ("ens5", True)
    assert ab.netdev_bytes(NETDEV, "ens5") == (1000, 2000)
    assert ab.netdev_bytes(NETDEV, "docker0") == (7000, 8000)


def test_wire_is_null_when_the_source_shares_the_default_route(monkeypatch):
    monkeypatch.setattr(ab, "read_text", lambda path: ROUTES)
    monkeypatch.setattr(ab.socket, "getaddrinfo", lambda *a: [(0, 0, 0, "", ("10.1.2.3", 0))])
    assert ab.wire_plan("host=rds.example port=5432") == dict(
        iface=None, reason="source_on_default_route"
    )
    monkeypatch.setattr(ab.socket, "getaddrinfo", lambda *a: [(0, 0, 0, "", ("172.17.0.2", 0))])
    assert ab.wire_plan("host=pg port=5432")["iface"] == "ens5"
    monkeypatch.setattr(ab.socket, "getaddrinfo", lambda *a: [(0, 0, 0, "", ("127.0.0.1", 0))])
    assert ab.wire_plan("host=127.0.0.1")["source_route"] == "loopback"
    assert ab.wire_plan("host=/var/run/postgresql dbname=x") == dict(
        iface="ens5", source_route="socket"
    )


def test_allow_local_follows_the_source_not_the_store(monkeypatch):
    monkeypatch.delenv("PERF_ALLOW_LOCAL_SOURCE", raising=False)
    assert ab.source_local("host=127.0.0.1 port=55432 dbname=x")
    assert ab.source_local("host=localhost dbname=x")
    assert ab.source_local("host=/var/run/postgresql dbname=x")
    assert not ab.source_local("host=db.example.com dbname=x")
    assert not ab.source_local("host=172.17.0.2 dbname=x")
    monkeypatch.setenv("PERF_ALLOW_LOCAL_SOURCE", "1")
    assert ab.source_local("host=172.17.0.2 dbname=x")


# The CPU cap -------------------------------------------------------------------------


def test_cap_check():
    assert ab.cap_check("none", 2, None, None) is None
    assert ab.cap_check("quota", 3, ab.QUOTA, None) == "nproc_below_4"
    assert ab.cap_check("quota", 16, "max 100000", None) == "quota_not_applied"
    assert ab.cap_check("quota", 16, ab.QUOTA, None) is None
    assert ab.cap_check("affinity", 16, None, 4) == "affinity_not_applied"
    assert ab.cap_check("affinity", 16, None, 2) is None
    assert ab.cap_check("taskset", 16, None, 2) == "unknown_cap_mode"
    assert ab.cap_command("quota", ["py"])[:5] == [
        "systemd-run",
        "--user",
        "--scope",
        "--quiet",
        "-p",
    ]
    assert ab.cap_command("affinity", ["py"]) == ["py"]


# Oracle ------------------------------------------------------------------------------

ROWS = [(1, "a" * 1500, 0), (2, "b", 3), (99_999_999_999, "xyz\n", 7)]


def duck_hash(rows):
    con = duckdb.connect()
    con.execute("CREATE TABLE t(id bigint, v text, n int)")
    con.executemany("INSERT INTO t VALUES (?,?,?)", rows)
    return ab.content_hash(con.execute(ab.DUCK_HASH.format(table="t")).fetchone())


def test_duckdb_hash_is_order_independent_and_byte_sensitive():
    assert duck_hash(ROWS) == duck_hash(list(reversed(ROWS)))
    mutant = [ROWS[0], (2, "c", 3), ROWS[2]]
    assert duck_hash(mutant) != duck_hash(ROWS)
    assert duck_hash(ROWS)[0] == 3


@pytest.mark.postgres
def test_postgres_and_duckdb_hashes_agree(pg_dsn):
    import psycopg2

    con = psycopg2.connect(pg_dsn)
    try:
        cur = con.cursor()
        cur.execute("CREATE TEMP TABLE t(id bigint, v text, n int)")
        cur.executemany("INSERT INTO t VALUES (%s,%s,%s)", ROWS)
        cur.execute(ab.PG_HASH.format(table="t"))
        assert ab.content_hash(cur.fetchone()) == duck_hash(ROWS)
        cur.execute("UPDATE t SET v='c' WHERE id=2")
        cur.execute(ab.PG_HASH.format(table="t"))
        assert ab.content_hash(cur.fetchone()) != duck_hash(ROWS)
    finally:
        con.close()


def test_oracle_gates_content_ops_and_txns():
    good = [3, 1, 2, 3, 4]
    ok = ab.oracle(good, list(good), {"insert": 3}, 1, "insert", 3, 1000)
    assert ok["ok"]
    assert not ab.oracle(good, [3, 1, 2, 3, 5], {"insert": 3}, 1, "insert", 3, 1000)["ok"]
    # A duplicated event the mirror's MERGE would mask still fails on the log's counts.
    assert not ab.oracle(good, list(good), {"insert": 4}, 1, "insert", 3, 1000)["ok"]
    assert not ab.oracle(good, list(good), {"insert": 3}, 2, "insert", 3, 1000)["ok"]
    update = ab.oracle(good, list(good), {"insert": 3, "update": 3}, 2, "update", 3, 1000)
    assert update["ok"] and update["txns_expected"] == 2


# Schedule, budget and summary ----------------------------------------------------------


def test_plan_balances_positions_and_drops_wide_reps_first():
    schedule, dropped, _ = ab.plan(["f1", "f2"], ab.WORKLOADS, dict(ab.REPS), 1e9)
    assert not dropped
    for name, reps in ab.REPS.items():
        items = [s for s in schedule if s["name"] == name]
        assert len(items) == 2 * reps
        firsts = [s["arm"] for s in items if s["position"] == 0]
        assert abs(firsts.count("f1") - firsts.count("f2")) <= 1
    each = {n: ab.estimate_s(w) * 2 for n, w in ab.WORKLOADS.items()}
    budget = sum(each[n] * r for n, r in ab.REPS.items()) - each["insert10k_batched"]
    schedule, dropped, total = ab.plan(["f1", "f2"], ab.WORKLOADS, dict(ab.REPS), budget)
    assert dropped == ["insert10k_batched"] and total <= budget
    with pytest.raises(SystemExit, match="over budget"):
        ab.plan(["f1", "f2"], ab.WORKLOADS, dict(ab.REPS), 10)


def rep_row(arm, ok=True, digest="d1", workload="w"):
    return dict(
        kind="rep",
        arm=arm,
        workload=workload,
        ok=ok,
        harness_sha256=digest,
        events=100,
        window_txns=1,
        cap=dict(mode="quota"),
        host=dict(cpu_model="cpu"),
        ingest=dict(
            wall_s=1.0,
            process_cpu_s=0.5,
            hooks={"spool_feed": [1, 0.1, 0.1], "append": [1, 0.2, 0.1]},
            statements={"commit": [1, 0.01]},
            arrow_bytes=1000,
            transactions=1,
        ),
        apply=dict(wall_s=1.0, hooks={"merge": [1, 0.5, 0.1]}),
        wire=dict(ingest_tx=500),
        log_storage=dict(bytes=2000),
        log_events=100,
        ledger_rows=3,
        oracle=dict(txns_expected=1),
        peak_rss_kb=1024,
    )


def write_rows(tmp_path, rows):
    path = tmp_path / "r.jsonl"
    path.write_text("".join(json.dumps(r) + "\n" for r in rows))
    return path


def test_summarize_refuses_mixed_harnesses(tmp_path):
    path = write_rows(tmp_path, [rep_row("f1"), rep_row("f2", digest="d2")])
    with pytest.raises(SystemExit, match="mix harness"):
        ab.summarize(path)


def test_summarize_prints_the_realised_schedule_and_ratios(tmp_path):
    plan_row = dict(
        kind="plan",
        harness_sha256="d1",
        dropped=["w10"],
        schedule=[dict(name="w", arm=a) for a in ("f1", "f2", "f1", "f2")],
    )
    rows = [plan_row, rep_row("f1"), rep_row("f2"), rep_row("f1"), rep_row("f2", ok=False)]
    text = ab.summarize(write_rows(tmp_path, rows))
    assert "w f1: 2 / 2 / 0" in text and "w f2: 2 / 1 / 1" in text
    assert "UNBALANCED w" in text and "dropped by budget: w10" in text
    assert "f2/f1 1.000" in text and "host=cpu cap=quota" in text


def test_journal_pending_until_released(tmp_path):
    journal = ab.Journal(tmp_path, "motherduck")
    journal.record("cdc_p0_perf_x", "allocated")
    assert ab.pending(tmp_path) == ["motherduck:cdc_p0_perf_x"]
    journal.record("cdc_p0_perf_x", "released")
    assert ab.pending(tmp_path) == []
    (tmp_path / "torn.jsonl").write_text('{"kind": "motherduck", "na')
    assert ab.pending(tmp_path) == ["incomplete_journal:torn.jsonl:0"]


def test_failures_record_the_type_and_step_never_the_text(tmp_path, monkeypatch):
    secret = "password=hunter2-not-real"
    rows = []
    spec = dict(
        id=0,
        arm="zz",
        workload=["insert", 1024, 2, 1],
        name="w",
        round=0,
        position=0,
        trees=str(tmp_path),
        store="local",
        journal_dir=str(tmp_path),
        dsn_env="X",
        cap="none",
    )
    monkeypatch.setenv("PERF_CHILD_SPEC", json.dumps(spec))
    monkeypatch.delenv("PERF_DEBUG", raising=False)

    def boom(*args):
        raise ValueError(secret)

    monkeypatch.setattr(ab, "load_arm", boom)
    monkeypatch.setattr(ab, "emitter", lambda sink: lambda kind, **d: rows.append((kind, d)))
    assert ab.child() == 1
    assert rows == [("failed", dict(spec_id=0, error="ValueError", step=ab.STEP[0]))]
    assert secret not in json.dumps(rows)


# The PG tier: one tiny local-store rep per workload through the f2 arm ------------------


@pytest.mark.postgres
def test_pg_payload_is_core_sha512_without_an_extension(pg_dsn):
    import base64
    import hashlib

    import psycopg2

    con = psycopg2.connect(pg_dsn)
    try:
        with con.cursor() as cur:
            cur.execute(f"SELECT {ab.pg_payload(1024, 7, 3)}")
            value = cur.fetchone()[0]
    finally:
        con.rollback()
        con.close()
    raw = b"".join(hashlib.sha512(f"7:3:{k}".encode()).digest() for k in range(1, 13))
    assert base64.b64decode(value) == raw
    assert "CREATE EXTENSION" not in AB_PATH.read_text()


def extensions(dsn):
    import psycopg2

    con = psycopg2.connect(dsn)
    try:
        with con.cursor() as cur:
            cur.execute("SELECT extname, extversion FROM pg_extension ORDER BY 1")
            return cur.fetchall()
    finally:
        con.close()


@pytest.fixture
def private_dsn(pg_dsn):
    """A fresh database of its own on the test server: pg_extension is per database,
    so nothing a rep does there can reach other tests' recorded source bindings."""
    with conftest.private_database(pg_dsn, "cdc_p0_perf_t") as dsn:
        yield dsn


@pytest.mark.postgres
def test_tiny_local_reps_through_the_f2_arm(tmp_path, private_dsn):
    pg_dsn = private_dsn
    before = extensions(pg_dsn)
    trees = tmp_path / "trees"
    prepared = subprocess.run(
        [sys.executable, str(AB_PATH), "prepare", "f2", "HEAD", str(trees)],
        capture_output=True,
        text=True,
        cwd=ROOT,
    )
    if prepared.returncode != 0:
        pytest.skip("git archive of HEAD is unavailable here")
    out = tmp_path / "results.jsonl"
    env = dict(
        os.environ,
        CDC_TEST_PG_DSN=pg_dsn,
        PERF_TREES=str(trees),
        PERF_ARMS="f2",
        PERF_STORE="local",
        PERF_CPU_CAP="none",
        PERF_EVENTS_SCALE="0.002",
        PERF_REPS="insert1k_batched=1,insert1k_single=1,update1k_batched=1,insert10k_batched=1",
        PERF_OUT=str(out),
    )
    proc = subprocess.run(
        [sys.executable, str(AB_PATH), "run"],
        env=env,
        capture_output=True,
        text=True,
        timeout=600,
    )
    rows = [json.loads(line) for line in out.read_text().splitlines()]
    reps = [r for r in rows if r["kind"] == "rep"]
    assert proc.returncode == 0, [(r.get("workload"), r.get("error"), r.get("step")) for r in reps]
    assert sorted(r["workload"] for r in reps) == sorted(ab.WORKLOADS)
    assert extensions(pg_dsn) == before  # a rep never changes the database's extensions
    for r in reps:
        assert r["ok"] and r["oracle"]["ok"] and r["oracle"]["log_check"]["ok"]
        assert r["ingest"]["arrow_bytes"] > 0, "a zero is a harness gap, not a format result"
        assert r["product_tree_sha256"] and r["harness_sha256"] == ab.harness_digest()
        # Every rep timed its MERGE: "merge" (the Python fold's MERGE) or "sql_merge" (the
        # SQL fold's, fold and MERGE in one statement; the f2 arm's common case).
        hooks = r["apply"]["hooks"]
        assert hooks.get("merge", [0])[0] + hooks.get("sql_merge", [0])[0] >= 1
        assert any(k.startswith("spool") for k in r["ingest"]["hooks"])
        assert r["ingest"]["statements"]["commit"][0] >= 1
        linux_only = {"wire_tx_per_event", *(m for m in ab.METRICS if m in PER_STEP_RSS)}
        for metric, fn in ab.METRICS.items():
            if sys.platform == "linux" or metric not in linux_only:  # null off Linux
                assert fn(r) is not None, metric
        if sys.platform == "linux":
            steps = r["peak_rss_by_step_kb"]
            assert {"ingest", "apply", "oracle"} <= set(steps), steps
            assert r["peak_rss_kb"] >= max(steps.values())
    done = [r for r in rows if r["kind"] == "done"]
    assert done and done[0]["journal_pending"] == [] and done[0]["failures"] == 0
    assert all(
        not c["store_left"] and c["pg_clean"] is True for c in rows if c["kind"] == "cleanup"
    )
    summary = subprocess.run(
        [sys.executable, str(AB_PATH), "summarize", str(out), "f2"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    assert "realised schedule" in summary


# Stopping: a forwarded signal or the deadline unwinds the rep through its cleanup -------


def test_a_stop_signal_raises_once_and_never_during_cleanup(monkeypatch):
    import signal

    monkeypatch.setattr(ab, "STOPPING", {"raised": False, "cleaning": False})
    with pytest.raises(ab.Stopped):
        ab.on_stop(signal.SIGTERM, None)
    assert ab.on_stop(signal.SIGTERM, None) is None  # a second signal is ignored
    monkeypatch.setattr(ab, "STOPPING", {"raised": False, "cleaning": False})
    ab.cleaning()
    assert ab.on_stop(signal.SIGINT, None) is None  # cleanup always runs to the end


@pytest.mark.parametrize("ignores", [False, True])
def test_stop_child_terminates_then_kills_after_the_grace(tmp_path, ignores):
    ready = tmp_path / "ready"
    code = (
        "import pathlib,signal,sys,time\n"
        + ("signal.signal(signal.SIGTERM, signal.SIG_IGN)\n" if ignores else "")
        + f"pathlib.Path({str(ready)!r}).touch()\n"
        + "time.sleep(60)\n"
    )
    proc = subprocess.Popen(
        [sys.executable, "-c", code],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )
    try:
        deadline = time.monotonic() + 30
        while not ready.exists() and time.monotonic() < deadline:
            time.sleep(0.02)
        assert ready.exists()
        started = time.monotonic()
        _, _, killed = ab.stop_child(proc, 1.0)
        elapsed = time.monotonic() - started
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait()
    assert killed is ignores
    assert proc.returncode == (-9 if ignores else -15)
    assert elapsed < (3 if ignores else 1)


def held_run(tmp_path, dsn, hold_at="ingest", **extra):
    """`ab.py run` of two tiny reps, held on entering ingest; returns (proc, out, hold)."""
    trees = tmp_path / "trees"
    prepared = subprocess.run(
        [sys.executable, str(AB_PATH), "prepare", "f2", "HEAD", str(trees)],
        capture_output=True,
        text=True,
        cwd=ROOT,
    )
    if prepared.returncode != 0:
        pytest.skip("git archive of HEAD is unavailable here")
    out, hold = tmp_path / "results.jsonl", tmp_path / "held"
    env = dict(
        os.environ,
        CDC_TEST_PG_DSN=dsn,
        PERF_TREES=str(trees),
        PERF_ARMS="f2",
        PERF_WORKLOADS="insert1k_batched",
        PERF_REPS="insert1k_batched=2",
        PERF_STORE="local",
        PERF_CPU_CAP="none",
        PERF_EVENTS_SCALE="0.002",
        PERF_OUT=str(out),
        PERF_HOLD_AT=hold_at,
        PERF_HOLD_FILE=str(hold),
        **{"PERF_STOP_GRACE_S": "20", **extra},
    )
    proc = subprocess.Popen(
        [sys.executable, str(AB_PATH), "run"],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    return proc, out, hold, trees


def assert_stopped_clean(proc, out, trees, dsn, reason, code):
    import psycopg2

    try:
        returncode = proc.wait(timeout=40)
    finally:
        if proc.poll() is None:
            proc.kill()
    assert returncode == code
    rows = [json.loads(line) for line in out.read_text().splitlines()]
    reps = [r for r in rows if r["kind"] == "rep"]
    assert len(reps) == 1, "the second rep must never start"
    assert reps[0]["error"] == "Stopped" and reps[0]["step"] == "ingest" and not reps[0]["ok"]
    (cleanup,) = [r for r in rows if r["kind"] == "cleanup"]
    assert cleanup["pg_clean"] is True and cleanup["store_left"] == []
    (child,) = [r for r in rows if r["kind"] == "child_done"]
    assert child["stopped"] == reason and child["killed"] is False
    assert child["unparsed_lines"] == 0
    (done,) = [r for r in rows if r["kind"] == "done"]
    assert done["stopped"] == reason and done["journal_pending"] == []
    assert [s["name"] for s in done["skipped"]] == ["insert1k_batched"]
    assert not list((trees / "stores").glob("*.duckdb*"))
    con = psycopg2.connect(dsn)
    try:
        with con.cursor() as cur:
            cur.execute(
                "SELECT (SELECT count(*) FROM pg_replication_slots WHERE database = current_database())"
                " + (SELECT count(*) FROM pg_publication)"
                " + (SELECT count(*) FROM pg_namespace WHERE nspname LIKE 'cdc_p0_perf_%%')"
            )
            assert cur.fetchone()[0] == 0
    finally:
        con.close()
    summary = ab.summarize(str(out), "f2")
    assert f"STOPPED ({reason}); skipped: insert1k_batched/f2" in summary


# The deadline the held-rep tests give `run`. The rep must reach its hold first: on CI
# (2 vCPUs, inside a sharded -n 2 job) that took at most 1.8 s from Popen, so 5 s is
# over twice the worst seen. A deadline that fell first would fail the test loudly
# (the rep stops before holding, so the hold never appears), never pass it.
HOLD_DEADLINE_S = 5


def wait_for(path, seconds=60):
    """The rep's pid, once it is at its hold. The rep writes the file with a bare
    write_text, so the file can exist before the pid is in it: wait for the content."""
    deadline = time.monotonic() + seconds
    while True:
        try:
            return int(path.read_text())
        except (FileNotFoundError, ValueError):
            if time.monotonic() >= deadline:
                raise AssertionError("the rep never reached its hold") from None
        time.sleep(0.05)


_TAIL_DRIVER = """
import importlib.util, os, signal, sys
spec = importlib.util.spec_from_file_location("perf_ab", sys.argv[1])
ab = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ab)
ab.run_schedule = lambda *args: (0, [])  # the schedule is already over
def pending(directory):
    os.kill(os.getpid(), signal.SIGTERM)  # a stop in run's tail, after the schedule
    return []
ab.pending = pending
rc = ab.run()
if sys.argv[2] == "exit":
    os.kill(os.getpid(), signal.SIGTERM)  # a second stop after run returned, before exit
if sys.argv[2] == "teardown":
    # Park the process at the very end of interpreter teardown, after CPython has reset
    # Python-level handlers to SIG_DFL (_PySignal_Fini): Py_AtExit functions run last.
    import ctypes
    libc = ctypes.CDLL(None)
    ctypes.pythonapi.Py_AtExit(ctypes.cast(libc.pause, ctypes.c_void_p))
    print("returned", flush=True)
sys.exit(rc)
"""


@pytest.mark.parametrize("when", ["tail", "exit"])
def test_a_signal_after_the_schedule_still_writes_done_and_exits_143(tmp_path, when):
    """A unit stop can deliver SIGTERM to `run` after its schedule has returned: in the
    `done` tail ("tail"), and a second one after `run` returns, before the process exits
    ("exit"). `run`'s handler must stay installed through exit, or the default handler
    kills the process (exit -15) instead of the recorded stop's 143."""
    trees = tmp_path / "trees"
    (trees / "f2").mkdir(parents=True)
    (trees / "f2" / "manifest.json").write_text("{}")
    driver = tmp_path / "tail_driver.py"
    driver.write_text(_TAIL_DRIVER)
    out = tmp_path / "out.jsonl"
    env = dict(
        os.environ,
        PERF_TREES=str(trees),
        PERF_ARMS="f2",
        PERF_STORE="local",
        PERF_CPU_CAP="none",
        PERF_WORKLOADS="insert1k_single",
        PERF_REPS="insert1k_single=1",
        PERF_DSN_ENV="PERF_TAIL_DSN",
        PERF_TAIL_DSN="unused",
        PERF_OUT=str(out),
    )
    proc = subprocess.run(
        [sys.executable, str(driver), str(AB_PATH), when],
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert proc.returncode == 143, (proc.returncode, proc.stderr[-500:])
    done = [json.loads(line) for line in out.read_text().splitlines() if '"done"' in line]
    assert len(done) == 1 and done[0]["stopped"] == "signal" and done[0]["skipped"] == [], done


def sigterm_disposition(pid):
    """(caught, ignored) for SIGTERM in a live process, from /proc."""
    status = Path(f"/proc/{pid}/status").read_text().splitlines()
    fields = dict(line.split(":\t", 1) for line in status if ":\t" in line)
    bit = 1 << (signal.SIGTERM - 1)
    return bool(int(fields["SigCgt"], 16) & bit), bool(int(fields["SigIgn"], 16) & bit)


@pytest.mark.skipif(not Path("/proc/self/status").exists(), reason="needs Linux /proc")
def test_a_signal_during_interpreter_teardown_does_not_kill_run(tmp_path):
    """A unit stop's second SIGTERM can reach `run` while the interpreter is exiting. By
    then CPython has reset `run`'s Python handler to SIG_DFL, so unless `run` ignores the
    signal at the kernel, it dies with -15 although `done` already said 143. The driver
    parks the process after that reset (a Py_AtExit pause), so this is deterministic.
    Before the fix the wait below ends only at the reset, so the SIGTERM always meets
    SIG_DFL. After it SIGTERM is never caught, so the signal may land before teardown;
    the final read while parked (ignored, not caught) is what proves SIG_IGN survives."""
    trees = tmp_path / "trees"
    (trees / "f2").mkdir(parents=True)
    (trees / "f2" / "manifest.json").write_text("{}")
    driver = tmp_path / "tail_driver.py"
    driver.write_text(_TAIL_DRIVER)
    env = dict(
        os.environ,
        PERF_TREES=str(trees),
        PERF_ARMS="f2",
        PERF_STORE="local",
        PERF_CPU_CAP="none",
        PERF_WORKLOADS="insert1k_single",
        PERF_REPS="insert1k_single=1",
        PERF_DSN_ENV="PERF_TAIL_DSN",
        PERF_TAIL_DSN="unused",
        PERF_OUT=str(tmp_path / "out.jsonl"),
    )
    proc = subprocess.Popen(
        [sys.executable, str(driver), str(AB_PATH), "teardown"],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
    )
    try:
        while (line := proc.stdout.readline()) and line.strip() != "returned":
            pass
        assert line, f"the driver exited early: {proc.wait()}"
        # Wait for teardown: SIGTERM is no longer caught by a Python handler.
        deadline = time.monotonic() + 30
        while sigterm_disposition(proc.pid)[0]:
            assert time.monotonic() < deadline, "teardown never reset the handler"
            time.sleep(0.01)
        proc.send_signal(signal.SIGTERM)
        try:
            returncode = proc.wait(timeout=0.5)
        except subprocess.TimeoutExpired:
            pass  # still parked: the signal was ignored
        else:
            pytest.fail(f"SIGTERM during teardown killed run: exit {returncode}")
        assert sigterm_disposition(proc.pid) == (False, True)
    finally:
        if proc.poll() is None:
            proc.kill()
        proc.wait()
    assert proc.returncode == -signal.SIGKILL


@pytest.mark.postgres
@pytest.mark.parametrize("direct", [False, True], ids=["forwarded", "forwarded+direct"])
def test_sigterm_to_run_stops_the_rep_cleanly_and_starts_no_other(tmp_path, private_dsn, direct):
    import signal

    proc, out, hold, trees = held_run(tmp_path, private_dsn)
    try:
        child = wait_for(hold)
        proc.send_signal(signal.SIGTERM)  # only `run` is signalled: it must forward
        if direct:
            # A unit stop signals every process: the rep also gets one directly, and a
            # second signal must not interrupt the cleanup the first one started.
            time.sleep(0.2)
            try:
                os.kill(child, signal.SIGTERM)
            except ProcessLookupError:
                pass
            proc.send_signal(signal.SIGTERM)
    finally:
        assert_stopped_clean(proc, out, trees, private_dsn, "signal", 143)


@pytest.mark.postgres
def test_the_deadline_stops_the_rep_cleanly_and_starts_no_other(tmp_path, private_dsn):
    started = time.monotonic()
    proc, out, hold, trees = held_run(tmp_path, private_dsn, PERF_DEADLINE_S=str(HOLD_DEADLINE_S))
    wait_for(hold)  # held before the deadline: the deadline, not a signal, ends the hold
    assert_stopped_clean(proc, out, trees, private_dsn, "deadline", 124)
    assert time.monotonic() - started < HOLD_DEADLINE_S + 30


@pytest.mark.postgres
def test_a_rep_stuck_in_cleanup_is_killed_after_the_grace(tmp_path, private_dsn):
    import signal

    proc, out, hold, trees = held_run(
        tmp_path, private_dsn, hold_at="cleanup", PERF_STOP_GRACE_S="2"
    )
    try:
        wait_for(hold)
        started = time.monotonic()
        proc.send_signal(signal.SIGTERM)  # ignored by the child: it is cleaning up
        returncode = proc.wait(timeout=30)
        elapsed = time.monotonic() - started
    finally:
        if proc.poll() is None:
            proc.kill()
    assert returncode == 143 and 2 <= elapsed < 10
    rows = [json.loads(line) for line in out.read_text().splitlines()]
    (child,) = [r for r in rows if r["kind"] == "child_done"]
    assert child["stopped"] == "signal" and child["killed"] is True
    assert not [r for r in rows if r["kind"] == "cleanup"]
    (done,) = [r for r in rows if r["kind"] == "done"]
    # Killed mid-cleanup: what it still held stays visible, never silently lost.
    assert len(done["journal_pending"]) == 2
    assert [s["name"] for s in done["skipped"]] == ["insert1k_batched"]


BACKSTOP = """
import importlib.util, pathlib, sys, time
spec = importlib.util.spec_from_file_location("ab", {ab!r})
ab = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ab)
ab.install_stop_handlers(time.time() - ab.CHILD_DEADLINE_SLACK_S + 0.3, 1.0)
try:
    time.sleep(30)
except ab.Stopped:
    pathlib.Path({marker!r}).touch()
ab.cleaning()
{hang}
"""


def backstop(tmp_path, hang, **env):
    marker = tmp_path / "stopped"
    code = BACKSTOP.format(ab=str(AB_PATH), marker=str(marker), hang=hang)
    started = time.monotonic()
    proc = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, timeout=60, env=dict(os.environ, **env)
    )
    return marker.exists(), proc.returncode, time.monotonic() - started


def test_a_rep_whose_parent_is_gone_stops_at_its_own_deadline(tmp_path):
    # A cleanup that never ends: the kernel's SIGALRM ends it a grace after the soft stop.
    stopped, returncode, elapsed = backstop(tmp_path, "time.sleep(30)")
    assert stopped and returncode == -signal.SIGALRM and elapsed < 5


@pytest.mark.postgres
def test_the_backstop_ends_a_rep_blocked_inside_a_postgres_call(tmp_path, pg_dsn):
    # Python handlers cannot run during a blocking libpq call; the hard stop must not
    # need one.
    # The DSN reaches the child through its environment, never its command line.
    hang = "import os, psycopg2\npsycopg2.connect(os.environ['PERF_TEST_DSN']).cursor().execute('SELECT pg_sleep(12)')"
    stopped, returncode, elapsed = backstop(tmp_path, hang, PERF_TEST_DSN=pg_dsn)
    assert stopped and returncode == -signal.SIGALRM and elapsed < 6


def test_the_hold_is_refused_outside_the_local_store(tmp_path):
    (tmp_path / "f2").mkdir()
    (tmp_path / "f2" / "manifest.json").write_text("{}")
    env = dict(
        os.environ,
        PERF_TREES=str(tmp_path),
        PERF_ARMS="f2",
        PERF_STORE="md",
        PERF_CPU_CAP="none",
        PERF_HOLD_AT="ingest",
        PERF_HOLD_FILE=str(tmp_path / "held"),
        CDC_TEST_PG_DSN="host=unused",
    )
    env.pop("motherduck_token", None)
    env.pop("MOTHERDUCK_TOKEN", None)
    proc = subprocess.run(
        [sys.executable, str(AB_PATH), "run"], env=env, capture_output=True, text=True
    )
    assert proc.returncode == 1 and "PERF_HOLD_AT is for tests" in proc.stderr


@pytest.mark.postgres
def test_a_rep_outliving_a_killed_run_stops_itself_after_the_deadline(tmp_path, private_dsn):
    import signal

    import psycopg2

    proc, out, hold, trees = held_run(
        tmp_path, private_dsn, PERF_DEADLINE_S=str(HOLD_DEADLINE_S), PERF_STOP_GRACE_S="5"
    )
    try:
        child = wait_for(hold)
        proc.kill()  # run dies outright: nothing will forward a stop to the rep
        proc.wait(timeout=10)
    finally:
        if proc.poll() is None:
            proc.kill()
    limit = time.monotonic() + HOLD_DEADLINE_S + ab.CHILD_DEADLINE_SLACK_S + 5 + 5
    while time.monotonic() < limit:
        try:
            os.kill(child, 0)
        except ProcessLookupError:
            break
        time.sleep(0.2)
    else:
        os.kill(child, signal.SIGKILL)
        pytest.fail("the orphaned rep kept running past its deadline and grace")
    # It stopped itself and cleaned up as for a forwarded stop.
    assert ab.pending(str(trees / "journal")) == []
    con = psycopg2.connect(private_dsn)
    try:
        with con.cursor() as cur:
            cur.execute(
                "SELECT (SELECT count(*) FROM pg_replication_slots WHERE database = current_database())"
                " + (SELECT count(*) FROM pg_namespace WHERE nspname LIKE 'cdc_p0_perf_%%')"
            )
            assert cur.fetchone()[0] == 0
    finally:
        con.close()
