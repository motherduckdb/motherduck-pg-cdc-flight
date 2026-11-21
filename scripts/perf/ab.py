"""Format A/B harness: one format-neutral driver for every arm's end-to-end reps.

Each rep runs in a fresh child process that holds exactly one archived product tree
(`ab.py prepare`), through a thin adapter `arm_<name>.py` beside this file. This file
never imports `cdc_flight`: every metric is defined here once, for every arm.

Commands:
  prepare ARM SHA DIR   `git archive SHA src` into DIR/ARM plus a digest manifest
  run                   the schedule from the environment (below); JSONL rows to stdout
                        and PERF_OUT
  summarize FILE        realised schedule, then median and min-max per arm and the
                        ratios against PERF_BASE_ARM (default f1)

Run configuration (environment; closed lists, validated):
  PERF_TREES            the directory `prepare` wrote; each arm is a subdirectory
  PERF_ARMS             comma list of arms (default "f1,f2")
  PERF_WORKLOADS        comma list from WORKLOADS (default all)
  PERF_REPS             reps per arm for each workload, "name=n,..." (default REPS)
  PERF_EVENTS_SCALE     float in (0, 1]: scales every event count (tests: tiny)
  PERF_STORE            md | local (default md). local: DuckDB files in PERF_STORE_DIR
  PERF_DSN_ENV          the variable holding the PostgreSQL DSN (default CDC_TEST_PG_DSN)
  PERF_CPU_CAP          quota | affinity | none (default quota)
  PERF_BUDGET_S         estimated-wall budget in seconds (default 1800)
  PERF_DEADLINE_S       hard wall-time cap from the start of `run` (default PERF_BUDGET_S):
                        the running rep is stopped, later reps are skipped (exit 124)
  PERF_STOP_GRACE_S     after a stop, seconds a rep gets to clean up before SIGKILL
                        (default 60). Worst-case wall is PERF_DEADLINE_S + this.
  PERF_JOURNAL_DIR      where resource journal JSONL goes (default PERF_TREES/journal)
  PERF_OUT              results JSONL (default: stdout only)

Stopping: SIGTERM or SIGINT to `run` is forwarded to the running rep, which unwinds
through its own cleanup (dropping its PostgreSQL objects and store databases); no later
rep starts (exit 143; 124 for the deadline; the first stop wins). The rep acts on the
first signal only, so a second one (a unit stop signals every process) never
interrupts its cleanup. A rep that has not exited PERF_STOP_GRACE_S after the stop is
killed, and what it held stays `allocated` in the journal (`done.journal_pending`). A
rep also stops itself shortly after the deadline if `run` is gone (killed outright).
Worst-case wall is PERF_DEADLINE_S plus PERF_STOP_GRACE_S: one long statement can
delay a rep's stop, never past the grace. A stop that arrives after the schedule finished,
but before `done` is written, still exits 143: `done.stopped` is set but `done.skipped`
is empty (stop received; the schedule may have finished). From `done` on, `run` ignores
SIGTERM and SIGINT, so a later stop can neither change nor kill the recorded outcome.

Tests only: PERF_HOLD_AT=<step> with PERF_HOLD_FILE=<path> makes the rep touch the file
and sleep on entering that step, a deterministic point to stop it at.

Security: no exception text, host, DSN or token is ever printed or stored, only the
exception type and the step. A MotherDuck store fails fast without a token.
"""

import hashlib
import importlib.util
import inspect
import json
import os
import platform
import re
import signal
import socket
import statistics
import struct
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path

HERE = Path(__file__).resolve().parent
WAL_LIMIT = 2 * 1024**3  # stop above 2 GB retained by any slot

# name: (op, width, events, per_txn). Pre-registered (format2-measurement-plan).
WORKLOADS = {
    "insert1k_batched": ("insert", 1024, 50_000, 1000),
    "insert1k_single": ("insert", 1024, 20_000, 1),
    "update1k_batched": ("update", 1024, 50_000, 1000),
    "insert10k_batched": ("insert", 10240, 20_000, 1000),
}
REPS = {"insert1k_batched": 3, "insert1k_single": 3, "update1k_batched": 3, "insert10k_batched": 2}
# Rough per-rep wall estimates (setup + generate + ingest + apply + oracle + storage),
# seconds per event plus a fixed part; labelled estimates, overridable (PERF_ESTIMATES).
ESTIMATE = {"fixed_s": 25.0, "per_event_s": {"insert": 0.0008, "update": 0.0016}, "per_kb": 0.6}
STEP = ["start"]
CAP_MODES = ("quota", "affinity", "none")
QUOTA = "200000 100000"  # cgroup v2 cpu.max for CPUQuota=200%


# Per-step peak RSS (Linux): when a step ends, its VmHWM is recorded and the high-water
# mark reset (clear_refs 5), so each step's peak is its own. No sampler thread. Where
# the reset is unavailable, no per-step peaks are recorded; peak_rss_kb is unaffected.
STEP_PEAKS = {}
_HWM_RESET = [None]  # None: untried; True/False after the first attempt


def _vm_hwm_kb():
    match = re.search(r"VmHWM:\s*(\d+)", read_text("/proc/self/status") or "")
    return int(match[1]) if match else None


def _close_step():
    if _HWM_RESET[0] is False:
        return
    peak = _vm_hwm_kb()
    if peak is None:
        _HWM_RESET[0] = False
        return
    try:
        with open("/proc/self/clear_refs", "w") as handle:
            handle.write("5")
    except OSError:
        _HWM_RESET[0] = False
        STEP_PEAKS.clear()  # a partial split would mislead; keep only the whole peak
        return
    _HWM_RESET[0] = True
    STEP_PEAKS[STEP[0]] = max(STEP_PEAKS.get(STEP[0], 0), peak)


def peak_rss_by_step_kb():
    """{step: peak RSS kb}, the current step included; {} where unsupported."""
    if not _HWM_RESET[0]:
        return {}
    peaks = dict(STEP_PEAKS)
    current = _vm_hwm_kb()
    if current is not None:
        peaks[STEP[0]] = max(peaks.get(STEP[0], 0), current)
    return peaks


def step(name):
    _close_step()
    STEP[0] = name
    if name == os.getenv("PERF_HOLD_AT"):  # tests only: a deterministic stop point
        Path(os.environ["PERF_HOLD_FILE"]).write_text(str(os.getpid()))
        end = time.monotonic() + 120
        while time.monotonic() < end:
            time.sleep(0.05)


# Stopping a rep ---------------------------------------------------------------------

STOPPING = {"raised": False, "cleaning": False}
CHILD_DEADLINE_SLACK_S = 5  # the parent stops a rep at the deadline; this is a backstop


class Stopped(BaseException):
    """A forwarded SIGTERM or SIGINT. A BaseException, like KeyboardInterrupt, so it
    unwinds the product's own cleanup and then the rep's `finally`."""


def on_stop(signum, frame):
    # Raise once; never inside the rep's cleanup, which must run to the end.
    if STOPPING["raised"] or STOPPING["cleaning"]:
        return
    STOPPING["raised"] = True
    raise Stopped(signal.Signals(signum).name)


def install_stop_handlers(deadline_at=None, grace=None):
    """SIGTERM/SIGINT raise Stopped once (on_stop). With a deadline, the rep also has its
    own backstop for when `run` is gone (the rep runs in its own session and scope, so a
    killed parent's stop never reaches it): a timer thread sends the rep SIGTERM just
    after the deadline (the soft stop: cleanup runs), and a grace later the kernel ends
    it with SIGALRM's default action. That hard stop is not a Python handler, so it works
    even while the rep is blocked inside a PostgreSQL or DuckDB call."""
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, on_stop)
    if deadline_at is not None:
        soft = max(0.01, deadline_at + CHILD_DEADLINE_SLACK_S - time.time())
        timer = threading.Timer(soft, os.kill, (os.getpid(), signal.SIGTERM))
        timer.daemon = True
        timer.start()
        signal.signal(signal.SIGALRM, signal.SIG_DFL)
        signal.setitimer(signal.ITIMER_REAL, soft + grace)


def cleaning():
    STOPPING["cleaning"] = True


def stop_child(proc, grace):
    """SIGTERM the child's process group; SIGKILL it if it has not exited after
    `grace` seconds. Returns (stdout, stderr, killed)."""
    for sig, wait in ((signal.SIGTERM, grace), (signal.SIGKILL, None)):
        try:
            os.killpg(proc.pid, sig)
        except ProcessLookupError:
            pass
        try:
            out, err = proc.communicate(timeout=wait)
            return out, err, sig == signal.SIGKILL
        except subprocess.TimeoutExpired:
            continue


def sha256_file(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def harness_digest():
    """The digest every rep records; summarize refuses a set that mixes harnesses."""
    return sha256_file(__file__)


# Statement timing and logical Arrow bytes ---------------------------------------


def sql_kind(sql):
    """A statement's kind: the verb and its target table, with day/partition suffixes
    folded (format 1 changes_<n>, format 2 *_<yyyymmdd>)."""
    text = " ".join(str(sql).split()[:16]).upper()
    verb = text.split(" ", 1)[0] if text else "?"
    if verb in ("BEGIN", "COMMIT", "ROLLBACK", "CHECKPOINT"):
        return verb.lower()
    match = re.search(r"\b(?:INTO|UPDATE|FROM|TABLE)\s+([\w\".]+)", text)
    target = match.group(1).replace('"', "").split(".")[-1].lower() if match else "?"
    target = re.sub(r"changes_\d+$", "changes_d", target)
    target = re.sub(r"_\d{8}$", "_d", target)
    return f"{verb.lower()}:{target}"


def arrow_nbytes(obj):
    """Logical in-memory bytes of an Arrow table or batch; None for anything else."""
    try:
        import pyarrow as pa
    except ImportError:  # pragma: no cover
        return None
    if isinstance(obj, (pa.Table, pa.RecordBatch)):
        return int(obj.nbytes)
    return None


def _materialized(obj):
    """A RecordBatchReader can only be sized by reading it: read it once into a table
    and hand DuckDB the table (same rows, same schema)."""
    try:
        import pyarrow as pa
    except ImportError:  # pragma: no cover
        return obj
    if isinstance(obj, pa.RecordBatchReader):
        return obj.read_all()
    return obj


class TimingProxy:
    """Wraps a product connection: count and wall per statement kind (BEGIN/COMMIT
    included; fetch time is not), per thread role, and the logical Arrow bytes handed
    to DuckDB on every path: register(), from_arrow(), Arrow parameters to execute(),
    and replacement scans (an Arrow object in the caller's locals named in the SQL).

    A replacement scan through the proxy fails loudly (DuckDB looks in the proxy's
    frame, not the product's), so it can't be silently uncounted either."""

    def __init__(self, con):
        self._con = con
        self.lock = threading.Lock()
        self.reset()

    def reset(self):
        with self.lock:
            self.stats, self.by_role = {}, {}
            self.arrow = {"register": 0, "from_arrow": 0, "parameter": 0, "replacement": 0}
            self.arrow_objects = 0

    def _arrow(self, path, nbytes):
        if nbytes is not None:
            with self.lock:
                self.arrow[path] += nbytes
                self.arrow_objects += 1

    def _timed(self, kind, call):
        started = time.perf_counter()
        try:
            return call()
        finally:
            elapsed = time.perf_counter() - started
            role = "main" if threading.current_thread() is threading.main_thread() else "worker"
            with self.lock:
                for table, key in ((self.stats, kind), (self.by_role, f"{role}:{kind}")):
                    entry = table.setdefault(key, [0, 0.0])
                    entry[0] += 1
                    entry[1] += elapsed

    def _scan_locals(self, sql, frame):
        names = set(re.findall(r"[A-Za-z_]\w*", str(sql)))
        seen = set()
        while frame is not None and len(seen) < 3:  # the caller and two levels above
            for name, value in frame.f_locals.items():
                if name in names and id(value) not in seen:
                    nbytes = arrow_nbytes(value)
                    if nbytes is not None:
                        seen.add(id(value))
                        self._arrow("replacement", nbytes)
            frame = frame.f_back
            seen.add(None)

    def execute(self, sql, *args):
        if args and isinstance(args[0], (list, tuple)):
            for value in args[0]:
                self._arrow("parameter", arrow_nbytes(value))
        frame = inspect.currentframe().f_back
        try:
            self._scan_locals(sql, frame)
        finally:
            del frame
        return self._timed(sql_kind(sql), lambda: self._con.execute(sql, *args))

    def sql(self, sql, *args, **kwargs):
        frame = inspect.currentframe().f_back
        try:
            self._scan_locals(sql, frame)
        finally:
            del frame
        return self._timed(sql_kind(sql), lambda: self._con.sql(sql, *args, **kwargs))

    def register(self, name, obj, *args):
        obj = _materialized(obj)
        self._arrow("register", arrow_nbytes(obj))
        return self._timed("register", lambda: self._con.register(name, obj, *args))

    def unregister(self, *args):
        return self._timed("unregister", lambda: self._con.unregister(*args))

    def from_arrow(self, obj, *args, **kwargs):
        obj = _materialized(obj)
        self._arrow("from_arrow", arrow_nbytes(obj))
        return self._timed("from_arrow", lambda: self._con.from_arrow(obj, *args, **kwargs))

    def __getattr__(self, name):
        return getattr(self._con, name)

    def summary(self):
        with self.lock:
            return dict(
                statements={k: [n, round(t, 4)] for k, (n, t) in sorted(self.stats.items())},
                statements_by_role={
                    k: [n, round(t, 4)] for k, (n, t) in sorted(self.by_role.items())
                },
                arrow_bytes=sum(self.arrow.values()),
                arrow_bytes_by_path=dict(self.arrow),
                arrow_objects=self.arrow_objects,
            )


class Hooks:
    """Exclusive wall and thread-CPU time per label on the arm's own methods. A hooked
    call nested in another (on the same thread) is subtracted from its parent, so the
    labels add up without double counting."""

    def __init__(self):
        self.local = threading.local()
        self.lock = threading.Lock()
        self.installed = []
        self.reset()

    def reset(self):
        with self.lock:
            self.totals = {}  # label -> [calls, wall, cpu]

    def install(self, owner, attribute, label):
        original = getattr(owner, attribute)
        hooks = self

        def timed(*args, **kwargs):
            stack = getattr(hooks.local, "stack", None)
            if stack is None:
                stack = hooks.local.stack = []
            frame = [0.0, 0.0]  # child wall, child cpu
            stack.append(frame)
            wall, cpu = time.perf_counter(), time.thread_time()
            try:
                return original(*args, **kwargs)
            finally:
                wall, cpu = time.perf_counter() - wall, time.thread_time() - cpu
                stack.pop()
                if stack:
                    stack[-1][0] += wall
                    stack[-1][1] += cpu
                with hooks.lock:
                    entry = hooks.totals.setdefault(label, [0, 0.0, 0.0])
                    entry[0] += 1
                    entry[1] += wall - frame[0]
                    entry[2] += cpu - frame[1]

        timed.__wrapped__ = original
        setattr(owner, attribute, timed)
        self.installed.append((owner, attribute, label))

    def summary(self):
        with self.lock:
            return {
                k: [n, round(w, 4), round(c, 4)] for k, (n, w, c) in sorted(self.totals.items())
            }


# Host: network, CPU cap, load ------------------------------------------------------


def parse_routes(text):
    """/proc/net/route -> [(iface, dest, mask)] as the kernel prints them."""
    routes = []
    for line in text.splitlines()[1:]:
        fields = line.split()
        if len(fields) < 8:
            continue
        # Printed as the kernel's raw network-order word read in host (little-endian) order;
        # route_interface packs addresses the same way.
        dest, mask = int(fields[1], 16), int(fields[7], 16)
        routes.append((fields[0], dest, mask))
    return routes


def default_interface(text):
    for iface, dest, mask in parse_routes(text):
        if dest == 0 and mask == 0:
            return iface
    return None


def route_interface(text, address):
    """The interface `address` (IPv4 text) leaves by: the longest matching prefix."""
    try:
        ip = struct.unpack("<I", socket.inet_aton(address))[0]
    except OSError:
        return None, False
    best = None
    for iface, dest, mask in parse_routes(text):
        if ip & mask == dest and (best is None or bin(mask).count("1") > bin(best[2]).count("1")):
            best = (iface, dest, mask)
    if best is None:
        return None, False
    return best[0], best[2] == 0


def netdev_bytes(text, iface):
    """(rx, tx) bytes of one interface from /proc/net/dev text."""
    for line in text.splitlines()[2:]:
        name, _, data = line.partition(":")
        if name.strip() == iface:
            fields = data.split()
            return int(fields[0]), int(fields[8])
    return None


def read_text(path):
    try:
        return Path(path).read_text()
    except OSError:
        return None


def dsn_host(dsn):
    match = re.search(r"(?:^|\s)host=(\S+)", dsn) or re.search(r"@([^:/?]+)", dsn)
    return match.group(1) if match else "localhost"


def source_local(dsn):
    """The product's allow_local (no TLS, loopback allowed) follows the *source*: a
    loopback or socket host, or PERF_ALLOW_LOCAL_SOURCE=1 for a local container on a
    bridge address. A remote source (RDS) keeps the product's TLS and host rules."""
    host = dsn_host(dsn)
    return (
        host in ("localhost", "::1")
        or host.startswith(("127.", "/"))
        or os.getenv("PERF_ALLOW_LOCAL_SOURCE") == "1"
    )


def wire_plan(dsn):
    """Which interface counts as the destination's wire, or why there is none."""
    routes = read_text("/proc/net/route")
    if routes is None:
        return dict(iface=None, reason="no_proc_net_route")
    iface = default_interface(routes)
    if iface is None:
        return dict(iface=None, reason="no_default_route")
    host = dsn_host(dsn)
    if host.startswith("/"):
        return dict(iface=iface, source_route="socket")  # a Unix socket never hits a NIC
    try:
        address = socket.getaddrinfo(host, None, socket.AF_INET)[0][4][0]
    except OSError:
        return dict(iface=None, reason="source_unresolved")
    if address.startswith("127."):
        return dict(iface=iface, source_route="loopback")
    source_iface, via_default = route_interface(routes, address)
    if source_iface == iface:
        # RDS or any remote source: its replication stream shares the interface.
        return dict(iface=None, reason="source_on_default_route")
    return dict(iface=iface, source_route="other_interface" if not via_default else "default")


def own_cgroup():
    text = read_text("/proc/self/cgroup") or ""
    for line in text.splitlines():
        if line.startswith("0::"):
            return Path("/sys/fs/cgroup") / line[3:].lstrip("/")
    return None


def cgroup_cpu(path):
    if path is None:
        return None
    stat, quota = read_text(path / "cpu.stat"), read_text(path / "cpu.max")
    if stat is None:
        return None
    values = dict(line.split() for line in stat.splitlines() if len(line.split()) == 2)
    keys = ("usage_usec", "nr_periods", "nr_throttled", "throttled_usec")
    out = {k: int(values[k]) for k in keys if k in values}
    out["cpu_max"] = quota.strip() if quota else None
    return out


def cap_check(mode, nproc, cpu_max, affinity):
    """None when the cap holds, else the refusal reason."""
    if mode not in CAP_MODES:
        return "unknown_cap_mode"
    if mode == "none":
        return None
    if nproc < 4:
        return "nproc_below_4"  # the 2-vCPU cap would not be what binds
    if mode == "quota" and cpu_max != QUOTA:
        return "quota_not_applied"
    if mode == "affinity" and affinity != 2:
        return "affinity_not_applied"
    return None


def cap_command(mode, argv):
    if mode == "quota":
        return ["systemd-run", "--user", "--scope", "--quiet", "-p", "CPUQuota=200%", *argv]
    return list(argv)


def loadavg():
    try:
        return [round(x, 2) for x in os.getloadavg()]
    except OSError:
        return None


def cpu_model():
    text = read_text("/proc/cpuinfo") or ""
    match = re.search(r"model name\s*:\s*(.+)", text)
    if match:
        return match.group(1).strip()
    try:
        return (
            subprocess.run(
                ["sysctl", "-n", "machdep.cpu.brand_string"],
                capture_output=True,
                text=True,
                timeout=5,
            ).stdout.strip()
            or platform.processor()
        )
    except (OSError, subprocess.SubprocessError):
        return platform.processor()


def peak_rss_kb():
    """The whole process's peak RSS so far (the per-step resets don't lower it)."""
    current = _vm_hwm_kb()
    if current is not None:
        return max([current, *STEP_PEAKS.values()])
    import resource

    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return peak // 1024 if platform.system() == "Darwin" else peak


# Oracle: the same arithmetic in both engines ----------------------------------------

# Per row md5(id|md5(v)|n); four 32-bit quarters of it, each summed exactly (NUMERIC in
# PostgreSQL, HUGEINT in DuckDB), plus the row count. Order independent and portable.
_QUARTERS = range(4)
PG_HASH = (
    "SELECT count(*),"
    + ",".join(
        f"coalesce(sum(('x'||substr(h,{1 + 8 * i},8))::bit(32)::bigint),0)" for i in _QUARTERS
    )
    + " FROM (SELECT md5(id::text||'|'||md5(v)||'|'||n::text) h FROM {table}) s"
)
DUCK_HASH = (
    "SELECT count(*),"
    + ",".join(
        f"coalesce(sum(('0x'||substr(h,{1 + 8 * i},8))::UBIGINT::HUGEINT),0)" for i in _QUARTERS
    )
    + " FROM (SELECT md5(id::VARCHAR||'|'||md5(v)||'|'||n::VARCHAR) h FROM {table}) s"
)


def content_hash(row):
    return [int(x) for x in row]


def expected_ops(op, events, per_txn):
    """What the source table's log must hold: (ops by kind, source txns)."""
    if op == "insert":
        return {"insert": events}, -(-events // per_txn)
    preload = -(-events // 50_000)
    return {"insert": events, "update": events}, preload + -(-events // per_txn)


def oracle(source_hash, mirror_hash, ops, txns, op, events, per_txn):
    want_ops, want_txns = expected_ops(op, events, per_txn)
    out = dict(
        source_hash=source_hash,
        mirror_hash=mirror_hash,
        content_ok=source_hash == mirror_hash and source_hash[0] == events,
        ops=ops,
        ops_expected=want_ops,
        ops_ok={k: v for k, v in ops.items() if v} == want_ops,
        txns=txns,
        txns_expected=want_txns,
        txns_ok=txns == want_txns,
    )
    out["ok"] = out["content_ok"] and out["ops_ok"] and out["txns_ok"]
    return out


# Payloads and the PostgreSQL side ---------------------------------------------------


def pg_payload(width, row, seed):
    """A deterministic, incompressible value of `width` base64 characters.

    Core sha512 (PostgreSQL 11+), never pgcrypto: a rep must not install an
    extension, a database-wide change other tests on the same DSN would see."""
    blocks = (width * 3 // 4) // 64
    return (
        "encode((SELECT string_agg(sha512(convert_to("
        f"{row}::text||':'||({seed})::text||':'||k,'UTF8')),"
        f"''::bytea ORDER BY k) FROM generate_series(1,{blocks}) k),'base64')"
    )


def wal_guard(cur):
    cur.execute(
        "SELECT coalesce(max(pg_wal_lsn_diff(pg_current_wal_lsn(),restart_lsn)),0) "
        "FROM pg_replication_slots"
    )
    if int(cur.fetchone()[0]) > WAL_LIMIT:
        raise RuntimeError("wal_guard")


def drop_pg(cur, name):
    for attempt in range(40):
        cur.execute(
            "SELECT active_pid FROM pg_replication_slots WHERE slot_name=%s AND active", [name]
        )
        active = [pid for (pid,) in cur.fetchall()]
        if not active:
            break
        if attempt == 10:  # a walsender left behind by a stopped or failed run
            for pid in active:
                cur.execute("SELECT pg_terminate_backend(%s)", [pid])
        time.sleep(0.5)
    cur.execute(
        "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
        "WHERE slot_name=%s AND NOT active",
        [name],
    )
    cur.execute(f"DROP PUBLICATION IF EXISTS {name}")
    cur.execute(f"DROP SCHEMA IF EXISTS {name} CASCADE")
    cur.execute("SELECT count(*) FROM pg_replication_slots WHERE slot_name=%s", [name])
    return not cur.fetchone()[0]


def generate(cur, table, op, width, first, last, per_txn, *, seed="0"):
    payload = pg_payload(width, "i", seed)
    if per_txn == 1:
        cur.execute(
            f"DO $$ BEGIN FOR i IN {int(first)}..{int(last)} LOOP "
            f"INSERT INTO {table} VALUES (i,{payload},0); COMMIT; END LOOP; END $$"
        )
        return
    for start in range(first, last + 1, per_txn):
        end = min(last, start + per_txn - 1)
        if op == "insert":
            cur.execute(
                f"INSERT INTO {table} SELECT i,{payload},0 FROM generate_series(%s,%s) i",
                [start, end],
            )
        else:
            cur.execute(
                f"UPDATE {table} SET v={pg_payload(width, 'id', 'n+1')},n=n+1 "
                "WHERE id BETWEEN %s AND %s",
                [start, end],
            )
        if (start // per_txn) % 20 == 0:
            wal_guard(cur)


# Destination stores and the resource journal ----------------------------------------


class Journal:
    """md_suite's format: {kind, name, event} JSONL; allocated before CREATE, released
    only after a verified absence."""

    def __init__(self, directory, kind):
        self.path = Path(directory) / f"perf-{os.getpid()}-{uuid.uuid4().hex[:8]}.jsonl"
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.kind = kind

    def record(self, name, event):
        with self.path.open("a") as out:
            out.write(json.dumps(dict(kind=self.kind, name=name, event=event)) + "\n")
            out.flush()
            os.fsync(out.fileno())


def pending(directory):
    """Every journalled resource whose latest event is not `released`."""
    latest = {}
    for path in sorted(Path(directory).glob("*.jsonl")):
        for index, line in enumerate(path.read_text().splitlines()):
            try:
                row = json.loads(line)
                latest[row["kind"], row["name"]] = row["event"]
            except (ValueError, KeyError):
                latest["incomplete_journal", f"{path.name}:{index}"] = "allocated"
    return sorted(f"{k}:{n}" for (k, n), state in latest.items() if state != "released")


def require_token():
    if not (os.getenv("MOTHERDUCK_TOKEN") or os.getenv("motherduck_token")):
        raise SystemExit("MOTHERDUCK_TOKEN (or motherduck_token) is not set; refusing md:")


class Store:
    def __init__(self, kind, journal, directory=None):
        import duckdb

        self.duckdb, self.kind, self.journal = duckdb, kind, journal
        self.directory = Path(directory) if directory else None
        self.admin = None
        if kind == "md":
            require_token()
            self.admin = duckdb.connect("md:")

    def path(self, name):
        return self.directory / f"{name}.duckdb"

    def create(self, name):
        self.journal.record(name, "allocated")
        if self.kind == "md":
            self.admin.execute(f"CREATE DATABASE {name}")
        else:
            if self.path(name).exists():
                raise RuntimeError("local_store_exists")
            self.duckdb.connect(str(self.path(name))).close()

    def connect(self, name):
        if self.kind == "md":
            return self.duckdb.connect(f"md:{name}?attach_mode=single")
        return self.duckdb.connect(str(self.path(name)))

    def attach(self, con, name):
        target = f"md:{name}" if self.kind == "md" else str(self.path(name))
        con.execute(f"ATTACH IF NOT EXISTS '{target}' AS {name}")

    def present(self, names):
        if self.kind == "md":
            return sorted(
                r[0]
                for r in self.admin.execute(
                    "SELECT name FROM md_information_schema.databases WHERE name IN "
                    "(SELECT unnest(?::VARCHAR[]))",
                    [list(names)],
                ).fetchall()
            )
        return sorted(n for n in names if self.path(n).exists())

    def drop(self, names):
        failed = []
        for name in names:
            try:
                if self.kind == "md":
                    self.admin.execute(f"DROP DATABASE IF EXISTS {name}")
                else:
                    for path in (self.path(name), Path(str(self.path(name)) + ".wal")):
                        path.unlink(missing_ok=True)
            except Exception as exc:
                failed.append([name, type(exc).__name__])
        left = self.present(names)
        for name in names:
            if name not in left:
                self.journal.record(name, "released")
        return left, failed

    def close(self):
        if self.admin is not None:
            self.admin.close()


# Storage: a bounded local copy of the log database ----------------------------------


def copy_bytes(con, database, cap=200_000_000):
    """The log database copied into a fresh local DuckDB file: used bytes with the rows
    minus with the tables empty. A 2,000-row probe sizes a sample to `cap` bytes; the
    sample is scaled by total rows over rows copied (append-only, uniform rows)."""
    import shutil
    import tempfile

    tables = [
        (
            schema,
            table,
            con.execute(f'SELECT count(*) FROM {database}."{schema}"."{table}"').fetchone()[0],
        )
        for schema, table in con.execute(
            "SELECT schema_name,table_name FROM duckdb_tables() WHERE database_name=?", [database]
        ).fetchall()
    ]
    total = sum(rows for *_, rows in tables)
    if not total:
        return dict(rows=0, bytes=0, fraction=1.0)

    def measure(limit_of):
        with tempfile.TemporaryDirectory(prefix="cdc_perf_size_") as directory:
            con.execute(f"ATTACH '{Path(directory) / 'copy.duckdb'}' AS perf_size_copy")
            try:

                def used():
                    con.execute("CHECKPOINT perf_size_copy")
                    blocks, block = con.execute(
                        "SELECT used_blocks,block_size FROM pragma_database_size() "
                        "WHERE database_name='perf_size_copy'"
                    ).fetchone()
                    return blocks * block

                for schema, table, _ in tables:
                    con.execute(
                        f'CREATE TABLE perf_size_copy."{schema}__{table}" AS '
                        f'SELECT * FROM {database}."{schema}"."{table}" LIMIT 0'
                    )
                empty, copied = used(), 0
                for schema, table, rows in tables:
                    n = min(rows, limit_of(rows))
                    con.execute(
                        f'INSERT INTO perf_size_copy."{schema}__{table}" '
                        f'SELECT * FROM {database}."{schema}"."{table}" LIMIT {n}'
                    )
                    copied += n
                return used() - empty, copied
            finally:
                con.execute("DETACH perf_size_copy")

    probe, probed = measure(lambda rows: 2000)
    estimate = max(1, probe) / probed * total
    free = shutil.disk_usage(tempfile.gettempdir()).free
    if free < 2 * min(cap, estimate):
        return dict(rows=total, bytes=None, skipped="temp_free_below_2x_copy")
    fraction = min(1.0, cap / estimate)
    size, copied = measure(lambda rows: max(1, int(rows * fraction + 0.5)))
    return dict(
        rows=total,
        bytes=round(size * total / copied),
        rows_copied=copied,
        fraction=round(copied / total, 4),
    )


def table_rows(con, database, schema):
    return {
        table: con.execute(f'SELECT count(*) FROM "{database}"."{schema}"."{table}"').fetchone()[0]
        for (table,) in con.execute(
            "SELECT table_name FROM duckdb_tables() WHERE database_name=? AND schema_name=? "
            "ORDER BY 1",
            [database, schema],
        ).fetchall()
    }


# Trees and adapters -----------------------------------------------------------------


def tree_digest(root):
    """sha256 over every file under root/src (sorted relative path + content digest)."""
    root = Path(root) / "src"
    digest = hashlib.sha256()
    for path in sorted(p for p in root.rglob("*") if p.is_file() and "__pycache__" not in p.parts):
        digest.update(f"{path.relative_to(root)}\0{sha256_file(path)}\n".encode())
    return digest.hexdigest()


def prepare(arm, sha, directory, repo=None):
    repo = Path(repo or HERE.parents[1])
    target = Path(directory) / arm
    if target.exists():
        raise SystemExit(f"{target} exists; prepare into a new directory")
    target.mkdir(parents=True)
    archive = subprocess.run(
        ["git", "-C", str(repo), "archive", sha, "src"], capture_output=True, check=True
    ).stdout
    commit = (
        subprocess.run(["git", "get-tar-commit-id"], input=archive, capture_output=True, check=True)
        .stdout.decode()
        .strip()
    )
    subprocess.run(["tar", "-x", "-C", str(target)], input=archive, check=True)
    manifest = dict(arm=arm, commit=commit, tree_sha256=tree_digest(target))
    (target / "manifest.json").write_text(json.dumps(manifest, indent=1) + "\n")
    return manifest


def load_arm(arm, tree):
    """In the child: put the arm's tree first, import its adapter, prove the product
    came from that tree (path and digest). Returns (adapter, identity)."""
    if not re.fullmatch(r"[a-z][a-z0-9]*", arm):
        raise SystemExit("bad arm name")
    tree = Path(tree).resolve()
    manifest = json.loads((tree / "manifest.json").read_text())
    if manifest.get("arm") != arm:
        raise RuntimeError("tree_arm_mismatch")
    if tree_digest(tree) != manifest["tree_sha256"]:
        raise RuntimeError("tree_digest_mismatch")
    if "cdc_flight" in sys.modules:
        raise RuntimeError("product_imported_before_tree")
    sys.path.insert(0, str(tree / "src"))
    adapter_path = HERE / f"arm_{arm}.py"
    if not adapter_path.exists():
        raise SystemExit(
            f"no adapter arm_{arm}.py here"
            + (" (the f1 adapter lives on the perf-f1-arm branch)" if arm == "f1" else "")
        )
    spec = importlib.util.spec_from_file_location(f"arm_{arm}", adapter_path)
    adapter = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(adapter)
    import cdc_flight

    package = Path(cdc_flight.__file__).resolve()
    if tree / "src" not in package.parents:
        raise RuntimeError("product_not_from_tree")
    return adapter, dict(
        arm=arm,
        product_commit=manifest["commit"],
        product_tree_sha256=manifest["tree_sha256"],
        adapter_sha256=sha256_file(adapter_path),
        harness_sha256=harness_digest(),
    )


# One rep (the child) ----------------------------------------------------------------


def apply_until_caught_up(adapter, apply, table, label, limit=50):
    calls, events, status = 0, 0, None
    while calls < limit:
        calls += 1
        result = adapter.apply_table(apply, table, f"{label}{calls}")
        events += result["events"]
        status = result["status"]
        if status != "caught_up" or not result["events"]:
            break
    return dict(calls=calls, events=events, status=status)


def rep(spec, emit):
    """One end-to-end repetition through a real slot. `spec` comes from the parent."""
    import psycopg2

    arm, (op, width, events, per_txn) = spec["arm"], spec["workload"]
    mode = spec["cap"]
    if mode == "affinity" and hasattr(os, "sched_setaffinity"):
        os.sched_setaffinity(0, sorted(os.sched_getaffinity(0))[:2])
    adapter, identity = load_arm(arm, Path(spec["trees"]) / arm)
    group = own_cgroup()
    cpu_first = cgroup_cpu(group)
    affinity = len(os.sched_getaffinity(0)) if hasattr(os, "sched_getaffinity") else None
    refusal = cap_check(mode, os.cpu_count() or 0, (cpu_first or {}).get("cpu_max"), affinity)
    if refusal:
        raise SystemExit(f"cpu cap refused: {refusal}")
    dsn = os.environ[spec["dsn_env"]]
    wire = wire_plan(dsn)
    name = "cdc_p0_perf_" + uuid.uuid4().hex[:10]
    mirror, table = name + "_m", f"{name}.t"
    out = dict(
        spec_id=spec["id"],
        workload=spec["name"],
        op=op,
        width=width,
        events=events,
        per_txn=per_txn,
        window_txns=-(-events // per_txn),
        round=spec["round"],
        position=spec["position"],
        store=spec["store"],
        cap=dict(
            mode=mode,
            cpu_max=(cpu_first or {}).get("cpu_max"),
            affinity=affinity,
            nproc=os.cpu_count(),
        ),
        host=dict(cpu_model=cpu_model(), python=platform.python_version(), load_start=loadavg()),
        **identity,
    )
    journal = Journal(spec["journal_dir"], "motherduck" if spec["store"] == "md" else "local")
    store = Store(spec["store"], journal, spec.get("store_dir"))
    pg = psycopg2.connect(dsn)
    pg.autocommit = True
    cur = pg.cursor()
    con, created = None, []
    try:
        step("pg_setup")
        wal_guard(cur)
        cur.execute(f"CREATE SCHEMA {name}")
        cur.execute(f"CREATE TABLE {table} (id bigint PRIMARY KEY, v text, n int)")
        cur.execute(f"ALTER TABLE {table} REPLICA IDENTITY FULL")
        cur.execute(f"ALTER TABLE {table} ALTER COLUMN v SET STORAGE EXTERNAL")
        cur.execute("SELECT %s::regclass::oid", [table])
        oid = int(cur.fetchone()[0])
        out["pg_major"] = pg.server_version // 10000
        out["source_local"] = source_local(dsn)
        step("store_setup")
        for database in (name, mirror):
            store.create(database)
            created.append(database)
        con = store.connect(name)
        store.attach(con, mirror)
        con.execute("SET TimeZone='UTC'")
        proxy = TimingProxy(con)
        config = dict(
            source_id=name,
            slot=name,
            publication=name,
            tables=(table,),
            heartbeat=f"{name}.pulse",
            database=name,
            dsn=dsn,
            allow_local=source_local(dsn),
            runtime_seconds=600,
        )
        hooks = Hooks()
        for owner, attribute, label in adapter.hooks():
            hooks.install(owner, attribute, label)
        step("bootstrap")
        started = time.perf_counter()
        log, info = adapter.open(proxy, config)
        out["bootstrap_s"] = round(time.perf_counter() - started, 3)
        apply_proxy = TimingProxy(con)
        apply = adapter.apply_setup(apply_proxy, config, mirror, table)
        if op == "update":
            step("preload")
            started = time.perf_counter()
            for first in range(1, events + 1, 50_000):
                generate(cur, table, "insert", width, first, min(events, first + 49_999), 50_000)
            adapter.run(log, info, {})
            out["preload_apply"] = apply_until_caught_up(adapter, apply, table, "preload")
            out["preload_s"] = round(time.perf_counter() - started, 2)
        step("generate")
        started = time.perf_counter()
        generate(cur, table, op, width, 1, events, per_txn)
        out["generate_s"] = round(time.perf_counter() - started, 2)
        wal_guard(cur)
        step("ingest")
        proxy.reset()
        hooks.reset()
        net_before = (
            netdev_bytes(read_text("/proc/net/dev") or "", wire["iface"])
            if wire.get("iface")
            else None
        )
        cg_before = cgroup_cpu(group)
        observe = {}
        cpu, wall = time.process_time(), time.perf_counter()
        result = adapter.run(log, info, observe)
        ingest_wall, ingest_cpu = time.perf_counter() - wall, time.process_time() - cpu
        net_mid = (
            netdev_bytes(read_text("/proc/net/dev") or "", wire["iface"])
            if wire.get("iface")
            else None
        )
        cg_mid = cgroup_cpu(group)
        out["ingest"] = dict(
            status=result["status"],
            transactions=result["transactions"],
            wall_s=round(ingest_wall, 3),
            process_cpu_s=round(ingest_cpu, 3),
            **proxy.summary(),
            hooks=hooks.summary(),
            native=result.get("native", {}),
        )
        step("apply")
        hooks_ingest = hooks.summary()
        hooks.reset()
        apply_proxy.reset()
        if isinstance(getattr(apply, "fold_stats", None), dict):
            apply.fold_stats.clear()  # count the measured apply's batches only
        cpu, wall = time.process_time(), time.perf_counter()
        applied = apply_until_caught_up(adapter, apply, table, "rep")
        apply_wall, apply_cpu = time.perf_counter() - wall, time.process_time() - cpu
        net_end = (
            netdev_bytes(read_text("/proc/net/dev") or "", wire["iface"])
            if wire.get("iface")
            else None
        )
        cg_end = cgroup_cpu(group)
        out["apply"] = dict(
            applied,
            wall_s=round(apply_wall, 3),
            process_cpu_s=round(apply_cpu, 3),
            **{k: v for k, v in apply_proxy.summary().items() if k.startswith("statements")},
            hooks=hooks.summary(),
        )
        # ApplyRun's fold path per batch ("sql" / "fallback:<reason>"); absent on arms
        # that predate the SQL fold.
        if isinstance(getattr(apply, "fold_stats", None), dict):
            out["apply"]["fold"] = getattr(apply, "fold", None)
            out["apply"]["fold_stats"] = dict(apply.fold_stats)
        out["ingest"]["hooks"] = hooks_ingest
        out["wire"] = dict(wire)
        if net_before and net_mid and net_end:
            out["wire"].update(
                ingest_rx=net_mid[0] - net_before[0],
                ingest_tx=net_mid[1] - net_before[1],
                apply_rx=net_end[0] - net_mid[0],
                apply_tx=net_end[1] - net_mid[1],
            )
        if cg_before and cg_mid and cg_end:
            delta = lambda a, b: {k: b[k] - a[k] for k in a if k != "cpu_max" and k in b}  # noqa: E731
            out["cgroup"] = dict(ingest=delta(cg_before, cg_mid), apply=delta(cg_mid, cg_end))
        step("oracle")
        cur.execute(PG_HASH.format(table=table))
        source_hash = content_hash(cur.fetchone())
        mirror_hash = content_hash(
            con.execute(DUCK_HASH.format(table=f'"{mirror}"."{name}"."t"')).fetchone()
        )
        ops, txns = adapter.op_counts(con, log, oid)
        out["oracle"] = oracle(source_hash, mirror_hash, ops, txns, op, events, per_txn)
        out["oracle"]["log_check"] = adapter.log_check(con, log)
        out["oracle"]["ok"] = out["oracle"]["ok"] and out["oracle"]["log_check"]["ok"]
        step("storage")
        rows = table_rows(con, name, name)
        payload = [t for t in rows if adapter.is_payload(t)]
        out["log_tables"] = rows
        out["ledger_rows"] = sum(n for t, n in rows.items() if t in adapter.LEDGER_TABLES)
        out["ledger_tables"] = sorted(t for t in rows if t in adapter.LEDGER_TABLES)
        out["payload_tables"] = len(payload)
        out["log_storage"] = copy_bytes(con, name)
        out["log_events"] = sum(ops.values())
        out["peak_rss_kb"] = peak_rss_kb()
        out["peak_rss_by_step_kb"] = peak_rss_by_step_kb()
        out["host"]["load_end"] = loadavg()
        out["ok"] = bool(out["oracle"]["ok"])
    except BaseException as exc:
        out.update(error=type(exc).__name__, step=STEP[0], ok=False)
        if os.getenv("PERF_DEBUG") == "1":
            raise
    finally:
        cleaning()  # from here on a stop signal is ignored: cleanup runs to the end
        emit("rep", **out)
        step("cleanup")
        if con is not None:
            con.close()
        try:
            pg_clean = drop_pg(cur, name)
        except Exception as exc:
            pg_clean = type(exc).__name__
        pg.close()
        left, failed = store.drop(list(reversed(created)))
        emit("cleanup", spec_id=spec["id"], pg_clean=pg_clean, store_left=left, store_failed=failed)
        store.close()
    return 0 if out.get("ok") else 1


# Schedule, budget and the parent ----------------------------------------------------


def estimate_s(workload, estimate=ESTIMATE):
    op, width, events, _ = workload
    per_event = estimate["per_event_s"][op] * max(1.0, width / 1024 * estimate["per_kb"])
    passes = 2 if op == "update" else 1
    return estimate["fixed_s"] + per_event * events * passes


def plan(arms, workloads, reps, budget_s, estimate=ESTIMATE):
    """The schedule: rounds; in each, every workload that still has reps (rotated by the
    round), arms in an order rotated by round and workload. 10 KB reps drop first if
    the estimate is over budget. Returns (schedule, dropped, estimate_s)."""
    reps = dict(reps)
    dropped = []

    def total():
        return sum(estimate_s(workloads[w], estimate) * reps[w] * len(arms) for w in reps)

    while total() > budget_s:
        wide = [w for w in reps if workloads[w][1] > 1024 and reps[w] > 0]
        if not wide:
            break
        reps[wide[0]] -= 1
        dropped.append(wide[0])
    if total() > budget_s:
        raise SystemExit(f"estimate {total():.0f}s over budget {budget_s}s after drops")
    names = [w for w in workloads if reps.get(w)]
    schedule, index = [], 0
    for r in range(max(reps.values(), default=0)):
        order = names[r % len(names) :] + names[: r % len(names)] if names else []
        for k, w in enumerate(order):
            if r >= reps[w]:
                continue
            shift = (r + names.index(w)) % len(arms)
            for position, arm in enumerate(arms[shift:] + arms[:shift]):
                schedule.append(dict(id=index, round=r, name=w, arm=arm, position=position))
                index += 1
    return schedule, dropped, round(total())


def emitter(sink):
    def emit(kind, **data):
        row = dict(kind=kind, at=round(time.time(), 3), **data)
        text = json.dumps(row, default=str)
        try:
            sys.stdout.write(text + "\n")  # one write: a stop never splits or glues rows
            sys.stdout.flush()
        except BrokenPipeError:
            # The reader (`run`) is gone. The rows are lost, but the cleanup that emits
            # them must still run: point stdout at /dev/null and carry on.
            devnull = os.open(os.devnull, os.O_WRONLY)
            os.dup2(devnull, sys.stdout.fileno())
            os.close(devnull)
        if sink is not None:
            sink.write(text + "\n")
            sink.flush()

    return emit


def env_choice(name, default, choices):
    value = os.getenv(name, default)
    if value not in choices:
        raise SystemExit(f"{name} must be one of {choices}")
    return value


def run():
    trees = Path(os.environ.get("PERF_TREES", ""))
    if not trees.is_dir():
        raise SystemExit("PERF_TREES must name the directory `prepare` wrote")
    arms = os.getenv("PERF_ARMS", "f1,f2").split(",")
    for arm in arms:
        if not (trees / arm / "manifest.json").exists():
            raise SystemExit(f"arm {arm} has no prepared tree")
    names = os.getenv("PERF_WORKLOADS", ",".join(WORKLOADS)).split(",")
    if any(n not in WORKLOADS for n in names):
        raise SystemExit("unknown workload")
    reps = {n: REPS[n] for n in names}
    for item in filter(None, os.getenv("PERF_REPS", "").split(",")):
        key, _, value = item.partition("=")
        if key not in reps or not value.isdigit() or not 0 <= int(value) <= 10:
            raise SystemExit("PERF_REPS must be name=n with n in 0..10")
        reps[key] = int(value)
    scale = float(os.getenv("PERF_EVENTS_SCALE", "1"))
    if not 0 < scale <= 1:
        raise SystemExit("PERF_EVENTS_SCALE must be in (0, 1]")
    workloads = {
        n: (op, width, max(2, int(events * scale)), per_txn)
        for n, (op, width, events, per_txn) in WORKLOADS.items()
        if n in names
    }
    store = env_choice("PERF_STORE", "md", ("md", "local"))
    if os.getenv("PERF_HOLD_AT") and (store != "local" or not os.getenv("PERF_HOLD_FILE")):
        raise SystemExit("PERF_HOLD_AT is for tests on the local store, with PERF_HOLD_FILE")
    cap = env_choice("PERF_CPU_CAP", "quota", CAP_MODES)
    if cap != "none" and (os.cpu_count() or 0) < 4:
        raise SystemExit("capped runs need nproc >= 4")
    dsn_env = os.getenv("PERF_DSN_ENV", "CDC_TEST_PG_DSN")
    if not re.fullmatch(r"[A-Za-z_]\w*", dsn_env) or not os.getenv(dsn_env):
        raise SystemExit("PERF_DSN_ENV must name a set variable holding the DSN")
    if store == "md":
        require_token()
    store_dir = os.getenv("PERF_STORE_DIR") or str(trees / "stores")
    journal_dir = os.getenv("PERF_JOURNAL_DIR") or str(trees / "journal")
    Path(store_dir).mkdir(parents=True, exist_ok=True)
    estimate = json.loads(os.getenv("PERF_ESTIMATES", "null")) or ESTIMATE
    budget = float(os.getenv("PERF_BUDGET_S", "1800"))
    try:
        deadline_s = float(os.getenv("PERF_DEADLINE_S") or budget)
        grace = float(os.getenv("PERF_STOP_GRACE_S", "60"))
    except ValueError:
        raise SystemExit("PERF_DEADLINE_S and PERF_STOP_GRACE_S must be numbers") from None
    if not (deadline_s > 0 and grace > 0):
        raise SystemExit("PERF_DEADLINE_S and PERF_STOP_GRACE_S must be positive")
    end = time.monotonic() + deadline_s
    deadline_at = time.time() + deadline_s  # the same deadline, for the reps' backstop
    schedule, dropped, total = plan(arms, workloads, reps, budget, estimate)
    sink = open(os.environ["PERF_OUT"], "a") if os.getenv("PERF_OUT") else None
    emit = emitter(sink)
    session = uuid.uuid4().hex[:12]
    emit(
        "plan",
        session=session,
        harness_sha256=harness_digest(),
        arms=arms,
        workloads=workloads,
        reps=reps,
        dropped=dropped,
        estimate_s=total,
        estimate_constants=estimate,
        budget_s=budget,
        deadline_s=deadline_s,
        stop_grace_s=grace,
        hold=os.getenv("PERF_HOLD_AT"),
        store=store,
        cap=cap,
        schedule=schedule,
        cpu_model=cpu_model(),
        nproc=os.cpu_count(),
        system=platform.system(),
    )
    failures, skipped, stop = 0, [], {"reason": None}
    shared = dict(
        workloads=workloads,
        trees=str(trees),
        store=store,
        store_dir=store_dir,
        journal_dir=journal_dir,
        dsn_env=dsn_env,
        cap=cap,
        session=session,
        deadline_at=deadline_at,
        stop_grace_s=grace,
    )

    def on_signal(signum, frame):
        stop["reason"] = stop["reason"] or "signal"

    # Never restored, only replaced by SIG_IGN below: `run` is the process's top level
    # (main -> sys.exit). A stop arriving after the schedule (a unit stop's second SIGTERM,
    # during the `done` tail) is recorded, never fatal.
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, on_signal)
    failures, skipped = run_schedule(schedule, stop, end, grace, emit, shared)
    left = pending(journal_dir)
    stopped = stop["reason"]
    # The outcome is settled: ignore later stops at the kernel. A Python handler is not
    # enough, because interpreter exit resets it to SIG_DFL (CPython's _PySignal_Fini), so
    # a SIGTERM during teardown would kill the process (-15) after `done` said otherwise.
    # SIG_IGN survives teardown. Children inherit SIG_IGN, so spawn nothing after this.
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, signal.SIG_IGN)
    emit(
        "done",
        session=session,
        failures=failures,
        journal_pending=left,
        stopped=stopped,
        skipped=skipped,
    )
    if sink is not None:
        sink.close()
    if stopped:
        return 143 if stopped == "signal" else 124
    return 1 if failures or left else 0


def run_schedule(schedule, stop, end, grace, emit, shared):
    """One child per schedule item, until the schedule ends or a stop (a signal to this
    process, or the deadline). Returns (failures, skipped schedule items)."""
    failures = 0
    session = shared["session"]
    for index, item in enumerate(schedule):
        if stop["reason"] is None and time.monotonic() >= end:
            stop["reason"] = "deadline"
        if stop["reason"]:
            return failures, [
                dict(id=i["id"], name=i["name"], arm=i["arm"]) for i in schedule[index:]
            ]
        spec = dict(
            item,
            workload=shared["workloads"][item["name"]],
            **{k: v for k, v in shared.items() if k != "workloads"},
        )
        argv = [sys.executable, str(Path(__file__).resolve()), "child"]
        env = dict(os.environ, PERF_CHILD_SPEC=json.dumps(spec), PYTHONDONTWRITEBYTECODE="1")
        env.pop("PYTHONPATH", None)
        started = time.perf_counter()
        debug = os.getenv("PERF_DEBUG") == "1"
        # Its own process group: a terminal's Ctrl-C reaches only this process, which
        # forwards it, and a stop reaches everything the child started.
        proc = subprocess.Popen(
            cap_command(shared["cap"], argv),
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            start_new_session=True,
        )
        stopped, killed = None, False
        while True:
            try:
                out, err = proc.communicate(timeout=0.25)
                break
            except subprocess.TimeoutExpired:
                if stop["reason"] is None and time.monotonic() >= end:
                    stop["reason"] = "deadline"
                if stop["reason"]:
                    stopped = stop["reason"]
                    out, err, killed = stop_child(proc, grace)
                    break
        if debug and err:
            sys.stderr.write(err)
        unparsed = 0
        for line in out.splitlines():
            try:
                row = json.loads(line)
            except ValueError:
                unparsed += 1
                continue
            if isinstance(row, dict) and "kind" in row:
                row.update(session=session)
                kind = row.pop("kind")
                row.pop("at", None)
                emit(kind, **row)
        emit(
            "child_done",
            spec_id=item["id"],
            returncode=proc.returncode,
            seconds=round(time.perf_counter() - started, 2),
            stopped=stopped,
            killed=killed,
            unparsed_lines=unparsed,
        )
        failures += proc.returncode != 0
        if stop["reason"]:
            rest = schedule[index + 1 :]
            return failures, [dict(id=i["id"], name=i["name"], arm=i["arm"]) for i in rest]
    return failures, []


def child():
    spec = json.loads(os.environ["PERF_CHILD_SPEC"])
    install_stop_handlers(spec.get("deadline_at"), spec.get("stop_grace_s"))
    emit = emitter(None)
    try:
        return rep(spec, emit)
    except SystemExit as exc:
        emit("refused", spec_id=spec.get("id"), reason=str(exc.code)[:120])
        return 2
    except BaseException as exc:  # before the rep's own row: the type and step only
        emit("failed", spec_id=spec.get("id"), error=type(exc).__name__, step=STEP[0])
        if os.getenv("PERF_DEBUG") == "1":
            raise
        return 1


# Summary ----------------------------------------------------------------------------


METRICS = {
    # name: (function of a rep row, unit)
    "ingest_wall_per_event_us": lambda r: r["ingest"]["wall_s"] / r["events"] * 1e6,
    "ingest_cpu_per_event_us": lambda r: r["ingest"]["process_cpu_s"] / r["events"] * 1e6,
    "spool_cpu_per_event_us": lambda r: (
        sum(v[2] for k, v in r["ingest"]["hooks"].items() if k.startswith("spool"))
        / r["events"]
        * 1e6
    ),
    "append_wall_per_event_us": lambda r: (
        sum(v[1] for k, v in r["ingest"]["hooks"].items() if k == "append") / r["events"] * 1e6
    ),
    "commit_s_per_txn_ms": lambda r: (
        r["ingest"]["statements"].get("commit", [0, 0])[1] / r["window_txns"] * 1e3
    ),
    "arrow_bytes_per_event": lambda r: r["ingest"]["arrow_bytes"] / r["events"],
    "wire_tx_per_event": lambda r: (
        r["wire"]["ingest_tx"] / r["events"] if "ingest_tx" in r["wire"] else None
    ),
    "log_bytes_per_event": lambda r: (
        r["log_storage"]["bytes"] / r["log_events"]
        if r["log_storage"].get("bytes") and r["log_events"]
        else None
    ),
    "ledger_rows_per_1k_txn": lambda r: r["ledger_rows"] / r["oracle"]["txns_expected"] * 1e3,
    "merge_wall_per_event_us": lambda r: (
        r["apply"]["hooks"].get("merge", [0, 0, 0])[1] / r["events"] * 1e6
    ),
    # The SQL fold's MERGE statement (the fold and the MERGE in one), per event.
    "sql_merge_wall_per_event_us": lambda r: (
        r["apply"]["hooks"].get("sql_merge", [0, 0, 0])[1] / r["events"] * 1e6
    ),
    "apply_wall_per_event_us": lambda r: r["apply"]["wall_s"] / r["events"] * 1e6,
    "end_to_end_events_per_s": lambda r: (
        r["events"] / (r["ingest"]["wall_s"] + r["apply"]["wall_s"])
    ),
    "peak_rss_mb": lambda r: r["peak_rss_kb"] / 1024,
    "peak_rss_ingest_mb": lambda r: r["peak_rss_by_step_kb"]["ingest"] / 1024,
    "peak_rss_apply_mb": lambda r: r["peak_rss_by_step_kb"]["apply"] / 1024,
    "peak_rss_oracle_mb": lambda r: r["peak_rss_by_step_kb"]["oracle"] / 1024,
    "fold_fallback_batches": lambda r: sum(
        n for k, n in r["apply"]["fold_stats"].items() if k.startswith("fallback:")
    ),
}


def summarize(path, base="f1"):
    rows = [json.loads(line) for line in Path(path).read_text().splitlines() if line.strip()]
    plans = [r for r in rows if r["kind"] == "plan"]
    reps = [r for r in rows if r["kind"] == "rep"]
    digests = {r.get("harness_sha256") for r in reps} | {p.get("harness_sha256") for p in plans}
    if len(digests) != 1:
        raise SystemExit("refusing: results mix harness digests")
    lines = []
    caps = {str(r["cap"].get("mode")) for r in reps}
    models = {r["host"]["cpu_model"] for r in reps}
    label = f"host={'/'.join(sorted(models))} cap={'/'.join(sorted(caps))}"
    lines.append(f"# {label}; harness {next(iter(digests))[:12]}")
    lines.append("## realised schedule (planned / ok / failed / dropped)")
    planned = {}
    for p in plans:
        for item in p["schedule"]:
            planned.setdefault((item["name"], item["arm"]), 0)
            planned[(item["name"], item["arm"])] += 1
        for name in p.get("dropped", []):
            lines.append(f"dropped by budget: {name}")
    done = {}
    for r in reps:
        key = (r["workload"], r["arm"])
        done.setdefault(key, [0, 0])
        done[key][0 if r.get("ok") else 1] += 1
    counts = {}
    for key in sorted(set(planned) | set(done)):
        ok, failed = done.get(key, [0, 0])
        counts[key] = ok
        lines.append(f"{key[0]} {key[1]}: {planned.get(key, 0)} / {ok} / {failed}")
    for workload in sorted({k[0] for k in counts}):
        arm_counts = {a: n for (w, a), n in counts.items() if w == workload}
        if len(set(arm_counts.values())) > 1:
            lines.append(f"UNBALANCED {workload}: {arm_counts}")
    for d in (r for r in rows if r["kind"] == "done" and r.get("stopped")):
        names = ", ".join(f"{i['name']}/{i['arm']}" for i in d.get("skipped", []))
        lines.append(f"STOPPED ({d['stopped']}); skipped: {names or 'none'}")
    lines.append("## metrics: median [min-max] per arm; ratio = arm median / base median")
    good = [r for r in reps if r.get("ok")]
    for workload in sorted({r["workload"] for r in good}):
        for metric, fn in METRICS.items():
            values = {}
            for r in good:
                if r["workload"] == workload:
                    try:
                        v = fn(r)
                    except (KeyError, TypeError, ZeroDivisionError):
                        v = None
                    if v is not None:
                        values.setdefault(r["arm"], []).append(v)
            if not values:
                continue
            cells, medians = [], {}
            for arm in sorted(values):
                vs = values[arm]
                medians[arm] = statistics.median(vs)
                cells.append(f"{arm} {medians[arm]:.4g} [{min(vs):.4g}-{max(vs):.4g}] n={len(vs)}")
            ratios = [
                f"{arm}/{base} {medians[arm] / medians[base]:.3f}"
                for arm in sorted(medians)
                if arm != base and base in medians and medians[base]
            ]
            lines.append(f"{workload} {metric}: " + "; ".join(cells + ratios) + f" ({label})")
    return "\n".join(lines)


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    if not argv:
        raise SystemExit(__doc__)
    command, rest = argv[0], argv[1:]
    if command == "prepare" and len(rest) == 3:
        print(json.dumps(prepare(*rest)))
        return 0
    if command == "run" and not rest:
        return run()
    if command == "child" and not rest:
        return child()
    if command == "summarize" and len(rest) in (1, 2):
        print(summarize(*rest))
        return 0
    raise SystemExit(__doc__)


if __name__ == "__main__":
    sys.exit(main())
