"""Flight Explainer progress lines: `@flight-run <node> <status> [field=value ...]`.

The chart lives in the Flight's `metadata.chart` (flights/charts/<command>.mmd), never in
the source; node ids are the join key. Emitting never fails or slows a run:
- one write plus a flush per line, under a lock (the ingest upload thread emits too);
- every error is swallowed. After a broken stdout (BrokenPipeError, OSError) emitting
  turns itself off and points the stdout descriptor at /dev/null, so the interpreter's
  exit-time flush can't turn a successful run into exit status 120;
- no I/O other than that write.

Lines carry node ids, statuses, counts, table names, run numbers and exception type
names only: never messages, hosts, DSNs or tokens.
"""

import os
import sys
import threading
from datetime import datetime, timezone

STATUSES = ("running", "ok", "failed", "skipped")

_lock = threading.Lock()
_enabled = True


def _quote(value):
    text = str(value)
    if any(c in text for c in ' "=') or not text:
        return '"' + text.replace("\\", "\\\\").replace('"', '\\"') + '"'
    return text


def line(node, status, **fields):
    """The text of one event (without the newline). `effect` may be a list."""
    parts = ["@flight-run", node, status]
    for name, value in fields.items():
        if value is None:
            continue
        for item in value if isinstance(value, (list, tuple)) else (value,):
            parts.append(f"{name}={_quote(item)}")
    return " ".join(parts)


def now():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _disable():
    global _enabled
    _enabled = False
    try:
        devnull = os.open(os.devnull, os.O_WRONLY)
        os.dup2(devnull, sys.stdout.fileno())
        os.close(devnull)
    except Exception:
        pass


def write(text):
    """Write one complete line and flush it, or nothing. Never raises."""
    if not _enabled:
        return
    try:
        with _lock:
            out = sys.stdout
            if out is None:
                return
            out.write(text + "\n")
            out.flush()
    except OSError:  # BrokenPipeError included
        _disable()
    except Exception:
        pass


def timing(kind, **fields):
    """One `@flight-log timing <kind> name=value ... ts=...` line: durations, counts and
    settings only (numbers or short words). Never raises. A plain log line, not a chart
    node: the Explainer ignores it; it is for reading ingest performance off Flight logs."""
    try:
        parts = ["@flight-log timing", kind]
        for name, value in fields.items():
            if value is None:
                continue
            if isinstance(value, float):
                value = f"{value:.4f}"
            parts.append(f"{name}={_quote(value)}")
        parts.append(f"ts={now()}")
        write(" ".join(parts))
    except Exception:
        pass


def emit(node, status, **fields):
    try:
        write(line(node, status, **fields, ts=now()))
    except Exception:
        pass


class Run:
    """Tracks the nodes (and keys) this run started, so a failure can close them.

    `step(node, status, **fields)` emits; `fail_open(error)` marks every node still
    running as failed with the exception type; `seen(node)` says whether a node
    emitted anything."""

    def __init__(self):
        self._open = {}
        self._seen = set()
        self.failed_node = None  # the first node that failed (for the status report)
        self._lock = threading.Lock()

    def step(self, node, status, **fields):
        try:
            key = fields.get("key")
            with self._lock:
                self._seen.add(node)
                if status == "failed" and self.failed_node is None:
                    self.failed_node = node
                if status == "running":
                    self._open[(node, key)] = True
                else:
                    self._open.pop((node, key), None)
            emit(node, status, **fields)
        except Exception:
            pass

    def seen(self, node):
        return node in self._seen

    def running(self, node, key=None):
        return (node, key) in self._open

    def fail_open(self, error):
        try:
            with self._lock:
                still = list(self._open)
            for node, key in still:
                self.step(node, "failed", key=key, error=type(error).__name__)
        except Exception:
            pass


class Stages:
    """A `fault=` callback that turns stage names into events, for production runs.

    `actions` maps a stage name to a function of (run, count), where count is how many
    times that stage has been seen. Unknown stages do nothing. It never raises, does
    no I/O but printing, and is safe to call from several threads."""

    def __init__(self, run, actions):
        self.run, self.actions = run, actions
        self.counts = {}
        self._lock = threading.Lock()

    def __call__(self, stage):
        try:
            action = self.actions.get(stage)
            if action is None:
                return
            with self._lock:
                count = self.counts[stage] = self.counts.get(stage, 0) + 1
            action(self.run, count)
        except Exception:
            pass
