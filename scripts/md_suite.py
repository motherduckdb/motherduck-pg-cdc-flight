#!/usr/bin/env python3
"""Run all local/PG tests and the fixed MD budget with a ten-minute outer deadline."""

import argparse
import json
import os
import signal
import subprocess
import time
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LIMIT = 600
SOFT_STOP = 540
FORCE_STOP = 590
CHILD_GROUP = None


def group_alive(pid):
    try:
        os.killpg(pid, 0)
        return True
    except ProcessLookupError:
        return False


def supervise(command, *, env, output, started, soft=SOFT_STOP, force=FORCE_STOP, limit=LIMIT):
    """The controller lives outside the child's session, including stuck teardowns.

    Durations are injectable only through this Python API for cheap subprocess tests;
    the public command exposes no way to extend the ten-minute limit.
    """
    global CHILD_GROUP
    child = subprocess.Popen(
        command, cwd=ROOT, env=env, stdout=output, stderr=subprocess.STDOUT, start_new_session=True
    )
    CHILD_GROUP = child.pid
    interrupted = []
    previous = {}

    def stop(signum, frame):
        interrupted.append(time.monotonic())

    for sig in (signal.SIGINT, signal.SIGTERM):
        previous[sig] = signal.signal(sig, stop)
    cancelled = forced = False
    try:
        while True:
            elapsed = time.monotonic() - started
            status = child.poll()
            living = group_alive(child.pid)
            if status is not None and not living:
                return (124 if cancelled else status), cancelled
            now = time.monotonic()
            if (elapsed >= soft or interrupted) and not cancelled:
                cancelled = True
                output.write("md-suite: budget/interrupt stop; allowing fixture cleanup\n")
                output.flush()
                if living:
                    os.killpg(child.pid, signal.SIGTERM)
            external_force = interrupted and now - interrupted[0] >= force - soft
            if (elapsed >= force or external_force) and not forced:
                forced = True
                if living:
                    os.killpg(child.pid, signal.SIGKILL)
                output.write("md-suite: forced stop; reconcile the resource registry\n")
                output.flush()
            if elapsed >= limit:
                if living:
                    os.killpg(child.pid, signal.SIGKILL)
                # No unbounded wait for a remote query, teardown or process group.
                return 124, True
            time.sleep(min(0.1, max(0, limit - elapsed)))
    finally:
        CHILD_GROUP = None
        for sig, handler in previous.items():
            signal.signal(sig, handler)
        if child.poll() is None:
            os.killpg(child.pid, signal.SIGKILL)
            # Reaping is bounded and never consumes the outer deadline reserve.
            try:
                child.wait(timeout=min(0.2, max(0, limit - (time.monotonic() - started))))
            except subprocess.TimeoutExpired:
                pass


def pending_resources(directory):
    latest = {}
    for path in directory.glob("*.jsonl"):
        for index, line in enumerate(path.read_text().splitlines()):
            try:
                row = json.loads(line)
                latest[row["kind"], row["name"]] = row["event"]
            except (ValueError, KeyError):
                # A killed writer may leave a partial final record. Fail closed;
                # never interpret malformed ownership data as complete cleanup.
                latest["incomplete_journal", f"{path.name}:{index}"] = "allocated"
    return [
        {"kind": kind, "name": name}
        for (kind, name), state in latest.items()
        if state != "released"
    ]


def pytest_command(python):
    return [
        str(python),
        "-m",
        "pytest",
        "-q",
        "-n",
        "4",
        "--maxschedchunk=1",
        "--md-suite",
        "-x",
        "--max-worker-restart=0",
        "-k",
        "not benchmark",
        "-p",
        "no:cacheprovider",
        "--durations=0",
        "--durations-min=0.5",
    ]


def main(argv=None):
    started = time.monotonic()

    def expired(signum, frame):
        if CHILD_GROUP:
            try:
                os.killpg(CHILD_GROUP, signal.SIGKILL)
            except ProcessLookupError:
                pass
        os._exit(124)

    previous = signal.signal(signal.SIGALRM, expired)
    signal.setitimer(signal.ITIMER_REAL, LIMIT)
    try:
        return run(argv, started)
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)


def run(argv, started):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("log", type=Path)
    parser.add_argument("--lock-slot", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--run-registry", type=Path, help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    python = ROOT / ".venv/bin/python"
    if not python.exists() or not os.getenv("CDC_TEST_PG_DSN"):
        parser.error("run uv sync --dev beforehand and set CDC_TEST_PG_DSN")
    args.log = args.log.resolve()
    args.log.parent.mkdir(parents=True, exist_ok=True)
    resources = Path(str(args.log) + ".resources-" + uuid.uuid4().hex[:8])
    resources.mkdir()
    events = resources / "events"
    events.mkdir()
    slot = args.lock_slot
    # The wrapper queues before exec; its PID becomes ours and its slot stays held.
    if slot and (slot / "pid").read_text().strip() != str(os.getpid()):
        parser.error("the admission slot belongs to another process")
    if args.run_registry:
        with args.run_registry.open("a") as out:
            out.write(f"{time.time():.0f} {os.getpid()} {ROOT} {args.log}\n")
    env = {
        **os.environ,
        "CDC_TEST_MD": "1",
        "MD_SUITE_RESOURCE_DIR": str(resources),
        "MD_SUITE_RESULTS": str(args.log) + ".results",
        "MD_SUITE_STARTED": str(started),
        "MD_SUITE_EVENTS_DIR": str(events),
    }
    Path(env["MD_SUITE_RESULTS"]).write_text("")
    command = pytest_command(python)

    try:
        with args.log.open("w", buffering=1) as output:
            admitted = time.time() - (time.monotonic() - started)
            stamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(admitted))
            deadline = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(admitted + LIMIT))
            output.write(
                f"md-suite: mode=bounded start={stamp} cwd={ROOT} workers=-n4 slot={slot.name if slot else 'direct'} cap=600s deadline={deadline}\n"
            )
            output.write(f"md-suite: resource registry={resources}\n")
            code, cancelled = supervise(command, env=env, output=output, started=started)
            pending = pending_resources(resources)
            if pending and code == 0:
                code = 1
            output.write(
                f"md-suite: exit={code} elapsed={time.monotonic() - started:.2f}s pending_resources={len(pending)} cancelled={cancelled}\n"
            )
            if pending:
                output.write("md-suite: cleanup incomplete; owner-scoped reconciliation required\n")
        print(f"md-suite: exit={code}; log={args.log}; resource registry={resources}")
        return code
    finally:
        if slot:
            for name in ("owner", "pid"):
                (slot / name).unlink(missing_ok=True)
            slot.rmdir()


if __name__ == "__main__":
    raise SystemExit(main())
