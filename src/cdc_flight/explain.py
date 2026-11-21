"""Flight Explainer wiring: which progress lines each command emits, and when.

The CLI calls these with live objects, and the drift tests call the same functions,
so the lines a Flight prints are the lines the tests check against
flights/charts/<command>.mmd. Everything that reaches the run goes through
flightlog, which never raises and does no I/O but printing. The only extra reads
here are the backfill's resume state and row estimate, made once before the run.
"""

import math
from datetime import datetime, timezone

from cdc_flight import flightlog
from cdc_flight.flightlog import Stages

INGEST_CHAIN = ("drained", "capped", "needs_action")


def step(run, node, action, **fields):
    """Run `action` as one node: running, then ok (or failed with the error type)."""
    run.step(node, "running", **fields)
    try:
        value = action()
    except Exception as exc:
        run.step(node, "failed", error=type(exc).__name__, **fields)
        raise
    run.step(node, "ok", **fields)
    return value


def failed(run, exc):
    """Close every node still running as failed (the exception type only)."""
    run.fail_open(exc)


# preflight ---------------------------------------------------------------------

PREFLIGHT_ORDER = ("endpoint", "connect", "server", "replication", "tables", "objects")


def preflight_stages(run):
    def advance(done, following):
        def action(r, _count):
            r.step(done, "ok")
            if following:
                r.step(following, "running")

        return action

    actions = {
        f"after_{node}": advance(node, PREFLIGHT_ORDER[i + 1] if i + 1 < 6 else None)
        for i, node in enumerate(PREFLIGHT_ORDER[:5])
    }
    actions["objects_missing"] = lambda r, _c: (
        r.step("objects", "ok", branch="missing"),
        r.step("provision", "ok"),
    )
    actions["after_objects_present"] = lambda r, _c: r.step("objects", "ok", branch="present")
    actions["after_objects_created"] = lambda r, _c: r.step("objects", "ok", branch="created")
    return Stages(run, actions)


def preflight(run, check, register):
    """`check(fault)` validates (and maybe provisions); `register(info)` records it."""
    run.step("endpoint", "running")
    try:
        info = check(preflight_stages(run))
    except Exception as exc:
        failed(run, exc)
        raise
    step(run, "register", lambda: register(info))
    return info


# ingest ------------------------------------------------------------------------


def ingest_stages(run, config):
    def changes():
        # The emit-time UTC day. append chooses the real partition day itself, so the
        # two can differ for a flush that straddles midnight.
        day = datetime.now(timezone.utc).strftime("%Y%m%d")
        return f"table:{config.database}.{config.source_id}.changes_{day}"

    def after_lease(r, _c):
        r.step("lease", "ok", branch="ok")
        r.step("slot", "running")

    def after_append(r, count):
        r.step("append", "ok", effect=changes())
        flightlog.write(f"@flight-log append batch {count} is durable")

    def before_finish(r, _c):
        if r.running("stream"):
            r.step("stream", "ok")
        r.step("finish", "running")

    stages = Stages(
        run,
        {
            "after_lease": after_lease,
            "after_slot_check": lambda r, _c: r.step("slot", "ok", branch="ok"),
            "before_stream": lambda r, _c: r.step("stream", "running"),
            "before_append": lambda r, _c: r.step("append", "running"),
            "after_append": after_append,
            "before_finish": before_finish,
        },
    )
    # run_once's per-append and per-run timing lines (log-only; see docs/flights.md).
    stages.timing = flightlog.timing
    return stages


def tables_held(run, log, info):
    """The held check: configured tables whose name no longer resolves to their OID.
    Reported, never a failure: the rest of the source keeps running."""
    try:
        held = getattr(info, "held", None) or {}
        names = {}
        if held:
            names = {
                r["relation_id"]: f"{r['schema_name']}.{r['table_name']}" for r in log.registry()
            }
        run.step("held", "ok", branch="held" if held else "all", rows=len(held))
        for oid, (reason, _detail) in sorted(held.items()):
            flightlog.write(f"@flight-log held {names.get(oid, oid)} {reason}")
    except Exception:
        pass


def ingest(run, config, log, info, run_once, chain, trigger, after_ingest, before):
    """Validate (already done by the caller as `preflight`), run once, then chain."""
    tables_held(run, log, info)
    run.step("lease", "running")
    try:
        result = run_once(config, log, info=info, fault=ingest_stages(run, config))
    except Exception as exc:
        failed(run, exc)
        raise
    if result.status == "busy":
        if run.running("lease"):
            run.step("lease", "ok", branch="busy")
            run.step("overlap", "ok")
        else:
            run.step("slot", "ok", branch="busy")
            run.step("busy", "ok")
        return result, "busy"
    if run.running("stream"):
        run.step("stream", "ok")
    # The run's transaction count goes on finish, which records it: append already
    # reported each batch, before finish started.
    run.step("finish", "ok", rows=result.transactions)
    advanced = result.status in INGEST_CHAIN and result.boundary > before
    run.step("outcome", "ok", branch="advanced" if advanced else "unchanged")
    if not advanced:
        run.step("idle", "ok")
    launched = {}
    outcome = after_ingest(
        result, before, chain, traced_trigger(run, "trigger", "apply", trigger, launched)
    )
    if advanced:
        number = (launched.get("run") or (None, None))[1]
        if outcome == "triggered":
            run.step("trigger", "ok", flight="apply", run=number)
        elif outcome == "trigger_failed":
            run.step("trigger", "failed", flight="apply", error="trigger_failed")
        else:
            run.step("trigger", "skipped", flight="apply")
    return result, outcome


# apply -------------------------------------------------------------------------

STATE_BRANCH = {
    "caught_up": "current",
    "capped": "current",
    "busy": "current",
    "awaiting_generation": "awaiting_generation",
    "needs_resync": "needs_resync",
    "needs_action": "needs_action",
    "source_missing": "source_missing",
    "paused": "paused",
}
SINK = {
    "current": "current",
    "awaiting_generation": "nogen",
    "needs_resync": "resync",
    "needs_action": "action",
    "source_missing": "missing",
    "paused": "paused",
}


def apply_tables(run, mirror):
    """The `on_table` callback for Apply.run: one fan-out instance per table."""

    def on_table(event, value):
        try:
            if event == "start":
                run.step("table", "running", key=value)
                return
            branch = STATE_BRANCH.get(value.status, "needs_action")
            schema, name = value.table.split(".")
            run.step(
                "table",
                "ok",
                key=value.table,
                rows=value.events,
                effect=f"table:{mirror}.{schema}.{name}",
            )
            run.step("state", "ok", key=value.table, branch=branch)
            run.step(SINK[branch], "ok", key=value.table)
        except Exception:
            pass

    return on_table


def apply(run, run_tables, mirror):
    """`run_tables(on_table)` applies every table (the caller ran `attach` as a step)."""
    try:
        return run_tables(apply_tables(run, mirror))
    except Exception as exc:
        failed(run, exc)
        raise


# compact -----------------------------------------------------------------------


def compaction_events(run):
    """The `on_event` callback for compact.run_compaction."""

    def on_event(event, value):
        try:
            if event == "lease":
                run.step("lease", "ok", branch=value)
                if value == "busy":
                    run.step("busy", "ok")
            elif event == "expire":
                run.step("expire", "ok", branch=value)
                run.step(EXPIRE_SINK[value], "ok")
            elif event == "day_start":
                run.step("day", "running", key=value)
            elif event == "day":
                day, dropped = value
                if dropped is None or not dropped:
                    run.step("day", "skipped", key=day)
                else:
                    run.step("day", "ok", key=day)
            elif event == "stop":
                run.step("stop", "ok", branch=value)
                run.step(COMPACT_SINK[value], "ok")
        except Exception:
            pass

    return on_event


EXPIRE_SINK = {"none": "kept", "report": "reported", "expired": "expired"}

COMPACT_SINK = {
    "done": "done",
    "pinned": "pinned",
    "not_sealed": "open",
    "no_pins": "nopins",
    "held": "held",
    "capped": "capped",
    "busy": "conflict",
    "invariant": "invariant",
}


# backfill ----------------------------------------------------------------------


def resume_point(gen):
    """Where a resumed generation continues: its scan, the marker, or catch-up."""
    if gen["state"] == "snapshotting" and not gen["scan_done"]:
        return "scan"
    if gen["state"] == "snapshotting" or gen["marker_lsn"] is None:
        return "marker"
    return "catch_up"


def estimate_chunks(pg, table, chunk_rows, query):
    """The planner's row estimate over chunk_rows, or None. One catalog read with a
    2 s statement timeout; never raises; None when the table was never analyzed."""
    try:
        query(pg, "BEGIN")
        try:
            query(pg, "SET LOCAL statement_timeout='2s'")
            (rows,) = query(pg, "SELECT reltuples FROM pg_class WHERE oid=%s::regclass", [table])[0]
        finally:
            query(pg, "COMMIT")
        return math.ceil(rows / chunk_rows) if rows and rows > 0 else None
    except Exception:
        return None


def backfill_stages(run, chunks_before, total, target):
    def scanned(r, count):
        done = chunks_before + count
        r.step("scan", "running", done=done, total=None if total is None else max(total, done))

    def scan_done(r, _c):
        r.step("scan", "ok")
        r.step("marker", "running")

    def after_pin(r, _c):
        r.step("pin", "ok")
        r.step("scan", "running", done=chunks_before, total=total)

    return Stages(
        run,
        {
            "after_pin": after_pin,
            "after_chunk_commit": scanned,
            "after_scan_done": scan_done,
            "after_marker": lambda r, _c: (r.step("marker", "ok"), r.step("catchup", "running")),
            "after_catch_up": lambda r, _c: (r.step("catchup", "ok"), r.step("publish", "running")),
            "after_publish_commit": lambda r, _c: r.step("publish", "ok", effect=f"table:{target}"),
        },
    )


NODE_OF = {"scan": "scan", "marker": "marker", "catch_up": "catchup"}


def backfill(run, gen, total, target, run_table, *, start=True):
    """`gen` is the active generation before the run (None: a new one starts if
    `start`); `target` the mirror table; `run_table(fault)` runs it."""
    if gen is None and not start:
        # Resume only, and nothing to resume: the run reports idle.
        run.step("start", "ok", branch="resume")
        run.step("resume", "skipped")
        return run_table(None)
    if gen is None:
        run.step("start", "ok", branch="new")
        run.step("pin", "running")
        chunks = 0
    else:
        point = resume_point(gen)
        run.step("start", "ok", branch="resume")
        run.step("resume", "ok", branch=point)
        chunks = gen["chunks"] or 0
        fields = dict(done=chunks, total=total) if point == "scan" else {}
        run.step(NODE_OF[point], "running", **fields)
    try:
        result = run_table(backfill_stages(run, chunks, total, target))
    except Exception as exc:
        failed(run, exc)
        raise
    # Nodes this run reached but didn't finish: they end here, where the next run resumes.
    if run.running("pin"):
        run.step("pin", "ok")
    for node in ("scan", "marker", "catchup", "publish"):
        if run.running(node):
            status = "failed" if result.status == "failed" else "ok"
            extra = dict(error="generation_failed") if status == "failed" else {}
            if node == "scan":
                extra.update(done=result.chunks)
            run.step(node, status, **extra)
            if status == "ok" and result.status != "published":
                flightlog.write(f"@flight-log {node} {result.status}; the next run resumes here")
    return result


def backfill_outcome(run, result, outcome, launched):
    """After chain.after_backfill: which way the run went."""
    if result.status == "published":
        run.step("outcome", "ok", branch="published")
        run.step("done", "ok")
    elif result.status == "failed":
        run.step("outcome", "ok", branch="failed")
        run.step("done", "ok")
    elif outcome in ("triggered", "trigger_failed"):
        run.step("outcome", "ok", branch="continue")
        number = (launched.get("run") or (None, None))[1]
        if outcome == "triggered":
            run.step("retrigger", "ok", flight="backfill", run=number)
        else:
            run.step("retrigger", "failed", flight="backfill", error="trigger_failed")
    else:
        run.step("outcome", "ok", branch="stopped")
        run.step("done", "ok")


def traced_trigger(run, node, flight, trigger, launched):
    def traced(target, overrides):
        run.step(node, "running", flight=flight)
        launched["run"] = trigger(target, overrides)
        return launched["run"]

    return traced
