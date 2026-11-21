"""Bounded micro-batch replication loop with keepalives during destination writes."""

import gc
import logging
import os
import re
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeout
from dataclasses import dataclass, field, replace
from datetime import datetime
from time import perf_counter, thread_time  # timing lines only (tests fake `time`)

import duckdb
from psycopg2 import sql

from cdc_flight import pg_ca
from cdc_flight.ingest_backend import Backend2
from cdc_flight.ingest_log import FenceLost, GuardedIngestConflict, LeaseBusy, SourceMismatch
from cdc_flight.positions import (
    PRIMARY,
    STANDBY,
    current_lsn,
    current_sql,
    receive_lsn,
    role_of,
    slots_now,
)
from cdc_flight.preflight import (
    catalog_facts,
    check,
    check_formats,
    connect,
    connection_options,
    qualified,
    query,
)
from cdc_flight.replication import ReplicationStream
from cdc_flight.retention import is_conflict
from cdc_flight.source_label import Guard

logger = logging.getLogger(__name__)


def cpu_counters():
    """Process CPU, garbage collections and the cgroup's CPU quota and throttling, for
    the run's timing line (log-only). Reads only local counters; never raises."""
    out = {}
    try:
        times = os.times()
        out["cpu_s"] = times.user + times.system
    except Exception:
        pass
    try:
        stats = gc.get_stats()
        out["gc"] = sum(s["collections"] for s in stats)
        out["gc2"] = stats[-1]["collections"]
    except Exception:
        pass
    # cgroup v2 first, then v1 (whose throttled_time is in nanoseconds).
    for path in ("/sys/fs/cgroup/cpu.stat", "/sys/fs/cgroup/cpu/cpu.stat"):
        try:
            with open(path) as f:
                rows = f.read().splitlines()
        except Exception:
            continue
        for row in rows:
            name, _, value = row.partition(" ")
            if not value.strip().isdigit():
                continue
            if name in ("nr_periods", "nr_throttled", "throttled_usec", "usage_usec"):
                out[name] = int(value)
            elif name == "throttled_time":
                out["throttled_usec"] = int(value) // 1000
        break
    return out


def cpu_quota():
    """The cgroup v2 CPU quota in CPUs (None if unlimited or unreadable)."""
    try:
        with open("/sys/fs/cgroup/cpu.max") as f:
            quota, period = f.read().split()[:2]
        return None if quota == "max" else round(int(quota) / int(period), 2)
    except Exception:
        return None


@dataclass(frozen=True)
class RunResult:
    run_id: str
    status: str
    boundary: int
    chunks: int
    transactions: int
    partial_transaction: bool
    # Reported, never a failure (see preflight.classify): configured tables held because
    # their name no longer resolves to their OID, CDC_TABLES names that were removed or
    # renamed, publication members nobody added, and active tables CDC_TABLES omits.
    held: list = field(default_factory=list)
    stale: list = field(default_factory=list)
    unmanaged: list = field(default_factory=list)
    undeclared: list = field(default_factory=list)


# Replica capture: B (the replay position) and the standby's diagnostics, one read.
# pg_stat_wal_receiver's status and contact columns are NULL without pg_read_all_stats,
# so they're reported "unknown", never "unreachable". The marker and the last replayed
# commit's time are the PRIMARY's clock; now() is the STANDBY's.
STANDBY_READ = (
    f"SELECT {current_lsn(STANDBY)}-'0/0'::pg_lsn,{receive_lsn()}-'0/0'::pg_lsn,"
    "pg_last_xact_replay_timestamp(),"
    "(SELECT status FROM pg_stat_wal_receiver),"
    "(SELECT last_msg_receipt_time FROM pg_stat_wal_receiver),"
    "(SELECT marker FROM {} WHERE id=%s),now()"
)


def _iso(value):
    try:
        return value.isoformat() if value is not None else None
    except AttributeError:
        return None


def _marker_time(text):
    """The DBA job's `clock_timestamp()::text`, parsed best effort (None if not a time)."""
    if not isinstance(text, str):
        return None
    candidate = text.strip().replace(" ", "T", 1)
    if candidate.endswith(("+00", "-00")) or (len(candidate) > 3 and candidate[-3] in "+-"):
        candidate += ":00"
    try:
        return datetime.fromisoformat(candidate).isoformat()
    except ValueError:
        return None


def standby_diagnostics(row, interval):
    """The `standby` record of a run's ops status: advisory, display only."""
    replay, receive, replayed_at, status, contact, marker, observed = row
    replay = None if replay is None else int(replay)
    receive = None if receive is None else int(receive)
    return dict(
        replay_lsn=replay,
        receive_lsn=receive,
        replay_lag_bytes=None if replay is None or receive is None else max(0, receive - replay),
        last_replay_commit_ts=_iso(replayed_at),  # the primary's clock
        receiver_status=status or "unknown",
        upstream_contact_ts=_iso(contact),  # None: unknown (needs pg_read_all_stats)
        heartbeat_marker=marker if isinstance(marker, str) else None,
        heartbeat_ts=_marker_time(marker),  # the primary's clock
        heartbeat_interval_s=interval,
        observed_ts=_iso(observed),  # the standby's clock
    )


def slot_health(con, slot, checkpoint, role=PRIMARY):
    if role == STANDBY:
        # F3: a recovery conflict invalidates a standby's slot. Its columns (conflicting
        # 16+, invalidation_reason 17+) through to_jsonb: no version gate, no error text.
        rows = query(
            con,
            "SELECT plugin,database,active,confirmed_flush_lsn-'0/0'::pg_lsn,"
            "restart_lsn-'0/0'::pg_lsn,wal_status,safe_wal_size,"
            "to_jsonb(s)->>'conflicting',to_jsonb(s)->>'invalidation_reason' "
            "FROM pg_replication_slots s WHERE slot_name=%s",
            [slot],
        )
        if rows and rows[0][7] == "true":
            reason = rows[0][8] or rows[0][5]
            raise SourceMismatch(
                f"replication slot was invalidated by a recovery conflict ({reason}); "
                "resync required"
            )
        rows = [row[:7] for row in rows]
    else:
        rows = query(
            con,
            "SELECT plugin,database,active,confirmed_flush_lsn-'0/0'::pg_lsn,"
            "restart_lsn-'0/0'::pg_lsn,wal_status,safe_wal_size FROM pg_replication_slots "
            "WHERE slot_name=%s",
            [slot],
        )
    if not rows:
        raise SourceMismatch("replication slot disappeared; explicit resync is required")
    plugin, database, active, confirmed, restart, status, safe = rows[0]
    if status == "lost" or confirmed is None or int(confirmed) > checkpoint:
        raise SourceMismatch(
            "slot history is unavailable or ahead of the durable checkpoint; resync required"
        )
    if active:
        raise LeaseBusy("Postgres replication slot already has an active reader")
    return dict(
        confirmed=int(confirmed), restart=int(restart or 0), wal_status=status, safe_wal_size=safe
    )


def catalog_keys(regular, info, *, keys_only=False, role=PRIMARY):
    """(observed keys, the facts of every managed table, checked LSN): one catalog
    statement for all tables (preflight.catalog_facts; only the keys for the start
    read), then the WAL position."""
    facts = catalog_facts(regular, [t["oid"] for t in info.tables], keys_only=keys_only)
    observed = {}
    for t in info.tables:
        if t.get("append"):
            continue  # keyless append capture: no key, no key observation or version
        # A table dropped during the run isn't a key change: omit it, and the next
        # run's preflight holds it. A table that exists without a key still is one.
        if t["oid"] in facts:
            observed[t["oid"]] = facts[t["oid"]]["key"]
    # Read after the keys: a change they missed happened after this position.
    checked = query(regular, current_sql(role))[0][0]
    return observed, facts, int(checked)


def membership(log, info):
    """What run output reports about tables, from preflight's view and the registry."""
    names = {r["relation_id"]: f"{r['schema_name']}.{r['table_name']}" for r in log.registry()}
    return dict(
        held=[
            dict(table=names.get(oid, str(oid)), reason=reason, detail=detail)
            for oid, (reason, detail) in sorted(info.held.items())
        ],
        stale=list(info.stale),
        unmanaged=[f"{t['schema']}.{t['name']}" for t in info.unmanaged],
        undeclared=list(info.undeclared),
    )


def record_catalog_keys(log, regular, info, boundary, role=PRIMARY):
    observed, _, checked = catalog_keys(regular, info, keys_only=True, role=role)  # the start read
    log.record_keys(observed, boundary, checked)


GAP_FOR_XACT_AGE = 1024**3  # read the oldest open transaction's age past this restart gap


def observed_slots(regular, base, current, epoch, extra=(), role=PRIMARY):
    """The current slot and any pending `<base>_e<n>` slot (an interrupted recovery),
    for the operations Dive: one query, matched exactly in Python. Best effort.

    `extra`: exact names of slots to report too, with role `leftover`: the previous
    epoch's slot a recover could not drop yet (explicit, exact
    match, default nothing)."""
    if role == STANDBY:
        # F3: a recovery conflict's columns, so the reason a run holds on is
        # in the Dive's slot detail too (16: conflicting only; 17+: the reason).
        rows = query(
            regular,
            "SELECT slot_name,active,wal_status,safe_wal_size,"
            "confirmed_flush_lsn-'0/0'::pg_lsn,restart_lsn-'0/0'::pg_lsn,"
            f"{slots_now(role)},"
            "to_jsonb(s)->>'conflicting',to_jsonb(s)->>'invalidation_reason' "
            "FROM pg_replication_slots s",
        )
    else:
        rows = query(
            regular,
            "SELECT slot_name,active,wal_status,safe_wal_size,"
            "confirmed_flush_lsn-'0/0'::pg_lsn,restart_lsn-'0/0'::pg_lsn,"
            f"{slots_now(role)} "
            "FROM pg_replication_slots",
        )
    pending = re.compile(re.escape(base) + r"_e(\d+)")
    slots = []
    for row in rows:
        name, active, status, safe, confirmed, restart, now = row[:7]
        conflict = row[7:] if len(row) > 7 and row[7] == "true" else None
        match = pending.fullmatch(name)
        leftover = name != current and name in extra
        if name != current and not match and not leftover:
            continue
        if name == current:
            slot_epoch = epoch
        elif match:
            slot_epoch = int(match.group(1))
        else:  # the previous epoch's slot, still present
            slot_epoch = epoch - 1 if epoch else None
        number = lambda v: None if v is None else int(v)  # noqa: E731
        confirmed, restart, now = number(confirmed), number(restart), number(now)
        slots.append(
            dict(
                name=name,
                epoch=slot_epoch,
                is_current=name == current,
                active=active,
                wal_status=status,
                safe_wal_size=number(safe),
                retained_bytes=None if now is None or restart is None else now - restart,
                lag_bytes=None if now is None or confirmed is None else now - confirmed,
                restart_gap_bytes=None
                if confirmed is None or restart is None
                else confirmed - restart,
                **({"role": "leftover"} if leftover else {}),
                **({"invalidated_by_conflict": conflict[1] or status} if conflict else {}),
            )
        )
    if not any(s["is_current"] for s in slots):
        slots.append(dict(name=current, epoch=epoch, is_current=True, wal_status="missing"))
    for slot in slots:
        if (slot.get("restart_gap_bytes") or 0) > GAP_FOR_XACT_AGE:
            age = query(
                regular,
                "SELECT extract(epoch FROM now()-min(xact_start)) FROM pg_stat_activity "
                "WHERE xact_start IS NOT NULL",
            )[0][0]
            # extract() is NUMERIC: psycopg2 returns a Decimal, which JSON can't encode.
            slot["oldest_xact_age_s"] = None if age is None else float(age)
    return slots


def observe_after_failure(config, slot, observe):
    """A run that failed before reading the slot (preflight refused, the slot is gone):
    read it once on a fresh connection so the operations status shows the slot as it
    is now, not as the last good run saw it. Best effort; never raises."""
    if "slots" in observe:
        return
    if (slot or "").startswith("deleted:"):
        # A source being deleted or deleted: its slot is the delete Flight's;
        # read nothing (the tombstone can't tell deleting from deleted).
        observe["lifecycle"] = "deleting_or_deleted"
        return
    try:
        regular = connect(config)
        try:
            observe["slots"] = observed_slots(
                regular,
                config.slot,
                slot or config.slot,
                observe.get("epoch"),
                role=role_of(config.heartbeat_writer),
            )
            observe["slots_read_now"] = True  # no lease: stamped with the write's time
        finally:
            regular.close()
    except Exception as exc:
        logger.warning("slot observation after a failure failed (%s)", type(exc).__name__)


def run_once(config, log, *, info=None, fault=None, observe=None):
    """The single loop, with only rolled-back v2 guard conflicts made retryable.

    The inner finally must finish first: no read or status write can race its
    worker, and release failures must still propagate. Only format 2 is supported.
    """
    observe = {} if observe is None else observe
    if getattr(log, "format_version", None) != 2:
        raise SourceMismatch("unsupported source protocol; recreate and re-backfill")
    log.release_attempts = None  # This invocation must not inherit a previous cleanup count.
    try:
        return _run_once(config, log, info=info, fault=fault, observe=observe)
    except GuardedIngestConflict as exc:
        if not is_conflict(exc) or log.token is not None:
            raise
        own, initial = observe.get("checkpoint"), observe.get("initial_checkpoint")
        if own is None or initial is None or "run_id" not in observe:
            raise
        try:
            log.forget()
            fresh = log.checkpoint()
            if fresh < own:
                raise exc
            status = "conflict" if own > initial else "busy"
            chunks = observe.get("collected_chunks", 0)
            transactions = observe.get("collected_transactions", 0)
            # The lease is released: this deliberately unguarded status write
            # touches only this run's own row, never another run's checkpoint.
            log.run_status(observe["run_id"], status, "compaction_conflict", boundary=own)
        except BaseException as read_error:
            if read_error is exc:
                raise
            raise exc from read_error
        observe.update(status=status, checkpoint=own, chunks=chunks, transactions=transactions)
        return RunResult(observe["run_id"], status, own, chunks, transactions, False)
    finally:
        observe["cleanup_release_attempts"] = getattr(log, "release_attempts", None)


def _run_once(config, log, *, info=None, fault=None, observe=None):
    """`observe`: a dict this run fills for the operations status (ops.report_ingest):
    run id, lease time, epoch, checkpoint, counts and the slots it read."""
    observe = {} if observe is None else observe
    # Log-only timing lines (explain.ingest_stages sets it): never read for a decision.
    timing = getattr(fault, "timing", None)
    clock = dict(wall=perf_counter(), counters=cpu_counters() if timing else {})
    clock.update(appends=0, frames=0, bytes=0)
    backend = Backend2(log)
    # Preflight only validates the slot; the run uses the one read under the lease.
    if info is None:
        slot = log.registered_slot()
        info = check(config, slot=slot, registry=log.registry())
    log.register(info.identity, info.initial_lsn, info.tables, timeline=info.timeline)
    run_id = uuid.uuid4().hex
    observe["run_id"] = run_id
    try:
        log.acquire(run_id)
    except (LeaseBusy, duckdb.TransactionException) as exc:
        if isinstance(exc, duckdb.TransactionException) and "conflict" not in str(exc).lower():
            raise
        log.forget()  # no lease to release; the next run reads its registration again
        return RunResult(run_id, "busy", log.checkpoint(), 0, 0, False)
    replication = regular = executor = inflight = None
    error = None
    try:
        boundary = log.acquired_lsn  # the checkpoint read with the lease
        observe.update(as_of=log.acquired_at_us, epoch=log.epoch, checkpoint=boundary)
        observe.update(initial_checkpoint=boundary, collected_chunks=0, collected_transactions=0)
        if fault:
            fault("after_lease")
        recovery = backend.recover()
        # Holds, cleared holds and unmanaged members, under the lease (nothing in
        # steady state); unmanaged rows join key_columns before the spool needs them.
        log.record_tables(info.held, info.present, info.unmanaged, info.held_names)
        reported = membership(log, info)  # from the cached registry, before finish clears it
        regular = connect(config)
        # The source label's withhold check (ops only): this connection's effective
        # values, kept in memory; never raises, fails closed.
        observe["label_guard"] = Guard.from_connection(regular)
        # Keys read now, under the lease, held for everything captured so far.
        # Preflight's view predates the lease and may be stale.
        record_catalog_keys(log, regular, info, boundary, role_of(config.heartbeat_writer))
        try:
            observe["slots"] = observed_slots(
                regular,
                config.slot,
                log.slot_name,
                log.epoch,
                extra=log.previous_slots(),
                role=role_of(config.heartbeat_writer),
            )
        except Exception as exc:  # observation never fails a run
            logger.warning("slot observation failed (%s)", type(exc).__name__)
        try:
            slot_health(regular, log.slot_name, boundary, role_of(config.heartbeat_writer))
            if fault:
                fault("after_slot_check")
        except LeaseBusy:
            log.run_status(run_id, "busy", "source replication reader still active")
            return RunResult(run_id, "busy", boundary, 0, 0, False)
        if config.heartbeat_writer == "external":
            # A standby: no heartbeat write (the primary's job writes it).
            # B, the replay position, is read under the lease after the slot check;
            # the run drains at the first unit ending after it.
            marker = None
            # The standby's diagnostics in B's own statement: expressions that can't raise (review
            # S1), parsed in Python; display only, never read for a decision.
            row = query(
                regular,
                sql.SQL(STANDBY_READ).format(qualified(config.heartbeat)),
                [f"external:{config.source_id}"],
            )[0]
            past = int(row[0])
            observe["standby"] = standby_diagnostics(row, config.heartbeat_interval_seconds)
        else:
            past = None
            marker = uuid.uuid4().hex
            query(
                regular,
                sql.SQL(
                    "INSERT INTO {} (id,marker) VALUES (%s,%s) "
                    "ON CONFLICT(id) DO UPDATE SET marker=excluded.marker"
                ).format(qualified(config.heartbeat)),
                [config.source_id, marker],
            )

        # Before the first status and whenever it has lapsed, a status is preceded by
        # a guarded write under this lease (which re-checks the epoch and slot).
        # Safe as a cached flag because a transition needs either a new slot name
        # (slot loss) or this slot inactive (skip); revisit it if either changes.
        # It runs on the local monotonic clock while the lease runs on the database
        # clock: the lease/3 margin covers drift and ordinary pauses. A longer pause
        # between check and send is covered by those slot preconditions.
        # Every fence base is a time taken before the guard's statement ran (a lower
        # bound of its current_timestamp), so the fence never outlives the lease the
        # guard set, however long the append or renew took.
        fenced_until = 0.0
        uploading = False  # a renew is being waited on (the worker owns the connection)

        def fenced(at):
            nonlocal fenced_until
            fenced_until = at + config.lease_seconds / 3

        def fence(lsn):
            # Called for every new status value: it must not exceed the boundary the
            # last guarded append returned (or the checkpoint read under the lease).
            nonlocal last_renew
            if lsn > boundary:
                raise RuntimeError("replication status would exceed the durable checkpoint")
            if time.monotonic() >= fenced_until:
                if uploading or inflight is not None:
                    # The destination connection is the worker's until the upload ends.
                    raise FenceLost("ingest lease fence lapsed during an upload")
                at = time.monotonic()
                log.renew()
                fenced(at)
                last_renew = at  # this renew counts: no second one right after

        # This stream sends every status update itself (see replication.py), so
        # feedback never exceeds the durable checkpoint.
        replication = ReplicationStream(connection_options(config), fence=fence)
        pg_ca.note_in_use(replication.conn.ssl_in_use)  # the negotiated state (cdc_ops)
        # pgoutput renders values in this session: check its formats on every run,
        # not only in preflight (a different driver could set them differently).
        backend.verify_session(replication.show, regular)
        check_formats(replication.show)
        if fault:
            fault("before_stream")
        replication.start(log.slot_name, boundary, config.publication)
        if fault:
            fault("after_start")
        log.run_status(run_id, "running")
        spool = backend.spool(
            replace(config, epoch=log.epoch),
            log.key_columns(),
            info.heartbeat_oid,
            marker,
            recovery,
        )
        executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="cdc-destination")
        started = last_flush = last_status = last_renew = time.monotonic()
        chunks = transactions = 0
        pending, pending_bytes, pending_units = [], 0, 0

        def feedback():
            nonlocal last_status
            replication.acknowledge(boundary)
            last_status = time.monotonic()

        def wait(task):
            # Blocks until the worker's future finishes, sending repeat statuses. Only
            # this thread ever operates the replication connection; the worker owns the
            # destination connection until the future finishes. No keepalive is read
            # while blocked here (a full batch waiting on the one in flight, the drain,
            # the end of the run, a renew): these status updates alone keep the
            # walsender alive, so wal_sender_timeout must exceed ~2x status_seconds.
            # Only the non-blocking in-flight state reads (and answers) keepalives.
            deadline = started + config.runtime_seconds + 60
            while True:
                try:
                    return task.result(timeout=min(config.status_seconds, 0.2))
                except FutureTimeout:
                    if time.monotonic() - last_status >= config.status_seconds:
                        feedback()
                    if time.monotonic() > deadline:
                        log.con.interrupt()
                        raise TimeoutError("destination write exceeded runtime grace period")

        def renew():
            nonlocal uploading, last_renew
            at = time.monotonic()
            uploading = True
            try:
                wait(executor.submit(log.renew))
            finally:
                uploading = False
            fenced(at)
            last_renew = at

        def append(batch):
            # On the worker: the time before the append's guard is its fence base.
            at = time.monotonic()
            if timing is None:
                return log.append(batch, fault=fault), at, None
            # Timing only: the stage marks pass every stage on to `fault` unchanged.
            began, cpu, marks = perf_counter(), thread_time(), {}

            def marked(stage):
                marks[stage] = perf_counter()
                if fault:
                    fault(stage)

            reached = log.append(batch, fault=marked)
            marks.update(began=began, end=perf_counter())
            return reached, at, dict(marks=marks, cpu=thread_time() - cpu)

        def main_clock():
            return perf_counter(), thread_time()

        # The batch being built: when it started (the last submit) and when it was full.
        filling = dict(start=main_clock(), full=None)

        def timed(batch, measured, waited, info):
            # One line per collected append (main thread). Never raises.
            try:
                marks = measured["marks"]

                def span(a, b):
                    return marks[b] - marks[a] if a in marks and b in marks else None

                frames = sum(getattr(i, "count", 0) for i in batch if hasattr(i, "table"))
                size = sum(getattr(i, "bytes", 0) for i in batch if hasattr(i, "table"))
                clock["appends"] += 1
                clock["frames"] += frames
                clock["bytes"] += size
                timing(
                    "append",
                    n=clock["appends"],
                    items=len(batch),
                    frames=frames,
                    bytes=size,
                    cum_bytes=clock["bytes"],
                    why=info["why"],
                    # main thread: building this batch (W when why=full), then waiting on
                    # the one in flight
                    fill=info["fill"],
                    fill_cpu=info["fill_cpu"],
                    blocked=info["blocked"],
                    # worker: queue delay, then the append itself by phase
                    queue=marks["began"] - info["submitted"],
                    wall=marks["end"] - marks["began"],
                    cpu=measured["cpu"],
                    guard=span("began", "before_payload"),
                    payload=span("before_payload", "after_payload"),
                    ledger=span("after_payload", "before_commit"),
                    commit=span("before_commit", "after_commit"),
                    # main thread: blocked in collect on this append
                    wait=waited,
                    lag=measured.get("lag"),
                )
            except Exception:
                pass

        def submit():
            # Pipelined flush: one append in flight while the loop builds the next
            # batch. Each batch may overshoot by one iteration's emitted payload;
            # v2's budget covers Arrow buffers, separately from protocol wire bytes.
            # The in-progress spool chunk and descriptor cache are additional, not RSS.
            # An open transaction's chunks keep flushing with the batch; holding them until
            # commit would make memory grow with the transaction.
            nonlocal inflight, pending, pending_bytes, pending_units, last_flush
            if fault:
                fault("before_append")
            info = None
            if timing is not None:
                now, cpu = main_clock()
                full = filling["full"] or (now, cpu)
                start = filling["start"]
                info = dict(
                    why=why,
                    fill=full[0] - start[0],
                    fill_cpu=full[1] - start[1],
                    blocked=now - full[0],
                    submitted=now,
                )
            inflight = (executor.submit(append, pending), pending, info)
            pending, pending_bytes, pending_units = [], 0, 0
            last_flush = time.monotonic()
            if timing is not None:
                filling.update(start=main_clock(), full=None)

        def collect():
            # The order is the invariant: collect, fence, acknowledge, and only then may
            # the next batch be submitted. Every status value increase therefore happens
            # with no future outstanding, so the fence never renews on this thread
            # while the worker owns the destination connection.
            nonlocal inflight, boundary, chunks, transactions, last_renew
            task, batch, info = inflight
            waiting = perf_counter()
            boundary, at, measured = wait(task)
            waited = perf_counter() - waiting
            if measured is not None:
                measured["lag"] = max(0.0, waiting - measured["marks"]["end"])
            inflight = None
            observe["checkpoint"] = boundary  # durable: a later failure still shows it
            fenced(at)  # the append's guard committed with it
            last_renew = at
            if fault:
                fault("after_append")
            if fault:
                fault("before_feedback")
            feedback()
            for item in batch:
                item_chunks, item_transactions = backend.counts(item)
                chunks += item_chunks
                transactions += item_transactions
            observe.update(collected_chunks=chunks, collected_transactions=transactions)
            through = getattr(backend, "durable_through", None)
            if through is not None:
                observe["durable_through"] = through  # (epoch, unit end, ts or None)
            if measured is not None and info is not None:
                timed(batch, measured, waited, info)  # after the batch's durable line

        drained = False
        why = "time"  # why the next submit happens (timing lines only)
        while time.monotonic() - started < config.runtime_seconds:
            if inflight is not None and inflight[0].done():
                collect()
            # Waits up to the poll interval when nothing has arrived. While a batch is
            # in flight, a short wait (never 0: that would spin against the worker).
            if inflight is not None:
                timeout = min(0.02, config.status_seconds)
            else:
                timeout = 0 if pending else min(0.1, config.status_seconds)
            message = replication.read(timeout)
            items = [*spool.feed(message.payload)] if message else []
            # Poll even on idle reads and while the worker owns the destination.
            # These hooks only operate on memory; emitted chunks queue in order.
            items.extend(backend.poll(spool))
            pending.extend(items)
            budgets = [backend.budget(item) for item in items]
            pending_bytes += sum(size for size, _ in budgets)
            pending_units += sum(count for _, count in budgets)
            drained = backend.drained(items) if past is None else backend.drained(items, past)
            full = pending_bytes >= config.batch_bytes or pending_units >= config.batch_events
            if pending and (
                full or drained or time.monotonic() - last_flush >= config.flush_seconds
            ):
                why = "full" if full else "drained" if drained else "time"
                if timing is not None and (full or drained) and filling["full"] is None:
                    filling["full"] = main_clock()  # W ends here, before any backpressure
                if inflight is None:
                    submit()
                elif full or drained:
                    collect()  # backpressure: the next batch waits on the one in flight
                    submit()
                # else time alone: keep reading; it is submitted once this one is collected
            if drained:
                break  # the marker's batch is collected below, before finish
            if time.monotonic() - last_status >= config.status_seconds:
                feedback()
            if inflight is None and time.monotonic() - last_renew >= min(
                30, config.lease_seconds / 3
            ):
                renew()
        pending.extend(backend.close_at_boundary(spool))
        if inflight is not None:
            collect()
        if pending:
            why = "end"
            submit()
            collect()
        # Re-read keys after the last durable flush: a key change during this
        # run is caught before apply can pass this run's checkpoint.
        observed, facts, checked = catalog_keys(
            regular, info, role=role_of(config.heartbeat_writer)
        )
        partial = backend.partial(spool)
        # Keys, progress, the final status and the release commit together. The
        # lease is released before cleanup closes the stream, so its last status
        # update is sent after the release. That status only repeats the fenced
        # boundary, and while this stream holds the slot active no successor can
        # use it (slot_health and new-epoch report busy), the precondition the
        # fence comment above relies on. Re-check if close() or those
        # preconditions change.
        if fault:
            fault("before_finish")
        status = log.finish(
            run_id,
            observed,
            boundary,
            checked,
            "drained" if drained else "capped",
            backend.progress(spool),
            lambda status: (
                "large_transaction: replay made no new durable progress"
                if status == "needs_action"
                else "partial transaction retained"
                if partial
                else ""
            ),
            facts=facts,
        )
        # catalog_checks' warnings (a relfilenode change alone): counted, never held.
        observe["catalog_warnings"] = getattr(log, "catalog_warnings", 0)
        if observe["catalog_warnings"]:
            logger.warning(
                "catalog check: %d table(s) rewritten without a hold (VACUUM FULL, CLUSTER, "
                "TRUNCATE or a same-type rewrite)",
                observe["catalog_warnings"],
            )
        observe["checkpoint"] = boundary  # the run's resulting checkpoint, not the lease's
        return RunResult(run_id, status, boundary, chunks, transactions, partial, **reported)
    except BaseException as exc:
        error = exc
        # Never persist raw exception strings: connection failures can contain endpoints
        # or user-supplied secrets. The caller gets a typed exception and a safe run id.
        raise
    finally:
        # Every step runs even if an earlier one fails: the executor first (nothing
        # else may use the destination connection), then the streams, then the run's
        # status and the lease. An original exception is preserved; otherwise the
        # first cleanup failure is raised after all steps ran.
        failure = None

        def settle():
            # A failure on this thread may leave an append in flight: wait for it within
            # the same deadline, interrupting it there. If it committed, the status shows
            # the checkpoint it reached.
            task = inflight[0]
            remaining = started + config.runtime_seconds + 60 - time.monotonic()
            try:
                reached, _, _ = task.result(timeout=max(0.0, remaining))
            except FutureTimeout:
                logger.error("destination write interrupted at the deadline")
                log.con.interrupt()
            except Exception:
                pass  # the worker's own failure; the run's error is already set
            else:
                observe["checkpoint"] = reached

        steps = [
            inflight and settle,
            executor and (lambda: executor.shutdown(wait=True, cancel_futures=True)),
            replication and replication.close,
            regular and regular.close,
            error and (lambda: log.run_status(run_id, "failed", type(error).__name__)),
            log.release,
        ]
        for step in steps:
            if not step:
                continue
            try:
                step()
            except Exception as exc:
                failure = failure or exc
                logger.error("ingest cleanup step failed; the lease expires automatically")
        if timing is not None:
            try:
                before, after = clock["counters"], cpu_counters()
                timing(
                    "run",
                    wall=perf_counter() - clock["wall"],
                    appends=clock["appends"],
                    frames=clock["frames"],
                    bytes=clock["bytes"],
                    quota=cpu_quota(),
                    cpus=len(os.sched_getaffinity(0)),
                    **{k: after[k] - before[k] for k in after if k in before},
                )
            except Exception:
                pass
        if failure is not None and error is None:
            raise failure
