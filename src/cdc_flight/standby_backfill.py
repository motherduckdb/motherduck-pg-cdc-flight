"""Backfill from a standby source (replica capture, CDC_HEARTBEAT_WRITER=external).

`backfill.standby_backfill()` imports this module lazily, only for an external source,
so a primary source's backfill never loads it. Every function takes the Backfill as its
first argument; no module imports this one at module level.
"""

import logging
import threading
import time
from dataclasses import replace

import psycopg2
import psycopg2.errors

from cdc_flight.backfill import GenerationFailed
from cdc_flight.ingest_common import SourceMismatch
from cdc_flight.pg_connect import connect
from cdc_flight.positions import STANDBY, current_lsn, current_sql
from cdc_flight.retention import HistoryDropped, PinMissing

logger = logging.getLogger(__name__)

# A chunk or catalog read ended by a recovery conflict (or a lost connection) is retried
# in the run this many times, on new sessions, each with half the chunk's rows (down to
# the floor).
CONFLICT_RETRIES = 3
CONFLICT_FLOOR_ROWS = 100
CONFLICT_DETAIL = (
    "recovery conflicts or lost connections on the standby ended the snapshot read {n} times "
    "in this run; it resumes at its last chunk (see max_standby_streaming_delay)"
)


class ReplayBehind(Exception):
    """The standby's replay position is below the generation's L (a lagging standby):
    the run ends `waiting_for_replay`, nothing changed."""


def recovery_conflict(exc, con):
    """A recovery conflict by SQLSTATE (40001: canceling the statement, or terminating
    an idle-in-transaction session), or the session found closed (a FATAL delivered
    while idle arrives as a bare closed connection).
    Never by message text."""
    return getattr(exc, "pgcode", None) == "40001" or (con is not None and con.closed != 0)


# Keyed scan ----------------------------------------------------------------


def read_chunk(backfill, statement, params, gen):
    """The chunk between two replay readings in its transaction. Before: still a standby,
    replay at or past L (else `waiting_for_replay`) and not below the persisted bound.
    After: replay R, read after the chunk's snapshot, so R bounds what the snapshot saw.
    Returns (rows, text bytes, byte-capped, R)."""
    con, bound = backfill.snapshot_pg, gen["replay_bound"] or 0
    try:
        with con:
            backfill._chunk_settings(con)
            with con.cursor() as cur:
                cur.execute(f"SELECT pg_is_in_recovery(),{current_lsn(STANDBY)}-'0/0'::pg_lsn")
                standby, before = cur.fetchone()
            if not standby:
                raise GenerationFailed(
                    "the source is no longer a standby (promoted?): promotion and failover "
                    "aren't supported; resync"
                )
            before = int(before)
            if before < bound:
                raise GenerationFailed(
                    f"the standby's replay position went backwards ({before} < {bound}): "
                    "another or a rewound instance; resync"
                )
            if before < gen["pin_lsn"]:
                raise ReplayBehind(before)
            rows, size, capped = backfill._fetch_chunk(con, statement, params)
            with con.cursor() as cur:
                cur.execute(current_sql(STANDBY))
                after = int(cur.fetchone()[0])
    except (psycopg2.errors.UndefinedColumn, psycopg2.errors.UndefinedTable) as exc:
        raise GenerationFailed(f"source table changed during backfill: {type(exc).__name__}")
    if after < before:
        raise GenerationFailed(
            f"the standby's replay position went backwards ({after} < {before}): another "
            "or a rewound instance; resync"
        )
    return rows, size, capped, after


def finish_scan(backfill, gen, generation_id, token):
    """Scan complete: B is the persisted bound; no marker is written."""
    if gen["replay_bound"] is None:
        raise GenerationFailed("the scan finished without a replay bound")
    with backfill.held(generation_id, token) as lease:
        lease.finish_scan(gen, wal_before_marker=gen["replay_bound"])


# Finality, identity and conflicts (keyed and append) ------------------------------------


def find_past(backfill, gen):
    """Rule U: the end of the first complete unit with end > B
    (strict), in the generation's epoch, through UnitReader, or None."""
    epoch, bound = gen["epoch"], gen["wal_before_marker"]
    after = max(gen["pin_lsn"], backfill._searched.get(gen["generation_id"], 0))
    try:
        high = backfill.reader.head().high
        while after < high:
            page = backfill._page(epoch, after, high)
            for unit in page:
                if unit.end > bound:
                    return unit.end
            after = page[-1].end
            backfill._searched[gen["generation_id"]] = after
    except (SourceMismatch, HistoryDropped, PinMissing) as exc:
        raise GenerationFailed(str(exc)) from exc
    return None


def check_identity(backfill):
    """Once per run: IDENTIFY_SYSTEM's system id and timeline
    must equal the registered source's; else the generation fails."""
    registered = backfill.execute(
        "SELECT system_id,(to_json(src)->>'timeline_id')::BIGINT FROM {s}source src WHERE id=1"
    ).fetchone()
    repl = connect(backfill.config, replication=True)
    try:
        with repl.cursor() as cur:
            cur.execute("IDENTIFY_SYSTEM")
            system_id, timeline = cur.fetchone()[:2]
    finally:
        repl.close()
    if registered is None or (str(system_id), int(timeline)) != (
        str(registered[0]),
        registered[1],
    ):
        raise GenerationFailed(
            "the standby's identity or timeline differs from the registered source's: "
            "promotion and failover aren't supported; resync"
        )


def retry_conflict(backfill, exc, con):
    """A recovery conflict (by SQLSTATE or a closed session) ends this attempt: the
    sessions are reopened (a terminated session is gone, and the next chunk recreates its
    cursor at the persisted resume key), the chunk limit halves, and the caller retries
    until CONFLICT_RETRIES. 57P04 (database dropped) fails the generation. False for any
    other error."""
    if getattr(exc, "pgcode", None) == "57P04":  # never retryable
        raise GenerationFailed(
            "the source database was dropped (a recovery conflict that can't be retried)"
        ) from exc
    if not recovery_conflict(exc, con):
        return False
    backfill._conflicts += 1
    backfill.close()
    # Never above the configured size: the floor only stops the halving.
    floor = min(CONFLICT_FLOOR_ROWS, backfill.config.chunk_rows)
    backfill._chunk_rows = max(floor, backfill._chunk_rows // 2)
    logger.warning(
        "recovery conflict on the standby (%s); retry %d",
        getattr(exc, "pgcode", None),
        backfill._conflicts,
    )
    if backfill._conflicts <= CONFLICT_RETRIES:
        time.sleep(2 ** (backfill._conflicts - 1))
    return True


def after_error(backfill, exc, con, table, generation_id):
    """A PostgreSQL read's error in run_table: the result to end the run with, None to
    retry, or the error re-raised (a GenerationFailed reaches run_table's handler)."""
    if isinstance(exc, ReplayBehind):
        return backfill.result(table, generation_id, "waiting_for_replay")
    if isinstance(exc, psycopg2.Error) and retry_conflict(backfill, exc, con):
        if backfill._conflicts > CONFLICT_RETRIES:
            result = backfill.result(
                table, generation_id, backfill.generation(generation_id)["state"]
            )
            return replace(result, detail=CONFLICT_DETAIL.format(n=backfill._conflicts))
        return None
    raise exc


# Append snapshot -----------------------------------------

# How long each escalation step after the cancel waits for the creation to return
# (spike item 6: the CancelRequest alone returned it at once on 16, 17 and 18).
ESCALATION_GRACE_SECONDS = 5


WAITING_DETAIL = (
    "the snapshot's slot was not created on the standby within {seconds:g} s: it waits for the "
    "primary to log a standby snapshot. Check the heartbeat job runs on the primary, or ask "
    "the DBA to run SELECT pg_log_standby_snapshot(); there (it helps only once no "
    "transaction is open on the primary). The backfill retries; nothing was copied."
)


IMPORT_RETRY_DETAIL = (
    "a recovery conflict or a lost connection on the standby ended the snapshot's import "
    "before any row was copied; the next run takes a new snapshot"
)


class WaitingForPrimary(Exception):
    """The exported-snapshot slot's creation waited past its bound for the primary's
    running-xacts record: nothing was written; the run ends `waiting_for_primary`."""


def _create_export_slot(config, repl, slot, wait):
    """CREATE_REPLICATION_SLOT … TEMPORARY … EXPORT_SNAPSHOT on the standby, bounded (review
    M1, never a socket close): the CancelRequest at `wait`, then pg_cancel_backend and
    pg_terminate_backend on the walsender's pid, each a grace later. Returns (C, snapshot)
    or raises WaitingForPrimary; a cancelled creation leaves no slot (spike item 6)."""
    from cdc_flight import append_backfill as ab

    pid, done, fired = repl.get_backend_pid(), threading.Event(), []

    def signal(function):
        con = None
        try:
            con = connect(config)
            ab.query(con, f"SELECT {function}(%s)", [pid])
        except psycopg2.Error:
            pass  # best effort: the next step (or the lease) still bounds it
        finally:
            if con is not None:
                con.close()

    steps = (
        (wait, repl.cancel),
        (wait + ESCALATION_GRACE_SECONDS, lambda: signal("pg_cancel_backend")),
        (wait + 2 * ESCALATION_GRACE_SECONDS, lambda: signal("pg_terminate_backend")),
    )

    def run(step):
        if not done.is_set():
            fired.append(step)
            step()

    timers = [threading.Timer(max(at, 0.1), run, [step]) for at, step in steps]
    for timer in timers:
        timer.daemon = True
        timer.start()
    try:
        with repl.cursor() as cur:
            cur.execute(
                f"CREATE_REPLICATION_SLOT {slot} TEMPORARY LOGICAL pgoutput (SNAPSHOT 'export')"
            )
            _, point, snapshot, _ = cur.fetchone()
        if fired:  # a step fired as it succeeded; the exporter may be dying
            raise WaitingForPrimary()
    except psycopg2.OperationalError as exc:  # QueryCanceled, or terminated (closed)
        if not fired:
            raise  # not our bound: a real failure
        raise WaitingForPrimary() from exc
    except psycopg2.errors.InsufficientPrivilege as exc:
        raise GenerationFailed(ab.CAPABILITY) from exc
    finally:
        done.set()
        for timer in timers:
            timer.cancel()
    return ab.lsn_int(point), snapshot


def append_copy(backfill, table, gen, token, *, deadline=None, fault=None):
    """External order: the bounded slot and the import come BEFORE
    `copy_started`, so a slot-wait timeout writes nothing (the generation stays
    `snapshotting`, retryable). Then B is read on the import session (≥ C), the exporter
    closes (the import keeps the snapshot), copy_started, the copy, and finish_scan with
    C and B (no marker). Returns None, or the `waiting_for_primary` result."""
    from cdc_flight import append_backfill as ab

    config, generation_id = backfill.config, gen["generation_id"]
    wait = min(config.slot_wait_seconds, ab.SLOT_WAIT_SECONDS, config.lease_seconds - 1)
    if deadline is not None:
        wait = min(wait, deadline - time.monotonic())
    if wait <= 0:
        return backfill.result(table, generation_id, "snapshotting")
    repl = imp = None
    copying = False  # copy_started is durable: from here a lost snapshot fails
    try:
        try:
            repl = connect(config, replication=True, keepalive=True)
        except psycopg2.Error as exc:
            raise GenerationFailed(ab.connect_refusal(exc)) from exc
        try:
            snapshot_lsn, snapshot = _create_export_slot(
                config, repl, ab.SLOT_PREFIX + generation_id[:12], wait
            )
        except WaitingForPrimary:
            result = backfill.result(table, generation_id, "waiting_for_primary")
            return replace(result, detail=WAITING_DETAIL.format(seconds=wait))
        if fault:
            fault("after_slot")
        if snapshot_lsn < gen["pin_lsn"]:
            raise GenerationFailed("the snapshot's consistent point is below the pin")
        imp = connect(config, keepalive=True)
        ab.check_formats(lambda n: ab.query(imp, "SHOW " + n)[0][0])
        # REPEATABLE READ (SERIALIZABLE is refused on a standby), no transaction before it.
        imp.set_session(isolation_level="REPEATABLE READ", readonly=True, autocommit=False)
        cur = imp.cursor()
        cur.execute("SET TRANSACTION SNAPSHOT %s", [snapshot])
        cur.execute("SET LOCAL idle_in_transaction_session_timeout=0")
        cur.execute(f"SELECT pg_is_in_recovery(),{current_lsn(STANDBY)}-'0/0'::pg_lsn")
        standby, bound = cur.fetchone()
        if not standby:
            raise GenerationFailed("the source is no longer a standby; resync")
        bound = int(bound)
        if bound < snapshot_lsn:
            raise GenerationFailed(f"the standby's replay position {bound} is below C")
        repl.close()  # F2: the export dies with it; the import keeps the snapshot
        repl = None
        if fault:
            fault("after_import")
        with backfill.held(generation_id, token) as lease:
            lease.progress_scan(gen, copy_started=True)
        copying = True
        gen = backfill.generation(generation_id)
        why = ab.source_refusal(imp, config, gen["relation_id"], gen["columns"])
        if why:
            raise GenerationFailed(f"before the snapshot, the source table {why}")
        if deadline is not None:
            size = ab.query(imp, "SELECT pg_table_size(%s)", [gen["relation_id"]])[0][0]
            if time.monotonic() + size / ab.COPY_RATE > deadline:
                raise GenerationFailed("the copy can't finish in this run: use start-from-log")
        gen = ab._copy_rows(backfill, imp, cur, table, gen, token, deadline, fault)
        with backfill.held(generation_id, token) as lease:
            lease.finish_scan(
                gen, scan_done=True, snapshot_lsn=snapshot_lsn, wal_before_marker=bound
            )
        imp.commit()
        if fault:
            fault("after_scan_done")
    except psycopg2.Error as exc:
        if getattr(exc, "pgcode", None) == "57P04":
            raise GenerationFailed("the source database was dropped") from exc
        session = imp if imp is not None else repl
        if not copying and recovery_conflict(exc, session):
            # Before copy_started nothing was copied, so it is retryable: the
            # generation stays snapshotting and the next run takes a new slot.
            result = backfill.result(table, generation_id, "snapshotting")
            return replace(result, detail=IMPORT_RETRY_DETAIL)
        if recovery_conflict(exc, imp):
            raise GenerationFailed(
                "the snapshot copy was cancelled by a recovery conflict or a lost connection "
                "on the standby; resync, or raise max_standby_streaming_delay"
            ) from exc
        raise GenerationFailed(f"snapshot copy failed: {type(exc).__name__}") from exc
    except ab.DATA_ERRORS as exc:
        raise GenerationFailed(f"{type(exc).__name__} in the snapshot copy") from exc
    finally:
        for con in (repl, imp):
            if con is not None:
                try:
                    con.close()
                except Exception:
                    pass
    return None
