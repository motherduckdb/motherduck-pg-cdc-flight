"""Source epoch transitions: the explicit way past a gap in the change log.

Two causes end an epoch:
- `slot_lost`: the slot is gone, lost, or ahead of the durable checkpoint. A new
  slot `<base>_e<n>` is created (or an unconsumed one from a crashed attempt is
  reused) and the new epoch starts at its consistent point.
- `skip`: an operator abandons WAL that can't be ingested. The current slot is
  advanced to `to_lsn` and keeps its name.

The PostgreSQL step comes first and is idempotent; the metadata transaction
(IngestLog.begin_epoch) follows under the ingest lease. Nothing here terminates a
backend or drops an active slot: an active slot is "busy, retry".

Stale workers of the old epoch are fenced twice: their lease guard fails on every
write, and the slot they read is either dropped (slot loss, new name) or can only
move forward past anything they would acknowledge (skip).
"""

import time
from dataclasses import dataclass

import psycopg2.errors

from cdc_flight.config import identifier, lsn_text
from cdc_flight.ingest_log import REASONS, LeaseBusy, SourceMismatch, leased
from cdc_flight.positions import PRIMARY, flush_sql, role_of
from cdc_flight.preflight import STATEMENT_TIMEOUT, connect, query


class EpochError(RuntimeError):
    """A transition that is refused; nothing was changed by this call."""


@dataclass(frozen=True)
class EpochResult:
    # "begun", "already" (an earlier call did it), "busy" (the lease or a slot reader)
    # or "waiting" (a bounded PostgreSQL wait ran out; nothing changed)
    status: str
    epoch: int
    slot: str
    start_lsn: int
    abandoned: tuple = ()  # (from, to) for a skip
    detail: str = ""
    leftover: str | None = None  # the previous epoch's slot, still present
    pending: str | None = None  # a slot this run created and kept
    requested_to_lsn: int | None = None  # v2 request; start_lsn is PG's actual landing
    clamped: bool = False


def slot_name_for(base, epoch):
    """The slot a slot-loss transition to `epoch` creates."""
    name = f"{base}_e{epoch}"
    try:
        return identifier(name)
    except ValueError:
        raise EpochError("the new epoch's slot name exceeds 63 bytes") from None


def slot_info(con, name):
    rows = query(
        con,
        "SELECT plugin,database,active,confirmed_flush_lsn-'0/0'::pg_lsn,wal_status "
        "FROM pg_replication_slots WHERE slot_name=%s",
        [name],
    )
    if not rows:
        return None
    plugin, database, active, confirmed, status = rows[0]
    return dict(
        plugin=plugin,
        database=database,
        active=active,
        confirmed=None if confirmed is None else int(confirmed),
        lost=status == "lost",
    )


def identify(config):
    """(system_id, timeline_id) from the replication protocol, as preflight reads it."""
    repl = connect(config, replication=True)
    try:
        with repl.cursor() as cur:
            cur.execute("IDENTIFY_SYSTEM")
            system_id, timeline, _, _ = cur.fetchone()
        return str(system_id), int(timeline)
    finally:
        repl.close()


def _act(con, statement, args):
    """Drop or advance a slot. A reader that attached after the activity check makes
    it ObjectInUse: busy, retry, as for a slot that was already active."""
    try:
        query(con, statement, args)
    except psycopg2.errors.ObjectInUse:
        raise LeaseBusy("replication slot is active; retry when its reader has stopped") from None


def _usable(con, slot, database):
    if slot["plugin"] != "pgoutput" or slot["database"] != database:
        raise EpochError("slot must use pgoutput and belong to this database")
    if slot["active"]:
        raise LeaseBusy("replication slot is active; retry when its reader has stopped")


# Bounded waits: slot creation waits for every running source transaction; an
# advance decodes the skipped range. Both stay well inside an epoch Flight's 600 s.
CREATE_SECONDS = 240
# A standby: slot creation waits for the primary's running-xacts
# record; the operator's fix is on the primary.
STANDBY_WAITING = (
    "creating the new slot on the standby waits for the primary to log a standby snapshot: "
    "check the heartbeat job runs on the primary, or ask the DBA to run SELECT "
    "pg_log_standby_snapshot(); there (it helps only once no transaction is open on the "
    "primary); then retry"
)


def same_timeline(config, log, timeline):
    """A standby source: IDENTIFY_SYSTEM's timeline must be the registered
    one; promotion and failover aren't supported. Primary mode reads nothing here."""
    if config.heartbeat_writer != "external":
        return
    recorded = log.execute(
        "SELECT (to_json(src)->>'timeline_id')::BIGINT FROM {s}source src WHERE id=1"
    ).fetchone()
    if recorded and recorded[0] is not None and recorded[0] != timeline:
        raise SourceMismatch(
            f"the standby's timeline changed ({recorded[0]} -> {timeline}): promotion or "
            "failover isn't supported; resync the source"
        )


# A cancelled pg_replication_slot_advance keeps no progress (PG16 confirms only after its
# decode loop), so a skip advances in steps whose progress persists.
ADVANCE_STEP_BYTES = 32 * 1024 * 1024
ADVANCE_STEP_SECONDS = 120
ADVANCE_BUDGET_SECONDS = 400
_clock = time.monotonic  # injectable for the budget test


class Waiting(RuntimeError):
    """A bounded wait ran out before anything changed: retry later."""


def _bounded(con, seconds, statement, args, message):
    query(con, f"SET statement_timeout='{int(seconds * 1000)}ms'")
    try:
        query(con, statement, args)
    except psycopg2.errors.QueryCanceled:
        raise Waiting(message) from None
    finally:
        query(con, f"SET statement_timeout='{STATEMENT_TIMEOUT}'")


def observe(con, config, slot, checkpoint, new_epoch, reason, to_lsn, database, *, unit_position):
    """Read only: what a transition would do, as the decision fields its preview hashes,
    or EpochError (refused) / LeaseBusy (a reader is attached). Nothing is changed."""
    current = slot_info(con, slot)
    observed = dict(
        epoch=new_epoch - 1,
        reason=reason,
        slot=slot,
        checkpoint=checkpoint,
        to_lsn=to_lsn,
        slot_present=current is not None,
        slot_active=bool(current and current["active"]),
        slot_lost=bool(current and current["lost"]),
    )
    if (
        unit_position["source_epoch"] != new_epoch - 1
        or unit_position["confirmed_end"] != checkpoint
    ):
        raise SourceMismatch("source unit position changed during epoch observation")
    observed.update(unit_position)
    if reason == "slot_lost":
        if current and current["active"]:
            raise LeaseBusy("replication slot is active; retry when its reader has stopped")
        if (
            current
            and not current["lost"]
            and current["confirmed"] is not None
            and current["confirmed"] <= checkpoint
        ):
            raise EpochError("the slot still holds the log from the checkpoint; nothing to recover")
        name = slot_name_for(config.slot, new_epoch)
        new = slot_info(con, name)
        if new is not None:
            if new["lost"]:
                raise EpochError("the new epoch's slot is lost; drop it and retry")
            _usable(con, new, database)
        observed.update(new_slot=name, new_slot_present=new is not None)
        return observed
    if to_lsn is None or to_lsn <= checkpoint:
        raise EpochError("skip needs a target past the durable checkpoint")
    last_end, active = unit_position["last_member_end_lsn"], unit_position["active_commit_lsn"]
    if (last_end is not None and to_lsn < last_end) or (active is not None and to_lsn <= active):
        raise EpochError("the target is not beyond the durable member prefix")
    if current is None or current["lost"]:
        raise EpochError("the slot is missing or lost; use slot_lost")
    _usable(con, current, database)
    if current["confirmed"] > to_lsn:
        raise EpochError("the slot is already past the target")
    if current["confirmed"] < to_lsn:
        flushed = int(query(con, flush_sql(role_of(config.heartbeat_writer)))[0][0])
        if to_lsn > flushed:
            observed["clamped"] = True
    observed.update(slot_confirmed=current["confirmed"], abandoned_bytes=to_lsn - checkpoint)
    if current["confirmed"] >= to_lsn:
        flushed = int(query(con, flush_sql(role_of(config.heartbeat_writer)))[0][0])
    observed.update(
        abandoned_bytes_upper_bound=to_lsn - checkpoint,
        abandoned_bytes_current=max(0, min(to_lsn, flushed) - checkpoint),
    )
    return observed


def _recreate(con, config, observed, new_epoch, database, fault, created):
    """Slot loss: create (or reuse an unconsumed) `<base>_e<n>`. Returns (name, start,
    details). `created` (the caller's list) gets the name the moment this call creates
    the slot, so a failure anywhere after that still lets the caller drop it. The
    old slot is left in place: it is dropped after the metadata commits."""
    name = observed["new_slot"]
    if slot_info(con, name) is None:
        if fault:
            fault("before_create_slot")
        standby = config.heartbeat_writer == "external"
        _bounded(
            con,
            min(CREATE_SECONDS, config.slot_wait_seconds) if standby else CREATE_SECONDS,
            "SELECT * FROM pg_create_logical_replication_slot(%s,'pgoutput')",
            [name],
            STANDBY_WAITING
            if standby
            else "creating the new slot waits for running source transactions; retry",
        )
        created.append(name)
    new = slot_info(con, name)
    # A slot left by a killed attempt: nothing sends feedback to it before the
    # metadata names it, so its confirmed position is still its start.
    if new["lost"]:
        raise EpochError("the new epoch's slot is lost; drop it and retry")
    _usable(con, new, database)
    if fault:
        fault("after_create_slot")
    return name, new["confirmed"], dict(abandoned_from=observed["checkpoint"])


def _advance(
    con, slot, checkpoint, to_lsn, database, fault, deadline=None, *, actual_start, role=PRIMARY
):
    """Skip up to the request, reporting the actual clamped landing."""
    if actual_start is not True:
        raise EpochError("the actual slot landing is required")
    current = slot_info(con, slot)
    if current is None or current["lost"]:
        raise EpochError("the slot is missing or lost; use slot_lost")
    _usable(con, current, database)
    if current["confirmed"] > to_lsn:
        raise EpochError("the slot passed the requested target; create a new preview")
    effective = to_lsn
    # A standby: the skip clamps to the replay position.
    flushed = int(query(con, flush_sql(role))[0][0])
    if current["confirmed"] > flushed:
        raise EpochError("slot position exceeds the source's flushed WAL")
    effective = min(to_lsn, flushed)
    if current["confirmed"] < effective:
        if fault:
            fault("before_advance")
        # A step starts only if it can finish before the run's own deadline (an epoch
        # Flight passes its start + max_runtime - margin), and within
        # this call's budget.
        limit = _clock() + ADVANCE_BUDGET_SECONDS
        if deadline is not None:
            limit = min(limit, deadline)

        def progress():
            return (
                f"advanced to {lsn_text(current['confirmed'])} of {lsn_text(to_lsn)} "
                "(the progress is kept); run a new dry run and click again to continue"
            )

        while current["confirmed"] < effective:
            if _clock() + ADVANCE_STEP_SECONDS > limit:
                raise Waiting(progress())
            step = min(effective, current["confirmed"] + ADVANCE_STEP_BYTES)
            if step == effective:
                # Native clamp on the final step. Later WAL belongs to the new epoch.
                step = to_lsn
            try:
                _bounded(
                    con,
                    ADVANCE_STEP_SECONDS,
                    "SELECT pg_replication_slot_advance(%s,%s::pg_lsn)",
                    [slot, lsn_text(step)],
                    "",
                )
            except Waiting:
                current = slot_info(con, slot) or current
                raise Waiting(progress()) from None
            except psycopg2.errors.ObjectInUse:
                raise LeaseBusy(
                    "replication slot is active; retry when its reader has stopped"
                ) from None
            current = slot_info(con, slot)
            if current is None or current["lost"]:
                raise EpochError("the slot was lost while advancing; use slot_lost")
    # A retry after a lost metadata commit lands here with the slot at the target.
    if current["confirmed"] < effective:
        raise EpochError("the slot did not advance to the target")
    if fault:
        fault("after_advance")
    actual = current["confirmed"]
    if actual > to_lsn:
        raise EpochError("slot passed the requested target; create a new preview")
    return (
        slot,
        actual,
        dict(abandoned_from=checkpoint, abandoned_to=actual, requested_to_lsn=to_lsn),
    )


def _drop_quietly(con, name, database):
    """Best effort: drop an inactive pgoutput slot of this database. Returns the name
    when it is still there. Slot names are cluster-wide: a same-named slot another
    database or tool made since is never dropped."""
    try:
        info = slot_info(con, name)
        if info is None:
            return None
        if info["active"] or info["plugin"] != "pgoutput" or info["database"] != database:
            return name
        query(con, "SELECT pg_drop_replication_slot(%s)", [name])
        return None
    except Exception:
        return name


def _give_up(con, log, old_epoch, old_slot, created, database):
    """A slot this run created is dropped only when a re-read shows
    the transition did not commit (same epoch, old slot still named). An ambiguous
    commit, or a failed re-read, keeps it: the next run's `already` or reuse settles it.
    Returns the slot's name when it is kept."""
    try:
        epoch, slot, _ = log.position()
    except Exception:
        return created
    if epoch == old_epoch and slot == old_slot:
        return _drop_quietly(con, created, database)
    return created


def _previous_slot(log, epoch):
    row = log.execute(
        "SELECT previous_slot_name FROM {s}source_epochs WHERE epoch=? AND reason='slot_lost'",
        [epoch],
    ).fetchone()
    return row[0] if row else None


def preview_epoch(config, log, reason, *, to_lsn=None):
    """A dry run of the next transition: the same checks as begin_epoch, read only, no
    lease and no write. Returns `observe`'s decision fields (the epoch, slot and
    checkpoint read now, the slot's state, the target), or raises EpochError /
    SourceMismatch (refused) or LeaseBusy (a reader is attached)."""
    if reason not in REASONS:
        raise ValueError("reason must be slot_lost or skip")
    if (to_lsn is None) != (reason != "skip"):
        raise ValueError("to_lsn is required for skip, and only for skip")
    epoch, slot, checkpoint = log.position()
    system_id, timeline = identify(config)
    same_timeline(config, log, timeline)
    registered = log.execute("SELECT system_id,database_oid FROM {s}source WHERE id=1").fetchone()
    if registered is None:
        raise SourceMismatch("source is not registered; run preflight")
    con = connect(config)
    try:
        database, database_oid = query(
            con, "SELECT current_database(),oid FROM pg_database WHERE datname=current_database()"
        )[0]
        if (system_id, int(database_oid)) != (registered[0], registered[1]):
            raise SourceMismatch("a different cluster or database is a new source, not an epoch")
        options = {"unit_position": log.unit_position()}
        return observe(
            con, config, slot, checkpoint, epoch + 1, reason, to_lsn, database, **options
        )
    finally:
        con.close()


def begin_epoch(
    config,
    log,
    new_epoch,
    reason,
    *,
    to_lsn=None,
    fault=None,
    claim=None,
    wait=0,
    started_by=None,
    deadline=None,
    sleep=time.sleep,
    clock=time.monotonic,
):
    """Move the source to `new_epoch` (the registered epoch + 1).

    Idempotent: a retry of a transition that committed returns "already"; any other
    transition to the same epoch is refused. Every mirror then holds needs_resync
    until a backfill generation of the new epoch publishes.

    The order: lease (waited up to `wait` s), position, the source's
    identity, a read-only `observe`, then `claim(observed)` (an action Flight spends its
    preview, in its own guarded transaction; it may raise), then the PostgreSQL step,
    then the metadata. Statuses: begun, already, busy (the lease or a slot reader),
    waiting (a bounded PostgreSQL wait ran out; nothing changed).
    """
    if reason not in REASONS:
        raise ValueError("reason must be slot_lost or skip")
    if (to_lsn is None) != (reason != "skip"):
        raise ValueError("to_lsn is required for skip, and only for skip")

    def action(run_id):
        return _transition(
            config, log, new_epoch, reason, to_lsn, fault, claim, started_by, deadline
        )

    result = leased(log, action, wait, sleep=sleep, clock=clock)
    if result is None:
        epoch, slot, checkpoint = log.position()
        return EpochResult("busy", epoch, slot, checkpoint, detail="ingest holds the lease")
    return result


def _transition(config, log, new_epoch, reason, to_lsn, fault, claim, started_by, deadline):
    epoch, slot, checkpoint = log.position()
    if epoch == new_epoch:
        done = log.epoch_record(new_epoch)
        request = done.get("requested_to") if done else None
        if (
            done
            and done["reason"] == reason
            and done["slot"] == slot
            and (
                reason != "skip"
                or (
                    request is not None
                    and request == to_lsn
                    and type(done.get("clamped")) is bool
                    and done["clamped"] == (done["start"] < request)
                )
            )
        ):
            abandoned = (done["from"], done["to"]) if reason == "skip" else ()
            leftover = None
            previous = _previous_slot(log, new_epoch)
            if previous:  # S1: finish dropping the old epoch's slot
                con = connect(config)
                try:
                    database = query(con, "SELECT current_database()")[0][0]
                    leftover = _drop_quietly(con, previous, database)
                finally:
                    con.close()
            return EpochResult(
                "already",
                epoch,
                slot,
                done["start"],
                abandoned,
                leftover=leftover,
                requested_to_lsn=request if reason == "skip" else None,
                clamped=bool(done.get("clamped")),
            )
        raise EpochError("this epoch already began differently")
    if new_epoch != epoch + 1:
        raise EpochError(f"the next epoch is {epoch + 1}")
    system_id, timeline = identify(config)
    same_timeline(config, log, timeline)
    registered = log.execute("SELECT system_id,database_oid FROM {s}source WHERE id=1").fetchone()
    if registered is None:
        raise SourceMismatch("source is not registered; run preflight")
    con = connect(config)
    try:
        database, database_oid = query(
            con, "SELECT current_database(),oid FROM pg_database WHERE datname=current_database()"
        )[0]
        if (system_id, int(database_oid)) != (registered[0], registered[1]):
            raise SourceMismatch("a different cluster or database is a new source, not an epoch")
        try:
            options = {"unit_position": log.unit_position()}
            observed = observe(
                con, config, slot, checkpoint, new_epoch, reason, to_lsn, database, **options
            )
        except LeaseBusy as exc:
            # A reader still holds a slot: never terminate it. Nothing changed.
            return EpochResult("busy", epoch, slot, checkpoint, detail=str(exc))
        if claim is not None:
            claim(observed)
        made = []  # the slot this run created, as soon as it exists
        try:
            if reason == "slot_lost":
                name, start, details = _recreate(
                    con, config, observed, new_epoch, database, fault, made
                )
            else:
                name, start, details = _advance(
                    con,
                    slot,
                    checkpoint,
                    to_lsn,
                    database,
                    fault,
                    deadline,
                    actual_start=True,
                    role=role_of(config.heartbeat_writer),
                )
            details.update(timeline_id=timeline, started_by=started_by)
            if fault:
                fault("before_metadata")
            log.begin_epoch(new_epoch, start, reason, name, details=details, fault=fault)
            if fault:
                fault("after_metadata")
        except Waiting as exc:
            return EpochResult("waiting", epoch, slot, checkpoint, detail=str(exc))
        except LeaseBusy as exc:
            pending = _give_up(con, log, epoch, slot, made[0], database) if made else None
            return EpochResult("busy", epoch, slot, checkpoint, detail=str(exc), pending=pending)
        except BaseException as exc:
            if made:
                pending = _give_up(con, log, epoch, slot, made[0], database)
                if pending:
                    exc.add_note(f"pending slot {pending} kept: retry to finish or reuse it")
            raise
        abandoned = (details["abandoned_from"], details["abandoned_to"]) if to_lsn else ()
        # D2/S1: the old slot goes only after the metadata names the new one; best effort.
        leftover = (
            _drop_quietly(con, slot, database) if reason == "slot_lost" and slot != name else None
        )
        return EpochResult(
            "begun",
            new_epoch,
            name,
            start,
            abandoned,
            leftover=leftover,
            requested_to_lsn=to_lsn if reason == "skip" else None,
            clamped=bool(reason == "skip" and start < to_lsn),
        )
    finally:
        con.close()
