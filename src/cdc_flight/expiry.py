"""Consumer expiry (Phase 3): which retention-pin holders may no longer pin the log.

Bounded storage, indefinite pause and lossless resume cannot all hold (DESIGN,
Protocol 4). A holder with unread data on a **sealed** day older than the maximum retained lag,
or a sealed day the storage budget can't keep, expires: its mirror moves to
`needs_resync` (a table) or it fails (an active generation), and its pin is released.
The active day never counts, so an ingest outage expires nothing. A mirror already
`needs_resync` is rebuilt from the source, so its pins protect nothing and are
released too, whoever set the state. Report mode (the default) only records.

Safe order, two databases, two transactions per holder:
1. the mirror first, under the table's apply lease: a compare-and-set to
   `needs_resync` on the state, generation, epoch and checkpoint observed (a holder
   that moved meanwhile is skipped);
2. then `Retention.expire`, guarded and scoped to the exact pin observed (a pin a
   publication transferred meanwhile is kept).
A crash between them leaves the table held with its pin: extra history only, and
the next run releases it by the `needs_resync` rule.
"""

import logging
from dataclasses import dataclass, field

import duckdb

from cdc_flight import generations
from cdc_flight.apply import Apply, FenceLost, LeaseBusy
from cdc_flight.attach import LocalStore, attach_motherduck
from cdc_flight.config import ApplyConfig
from cdc_flight.generation_spec import InvariantViolation
from cdc_flight.generations import ACTIVE, stale_generation
from cdc_flight.retention import is_conflict

logger = logging.getLogger(__name__)

GIB = 1024**3


@dataclass
class Outcome:
    owner: str
    kind: str
    relation_id: int | None
    generation_id: str | None
    epoch: int
    lsn: int
    reason: str
    # would_expire, expired, moved, busy, no_mirror; invariant (a generation's failure
    # rolled back on an InvariantViolation, its pin kept), invariant_repaired (failed and
    # its pin expired, but its claim was already missing).
    outcome: str


@dataclass
class Summary:
    outcomes: list = field(default_factory=list)
    budget: str = "off"  # off, ok, over, unknown
    # For the operations Dive (ops.report_compaction): every pin's standing and the
    # retained days, as of this run.
    pins: list = field(default_factory=list)
    days: list = field(default_factory=list)

    def count(self, outcome):
        return sum(1 for o in self.outcomes if o.outcome == outcome)

    def annotated_pins(self):
        """The pins with this run's outcome; a superseded outcome extends the count."""
        outcomes = {o.owner: o.outcome for o in self.outcomes}
        out = []
        for pin in self.pins:
            outcome = outcomes.get(pin["owner"])
            count = pin["consecutive_superseded"] + (outcome == "superseded")
            out.append(dict(pin, outcome=outcome, consecutive_superseded=count))
        return out


def thresholds(retention, config):
    """The newest (epoch, lsn) a pin may sit at or below before it may expire, by lag
    and by budget, over sealed days only. None means nothing expires by that rule.

    Lag: a sealed day counts once the day after it began more than the limit ago:
    newer data has been in the log that long, so a consumer with unread data on the day
    has been left behind (DESIGN: a consumer that hasn't advanced). Right after an
    outage, the day that seals the last pre-outage day is new, so nothing counts."""
    rows = retention.retained_days(config.max_retained_lag_hours)
    days, report = {}, []
    for day, epoch, lsn, status, n, size, began_long_ago, low, start_us in rows:
        d = days.setdefault(
            day,
            dict(
                day=day,
                high=(0, 0),
                sealed=True,
                n=0,
                bytes=0,
                began_long_ago=began_long_ago,
                start_us=start_us,
            ),
        )
        d["high"] = max(d["high"], (epoch, lsn))
        d["sealed"] = d["sealed"] and status == "sealed"
        d["n"] += n
        d["bytes"] = None if d["bytes"] is None or size is None else d["bytes"] + size
        report.append(
            dict(
                day=day, epoch=epoch, status=status, events=n, bytes=size, min_lsn=low, max_lsn=lsn
            )
        )
    ordered = [days[day] for day in sorted(days)]
    sealed = [d for d in ordered if d["sealed"]]
    lag = max(
        (
            d["high"]
            for d, after in zip(ordered, ordered[1:])
            if d["sealed"] and after["began_long_ago"]
        ),
        default=None,
    )
    budget, status = None, "off"
    if config.storage_budget_gb is not None:
        known = [d for d in ordered if d["bytes"] is not None]
        if sealed and not [d for d in sealed if d["bytes"] is not None]:
            status = "unknown"
        else:
            per_event = sum(d["bytes"] for d in known) / max(1, sum(d["n"] for d in known))
            sizes = [d["bytes"] if d["bytes"] is not None else d["n"] * per_event for d in ordered]
            total, limit = sum(sizes), config.storage_budget_gb * GIB
            status = "ok"
            for d, size in zip(ordered, sizes):
                if total <= limit or not d["sealed"]:
                    break
                total -= size
                budget, status = d["high"], "over"
    return lag, budget, status, ordered, report


def standing(pins, ordered, budget, config, retention):
    """Every pin's standing for the Dive: its first sealed unread-data day, when it
    last advanced, the rule that would expire it and the estimated time (epoch us;
    None = never: it holds no sealed day, or nothing follows that day yet), plus how
    many trailing runs reported it superseded (from `expirations`)."""
    limit_us = int(config.max_retained_lag_hours * 3600 * 1_000_000)
    superseded = {}
    try:
        for owner, outcome in retention.execute(
            "SELECT owner,outcome FROM {s}expirations ORDER BY expired_at DESC"
        ).fetchall():
            if owner in superseded and superseded[owner][1]:
                continue
            count, done = superseded.get(owner, (0, False))
            superseded[owner] = (count + 1, False) if outcome == "superseded" else (count, True)
    except duckdb.CatalogException:
        pass
    out = []
    for owner, kind, rel, gen, epoch, lsn, _, _, updated_us in pins:
        held = next(
            (i for i, d in enumerate(ordered) if d["sealed"] and (epoch, lsn) < d["high"]), None
        )
        rule, expires = "none", None
        if held is not None:
            rule = "lag"
            if held + 1 < len(ordered):
                expires = max(updated_us or 0, ordered[held + 1]["start_us"]) + limit_us
        if budget is not None and (epoch, lsn) <= budget:
            rule, expires = "budget", retention.acquired_at_us
        out.append(
            dict(
                owner=owner,
                kind=kind,
                relation_id=rel,
                generation_id=gen,
                epoch=epoch,
                lsn=lsn,
                held_day=ordered[held]["day"] if held is not None else None,
                updated_at_us=updated_us,
                rule=rule,
                expires_at_us=expires,
                consecutive_superseded=superseded.get(owner, (0, False))[0],
            )
        )
    return out


def mirror_attached(retention, config):
    """Attach the mirror database if needed (see attach.py). False only when it
    certainly doesn't exist (no mirror yet): any other failure propagates."""
    try:
        return attach_motherduck(retention.con, config.database, config.mirror_database)
    except LocalStore as exc:
        raise RuntimeError(
            f"mirror database {config.mirror_database} is not attached to this local store"
        ) from exc


def run(config, retention, token, run_id, *, mode, fault=None, out_of_time=None):
    """Judge every pin holder; in `on` mode, expire the ones that must go. Returns a
    Summary. Renews the compaction lease between holders (its own column, never the
    guard) and stops at the runtime cap."""
    summary = Summary()
    lag, budget, summary.budget, ordered, summary.days = thresholds(retention, config)
    # `stalled`: the pin hasn't advanced within the lag limit (advance and transfer
    # set updated_at). Lag expiry needs it; the budget doesn't.
    pins = retention.execute(
        "SELECT owner,kind,relation_id,generation_id,source_epoch,pinned_lsn,"
        "created_at<=current_timestamp-?*INTERVAL '1 second',"
        "updated_at<=current_timestamp-?*INTERVAL '1 hour',epoch_us(updated_at) "
        "FROM {s}retention_pins ORDER BY source_epoch,pinned_lsn",
        [retention.lease_seconds, config.max_retained_lag_hours],
    ).fetchall()
    summary.pins = standing(pins, ordered, budget, config, retention)
    pins = [p[:8] for p in pins]
    if not pins:
        return summary

    def why(epoch, lsn, stalled):
        # Equality retains a valid unit-END cut, but all data through it is consumed.
        # Lag judges unread data; the storage budget still judges physical retention.
        if lag is not None and stalled and (epoch, lsn) < lag:
            return "lag"
        if budget is not None and (epoch, lsn) <= budget:
            return "budget"
        return None

    if not mirror_attached(retention, config):
        for owner, kind, rel, gen, epoch, lsn, _, stalled in pins:
            reason = why(epoch, lsn, stalled)
            if reason:
                summary.outcomes.append(
                    Outcome(owner, kind, rel, gen, epoch, lsn, reason, "no_mirror")
                )
        return summary
    names = [
        f"{s}.{t}"
        for s, t in retention.execute("SELECT schema_name,table_name FROM {s}tables").fetchall()
    ]
    apply = Apply(
        retention.con,
        ApplyConfig(
            config.source_id,
            tuple(dict.fromkeys(names)),
            database=config.database,
            mirror_database=config.mirror_database,
            lease_seconds=config.lease_seconds,
            runtime_seconds=min(config.runtime_seconds, config.lease_seconds - 1),
        ),
    )
    for owner, kind, rel, gen_id, epoch, lsn, old, stalled in pins:
        if out_of_time and out_of_time():
            break
        retention._lease(token)
        reason = why(epoch, lsn, stalled)
        current = apply.state(rel)
        if kind == "table" and current is not None and current["generation"] != gen_id:
            # Superseded: a publication moved the mirror to a newer generation and
            # its transfer is pending. The mirror is protected by that generation's
            # pin; this one is replaced by the transfer. Never write the mirror for it.
            if reason:
                summary.outcomes.append(
                    Outcome(owner, kind, rel, gen_id, epoch, lsn, reason, "superseded")
                )
            continue
        if kind == "generation":
            gen = generations.generation_row(apply, gen_id)
            mirror_owned = (
                gen is not None
                and gen["state"] == "published"
                and not stale_generation(gen, gen_id, current, old)
            )
            if not mirror_owned:
                outcome = _generation(
                    apply,
                    retention,
                    gen,
                    gen_id,
                    current,
                    old,
                    reason,
                    mode,
                    run_id,
                    owner,
                    epoch,
                    lsn,
                    fault,
                )
                if outcome:
                    summary.outcomes.append(
                        Outcome(owner, kind, rel, gen_id, epoch, lsn, outcome[0], outcome[1])
                    )
                continue
        # A table pin of the mirror's generation, or the generation pin of the mirror's
        # current generation (a publication whose transfer is pending): judged by the
        # mirror.
        if current is None:
            if reason:
                summary.outcomes.append(
                    Outcome(owner, kind, rel, gen_id, epoch, lsn, reason, "no_mirror")
                )
            continue
        resync = current["state"] == "needs_resync"
        if not (reason or resync):
            continue
        reason = reason or "needs_resync"
        if mode != "on":
            outcome = "would_expire"
        elif resync:
            outcome = _expire(retention, owner, gen_id, epoch, lsn)
        else:
            outcome = _hold_mirror(apply, current, rel, run_id, reason, epoch, lsn, fault)
            if outcome == "held":
                if fault:
                    fault("between_expiry_steps")
                outcome = _expire(retention, owner, gen_id, epoch, lsn)
        summary.outcomes.append(Outcome(owner, kind, rel, gen_id, epoch, lsn, reason, outcome))
    return summary


def _expire(retention, owner, gen_id, epoch, lsn):
    """One pin's expiry, isolated: an InvariantViolation (a table pin whose
    registry row the pin_state update can't find) rolls the delete back, keeps the pin and
    is reported, and the other pins, the day drops and the prune still run (compaction
    fails the run at its end, as for a generation's)."""
    try:
        return "expired" if retention.expire(owner, gen_id, epoch, lsn) else "moved"
    except InvariantViolation as exc:
        logger.error("expiry: pin %s: %s", owner, exc)
        return "invariant"


def _hold_mirror(apply, current, relation_id, run_id, reason, epoch, lsn, fault):
    """Step 1: the mirror to needs_resync, compare-and-set on what was observed."""
    try:
        token = apply.acquire(relation_id, run_id, token=current["token"])
    except (LeaseBusy, duckdb.TransactionException):
        return "busy"
    try:
        if fault:
            fault("before_mirror_hold")
        with apply.transaction():
            apply.guard(relation_id, token)
            held = apply.set_state(
                relation_id,
                token,
                current,
                state="needs_resync",
                detail=f"retention expired ({reason}): pinned at {epoch}/{lsn:X}",
                code="retention_expired",
            )
        return "held" if held else "moved"
    finally:
        apply.release(relation_id, token)


def _generation(
    apply, retention, gen, gen_id, current, old, reason, mode, run_id, owner, epoch, lsn, fault
):
    """A generation's own pin: released when it protects nothing (as sweep_pins
    would); an active generation past the limit fails under its lease, then its pin
    goes. Generation pins never advance, so a backfill running longer than the lag
    limit expires: the lag limit is also the longest a backfill can run."""
    if stale_generation(gen, gen_id, current, old):
        if mode != "on":
            return ("stale", "would_expire")
        return ("stale", _expire(retention, owner, gen_id, epoch, lsn))
    if gen is None or gen["state"] not in ACTIVE or not reason:
        return None
    if mode != "on":
        return (reason, "would_expire")
    try:
        token = generations.acquire_generation(apply, gen_id, run_id, token=gen["token"])
    except (LeaseBusy, duckdb.TransactionException):
        return (reason, "busy")
    try:
        repaired = generations.fail_generation(
            apply,
            gen,
            gen_id,
            token,
            "retention expired: the backfill fell too far behind",
            FenceLost,
            fault=fault,
        )
    except FenceLost:
        return (reason, "moved")
    except InvariantViolation as exc:
        # Rolled back: this generation and its pin stay. The other pins, the day drops
        # and the prune still run; compaction fails the run at its end (compact.run).
        logger.error("expiry: generation %s: %s", gen_id, exc)
        return (reason, "invariant")
    except duckdb.TransactionException as exc:
        if not is_conflict(exc):
            raise
        return (reason, "busy")  # rolled back: the generation runs on, its pin kept
    finally:
        generations.release_generation(apply, gen_id, token)
    # After the failure commits: its pin, exactly as observed.
    expired = _expire(retention, owner, gen_id, epoch, lsn)
    if expired == "invariant":  # the pin's own violation wins: the pin is kept
        return (reason, "invariant")
    if repaired:  # failed without its claim: reported (compaction status "invariant")
        return (reason, "invariant_repaired")
    return (reason, expired)


def record(retention, run_id, summary, mode):
    """The run's expiry rows (best effort), with the floor after the run."""
    if not summary.outcomes:
        return
    try:
        floor = retention.floor() or (None, None)
        with retention.transaction():
            retention.execute(
                "INSERT INTO {s}expirations (run_id,expired_at,owner,kind,relation_id,"
                "generation_id,source_epoch,pinned_lsn,reason,mode,outcome,floor_epoch,floor_lsn) "
                "SELECT ?,current_timestamp,unnest(?),unnest(?),"
                "unnest(?),unnest(?),unnest(?),unnest(?),unnest(?),?,unnest(?),?,?",
                [
                    run_id,
                    [o.owner for o in summary.outcomes],
                    [o.kind for o in summary.outcomes],
                    [o.relation_id for o in summary.outcomes],
                    [o.generation_id for o in summary.outcomes],
                    [o.epoch for o in summary.outcomes],
                    [o.lsn for o in summary.outcomes],
                    [o.reason for o in summary.outcomes],
                    mode,
                    [o.outcome for o in summary.outcomes],
                    floor[0],
                    floor[1],
                ],
            )
    except Exception:
        pass
