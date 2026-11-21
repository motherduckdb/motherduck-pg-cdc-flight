"""Compaction (Phase 3): drop sealed change-log days below every retention pin, and
prune the ledger rows nothing can read again, for one source per run.

Each day is one transaction under the source's compaction lease
(`Retention.drop_partition`, which re-checks everything). Days are tried oldest
first and the run stops at the first day kept: ingestion days never move backwards
and epochs only increase, so each day's greatest `(epoch, commit)` is
non-decreasing, and a day that a pin still needs means every later day is needed
too. The run writes only cdc_internal; its record row is best effort.
"""

import os
import time
import uuid
from dataclasses import dataclass, field

import duckdb

from cdc_flight import expiry
from cdc_flight.config import identifier
from cdc_flight.generation_spec import InvariantViolation
from cdc_flight.retention import (
    CompactionBusy,
    FloorAboveCheckpoint,
    Retention,
    check_lifecycle,
    is_conflict,
)

# How long the run records (compactions, expirations) are kept.
RECORD_DAYS = 90


@dataclass(frozen=True)
class CompactConfig:
    source_id: str
    database: str = "cdc_internal"
    runtime_seconds: float = 540
    lease_seconds: int = 900
    mirror_database: str = ""
    # Owner settings (the compact Flight's RUN stays with its owner): `report` only
    # records which pin holders would expire; `on` expires them.
    expire: str = "report"
    max_retained_lag_hours: float = 168
    storage_budget_gb: float | None = None

    def __post_init__(self):
        identifier(self.source_id)
        identifier(self.database)
        if not self.mirror_database:
            object.__setattr__(self, "mirror_database", f"{self.source_id}_mirror")
        identifier(self.mirror_database)
        if not 0 < self.runtime_seconds < self.lease_seconds:
            raise ValueError("compaction runtime must be positive and below the lease TTL")
        if self.expire not in ("report", "on"):
            raise ValueError("CDC_COMPACT_EXPIRE must be report or on")
        if not 1 <= self.max_retained_lag_hours <= 8760:
            raise ValueError("CDC_MAX_RETAINED_LAG_HOURS must be 1..8760")
        if self.storage_budget_gb is not None and not 0 < self.storage_budget_gb <= 1_000_000:
            raise ValueError("CDC_LOG_STORAGE_BUDGET_GB must be above 0")

    @classmethod
    def from_env(cls):
        budget = os.getenv("CDC_LOG_STORAGE_BUDGET_GB", "")
        return cls(
            os.environ["CDC_SOURCE_ID"],
            database=os.getenv("CDC_DATABASE", "cdc_internal"),
            runtime_seconds=float(os.getenv("CDC_RUNTIME_SECONDS", "540")),
            mirror_database=os.getenv("CDC_MIRROR_DATABASE", ""),
            expire=os.getenv("CDC_COMPACT_EXPIRE", "report"),
            max_retained_lag_hours=float(os.getenv("CDC_MAX_RETAINED_LAG_HOURS", "168")),
            storage_budget_gb=float(budget) if budget else None,
        )


@dataclass
class CompactionResult:
    run_id: str
    # done: every droppable day dropped; busy: the lease is held (or a conflict
    # stopped a day); capped: the runtime cap; held: a published table lost its pin;
    # invariant: a drop would have raised the floor past the ingest checkpoint.
    status: str = "done"
    days: list = field(default_factory=list)
    batches: int = 0
    manifests: int = 0
    kept_day: str | None = None
    kept_reason: str | None = None
    held_relation: int | None = None
    error: str | None = None
    # Expiry: holders expired, recorded as would-expire (report mode), and skipped
    # because they moved or were busy; the storage budget's state.
    expired: int = 0
    would_expire: int = 0
    skipped: int = 0
    budget: str = "off"
    format_version: int = 2
    chunks: int = 0
    units: int = 0


def run_compaction(
    config,
    con,
    *,
    run_id=None,
    on_event=None,
    fault=None,
    clock=time.monotonic,
    observe=None,
    retention=None,
):
    """One compaction run. `on_event(event, value)` receives `lease` (ok/busy),
    `day_start` (day), `day` ((day, DropResult)) and `stop` (the reason)."""

    def emit(event, value):
        if on_event:
            on_event(event, value)

    result = CompactionResult(run_id or f"compact:{uuid.uuid4().hex}")
    # For the operations status (ops.report_compaction): the result as it stands, the
    # lease's server time and the expiry pass's summary.
    observe = {} if observe is None else observe
    observe["result"] = result
    retention = retention or Retention(con, config.database, config.source_id, config.lease_seconds)
    if getattr(retention, "format_version", None) != 2:
        raise ValueError("compaction requires a format-2 store")
    retention.initialize()
    check_lifecycle(retention)  # a deleting or deleted source is never compacted
    started = clock()
    try:
        token = retention.acquire_compaction(result.run_id)
    except CompactionBusy:
        result.status = "busy"
        emit("lease", "busy")
        record(retention, result, clock() - started)
        return result
    emit("lease", "ok")
    observe["as_of"] = retention.acquired_at_us
    summary = None
    try:

        def out_of_time():
            return clock() - started >= config.runtime_seconds

        summary = expiry.run(
            config,
            retention,
            token,
            result.run_id,
            mode=config.expire,
            fault=fault,
            out_of_time=out_of_time,
        )
        result.expired = summary.count("expired")
        result.would_expire = summary.count("would_expire")
        result.skipped = len(summary.outcomes) - result.expired - result.would_expire
        result.budget = summary.budget
        observe["summary"] = summary
        emit("expire", "expired" if result.expired else "report" if result.would_expire else "none")
        # Every day not yet dropped, oldest first. An epoch transition's day stays
        # (`not_sealed`) until its new-epoch row seals at the next day boundary, and
        # the newest day is always active: `not_sealed` on every run is normal.
        days = retention.drop_candidates()
        for day in days:
            if clock() - started >= config.runtime_seconds:
                result.status, result.kept_day = "capped", day
                break
            emit("day_start", day)
            stopped = busy(result, day, lambda: retention.drop_partition(day, token, fault=fault))
            if stopped:
                emit("day", (day, None))
                break
            dropped = stopped.value
            emit("day", (day, dropped))
            if not dropped:
                result.kept_day, result.kept_reason = day, dropped.reason
                if dropped.reason == "held":
                    result.status, result.held_relation = "held", dropped.relation
                break
            result.days.append(day)
        # The ledger rows of dropped days (including any an earlier run left),
        # without the shared guard, so pin writes never wait on it.
        if result.status != "busy":
            pruned = busy(
                result,
                None,
                lambda: retention.prune(token, fault=fault, out_of_time=out_of_time),
            )
            if not pruned:
                result.chunks, result.units = pruned.value
            busy(result, None, lambda: retention.forget(token, days=RECORD_DAYS))
        # Expiry isolated a generation it could not fail (rolled back, its pin kept):
        # the rest of the run is done; now fail it loudly. A generation failed without
        # its claim is reported, not raised.
        if summary.count("invariant"):
            raise InvariantViolation(
                f"{summary.count('invariant')} generation(s) could not be failed; see the "
                "expiry rows (outcome invariant) and the compaction log"
            )
        if summary.count("invariant_repaired"):
            # Reported, not raised; named in `error` too, so a capped or held run shows it.
            result.error = result.error or "invariant_repaired"
            if result.status == "done":
                result.status = "invariant"
    except (FloorAboveCheckpoint, InvariantViolation) as exc:
        result.status, result.error = "invariant", type(exc).__name__
        raise
    except Exception as exc:
        result.status, result.error = "failed", type(exc).__name__
        raise
    finally:
        try:
            retention.release_compaction(token)
        finally:
            if result.status == "done":
                emit("stop", result.kept_reason or "done")
            elif result.status != "failed":
                emit("stop", result.status)
            record(retention, result, clock() - started)
            if summary is not None:
                expiry.record(retention, result.run_id, summary, config.expire)
    return result


class _Outcome:
    def __init__(self, value=None, stopped=False):
        self.value, self.stopped = value, stopped

    def __bool__(self):
        return self.stopped


def busy(result, day, action):
    """Run `action`; a lost lease or a concurrent catalog change (ingest's day
    boundary) marks the run busy (the next run continues) and returns a truthy
    outcome."""
    try:
        return _Outcome(action())
    except duckdb.TransactionException as exc:
        if not is_conflict(exc):
            raise
        result.status, result.kept_day, result.kept_reason = "busy", day, "conflict"
    except CompactionBusy:
        result.status, result.kept_day, result.kept_reason = "busy", day, "lease"
    return _Outcome(stopped=True)


def record(retention, result, seconds):
    """The run's row in `compactions`, after the lease (best effort: never fails
    the run, and a failed run's own exception is the one that propagates)."""
    try:
        floor = retention.floor() or (None, None)
        with retention.transaction():
            retention.execute(
                "INSERT INTO {s}compactions (run_id,started_at,finished_at,status,days_dropped,"
                "format_version,chunks_pruned,units_pruned,floor_epoch,floor_lsn,kept_day,kept_reason,"
                "held_relation,error) VALUES (?,current_timestamp-?*INTERVAL '1 second',"
                "current_timestamp,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT DO NOTHING",
                [
                    result.run_id,
                    seconds,
                    result.status,
                    len(result.days),
                    2,
                    result.chunks,
                    result.units,
                    floor[0],
                    floor[1],
                    result.kept_day,
                    result.kept_reason,
                    result.held_relation,
                    result.error,
                ],
            )
    except Exception:
        pass
