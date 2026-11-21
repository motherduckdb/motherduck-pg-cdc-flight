"""Flight chaining: ingest triggers apply; backfill re-triggers itself until done.

A Flight starts another with MD_RUN_FLIGHT, which is asynchronous, runs the target
as its owner, and may override only config keys the target declares. Concurrent
runs are allowed; our leases make an overlapping run exit busy. The decisions here
are pure functions over a run's result and an injected trigger, so they are tested
without MotherDuck.

Config is untrusted: anyone who can run a Flight can pass overrides. Every Flight
declares its keys with a "" default, and "" means "use the default".
"""

import json
import logging
import os
import re
import sys
import time
import uuid
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

# Keys every Flight reads through config.py.
_SHARED = (
    "CDC_SOURCE_ID",
    "CDC_TABLES",
    "CDC_DATABASE",
    "CDC_EPOCH",
    "CDC_RUNTIME_SECONDS",
    # The operations status database (ops.py), an owner setting.
    "CDC_OPS_DATABASE",
)
_SOURCE = (
    "CDC_DSN_ENV",
    "CDC_HEARTBEAT",
    "CDC_HEARTBEAT_WRITER",
    "CDC_SLOT_WAIT_SECONDS",
    "CDC_HEARTBEAT_INTERVAL_SECONDS",
)
_MIRROR = ("CDC_MIRROR_DATABASE",)

# Declared Flight config per command; build_flight.py writes these as the template.
FLIGHT_CONFIG = {
    "preflight": _SHARED + _SOURCE + ("CDC_SLOT", "CDC_PUBLICATION", "CDC_CREATE_OBJECTS"),
    "ingest": _SHARED
    + _SOURCE
    + ("CDC_SLOT", "CDC_PUBLICATION", "CDC_APPLY_FLIGHT_ID", "CDC_CHAIN_APPLY"),
    "apply": _SHARED + _MIRROR,
    # Compaction reads no table list and no PostgreSQL.
    # Owner-only settings: never passed through from a runner (Phase 3 plan M3).
    "compact": (
        "CDC_SOURCE_ID",
        "CDC_DATABASE",
        "CDC_RUNTIME_SECONDS",
        "CDC_MIRROR_DATABASE",
        "CDC_COMPACT_EXPIRE",
        "CDC_MAX_RETAINED_LAG_HOURS",
        "CDC_LOG_STORAGE_BUDGET_GB",
        "CDC_OPS_DATABASE",
    ),
    "backfill": _SHARED
    + _SOURCE
    + _MIRROR
    + (
        "CDC_BACKFILL_TABLE",
        "CDC_BACKFILL_ABANDON",
        "CDC_BACKFILL_START",
        "CDC_BACKFILL_CHUNK_ROWS",
        "CDC_APPEND_SNAPSHOT_MAX_BYTES",
        "CDC_PUBLICATION",
        "CDC_BACKFILL_RETRIGGER",
        "CDC_BACKFILL_MAX_STALLED_RUNS",
        "CDC_BACKFILL_MAX_WAITING_RUNS",
        "CDC_BACKFILL_RETRIGGER_DELAY_SECONDS",
        "CDC_BACKFILL_STALLED_RUNS",
        "CDC_BACKFILL_WAITING_RUNS",
        "CDC_BACKFILL_LAST_PROGRESS",
    ),
    # An action Flight (action_flight.py) declares only its action's inputs: owner
    # settings are baked into its stub, so no runner can override them.
    "table-action": (
        "CDC_ACTION",
        "CDC_TABLE",
        "CDC_EXPECTED_STATE",
        "CDC_EXPECTED_BLOCKED_LSN",
        "CDC_CONFIRM",
        "CDC_DRY_RUN",
        "CDC_PREVIEW_ID",
        "CDC_REQUEST_ID",
    ),
    # Skip the whole unit a table is held before (quarantine_flight.py): the repair Flight.
    "quarantine": (
        "CDC_ACTION",
        "CDC_TABLE",
        "CDC_EXPECTED_EPOCH",
        "CDC_CONFIRM",
        "CDC_DRY_RUN",
        "CDC_PREVIEW_ID",
        "CDC_REQUEST_ID",
    ),
    # Recover a lost slot or skip to an LSN (epoch_flight.py): destructive.
    "epoch": (
        "CDC_ACTION",
        "CDC_EXPECTED_EPOCH",
        "CDC_TO_LSN",
        "CDC_CONFIRM",
        "CDC_DRY_RUN",
        "CDC_PREVIEW_ID",
        "CDC_REQUEST_ID",
    ),
    # Delete a source (delete_source_flight.py): destructive.
    "delete-source": (
        "CDC_ACTION",
        "CDC_EXPECTED_EPOCH",
        "CDC_CONFIRM",
        "CDC_DRY_RUN",
        "CDC_PREVIEW_ID",
        "CDC_REQUEST_ID",
    ),
    # Add and remove a table (membership_flight.py): its own Flight, the only action
    # Flight holding the source's PostgreSQL secret.
    "table-membership": (
        "CDC_ACTION",
        "CDC_TABLE",
        "CDC_FORCE",
        "CDC_CONFIRM",
        "CDC_MODE",
        "CDC_IGNORE_DELETES",
        "CDC_DRY_RUN",
        "CDC_PREVIEW_ID",
        "CDC_REQUEST_ID",
    ),
}

# Statuses after which the generation still has work and nothing else will resume it.
CONTINUE = (
    "snapshotting",
    "catching_up",
    "waiting_for_marker",
    "waiting_for_ingest",
    "busy_publish",
    # Replica capture (external mode): the standby's replay is behind the generation's L,
    # or no complete unit ends past B yet (the DBA's heartbeat job guarantees one).
    "waiting_for_replay",
    "waiting_past_replay",
    # An append snapshot's slot on the standby waited past its bound.
    "waiting_for_primary",
)
# Statuses that wait on another Flight (ingest, apply) or on the standby; re-triggering
# at once would spin.
WAITING = (
    "waiting_for_marker",
    "waiting_for_ingest",
    "busy_publish",
    "waiting_for_replay",
    "waiting_past_replay",
    "waiting_for_primary",
)
# A drained, capped, needs_action or conflicted ingest may have durable progress.
INGEST_CHAIN_STATUSES = ("drained", "capped", "needs_action", "conflict")


class ConfigError(ValueError):
    """An invalid config value. The message names the key, never the value."""


def blank_to_unset(environ=None, prefix="CDC_"):
    """Declared-but-empty config arrives as "". Treat it as unset, so the defaults in
    config.py apply. Only our own prefix is touched; secrets never use it."""
    environ = os.environ if environ is None else environ
    for key in [k for k, v in environ.items() if k.startswith(prefix) and v == ""]:
        del environ[key]


def flag(environ, key, default):
    value = environ.get(key, "")
    if value == "":
        return default
    if value not in ("0", "1"):
        raise ConfigError(f"{key} must be 0 or 1")
    return value == "1"


def integer(environ, key, default, low, high):
    value = environ.get(key, "")
    if value == "":
        return default
    if not re.fullmatch(r"[0-9]{1,9}", value) or not low <= int(value) <= high:
        raise ConfigError(f"{key} must be an integer in {low}..{high}")
    return int(value)


def flight_id(environ, key, default=""):
    value = environ.get(key, "") or default
    if value == "":
        return ""
    try:
        return str(uuid.UUID(value))
    except ValueError:
        raise ConfigError(f"{key} must be a Flight id (UUID)") from None


# Chain state the loop sets itself; everything else the backfill Flight declares is
# carried forward unchanged.
CHAIN_STATE = (
    "CDC_BACKFILL_TABLE",
    "CDC_BACKFILL_START",
    "CDC_BACKFILL_ABANDON",
    "CDC_BACKFILL_STALLED_RUNS",
    "CDC_BACKFILL_WAITING_RUNS",
    "CDC_BACKFILL_LAST_PROGRESS",
)
CARRIED = tuple(k for k in FLIGHT_CONFIG["backfill"] if k not in CHAIN_STATE)

PROGRESS = re.compile(r"[0-9a-z_:]{0,256}")


@dataclass(frozen=True)
class ApplyChain:
    apply_flight_id: str = ""
    enabled: bool = True

    @classmethod
    def from_env(cls, environ=None):
        environ = os.environ if environ is None else environ
        return cls(
            flight_id(environ, "CDC_APPLY_FLIGHT_ID"),
            flag(environ, "CDC_CHAIN_APPLY", True),
        )


@dataclass(frozen=True)
class BackfillChain:
    table: str
    start: bool = True
    enabled: bool = True
    # Always the runtime's MOTHERDUCK_FLIGHT_ID, never config: a run override could
    # otherwise point this owner-token trigger at any Flight the owner has.
    self_flight_id: str = ""
    max_stalled_runs: int = 10
    delay_seconds: int = 30
    stalled_runs: int = 0
    max_waiting_runs: int = 60
    waiting_runs: int = 0
    last_progress: str = ""
    carried: dict = field(default_factory=dict)

    @classmethod
    def from_env(cls, table, environ=None):
        environ = os.environ if environ is None else environ
        progress = environ.get("CDC_BACKFILL_LAST_PROGRESS", "")
        if not PROGRESS.fullmatch(progress):
            raise ConfigError("CDC_BACKFILL_LAST_PROGRESS is malformed")
        chain = cls(
            table,
            start=flag(environ, "CDC_BACKFILL_START", True),
            enabled=flag(environ, "CDC_BACKFILL_RETRIGGER", True),
            self_flight_id=flight_id(environ, "MOTHERDUCK_FLIGHT_ID"),
            max_stalled_runs=integer(environ, "CDC_BACKFILL_MAX_STALLED_RUNS", 10, 1, 100),
            delay_seconds=integer(environ, "CDC_BACKFILL_RETRIGGER_DELAY_SECONDS", 30, 0, 600),
            stalled_runs=integer(environ, "CDC_BACKFILL_STALLED_RUNS", 0, 0, 100),
            max_waiting_runs=integer(environ, "CDC_BACKFILL_MAX_WAITING_RUNS", 60, 1, 1000),
            waiting_runs=integer(environ, "CDC_BACKFILL_WAITING_RUNS", 0, 0, 1000),
            last_progress=progress,
            # A chain behaves like one long run: the operator's one-off overrides
            # (already validated by this run) carry into every re-triggered run.
            carried={k: environ[k] for k in CARRIED if environ.get(k, "") != ""},
        )
        return chain


def run_flight(con, target, overrides=None):
    """Start a run of `target` with config overrides; return (run_id, run_number).
    Values are bound as parameters, never formatted into SQL. The run id joins to
    query_history.user_agent `md-flights(<flight_id>,run_id=...)`."""
    target = str(uuid.UUID(target))
    overrides = overrides or {}
    keys, values = list(overrides), [overrides[k] for k in overrides]
    if not all(isinstance(x, str) for x in keys + values):
        raise TypeError("Flight config overrides must be strings")
    row = con.execute(
        "SELECT run_id, run_number FROM MD_RUN_FLIGHT(flight_id := ?::UUID, "
        "config := map(?::VARCHAR[], ?::VARCHAR[]))",
        [target, keys, values],
    ).fetchone()
    return (None, None) if row is None else (str(row[0]), row[1])


def emit(**fields):
    print(json.dumps(dict(chain=fields.pop("event"), **fields)), file=sys.stderr, flush=True)


def after_ingest(result, before, chain, trigger):
    """Chain apply after an ingest run that durably advanced the checkpoint. Best
    effort: a failed trigger never fails ingest, because apply's safety cron catches
    up. Returns the outcome for logging and tests."""
    if not chain.enabled or not chain.apply_flight_id:
        return "disabled"
    if result.status not in INGEST_CHAIN_STATUSES or result.boundary <= before:
        return "skipped"
    try:
        run = trigger(chain.apply_flight_id, {})
    except Exception as exc:
        # Messages can carry hosts or ids; the type is enough to act on.
        emit(event="trigger_failed", target="apply", error=type(exc).__name__)
        return "trigger_failed"
    emit(event="triggered", target="apply", run=run)
    return "triggered"


def progress_signature(result, ingest_lsn=0):
    """What moved since the last run: the generation's own counters, and the ingest
    checkpoint, which the waiting statuses depend on. The bound then means neither
    backfill nor ingest moved, not "ingest is still draining a backlog"."""
    return (
        f"{result.generation}:{result.status}:{result.chunks}:{result.rows}:"
        f"{result.boundary}:{ingest_lsn}"
    )


def after_backfill(result, chain, trigger, *, ingest_lsn=0, sleep=time.sleep):
    """Re-trigger this backfill Flight while the generation has work left.

    Every re-triggered run resumes only (CDC_BACKFILL_START=0), so a retry never
    begins a second generation. Two bounds stop the chain visibly:
    - stalled: neither the generation nor the ingest checkpoint moved for
      max_stalled_runs runs (ingest is down, or nothing at all is happening);
    - waiting: the generation itself didn't move for max_waiting_runs runs, although
      ingest did (stuck on something ingest progress doesn't fix).

    Returns (outcome, detail). The caller exits nonzero for "stalled", "waiting" and
    "trigger_failed", because nothing else resumes a backfill."""
    counts = dict(stalled_runs=chain.stalled_runs, waiting_runs=chain.waiting_runs)
    if result.status not in CONTINUE:
        return "stopped", counts
    if not chain.enabled:
        return "disabled", counts
    signature = progress_signature(result, ingest_lsn)
    generation_moved = signature.rsplit(":", 1)[0] != chain.last_progress.rsplit(":", 1)[0]
    # BackfillResult has no catch-up position; catching_up returns only after a full
    # run of catch-up work against the deadline, so it counts as progress.
    if result.status == "catching_up":
        generation_moved = True
    stalled = 0 if generation_moved or signature != chain.last_progress else chain.stalled_runs + 1
    waiting = 0 if generation_moved else chain.waiting_runs + 1
    counts = dict(stalled_runs=stalled, waiting_runs=waiting)
    if stalled >= chain.max_stalled_runs:
        return "stalled", counts
    if waiting >= chain.max_waiting_runs:
        return "waiting", counts
    if not chain.self_flight_id:
        return "no_flight_id", counts
    if result.status in WAITING and chain.delay_seconds:
        sleep(chain.delay_seconds)
    overrides = {
        **chain.carried,
        "CDC_BACKFILL_TABLE": chain.table,
        "CDC_BACKFILL_START": "0",
        "CDC_BACKFILL_ABANDON": "",
        "CDC_BACKFILL_STALLED_RUNS": str(stalled),
        "CDC_BACKFILL_WAITING_RUNS": str(waiting),
        "CDC_BACKFILL_LAST_PROGRESS": signature,
    }
    try:
        run = trigger(chain.self_flight_id, overrides)
    except Exception as exc:
        emit(event="trigger_failed", target="backfill", error=type(exc).__name__)
        return "trigger_failed", counts
    emit(event="triggered", target="backfill", run=run, **counts)
    return "triggered", counts
