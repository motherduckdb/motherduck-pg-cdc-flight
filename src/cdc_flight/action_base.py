"""The action-Flight framework, shared by every action Flight: input validation (Spec,
parse), previews (decision, record_preview, spender, checker), who started a run, and
the best-effort audit copy (finish). It imports neither apply nor table_actions, so an
action Flight imports only the cores it uses (action_flight.py is the table-action
Flight on top of it).
"""

import hashlib
import json
import logging
import re
import threading
import uuid
from dataclasses import dataclass, field

import duckdb

from cdc_flight import ops
from cdc_flight.action_types import Refused, lsn_or_none
from cdc_flight.chain import ConfigError, flag
from cdc_flight.config import quoted
from cdc_flight.config import table_name as split_table

logger = logging.getLogger(__name__)

# Observed values stay raw integers in previews and audits, as on format 1. These say
# which ones human text renders as X/Y LSNs and which as plain counts or ordinals
# (durable_frame_end is an exclusive frame ordinal, never an LSN).
LSN_KEYS = frozenset(
    (
        "checkpoint",
        "confirmed_end",
        "open_unit_anchor",
        "active_commit_lsn",
        "last_member_commit_lsn",
        "last_member_end_lsn",
        "unit_start_lsn",
        "unit_end_lsn",
        "first_commit_lsn",
        "last_commit_lsn",
        "blocked_commit_lsn",
        "abandoned_unit_start_lsn",
        "abandoned_first_commit_lsn",
        "abandoned_last_commit_lsn",
        "abandoned_active_commit_lsn",
    )
)


INT_KEYS = frozenset(
    (
        "format_version",
        "source_epoch",
        "generation_id",
        "open_chunks",
        "durable_frame_end",
        "unit_commit_count",
        "unit_event_count",
        "config_revision",
        "abandoned_frame_end",
        "abandoned_chunk_count",
        "abandoned_into_epoch",
    )
)


PREVIEW_MINUTES = 15


NO_PREVIEW = (
    f"no matching unused preview from the last {PREVIEW_MINUTES} minutes; run a dry run again"
)


EXIT = {"done": 0, "already": 0, "would": 0, "waiting": 0, "refused": 2, "busy": 3}


class InputError(ValueError):
    """An invalid input. The message names the key, never the value."""


@dataclass(frozen=True)
class Inputs:
    action: str
    table: str | None = None
    values: dict = field(default_factory=dict)  # the library's expected_* arguments
    dry_run: bool = False
    preview_id: str | None = None
    # The caller's correlation id (CDC_REQUEST_ID), echoed into the audit row so the Dive
    # matches a preview or result by it. Not a security control (actions are owner-only).
    request_id: str | None = None

    def shown(self):
        """For the audit: LSNs as text."""
        values = dict(self.values)
        if "expected_blocked_lsn" in values:
            values["expected_blocked_lsn"] = lsn_or_none(values["expected_blocked_lsn"])
        return dict(table=self.table, preview_id=self.preview_id, **values)


@dataclass(frozen=True)
class Spec:
    """An action Flight's inputs: its declared keys, the keys each action requires
    (`uses`) or may set (`optional`), a parser per value key (text -> (argument, value),
    raising InputError), and an optional cross-key `check(action, table, values)`."""

    keys: tuple
    uses: dict
    values: dict
    optional: dict = field(default_factory=dict)
    check: object = None


# Framework keys, read the same way by every action Flight.
FRAMEWORK = ("CDC_ACTION", "CDC_DRY_RUN", "CDC_PREVIEW_ID", "CDC_REQUEST_ID")
REQUEST_ID = re.compile(r"[A-Za-z0-9_-]{1,64}")


def parse(environ, spec):
    """The declared keys, checked before any connection (blank means unset)."""
    raw = {key: environ.get(key, "") for key in spec.keys}
    action = raw["CDC_ACTION"]
    if action not in spec.uses:
        raise InputError(f"CDC_ACTION must be one of {', '.join(spec.uses)}")
    for key in spec.keys:
        if key in FRAMEWORK:
            continue
        if key in spec.uses[action] and raw[key] == "":
            raise InputError(f"{key} is required for {action}")
        allowed = (*spec.uses[action], *spec.optional.get(action, ()))
        if key not in allowed and raw[key] != "":
            raise InputError(f"{key} is not used by {action}; leave it blank")
    table, values = raw.get("CDC_TABLE") or None, {}
    if table is not None:
        try:
            split_table(table)
        except ValueError:
            raise InputError('CDC_TABLE must be a "schema.table" name') from None
    for key, parser in spec.values.items():
        if raw[key]:
            name, value = parser(raw[key])
            values[name] = value
    if spec.check is not None:
        spec.check(action, table, values)
    try:
        dry_run = flag(raw, "CDC_DRY_RUN", False)
    except ConfigError as exc:
        raise InputError(str(exc)) from None
    preview = raw["CDC_PREVIEW_ID"]
    if dry_run and preview:
        raise InputError("CDC_PREVIEW_ID is not used by a dry run; leave it blank")
    if not dry_run:
        if not preview:
            raise InputError("CDC_PREVIEW_ID is required: run a dry run first (CDC_DRY_RUN=1)")
        try:
            preview = str(uuid.UUID(preview))
        except ValueError:
            raise InputError("CDC_PREVIEW_ID must be a dry run's run id (a UUID)") from None
    request_id = raw.get("CDC_REQUEST_ID", "")
    if request_id and not REQUEST_ID.fullmatch(request_id):
        raise InputError("CDC_REQUEST_ID must be 1-64 characters of A-Z, a-z, 0-9, _ or -")
    return Inputs(action, table, values, dry_run, preview or None, request_id or None)


def run_id_from(environ):
    """The runtime's MOTHERDUCK_FLIGHT_RUN_ID (a dry run's preview id); outside a
    Flight, a new one."""
    try:
        return str(uuid.UUID(environ.get("MOTHERDUCK_FLIGHT_RUN_ID", "")))
    except ValueError:
        return str(uuid.uuid4())


def merged(observed, extra):
    """`observed` plus fields read beside it (a format-2 unit position). A key both report
    with different values refuses, so a core key can never be shadowed out of the CAS."""
    clash = sorted(k for k in extra if k in observed and observed[k] != extra[k])
    if clash:
        raise Refused(f"conflicting observation key {', '.join(clash)}")
    return dict(observed, **extra)


# Inputs a preview doesn't hash, by action: accept-types' literal comes from its dry run,
# so only the real run carries it. Its digest and the type_digest decision field already
# bind the change, and the library compares the literal exactly. A typed-first literal
# (quarantine, epoch, delete-source, a forced remove) is sent on both runs and stays hashed.
UNHASHED = {"accept-types": ("confirm",)}


def preview_hash(inputs, decided):
    """sha256 of the canonical JSON of the inputs and the decision fields:
    LSNs as integers, so 0/5a and 0/5A hash alike."""
    skip = UNHASHED.get(inputs.action, ())
    payload = dict(
        action=inputs.action,
        table=list(split_table(inputs.table)) if inputs.table else None,
        inputs={k: v for k, v in inputs.values.items() if k not in skip},
        decision=decided,
    )
    text = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(text.encode()).hexdigest()


# The previews table of the database an action mutates: the mirror for table-action,
# cdc_internal for table-membership. `store` is an Apply or IngestLog (execute()).
MIRROR_PREVIEWS = "{m}previews"


def record_preview(store, preview_id, inputs, decided, table=MIRROR_PREVIEWS):
    """The dry run's only write (apart from creating missing tables). Then previews
    older than a day are pruned in their own statement, best effort: two overlapping
    dry runs may both try, and the loser's conflict must not fail its dry run."""
    store.execute(
        f"INSERT INTO {table} (id,action,inputs_hash,created_at) VALUES (?,?,?,current_timestamp)",
        [preview_id, inputs.action, preview_hash(inputs, decided)],
    )
    try:
        store.execute(f"DELETE FROM {table} WHERE created_at<current_timestamp-INTERVAL 1 DAY")
    except duckdb.Error as exc:
        logger.warning("preview pruning skipped (%s)", type(exc).__name__)


MATCHING = (
    "WHERE id=? AND action=? AND inputs_hash=? AND used_at IS NULL "
    f"AND created_at>current_timestamp-INTERVAL {PREVIEW_MINUTES} MINUTE"
)


PREVIEWS_DDL = (
    "CREATE TABLE IF NOT EXISTS {s}previews (id VARCHAR PRIMARY KEY, action VARCHAR, "
    "inputs_hash VARCHAR, created_at TIMESTAMPTZ, used_at TIMESTAMPTZ, used_by VARCHAR, "
    "actor VARCHAR)"
)


def ensure_previews(log):
    """Create `<source>.previews` in cdc_internal if missing: catalog-checked, in its own
    transaction, never inside a guarded one, retried once on a concurrent create
    (shared by every action Flight whose previews live in cdc_internal).
    Refused when the source schema itself doesn't exist: this Flight never creates it."""
    for attempt in range(2):
        catalog = log.catalog()
        if not catalog:
            return False
        if "previews" in catalog:
            return True
        try:
            with log.transaction():
                log.execute(PREVIEWS_DDL)
            return True
        except duckdb.TransactionException:
            if attempt:
                raise
    return True


def backfill_config(table):
    """The only config resync passes to the backfill Flight."""
    return {"CDC_BACKFILL_TABLE": table, "CDC_BACKFILL_START": "1"}


# The run's own row says who started it; the owner's token can read it.
WHOIS_SQL = (
    'SELECT started_by_name FROM MD_LIST_FLIGHT_RUNS(flight_id := ?::UUID, "limit" := 100) '
    "WHERE run_id::VARCHAR=?"
)


WHOIS_SECONDS = 5


def whois(con, environ, run, *, seconds=WHOIS_SECONDS, sql=WHOIS_SQL):
    """Who started this run, or None: best effort, bounded by interrupting the query
    (DuckDB has no statement timeout), never raising. Looked up before the action, so
    the name is written in the action's own transaction."""
    flight = environ.get("MOTHERDUCK_FLIGHT_ID", "")
    run_id = environ.get("MOTHERDUCK_FLIGHT_RUN_ID", "")
    if not flight or not run_id:
        run.step("whois", "skipped")
        return None
    timer = threading.Timer(seconds, con.interrupt)
    timer.daemon = True
    timer.start()
    try:
        row = con.execute(sql, [flight, run_id]).fetchone()
        name = row[0] if row and row[0] else None
        if name:
            run.step("whois", "ok")
        else:
            # In a Flight but not found (or no name): distinct from `skipped`, which means
            # outside a Flight. NoRow would also show an oldest-first listing.
            run.step("whois", "failed", error="NoRow" if row is None else "NoName")
        return name
    except Exception as exc:
        run.step("whois", "failed", error=type(exc).__name__)
        return None
    finally:
        timer.cancel()


def actor_of(con, started_by):
    """The actor written in the action's audit rows: the Flight's owner (current_user)
    and, when known, who started the run."""
    actor = "flight:" + con.execute("SELECT current_user").fetchone()[0]
    return actor + (f" (started by {started_by})" if started_by else "")


def decision(action, observed, fields, store):
    """The fields a preview promises. Only format 2 exists: the store is required, and
    one that doesn't declare format_version 2 (the class attribute), an observation
    reporting any other format, or a missing promised field refuses before any write."""
    if getattr(store, "format_version", None) != 2:
        raise Refused(
            f"unsupported store format {getattr(store, 'format_version', None)}: "
            "only format 2 stores are served"
        )
    if observed.get("format_version", 2) != 2:
        raise Refused(f"unsupported store format {observed['format_version']}")
    names = fields[action]
    missing = [name for name in names if name not in observed]
    if missing:
        raise Refused(f"the observation lacks {', '.join(missing)}")
    return {name: observed[name] for name in names}


def _match(inputs, observed, fields, store):
    return [
        inputs.preview_id,
        inputs.action,
        preview_hash(inputs, decision(inputs.action, observed, fields, store)),
    ]


def spender(store, inputs, run_id, actor, table, fields):
    """The `consume` hook: spend the named preview if it matches what this run observes,
    inside the caller's transaction; else raise Refused (which rolls it back)."""

    def consume(observed):
        count = store.execute(
            f"UPDATE {table} SET used_at=current_timestamp,used_by=?,actor=? {MATCHING}",
            [run_id, actor, *_match(inputs, observed, fields, store)],
        ).fetchone()[0]
        if count != 1:
            raise Refused(NO_PREVIEW)

    return consume


def checker(store, inputs, table, fields):
    """A read-only `check` hook: refuse before any write unless the named preview
    would still be spent. The spend itself stays the guarded
    compare-and-set in the action's transaction."""

    def check(observed):
        count = store.execute(
            f"SELECT count(*) FROM {table} {MATCHING}", _match(inputs, observed, fields, store)
        ).fetchone()[0]
        if count != 1:
            raise Refused(NO_PREVIEW)

    return check


def finish(
    con, owner, inputs, out, run, *, audit=ops.write_audit, started_by=None, tls=None, refresh=None
):
    """Best effort, after the authoritative writes: the cdc_ops audit copy, then one
    CREATE SNAPSHOT whenever that copy was written, for every status: the Dive reads an
    action's result (a dry run's preview id, a refusal) only from cdc_ops.
    Skipped without cdc_ops; never on a timer; never raises.

    `refresh` (table-action and quarantine): called first, to refresh what the action
    changed in cdc_ops before the audit copy."""
    if refresh is not None:
        refresh()
    try:
        written = audit(
            con,
            owner.database,
            owner.source_id,
            out["run_id"],
            inputs.action,
            inputs.shown(),
            inputs.dry_run,
            json.dumps(out, sort_keys=True, default=str),
            database=owner.ops_database,
            started_by=started_by,
            request_id=inputs.request_id,
            # PostgreSQL TLS, from the Flights that connect to it (pg_ca.fields); None
            # (NULL) for the actions that never do.
            tls=tls,
        )
    except Exception as exc:
        logger.warning("audit copy failed (%s)", type(exc).__name__)
        written = False
    run.step("audit", "ok" if written else "skipped")
    if not written:
        run.step("snapshot", "skipped")
        return
    try:
        con.execute(f"CREATE SNAPSHOT OF {quoted(owner.ops_database)}")
        run.step("snapshot", "ok")
    except Exception as exc:
        logger.warning("snapshot failed (%s)", type(exc).__name__)
        run.step("snapshot", "failed", error=type(exc).__name__)
