"""Action Flights: the framework, and the table-action Flight.

Only the Flights' owner runs an action Flight (no RUN grants; deploy refuses grantees),
and a run may override any key the Flight declares. So an action Flight declares only its action's inputs (KEYS). The
owner's settings (the source, its databases, the Flights it may start) are baked into
the Flight's stub as OWNER at build time and read from nothing else: no runner key can name
a database, a table list, a threshold or a target Flight (M3 by construction).

A run:
1. validate: every key's format, before any connection. A key the action doesn't use
   must be blank. A refusal names the key, never the value.
2. a dry run reads and validates, then records a preview (its only write; no lease).
   A real run names that preview (CDC_PREVIEW_ID, the dry run's run id). It is spent
   once, within 15 minutes, and only while the inputs and the action's decision
   fields (DECISION_FIELDS) hash the same as what the real run observes.
3. the action: table_actions (pause, resume, retry; the preview is spent in the
   action's own transaction), or a trigger with config built here (resync and
   resync-all start the backfill Flight, compact-now the compaction Flight; the
   preview and an audit row are committed first).
4. audit: the mirror's own rows (runs, previews), then a best-effort cdc_ops copy.
5. after the audit copy is written, one best-effort CREATE SNAPSHOT of the operations
   database, whatever the status, so the Dive sees a dry run's preview or a refusal at once.

The guards stop mis-clicks and stale screens, not an owner who means it.
"""

import json
import logging
import os
import uuid
from dataclasses import asdict, dataclass, fields

import duckdb

from cdc_flight import action_base as base
from cdc_flight import explain, ops, ops_apply, table_actions
from cdc_flight.action_base import (  # noqa: F401 (re-exported)
    EXIT,
    FRAMEWORK,
    INT_KEYS,
    LSN_KEYS,
    MATCHING,
    MIRROR_PREVIEWS,
    NO_PREVIEW,
    PREVIEW_MINUTES,
    PREVIEWS_DDL,
    WHOIS_SECONDS,
    WHOIS_SQL,
    InputError,
    Inputs,
    Spec,
    actor_of,
    backfill_config,
    ensure_previews,
    merged,
    preview_hash,
    record_preview,
    run_id_from,
    whois,
)
from cdc_flight.apply import Apply, ApplyError
from cdc_flight.chain import FLIGHT_CONFIG, run_flight
from cdc_flight.config import (
    ApplyConfig,
    heartbeat_writer,
    identifier,
    quoted,
)
from cdc_flight.flightlog import Run
from cdc_flight.ingest_common import SourceMismatch
from cdc_flight.releases import record_release
from cdc_flight.retention import SourceDeleted, check_lifecycle
from cdc_flight.table_actions import ActionResult, Refused, lsn_text, parse_lsn

logger = logging.getLogger(__name__)

# The table-action Flight's declared keys.
KEYS = FLIGHT_CONFIG["table-action"]
# The inputs each action uses; every other input key must be blank.
USES = {
    "pause": ("CDC_TABLE", "CDC_EXPECTED_STATE"),
    "resume": ("CDC_TABLE", "CDC_EXPECTED_STATE"),
    "retry": ("CDC_TABLE", "CDC_EXPECTED_BLOCKED_LSN"),
    "resync": ("CDC_TABLE", "CDC_EXPECTED_STATE"),
    # An append (insert-only) table's first mirror generation, from the log.
    "start-from-log": ("CDC_TABLE",),
    # A registered table's first snapshot (no generation yet): the backfill Flight, start.
    "start-snapshot": ("CDC_TABLE",),
    # A held source retype's stored type, accepted (CDC_CONFIRM: the real run's literal).
    "accept-types": ("CDC_TABLE",),
    "resync-all": (),
    "compact-now": (),
}
# What a preview promises: the real run is refused if any of these moved since the
# dry run. The token and checkpoint are shown but not hashed, so an apply run between
# the dry run and the click doesn't spend the preview. A blocked table is
# held before one whole unit, so quarantine and retry promise that exact unit: its
# bounds, member count and terminal digest.
UNIT_FIELDS = (
    "format_version",
    "source_epoch",
    "generation_id",
    "unit_start_lsn",
    "unit_end_lsn",
    "unit_commit_count",
    "unit_digest",
    "config_revision",
)
DECISION_FIELDS = {
    "pause": ("state", "generation", "epoch", "format_version", "generation_id"),
    "resume": ("state", "generation", "epoch", "format_version", "generation_id"),
    "retry": ("state", "generation", "epoch", "blocked", *UNIT_FIELDS),
    "resync": (
        "state",
        "generation",
        "active_generation",
        "format_version",
        "generation_id",
        # An append table's resync is an exact re-snapshot; under ignore_deletes it loses
        # the purged rows from the live mirror, so the preview binds both (Q3a).
        "capture_mode",
        "ignore_deletes",
    ),
    "resync-all": ("needs_resync",),
    "start-from-log": ("capture_mode", "generation", "format_version"),
    "start-snapshot": ("generation", "active_generation", "capture_mode", "source_held"),
    "accept-types": ("state", "generation", "epoch", "blocked", *UNIT_FIELDS, "type_digest"),
    "compact-now": (),
    # The quarantine Flight: observe() fields that decide it. The poison match on the
    # detail is re-checked by the library under the lease.
    "quarantine": ("state", "generation", "epoch", "blocked", *UNIT_FIELDS),
}


STATES = ("live", "paused", "needs_action", "needs_resync")
# What a resync costs, by the state it starts from.
RESYNC_COST = {
    "needs_resync": "a full reload of the table from the source",
    "needs_action": "replaces the held mirror table with a full reload from the source",
    "paused": "ends the pause; a full reload of the table from the source",
    "live": "a full reload of a current table from the source",
}


@dataclass(frozen=True)
class Owner:
    """The owner's settings, baked into the stub (OWNER) by build_flight.py."""

    source_id: str
    backfill_flight: str
    compact_flight: str
    database: str = "cdc_internal"
    heartbeat_writer: str = "self"  # replica capture: the source's mode (trusted)
    mirror_database: str = ""
    ops_database: str = ops.DATABASE

    def __post_init__(self):
        heartbeat_writer(self.heartbeat_writer)
        for name in ("source_id", "database", "ops_database"):
            identifier(getattr(self, name))
        object.__setattr__(
            self, "mirror_database", identifier(self.mirror_database or f"{self.source_id}_mirror")
        )
        for name in ("backfill_flight", "compact_flight"):
            object.__setattr__(self, name, str(uuid.UUID(getattr(self, name))))
        self.config()  # the mirror and change-log databases must differ
        if self.ops_database in (self.database, self.mirror_database):
            raise ValueError("the operations database must differ from the stores")

    @classmethod
    def from_dict(cls, values):
        unknown = set(values) - {f.name for f in fields(cls)}
        if unknown:
            raise ValueError(f"unknown owner settings: {sorted(unknown)}")
        return cls(**values)

    def config(self):
        """Built from values: never ApplyConfig.from_env."""
        return ApplyConfig(
            self.source_id, (), database=self.database, mirror_database=self.mirror_database
        )


def _state(text):
    if text not in STATES:
        raise InputError(f"CDC_EXPECTED_STATE must be one of {', '.join(STATES)}")
    return "expected_state", text


def _blocked(text):
    try:
        return "expected_blocked_lsn", None if text == "none" else parse_lsn(text)
    except ValueError:
        raise InputError('CDC_EXPECTED_BLOCKED_LSN must be "X/Y" in hex, or none') from None


def _confirm(text):
    return "confirm", text


TABLE_ACTION = Spec(
    KEYS,
    USES,
    {"CDC_EXPECTED_STATE": _state, "CDC_EXPECTED_BLOCKED_LSN": _blocked, "CDC_CONFIRM": _confirm},
    optional={"accept-types": ("CDC_CONFIRM",)},
)


# Previews -----------------------------------------------------------------------


def unit_literal(table, epoch, start, end):
    """The format-2 quarantine confirmation, built from the observed blocked unit and
    compared exactly, never parsed."""
    return f"QUARANTINE {table} UNIT {epoch} {lsn_text(start)}..{lsn_text(end)}"


def _quarantined(apply, relation_id, observed, table, inputs):
    """(epoch, start, end) of this generation's quarantined unit whose exact literal was
    typed, or None. The literal is built from the stored row, its own epoch included,
    as the original run's was."""
    rows = apply.execute(
        "SELECT epoch,unit_start_lsn,unit_end_lsn FROM {m}quarantine WHERE relation_id=? "
        "AND generation_id=? AND unit_start_lsn IS NOT NULL",
        [relation_id, observed["generation"]],
    ).fetchall()
    for epoch, start, end in rows:
        if inputs.values.get("confirm") == unit_literal(table, epoch, start, end):
            return epoch, start, end
    return None


def unit_arguments(apply, inputs):
    """(extra library arguments, refusal or None) for quarantine and retry: they act on
    the observed blocked unit, which the library re-checks under its lease."""
    if inputs.action not in ("quarantine", "retry"):
        return {}, None
    table = inputs.table
    try:
        relation_id, observed = table_actions.observe(apply, table)
    except ApplyError:
        return {}, None  # the library reports it
    if observed is None:
        return {}, None
    shown = _shown(observed)
    if observed.get("format_version") != 2:
        return {}, ActionResult(
            "refused", table, "the store did not report format_version 2", [], shown
        )
    needed = ("source_epoch", "unit_start_lsn", "unit_end_lsn")
    missing = [name for name in needed if name not in observed]
    if missing:
        detail = f"the format-2 observation lacks {', '.join(missing)}"
        return {}, ActionResult("refused", table, detail, [], shown)
    start, end = observed["unit_start_lsn"], observed["unit_end_lsn"]
    if start is not None and end is None:
        # A blocked start with no complete unit in the table's current epoch: the state
        # moved since the dry run (an epoch change). Refused before any read or write.
        detail = (
            f"state changed: no complete unit starts at {lsn_text(start)} in epoch "
            f"{observed.get('source_epoch')}; run a new dry run"
        )
        return {}, ActionResult("refused", table, detail, [], _shown(observed))
    if start is None or end is None:
        # No blocked unit. A repeat of a done quarantine (a lost response) names a unit
        # already quarantined for this generation: its literal is built from the stored
        # row and compared exactly (never parsed), and the library reports already.
        if inputs.action == "quarantine":
            done = _quarantined(apply, relation_id, observed, table, inputs)
            if done is not None:
                epoch, done_start, done_end = done
                if inputs.values.get("expected_epoch") != epoch:
                    detail = (
                        f"state changed: expected epoch {inputs.values.get('expected_epoch')}, "
                        f"found {epoch}"
                    )
                    return {}, ActionResult("refused", table, detail, [], _shown(observed))
                return dict(unit_start_lsn=done_start, unit_end_lsn=done_end), None
        return {}, None  # the library refuses
    if inputs.action == "retry":
        return dict(expected_unit=(start, end)), None
    epoch = observed.get("source_epoch")
    if inputs.values.get("expected_epoch") != epoch:
        detail = (
            f"state changed: expected epoch {inputs.values.get('expected_epoch')}, found {epoch}"
        )
        return {}, ActionResult("refused", table, detail, [], shown)
    expected = unit_literal(table, epoch, start, end)
    if not inputs.dry_run and inputs.values.get("confirm") != expected:
        return {}, ActionResult("refused", table, f"type exactly: {expected}", [], shown)
    return dict(unit_start_lsn=start, unit_end_lsn=end), None


# The table-action Flight's defaults over the framework: its own decision fields, its
# mirror's previews table, and the apply_status refresh after a done table action.
def parse(environ, spec=TABLE_ACTION):
    return base.parse(environ, spec)


def decision(action, observed, fields=None, store=None):
    return base.decision(action, observed, DECISION_FIELDS if fields is None else fields, store)


def spender(store, inputs, run_id, actor, table=MIRROR_PREVIEWS, fields=None):
    return base.spender(
        store, inputs, run_id, actor, table, DECISION_FIELDS if fields is None else fields
    )


def checker(store, inputs, table=MIRROR_PREVIEWS, fields=None):
    return base.checker(store, inputs, table, DECISION_FIELDS if fields is None else fields)


# Actions whose done changes a table's apply_status row, refreshed in cdc_ops by finish
# (quarantine runs through finish too).
REFRESHES = ("pause", "resume", "retry", "quarantine", "start-from-log")


def finish(con, owner, inputs, out, run, *, audit=ops.write_audit, started_by=None, tls=None):
    """base.finish, first refreshing a done pause, resume, retry, quarantine or
    start-from-log's apply_status row, so the Dive shows the new state before the next
    apply run."""
    refresh = None
    if out.get("status") == "done" and inputs.action in REFRESHES and inputs.table:

        def refresh():
            ops_apply.report_table(
                con, owner, out["run_id"], inputs.table, database=owner.ops_database
            )

    base.finish(
        con, owner, inputs, out, run, audit=audit, started_by=started_by, tls=tls, refresh=refresh
    )


# Triggers -------------------------------------------------------------------------


COMPACT_CONFIG = {}  # compact-now passes nothing: thresholds are the owner's


def _active(apply):
    """Relation ids with an active backfill generation."""
    try:
        return {r[0] for r in apply.execute("SELECT relation_id FROM {m}backfills").fetchall()}
    except duckdb.CatalogException:
        return set()  # backfill never initialized this mirror


def _shown(observed):
    return {k: v for k, v in observed.items() if k != "detail"}


def resync_check(apply, inputs):
    """(relation_id, observed, None) for a table resync may start, else a refusal."""
    table = inputs.table
    try:
        relation_id, observed = table_actions.observe(apply, table)
    except ApplyError as exc:
        return None, None, ActionResult("refused", table, str(exc))
    if observed is None:
        return None, None, ActionResult("refused", table, "the table has no mirror yet")
    observed["ignore_deletes"] = False  # keyed; an append table's is read below
    if observed["capture_mode"] == "append":
        # An insert-only mirror is rebuilt by an exact re-snapshot (keyless phase 2), once
        # the backfill Flight's release can take one; at most one replaced mirror is kept
        # aside.
        refused = _append_resync_refusal(apply, table, relation_id, observed)
        if refused:
            return None, None, refused
    observed["active_generation"] = apply.active_generation(relation_id)
    shown = _shown(observed)
    if observed["source_held"]:
        detail = f"the source table is held ({observed['source_held']}); see cdc-flight-tables"
        return None, None, ActionResult("refused", table, detail, [], shown)
    expected = inputs.values["expected_state"]
    if expected != observed["state"]:
        detail = f"state changed: expected {expected}, found {observed['state']}"
        return None, None, ActionResult("refused", table, detail, [], shown)
    if observed["state"] not in RESYNC_COST:
        detail = f"a {observed['state']} table cannot be resynced"
        return None, None, ActionResult("refused", table, detail, [], shown)
    if observed["active_generation"]:
        detail = "a backfill is already running for this table"
        return None, None, ActionResult("refused", table, detail, [], shown)
    return relation_id, observed, None


def _append_resync_refusal(apply, table, relation_id, observed):
    """An append table's resync (a re-snapshot): refused while the backfill Flight's
    release predates append snapshots, or while a replaced mirror is still kept aside (Q3b).
    Records ignore_deletes in `observed` for the preview."""
    from cdc_flight.append_apply import kept_refusal
    from cdc_flight.config import table_name
    from cdc_flight.releases import APPEND_SNAPSHOT, release_level

    (ignore,) = apply.execute(
        "SELECT coalesce(ignore_deletes,false) FROM {s}tables WHERE relation_id=?", [relation_id]
    ).fetchone()
    observed["ignore_deletes"] = bool(ignore)
    if observed["state"] == "paused":
        detail = "the table is paused: resume it, then resync"
        return ActionResult("refused", table, detail, [], _shown(observed))
    level = release_level(apply.execute, "backfill")
    if level is None or level < APPEND_SNAPSHOT:
        detail = (
            "the backfill Flight predates append snapshots: start the table again from the "
            "log (start-from-log), or deploy the backfill Flight first"
        )
        return ActionResult("refused", table, detail, [], _shown(observed))
    kept = kept_refusal(apply, *table_name(table))
    if kept:
        return ActionResult("refused", table, kept, [], _shown(observed))
    return None


def append_resync_cost(table, observed):
    """The resync preview's text for an append table: what a re-snapshot costs."""
    name = table.split(".")[-1]
    kept = f"{name}__gen_{(observed['generation'] or '')[:8]}"
    text = (
        "an exact snapshot of the source table replaces the mirror; the current mirror is "
        f"kept aside as {kept}"
    )
    if observed.get("ignore_deletes"):
        text += (
            ". DELETEs are skipped for this table, so rows the source has purged are in the "
            f"current mirror but not in the new one: they remain only in {kept}"
        )
    return text


def snapshot_check(apply, inputs):
    """(relation_id, observed, None) for a registered table with no generation yet whose
    first snapshot may start, else (None, None, already or a refusal)."""
    table = inputs.table
    try:
        relation_id, observed = table_actions.observe(apply, table)
    except ApplyError as exc:
        return None, None, ActionResult("refused", table, str(exc))
    if observed is not None:
        detail = "the table already has a mirror generation; use resync"
        return None, None, ActionResult("already", table, detail, [], _shown(observed))
    mode, held = apply.execute(
        "SELECT capture_mode,held_reason FROM {s}tables WHERE relation_id=?", [relation_id]
    ).fetchone()
    observed = dict(
        generation=None,
        capture_mode=mode,
        source_held=held,
        active_generation=apply.active_generation(relation_id),
    )
    shown = _shown(observed)
    if held:
        detail = f"the source table is held ({held}); see cdc-flight-tables"
        return None, None, ActionResult("refused", table, detail, [], shown)
    if observed["active_generation"]:
        detail = "a backfill is already running for this table"
        return None, None, ActionResult("refused", table, detail, [], shown)
    if mode == "append":
        from cdc_flight.releases import APPEND_SNAPSHOT, release_level

        level = release_level(apply.execute, "backfill")
        if level is None or level < APPEND_SNAPSHOT:
            detail = (
                "the backfill Flight predates append snapshots: deploy it first, or start the "
                "table from the log (start-from-log)"
            )
            return None, None, ActionResult("refused", table, detail, [], shown)
    return relation_id, observed, None


def resync_pick(apply):
    """(observed, pick) for resync-all: every needs_resync table with no active
    generation and no source hold, longest held first; pick is (relation_id, table)."""
    active = _active(apply)
    held = {
        r[0]
        for r in apply.execute(
            "SELECT relation_id FROM {s}tables WHERE held_reason IS NOT NULL"
        ).fetchall()
    }
    rows = apply.execute(
        "SELECT relation_id,schema_name,table_name FROM {m}apply_state "
        "WHERE state='needs_resync' AND capture_mode IS DISTINCT FROM 'append' "
        "ORDER BY held_since NULLS LAST,schema_name,table_name"
    ).fetchall()
    eligible = [(r[0], f"{r[1]}.{r[2]}") for r in rows if r[0] not in active | held]
    observed = dict(needs_resync=sorted(r for r, _ in eligible), remaining=len(eligible))
    return observed, eligible[0] if eligible else None


def claim(apply, inputs, observed, *, run_id, actor, relation_id, audit_detail):
    """Spend the preview and write the audit row in one mirror transaction, before
    the trigger. Refused when no preview matches."""
    with apply.transaction():
        spender(apply, inputs, run_id, actor)(observed)
        if relation_id is not None:
            apply.execute(
                "INSERT INTO {m}runs (run_id,relation_id,status,boundary,detail,updated_at) "
                "VALUES (?,?,?,?,?,current_timestamp)",
                [run_id, relation_id, inputs.action, observed.get("applied"), audit_detail],
            )


# The run ---------------------------------------------------------------------------


def _out(inputs, result, run_id, **extra):
    return dict(
        status=result.status,
        action=inputs.action,
        table=result.table,
        detail=result.detail,
        next_steps=result.next_steps,
        observed=result.observed,
        run_id=run_id,
        **extra,
    )


def perform(apply, inputs, *, run_id, actor, owner, trigger, run=None, fault=None):
    """One action on an attached Apply; returns the result line (a dict). `fault(stage)`
    (tests only; no-op default) reaches the table action's own stages
    (before_action_commit, after_action_commit) on its real run."""
    run = run or Run()
    action, table = inputs.action, inputs.table
    mirror = apply.config.mirror_database
    previews = f"table:{mirror}._cdc.previews"

    def decided(branch, result, **extra):
        run.step("decide", "ok", branch=branch)
        if branch in ("refused", "already"):
            run.step(branch, "ok")
        return _out(inputs, result, run_id, **extra)

    def preview(result, observed, **extra):
        try:
            promised = decision(action, observed, store=apply)
        except Refused as exc:  # an observation that can't be promised: no write
            refused = ActionResult("refused", result.table, str(exc), [], result.observed)
            return _out(inputs, refused, run_id, **extra)
        explain.step(
            run,
            "preview",
            lambda: record_preview(apply, run_id, inputs, promised),
            effect=previews,
        )
        # The literal the real run needs: accept-types' comes from this dry run; a
        # typed-first one (quarantine) is the one this run checked.
        confirm = (
            result.observed.get("confirm")
            if action == "accept-types"
            else inputs.values.get("confirm")
        )
        return _out(inputs, result, run_id, preview_id=run_id, confirm=confirm, **extra)

    if action in table_actions.ACTIONS:
        library = table_actions.ACTIONS[action]
        common = dict(run_id=run_id, actor=actor, **inputs.values)
        unit, refused = unit_arguments(apply, inputs)
        if refused:
            return decided("refused", refused)
        common.update(unit)
        if inputs.dry_run:
            result = library(apply, table, dry_run=True, **common)
            if result.status != "would":
                return decided(result.status, result)
            run.step("decide", "ok", branch="would")
            return preview(result, result.observed)
        # The library refuses or reports `already` before any lease; otherwise it
        # spends the preview inside its own transaction.
        run.step("decide", "ok", branch="act")
        run.step("act", "running", effect=f"table:{mirror}._cdc.apply_state")
        result = library(
            apply, table, consume=spender(apply, inputs, run_id, actor), fault=fault, **common
        )
        run.step("act", "ok", effect=f"table:{mirror}._cdc.apply_state")
        run.step("outcome", "ok", branch=result.status)
        run.step(
            {"done": "done", "busy": "busy", "already": "already"}.get(result.status, "moved"), "ok"
        )
        return _out(inputs, result, run_id)

    # Triggers: resync, resync-all, compact-now.
    relation_id, extra = None, {}

    if action == "resync":
        relation_id, observed, refused = resync_check(apply, inputs)
        if refused:
            return decided("refused", refused)
        target, overrides = owner.backfill_flight, backfill_config(table)
        cost = (
            append_resync_cost(table, observed)
            if observed["capture_mode"] == "append"
            else RESYNC_COST[observed["state"]]
        )
        what = f"resync {table}: {cost}"
        steps = []
    elif action == "start-snapshot":
        relation_id, observed, refused = snapshot_check(apply, inputs)
        if refused:
            return decided(refused.status, refused)
        target, overrides = owner.backfill_flight, backfill_config(table)
        what = f"start the first snapshot of {table} (a backfill from the source)"
        steps = []
    elif action == "resync-all":
        observed, pick = resync_pick(apply)
        if pick is None:
            nothing = ActionResult("already", None, "no table needs a resync", [], observed)
            return decided("already", nothing)
        relation_id, table = pick
        extra = dict(remaining=observed["remaining"] - 1)
        target, overrides = owner.backfill_flight, backfill_config(table)
        what = f"resync {table} (longest held): {RESYNC_COST['needs_resync']}"
        steps = ["resync-all again"] if extra["remaining"] else []
    else:
        observed = {}
        target, overrides = owner.compact_flight, COMPACT_CONFIG
        what, steps = "compact now with the owner's settings", []
    shown = _shown(observed)
    if inputs.dry_run:
        run.step("decide", "ok", branch="would")
        return preview(ActionResult("would", table, what, steps, shown), observed, **extra)
    run.step("decide", "ok", branch="trigger")
    try:
        explain.step(
            run,
            "claim",
            lambda: claim(
                apply,
                inputs,
                observed,
                run_id=run_id,
                actor=actor,
                relation_id=relation_id,
                audit_detail=f"{action} by {actor}",
            ),
            effect=previews,
        )
    except Refused as exc:
        run.step("outcome", "ok", branch="refused")
        run.step("moved", "ok")
        return _out(inputs, ActionResult("refused", table, str(exc), [], shown), run_id, **extra)
    run.step("outcome", "ok", branch="trigger")
    kind = "backfill" if target == owner.backfill_flight else "compact"
    run.step("trigger", "running", flight=kind)
    try:
        started, number = trigger(target, overrides)
    except Exception as exc:
        run.step("trigger", "failed", flight=kind, error=type(exc).__name__)
        detail = f"trigger failed ({type(exc).__name__}); the preview is spent; run a new dry run"
        return _out(inputs, ActionResult("failed", table, detail, [], shown), run_id, **extra)
    run.step("trigger", "ok", flight=kind, run=number if number is not None else 0)
    run.step("done", "ok")
    triggered = dict(flight=kind, run_id=started, run_number=number)
    result = ActionResult("done", table, f"started: {what}", steps, shown)
    return _out(inputs, result, run_id, triggered=triggered, **extra)


def main(owner, environ=None, spec=None, *, fault=None, command="table-action"):
    """The table-action Flight, or another action Flight on the mirror (quarantine,
    with its own spec). Returns the exit code: 0 done, already or would; 2 refused;
    3 busy; 1 failed."""
    environ = os.environ if environ is None else environ
    run = Run()  # Flight Explainer lines; see flights/charts/<command>.mmd
    try:
        inputs = parse(environ, spec or TABLE_ACTION)
    except InputError as exc:
        run.step("validate", "ok", branch="refused")
        run.step("invalid", "ok")
        print(json.dumps(base.refusal(environ, spec or TABLE_ACTION, str(exc))))
        return EXIT["refused"]
    run.step("validate", "ok", branch="ok")
    run_id, config = run_id_from(environ), owner.config()
    con = duckdb.connect(f"md:{config.mirror_database}?attach_mode=single")
    try:

        def attach():
            con.execute(f"ATTACH IF NOT EXISTS 'md:{config.database}' AS {quoted(config.database)}")
            con.execute("SET TimeZone='UTC'")
            apply = Apply(con, config)  # no I/O
            # The stored format is checked read-only first; anything but a
            # format-2 store refuses here, before initialize() writes any mirror table.
            try:
                apply.check_format()
            except SourceMismatch as exc:
                return apply, f"the source is not a format-2 store ({exc}); run preflight first"
            apply.initialize()  # the mirror's _cdc tables, previews included
            record_release(apply.execute, command, environ)  # D5: inside a Flight only
            return apply, None

        apply, refusal = explain.step(run, "attach", attach)
        # A run's current_user is the Flight's owner; who started it comes from its row.
        started_by = whois(con, environ, run)
        actor = actor_of(con, started_by)
        try:
            if refusal is None:
                check_lifecycle(apply.retention)  # one statement per click
        except SourceDeleted as exc:
            refusal = str(exc)
        if refusal is not None:
            # Through the normal path, so the audit copy records the reason.
            run.step("decide", "ok", branch="refused")
            run.step("refused", "ok")
            out = base.envelope(
                dict(
                    status="refused",
                    action=inputs.action,
                    table=inputs.table,
                    detail=refusal,
                    run_id=run_id,
                )
            )
        else:
            out = None
        try:
            out = out or perform(
                apply,
                inputs,
                run_id=run_id,
                actor=actor,
                owner=owner,
                trigger=lambda target, overrides: run_flight(con, target, overrides),
                run=run,
                fault=fault,
            )
        except Exception as exc:
            explain.failed(run, exc)
            raise
        finish(con, owner, inputs, out, run, started_by=started_by)
    finally:
        con.close()
    print(json.dumps(out, default=str))
    return EXIT.get(out["status"], 1)


def owner_json(owner):
    return json.dumps(asdict(owner), sort_keys=True)
