"""The table-membership Flight: add or remove a table, from the Dive.

It's its own Flight because it holds the source's PostgreSQL secret, which add uses to
check the table against the publication (read only). The table-action Flight never
holds it. Everything else follows action_flight's framework:

- the owner's settings (source, slot, publication, heartbeat, the secret's variable
  name, databases, the backfill Flight) are baked into the stub as OWNER;
- the Flight declares only CDC_ACTION, CDC_TABLE, CDC_FORCE, CDC_CONFIRM, CDC_MODE,
  CDC_IGNORE_DELETES, CDC_DRY_RUN, CDC_PREVIEW_ID and CDC_REQUEST_ID, checked before any connection
  (CDC_MODE=append, with CDC_IGNORE_DELETES=1 optional: insert-only capture of a keyless
  table, which starts no backfill);
- a dry run reads (PostgreSQL included, for add) and records a preview in
  cdc_internal's `<source>.previews`, with no lease;
- a real run waits up to WAIT seconds for the ingest lease, checks its preview
  read-only before any write (a relabel's mirror rename, remove's apply fence), then
  spends it inside membership's guarded registry transaction;
- after an add that needs a backfill, it starts the backfill Flight with built config.

A forced remove (a table that still exists) needs CDC_CONFIRM = `REMOVE <table>`: it
stops capturing a live table, which only a re-add and a backfill undo.
"""

import dataclasses
import json
import logging
import os
import re
import uuid
from dataclasses import asdict, dataclass, fields

import duckdb

from cdc_flight import explain, membership, ops, pg_ca
from cdc_flight.action_base import (
    EXIT,
    InputError,
    Refused,
    Spec,
    actor_of,
    backfill_config,
    checker,
    decision,
    ensure_previews,
    envelope,
    finish,
    merged,
    parse,
    record_preview,
    run_id_from,
    spender,
    whois,
)
from cdc_flight.action_base import refusal as refusal_line
from cdc_flight.apply import Apply
from cdc_flight.chain import FLIGHT_CONFIG, run_flight
from cdc_flight.config import ApplyConfig, IngestConfig, heartbeat_writer, identifier, quoted
from cdc_flight.config import table_name as split_table
from cdc_flight.flightlog import Run
from cdc_flight.ingest_common import SourceMismatch
from cdc_flight.releases import APPEND_SNAPSHOT, record_release, release_level
from cdc_flight.retention import SourceDeleted
from cdc_flight.runtime import open_log

logger = logging.getLogger(__name__)

KEYS = FLIGHT_CONFIG["table-membership"]
WAIT = 120  # seconds to wait for the ingest lease; below max_runtime_sec
PREVIEWS = "{s}previews"
# What a preview promises (hashed): add's branch depends on both registry rows it
# reads, by OID and by name, and on what PostgreSQL says now.
# A change activates only at a complete-unit boundary, so the preview also promises the
# source's unit position it was decided at; an open unit completing or opening in
# between makes it stale.
POSITION = ("format_version", "source_epoch", "checkpoint", "partial", "open_unit_anchor")
DECISION_FIELDS = {
    "add": ("oid", "published", "by_oid", "holder", *POSITION, "config_revision"),
    "remove": ("row", "active_generation", *POSITION, "config_revision"),
}
FIELDS = DECISION_FIELDS


def promised(observed):
    """The observation a preview promises. The core reports the source's unit position
    nested (observed["unit_position"]); its fields are promised beside the others. An
    observation without one lacks promised fields, so decision() refuses it."""
    position = observed.get("unit_position") or {}
    return merged(observed, position) if position else observed


# A membership result that registered a table whose mirror a backfill must build.
NEEDS_BACKFILL = ("added", "adopted", "reactivated")
DONE = (*NEEDS_BACKFILL, "relabelled", "removed")


def confirmation(table):
    return f"REMOVE {table}"


def _force(text):
    if text not in ("0", "1"):
        raise InputError("CDC_FORCE must be 0 or 1")
    return "force", text == "1"


def _confirm(text):
    if len(text) > 200:
        raise InputError("CDC_CONFIRM is too long")
    return "confirm", text


def _mode(text):
    if text not in membership.MODES:
        raise InputError(f"CDC_MODE must be one of {', '.join(membership.MODES)}")
    return "mode", text


def _ignore_deletes(text):
    if text not in ("0", "1"):
        raise InputError("CDC_IGNORE_DELETES must be 0 or 1")
    return "ignore_deletes", text == "1"


def _check(action, table, values):
    """A forced remove needs the typed confirmation, exactly; nothing else takes one.
    CDC_IGNORE_DELETES=1 needs CDC_MODE=append. Defaults (keyed, 0) hash as unset."""
    if values.get("ignore_deletes") and values.get("mode") != "append":
        raise InputError("CDC_IGNORE_DELETES=1 is only for CDC_MODE=append")
    if values.get("mode") == "keyed":
        values.pop("mode")
    if values.get("ignore_deletes") is False:
        values.pop("ignore_deletes")
    force, confirm = values.get("force", False), values.get("confirm")
    if not force:
        if confirm is not None:
            raise InputError("CDC_CONFIRM is only for a forced remove (CDC_FORCE=1)")
        values.pop("force", None)  # 0 and blank hash alike
        return
    if confirm != confirmation(table):
        raise InputError(f'a forced remove needs CDC_CONFIRM exactly "{confirmation(table)}"')


MEMBERSHIP = Spec(
    KEYS,
    {"add": ("CDC_TABLE",), "remove": ("CDC_TABLE",)},
    {
        "CDC_FORCE": _force,
        "CDC_CONFIRM": _confirm,
        "CDC_MODE": _mode,
        "CDC_IGNORE_DELETES": _ignore_deletes,
    },
    optional={"add": ("CDC_MODE", "CDC_IGNORE_DELETES"), "remove": ("CDC_FORCE", "CDC_CONFIRM")},
    check=_check,
)


@dataclass(frozen=True)
class Owner:
    """The owner's settings, baked into the stub (OWNER) by build_flight.py."""

    source_id: str
    backfill_flight: str
    slot: str = ""
    publication: str = ""
    heartbeat: str = "public.md_cdc_heartbeat"
    heartbeat_writer: str = "self"  # replica capture: the source's mode (trusted)
    dsn_env: str = "CDC_PG_DSN"
    database: str = "cdc_internal"
    mirror_database: str = ""
    ops_database: str = ops.DATABASE

    def __post_init__(self):
        heartbeat_writer(self.heartbeat_writer)
        source = identifier(self.source_id)
        defaults = dict(
            slot=f"md_cdc_{source}",
            publication=f"md_cdc_{source}_pub",
            mirror_database=f"{source}_mirror",
        )
        for name, default in defaults.items():
            object.__setattr__(self, name, identifier(getattr(self, name) or default))
        for name in ("database", "ops_database"):
            identifier(getattr(self, name))
        split_table(self.heartbeat)
        object.__setattr__(self, "backfill_flight", str(uuid.UUID(self.backfill_flight)))
        # The same rule as cli.required: only ever a *_DSN variable, never a runtime one.
        if not re.fullmatch(
            r"[A-Za-z][A-Za-z0-9_]{0,126}_DSN", self.dsn_env
        ) or self.dsn_env.upper().startswith("MOTHERDUCK_"):
            raise ValueError("dsn_env must name a variable ending in _DSN")
        self.apply_config()  # the mirror and change-log databases must differ
        if self.ops_database in (self.database, self.mirror_database):
            raise ValueError("the operations database must differ from the stores")

    @classmethod
    def from_dict(cls, values):
        unknown = set(values) - {f.name for f in fields(cls)}
        if unknown:
            raise ValueError(f"unknown owner settings: {sorted(unknown)}")
        return cls(**values)

    def ingest_config(self, dsn=""):
        """Built from values: never IngestConfig.from_env."""
        return IngestConfig(
            self.source_id,
            self.slot,
            self.publication,
            (),
            heartbeat=self.heartbeat,
            database=self.database,
            heartbeat_writer=self.heartbeat_writer,
            dsn=dsn,
        )

    def apply_config(self):
        return ApplyConfig(
            self.source_id, (), database=self.database, mirror_database=self.mirror_database
        )


def _flat(hook):
    return lambda observed: hook(promised(observed))


def _out(inputs, result, run_id, **extra):
    # drain_required (format 2): nothing changed; an open unit must drain under its
    # recorded configuration first, so retry later, as for busy. `result` keeps it.
    status = "done" if result.status in DONE else result.status
    status = "busy" if status == "drain_required" else status
    return dict(
        status=status,
        action=inputs.action,
        table=result.table,
        result=result.status,
        detail=result.detail,
        next_steps=result.next_steps,
        observed=result.observed,
        run_id=run_id,
        **extra,
    )


def _snapshot_ready(log):
    """Whether the backfill Flight takes append snapshots."""
    try:
        level = release_level(log.execute, "backfill")
    except Exception:
        return False
    return level is not None and level >= APPEND_SNAPSHOT


def perform(log, apply, pg, inputs, *, run_id, actor, owner, trigger, run=None, wait=WAIT):
    """One add or remove; returns the result line (a dict). `pg()` opens the
    PostgreSQL connection, for add only."""
    run = run or Run()
    table = inputs.table
    previews = f"table:{owner.database}.{owner.source_id}.previews"
    try:  # dry runs too: no preview for a deleting source
        log.refuse_lifecycle()
    except SourceDeleted as exc:
        run.step("decide", "ok", branch="refused")
        run.step("refused", "ok")
        return envelope(
            dict(
                status="refused", action=inputs.action, table=table, detail=str(exc), run_id=run_id
            )
        )
    hooks = {}
    if not inputs.dry_run:
        hooks = dict(
            check=_flat(checker(log, inputs, PREVIEWS, FIELDS)),
            consume=_flat(spender(log, inputs, run_id, actor, PREVIEWS, FIELDS)),
            wait=wait,
        )
    if inputs.action == "add":
        result = membership.add_table(
            log,
            apply,
            pg(),
            owner.publication,
            table,
            # Only when set (the defaults are unset): a keyed add calls exactly as before.
            **{k: inputs.values[k] for k in ("mode", "ignore_deletes") if k in inputs.values},
            dry_run=inputs.dry_run,
            **hooks,
        )
    else:
        result = membership.remove_table(
            log,
            apply,
            table,
            force=inputs.values.get("force", False),
            dry_run=inputs.dry_run,
            **hooks,
        )
    if inputs.dry_run:
        branch = "drain" if result.status == "drain_required" else result.status
        run.step("decide", "ok", branch=branch)
        if result.status != "would":  # drain_required included: nothing to confirm yet
            run.step(branch, "ok")
            return _out(inputs, result, run_id)
        try:
            decided = decision(inputs.action, promised(result.observed), FIELDS, log)
        except Refused as exc:  # a format-2 observation that can't be promised: no write
            refused = membership.MembershipResult("refused", table, str(exc), [], result.observed)
            return _out(inputs, refused, run_id)
        explain.step(
            run,
            "preview",
            lambda: record_preview(log, run_id, inputs, decided, PREVIEWS),
            effect=previews,
        )
        # A forced remove's literal (checked at parse); none for anything else.
        confirm = inputs.values.get("confirm")
        return _out(inputs, result, run_id, preview_id=run_id, confirm=confirm)
    run.step("decide", "ok", branch="act")
    run.step("act", "ok", effect=f"table:{owner.database}.{owner.source_id}.tables")
    outcome = "done" if result.status in DONE else result.status
    # An append add's snapshot needs a backfill release stamped APPEND_SNAPSHOT.
    append = inputs.values.get("mode") == "append"
    if append and result.status in NEEDS_BACKFILL and not _snapshot_ready(log):
        detail = f"{result.detail}; no exact snapshot: the backfill Flight predates them"
        result = dataclasses.replace(result, detail=detail)
        append = "no_snapshot"
    if result.status not in NEEDS_BACKFILL or append == "no_snapshot":
        if result.status == "drain_required":
            run.step("outcome", "ok", branch="drain")
            run.step("drain", "ok")
            return _out(inputs, result, run_id)
        run.step(
            "outcome", "ok", branch=outcome if outcome in ("done", "busy", "already") else "refused"
        )
        run.step({"done": "done", "busy": "busy", "already": "already"}.get(outcome, "moved"), "ok")
        return _out(inputs, result, run_id)
    run.step("outcome", "ok", branch="trigger")
    run.step("trigger", "running", flight="backfill")
    try:
        started, number = trigger(owner.backfill_flight, backfill_config(table))
    except Exception as exc:
        # The table *is* added: the way on is its backfill, not another add.
        run.step("trigger", "failed", flight="backfill", error=type(exc).__name__)
        out = _out(inputs, result, run_id)
        out.update(
            status="failed",
            detail=f"{table} was {result.status}, but starting its backfill failed "
            f"({type(exc).__name__})",
            next_steps=[f"resync {table} (table-action)"],
        )
        return out
    run.step("trigger", "ok", flight="backfill", run=number if number is not None else 0)
    run.step("done", "ok")
    triggered = dict(flight="backfill", run_id=started, run_number=number)
    out = _out(inputs, result, run_id, triggered=triggered)
    out["next_steps"] = [
        s
        for s in out["next_steps"]
        if not s.startswith("run backfill") and not (append and s.startswith(f"start {table} from"))
    ]
    return out


def main(owner, environ=None):
    """The table-membership Flight. Returns the exit code: 0 done, already or would;
    2 refused; 3 busy; 1 failed."""
    environ = os.environ if environ is None else environ
    run = Run()  # Flight Explainer lines; see flights/charts/table-membership.mmd
    try:
        inputs = parse(environ, MEMBERSHIP)
    except InputError as exc:
        run.step("validate", "ok", branch="refused")
        run.step("invalid", "ok")
        print(json.dumps(refusal_line(environ, MEMBERSHIP, str(exc))))
        return EXIT["refused"]
    run.step("validate", "ok", branch="ok")
    run_id = run_id_from(environ)
    # The secret is read only inside pg(), which only add calls: remove never holds it.
    config, pg_con = owner.ingest_config(), None

    def pg():
        nonlocal pg_con
        from cdc_flight.preflight import connect

        dsn = environ.get(owner.dsn_env, "")
        if not dsn:
            raise SystemExit(f"the Flight's PostgreSQL secret ({owner.dsn_env}) is not set")
        pg_con = connect(owner.ingest_config(dsn))
        return pg_con

    con = duckdb.connect(f"md:{owner.mirror_database}?attach_mode=single")
    try:

        def attach():
            con.execute(f"ATTACH IF NOT EXISTS 'md:{owner.database}' AS {quoted(owner.database)}")
            con.execute("SET TimeZone='UTC'")
            # The stored format-2 store only, opened read-only with its recorded binding
            # and unit config; anything else (empty, legacy, unknown) refuses here, before
            # any previews table, lease or PostgreSQL I/O. The store is never migrated.
            try:
                log = open_log(con, config)
            except SourceMismatch as exc:
                return (
                    None,
                    None,
                    f"the source is not a format-2 store ({exc}); run preflight first",
                )
            if not ensure_previews(log):
                return log, None, "the source is not initialized; run preflight first"
            apply = Apply(con, owner.apply_config())
            apply.initialize()  # remove's fence and relabel's rename need the mirror's _cdc
            record_release(apply.execute, "table-membership", environ)  # D5: Flight only
            return log, apply, None

        log, apply, refusal = explain.step(run, "attach", attach)
        started_by = None
        if refusal:
            run.step("decide", "ok", branch="refused")
            run.step("refused", "ok")
            out = envelope(
                dict(
                    status="refused",
                    action=inputs.action,
                    table=inputs.table,
                    detail=refusal,
                    run_id=run_id,
                )
            )
        else:
            started_by = whois(con, environ, run)
            actor = actor_of(con, started_by)
            try:
                out = perform(
                    log,
                    apply,
                    pg,
                    inputs,
                    run_id=run_id,
                    actor=actor,
                    owner=owner,
                    trigger=lambda target, overrides: run_flight(con, target, overrides),
                    run=run,
                )
            except Exception as exc:
                explain.failed(run, exc)
                raise
        finish(con, owner, inputs, out, run, started_by=started_by, tls=pg_ca.fields())
    finally:
        if pg_con is not None:
            pg_con.close()
        con.close()
    print(json.dumps(out, default=str))
    return EXIT.get(out["status"], 1)


def owner_json(owner):
    return json.dumps(asdict(owner), sort_keys=True)
