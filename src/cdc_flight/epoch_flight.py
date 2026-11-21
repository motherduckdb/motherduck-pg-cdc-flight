"""The epoch Flight: recover a lost slot, or skip to an LSN.

The destructive tier, in its own Flight, granted to the org: every guard here stops
mis-clicks and stale screens, not an org member who means it. It wraps epoch.begin_epoch:

- the owner's settings (source, slot, publication, heartbeat, the secret's *name*,
  databases) are baked in as OWNER; the Flight declares only CDC_ACTION
  (recover-slot / skip-to-lsn), CDC_EXPECTED_EPOCH, CDC_TO_LSN (skip), CDC_CONFIRM and
  the framework's CDC_DRY_RUN / CDC_PREVIEW_ID;
- the confirmation is exact: `RECOVER <source_id> EPOCH <n+1>` or
  `SKIP <source_id> TO <X/Y>` (PostgreSQL's uppercase form);
- a dry run is read only (no lease, no PostgreSQL change): it reports what would happen
  and records the preview in cdc_internal's `<source>.previews`;
- the real run waits for the ingest lease, re-reads read-only, spends the preview in its
  own guarded transaction (claim first: the PostgreSQL step that follows changes slots),
  then creates or advances the slot and records the epoch. A claimed preview is spent
  even if the transition then stops; a re-click needs a new dry run.

Statuses: done (begun), already, would, refused, waiting (the lease or a slot reader, or
a bounded PostgreSQL wait ran out; nothing changed; exit 0, Dive shows it distinctly).
Afterwards every mirror needs a resync: the next step is resync-all on table-action,
once per table. Nothing here starts another Flight (V6 doesn't apply).
"""

import json
import logging
import os
import re
from dataclasses import asdict, dataclass, fields

import duckdb

from cdc_flight import epoch as epoch_module
from cdc_flight import explain, ops, pg_ca
from cdc_flight.action_base import (
    EXIT,
    InputError,
    Spec,
    actor_of,
    decision,
    ensure_previews,
    finish,
    parse,
    record_preview,
    run_id_from,
    spender,
    whois,
)
from cdc_flight.action_types import Refused, parse_lsn
from cdc_flight.chain import FLIGHT_CONFIG
from cdc_flight.config import (
    IngestConfig,
    LeaseBusy,
    heartbeat_writer,
    identifier,
    lsn_text,
)
from cdc_flight.config import table_name as split_table
from cdc_flight.epoch import EpochError, begin_epoch, preview_epoch
from cdc_flight.flightlog import Run
from cdc_flight.ingest_common import SourceMismatch
from cdc_flight.retention import SourceDeleted
from cdc_flight.runtime import open_log

logger = logging.getLogger(__name__)

KEYS = FLIGHT_CONFIG["epoch"]
WAIT = 120  # seconds to wait for the ingest lease (shared `leased`)
# The Flight's max_runtime_sec, less a margin for the metadata, audit and snapshot: a
# skip's steps stop before it, so the lease is released, not left to expire.
RUN_SECONDS, MARGIN_SECONDS = 600, 60
PREVIEWS = "{s}previews"
REASON = {"recover-slot": "slot_lost", "skip-to-lsn": "skip"}
_COMMON = ("epoch", "reason", "slot", "checkpoint", "slot_present", "slot_active", "slot_lost")
# What a preview promises: epoch.observe's fields for that transition.
# A new epoch abandons the WHOLE old open unit, so the preview also promises the unit
# position it was decided at, the durable prefix included:
# a member or chunk added between the dry run and the real run changes what is abandoned.
# The prefix bounds themselves are the core's (preview_epoch/begin_epoch); this wrapper
# never checks or widens a target.
_UNIT = (
    "format_version",
    "source_epoch",
    "partial",
    "open_unit_anchor",
    "open_chunks",
    "active_commit_lsn",
    "last_member_end_lsn",
    "durable_frame_end",
)
DECISION_FIELDS = {
    "recover-slot": (*_COMMON, "new_slot", "new_slot_present", *_UNIT),
    "skip-to-lsn": (*_COMMON, "to_lsn", "slot_confirmed", "abandoned_bytes", *_UNIT),
}
FIELDS = DECISION_FIELDS
# The core's abandonment record (source_epochs details), reported as returned.
ABANDONED_KEYS = (
    "abandoned_unit_start_lsn",
    "abandoned_frame_end",
    "abandoned_chunk_count",
    "abandoned_first_commit_lsn",
    "abandoned_last_commit_lsn",
    "abandoned_active_commit_lsn",
    "abandoned_into_epoch",
    "abandoned_reason",
)


def _epoch(text):
    if not text.isdigit() or not 1 <= int(text) < 2**31:
        raise InputError("CDC_EXPECTED_EPOCH must be a positive integer")
    return "expected_epoch", int(text)


def _to_lsn(text):
    try:
        return "to_lsn", parse_lsn(text)
    except ValueError:
        raise InputError('CDC_TO_LSN must be "X/Y" in hex') from None


def _confirm(text):
    if len(text) > 200:
        raise InputError("CDC_CONFIRM is too long")
    return "confirm", text


EPOCH = Spec(
    KEYS,
    {
        "recover-slot": ("CDC_EXPECTED_EPOCH", "CDC_CONFIRM"),
        "skip-to-lsn": ("CDC_EXPECTED_EPOCH", "CDC_TO_LSN", "CDC_CONFIRM"),
    },
    {"CDC_EXPECTED_EPOCH": _epoch, "CDC_TO_LSN": _to_lsn, "CDC_CONFIRM": _confirm},
)


def confirmation(source_id, action, epoch, to_lsn=None):
    if action == "recover-slot":
        return f"RECOVER {source_id} EPOCH {epoch + 1}"
    return f"SKIP {source_id} TO {lsn_text(to_lsn)}"


@dataclass(frozen=True)
class Owner:
    """The owner's settings, baked into the stub (OWNER) by build_flight.py."""

    source_id: str
    slot: str = ""
    publication: str = ""
    heartbeat: str = "public.md_cdc_heartbeat"
    heartbeat_writer: str = "self"  # replica capture: the source's mode (trusted)
    dsn_env: str = "CDC_PG_DSN"
    database: str = "cdc_internal"
    ops_database: str = ops.DATABASE

    def __post_init__(self):
        heartbeat_writer(self.heartbeat_writer)
        source = identifier(self.source_id)
        for name, default in (
            ("slot", f"md_cdc_{source}"),
            ("publication", f"md_cdc_{source}_pub"),
        ):
            object.__setattr__(self, name, identifier(getattr(self, name) or default))
        for name in ("database", "ops_database"):
            identifier(getattr(self, name))
        split_table(self.heartbeat)
        if not re.fullmatch(
            r"[A-Za-z][A-Za-z0-9_]{0,126}_DSN", self.dsn_env
        ) or self.dsn_env.upper().startswith("MOTHERDUCK_"):
            raise ValueError("dsn_env must name a variable ending in _DSN")
        if self.ops_database == self.database:
            raise ValueError("the operations database must differ from the change log")

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


def _out(inputs, status, detail, *, run_id, observed=None, next_steps=(), **extra):
    return dict(
        status=status,
        action=inputs.action,
        detail=detail,
        next_steps=list(next_steps),
        observed=observed or {},
        run_id=run_id,
        **extra,
    )


def _open_unit(observed):
    """The open unit a new epoch abandons whole, in words ('' when none)."""
    if not observed.get("partial"):
        return ""
    return (
        f"; the open unit from {lsn_text(observed['open_unit_anchor'])} "
        f"({observed['open_chunks']} durable chunk(s), frames below "
        f"{observed['durable_frame_end']}) is abandoned whole"
    )


def _bytes(observed):
    """Format 2: the abandoned range in bytes, both as the core reports them:
    the bound promised by the typed target, and what exists now. The latter is
    informational only and never part of the CAS (the agreed bounded anchor growth)."""
    if "abandoned_bytes_upper_bound" not in observed:
        return ""
    return (
        f"; abandons at most {observed['abandoned_bytes_upper_bound']} bytes of WAL (to the "
        f"typed target); {observed['abandoned_bytes_current']} bytes exist now"
    )


def _would(action, observed):
    return _would_v1(action, observed) + _bytes(observed) + _open_unit(observed)


def _would_v1(action, observed):
    if action == "recover-slot":
        return (
            f"create {observed['new_slot']} and begin epoch {observed['epoch'] + 1}; WAL after "
            f"{lsn_text(observed['checkpoint'])} that the old slot lost is abandoned; every "
            "mirror then needs a resync"
        )
    if observed.get("clamped"):
        # Format 2: the target is beyond the source's current WAL, so the skip stops there
        # (the requested byte count would overstate it); the anchor is the WAL at the click.
        return (
            f"advance {observed['slot']} to the source's current WAL and begin epoch "
            f"{observed['epoch'] + 1}: everything from {lsn_text(observed['checkpoint'])} up to "
            f"the source's WAL at the click (at most the requested "
            f"{lsn_text(observed['to_lsn'])}) is never ingested; WAL written after the "
            "advance is ingested by the new epoch; every mirror then needs a resync"
        )
    return (
        f"advance {observed['slot']} to {lsn_text(observed['to_lsn'])} and begin epoch "
        f"{observed['epoch'] + 1}: [{lsn_text(observed['checkpoint'])}, "
        f"{lsn_text(observed['to_lsn'])}) ({observed['abandoned_bytes']} bytes of WAL) is never "
        "ingested; every mirror then needs a resync"
    )


RESYNC_ALL = "resync-all on the table-action Flight, once per table, until none remain"


def perform(
    log,
    config,
    inputs,
    *,
    run_id,
    actor,
    owner,
    run=None,
    started_by=None,
    wait=WAIT,
    deadline=None,
):
    """One recover or skip; returns the result line (a dict). `config` holds the DSN."""
    run = run or Run()
    action, values = inputs.action, inputs.values
    reason, to_lsn = REASON[action], values.get("to_lsn")

    def refused(detail, observed=None, steps=()):
        run.step("decide", "ok", branch="refused")
        run.step("refused", "ok")
        return _out(inputs, "refused", detail, run_id=run_id, observed=observed, next_steps=steps)

    try:  # dry runs too: no preview for a deleting source
        log.refuse_lifecycle()
    except SourceDeleted as exc:
        return refused(str(exc))
    epoch, _, _ = log.position()

    repeat = (
        epoch > 1
        and values["expected_epoch"] == epoch - 1
        and values["confirm"] == confirmation(owner.source_id, action, epoch - 1, to_lsn)
    )
    if repeat:
        # Repeating a committed transition reports `already`; the real run
        # also finishes its cleanup (the old slot's drop). It changes nothing
        # else, so it spends no preview; begin_epoch refuses a different transition.
        if inputs.dry_run:
            run.step("decide", "ok", branch="already")
            run.step("already", "ok")
            return _out(
                inputs,
                "already",
                f"epoch {epoch} already began; a real run drops the old slot if it remains",
                run_id=run_id,
                next_steps=[RESYNC_ALL],
            )
        run.step("decide", "ok", branch="act")
        run.step("act", "running", effect=f"table:{owner.database}.{owner.source_id}.source_epochs")
        try:
            result = begin_epoch(
                config, log, epoch, reason, to_lsn=to_lsn, wait=wait, started_by=started_by
            )
        except (EpochError, SourceMismatch) as exc:
            run.step("act", "ok")
            run.step("outcome", "ok", branch="refused")
            run.step("moved", "ok")
            return _out(inputs, "refused", str(exc), run_id=run_id)
        run.step("act", "ok")
        return _report(inputs, result, action, run, run_id, log)
    if values["expected_epoch"] != epoch:
        return refused(f"state changed: expected epoch {values['expected_epoch']}, found {epoch}")
    literal = confirmation(owner.source_id, action, epoch, to_lsn)
    if values["confirm"] != literal:
        return refused(f'type the confirmation exactly: "{literal}"')
    if inputs.dry_run:
        try:
            observed = preview_epoch(config, log, reason, to_lsn=to_lsn)
        except (EpochError, SourceMismatch) as exc:
            return refused(str(exc))
        except LeaseBusy as exc:
            run.step("decide", "ok", branch="waiting")
            run.step("waiting", "ok")
            return _out(inputs, "waiting", str(exc), run_id=run_id)
        try:
            decided = decision(action, observed, FIELDS, log)
        except Refused as exc:  # a format-2 observation that can't be promised: no write
            return refused(str(exc), observed)
        run.step("decide", "ok", branch="would")
        explain.step(
            run,
            "preview",
            lambda: record_preview(log, run_id, inputs, decided, PREVIEWS),
            effect=f"table:{owner.database}.{owner.source_id}.previews",
        )
        return _out(
            inputs,
            "would",
            _would(action, observed),
            run_id=run_id,
            observed=observed,
            preview_id=run_id,
            next_steps=[RESYNC_ALL],
        )

    spend = spender(log, inputs, run_id, actor, PREVIEWS, FIELDS)

    def claim(observed):
        # Claim first: its own guarded transaction, before the PostgreSQL
        # step that changes slots.
        with log.transaction():
            log.guard()
            spend(observed)

    run.step("decide", "ok", branch="act")
    run.step("act", "running", effect=f"table:{owner.database}.{owner.source_id}.source_epochs")
    try:
        result = begin_epoch(
            config,
            log,
            epoch + 1,
            reason,
            to_lsn=to_lsn,
            claim=claim,
            wait=wait,
            started_by=started_by,
            deadline=deadline,
        )
    except Refused as exc:
        run.step("act", "ok")
        run.step("outcome", "ok", branch="refused")
        run.step("moved", "ok")
        return _out(inputs, "refused", str(exc), run_id=run_id)
    except (EpochError, SourceMismatch) as exc:
        run.step("act", "ok")
        run.step("outcome", "ok", branch="refused")
        run.step("moved", "ok")
        return _out(inputs, "refused", str(exc), run_id=run_id)
    run.step("act", "ok")
    return _report(inputs, result, action, run, run_id, log)


def _abandonment(result, log):
    """The core's record of the whole open unit an epoch abandoned, as returned: on the
    result if it carries one, else the recorded transition's unit_abandonment."""
    details = getattr(result, "details", None)
    if details is None and log is not None:
        record = log.epoch_record(result.epoch) or {}
        details = record.get("unit_abandonment")
        if isinstance(details, str):
            details = json.loads(details)
    return details or {}


def _report(inputs, result, action, run, run_id, log=None):
    """An EpochResult as the Flight's result line."""
    extra = dict(epoch=result.epoch, slot=result.slot, start_lsn=lsn_text(result.start_lsn))
    requested = getattr(result, "requested_to_lsn", None) or inputs.values.get("to_lsn")
    details = _abandonment(result, log)
    if getattr(result, "requested_to_lsn", None) is not None and result.abandoned:
        # Format 2: what was actually abandoned, from the core's recorded range.
        extra["abandoned_bytes_effective"] = result.abandoned[1] - result.abandoned[0]
    if requested is not None and result.start_lsn not in (None, requested):
        # Format 2: the new epoch is anchored where PostgreSQL actually advanced to. A
        # target beyond the source's current WAL is clamped to it (it skips all WAL that
        # exists, never more); later changes are ingested by the new epoch.
        extra["requested_to_lsn"] = lsn_text(requested)
        clamped = getattr(result, "clamped", None)
        if clamped is None:
            clamped = details.get("clamped", result.start_lsn < requested)
        extra["clamped"] = bool(clamped)
    extra.update({k: details[k] for k in ABANDONED_KEYS if k in details})
    if result.abandoned:
        extra["abandoned"] = [lsn_text(x) for x in result.abandoned]
    steps = []
    if result.leftover:
        extra["leftover_slot"] = result.leftover
        steps.append(
            f"the previous slot {result.leftover} still exists and retains WAL: run "
            f"{action} again (it reports already and drops it once no reader is attached)"
        )
    if result.pending:
        extra["pending_slot"] = result.pending
        steps.append(f"slot {result.pending} was kept: run {action} again to finish")
    if result.status in ("begun", "already"):
        status = "done" if result.status == "begun" else "already"
        run.step("outcome", "ok", branch=status)
        run.step(status, "ok")
        detail = (
            f"epoch {result.epoch} began on slot {result.slot} at {lsn_text(result.start_lsn)}"
            if status == "done"
            else f"epoch {result.epoch} already began"
        )
        if extra.get("clamped"):
            detail += (
                f" (requested {extra['requested_to_lsn']}, beyond the source's WAL: clamped; "
                "later changes are ingested)"
            )
        return _out(inputs, status, detail, run_id=run_id, next_steps=[RESYNC_ALL] + steps, **extra)
    # busy (the lease after the wait, or a slot reader) and waiting: nothing changed.
    run.step("outcome", "ok", branch="waiting")
    run.step("waiting", "ok")
    detail = result.detail or "ingest holds the lease; retry"
    return _out(inputs, "waiting", detail, run_id=run_id, next_steps=steps, **extra)


def main(owner, environ=None):
    """The epoch Flight. Exit codes: 0 done, already, would or waiting; 2 refused;
    1 failed."""
    environ = os.environ if environ is None else environ
    deadline = epoch_module._clock() + RUN_SECONDS - MARGIN_SECONDS
    run = Run()  # Flight Explainer lines; see flights/charts/epoch.mmd
    try:
        inputs = parse(environ, EPOCH)
    except InputError as exc:
        run.step("validate", "ok", branch="refused")
        run.step("invalid", "ok")
        print(json.dumps(dict(status="refused", detail=str(exc))))
        return EXIT["refused"]
    run.step("validate", "ok", branch="ok")
    dsn = environ.get(owner.dsn_env, "")
    if not dsn:
        raise SystemExit(f"the Flight's PostgreSQL secret ({owner.dsn_env}) is not set")
    run_id, config = run_id_from(environ), owner.ingest_config(dsn)
    con = duckdb.connect(f"md:{owner.database}?attach_mode=single")
    started_by = None
    try:

        def attach():
            con.execute("SET TimeZone='UTC'")
            # The stored format-2 store only, opened read-only with its recorded binding
            # and unit config; anything else (empty, legacy, unknown) refuses here, before
            # any previews table, lease or PostgreSQL I/O. The store is never migrated.
            try:
                log = open_log(con, config)
            except SourceMismatch as exc:
                return None, f"the source is not a format-2 store ({exc}); run preflight first"
            if not ensure_previews(log):
                return None, "the source is not initialized; run preflight first"
            return log, None

        log, refusal = explain.step(run, "attach", attach)
        if refusal:
            run.step("decide", "ok", branch="refused")
            run.step("refused", "ok")
            out = _out(inputs, "refused", refusal, run_id=run_id)
        else:
            started_by = whois(con, environ, run)
            actor = actor_of(con, started_by)
            try:
                out = perform(
                    log,
                    log.config,  # the recorded binding/unit config, plus this run's DSN
                    inputs,
                    run_id=run_id,
                    actor=actor,
                    owner=owner,
                    run=run,
                    started_by=started_by,
                    deadline=deadline,
                )
            except Exception as exc:
                explain.failed(run, exc)
                raise
        finish(con, owner, inputs, out, run, started_by=started_by, tls=pg_ca.fields())
    finally:
        con.close()
    print(json.dumps(out, default=str))
    return EXIT.get(out["status"], 1)


def owner_json(owner):
    return json.dumps(asdict(owner), sort_keys=True)
