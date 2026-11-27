"""The delete-source Flight: the destructive tier, run by its owner only.

It wraps delete_source.delete_source on the action framework: OWNER baked in (the
source, slot, publication, heartbeat, the secret's *name*, databases); the Flight
declares only CDC_ACTION (delete-source), CDC_EXPECTED_EPOCH, CDC_CONFIRM and the
framework's CDC_DRY_RUN / CDC_PREVIEW_ID. The confirmation is exactly
`DELETE <source_id>`, and EXPECTED_EPOCH must match, on every path.

- A dry run is read only: it lists what would be dropped and the DDL left for the DBA,
  and records the preview (only for a source not yet being deleted).
- The real run of an active source spends the preview in the transaction that marks it
  `deleting` (claim first: the drops that follow change PostgreSQL).
- A `deleting` source (a reader was attached, or a drop was in use) is continued by a
  real run with no preview: the claim is spent and the state is the authority. A
  `deleted` source is `already`.

The result (and so its cdc_ops audit copy, which the Dive reads) carries
`lifecycle`, what was dropped and the DBA's DDL.
"""

import json
import os
import re
from dataclasses import asdict, dataclass, fields

import duckdb

from cdc_flight import explain, ops, pg_ca
from cdc_flight.action_base import (
    EXIT,
    InputError,
    Spec,
    actor_of,
    decision,
    ensure_previews,
    finish,
    merged,
    parse,
    record_preview,
    run_id_from,
    spender,
    whois,
)
from cdc_flight.action_types import Refused
from cdc_flight.chain import FLIGHT_CONFIG
from cdc_flight.config import IngestConfig, heartbeat_writer, identifier
from cdc_flight.config import table_name as split_table
from cdc_flight.delete_source import delete_source, preview_delete, source_row
from cdc_flight.flightlog import Run
from cdc_flight.ingest_common import SourceMismatch
from cdc_flight.runtime import open_log

KEYS = FLIGHT_CONFIG["delete-source"]
WAIT = 120
PREVIEWS = "{s}previews"
# The preview also promises the source's unit position. The drop list stays PostgreSQL
# objects only; no log or MotherDuck table ever joins it.
DECISION_FIELDS = {
    "delete-source": (
        "epoch",
        "lifecycle",
        "slot",
        "slot_active",
        "drop",
        "dba",
        "format_version",
        "source_epoch",
        "checkpoint",
        "partial",
        "open_unit_anchor",
    )
}
FIELDS = DECISION_FIELDS


def with_position(log, observed):
    """The delete core observes PostgreSQL objects only; the preview also promises the
    source's unit position, read here (inside the core's guarded claim transaction on
    the real run)."""
    return merged(observed, log.unit_position())


def _epoch(text):
    if not text.isdigit() or not 1 <= int(text) < 2**31:
        raise InputError("CDC_EXPECTED_EPOCH must be a positive integer")
    return "expected_epoch", int(text)


def _confirm(text):
    if len(text) > 200:
        raise InputError("CDC_CONFIRM is too long")
    return "confirm", text


DELETE = Spec(
    KEYS,
    {"delete-source": ("CDC_EXPECTED_EPOCH", "CDC_CONFIRM")},
    {"CDC_EXPECTED_EPOCH": _epoch, "CDC_CONFIRM": _confirm},
)


def confirmation(source_id):
    return f"DELETE {source_id}"


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


def _out(inputs, status, detail, *, run_id, **extra):
    return dict(status=status, action=inputs.action, detail=detail, run_id=run_id, **extra)


def perform(log, config, inputs, *, run_id, actor, owner, run=None, started_by=None, wait=WAIT):
    run = run or Run()
    values = inputs.values
    epoch, _, lifecycle, *_ = source_row(log)

    def stop(status, detail, **extra):
        run.step("decide", "ok", branch=status)
        run.step(status, "ok")
        return _out(inputs, status, detail, run_id=run_id, lifecycle=lifecycle, **extra)

    if values["expected_epoch"] != epoch:
        return stop(
            "refused", f"state changed: expected epoch {values['expected_epoch']}, found {epoch}"
        )
    if values["confirm"] != confirmation(owner.source_id):
        return stop("refused", f'type the confirmation exactly: "{confirmation(owner.source_id)}"')
    if lifecycle == "deleted":
        return stop("already", f"source {owner.source_id} was deleted")
    if inputs.dry_run:
        try:
            observed = with_position(log, preview_delete(config, log))
        except Refused as exc:  # a conflicting observation key: no write
            return stop("refused", str(exc))
        lists = dict(
            drop=observed["drop"], dba=observed["dba"], retained_bytes=observed["retained_bytes"]
        )
        if lifecycle == "deleting":  # read only; a real run continues with no preview
            return stop(
                "already",
                f"the source is being deleted; a real run finishes it: {len(lists['drop'])} "
                f"to drop, {len(lists['dba'])} left for the DBA",
                **lists,
            )
        try:
            decided = decision("delete-source", observed, FIELDS, log)
        except Refused as exc:  # a format-2 observation that can't be promised: no write
            return stop("refused", str(exc))
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
            f"delete source {owner.source_id}: stop capturing it; drop "
            f"{len(observed['drop'])} object(s) this code created; "
            f"{len(observed['dba'])} left for the DBA; mirrors and the log are kept",
            run_id=run_id,
            lifecycle=lifecycle,
            preview_id=run_id,
            **lists,
        )
    claim = None
    if lifecycle is None:
        spend = spender(log, inputs, run_id, actor, PREVIEWS, FIELDS)

        def claim(observed):
            spend(with_position(log, observed))

    run.step("decide", "ok", branch="act")
    run.step("act", "running", effect=f"table:{owner.database}.{owner.source_id}.source")
    try:
        result = delete_source(
            config, log, run_id=run_id, claim=claim, wait=wait, started_by=started_by
        )
    except Refused as exc:
        run.step("act", "ok")
        run.step("outcome", "ok", branch="refused")
        run.step("moved", "ok")
        return _out(inputs, "refused", str(exc), run_id=run_id, lifecycle=lifecycle)
    run.step("act", "ok")
    status = {"deleted": "done", "already": "already", "waiting": "waiting"}[result.status]
    run.step("outcome", "ok", branch=status)
    run.step(status, "ok")
    detail = result.detail or (
        f"source {owner.source_id} deleted: {len(result.dropped)} object(s) dropped; "
        "mirrors and the log are kept"
    )
    steps = [f"the DBA runs: {d}" for d in result.dba]
    if status == "waiting":
        steps.insert(0, "run delete-source again to finish")
    return _out(
        inputs,
        status,
        detail,
        run_id=run_id,
        lifecycle=result.lifecycle,
        dropped=result.dropped,
        dba=result.dba,
        retained_bytes=result.retained_bytes,
        next_steps=steps,
    )


def main(owner, environ=None):
    """Exit codes: 0 done, already, would or waiting; 2 refused; 1 failed."""
    environ = os.environ if environ is None else environ
    run = Run()  # Flight Explainer lines; see flights/charts/delete-source.mmd
    try:
        inputs = parse(environ, DELETE)
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
