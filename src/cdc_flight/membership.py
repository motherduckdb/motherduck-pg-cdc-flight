"""Operator commands that change which tables a source captures: add-table and
remove-table.

The registry (cdc_internal `tables`) is the truth and the table set; CDC_TABLES is
optional, a declaration checked against it (preflight.classify) that also restricts
apply and backfill when set. Both commands run under the ingest lease
and touch PostgreSQL only to read: they never alter the publication. Each is
idempotent: a retry after a lost response reports `already` (or finishes the rest).

- remove-table: a held table (dropped, renamed, replaced, unpublished), or with
  `force` one that still exists. The table's apply lease is taken first and its
  guard bumped, so no apply run of it writes again; then one guarded cdc_internal
  transaction marks the row `removed` and releases its retention pins. The row
  stays, so changes still in the WAL decode (logged, never applied). The mirror
  table is kept; dropping it is the operator's call.
- add-table: a table in the publication that passes preflight's checks.
  - a new OID: registered (a backfill builds its mirror);
  - an `unmanaged` OID (already published, its changes already logged): adopted;
  - a `removed` OID: re-activated with a new key version (backfill again);
  - a `renamed` hold whose OID now has this name: relabelled in place. The OID's log
    never broke, so the mirror stays exact: its apply_state and table are renamed
    (mirror transaction first, under its apply lease), then the registry.
  - another OID holding the name (`dropped`/`replaced`, the drop/recreate case) is
    retired in the same transaction (`removed`, `retired_by`, pins released); the
    new generation then replaces that mirror under the same name.
"""

import json
from dataclasses import dataclass, field, replace

from cdc_flight import lifecycle_commands as lc
from cdc_flight.action_types import Refused
from cdc_flight.config import quoted, table_name
from cdc_flight.ingest_common import append_refusal
from cdc_flight.ingest_log import LeaseBusy
from cdc_flight.ingest_log import leased as _leased
from cdc_flight.preflight import (
    PreflightError,
    catalog_key,
    full_statement,
    identity_statements,
    qualifying_unique_indexes,
    query,
    resolve,
    validate_append,
    validate_table,
)
from cdc_flight.releases import release_status


@dataclass(frozen=True)
class MembershipResult:
    status: str  # added, adopted, reactivated, relabelled, removed, already, busy, refused,
    # would (a dry run)
    table: str
    detail: str = ""
    next_steps: list = field(default_factory=list)
    observed: dict = field(default_factory=dict)  # the decision's reads: for previews


def _row(log, relation_id=None, name=None):
    for row in log.registry():
        if relation_id is not None and row["relation_id"] == relation_id:
            return row
    if name is None:
        return None
    rows = [
        r
        for r in log.registry()
        if f"{r['schema_name']}.{r['table_name']}" == name and r["held_reason"] != "unmanaged"
    ]
    rows.sort(key=lambda r: r["held_reason"] == "removed")
    return rows[0] if rows else None


def _fence_apply(apply, relation_id, run_id, detail):
    """Take the table's apply lease (if it has a mirror) and bump its guard, so a
    running apply of it can't write again. Returns the token, or None."""
    current = apply.state(relation_id)
    if current is None:
        return None
    token = apply.acquire(relation_id, run_id, token=current["token"])
    with apply.transaction():
        apply.guard(relation_id, token)
        apply.execute(
            "UPDATE {m}apply_state SET detail=? WHERE relation_id=? AND token=?",
            [detail, relation_id, token],
        )
    return token


def _release_pins(log, relation_ids):
    """Inside the caller's cdc_internal transaction: the retention guard (which a
    generation's pin registration also takes), then every pin of these relations."""
    log.execute("UPDATE {s}retention SET guard=uuid()::VARCHAR WHERE id=1")
    lc.statement(
        log,
        "pin_release",
        "pin_delete",
        "DELETE FROM {s}retention_pins WHERE relation_id IN (SELECT unnest(?::UINTEGER[]))",
        [relation_ids],
    )


def _brief(row):
    return None if row is None else dict(relation_id=row["relation_id"], held=row["held_reason"])


def _refusal(table, observed):
    def refused(exc):
        return MembershipResult("refused", table, str(exc), [], observed)

    return refused


def _unit_observation(log, table):
    if getattr(log, "format_version", None) != 2:
        raise Refused("membership requires a format-2 store")
    position = log.unit_position()
    spool = getattr(log, "_spool", None)
    if position["partial"] or (spool is not None and (spool.active or spool.frame_count)):
        return position, MembershipResult(
            "drain_required",
            table,
            "drain the open unit before changing membership",
            observed=dict(unit_position=position),
        )
    return position, None


def _require_boundary(log):
    from cdc_flight.ingest_log import SourceMismatch

    try:
        log.require_unit_boundary()
    except SourceMismatch as exc:
        raise Refused(str(exc)) from exc


def remove_table(
    log, apply, table, *, force=False, dry_run=False, check=None, consume=None, wait=0
):
    """`check(observed)` (read-only) runs before the apply fence and `consume(observed)`
    inside the registry transaction, right after its guard; either may raise Refused.
    A dry run reads only: no lease, no write."""

    def action(run_id):
        position, drain = _unit_observation(log, table)
        if drain is not None:
            return drain
        row = _row(log, name=table)
        if row is None:
            return MembershipResult("refused", table, "not registered for this source")
        relation_id = row["relation_id"]
        if row["held_reason"] == "removed":
            return MembershipResult("already", table, "removed")
        if row["held_reason"] is None and not force:
            return MembershipResult(
                "refused",
                table,
                "the table still exists and is captured; use --force to remove it anyway",
            )
        if apply.active_generation(relation_id):
            return MembershipResult(
                "refused", table, "a backfill generation is active; abandon it first"
            )
        observed = dict(row=_brief(row), active_generation=None)
        if position is not None:
            observed["unit_position"] = position
        if dry_run:
            what = "stop capturing it and release its pins; the mirror table is kept"
            return MembershipResult("would", table, f"remove {table}: {what}", [], observed)
        try:
            if check is not None:
                check(observed)  # before the fence: a stale click writes nothing
        except Refused as exc:
            return _refusal(table, observed)(exc)
        try:
            token = _fence_apply(apply, relation_id, run_id, "source table removed")
        except LeaseBusy:
            return MembershipResult("busy", table, "an apply run holds this table; retry")
        try:
            with log.transaction():
                log.guard()
                _require_boundary(log)
                if consume is not None:
                    consume(observed)
                _release_pins(log, [relation_id])
                log.execute(
                    "UPDATE {s}tables SET held_reason='removed',held_detail=?,"
                    "held_since=coalesce(held_since,current_timestamp),"
                    "held_checked_at=current_timestamp WHERE relation_id=?",
                    [f"removed by remove-table ({row['held_reason'] or 'present'})", relation_id],
                )
        except Refused as exc:
            return _refusal(table, observed)(exc)
        finally:
            if token is not None:
                apply.release(relation_id, token)
        return MembershipResult(
            "removed",
            table,
            "its retention pins are released; the mirror table is kept",
            (
                [
                    f"remove {table} from CDC_TABLES on every Flight that sets it (a stale "
                    "entry is ignored and reported)"
                ]
                if log.config.tables
                else []
            )
            + ["drop the mirror table when no one needs it"]
            + (
                [
                    f"ask the DBA to remove {table} from the publication{on_primary(log)} (it stays logged)"
                ]
                if force
                else []
            ),
            observed,
        )

    if dry_run:
        return action(None)
    return _leased(log, action, wait) or MembershipResult("busy", table, "ingest holds the lease")


MODES = ("keyed", "append")


def _start_step(table):
    return (
        f"start {table} from the log (table-action start-from-log, accepting that rows "
        "already in the table are missing)"
    )


def add_table(
    log,
    apply,
    pg,
    publication,
    table,
    *,
    mode="keyed",
    ignore_deletes=False,
    dry_run=False,
    check=None,
    consume=None,
    wait=0,
):
    """As remove_table: `check` before any write (the relabel's mirror rename included),
    `consume` inside the registry transaction. PostgreSQL is only read.

    `mode="append"`: insert-only capture of a keyless table (`ignore_deletes` only
    there): registered with no key, never backfilled; its mirror starts from the log
    (table-action start-from-log). Checked in the order the owner fixes things
    (_append_refusal), the version stamps last."""
    schema, name = table_name(table)
    if mode not in MODES:
        return MembershipResult("refused", table, f"mode must be one of {', '.join(MODES)}")
    if ignore_deletes and mode != "append":
        return MembershipResult("refused", table, "ignore_deletes is only for append mode")
    if table == log.config.heartbeat:
        # Published, but managed separately: never a captured or applied table.
        return MembershipResult("refused", table, "the heartbeat table is not a captured table")

    def action(run_id):
        position, drain = _unit_observation(log, table)
        if drain is not None:
            return drain
        found = resolve(pg, schema, name)
        if found is None:
            return MembershipResult("refused", table, "no such table in PostgreSQL")
        if mode == "append":
            return _add_append(
                log,
                run_id,
                position,
                pg,
                publication,
                table,
                found,
                ignore_deletes,
                dry_run=dry_run,
                check=check,
                consume=consume,
            )
        try:
            info = validate_table(pg, table, found)
        except PreflightError as exc:
            return MembershipResult("refused", table, str(exc))
        relation_id = info["oid"]
        published = query(
            pg,
            "SELECT 1 FROM pg_publication_rel r JOIN pg_publication p ON p.oid=r.prpubid "
            "WHERE p.pubname=%s AND r.prrelid=%s",
            [publication, relation_id],
        )
        if not published:
            return MembershipResult(
                "refused",
                table,
                f"not in the publication; ask the DBA to run{on_primary(log)}: "
                f"ALTER PUBLICATION {quoted(publication)} ADD TABLE {quoted(schema)}.{quoted(name)};",
            )
        row = _row(log, relation_id=relation_id)
        current = row["held_reason"] if row else None
        # Another OID holding this name (a drop/recreate): retire it for this one.
        holder = _row(log, name=table)
        # Both registry rows decide the branch, so a preview covers both.
        by_oid = _brief(row)
        if row is not None:
            by_oid["key_version"], row_mode, registered = log.execute(
                "SELECT key_version,capture_mode,key_columns FROM {s}tables WHERE relation_id=?",
                [relation_id],
            ).fetchone()
            # A row a later release registered for keyless (append) capture: re-adding,
            # relabelling or re-keying it here would treat it as keyed. Removing it stays
            # possible (remove_table is key-agnostic). An unmanaged row may have no key
            # (a keyless publication member) and is added once it has one, as before.
            if row_mode is not None:
                return MembershipResult(
                    "refused", table, append_refusal(table, row_mode, registered)
                )
        observed = dict(oid=relation_id, published=True, by_oid=by_oid, holder=_brief(holder))
        if position is not None:
            observed["unit_position"] = position
        refused = _refusal(table, observed)
        if row and current is None and (row["schema_name"], row["table_name"]) == (schema, name):
            return MembershipResult("already", table, "captured", [], observed)
        if row and current == "renamed":
            if dry_run:
                return MembershipResult(
                    "would",
                    table,
                    f"relabel {row['schema_name']}.{row['table_name']} as {table}",
                    [],
                    observed,
                )
            try:
                if check is not None:
                    check(observed)  # before the mirror rename
                claim = consume and (lambda: consume(observed))
                result = _relabel(log, apply, run_id, row, schema, name, claim)
                return replace(result, observed=observed)
            except Refused as exc:
                return refused(exc)
        keys = json.dumps(info["keys"])
        retire = (
            holder["relation_id"]
            if holder
            and holder["relation_id"] != relation_id
            and holder["held_reason"] in ("dropped", "replaced", "unpublished", "removed")
            else None
        )
        if holder and holder["relation_id"] != relation_id and retire is None:
            return MembershipResult(
                "refused",
                table,
                "another registered table is captured under this name",
                [],
                observed,
            )
        if dry_run:
            kind = (
                "register" if row is None else "adopt" if current == "unmanaged" else "re-activate"
            )
            also = "; retire the dropped table that has this name" if retire else ""
            return MembershipResult(
                "would", table, f"{kind} {table}{also}, then start its backfill", [], observed
            )
        try:
            if check is not None:
                check(observed)
            with log.transaction():
                log.guard()
                _require_boundary(log)
                if consume is not None:
                    consume(observed)
                _add(log, relation_id, schema, name, keys, row, current, retire)
        except Refused as exc:
            return refused(exc)
        status = "added" if row is None else "adopted" if current == "unmanaged" else "reactivated"
        return MembershipResult(
            status,
            table,
            "registered" + ("; retired the dropped table that had this name" if retire else ""),
            [f"run backfill for {table} (its mirror is built by a generation)"]
            + (
                [f"add {table} to CDC_TABLES on every Flight that sets it"]
                if log.config.tables
                else []
            ),
            observed,
        )

    if dry_run:
        return action(None)
    return _leased(log, action, wait) or MembershipResult("busy", table, "ingest holds the lease")


def _published(pg, publication, relation_id):
    return bool(
        query(
            pg,
            "SELECT 1 FROM pg_publication_rel r JOIN pg_publication p ON p.oid=r.prpubid "
            "WHERE p.pubname=%s AND r.prrelid=%s",
            [publication, relation_id],
        )
    )


def on_primary(log):
    """Replica capture: on a standby source the publication is the primary's."""
    return " on the primary" if log.config.heartbeat_writer == "external" else ""


def _publish_statement(publication, schema, name):
    return f"ALTER PUBLICATION {quoted(publication)} ADD TABLE {quoted(schema)}.{quoted(name)};"


def _append_refusal(log, pg, publication, table, found):
    """add-table in append mode: the first check that fails, in the order the owner fixes
    them. The key is read live from PostgreSQL (the registry's catalog_key is '[]' for an
    unmanaged row and stale for a removed one). Every check passing still refuses here."""
    schema, name = table_name(table)
    oid = found[0]
    if catalog_key(pg, oid):
        return (
            f"{table} has a primary key or replica identity index: add it keyed, not in append mode"
        )
    indexes = qualifying_unique_indexes(pg, oid)
    if indexes:
        return (
            f"{table} has a unique index that can be its key: capture it keyed instead. Run "
            + " or ".join(identity_statements(table, indexes))
            + " then add it keyed"
        )
    published = _published(pg, publication, oid)
    try:
        validate_append(pg, table, found)
    except PreflightError as exc:
        if getattr(exc, "reason", None) != "append_identity":
            return str(exc)
        if published:
            # Already published without FULL: its UPDATE and DELETE fail on the source now.
            return f"{exc}. Run {full_statement(table)}"
        return f"{exc}. Run, in this order: {full_statement(table)} then " + _publish_statement(
            publication, schema, name
        )
    if not published:
        return (
            f"not in the publication; ask the DBA to run{on_primary(log)}: "
            + _publish_statement(publication, schema, name)
        )
    row = _row(log, relation_id=oid)
    if row is not None and row["held_reason"] not in ("unmanaged", "removed"):
        reason = row["held_reason"]
        return (
            f"{table} is registered ({'captured keyed' if reason is None else 'held: ' + reason}): "
            "remove it, then add it in append mode"
        )
    holder = _row(log, name=table)
    if holder and holder["relation_id"] != oid and holder["held_reason"] != "removed":
        return (
            "another registered table is captured under this name: remove it "
            f"(remove-table {table}), then add this one in append mode"
        )
    status = release_status(log.execute)
    waiting = [c for c, s in status.items() if s == "missing"]
    if waiting:
        listed = ", ".join(f"{c}: {s}" for c, s in status.items())
        return (
            "append mode needs every Flight that runs a keyed path on this release or later "
            f"({listed}). Run each missing one once in its Flight, or, if this source has "
            "no such Flight, declare it: cdc-flight-tables releases --not-deployed <command>"
        )
    return None


def _add_append(
    log,
    run_id,
    position,
    pg,
    publication,
    table,
    found,
    ignore_deletes,
    *,
    dry_run,
    check,
    consume,
):
    """add-table in append mode, after the drain check: refusals (_append_refusal), then
    one guarded registry write. Nothing is backfilled: the next step is start-from-log."""
    schema, name = table_name(table)
    oid = found[0]
    row = _row(log, relation_id=oid)
    if row is not None and row["held_reason"] is None and row.get("capture_mode") == "append":
        return MembershipResult("already", table, "captured insert-only", [_start_step(table)])
    observed = dict(
        oid=oid, published=True, by_oid=_brief(row), holder=_brief(_row(log, name=table))
    )
    if position is not None:
        observed["unit_position"] = position
    refusal = _append_refusal(log, pg, publication, table, found)
    if refusal:
        return MembershipResult("refused", table, refusal, [], observed)
    current = row["held_reason"] if row else None
    kind = "register" if row is None else "adopt" if current == "unmanaged" else "re-activate"
    if dry_run:
        return MembershipResult(
            "would",
            table,
            f"{kind} {table} for insert-only capture"
            + (", skipping DELETEs" if ignore_deletes else "")
            + ", then start it from the log",
            [],
            observed,
        )
    try:
        if check is not None:
            check(observed)
        with log.transaction():
            log.guard()
            _require_boundary(log)
            if consume is not None:
                consume(observed)
            log.activate_member(
                oid, schema, name, "[]", current, append=True, ignore_deletes=ignore_deletes
            )
            # A removed same-name table: retired for this one, as a keyed add does.
            log.execute(
                "UPDATE {s}tables SET retired_by=? WHERE schema_name=? AND table_name=? "
                "AND relation_id<>? AND held_reason='removed' AND retired_by IS NULL",
                [oid, schema, name, oid],
            )
    except Refused as exc:
        return MembershipResult("refused", table, str(exc), [], observed)
    status = "added" if row is None else "adopted" if current == "unmanaged" else "reactivated"
    return MembershipResult(
        status,
        table,
        "registered for insert-only capture" + (" (DELETEs skipped)" if ignore_deletes else ""),
        [_start_step(table)]
        + (
            [f"add {table} to CDC_TABLES on every Flight that sets it"] if log.config.tables else []
        ),
        observed,
    )


def _add(log, relation_id, schema, name, keys, row, current, retire):
    """add-table's registry writes, inside its guarded transaction."""
    if retire is not None:
        _release_pins(log, [retire])
        log.execute(
            "UPDATE {s}tables SET held_reason='removed',retired_by=?,"
            "held_detail='retired by add-table for a new table of the same name',"
            "held_checked_at=current_timestamp WHERE relation_id=?",
            [relation_id, retire],
        )
    log.activate_member(relation_id, schema, name, keys, current)


def _relabel(log, apply, run_id, row, schema, name, consume=None):
    """A renamed table, same OID: rename its mirror state and table, then the registry.
    Each step is idempotent, so a retry after a crash between them finishes the rest."""
    relation_id = row["relation_id"]
    old = f"{row['schema_name']}.{row['table_name']}"
    new = f"{schema}.{name}"
    if schema != row["schema_name"]:
        return MembershipResult(
            "refused",
            new,
            f"{old} moved to another schema; a mirror can't follow that: remove-table {old}, "
            f"then add-table {new} (a backfill rebuilds it)",
        )
    current = apply.state(relation_id)
    if current is not None:
        target = f"{apply.mirror}.{quoted(schema)}.{quoted(name)}"
        if current["target"] != target:
            taken = apply.con.execute(
                "SELECT count(*) FROM duckdb_tables() WHERE database_name=? AND schema_name=? "
                "AND table_name=?",
                [apply.config.mirror_database, schema, name],
            ).fetchone()[0]
            if taken:
                return MembershipResult(
                    "refused", new, f"{target} already exists in the mirror database"
                )
            try:
                token = apply.acquire(relation_id, run_id, token=current["token"])
            except LeaseBusy:
                return MembershipResult("busy", new, "an apply run holds this table; retry")
            try:
                with apply.transaction():
                    apply.guard(relation_id, token)
                    apply.con.execute(f"ALTER TABLE {current['target']} RENAME TO {quoted(name)}")
                    apply.execute(
                        "UPDATE {m}apply_state SET schema_name=?,table_name=?,detail=NULL "
                        "WHERE relation_id=? AND token=?",
                        [schema, name, relation_id, token],
                    )
            finally:
                apply.release(relation_id, token)
    with log.transaction():
        log.guard()
        _require_boundary(log)
        if consume is not None:
            consume()  # the caller's preview claim, bound to its observation
        log.execute(
            "UPDATE {s}tables SET schema_name=?,table_name=?,former_name=?,held_reason=NULL,"
            "held_detail=NULL,held_since=NULL,held_checked_at=NULL WHERE relation_id=?",
            [schema, name, old, relation_id],
        )
    return MembershipResult(
        "relabelled",
        new,
        f"renamed from {old}: the mirror keeps its data and resumes from its checkpoint",
        (
            [f"replace {old} with {new} in CDC_TABLES on every Flight that sets it"]
            if log.config.tables
            else []
        ),
    )
