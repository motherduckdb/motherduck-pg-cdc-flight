"""Keyless append (insert-only) capture's apply: the fold's counterpart, the writer and
the start from the log. Imported lazily by Apply only for
an append table, so a command that never applies units (table-membership) doesn't load
it."""

import json
import re
import tempfile
import uuid
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from cdc_flight import publication_commands as pub
from cdc_flight.apply import PG_EPOCH_US, ApplyError, _DryRun, pin_holds
from cdc_flight.config import LeaseBusy, quoted, table_name
from cdc_flight.generation_commands import inserted
from cdc_flight.mirror import Blocked, cast_sql, reconcile
from cdc_flight.retention import (
    HistoryDropped,
    PinMissing,
    TableHeld,
    generation_owner,
    table_owner,
)


class AppendRows:
    append = True  # Apply._write's switch, without importing this module
    """An append batch's write (keyless insert-only capture): the INSERT rows after the
    batch's last TRUNCATE (each `(values, commit_lsn, event_ordinal, commit_ts_us)`), the
    last TRUNCATE's commit lsn (the mirror is emptied first) or None, and the DELETE
    events skipped under ignore_deletes. Built fresh: events are never mutated."""

    def __init__(self, relation_id, rows, truncate_lsn, deletes_ignored):
        self.relation_id = relation_id
        self.rows, self.truncate_lsn = rows, truncate_lsn
        self.deletes_ignored = deletes_ignored

    def __bool__(self):
        return bool(self.rows) or self.truncate_lsn is not None or bool(self.deletes_ignored)


def append_batch(apply, relation_id, current, events, descriptors):
    """Keyless insert-only capture's counterpart of the fold.

    `events` are presented (_present: to_v1 has rejected malformed images, a bad
    old_tuple_kind and any width or tag error). One pass in commit order; the first hold
    wins, so the events before it never hold. Per event:
    - its schema version, the first time it appears, reconciles as keyed tables do
      (ADD COLUMN; drop, rename or retype holds), never reading the identity: an INSERT
      carries every column whatever it is, and UPDATE/DELETE hold below;
    - insert → one row; its new image must be complete (no UNCHANGED cell), and a
      column absent from its version is NULL, as the fold does;
    - update → hold `append_update`; delete → hold `append_delete`, or skipped and
      counted under ignore_deletes (every skipped DELETE counts, before a TRUNCATE
      too: an event count, not a row effect);
    - truncate → the mirror is emptied at that event: only later rows remain.
    Returns (usable, columns, AppendRows, blocked) like fold_batch: on a hold, the
    events of earlier commits (the caller cuts again at the hold's whole unit)."""
    columns = current["columns"]
    names = [c.name for c in columns] if columns else []
    seen = set()
    rows, truncate, ignored = [], None, 0
    for event in events:
        op, commit = event["op"], event["commit_lsn"]
        if op == "truncate":
            rows, truncate = [], commit
            continue
        version = event["schema_version"]
        if version not in seen:
            seen.add(version)
            try:
                # Identity-agnostic: reconcile's identity checks are about a key.
                value = json.loads(descriptors[version])
                columns = reconcile(columns, dict(value, identity="f"), ())
            except Blocked as exc:
                exc.commit_lsn = commit
                return _append_cut(apply, relation_id, current, events, exc, descriptors)
            names = [c.name for c in columns]
        if op == "delete" and current.get("ignore_deletes"):
            ignored += 1
            continue
        if op in ("update", "delete"):
            blocked = Blocked(
                "needs_action",
                f"append_{op}: an {op.upper()} on an insert-only table; resync it (an "
                "exact re-snapshot, which holds again if the table keeps changing rows), "
                "quarantine its unit, start it again from the log, or remove the table",
                commit,
                code=f"append_{op}",
            )
            return _append_cut(apply, relation_id, current, events, blocked, descriptors)
        new = event["payload"].get("new")
        if op != "insert" or new is None:
            blocked = Blocked(
                "needs_action", f"unsupported change {op!r}", commit, code="unsupported_change"
            )
            return _append_cut(apply, relation_id, current, events, blocked, descriptors)
        values = {}
        for name in names:
            if name not in new:
                values[name] = None  # not in this schema version yet (ADD COLUMN)
                continue
            cell = new[name]
            if cell["kind"] not in ("value", "null"):
                blocked = Blocked(
                    "needs_action",
                    "insert without a complete new image",
                    commit,
                    code="incomplete_insert",
                )
                return _append_cut(apply, relation_id, current, events, blocked, descriptors)
            values[name] = cell["value"]
        rows.append((values, commit, event["event_ordinal"], event["commit_ts_us"]))
    return events, columns, AppendRows(relation_id, rows, truncate, ignored), None


def _append_cut(apply, relation_id, current, events, blocked, descriptors):
    """append_batch's hold: the events of earlier commits, applied; then the hold."""
    usable = [e for e in events if e["commit_lsn"] < blocked.commit_lsn]
    _, columns, rows, again = append_batch(apply, relation_id, current, usable, descriptors)
    if again:
        raise AssertionError("events before a blocking commit cannot block")
    return usable, columns, rows, blocked


def write_append(
    apply, current, columns, batch, *, guard, commit_state, fault, dry_run, count_state=True
):
    """An append batch's one transaction: guard, create or ALTER ADD COLUMN, empty the
    mirror if a TRUNCATE is in the batch, INSERT the rows after it, count skipped
    DELETEs, then the checkpoint. Exactly once by the checkpoint rule: no key, no MERGE.

    `count_state=False` (a backfill catch-up into staging): the skipped DELETEs and the
    last TRUNCATE are the caller's to record, in its `commit_state`; the live mirror's
    apply_state row is never touched."""
    target = current["target"]
    with tempfile.TemporaryDirectory(prefix="cdc_append_") as directory:
        path = None
        if batch.rows:
            data = {
                f"c{i}": pa.array([r[0].get(c.name) for r in batch.rows], pa.string())
                for i, c in enumerate(columns)
            }
            data["_s_lsn"] = pa.array([r[1] for r in batch.rows], pa.uint64())
            data["_s_ord"] = pa.array([r[2] for r in batch.rows], pa.uint64())
            data["_s_ts"] = pa.array(
                [r[3] + PG_EPOCH_US for r in batch.rows], pa.timestamp("us", tz="UTC")
            )
            path = str(Path(directory) / f"append_{uuid.uuid4().hex}.parquet")
            pq.write_table(pa.table(data), path)
        with apply.transaction():
            guard()
            if columns and current["columns"] is None:
                apply._create(current, columns)
            elif columns:
                for column in columns[len(current["columns"]) :]:
                    apply.execute(
                        f"ALTER TABLE {target} ADD COLUMN {quoted(column.name)} {column.dtype}"
                    )
            if batch.truncate_lsn is not None and current["columns"] is not None:
                apply.con.execute(f"DELETE FROM {target}")
            if path:
                names = ",".join(quoted(c.name) for c in columns)
                casts = ",".join(cast_sql(f"c{i}", c.dtype) for i, c in enumerate(columns))
                apply.con.execute(
                    f"INSERT INTO {target} ({names},_cdc_commit_lsn,_cdc_event_ordinal,"
                    f"_cdc_updated_at) SELECT {casts},_s_lsn,_s_ord,_s_ts "
                    "FROM read_parquet(?)",
                    [path],
                )
            if count_state and (batch.deletes_ignored or batch.truncate_lsn is not None):
                apply.execute(
                    "UPDATE {m}apply_state SET deletes_ignored=coalesce(deletes_ignored,0)+?,"
                    "last_truncate_lsn=coalesce(?,last_truncate_lsn) WHERE relation_id=?",
                    [batch.deletes_ignored, batch.truncate_lsn, batch.relation_id],
                )
            if dry_run:
                raise _DryRun()
            if fault:
                fault("after_merge")
            commit_state()
        if fault:
            fault("after_commit")


def start_decision(apply, relation_id, *, repair=True):
    """What start-from-log does for an append table: ("start", None, None) with no
    generation; ("already", generation, None) for a healthy one (append, live, its table
    pin on that generation); else ("replace", generation, why) for one that can't apply:
    held (needs_action, needs_resync), a keyed generation left from before a mode switch,
    or pins released by a remove. ("paused", generation, None): a deliberate
    pause is resumed, not replaced."""
    current = apply.state(relation_id)
    if current is None:
        return "start", None, None
    if current["capture_mode"] != "append":
        why = "its mirror generation is keyed (added again in append mode)"
    elif not _pinned(apply, relation_id, current, repair=repair):
        why = "its log pins were released (removed and added again)"
    elif current["state"] == "paused":
        return "paused", current["generation"], None  # resume it: nothing is wrong
    elif current["state"] != "live":
        why = f"it is {current['state']}"
    else:
        return "already", current["generation"], None
    return "replace", current["generation"], why


def _pinned(apply, relation_id, current, *, repair=True):
    """Whether the generation's log is pinned from its checkpoint on, completing a pin
    transfer a crash interrupted first (as apply's ensure_pin does): an interrupted
    transfer is a healthy generation, never one to replace.
    `repair=False` (a dry run, read only): a pending transfer (the generation's own pin
    still there) counts as pinned, without completing it."""
    if not repair:
        table_pin = apply.retention.pin(table_owner(relation_id))
        return pin_holds(table_pin, current) or (
            apply.retention.pin(generation_owner(current["generation"])) is not None
        )
    try:
        apply.ensure_pin(relation_id, current)
    except (PinMissing, HistoryDropped):
        return False
    return True


def passed_catalog_version(apply, relation_id, epoch, lsn):
    """The key version an append generation starting at (epoch, lsn) records: past every
    catalog hold (catalog_checks) cut at or before it, whatever its version number (a
    remove and re-add restarts the registry's). A hold cut beyond it still holds the
    generation. A snapshot publication passes its own position."""
    (version,) = apply.execute(
        "SELECT coalesce(max(version),0) FROM {s}key_changes WHERE relation_id=? "
        "AND reason IS NOT NULL AND (epoch<? OR (epoch=? AND cut_lsn<=?))",
        [relation_id, epoch, epoch, lsn],
    ).fetchone()
    return version


def start_append(apply, table, *, accept_incomplete=False, claim=None, audit=None, run_id=None):
    """An append (keyless insert-only) table's log-only generation, at S: the
    current durable unit boundary of the current epoch, with its pin at S (review
    M1). Rows inserted before S are never captured, so it is always
    `log_incomplete` and needs accept_incomplete. Nothing before S is read: any epoch,
    and any retention floor below S, will do.

    It replaces a generation that can't apply (start_decision), never a healthy one:
    under that table's apply lease, the old mirror table is renamed to
    `<table>__gen_<old generation>` (its rows are kept, never reused: a keyed mirror has
    a primary key and no ordinal) and the state row moves to the new generation; the
    pin transfer then releases the old generation's pins. Returns (generation, epoch,
    S, the kept mirror's name or None). `claim()` (the table action's preview) runs and
    `audit` (run_id, detail) is written inside the mirror transaction."""
    relation_id, _ = apply.registered(table, append_ok=True)
    if not accept_incomplete:
        raise ApplyError(
            f"{table} is captured insert-only: its mirror holds only rows inserted from "
            "now on (rows already in the table are missing); accept_incomplete to start"
        )
    held, ignore_deletes = apply.execute(
        "SELECT held_reason,coalesce(ignore_deletes,false) FROM {s}tables WHERE relation_id=?",
        [relation_id],
    ).fetchone()
    if held:
        raise ApplyError(f"{table}: the source table is held ({held})")
    claimed = _claim(apply, relation_id)  # None: no claims table in this mirror
    if claimed:
        raise ApplyError(f"{table}: an append backfill is running; abandon it or let it finish")
    decision, old, _ = start_decision(apply, relation_id)
    if decision == "already":
        raise ApplyError(f"{table} already has a live mirror generation")
    if decision == "paused":
        raise ApplyError(f"{table} is paused: resume it instead")
    if decision == "replace":
        refusal = kept_refusal(apply, *table_name(table))
        if refusal:
            raise ApplyError(f"{table}: {refusal}")
    epoch, start = apply.source_position()
    version = passed_catalog_version(apply, relation_id, epoch, start)
    schema, name = table_name(table)
    generation = uuid.uuid4().hex
    token = kept = None
    if decision == "replace":
        current = apply.state(relation_id)
        try:
            token = apply.acquire(relation_id, run_id or generation, token=current["token"])
        except LeaseBusy:
            raise ApplyError(f"{table}: an apply run holds this table; retry") from None
        # Decided before the lease: decide again under it (an apply run may have moved
        # the table in between).
        if start_decision(apply, relation_id)[:2] != ("replace", old):
            _release_lease(apply, relation_id, token)
            raise ApplyError(f"{table}: its mirror generation changed; look again")
    try:
        apply.retention.register_generation(relation_id, generation, epoch, start)
    except TableHeld as exc:
        _release_lease(apply, relation_id, token)
        raise ApplyError(f"{table}: {exc}") from exc
    try:
        with apply.transaction():
            if token is not None:
                apply.guard(relation_id, token)
            elif apply.execute(
                "SELECT 1 FROM {m}apply_state WHERE relation_id=?", [relation_id]
            ).fetchone():
                raise ApplyError(f"{table} already has a generation")
            # Claim exclusivity (publish's premise): a backfill that claimed the
            # table since the early check moves apply_state.generation_id at its publish, so
            # refuse again under the lease. A claims table missing at the early check may
            # have been created since (a first backfill): probe the catalog, which can't
            # abort this transaction as reading a missing table would. On MotherDuck the
            # client catalog can lag, so this narrows that window rather than closing it;
            # a miss there ends in a false InvariantViolation at publish that the next run
            # heals.
            if claimed is None and _claims_table_now(apply):
                claimed = False
            if claimed is not None and backfill_running(apply, relation_id):
                raise ApplyError(
                    f"{table}: an append backfill is running; abandon it or let it finish"
                )
            if claim is not None:
                claim()
            if audit is not None:
                apply.execute(
                    "INSERT INTO {m}runs (run_id,relation_id,status,boundary,detail,"
                    "updated_at) VALUES (?,?,'start-from-log',?,?,current_timestamp)",
                    [audit[0], relation_id, start, audit[1]],
                )
            inserted(
                apply,
                "create_log_only_append",
                "generation_insert",
                "INSERT INTO {m}generations (generation_id,relation_id,mode,epoch,pin_lsn,"
                "marker_lsn,state,created_at) VALUES (?,?,'log_incomplete',?,?,?,'published',"
                "current_timestamp)",
                [generation, relation_id, epoch, start, start],
            )
            if token is None:
                inserted(
                    apply,
                    "create_log_only_append",
                    "apply_insert",
                    "INSERT INTO {m}apply_state (relation_id,schema_name,table_name,key_columns,"
                    "generation_id,epoch,applied_through_lsn,columns,state,token,guard,"
                    "updated_at,hold_code_at,key_version,capture_mode,ignore_deletes,"
                    "deletes_ignored) VALUES (?,?,?,'[]',?,?,?,NULL,'live',0,uuid()::VARCHAR,"
                    "current_timestamp,current_timestamp,?,'append',?,0)",
                    [relation_id, schema, name, generation, epoch, start, version, ignore_deletes],
                )
            else:
                kept = _keep_mirror(apply, schema, name, old)
                pub.replace(apply, "create_log_only_append", old, stamped=False)
                pub.statement(
                    apply,
                    "create_log_only_append",
                    "apply_cas",
                    "UPDATE {m}apply_state SET schema_name=?,table_name=?,key_columns='[]',"
                    "generation_id=?,epoch=?,applied_through_lsn=?,columns=NULL,state='live',"
                    "detail=NULL,blocked_commit_lsn=NULL,blocked_unit_start_lsn=NULL,"
                    "held_since=NULL,key_version=?,capture_mode='append',ignore_deletes=?,"
                    "deletes_ignored=0,last_truncate_lsn=NULL,guard=uuid()::VARCHAR,"
                    "holder=NULL,expires_at=NULL,hold_code=NULL,hold_code_at=current_timestamp,"
                    "updated_at=current_timestamp "
                    "WHERE relation_id=? AND token=?",
                    [
                        schema,
                        name,
                        generation,
                        epoch,
                        start,
                        version,
                        ignore_deletes,
                        relation_id,
                        token,
                    ],
                )
    except Exception:
        # Refused inside the transaction (rolled back): the pin protects nothing.
        apply._release_quietly(generation_owner(generation), generation)
        _release_lease(apply, relation_id, token)
        raise
    apply.transfer_quietly(relation_id, generation, epoch, start)
    return generation, epoch, start, kept


def _claim(apply, relation_id):
    """Whether a backfill holds this table's claim; None if the mirror has no claims
    table (outside a transaction: the missing table's error would abort one)."""
    try:
        return (
            apply.execute(
                "SELECT 1 FROM {m}backfills WHERE relation_id=?", [relation_id]
            ).fetchone()
            is not None
        )
    except duckdb.CatalogException:
        return None


def _claims_table_now(apply):
    """Whether the mirror has the claims table, from the catalog (safe in a transaction)."""
    return (
        apply.execute(
            "SELECT count(*) FROM duckdb_tables() WHERE database_name=? AND schema_name='_cdc' "
            "AND table_name='backfills'",
            [apply.config.mirror_database],
        ).fetchone()[0]
        > 0
    )


def backfill_running(apply, relation_id):
    """Whether a backfill generation (an append snapshot) holds this table's claim."""
    return bool(_claim(apply, relation_id))


def kept_aside(apply, schema, name):
    """This table's kept-aside mirrors: exactly `<name>__gen_` plus 8 hex digits
    (_keep_mirror's names). A staging table (`__gen_` plus 12) never matches."""
    pattern = re.compile(re.escape(name) + "__gen_[0-9a-f]{8}")
    rows = apply.con.execute(
        "SELECT table_name FROM duckdb_tables() WHERE database_name=? AND schema_name=?",
        [apply.config.mirror_database, schema],
    ).fetchall()
    return sorted(r[0] for r in rows if pattern.fullmatch(r[0]))


def kept_refusal(apply, schema, name):
    """At most one kept-aside mirror per table (phase 2 Q3b): a replacement that would
    keep another is refused while one exists, with the statement that drops it. Nothing
    is ever dropped automatically. None when there is none."""
    kept = kept_aside(apply, schema, name)
    if not kept:
        return None
    drops = "; ".join(f"DROP TABLE {apply.mirror}.{quoted(schema)}.{quoted(k)}" for k in kept)
    return (
        f"a replaced mirror is still kept aside ({', '.join(kept)}): keep a copy if you "
        f"need its rows, then drop it ({drops}) and retry"
    )


def _keep_mirror(apply, schema, name, old):
    """The replaced generation's mirror table, renamed aside (inside the caller's mirror
    transaction), or None when it never had one."""
    database = apply.config.mirror_database
    exists = "SELECT count(*) FROM duckdb_tables() WHERE database_name=? AND schema_name=? AND table_name=?"
    if not apply.con.execute(exists, [database, schema, name]).fetchone()[0]:
        return None
    kept = f"{name}__gen_{old[:8]}"
    if apply.con.execute(exists, [database, schema, kept]).fetchone()[0]:
        raise ApplyError(f"{schema}.{kept} already exists in the mirror database")
    apply.con.execute(
        f"ALTER TABLE {apply.mirror}.{quoted(schema)}.{quoted(name)} RENAME TO {quoted(kept)}"
    )
    return f"{schema}.{kept}"


def _release_lease(apply, relation_id, token):
    if token is not None:
        try:
            apply.release(relation_id, token)
        except Exception:
            pass  # it expires
