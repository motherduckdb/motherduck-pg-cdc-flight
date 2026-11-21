"""Apply's and backfill's operations status (see ops.py): snapshots of the mirror,
read inside the write, newest read wins per row."""

from cdc_flight.config import quoted
from cdc_flight.ingest_common import managed
from cdc_flight.ops import Ops, commit_time_sql, prune_window
from cdc_flight.ops_schema import APPLY as APPLY
from cdc_flight.ops_schema import APPLY_V2 as APPLY_V2
from cdc_flight.ops_schema import BACKFILL as BACKFILL
from cdc_flight.ops_schema import BACKFILL_V2 as BACKFILL_V2

# The newest quarantined commits listed across the whole source (apply rewrites every
# row every run, so the bound must not grow with the table count); counts stay exact.
QUARANTINES = 200


# Format 2 only (apply2/backfill2): created and written only by a format-2 report, so v1
# runs never ALTER cdc_ops for them. format_version is NULL on v1 rows.
_STATUS_V2 = ",format_version,blocked_unit_start_lsn,blocked_unit_end_lsn"
_APPEND = ("capture_mode", "ignore_deletes", "deletes_ignored", "last_truncate_lsn")
# The hold's code: the registry's held_reason when the log holds the table (it isn't
# ingested), the not-started row's own code, else the mirror's code, but only when it was
# written by the statement that set the current state (hold_code_at = updated_at): an
# older build's later state change leaves them unequal, and the code reads NULL (unknown).
_HOLD_CODE = (
    "CASE WHEN t.held_reason IS NOT NULL THEN t.held_reason "
    "WHEN s.state='not_started' THEN 'not_started' "
    "WHEN s.state<>'live' AND s.hold_code_at=s.updated_at THEN s.hold_code END"
)


def _not_started(config, log):
    """apply_state plus a row for each table apply covers (apply's own table set: the
    registry's managed tables, never the heartbeat, an unmanaged member or a removed row;
    only CDC_TABLES' when set) that has neither an apply_state row nor an active
    generation (registered, never started; or its first backfill was refused): state
    not_started."""
    m = f"{quoted(config.mirror_database)}._cdc."
    configured = ""
    if getattr(config, "tables", ()):  # CDC_TABLES set: one it leaves out is not_configured
        names = ",".join("'" + t.replace("'", "''") + "'" for t in config.tables)
        configured = f"AND n.schema_name||'.'||n.table_name IN ({names}) "
    return (
        f"(SELECT * FROM {m}apply_state UNION ALL BY NAME "
        "SELECT n.relation_id,n.schema_name,n.table_name,'not_started' AS state,"
        f"n.capture_mode FROM {log}tables n WHERE "
        + managed("n", log)
        + " "
        + configured
        + f"AND n.relation_id NOT IN (SELECT relation_id FROM {m}apply_state) "
        # An active generation (generations.ACTIVE): its backfill_status row shows it.
        f"AND n.relation_id NOT IN (SELECT relation_id FROM {m}generations "
        "WHERE state IN ('snapshotting','catching_up')))"
    )


# "Current as of": the commit time at the table's checkpoint and the (epoch, LSN) it
# belongs to (ops_schema). A run that can't tell keeps the stored triple: its pair still
# says which position it belongs to, so it is never shown against another one.
_THROUGH = ("applied_through_commit_ts", "applied_through_epoch", "applied_through_lsn")
_TIMES = "STRUCT(epoch INTEGER,lsn UBIGINT,ts BIGINT)[]"


def _through(ops, times):
    """(select expressions, joins, join params) for the triple: this run's unit times
    first (the units its reads checked: unit_store.check_frames), else an exact (epoch,
    checkpoint) match in ingest's run history (an idle table reads no units), else NULL,
    which keeps the stored triple."""
    entries = [dict(epoch=e, lsn=lsn, ts=ts) for (e, lsn), ts in sorted((times or {}).items())]
    found = [commit_time_sql("mt.ts")]
    joins = (
        f" LEFT JOIN (SELECT unnest(?::{_TIMES},recursive:=true)) mt "
        "ON mt.epoch=s.epoch AND mt.lsn=s.applied_through_lsn"
    )
    params = [entries]
    # Only when ingest's new columns exist (an older ingest's table lacks them).
    if {"epoch", "confirmed_end", "durable_through_commit_ts"} <= ops.columns.get(
        "ingest_runs", set()
    ):
        found.append("ir.ts")
        joins += (
            " LEFT JOIN (SELECT epoch,confirmed_end,max(durable_through_commit_ts) AS ts "
            "FROM {o}ingest_runs WHERE source_id=? AND durable_through_commit_ts IS NOT NULL "
            "GROUP BY epoch,confirmed_end) ir "
            "ON ir.epoch=s.epoch AND ir.confirmed_end=s.applied_through_lsn"
        )
        params.append(ops.source_id)
    ts = f"coalesce({','.join(found)})"
    select = (
        f",{ts},CASE WHEN {ts} IS NOT NULL THEN s.epoch END,"
        f"CASE WHEN {ts} IS NOT NULL THEN s.applied_through_lsn END"
    )
    return select, joins, params


def snapshot(ops, table, key, columns, select, source, params, keep=()):
    """INSERT … SELECT from the stores; each row replaced only by a newer read.
    `keep`: columns a run may not know (NULL), which then keep their stored value."""
    sets = ",".join(
        f"{c}=coalesce(excluded.{c},{table}.{c})" if c in keep else f"{c}=excluded.{c}"
        for c in columns.split(",")[2:]
    )
    ops.execute(
        f"INSERT INTO {{o}}{table} ({columns}) SELECT {select} FROM {source} "
        f"ON CONFLICT(source_id,{key}) DO UPDATE SET {sets} WHERE excluded.as_of>{table}.as_of",
        params,
    )


def apply_snapshot(ops, config, run_id, table=None, *, format_version=2, times=None):
    """Every table's state (or one table's, `schema.table`) and its quarantined commits,
    read from the mirror inside the write (one statement). `format_version=2`: also
    the format-2 mirror's blocked unit (only a format-2 mirror has the column)."""
    log = f"{quoted(config.database)}.{quoted(config.source_id)}."
    v2 = format_version == 2
    through, joins, through_params = _through(ops, times) if v2 else ("", "", [])
    snapshot(
        ops,
        "apply_status",
        "relation_id",
        "source_id,relation_id,as_of,written_at,run_id,schema_name,table_name,state,"
        "held_since,detail,blocked_commit_lsn,epoch,applied_lsn,generation_id,"
        "key_version,held_reason,pin_lsn,pin_updated_at,quarantine_count,quarantines"
        + (
            _STATUS_V2 + "," + ",".join(_THROUGH) + "," + ",".join(_APPEND) + ",hold_code"
            if v2
            else ""
        ),
        "?,s.relation_id,epoch_us(current_timestamp),current_timestamp,?,s.schema_name,"
        "s.table_name,s.state,s.held_since,s.detail,s.blocked_commit_lsn,s.epoch,"
        "s.applied_through_lsn,s.generation_id,s.key_version,t.held_reason,p.pinned_lsn,"
        "p.updated_at,q.n,coalesce(q.items,'[]')"
        + (
            ",2,s.blocked_unit_start_lsn,u.end_lsn"
            + through
            + ",s.capture_mode,s.ignore_deletes,s.deletes_ignored,s.last_truncate_lsn,"
            + _HOLD_CODE
            if v2
            else ""
        ),
        (_not_started(config, log) if v2 else f"{quoted(config.mirror_database)}._cdc.apply_state")
        + f" s LEFT JOIN {log}tables t "
        f"USING (relation_id) LEFT JOIN {log}retention_pins p "
        "ON p.owner='table:'||s.relation_id "
        # The current generation's quarantined commits (the recorded gaps): no
        # reason text (operator-entered; the audit row holds it, by run_id).
        "LEFT JOIN (SELECT relation_id,count(*)::INTEGER AS n,"
        "to_json(coalesce(list({'v':1,'epoch':epoch,'commit_lsn':commit_lsn,'run_id':run_id,"
        "'created_at':created_at::VARCHAR"
        # Format 2 quarantines a whole unit: its start and end (the mirror always has them).
        + (",'unit_start_lsn':unit_start_lsn,'unit_end_lsn':unit_end_lsn" if v2 else "")
        + "} ORDER BY created_at DESC,commit_lsn DESC) "
        f"FILTER (WHERE k<={QUARANTINES}),[]))::VARCHAR AS items FROM (SELECT x.*,"
        "row_number() OVER (ORDER BY x.created_at DESC,x.commit_lsn DESC,x.relation_id) AS k "
        f"FROM {quoted(config.mirror_database)}._cdc.quarantine x "
        f"JOIN {quoted(config.mirror_database)}._cdc.apply_state c "
        "USING (relation_id,generation_id,epoch)) GROUP BY relation_id) q "
        "ON q.relation_id=s.relation_id"
        + (
            f" LEFT JOIN {log}units_v2 u ON u.epoch=s.epoch "
            "AND u.unit_start_lsn=s.blocked_unit_start_lsn AND u.complete"
            if v2
            else ""
        )
        + joins
        + (" WHERE s.schema_name||'.'||s.table_name=?" if table else ""),
        [config.source_id, run_id] + through_params + ([table] if table else []),
        keep=_THROUGH if v2 else (),
    )


def report_apply(
    con,
    config,
    run_id,
    results,
    status="done",
    error=None,
    step=None,
    remaining=None,
    database="cdc_ops",
    format_version=2,
    times=None,
):
    """Every table's state and its quarantined commits (one statement), then the run's own row and its per-table
    results (one INSERT, also for a run that failed before any table): 3 MotherDuck
    statements with the ATTACH, 4 on a pruning run (5 on format 2: stale not_started rows). `format_version=2` (apply2) adds
    the format-2 columns; v1's statements are unchanged."""
    ops = Ops(con, config.database, config.source_id, database)
    v2 = format_version == 2

    def write():
        apply_snapshot(ops, config, run_id, format_version=format_version, times=times)
        rows = [(None, status, None, None, error, step) + ((2, None) if v2 else ())] + [
            (r.table, r.status, r.events, r.boundary, None, None) + ((2, r.units) if v2 else ())
            for r in results or ()
        ]
        columns = "source_id,run_id,table_name,written_at,status,events,applied_lsn,error,step" + (
            ",format_version,units_applied" if v2 else ""
        )
        unnested = "unnest(?),current_timestamp,unnest(?),unnest(?),unnest(?),unnest(?),unnest(?)"
        if v2:
            # Each table's time at its row's applied_lsn: the triple just written, when
            # it belongs to that LSN (NULL otherwise, and on the run's own row).
            ops.execute(
                "INSERT INTO {o}apply_runs (" + columns + ",applied_through_commit_ts) "
                "SELECT ?,?,x.*,(SELECT a.applied_through_commit_ts FROM {o}apply_status a "
                "WHERE a.source_id=? AND a.schema_name||'.'||a.table_name=x.t "
                "AND a.applied_through_epoch=a.epoch AND a.applied_through_lsn=x.l "
                "AND a.applied_lsn=x.l) FROM (SELECT "
                "unnest(?) AS t,current_timestamp,unnest(?),unnest(?),unnest(?) AS l,"
                "unnest(?),unnest(?),unnest(?),unnest(?)) x",
                [config.source_id, run_id, config.source_id]
                + [list(column) for column in zip(*rows)],
            )
        else:
            ops.execute(
                "INSERT INTO {o}apply_runs (" + columns + ") SELECT ?,?," + unnested,
                [config.source_id, run_id] + [list(column) for column in zip(*rows)],
            )
        if prune_window():
            ops.execute(
                "DELETE FROM {o}apply_runs WHERE source_id=? "
                "AND written_at<current_timestamp-INTERVAL 30 DAY",
                [config.source_id],
            )
            if v2:
                # not_started rows this full snapshot no longer writes (the table started,
                # left apply's set, or was never a managed table: the heartbeat row).
                # Daily, with the history prune: no statement in the steady state.
                # Older than this snapshot's rows, not merely another run's: an overlapping
                # newer run's rows (newer as_of, kept by newer-wins) stay.
                ops.execute(
                    "DELETE FROM {o}apply_status WHERE source_id=? AND state='not_started' "
                    "AND as_of<(SELECT max(as_of) FROM {o}apply_status WHERE source_id=? "
                    "AND run_id=?)",
                    [config.source_id, config.source_id, run_id],
                )

    return ops.report(APPLY_V2 if v2 else APPLY, write, remaining)


def report_table(con, config, run_id, table, database="cdc_ops", *, format_version=2):
    """One table's apply_status row, refreshed by an action that changed its state
    (pause, resume, retry) so the Dive shows it before the next apply run. 2 MotherDuck
    statements with the ATTACH; best effort, never raises."""
    ops = Ops(con, config.database, config.source_id, database)
    return ops.report(
        APPLY_V2 if format_version == 2 else APPLY,
        lambda: apply_snapshot(ops, config, run_id, table, format_version=format_version),
    )


def report_backfill(
    con,
    config,
    run_id,
    totals=None,
    table=None,
    generation=None,
    status="done",
    error=None,
    step=None,
    remaining=None,
    database="cdc_ops",
    tls=None,
    format_version=2,
    units=None,
    fold=None,
    detail=None,
):
    """Every generation's progress (one statement; `totals`: {generation: chunks
    estimate}), then the run's row (one INSERT, also for a failed run): 3 MotherDuck
    statements with the ATTACH. Generation rows are kept (one per backfill)."""
    ops = Ops(con, config.database, config.source_id, database)
    log = f"{quoted(config.database)}.{quoted(config.source_id)}."

    def write():  # values built inside the guard
        entries = [{"key": k, "value": v} for k, v in (totals or {}).items()]
        v2 = format_version == 2
        snapshot(
            ops,
            "backfill_status",
            "generation_id",
            "source_id,generation_id,as_of,written_at,run_id,relation_id,table_name,mode,"
            "state,chunks,chunks_total,rows,scan_done,catchup_lsn,marker_lsn,created_at,"
            "updated_at" + (",pin_lsn,snapshot_lsn" if v2 else ""),
            "?,g.generation_id,epoch_us(current_timestamp),current_timestamp,?,g.relation_id,"
            "t.schema_name||'.'||t.table_name,g.mode,g.state,g.chunks,"
            "map_from_entries(?::STRUCT(key VARCHAR,value BIGINT)[])[g.generation_id],"
            "g.rows,g.scan_done,g.catchup_lsn,g.marker_lsn,g.created_at,g.updated_at"
            + (",g.pin_lsn,g.snapshot_lsn" if v2 else ""),
            f"{quoted(config.mirror_database)}._cdc.generations g LEFT JOIN {log}tables t "
            "USING (relation_id)",
            [config.source_id, run_id, entries],
            # The chunk estimate is known only to the run that started the scan; later
            # runs of the same backfill (generation) keep it.
            keep=("chunks_total",),
        )
        # `fold`: the run's Backfill.fold_stats (None: no Backfill was built).
        batches = [None, None, None]
        if fold is not None:
            batches = [
                fold.get("sql", 0),
                sum(n for k, n in fold.items() if k.startswith("fallback:")),
                sum(n for k, n in fold.items() if k.startswith("redo:")),
            ]
        ops.execute(
            "INSERT INTO {o}backfill_runs (source_id,run_id,written_at,table_name,"
            "generation_id,status,error,step,tls_mode,tls_ca,tls_in_use"
            + (
                ",format_version,units_applied,sql_batches,fallback_batches,redo_batches,detail"
                if v2
                else ""
            )
            + ") VALUES (?,?,current_timestamp,?,?,?,?,?,?,?,?"
            + (",?,?,?,?,?,?" if v2 else "")
            + ")",
            [config.source_id, run_id, table, generation, status, error, step]
            + [(tls or {}).get(c) for c in ("tls_mode", "tls_ca", "tls_in_use")]
            + ([2, units, *batches, detail] if v2 else []),
        )

    return ops.report(BACKFILL_V2 if format_version == 2 else BACKFILL, write, remaining)
