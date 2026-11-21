"""Shared operations DDL: each writer re-exports its shapes; bootstrap creates all
missing tables without importing other writers. No store data or runtime imports.
"""


def extended(tables, added):
    """`tables` with format-2-only columns appended ({table: "col TYPE,..."}). Only a
    format-2 writer passes these to Ops.report (every writer since the format-2 cutover).
    A report adds a missing column to its own tables; the base shapes are those of
    the pre-cutover tables."""
    out = dict(tables)
    for table, columns in added.items():
        ddl = out[table]
        head, sep, key = ddl.partition(",PRIMARY KEY(")
        out[table] = head + "," + columns + (sep + key if sep else "")
    return out


AUDIT = {
    "audit": "source_id VARCHAR,run_id VARCHAR,written_at TIMESTAMPTZ,token_user VARCHAR,"
    "action VARCHAR,inputs VARCHAR,dry_run BOOLEAN,result VARCHAR,"
    # Who started the run (MD_LIST_FLIGHT_RUNS.started_by_name), when the run could tell.
    "started_by VARCHAR,"
    # PostgreSQL TLS (pg_ca.fields). NULL means a pre-TLS build OR an action that never
    # connects to PostgreSQL: read a source's TLS state from ingest_runs, never from here.
    "tls_mode VARCHAR,tls_ca VARCHAR,tls_in_use BOOLEAN,"
    # The caller's CDC_REQUEST_ID (validated; NULL when not given). A run refused for an
    # invalid input exits before any connection and writes no audit row at all.
    "request_id VARCHAR"
}


INGEST = {
    "ingest_status": "source_id VARCHAR PRIMARY KEY,written_at TIMESTAMPTZ,"
    "status_as_of BIGINT,run_id VARCHAR,status VARCHAR,step VARCHAR,error VARCHAR,"
    "epoch INTEGER,checkpoint_lsn UBIGINT,transactions BIGINT,chunks BIGINT,"
    "duration_s DOUBLE,last_success_as_of BIGINT,last_success_run_id VARCHAR,"
    "slots_as_of BIGINT,slots VARCHAR,holds_as_of BIGINT,holds VARCHAR,"
    "last_busy_at TIMESTAMPTZ,last_busy_run_id VARCHAR,last_failure_at TIMESTAMPTZ,"
    "last_failure_run_id VARCHAR,last_failure_step VARCHAR,last_failure_error VARCHAR,"
    # A deleted or deleting source, as a failed run saw it (its slot name is a
    # tombstone, so ingest.observe_after_failure reads nothing and reports this).
    "lifecycle_as_of BIGINT,lifecycle VARCHAR",
    "ingest_runs": "source_id VARCHAR,run_id VARCHAR,as_of BIGINT,written_at TIMESTAMPTZ,"
    "status VARCHAR,transactions BIGINT,chunks BIGINT,duration_s DOUBLE,"
    "retained_bytes BIGINT,lag_bytes BIGINT,"
    # PostgreSQL TLS as the run asked for and got (pg_ca.fields): run rows only, never a
    # status upsert, so an older build leaves NULL (unknown) instead of a stale value.
    "tls_mode VARCHAR,tls_ca VARCHAR,tls_in_use BOOLEAN",
}


_UNIT_DDL = (
    "format_version INTEGER,confirmed_end UBIGINT,open_unit_anchor UBIGINT,partial BOOLEAN,"
    "open_chunks BIGINT,durable_frame_end UBIGINT,config_revision UBIGINT"
)


INGEST_V2 = extended(
    INGEST,
    {
        # "Current as of" (source clock): the commit time of the last commit of the unit
        # ending at the durable checkpoint, with the (epoch, LSN) it belongs to. It is
        # current only while that pair equals (epoch, checkpoint_lsn): an older writer
        # moves the checkpoint without it, so a stale time never reads as current.
        "ingest_status": _UNIT_DDL + ",last_conflict_at TIMESTAMPTZ,last_conflict_run_id VARCHAR,"
        "durable_through_as_of BIGINT,durable_through_commit_ts TIMESTAMPTZ,"
        "durable_through_epoch INTEGER,durable_through_lsn UBIGINT,"
        # A read replica's diagnostics (docs/read-replica.md); NULL for a primary.
        "standby_as_of BIGINT,standby VARCHAR",
        # The run's own time (NULL when unknown) belongs to its (epoch, confirmed_end).
        "ingest_runs": _UNIT_DDL + ",cleanup_release_attempts INTEGER,epoch INTEGER,"
        "durable_through_commit_ts TIMESTAMPTZ,standby VARCHAR",
    },
)


PREFLIGHT = {
    "preflight_status": "source_id VARCHAR PRIMARY KEY,written_at TIMESTAMPTZ,as_of BIGINT,"
    "run_id VARCHAR,status VARCHAR,error VARCHAR,warnings VARCHAR"
}


APPLY = {
    "apply_status": "source_id VARCHAR,relation_id UINTEGER,as_of BIGINT,written_at TIMESTAMPTZ,"
    "run_id VARCHAR,schema_name VARCHAR,table_name VARCHAR,state VARCHAR,"
    "held_since TIMESTAMPTZ,detail VARCHAR,blocked_commit_lsn UBIGINT,epoch INTEGER,"
    "applied_lsn UBIGINT,generation_id VARCHAR,key_version INTEGER,held_reason VARCHAR,"
    "pin_lsn UBIGINT,pin_updated_at TIMESTAMPTZ,quarantine_count INTEGER,quarantines VARCHAR,"
    "PRIMARY KEY(source_id,relation_id)",
    # One row per table per run, plus the run's own row (table_name NULL): its status,
    # and for a failed run the exception type and the step.
    "apply_runs": "source_id VARCHAR,run_id VARCHAR,table_name VARCHAR,"
    "written_at TIMESTAMPTZ,status VARCHAR,events BIGINT,applied_lsn UBIGINT,"
    "error VARCHAR,step VARCHAR",
}


BACKFILL = {
    "backfill_status": "source_id VARCHAR,generation_id VARCHAR,as_of BIGINT,"
    "written_at TIMESTAMPTZ,run_id VARCHAR,relation_id UINTEGER,table_name VARCHAR,"
    "mode VARCHAR,state VARCHAR,chunks BIGINT,chunks_total BIGINT,rows BIGINT,"
    "scan_done BOOLEAN,catchup_lsn UBIGINT,marker_lsn UBIGINT,created_at TIMESTAMPTZ,"
    "updated_at TIMESTAMPTZ,PRIMARY KEY(source_id,generation_id)",
    "backfill_runs": "source_id VARCHAR,run_id VARCHAR,written_at TIMESTAMPTZ,"
    "table_name VARCHAR,generation_id VARCHAR,status VARCHAR,error VARCHAR,step VARCHAR,"
    # PostgreSQL TLS as the run asked for and got (pg_ca.fields): run rows only, never a
    # status upsert, so an older build leaves NULL (unknown) instead of a stale value.
    "tls_mode VARCHAR,tls_ca VARCHAR,tls_in_use BOOLEAN",
}


APPLY_V2 = extended(
    APPLY,
    {
        # The held unit's end too: the Dive builds quarantine's UNIT confirmation from it.
        "apply_status": "format_version INTEGER,blocked_unit_start_lsn UBIGINT,"
        "blocked_unit_end_lsn UBIGINT,"
        # The same "current as of" for the table's checkpoint: current only while
        # (applied_through_epoch, applied_through_lsn) equals (epoch, applied_lsn).
        "applied_through_commit_ts TIMESTAMPTZ,applied_through_epoch INTEGER,"
        "applied_through_lsn UBIGINT,"
        # Keyless append (insert-only) capture: NULL capture_mode is keyed. Under
        # ignore_deletes, deletes_ignored counts the skipped DELETE events; a TRUNCATE
        # empties the mirror in place (last_truncate_lsn, its commit).
        "capture_mode VARCHAR,ignore_deletes BOOLEAN,deletes_ignored UBIGINT,"
        "last_truncate_lsn UBIGINT,"
        # Why the table is held, as a stable code (hold_codes.HOLD_CODES; operations.md).
        # NULL: live, or held with no current code (an older writer): unknown.
        "hold_code VARCHAR",
        # Per table: the time at the row's applied_lsn (NULL when unknown or the run's row).
        "apply_runs": "format_version INTEGER,units_applied BIGINT,"
        "applied_through_commit_ts TIMESTAMPTZ",
    },
)


BACKFILL_V2 = extended(
    BACKFILL,
    {
        # Catch-up batches this run folded in SQL, on the Python fold (any reason), and
        # SQL batches redone on the Python fold after the MERGE refused them: run rows
        # only, so an older build leaves NULL.
        "backfill_runs": "format_version INTEGER,units_applied BIGINT,"
        "sql_batches BIGINT,fallback_batches BIGINT,redo_batches BIGINT,"
        "detail VARCHAR",
        "backfill_status": "pin_lsn UBIGINT,snapshot_lsn UBIGINT",
    },
)


COMPACTION = {
    "compaction_status": "source_id VARCHAR PRIMARY KEY,written_at TIMESTAMPTZ,"
    "run_as_of BIGINT,run_id VARCHAR,status VARCHAR,error VARCHAR,days_dropped INTEGER,"
    "batches_pruned BIGINT,manifests_pruned BIGINT,kept_day VARCHAR,kept_reason VARCHAR,"
    "budget VARCHAR,expired INTEGER,would_expire INTEGER,skipped INTEGER,limits VARCHAR,"
    "pins_as_of BIGINT,pins VARCHAR,days_as_of BIGINT,days VARCHAR,"
    "expirations_as_of BIGINT,expirations VARCHAR,last_busy_at TIMESTAMPTZ,last_busy_run_id VARCHAR"
}


COMPACTION_V2 = extended(
    COMPACTION,
    {"compaction_status": "format_version INTEGER,chunks_pruned BIGINT,units_pruned BIGINT"},
)


# One row per source, written only by ingest (source_label.publish): the owner-set
# label (NULL when unset or withheld), why it is withheld, and the token's user.
SOURCES = {
    "sources": "source_id VARCHAR PRIMARY KEY,written_at TIMESTAMPTZ,label VARCHAR,"
    "description VARCHAR,label_revision BIGINT,label_withheld VARCHAR,owner_user VARCHAR"
}


TABLES_V2 = {
    **INGEST_V2,
    **SOURCES,
    **PREFLIGHT,
    **APPLY_V2,
    **BACKFILL_V2,
    **COMPACTION_V2,
    **AUDIT,
}
