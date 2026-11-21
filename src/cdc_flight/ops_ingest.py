"""Ingest's and preflight's operations status (see ops.py)."""

import json
import logging
import uuid

from cdc_flight import source_label
from cdc_flight.config import quoted
from cdc_flight.ops import NOW, Ops, Sql, commit_time_sql, prune_window, to_json
from cdc_flight.ops_schema import INGEST, SOURCES
from cdc_flight.ops_schema import INGEST_V2 as TABLES_V2
from cdc_flight.ops_schema import PREFLIGHT as PREFLIGHT

logger = logging.getLogger(__name__)

TABLES = INGEST  # Retain the writer’s legacy DDL import for existing callers.

# Format 2 only (IngestLog2.unit_position(), read by the caller before this report):
# written on every report (format 2 only); format_version is 2 when absent.
UNIT_POSITION = (
    "format_version",
    "confirmed_end",
    "open_unit_anchor",
    "partial",
    "open_chunks",
    "durable_frame_end",
    "config_revision",
)
# A v2 run that kept durable progress but stopped on a compaction conflict (status
# "conflict": leased, not a success, not busy); repeated ones show here.
CONFLICT = ("last_conflict_at", "last_conflict_run_id")
# The registry's held tables (reason, detail, first seen).
HOLDS = (
    "(SELECT to_json(coalesce(list({{'v':1,'relation_id':relation_id,"
    "'table':schema_name||'.'||table_name,'reason':held_reason,'detail':held_detail,"
    "'held_since':held_since::VARCHAR}}),[])) FROM {log}tables WHERE held_reason IS NOT NULL)"
)
# Format 2 also lists tables whose key change is queued behind an open unit: apply stops
# at the frozen cut while the table still reads live, so this is the operator's signal.
# The queue is read in the same statement; it never sets held_reason.
PENDING_HOLDS = (
    "(SELECT to_json(coalesce(list(h ORDER BY h.relation_id),[])) FROM ("
    "SELECT {{'v':1,'relation_id':relation_id,'table':schema_name||'.'||table_name,"
    "'reason':held_reason,'detail':held_detail,'held_since':held_since::VARCHAR}} AS h,"
    "relation_id FROM {log}tables WHERE held_reason IS NOT NULL UNION ALL "
    "SELECT {{'v':1,'relation_id':relation_id,'table':schema_name||'.'||table_name,"
    "'reason':'key_change_pending','pending_count':json_array_length(pending_keys_v2),"
    "'frozen_cut':json_extract(pending_keys_v2,'$[0].cut_lsn')::UBIGINT,"
    "'first_observed_lsn':json_extract(pending_keys_v2,'$[0].observed_lsn')::UBIGINT}} AS h,"
    "relation_id FROM {log}tables WHERE pending_keys_v2 IS NOT NULL))"
)
STATUS = "run_id,status,step,error,epoch,checkpoint_lsn,transactions,chunks,duration_s"
FAILURE = "last_failure_at,last_failure_run_id,last_failure_step,last_failure_error"
SUCCESS = ("drained", "capped")
TLS = ("tls_mode", "tls_ca", "tls_in_use")
THROUGH = ("durable_through_commit_ts", "durable_through_epoch", "durable_through_lsn")


def durable_through(observed):
    """(epoch, LSN, commit_ts_us) when the run knows the commit time at its resulting
    checkpoint (ingest.run_once's observe["durable_through"]), else None: exact or nothing."""
    through = observed.get("durable_through")
    if not through or through[2] is None:
        return None
    epoch, lsn, ts = through
    checkpoint = observed.get("checkpoint")
    if epoch != observed.get("epoch") or lsn != checkpoint:
        return None
    if observed.get("confirmed_end", lsn) != lsn:
        return None
    return through


def report_ingest(con, config, observed, remaining=None, database="cdc_ops"):
    """`observed`: what the run read and did (ingest.run_once's `observe`) plus the
    CLI's status, error, step and duration. 4 MotherDuck statements in steady state (5
    on a pruning run): ATTACH, the status upsert (the registry's holds read inside it),
    the run's history row and the source label's read; one more, the label's write, only
    when the published label changed (source_label.publish; a failure there is logged
    and never undoes the rest)."""
    ops = Ops(con, config.database, config.source_id, database)

    def write():
        # Built here, inside Ops.report's guard: a bad value never fails the run.
        status, as_of = observed.get("status"), observed.get("as_of")
        # A run refused before run_once assigned an id still gets its failure recorded.
        run_id = observed.get("run_id") or "unassigned-" + uuid.uuid4().hex[:12]
        busy, failed = status == "busy", bool(observed.get("error"))
        v2 = UNIT_POSITION  # format 2 only: the unit position and conflict markers
        log = f"{quoted(config.database)}.{quoted(config.source_id)}."
        leased = as_of is not None and not busy
        ok = leased and status in SUCCESS
        through = durable_through(observed) if leased else None
        when = Sql(commit_time_sql(int(through[2]))) if through else None
        values = dict(
            source_id=config.source_id,
            status_as_of=as_of if leased else None,
            run_id=run_id,
            **{c: observed.get(c) for c in ("status", "step", "error", "epoch")},
            checkpoint_lsn=observed.get("checkpoint"),
            **{c: observed.get(c) for c in ("transactions", "chunks", "duration_s")},
            last_success_as_of=as_of if ok else None,
            last_success_run_id=run_id if ok else None,
            slots_as_of=Sql("epoch_us(current_timestamp)")
            if observed.get("slots_read_now")
            else as_of
            if "slots" in observed
            else None,
            slots=to_json(observed.get("slots", ())),
            holds_as_of=Sql("epoch_us(current_timestamp)"),
            holds=Sql(PENDING_HOLDS.format(log=log)),
            last_busy_at=NOW if busy else None,
            last_busy_run_id=run_id if busy else None,
            last_failure_at=NOW if failed else None,
            last_failure_run_id=run_id if failed else None,
            last_failure_step=observed.get("step") if failed else None,
            last_failure_error=observed.get("error") if failed else None,
            lifecycle_as_of=Sql("epoch_us(current_timestamp)") if "lifecycle" in observed else None,
            lifecycle=observed.get("lifecycle"),
            **{c: observed.get(c, 2 if c == "format_version" else None) for c in v2},
            # Its own part: a run that doesn't know it keeps the stored triple, whose pair
            # then no longer matches a moved checkpoint (shown as unknown, never stale).
            durable_through_as_of=as_of if through else None,
            durable_through_commit_ts=when,
            durable_through_epoch=through[0] if through else None,
            durable_through_lsn=through[1] if through else None,
            **(
                dict(
                    last_conflict_at=NOW if status == "conflict" else None,
                    last_conflict_run_id=run_id if status == "conflict" else None,
                )
                if v2
                else {}
            ),
        )
        parts = {
            "status_as_of": STATUS.split(",") + list(v2),
            "slots_as_of": ("slots",),
            "holds_as_of": ("holds",),
            "last_success_as_of": ("last_success_run_id",),
            "lifecycle_as_of": ("lifecycle",),
            "durable_through_as_of": THROUGH,
        }
        markers = (
            ("last_busy_run_id", ("last_busy_at", "last_busy_run_id")),
            ("last_failure_run_id", FAILURE.split(",")),
        ) + ((("last_conflict_run_id", CONFLICT),) if v2 else ())

        standby = observed.get("standby")
        if standby is not None:  # a read replica source only: primary statements unchanged
            # One JSON object, versioned like the slot records (docs/read-replica.md).
            standby = json.dumps(dict(v=1, **standby), separators=(",", ":"), default=str)
            values.update(standby_as_of=as_of, standby=standby)
            parts["standby_as_of"] = ("standby",)
        ops.upsert("ingest_status", values, parts, markers)
        if busy:
            return
        slot = next((s for s in observed.get("slots", ()) if s.get("is_current")), {})
        ops.execute(
            "INSERT INTO {o}ingest_runs (source_id,run_id,as_of,written_at,status,transactions,"
            "chunks,duration_s,retained_bytes,lag_bytes,tls_mode,tls_ca,tls_in_use"
            + "".join("," + c for c in v2)
            + ",cleanup_release_attempts,epoch,durable_through_commit_ts"
            + (",standby" if standby is not None else "")
            + ") VALUES (?,?,?,current_timestamp,?,?,?,?,?,?,?,?,?"
            + ",?" * len(v2)
            + ",?,?,"
            + (when.expression if when else "NULL")
            + (",?" if standby is not None else "")
            + ")",
            [config.source_id, run_id, as_of, status]
            + [observed.get(c) for c in ("transactions", "chunks", "duration_s")]
            + [slot.get("retained_bytes"), slot.get("lag_bytes")]
            + [observed.get(c) for c in TLS]
            + [observed.get(c, 2 if c == "format_version" else None) for c in v2]
            + [observed.get("cleanup_release_attempts"), observed.get("epoch")]
            + ([standby] if standby is not None else []),
        )
        if prune_window():
            ops.execute(
                "DELETE FROM {o}ingest_runs WHERE source_id=? "
                "AND written_at<current_timestamp-INTERVAL 7 DAY",
                [config.source_id],
            )
        try:
            source_label.publish(
                ops, config.database, config.source_id, observed.get("label_guard")
            )
        except Exception as exc:  # best effort: the published row stays as it was
            logger.warning("source label publish failed (%s)", type(exc).__name__)

    return ops.report({**TABLES_V2, **SOURCES}, write, remaining)


def report_preflight(
    con, config, run_id, status, error=None, warnings=(), remaining=None, database="cdc_ops"
):
    ops = Ops(con, config.database, config.source_id, database)

    def write():  # values built inside the guard
        values = dict(
            source_id=config.source_id,
            as_of=Sql("epoch_us(current_timestamp)"),
            run_id=run_id,
            status=status,
            error=error,
            warnings=to_json({"warning": w} for w in warnings),
        )
        parts = {"as_of": ("run_id", "status", "error", "warnings")}
        ops.upsert("preflight_status", values, parts)

    return ops.report(PREFLIGHT, write, remaining)
