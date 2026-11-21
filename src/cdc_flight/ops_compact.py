"""Compaction's operations status (see ops.py): the run, every pin's standing (the
expires-at estimate) and the retained log days, as of the compaction lease."""

import json

from cdc_flight.config import quoted
from cdc_flight.ops import NOW, Ops, Sql, to_json
from cdc_flight.ops_schema import COMPACTION as TABLES
from cdc_flight.ops_schema import COMPACTION_V2 as TABLES_V2

EXPIRATION_DAYS = 30  # must stay within compact.RECORD_DAYS (the records kept)

RUN = "run_id,status,error,days_dropped,batches_pruned,manifests_pruned,kept_day,"
RUN += "kept_reason,budget,expired,would_expire,skipped,limits"
# Format 2 only (Retention2.prune's (chunks, units)), created and written only by a
# format-2 result: batches_pruned/manifests_pruned stay v1's and are NULL on v2, never
# relabelled.
RUN_V2 = ("format_version", "chunks_pruned", "units_pruned")


def report_compaction(con, config, result, as_of, summary, remaining=None, database="cdc_ops"):
    """2 MotherDuck statements with the ATTACH. A busy run (no lease) writes only
    `last_busy_*`."""
    ops = Ops(con, config.database, config.source_id, database)

    v2 = getattr(result, "format_version", 1) == 2

    def write():  # values built inside the guard
        busy = as_of is None
        limits = dict(
            expire=config.expire,
            max_retained_lag_hours=config.max_retained_lag_hours,
            storage_budget_gb=config.storage_budget_gb,
        )
        values = dict(
            source_id=config.source_id,
            run_as_of=as_of,
            run_id=result.run_id,
            status=result.status,
            error=result.error,
            days_dropped=len(result.days),
            batches_pruned=None if v2 else result.batches,
            manifests_pruned=None if v2 else result.manifests,
            kept_day=result.kept_day,
            kept_reason=result.kept_reason,
            budget=result.budget,
            expired=result.expired,
            would_expire=result.would_expire,
            skipped=result.skipped,
            limits=json.dumps(limits),
            pins_as_of=as_of if summary else None,
            pins=to_json(summary.annotated_pins() if summary else ()),
            days_as_of=as_of if summary else None,
            # Read before this run's drops: leave out the days it dropped.
            days=to_json(
                d for d in (summary.days if summary else ()) if d["day"] not in result.days
            ),
            # The recorded expirations (the gaps they opened), read inside the write.
            expirations_as_of=None if busy else Sql("epoch_us(current_timestamp)"),
            expirations=Sql(
                "(SELECT to_json(coalesce(list({'v':1,'run_id':run_id,"
                "'expired_at':expired_at::VARCHAR,'owner':owner,'kind':kind,"
                "'relation_id':relation_id,'generation_id':generation_id,'epoch':source_epoch,"
                "'pinned_lsn':pinned_lsn,'reason':reason,'mode':mode,'outcome':outcome,"
                "'floor_epoch':floor_epoch,'floor_lsn':floor_lsn} ORDER BY expired_at DESC),[])) "
                f"FROM {quoted(config.database)}.{quoted(config.source_id)}.expirations WHERE "
                f"expired_at>current_timestamp-INTERVAL {EXPIRATION_DAYS} DAY)"
            ),
            last_busy_at=NOW if busy else None,
            last_busy_run_id=result.run_id if busy else None,
            **(
                dict(format_version=2, chunks_pruned=result.chunks, units_pruned=result.units)
                if v2
                else {}
            ),
        )
        parts = {
            "run_as_of": RUN.split(",") + (list(RUN_V2) if v2 else []),
            "pins_as_of": ("pins",),
            "days_as_of": ("days",),
            "expirations_as_of": ("expirations",),
        }
        markers = (("last_busy_run_id", ("last_busy_at", "last_busy_run_id")),)
        ops.upsert("compaction_status", values, parts, markers)

    return ops.report(TABLES_V2 if v2 else TABLES, write, remaining)
