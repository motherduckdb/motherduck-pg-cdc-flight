"""Format 2 aliases, kept for one release. The format-2 consumer IS the public
ApplyRun now (apply.py / apply_run.py); new code constructs it directly."""

from cdc_flight.apply import V2_COLUMNS, Apply, with_v2_columns  # noqa: F401 (aliases)
from cdc_flight.apply_run import CACHE_CAP_BYTES, ApplyRun, UnitBatch  # noqa: F401 (aliases)
from cdc_flight.ingest_common import SourceMismatch

Apply2 = ApplyRun


def has_protocol_v2(con, config):
    return bool(
        con.execute(
            "SELECT count(*) FROM duckdb_tables() WHERE database_name=? AND schema_name=? "
            "AND table_name='protocol_v2'",
            [config.database, config.source_id],
        ).fetchone()[0]
    )


def consumer_for(con, config, *, retention_v2=None):
    """ApplyRun for a format-2 store; a store without protocol_v2 raises SourceMismatch
    (legacy layouts need recreation and re-backfill). `retention_v2`: an optional factory."""
    if not has_protocol_v2(con, config):
        raise SourceMismatch("unsupported source protocol; recreate and re-backfill")
    return ApplyRun(con, config, retention=retention_v2() if retention_v2 else None)
