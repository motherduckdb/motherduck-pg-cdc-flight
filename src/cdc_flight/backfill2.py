"""Format 2 aliases, kept for one release. The format-2 backfill IS the public
Backfill now (backfill.py); new code constructs it directly."""

from cdc_flight.apply2 import has_protocol_v2
from cdc_flight.backfill import MARKER_COLUMN, Backfill  # noqa: F401 (aliases)
from cdc_flight.ingest_common import SourceMismatch

Backfill2 = Backfill


def backfill_for(con, config, pg=None, *, retention_v2=None):
    """Backfill for a format-2 store; a store without protocol_v2 raises SourceMismatch."""
    if not has_protocol_v2(con, config):
        raise SourceMismatch("unsupported source protocol; recreate and re-backfill")
    return Backfill(con, config, pg, retention=retention_v2() if retention_v2 else None)
