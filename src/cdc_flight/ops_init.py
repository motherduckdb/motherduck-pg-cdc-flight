"""Explicit operations schema initialization, using the writers' shared DDL.

Normal reports create every missing ops table within their best-effort guard.
This explicit helper also heals missing columns in all tables and requires the
ops database to be attached; it writes no status rows.
"""

from cdc_flight.ops import DATABASE, Ops
from cdc_flight.ops_schema import TABLES_V2 as TABLES_V2


class OpsUnavailable(RuntimeError):
    """cdc_ops could not be attached (a local store, or the database is missing)."""


def initialize_v2(con, store, source_id, database=DATABASE):
    """Create or extend every cdc_ops table (columns are only ever added); no status rows.
    Raises OpsUnavailable unless cdc_ops is attached. Idempotent."""
    ops = Ops(con, store, source_id, database)
    attached = ops._attach()
    if attached != "attached":
        raise OpsUnavailable(f"cdc_ops not attached ({attached})")
    ops._schema(TABLES_V2)
    return sorted(TABLES_V2)
