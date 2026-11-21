"""Which tables an apply run covers (the apply command only, so backfill, which
imports apply.py, doesn't import it).

The registry is the table set (Phase 3, U4): with CDC_TABLES unset, a run covers every
registered table; set, exactly its tables, and a registered table it leaves out is
reported `not_configured`.
"""

from cdc_flight.apply import TableResult
from cdc_flight.ingest_common import managed

OMITTED = (
    "registered but not in CDC_TABLES: not applied, and its pin holds the log; "
    "add it to CDC_TABLES or unset CDC_TABLES"
)


def registered(apply):
    """[(name, live)] for every registry name apply covers when CDC_TABLES is unset,
    sorted by name.

    Unmanaged members (logged, never applied) and removed rows are left out, so a
    removed-only name is gone and a drop/recreate pair is one name (its active row, as
    BY_NAME resolves it). The registered heartbeat is excluded (the registration's
    binding names it). Held tables stay, reporting source_missing; `live` is False for
    them."""
    return [
        (f"{schema}.{name}", live)
        for schema, name, live in apply.execute(
            "SELECT schema_name,table_name,bool_or(held_reason IS NULL) FROM {s}tables t "
            "WHERE " + managed("t", "{s}") + " GROUP BY ALL ORDER BY ALL"
        ).fetchall()
    ]


def run(apply, on_table=None, steps=None, *, fault=None, run_id=None):
    """Apply.run over the table set: CDC_TABLES if set, else every registered table.
    `steps` (the Flight Explainer run) gets the `omitted` lines; see apply.mmd."""
    names = registered(apply)
    tables = apply.config.tables or tuple(name for name, _ in names)
    results = apply.run(fault=fault, on_table=on_table, tables=tables, run_id=run_id)
    # Registered, not held, and left out by CDC_TABLES: captured but never applied, so
    # its pin holds the log. Reported (a warning, never a failure). No boundary: the
    # run never reads the table.
    for table in (name for name, live in names if live and name not in tables):
        results.append(TableResult(table, "not_configured", None, detail=OMITTED))
        if steps is not None:
            # Never touched, so the table line carries no effect.
            steps.step("table", "ok", key=table, rows=0)
            steps.step("state", "ok", key=table, branch="not_configured")
            steps.step("omitted", "ok", key=table)
    return results
