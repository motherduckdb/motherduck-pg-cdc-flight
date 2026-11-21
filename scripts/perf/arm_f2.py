"""The format-2 arm for ab.py: the product's public format-2 entry points only.

Imported by ab.py inside a rep child, after the archived product tree is first on
sys.path (ab.load_arm checks where `cdc_flight` came from). Never imported on its own.
"""

from collections import Counter

from cdc_flight import bootstrap
from cdc_flight.apply import Apply
from cdc_flight.apply_run import ApplyRun
from cdc_flight.config import ApplyConfig, IngestConfig
from cdc_flight.ingest import run_once
from cdc_flight.ingest_log2 import IngestLog2
from cdc_flight.spool2 import Spool
from cdc_flight.unit_store import UnitReader

FORMAT = 2
# Metadata rows that grow with the log (the "ledger"); payload tables are separate.
LEDGER_TABLES = ("units_v2", "chunks_v2", "descriptors_v2", "frame_partitions_v2")


def is_payload(table):
    return table.startswith("frames_v2_")


def hooks():
    """(owner, attribute, label). Labels starting "spool" make up spool CPU; "append" is
    the log append (on the pipelined worker); "merge" is apply's MERGE."""
    return [
        (Spool, "feed", "spool_feed"),
        (Spool, "_flush", "spool_flush"),
        (Spool, "poll", "spool_poll"),
        (Spool, "close", "spool_close"),
        (IngestLog2, "append", "append"),
        (IngestLog2, "finish", "finish"),
        (Apply, "_merge", "merge"),
        # The SQL fold's MERGE is the fold and the MERGE in one statement: its own label, so
        # "merge" stays the MERGE-only figure (the Python fold, older arms). Hooked where it
        # is defined, so backfill catch-up's SQL MERGE counts too (sql_path.SqlWrite; an
        # archived tree before sql_path defines it on ApplyRun).
        (sql_merge_owner(), "_sql_merge", "sql_merge"),
    ]


def sql_merge_owner():
    try:
        from cdc_flight.sql_path import SqlWrite
    except ImportError:
        return ApplyRun
    return SqlWrite


def open(con, config):  # noqa: A001 (the adapter interface's name)
    """A fresh store: explicit bootstrap (the binding observed from the slot session)."""
    return bootstrap.bootstrap_log(con, IngestConfig(**config), create_objects=True)


def run(log, info, observe):
    result = run_once(log.config, log, info=info, observe=observe)
    return dict(
        status=result.status,
        transactions=result.transactions,
        native=dict(format=FORMAT, units_or_chunks=result.chunks),
    )


def apply_setup(con, config, mirror, table):
    apply = ApplyRun(
        con,
        ApplyConfig(
            config["source_id"], (table,), database=config["database"], mirror_database=mirror
        ),
    )
    apply.initialize()
    apply.start_from_log(table)
    return apply


def apply_table(apply, table, run_id):
    result = apply.apply_table(table, run_id)
    return dict(status=result.status, events=result.events)


def _units(con, log):
    """Every complete unit of the current epoch, through the product's reader."""
    reader = UnitReader(con, log.config)
    head = reader.head()
    after = con.execute(
        f"SELECT start_lsn FROM {log.prefix}source_epochs WHERE epoch=?", [head.epoch]
    ).fetchone()[0]
    units = []
    while after < head.high:
        page = reader.units(head.epoch, after, head.high)
        if not page:
            break
        units.extend(page)
        after = page[-1].end
    return reader, head, units


def op_counts(con, log, oid):
    """Logical events of the measured table by op, and its distinct source txns."""
    reader, head, units = _units(con, log)
    ops, commits = Counter(), set()
    for start in range(0, len(units), 64):
        for event in reader.events(units[start : start + 64], oid, high=head.high):
            ops[event["op"]] += 1
            commits.add(event["commit_lsn"])
    return dict(ops), len(commits)


def log_check(con, log):
    """The product reader validates every unit's chain and frame counts; the logical
    event total must equal the units' recorded event counts."""
    try:
        reader, head, units = _units(con, log)
        logical = 0
        for start in range(0, len(units), 64):
            logical += sum(1 for _ in reader.events(units[start : start + 64], high=head.high))
        recorded = sum(u.event_count for u in units)
        return dict(
            ok=logical == recorded,
            units=len(units),
            frames=sum(u.frame_count for u in units),
            events=logical,
            recorded_events=recorded,
        )
    except Exception as exc:
        return dict(ok=False, error=type(exc).__name__)
