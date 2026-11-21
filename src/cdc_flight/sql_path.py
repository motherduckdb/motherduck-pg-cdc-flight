"""The SQL fold's per-table pieces, apart from the apply loop (apply_run.py).

sql_fold.py builds the statements; this module holds what runs them for one table: the
batch and MERGE-source types, the decision whether a page's facts allow the SQL fold, and
the write (guard, create or ALTER ADD COLUMN, MERGE, the caller's checkpoint). It is kept
out of apply_run.py so that a command other than apply can use the SQL fold without
importing the apply loop.
"""

import json

import duckdb

from cdc_flight import sql_fold
from cdc_flight.apply import _DryRun
from cdc_flight.config import quoted
from cdc_flight.mirror import Blocked, reconcile

# C4: a page's frames are read client-side in one piece only when its charged wire
# bytes (the unit ledger's bytes, known before any frame is read) are at most this:
# apply's run cache of a page's frames, and backfill's redo of a page the SQL MERGE
# refused. Validated by the C4b gate: the peak RSS delta of the cache at a page this size
# was 737 MiB against a 1 GiB budget (5.8x the ledger bytes). apply_run reads it as its
# own module global (tests patch apply_run.CACHE_CAP_BYTES); backfill reads it here.
CACHE_CAP_BYTES = 128 * 1024 * 1024


class SqlBatch:
    """One table's page on the SQL path: the units, the frames' SQL and, when the page has
    events for the table, the layout to fold them with. Counts come from the plan."""

    def __init__(self, page, high, frames, events, commits, columns, layout):
        self.page, self.high, self.frames = page, high, frames
        self.events, self.commits, self.columns, self.layout = events, commits, columns, layout


class PageFacts:
    """One relation's immutable facts on one page (the plan's row): never table state."""

    __slots__ = (
        "events",
        "commits",
        "layouts",
        "block_at",
        "block_reason",
        "change_at",
        "key_changes",
    )

    def __init__(self, events, commits, layouts, block_at, block_reason, change_at, key_changes):
        self.events, self.commits, self.layouts = events, commits, layouts or []
        self.block_at, self.block_reason = block_at, block_reason
        self.change_at, self.key_changes = change_at, key_changes


class SqlImages:
    """`images` for a SQL batch: the MERGE source; falsy when the table has no events."""

    def __init__(self, batch, relation_id):
        self.batch, self.relation_id = batch, relation_id

    def __bool__(self):
        return self.batch.events > 0

    def source(self):
        return sql_fold.source_sql(self.batch.layout, self.relation_id, self.batch.frames)


def versions_of(descriptors, relation_id):
    """sql_fold.plan_sql's `versions` for one relation: {version: (column count, key
    positions)}. None: one version under two key layouts (the Python fold decides)."""
    versions = {}
    for (oid, _, _), desc in descriptors.items():
        if oid != relation_id:
            continue
        if desc.version in versions:
            return None
        versions[desc.version] = (
            len(desc.names),
            tuple(desc.names.index(k) + 1 for k in desc.keys),
        )
    return versions


def decide(relation_id, facts, descriptors, columns, keys):
    """Whether one table's page folds in SQL, from the page's facts and the table's own
    mirror `columns` and `keys` (read under its lease).

    Returns (reason, columns, layout): a fallback `reason` (the Python fold takes the
    batch, and raises or holds as it always has), or reason None with the reconciled
    columns and the layout to fold with. No events: (None, columns, None), where columns
    is the caller's own object, returned as is (never copied or reconciled)."""
    if facts.block_at is not None:
        return facts.block_reason, None, None
    if facts.key_changes:
        return "key_change", None, None
    if not facts.events:
        return None, columns, None
    if len(facts.layouts) != 1:
        return "layouts", None, None
    version, key_hash = facts.layouts[0]
    desc = descriptors.get((relation_id, version, key_hash))
    if desc is None:
        raise ValueError("format-2 event references an unknown descriptor")
    if tuple(desc.keys) != tuple(keys):
        return "key_layout", None, None  # the Python path holds needs_resync
    try:
        merged = reconcile(columns, desc.json, keys)
    except Blocked:
        return "reconcile", None, None  # the Python path holds with its commit
    layout = sql_fold.Layout(
        tuple(merged),
        tuple(keys),
        tuple(desc.names),
        json.loads(desc.json)["identity"] == "f",
    )
    return None, merged, layout


class SqlWrite:
    """A mixin before Apply: `_write` takes a SqlImages batch through the SQL fold's MERGE,
    and every other batch through Apply._write unchanged."""

    def _sql_merge(self, statement):
        """The SQL fold's MERGE. Only its own precheck clause becomes the hold (the Python
        fold's detail); any other constraint failure is a data error (poison isolation)."""
        try:
            self.execute(statement)
        except duckdb.ConstraintException as exc:
            if sql_fold.BASELINE_ERROR not in str(exc):
                raise
            raise Blocked(
                "needs_action",
                "update of a row absent from the mirror lacks a full image",
                code="missing_baseline",
            ) from None

    def _write(self, current, columns, images, *, guard, commit_state, fault=None, dry_run=False):
        """Apply._write, with the SQL batch's MERGE in place of stage + _merge: the same
        transaction (guard, create or ALTER ADD COLUMN, MERGE, the caller's checkpoint)."""
        if not isinstance(images, SqlImages):
            return super()._write(
                current,
                columns,
                images,
                guard=guard,
                commit_state=commit_state,
                fault=fault,
                dry_run=dry_run,
            )
        target = current["target"]
        with self.transaction():
            guard()
            if columns and current["columns"] is None:
                self._create(current, columns)
            elif columns:
                for column in columns[len(current["columns"]) :]:
                    self.execute(
                        f"ALTER TABLE {target} ADD COLUMN {quoted(column.name)} {column.dtype}"
                    )
            if images:
                self._sql_merge(
                    sql_fold.merge_sql(target, current["keys"], columns, images.source())
                )
            if dry_run:
                raise _DryRun()
            if fault:
                fault("after_merge")
            commit_state()
        if fault:
            fault("after_commit")
