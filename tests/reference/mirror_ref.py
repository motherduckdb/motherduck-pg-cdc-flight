# FROZEN reference copy of src/cdc_flight/mirror.py at 0863bd1. Never edit.
# Test-only oracle for tests/test_mirror2.py (format 2's fold adapter); its sha256 is pinned there.
"""Typed mirror contract: type mapping, schema reconciliation and ordered folding.

Pure functions only; database effects live in apply.py. Values stay lossless
PostgreSQL text until the MERGE casts them, so a cast failure blocks the table
inside the database transaction instead of corrupting a row.
"""

import json
from dataclasses import dataclass, field

RESERVED_PREFIX = "_cdc_"

# Built-in type OIDs. Anything else (enums, domains, arrays, ranges, extension
# types) is kept as its exact text form.
_INTEGER = {20, 21, 23, 26}  # int8, int2, int4, oid
_FLOAT = {700, 701}
_TEXT_EXACT = {25, 1043, 1042, 18, 19, 2950, 1186, 869, 650, 829, 774, 790, 1266}


def duck_type(type_oid, typmod):
    """Map a pgoutput column to the mirror's DuckDB type (DESIGN type table)."""
    if type_oid == 16:
        return "BOOLEAN"
    if type_oid in _INTEGER:
        return "BIGINT"
    if type_oid in _FLOAT:
        return "DOUBLE"
    if type_oid == 1700:
        if typmod >= 4:
            precision = ((typmod - 4) >> 16) & 0xFFFF
            scale = (typmod - 4) & 0xFFFF
            if 1 <= precision <= 38 and 0 <= scale <= precision:
                return f"DECIMAL({precision},{scale})"
        # Unconstrained or out-of-range numeric: exact text, never a lossy decimal.
        return "VARCHAR"
    if type_oid == 1082:
        return "DATE"
    if type_oid == 1083:
        return "TIME"
    if type_oid == 1114:
        return "TIMESTAMP"
    if type_oid == 1184:
        return "TIMESTAMPTZ"
    if type_oid in (114, 3802):
        return "JSON"
    if type_oid == 17:
        return "BLOB"
    return "VARCHAR"


def cast_sql(expression, dtype):
    """SQL converting a staged PostgreSQL text value to the mirror type; errors are loud."""
    if dtype == "VARCHAR":
        return expression
    if dtype == "BLOB":
        # bytea_output=hex is checked by preflight; never guess the escape format.
        return (
            f"CASE WHEN {expression} IS NULL THEN NULL WHEN left({expression},2)='\\x' "
            f"THEN from_hex(substr({expression},3)) "
            f"ELSE error('bytea value is not in hex output format') END"
        )
    return f"CAST({expression} AS {dtype})"


@dataclass(frozen=True)
class Column:
    name: str
    dtype: str
    type_oid: int
    typmod: int


def descriptor_columns(descriptor):
    """Columns of a stored Relation descriptor (Phase 1 schema_versions JSON)."""
    value = json.loads(descriptor) if isinstance(descriptor, str) else descriptor
    return [
        Column(name, duck_type(type_oid, typmod), type_oid, typmod)
        for _flags, name, type_oid, typmod in value["columns"]
    ]


class Blocked(Exception):
    """A change that apply must not guess about; the table moves to a held state."""

    def __init__(self, state, reason, commit_lsn=None):
        super().__init__(reason)
        self.state, self.reason, self.commit_lsn = state, reason, commit_lsn


def reconcile(columns, descriptor, keys):
    """Return the mirror columns after accepting descriptor, or raise Blocked.

    `columns` is the current mirror column list, or None before the mirror exists.
    Supported: identical columns, and new columns appended. Anything else needs
    an operator: a missing column (drop/rename), a changed mirror type, or a
    changed replica identity.
    """
    value = json.loads(descriptor) if isinstance(descriptor, str) else descriptor
    incoming = descriptor_columns(value)
    names = [column.name for column in incoming]
    if len(set(names)) != len(names):
        raise Blocked("needs_action", "duplicate column names in relation descriptor")
    for name in names:
        if name.startswith(RESERVED_PREFIX):
            raise Blocked("needs_action", f"source column uses reserved prefix {RESERVED_PREFIX}")
    missing_keys = [key for key in keys if key not in names]
    if missing_keys:
        raise Blocked("needs_action", "replica identity column missing from relation")
    identity = value["identity"]
    if identity == "n":
        raise Blocked("needs_action", "replica identity NOTHING cannot be applied")
    if identity in ("d", "i"):
        flagged = {name for flags, name, _oid, _mod in value["columns"] if flags & 1}
        if flagged != set(keys):
            raise Blocked("needs_action", "replica identity columns differ from registered key")
    if columns is None:
        return incoming
    by_name = {column.name: column for column in incoming}
    for column in columns:
        other = by_name.get(column.name)
        if other is None:
            raise Blocked("needs_action", f"column {column.name!r} dropped or renamed")
        if other.dtype != column.dtype:
            raise Blocked("needs_action", f"column {column.name!r} changed type")
    known = {column.name for column in columns}
    # Mirror order is append-only; the descriptor's relative order is irrelevant.
    return list(columns) + [column for column in incoming if column.name not in known]


@dataclass
class Image:
    """Latest state of one key after folding events in commit order.

    Absent `values` entries are unknown and must come from the existing mirror
    row; `complete` images can be inserted when the mirror lacks the key.
    """

    op: str  # upsert | delete
    values: dict = field(default_factory=dict)
    complete: bool = False
    commit_lsn: int = 0
    commit_ts_us: int = 0


def fold(state, events, keys, mirror_names):
    """Fold events (already in commit/ordinal order) into per-key images.

    Each event is a dict with commit_lsn, commit_ts_us, op, full_old (the event's
    relation had REPLICA IDENTITY FULL) and payload where payload holds raw old/new
    cells keyed by column name. SQL NULL, unchanged TOAST and a
    column absent from the event's schema version are distinct: absent columns did
    not exist yet and fold to NULL; unchanged cells inherit an earlier image, the
    full old image, or the mirror row. A key change deletes the old key.
    """
    for event in events:
        op = event["op"]
        payload = event["payload"]
        old, new = payload.get("old"), payload.get("new")
        commit = event["commit_lsn"]

        def key_of(image, fallback=None):
            values = []
            for name in keys:
                cell = image.get(name, {"kind": "absent"}) if image else {"kind": "absent"}
                if cell["kind"] == "unchanged" and fallback is not None:
                    cell = fallback.get(name, {"kind": "absent"})
                if cell["kind"] != "value":
                    raise Blocked(
                        "needs_action",
                        f"replica identity cell is {cell['kind']}, not a value",
                        commit,
                    )
                values.append(cell["value"])
            return tuple(values)

        if op == "truncate":
            raise Blocked("needs_resync", "TRUNCATE requires a new generation", commit)
        if op == "delete":
            state[key_of(old)] = Image(
                "delete", commit_lsn=commit, commit_ts_us=event["commit_ts_us"]
            )
            continue
        if op not in ("insert", "update") or new is None:
            raise Blocked("needs_action", f"unsupported change {op!r}", commit)
        new_key = key_of(new, fallback=old)
        old_key = key_of(old) if old else new_key
        if op == "insert":
            base = Image("upsert", complete=True)
        else:
            previous = state.get(old_key)
            if previous is None:
                base = Image("upsert")
            elif previous.op == "delete":
                # The source cannot update a deleted row; treat as unknown baseline.
                base = Image("upsert")
            else:
                base = Image("upsert", dict(previous.values), previous.complete)
        values = dict(base.values)
        # A key-only old tuple (identity default/index) carries NULL placeholders for
        # non-key columns; only a FULL old image describes the whole prior row.
        full_old = event.get("full_old", False)
        for image, is_old in ((old, True), (new, False)):
            for name, cell in (image or {}).items():
                if is_old and not full_old and name not in keys:
                    continue
                if cell["kind"] in ("value", "null"):
                    values[name] = cell["value"]
        for name in mirror_names:
            if name not in new:
                # Column did not exist in this schema version. An ADD COLUMN waits
                # for writers of the table, so versions are monotonic per relation.
                values[name] = None
            elif op == "insert" and new[name]["kind"] != "value" and new[name]["kind"] != "null":
                raise Blocked("needs_action", "insert without a complete new image", commit)
        complete = base.complete or all(name in values for name in mirror_names)
        if old_key != new_key:
            if not complete:
                raise Blocked(
                    "needs_action",
                    "key change with unchanged columns and no full old image",
                    commit,
                )
            state[old_key] = Image("delete", commit_lsn=commit, commit_ts_us=event["commit_ts_us"])
        state[new_key] = Image("upsert", values, complete, commit, event["commit_ts_us"])
    return state
