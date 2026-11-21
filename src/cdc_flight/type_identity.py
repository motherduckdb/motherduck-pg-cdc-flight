"""The pre-deploy type identity report and its accept.

From the release that compares each column's source type (OID, typmod) in reconcile, a
mirror column whose stored type differs from the source's current one holds at its next
event, unless the change keeps every value (mirror.type_kept). A table retyped before
that release would hold after the deploy. Before deploying:

- `report`: read-only, every mirror table. Per column: `holds` (the release would hold
  it) or `widened` (kept; recorded as the current type at the next batch).
- each `holds` table is then resynced, or accepted when the operator knows the retype
  changed no value: `accept` rewrites only those columns' stored type to the current
  one, under the table's apply lease, with a typed confirmation that names the exact
  change (a dry run shows it) and a runs row as its audit.
"""

import hashlib
import json

from cdc_flight.apply import ApplyError, LeaseBusy, table_name
from cdc_flight.mirror import Column, duck_type, type_kept
from cdc_flight.preflight import query


def _source_types(pg, relation_ids):
    rows = query(
        pg,
        "SELECT attrelid::bigint,attname::text,atttypid::bigint,atttypmod FROM pg_attribute "
        "WHERE attrelid=ANY(%s::oid[]) AND attnum>0 AND NOT attisdropped",
        [sorted(relation_ids)],
    )
    found = {}
    for relation_id, name, type_oid, typmod in rows:
        found.setdefault(int(relation_id), {})[name] = (int(type_oid), int(typmod))
    return found


def _mirrors(apply):
    rows = apply.execute(
        "SELECT relation_id,schema_name,table_name,columns FROM {m}apply_state "
        "WHERE columns IS NOT NULL ORDER BY schema_name,table_name"
    ).fetchall()
    return [(rid, f"{s}.{t}", [Column(*c) for c in json.loads(cols)]) for rid, s, t, cols in rows]


def _compare(columns, source):
    """[(column name, stored (oid, typmod), current (oid, typmod), verdict)]."""
    out = []
    for column in columns:
        current = source.get(column.name)
        if current is None:
            continue  # dropped or renamed: reconcile holds that already, unchanged
        other = Column(column.name, duck_type(*current), *current)
        if other.dtype != column.dtype or (column.type_oid, column.typmod) == current:
            continue  # a mirror type change holds already; equal needs nothing
        verdict = "widened" if type_kept(column, other) else "holds"
        out.append((column.name, [column.type_oid, column.typmod], list(current), verdict))
    return out


def report(apply, pg):
    """{table: [{column, stored, current, verdict}]} for every mirror table with a
    difference. Read-only."""
    mirrors = _mirrors(apply)
    source = _source_types(pg, [rid for rid, _, _ in mirrors])
    out = {}
    for relation_id, table, columns in mirrors:
        found = _compare(columns, source.get(relation_id, {}))
        if found:
            out[table] = [dict(column=n, stored=s, current=c, verdict=v) for n, s, c, v in found]
    return out


def _digest(found):
    return hashlib.sha256(json.dumps(found, sort_keys=True).encode()).hexdigest()[:12]


def confirmation(table, found):
    return f"ACCEPT TYPES {table} {_digest(found)}"


def accept(apply, pg, table, *, confirm=None, dry_run=False, run_id="accept-types"):
    """Rewrite the stored source type of `table`'s `holds` columns to the current one.
    A dry run returns the change and the confirmation text; a real run needs exactly
    that text (so it acts only on the change the dry run showed)."""
    table_name(table)
    mirrors = {t: (rid, cols) for rid, t, cols in _mirrors(apply)}
    if table not in mirrors:
        raise ApplyError(f"{table} has no mirror columns")
    relation_id, columns = mirrors[table]
    found = [
        f
        for f in _compare(columns, _source_types(pg, [relation_id]).get(relation_id, {}))
        if f[3] == "holds"
    ]
    if not found:
        return dict(status="already", table=table, columns=[])
    text = confirmation(table, found)
    shown = dict(table=table, columns=[f[0] for f in found], confirm=text)
    if dry_run:
        return dict(status="would", **shown)
    if confirm != text:
        return dict(
            status="refused", detail="the confirmation must be exactly the dry run's", **shown
        )
    current = apply.state(relation_id)
    try:
        token = apply.acquire(relation_id, run_id, token=current["token"])
    except LeaseBusy:
        return dict(status="busy", **shown)
    try:
        changed = {name: tuple(now) for name, _, now, _ in found}
        rewritten = [
            Column(c.name, c.dtype, *changed[c.name]) if c.name in changed else c for c in columns
        ]
        with apply.transaction():
            apply.guard(relation_id, token)
            count = apply.execute(
                "UPDATE {m}apply_state SET columns=? WHERE relation_id=? AND token=? AND columns=?",
                [
                    json.dumps([[c.name, c.dtype, c.type_oid, c.typmod] for c in rewritten]),
                    relation_id,
                    token,
                    json.dumps([[c.name, c.dtype, c.type_oid, c.typmod] for c in columns]),
                ],
            ).fetchone()[0]
            if count != 1:
                raise ApplyError(f"{table}: its columns changed underneath; look again")
            apply.execute(
                "INSERT INTO {m}runs (run_id,relation_id,status,boundary,detail,updated_at) "
                "VALUES (?,?,'accept-types',?,?,current_timestamp) ON CONFLICT DO UPDATE SET "
                "status=excluded.status,detail=excluded.detail,updated_at=excluded.updated_at",
                [run_id, relation_id, current["applied"], text],
            )
    finally:
        apply.release(relation_id, token)
    return dict(status="done", **shown)
