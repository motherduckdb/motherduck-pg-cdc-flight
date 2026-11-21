"""Format-2 apply's server-side fold: the SQL the common-case batch runs where the frames are.

Pure SQL generation, no I/O. `ApplyRun` (apply_run.py) runs two statements per table and
page: `plan_sql` (the page's frame checks plus what the control plane decides on, one row)
and `merge_sql` over `fold_sql` (the fold as the MERGE source). The frames never leave the
database.

`fold_sql` restates `mirror.fold` + `Apply.stage` per key (the contract; the differential
tests in tests/test_sql_fold*.py hold the two to identical staged rows and mirrors):
- events are ordered as `UnitReader.frames` orders them: (unit_start_lsn, frame_ordinal);
- an event sets a mirror column when its op is not a delete and its new cell is TEXT or NULL
  (that value), else when its old image is eligible (REPLICA IDENTITY FULL, or a key column)
  and that cell is TEXT or NULL (the old value). UNCHANGED sets nothing; old before new, as
  fold applies them;
- a key's base resets at its last delete (a later update starts from an unknown baseline)
  and at its last insert (complete): the live segment is
  `ord > last_delete AND ord >= last_insert`, and a column is known iff the last event that
  sets it lies in the segment (`arg_max` over a struct, so a NULL value still wins);
- the image is a delete iff the key's last event is a delete (known: the key columns only);
  complete iff an insert opens the segment or every mirror column is known;
- _s_lsn and _s_ts come from the key's last event.

The common case is one relation, one schema version and key layout, no key change and no
blocking event. `plan_sql` finds everything else (`mirror.fold`'s blocks, `mirror2.cells`'
malformed images, key changes, unknown record kinds) and ApplyRun falls back to the Python
fold for that batch, which raises or holds exactly as before.
"""

from dataclasses import dataclass

from cdc_flight.config import quoted
from cdc_flight.mirror import NEGATIVE_ZERO, cast_sql, float_keys

ABSENT, NULL, UNCHANGED, TEXT = range(4)  # format2's persisted cell tags
VALUE_KINDS = f"({NULL},{TEXT})"
PG_EPOCH_US = 946_684_800_000_000
# The MERGE's baseline precheck (a WHEN NOT MATCHED ... THEN ERROR clause): ApplyRun maps
# this marker back to apply's Blocked.
BASELINE_ERROR = "cdc apply: update of a row absent from the mirror lacks a full image"
# An event's position in frames() order, as one orderable integer.
ORD = "(f.unit_start_lsn::HUGEINT*18446744073709551616+f.frame_ordinal)"


def literal(text):
    return "'" + text.replace("'", "''") + "'"


def frames_sql(days, epoch, low, end):
    """The page's physical frames (every relation): UnitReader.frames' branches."""
    for day in days:
        if type(day) is not str or len(day) != 8 or not day.isascii() or not day.isdigit():
            raise ValueError("invalid physical day")
    for value in (epoch, low, end):
        if type(value) is not int or value < 0:
            raise ValueError("invalid frame bound")
    if not days:
        raise ValueError("complete unit has no physical frame days")
    return " UNION ALL ".join(
        f"SELECT * FROM {{s}}frames_v2_{day} WHERE source_epoch={epoch} "
        f"AND unit_start_lsn>={low} AND unit_start_lsn<{end}"
        for day in days
    )


def _relation(relation_id):
    if type(relation_id) is not int or not 0 <= relation_id < 1 << 32:
        raise ValueError("invalid relation id")
    return relation_id


def key_cell(p):
    """A key cell's (kind, value) as fold's key_of reads it: the new cell, falling back to
    the old image when the new one is UNCHANGED; the old image for a delete."""
    kind = (
        f"CASE WHEN f.op='delete' THEN f.old.kinds[{p}] "
        f"WHEN f.new.kinds[{p}]={UNCHANGED} THEN f.old.kinds[{p}] ELSE f.new.kinds[{p}] END"
    )
    value = (
        f"CASE WHEN f.op='delete' THEN f.old.values[{p}] "
        f"WHEN f.new.kinds[{p}]={UNCHANGED} THEN f.old.values[{p}] ELSE f.new.values[{p}] END"
    )
    return kind, value


def _key_value(layout, name, p):
    """A key cell's value as the fold groups and writes it: a float key's -0 is 0
    (mirror.canonical_key, the same rule on the Python fold)."""
    value = key_cell(p)[1]
    if name not in float_keys(layout.columns, layout.keys):
        return value
    return f"CASE WHEN ({value})={literal(NEGATIVE_ZERO)} THEN '0' ELSE ({value}) END"


@dataclass(frozen=True)
class Layout:
    """One schema version's projection onto the mirror's columns (matched by name)."""

    columns: tuple  # mirror Columns, in mirror order
    keys: tuple
    names: tuple  # the descriptor's column names, source order
    full_old: bool  # REPLICA IDENTITY FULL (fold_batch's rule)

    def position(self, name):
        """1-based position in the frame image, or None (absent from this version)."""
        return self.names.index(name) + 1 if name in self.names else None


def fold_sql(layout, relation_id):
    """SELECT over the CTE `frames` yielding Apply.stage's rows: c0..cN as PostgreSQL text
    plus _s_op, _s_known, _s_complete, _s_lsn, _s_ts."""
    relation_id = _relation(relation_id)
    keys = layout.keys
    key_pos = [layout.position(k) for k in keys]
    if None in key_pos:
        raise ValueError("key column absent from the descriptor")
    old_ok = "TRUE" if layout.full_old else "FALSE"
    per_event, aggregates, known_cols = [], [], []
    for i, column in enumerate(layout.columns):
        p = layout.position(column.name)
        if column.name in keys or p is None:
            continue
        known_cols.append(i)
        per_event.append(
            f"(f.op<>'delete' AND (f.new.kinds[{p}] IN {VALUE_KINDS} OR "
            f"({old_ok} AND f.old.kinds[{p}] IN {VALUE_KINDS}))) AS s{i},"
            f"CASE WHEN f.new.kinds[{p}] IN {VALUE_KINDS} THEN f.new.values[{p}] "
            f"ELSE f.old.values[{p}] END AS v{i}"
        )
        aggregates.append(f"arg_max({{'v':v{i},'o':ord}},ord) FILTER (WHERE s{i}) AS a{i}")
    events = (
        f"SELECT {ORD} AS ord,f.op,f.commit_lsn,f.commit_ts_us,"
        + ",".join(
            [_key_value(layout, k, p) + f" AS k{j}" for j, (k, p) in enumerate(zip(keys, key_pos))]
            + per_event
        )
        + f" FROM frames f WHERE f.record_kind=1 AND f.relation_id={relation_id} "
        "AND f.op IN ('insert','update','delete')"
    )
    group = ",".join(f"k{j}" for j in range(len(keys)))
    grouped = (
        f"SELECT {group},max(ord) FILTER (WHERE op='delete') AS last_del,"
        "max(ord) FILTER (WHERE op='insert') AS last_ins,arg_max(op,ord) AS last_op,"
        "arg_max(commit_lsn,ord) AS lsn,arg_max(commit_ts_us,ord) AS ts"
        + "".join("," + a for a in aggregates)
        + f" FROM ({events}) GROUP BY {group}"
    )

    def live(i):
        return (
            f"coalesce((g.a{i}.o>coalesce(g.last_del,-1) AND "
            f"g.a{i}.o>=coalesce(g.last_ins,-1)),FALSE)"
        )

    cells, known = [], []
    for i, column in enumerate(layout.columns):
        if column.name in keys:
            cells.append(f"g.k{keys.index(column.name)} AS c{i}")
            known.append(str(i))  # a delete stages its key; an upsert always knows it
        elif layout.position(column.name) is None:
            # absent from this version: a known NULL for an upsert (fold's rule)
            cells.append(f"NULL::VARCHAR AS c{i}")
            known.append(f"CASE WHEN g.last_op<>'delete' THEN {i} END")
        else:
            cells.append(f"CASE WHEN g.last_op<>'delete' AND {live(i)} THEN g.a{i}.v END AS c{i}")
            known.append(f"CASE WHEN g.last_op<>'delete' AND {live(i)} THEN {i} END")
    every = " AND ".join(live(i) for i in known_cols) or "TRUE"
    return (
        "SELECT "
        + ",".join(cells)
        + ",CASE WHEN g.last_op='delete' THEN 'delete' ELSE 'upsert' END AS _s_op,"
        "list_filter([" + ",".join(known) + "]::INTEGER[],x->x IS NOT NULL) AS _s_known,"
        "(g.last_op<>'delete' AND (coalesce(g.last_ins>coalesce(g.last_del,-1),FALSE) OR "
        f"({every}))) AS _s_complete,g.lsn::UBIGINT AS _s_lsn,"
        f"TIMESTAMPTZ 'epoch'+to_microseconds(g.ts+{PG_EPOCH_US}) AS _s_ts "
        f"FROM ({grouped}) g"
    )


def _malformed(image, width):
    """mirror2.cells' refusals for one image (FALSE when the image is NULL)."""
    v, k = f"f.{image}.values", f"f.{image}.kinds"
    return (
        f"(f.{image} IS NOT NULL AND (len({v})<>{width} OR len({k})<>{width} OR "
        f"list_min({k})<{NULL} OR list_max({k})>{TEXT} OR "
        f"len(list_filter({v},x->x IS NULL))<>len({v})-len(list_filter({k},x->x={TEXT}))))"
    )


def plan_sql(frames, relation_id, versions):
    """One statement over the page, returning one row:

    floor, position, counts (UnitReader.frames' checks, same shapes), bad_kinds (frames whose
    record_kind is neither EVENT nor COMMIT), events, commits, layouts ([version, key hash]
    pairs), block_at ([commit, unit] of the first blocking event in frames() order),
    block_reason, change_at (the first key change), key_changes.

    `versions`: {schema_version: (width, key 1-based positions)} from the page's
    descriptors for this relation (metadata). An event of any other version blocks."""
    rel = _relation(relation_id)
    malformed, key_block, changes = [], [], []
    for version, (width, positions) in versions.items():
        v = f"f.schema_version={literal(version)}"
        malformed.append(f"({v} AND ({_malformed('old', width)} OR {_malformed('new', width)}))")
        for p in positions:
            kind, _ = key_cell(p)
            key_block.append(
                f"({v} AND (coalesce({kind},{ABSENT})<>{TEXT} OR (f.op='update' AND "
                f"f.old IS NOT NULL AND coalesce(f.old.kinds[{p}],{ABSENT})<>{TEXT})))"
            )
        new = "[" + ",".join(key_cell(p)[1] for p in positions) + "]"
        old = "[" + ",".join(f"f.old.values[{p}]" for p in positions) + "]"
        changes.append(f"({v} AND {old} IS DISTINCT FROM {new})")
    known = ",".join(literal(v) for v in versions) or "NULL"
    reason = (
        "CASE WHEN f.op='truncate' THEN 'truncate' "
        "WHEN f.op IS NULL OR f.schema_version IS NULL THEN 'unsupported' "
        f"WHEN f.schema_version NOT IN ({known}) THEN 'unknown_version' "
        "WHEN f.old_tuple_kind IS NULL OR f.old_tuple_kind NOT IN (0,1,2) "
        "OR ((f.old IS NULL)<>(f.old_tuple_kind=0)) THEN 'malformed' "
        f"WHEN {' OR '.join(malformed) or 'FALSE'} THEN 'malformed' "
        "WHEN f.op NOT IN ('insert','update','delete') THEN 'unsupported' "
        "WHEN f.new IS NULL AND f.op<>'delete' THEN 'unsupported' "
        f"WHEN {' OR '.join(key_block) or 'FALSE'} THEN 'key_cell' "
        f"WHEN f.op='insert' AND list_contains(f.new.kinds,{UNCHANGED}::UTINYINT) "
        "THEN 'incomplete_insert' END"
    )
    key_change = (
        "coalesce(f.op='update' AND f.old IS NOT NULL AND ("
        + (" OR ".join(changes) or "FALSE")
        + "),FALSE)"
    )
    events = (
        f"SELECT {ORD} AS ord,f.op,f.commit_lsn,f.unit_start_lsn,f.schema_version,"
        f"f.key_layout_hash,{reason} AS reason,{key_change} AS key_change "
        f"FROM frames f WHERE f.record_kind=1 AND (f.relation_id={rel} OR "
        f"(f.op='truncate' AND list_contains(f.truncate_oids,{rel}::UINTEGER)))"
    )
    return (
        f"WITH frames AS MATERIALIZED ({frames}), events AS ({events}) SELECT "
        "(SELECT [floor_epoch,floor_lsn] FROM {s}retention WHERE id=1),"
        "(SELECT {'epoch':s.epoch,'high':s.confirmed_lsn,'format':p.format_version,"
        "'reader':p.min_reader,'binding_hash':p.binding_hash,'binding_json':p.binding_json} "
        "FROM {s}source s,{s}protocol_v2 p WHERE s.id=1 AND p.id=1),"
        "(SELECT list(c) FROM (SELECT unit_start_lsn,count(*) AS n,"
        "count(DISTINCT frame_ordinal) AS distinct_n,min(frame_ordinal) AS first,"
        "max(frame_ordinal) AS last,"
        "arg_max(commit_ts_us,frame_ordinal) FILTER (WHERE record_kind=2) AS last_commit_ts FROM frames GROUP BY unit_start_lsn) c),"
        "(SELECT count(*) FROM frames WHERE record_kind IS NULL OR record_kind NOT IN (1,2)),"
        "* FROM (SELECT count(*) FILTER (WHERE op IS DISTINCT FROM 'truncate') AS events,"
        "count(DISTINCT commit_lsn) FILTER (WHERE op IS DISTINCT FROM 'truncate') AS commits,"
        "list(DISTINCT [schema_version,key_layout_hash]) "
        "FILTER (WHERE op IS DISTINCT FROM 'truncate') AS layouts,"
        "arg_min([commit_lsn,unit_start_lsn],ord) FILTER (WHERE reason IS NOT NULL) AS block_at,"
        "arg_min(reason,ord) FILTER (WHERE reason IS NOT NULL) AS block_reason,"
        "arg_min([commit_lsn,unit_start_lsn],ord) FILTER (WHERE key_change) AS change_at,"
        "count(*) FILTER (WHERE key_change) AS key_changes FROM events)"
    )


PAGE_CHECKS = (
    "(SELECT [floor_epoch,floor_lsn] FROM {s}retention WHERE id=1),"
    "(SELECT {'epoch':s.epoch,'high':s.confirmed_lsn,'format':p.format_version,"
    "'reader':p.min_reader,'binding_hash':p.binding_hash,'binding_json':p.binding_json} "
    "FROM {s}source s,{s}protocol_v2 p WHERE s.id=1 AND p.id=1),"
    "(SELECT list(c) FROM (SELECT unit_start_lsn,count(*) AS n,"
    "count(DISTINCT frame_ordinal) AS distinct_n,min(frame_ordinal) AS first,"
    "max(frame_ordinal) AS last,"
    "arg_max(commit_ts_us,frame_ordinal) FILTER (WHERE record_kind=2) AS last_commit_ts FROM frames GROUP BY unit_start_lsn) c),"
    "(SELECT count(*) FROM frames WHERE record_kind IS NULL OR record_kind NOT IN (1,2))"
)


def page_plan_sql(frames, relations, only=None):
    """One statement for a whole page and every relation in it, returning one row:

    floor, position, counts, bad_kinds (as plan_sql: the page's checks over the unfiltered
    frames), then `relations`: a list of one struct per relation with events in the page:
    {relation_id, events, commits, layouts, block_at, block_reason, change_at, key_changes},
    each field exactly what plan_sql returns for that relation alone.

    `relations`: [(relation_id, schema_version, width, key 1-based positions)] from the
    page's descriptors. They go in as a VALUES table joined per event, so the SQL's size
    doesn't grow with the relation count; the key checks are list lambdas over each
    relation's own key positions. A TRUNCATE counts for every relation it lists in
    `truncate_oids`, as logical_events expands it (spool2 writes every TRUNCATE frame with
    relation_id=0, so plan_sql's `relation_id=R` branch for TRUNCATE never fires).

    `only`: the relation ids to judge (the run's tables). Other relations' events are never
    judged and get no row; the page checks still cover every frame."""
    rows = []
    for relation_id, version, width, positions in relations:
        _relation(relation_id)
        if (
            type(width) is not int
            or width < 0
            or any(type(p) is not int or p < 1 for p in positions)
        ):
            raise ValueError("invalid descriptor layout")
        keys = "[" + ",".join(str(p) for p in positions) + "]::INTEGER[]"
        rows.append(f"({relation_id}::UINTEGER,{literal(version)},{width},{keys})")
    descriptors = (
        "SELECT * FROM (VALUES " + ",".join(rows) + ") d(relation_id,version,width,kp)"
        if rows
        else "SELECT NULL::UINTEGER AS relation_id,NULL::VARCHAR AS version,"
        "NULL::INTEGER AS width,NULL::INTEGER[] AS kp WHERE FALSE"
    )
    kind = (
        "CASE WHEN e.op='delete' THEN e.old.kinds[p] WHEN e.new.kinds[p]=2 "
        "THEN e.old.kinds[p] ELSE e.new.kinds[p] END"
    )
    new_key = "CASE WHEN e.new.kinds[p]=2 THEN e.old.values[p] ELSE e.new.values[p] END"

    def malformed(image):
        v, k = f"e.{image}.values", f"e.{image}.kinds"
        return (
            f"(e.{image} IS NOT NULL AND (len({v})<>d.width OR len({k})<>d.width OR "
            f"list_min({k})<{NULL} OR list_max({k})>{TEXT} OR "
            f"len(list_filter({v},x->x IS NULL))<>len({v})-len(list_filter({k},x->x={TEXT}))))"
        )

    reason = (
        "CASE WHEN e.op='truncate' THEN 'truncate' "
        "WHEN e.op IS NULL OR e.schema_version IS NULL THEN 'unsupported' "
        "WHEN d.relation_id IS NULL THEN 'unknown_version' "
        "WHEN e.old_tuple_kind IS NULL OR e.old_tuple_kind NOT IN (0,1,2) "
        "OR ((e.old IS NULL)<>(e.old_tuple_kind=0)) THEN 'malformed' "
        f"WHEN {malformed('old')} OR {malformed('new')} THEN 'malformed' "
        "WHEN e.op NOT IN ('insert','update','delete') THEN 'unsupported' "
        "WHEN e.new IS NULL AND e.op<>'delete' THEN 'unsupported' "
        f"WHEN coalesce(list_bool_or(list_transform(d.kp,p->coalesce({kind},{ABSENT})<>{TEXT} "
        f"OR (e.op='update' AND e.old IS NOT NULL AND coalesce(e.old.kinds[p],{ABSENT})<>{TEXT}))),"
        "FALSE) THEN 'key_cell' "
        f"WHEN e.op='insert' AND list_contains(e.new.kinds,{UNCHANGED}::UTINYINT) "
        "THEN 'incomplete_insert' END"
    )
    key_change = (
        "coalesce(e.op='update' AND e.old IS NOT NULL AND "
        "list_transform(d.kp,p->e.old.values[p]) IS DISTINCT FROM "
        f"list_transform(d.kp,p->{new_key}),FALSE)"
    )
    events = (
        f"SELECT {ORD} AS ord,f.relation_id AS rel,f.* EXCLUDE (relation_id) FROM frames f "
        "WHERE f.record_kind=1 AND f.op IS DISTINCT FROM 'truncate' "
        f"UNION ALL SELECT {ORD} AS ord,unnest(f.truncate_oids) AS rel,"
        "f.* EXCLUDE (relation_id) FROM frames f WHERE f.record_kind=1 AND f.op='truncate'"
    )
    if only is not None:
        oids = sorted({_relation(oid) for oid in only})
        listed = ",".join(f"{oid}::UINTEGER" for oid in oids) or "NULL::UINTEGER"
        events = f"SELECT * FROM ({events}) WHERE rel IN ({listed})"
    judged = (
        f"SELECT e.ord,e.rel,e.op,e.commit_lsn,e.unit_start_lsn,e.schema_version,"
        f"e.key_layout_hash,{reason} AS reason,{key_change} AS key_change "
        "FROM events e LEFT JOIN descriptors d ON d.relation_id=e.rel AND d.version=e.schema_version"
    )
    per_relation = (
        "SELECT rel AS relation_id,"
        "count(*) FILTER (WHERE op IS DISTINCT FROM 'truncate') AS events,"
        "count(DISTINCT commit_lsn) FILTER (WHERE op IS DISTINCT FROM 'truncate') AS commits,"
        "list(DISTINCT [schema_version,key_layout_hash]) "
        "FILTER (WHERE op IS DISTINCT FROM 'truncate') AS layouts,"
        "arg_min([commit_lsn,unit_start_lsn],ord) FILTER (WHERE reason IS NOT NULL) AS block_at,"
        "arg_min(reason,ord) FILTER (WHERE reason IS NOT NULL) AS block_reason,"
        "arg_min([commit_lsn,unit_start_lsn],ord) FILTER (WHERE key_change) AS change_at,"
        "count(*) FILTER (WHERE key_change) AS key_changes FROM judged GROUP BY rel"
    )
    return (
        f"WITH frames AS MATERIALIZED ({frames}), descriptors AS ({descriptors}), "
        f"events AS ({events}), judged AS ({judged}), per_relation AS ({per_relation}) "
        f"SELECT {PAGE_CHECKS},(SELECT list(r) FROM per_relation r)"
    )


def source_sql(layout, relation_id, frames):
    """The MERGE source: the fold, cast to the mirror types as Apply._merge casts."""
    casts = ",".join(f"{cast_sql(f'c{i}', c.dtype)} AS c{i}" for i, c in enumerate(layout.columns))
    return (
        f"(WITH frames AS ({frames}) SELECT {casts},_s_op,_s_known,_s_complete,_s_lsn,_s_ts "
        f"FROM ({fold_sql(layout, relation_id)}))"
    )


def merge_sql(target, keys, columns, source):
    """Apply._merge's MERGE, with its baseline precheck as a MERGE ERROR action: an
    incomplete upsert of a key the mirror lacks fails the statement (and the transaction)."""
    position = {c.name: i for i, c in enumerate(columns)}
    on = " AND ".join(f"m.{quoted(k)}=s.c{position[k]}" for k in keys)
    updates = [
        f"{quoted(c.name)}=CASE WHEN s._s_complete OR list_contains(s._s_known,{i}) "
        f"THEN s.c{i} ELSE m.{quoted(c.name)} END"
        for i, c in enumerate(columns)
        if c.name not in keys
    ] + ["_cdc_commit_lsn=s._s_lsn", "_cdc_updated_at=s._s_ts"]
    names = ",".join(quoted(c.name) for c in columns)
    values = ",".join(f"s.c{i}" for i in range(len(columns)))
    return (
        f"MERGE INTO {target} m USING {source} s ON {on} "
        "WHEN MATCHED AND s._s_op='delete' THEN DELETE "
        f"WHEN MATCHED THEN UPDATE SET {','.join(updates)} "
        "WHEN NOT MATCHED AND s._s_op='upsert' AND NOT s._s_complete "
        f"THEN ERROR {literal(BASELINE_ERROR)} "
        "WHEN NOT MATCHED AND s._s_op='upsert' THEN "
        f"INSERT ({names},_cdc_commit_lsn,_cdc_updated_at) VALUES ({values},s._s_lsn,s._s_ts)"
    )
