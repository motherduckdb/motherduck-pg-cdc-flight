"""The SQL fold's contract, held to the Python fold's (sql_fold.py vs mirror.fold).

Seeded, wire-consistent format-2 event sequences (FULL identity sends O old tuples; the
default identity sends K old tuples with NULL placeholders) go through
- the oracle: mirror2.to_v1, fold_batch's full_old rule, mirror.fold, Apply.stage;
- sql_fold.fold_sql over an in-memory frames table.
Staged rows must be identical: every cell, _s_known, _s_complete, _s_lsn and the
microsecond _s_ts. For a sequence the fold blocks, sql_fold.plan_sql must name the same
first blocking commit (the batch then falls back to the Python fold).

CI runs CDC_TEST_SQL_FOLD_SEEDS (default 400) seeds; set it higher for a long run. The
coverage buckets must all be hit at that count, and every mutant below must be caught.
"""

import json
import os
import random
import re
import tempfile
from collections import Counter

import duckdb
import pyarrow as pa
import pytest

from cdc_flight import mirror, sql_fold
from cdc_flight.apply import Apply
from cdc_flight.format2 import FRAME_SCHEMA, NO_OLD, OLD_FULL, OLD_KEY, project
from cdc_flight.mirror import Blocked, descriptor_columns, reconcile
from cdc_flight.mirror2 import to_v1

NULL, UNCH, TEXT = 1, 2, 3
REL, OTHER = 101, 202
SEEDS = int(os.getenv("CDC_TEST_SQL_FOLD_SEEDS", "400"))
BUCKETS = (
    "compared_rows",
    "blocked",
    "key_change",
    "truncate_blocked",
    "unchanged_after_earlier_event",
    "update_after_delete",
    "insert_after_delete",
    "full_old_fills_unchanged",
    "full_old_with_unchanged",
    "key_only_old_placeholders",
    "mirror_order_differs",
    "known_null",
    "partial_upsert",
    "update_only_key",
)


def descriptor(names, keys, identity):
    columns = [[1 if n in keys else 0, n, 25, -1] for n in names]
    return json.dumps(dict(identity=identity, columns=columns))


class Gen:
    """One relation's layout (keys, shuffled source order, identity) and its events."""

    def __init__(self, rng, mode):
        self.rng, self.mode = rng, mode
        self.keys = tuple(f"k{i}" for i in range(rng.choice((1, 1, 2))))
        names = list(self.keys) + [f"d{i}" for i in range(rng.randint(1, 4))]
        rng.shuffle(names)  # the descriptor's order differs from the key order
        self.names = tuple(names)
        self.identity = rng.choice("fd")
        self.key_idx = tuple(self.names.index(k) for k in self.keys)

    def value(self):
        if self.rng.random() < 0.2:
            return (None, NULL)
        return (self.rng.choice(["a", "b", "", "x'y", "1"]) + str(self.rng.randint(0, 3)), TEXT)

    @staticmethod
    def image(cells):
        return {"values": [c[0] for c in cells], "kinds": [c[1] for c in cells]}

    def event(self, op, key):
        rng, mode = self.rng, self.mode
        kc = {k: (v, TEXT) for k, v in zip(self.keys, key)}
        old = new = None
        kind = NO_OLD
        if op == "insert":
            cells = [kc.get(n) or self.value() for n in self.names]
            if mode == "block" and rng.random() < 0.05:
                cells[rng.randrange(len(cells))] = (None, UNCH)
            new = self.image(cells)
        elif op == "update":
            cells = []
            for n in self.names:
                if n in kc:
                    cells.append((None, UNCH) if rng.random() < 0.1 else kc[n])
                else:
                    cells.append((None, UNCH) if rng.random() < 0.4 else self.value())
            new = self.image(cells)
            if self.identity == "f":
                if rng.random() < 0.85:
                    kind = OLD_FULL
                    old = self.image(
                        [
                            kc[n]
                            if n in kc
                            else (None, UNCH)
                            if rng.random() < 0.15
                            else self.value()
                            for n in self.names
                        ]
                    )
            elif rng.random() < 0.5:
                kind = OLD_KEY
                old = self.image([kc[n] if n in kc else (None, NULL) for n in self.names])
            if mode == "change" and old is not None and rng.random() < 0.1:
                old["values"][self.key_idx[0]] = "other"
            if mode == "block" and rng.random() < 0.04:
                i = self.key_idx[0]
                new["kinds"][i], new["values"][i] = UNCH, None
                if old is not None and rng.random() < 0.5:
                    old["kinds"][i], old["values"][i] = NULL, None
        else:
            if self.identity == "f":
                kind = OLD_FULL
                old = self.image([kc[n] if n in kc else self.value() for n in self.names])
            else:
                kind = OLD_KEY
                old = self.image([kc[n] if n in kc else (None, NULL) for n in self.names])
            if mode == "block" and rng.random() < 0.04:
                i = self.key_idx[0]
                old["kinds"][i], old["values"][i] = rng.choice((NULL, UNCH)), None
        return dict(op=op, old=old, new=new, old_tuple_kind=kind)


def frame(**kw):
    row = {f.name: None for f in FRAME_SCHEMA}
    row.update(kw)
    return row


def scenario(seed):
    """(gen, version, descriptor, mirror columns, physical frames, this relation's events)."""
    rng = random.Random(seed)
    mode = rng.choice(["plain", "plain", "plain", "block", "change"])
    g = Gen(rng, mode)
    version = f"v{seed}"
    keyspace = [tuple(f"{k}{rng.randint(0, 3)}" for k in g.keys) for _ in range(rng.randint(1, 4))]
    frames, logical = [], []
    lsn = unit = 1000
    ordinal = 0
    ts = 800_000_000_123_457
    base = dict(record_kind=1, source_epoch=1, txid=1, old_tuple_kind=0)
    for _ in range(rng.randint(1, 12)):
        lsn += rng.randint(10, 100)
        ts += rng.randint(1, 10_000_000) * 7 + 1  # odd microseconds
        if rng.random() < 0.3:
            unit, ordinal = lsn, 0
        envelope = dict(unit_start_lsn=unit, commit_lsn=lsn, commit_ts_us=ts)
        for _ in range(rng.randint(1, 4)):
            r = rng.random()
            if r < 0.1 or (mode == "block" and r < 0.13):
                # another relation's event, a TRUNCATE of it (filtered out), or, in
                # block mode, a TRUNCATE of this relation too (blocks)
                oids = [OTHER, REL] if r >= 0.1 else [OTHER]
                op = "truncate" if r >= 0.05 else "insert"
                frames.append(
                    frame(
                        **base,
                        **envelope,
                        frame_ordinal=ordinal,
                        event_ordinal=ordinal,
                        relation_id=0 if op == "truncate" else OTHER,
                        op=op,
                        truncate_oids=oids if op == "truncate" else None,
                        schema_version=None if op == "truncate" else "other",
                        key_layout_hash="h",
                        new=None if op == "truncate" else {"values": ["z"], "kinds": [TEXT]},
                    )
                )
                if op == "truncate" and REL in oids:
                    logical.append(dict(frames[-1], relation_id=REL))
                ordinal += 1
                continue
            op = rng.choice(["insert", "update", "update", "update", "delete"])
            e = g.event(op, rng.choice(keyspace))
            row = frame(
                **base,
                **envelope,
                frame_ordinal=ordinal,
                event_ordinal=ordinal,
                relation_id=REL,
                schema_name="public",
                table_name="t",
                schema_version=version,
                key_layout_hash="kh",
                op=op,
                old=e["old"],
                new=e["new"],
                old_key=project(e["old"], g.key_idx),
                new_key=project(e["new"], g.key_idx),
            )
            row["old_tuple_kind"] = e["old_tuple_kind"]
            frames.append(row)
            logical.append(row)
            ordinal += 1
        frames.append(frame(record_kind=2, source_epoch=1, frame_ordinal=ordinal, **envelope))
        ordinal += 1
    desc = descriptor(g.names, g.keys, g.identity)
    full = descriptor_columns(desc)
    # The mirror's current columns: none yet, or a permuted subset of the descriptor's
    # (keys included); reconcile appends the rest, so mirror order != source order.
    current = None
    if rng.random() >= 0.2:
        current = rng.sample(full, rng.randint(len(full) // 2, len(full)))
        for k in g.keys:
            if k not in [c.name for c in current]:
                key_column = next(c for c in full if c.name == k)
                current.insert(rng.randrange(len(current) + 1), key_column)
    return g, version, desc, reconcile(current, desc, g.keys), frames, logical


def oracle(g, desc, columns, logical):
    full_old = json.loads(desc)["identity"] == "f"
    events = []
    for row in logical:
        if row["op"] == "truncate":
            events.append(dict(row, full_old=False, payload={"old": None, "new": None}))
            continue
        events.append(dict(to_v1(dict(row), g.names), full_old=full_old))
    return mirror.fold(
        {}, events, g.keys, [c.name for c in columns], float_keys=mirror.float_keys(columns, g.keys)
    )


STUBS = (
    "CREATE TABLE retention(id INTEGER, floor_epoch INTEGER, floor_lsn UBIGINT);"
    "INSERT INTO retention VALUES (1, NULL, NULL);"
    "CREATE TABLE source(id INTEGER, epoch INTEGER, confirmed_lsn UBIGINT);"
    "INSERT INTO source VALUES (1, 1, 0);"
    "CREATE TABLE protocol_v2(id INTEGER, format_version INTEGER, min_reader INTEGER,"
    " binding_hash VARCHAR, binding_json VARCHAR);"
    "INSERT INTO protocol_v2 VALUES (1, 2, 2, '', '');"
)


def connect():
    """An in-memory DuckDB with the plan's metadata subqueries stubbed ({s} = main)."""
    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'")
    con.execute(STUBS)
    return con


def plan_row(con, plan, frames_sql, versions):
    """The plan's decision columns (events, commits, layouts, block_at, block_reason,
    change_at, key_changes)."""
    return con.execute(plan(frames_sql, REL, versions).replace("{s}", "")).fetchone()[4:]


def staged(con, sql):
    """Rows with _s_ts as epoch µs (no pytz) and _s_known sorted, comparable by repr."""
    rows = con.execute(f"SELECT * REPLACE (epoch_us(_s_ts) AS _s_ts) FROM ({sql})").fetchall()
    out = []
    for row in rows:
        row = list(row)
        row[-4] = sorted(row[-4])
        out.append(tuple(map(repr, row)))
    return sorted(out)


def count_hits(hits, g, columns, logical, images):
    by_key = {}
    for row in logical:
        if row["op"] == "truncate":
            continue
        image = row["old"] if row["op"] == "delete" else row["new"]
        key = []
        for i in g.key_idx:
            value = image["values"][i]
            if row["op"] != "delete" and image["kinds"][i] == UNCH and row["old"]:
                value = row["old"]["values"][i]
            key.append(value)
        by_key.setdefault(tuple(key), []).append(row)
    for sequence in by_key.values():
        for a, b in zip(sequence, sequence[1:]):
            if a["op"] == "delete" and b["op"] == "update":
                hits["update_after_delete"] += 1
            if a["op"] == "delete" and b["op"] == "insert":
                hits["insert_after_delete"] += 1
            if a["op"] != "delete" and b["op"] == "update" and UNCH in b["new"]["kinds"]:
                hits["unchanged_after_earlier_event"] += 1
        for r in sequence:
            if r["op"] == "update" and r["old_tuple_kind"] == OLD_FULL:
                pairs = zip(r["new"]["kinds"], r["old"]["kinds"])
                if any(n == UNCH and o in (NULL, TEXT) for n, o in pairs):
                    hits["full_old_fills_unchanged"] += 1
                if UNCH in r["old"]["kinds"]:
                    hits["full_old_with_unchanged"] += 1
            if r["op"] == "update" and r["old_tuple_kind"] == OLD_KEY:
                hits["key_only_old_placeholders"] += 1
        ops = [r["op"] for r in sequence]
        if ops[-1] != "delete" and "insert" not in ops:
            hits["update_only_key"] += 1
    for image in images.values():
        if image.op == "upsert" and None in image.values.values():
            hits["known_null"] += 1
        if image.op == "upsert" and not image.complete:
            hits["partial_upsert"] += 1
    names = [c.name for c in columns]
    if names != [n for n in g.names if n in names]:
        hits["mirror_order_differs"] += 1


def differential(seeds, fold=sql_fold.fold_sql, plan=sql_fold.plan_sql, *, first=False):
    """(coverage hits, failures) over `seeds`; a failure names its seed. With `first`, stop
    at the first failing seed: a mutant test asks only whether any seed catches it."""
    con = connect()
    hits, failures = Counter(), []
    try:
        for seed in seeds:
            if first and failures:
                break
            g, version, desc, columns, frames, logical = scenario(seed)
            con.register("frames_in", pa.Table.from_pylist(frames, schema=FRAME_SCHEMA))
            frames_sql = "SELECT * FROM frames_in"
            versions = {version: (len(g.names), tuple(g.names.index(k) + 1 for k in g.keys))}
            events, commits, _, block_at, reason, change_at, changes = plan_row(
                con, plan, frames_sql, versions
            )
            mine = [r for r in logical if r["op"] != "truncate"]
            if (events, commits) != (len(mine), len({r["commit_lsn"] for r in mine})):
                failures.append(f"seed {seed}: plan counts")
                continue
            try:
                images, blocked = oracle(g, desc, columns, logical), None
            except Blocked as exc:
                images, blocked = None, exc
            if blocked is not None:
                hits["blocked"] += 1
                if blocked.state == "needs_resync":
                    hits["truncate_blocked"] += 1
                # The SQL path stops at the plan's first block or key change (whichever
                # is first) and the batch falls back; a block the plan names first must be
                # the fold's own first blocking commit.
                if block_at is None and change_at is None:
                    failures.append(f"seed {seed}: the fold blocks ({blocked.reason}), plan not")
                elif block_at is not None and (change_at is None or block_at[0] <= change_at[0]):
                    if block_at[0] != blocked.commit_lsn:
                        failures.append(f"seed {seed}: first blocking commit differs ({reason})")
                continue
            if block_at is not None:
                failures.append(f"seed {seed}: the plan blocks ({reason}), the fold does not")
                continue
            if changes:
                hits["key_change"] += 1
                continue
            count_hits(hits, g, columns, logical, images)
            layout = sql_fold.Layout(
                tuple(columns), g.keys, g.names, json.loads(desc)["identity"] == "f"
            )
            got = staged(con, f"WITH frames AS ({frames_sql}) " + fold(layout, REL))
            with tempfile.TemporaryDirectory() as directory:
                path = Apply.stage(None, directory, g.keys, columns, images) if images else None
                want = staged(con, f"SELECT * FROM read_parquet('{path}')") if path else []
            if got != want:
                failures.append(f"seed {seed}: staged rows differ")
                continue
            hits["compared_rows"] += len(want)
    finally:
        con.close()
    return hits, failures


def test_the_sql_fold_matches_the_python_fold():
    hits, failures = differential(range(SEEDS))
    assert failures == [], failures[:10]
    missing = [b for b in BUCKETS if not hits[b]]
    assert missing == [], f"coverage buckets never hit at {SEEDS} seeds: {missing}; {dict(hits)}"


# Mutants: each must be caught at the CI seed count. Three are equivalent by construction
# (with one schema version per batch) and deliberately absent: `ord > last_delete` -> `>=`
# (a delete sets no column); dropping `ord >= last_insert`, and dropping "an insert opens
# the segment" from _s_complete (an insert sets every column, or it blocks, so every
# column is then known anyway).


def _text(old, new, *, regex=False):
    """A text mutation; a layout it doesn't apply to (say, no FULL identity) is unchanged,
    and the mutant test then relies on the seeds where it does."""

    def mutate(sql):
        return re.sub(old, new, sql) if regex else sql.replace(old, new)

    return mutate


FOLD_MUTANTS = {
    "null_value_skipped": _text(
        r"FILTER \(WHERE s(\d+)\)", r"FILTER (WHERE s\1 AND v\1 IS NOT NULL)", regex=True
    ),
    "old_image_ignored": _text("(TRUE AND f.old.kinds", "(FALSE AND f.old.kinds"),
    "always_complete": _text("FALSE) OR (", "FALSE) OR TRUE OR ("),
    "any_delete_deletes": _text(
        "CASE WHEN g.last_op='delete' THEN 'delete'",
        "CASE WHEN g.last_del IS NOT NULL THEN 'delete'",
    ),
    "segment_ignores_delete": _text(
        r"g\.a(\d+)\.o>coalesce\(g\.last_del,-1\) AND ", "", regex=True
    ),
}
PLAN_MUTANTS = {
    "key_cell_without_fallback": _text(
        r"WHEN f\.new\.kinds\[(\d+)\]=2 THEN f\.old\.kinds\[\d+\] ", "", regex=True
    ),
    "no_incomplete_insert": _text("THEN 'incomplete_insert'", "AND FALSE THEN 'incomplete_insert'"),
    "no_truncate_of_this_relation": _text(
        "(f.op='truncate' AND list_contains(f.truncate_oids,",
        "(FALSE AND list_contains(f.truncate_oids,",
    ),
    "no_key_change": _text("IS DISTINCT FROM", "IS NOT DISTINCT FROM"),
}


@pytest.mark.parametrize("name", sorted(FOLD_MUTANTS))
def test_a_fold_mutant_is_caught(name):
    mutate = FOLD_MUTANTS[name]
    _, failures = differential(
        range(SEEDS), fold=lambda *a: mutate(sql_fold.fold_sql(*a)), first=True
    )
    assert failures, f"fold mutant {name} survived {SEEDS} seeds"


@pytest.mark.parametrize("name", sorted(PLAN_MUTANTS))
def test_a_plan_mutant_is_caught(name):
    mutate = PLAN_MUTANTS[name]
    _, failures = differential(
        range(SEEDS), plan=lambda *a: mutate(sql_fold.plan_sql(*a)), first=True
    )
    assert failures, f"plan mutant {name} survived {SEEDS} seeds"


def test_a_column_matched_by_mirror_position_is_caught(monkeypatch):
    """Columns are matched by name: the mirror's order is not the descriptor's."""
    monkeypatch.setattr(
        sql_fold.Layout,
        "position",
        lambda self, name: (
            [c.name for c in self.columns].index(name) + 1 if name in self.names else None
        ),
    )
    _, failures = differential(range(SEEDS), first=True)
    assert failures


# Refusals mirror2.cells and logical_events make, as plan reasons --------------------


def one_frame(**changes):
    base = dict(
        record_kind=1,
        unit_start_lsn=1,
        frame_ordinal=0,
        source_epoch=1,
        commit_lsn=10,
        event_ordinal=0,
        commit_ts_us=1,
        relation_id=REL,
        schema_version="v",
        key_layout_hash="h",
        old_tuple_kind=0,
        op="insert",
        new={"values": ["1", "a"], "kinds": [TEXT, TEXT]},
    )
    return frame(**{**base, **changes})


@pytest.mark.parametrize(
    "changes, reason",
    [
        ({"op": None}, "unsupported"),
        ({"schema_version": None}, "unsupported"),
        ({"op": "upsert"}, "unsupported"),
        ({"op": "update", "new": None}, "unsupported"),
        ({"schema_version": "w"}, "unknown_version"),
        ({"old_tuple_kind": 2}, "malformed"),  # FULL without an old image
        ({"new": {"values": ["1"], "kinds": [TEXT]}}, "malformed"),  # width
        ({"new": {"values": ["1", None], "kinds": [TEXT, TEXT]}}, "malformed"),  # TEXT, no value
        ({"new": {"values": ["1", "a"], "kinds": [TEXT, 0]}}, "malformed"),  # ABSENT
        ({"new": {"values": ["1", "a"], "kinds": [TEXT, UNCH]}}, "malformed"),  # value + UNCH
        ({"new": {"values": [None, "a"], "kinds": [UNCH, TEXT]}}, "key_cell"),  # no old image
        ({"new": {"values": ["1", None], "kinds": [TEXT, UNCH]}}, "incomplete_insert"),
    ],
)
def test_each_refusal_is_a_plan_reason(changes, reason):
    con = connect()
    con.register("frames_in", pa.Table.from_pylist([one_frame(**changes)], schema=FRAME_SCHEMA))
    row = plan_row(con, sql_fold.plan_sql, "SELECT * FROM frames_in", {"v": (2, (1,))})
    assert row[4] == reason, row


def test_unknown_record_kinds_are_counted_over_the_whole_page():
    con = connect()
    frames = [one_frame(), one_frame(record_kind=3, frame_ordinal=1, relation_id=OTHER)]
    con.register("frames_in", pa.Table.from_pylist(frames, schema=FRAME_SCHEMA))
    sql = sql_fold.plan_sql("SELECT * FROM frames_in", REL, {"v": (2, (1,))}).replace("{s}", "")
    assert con.execute(sql).fetchone()[3] == 1


# The batched page plan (sql_fold.page_plan_sql) against per-relation plan_sql ---------------
#
# plan_sql is held to the Python fold above; here every relation's row of the batched plan
# must equal what plan_sql returns for that relation alone (all seven decision fields), on
# pages mixing 3-5 relations with their own descriptors (widths, key positions; version
# names deliberately shared across relations), TRUNCATEs listing several of them, and a
# relation with a descriptor but no events.

MULTI_SEEDS = int(os.getenv("CDC_TEST_SQL_FOLD_MULTI_SEEDS", "150"))


def multi_page(seed):
    rng = random.Random(10_000 + seed)
    count = rng.randint(3, 5)
    streams, relations = [], []
    for j in range(count):
        g, version, desc, columns, frames, _ = scenario(seed * 13 + j)
        rel = 1000 + j
        shared = f"v{j % 2}"  # two relations may share a version name
        mine = []
        for f in frames:
            if f["record_kind"] == 1 and f["relation_id"] == REL:
                mine.append(dict(f, relation_id=rel, schema_version=shared))
            elif (
                f["record_kind"] == 1
                and f["op"] == "truncate"
                and REL in (f["truncate_oids"] or [])
            ):
                mine.append(dict(f, truncate_oids=[rel]))
        streams.append(mine)
        relations.append((rel, shared, len(g.names), tuple(g.names.index(k) + 1 for k in g.keys)))
    idle = 1000 + count  # a descriptor, no events
    relations.append((idle, "v0", 2, (1,)))
    merged = []
    while any(streams):
        merged.append(rng.choice([s for s in streams if s]).pop(0))
        if rng.random() < 0.05:  # a TRUNCATE listing several relations
            listed = rng.sample([r[0] for r in relations], rng.randint(2, 3))
            merged.append(
                frame(
                    record_kind=1,
                    source_epoch=1,
                    commit_lsn=merged[-1]["commit_lsn"],
                    commit_ts_us=1,
                    relation_id=0,
                    op="truncate",
                    truncate_oids=listed,
                    old_tuple_kind=0,
                )
            )
    for ordinal, f in enumerate(merged):
        f.update(unit_start_lsn=1000, frame_ordinal=ordinal, event_ordinal=ordinal)
    # Refusals at a low rate (the reviewer's parity probe, SF2): malformed images, an
    # old_tuple_kind mismatch, an unknown version, a NULL op.
    events = [f for f in merged if f["op"] in ("insert", "update", "delete")]
    for f in rng.sample(events, min(len(events), rng.randint(0, 3))):
        kind = rng.choice(
            ["width", "kind_range", "text_no_value", "old_kind", "version", "null_op"]
        )
        image = "new" if f["new"] is not None else "old"
        cells = f[image]
        if kind == "width" and cells:
            f[image] = {"values": cells["values"][:-1], "kinds": cells["kinds"][:-1]}
        elif kind == "kind_range" and cells:
            f[image] = {"values": cells["values"], "kinds": [0] + cells["kinds"][1:]}
        elif kind == "text_no_value" and cells:
            f[image] = {
                "values": [None] + cells["values"][1:],
                "kinds": [TEXT] + cells["kinds"][1:],
            }
        elif kind == "old_kind":
            f["old_tuple_kind"] = 2 if f["old"] is None else 0
        elif kind == "version":
            f["schema_version"] = "nope"
        elif kind == "null_op":
            f["op"] = None
    return relations, merged


def batched_rows(con, frames_sql, relations, plan=sql_fold.page_plan_sql):
    sql = plan(frames_sql, relations).replace("{s}", "")
    rows = con.execute(sql).fetchone()[4] or []
    return {r["relation_id"]: r for r in rows}


def norm_facts(row):
    """The seven decision fields, layouts order-free."""
    events, commits, layouts, block_at, reason, change_at, changes = row
    return (
        events,
        commits,
        sorted(map(tuple, layouts or [])),
        block_at,
        reason,
        change_at,
        changes,
    )


def multi_differential(seeds, plan=sql_fold.page_plan_sql, *, first=False):
    """(coverage hits, failures) over `seeds`. With `first`, stop at the first failing
    seed: a mutant test asks only whether any seed catches it."""
    con = connect()
    failures, hits = [], Counter()
    try:
        for seed in seeds:
            if first and failures:
                break
            relations, frames = multi_page(seed)
            con.register("frames_in", pa.Table.from_pylist(frames, schema=FRAME_SCHEMA))
            frames_sql = "SELECT * FROM frames_in"
            try:
                rows = batched_rows(con, frames_sql, relations, plan)
            except duckdb.Error as exc:
                failures.append(f"seed {seed}: batched plan raised {type(exc).__name__}")
                continue
            for rel in {r[0] for r in relations}:
                versions = {v: (w, kp) for (o, v, w, kp) in relations if o == rel}
                sql = sql_fold.plan_sql(frames_sql, rel, versions).replace("{s}", "")
                single = norm_facts(con.execute(sql).fetchone()[4:])
                got = rows.get(rel)
                if got is None:
                    want_empty = single[0] == 0 and single[3] is None and single[6] == 0
                    if not want_empty:
                        failures.append(f"seed {seed} rel {rel}: no batched row, single {single}")
                    hits["absent_rows"] += 1
                    continue
                batched = norm_facts(
                    (
                        got["events"],
                        got["commits"],
                        got["layouts"],
                        got["block_at"],
                        got["block_reason"],
                        got["change_at"],
                        got["key_changes"],
                    )
                )
                if batched != single:
                    failures.append(f"seed {seed} rel {rel}: {batched} != {single}")
                hits["compared"] += 1
                hits["blocked"] += single[3] is not None
                hits["key_change"] += bool(single[6])
                hits["truncate"] += single[4] == "truncate"
                hits[single[4] or "none"] += 1
    finally:
        con.close()
    return hits, failures


def test_each_relations_batched_row_equals_its_own_plan():
    hits, failures = multi_differential(range(MULTI_SEEDS))
    assert failures == [], failures[:5]
    wanted = (
        "compared",
        "blocked",
        "key_change",
        "truncate",
        "absent_rows",
        "malformed",
        "unknown_version",
        "unsupported",
        "key_cell",
    )
    missing = [b for b in wanted if not hits[b]]
    assert missing == [], (missing, dict(hits))


BATCH_MUTANTS = {
    "join_ignores_relation": _text("ON d.relation_id=e.rel AND d.version", "ON d.version"),
    "truncate_not_exploded": _text("unnest(f.truncate_oids) AS rel", "f.relation_id AS rel"),
    "key_positions_of_another_relation": _text(
        "list_transform(d.kp,", "list_transform((SELECT max(kp) FROM descriptors),"
    ),
    "width_of_another_relation": _text("<>d.width", "<>(SELECT max(width) FROM descriptors)"),
}


@pytest.mark.parametrize("name", sorted(BATCH_MUTANTS))
def test_a_batched_plan_mutant_is_caught(name):
    mutate = BATCH_MUTANTS[name]
    _, failures = multi_differential(
        range(MULTI_SEEDS),
        plan=lambda *a, **k: mutate(sql_fold.page_plan_sql(*a, **k)),
        first=True,
    )
    assert failures, f"batched-plan mutant {name} survived {MULTI_SEEDS} seeds"


def test_only_judges_the_listed_relations():
    """Relations outside `only` (not the run's tables) get no row and can't influence any."""
    con = connect()
    relations, frames = multi_page(3)
    con.register("frames_in", pa.Table.from_pylist(frames, schema=FRAME_SCHEMA))
    keep = {relations[0][0]}
    sql = sql_fold.page_plan_sql("SELECT * FROM frames_in", relations, keep).replace("{s}", "")
    rows = con.execute(sql).fetchone()[4] or []
    assert {r["relation_id"] for r in rows} <= keep
    full = batched_rows(con, "SELECT * FROM frames_in", relations)

    def order_free(row):  # layouts is list(DISTINCT ...): compared as a set, as norm_facts does
        row = dict(row)
        return {**row, "layouts": norm_facts((0, 0, row["layouts"], None, None, None, 0))[2]}

    for r in rows:
        assert order_free(r) == order_free(full[r["relation_id"]])
