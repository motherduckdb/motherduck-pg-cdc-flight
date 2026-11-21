"""Format 2: the same WIRE stream materializes the same mirror.

Each stream is real pgoutput (built with pgstream's encoders) and is fed, byte for byte,
to both spools:
- v1: frozen reference.spool_ref.Spool -> chunk rows (payload JSON) -> apply's events (json.loads, full_old
  from the descriptor's replica identity; tests/reference/payload_v1_ref) -> the frozen
  mirror_ref.fold;
- v2: spool2.Spool -> chunk rows -> spool2.logical_events -> mirror2.fold, with names
  from Spool2's own descriptors.
Each result is written with the unchanged product code (apply.create_mirror_table,
Apply.stage, Apply._merge with Apply._write's precheck rule) into its own copy of one
baseline mirror, and every column is compared, including the per-row checkpoint
(_cdc_commit_lsn) and _cdc_updated_at, as is the outcome when fold or the MERGE
precheck blocks. The table-level applied checkpoint is the caller's, not fold's.

One in-memory DuckDB connection serves every stream: per stream and relation, two
tables, two stages, two MERGEs, then DROP. Nothing here changes product or timed code.
"""

import json
import random
import tempfile
from types import SimpleNamespace

import duckdb
import pgstream
import pytest
from reference import mirror_ref, payload_v1_ref, pgoutput_ref, spool_ref
from test_format2_protocol import binding, config

from cdc_flight import mirror, mirror2
from cdc_flight.apply import Apply, create_mirror_table
from cdc_flight.format2 import Chunk as Chunk2
from cdc_flight.mirror import descriptor_columns
from cdc_flight.spool2 import Spool as Spool2
from cdc_flight.spool2 import logical_events

# Independent historical oracle: never run the v1 arm through active decoder code.
spool_ref.decode = pgoutput_ref.decode
Chunk1, Spool1 = spool_ref.Chunk, spool_ref.Spool

SEEDS = 200
FIRST_LSN = 0x10_0000_0000
U = pgstream.UNCHANGED


class Wire:
    """pgoutput transactions for a few relations of text columns, LSNs only growing."""

    def __init__(self, identity, keys=("id",), extra=1, oids=(101,)):
        self.identity, self.keys, self.oids = identity, tuple(keys), tuple(oids)
        self.columns = {oid: list(self.keys) + [f"c{i}" for i in range(extra)] for oid in oids}
        self.messages, self.sent = [], set()
        self.lsn, self.xid, self.time = FIRST_LSN, 5000, 800_000_000_000_000

    def add_column(self, oid, name="added"):
        self.columns[oid].append(name)
        self.sent.discard(oid)  # pgoutput re-sends Relation before the next change

    def relation(self, oid):
        columns = [(1 if n in self.keys else 0, n, 25, -1) for n in self.columns[oid]]
        return pgstream.relation(oid, "public", f"t{oid}", self.identity, columns)

    def transaction(self, changes):
        """changes: (oid or oids, message bytes); Relation messages are added as needed."""
        body = []
        for oids, message in changes:
            for oid in oids if isinstance(oids, tuple) else (oids,):
                if oid not in self.sent:
                    body.append(self.relation(oid))
                    self.sent.add(oid)
            body.append(message)
        self.xid += 1
        self.time += 1000
        final = self.lsn + sum(len(m) + 24 for m in body) + 8
        end = final + 48
        self.lsn = end + 8
        self.messages += [pgstream.begin(final, self.time, self.xid), *body]
        self.messages.append(pgstream.commit(final, end, self.time))
        return self

    def names(self, oid):
        return self.columns[oid]


def v1_rows(messages, key_columns):
    s = Spool1(config(), key_columns, 900, "marker")
    rows, descriptors = [], {}
    for message in messages:
        for item in s.feed(message):
            if isinstance(item, Chunk1):
                rows += item.table.to_pylist()
                for oid, version, descriptor in item.descriptors:
                    descriptors[(oid, version)] = json.loads(descriptor)
    return rows, descriptors


def v2_rows(messages, key_columns):
    s = Spool2(
        config(),
        key_columns,
        900,
        "marker",
        binding=binding(),
        start_lsn=FIRST_LSN - 8,
        clock=lambda: 0.0,
    )
    out = []
    for message in messages:
        out += s.feed(message)
    out += s.close()
    rows, descriptors = [], {}
    for item in out:
        if isinstance(item, Chunk2):
            rows += item.table.to_pylist()
            for d in item.descriptors:
                descriptors[(d.oid, d.version)] = tuple(d.names)
    return rows, descriptors


class Mirrors:
    """Two copies of one baseline mirror (one per path); written, compared, dropped."""

    def __init__(self, con, n, keys, names, baseline):
        self.con, self.keys, self.names = con, keys, names
        self.columns = [mirror.Column(name, "VARCHAR", 25, -1) for name in names]
        self.targets = {path: f'"memory"."public"."m_{path}_{n}"' for path in ("v1", "v2")}
        rows = [
            [
                *key,
                *(
                    None if c == "added" else f"base_{c}_{'_'.join(key)}"
                    for c in names[len(keys) :]
                ),
            ]
            for key in baseline
        ]
        for target in self.targets.values():
            create_mirror_table(
                con, "memory", dict(keys=keys, schema="public", target=target), self.columns
            )
            if rows:
                con.executemany(
                    f"INSERT INTO {target} VALUES ({','.join('?' * (len(names) + 2))})",
                    [[*row, 1, "2026-01-01 00:00:00+00"] for row in rows],
                )

    def write(self, path, images):
        """Apply._write's stage and MERGE for these images (no lease, guard or state)."""
        if not images:
            return
        owner = SimpleNamespace(con=self.con)
        with tempfile.TemporaryDirectory(prefix="cdc_g2_") as directory:
            staged = Apply.stage(owner, directory, self.keys, self.columns, images)
            partial = any(i.op == "upsert" and not i.complete for i in images.values())
            Apply._merge(
                owner, self.targets[path], self.keys, self.columns, staged, precheck=partial
            )

    def rows(self, path):
        order = ",".join(f'"{k}"' for k in self.keys)
        # epoch_us: exact, and no tz-aware Python conversion (pytz isn't a dependency).
        return self.con.execute(
            "SELECT * REPLACE (epoch_us(_cdc_updated_at) AS _cdc_updated_at) "
            f"FROM {self.targets[path]} ORDER BY {order}"
        ).fetchall()

    def drop(self):
        for target in self.targets.values():
            self.con.execute(f"DROP TABLE {target}")


def materialize(mirrors, path, fold):
    try:
        mirrors.write(path, fold())
    except (mirror.Blocked, mirror_ref.Blocked) as exc:
        return ("blocked", exc.state, str(exc), exc.commit_lsn)
    return ("rows", mirrors.rows(path))


def both(con, n, wire, oid, baseline):
    """Feed the wire to both spools; fold and materialize relation `oid` both ways."""
    key_columns = {o: wire.keys for o in wire.oids}
    rows1, descriptors1 = v1_rows(wire.messages, key_columns)
    rows2, descriptors2 = v2_rows(wire.messages, key_columns)
    mine = [r for r in rows1 if r["relation_id"] == oid]
    versions = [r["schema_version"] for r in mine if r["op"] != "truncate"]
    names = (
        [c.name for c in descriptor_columns(descriptors1[(oid, versions[-1])])]
        if versions
        else list(wire.names(oid))
    )
    events1 = [
        payload_v1_ref.event(
            r["commit_lsn"],
            r["event_ordinal"],
            r["commit_ts_us"],
            r["op"],
            r["schema_version"],
            r["payload"],
            descriptors1[(oid, r["schema_version"])]["identity"] if r["op"] != "truncate" else None,
        )
        for r in mine
    ]
    events2 = [e for e in logical_events(rows2) if e["relation_id"] == oid]
    mirrors = Mirrors(con, n, wire.keys, names, baseline)
    try:
        v1 = materialize(mirrors, "v1", lambda: mirror_ref.fold({}, events1, wire.keys, names))
        v2 = materialize(
            mirrors,
            "v2",
            lambda: mirror2.fold(
                {}, events2, wire.keys, names, descriptors2, float_keys=frozenset()
            ),
        )
    finally:
        mirrors.drop()
    return v1, v2


@pytest.fixture(scope="module")
def con():
    connection = duckdb.connect()
    connection.execute("SET TimeZone='UTC'")
    yield connection
    connection.close()


def fuzz(seed):
    """An identity-faithful stream: FULL sends O on update and delete; DEFAULT sends K
    only when the key changes, and always on delete. A small key space so keys recur."""
    rng = random.Random(seed)
    identity = rng.choice("fd")
    keys = ("id",) if rng.random() < 0.7 else ("id", "id2")
    wire = Wire(identity, keys, extra=rng.randint(1, 3))
    oid = 101

    def key():
        return [str(rng.randint(0, 3)) for _ in keys]

    def row(k, unchanged=0.0):
        out = list(k)
        for _ in wire.names(oid)[len(keys) :]:
            roll = rng.random()
            out.append(
                U
                if roll < unchanged
                else None
                if roll < unchanged + 0.15
                else f"v{rng.randint(0, 99)}"
            )
        return out

    def key_only(k):
        return list(k) + [None] * (len(wire.names(oid)) - len(keys))

    for _ in range(rng.randint(1, 8)):
        # ADD COLUMN takes effect between transactions here: every change of a
        # transaction is encoded at the width of the Relation sent before it.
        if rng.random() < 0.15 and "added" not in wire.names(oid):
            wire.add_column(oid)
        changes = []
        for _ in range(rng.randint(1, 4)):
            k, roll = key(), rng.random()
            if roll < 0.02:
                changes.append(((oid,), pgstream.truncate([oid])))
            elif roll < 0.35:
                new = row(k)
                if rng.random() < 0.04:
                    new[-1] = U  # an insert can't carry UNCHANGED: blocked
                changes.append((oid, pgstream.insert(oid, new)))
            elif roll < 0.85:
                new_key = key() if rng.random() < 0.2 else k
                new = row(new_key, unchanged=0.3)
                if rng.random() < 0.02:
                    new[0] = U  # an unchanged identity cell: blocked
                if identity == "f":
                    message = pgstream.update(oid, new, row(k), "O")
                elif new_key != k:
                    message = pgstream.update(oid, new, key_only(k), "K")
                else:
                    message = pgstream.update(oid, new)
                changes.append((oid, message))
            elif identity == "f":
                changes.append((oid, pgstream.delete(oid, row(k), "O")))
            else:
                changes.append((oid, pgstream.delete(oid, key_only(k), "K")))
        wire.transaction(changes)
    space = [tuple(str(a) for a in combo) for combo in _space(len(keys))]
    baseline = [k for k in space if rng.random() < 0.75]  # some keys absent: precheck
    return wire, oid, baseline


def _space(width):
    return [(a,) for a in range(4)] if width == 1 else [(a, b) for a in range(4) for b in range(4)]


def test_fuzzed_wire_streams_materialize_identical_mirrors(con):
    seen = {"rows": 0, "blocked": 0}
    for seed in range(SEEDS):
        wire, oid, baseline = fuzz(seed)
        v1, v2 = both(con, seed, wire, oid, baseline)
        assert v2 == v1, seed
        seen[v1[0]] += 1
    assert seen["rows"] > 60 and seen["blocked"] > 20, seen


# Named cases: one key column "id", one value column "c0" (+ "added" where stated); the
# baseline mirror holds keys 0, 1 and 2, so key 3 is absent.
BASELINE = [("0",), ("1",), ("2",)]


def named(con, n, wire, oid=101, kind="rows"):
    v1, v2 = both(con, 1000 + n, wire, oid, BASELINE)
    assert v2 == v1
    assert v1[0] == kind, v1
    return {row[0]: row for row in v1[1]} if kind == "rows" else v1


def test_named_absent_column_of_an_older_version_folds_to_null(con):
    wire = Wire("f").transaction([(101, pgstream.update(101, ["1", "new"], ["1", "old"], "O"))])
    wire.add_column(101)
    wire.transaction([(101, pgstream.insert(101, ["2", "x", "set"]))])
    rows = named(con, 1, wire)
    assert rows["1"][:3] == ("1", "new", None)  # "added" didn't exist in that event's version
    assert rows["2"][:3] == ("2", "x", "set")


def test_named_explicit_null_replaces_a_value(con):
    """NULL is a value: distinct from ABSENT (a column the event's version lacks)."""
    wire = Wire("d").transaction([(101, pgstream.update(101, ["1", None]))])
    rows = named(con, 2, wire)
    assert rows["1"][1] is None and rows["0"][1] == "base_c0_0"


def test_named_unchanged_toast_keeps_the_mirror_value(con):
    wire = Wire("d").transaction([(101, pgstream.update(101, ["0", U]))])
    assert named(con, 3, wire)["0"][1] == "base_c0_0"


def test_named_unchanged_toast_over_a_missing_row_blocks_in_the_precheck(con):
    wire = Wire("d").transaction([(101, pgstream.update(101, ["3", U]))])
    outcome = named(con, 4, wire, kind="blocked")
    assert outcome[1] == "needs_action" and "lacks a full image" in outcome[2]


def test_named_full_old_image_fills_unchanged_cells(con):
    wire = Wire("f").transaction([(101, pgstream.update(101, ["1", U], ["1", "from_old"], "O"))])
    assert named(con, 5, wire)["1"][1] == "from_old"


def test_named_key_only_delete(con):
    wire = Wire("d").transaction([(101, pgstream.delete(101, ["2", None], "K"))])
    assert set(named(con, 6, wire)) == {"0", "1"}


def test_named_complete_key_change_moves_the_row(con):
    wire = Wire("d").transaction([(101, pgstream.update(101, ["5", "moved"], ["0", None], "K"))])
    rows = named(con, 7, wire)
    assert "0" not in rows and rows["5"][1] == "moved"


def test_named_incomplete_key_change_blocks(con):
    wire = Wire("d").transaction([(101, pgstream.update(101, ["5", U], ["0", None], "K"))])
    assert "key change" in named(con, 8, wire, kind="blocked")[2]


def test_named_key_change_and_revert_in_one_batch(con):
    wire = Wire("d").transaction(
        [
            (101, pgstream.update(101, ["5", "away"], ["0", None], "K")),
            (101, pgstream.update(101, ["0", "back"], ["5", None], "K")),
        ]
    )
    rows = named(con, 9, wire)
    assert rows["0"][1] == "back" and "5" not in rows


def test_named_multi_relation_truncate_blocks_both_before_any_write(con):
    """One physical TRUNCATE frame of two relations: each relation's fold blocks."""
    wire = Wire("d", oids=(101, 102)).transaction(
        [
            (101, pgstream.insert(101, ["7", "a"])),
            (102, pgstream.insert(102, ["8", "b"])),
            ((101, 102), pgstream.truncate([101, 102])),
        ]
    )
    for n, oid in ((10, 101), (11, 102)):
        outcome = named(con, n, wire, oid=oid, kind="blocked")
        assert outcome[1] == "needs_resync"


def test_named_insert_then_delete_leaves_no_row(con):
    wire = Wire("d").transaction(
        [
            (101, pgstream.insert(101, ["7", "brief"])),
            (101, pgstream.delete(101, ["7", None], "K")),
        ]
    )
    assert set(named(con, 12, wire)) == {"0", "1", "2"}


def test_named_delete_of_a_key_absent_from_the_mirror_is_a_no_op(con):
    """A delete never trips the precheck (only partial upserts do) and leaves
    the mirror as it was."""
    wire = Wire("d").transaction([(101, pgstream.delete(101, ["3", None], "K"))])
    rows = named(con, 13, wire)
    assert set(rows) == {"0", "1", "2"} and rows["0"][1] == "base_c0_0"
