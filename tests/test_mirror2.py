"""Format 2: mirror2 presents structured cells to the unchanged mirror.fold.

The differential: seeded event streams, generated as pgoutput would send them for each
replica identity, go through format 1's frozen path (cells -> canonical payload JSON ->
json.loads -> the frozen fold, with full_old from the identity) and through
mirror2.fold (positional cells, full_old from old_tuple_kind). The outcomes must be equal:
the same per-key images, or the same Blocked (state, reason, commit).
"""

import dataclasses
import hashlib
import random
from pathlib import Path

import pytest
from reference import mirror_ref, payload_v1_ref

from cdc_flight import mirror, mirror2
from cdc_flight.mirror2 import NULL, OLD_FULL, OLD_KEY, OLD_NONE, TEXT, UNCHANGED, Malformed

REFERENCE = Path(__file__).parent / "reference"
PINNED = {
    "mirror_ref.py": "21c69bf9f28656afc06bdfe82e99c30a065617d809670b7c07c7328fd167b81a",
    "payload_v1_ref.py": "4291eefd9f11b413657034c4c7b86bbabdcb2a772c705632fa54fa95449b116c",
}
V1_KIND = {TEXT: "value", NULL: "null", UNCHANGED: "unchanged"}
RELATION = 16384


def test_the_references_are_frozen():
    for name, digest in PINNED.items():
        assert hashlib.sha256((REFERENCE / name).read_bytes()).hexdigest() == digest, name


class Stream:
    """One table's events, built once and rendered for both paths."""

    def __init__(self, rng):
        self.rng = rng
        width = rng.randint(2, 6)
        self.keys = tuple(f"c{i}" for i in range(rng.choice((1, 1, 2))))
        self.identity = rng.choice(("f", "d"))
        v1 = tuple(f"c{i}" for i in range(width))
        self.versions = {"v1": v1, "v2": v1 + ("added",)}  # ADD COLUMN: appended
        self.events = []  # (op, version, old_kind, old cells, new cells)
        self.features = set()

    def value(self):
        return self.rng.choice(("a", "b", "", "ü", "x" * self.rng.randint(1, 4)))

    def key_value(self):
        return str(self.rng.randint(0, 3))  # a small key space, so keys recur

    def row(self, names, key, unchanged=0.0, nulls=0.15):
        out = []
        for name in names:
            if name in self.keys:
                out.append((TEXT, key[self.keys.index(name)]))
            elif self.rng.random() < unchanged:
                out.append((UNCHANGED, None))
            elif self.rng.random() < nulls:
                out.append((NULL, None))
            else:
                out.append((TEXT, self.value()))
        return out

    def key_only(self, names, key):
        # A K tuple: the key's values, NULL placeholders elsewhere (as pgoutput sends).
        return [(TEXT, key[self.keys.index(n)]) if n in self.keys else (NULL, None) for n in names]

    def build(self, count):
        version = "v1"
        for _ in range(count):
            if version == "v1" and self.rng.random() < 0.08:
                version = "v2"
                self.features.add("added column")
            names = self.versions[version]
            key = tuple(self.key_value() for _ in self.keys)
            roll = self.rng.random()
            if roll < 0.02:
                self.events.append(("truncate", version, OLD_NONE, None, None))
                self.features.add("truncate")
            elif roll < 0.35:
                new = self.row(names, key)
                if self.rng.random() < 0.04:  # an insert can't carry UNCHANGED: blocked
                    i = self.rng.randrange(len(names))
                    new[i] = (UNCHANGED, None)
                self.events.append(("insert", version, OLD_NONE, None, new))
            elif roll < 0.85:
                changed = self.rng.random() < 0.2
                new_key = tuple(self.key_value() for _ in self.keys) if changed else key
                new = self.row(names, new_key, unchanged=0.3)
                if self.rng.random() < 0.02:  # an unchanged identity cell: blocked
                    new[names.index(self.keys[0])] = (UNCHANGED, None)
                if self.identity == "f":
                    old_kind, old = OLD_FULL, self.row(names, key)
                elif new_key != key:
                    old_kind, old = OLD_KEY, self.key_only(names, key)
                else:
                    old_kind, old = OLD_NONE, None
                self.events.append(("update", version, old_kind, old, new))
                if changed:
                    self.features.add("key change")
                if any(kind == UNCHANGED for kind, _ in new):
                    self.features.add("unchanged")
            else:
                if self.identity == "f":
                    old_kind, old = OLD_FULL, self.row(names, key)
                else:
                    old_kind, old = OLD_KEY, self.key_only(names, key)
                self.events.append(("delete", version, old_kind, old, None))
        return self

    def mirror_names(self):
        used = {version for _, version, *_ in self.events}
        return list(self.versions["v2" if "v2" in used else "v1"])

    def v1(self):
        out = []
        for ordinal, (op, version, _kind, old, new) in enumerate(self.events):
            names = self.versions[version]
            as_v1 = lambda row: None if row is None else [(V1_KIND[k], v) for k, v in row]  # noqa: E731
            text = payload_v1_ref.payload(as_v1(old), as_v1(new), names)
            out.append(
                payload_v1_ref.event(
                    100 + ordinal, ordinal, 1000 + ordinal, op, version, text, self.identity
                )
            )
        return out

    def v2(self):
        out = []
        for ordinal, (op, version, old_kind, old, new) in enumerate(self.events):
            structured = lambda row: (  # noqa: E731
                None
                if row is None
                else {"values": [v for _, v in row], "kinds": [k for k, _ in row]}
            )
            out.append(
                dict(
                    source_epoch=1,
                    commit_lsn=100 + ordinal,
                    event_ordinal=ordinal,
                    commit_ts_us=1000 + ordinal,
                    txid=7,
                    relation_id=RELATION,
                    schema_name="public",
                    table_name="t",
                    schema_version=version,
                    op=op,
                    old_tuple_kind=old_kind,
                    old=structured(old),
                    new=structured(new),
                )
            )
        return out

    def descriptors(self):
        return {(RELATION, version): names for version, names in self.versions.items()}


def outcome(run):
    try:
        state = run()
    except (mirror.Blocked, mirror_ref.Blocked) as exc:
        return ("blocked", exc.state, str(exc), exc.commit_lsn)
    return ("images", {key: dataclasses.asdict(image) for key, image in state.items()})


def test_the_adapter_folds_exactly_as_format_1():
    """At least 2,000 seeded streams; both outcomes and every feature well covered."""
    seen = {"images": 0, "blocked": 0}
    features = set()
    for seed in range(2000):
        s = Stream(random.Random(seed)).build(random.Random(seed).randint(1, 40))
        names = s.mirror_names()
        expected = outcome(lambda: mirror_ref.fold({}, s.v1(), s.keys, names))
        actual = outcome(
            lambda: mirror2.fold({}, s.v2(), s.keys, names, s.descriptors(), float_keys=frozenset())
        )
        assert actual == expected, seed
        seen[expected[0]] += 1
        features |= s.features | {s.identity}
    assert seen["images"] > 500 and seen["blocked"] > 200, seen
    assert features >= {"added column", "truncate", "key change", "unchanged", "f", "d"}


def event(**fields):
    base = dict(
        commit_lsn=5,
        event_ordinal=0,
        commit_ts_us=9,
        txid=7,
        relation_id=RELATION,
        schema_version="v1",
        op="insert",
        old_tuple_kind=OLD_NONE,
        old=None,
        new={"values": ["1", "x"], "kinds": [TEXT, TEXT]},
    )
    return dict(base, **fields)


DESCRIPTORS = {(RELATION, "v1"): ("c0", "c1")}


@pytest.mark.parametrize(
    "fields",
    [
        dict(new={"values": ["1", None], "kinds": [TEXT, 0]}),  # ABSENT in a row image
        dict(new={"values": ["1", None], "kinds": [TEXT, 9]}),  # an unknown tag
        dict(new={"values": ["1"], "kinds": [TEXT]}),  # narrower than its descriptor
        dict(op="update", old_tuple_kind=OLD_KEY, old=None),  # K without an old image
        dict(
            op="update", old_tuple_kind=OLD_NONE, old={"values": ["1", None], "kinds": [TEXT, NULL]}
        ),
        dict(old_tuple_kind=5),
        dict(new={"values": ["1", None], "kinds": [TEXT, TEXT]}),  # G1: TEXT without a value
        dict(new={"values": ["1", "x"], "kinds": [TEXT, NULL]}),  # a NULL carrying a value
    ],
)
def test_malformed_events_are_rejected(fields):
    with pytest.raises(Malformed):
        mirror2.fold(
            {}, [event(**fields)], ("c0",), ["c0", "c1"], DESCRIPTORS, float_keys=frozenset()
        )


def test_shared_protocol_codes_satisfy_the_access_layer_invariants():
    from cdc_flight import format2

    # cells() uses a contiguous non-ABSENT range, not just equality of aliases.
    assert {NULL, UNCHANGED, TEXT} == set(range(NULL, TEXT + 1))
    assert format2.ABSENT not in range(NULL, TEXT + 1)
    assert (format2.NO_OLD, OLD_KEY, OLD_FULL) == (0, 1, 2)
    assert len({OLD_NONE, OLD_KEY, OLD_FULL}) == 3


def test_a_missing_descriptor_is_never_guessed():
    with pytest.raises(KeyError):
        mirror2.fold(
            {},
            [event(schema_version="v9")],
            ("c0",),
            ["c0", "c1"],
            DESCRIPTORS,
            float_keys=frozenset(),
        )


def test_truncate_blocks_without_a_descriptor():
    with pytest.raises(mirror.Blocked) as exc:
        mirror2.fold(
            {},
            [event(op="truncate", schema_version=None, new=None)],
            ("c0",),
            ["c0"],
            {},
            float_keys=frozenset(),
        )
    assert exc.value.state == "needs_resync" and exc.value.commit_lsn == 5


def test_the_access_is_lazy_and_the_envelope_passes_through():
    row = event(new={"values": ["1", None, None], "kinds": [TEXT, NULL, UNCHANGED]})
    names = ("c0", "c1", "c2")
    presented = mirror2.to_v1(row, names)
    new = presented["payload"]["new"]
    assert isinstance(new, mirror2.Cells)  # no per-image dict
    assert new["c1"] is mirror2._NULL and new["c2"] is mirror2._UNCHANGED  # shared constants
    with pytest.raises(TypeError):
        mirror2._NULL["value"] = "x"  # read-only, so a shared cell can't be corrupted
    assert new["c0"] == {"kind": "value", "value": "1"} and "c3" not in new
    assert dict(new.items()) == {n: new[n] for n in names}
    assert {k: presented[k] for k in ("event_ordinal", "txid", "commit_lsn")} == dict(
        event_ordinal=0, txid=7, commit_lsn=5
    )
    assert presented["full_old"] is False
