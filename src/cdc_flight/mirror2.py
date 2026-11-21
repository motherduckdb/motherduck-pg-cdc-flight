"""Format 2: structured-cell access for the existing fold.

`mirror.fold` stays the single source of the fold's semantics (unchanged TOAST
inheritance, key-only versus full old images, absent columns of an older schema version,
key changes, the blocked cases). This module only presents format 2's positional cells
(`{values: list[str|None], kinds: list[int]}`, in the descriptor's original column order)
as the name-keyed mapping `mirror.fold` reads, lazily: no per-image dict is rebuilt.
NULL and UNCHANGED cells are shared read-only constants, and a TEXT cell is built only
when the fold reads it.

Source row images carry only TEXT, NULL and UNCHANGED. A key-only old tuple keeps NULL
placeholders for non-key columns, as v1 does; that's safe because fold reads an old image's
non-key cells only when full_old is set (mirror.py, the `is_old and not full_old` skip),
and a K tuple presents full_old=False. ABSENT never originates from the wire in a row
image, so one there is malformed and rejected, as is a TEXT cell without a value. `full_old` is
`old_tuple_kind == OLD_FULL`. v1 took it from the descriptor's replica identity; on the
wire a FULL identity always sends an O tuple when there is an old tuple, and without one
`full_old` has no effect, so the two agree (the differential test proves it).
"""

from collections.abc import Mapping
from types import MappingProxyType

from cdc_flight import mirror
from cdc_flight.format2 import ABSENT as ABSENT
from cdc_flight.format2 import NO_OLD as OLD_NONE
from cdc_flight.format2 import NULL, OLD_FULL, OLD_KEY, TEXT
from cdc_flight.format2 import UNCHANGED as UNCHANGED

# The fold reads cells and never mutates them; read-only, so it can't start to.
_NULL = MappingProxyType({"kind": "null", "value": None})
_UNCHANGED = MappingProxyType({"kind": "unchanged", "value": None})


class Malformed(ValueError):
    """A structured event that can't come from a valid wire stream."""


class Cells(Mapping):
    """One row image, keyed by column name, over positional values and tags."""

    __slots__ = ("_names", "_positions", "_values", "_kinds")

    def __init__(self, names, positions, values, kinds):
        self._names, self._positions = names, positions
        self._values, self._kinds = values, kinds

    def __getitem__(self, name):
        i = self._positions[name]
        kind = self._kinds[i]
        if kind == TEXT:
            return {"kind": "value", "value": self._values[i]}
        return _NULL if kind == NULL else _UNCHANGED

    def __contains__(self, name):
        return name in self._positions

    def __iter__(self):
        return iter(self._names)

    def __len__(self):
        return len(self._names)

    def items(self):
        for name, value, kind in zip(self._names, self._values, self._kinds):
            if kind == TEXT:
                yield name, {"kind": "value", "value": value}
            else:
                yield name, _NULL if kind == NULL else _UNCHANGED


def cells(image, names, positions):
    """A row image as Cells, or None; rejects a malformed image."""
    if image is None:
        return None
    values, kinds = image["values"], image["kinds"]
    if len(values) != len(names) or len(kinds) != len(names):
        raise Malformed("row image width differs from its descriptor")
    if kinds and (min(kinds) < NULL or max(kinds) > TEXT):
        raise Malformed("ABSENT or an unknown tag in a source row image")
    # A TEXT cell must carry its value (None would stage as NULL). C-level counts: as many
    # Nones as non-TEXT cells. A TEXT None offset by a non-TEXT cell carrying a value
    # still balances; non-TEXT values are never read here, and format 2's writer-side
    # validation rejects that pairing.
    if values.count(None) != len(values) - kinds.count(TEXT):
        raise Malformed("a TEXT cell without a value, or a NULL/UNCHANGED cell with one")
    return Cells(names, positions, values, kinds)


def to_v1(event, names, positions=None):
    """One logical event as the dict mirror.fold reads. The envelope (commit_lsn,
    event_ordinal, commit_ts_us, txid, relation_id, ..., op) passes through unchanged."""
    if positions is None:
        positions = {name: i for i, name in enumerate(names)}
    kind = event["old_tuple_kind"]
    if kind not in (OLD_NONE, OLD_KEY, OLD_FULL):
        raise Malformed(f"unknown old_tuple_kind {kind!r}")
    old = cells(event["old"], names, positions)
    if (old is None) != (kind == OLD_NONE):
        raise Malformed("old_tuple_kind disagrees with the old image")
    new = cells(event["new"], names, positions)
    return dict(event, full_old=kind == OLD_FULL, payload={"old": old, "new": new})


def fold(state, events, keys, mirror_names, descriptors, *, float_keys):
    """mirror.fold over format 2's logical table events (no COMMIT or control rows; a
    TRUNCATE already expanded per relation, which fold blocks as before).

    `descriptors`: (relation_id, schema_version) -> the column names in the source's
    original order. A missing entry raises KeyError; it is never guessed."""
    layouts = {}

    def presented():
        for event in events:
            if event["op"] == "truncate":
                yield dict(event, full_old=False, payload={"old": None, "new": None})
                continue
            key = (event["relation_id"], event["schema_version"])
            layout = layouts.get(key)
            if layout is None:
                names = tuple(descriptors[key])
                layout = layouts[key] = (names, {name: i for i, name in enumerate(names)})
            yield to_v1(event, *layout)

    return mirror.fold(state, presented(), keys, mirror_names, float_keys=float_keys)
