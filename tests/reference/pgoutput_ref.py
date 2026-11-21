# FROZEN reference copy of src/cdc_flight/pgoutput.py at 2462253. Never edit.
# Test-only oracle for tests/test_spool_equivalence.py; its sha256 is pinned there.
"""Strict pgoutput v1 decoder. Values remain lossless text, not inferred numbers.

Wire reference: https://www.postgresql.org/docs/18/protocol-logicalrep-message-formats.html
"""

import hashlib
import json
import struct
from dataclasses import dataclass


@dataclass(frozen=True)
class Cell:
    kind: str
    value: str | None = None


@dataclass(frozen=True)
class Relation:
    oid: int
    namespace: str
    name: str
    identity: str
    columns: tuple  # (flags, name, type_oid, typmod)

    @property
    def version(self):
        return hashlib.sha256(json.dumps(self.__dict__, sort_keys=True).encode()).hexdigest()


class Reader:
    def __init__(self, data):
        self.data, self.pos = data, 0

    def take(self, n):
        if n < 0 or self.pos + n > len(self.data):
            raise ValueError("truncated pgoutput message")
        value = self.data[self.pos : self.pos + n]
        self.pos += n
        return value

    def number(self, fmt):
        return struct.unpack("!" + fmt, self.take(struct.calcsize("!" + fmt)))[0]

    def char(self):
        return self.take(1).decode("ascii")

    def string(self):
        end = self.data.find(b"\0", self.pos)
        if end < 0:
            raise ValueError("unterminated string")
        return self.take(end - self.pos + 1)[:-1].decode("utf8")

    def row(self):
        result = []
        for _ in range(self.number("H")):
            kind = self.char()
            if kind == "t":
                result.append(Cell("value", self.take(self.number("I")).decode("utf8")))
            elif kind in ("n", "u"):
                result.append(Cell({"n": "null", "u": "unchanged"}[kind]))
            else:
                raise ValueError(f"unsupported tuple tag {kind!r}; binary mode is disabled")
        return tuple(result)


def decode(data: bytes):
    r = Reader(data)
    tag = r.char()
    if tag == "B":
        out = dict(kind="begin", final_lsn=r.number("Q"), time=r.number("q"), xid=r.number("I"))
    elif tag == "C":
        if r.number("B") != 0:
            raise ValueError("unsupported commit flags")
        out = dict(kind="commit", lsn=r.number("Q"), end=r.number("Q"), time=r.number("q"))
    elif tag == "R":
        oid, namespace, name, identity = r.number("I"), r.string(), r.string(), r.char()
        cols = tuple(
            (r.number("B"), r.string(), r.number("I"), r.number("i")) for _ in range(r.number("H"))
        )
        out = dict(kind="relation", relation=Relation(oid, namespace, name, identity, cols))
    elif tag in ("I", "U", "D"):
        oid, marker = r.number("I"), r.char()
        old = None
        if marker in ("K", "O") and tag in ("U", "D"):
            old = r.row()
            marker = r.char() if tag == "U" else marker
        if tag != "D":
            if marker != "N":
                raise ValueError("expected new tuple")
            new = r.row()
        else:
            if old is None:
                raise ValueError("delete missing old identity")
            new = None
        out = dict(
            kind={"I": "insert", "U": "update", "D": "delete"}[tag], oid=oid, old=old, new=new
        )
    elif tag == "T":
        count, options = r.number("I"), r.number("B")
        out = dict(
            kind="truncate", options=options, relations=tuple(r.number("I") for _ in range(count))
        )
    elif tag == "O":
        out = dict(kind="origin", lsn=r.number("Q"), name=r.string())
    elif tag == "Y":
        out = dict(kind="type", oid=r.number("I"), namespace=r.string(), name=r.string())
    else:
        raise ValueError(f"unsupported pgoutput message {tag!r}; protocol v1 required")
    if r.pos != len(data):
        raise ValueError("trailing pgoutput bytes")
    return out


@dataclass(frozen=True)
class Event:
    source: str
    epoch: int
    commit: int
    end: int
    ordinal: int
    relation: Relation
    op: str
    old: tuple | None
    new: tuple | None

    @property
    def identity(self):
        return self.source, self.epoch, self.commit, self.ordinal


class EventBatch(list):
    """Committed changes plus a commit identity, including for empty transactions."""

    def __init__(self, events, commit_lsn):
        super().__init__(events)
        self.commit_lsn = commit_lsn


class Decoder:
    """Phase 0 bounded transaction decoder; oversize transactions fail explicitly.

    Phase 1 must replace this memory buffer with disk spooling.
    """

    def __init__(self, source="probe", epoch=1, max_bytes=64 * 1024 * 1024):
        self.source, self.epoch, self.max_bytes = source, epoch, max_bytes
        self.relations, self.pending = {}, None
        self.bytes = 0

    def feed(self, wire):
        msg = decode(wire)
        kind = msg["kind"]
        if kind == "begin":
            if self.pending is not None:
                raise ValueError("nested transaction")
            self.pending, self.bytes = [], 0
            self.final_lsn = msg["final_lsn"]
        elif kind == "relation":
            self.relations[msg["relation"].oid] = msg["relation"]
        elif kind in ("insert", "update", "delete", "truncate"):
            if self.pending is None:
                raise ValueError("change outside transaction")
            if kind == "truncate":
                raise ValueError("TRUNCATE requires resync")
            self.bytes += len(wire)
            if self.bytes > self.max_bytes:
                raise ValueError("transaction exceeds Phase 0 memory cap")
            relation = self.relations[msg["oid"]]
            for row in (msg["old"], msg["new"]):
                if row is not None and len(row) != len(relation.columns):
                    raise ValueError("tuple/descriptor width mismatch")
            self.pending.append((relation, msg))
        elif kind == "commit":
            if self.pending is None or msg["end"] <= msg["lsn"] or self.final_lsn != msg["lsn"]:
                raise ValueError("invalid commit")
            events = [
                Event(
                    self.source,
                    self.epoch,
                    msg["lsn"],
                    msg["end"],
                    i,
                    rel,
                    change["kind"],
                    change["old"],
                    change["new"],
                )
                for i, (rel, change) in enumerate(self.pending)
            ]
            self.pending = None
            return msg["end"], EventBatch(events, msg["lsn"])
        return None


def normalize(rows: dict, events: list[Event], keys: tuple[str, ...]):
    """Reference ordered fold; full rows are needed before latest-key reduction.

    Test oracle for REPLICA IDENTITY FULL only: it treats a key-only old tuple's
    NULL placeholders as values. Production apply uses cdc_flight.mirror.fold.

    An old FULL image can supply unchanged TOAST during backfill replay when
    the fuzzy scan already saw a later key change or delete.
    """
    if len({(event.source, event.epoch) for event in events}) > 1:
        raise ValueError("normalization requires one source epoch")
    rows = {key: dict(value) for key, value in rows.items()}
    for event in sorted(events, key=lambda e: (e.commit, e.ordinal)):
        names = [col[1] for col in event.relation.columns]

        def values(row):
            return dict(zip(names, row)) if row is not None else {}

        old, new = values(event.old), values(event.new)

        def key_of(image):
            cells = [image.get(name, Cell("absent")) for name in keys]
            if any(cell.kind != "value" for cell in cells):
                raise ValueError("missing/non-value replica identity")
            return tuple(cell.value for cell in cells)

        new_key = key_of(new) if new else None
        old_key = key_of(old) if old else new_key
        if event.op == "delete":
            rows.pop(old_key, None)
            continue
        base = dict(rows.get(old_key, {})) if event.op == "update" else {}
        for name, cell in old.items():
            if cell.kind in ("value", "null"):
                base[name] = cell.value
        for name, cell in new.items():
            if cell.kind in ("value", "null"):
                base[name] = cell.value
            elif cell.kind == "unchanged" and name not in base:
                raise ValueError("unchanged TOAST without baseline")
        if old_key != new_key:
            rows.pop(old_key, None)
        rows[new_key] = base
    return rows


def event_dict(event):
    from dataclasses import asdict

    return asdict(event)


def event_from_dict(value):
    value = dict(value)
    relation = dict(value["relation"])
    relation["columns"] = tuple(tuple(col) for col in relation["columns"])
    value["relation"] = Relation(**relation)
    for key in ("old", "new"):
        if value[key] is not None:
            value[key] = tuple(Cell(**cell) for cell in value[key])
    return Event(**value)
