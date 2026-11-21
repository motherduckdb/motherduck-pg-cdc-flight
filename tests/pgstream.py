"""pgoutput v1 message builders and a seeded, structure-aware stream generator for the
spool/decoder equivalence tests (tests/test_spool_equivalence.py).

Streams are built from valid transactions over several relations, then optionally
mutated the way real faults look: truncation, a flipped tag or length byte, reordered,
dropped or duplicated messages, and swapped relation OIDs. Random bytes alone would die
at the first tag and never reach the deep paths.
"""

import random
import struct

UNCHANGED = object()  # a 'u' cell: unchanged TOAST


def _string(value):
    return (value if isinstance(value, bytes) else value.encode()) + b"\0"


def begin(final_lsn, time_us, xid):
    return b"B" + struct.pack("!QqI", final_lsn, time_us, xid)


def commit(lsn, end, time_us, flags=0):
    return b"C" + struct.pack("!BQQq", flags, lsn, end, time_us)


def relation(oid, namespace, name, identity, columns):
    out = [b"R", struct.pack("!I", oid), _string(namespace), _string(name), identity.encode()]
    out.append(struct.pack("!H", len(columns)))
    for flags, column, type_oid, typmod in columns:
        out += [struct.pack("!B", flags), _string(column), struct.pack("!Ii", type_oid, typmod)]
    return b"".join(out)


def tuple_data(cells):
    out = [struct.pack("!H", len(cells))]
    for cell in cells:
        if cell is None:
            out.append(b"n")
        elif cell is UNCHANGED:
            out.append(b"u")
        else:
            data = cell if isinstance(cell, bytes) else cell.encode()
            out += [b"t", struct.pack("!I", len(data)), data]
    return b"".join(out)


def insert(oid, new):
    return b"I" + struct.pack("!I", oid) + b"N" + tuple_data(new)


def update(oid, new, old=None, identity="O"):
    head = b"U" + struct.pack("!I", oid)
    if old is not None:
        head += identity.encode() + tuple_data(old)
    return head + b"N" + tuple_data(new)


def delete(oid, old, identity="O"):
    return b"D" + struct.pack("!I", oid) + identity.encode() + tuple_data(old)


def truncate(oids, options=0):
    return (
        b"T" + struct.pack("!IB", len(oids), options) + b"".join(struct.pack("!I", o) for o in oids)
    )


TEXTS = [
    "plain",
    "",
    'quote " and backslash \\',
    "newline\nand\ttab",
    "unicode é ü 中文 🦆",
    "control \x01\x1f",
    '{"looks": ["like json"]}',
    "x" * 300,
]


class Generator:
    """Valid pgoutput transactions over a few relations, with LSNs that only grow.

    `key_columns` and `heartbeat_oid` describe what a Spool is given; the stream may
    include a relation whose key column is absent, and a schema change mid-stream (a
    re-sent Relation with different columns for the same OID)."""

    HEARTBEAT = 900

    def __init__(self, seed):
        self.rng = random.Random(seed)
        self.lsn, self.xid, self.time = 0x10_0000_0000, 5000, 800_000_000_000_000
        self.relations = {}
        self.key_columns = {}
        self.marker = "m" * 32
        for oid in (101, 102, 103):
            self._define(oid)
        self._define(self.HEARTBEAT, columns=[(1, "id", 25, -1), (0, "marker", 25, -1)])
        self.key_columns[self.HEARTBEAT] = ("id",)
        self.sent = set()

    def _define(self, oid, columns=None):
        rng = self.rng
        if columns is None:
            count = rng.randint(1, 6)
            columns = [(1, "id", 20, -1)] + [
                (rng.choice((0, 1)), f"c{k}_{rng.randint(0, 9)}", rng.choice((25, 23, 20, 16)), -1)
                for k in range(count - 1)
            ]
            # One relation's key names a column it lacks: the spool records "absent".
            keys = ("id",) if oid != 103 else ("id", "missing")
            self.key_columns[oid] = keys
        self.relations[oid] = (f"ns{oid}", f"t{oid}", self.rng.choice("dfni"), columns)

    def _relation_message(self, oid):
        namespace, name, identity, columns = self.relations[oid]
        return relation(oid, namespace, name, identity, columns)

    def _cells(self, oid, allow_unchanged=False):
        columns = self.relations[oid][3]
        out = []
        for _ in columns:
            roll = self.rng.random()
            if roll < 0.1:
                out.append(None)
            elif roll < 0.18 and allow_unchanged:
                out.append(UNCHANGED)
            else:
                out.append(self.rng.choice(TEXTS) + str(self.rng.randint(0, 10**6)))
        return out

    def transaction(self):
        rng = self.rng
        changes, relations = [], []
        kind = rng.random()
        if kind < 0.05:
            pass  # an empty transaction
        else:
            for _ in range(rng.randint(1, 6)):
                oid = rng.choice((101, 102, 103))
                if rng.random() < 0.05:
                    self._define(oid)  # schema change: the next change re-sends Relation
                    self.sent.discard(oid)
                if oid not in self.sent:
                    # pgoutput sends a Relation right before the first change needing it.
                    changes.append(self._relation_message(oid))
                    self.sent.add(oid)
                roll = rng.random()
                if roll < 0.45:
                    changes.append(insert(oid, self._cells(oid)))
                elif roll < 0.7:
                    old = self._cells(oid) if rng.random() < 0.6 else None
                    changes.append(update(oid, self._cells(oid, True), old, rng.choice("OK")))
                elif roll < 0.9:
                    changes.append(delete(oid, self._cells(oid), rng.choice("OK")))
                else:
                    changes.append(
                        truncate(sorted({oid, rng.choice((101, 102, 103))}), rng.randint(0, 3))
                    )
            if rng.random() < 0.15:
                if self.HEARTBEAT not in self.sent:
                    changes.append(self._relation_message(self.HEARTBEAT))
                    self.sent.add(self.HEARTBEAT)
                marker = self.marker if rng.random() < 0.5 else "other"
                changes.append(insert(self.HEARTBEAT, ["source", marker]))
        self.xid += 1
        self.time += 1000
        final = self.lsn + sum(len(c) + 24 for c in changes) + 8
        end = final + 48
        self.lsn = end + 8
        messages = [begin(final, self.time, self.xid)]
        messages += relations + changes
        messages.append(commit(final, end, self.time))
        return messages

    def stream(self, transactions):
        out = []
        for _ in range(transactions):
            out += self.transaction()
        return out


def mutate(messages, rng):
    """One structure-aware fault applied to a valid stream."""
    messages = list(messages)
    if not messages:
        return messages
    index = rng.randrange(len(messages))
    message = messages[index]
    kind = rng.randrange(7)
    if kind == 0 and len(message) > 1:  # truncate one message
        messages[index] = message[: rng.randrange(1, len(message))]
    elif kind == 1:  # flip the tag
        messages[index] = bytes([rng.randrange(256)]) + message[1:]
    elif kind == 2 and len(message) > 6:  # flip a byte in a length/field area
        at = rng.randrange(1, min(len(message), 16))
        messages[index] = (
            message[:at] + bytes([message[at] ^ (1 << rng.randrange(8))]) + message[at + 1 :]
        )
    elif kind == 3 and index + 1 < len(messages):  # swap two adjacent messages
        messages[index], messages[index + 1] = messages[index + 1], message
    elif kind == 4:  # drop a message
        del messages[index]
    elif kind == 5:  # duplicate a message
        messages.insert(index, message)
    elif kind == 6 and message[:1] in (b"I", b"U", b"D") and len(message) >= 5:  # swap OID
        oid = rng.choice((101, 102, 103, 104, Generator.HEARTBEAT))
        messages[index] = message[:1] + struct.pack("!I", oid) + message[5:]
    return messages
