"""The fast `pgoutput.decode` against its two oracles: `_decode_reader` (the original
field-by-field decoder, kept in pgoutput.py) and the frozen, sha-pinned
tests/reference/pgoutput_ref.py, which guards against both drifting together.

Compared on every node of the output (type, value, dict key order, `Relation.version`) and on
the full exception (type, args, and UnicodeDecodeError's encoding/object/start/end/reason).

Bounded and deterministic by default (fixed seeds, fixed case counts). Long mode:
CDC_TEST_PGOUTPUT_FUZZ=<multiplier> scales every corpus. CDC_TEST_PGOUTPUT_WIRE=<dir> adds a
directory of ingest-profile wire pickles (each a dict with a `messages` list of bytes).
"""

import dataclasses
import functools
import importlib.util
import os
import pickle
import random
import struct
import sys
from collections import Counter
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
import pgstream  # noqa: E402

from cdc_flight import pgoutput  # noqa: E402

_spec = importlib.util.spec_from_file_location(
    "_fast_reference_pgoutput", Path(__file__).resolve().parent / "reference" / "pgoutput_ref.py"
)
ref = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(ref)
SCALE = int(os.getenv("CDC_TEST_PGOUTPUT_FUZZ", "1"))
FAST_TAGS = frozenset(b"BCIUD")


def typed(value):
    """Every node's type and value; the two modules' classes compare by name."""
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        node = (type(value).__qualname__,) + tuple(
            (f.name, typed(getattr(value, f.name))) for f in dataclasses.fields(value)
        )
        if type(value).__qualname__ == "Relation":
            node += (("version", value.version),)
        return node
    if isinstance(value, dict):
        return ("dict", tuple((k, typed(v)) for k, v in value.items()))  # key order kept
    if isinstance(value, list):
        extra = (("commit_lsn", value.commit_lsn),) if hasattr(value, "commit_lsn") else ()
        return (type(value).__qualname__, tuple(typed(v) for v in value)) + extra
    if isinstance(value, tuple):
        return ("tuple", tuple(typed(v) for v in value))
    return (type(value).__qualname__, value)


def outcome(call):
    try:
        return ("ok", typed(call()))
    except Exception as exc:  # noqa: BLE001 - parity on whatever escapes
        out = (type(exc).__qualname__, exc.args)
        if isinstance(exc, UnicodeDecodeError):
            out += (exc.encoding, bytes(exc.object), exc.start, exc.end, exc.reason)
        return ("raise", out)


def check(message, label=""):
    """Assert parity with both oracles; return the outcome class for coverage."""
    got = outcome(lambda: pgoutput.decode(message))
    frozen = outcome(lambda: ref.decode(message))
    reader = outcome(lambda: pgoutput._decode_reader(message))
    assert got == frozen, (label, bytes(message[:64]), got, frozen)
    assert got == reader, (label, bytes(message[:64]), got, reader)
    if got[0] == "ok":
        return ("ok", got[1][1][0][1][1])  # the message kind
    exc_type, args = got[1][0], got[1][1]
    if exc_type == "UnicodeDecodeError":
        return (exc_type, f"{got[1][2]}:{got[1][6]}")
    text = str(args[0]) if args else ""
    return (exc_type, text.split(" '")[0].split(' "')[0])


# ---- corpora (fixed seeds) -------------------------------------------------------------------

CELLS = [None, pgstream.UNCHANGED, "", "a", "é中🦆", "x" * 10_000, b"\x00\x01"]


@functools.cache
def streams():
    return tuple(tuple(pgstream.Generator(seed).stream(30)) for seed in range(15 * SCALE))


@functools.cache
def extremes():
    out = []
    for a in (0, 1, 2**63, 2**64 - 1):
        for b in (-(2**63), -1, 0, 2**63 - 1):
            out += [
                pgstream.begin(a, b, 2**32 - 1),
                pgstream.begin(a, b, 0),
                pgstream.commit(a, a, b),
            ]
    for oid in (0, 1, 2**32 - 1):
        out += [
            pgstream.insert(oid, []),
            pgstream.insert(oid, CELLS),
            pgstream.update(oid, CELLS),
            pgstream.update(oid, CELLS, old=CELLS, identity="O"),
            pgstream.update(oid, CELLS, old=["k"], identity="K"),
            pgstream.update(oid, [], old=[], identity="K"),
            pgstream.delete(oid, CELLS, identity="O"),
            pgstream.delete(oid, ["k"], identity="K"),
            pgstream.delete(oid, [], identity="K"),
            pgstream.truncate([]),
            pgstream.truncate([oid]),
            pgstream.truncate([oid, 1, 2, 3], options=3),
            pgstream.relation(oid, "s", "t", "f", [(1, "id", 23, -1), (0, "é", 25, 2**31 - 1)]),
            pgstream.relation(oid, "", "", "d", []),
        ]
    out += [b"O" + struct.pack("!Q", 7) + b"origin\0", b"Y" + struct.pack("!I", 5) + b"s\0t\0"]
    # COMMIT checks its flags before reading the LSNs; empty and one-byte messages
    out += [b"", b"C\x01", b"C\x01" + bytes(24), b"C\x01" + bytes(25), b"C", b"C\x00", b"B"]
    # every marker byte class on I/U/D, with and without bodies (spool2 relies on this check)
    body = pgstream.tuple_data(["v"])
    for tag in b"IUD":
        for marker in b"KONXk\x00\x7f\x80\xff":
            head = bytes([tag]) + struct.pack("!I", 9) + bytes([marker])
            out += [head + body, head, head + body + bytes([marker]) + body]
    # length fields at 0, short, long and 2^32-1; cell counts beyond the cells present
    for n in (0, 4, 6, 2**32 - 1):
        out.append(
            b"I" + bytes(4) + b"N" + struct.pack("!H", 1) + b"t" + struct.pack("!I", n) + b"abcde"
        )
    for n in (0, 1, 3, 2**16 - 1):
        out.append(b"I" + bytes(4) + b"N" + struct.pack("!H", n) + b"nn")
    out += [  # invalid UTF-8 in a value and in a string; unterminated string; binary cell tag
        b"I" + bytes(4) + b"N" + struct.pack("!H", 1) + b"t" + struct.pack("!I", 2) + b"\xc3(",
        b"R" + bytes(4) + b"\xc3(\0t\0f\0\0",
        b"R" + bytes(4) + b"s",
        b"I" + bytes(4) + b"N" + struct.pack("!H", 1) + b"b" + bytes(4),
    ]
    return tuple(out)


def corrupt(message, rng):
    """Every prefix; four byte values and one bit flip at every offset; added bytes."""
    out = [message[:i] for i in range(len(message))]
    out += [message + b"\x00", message + b"\xff" * 3, b"\x00" + message]
    for at in range(len(message)):
        for value in {0, 0x7F, 0x80, 0xFF, message[at] ^ (1 << rng.randrange(8))}:
            out.append(message[:at] + bytes([value]) + message[at + 1 :])
    at = rng.randrange(len(message) + 1)
    out.append(message[:at] + bytes([rng.randrange(256)]) + message[at:])
    return out


# ---- tests -------------------------------------------------------------------------------------


def test_the_frozen_oracle_is_the_pinned_one():
    import hashlib

    path = Path(__file__).resolve().parent / "reference" / "pgoutput_ref.py"
    digest = "91b09b01e754664bad32451c414dc8a8a855738bdab3d876a22efc62fb61d4d4"
    assert hashlib.sha256(path.read_bytes()).hexdigest() == digest
    assert ref.decode is not pgoutput.decode and ref.decode is not pgoutput._decode_reader


def test_generated_and_mutated_streams():
    for messages in streams():
        for message in messages:
            check(message, "stream")
    rng = random.Random(41)
    for index in range(60 * SCALE):
        messages = pgstream.Generator(50_000 + index).stream(rng.randint(1, 10))
        for _ in range(rng.randint(1, 3)):
            messages = pgstream.mutate(messages, rng)
        for message in messages:
            check(message, "mutate")


def test_hand_built_extremes_and_non_bytes_input():
    for message in extremes():
        check(message, "extreme")
    for raw in (pgstream.begin(1, 2, 3), pgstream.commit(1, 2, 3), pgstream.insert(1, ["a"])):
        check(bytearray(raw), "bytearray")
        check(memoryview(raw), "memoryview")


def test_a_non_ascii_byte_at_every_char_site():
    body = pgstream.tuple_data(["v"])
    sites = {
        "message tag": b"\x80" + bytes(20),
        "relation identity": b"R" + bytes(4) + b"s\0t\0\x80" + struct.pack("!H", 0),
        "change marker": b"I" + bytes(4) + b"\x80" + body,
        "update second marker": b"U" + bytes(4) + b"K" + body + b"\xff" + body,
        "new cell tag": b"I" + bytes(4) + b"N" + struct.pack("!H", 1) + b"\x80",
        "old cell tag": b"D" + bytes(4) + b"O" + struct.pack("!H", 2) + b"n\xfe",
    }
    for site, message in sites.items():
        assert check(message, site) == ("UnicodeDecodeError", "ascii:ordinal not in range(128)"), (
            site
        )


def test_structured_malformed_input_and_coverage():
    """Every prefix and every byte class at every offset of a fixed sample; every error class
    the reference decoder can raise is reached."""
    rng = random.Random(43)
    sample = [m for messages in streams()[: 3 * SCALE] for m in messages if len(m) < 300]
    sample += [m for m in extremes() if 0 < len(m) < 300]
    seen = Counter()
    for message in sample:
        for bad in corrupt(message, rng):
            seen[check(bad, "fuzz")] += 1
    for message in extremes():
        seen[check(message, "extreme")] += 1
    errors = {text for kind, text in seen if kind == "ValueError"}
    assert errors >= {
        "truncated pgoutput message",
        "trailing pgoutput bytes",
        "unterminated string",
        "unsupported commit flags",
        "expected new tuple",
        "delete missing old identity",
        "unsupported tuple tag",
        "unsupported pgoutput message",
    }, errors
    assert {text.split(":")[0] for kind, text in seen if kind == "UnicodeDecodeError"} == {
        "ascii",
        "utf-8",
    }
    kinds = {text for kind, text in seen if kind == "ok"}
    assert kinds >= {
        "begin",
        "commit",
        "insert",
        "update",
        "delete",
        "relation",
        "truncate",
        "origin",
        "type",
    }


def test_decoder_feed_parity():
    """Format 1's path, `Decoder.feed`, on clean and mutated streams."""
    rng = random.Random(47)
    for index in range(40 * SCALE):
        messages = pgstream.Generator(70_000 + index).stream(rng.randint(1, 10))
        if index % 2:
            messages = pgstream.mutate(messages, rng)
        new, old = pgoutput.Decoder(), ref.Decoder()
        for message in messages:
            got = outcome(lambda: new.feed(message))
            assert got == outcome(lambda: old.feed(message)), index
            if got[0] == "raise":
                break


def test_valid_change_streams_take_the_fast_path(monkeypatch):
    """`_decode_reader` is reached only for RELATION / TRUNCATE / ORIGIN / TYPE and non-bytes."""
    reached = Counter()
    original = pgoutput._decode_reader

    def recording(data):
        reached[(type(data).__name__, bytes(data[:1]))] += 1
        return original(data)

    monkeypatch.setattr(pgoutput, "_decode_reader", recording)
    fast = 0
    for messages in streams():
        for message in messages:
            pgoutput.decode(message)
            fast += message[0] in FAST_TAGS
    assert fast > 1000
    assert {tag for kind, tag in reached} <= {b"R", b"T", b"O", b"Y"}, reached
    assert all(kind == "bytes" for kind, _ in reached)
    assert sum(reached.values()) == sum(m[0] not in FAST_TAGS for s in streams() for m in s)
    reached.clear()
    pgoutput.decode(bytearray(pgstream.begin(1, 2, 3)))
    assert reached == {("bytearray", b"B"): 1}


def test_profile_wire_workloads():
    directory = os.getenv("CDC_TEST_PGOUTPUT_WIRE")
    if not directory:
        pytest.skip("CDC_TEST_PGOUTPUT_WIRE not set (the ingest profile's wire pickles)")
    seen = 0
    for path in sorted(Path(directory).glob("*.pkl")):
        with open(path, "rb") as handle:
            for message in pickle.load(handle)["messages"]:
                check(message, path.stem)
                seen += 1
    assert seen > 0
