"""Live decoder equality against frozen format-1 references.

Format-1 spool layout is historical, not an active writer. Its reference hash and
independent direct digest remain pinned. Active format-2 framing/replay is covered
by test_format2_protocol/test_spool2, and G2 compares materialized mirrors using
this frozen v1 spool. Prefix, malformed UTF8/tag and mutated-wire decoder claims
remain live-vs-reference. Set CDC_EQUIVALENCE_STREAMS=100000 for the pre-PR corpus.
"""

import dataclasses
import hashlib
import importlib.util
import json
import os
import random
import struct
import sys
from pathlib import Path

import pyarrow as pa
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
import pgstream  # noqa: E402

from cdc_flight import pgoutput  # noqa: E402
from cdc_flight.config import IngestConfig  # noqa: E402

REFERENCE = Path(__file__).resolve().parent / "reference"
PINS = {
    "pgoutput_ref.py": "91b09b01e754664bad32451c414dc8a8a855738bdab3d876a22efc62fb61d4d4",
    "spool_ref.py": "09eeff15cf40fbc41de2e2c072a1a2abb0ee6856b3f040d58fff6b31bf0eb0e1",
}
STREAMS = int(os.getenv("CDC_EQUIVALENCE_STREAMS", "200"))


def _load(name, path):
    module_spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(module)
    return module


# The reference spool imports `cdc_flight.pgoutput.decode`, the code under test. Rebind
# it to the reference decoder, or the decoder would be compared against itself.
ref_pgoutput = _load("_cdc_reference_pgoutput", REFERENCE / "pgoutput_ref.py")
ref_spool = _load("_cdc_reference_spool", REFERENCE / "spool_ref.py")
ref_spool.decode = ref_pgoutput.decode


def test_the_reference_copies_are_frozen_and_wired_to_each_other():
    for name, digest in PINS.items():
        assert hashlib.sha256((REFERENCE / name).read_bytes()).hexdigest() == digest, name
    assert ref_spool.decode is ref_pgoutput.decode
    assert ref_spool.decode is not pgoutput.decode


def test_cell_has_exactly_kind_and_value():
    # The spool builds cell dicts by hand; a new Cell field must not be dropped silently.
    assert [f.name for f in dataclasses.fields(pgoutput.Cell)] == ["kind", "value"]


def plain(value):
    """A comparable form across the two class sets (dataclass == is class-sensitive)."""
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return (type(value).__name__,) + tuple(
            plain(getattr(value, f.name)) for f in dataclasses.fields(value) if f.name != "table"
        )
    if isinstance(value, dict):
        return {k: plain(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return type(value)(plain(v) for v in value)
    return value


def outcome(call):
    try:
        return ("ok", call())
    except Exception as exc:
        return ("raise", type(exc).__name__, str(exc))


def decoded(module, message):
    return outcome(lambda: plain(module.decode(message)))


def configs():
    return [
        IngestConfig("eq", "eq", "eq", ("ns101.t101",), allow_local=True),
        # Small chunks force chunk boundaries inside transactions.
        IngestConfig("eq", "eq", "eq", ("ns101.t101",), chunk_bytes=1500, allow_local=True),
        # Tight limits reach the oversize event/transaction errors.
        IngestConfig(
            "eq",
            "eq",
            "eq",
            ("ns101.t101",),
            chunk_bytes=1024,
            max_event_bytes=2048,
            max_transaction_bytes=6000,
            allow_local=True,
        ),
    ]


@pytest.mark.parametrize("seed", range(20))
def test_generated_streams_decode_identically(seed):
    generator = pgstream.Generator(seed)
    messages = generator.stream(40)
    for message in messages:
        assert decoded(pgoutput, message) == decoded(ref_pgoutput, message)


def test_mutated_streams_fail_or_succeed_identically():
    """Every mutated message has the same decoded fields or exception as the oracle."""
    rng = random.Random(7)
    for index in range(STREAMS):
        generator = pgstream.Generator(10_000 + index)
        messages = generator.stream(rng.randint(1, 12))
        for _ in range(rng.randint(1, 3)):
            messages = pgstream.mutate(messages, rng)
        for message in messages:
            assert decoded(pgoutput, message) == decoded(ref_pgoutput, message), index


def test_every_prefix_of_every_message_kind_decodes_identically():
    generator = pgstream.Generator(3)
    samples = {}
    for message in generator.stream(80):
        samples.setdefault(message[:1], []).append(message)
    samples[b"O"] = [b"O" + struct.pack("!Q", 77) + b"origin\0"]
    samples[b"Y"] = [b"Y" + struct.pack("!I", 5) + b"pg_catalog\0int4\0"]
    assert {b"B", b"C", b"R", b"I", b"U", b"D", b"T"} <= set(samples)
    for kind, messages in samples.items():
        for message in messages[:5]:
            for end in range(len(message) + 1):
                prefix = message[:end]
                assert decoded(pgoutput, prefix) == decoded(ref_pgoutput, prefix), (kind, end)


def test_invalid_utf8_and_unsupported_tags_raise_identically():
    bad = b"\xff\xfe\x80"
    messages = [
        pgstream.relation(101, bad, "t", "f", [(1, "id", 20, -1)]),
        pgstream.relation(101, "ns", bad, "f", [(1, "id", 20, -1)]),
        pgstream.relation(101, "ns", "t", "f", [(1, b"col\xc3", 20, -1)]),
        pgstream.insert(101, [b"ok\xe2\x82"]),
        pgstream.insert(101, ["ok", b"\xed\xa0\x80"]),
        b"I"
        + struct.pack("!I", 101)
        + b"N"
        + struct.pack("!H", 1)
        + b"b"
        + struct.pack("!I", 1)
        + b"x",
        b"I" + struct.pack("!I", 101) + b"N" + struct.pack("!H", 1) + b"\xff",
        b"C" + struct.pack("!BQQq", 1, 1, 2, 3),
        b"Z",
        b"",
        pgstream.begin(1, 2, 3) + b"trailing",
    ]
    for message in messages:
        assert decoded(pgoutput, message) == decoded(ref_pgoutput, message), message[:12]


def test_historical_reference_digest_matches_its_direct_canonical_hash():
    """The chunk digest is sha256 over each row's canonical JSON plus a newline, the
    format-1 definition, computed here from the table independently."""
    generator = pgstream.Generator(5)
    messages = generator.stream(30)
    s = ref_spool.Spool(configs()[1], generator.key_columns, generator.HEARTBEAT, generator.marker)
    chunks = [i for m in messages for i in s.feed(m) if isinstance(i, ref_spool.Chunk)]
    assert chunks
    for chunk in chunks:
        rows = chunk.table.to_pylist()
        digest = hashlib.sha256(
            b"".join(
                json.dumps(row, sort_keys=True, separators=(",", ":")).encode() + b"\n"
                for row in rows
            )
        ).hexdigest()
        assert chunk.digest == digest
        assert pa.Table.from_pylist(rows, schema=ref_spool.EVENT_SCHEMA).equals(chunk.table)
