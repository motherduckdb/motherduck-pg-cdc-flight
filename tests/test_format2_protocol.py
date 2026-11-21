"""The format-2 framing/schema golden is separate from the frozen format-1 proof."""

import hashlib
import json
import struct
import sys
from dataclasses import replace
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
import pgstream  # noqa: E402

from cdc_flight import format2 as f2  # noqa: E402
from cdc_flight.config import IngestConfig  # noqa: E402
from cdc_flight.pgoutput import Cell  # noqa: E402

SETTINGS = tuple(
    zip(
        f2.OUTPUT_SETTINGS,
        ("UTC", "ISO, YMD", "postgres", "3", "hex", "C", "base64", "content", "UTF8", "UTF8"),
    )
)


def binding():
    return f2.Binding("f2", SETTINGS, (("plpgsql", "1.0"),))


def config(**kwargs):
    return IngestConfig("f2", "f2_slot", "f2_pub", ("public.items",), epoch=1, **kwargs)


def test_protocol_header_bytes_and_mapping_revision_are_golden():
    golden = json.loads((Path(__file__).parent / "fixtures/format2-protocol.json").read_text())
    b = binding().digest
    keys = f2.key_layout(("id",))
    wire = pgstream.insert(101, ("1", 'é\n"\\'))
    header = f2.EVENT_HEADER.pack(
        f2.EVENT,
        f2.FORMAT_VERSION,
        b,
        1,
        100,
        -5,
        7,
        0,
        ord("I"),
        101,
        bytes.fromhex("12" * 32),
        keys,
        len(wire),
    )
    digest = bytes.fromhex("34" * 32)
    cwire = pgstream.commit(100, 101, -4)
    cheader = f2.COMMIT_HEADER.pack(
        f2.COMMIT,
        f2.FORMAT_VERSION,
        b,
        1,
        100,
        -5,
        7,
        1,
        digest,
        len(cwire),
    )
    assert f2.CELL_MAPPING_REVISION == golden["mapping_revision"]
    assert [f2.ABSENT, f2.NULL, f2.UNCHANGED, f2.TEXT] == golden["cell_tags"]
    assert b.hex() == golden["binding"]
    assert keys.hex() == golden["key_layout"]
    assert header.hex() == golden["event_header"]
    assert wire.hex() == golden["event_wire"]
    assert cheader.hex() == golden["commit_header"]
    assert cwire.hex() == golden["commit_wire"]
    assert len(header) == 144
    assert len(cheader) == 107
    assert 8 + len(cheader) + len(cwire) == 141
    assert f2.FRAME_SCHEMA.to_string() == golden["arrow_schema"]
    assert f2.UnitConfig().header(b, 1, 90).hex() == golden["unit_header"]


def test_binding_is_immutable_closed_and_checked_before_streaming():
    b = binding()
    b.verify(SETTINGS, (("plpgsql", "1.0"),))
    with pytest.raises(ValueError, match="changed"):
        b.verify(tuple((k, "UTC+1" if k == "TimeZone" else v) for k, v in SETTINGS), b.extensions)
    with pytest.raises(ValueError, match="changed"):
        b.verify(SETTINGS, (("plpgsql", "2.0"),))
    for field, value in (
        ("settings", SETTINGS[:-1]),
        ("settings", list(SETTINGS)),
        ("mapping_revision", True),
        ("mapping_revision", 2),
    ):
        with pytest.raises(ValueError):
            replace(b, **{field: value})
    bad_encoding = tuple((k, "LATIN1" if k == "server_encoding" else v) for k, v in SETTINGS)
    with pytest.raises(ValueError, match="UTF8"):
        replace(b, settings=bad_encoding)
    assert replace(b, source_id="other").digest != b.digest
    assert replace(b, settings=tuple(reversed(SETTINGS))).digest == b.digest


def test_images_preserve_text_null_unchanged_absent_and_empty_images():
    value = f2.image((Cell("value", ""), Cell("null"), Cell("unchanged")))
    assert value == {"values": ["", None, None], "kinds": [3, 1, 2]}
    assert f2.image(None) is None
    assert f2.image(()) == {"values": [], "kinds": []}
    key = f2.project(value, (0, 2, -1))
    assert key == {"values": ["", None, None], "kinds": [3, 2, 0]}
    f2.validate_image(value, 3)
    f2.validate_image(key, 3)
    f2.validate_projection(value, key, (0, 2, -1))
    with pytest.raises(ValueError, match="projection"):
        f2.validate_projection(value, {"values": [None], "kinds": [1]}, (0,))


@pytest.mark.parametrize(
    "value",
    [
        {"values": ["1"], "kinds": []},
        {"values": ["1"], "kinds": [True]},
        {"values": [1], "kinds": [3]},
        {"values": ["1"], "kinds": [1]},
        {"values": [None], "kinds": [3]},
        {"values": [None], "kinds": [9]},
        {"values": [None], "kinds": [0], "extra": []},
    ],
)
def test_malformed_images_are_rejected_without_sql_type_coercion(value):
    with pytest.raises(ValueError):
        f2.validate_image(value, 1)


def test_unit_configuration_rejects_coercible_values_and_unknown_mapping():
    for field in f2.UnitConfig.__dataclass_fields__:
        for value in (0, -1, True, "1", 1.5):
            with pytest.raises(ValueError):
                f2.UnitConfig(**{field: value})
    with pytest.raises(ValueError, match="mapping"):
        f2.UnitConfig(mapping_revision=2)
    assert hashlib.sha256(f2.UnitConfig().header(binding().digest, 1, 90)).digest() != (
        hashlib.sha256(f2.UnitConfig(max_commits=1).header(binding().digest, 1, 90)).digest()
    )
    assert struct.calcsize("!Q") == f2.ORDINAL.size


def test_mapping_golden_covers_every_tuple_marker_and_absent_key():
    from cdc_flight.spool2 import Spool

    golden = json.loads((Path(__file__).parent / "fixtures/format2-protocol.json").read_text())
    # Changing these projections requires bumping CELL_MAPPING_REVISION.
    assert [f2.NO_OLD, f2.OLD_KEY, f2.OLD_FULL] == golden["old_tuple_tags"]
    relation = pgstream.relation(
        101,
        "public",
        "items",
        "f",
        ((1, "id", 25, -1), (0, "escape", 25, -1), (0, "nullable", 25, -1), (0, "toast", 25, -1)),
    )
    new = ("", 'é\n"\\', None, pgstream.UNCHANGED)
    key_old = ("key", None, None, None)
    full_old = ("old", "full", None, pgstream.UNCHANGED)
    messages = (
        pgstream.insert(101, new),
        pgstream.update(101, new),
        pgstream.update(101, new, key_old, "K"),
        pgstream.update(101, new, full_old, "O"),
        pgstream.delete(101, key_old, "K"),
        pgstream.delete(101, full_old, "O"),
        pgstream.truncate((101, 102), options=3),
    )
    names = (
        "old",
        "new",
        "old_key",
        "new_key",
        "old_tuple_kind",
        "truncate_oids",
        "truncate_options",
    )
    projected = []
    for wire in messages:
        s = Spool(
            config(),
            {101: ("id", "not_here"), 102: ("id",)},
            900,
            "m",
            binding=binding(),
            start_lsn=90,
            unit_config=f2.UnitConfig(max_commits=1),
            clock=lambda: 0,
        )
        s.feed(relation)
        s.feed(pgstream.relation(102, "public", "other", "d", ((1, "id", 25, -1),)))
        s.feed(pgstream.begin(100, -5, 7))
        s.feed(wire)
        output = s.feed(pgstream.commit(100, 101, -4))
        chunk = next(item for item in output if isinstance(item, f2.Chunk))
        row, commit_row = chunk.table.to_pylist()
        projected.append({name: row[name] for name in names})
        assert commit_row["event_ordinal"] == (2 if wire[:1] == b"T" else 1)
        # On COMMIT controls event_ordinal is the stored transaction_event_count.
        assert commit_row["record_kind"] == f2.COMMIT
        if row["relation_id"]:
            assert row["schema_version"] == chunk.descriptors[0].version
            assert row["key_layout_hash"] == chunk.descriptors[0].key_hash.hex()
            assert row["schema_version"] == bytes.fromhex(row["schema_version"]).hex()
            assert row["key_layout_hash"] == bytes.fromhex(row["key_layout_hash"]).hex()
    assert projected == golden["mapping_projections"]


def test_truncate_header_golden_binds_first_ordinal_and_oid_order_once():
    golden = json.loads((Path(__file__).parent / "fixtures/format2-protocol.json").read_text())
    wire = pgstream.truncate((101, 102), options=3)
    header = f2.EVENT_HEADER.pack(
        1,
        2,
        binding().digest,
        1,
        100,
        -5,
        7,
        7,
        ord("T"),
        0,
        bytes(32),
        bytes(32),
        len(wire),
    )
    assert header.hex() == golden["truncate_header"]
    assert wire.hex() == golden["truncate_wire"]
    assert 8 + len(header) + len(wire) == golden["truncate_charge"]
    assert list(range(7, 9)) == golden["truncate_logical_ordinals"]
