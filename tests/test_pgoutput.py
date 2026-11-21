import struct

import pytest

from cdc_flight.pgoutput import Cell, Decoder, Event, Relation, decode, normalize


def wire_row(*cells):
    out = struct.pack("!H", len(cells))
    for value in cells:
        if value is None:
            out += b"n"
        elif value == Cell("unchanged"):
            out += b"u"
        else:
            data = str(value).encode()
            out += b"t" + struct.pack("!I", len(data)) + data
    return out


REL = Relation(1, "public", "items", "f", ((1, "id", 23, -1), (0, "value", 25, -1)))


def event(ordinal, op, old, new, commit=10, rel=REL):
    def cells(values):
        if values is None:
            return None
        return tuple(
            v if isinstance(v, Cell) else Cell("null" if v is None else "value", v) for v in values
        )

    return Event("source", 1, commit, commit + 1, ordinal, rel, op, cells(old), cells(new))


def test_wire_messages_and_strict_validation():
    begin = b"B" + struct.pack("!QqI", 10, 0, 7)
    assert decode(begin)["final_lsn"] == 10
    commit = b"C\0" + struct.pack("!QQq", 10, 11, 0)
    assert decode(commit)["end"] == 11
    relation = b"R" + struct.pack("!I", 1) + b"public\0items\0f" + struct.pack("!H", 2)
    for flags, name, oid, mod in REL.columns:
        relation += bytes([flags]) + name.encode() + b"\0" + struct.pack("!Ii", oid, mod)
    assert decode(relation)["relation"] == REL
    insert = b"I" + struct.pack("!I", 1) + b"N" + wire_row("1", None)
    update = (
        b"U"
        + struct.pack("!I", 1)
        + b"O"
        + wire_row("1", "old")
        + b"N"
        + wire_row("2", Cell("unchanged"))
    )
    delete = b"D" + struct.pack("!I", 1) + b"K" + wire_row("2", None)
    decoder = Decoder()
    for wire in [begin, relation, insert, update, delete]:
        assert decoder.feed(wire) is None
    boundary, events = decoder.feed(commit)
    assert boundary == 11
    assert [e.ordinal for e in events] == [0, 1, 2]
    assert events[1].new[1].kind == "unchanged"
    assert events[0].new[1].kind == "null"
    assert decode(b"O" + struct.pack("!Q", 3) + b"origin\0")["name"] == "origin"
    assert decode(b"Y" + struct.pack("!I", 23) + b"pg_catalog\0int4\0")["oid"] == 23
    assert decode(b"T" + struct.pack("!IBI", 1, 0, 1))["relations"] == (1,)
    for bad in [
        b"S",
        b"E",
        b"c",
        b"A",
        begin[:-1],
        begin + b"x",
        b"R\0",
        b"D" + struct.pack("!I", 1) + b"N",
    ]:
        with pytest.raises(ValueError):
            decode(bad)


def test_fold_key_changes_toast_null_and_order():
    events = [
        event(0, "insert", None, ("1", "toast")),
        event(1, "update", ("1", "toast"), ("2", Cell("unchanged"))),
        event(2, "update", None, ("2", None)),
        event(3, "delete", ("2", None), None),
        event(4, "insert", None, ("2", "reborn")),
    ]
    assert normalize({}, list(reversed(events)), ("id",)) == {
        ("2",): {"id": "2", "value": "reborn"}
    }
    # FULL old image supplies TOAST even if the fuzzy scan saw the row already deleted.
    assert normalize({}, events[1:2], ("id",))[("2",)]["value"] == "toast"
    with pytest.raises(ValueError, match="baseline"):
        normalize({}, [event(0, "update", None, ("2", Cell("unchanged")))], ("id",))


def test_schema_version_stamped_before_later_relation_and_composite_key():
    newer = Relation(1, "public", "items", "f", REL.columns + ((0, "extra", 25, -1),))
    assert REL.version != newer.version
    e = event(0, "insert", None, ("1", "x"))
    assert e.relation.version == REL.version
    rows = normalize({}, [e], ("id", "value"))
    assert ("1", "x") in rows
    assert "extra" not in rows[("1", "x")]


def test_transaction_cap_and_missing_begin():
    insert = b"I" + struct.pack("!I", 1) + b"N" + wire_row("1", "x")
    decoder = Decoder(max_bytes=1)
    decoder.relations[1] = REL
    with pytest.raises(ValueError, match="outside"):
        decoder.feed(insert)
    decoder.feed(b"B" + struct.pack("!QqI", 10, 0, 1))
    with pytest.raises(ValueError, match="cap"):
        decoder.feed(insert)


def test_empty_transaction_preserves_commit_identity_and_checks_begin():
    decoder = Decoder()
    decoder.feed(b"B" + struct.pack("!QqI", 10, 0, 1))
    end, events = decoder.feed(b"C\0" + struct.pack("!QQq", 10, 11, 0))
    assert end == 11 and events == [] and events.commit_lsn == 10
    decoder.feed(b"B" + struct.pack("!QqI", 20, 0, 1))
    with pytest.raises(ValueError, match="invalid commit"):
        decoder.feed(b"C\0" + struct.pack("!QQq", 21, 22, 0))
