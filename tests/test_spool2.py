"""Deterministic wire chunks, grouped completion and durable-prefix replay."""

import struct
import sys
from dataclasses import replace
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
import pgstream  # noqa: E402
from test_format2_protocol import binding, config  # noqa: E402

from cdc_flight.format2 import COMMIT, EVENT, Chunk, UnitComplete, UnitConfig  # noqa: E402
from cdc_flight.spool2 import Spool  # noqa: E402

REL = pgstream.relation(101, "public", "items", "f", ((1, "id", 25, -1), (0, "v", 25, -1)))


def relation_for(oid):
    return pgstream.relation(
        oid, "public", f"items{oid}", "f", ((1, "id", 25, -1), (0, "v", 25, -1))
    )


def spool(**kwargs):
    return Spool(
        config(),
        {oid: ("id",) for oid in (101, 102, 103)},
        900,
        "marker",
        binding=binding(),
        start_lsn=90,
        clock=lambda: 0.0,
        **kwargs,
    )


def transaction(lsn=100, values=("1", "text"), *, change=None):
    return [
        pgstream.begin(lsn, -5, 7),
        change or pgstream.insert(101, values),
        pgstream.commit(lsn, lsn + 1, -4),
    ]


def collect(s, messages, close=True):
    result = []
    for wire in messages:
        result.extend(s.feed(wire))
    if close:
        result.extend(s.close())
    return result


def chunks(result):
    return [c for c in result if isinstance(c, Chunk)]


def rows(result):
    return [r for c in chunks(result) for r in c.table.to_pylist()]


def test_group_has_inline_commits_original_transaction_identity_and_no_marker_in_digest():
    stream = [REL] + transaction() + transaction(101, ("2", "two"))
    output = collect(spool(unit_config=UnitConfig(max_commits=2)), stream)
    assert len(chunks(output)) == 1
    complete = output[-1]
    assert isinstance(complete, UnitComplete)
    assert (complete.unit_start_lsn, complete.end_lsn, complete.member_count) == (90, 102, 2)
    assert (complete.event_count, complete.frame_count) == (2, 4)
    data = rows(output)
    assert [r["record_kind"] for r in data] == [EVENT, COMMIT, EVENT, COMMIT]
    assert [r["commit_lsn"] for r in data] == [100, 100, 101, 101]
    assert [r["event_ordinal"] for r in data] == [0, 1, 0, 1]
    assert [r["frame_ordinal"] for r in data] == [0, 1, 2, 3]
    assert data[0]["new"] == {"values": ["1", "text"], "kinds": [3, 3]}
    assert len(data[1]["transaction_digest"]) == 64
    assert data[1]["transaction_digest"] != data[3]["transaction_digest"]
    assert chunks(output)[0].id == "1:90:0:4"
    # A run marker is ephemeral; it never enters this stream's identity.
    other = spool(unit_config=UnitConfig(max_commits=2))
    other.marker = "another-run"
    assert collect(other, stream) == output


@pytest.mark.parametrize("identity,kind", [("K", 1), ("O", 2)])
def test_u_k_u_o_d_k_d_o_and_u_n_preserve_old_presence(identity, kind):
    stream = [REL] + transaction(
        change=pgstream.update(101, ("1", pgstream.UNCHANGED), ("1", None), identity)
    )
    data = rows(collect(spool(), stream))
    assert data[0]["old_tuple_kind"] == kind
    assert data[0]["old"] == {"values": ["1", None], "kinds": [3, 1]}
    assert data[0]["new"]["kinds"] == [3, 2]
    data = rows(
        collect(spool(), [REL] + transaction(change=pgstream.delete(101, ("1", None), identity)))
    )
    assert data[0]["old_tuple_kind"] == kind
    assert data[0]["new"] is None
    data = rows(collect(spool(), [REL] + transaction(change=pgstream.update(101, ("1", "new")))))
    assert data[0]["old_tuple_kind"] == 0
    assert data[0]["old"] is None


def test_metadata_resends_origin_and_custom_types_never_move_boundaries_or_hashes():
    stream = [REL] + transaction() + transaction(101, ("2", "second"))
    typ = b"Y" + struct.pack("!I", 50000) + b"public\0custom\0"
    origin = b"O" + struct.pack("!Q", 80) + b"upstream\0"
    resend = stream[:3] + [REL, typ, origin, REL] + stream[3:]
    settings = UnitConfig(chunk_frames=2)
    assert collect(spool(unit_config=settings), stream) == collect(
        spool(unit_config=settings), resend
    )


def test_chunk_equality_stays_and_one_extra_byte_flushes_before_frame():
    wire = pgstream.insert(101, ("1", "a"))
    charge = 8 + 144 + len(wire)
    stream = [REL, pgstream.begin(100, -5, 7), wire, wire, pgstream.commit(100, 101, -4)]
    exact = chunks(collect(spool(unit_config=UnitConfig(chunk_bytes=2 * charge)), stream))
    overflow = chunks(collect(spool(unit_config=UnitConfig(chunk_bytes=2 * charge - 1)), stream))
    assert (exact[0].count, exact[0].bytes) == (2, 2 * charge)
    assert (overflow[0].count, overflow[0].bytes) == (1, charge)
    assert sum(c.event_count for c in exact) == 2
    assert sum(c.member_count for c in exact) == 1
    assert sum(c.count for c in exact) == 3
    assert sum(c.bytes for c in exact) == 2 * charge + 141


def test_frame_count_target_and_single_oversize_frame_are_not_tuple_splits():
    output = collect(
        spool(unit_config=UnitConfig(chunk_bytes=1, chunk_frames=1)), [REL] + transaction()
    )
    assert [(c.first_frame, c.count) for c in chunks(output)] == [(0, 1), (1, 1)]
    assert chunks(output)[0].bytes > 1
    assert rows(output)[0]["new"]["values"] == ["1", "text"]


def test_truncate_is_one_physical_frame_with_all_logical_table_events():
    wire = pgstream.truncate((101, 102, 103), options=3)
    output = collect(spool(), [relation_for(i) for i in (101, 102, 103)] + transaction(change=wire))
    assert len(rows(output)) == 2
    data = rows(output)
    assert data[0]["truncate_oids"] == [101, 102, 103]
    assert data[0]["truncate_options"] == 3
    assert data[0]["relation_id"] == 0
    assert output[-1].event_count == 3
    assert output[-1].frame_count == 2
    assert output[-1].bytes == 8 + 144 + len(wire) + 141


def test_empty_commits_are_bounded_and_have_distinct_transaction_digests():
    # Build only delivered messages; no dependence on a PG major's empty behavior.
    stream = [
        w
        for i in range(100)
        for w in (pgstream.begin(100 + i, -5, i), pgstream.commit(100 + i, 101 + i, -4))
    ]
    output = collect(spool(unit_config=UnitConfig(max_commits=100, chunk_frames=7)), stream)
    assert output[-1].member_count == 100
    assert output[-1].event_count == 0
    assert sum(c.count for c in chunks(output)) == 100
    assert all(c.count <= 7 for c in chunks(output))
    assert len({r["transaction_digest"] for r in rows(output)}) == 100


def test_runtime_request_does_not_flush_an_incomplete_short_tail():
    s = spool()
    assert collect(s, [REL, *transaction()[:2]], close=False) == []
    s.request_close()
    with pytest.raises(ValueError, match="inside"):
        s.close()
    output = s.feed(transaction()[-1])
    assert output[-1].end_lsn == 101
    assert output[-1].frame_count == 2


def test_timer_at_c3_cannot_close_before_the_durable_c5_prefix():
    settings = UnitConfig(chunk_frames=3)
    stream = [REL] + [w for i in range(5) for w in transaction(100 + i, (str(i), "x"))]
    # Prefix has the first frame of C5 (frame 8) but no C5 COMMIT.
    first = spool(unit_config=settings)
    durable = collect(first, stream, close=False)
    assert [(c.first_frame, c.count) for c in chunks(durable)] == [(0, 3), (3, 3), (6, 3)]
    retry = spool(unit_config=settings, replay=chunks(durable))
    rebuilt = []
    for i, wire in enumerate(stream):
        if i == 1 + 3 * 3:
            retry.request_close()
        rebuilt.extend(retry.feed(wire))
    assert not any(isinstance(c, UnitComplete) for c in rebuilt[:-1])
    assert rebuilt[-1].end_lsn == 105
    assert chunks(rebuilt)[:3] == chunks(durable)


def test_complete_terminal_overrides_new_timer_and_transaction_digest_ignores_anchor():
    settings = UnitConfig(chunk_frames=2)
    stream = [REL] + transaction() + transaction(101, ("2", "two"))
    original = collect(spool(unit_config=settings), stream)
    replay = spool(unit_config=settings, replay=chunks(original), terminal=original[-1])
    replay.request_close()
    assert collect(replay, stream) == original
    other = Spool(
        config(),
        {101: ("id",)},
        900,
        "new",
        binding=binding(),
        start_lsn=80,
        unit_config=settings,
        clock=lambda: 0,
    )
    shifted = collect(other, [REL] + transaction())
    assert rows(shifted)[1]["transaction_digest"] == rows(original)[1]["transaction_digest"]
    assert chunks(shifted)[0].digest != chunks(original)[0].digest


def test_changed_prefix_or_revision_fails_closed_at_the_original_ordinal():
    settings = UnitConfig(chunk_frames=1)
    original = chunks(collect(spool(unit_config=settings), [REL] + transaction()))
    replay = spool(unit_config=settings, replay=original)
    with pytest.raises(ValueError, match="frame 0"):
        collect(replay, [REL] + transaction(values=("1", "different")))
    with pytest.raises(ValueError, match="revision"):
        spool(unit_config=settings, replay=[replace(original[0], mapping_revision=2)])
    with pytest.raises(ValueError, match="prefix"):
        spool(unit_config=settings, replay=[replace(original[0], first_frame=1)])


def test_key_projection_and_descriptor_budget_fail_before_arrow_upload():
    s = spool()
    collect(s, [REL, *transaction()[:2]], close=False)
    s.rows[0]["new_key"]["values"][0] = "wrong"
    s.feed(transaction()[-1])
    with pytest.raises(ValueError, match="projection"):
        s.close()
    s = spool(max_descriptor_bytes=1)
    with pytest.raises(ValueError, match="descriptor memory"):
        s.feed(REL)


def test_transaction_byte_cap_counts_commit_controls_not_just_events():
    s = Spool(
        config(max_event_bytes=10, max_transaction_bytes=140),
        {101: ("id",)},
        900,
        "m",
        binding=binding(),
        start_lsn=90,
        clock=lambda: 0,
    )
    # Raise only the message cap: the transaction wire/framing cap remains 140.
    s.config = replace(s.config, max_event_bytes=100)
    s.feed(pgstream.begin(100, -5, 7))
    with pytest.raises(ValueError, match="charged-wire"):
        s.feed(pgstream.commit(100, 101, -4))


def _wire_from_row(row):
    """Independent protocol-builder oracle, never called on the runtime path."""

    def values(image):
        if image is None:
            return None
        return [
            value if kind == 3 else pgstream.UNCHANGED if kind == 2 else None
            for value, kind in zip(image["values"], image["kinds"])
        ]

    if row["record_kind"] == COMMIT:
        return pgstream.commit(row["commit_lsn"], row["end_lsn"], row["end_ts_us"])
    oid, op = row["relation_id"], row["op"]
    if op == "insert":
        return pgstream.insert(oid, values(row["new"]))
    if op == "update":
        return pgstream.update(
            oid,
            values(row["new"]),
            values(row["old"]),
            {0: "N", 1: "K", 2: "O"}[row["old_tuple_kind"]],
        )
    if op == "delete":
        return pgstream.delete(oid, values(row["old"]), {1: "K", 2: "O"}[row["old_tuple_kind"]])
    return pgstream.truncate(row["truncate_oids"], row["truncate_options"])


def test_wire_reconstruction_is_lossless_over_seeded_schema_and_cell_fuzz_corpus(monkeypatch):
    import os

    import pyarrow as pa

    from cdc_flight import spool2
    from cdc_flight.format2 import FRAME_SCHEMA

    builder = spool2._frame_table

    def checked(rows):
        # Reference first: a bad builder must not alter the oracle's input.
        expected = pa.Table.from_pylist(rows, schema=FRAME_SCHEMA)
        actual = builder(rows)
        assert actual.schema.equals(FRAME_SCHEMA, check_metadata=True)
        assert actual.equals(expected, check_metadata=True)
        return actual

    monkeypatch.setattr(spool2, "_frame_table", checked)
    count = int(os.getenv("CDC_FORMAT2_FUZZ_STREAMS", "1000"))
    for seed in range(count):
        generator = pgstream.Generator(seed)
        # Session metadata for relations that first appear only in a T. The shared
        # v1 fuzz builder may omit that resend; keep its source byte-frozen.
        metadata = [generator._relation_message(oid) for oid in generator.relations]
        stream = metadata + generator.stream(5)
        s = Spool(
            config(),
            generator.key_columns,
            generator.HEARTBEAT,
            generator.marker,
            binding=binding(),
            start_lsn=0,
            unit_config=UnitConfig(chunk_bytes=1500),
            clock=lambda: 0,
        )
        data = rows(collect(s, stream))
        expected = [w for w in stream if w[:1] in (b"I", b"U", b"D", b"T", b"C")]
        assert [_wire_from_row(r) for r in data] == expected, seed
        assert sum(r["wire_length"] for r in data) == sum(map(len, expected)), seed


def test_logical_projection_expands_truncate_and_keeps_original_event_ordinals():
    from cdc_flight.spool2 import logical_events

    output = collect(
        spool(),
        [
            REL,
            relation_for(102),
            pgstream.begin(100, -5, 7),
            pgstream.truncate((101, 102)),
            pgstream.insert(101, ("1", "after")),
            pgstream.commit(100, 101, -4),
        ],
    )
    events = list(logical_events(rows(output)))
    assert [(r["relation_id"], r["event_ordinal"]) for r in events] == [
        (101, 0),
        (102, 1),
        (101, 2),
    ]
    assert all(r["record_kind"] == EVENT for r in events)
    with pytest.raises(ValueError, match="frame kind"):
        list(logical_events([{"record_kind": 99}]))


def test_backfill_marker_requests_commit_boundary_without_becoming_run_marker():
    hb = pgstream.relation(900, "public", "hb", "d", ((1, "id", 25, -1), (0, "marker", 25, -1)))
    s = Spool(
        config(),
        {900: ("id",)},
        900,
        "current-run",
        binding=binding(),
        start_lsn=90,
        clock=lambda: 0,
    )
    s.feed(hb)
    s.feed(pgstream.begin(100, -5, 7))
    assert s.feed(pgstream.insert(900, ("backfill:101", "generation-uuid"))) == []
    output = s.feed(pgstream.commit(100, 101, -4))
    assert output[-1].end_lsn == 101
    assert output[-1].marker is False


def test_both_integrity_digests_cover_original_wire_and_the_terminal_counts():
    import hashlib

    from cdc_flight.format2 import domain

    settings = UnitConfig(max_commits=2, chunk_frames=2)
    stream = [
        REL,
        relation_for(102),
        pgstream.begin(100, -5, 7),
        pgstream.insert(101, ("1", "é\\\n")),
        pgstream.truncate((101, 102), 3),
        pgstream.commit(100, 101, -4),
    ]
    stream += transaction(101, ("2", "second"))
    output = collect(spool(unit_config=settings), stream)
    b = binding().digest
    unit = settings.header(b, 1, 90)
    prefix = hashlib.sha256(domain("prefix") + unit).digest()
    transaction_digest = None
    previous_commit = None
    for chunk in chunks(output):
        chunk_digest = hashlib.sha256(domain("chunk") + unit + struct.pack("!Q", chunk.first_frame))
        for row in chunk.table.to_pylist():
            wire = _wire_from_row(row)
            if row["commit_lsn"] != previous_commit:
                transaction_digest = hashlib.sha256(
                    domain("transaction")
                    + struct.pack(
                        "!32sIQqI", b, 1, row["commit_lsn"], row["commit_ts_us"], row["txid"]
                    )
                )
                previous_commit = row["commit_lsn"]
            if row["record_kind"] == EVENT:
                header = struct.pack(
                    "!BH32sIQqIQBI32s32sQ",
                    1,
                    2,
                    b,
                    1,
                    row["commit_lsn"],
                    row["commit_ts_us"],
                    row["txid"],
                    row["event_ordinal"],
                    wire[0],
                    row["relation_id"],
                    bytes.fromhex(row["schema_version"]) if row["schema_version"] else bytes(32),
                    bytes.fromhex(row["key_layout_hash"]) if row["key_layout_hash"] else bytes(32),
                    len(wire),
                )
                transaction_digest.update(header + wire)
            else:
                terminal = struct.pack(
                    "!BH32sIQqIQQ",
                    2,
                    2,
                    b,
                    1,
                    row["commit_lsn"],
                    row["commit_ts_us"],
                    row["txid"],
                    row["event_ordinal"],
                    len(wire),
                )
                transaction_digest.update(terminal + wire)
                assert row["transaction_digest"] == transaction_digest.hexdigest()
                header = terminal[:-8] + transaction_digest.digest() + terminal[-8:]
            chunk_digest.update(struct.pack("!Q", row["frame_ordinal"]) + header + wire)
        assert chunk.digest == chunk_digest.hexdigest()
        prefix = hashlib.sha256(
            domain("chain")
            + prefix
            + unit
            + struct.pack(
                "!5Q",
                chunk.first_frame,
                chunk.count,
                chunk.event_count,
                chunk.member_count,
                chunk.bytes,
            )
            + chunk_digest.digest()
        ).digest()
        assert chunk.chain_digest == prefix.hex()
    terminal = output[-1]
    expected = hashlib.sha256(
        domain("terminal")
        + unit
        + prefix
        + struct.pack(
            "!5Q",
            terminal.end_lsn,
            terminal.member_count,
            terminal.event_count,
            terminal.frame_count,
            terminal.bytes,
        )
    ).hexdigest()
    assert terminal.digest == expected


def test_truncate_only_chunks_keep_every_touched_descriptor_and_reject_missing_ones():
    stream = [relation_for(i) for i in (101, 102)] + transaction(
        change=pgstream.truncate((101, 102))
    )
    output = collect(spool(), stream)
    assert {d.oid for d in chunks(output)[0].descriptors} == {101, 102}
    assert rows(output)[0]["schema_version"] is None  # T header still carries zero hashes.
    with pytest.raises(ValueError, match="descriptor for TRUNCATE"):
        collect(spool(), [REL] + transaction(change=pgstream.truncate((101, 102))))


@pytest.mark.parametrize(
    "messages,reason",
    [
        ([pgstream.begin(100, 1, 1), REL, pgstream.insert(101, ["1"])], "width"),
        ([pgstream.begin(100, 1, 1), relation_for(999)], "unregistered relation"),
        ([pgstream.begin(100, 1, 1), pgstream.insert(102, ["1"])], "missing Relation"),
        ([REL, pgstream.insert(101, ["1", "x"])], "outside transaction"),
        ([pgstream.begin(100, 1, 1), pgstream.begin(100, 1, 2)], "nested transaction"),
        ([pgstream.begin(100, 1, 1), pgstream.commit(101, 102, 1)], "commit boundary"),
        ([pgstream.begin(100, 1, 1), pgstream.commit(100, 100, 1)], "commit boundary"),
        ([pgstream.commit(100, 101, 1)], "outside transaction"),
        ([pgstream.begin(100, 1, 1), pgstream.truncate([777])], "missing Relation"),
    ],
)
def test_malformed_streams_never_complete_a_unit(messages, reason):
    output = []
    s = spool()
    with pytest.raises(ValueError, match=reason):
        for wire in messages:
            output.extend(s.feed(wire))
    assert not any(isinstance(item, UnitComplete) for item in output)


def test_every_truncated_message_in_a_transaction_refuses_without_completion():
    messages = [REL, *transaction()]
    for index, message in enumerate(messages):
        for end in range(len(message)):
            s, output = spool(), []
            for previous in messages[:index]:
                output.extend(s.feed(previous))
            with pytest.raises(ValueError):
                output.extend(s.feed(message[:end]))
            assert not any(isinstance(item, UnitComplete) for item in output)
