"""The small-chunk builder preserves the exact typed row and error contract."""

import copy
import json
from pathlib import Path

import pyarrow as pa
import pytest

from cdc_flight import spool2
from cdc_flight.format2 import FRAME_SCHEMA


def frames():
    projections = json.loads(
        (Path(__file__).parent / "fixtures/format2-protocol.json").read_text()
    )["mapping_projections"]
    for i, projection in enumerate(projections):
        event = dict(
            record_kind=1,
            unit_start_lsn=90,
            frame_ordinal=0,
            source_epoch=1,
            commit_lsn=2**63 + 123,
            event_ordinal=0,
            commit_ts_us=-5,
            txid=7,
            relation_id=0 if i == 6 else 101,
            schema_name=None if i == 6 else "public",
            table_name=None if i == 6 else "items",
            schema_version=None if i == 6 else "12" * 32,
            key_layout_hash=None if i == 6 else "34" * 32,
            op=("insert", "update", "update", "update", "delete", "delete", "truncate")[i],
            wire_length=31,
            **projection,
        )
        commit = dict(
            record_kind=2,
            unit_start_lsn=90,
            frame_ordinal=1,
            source_epoch=1,
            commit_lsn=2**63 + 123,
            event_ordinal=2 if i == 6 else 1,
            commit_ts_us=-5,
            txid=7,
            op="commit",
            old_tuple_kind=0,
            end_lsn=2**63 + 124,
            end_ts_us=-4,
            transaction_digest="56" * 32,
            wire_length=26,
        )
        yield [event, commit]
    # Existing zero-column image is different from a missing tuple image.
    empty = dict(
        event,
        relation_id=102,
        schema_name="public",
        table_name="empty",
        schema_version="78" * 32,
        key_layout_hash="9a" * 32,
        op="insert",
        old=None,
        old_key=None,
        new={"values": [], "kinds": []},
        new_key={"values": [], "kinds": []},
        old_tuple_kind=0,
        truncate_oids=None,
        truncate_options=None,
    )
    yield [empty, dict(commit, event_ordinal=1)]
    yield [
        dict(commit, event_ordinal=0)
    ]  # A delivered empty source transaction has a COMMIT-only chunk.


def assert_equivalent(rows):
    before = copy.deepcopy(rows)
    expected = pa.Table.from_pylist(rows, schema=FRAME_SCHEMA)
    for builder in (spool2._small_frame_table, spool2._frame_table):
        actual = builder(rows)
        assert rows == before
        assert actual.schema.equals(FRAME_SCHEMA, check_metadata=True)
        assert actual.equals(expected, check_metadata=True)


def test_both_builders_preserve_protocol_cells_controls_and_unsigned_values():
    for rows in frames():
        assert_equivalent(rows)
        assert_equivalent(rows[:1])
        assert_equivalent(rows + rows[:1])  # Equivalence also holds beyond the cutoff.


def test_small_builder_preserves_schema_metadata(monkeypatch):
    schema = FRAME_SCHEMA.with_metadata({b"format": b"2", b"purpose": b"frozen-row-contract"})
    monkeypatch.setattr(spool2, "FRAME_SCHEMA", schema)
    rows = next(frames())
    expected = pa.Table.from_pylist(rows, schema=schema)
    actual = spool2._small_frame_table(rows)
    assert actual.schema.equals(schema, check_metadata=True)
    assert actual.equals(expected, check_metadata=True)


def test_null_top_level_frames_are_rejected():
    with pytest.raises(ValueError, match="null frame rows"):
        spool2._small_frame_table([None])


def test_small_builder_errors_propagate_instead_of_silent_fallback(monkeypatch):
    def fail(_rows):
        raise RuntimeError("injected Arrow conversion failure")

    monkeypatch.setattr(spool2, "_small_frame_table", fail)
    with pytest.raises(RuntimeError, match="injected Arrow conversion"):
        spool2._frame_table(next(frames()))
