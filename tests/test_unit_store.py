"""Independent unit-chain corruption tests (format 2).

A real multi-unit, two-day store is built only through the public API (Spool2 ->
IngestLog2.append). Each test then corrupts the stored metadata or frames directly,
as a crash or an operator could, and asserts the reader or the writer fails closed:
an error, never "caught up", never a silent skip or a shortened chain.
"""

from dataclasses import replace

import duckdb
import pytest
from test_format2_protocol import binding, config
from test_spool2 import REL, transaction

from cdc_flight.format2 import Chunk, UnitComplete, UnitConfig
from cdc_flight.ingest_log import SourceMismatch
from cdc_flight.ingest_log2 import IngestConfig2, IngestLog2
from cdc_flight.spool2 import Spool

UNIT = UnitConfig(max_commits=2, chunk_frames=3)  # 2 commits per unit, small chunks
LSNS = (100, 110, 120, 130, 140, 150)  # 6 commits: 3 units of 2


@pytest.fixture
def store(stores):
    s, _ = stores
    base = replace(config(), database=s.database.strip('"'))
    cfg = IngestConfig2(**base.__dict__, text_binding=binding(), unit_config=UNIT)
    log = IngestLog2(s.con, cfg)
    log.initialize()
    log.register(("system", 1), 90, [dict(oid=101, schema="public", name="items", keys=["id"])])
    log.acquire("writer")
    try:
        spool = Spool(
            cfg,
            {101: ("id",)},
            900,
            "marker",
            binding=cfg.text_binding,
            start_lsn=90,
            unit_config=UNIT,
            clock=lambda: 0,
        )
        items = []
        for i, lsn in enumerate(LSNS):
            for wire in ([REL] if i == 0 else []) + transaction(lsn, (str(i), "t")):
                items.extend(spool.feed(wire))
        items.extend(spool.close())
        units = [i for i in items if isinstance(i, UnitComplete)]
        assert len(units) == 3
        # The first unit lands on one day, the rest on the next: a cross-day chain.
        first_end = [i for i, x in enumerate(items) if isinstance(x, UnitComplete)][0] + 1
        assert log.append(items[:first_end], partition_day="20261106") == units[0].end_lsn
        high = log.append(items[first_end:], partition_day="20261107")
        assert high == units[-1].end_lsn
        yield log, items, high
    finally:
        log.release()


def chain(log, high, after=90):
    reader = log.reader()
    return reader, reader.units(1, after, high)


# The baseline -----------------------------------------------------------------------------


def test_the_baseline_chain_is_whole_contiguous_and_cross_day(store):
    log, items, high = store
    reader, units = chain(log, high)
    assert len(units) == 3
    assert [u.start for u in units[1:]] == [u.end for u in units[:-1]]
    assert units[0].start == 90 and units[-1].end == high
    assert units[0].days == ("20261106",) and units[-1].days == ("20261107",)
    frames = reader.frames(units, high=high)
    assert len(frames) == sum(u.frame_count for u in units)
    stored = {(c.id, c.digest) for c in items if isinstance(c, Chunk)}
    assert set(log.execute("SELECT id,digest FROM {s}chunks_v2").fetchall()) == stored


def test_resource_limits_take_whole_units_and_the_first_may_overshoot(store):
    log, _, high = store
    reader = log.reader()
    first = reader.units(1, 90, high, max_events=1)
    assert len(first) == 1 and first[0].event_count >= 1  # indivisible: overshoots
    two = reader.units(1, 90, high, max_events=first[0].event_count + 1)
    assert len(two) == 1  # never splits the second unit to fit


# The unit chain (units) -------------------------------------------------------------------


def test_a_missing_middle_unit_is_a_gap_not_caught_up(store):
    log, _, high = store
    _, units = chain(log, high)
    log.execute("DELETE FROM {s}units_v2 WHERE unit_start_lsn=?", [units[1].start])
    with pytest.raises(ValueError, match="gap"):
        log.reader().units(1, 90, high)


def test_a_missing_last_unit_does_not_read_as_caught_up(store):
    """The chain stops before the snapshotted high: a shorter chain must not pass."""
    log, _, high = store
    _, units = chain(log, high)
    log.execute("DELETE FROM {s}units_v2 WHERE unit_start_lsn=?", [units[-1].start])
    with pytest.raises(ValueError, match="does not reach"):
        log.reader().units(1, 90, high)


def test_an_overlapping_unit_fails(store):
    log, _, high = store
    _, units = chain(log, high)
    log.execute(
        "UPDATE {s}units_v2 SET end_lsn=? WHERE unit_start_lsn=?",
        [units[1].end + 1, units[0].start],
    )
    with pytest.raises(ValueError, match="gap|overlap"):
        log.reader().units(1, 90, high)


def test_a_read_starting_or_ending_inside_a_unit_fails(store):
    log, _, high = store
    _, units = chain(log, high)
    inside = units[1].start + 1
    with pytest.raises(ValueError):
        log.reader().units(1, inside, high)  # after inside a unit
    with pytest.raises(ValueError):
        log.reader().units(1, 90, units[1].end - 1)  # high inside a unit


def test_an_incomplete_unit_below_high_fails(store):
    log, _, high = store
    _, units = chain(log, high)
    log.execute("UPDATE {s}units_v2 SET complete=false WHERE unit_start_lsn=?", [units[1].start])
    with pytest.raises(ValueError, match="incomplete"):
        log.reader().units(1, 90, high)


def test_a_high_beyond_the_durable_end_or_another_epoch_fails(store):
    log, _, high = store
    with pytest.raises(ValueError):
        log.reader().units(1, 90, high + 1)
    with pytest.raises(ValueError):
        log.reader().units(2, 90, high)


def test_an_open_anchor_other_than_the_confirmed_end_fails(store):
    log, _, high = store
    log.execute("UPDATE {s}source SET open_unit_anchor=? WHERE id=1", [high - 1])
    with pytest.raises(SourceMismatch, match="anchor"):
        log.reader().units(1, 90, high)


def test_a_reader_with_another_binding_is_refused(store):
    log, _, high = store
    other = replace(
        log.config, text_binding=type(binding())(binding().source_id, binding().settings, ())
    )
    if other.text_binding.digest == log.config.text_binding.digest:
        pytest.skip("the test binding has no extensions to drop")
    from cdc_flight.unit_store import UnitReader

    with pytest.raises(SourceMismatch, match="binding"):
        UnitReader(log.con, other).units(1, 90, high)


# Physical frames (frames) ------------------------------------------------------------------


def test_a_lost_frame_fails_the_physical_count(store):
    log, _, high = store
    reader, units = chain(log, high)
    log.execute(
        "DELETE FROM {s}frames_v2_20261107 WHERE unit_start_lsn=? AND frame_ordinal=1",
        [units[1].start],
    )
    with pytest.raises(ValueError, match="count|gap"):
        reader.frames(units, high=high)


def test_a_duplicated_frame_fails(store):
    log, _, high = store
    reader, units = chain(log, high)
    log.execute(
        "INSERT INTO {s}frames_v2_20261107 SELECT * FROM {s}frames_v2_20261107 "
        "WHERE unit_start_lsn=? AND frame_ordinal=0",
        [units[1].start],
    )
    with pytest.raises(ValueError, match="count|duplicate"):
        reader.frames(units, high=high)


def test_a_stray_frame_inside_the_range_fails_the_total_count(store):
    """A frame whose unit_start_lsn lies inside the read range but matches no unit:
    every unit's own ordinals stay whole, only the total count reveals it."""
    log, _, high = store
    reader, units = chain(log, high)
    log.execute(
        "INSERT INTO {s}frames_v2_20261107 SELECT * REPLACE (unit_start_lsn+1 AS unit_start_lsn) "
        "FROM {s}frames_v2_20261107 WHERE unit_start_lsn=? AND frame_ordinal=0",
        [units[1].start],
    )
    with pytest.raises(ValueError, match="count"):
        reader.frames(units, high=high)


def test_a_lost_frame_compensated_elsewhere_fails_the_ordinals(store):
    """One unit loses a frame while another gains a duplicate: the total count still
    matches, only the per-unit ordinals reveal it."""
    log, _, high = store
    reader, units = chain(log, high)
    log.execute(
        "DELETE FROM {s}frames_v2_20261107 WHERE unit_start_lsn=? AND frame_ordinal=1",
        [units[1].start],
    )
    log.execute(
        "INSERT INTO {s}frames_v2_20261107 SELECT * FROM {s}frames_v2_20261107 "
        "WHERE unit_start_lsn=? AND frame_ordinal=0",
        [units[2].start],
    )
    with pytest.raises(ValueError, match="gap or duplicate"):
        reader.frames(units, high=high)


def test_frames_on_a_day_the_chunks_do_not_name_are_not_silently_omitted(store):
    """Moving a unit's frames to another (unreferenced) day table must not read as a
    smaller, valid unit: the count check fails."""
    log, _, high = store
    reader, units = chain(log, high)
    log.execute("CREATE TABLE {s}frames_v2_20261108 AS SELECT * FROM {s}frames_v2_20261107")
    log.execute("DELETE FROM {s}frames_v2_20261107 WHERE unit_start_lsn=?", [units[2].start])
    with pytest.raises(ValueError, match="count|gap"):
        reader.frames(units, high=high)


def test_a_chunk_naming_a_missing_day_table_fails(store):
    log, _, high = store
    log.execute(
        "UPDATE {s}chunks_v2 SET partition_day='20261109' WHERE unit_start_lsn="
        "(SELECT max(unit_start_lsn) FROM {s}units_v2)"
    )
    reader, units = chain(log, high)
    # It fails closed, as a catalog error: a missing table never reads as empty.
    with pytest.raises(duckdb.CatalogException, match="frames_v2_20261109"):
        reader.frames(units, high=high)


def test_a_non_contiguous_unit_list_is_refused(store):
    log, _, high = store
    reader, units = chain(log, high)
    with pytest.raises(ValueError, match="contiguous"):
        reader.frames((units[0], units[2]), high=high)


def test_relation_filter_keeps_commits_and_order(store):
    log, _, high = store
    reader, units = chain(log, high)
    every = reader.frames(units, high=high)
    filtered = reader.frames(units, 101, high=high)
    assert filtered == every  # one relation: nothing dropped, the original order kept
    assert [(r["unit_start_lsn"], r["frame_ordinal"]) for r in filtered] == sorted(
        (r["unit_start_lsn"], r["frame_ordinal"]) for r in filtered
    )


# The writer (append / recovery) -------------------------------------------------------------


def test_reappending_the_same_items_is_a_no_op(store):
    log, items, high = store
    before = log.execute("SELECT count(*) FROM {s}frames_v2_20261107").fetchone()[0]
    first_end = [i for i, x in enumerate(items) if isinstance(x, UnitComplete)][0] + 1
    assert log.append(items[first_end:], partition_day="20261107") == high
    assert log.execute("SELECT count(*) FROM {s}frames_v2_20261107").fetchone()[0] == before


def test_a_ledger_whose_physical_frames_are_gone_is_not_acknowledged(store):
    """The ledger says the chunks exist, but their frames are gone: a retry must not
    treat the ledger alone as proof, nor silently re-acknowledge the end."""
    log, items, high = store
    first_end = [i for i, x in enumerate(items) if isinstance(x, UnitComplete)][0] + 1
    log.execute("DELETE FROM {s}frames_v2_20261107")
    with pytest.raises(ValueError, match="physical count mismatch"):
        log.append(items[first_end:], partition_day="20261107")


def test_a_terminal_with_different_counts_is_refused(store):
    """A complete unit's terminal is immutable: re-sending it with different counts
    (or chain) must not overwrite it."""
    log, items, high = store
    last = [i for i in items if isinstance(i, UnitComplete)][-1]
    forged = replace(last, member_count=last.member_count + 1)
    with pytest.raises(ValueError, match="terminal identity changed"):
        log.append([forged], partition_day="20261107")
    (members,) = log.execute(
        "SELECT member_count FROM {s}units_v2 WHERE unit_start_lsn=?", [last.unit_start_lsn]
    ).fetchone()
    assert members == last.member_count


def test_recovery_after_corrupting_a_stored_chunk_digest_fails(store):
    log, items, high = store
    log.execute(
        "UPDATE {s}chunks_v2 SET digest=repeat('0',64) WHERE unit_start_lsn="
        "(SELECT max(unit_start_lsn) FROM {s}units_v2)"
    )
    first_end = [i for i, x in enumerate(items) if isinstance(x, UnitComplete)][0] + 1
    with pytest.raises(ValueError, match="different contents"):
        log.append(items[first_end:], partition_day="20261107")
