"""Unit lifecycle positions and immutable epoch starts, without remote resources."""

from dataclasses import replace

import pytest
from test_ingest_log2 import build, log2  # noqa: F401 (fixture)

from cdc_flight.ingest_log import SourceMismatch
from cdc_flight.ingest_log2 import IngestLog2


def test_initial_epoch_start_survives_checkpoint_advance(log2):  # noqa: F811
    assert log2.epoch_record(1)["start"] == 90
    log2.append(build(log2), partition_day="20261106")
    assert log2.checkpoint() == 102
    assert log2.epoch_record(1)["start"] == 90
    assert log2.reader().units(1, 90, 102)[0].start == 90


def test_registration_rolls_back_source_and_epoch_start_together(log2, monkeypatch):  # noqa: F811
    cfg = replace(
        log2.config,
        source_id="atomic_initial",
        text_binding=replace(log2.config.text_binding, source_id="atomic_initial"),
    )
    fresh = IngestLog2(log2.con, cfg)
    fresh.initialize()
    record = fresh._record_initial_epoch

    def fail(initial_lsn):
        record(initial_lsn)
        raise RuntimeError("after initial epoch record")

    monkeypatch.setattr(fresh, "_record_initial_epoch", fail)
    with pytest.raises(RuntimeError, match="after initial epoch"):
        fresh.register(("system", 1), 77, [])
    assert fresh.execute("SELECT count(*) FROM {s}source").fetchone()[0] == 0
    assert fresh.execute("SELECT count(*) FROM {s}source_epochs").fetchone()[0] == 0
    fresh = IngestLog2(log2.con, cfg)
    fresh.register(("system", 1), 77, [])
    assert fresh.checkpoint() == fresh.epoch_record(1)["start"] == 77


def test_unit_position_distinguishes_frame_ordinals_and_source_member_lsns(log2):  # noqa: F811
    before = log2.unit_position()
    assert before["checkpoint"] == before["confirmed_end"] == 90
    assert before["open_unit_anchor"] is None and not before["partial"]
    assert before["open_chunks"] == before["durable_frame_end"] == 0
    items = build(log2)
    log2.append(items[:1], partition_day="20261106")
    prefix = log2.unit_position()
    assert prefix["source_epoch"] == 1 and prefix["format_version"] == 2
    assert prefix["checkpoint"] == prefix["open_unit_anchor"] == 90
    assert prefix["partial"] and prefix["open_chunks"] == 1
    assert prefix["durable_frame_end"] == 3
    assert prefix["active_commit_lsn"] == 101
    assert prefix["last_member_commit_lsn"] == 100
    assert prefix["last_member_end_lsn"] == 101
    log2.append(items, partition_day="20261106")
    after = log2.unit_position()
    assert after["checkpoint"] == after["confirmed_end"] == 102
    assert after["open_unit_anchor"] is None and not after["partial"]
    assert after["open_chunks"] == after["durable_frame_end"] == 0
    assert after["active_commit_lsn"] is after["last_member_end_lsn"] is None


def test_unit_position_refuses_a_missing_open_ledger(log2):  # noqa: F811
    log2.append(build(log2)[:1], partition_day="20261106")
    log2.execute("DELETE FROM {s}units_v2")
    with pytest.raises(SourceMismatch, match="inconsistent durable open unit"):
        log2.unit_position()


def test_unit_pins_share_the_recorded_initial_cut_and_reject_interior_members(log2):  # noqa: F811
    from cdc_flight.retention import generation_owner
    from cdc_flight.retention2 import Retention2

    retention = Retention2(log2.con, log2.config.database, log2.config.source_id)
    retention.initialize()
    assert retention.initial_cut(1) == 90
    retention.register_generation(101, "generation", 1, 90)
    owner = generation_owner("generation")
    before = retention.pin(owner)
    log2.append(build(log2), partition_day="20261106")
    assert retention.initial_cut(1) == 90  # not the advanced checkpoint
    for interior in (0, 91, 100, 101, 103):
        with pytest.raises(SourceMismatch, match="not an epoch start or complete unit end"):
            retention.advance(owner, "generation", 1, interior)
        assert retention.pin(owner) == before
    assert retention.advance(owner, "generation", 1, 102)
    assert retention.require(owner, "generation", 1, 102)["lsn"] == 102


def test_missing_epoch_start_is_not_guessed_from_existing_units(log2):  # noqa: F811
    from cdc_flight.retention2 import Retention2

    log2.append(build(log2), partition_day="20261106")
    log2.execute("DELETE FROM {s}source_epochs WHERE epoch=1")
    retention = Retention2(log2.con, log2.config.database, log2.config.source_id)
    with pytest.raises(SourceMismatch, match="missing immutable epoch start"):
        retention.register_generation(101, "generation", 1, 102)
    assert retention.pin("generation:generation") is None


def test_epoch_abandons_the_whole_prefix_without_inventing_a_commit(log2):  # noqa: F811
    import json

    from test_spool2 import REL, transaction

    from cdc_flight.ingest_log import FenceLost
    from cdc_flight.retention2 import Retention2
    from cdc_flight.spool2 import Spool

    items = build(log2)
    log2.append(items[:1], partition_day="20261106")
    stale = IngestLog2(log2.con, log2.config)
    stale.token, stale.epoch, stale.slot_name = log2.token, log2.epoch, log2.slot_name
    assert log2.begin_epoch(
        2, 200, "skip", "epoch_2", details={"abandoned_from": 90, "abandoned_to": 200}
    )
    assert log2.unit_position()["confirmed_end"] == 200
    assert not log2.unit_position()["partial"]
    assert log2.execute(
        "SELECT complete,end_lsn,abandoned_into_epoch,abandoned_reason,abandoned_frame_end "
        "FROM {s}units_v2 WHERE epoch=1"
    ).fetchone() == (False, None, 2, "skip", 3)
    stored = log2.execute("SELECT unit_abandonment FROM {s}source_epochs WHERE epoch=2").fetchone()[
        0
    ]
    audit = json.loads(stored)
    assert audit == dict(
        abandoned_unit_start_lsn=90,
        abandoned_frame_end=3,
        abandoned_chunk_count=1,
        abandoned_first_commit_lsn=100,
        abandoned_last_commit_lsn=100,
        abandoned_active_commit_lsn=101,
        abandoned_into_epoch=2,
        abandoned_reason="skip",
    )
    assert all(type(value) is int for key, value in audit.items() if key != "abandoned_reason")
    assert log2.execute(
        "SELECT abandoned_frame_end,abandoned_into_epoch,abandoned_reason FROM {s}units_v2 WHERE epoch=1"
    ).fetchone() == (
        audit["abandoned_frame_end"],
        audit["abandoned_into_epoch"],
        audit["abandoned_reason"],
    )
    assert log2.execute("SELECT status FROM {s}frame_partitions_v2").fetchone() == ("sealed",)
    assert log2.execute("SELECT state FROM {s}tables WHERE relation_id=101").fetchone() == (
        "needs_resync",
    )
    with pytest.raises(FenceLost):
        stale.append(items, partition_day="20261106")
    assert not log2.begin_epoch(2, 200, "skip", "epoch_2")
    assert (
        log2.execute("SELECT unit_abandonment FROM {s}source_epochs WHERE epoch=2").fetchone()[0]
        == stored
    )
    assert log2.epoch_record(2)["unit_abandonment"] == audit
    retention = Retention2(log2.con, log2.config.database, log2.config.source_id)
    assert retention.initial_cut(2) == 200
    spool = Spool(
        replace(log2.config, epoch=log2.epoch),
        {101: ("id",)},
        900,
        "marker",
        binding=log2.config.text_binding,
        start_lsn=retention.initial_cut(2),
        unit_config=log2.config.unit_config,
        clock=lambda: 0,
    )
    new_items = [item for wire in [REL] + transaction(200) for item in spool.feed(wire)]
    new_items.extend(spool.close())
    log2.append(new_items, partition_day="20261107")
    assert log2.reader().units(2, 200, 201)[0].start == retention.initial_cut(2)
    # No old-epoch consumer can cross the abandoned range as if it completed.
    with pytest.raises(ValueError):
        log2.reader().units(1, 90, 102)


@pytest.mark.parametrize("stage", ["before_epoch_commit", "after_epoch_commit"])
def test_epoch_fault_leaves_old_prefix_or_complete_abandonment(log2, stage):  # noqa: F811
    log2.append(build(log2)[:1], partition_day="20261106")

    def fail(at):
        if at == stage:
            raise RuntimeError(stage)

    with pytest.raises(RuntimeError, match=stage):
        log2.begin_epoch(2, 200, "skip", "epoch_2", fault=fail)
    committed = stage == "after_epoch_commit"
    assert log2.execute(
        "SELECT epoch,confirmed_lsn,open_unit_anchor FROM {s}source"
    ).fetchone() == ((2, 200, None) if committed else (1, 90, 90))
    assert log2.execute("SELECT count(*) FROM {s}source_epochs WHERE epoch=2").fetchone()[0] == int(
        committed
    )
    assert log2.execute("SELECT abandoned_into_epoch FROM {s}units_v2").fetchone()[0] == (
        2 if committed else None
    )


def test_epoch_with_no_open_unit_has_sql_null_abandonment(log2):  # noqa: F811
    log2.append(build(log2), partition_day="20261106")
    assert log2.begin_epoch(2, 200, "slot_lost", "epoch_2")
    assert log2.execute(
        "SELECT unit_abandonment FROM {s}source_epochs WHERE epoch=2"
    ).fetchone() == (None,)


@pytest.mark.parametrize("start", [90, 91, 100, 101])
def test_epoch_skip_cannot_start_behind_durable_members(log2, start):  # noqa: F811
    log2.append(build(log2)[:1], partition_day="20261106")
    with pytest.raises(SourceMismatch, match="durable member prefix"):
        log2.begin_epoch(2, start, "skip", "epoch_2")
    assert log2.checkpoint() == 90
    assert log2.execute("SELECT count(*) FROM {s}source_epochs WHERE epoch=2").fetchone()[0] == 0


def test_sql_unit_upsert_cannot_reopen_an_abandoned_prefix(log2):  # noqa: F811
    from test_unit_plans import Recording

    from cdc_flight.ingest_log2 import NULLABLE_UNIT
    from cdc_flight.unit_store import UNIT_TYPES, metadata_parameter

    recording = Recording(log2.con)
    log2.con = recording
    log2.append(build(log2)[:1], partition_day="20261106")
    sql = next(s for s in recording.statements if s.startswith("INSERT INTO") and "units_v2" in s)
    log2.begin_epoch(2, 200, "skip", "epoch_2")
    before = log2.execute("SELECT * FROM {s}units_v2 WHERE epoch=1").fetchone()
    replacement = dict(zip(UNIT_TYPES, before, strict=True))
    replacement.update(abandoned_into_epoch=None, abandoned_reason=None, abandoned_frame_end=None)
    log2.con.execute(sql, [metadata_parameter([replacement], UNIT_TYPES, NULLABLE_UNIT)])
    assert log2.execute("SELECT * FROM {s}units_v2 WHERE epoch=1").fetchone() == before


def test_epoch_skip_just_after_active_commit_is_a_new_epoch_anchor(log2):  # noqa: F811
    log2.append(build(log2)[:1], partition_day="20261106")
    assert log2.unit_position()["active_commit_lsn"] == 101
    assert log2.begin_epoch(2, 102, "skip", "epoch_2")
    assert log2.epoch_record(2)["start"] == 102
