"""Steady key checks have fixed destination statement cost and exact unit cuts."""

import pytest
from reference.key_updates_ref import record_keys as per_table_reference
from test_ingest_log2 import build, log2  # noqa: F401
from test_retention2 import emit
from test_roundtrips import Counting

from cdc_flight.ingest_common import SourceMismatch


def tables(log, count):
    log.execute("UPDATE {s}tables SET catalog_key=key_columns,key_checked_lsn=90,key_epoch=1")
    for oid in range(102, 101 + count):
        log.execute(
            "INSERT INTO {s}tables (relation_id,schema_name,table_name,key_columns,catalog_key,"
            "key_version,key_epoch,key_checked_lsn,state) VALUES (?,'public',?,'[\"id\"]',"
            "'[\"id\"]',0,1,90,'snapshotting')",
            [oid, f"items{oid}"],
        )
    log._clear_registry()
    return {oid: ["id"] for oid in range(101, 101 + count)}


def snapshot(log):
    return (
        log.execute("SELECT * FROM {s}tables ORDER BY relation_id").fetchall(),
        log.execute("SELECT * FROM {s}key_changes ORDER BY relation_id,version").fetchall(),
    )


def counted(log):
    counting = Counting(log.con, log.prefix)
    log.con = counting
    return counting


@pytest.mark.parametrize("count", [2, 50])
def test_steady_key_and_finish_statements_do_not_scale_with_tables(log2, count):  # noqa: F811
    log2.append(build(log2), partition_day="20261106")
    observed = tables(log2, count)
    counting = counted(log2)
    log2.record_keys(observed, 102, 110)
    kinds = counting.take()
    assert len(kinds) == 10
    assert kinds.count("read:cut") == 2  # boundary + all distinct prior cuts
    assert kinds.count("update:tables") == 1
    assert (
        log2.finish("finish", observed, 102, 120, "drained", None, lambda status: status)
        == "drained"
    )
    kinds = counting.take()
    assert len(kinds) == 15
    assert kinds.count("read:cut") == 2 and kinds.count("update:tables") == 1
    assert log2.execute(
        "SELECT key_checked_lsn,key_epoch,count(*) FROM {s}tables GROUP BY ALL"
    ).fetchall() == [(102, 1, count)]


def test_a_second_invalid_cut_among_ten_tables_rolls_back_every_row(log2):  # noqa: F811
    log2.append(build(log2), partition_day="20261106")
    observed = tables(log2, 10)
    log2.execute("UPDATE {s}tables SET key_checked_lsn=102")
    log2.execute("UPDATE {s}tables SET key_checked_lsn=90 WHERE relation_id=101")
    log2.execute("UPDATE {s}tables SET key_checked_lsn=91 WHERE relation_id=110")
    assert log2.execute("SELECT DISTINCT key_checked_lsn FROM {s}tables ORDER BY 1").fetchall() == [
        (90,),
        (91,),
        (102,),
    ]
    before = snapshot(log2)
    with pytest.raises(SourceMismatch, match="complete unit end"):
        log2.record_keys(observed, 102, 110)
    assert snapshot(log2) == before


def test_pair_deduplication_does_not_accept_an_end_from_another_epoch(log2):  # noqa: F811
    log2.append(build(log2), partition_day="20261106")
    log2.begin_epoch(2, 50, "slot_lost", "slot_e2")
    with pytest.raises(SourceMismatch, match="complete unit end"):
        log2._key_cuts(((1, 102), (2, 102)))
    with pytest.raises(SourceMismatch, match="complete unit end"):
        log2._key_cuts(((1, 90), (2, 90)))  # epoch 1's start is not epoch 2's start
    # NULL key_epoch still means validate the saved cut in CURRENT epoch 2.
    log2.execute("UPDATE {s}tables SET key_checked_lsn=102,key_epoch=NULL")
    before = snapshot(log2)
    with pytest.raises(SourceMismatch, match="complete unit end"):
        log2.record_keys({101: ["id"]}, 50, 110)
    assert snapshot(log2) == before


def test_retained_endpoints_remain_valid_key_cuts_at_the_floor(log2):  # noqa: F811
    log2.append(build(log2), partition_day="20261105")
    log2.append(emit(log2, 102, 102, 103), partition_day="20261106")
    log2.execute("UPDATE {s}retention SET floor_epoch=1,floor_lsn=102")
    log2.execute("UPDATE {s}tables SET key_checked_lsn=102,key_epoch=1")
    log2.record_keys({101: ["id"]}, 104, 110)
    assert log2.execute("SELECT key_checked_lsn FROM {s}tables").fetchone() == (104,)


@pytest.mark.parametrize("position", [2**53 + 1, 0xFFFF_FFFF_0000_0001, 2**64 - 1])
def test_cut_validation_and_batched_updates_keep_unsigned_lsn_bits(log2, position):  # noqa: F811
    log2.append(build(log2), partition_day="20261106")
    # Metadata binding test: the whole-unit END and source high are set together.
    log2.execute("UPDATE {s}units_v2 SET end_lsn=?", [position])
    log2.execute("UPDATE {s}source SET confirmed_lsn=?", [position])
    log2.execute("UPDATE {s}tables SET key_checked_lsn=?,key_epoch=1", [position])
    log2._key_cuts(((1, position),))
    log2.record_keys({101: ["id"]}, position, position)
    assert log2.execute("SELECT key_checked_lsn,key_epoch FROM {s}tables").fetchone() == (
        position,
        1,
    )


def test_empty_prior_cut_and_confirmation_sets_emit_no_statements(log2):  # noqa: F811
    counting = counted(log2)
    log2._key_cuts(())
    assert counting.take() == []
    log2.record_keys({}, 90, 95)
    kinds = counting.take()
    assert len(kinds) == 8 and kinds.count("read:cut") == 1  # boundary only
    assert "update:tables" not in kinds


def test_one_changed_key_adds_only_its_rare_queue_and_activation_work(log2):  # noqa: F811
    log2.append(build(log2), partition_day="20261106")
    observed = tables(log2, 10)
    observed[110] = ["v"]
    counting = counted(log2)
    log2.record_keys(observed, 102, 110)
    assert len(counting.take()) == 15  # steady10 + queue1 + activation4
    assert log2.execute(
        "SELECT relation_id,key_checked_lsn,key_version,pending_keys_v2 FROM {s}tables ORDER BY relation_id"
    ).fetchall() == [(oid, 102, 0, None) for oid in range(101, 110)] + [(110, 90, 1, None)]


@pytest.mark.parametrize("mixed", [False, True])
def test_stored_rows_match_the_frozen_per_table_path(log2, mixed):  # noqa: F811
    log2.append(build(log2), partition_day="20261106")
    observed = tables(log2, 10)
    if mixed:
        observed[110] = ["v"]
    outputs = []
    for run in (per_table_reference, type(log2)._record_keys):
        log2.execute("BEGIN")
        try:
            log2.guard()
            run(log2, observed, 102, 110)
            outputs.append(snapshot(log2))
        finally:
            log2.execute("ROLLBACK")
    assert outputs[0] == outputs[1]


@pytest.mark.parametrize("abandoned", [False, True])
def test_an_incomplete_or_abandoned_end_never_authorizes_key_confirmation(log2, abandoned):  # noqa: F811
    log2.append(build(log2), partition_day="20261106")
    log2.execute(
        "UPDATE {s}units_v2 SET complete=false,abandoned_into_epoch=?", [2 if abandoned else None]
    )
    log2.execute("UPDATE {s}tables SET key_checked_lsn=102,key_epoch=1")
    before = snapshot(log2)
    with pytest.raises(SourceMismatch, match="complete unit end"):
        log2.record_keys({101: ["id"]}, 90, 110)
    assert snapshot(log2) == before


def test_an_old_key_epoch_confirms_at_the_current_epochs_initial_cut(log2):  # noqa: F811
    log2.append(build(log2), partition_day="20261106")
    log2.begin_epoch(2, 50, "slot_lost", "slot_e2")
    log2.execute("UPDATE {s}tables SET key_epoch=1,key_checked_lsn=102")
    log2.record_keys({101: ["id"]}, 50, 110)
    assert log2.execute("SELECT key_checked_lsn,key_epoch FROM {s}tables").fetchone() == (50, 2)
