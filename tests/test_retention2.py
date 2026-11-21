"""Whole-unit retention cuts, cross-day prefixes and source-guard races."""

from dataclasses import replace

import duckdb
import pytest
from test_ingest_log2 import build, log2  # noqa: F401 (fixture)
from test_spool2 import REL, transaction

from cdc_flight.ingest_log2 import IngestLog2
from cdc_flight.retention import HistoryDropped, generation_owner
from cdc_flight.retention2 import Retention2
from cdc_flight.spool2 import Spool


def emit(log, start, *commits):
    spool = Spool(
        replace(log.config, epoch=log.epoch),
        {101: ("id",)},
        900,
        "marker",
        binding=log.config.text_binding,
        start_lsn=start,
        unit_config=log.config.unit_config,
        clock=lambda: 0,
    )
    items = [
        item
        for wire in [REL] + [w for c in commits for w in transaction(c)]
        for item in spool.feed(wire)
    ]
    items.extend(spool.close())
    return items


@pytest.fixture
def retained(log2):  # noqa: F811
    retention = Retention2(log2.con, log2.config.database, log2.config.source_id)
    retention.initialize()
    token = retention.acquire_compaction("compactor")
    try:
        yield retention, token
    finally:
        retention.release_compaction(token)


def three_days(log):
    log.append(build(log), partition_day="20261104")
    log.append(emit(log, 102, 102, 103), partition_day="20261105")
    log.append(emit(log, 104, 104, 105), partition_day="20261106")


@pytest.mark.parametrize("operation", ["register", "transfer"])
def test_consumer_pin_writes_change_the_shared_retention_guard(log2, retained, operation):  # noqa: F811
    """An unchanged-value UPDATE cannot fence a concurrent DROP on MotherDuck."""
    retention, _ = retained
    log2.append(build(log2))
    if operation == "transfer":
        retention.register_generation(101, "reader", 1, 90)
    before = retention.execute("SELECT guard FROM {s}retention WHERE id=1").fetchone()
    if operation == "register":
        retention.register_generation(101, "reader", 1, 90)
    else:
        assert retention.transfer(101, "reader", 1, 102)
    after = retention.execute("SELECT guard FROM {s}retention WHERE id=1").fetchone()
    assert before[0] is not None and after[0] is not None
    assert after != before


def test_a_read_above_the_strict_floor_survives_compaction(log2, retained):  # noqa: F811
    retention, token = retained
    three_days(log2)
    retention.register_generation(101, "reader", 1, 104)
    assert retention.drop_partition("20261104", token)
    assert retention.floor() == (1, 102)
    assert retention.require(generation_owner("reader"), "reader", 1, 104)
    reader = log2.reader()
    units = reader.units(1, 104, 106)
    assert len(reader.events(units, 101, high=106)) == 2
    # Equality would delete the very boundary that this reader is entitled to use.
    assert not retention.drop_partition("20261105", token)
    retention.check_read(1, 104)
    with pytest.raises(HistoryDropped):
        retention.check_read(1, 102)


def test_cross_day_open_unit_protects_both_days_and_metadata_until_last_drop(log2, retained):  # noqa: F811
    retention, token = retained
    items = build(log2)
    log2.append(items[:1], partition_day="20261104")
    log2.append(items[1:2], partition_day="20261105")
    retention.register_generation(101, "reader", 1, 90)
    assert log2.unit_position()["partial"]
    assert not retention.drop_partition("20261104", token)
    assert retention.floor() is None
    log2.append(items[2:], partition_day="20261105")
    log2.append(emit(log2, 102, 102, 103), partition_day="20261106")
    retention.advance(generation_owner("reader"), "reader", 1, 104)
    assert retention.drop_partition("20261104", token)
    assert retention.prune(token) == (1, 0)
    assert (
        log2.execute("SELECT count(*) FROM {s}units_v2 WHERE unit_start_lsn=90").fetchone()[0] == 1
    )
    assert retention.drop_partition("20261105", token)
    assert retention.prune(token) == (1, 1)
    assert (
        log2.execute("SELECT count(*) FROM {s}units_v2 WHERE unit_start_lsn=90").fetchone()[0] == 0
    )
    assert log2.execute("SELECT count(*) FROM {s}descriptors_v2").fetchone()[0] == 1


def test_abandoned_prefix_reclaims_after_resync_but_pin_at_anchor_blocks(log2, retained):  # noqa: F811
    retention, token = retained
    log2.append(build(log2)[:1], partition_day="20261104")
    retention.register_generation(101, "old", 1, 90)
    log2.begin_epoch(2, 200, "skip", "epoch_2")
    assert not retention.drop_partition("20261104", token)
    assert log2.execute("SELECT state FROM {s}tables WHERE relation_id=101").fetchone() == (
        "needs_resync",
    )
    retention.register_generation(101, "new", 2, 200)
    retention.release(generation_owner("old"), "old")
    assert retention.drop_partition("20261104", token)
    assert retention.floor() == (1, 90)
    assert retention.prune(token) == (1, 1)
    with pytest.raises(HistoryDropped):
        retention.check_read(1, 90)


@pytest.mark.parametrize("erase_counts", [False, True])
def test_missing_ledger_cannot_authorize_a_partition_drop(log2, retained, erase_counts):  # noqa: F811
    from cdc_flight.ingest_log import SourceMismatch

    retention, token = retained
    three_days(log2)
    retention.register_generation(101, "reader", 1, 104)
    log2.execute("DELETE FROM {s}chunks_v2 WHERE partition_day='20261104'")
    if erase_counts:
        log2.execute("UPDATE {s}frame_partitions_v2 SET n=0 WHERE partition_day='20261104'")
    with pytest.raises(SourceMismatch, match="inconsistent unit partition inventory"):
        retention.drop_partition("20261104", token)
    assert "frames_v2_20261104" in log2.catalog()
    assert retention.floor() is None


def other_writer(log, stores):
    other = IngestLog2(stores[1].con, log.config)
    other.token, other.epoch, other.slot_name = log.token, log.epoch, log.slot_name
    return other


def test_append_winning_after_drop_plan_aborts_the_drop(log2, retained, stores):  # noqa: F811
    retention, token = retained
    three_days(log2)
    retention.register_generation(101, "reader", 1, 104)
    other = other_writer(log2, stores)
    pending = emit(log2, 106, 106, 107)

    def fault(stage):
        if stage == "after_drop_plan":
            assert other.append(pending, partition_day="20261106") == 108

    with pytest.raises(duckdb.TransactionException, match="[Cc]onflict"):
        retention.drop_partition("20261104", token, fault=fault)
    assert "frames_v2_20261104" in log2.catalog()
    assert retention.floor() is None
    assert log2.checkpoint() == 108


def test_drop_guard_winning_aborts_append_without_changing_the_ingest_lease(log2, retained, stores):  # noqa: F811
    retention, token = retained
    three_days(log2)
    retention.register_generation(101, "reader", 1, 104)
    other = other_writer(log2, stores)
    pending = emit(log2, 106, 106, 107)
    lease = log2.execute("SELECT token,holder,epoch_us(expires_at) FROM {s}source").fetchone()
    guard = log2.execute("SELECT guard FROM {s}source").fetchone()[0]
    retention_guard = retention.execute("SELECT guard FROM {s}retention").fetchone()[0]
    attempted = []

    def fault(stage):
        if stage == "after_source_guard":
            with pytest.raises(duckdb.TransactionException, match="[Cc]onflict"):
                other.append(pending, partition_day="20261106")
            attempted.append(True)

    assert retention.drop_partition("20261104", token, fault=fault)
    assert attempted == [True]
    assert log2.execute("SELECT guard FROM {s}source").fetchone()[0] != guard
    assert retention.execute("SELECT guard FROM {s}retention").fetchone()[0] != retention_guard
    assert log2.checkpoint() == 106
    assert log2.execute("SELECT count(*) FROM {s}frames_v2_20261106").fetchone()[0] == 4
    assert (
        log2.execute("SELECT token,holder,epoch_us(expires_at) FROM {s}source").fetchone() == lease
    )


def test_shared_day_is_not_dropped_while_any_epoch_is_active(log2, retained):  # noqa: F811
    retention, token = retained
    log2.append(build(log2)[:1], partition_day="20261104")
    log2.begin_epoch(2, 200, "skip", "epoch_2")
    log2.append(emit(log2, 200, 200), partition_day="20261104")
    retention.register_generation(101, "reader", 2, 201)
    assert log2.execute(
        "SELECT epoch,status FROM {s}frame_partitions_v2 WHERE partition_day='20261104' ORDER BY epoch"
    ).fetchall() == [(1, "sealed"), (2, "active")]
    assert not retention.drop_partition("20261104", token)
    assert log2.execute("SELECT count(*) FROM {s}frames_v2_20261104").fetchone()[0] == 5
    log2.append(emit(log2, 201, 201), partition_day="20261105")
    retention.advance(generation_owner("reader"), "reader", 2, 202)
    assert retention.drop_partition("20261104", token)
    assert retention.floor() == (2, 201)
    assert retention.prune(token) == (2, 2)


def test_cut_validation_includes_floor_and_epoch_start_in_one_statement(retained, monkeypatch):
    retention, _ = retained
    execute = retention.execute
    statements = []

    def recorded(sql, parameters=None):
        statements.append(sql)
        return execute(sql, parameters)

    monkeypatch.setattr(retention, "execute", recorded)
    retention.check_read(1, 90)
    assert len(statements) == 1


def test_retention_uses_typed_abandonment_not_audit_json(log2, retained):  # noqa: F811
    retention, token = retained
    log2.append(build(log2)[:1], partition_day="20261104")
    log2.begin_epoch(2, 200, "skip", "epoch_2")
    retention.register_generation(101, "new", 2, 200)
    # Deliberately corrupt the display/audit copy: it is never retention authority.
    log2.execute("UPDATE {s}source_epochs SET unit_abandonment='invalid-json' WHERE epoch=2")
    assert retention.drop_partition("20261104", token)
    assert retention.floor() == (1, 90)


def test_expiry_day_summary_never_treats_an_open_unit_as_sealed(log2, retained):  # noqa: F811
    from cdc_flight.compact import CompactConfig
    from cdc_flight.expiry import thresholds

    retention, _ = retained
    items = build(log2)
    log2.append(items[:1], partition_day="20200101")
    log2.append(items[1:2], partition_day="20200102")
    config = CompactConfig(
        log2.config.source_id, database=log2.config.database, max_retained_lag_hours=1
    )
    lag, _, _, _, days = thresholds(retention, config)
    assert lag is None
    assert len(days) == 2 and all(d["status"] == "active" for d in days)
    log2.append(items[2:], partition_day="20200102")
    log2.append(emit(log2, 102, 102), partition_day="20200103")
    lag, _, _, _, days = thresholds(retention, config)
    assert lag == (1, 102)
    assert all(d["max_lsn"] == 102 for d in days[:2])  # unit END, not the anchor


def test_injected_compaction_records_unit_counters_without_legacy_tables(log2, monkeypatch):  # noqa: F811
    from cdc_flight import compact, expiry

    three_days(log2)
    retention = Retention2(log2.con, log2.config.database, log2.config.source_id)
    retention.initialize()
    retention.register_generation(101, "reader", 1, 104)
    # This test isolates the orchestration/record seam; expiry policy is exercised
    # separately and its mirror-failure ordering joins the consumer integration.
    monkeypatch.setattr(expiry, "run", lambda *a, **kw: expiry.Summary())
    result = compact.run_compaction(
        compact.CompactConfig(log2.config.source_id, database=log2.config.database),
        log2.con,
        retention=retention,
    )
    assert result.days == ["20261104"]
    assert (result.format_version, result.chunks, result.units) == (2, 2, 1)
    assert retention.execute(
        "SELECT format_version,chunks_pruned,units_pruned,batches_pruned,manifests_pruned "
        "FROM {s}compactions WHERE run_id=?",
        [result.run_id],
    ).fetchone() == (2, 2, 1, None, None)
    assert "log_partitions" not in log2.catalog()


@pytest.mark.parametrize("prune", [False, True])
def test_a_fresh_writer_recovers_and_appends_after_drop(log2, retained, stores, prune):  # noqa: F811
    retention, token = retained
    three_days(log2)
    retention.register_generation(101, "reader", 1, 104)
    assert retention.drop_partition("20261104", token)
    if prune:
        retention.prune(token)
    fresh = other_writer(log2, stores)
    fresh.acquired_lsn = log2.checkpoint()
    assert fresh.recovery().confirmed_end == 106
    assert fresh._days["20261104", 1]["status"] == "dropped"
    assert fresh.append(emit(fresh, 106, 106), partition_day="20261106") == 107


def test_a_dropped_day_with_its_old_physical_table_still_present_refuses(log2):  # noqa: F811
    log2.append(build(log2), partition_day="20261104")
    log2.execute("UPDATE {s}frame_partitions_v2 SET status='dropped'")
    fresh = IngestLog2(log2.con, log2.config)
    with pytest.raises(ValueError, match="physical frame day"):
        fresh._load_days()


def test_a_new_epoch_can_reuse_a_dropped_day_without_resurrecting_old_rows(log2, retained):  # noqa: F811
    retention, token = retained
    log2.append(build(log2)[:1], partition_day="20261104")
    log2.begin_epoch(2, 200, "skip", "epoch_2")
    retention.register_generation(101, "reader", 2, 200)
    # Capture a pre-drop worker cache: compaction may finish before its first append.
    log2.recovery()
    assert retention.drop_partition("20261104", token)
    assert log2.append(emit(log2, 200, 200), partition_day="20261104") == 201
    assert log2.execute("SELECT DISTINCT source_epoch FROM {s}frames_v2_20261104").fetchall() == [
        (2,)
    ]
    assert retention.prune(token) == (1, 1)  # only old dropped epoch, not new day's data
    log2.append(emit(log2, 201, 201), partition_day="20261105")
    retention.advance(generation_owner("reader"), "reader", 2, 202)
    assert retention.drop_partition("20261104", token)
    assert retention.prune(token) == (1, 1)


@pytest.mark.parametrize("reference", ["checked", "pending", "history"])
def test_prune_keeps_referenced_complete_endpoints_without_readable_history(
    log2,  # noqa: F811
    retained,
    reference,
):
    import json

    from cdc_flight.ingest_log import SourceMismatch

    retention, token = retained
    three_days(log2)
    endpoint = log2.execute("SELECT * FROM {s}units_v2 WHERE unit_start_lsn=90").fetchone()
    if reference == "checked":
        log2.execute("UPDATE {s}tables SET key_checked_lsn=102,key_epoch=1")
    elif reference == "pending":
        pending = [dict(epoch=1, keys=["v"], cut_lsn=102, observed_lsn=105)]
        log2.execute("UPDATE {s}tables SET pending_keys_v2=?", [json.dumps(pending)])
    else:
        log2.execute(
            "INSERT INTO {s}key_changes (relation_id,version,key_columns,cut_lsn,new_from_lsn,epoch) "
            "VALUES (101,1,?,102,105,1)",
            ['["v"]'],
        )
    retention.register_generation(101, "keeper", 1, 106)
    assert retention.drop_partition("20261104", token)
    assert retention.drop_partition("20261105", token)
    before_units = log2.execute("SELECT count(*) FROM {s}units_v2").fetchone()[0]
    chunks, units = retention.prune(token)
    after_units = log2.execute("SELECT count(*) FROM {s}units_v2").fetchone()[0]
    assert units == before_units - after_units
    assert chunks > 0 and units == 1
    assert log2.execute("SELECT * FROM {s}units_v2 WHERE unit_start_lsn=90").fetchone() == endpoint
    assert not log2.execute("SELECT count(*) FROM {s}chunks_v2 WHERE unit_start_lsn=90").fetchone()[
        0
    ]
    log2._key_cut(1, 102)  # A historical endpoint is still independently provable.
    with pytest.raises(HistoryDropped):
        log2.reader().units(1, 102, 106)
    with pytest.raises(HistoryDropped):
        retention.register_generation(101, "not-readable", 1, 102)
    if reference == "pending":
        log2.record_keys({101: ["v"]}, 106, 110)
        assert log2.key_columns()[101] == ("v",)
        assert log2.execute("SELECT cut_lsn FROM {s}key_changes").fetchall() == [(102,)]
    else:
        with pytest.raises(SourceMismatch, match="complete unit end"):
            log2._key_cut(1, 101)


def test_a_malformed_pending_queue_cannot_authorize_endpoint_pruning(log2, retained):  # noqa: F811
    retention, token = retained
    three_days(log2)
    retention.register_generation(101, "keeper", 1, 106)
    assert retention.drop_partition("20261104", token)
    log2.execute("UPDATE {s}tables SET pending_keys_v2='{}'")
    from cdc_flight.ingest_log import SourceMismatch

    with pytest.raises(SourceMismatch, match="malformed pending"):
        retention.prune(token)
    assert (
        log2.execute("SELECT count(*) FROM {s}units_v2 WHERE unit_start_lsn=90").fetchone()[0] == 1
    )


def grouped_retention_round_trip(store, lsn):
    """Same owned DB, fresh schema and connection: atomic DDL plus post-drop ingest.

    The allowlisted unsigned-metadata node and its local twin call this helper.
    No database is created/dropped here; the fixture owns its journal and cleanup.
    """
    from test_format2_protocol import binding, config

    from cdc_flight.format2 import UnitConfig
    from cdc_flight.ingest_log2 import IngestConfig2

    base = replace(config(), source_id="f2_retention", database=store.database.strip('"'))
    cfg = IngestConfig2(
        **base.__dict__,
        text_binding=replace(binding(), source_id=base.source_id),
        unit_config=UnitConfig(max_commits=2, chunk_frames=3),
    )
    log = IngestLog2(store.con, cfg)
    log.initialize()
    log.register(
        ("retention-proof", 1),
        lsn - 10,
        [dict(oid=101, schema="public", name="items", keys=["id"])],
    )
    retention = Retention2(store.con, cfg.database, cfg.source_id)
    retention.initialize()
    token = None
    fresh = None
    log.acquire("lifecycle-proof")
    try:
        first = emit(log, lsn - 10, lsn, lsn + 1)
        second = emit(log, lsn + 2, lsn + 2, lsn + 3)
        assert log.append(first, partition_day="20260101") == lsn + 2
        assert log.append(second, partition_day="20260102") == lsn + 4
        retention.register_generation(101, "proof-reader", 1, lsn + 4)
        token = retention.acquire_compaction("atomic-ddl-proof")

        def snapshot(con):
            prefix = log.prefix
            return (
                con.execute(f"SELECT count(*) FROM {prefix}frames_v2_20260101").fetchone(),
                con.execute(
                    f"SELECT status FROM {prefix}frame_partitions_v2 WHERE partition_day='20260101'"
                ).fetchone(),
                con.execute(
                    f"SELECT floor_epoch,floor_lsn FROM {prefix}retention WHERE id=1"
                ).fetchone(),
                con.execute(f"SELECT guard FROM {prefix}source WHERE id=1").fetchone(),
                con.execute(f"SELECT guard FROM {prefix}retention WHERE id=1").fetchone(),
            )

        before = snapshot(log.con)

        def crash(stage):
            if stage == "before_drop_commit":
                raise RuntimeError("rollback guarded DROP")

        with pytest.raises(RuntimeError, match="rollback guarded DROP"):
            retention.drop_partition("20260101", token, fault=crash)
        # Open AFTER the rollback. No reused client catalog may prove atomic DDL.
        fresh = duckdb.connect(store.address)
        fresh.execute("SET TimeZone='UTC'")
        assert snapshot(fresh) == before
        assert retention.drop_partition("20260101", token)
        fresh.close()
        fresh = duckdb.connect(store.address)  # no catalog from before committed DROP
        fresh.execute("SET TimeZone='UTC'")
        prefix = log.prefix
        assert (
            fresh.execute(
                "SELECT count(*) FROM duckdb_tables() WHERE database_name=? AND schema_name=? AND table_name=?",
                [cfg.database, cfg.source_id, "frames_v2_20260101"],
            ).fetchone()[0]
            == 0
        )
        assert fresh.execute(
            f"SELECT status FROM {prefix}frame_partitions_v2 WHERE partition_day='20260101'"
        ).fetchone() == ("dropped",)
        assert fresh.execute(
            f"SELECT floor_epoch,floor_lsn FROM {prefix}retention WHERE id=1"
        ).fetchone() == (1, lsn + 2)
        assert fresh.execute(f"SELECT guard FROM {prefix}source WHERE id=1").fetchone() != before[3]
        assert (
            fresh.execute(f"SELECT guard FROM {prefix}retention WHERE id=1").fetchone() != before[4]
        )
        assert retention.prune(token) == (len([i for i in first if hasattr(i, "table")]), 1)
        log.release()
        recovered = IngestLog2(fresh, cfg)
        try:
            recovered.acquire("fresh-after-drop")
            assert recovered.recovery().confirmed_end == lsn + 4
            third = emit(recovered, lsn + 4, lsn + 4, lsn + 5)
            high = recovered.append(third, partition_day="20260102")
            assert high == lsn + 6
            reader = recovered.reader()
            units = reader.units(1, lsn + 4, high)
            expected = [
                row for item in third if hasattr(item, "table") for row in item.table.to_pylist()
            ]
            assert reader.frames(units, high=high) == expected
        finally:
            recovered.release()
        return dict(rollback=True, drop=True, floor=lsn + 2, fresh_boundary=high)
    finally:
        log.release()
        if token is not None:
            retention.release_compaction(token)
        if fresh is not None:
            fresh.close()


def test_guarded_drop_rolls_back_and_a_fresh_connection_resumes(stores):
    assert grouped_retention_round_trip(stores[0], 2**63 + 123)["rollback"] is True
