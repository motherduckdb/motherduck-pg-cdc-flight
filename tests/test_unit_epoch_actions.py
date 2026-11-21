"""V2 epoch requests retain PG's actual anchor and retry the original request."""

from dataclasses import replace
from types import SimpleNamespace

import pytest
from test_ingest_log2 import build, log2  # noqa: F401 (fixture)
from test_phase1 import pg_phase1  # noqa: F401 (fixture)

from cdc_flight import epoch
from cdc_flight.ingest_log import SourceMismatch


def fake_pg(monkeypatch, actual):
    calls = []

    def identify(config):
        calls.append("identify")
        return "system", 1

    def connect(config):
        calls.append("connect")
        return SimpleNamespace(close=lambda: calls.append("close"))

    def observe(con, config, slot, checkpoint, new_epoch, reason, to_lsn, database, **kw):
        calls.append("observe")
        assert kw["unit_position"]["confirmed_end"] == checkpoint
        return dict(checkpoint=checkpoint)

    def advance(con, slot, checkpoint, to_lsn, database, fault, deadline, **kw):
        calls.append("advance")
        assert kw["actual_start"] is True
        return (
            slot,
            actual,
            dict(abandoned_from=checkpoint, abandoned_to=actual, requested_to_lsn=to_lsn),
        )

    monkeypatch.setattr(epoch, "identify", identify)
    monkeypatch.setattr(epoch, "connect", connect)
    monkeypatch.setattr(epoch, "query", lambda *a: [("database", 1)])
    monkeypatch.setattr(epoch, "observe", observe)
    monkeypatch.setattr(epoch, "_advance", advance)
    return calls


def prepare(log):
    log.release()
    log.config = replace(log.config, epoch=None)


def test_epoch_retry_matches_request_not_actual_and_performs_no_more_pg(log2, monkeypatch):  # noqa: F811
    calls = fake_pg(monkeypatch, 198)
    prepare(log2)
    result = epoch.begin_epoch(log2.config, log2, 2, "skip", to_lsn=199)
    assert (result.start_lsn, result.requested_to_lsn, result.abandoned) == (198, 199, (90, 198))
    assert result.clamped is True
    assert log2.epoch_record(2)["requested_to"] == 199
    assert log2.epoch_record(2)["clamped"] is True
    before = list(calls)
    repeated = epoch.begin_epoch(log2.config, log2, 2, "skip", to_lsn=199)
    assert repeated.status == "already" and repeated.start_lsn == 198
    assert repeated.requested_to_lsn == 199
    assert calls == before  # not even identify/connect, much less another advance
    for target in (198, 200):
        with pytest.raises(epoch.EpochError, match="already began differently"):
            epoch.begin_epoch(log2.config, log2, 2, "skip", to_lsn=target)
        assert calls == before
    log2.execute("UPDATE {s}source_epochs SET requested_to_lsn=NULL WHERE epoch=2")
    with pytest.raises(epoch.EpochError, match="already began differently"):
        epoch.begin_epoch(log2.config, log2, 2, "skip", to_lsn=199)
    assert calls == before


@pytest.mark.parametrize("actual", [89, 100, 101])
def test_an_actual_landing_behind_the_active_member_cannot_install_an_epoch(
    log2,  # noqa: F811
    monkeypatch,
    actual,
):
    log2.append(build(log2)[:1], partition_day="20261106")
    fake_pg(monkeypatch, actual)
    prepare(log2)
    with pytest.raises(SourceMismatch, match="durable member prefix"):
        epoch.begin_epoch(log2.config, log2, 2, "skip", to_lsn=200)
    assert log2.checkpoint() == 90
    assert log2.unit_position()["open_unit_anchor"] == 90
    assert log2.epoch_record(2) is None


@pytest.mark.postgres
@pytest.mark.parametrize("offset", [-1, 1024**4], ids=["interior", "clamp"])
def test_real_pg_advance_records_requested_and_actual_then_retries_without_advancing(
    stores,
    pg_phase1,  # noqa: F811
    monkeypatch,
    offset,
):
    from test_ingest_batching import small_transactions
    from test_ingest_pipeline import prepared

    from cdc_flight.preflight import query

    source, config, log, _ = prepared(stores, pg_phase1, monkeypatch, "v2")
    small_transactions(source, 5)
    flushed = int(query(source.con, "SELECT pg_current_wal_flush_lsn()-'0/0'::pg_lsn")[0][0])
    # Method ruling: interior advances are exact; beyond-flush advances clamp.
    # Both offsets are fixed before the new controls; the original red log stays.
    requested = flushed + offset
    bounded = epoch._bounded
    advances = []

    def recorded(con, seconds, statement, args, message):
        if "pg_replication_slot_advance" in statement:
            advances.append(tuple(args))
        return bounded(con, seconds, statement, args, message)

    monkeypatch.setattr(epoch, "_bounded", recorded)
    result = epoch.begin_epoch(config, log, 2, "skip", to_lsn=requested)
    assert result.status == "begun"
    actual = int(
        query(
            source.con,
            "SELECT confirmed_flush_lsn-'0/0'::pg_lsn FROM pg_replication_slots WHERE slot_name=%s",
            [result.slot],
        )[0][0]
    )
    after = int(query(source.con, "SELECT pg_current_wal_flush_lsn()-'0/0'::pg_lsn")[0][0])
    # Routine xdist workers share WAL: keep the strict stable-flush control in
    # its recorded serial artifact, and make the regression parallel-safe.
    if offset > 0:
        assert flushed <= actual <= after
        assert actual < requested
    else:
        assert actual == requested < flushed <= after
    assert result.clamped == (offset > 0)
    assert log.epoch_record(2)["clamped"] == result.clamped
    assert result.start_lsn == log.checkpoint() == actual
    assert result.requested_to_lsn == requested
    assert log.epoch_record(2)["to"] == actual
    print(
        "epoch advance control:",
        dict(
            offset=offset,
            flushed_before=flushed,
            flushed_after=after,
            requested=requested,
            actual=actual,
            clamped=result.clamped,
            native_calls=len(advances),
        ),
    )
    count = len(advances)
    assert count > 0
    repeated = epoch.begin_epoch(config, log, 2, "skip", to_lsn=requested)
    assert repeated.status == "already" and repeated.start_lsn == actual
    assert len(advances) == count
    if offset > 0:
        from cdc_flight.ingest import run_once

        small_transactions(source, 2, start=100)
        captured = run_once(config, log)
        assert captured.status == "drained" and actual < captured.boundary < requested
        ((oid,),) = query(source.con, "SELECT %s::regclass::oid", [source.schema + ".items"])
        reader = log.reader()
        units = reader.units(2, actual, captured.boundary)
        events = reader.events(units, int(oid), high=captured.boundary)
        assert [int(event["new"]["values"][0]) for event in events] == [100, 101]


@pytest.mark.parametrize("flushed", [150, 200, 250])
def test_v2_skip_preview_labels_current_and_upper_bound_bytes(log2, monkeypatch, flushed):  # noqa: F811
    monkeypatch.setattr(
        epoch,
        "slot_info",
        lambda *a: dict(
            plugin="pgoutput", database="database", active=False, confirmed=90, lost=False
        ),
    )
    monkeypatch.setattr(epoch, "query", lambda *a, **kw: [(flushed,)])
    observed = epoch.observe(
        None,
        log2.config,
        log2.slot_name,
        90,
        2,
        "skip",
        200,
        "database",
        unit_position=log2.unit_position(),
    )
    assert observed["abandoned_bytes_upper_bound"] == observed["abandoned_bytes"] == 110
    assert observed["abandoned_bytes_current"] == min(flushed, 200) - 90
    assert observed.get("clamped", False) is (flushed < 200)
