"""A committed unit can be ahead of PG feedback; restart uses the durable END."""

from dataclasses import replace

import conftest
import pytest
from test_ingest_batching import small_transactions
from test_ingest_pipeline import prepared, stored_ids, stored_once_any
from test_phase1 import pg_phase1  # noqa: F401 (fixture)

from cdc_flight.ingest import run_once
from cdc_flight.preflight import query
from cdc_flight.replication import ReplicationStream


@pytest.mark.postgres
def test_terminal_commit_before_feedback_restarts_at_the_durable_end(
    stores,
    pg_phase1,  # noqa: F811
    monkeypatch,
):
    source, config, log, harness = prepared(stores, pg_phase1, monkeypatch, "v2")
    small_transactions(source, 80)
    initial = log.checkpoint()
    starts = []
    start = ReplicationStream.start

    def recorded_start(stream, slot, lsn, publication):
        starts.append(lsn)
        return start(stream, slot, lsn, publication)

    monkeypatch.setattr(ReplicationStream, "start", recorded_start)

    def crash_after_a_complete_unit(stage):
        # This hook is on the main thread after collect, before the new value is
        # acknowledged. Earlier appends may contain only an open unit's chunks.
        if stage == "before_feedback" and log.checkpoint() > initial:
            raise RuntimeError("terminal committed before feedback")

    observed = {}
    with pytest.raises(RuntimeError, match="terminal committed before feedback"):
        run_once(config, log, fault=crash_after_a_complete_unit, observe=observed)
    harness.assert_invariants()
    conftest.wait_slot_released(source.con, config.slot)
    durable = log.checkpoint()
    confirmed = int(
        query(
            source.con,
            "SELECT confirmed_flush_lsn-'0/0'::pg_lsn FROM pg_replication_slots WHERE slot_name=%s",
            [config.slot],
        )[0][0]
    )
    assert durable > initial and confirmed < durable
    assert observed["checkpoint"] == durable
    assert max(value for value, _, _ in harness.statuses) < durable
    result = run_once(config, log)
    assert result.status == "drained" and not result.partial_transaction
    assert starts == [initial, durable]
    harness.assert_invariants()
    stored_once_any(log)
    actual, expected = stored_ids(log, source)
    assert actual == expected


@pytest.mark.postgres
@pytest.mark.parametrize(
    "setting",
    [
        "TimeZone",
        "DateStyle",
        "IntervalStyle",
        "extra_float_digits",
        "bytea_output",
        "lc_monetary",
        "xmlbinary",
        "xmloption",
        "server_encoding",
        "client_encoding",
    ],
)
def test_changed_replication_setting_records_action_before_start(
    stores,
    pg_phase1,  # noqa: F811
    monkeypatch,
    setting,
):
    from cdc_flight.ingest_log import SourceMismatch

    source, config, log, harness = prepared(stores, pg_phase1, monkeypatch, "v2")
    original = ReplicationStream.show

    def changed_show(stream, name):
        return "different" if name == setting else original(stream, name)

    def forbidden_start(*args):
        pytest.fail("binding mismatch reached START_REPLICATION")

    monkeypatch.setattr(ReplicationStream, "show", changed_show)
    monkeypatch.setattr(ReplicationStream, "start", forbidden_start)
    with pytest.raises(SourceMismatch, match="binding_changed"):
        run_once(config, log)
    harness.assert_invariants()
    assert (
        log.execute("SELECT state FROM {s}source").fetchone()[0] == "needs_action:binding_changed"
    )
    assert not any(name.startswith("frames_v2_") for name in log.catalog())


@pytest.mark.postgres
@pytest.mark.parametrize("changed", ["epoch", "confirmed_end"])
def test_recovery_mismatch_refuses_before_start_and_releases_the_lease(
    stores,
    pg_phase1,  # noqa: F811
    monkeypatch,
    changed,
):
    from cdc_flight.ingest_log import SourceMismatch

    _, config, log, _ = prepared(stores, pg_phase1, monkeypatch, "v2")
    log.acquire("capture")
    recovery = log.recovery()
    log.release()
    bad = replace(recovery, **{changed: getattr(recovery, changed) + 1})
    monkeypatch.setattr(log, "recovery", lambda: bad)
    starts = []
    monkeypatch.setattr(ReplicationStream, "start", lambda *args: starts.append(args))
    with pytest.raises(SourceMismatch, match="spool anchor differs"):
        run_once(config, log)
    assert starts == []
    assert log.token is None
    assert log.execute(
        "SELECT holder IS NULL AND expires_at IS NULL FROM {s}source"
    ).fetchone() == (True,)
    assert not any(name.startswith("frames_v2_") for name in log.catalog())
