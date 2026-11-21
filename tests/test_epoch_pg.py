"""Source epoch transitions against real PostgreSQL: slot loss, skip, faults."""

import conftest
import psycopg2
import psycopg2.errors
import pytest
from psycopg2.extras import LogicalReplicationConnection
from test_backfill import drive, populated  # noqa: F401 (fixture)
from test_phase2 import assert_mirror_equals_source

from cdc_flight import epoch
from cdc_flight.apply_run import ApplyRun
from cdc_flight.epoch import EpochError, begin_epoch
from cdc_flight.ingest import run_once
from cdc_flight.ingest_log import FenceLost, IngestLog, LeaseBusy, SourceMismatch
from cdc_flight.preflight import PreflightError, query
from cdc_flight.replication import ReplicationError


def crash(at):
    def fault(stage):
        if stage == at:
            raise RuntimeError(at)

    return fault


def wal_lsn(source):
    return int(query(source.con, "SELECT pg_current_wal_flush_lsn()-'0/0'::pg_lsn")[0][0])


def drop_slot(source, name):
    conftest.wait_slot_released(source.con, name)
    query(
        source.con,
        "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name=%s",
        [name],
    )


def confirmed(source, name):
    rows = query(
        source.con,
        "SELECT confirmed_flush_lsn-'0/0'::pg_lsn FROM pg_replication_slots WHERE slot_name=%s",
        [name],
    )
    return int(rows[0][0]) if rows else None


def logged(log, epoch, text):
    """Inspect structured cells in retained frames, without a legacy JSON payload."""
    days = [
        d
        for (d,) in log.execute(
            "SELECT DISTINCT partition_day FROM {s}frame_partitions_v2 WHERE status<>'dropped'"
        ).fetchall()
    ]
    return any(
        text in image["values"]
        for day in days
        for row in log.execute(
            f"SELECT old,new FROM {{s}}frames_v2_{day} WHERE source_epoch=?", [epoch]
        ).fetchall()
        for image in row
        if image is not None
    )


@pytest.mark.postgres
@pytest.mark.md_gate
def test_slot_loss_begins_an_epoch_and_a_resync_rebuilds_the_mirror(stores, populated):  # noqa: F811
    source, _, ingest, log, backfill = populated
    pairs = f"{source.schema}.pairs"
    new_slot = ingest.slot + "_e2"
    try:
        assert run_once(ingest, log).status == "drained"
        assert drive(backfill, pairs, ingest, log).status == "published"
        apply = ApplyRun(stores[0].con, backfill.config)
        source.execute("UPDATE pairs SET v=v+1 WHERE a=0")
        assert run_once(ingest, log).status == "drained"
        assert apply.apply_table(pairs, "run").status == "caught_up"

        # The slot is lost; changes made meanwhile reach no log.
        drop_slot(source, ingest.slot)
        source.execute("DELETE FROM pairs WHERE a=1")
        source.execute("INSERT INTO pairs VALUES (9,'gone',1)")
        with pytest.raises(PreflightError, match="missing"):
            run_once(ingest, log)

        # A failure after this run created the slot and before the metadata commits:
        # the re-read shows nothing committed, so the slot it created is dropped and
        # retains no WAL.
        with pytest.raises(RuntimeError, match="before_metadata"):
            begin_epoch(ingest, log, 2, "slot_lost", fault=crash("before_metadata"))
        assert confirmed(source, new_slot) is None
        assert log.position()[0] == 1
        # A lost response after the metadata commit (ambiguous to the caller): the
        # re-read shows the new epoch names the slot, so it is kept; the retry is a no-op.
        with pytest.raises(RuntimeError, match="after_metadata") as raised:
            begin_epoch(ingest, log, 2, "slot_lost", fault=crash("after_metadata"))
        assert any(new_slot in note for note in getattr(raised.value, "__notes__", []))
        start = confirmed(source, new_slot)
        assert start is not None and log.position() == (2, new_slot, start)
        result = begin_epoch(ingest, log, 2, "slot_lost")
        assert (result.status, result.epoch, result.slot, result.start_lsn) == (
            "already",
            2,
            new_slot,
            start,
        )
        assert log.position() == (2, new_slot, start)

        # Ingest follows the registered slot and epoch.
        source.execute("INSERT INTO pairs VALUES (9,'after',2)")
        assert run_once(ingest, log).status == "drained"
        assert logged(log, 2, "after") and not logged(log, 2, "gone")
        # The mirror of the old history holds; a new-epoch generation rebuilds it.
        assert apply.apply_table(pairs, "run").status == "needs_resync"
        assert drive(backfill, pairs, ingest, log).status == "published"
        relation_id, _ = apply.registered(pairs)
        assert apply.state(relation_id)["epoch"] == 2
        source.execute("UPDATE pairs SET v=0 WHERE a=2")
        assert run_once(ingest, log).status == "drained"
        assert apply.apply_table(pairs, "run").status == "caught_up"
        # The rows deleted while the slot was gone are gone from the mirror too.
        assert_mirror_equals_source(apply, source, pairs)
    finally:
        drop_slot(source, new_slot)


@pytest.mark.postgres
def test_skip_keeps_the_slot_then_slot_loss_names_by_cause(stores, populated):  # noqa: F811
    source, _, ingest, log, _ = populated
    third = ingest.slot + "_e3"
    try:
        assert run_once(ingest, log).status == "drained"
        checkpoint = log.checkpoint()
        # A healthy slot is not lost; a skip needs a target past the checkpoint.
        with pytest.raises(EpochError, match="nothing to recover"):
            begin_epoch(ingest, log, 2, "slot_lost")
        with pytest.raises(EpochError, match="past the durable checkpoint"):
            begin_epoch(ingest, log, 2, "skip", to_lsn=checkpoint)

        source.execute("INSERT INTO pairs VALUES (7,'poison',1)")
        target = wal_lsn(source)
        # Never takes an active slot over: busy, and nothing changes.
        reader = psycopg2.connect(ingest.dsn, connection_factory=LogicalReplicationConnection)
        try:
            reader.cursor().start_replication(
                slot_name=ingest.slot,
                start_lsn=checkpoint,
                options={"proto_version": "1", "publication_names": ingest.publication},
            )
            result = begin_epoch(ingest, log, 2, "skip", to_lsn=target)
            assert result.status == "busy" and log.position()[0] == 1
        finally:
            reader.close()
        conftest.wait_slot_released(source.con, ingest.slot)

        # A crash after the advance: ingest fails closed until the retry.
        with pytest.raises(RuntimeError, match="before_metadata"):
            begin_epoch(ingest, log, 2, "skip", to_lsn=target, fault=crash("before_metadata"))
        assert confirmed(source, ingest.slot) == target
        with pytest.raises(SourceMismatch, match="ahead of the durable checkpoint"):
            run_once(ingest, log)
        # The lease lapses during the PG step and ingest takes it: nothing is written.
        other = IngestLog(stores[1].con, ingest)

        def lapse(stage):
            if stage == "before_metadata":
                log.execute("UPDATE {s}source SET expires_at=current_timestamp-INTERVAL 1 SECOND")
                other.acquire("ingest")

        with pytest.raises(FenceLost):
            begin_epoch(ingest, log, 2, "skip", to_lsn=target, fault=lapse)
        other.release()
        assert log.position()[0] == 1
        # The retry finds the slot at the target.
        result = begin_epoch(ingest, log, 2, "skip", to_lsn=target)
        assert (result.status, result.slot, result.start_lsn, result.abandoned) == (
            "begun",
            ingest.slot,
            target,
            (checkpoint, target),
        )
        record = log.epoch_record(2)
        assert (record["from"], record["to"], record["reason"]) == (checkpoint, target, "skip")

        source.execute("INSERT INTO pairs VALUES (8,'kept',1)")
        assert run_once(ingest, log).status == "drained"
        assert logged(log, 2, "kept") and not logged(log, 2, "poison")

        # Then a slot loss: epoch 3's slot is named by its cause, not epoch 2's.
        drop_slot(source, ingest.slot)
        result = begin_epoch(ingest, log, 3, "slot_lost")
        assert (result.status, result.slot) == ("begun", third)
        assert log.position()[:2] == (3, third)
        source.execute("INSERT INTO pairs VALUES (8,'third',2)")
        assert run_once(ingest, log).status == "drained"
        assert logged(log, 3, "third")
        with pytest.raises(EpochError, match="the next epoch is 4"):
            begin_epoch(ingest, log, 5, "slot_lost")
    finally:
        drop_slot(source, third)


def test_a_reader_attaching_after_the_check_is_busy(monkeypatch):
    """pg_replication_slot_advance raises ObjectInUse when a reader attached after
    slot_info: that is busy, retry, not an error. A post-commit drop that meets one
    reports the slot as a leftover instead of failing."""

    def in_use(con, statement, args=None):
        if "pg_drop_replication_slot" in statement or "pg_replication_slot_advance" in statement:
            raise psycopg2.errors.ObjectInUse()
        if "pg_current_wal_flush_lsn" in statement:
            return [(10_000,)]
        return []

    slots = {
        "s": dict(plugin="pgoutput", database="db", active=False, confirmed=100, lost=True),
        "s_e2": dict(plugin="pgoutput", database="db", active=False, confirmed=200, lost=False),
    }
    monkeypatch.setattr(epoch, "query", in_use)
    monkeypatch.setattr(epoch, "slot_info", lambda con, name: slots.get(name))
    # Recover no longer drops the old slot before the metadata: after the
    # commit the drop is best effort, and a reader there leaves it as a leftover.
    assert epoch._drop_quietly(None, "s", "db") == "s"
    slots["s"] = dict(slots["s"], lost=False)
    with pytest.raises(LeaseBusy):
        epoch._advance(None, "s", 50, 500, "db", None, actual_start=True)


def lapse(log):
    log.execute("UPDATE {s}source SET expires_at=current_timestamp-INTERVAL 1 SECOND")


@pytest.mark.postgres
def test_stale_worker_paused_before_its_stream_never_reaches_the_new_slot(stores, populated):  # noqa: F811
    source, _, ingest, log, _ = populated
    new_slot = ingest.slot + "_e2"
    try:
        assert run_once(ingest, log).status == "drained"
        old = log.checkpoint()
        other = IngestLog(stores[1].con, ingest)
        state = {}

        def pause(stage):
            if stage == "before_stream":
                # While the worker is paused its lease lapses, the slot is lost and an
                # operator begins epoch 2.
                lapse(log)
                drop_slot(source, ingest.slot)
                state["result"] = begin_epoch(ingest, other, 2, "slot_lost")
                state["start"] = confirmed(source, new_slot)
                source.execute("INSERT INTO pairs VALUES (6,'meanwhile',1)")
            if stage == "after_start":
                state["streamed"] = True

        with pytest.raises((ReplicationError, FenceLost)):
            run_once(ingest, log, fault=pause)
        assert state["result"].status == "begun" and "streamed" not in state
        # Nothing acknowledged on the new slot, and epoch 2 captures what followed.
        assert confirmed(source, new_slot) == state["start"]
        assert run_once(ingest, log).status == "drained"
        assert log.position()[:2] == (2, new_slot) and logged(log, 2, "meanwhile")
        assert state["start"] >= old
    finally:
        drop_slot(source, new_slot)


@pytest.mark.postgres
def test_first_status_is_fenced_after_a_lease_takeover(stores, populated):  # noqa: F811
    source, _, ingest, log, _ = populated
    assert run_once(ingest, log).status == "drained"
    position = confirmed(source, ingest.slot)
    other = IngestLog(stores[1].con, ingest)
    seen = []

    def pause(stage):
        seen.append(stage)
        if stage == "before_stream":
            lapse(log)
            other.acquire("other")

    renews = []
    real_renew = log.renew

    def renew():
        renews.append(1)
        return real_renew()

    log.renew = renew
    try:
        with pytest.raises(FenceLost):
            run_once(ingest, log, fault=pause)
    finally:
        other.release()
        log.renew = real_renew
    # The fence ran before the first status left: the run never streamed.
    assert "before_stream" in seen and "after_start" not in seen
    assert confirmed(source, ingest.slot) == position
    # Once: cleanup never re-runs the fence, and it still records the failed run.
    assert renews == [1]
    assert log.execute(
        "SELECT status,detail FROM {s}runs ORDER BY updated_at DESC LIMIT 1"
    ).fetchone() == ("failed", "FenceLost")


@pytest.mark.postgres
def test_streaming_stale_worker_is_fenced_and_the_transition_waits(stores, populated):  # noqa: F811
    source, _, ingest, log, _ = populated
    assert run_once(ingest, log).status == "drained"
    position = confirmed(source, ingest.slot)
    other = IngestLog(stores[1].con, ingest)
    state = {}

    def pause(stage):
        if stage == "after_start":
            # Paused after its first fenced status, with the slot active: a skip
            # can't take it over, but the attempt still replaces the lease.
            lapse(log)
            source.execute("INSERT INTO pairs VALUES (5,'late',1)")
            state["target"] = wal_lsn(source)
            state["busy"] = begin_epoch(ingest, other, 2, "skip", to_lsn=state["target"])

    with pytest.raises(FenceLost):
        run_once(ingest, log, fault=pause)
    assert state["busy"].status == "busy"
    # Its later appends and acknowledgements were fenced: the slot did not move.
    assert confirmed(source, ingest.slot) == position
    assert log.position()[0] == 1
    conftest.wait_slot_released(source.con, ingest.slot)
    result = begin_epoch(ingest, other, 2, "skip", to_lsn=state["target"])
    assert result.status == "begun"
