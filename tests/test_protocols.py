"""Backend primitives run on both stores. Reference lifecycle scenarios whose
production counterparts run on MotherDuck run locally only (see dev-docs/test-map.md).
"""

import json

import duckdb
import pytest

from cdc_flight.pgoutput import Cell, Event, Relation, event_dict
from cdc_flight.store import LeaseHeld, LostCommitResponse, StaleFence


def contents(s):
    return s.sql("SELECT * FROM {m}mirror ORDER BY key").fetchall()


def test_ingest_atomic_retry_and_unsigned_lsn(stores):
    s, observer = stores
    token = s.acquire("ingest")
    commit = 2**63 + 5
    with pytest.raises(RuntimeError, match="injected"):
        s.append(1, commit, commit + 8, [{"key": "a"}], token, fail=True)
    assert observer.checkpoint("ingest") == 0
    assert observer.sql("SELECT count(*) FROM {i}events").fetchone() == (0,)
    with pytest.raises(LostCommitResponse):
        s.append(1, commit, commit + 8, [{"key": "a"}], token, lose_response=True)
    s.append(1, commit, commit + 8, [{"key": "a"}], token)
    assert observer.checkpoint("ingest") == commit + 8
    assert observer.sql("SELECT count(*) FROM {i}events").fetchone() == (1,)
    assert observer.sql("SELECT n FROM {i}manifests").fetchone() == (1,)
    with pytest.raises(ValueError, match="different contents"):
        s.append(1, commit, commit + 8, [{"key": "bad"}], token)


def test_apply_atomicity(stores):
    s, observer = stores
    token = s.acquire("apply")
    s.merge({"a": "one", "b": "two"}, 10, token)
    with pytest.raises(RuntimeError, match="injected"):
        s.merge({"a": None, "b": "new"}, 20, token, fail=True)
    assert contents(observer) == [("a", "one"), ("b", "two")]
    assert observer.checkpoint("apply") == 10
    with pytest.raises(LostCommitResponse):
        s.merge({"a": None, "b": "new"}, 20, token, lose_response=True)
    s.merge({"a": None, "b": "new"}, 20, token)
    assert contents(observer) == [("b", "new")]
    assert observer.checkpoint("apply") == 20


def test_fence_takeover_rejects_stale_worker(stores):
    old, new = stores
    token = old.acquire("ingest")
    # Hold an old MVCC snapshot across takeover; a SELECT-only guard would pass.
    old.con.execute("BEGIN")
    assert old.sql("SELECT token FROM {i}fence WHERE scope='ingest'").fetchone()[0] == token
    replacement = new.acquire("ingest", test_clock_offset_seconds=3600)
    try:
        with pytest.raises((duckdb.TransactionException, StaleFence)):
            old.guard("ingest", token)
    finally:
        old.con.execute("ROLLBACK")
    with pytest.raises(StaleFence):
        old.append(1, 10, 11, [{"a": 1}], token)
    new.append(1, 10, 11, [{"a": 1}], replacement)
    assert new.checkpoint("ingest") == 11


def test_inflight_guard_conflicts_with_takeover(stores):
    worker, takeover = stores
    token = worker.acquire("apply")
    worker.con.execute("BEGIN")
    worker.guard("apply", token)
    try:
        with pytest.raises(duckdb.TransactionException):
            takeover.acquire("apply", test_clock_offset_seconds=3600)
    finally:
        worker.con.execute("ROLLBACK")
    assert takeover.acquire("apply", test_clock_offset_seconds=3600) == token + 1


def test_generation_publish_is_atomic_and_abandoned_worker_rejected(stores):
    s, observer = stores
    token, ingest = s.acquire("apply"), s.acquire("ingest")
    s.merge({"stale": "deleted while slot lost"}, 5, token)
    s.append(1, 9, 10, [], ingest)
    with pytest.raises(LostCommitResponse):
        s.begin_generation("first", 10, token, lose_response=True)
    s.begin_generation("first", 10, token)
    scan = {json.dumps(("a",)): json.dumps({"id": "a", "value": "scan"}, sort_keys=True)}
    with pytest.raises(RuntimeError):
        s.chunk("first", 0, scan, token, fail=True)
    assert s.sql("SELECT count(*) FROM {m}stage_first").fetchone() == (0,)
    with pytest.raises(LostCommitResponse):
        s.chunk("first", 0, scan, token, lose_response=True)
    s.chunk("first", 0, scan, token)
    del scan
    assert contents(observer) == [("stale", "deleted while slot lost")]
    rel = Relation(1, "public", "items", "f", ((1, "id", 25, -1), (0, "value", 25, -1)))
    marker = Relation(2, "public", "heartbeat", "d", ((1, "id", 25, -1),))
    events = [
        Event(
            "probe",
            1,
            19,
            20,
            0,
            rel,
            "update",
            None,
            (Cell("value", "a"), Cell("value", "caught up")),
        ),
        Event("probe", 1, 19, 20, 1, marker, "insert", None, (Cell("value", "marker"),)),
    ]
    with pytest.raises(ValueError, match="inactive"):
        observer.publish("first", 20, token)
    s.finish_scan("first", "marker", 2, token)
    with pytest.raises(ValueError, match="marker"):
        observer.publish("first", 20, token)
    s.append(1, 19, 20, [event_dict(e) for e in events], ingest)
    with pytest.raises(RuntimeError):
        observer.publish("first", 20, token, fail=True)
    assert contents(s) == [("stale", "deleted while slot lost")]
    assert s.checkpoint("apply") == 5
    with pytest.raises(LostCommitResponse):
        observer.publish("first", 20, token, lose_response=True)
    # Failure between mirror commit and pin advance keeps extra history pinned.
    assert s.checkpoint("apply") == 20
    assert s.sql("SELECT boundary FROM {i}pins WHERE owner='generation:first'").fetchone() == (10,)
    observer.publish("first", 20, token)
    assert s.sql("SELECT boundary FROM {i}pins WHERE owner='table:mirror'").fetchone() == (20,)
    assert s.sql("SELECT count(*) FROM {i}pins WHERE owner='generation:first'").fetchone() == (0,)
    assert contents(s) == [
        (json.dumps(("a",)), json.dumps({"id": "a", "value": "caught up"}, sort_keys=True))
    ]
    s.begin_generation("second", 20, token)
    s.begin_generation("third", 20, token)
    observer.publish("first", 20, token)
    s.begin_generation("first", 10, token)
    assert s.sql("SELECT id FROM {m}current_generation").fetchone() == ("third",)
    assert s.sql("SELECT count(*) FROM {i}pins WHERE owner='generation:first'").fetchone() == (0,)
    with pytest.raises(ValueError, match="inactive"):
        s.chunk("second", 0, {}, token)


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_retention_pin_boundary_sealing_rollback_and_floor(stores):
    s, observer = stores
    token = s.acquire("retention")
    s.sql("CREATE TABLE {i}day_one AS SELECT 1 x")
    s.sql("INSERT INTO {i}partitions(name,high,sealed) VALUES ('day_one',20,false)")
    assert not s.compact("day_one", token)
    s.sql("UPDATE {i}partitions SET sealed=true")
    for owner in ("live", "paused", "errored", "generation", "replay"):
        s.pin(owner, 20)
    assert not s.compact("day_one", token)
    for owner in ("live", "paused", "errored", "generation"):
        s.pin(owner, 21)
    assert not s.compact("day_one", token)
    s.pin("replay", 21)
    with pytest.raises(RuntimeError):
        s.compact("day_one", token, fail=True)
    assert observer.sql("SELECT * FROM {i}day_one").fetchone() == (1,)
    assert s.compact("day_one", token)
    with pytest.raises(ValueError, match="compacted"):
        observer.pin("late", 19)


def test_pin_registration_conflicts_with_compaction(stores):
    s, other = stores
    token = s.acquire("retention")
    s.con.execute("BEGIN")
    s.guard("retention", token)
    try:
        with pytest.raises(duckdb.TransactionException):
            other.pin("racing", 0)
    finally:
        s.con.execute("ROLLBACK")
    other.pin("racing", 0)


def test_two_database_writes_rejected_and_rolled_back(stores):
    s, other = stores
    with pytest.raises(duckdb.TransactionException):
        with s.transaction():
            s.sql("UPDATE {i}checkpoint SET boundary=5")
            s.sql("UPDATE {m}checkpoint SET boundary=5")
    assert other.checkpoint("ingest") == other.checkpoint("apply") == 0


def test_missing_consumer_pin_fails_closed(stores):
    s, _ = stores
    token = s.acquire("retention")
    s.sql("CREATE TABLE {i}day_one AS SELECT 1 x")
    s.sql("INSERT INTO {i}partitions(name,high,sealed) VALUES ('day_one',20,true)")
    assert not s.compact("day_one", token)
    s.pin("a", 21)
    s.sql("INSERT INTO {i}consumers VALUES ('missing')")
    assert not s.compact("day_one", token)


def probe_outcome(result):
    """The JSON line a probe subprocess printed. The probe prints only after its action
    completes, so a nonzero status after that line (a native abort at teardown, -6) is
    not a probe failure. A probe that exits before printing fails the test."""
    lines = result.stdout.strip().splitlines()
    try:
        return json.loads(lines[-1])
    except (IndexError, ValueError):
        pytest.fail(
            f"second client failed before returning a probe result "
            f"(status {result.returncode}): {result.stderr[-500:]}"
        )


@pytest.mark.parametrize(
    "returncode,stdout,expected",
    [
        (0, '{"ok": true}\n', {"ok": True}),
        (-6, '{"error": "TransactionException"}\n', {"error": "TransactionException"}),
        (1, "", None),
        (-6, "", None),
        (0, "", None),
        (-6, '{"error": "Trans', None),
    ],
)
def test_probe_outcome_fails_only_a_probe_that_did_not_print(returncode, stdout, expected):
    import subprocess

    result = subprocess.CompletedProcess([], returncode, stdout, "Traceback ...")
    if expected is None:
        with pytest.raises(pytest.fail.Exception, match="before returning a probe result"):
            probe_outcome(result)
    else:
        assert probe_outcome(result) == expected


@pytest.mark.parametrize("action,scope", [("takeover", "apply"), ("pin", "retention")])
def test_guard_conflicts_across_motherduck_processes(stores, action, scope):
    import subprocess
    import sys
    from pathlib import Path

    s, _ = stores
    if not s.remote:
        pytest.skip(
            "local DuckDB file locking disallows multiple writer processes; test MotherDuck"
        )
    token = s.acquire(scope)
    s.con.execute("BEGIN")
    s.guard(scope, token)
    args = dict(
        address=s.address,
        database=s.database.strip('"'),
        mirror=s.mirror_database.strip('"'),
        action=action,
    )
    try:
        result = subprocess.run(
            [sys.executable, str(Path("scripts/fence_actor.py").resolve())],
            input=json.dumps(args),
            text=True,
            capture_output=True,
            timeout=45,
        )
        outcome = probe_outcome(result)
        assert outcome.get("error") == "TransactionException", outcome
    finally:
        s.con.execute("ROLLBACK")


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_apply_pin_advances_only_after_commit_and_quiet_apply_releases_history(stores):
    s, _ = stores
    token = s.acquire("apply")
    s.merge({"a": "one"}, 10, token)
    with pytest.raises(LostCommitResponse):
        s.merge({"a": "two"}, 20, token, lose_response=True)
    assert s.checkpoint("apply") == 20
    assert s.sql("SELECT boundary FROM {i}pins WHERE owner='table:mirror'").fetchone() == (10,)
    s.merge({"a": "two"}, 20, token)
    s.merge({}, 30, token)
    assert s.sql("SELECT boundary FROM {i}pins WHERE owner='table:mirror'").fetchone() == (30,)
    s.sql("CREATE TABLE {i}day_one AS SELECT 1 x")
    s.sql("INSERT INTO {i}partitions(name,high,sealed) VALUES ('day_one',20,true)")
    assert s.compact("day_one", s.acquire("retention"))


def test_generation_requires_durable_low_and_releases_abandoned_pins(stores):
    s, _ = stores
    apply, ingest = s.acquire("apply"), s.acquire("ingest")
    with pytest.raises(ValueError, match="committed ingest"):
        s.begin_generation("future", 100, apply)
    assert s.sql("SELECT count(*) FROM {i}pins").fetchone() == (0,)
    s.append(1, 9, 10, [], ingest)
    s.begin_generation("old", 10, apply)
    s.begin_generation("new", 10, apply)
    assert s.sql("SELECT owner FROM {i}pins ORDER BY owner").fetchall() == [("generation:new",)]
    assert s.sql("SELECT owner FROM {i}consumers ORDER BY owner").fetchall() == [
        ("generation:new",)
    ]


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_new_epoch_with_lower_lsn_can_resync_and_retire_old_history(stores):
    s, _ = stores
    ingest, apply = s.acquire("ingest"), s.acquire("apply")
    s.append(1, 99, 100, [], ingest)
    s.merge({"obsolete": "row"}, 100, apply)
    with pytest.raises(ValueError, match="active source epoch"):
        s.append(2, 1, 2, [], ingest)
    s.start_epoch(2, ingest)
    with pytest.raises(ValueError, match="regression"):
        s.merge({}, 101, apply, epoch=1)
    s.append(2, 1, 2, [], ingest)
    s.begin_generation("restored", 2, apply, epoch=2)
    s.chunk(
        "restored", 0, {json.dumps(("a",)): json.dumps({"id": "a", "value": "restored"})}, apply
    )
    marker = Relation(2, "public", "heartbeat", "d", ((1, "id", 25, -1),))
    event = Event("probe", 2, 3, 4, 0, marker, "insert", None, (Cell("value", "restored"),))
    s.finish_scan("restored", "restored", 2, apply)
    s.append(2, 3, 4, [event_dict(event)], ingest)
    s.publish("restored", 4, apply)
    assert s.position("apply") == (2, 4)
    assert s.sql("SELECT epoch,boundary FROM {i}pins WHERE owner='table:mirror'").fetchone() == (
        2,
        4,
    )
    s.append(2, 4, 5, [], ingest)
    s.merge({}, 5, apply, epoch=2)
    assert s.position("ingest") == s.position("apply") == (2, 5)
    assert "obsolete" not in dict(contents(s))
    s.sql("CREATE TABLE {i}old_day AS SELECT 1 x")
    s.sql("INSERT INTO {i}partitions VALUES ('old_day',100,true,1)")
    assert s.compact("old_day", s.acquire("retention"))
    assert s.sql("SELECT epoch,floor FROM {i}retention").fetchone() == (1, 100)
    s.pin("new_epoch", 1, epoch=2)
    with pytest.raises(ValueError, match="compacted"):
        s.pin("old_epoch", 1, epoch=1)


def test_guard_renews_lease_using_database_clock(stores):
    s, other = stores
    token = s.acquire("ingest", ttl_seconds=30)
    before = s.sql("SELECT epoch(expires_at) FROM {i}fence WHERE scope='ingest'").fetchone()[0]
    # Simulate time spent working by moving only this fixture's expiry earlier.
    s.sql("UPDATE {i}fence SET expires_at=expires_at-INTERVAL '20 seconds' WHERE scope='ingest'")
    shortened = s.sql("SELECT epoch(expires_at) FROM {i}fence WHERE scope='ingest'").fetchone()[0]
    s.append(1, 1, 2, [], token)
    after = s.sql("SELECT epoch(expires_at) FROM {i}fence WHERE scope='ingest'").fetchone()[0]
    assert after >= before and (after - shortened) >= 20
    with pytest.raises(LeaseHeld):
        other.acquire("ingest", test_clock_offset_seconds=10)


def test_lease_release_allows_next_run_and_stale_release_cannot_unlock(stores):
    s, next_run = stores
    with s.lease("ingest") as first:
        s.append(1, 1, 2, [], first)
        with pytest.raises(LeaseHeld):
            next_run.acquire("ingest")
    with next_run.lease("ingest") as second:
        assert second == first + 1
        assert not s.release("ingest", first)
        with pytest.raises(LeaseHeld):
            s.acquire("ingest")
    with pytest.raises(RuntimeError, match="run failed"):
        with s.lease("ingest"):
            raise RuntimeError("run failed")
    with next_run.lease("ingest") as third:
        next_run.append(1, 2, 3, [], third)
    assert s.checkpoint("ingest") == 3
    assert s.sql("SELECT count(*) FROM {i}events").fetchone() == (0,)
