"""Only rolled-back v2 guard conflicts become retryable, after loop cleanup."""

import duckdb
import pytest
from test_ingest_log2 import log2  # noqa: F401 (fixture)

from cdc_flight import ingest
from cdc_flight.chain import ApplyChain, after_ingest
from cdc_flight.ingest_log import FenceLost, GuardedIngestConflict, SourceMismatch


class Log:
    def __init__(self, fresh=102, v2=True):
        self.fresh, self.v2, self.token = fresh, v2, None
        self.format_version = 2 if v2 else 1
        self.calls = []

    def ingest_backend(self):
        return object() if self.v2 else None

    def forget(self):
        self.calls.append("forget")

    def checkpoint(self):
        self.calls.append("checkpoint")
        return self.fresh

    def run_status(self, run_id, status, detail, *, boundary):
        self.calls.append((run_id, status, detail, boundary))


def conflict_loop(monkeypatch, own=102, initial=90, error=None, released=True):
    error = error or GuardedIngestConflict("format-2 guard conflict")

    def loop(config, log, *, info, fault, observe):
        observe.update(
            run_id="run",
            checkpoint=own,
            initial_checkpoint=initial,
            collected_chunks=3,
            collected_transactions=2,
        )
        try:
            raise error
        finally:
            log.calls.append("settled")
            log.release_attempts = 2
            log.token = None if released else 123

    monkeypatch.setattr(ingest, "_run_once", loop)
    return error


@pytest.mark.parametrize("own,status", [(90, "busy"), (102, "conflict")])
def test_retryable_outcome_keeps_own_progress_after_cleanup(monkeypatch, own, status):
    conflict_loop(monkeypatch, own=own)
    log, observe = Log(fresh=999), {}
    result = ingest.run_once(None, log, observe=observe)
    assert (result.status, result.boundary, result.chunks, result.transactions) == (
        status,
        own,
        3,
        2,
    )
    assert observe["checkpoint"] == own
    assert observe["cleanup_release_attempts"] == 2
    assert log.calls == [
        "settled",
        "forget",
        "checkpoint",
        ("run", status, "compaction_conflict", own),
    ]
    assert log.token is None


@pytest.mark.parametrize(
    "error,v2",
    [
        (duckdb.TransactionException("Conflict on update"), True),
        (duckdb.TransactionException("a different transaction failure"), True),
        (FenceLost("lease lost"), True),
        (SourceMismatch("source changed"), True),
        (TimeoutError("deadline"), True),
    ],
)
def test_untagged_errors_still_raise(monkeypatch, error, v2):
    conflict_loop(monkeypatch, error=error)
    log = Log(v2=v2)
    with pytest.raises(type(error)) as caught:
        ingest.run_once(None, log)
    assert caught.value is error
    assert log.calls == ["settled"]


@pytest.mark.parametrize("reason", ["release", "checkpoint", "backward"])
def test_failed_cleanup_or_consistency_read_preserves_original_conflict(monkeypatch, reason):
    error = conflict_loop(monkeypatch, released=reason != "release")
    log = Log(fresh=101 if reason == "backward" else 102)
    if reason == "checkpoint":

        def fail():
            raise RuntimeError("unavailable checkpoint")

        log.checkpoint = fail
    with pytest.raises(GuardedIngestConflict) as caught:
        ingest.run_once(None, log)
    assert caught.value is error
    assert not any(isinstance(c, tuple) for c in log.calls)
    if reason == "release":
        assert log.calls == ["settled"]


@pytest.mark.parametrize(
    "status,boundary,expected", [("conflict", 102, "triggered"), ("busy", 90, "skipped")]
)
def test_only_conflict_with_durable_progress_chains_apply(status, boundary, expected):
    calls = []
    result = ingest.RunResult("run", status, boundary, 3, 2, False)
    assert (
        after_ingest(result, 90, ApplyChain("apply"), lambda *args: calls.append(args)) == expected
    )
    assert len(calls) == int(expected == "triggered")


@pytest.mark.parametrize("failures", [1, 3, 4])
def test_v2_release_retries_only_the_token_matched_autocommit(log2, monkeypatch, caplog, failures):  # noqa: F811
    from cdc_flight.ingest_log import RELEASE

    execute, token = log2.execute, log2.token
    attempts, applied, sleeps = [], [], []

    def contested(statement, parameters=None):
        if statement == RELEASE:
            attempts.append(tuple(parameters))
            if len(attempts) <= failures:
                raise duckdb.TransactionException("Conflict on update")
            applied.append(True)
        return execute(statement, parameters)

    monkeypatch.setattr(log2, "execute", contested)
    if failures == 4:
        with pytest.raises(GuardedIngestConflict, match="after 4 attempts"):
            log2.release(sleep=sleeps.append)
        assert log2.token == token and applied == []
    else:
        log2.release(sleep=sleeps.append)
        assert log2.token is None and applied == [True]
        assert execute(
            "SELECT holder IS NULL AND expires_at IS NULL FROM {s}source"
        ).fetchone() == (True,)
    assert attempts == [(token,)] * min(failures + 1, 4)
    assert log2.release_attempts == len(attempts)
    assert sleeps == [0.1, 0.3, 0.9][: len(attempts) - 1]
    assert f"attempts={len(attempts)}" in caplog.text
    monkeypatch.setattr(log2, "execute", execute)


def test_v2_release_never_retries_an_unrelated_failure(log2, monkeypatch):  # noqa: F811
    from cdc_flight.ingest_log import RELEASE

    execute, token = log2.execute, log2.token
    error, sleeps = duckdb.TransactionException("unrelated transaction failure"), []

    def failed(statement, parameters=None):
        if statement == RELEASE:
            raise error
        return execute(statement, parameters)

    monkeypatch.setattr(log2, "execute", failed)
    with pytest.raises(duckdb.TransactionException) as caught:
        log2.release(sleep=sleeps.append)
    assert caught.value is error
    assert log2.token == token and log2.release_attempts == 1 and sleeps == []
    monkeypatch.setattr(log2, "execute", execute)


def test_legacy_log_is_refused_before_the_loop(monkeypatch):
    conflict_loop(monkeypatch)
    log = Log(v2=False)
    with pytest.raises(SourceMismatch, match="unsupported source protocol"):
        ingest.run_once(None, log)
    assert log.calls == []


@pytest.mark.parametrize(
    "failures,unrelated", [(0, False), (1, False), (3, False), (4, False), (0, True)]
)
def test_run_observes_cleanup_release_even_when_it_raises(log2, monkeypatch, failures, unrelated):  # noqa: F811
    from cdc_flight.ingest_log import RELEASE

    execute, attempts = log2.execute, []
    nonconflict = duckdb.TransactionException("unrelated transaction failure")
    result = object()

    def contested(statement, parameters=None):
        if statement == RELEASE:
            attempts.append(tuple(parameters))
            if unrelated:
                raise nonconflict
            if len(attempts) <= failures:
                raise duckdb.TransactionException("Conflict on update")
        return execute(statement, parameters)

    def loop(config, log, **kwargs):
        log.release(sleep=lambda _: None)
        return result

    try:
        monkeypatch.setattr(log2, "execute", contested)
        monkeypatch.setattr(ingest, "_run_once", loop)
        observed = {}
        if unrelated:
            with pytest.raises(duckdb.TransactionException) as caught:
                ingest.run_once(None, log2, observe=observed)
            assert caught.value is nonconflict
        elif failures == 4:
            with pytest.raises(GuardedIngestConflict, match="after 4 attempts"):
                ingest.run_once(None, log2, observe=observed)
        else:
            assert ingest.run_once(None, log2, observe=observed) is result
        assert (
            observed["cleanup_release_attempts"]
            == len(attempts)
            == (1 if unrelated else min(failures + 1, 4))
        )
    finally:
        monkeypatch.setattr(log2, "execute", execute)


def test_cleanup_observation_does_not_reuse_a_previous_runs_count(log2, monkeypatch):  # noqa: F811
    from cdc_flight.ingest_log import RELEASE

    execute, attempts = log2.execute, []
    result = object()

    def contested(statement, parameters=None):
        if statement == RELEASE:
            attempts.append(tuple(parameters))
            if len(attempts) == 1:
                raise duckdb.TransactionException("Conflict on update")
        return execute(statement, parameters)

    def released(config, log, **kwargs):
        log.release(sleep=lambda _: None)
        return result

    try:
        monkeypatch.setattr(log2, "execute", contested)
        monkeypatch.setattr(ingest, "_run_once", released)
        observed = {}
        assert ingest.run_once(None, log2, observe=observed) is result
        assert observed["cleanup_release_attempts"] == len(attempts) == 2
    finally:
        monkeypatch.setattr(log2, "execute", execute)

    # Reuse both objects on a subsequent acquisition-busy path that never cleans up.
    busy = ingest.RunResult("busy", "busy", 900, 0, 0, False)
    monkeypatch.setattr(ingest, "_run_once", lambda *args, **kwargs: busy)
    assert ingest.run_once(None, log2, observe=observed) is busy
    assert observed["cleanup_release_attempts"] is None

    # A pre-acquisition refusal or BaseException before release also overwrites stale data.
    for error in (RuntimeError("preflight refused"), KeyboardInterrupt()):
        log2.release_attempts = observed["cleanup_release_attempts"] = 3

        def refused(*args, **kwargs):
            raise error

        monkeypatch.setattr(ingest, "_run_once", refused)
        with pytest.raises(type(error)) as caught:
            ingest.run_once(None, log2, observe=observed)
        assert caught.value is error
        assert observed["cleanup_release_attempts"] is None
