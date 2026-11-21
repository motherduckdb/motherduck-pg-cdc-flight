"""Format 2: the table-action core on an Apply2 mirror. Quarantine skips
one WHOLE unit for one table and generation; every refusal happens before the lease
and any write, the spend included; retry names the blocked unit.

Every test runs on both apply folds (the `fold` fixture): the SQL fold, where the poison
is a data error from the server-side MERGE and quarantine falls back to the Python fold
for the page holding the skipped unit, and the Python fold."""

import functools
import sys

import pgstream
import pytest
from test_apply2 import OID, TABLE, ins
from test_apply2 import Env as _Env

from cdc_flight import table_actions as ta

INT_ITEMS = ((1, "id", 25, -1), (0, "v", 23, -1))  # v: integer (type OID 23)


@pytest.fixture(autouse=True, params=["sql", "python"])
def fold(request, monkeypatch):
    """Runs the module on both folds: `Env(...)` below builds ApplyRun(fold=...)."""
    monkeypatch.setattr(sys.modules[__name__], "Env", functools.partial(_Env, fold=request.param))
    return request.param


Env = functools.partial(_Env, fold="sql")


def poisoned(env, monkeypatch=None):
    """Units (1, 2) and (3, 4); commit 3's integer cell is not a number, so the mirror's
    MERGE cast raises ConversionException (a real poison, the same on either fold, with
    no write-path patch): held on unit 2. `monkeypatch` is unused (callers' signature)."""
    env.feed([pgstream.relation(OID, "public", "items", "f", INT_ITEMS)])
    env.apply.start_from_log(TABLE)
    env.commit(ins("1", "1"))
    env.commit(ins("2", "2"))
    c3 = env.commit(ins("3", "not-a-number"))
    env.commit(ins("4", "4"))
    assert env.apply.apply_table(TABLE, "run").status == "needs_action"
    assert env.state()["detail"].startswith("ConversionException applying transaction")
    first, second = env.units()
    return c3, second


def quarantines(env):
    return env.store.con.execute(
        f"SELECT commit_lsn,unit_start_lsn,unit_end_lsn,unit_digest "
        f"FROM {env.apply.mirror}._cdc.quarantine"
    ).fetchall()


class Spend:
    def __init__(self):
        self.calls = 0

    def __call__(self, observed):
        self.calls += 1


def act(env, unit=None, **kwargs):
    args = dict(run_id="q", actor="t", expected_epoch=1, confirm=None)
    if unit is not None:
        args.update(unit_start_lsn=unit[0], unit_end_lsn=unit[1])
    args.update(kwargs)
    return ta.quarantine(env.apply, TABLE, **args)


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_observe_names_the_whole_blocked_unit(stores, monkeypatch):
    env = Env(stores)
    c3, (start, end, digest) = poisoned(env, monkeypatch)
    _, observed = ta.observe(env.apply, TABLE)
    assert observed["format_version"] == 2 and observed["consistent"]
    assert (observed["unit_start_lsn"], observed["unit_end_lsn"]) == (start, end)
    assert (observed["unit_digest"], observed["unit_commit_count"]) == (digest, 2)
    assert observed["blocked_commit_lsn"] == observed["blocked"] == c3
    assert observed["first_commit_lsn"] == c3 and observed["last_commit_lsn"] == c3 + 10
    assert observed["applied"] == start and observed["generation_id"] == observed["generation"]


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_every_refusal_writes_nothing_and_spends_nothing(stores, monkeypatch):
    env = Env(stores)
    c3, (start, end, _) = poisoned(env, monkeypatch)
    literal = ta.unit_confirmation(TABLE, 1, start, end)
    spend = Spend()
    cases = [
        # A legacy single-commit request is refused, naming the unit: never widened.
        (dict(commit_lsn=c3, confirm=ta.confirmation(TABLE, c3)), literal),
        (dict(confirm=literal), "name the blocked unit"),  # no unit arguments
        (dict(unit=(start, end), confirm=ta.confirmation(TABLE, c3)), "exactly"),
        (dict(unit=(start, end), confirm=literal + " "), "exactly"),
        (dict(unit=(start, end + 1), confirm=literal), "state changed"),
        (dict(unit=(start, end), confirm=literal, expected_epoch=2), "state changed"),
    ]
    for kwargs, message in cases:
        result = act(env, consume=spend, **kwargs)
        assert result.status == "refused" and message in result.detail, (kwargs, result)
    assert spend.calls == 0 and quarantines(env) == []
    assert env.state()["state"] == "needs_action" and env.state()["unit"] == start


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_quarantine_skips_the_whole_unit_then_apply_advances(stores, monkeypatch, fold):
    env = Env(stores)
    c3, (start, end, digest) = poisoned(env, monkeypatch)
    if fold == "sql":  # the poisoned page ran on the SQL fold, then bisected on Python
        assert env.apply.fold_stats == {"sql": 1}
    literal = ta.unit_confirmation(TABLE, 1, start, end)
    preview = act(env, unit=(start, end), confirm=literal, dry_run=True)
    assert preview.status == "would" and "whole unit" in preview.detail
    assert "2 commits" in preview.detail and quarantines(env) == []
    spend = Spend()
    done = act(env, unit=(start, end), confirm=literal, consume=spend)
    assert done.status == "done" and spend.calls == 1, done
    assert quarantines(env) == [(c3, start, end, digest)]
    state = env.state()
    assert (state["state"], state["blocked"], state["unit"]) == ("live", None, None)
    assert act(env, unit=(start, end), confirm=literal).status == "already"
    env.apply.fold_stats.clear()
    result = env.apply.apply_table(TABLE, "run2")
    assert result.status == "caught_up" and result.boundary == end
    assert env.mirror() == {"1": 1, "2": 2}  # commit 4 is skipped with its unit
    if fold == "sql":  # the page with the skipped unit falls back to the Python fold
        assert env.apply.fold_stats == {"fallback:quarantine": 1}
        env.commit(ins("5", "5"))
        env.commit(ins("6", "6"))  # closes a unit after the skipped one
        env.apply.fold_stats.clear()
        assert env.apply.apply_table(TABLE, "run3").status == "caught_up"
        assert env.apply.fold_stats == {"sql": 1}  # the next page is back on SQL
        assert env.mirror() == {"1": 1, "2": 2, "5": 5, "6": 6}


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_a_changed_unit_digest_or_member_count_refuses_inside_the_transaction(stores, monkeypatch):
    env = Env(stores)
    _, (start, end, _) = poisoned(env, monkeypatch)
    literal = ta.unit_confirmation(TABLE, 1, start, end)
    real = ta.observe

    def stale(apply, table):
        relation_id, observed = real(apply, table)
        return relation_id, dict(observed, unit_digest="0" * 64)

    monkeypatch.setattr(ta, "observe", stale)
    result = act(env, unit=(start, end), confirm=literal)
    assert result.status == "refused" and "changed" in result.detail
    assert quarantines(env) == [] and env.state()["state"] == "needs_action"


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_an_inconsistent_held_row_is_refused(stores, monkeypatch):
    env = Env(stores)
    _, (start, end, _) = poisoned(env, monkeypatch)
    env.store.con.execute(
        f"UPDATE {env.apply.mirror}._cdc.apply_state SET blocked_unit_start_lsn=? "
        "WHERE relation_id=?",
        [end, OID],
    )
    literal = ta.unit_confirmation(TABLE, 1, start, end)
    result = act(env, unit=(start, end), confirm=literal)
    assert result.status == "refused"
    retried = ta.retry(
        env.apply, TABLE, run_id="r", actor="t", expected_blocked_lsn=None, expected_unit=None
    )
    assert retried.status == "refused" and "inconsistent" in retried.detail


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_retry_names_the_unit_and_apply_replays_it(stores, monkeypatch, fold):
    env = Env(stores)
    c3, (start, end, _) = poisoned(env, monkeypatch)

    def retry(**kwargs):
        return ta.retry(env.apply, TABLE, run_id="r", actor="t", expected_blocked_lsn=c3, **kwargs)

    assert "name the blocked unit" in retry().detail  # expected_unit is required
    assert retry(expected_unit=(start, end + 1)).status == "refused"
    assert retry(expected_unit=None).status == "refused"
    assert "replaying the whole unit" in retry(expected_unit=(start, end), dry_run=True).detail
    assert retry(expected_unit=(start, end)).status == "done"
    state = env.state()
    assert (state["state"], state["blocked"], state["unit"]) == ("live", None, None)
    # The data is still poisoned: apply replays the whole unit and holds again.
    env.apply.fold_stats.clear()
    assert env.apply.apply_table(TABLE, "run2").status == "needs_action"
    if fold == "sql":  # the replay ran on the SQL fold (and bisected on Python)
        assert env.apply.fold_stats == {"sql": 1}
    assert env.state()["unit"] == start and env.mirror() == {"1": 1, "2": 2}


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_a_fault_before_commit_rolls_back_everything_and_the_preview_survives(stores, monkeypatch):
    """The no-op-by-default seam the remote quarantine node uses: a failure after every
    write and before COMMIT leaves no quarantine row, no state change and no spend."""
    env = Env(stores)
    c3, (start, end, _) = poisoned(env, monkeypatch)
    literal = ta.unit_confirmation(TABLE, 1, start, end)
    spent = []

    def consume(observed):
        spent.append(1)
        env.store.con.execute(
            f"INSERT INTO {env.apply.mirror}._cdc.runs (run_id,relation_id,status,updated_at) "
            "VALUES ('spend',?,'spent',current_timestamp)",
            [OID],
        )

    def fault(stage):
        if stage == "before_action_commit":
            raise RuntimeError("injected before commit")

    with pytest.raises(RuntimeError, match="injected before commit"):
        act(env, unit=(start, end), confirm=literal, consume=consume, fault=fault)
    assert spent == [1] and quarantines(env) == []
    state = env.state()
    assert (state["state"], state["blocked"], state["unit"]) == ("needs_action", c3, start)
    spends = env.store.con.execute(
        f"SELECT count(*) FROM {env.apply.mirror}._cdc.runs WHERE run_id='spend'"
    ).fetchone()[0]
    assert spends == 0  # the spend rolled back with the rest
    # The lease was released: the same action now succeeds.
    assert act(env, unit=(start, end), confirm=literal, consume=consume).status == "done"


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_a_lost_response_after_commit_leaves_the_action_done_and_a_rerun_already(
    stores, monkeypatch
):
    env = Env(stores)
    c3, (start, end, digest) = poisoned(env, monkeypatch)
    literal = ta.unit_confirmation(TABLE, 1, start, end)
    spend = Spend()

    def fault(stage):
        if stage == "after_action_commit":
            raise ConnectionError("response lost after COMMIT")

    with pytest.raises(ConnectionError):
        act(env, unit=(start, end), confirm=literal, consume=spend, fault=fault)
    assert spend.calls == 1 and quarantines(env) == [(c3, start, end, digest)]
    assert env.state()["state"] == "live"
    rerun = act(env, unit=(start, end), confirm=literal, consume=spend)
    assert rerun.status == "already" and spend.calls == 1  # never spent twice
