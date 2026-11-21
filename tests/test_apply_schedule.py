"""The page-major apply schedule (against apply starvation and plan cache misses).

One run steps every table a page at a time, in rounds, grouped by checkpoint: a lagging
group is capped at the next group's checkpoint so the two line up, and from then on they
share each page's plan. Each case runs a SQL-fold side and checks it against a Python-fold
side (test_sql_fold_e2e's Side: real Spool and IngestLog2, local stores).
"""

import types

import pgstream
import pytest
from test_sql_fold_e2e import (
    OID,
    OTHER,
    OTHER_TABLE,
    START,
    TABLE,
    I,
    O,
    Side,
    text_layout,
)

from cdc_flight import apply_run

LAYOUT = text_layout("f")


class Pair:
    """A Python-fold oracle and a SQL-fold side fed the same wire, run step by step."""

    def __init__(self, tmp_path, **apply_options):
        tmp_path.mkdir(parents=True, exist_ok=True)
        self.sides = [
            Side(tmp_path, "o", "python", LAYOUT, max_commits=1, apply_options=apply_options),
            Side(tmp_path, "s", "sql", LAYOUT, max_commits=1, apply_options=apply_options),
        ]
        self.lsn = 100

    def commit(self, *messages, sides=None, lsn=None):
        if lsn is not None:
            self.lsn = lsn
        for side in sides or self.sides:
            side.feed(
                [
                    pgstream.begin(self.lsn, -5 + self.lsn * 1_000_003, 7),
                    *messages,
                    pgstream.commit(self.lsn, self.lsn + 5, self.lsn * 1_000_003 + 7),
                ]
            )
        self.lsn += 10

    def run(self, tables=(TABLE, OTHER_TABLE), run_id="run"):
        out = []
        for side in self.sides:
            side.apply.plan_stats.update(fills=0, hits=0, batched_failed=0)
            side.apply.round_stats.update(
                rounds=0, max_round_seconds=0.0, two_step_rounds=0, max_two_step_round_seconds=0.0
            )
            out.append(
                [
                    (r.table, r.status, r.boundary, r.events, r.units)
                    for r in side.apply.run(tables=tables, run_id=run_id)
                ]
            )
        assert out[0] == out[1]
        return {r[0]: r[1:] for r in out[1]}

    @property
    def sql(self):
        return self.sides[1].apply

    def leases(self):
        return [
            side.con.execute(
                f"SELECT relation_id,holder,expires_at FROM {side.apply.mirror}._cdc.apply_state "
                "WHERE holder IS NOT NULL OR expires_at IS NOT NULL ORDER BY 1"
            ).fetchall()
            for side in self.sides
        ]

    def close(self):
        snaps = [side.snapshot([]) for side in self.sides]
        for side in self.sides:
            side.con.close()
        assert snaps[0] == snaps[1]
        return snaps[1]


@pytest.fixture
def pair(tmp_path):
    made = []

    def make(**options):
        made.append(Pair(tmp_path / f"p{len(made)}", **options))
        return made[-1]

    yield make
    for p in made:
        for side in p.sides:
            try:
                side.con.close()
            except Exception:
                pass
    for path in tmp_path.rglob("cdc_p0_sf_*.duckdb*"):
        path.unlink()


class Clock:
    """apply_run's clock: each table step takes `per_step` seconds."""

    def __init__(self, monkeypatch, per_step):
        self.now = 0.0
        self.per_step = per_step
        monkeypatch.setattr(apply_run, "time", types.SimpleNamespace(monotonic=lambda: self.now))
        step = apply_run.ApplyRun._step

        def timed(apply, *args, **kwargs):
            self.now += self.per_step
            return step(apply, *args, **kwargs)

        monkeypatch.setattr(apply_run.ApplyRun, "_step", timed)


def backlog(p, n):
    """n commits, each one unit touching both tables (2 events: one page per unit at
    max_batch_events=2)."""
    for i in range(n):
        p.commit(I(str(i), "a", "b"), O(str(i), "x"))


def test_no_table_starves_behind_a_backlog(pair, monkeypatch):
    """#6: under a deadline, every table advances in the same run (table-major order let
    the first table use the whole budget)."""
    p = pair(max_batch_events=2, runtime_seconds=25)
    backlog(p, 6)
    Clock(monkeypatch, per_step=5)  # a round of 2 tables: 10 s; the 3rd would end at 30
    results = p.run()
    for table in (TABLE, OTHER_TABLE):
        status, boundary, events, _ = results[table]
        assert status == "capped" and boundary > START and events == 2, results
    # Both tables stepped the same pages: one plan per page, the other table hits it.
    assert p.sql.round_stats["rounds"] == 2
    assert p.sql.plan_stats == {"fills": 2, "hits": 2, "batched_failed": 0}
    assert p.leases() == [[], []]
    p.close()


def test_a_round_that_would_cross_the_deadline_is_not_started(pair, monkeypatch):
    """P-2: the first round always runs; a later one only if the last round's length still
    fits before the deadline. So a run overruns by at most one round."""
    p = pair(max_batch_events=2, runtime_seconds=15)
    backlog(p, 4)
    Clock(monkeypatch, per_step=5)
    results = p.run()
    assert p.sql.round_stats["rounds"] == 1  # 10 s round; 10 + 10 > 15
    assert {r[0] for r in results.values()} == {"capped"}
    p.close()


def test_the_whole_backlog_plans_each_page_once(pair):
    """#7: an uncapped multi-table backlog run plans each page once (P fills, not P x T)."""
    p = pair(max_batch_events=2)
    backlog(p, 5)
    results = p.run()
    assert {r[0] for r in results.values()} == {"caught_up"}
    pages = p.sql.round_stats["rounds"] - 1  # the last round finds both caught up
    assert pages == 5 and p.sql.plan_stats == {"fills": pages, "hits": pages, "batched_failed": 0}
    p.close()


def test_a_lagging_table_lands_on_the_next_checkpoint_then_shares_pages(pair):
    """S2b + P-1: `others` is one page behind `items`. Groups step top-down; others pages
    up to items' new checkpoint (two capped steps), lands on it and merges: from round 2
    on, each page is planned once and the other table hits it."""
    p = pair(max_batch_events=2)
    backlog(p, 1)
    p.run(tables=(TABLE,))  # items one unit ahead
    backlog(p, 3)
    results = p.run()
    assert {r[0] for r in results.values()} == {"caught_up"}
    assert results[TABLE][1] == results[OTHER_TABLE][1]
    # Round 1: items' page, others' two capped pages (3 fills); rounds 2-3 shared.
    assert p.sql.plan_stats == {"fills": 5, "hits": 2, "batched_failed": 0}, p.sql.plan_stats
    p.close()


def test_a_wider_gap_closes_by_a_page_a_round(pair):
    """M-1: with a gap over one page, the lower group gains one page a round on the group
    above (two capped steps to its one) and never passes its checkpoint; here the upper
    group reaches confirmed first, and the lower one finishes alone. Nothing waits."""
    p = pair(max_batch_events=2)
    backlog(p, 3)
    p.run(tables=(TABLE,))  # items 3 units ahead
    backlog(p, 2)
    steps = []
    step = apply_run.ApplyRun._step

    def tracing(apply, t, round_high, cap, fault):
        if apply.fold == "sql":
            steps.append((t.table, t.committed, cap))
        return step(apply, t, round_high, cap, fault)

    with pytest.MonkeyPatch.context() as m:
        m.setattr(apply_run.ApplyRun, "_step", tracing)
        results = p.run()
    assert {r[0] for r in results.values()} == {"caught_up"}
    assert results[TABLE][1] == results[OTHER_TABLE][1]
    # items steps in every round (no freeze); others never passes items' checkpoint.
    items = [c for table, c, _ in steps if table == TABLE]
    others = [(c, cap) for table, c, cap in steps if table == OTHER_TABLE]
    assert items == [125, 135, 145]
    assert all(cap is None or c <= cap for c, cap in others)
    # Others' capped step stops short of its cap (max_batch_events=2 ends the
    # page at one unit), so it takes its second step, still below the cap.
    assert others[:2] == [(90, 135), (105, 135)]
    assert p.sql.round_stats["two_step_rounds"] == 2  # round 3: one step reaches items
    assert p.sql.plan_stats["fills"] == 7  # 2 pages for items + 5 for others
    p.close()


def test_a_commit_mid_round_does_not_split_the_rounds_page(pair, monkeypatch):
    """S2a: the round shares one confirmed position, so an ingest commit between two
    tables' steps doesn't give the second table a different page key."""
    p = pair(max_batch_events=100)
    backlog(p, 2)
    step = apply_run.ApplyRun._step
    fed, late = set(), p.lsn

    def feeding(apply, t, *args, **kwargs):
        out = step(apply, t, *args, **kwargs)
        side = next(s for s in p.sides if s.apply is apply)
        if t.table == TABLE and id(side) not in fed:
            fed.add(id(side))
            # This side only (each side runs in turn), at the same LSN on both, after
            # items' step and before others'.
            p.commit(I("late", "a", "b"), O("late", "x"), sides=[side], lsn=late)
        return out

    monkeypatch.setattr(apply_run.ApplyRun, "_step", feeding)
    p.run()
    stats = p.sql.plan_stats
    # Round 1: items fills, others hits the same page (not a new key from the late commit);
    # round 2: the late commit's page, shared.
    assert stats == {"fills": 2, "hits": 2, "batched_failed": 0}, stats
    p.close()


def test_every_lease_is_released_when_a_table_fails(pair, monkeypatch):
    """N-2: an unexpected error closes every open table (its committed pages kept, its
    lease released) and is raised."""
    p = pair(max_batch_events=2)
    backlog(p, 3)
    write = apply_run.ApplyRun.write
    calls = []

    def failing(apply, relation_id, *args, **kwargs):
        calls.append(relation_id)
        if relation_id == OTHER and calls.count(OTHER) == 2:
            raise RuntimeError("boom")
        return write(apply, relation_id, *args, **kwargs)

    monkeypatch.setattr(apply_run.ApplyRun, "write", failing)
    for side in p.sides:
        calls.clear()
        with pytest.raises(RuntimeError, match="boom"):
            side.apply.run(tables=(TABLE, OTHER_TABLE), run_id="run")
    assert p.leases() == [[], []]
    rows = (
        p.sides[1]
        .con.execute(
            f"SELECT relation_id,status,detail FROM {p.sides[1].apply.mirror}._cdc.runs ORDER BY 1"
        )
        .fetchall()
    )
    assert rows == [(OID, "capped", "run stopped: another table failed")], rows


def test_a_pending_key_cut_is_never_passed_in_a_lockstep_round(pair):
    """A table with a pending replica identity key change pages only up to its
    cut (its own key limit), never to the round's shared high, and holds there; its pages
    up to the cut have their own page key (an extra fill). The other table goes on."""
    p = pair(max_batch_events=2)
    backlog(p, 2)
    for side in p.sides:
        side.catalog_keys[OID] = ["a"]  # ingest records items' key change from here
    backlog(p, 2)
    results = p.run()
    status, boundary, _, _ = results[TABLE]
    assert status == "needs_resync" and boundary < results[OTHER_TABLE][1], results
    assert results[OTHER_TABLE][0] == "caught_up"
    assert boundary == 115  # the cut: the first unit after the key change starts there
    # items' two pages up to its cut (high 115) are its own keys; others' 4 (high 135) too,
    # so no page is shared while items is below its cut: 6 fills.
    assert p.sql.plan_stats == {"fills": 6, "hits": 0, "batched_failed": 0}, p.sql.plan_stats
    p.close()


def test_after_a_deadline_cut_the_next_run_stays_aligned(pair, monkeypatch):
    """The alignment rule: a cut lands only at a round boundary, so after a capped
    run every table is at the same checkpoint and the next run plans each remaining page
    once (P fills, P hits), never P x T."""
    p = pair(max_batch_events=2, runtime_seconds=25)
    backlog(p, 6)
    with pytest.MonkeyPatch.context() as m:
        Clock(m, per_step=5)
        first = p.run(run_id="r1")
    assert {r[0] for r in first.values()} == {"capped"}
    assert first[TABLE][1] == first[OTHER_TABLE][1]  # cut at a round boundary
    second = p.run(run_id="r2")
    assert {r[0] for r in second.values()} == {"caught_up"}
    remaining = p.sql.round_stats["rounds"] - 1
    assert remaining == 4
    assert p.sql.plan_stats == {"fills": remaining, "hits": remaining, "batched_failed": 0}
    p.close()


def test_a_run_long_lease_is_renewed_when_a_table_goes_a_third_of_it_without_writing(
    pair, monkeypatch
):
    """Leases are held across rounds. Every write renews one (guard); a table
    that steps for a third of the lease without writing renews it before it reads, so no
    round outlives a lease. In the steady state (short rounds) nothing extra is sent."""
    renewals = []
    renew = apply_run.ApplyRun._renew

    def counting(apply, t):
        before = t.renewed
        renew(apply, t)
        if apply.fold == "sql" and t.renewed != before:  # _renew sent a guard
            renewals.append(t.relation_id)

    monkeypatch.setattr(apply_run.ApplyRun, "_renew", counting)
    p = pair(max_batch_events=2)  # lease 900 s, runtime 540 s
    backlog(p, 3)
    p.run()
    assert renewals == []  # fast rounds: every write renewed it already
    backlog(p, 3)
    with pytest.MonkeyPatch.context() as m:
        # Both leases are taken at 0 s; items steps at 160 s (under a third of the lease:
        # no renewal, and its write renews it), others at 320 s (over: renewed first).
        # The next round (320 s long) wouldn't fit before 540 s: both capped.
        Clock(m, per_step=160)
        results = p.run(run_id="slow")
    assert {r[0] for r in results.values()} == {"capped"}
    assert renewals == [OTHER], renewals
    assert p.leases() == [[], []]
    p.close()


def test_the_apply_runs_stats_line_is_counts_only_and_never_a_result(pair):
    """The apply CLI prints apply_stats after its results: plan fills/hits, rounds, fold
    paths and cache use, as counts, with no "status" key (a log reader's result line)."""
    import json

    from cdc_flight.cli import apply_stats

    p = pair(max_batch_events=2)
    backlog(p, 3)
    p.run()
    line = json.loads(json.dumps(apply_stats(p.sql)))
    stats = line["apply_stats"]
    assert "status" not in line and set(stats) == {"plan", "rounds", "fold", "cache"}
    assert stats["plan"]["fills"] == 3 and stats["rounds"]["rounds"] == 4
    assert all(isinstance(v, (int, float)) for part in stats.values() for v in part.values())
    p.close()
