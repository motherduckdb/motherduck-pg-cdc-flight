"""A real-PostgreSQL proof of the compaction-conflict pipeline.

A compaction drop of a legitimately droppable day holds the source guard (its pending
UPDATE, in Retention2.drop_partition's fixed order) on its own connection while a real
format-2 ingest run writes to the same row: the worker's append (W) or the main thread's
idle lease renew (R). The run must end retryable, after its bounded cleanup:
`busy` with no END progress in this run, `conflict` with some; nothing committed past
its own boundary, no feedback past it, the lease released and the append future
settled; then the drop commits and the next run resumes exactly once.

Real WAL, real IngestLog2/Spool2/Retention2 on local DuckDB files and the stream's own
PostgreSQL; the old day is made by the first batch's partition_day, the pin through the
Retention2 API. No MotherDuck, no fake WAL or metadata. Ordering uses Events only.
"""

import threading
import time

import pytest
from test_ingest_batching import small_transactions
from test_ingest_pipeline import prepared
from test_phase1 import pg_phase1  # noqa: F401 (fixture)

from cdc_flight import format2 as f2
from cdc_flight import ingest as ingest_module
from cdc_flight import ops_ingest
from cdc_flight.ingest import run_once
from cdc_flight.ingest_log import GuardedIngestConflict
from cdc_flight.preflight import query
from cdc_flight.retention2 import Retention2

OLD_DAY = "20000101"
WAIT = 30  # seconds: a stuck interleaving fails loudly instead of hanging


def live_days(log):
    """Days whose frames still exist: a legitimately dropped day is gone by design."""
    rows = log.execute(
        "SELECT DISTINCT partition_day FROM {s}frame_partitions_v2 WHERE status<>'dropped' "
        "ORDER BY 1"
    ).fetchall()
    return [day for (day,) in rows]


def item_ids(log, source, days):
    """Inserted `items` ids per day set, from the stored frames (with multiplicity)."""
    if not days:
        return []
    ((oid,),) = query(source.con, "SELECT %s::regclass::oid", [source.schema + ".items"])
    union = " UNION ALL ".join(f"SELECT * FROM {{s}}frames_v2_{day}" for day in days)
    rows = log.execute(
        f"SELECT (new['values'][1])::INTEGER FROM ({union}) "
        "WHERE record_kind=? AND op='insert' AND relation_id=? ORDER BY 1",
        [f2.EVENT, int(oid)],
    ).fetchall()
    return [i for (i,) in rows]


def assert_exactly_once(log, source, dropped_ids):
    """Every surviving frame stored once and counted by its chunks; every source id
    stored exactly once, either in a surviving day or in the dropped one, never both."""
    days = live_days(log)
    union = " UNION ALL ".join(
        f"SELECT source_epoch,unit_start_lsn,frame_ordinal FROM {{s}}frames_v2_{day}"
        for day in days
    )
    frames, distinct = log.execute(
        f"SELECT count(*),count(DISTINCT (source_epoch,unit_start_lsn,frame_ordinal)) FROM ({union})"
    ).fetchone()
    assert frames == distinct, "a frame was stored twice"
    counted = log.execute(
        "SELECT sum(n) FROM {s}chunks_v2 WHERE list_contains(?,partition_day)", [days]
    ).fetchone()[0]
    assert counted == frames, "a surviving chunk row doesn't match its frames"
    live = item_ids(log, source, days)
    assert len(live) == len(set(live)), "an id was stored twice"
    assert not set(live) & set(dropped_ids), "an id is both dropped and stored again"
    expected = [i for (i,) in query(source.con, "SELECT id FROM items ORDER BY id")]
    assert sorted(live + list(dropped_ids)) == expected


class Drop(threading.Thread):
    """drop_partition on its own connection, parked right after its source-guard UPDATE
    until `release`: the window in which ingest's write must conflict."""

    def __init__(self, retention, token, day=OLD_DAY):
        super().__init__(daemon=True)
        self.retention, self.token, self.day = retention, token, day
        self.held, self.release = threading.Event(), threading.Event()
        self.result = self.error = self.committed = None

    def run(self):
        def fault(stage):
            if stage == "after_source_guard":
                self.held.set()
                if not self.release.wait(WAIT):
                    raise TimeoutError("the test never released the drop")

        try:
            self.result = self.retention.drop_partition(self.day, self.token, fault=fault)
        except BaseException as exc:  # reported by finish()
            self.error = exc

    def start_and_hold(self):
        self.start()
        if not self.held.wait(WAIT):
            raise AssertionError(f"the drop never reached its source guard: {self.error!r}")

    def finish(self):
        self.release.set()
        self.join(WAIT)
        assert not self.is_alive(), "the drop did not finish"
        if self.error is not None:
            raise self.error
        return self.result


class World:
    """A seeded format-2 source: history on OLD_DAY (the first batch) and today (the
    rest), the old day sealed by ingest itself, a real generation pin above it, and a
    compaction lease on a second connection."""

    def __init__(self, stores, pg_phase1, monkeypatch, **config):  # noqa: F811
        self.source, self.config, self.log, self.h = prepared(
            stores, pg_phase1, monkeypatch, fmt="v2", **config
        )
        self.stores, self.monkeypatch = stores, monkeypatch
        self.next_id = 1
        on_old_day = [True]
        harnessed = self.log.append

        def first_batch_on_the_old_day(items, **options):
            # Appends go to OLD_DAY up to and including the first that carries an event.
            # On PostgreSQL 13 other sessions' transactions reach this slot as empty
            # members. Under parallel tests they can fill a unit (max_commits; max_delay_ms
            # is out of reach) ahead of the seeded inserts, and the 0.1 s time flush then
            # appends it alone: a batch of those isn't "the first batch" of history.
            if on_old_day[0]:
                options["partition_day"] = OLD_DAY
                on_old_day[0] = not any(item.event_count for item in items)
            return harnessed(items, **options)

        monkeypatch.setattr(self.log, "append", first_batch_on_the_old_day)
        self.statuses = []
        recorded = self.log.run_status

        def run_status(run_id, status, detail="", **kwargs):
            self.statuses.append((run_id, status, detail, kwargs.get("boundary")))
            return recorded(run_id, status, detail, **kwargs)

        monkeypatch.setattr(self.log, "run_status", run_status)
        self.commit(60)
        seeded = run_once(self.config, self.log)
        assert seeded.status == "drained", seeded
        parts = self.log.execute(
            "SELECT status FROM {s}frame_partitions_v2 WHERE partition_day=?", [OLD_DAY]
        ).fetchall()
        assert parts == [("sealed",)], parts
        self.old_ids = item_ids(self.log, self.source, [OLD_DAY])
        assert self.old_ids, "the old day holds no inserts"
        ((oid,),) = query(
            self.source.con, "SELECT %s::regclass::oid", [self.source.schema + ".items"]
        )
        self.oid = int(oid)
        # Compaction's own connection (a separate transaction context on the database).
        con = stores[0].con.cursor()
        self.retention = Retention2(con, self.config.database, self.config.source_id)
        self.retention.initialize()
        self.retention.register_generation(self.oid, "reader", 1, self.log.checkpoint())
        self.token = self.retention.acquire_compaction("compactor")

    def commit(self, count):
        small_transactions(self.source, count, start=self.next_id)
        self.next_id += count

    def drop(self):
        return Drop(self.retention, self.token)

    def commit_drop_on_conflict(self, drop, method):
        """Ordering (a): compaction's short transaction commits as soon as ingest's write
        has conflicted, before ingest's cleanup releases its lease (the catcher's path)."""
        original = getattr(self.log, method)

        def conflicted(*args, **kwargs):
            try:
                return original(*args, **kwargs)
            except GuardedIngestConflict:
                if drop.is_alive():
                    drop.committed = drop.finish()
                raise

        self.monkeypatch.setattr(self.log, method, conflicted)

    def progressed(self, stage, observe, initial):
        """`after_append` for a batch that moved the checkpoint: the tests "after
        progress" start the drop here. The first collected batch need not be progress:
        a time flush (flush_seconds) can append a chunk of a still-open unit, which
        leaves the checkpoint at `initial` (on a slow walsender, e.g. a loaded CI
        runner). A drop taken then gives `busy`, not the `conflict` under test."""
        return stage == "after_append" and observe.get("checkpoint", initial) > initial

    def run(self, hook, *, observe=None):
        """One run with `hook(stage)` as its fault injector; returns (result, observe)."""
        observe = {} if observe is None else observe
        threads = set(threading.enumerate())
        result = run_once(self.config, self.log, fault=hook, observe=observe)
        workers = [t for t in threading.enumerate() if t not in threads and t.is_alive()]
        workers = [t for t in workers if not isinstance(t, Drop)]
        assert workers == [], f"threads left by the run: {workers}"
        return result, observe

    def slot_flush(self):
        ((lsn,),) = query(
            self.source.con,
            "SELECT confirmed_flush_lsn-'0/0'::pg_lsn FROM pg_replication_slots WHERE slot_name=%s",
            [self.config.slot],
        )
        return int(lsn)

    def assert_nothing_past(self, own, before):
        """The source row, the unit ledger and the slot stop at
        the run's own boundary; the open anchor and capped-replay fields don't move."""
        row = self.log.execute(
            "SELECT confirmed_lsn,open_unit_anchor,last_capped_commit,last_capped_count,"
            "holder IS NULL AND expires_at IS NULL FROM {s}source WHERE id=1"
        ).fetchone()
        assert row[0] == own, (row, own)
        # A batch may end inside a unit: its durable prefix is anchored exactly at the
        # run's own boundary (the designed resume state), never beyond it.
        assert row[1] in (None, own), ("open unit anchor moved past own", row, own)
        assert row[2:4] == before[2:4], "capped-replay fields moved"
        assert row[4], "the ingest lease was not released (holder/expiry left set)"
        complete = self.log.execute(
            "SELECT max(end_lsn) FROM {s}units_v2 WHERE epoch=1 AND complete"
        ).fetchone()[0]
        open_units = self.log.execute(
            "SELECT unit_start_lsn FROM {s}units_v2 WHERE epoch=1 AND NOT complete"
        ).fetchall()
        assert complete == own, (complete, own)
        assert open_units in ([], [(own,)]), ("an incomplete unit past own", open_units, own)
        assert (row[1] is None) == (open_units == [])
        chunks_past = self.log.execute(
            "SELECT count(*) FROM {s}chunks_v2 WHERE epoch=1 AND unit_start_lsn>?", [own]
        ).fetchone()[0]
        assert chunks_past == 0, "chunks recorded past the run's own boundary"
        assert self.slot_flush() <= own, "feedback acknowledged past the run's boundary"

    def source_row(self):
        return self.log.execute(
            "SELECT confirmed_lsn,open_unit_anchor,last_capped_commit,last_capped_count,"
            "holder IS NULL AND expires_at IS NULL FROM {s}source WHERE id=1"
        ).fetchone()

    def assert_retryable(self, result, observe, status, initial):
        own = result.boundary
        assert result.status == status, result
        assert observe["checkpoint"] == own
        if status == "busy":
            assert own == initial
        else:
            assert own > initial
        terminal = [s for s in self.statuses if s[0] == result.run_id and s[1] == status]
        assert terminal == [(result.run_id, status, "compaction_conflict", own)], self.statuses
        rows = self.log.execute(
            "SELECT status,detail,boundary FROM {s}runs WHERE run_id=?", [result.run_id]
        ).fetchall()
        assert rows == [(status, "compaction_conflict", own)], rows
        assert self.log.token is None

    def finish_and_resume(self, drop, own):
        """The drop has committed; the next run resumes exactly once."""
        dropped = drop.committed if drop.committed is not None else drop.finish()
        assert dropped, dropped
        assert f"frames_v2_{OLD_DAY}" not in self.log.catalog()
        assert self.retention.floor() is not None
        self.commit(5)
        after = run_once(self.config, self.log)
        assert after.status == "drained" and after.boundary > own, after
        assert_exactly_once(self.log, self.source, self.old_ids)


def built_world(stores, pg_phase1, monkeypatch, **config):  # noqa: F811
    if stores[0].remote:
        pytest.skip("local stores only (C3b uses no MotherDuck)")
    w = World(stores, pg_phase1, monkeypatch, **config)
    try:
        yield w
    finally:
        try:
            w.retention.release_compaction(w.token)
        except Exception:
            pass


@pytest.fixture
def world(stores, pg_phase1, monkeypatch):  # noqa: F811
    yield from built_world(stores, pg_phase1, monkeypatch)


# The "after progress" tests also run with an eager time flush, so the first collected
# batch is a chunk of a still-open unit: what a slow walsender gives with the default
# flush. Deterministic, it pins World.progressed (CI: busy, chunks=1, transactions=0).
EAGER_FLUSH_S = 0.001


@pytest.fixture(params=["flush", "eager_flush"])
def progress_world(request, stores, pg_phase1, monkeypatch):  # noqa: F811
    config = {"flush_seconds": EAGER_FLUSH_S} if request.param == "eager_flush" else {}
    yield from built_world(stores, pg_phase1, monkeypatch, **config)


@pytest.mark.postgres
def test_W_busy_an_append_conflict_before_any_progress_is_busy(world):
    """The drop takes the guard after the lease, before the run's first append."""
    world.commit(30)
    before, initial = world.source_row(), world.log.checkpoint()
    drop = world.drop()
    world.commit_drop_on_conflict(drop, "append")

    def hook(stage):
        if stage == "before_append" and not drop.is_alive() and drop.result is None:
            drop.start_and_hold()

    result, observe = world.run(hook)
    world.assert_retryable(result, observe, "busy", initial)
    world.assert_nothing_past(result.boundary, before)
    world.finish_and_resume(drop, result.boundary)


@pytest.mark.postgres
def test_W_conflict_an_append_conflict_after_progress_keeps_its_own_boundary(progress_world):
    """A batch that moved the checkpoint is collected and fenced; the drop then takes
    the guard and the worker's next append conflicts."""
    world = progress_world
    world.commit(120)
    before, initial = world.source_row(), world.log.checkpoint()
    drop = world.drop()
    world.commit_drop_on_conflict(drop, "append")
    observe = {}

    def hook(stage):
        if (
            world.progressed(stage, observe, initial)
            and not drop.is_alive()
            and drop.result is None
        ):
            drop.start_and_hold()

    result, observe = world.run(hook, observe=observe)
    world.assert_retryable(result, observe, "conflict", initial)
    world.assert_nothing_past(result.boundary, before)
    world.finish_and_resume(drop, result.boundary)


@pytest.mark.postgres
def test_R_conflict_an_idle_lease_renew_conflict_after_progress(progress_world):
    """The main thread's renew is the conflicting write. After the first
    collected batch the reads go quiet and the renew falls due on the harness clock."""
    world = progress_world
    world.commit(5)  # one batch: the run then idles until its marker is read
    before, initial = world.source_row(), world.log.checkpoint()
    drop = world.drop()
    renews = []
    observe = {}
    renew = world.log.renew

    def counted():
        renews.append(threading.get_ident() == world.h.main)
        return renew()

    world.monkeypatch.setattr(world.log, "renew", counted)
    world.commit_drop_on_conflict(drop, "renew")

    def hook(stage):
        if (
            world.progressed(stage, observe, initial)
            and not drop.is_alive()
            and drop.result is None
        ):
            world.h.quiet_until = time.monotonic() + WAIT  # no WAL arrives
            drop.start_and_hold()
            world.h.clock.offset += world.config.lease_seconds  # the renew falls due

    result, observe = world.run(hook, observe=observe)
    world.h.quiet_until = 0.0
    assert renews and renews[-1] is True, "the conflicting write was not the main renew"
    world.assert_retryable(result, observe, "conflict", initial)
    world.assert_nothing_past(result.boundary, before)
    world.finish_and_resume(drop, result.boundary)


@pytest.mark.postgres
def test_a_conflict_run_reports_leased_with_its_own_checkpoint(progress_world):
    """pins (3): fed to cdc_ops, a conflict is a leased run (not busy, not a success)
    whose checkpoint is the run's own boundary."""
    if not hasattr(ops_ingest, "CONFLICT"):
        pytest.skip("the format-2 cdc_ops conflict writer is not on this branch")
    world = progress_world
    world.commit(120)
    initial = world.log.checkpoint()
    drop = world.drop()
    world.commit_drop_on_conflict(drop, "append")
    observe = {}

    def hook(stage):
        if (
            world.progressed(stage, observe, initial)
            and not drop.is_alive()
            and drop.result is None
        ):
            drop.start_and_hold()

    result, observe = world.run(hook, observe=observe)
    assert result.status == "conflict"
    con = world.stores[0].con
    con.execute("ATTACH ':memory:' AS cdc_ops")
    # The ops contract: the caller reads IngestLog2.unit_position() before the report.
    observe.update(world.log.unit_position())
    ops_ingest.report_ingest(con, world.config, observe, database="cdc_ops")
    row = con.execute(
        "SELECT status,checkpoint_lsn,last_conflict_run_id,last_busy_run_id "
        "FROM cdc_ops.ingest_status"
    ).fetchone()
    # A leased run: recorded as the source's last leased run (status conflict, its own
    # checkpoint), named by last_conflict_*, and never counted as busy.
    assert row[:3] == ("conflict", result.boundary, result.run_id), row
    assert row[3] != result.run_id, row


def test_the_catcher_is_installed_on_this_branch():
    """The proof above is about run_once's catcher (cleanup 6f88cd3)."""
    assert hasattr(ingest_module, "_run_once")


# A bounded, v2-only retry of the lease release on a conflict, inside
# IngestLog2.release. F1 (cleanup): a drop must leave a fresh log able to ingest. ---------

from cdc_flight.ingest_log import RELEASE  # noqa: E402
from cdc_flight.ingest_log2 import IngestLog2  # noqa: E402


def release_attempts(world, on_conflict=None):
    """Record the log's RELEASE statements (attempt, outcome) without depending on how the
    retry is written; `on_conflict(n)` runs after the n-th conflicting attempt."""
    attempts = []
    execute = world.log.execute

    def watched(sql, params=None):
        if sql != RELEASE:
            return execute(sql, params)
        try:
            out = execute(sql, params)
        except Exception as exc:
            attempts.append(type(exc).__name__)
            if on_conflict is not None:
                on_conflict(len(attempts))
            raise
        attempts.append("ok")
        return out

    world.monkeypatch.setattr(world.log, "execute", watched)
    return attempts


@pytest.mark.postgres
def test_b1_a_drop_committing_during_the_release_retries_still_ends_retryable(progress_world):
    """The drop is still holding when ingest's cleanup releases its lease; it commits
    after the first conflicting release attempt, and a bounded retry then releases."""
    world = progress_world
    world.commit(120)
    before, initial = world.source_row(), world.log.checkpoint()
    drop = world.drop()

    def commit_drop(n):
        if n == 1 and drop.is_alive():
            drop.committed = drop.finish()

    attempts = release_attempts(world, commit_drop)
    observe = {}

    def hook(stage):
        if (
            world.progressed(stage, observe, initial)
            and not drop.is_alive()
            and drop.result is None
        ):
            drop.start_and_hold()

    result, observe = world.run(hook, observe=observe)
    assert attempts[0] != "ok" and attempts[-1] == "ok" and len(attempts) <= 4, attempts
    assert observe["cleanup_release_attempts"] == len(attempts)
    world.assert_retryable(result, observe, "conflict", initial)
    world.assert_nothing_past(result.boundary, before)
    world.finish_and_resume(drop, result.boundary)


@pytest.fixture
def short_lease(stores, pg_phase1, monkeypatch):  # noqa: F811
    # The shortest lease the config allows (30 s): the fallback is proven by real expiry,
    # never by editing the lease row, so this one test waits about 31 s.
    yield from built_world(stores, pg_phase1, monkeypatch, lease_seconds=30)


@pytest.mark.postgres
def test_b2_a_drop_holding_past_every_retry_fails_the_run_until_the_lease_expires(short_lease):
    """The documented fallback: the run raises the original conflict, its lease row is
    still held, and after expiry the next run acquires and resumes exactly once."""
    world = short_lease
    world.commit(120)
    drop = world.drop()
    attempts = release_attempts(world)

    def hook(stage):
        if stage == "after_append" and not drop.is_alive() and drop.result is None:
            drop.start_and_hold()

    started = time.monotonic()
    observe = {}
    with pytest.raises(GuardedIngestConflict):
        world.run(hook, observe=observe)
    elapsed = time.monotonic() - started
    assert attempts and "ok" not in attempts and len(attempts) == 4, attempts  # 1 + 3 retries
    assert observe["cleanup_release_attempts"] == len(attempts)
    assert elapsed < 10, elapsed  # the retry is bounded (~1.3 s), never a long wait
    held = world.log.execute(
        "SELECT holder IS NOT NULL AND expires_at IS NOT NULL FROM {s}source WHERE id=1"
    ).fetchone()[0]
    assert held, "the lease row was released despite the conflict"
    drop.committed = drop.finish()
    time.sleep(world.config.lease_seconds + 0.5)  # the lease expires on its own
    world.finish_and_resume(drop, world.log.checkpoint())


@pytest.mark.postgres
@pytest.mark.parametrize("pruned", [False, True])
def test_a_fresh_log_ingests_after_a_drop(world, pruned):
    """A committed drop (and its prune) must leave a fresh
    IngestLog2 able to load its days and ingest."""
    assert world.retention.drop_partition(OLD_DAY, world.token)
    if pruned:
        world.retention.prune(world.token)
    fresh = IngestLog2(world.stores[0].con, world.config)
    world.commit(10)
    result = run_once(world.config, fresh)
    assert result.status == "drained", result
    assert_exactly_once(fresh, world.source, world.old_ids)


@pytest.mark.postgres
def test_a_non_conflict_release_error_is_not_retried(world):
    """Mutant guard: only a conflict is retried. A different transaction error
    on the release (injected on that one statement) propagates after one attempt and
    leaves the lease held for expiry, as before the retry existed."""
    import duckdb

    world.log.acquire("probe")
    attempts = []
    execute = world.log.execute

    def failing(sql, params=None):
        if sql == RELEASE:
            attempts.append(1)
            raise duckdb.TransactionException("a different transaction failure")
        return execute(sql, params)

    world.monkeypatch.setattr(world.log, "execute", failing)
    with pytest.raises(duckdb.TransactionException, match="different transaction failure"):
        world.log.release(sleep=lambda s: None)
    assert attempts == [1] and world.log.token is not None


# Append's own conflict tag, for a conflict raised by a
# statement AFTER its guard, inside its transaction. A real run can't reach it (the guard
# conflicts first), so a fault callback raises the conflict at that point. -------------

import duckdb  # noqa: E402
from test_ingest_log2 import build, log2  # noqa: E402,F401 (fixture)


@pytest.mark.parametrize("stage", ["after_payload", "before_commit"])
def test_R1_a_conflict_after_the_guard_inside_append_is_tagged_chained_and_rolled_back(
    log2,  # noqa: F811
    stage,
):
    items = build(log2)
    before = log2.checkpoint()
    conflict = duckdb.TransactionException("TransactionContext Error: Conflict on update!")

    def fault(at):
        if at == stage:
            raise conflict

    with pytest.raises(GuardedIngestConflict) as caught:
        log2.append(items, fault=fault)
    assert caught.value.__cause__ is conflict, "the original conflict must be chained"
    # Rolled back: no unit, chunk, descriptor or frame row, and the checkpoint is unchanged.
    assert log2.checkpoint() == before
    for table in ("units_v2", "chunks_v2", "descriptors_v2"):
        assert log2.execute(f"SELECT count(*) FROM {{s}}{table}").fetchone()[0] == 0, table
    frames = [t for t in log2.catalog() if t.startswith("frames_v2_")]
    for table in frames:
        assert log2.execute(f"SELECT count(*) FROM {{s}}{table}").fetchone()[0] == 0, table
    # The log stays usable: the same items append once the conflict is gone.
    assert log2.append(items) > before


def test_R1_a_non_conflict_error_after_the_guard_is_not_tagged(log2):  # noqa: F811
    items = build(log2)
    other = duckdb.TransactionException("TransactionContext Error: something else")

    def fault(at):
        if at == "after_payload":
            raise other

    with pytest.raises(duckdb.TransactionException) as caught:
        log2.append(items, fault=fault)
    assert caught.value is other and not isinstance(caught.value, GuardedIngestConflict)
