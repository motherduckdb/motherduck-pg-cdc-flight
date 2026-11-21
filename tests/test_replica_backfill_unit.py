"""Replica capture without PostgreSQL: the generation's finality mode, the standby
chunk's replay readings, rule U's H, the bounded conflict retry, statuses and routing."""

from types import SimpleNamespace

import duckdb
import psycopg2
import pytest

from cdc_flight import action_flight, append_backfill, chain, membership
from cdc_flight import standby_backfill as sb
from cdc_flight.apply import ApplyError
from cdc_flight.backfill import Backfill, GenerationFailed
from cdc_flight.standby_backfill import (
    CONFLICT_FLOOR_ROWS,
    CONFLICT_RETRIES,
    ReplayBehind,
    recovery_conflict,
)

UUID = "00000000-0000-0000-0000-000000000001"


# The recorded mode ----------------------------------------------------------------------


def bare(**config):
    backfill = Backfill.__new__(Backfill)
    defaults = dict(epoch=None, source_id="s", heartbeat_writer="self", chunk_rows=40)
    backfill.config = SimpleNamespace(**{**defaults, **config})
    return backfill


@pytest.mark.parametrize("columns", [False, True])
def test_start_position_reads_the_recorded_mode_and_a_pre_column_store_is_self(columns):
    con = duckdb.connect()
    con.execute("CREATE SCHEMA x")
    con.execute(
        "CREATE TABLE x.source (id INTEGER, epoch INTEGER, confirmed_lsn UBIGINT, lifecycle VARCHAR)"
    )
    con.execute("INSERT INTO x.source VALUES (1,1,77,NULL)")
    if columns:
        con.execute("ALTER TABLE x.source ADD COLUMN heartbeat_writer VARCHAR")
        con.execute("UPDATE x.source SET heartbeat_writer='external'")
    backfill = bare()
    backfill.execute = lambda statement, *args: con.execute(statement.replace("{s}", "x."), *args)
    assert backfill.start_position() == (1, 77, "external" if columns else "self")


def test_a_config_that_disagrees_with_the_recorded_mode_is_refused():
    bare().check_writer("t", "self")
    with pytest.raises(
        ApplyError, match="CDC_HEARTBEAT_WRITER=self but the source is registered as external"
    ):
        bare().check_writer("t", "external")


def test_the_generation_mode_decides_never_config():
    assert Backfill.external(dict(finality_mode="external"))
    assert not Backfill.external(dict(finality_mode=None))  # NULL = self
    assert not Backfill.external(dict(finality_mode="self"))


# The standby chunk's readings -------------------------------------------------------------


class Script:
    """A psycopg2-like connection answering the chunk transaction's reads in order:
    (in_recovery, replay before) then replay after; the chunk itself is stubbed."""

    def __init__(self, before, after, standby=True):
        self.answers = [(standby, before), (after,)]
        self.closed = 0

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def cursor(self):
        return self

    def execute(self, statement, params=None):
        self.statement = statement

    def fetchone(self):
        return self.answers.pop(0)


def chunk(before, after, *, bound=None, pin=100, standby=True):
    backfill = bare(chunk_timeout_seconds=30)
    backfill._snapshot_pg = Script(before, after, standby)
    backfill._chunk_settings = lambda con: None
    backfill._fetch_chunk = lambda con, statement, params: ([("r",)], 1, False)
    gen = dict(replay_bound=bound, pin_lsn=pin)
    return sb.read_chunk(backfill, "SELECT", [], gen)


def test_a_standby_chunk_returns_the_replay_read_after_its_snapshot():
    assert chunk(150, 160, bound=140) == ([("r",)], 1, False, 160)


def test_replay_behind_l_waits_and_changes_nothing():
    with pytest.raises(ReplayBehind):
        chunk(90, 95, pin=100)


@pytest.mark.parametrize(
    "before,after,bound",
    [(130, 160, 140), (150, 149, 140)],  # below the persisted bound; after below before
)
def test_a_replay_reading_going_backwards_fails_the_generation(before, after, bound):
    """Addendum A (USER): fail, never accept silently."""
    with pytest.raises(GenerationFailed, match="went backwards"):
        chunk(before, after, bound=bound)


def test_a_promoted_source_fails_the_generation():
    with pytest.raises(GenerationFailed, match="no longer a standby"):
        chunk(150, 160, standby=False)


# H: the first unit past B ----------------------------------------------------------------


class Reader:
    def __init__(self, ends):
        self.ends = ends

    def head(self):
        return SimpleNamespace(high=max(self.ends))

    def units(self, epoch, low, high, **limits):
        return [SimpleNamespace(end=e) for e in self.ends if low < e <= high][:2]


@pytest.mark.parametrize("bound,expected", [(300, 400), (299, 300), (500, None)])
def test_h_is_the_first_unit_ending_strictly_after_b(bound, expected):
    """Rule U by name: a unit ending exactly at B is not H (`>` not `>=`)."""
    backfill = bare(max_batch_events=10, max_batch_bytes=10)
    backfill._searched = {}
    backfill.reader = Reader([100, 200, 300, 400, 500])
    gen = dict(generation_id="g", epoch=1, wal_before_marker=bound, pin_lsn=50)
    assert sb.find_past(backfill, gen) == expected


# Bounded recovery conflicts ------------------------------------------------------------------


class Conflict(psycopg2.Error):
    def __init__(self, pgcode):
        super().__init__("x")
        self._code = pgcode

    @property
    def pgcode(self):
        return self._code


def test_a_recovery_conflict_is_classified_by_sqlstate_or_a_closed_session():
    """40001 cancels or terminates; 57P04 (database dropped) isn't
    retryable; 40P01 is never a recovery conflict. A closed session counts (a FATAL
    delivered while idle)."""
    open_con, closed = SimpleNamespace(closed=0), SimpleNamespace(closed=2)
    assert recovery_conflict(Conflict("40001"), open_con)
    assert recovery_conflict(Conflict(None), closed)
    assert not recovery_conflict(Conflict("57P04"), open_con)
    assert not recovery_conflict(Conflict("40P01"), open_con)
    assert not recovery_conflict(Conflict("42P01"), open_con)


def test_each_conflict_retry_reopens_sessions_halves_the_rows_and_is_bounded(monkeypatch):
    monkeypatch.setattr("cdc_flight.standby_backfill.time.sleep", lambda s: None)
    backfill = bare()
    backfill.config.chunk_rows = 400
    backfill._pg = backfill._snapshot_pg = None
    backfill._conflicts, backfill._chunk_rows = 0, 400
    closed = []
    backfill.close = lambda: closed.append(True)
    limits = []
    for _ in range(CONFLICT_RETRIES + 1):
        assert sb.retry_conflict(backfill, Conflict("40001"), None)
        limits.append(backfill._chunk_rows)
    assert limits == [200, 100, CONFLICT_FLOOR_ROWS, CONFLICT_FLOOR_ROWS]
    assert len(closed) == CONFLICT_RETRIES + 1
    assert backfill._conflicts == CONFLICT_RETRIES + 1  # the caller stops past the bound
    assert not sb.retry_conflict(bare(), Conflict("23505"), SimpleNamespace(closed=0))
    small = bare()  # a configured size below the floor is never raised
    small.close, small._conflicts, small._chunk_rows = (lambda: None), 0, 40
    sb.retry_conflict(small, Conflict("40001"), None)
    assert small._chunk_rows == 40


# Statuses and the chain -----------------------------------------------------------------------


@pytest.mark.parametrize("status", ["waiting_for_replay", "waiting_past_replay"])
def test_the_new_statuses_continue_the_chain_and_are_bounded(status):
    """A standby that never catches up (or a dead heartbeat job) stops the chain
    visibly, not forever."""
    from cdc_flight.backfill import BackfillResult

    assert status in chain.CONTINUE and status in chain.WAITING
    result = BackfillResult("s.t", "g", status, 1, 1, 0)
    same = chain.progress_signature(result, 0)
    fresh = chain.BackfillChain("s.t", self_flight_id=UUID, delay_seconds=0)
    assert chain.after_backfill(result, fresh, lambda *a, **k: None, sleep=lambda s: None)[0] == (
        "triggered"
    )
    last = chain.BackfillChain(
        "s.t", self_flight_id=UUID, delay_seconds=0, last_progress=same, stalled_runs=9
    )
    assert chain.after_backfill(result, last, lambda *a, **k: None, sleep=lambda s: None)[0] == (
        "stalled"
    )


# Routing --------------------------------------------------------------------------------------


@pytest.mark.parametrize("action", ["resync", "start-snapshot"])
@pytest.mark.parametrize("mode", ["append", None])
def test_table_action_previews_a_standby_backfill_like_a_primary_one(action, mode, monkeypatch):
    """Keyed and append tables both backfill from a standby. Every backfill Flight
    is a package Flight (bundles removed, USER 2026-11-17), so the preview no longer
    names one: it is the same for both modes."""
    apply = SimpleNamespace(config=SimpleNamespace(mirror_database="m"))
    inputs = SimpleNamespace(action=action, table="public.t", values={}, dry_run=True)
    check = "resync_check" if action == "resync" else "snapshot_check"
    observed = dict(capture_mode=mode, state="needs_resync", rows=0, size_bytes=0)
    monkeypatch.setattr(action_flight, check, lambda apply, inputs: (1, observed, None))
    monkeypatch.setattr(action_flight, "decision", lambda *a, **k: {})
    monkeypatch.setattr(action_flight, "record_preview", lambda *a, **k: None)
    monkeypatch.setattr(action_flight, "append_resync_cost", lambda *a: "a re-snapshot")
    details = set()
    for writer in ("external", "self"):
        owner = action_flight.Owner(
            source_id="s", backfill_flight=UUID, compact_flight=UUID, heartbeat_writer=writer
        )
        out = action_flight.perform(apply, inputs, run_id="r", actor="a", owner=owner, trigger=None)
        assert out["status"] == "would"
        details.add(out["detail"])
    assert len(details) == 1 and "package" not in details.pop()


def test_an_append_generation_records_the_sources_mode(monkeypatch):
    """No per-table refusal; the append INSERT carries finality_mode."""
    monkeypatch.setattr(append_backfill, "refusal", lambda *a: None)
    inserted = []
    monkeypatch.setattr(
        append_backfill.commands,
        "inserted",
        lambda owner, command, name, statement, args: inserted.append((name, statement, args)),
    )
    backfill = bare(lease_seconds=900)
    backfill.config.heartbeat_writer = "external"
    backfill.start_position = lambda: (1, 10, "external")
    backfill.catalog = lambda relation_id, table: ([], None)
    backfill.pin_generation = lambda *a: None
    backfill.mirror = "m"
    backfill.con = None

    class Txn:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    backfill.transaction = Txn
    monkeypatch.setattr(append_backfill, "create_mirror_table", lambda *a: None)
    backfill.generation = lambda generation_id: {"generation_id": generation_id}
    append_backfill.start(backfill, "public.events", 1, "r")
    (name, statement, args) = [x for x in inserted if x[0] == "generation_insert"][0]
    assert "finality_mode) VALUES" in statement and args[-1] == "external"


def test_membership_names_the_primary_for_a_standby_sources_publication():
    standby = SimpleNamespace(config=SimpleNamespace(heartbeat_writer="external"))
    primary = SimpleNamespace(config=SimpleNamespace(heartbeat_writer="self"))
    assert membership.on_primary(standby) == " on the primary"
    assert membership.on_primary(primary) == ""


def test_a_dropped_database_fails_the_generation_instead_of_retrying():
    """57P04 is a recovery conflict that can't be retried."""
    backfill = bare()
    backfill._conflicts, backfill._chunk_rows = 0, 40
    with pytest.raises(GenerationFailed, match="database was dropped"):
        sb.retry_conflict(backfill, Conflict("57P04"), SimpleNamespace(closed=2))
    assert backfill._conflicts == 0


def test_each_run_starts_with_fresh_conflict_state():
    """The retry count and the halved chunk size don't carry across runs of
    one Backfill instance (tests and a long-lived embedder reuse it)."""
    backfill = bare()
    backfill._conflicts, backfill._chunk_rows = CONFLICT_RETRIES + 1, 10

    def registered(*a, **k):
        raise RuntimeError("stop after the reset")

    backfill.registered = registered
    with pytest.raises(RuntimeError, match="stop after the reset"):
        backfill.run_table("public.t")
    assert (backfill._conflicts, backfill._chunk_rows) == (0, backfill.config.chunk_rows)


class Blocked:
    """A replication connection whose slot creation waits until cancelled (spike item 6:
    the CancelRequest returns it with QueryCanceled)."""

    closed = 0

    def __init__(self):
        import threading

        self.cancelled = threading.Event()

    def get_backend_pid(self):
        return 4242

    def cancel(self):
        self.cancelled.set()

    def cursor(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, statement):
        assert "TEMPORARY LOGICAL pgoutput (SNAPSHOT 'export')" in statement
        self.cancelled.wait(5)
        raise psycopg2.errors.QueryCanceled("canceling statement due to user request")


def test_a_blocked_export_slot_is_bounded_by_the_cancel_request():
    repl = Blocked()
    with pytest.raises(sb.WaitingForPrimary):
        sb._create_export_slot(SimpleNamespace(), repl, "cdc_snap_x", 0.2)
    assert repl.cancelled.is_set()


def test_a_slot_wait_timeout_writes_nothing_and_waits_for_the_primary(monkeypatch):
    """The slot and import come before copy_started, so a timeout leaves the
    generation snapshotting and retryable; the run ends waiting_for_primary."""
    monkeypatch.setattr(sb, "connect", lambda *a, **k: SimpleNamespace(close=lambda: None))

    def timed_out(*a):
        raise sb.WaitingForPrimary()

    monkeypatch.setattr(sb, "_create_export_slot", timed_out)
    backfill = bare(slot_wait_seconds=2, lease_seconds=900)
    backfill.held = lambda *a: pytest.fail("nothing is written on a timeout")
    from cdc_flight.backfill import BackfillResult

    backfill.result = lambda table, generation_id, status: BackfillResult(
        table, generation_id, status
    )
    gen = dict(generation_id="g" * 32, pin_lsn=1)
    out = sb.append_copy(backfill, "public.events", gen, 1)
    assert out.status == "waiting_for_primary"
    assert "pg_log_standby_snapshot()" in out.detail and "nothing was copied" in out.detail


def test_waiting_for_primary_continues_the_chain_and_is_bounded():
    from cdc_flight.backfill import BackfillResult

    assert "waiting_for_primary" in chain.CONTINUE and "waiting_for_primary" in chain.WAITING
    result = BackfillResult("s.t", "g", "waiting_for_primary", 1, 1, 0)
    last = chain.BackfillChain(
        "s.t",
        self_flight_id=UUID,
        delay_seconds=0,
        last_progress=chain.progress_signature(result, 0),
        stalled_runs=9,
    )
    assert chain.after_backfill(result, last, lambda *a, **k: None, sleep=lambda s: None)[0] == (
        "stalled"
    )


# Epoch on a standby ---------------------------------------------------------------------------


def test_epoch_refuses_a_changed_timeline_on_a_standby_and_reads_nothing_on_a_primary():
    from cdc_flight.epoch import same_timeline
    from cdc_flight.ingest_common import SourceMismatch

    class Log:
        def __init__(self, timeline):
            self.timeline, self.reads = timeline, 0

        def execute(self, statement):
            self.reads += 1
            return SimpleNamespace(fetchone=lambda: (self.timeline,))

    primary = Log(1)
    same_timeline(SimpleNamespace(heartbeat_writer="self"), primary, 9)
    assert primary.reads == 0
    same_timeline(SimpleNamespace(heartbeat_writer="external"), Log(2), 2)
    with pytest.raises(SourceMismatch, match=r"timeline changed \(2 -> 3\)"):
        same_timeline(SimpleNamespace(heartbeat_writer="external"), Log(2), 3)


class ImportConflict:
    """An import session ended by a recovery conflict at its first statement."""

    closed = 0

    def cursor(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, *a):
        raise Conflict("40001")

    def set_session(self, **k):
        pass

    def close(self):
        pass


def test_a_conflict_before_copy_started_is_retryable_not_a_failure(monkeypatch):
    """Before copy_started nothing was copied, so a conflict or lost
    session during the import ends the run snapshotting; the next run takes a new slot."""
    from cdc_flight.backfill import BackfillResult

    replication = SimpleNamespace(close=lambda: None)
    sessions = iter([replication, ImportConflict()])
    monkeypatch.setattr(sb, "connect", lambda *a, **k: next(sessions))
    monkeypatch.setattr(sb, "_create_export_slot", lambda *a: (100, "snap"))
    backfill = bare(slot_wait_seconds=2, lease_seconds=900)
    backfill.held = lambda *a: pytest.fail("nothing is written before copy_started")
    backfill.result = lambda table, generation_id, status: BackfillResult(
        table, generation_id, status
    )
    out = sb.append_copy(backfill, "public.events", dict(generation_id="g" * 32, pin_lsn=1), 1)
    assert out.status == "snapshotting" and "before any row was copied" in out.detail


class LateSuccess(Blocked):
    """The creation returns its row only after a step fired."""

    def execute(self, statement):
        self.cancelled.wait(5)

    def fetchone(self):
        return ("slot", "0/10", "snap", "pgoutput")


def test_a_creation_that_succeeds_after_a_step_fired_is_treated_as_the_bound():
    with pytest.raises(sb.WaitingForPrimary):
        sb._create_export_slot(SimpleNamespace(), LateSuccess(), "cdc_snap_x", 0.2)
