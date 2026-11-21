"""Decode stage 2: run_once spools the next batch while one append is in flight.

The harness forces overlap by holding each append on the worker until the main thread
has spooled more, and checks the pipeline's invariants on every run:
- no main-thread use of the destination connection (or a renew or guard) while the
  worker runs an append or renew, except interrupt();
- every status sent is at most the durable checkpoint, read on another connection;
- statuses sent while the worker is busy repeat the previous value;
- the stored chunks equal what a fresh Spool makes of the same pgoutput payloads;
- chunk payload held in memory stays within two batches.

Every test runs against both log representations: format 1 (IngestLog,
Spool) and format 2 (IngestLog2, Spool2), through the same single run_once loop. The
format-2 binding is read from an actual replication session's SHOW, never assumed.
"""

import logging
import threading
import time
from dataclasses import replace

import conftest
import duckdb
import pytest
from test_ingest_batching import small_transactions, stored_once
from test_phase1 import pg_phase1  # noqa: F401 (fixture)

import cdc_flight.ingest as ingest_module
from cdc_flight import format2 as f2
from cdc_flight.ingest import run_once
from cdc_flight.ingest_log2 import IngestConfig2, IngestLog2
from cdc_flight.preflight import check, connect, connection_options, query
from cdc_flight.replication import ReplicationStream
from cdc_flight.spool2 import Spool as Spool2

CHUNKS = (f2.Chunk,)


@pytest.fixture(params=["v2"])
def fmt(request):
    return request.param


class Clock:
    """ingest's `time`: real monotonic time plus an offset a test can jump."""

    def __init__(self):
        self.offset = 0.0

    def monotonic(self):
        return time.monotonic() + self.offset


class Owned:
    """The destination connection, failing main-thread use while the worker is busy."""

    def __init__(self, con, harness):
        self._con, self._harness = con, harness

    def __getattr__(self, name):
        attribute = getattr(self._con, name)
        if not callable(attribute):
            return attribute

        def checked(*args, **kwargs):
            self._harness.check(name)
            return attribute(*args, **kwargs)

        return checked


class Harness:
    def __init__(self, monkeypatch, log, durable, *, hold=0.5, fmt="v2"):
        self.main = threading.get_ident()
        self.log, self.durable, self.hold = log, durable, hold
        self.clock = Clock()
        self.busy = False  # the worker is inside an append or a renew
        self.violations, self.interrupts, self.main_renews = [], 0, 0
        self.statuses = []  # (value, durable checkpoint, worker busy)
        self.payloads, self.spool_args, self.spool_kwargs = [], None, {}
        self.spool_class = Spool2
        self.fed = self.fed_bytes = self.released_bytes = self.peak_bytes = 0
        # Held payload is measured as Arrow buffers; .bytes are protocol charges.
        self.iteration_bytes = self.max_iteration_bytes = 0
        self.spool = None  # the run's spool; a respooled() copy isn't counted
        self.depth = 0  # only the outermost spool call counts (feed may close or poll)
        self.appends = self.overlapped = 0
        self.jump = {}  # append number -> seconds the clock jumps while it is held
        self.sleep = {}  # append number -> real seconds it is held instead
        self.crash_feed_when_busy = False
        self.first_append_barrier = None  # opt-in (started, release) Events
        self.quiet_until = 0.0
        self.renews_at_jump = None
        self.worker_renews = self.worker_renews_at_jump = 0
        self.after_fail = None  # called just before a crashing feed raises
        monkeypatch.setattr(ingest_module, "time", self.clock)
        log.con = Owned(log.con, self)
        self._wrap_log(monkeypatch)
        self._wrap_spool(monkeypatch)
        self._wrap_replication(monkeypatch)

    def check(self, name):
        if threading.get_ident() != self.main:
            return
        if name == "interrupt":
            self.interrupts += 1
        elif self.busy:
            self.violations.append(name)

    def _wrap_log(self, monkeypatch):
        harness, log = self, self.log
        append, renew, guard = log.append, log.renew, log.guard

        def held_append(items, **options):
            harness.appends += 1
            number = harness.appends
            size = harness.measure(items)  # before append: it may rebuild the tables
            harness.busy = True
            try:
                if number in harness.jump:  # the append "takes" this long: statuses are due
                    harness.renews_at_jump = harness.main_renews
                    harness.worker_renews_at_jump = harness.worker_renews
                    harness.clock.offset += harness.jump[number]
                if number == 1 and harness.first_append_barrier is not None:
                    started, release = harness.first_append_barrier
                    started.set()  # The worker owns the connection before readers proceed.
                    assert release.wait(5), "first append was never released"
                elif number in harness.sleep:
                    time.sleep(harness.sleep[number])
                else:
                    fed = harness.fed
                    deadline = time.monotonic() + harness.hold
                    while harness.fed == fed and time.monotonic() < deadline:
                        time.sleep(0.002)
                    if harness.fed != fed:
                        harness.overlapped += 1
                return append(items, **options)
            finally:
                harness.busy = False
                harness.released_bytes += size

        def checked(method, name):
            def call(*args, **kwargs):
                if threading.get_ident() == harness.main:
                    harness.check(name)
                    if name == "renew":
                        harness.main_renews += 1
                    return method(*args, **kwargs)
                harness.busy = True
                if name == "renew":
                    harness.worker_renews += 1
                try:
                    return method(*args, **kwargs)
                finally:
                    harness.busy = False

            return call

        monkeypatch.setattr(log, "append", held_append)
        monkeypatch.setattr(log, "renew", checked(renew, "renew"))
        monkeypatch.setattr(log, "guard", checked(guard, "guard"))

    def measure(self, items):
        return sum(i.table.nbytes for i in items if isinstance(i, f2.Chunk))

    def emitted(self, items):
        """Count a spool emission into the loop iteration that made it and the payload
        held (pending plus in flight). Feed, poll and the boundary close all emit."""
        size = self.measure(items)
        self.fed_bytes += size
        self.iteration_bytes += size
        self.max_iteration_bytes = max(self.max_iteration_bytes, self.iteration_bytes)
        self.peak_bytes = max(self.peak_bytes, self.fed_bytes - self.released_bytes)

    def _wrap_spool(self, monkeypatch):
        harness, cls = self, self.spool_class
        init, feed = cls.__init__, cls.feed
        self.original_feed = feed

        def record_init(spool, *args, **kwargs):
            harness.spool_args, harness.spool_kwargs = args, kwargs
            if harness.spool is None:
                harness.spool = spool
            init(spool, *args, **kwargs)

        if cls is Spool2:
            for name in ("poll", "close"):

                def emitting(spool, *args, _method=getattr(cls, name), **kwargs):
                    harness.depth += 1
                    try:
                        items = _method(spool, *args, **kwargs)
                    finally:
                        harness.depth -= 1
                    if spool is harness.spool and harness.depth == 0:
                        if _method.__name__ == "close":
                            harness.iteration_bytes = 0  # after the loop: its own step
                        harness.emitted(items)
                    return items

                monkeypatch.setattr(cls, name, emitting)

        def recorded_feed(spool, payload):
            if harness.crash_feed_when_busy and harness.busy:
                harness.crash_feed_when_busy = False
                harness.fed += 1  # releases a held append
                if harness.after_fail:
                    harness.after_fail()
                raise RuntimeError("main thread failed with an append in flight")
            harness.payloads.append(bytes(payload))
            harness.depth += 1
            try:
                items = feed(spool, payload)
            finally:
                harness.depth -= 1
            harness.fed += 1
            harness.emitted(items)
            return items

        monkeypatch.setattr(cls, "__init__", record_init)
        monkeypatch.setattr(cls, "feed", recorded_feed)

    def _wrap_replication(self, monkeypatch):
        harness = self
        send, read = ReplicationStream._send_status, ReplicationStream.read

        def recorded_send(stream, reply=False):
            harness.statuses.append((stream.acked, harness.durable.checkpoint(), harness.busy))
            return send(stream, reply)

        def idle_read(stream, timeout=0.0):
            harness.iteration_bytes = 0  # each loop iteration starts with one read
            if time.monotonic() < harness.quiet_until:
                time.sleep(timeout)  # no WAL arriving
                return None
            return read(stream, timeout)

        monkeypatch.setattr(ReplicationStream, "_send_status", recorded_send)
        monkeypatch.setattr(ReplicationStream, "read", idle_read)

    def assert_invariants(self):
        assert self.violations == [], "main-thread destination use during an upload"
        for value, durable, _ in self.statuses:
            assert value <= durable, "a status exceeded the durable checkpoint"
        previous = None
        for value, _, busy in self.statuses:
            if busy and previous is not None:
                assert value == previous, "a status value increased during an upload"
            previous = value

    def memory_bound(self, config):
        """At most two batches of chunk payload: one in flight, one pending. A batch is cut
        only after the iteration that made it full, so it can overshoot batch_bytes by
        that iteration's emissions (feed plus poll, per iteration, not per chunk).
        Format 2 holds that with a budget at least each chunk's Arrow nbytes; v1's bound
        is unchanged. Not covered: the chunk Spool2 is still building (wire chunk_bytes
        plus one row) and the descriptor cache."""
        if self.spool_class is Spool2:
            assert self.max_iteration_bytes > 0
            return 2 * (config.batch_bytes + self.max_iteration_bytes)
        return 2 * (config.batch_bytes + config.chunk_bytes)

    def respooled(self):
        """The chunks a fresh spool makes of the payloads this run consumed (format 2:
        then closed at the drained boundary, as run_once does)."""
        cls = self.spool_class
        fresh = cls.__new__(cls)
        cls.__init__(fresh, *self.spool_args, **self.spool_kwargs)
        items = []
        for payload in self.payloads:
            items += self.original_feed(fresh, payload)
        if cls is Spool2:
            items += fresh.close()
        return {(c.id, c.digest, c.chain_digest) for c in items if isinstance(c, CHUNKS)}


def is_v2(log):
    return isinstance(log, IngestLog2)


def stored_chunks(log):
    table = "chunks_v2" if is_v2(log) else "batches"
    return set(log.execute(f"SELECT id,digest,chain_digest FROM {{s}}{table}").fetchall())


def replication_binding(config):
    """The format-2 binding exactly as run_once verifies it: every output setting by
    ReplicationStream.show on an actual replication session (the same class and
    connection options run_once uses), the extension inventory, and the server major.
    Nothing is assumed."""
    stream = ReplicationStream(connection_options(config))
    try:
        settings = tuple((name, stream.show(name)) for name in f2.OUTPUT_SETTINGS)
        major = int(stream.show("server_version_num")) // 10000
    finally:
        stream.conn.finish()  # no copy was started, so a plain disconnect
    regular = connect(config)
    try:
        extensions = tuple(
            tuple(r)
            for r in query(regular, "SELECT extname,extversion FROM pg_extension ORDER BY extname")
        )
    finally:
        regular.close()
    return f2.Binding(config.source_id, settings, extensions), major


def setup_v2(stores, config, max_commits=10):
    """The same source registration as format 1's setup(), on a fresh format-2 store.
    Units close only on max_commits (a long max_delay), so a fresh Spool2 over the same
    payloads reproduces the run's grouping exactly (respooled). A huge max_commits
    closes a unit only at a run's heartbeat marker or its end."""
    info = check(config, create_objects=True)
    binding, major = replication_binding(config)
    config2 = IngestConfig2(
        **config.__dict__,
        text_binding=binding,
        server_major=major,
        unit_config=f2.UnitConfig(
            max_commits=max_commits, max_delay_ms=600_000, chunk_bytes=config.chunk_bytes
        ),
    )
    log = IngestLog2(stores[0].con, config2)
    log.initialize()
    log.register(info.identity, info.initial_lsn, info.tables)
    return config2, log


def prepared(stores, pg_phase1, monkeypatch, fmt="v2", max_commits=10, **config):  # noqa: F811
    source, base = pg_phase1
    config = replace(base, **config)
    config, log = setup_v2(stores, config, max_commits)
    durable = IngestLog2(stores[1].con, config)
    return source, config, log, Harness(monkeypatch, log, durable, fmt=fmt)


def frame_days(log):
    return [d for (d,) in log.execute("SELECT DISTINCT partition_day FROM {s}chunks_v2").fetchall()]


def stored_once_any(log):
    """Every chunk, frame/event and transaction stored exactly once, in either format."""
    if not is_v2(log):
        stored_once(log)
        return
    union = " UNION ALL ".join(
        f"SELECT source_epoch,unit_start_lsn,frame_ordinal FROM {{s}}frames_v2_{day}"
        for day in frame_days(log)
    )
    frames, distinct = log.execute(
        "SELECT count(*),count(DISTINCT (source_epoch,unit_start_lsn,frame_ordinal)) FROM ("
        + union
        + ")"
    ).fetchone()
    assert frames == distinct, "a frame was stored twice"
    (counted,) = log.execute("SELECT sum(n) FROM {s}chunks_v2").fetchone()
    assert counted == frames, "a chunk row doesn't match the stored frames"
    incomplete = log.execute("SELECT count(*) FROM {s}units_v2 WHERE NOT complete").fetchone()[0]
    assert incomplete == 0, "an incomplete unit is left after a drained run"


def stored_ids(log, source):
    ((oid,),) = query(source.con, "SELECT %s::regclass::oid", [source.schema + ".items"])
    if is_v2(log):
        union = " UNION ALL ".join(f"SELECT * FROM {{s}}frames_v2_{day}" for day in frame_days(log))
        # `id` is the relation's first column: the first cell of the new image.
        stored = log.execute(
            "SELECT (new['values'][1])::INTEGER AS id,count(*) FROM ("
            + union
            + ") WHERE record_kind=? AND op='insert' AND relation_id=? GROUP BY 1 ORDER BY 1",
            [f2.EVENT, int(oid)],
        ).fetchall()
    else:
        days = [
            d for (d,) in log.execute("SELECT DISTINCT partition_day FROM {s}batches").fetchall()
        ]
        union = " UNION ALL ".join(f"SELECT * FROM {{s}}changes_{day}" for day in days)
        stored = log.execute(
            "SELECT (new_key::JSON->'id'->>'value')::INTEGER AS id,count(*) FROM ("
            + union
            + ") WHERE op='insert' AND relation_id=? GROUP BY 1 ORDER BY 1",
            [int(oid)],
        ).fetchall()
    expected = [(i, 1) for (i,) in query(source.con, "SELECT id FROM items ORDER BY id")]
    return stored, expected


@pytest.mark.postgres
def test_the_next_batch_spools_while_an_append_is_in_flight(stores, pg_phase1, monkeypatch, fmt):  # noqa: F811
    """The overlap happens, the stored chunks equal a fresh spool of the same payloads,
    and memory holds at most two batches."""
    source, config, log, h = prepared(stores, pg_phase1, monkeypatch, fmt)
    small_transactions(source, 300)
    result = run_once(config, log)
    assert result.status == "drained" and result.transactions >= 300
    assert h.appends >= 3 and h.overlapped >= 2, (h.appends, h.overlapped)
    h.assert_invariants()
    stored = stored_chunks(log)
    assert stored and stored == h.respooled()
    stored_once_any(log)
    stored, expected = stored_ids(log, source)
    assert stored == expected
    assert h.fed_bytes == h.released_bytes > 0  # every emitted chunk counted once, then appended
    assert h.peak_bytes <= h.memory_bound(config), (h.peak_bytes, h.max_iteration_bytes)
    assert result.boundary == h.durable.checkpoint()


@pytest.mark.postgres
def test_a_drained_run_knows_the_commit_time_at_its_checkpoint(stores, pg_phase1, monkeypatch):  # noqa: F811
    """'Current as of' (ops_ingest): the run's last durable unit's last COMMIT frame, in
    memory, paired with the checkpoint it ends at; the same value the stored frames hold."""
    source, config, log, _ = prepared(stores, pg_phase1, monkeypatch)
    small_transactions(source, 20)
    observed = {}
    result = run_once(config, log, observe=observed)
    assert result.status == "drained"
    epoch, lsn, ts = observed["durable_through"]
    assert (epoch, lsn) == (observed["epoch"], result.boundary)
    frames = " UNION ALL ".join(f"SELECT * FROM {{s}}frames_v2_{d}" for d in frame_days(log))
    (stored,) = log.execute(
        f"SELECT f.commit_ts_us FROM ({frames}) f JOIN {{s}}units_v2 u "
        "ON u.epoch=f.source_epoch AND u.unit_start_lsn=f.unit_start_lsn "
        "WHERE u.complete AND u.end_lsn=? AND f.record_kind=2 "
        "ORDER BY f.frame_ordinal DESC LIMIT 1",
        [lsn],
    ).fetchone()
    assert ts == stored
    # The source clock: the heartbeat this run wrote committed moments ago.
    now_us = int(time.time() * 1_000_000) - 946_684_800_000_000
    assert 0 <= now_us - ts < 300_000_000


@pytest.mark.postgres
@pytest.mark.parametrize(
    "stage", ["feed", "after_append", "before_feedback", "after_payload", "after_commit"]
)
def test_a_crash_with_the_next_batch_pending_replays_once(
    stores,
    pg_phase1,  # noqa: F811
    monkeypatch,
    stage,
    fmt,
):
    """A failure while batch N+1 is pending never acknowledges past the durable
    checkpoint, and the next run stores every event exactly once."""
    source, config, log, h = prepared(stores, pg_phase1, monkeypatch, fmt)
    small_transactions(source, 200)
    seen = []

    def crash(name):
        if name == stage:
            seen.append(name)
            if len(seen) == 2:
                raise RuntimeError(f"crash at {name}")

    observe = {}
    h.crash_feed_when_busy = stage == "feed"
    with pytest.raises(RuntimeError, match="crash|main thread failed"):
        run_once(config, log, fault=None if stage == "feed" else crash, observe=observe)
    h.assert_invariants()
    # The status shows the checkpoint the run durably reached. An append that raised
    # after its commit never returned its boundary (as before the pipeline).
    if stage == "after_commit":
        assert observe["checkpoint"] <= h.durable.checkpoint()
    else:
        assert observe["checkpoint"] == h.durable.checkpoint()
    assert log.execute("SELECT holder FROM {s}source").fetchone() == (None,)
    conftest.wait_slot_released(source.con, config.slot)
    result = run_once(config, log)
    assert result.status == "drained"
    h.assert_invariants()
    stored_once_any(log)
    stored, expected = stored_ids(log, source)
    assert stored == expected


@pytest.mark.postgres
def test_a_long_append_neither_renews_nor_fences_until_collected(
    stores,
    pg_phase1,  # noqa: F811
    monkeypatch,
    fmt,
):
    """An append longer than lease/3: while it runs, statuses repeat and nothing renews
    on the main thread; its collect fences from before its guard, so the next status
    increase renews first (on the main thread, nothing in flight)."""
    source, config, log, h = prepared(stores, pg_phase1, monkeypatch, fmt, lease_seconds=30)
    small_transactions(source, 100)
    h.jump = {2: config.lease_seconds / 3 + 1}  # the second append "takes" 11 s
    h.sleep = {2: 0.3}
    submitted = []

    def quiet_from_the_second_submit(stage):
        # From that submit (on this thread, before the next read), no WAL arrives until
        # just after the collect, so nothing is pending then: the periodic renew check
        # runs with nothing in flight.
        if stage == "before_append":
            submitted.append(stage)
            if len(submitted) == 2:
                h.quiet_until = time.monotonic() + 0.6

    result = run_once(config, log, fault=quiet_from_the_second_submit)
    assert result.status == "drained"
    h.assert_invariants()
    assert any(busy for _, _, busy in h.statuses), "no status was sent during an upload"
    # Exactly one renew follows: the main thread's, when the collect's fence (from
    # before the guard) has lapsed. The periodic renew sees it and doesn't repeat it.
    assert h.main_renews - h.renews_at_jump == 1, "the collect did not fence from before the guard"
    assert h.worker_renews == h.worker_renews_at_jump, "a second renew followed the first"


@pytest.mark.postgres
def test_the_loop_waits_rather_than_spins_while_an_append_is_in_flight(
    stores,
    pg_phase1,  # noqa: F811
    monkeypatch,
    fmt,
):
    """With no WAL arriving during a held append, reads wait (bounded), never spin."""
    from cdc_flight.ingest_backend import Backend2

    source, config, log, h = prepared(stores, pg_phase1, monkeypatch, fmt, batch_bytes=1024 * 1024)
    small_transactions(source, 50)
    backend = Backend2(log)
    started, release = threading.Event(), threading.Event()
    h.first_append_barrier = started, release
    submitted, jumps = False, 0
    pending_items = pending_bytes = pending_units = 0
    marker_before_observations = False
    real_reads_after_release = 0
    observation_deadline = None
    observations = []
    read, emitted = ReplicationStream.read, h.emitted

    def record_emission(items):
        nonlocal jumps, pending_items, pending_bytes, pending_units, marker_before_observations
        emitted(items)
        if items and jumps == 0:
            # Submit the first real emission, leaving pre-marker WAL for pending work.
            h.clock.offset += config.flush_seconds + 0.001
            jumps += 1
        if submitted and len(observations) < 5:
            pending_items += len(items)
            for item in items:
                size, units = backend.budget(item)
                pending_bytes += size
                pending_units += units
        if len(observations) < 5:
            marker_before_observations |= backend.drained(items)

    def arm_first_submit(stage):
        nonlocal submitted, observation_deadline
        if stage == "before_append" and not submitted:
            submitted = True
            observation_deadline = time.monotonic() + 5

    def controlled_idle_read(stream, timeout=0.0):
        nonlocal real_reads_after_release
        if not submitted or len(observations) == 5:
            if release.is_set():
                real_reads_after_release += 1
            return read(stream, timeout)
        try:
            assert started.wait(5), "first append never started"
            assert time.monotonic() < observation_deadline, "no pending WAL after submit"
            if not pending_items:
                return read(stream, timeout)  # Reach real pending work before observing idle reads.
            h.iteration_bytes = 0
            observations.append(
                (timeout, h.busy, h.appends, pending_items, pending_bytes, pending_units)
            )
            time.sleep(timeout)  # Simulate no WAL arriving during the requested wait.
            return None
        except BaseException:
            release.set()
            raise
        finally:
            if len(observations) == 5:
                release.set()

    monkeypatch.setattr(h, "emitted", record_emission)
    monkeypatch.setattr(ReplicationStream, "read", controlled_idle_read)
    try:
        result = run_once(config, log, fault=arm_first_submit)
    finally:
        release.set()
    assert result.status == "drained"
    h.assert_invariants()
    assert jumps == 1 and not marker_before_observations
    assert real_reads_after_release > 0
    assert len(observations) == 5, observations
    for timeout, busy, appends, items, size, units in observations:
        assert 0 < timeout <= min(0.02, config.status_seconds), observations
        assert busy and appends == 1, observations
        assert items > 0 and 0 < size < config.batch_bytes, observations
        assert 0 < units < config.batch_events, observations
    stored_once_any(log)
    stored, expected = stored_ids(log, source)
    assert stored == expected and len(expected) == 50


@pytest.mark.postgres
def test_a_main_thread_failure_bounds_the_wait_for_the_append(
    stores,
    pg_phase1,  # noqa: F811
    monkeypatch,
    caplog,
    fmt,
):
    """A main-thread failure past the deadline interrupts the append in flight, and the
    lease is still released."""
    source, config, log, h = prepared(stores, pg_phase1, monkeypatch, fmt)
    small_transactions(source, 50)
    h.sleep = {1: 1.0}
    h.crash_feed_when_busy = True

    def past_the_deadline():
        h.clock.offset += config.runtime_seconds + 120

    h.after_fail = past_the_deadline
    with caplog.at_level(logging.ERROR, logger="cdc_flight.ingest"):
        with pytest.raises(RuntimeError, match="main thread failed"):
            run_once(config, log)
    assert h.interrupts >= 1, "the bounded wait did not interrupt the append"
    assert "destination write interrupted at the deadline" in caplog.text
    assert log.execute("SELECT holder FROM {s}source").fetchone() == (None,)
    assert h.violations == []


@pytest.mark.postgres
def test_a_capped_run_closes_its_short_unit_at_the_boundary(
    stores,
    private_pg_database,
    pg_phase1,  # noqa: F811
    monkeypatch,
    fmt,
):
    """A run that ends at its runtime, not at the marker: commits made after the marker
    (once the stream has started) form a short open unit that only close_at_boundary
    can close. They are stored exactly once and nothing is left partial. (The marker's
    own commit closes its unit, so every drained case closes before this path.)

    Its own database: the run reads until its cap, so on PostgreSQL 13/14 another test's
    transaction (an empty member here) could be mid-stream at the cap, and the run would
    rightly retain it as a partial transaction instead of closing the short unit."""
    source, config, log, h = prepared(stores, pg_phase1, monkeypatch, fmt, runtime_seconds=3)
    assert config.dsn == private_pg_database
    small_transactions(source, 20)  # 2 full units of 10 commits

    def after_the_marker(stage):
        if stage == "after_start":  # the marker was written before the stream started
            small_transactions(source, 5, start=21)  # the short tail, after the marker

    from cdc_flight.ingest_backend import Backend2

    backend = Backend2
    monkeypatch.setattr(backend, "drained", lambda self, items: False)  # never drains
    result = run_once(config, log, fault=after_the_marker)
    assert result.status == "capped", result
    assert not result.partial_transaction
    h.assert_invariants()
    stored_once_any(log)
    stored, expected = stored_ids(log, source)
    assert stored == expected and len(expected) == 25
    assert result.boundary == h.durable.checkpoint()


def test_an_open_unit_between_source_commits_is_partial():
    """Format 2's partial state includes an open unit holding complete source commits
    that hasn't closed yet, even when no source transaction is active (design: the
    v2 partial includes the open unit between source commits)."""
    import pgstream
    from test_format2_protocol import binding
    from test_format2_protocol import config as f2_config

    from cdc_flight.ingest_backend import Backend2

    spool = Spool2(
        f2_config(),
        {101: ("id",)},
        900,
        "m",
        binding=binding(),
        start_lsn=90,
        unit_config=f2.UnitConfig(max_commits=10),
        clock=lambda: 0,
    )
    for wire in (
        pgstream.relation(101, "public", "items", "f", ((1, "id", 25, -1),)),
        pgstream.begin(100, 1, 7),
        pgstream.insert(101, ("1",)),
        pgstream.commit(100, 101, 2),
    ):
        spool.feed(wire)
    assert not spool.active and spool.frame_count > 0  # between commits, unit open
    assert Backend2(None).partial(spool) is True


def _descriptor_bytes(item):
    return sum(
        len(d.json.encode()) + sum(len(k.encode()) for k in d.keys) for d in item.descriptors
    )


def wire_budget(backend, item):
    """Format 2's budget before the Arrow fix: protocol wire bytes."""
    if isinstance(item, f2.Chunk):
        return item.bytes + _descriptor_bytes(item), item.count
    return 256, 1


def arrow_budget(backend, item):
    """The fixed budget: at least the chunk's Arrow buffers."""
    if isinstance(item, f2.Chunk):
        return max(item.bytes, item.table.nbytes) + _descriptor_bytes(item), item.count
    return 256, 1


def durable_state(log, end):
    """Everything durable about the units that end at or before `end`: unit rows, chunk
    rows (but the clock's partition day), frames and descriptors, in order."""
    units = log.execute(
        "SELECT * FROM {s}units_v2 WHERE end_lsn<=? ORDER BY unit_start_lsn", [end]
    ).fetchall()
    starts = [u[1] for u in units]
    chunks = log.execute(
        "SELECT * EXCLUDE (partition_day) FROM {s}chunks_v2 WHERE list_contains(?,unit_start_lsn) "
        "ORDER BY unit_start_lsn,first_frame",
        [starts],
    ).fetchall()
    union = " UNION ALL ".join(f"SELECT * FROM {{s}}frames_v2_{day}" for day in frame_days(log))
    frames = log.execute(
        f"SELECT * FROM ({union}) WHERE list_contains(?,unit_start_lsn) "
        "ORDER BY unit_start_lsn,frame_ordinal",
        [starts],
    ).fetchall()
    descriptors = log.execute("SELECT * FROM {s}descriptors_v2 ORDER BY ALL").fetchall()
    return dict(units=units, chunks=chunks, frames=frames, descriptors=descriptors)


@pytest.mark.postgres
def test_the_arrow_budget_moves_batch_cuts_only(stores, pg_phase1, tmp_path, monkeypatch):  # noqa: F811
    """The same WAL ingested once with the old (wire) budget and once with the
    Arrow budget leaves identical durable state: unit rows, chunk ids, digests, chain
    digests and wire `bytes`, frames and descriptors. Only the batch count and the batch
    cuts may differ, and they must, or the comparison would be vacuous.

    Two slots on the one publication, created back to back before the workload, see the
    same commits; both stores register at the later slot's position (the earlier slot
    replays from behind, the normal crash case).

    Waived: the source row, the checkpoint and the open tail. Each arm sees the
    other's marker after the workload, so those legitimately differ; only units that end
    inside the workload are compared.

    Required: in each arm, at least one batch cut falls inside an open unit (the
    chunks either side of it share a unit_start_lsn), so the durable-prefix path of a
    unit with no terminal yet is exercised, not only cuts between units. At
    chunk_bytes=2048 a 10-commit unit spans two chunks; at 8192 every cut falls between
    units and this fails."""
    from cdc_flight.ingest_backend import Backend2
    from cdc_flight.store import Store

    if stores[0].remote:
        pytest.skip("local: two full ingests of one workload")
    source, base = pg_phase1
    other = "cdc_p0_t3_" + base.slot[-8:]
    con = duckdb.connect(str(tmp_path / f"{other}.duckdb"))
    con.execute(f"ATTACH '{tmp_path / (other + '_mirror.duckdb')}' AS {other}_mirror")
    con.execute("SET TimeZone='UTC'")
    Store(con, other, other + "_mirror").initialize()
    second = replace(base, slot=base.slot + "_t3", database=other)
    try:
        info = check(base, create_objects=True)
        info_b = check(second, create_objects=True)
        anchor = max(info.initial_lsn, info_b.initial_lsn)
        logs, configs = [], []
        for config, con_ in ((base, stores[0].con), (second, con)):
            binding, major = replication_binding(config)
            config2 = IngestConfig2(
                **config.__dict__,
                text_binding=binding,
                server_major=major,
                unit_config=f2.UnitConfig(
                    max_commits=10, max_delay_ms=600_000, chunk_bytes=config.chunk_bytes
                ),
            )
            log = IngestLog2(con_, config2)
            log.initialize()
            log.register(info.identity, anchor, info.tables)
            logs.append(log)
            configs.append(config2)
        small_transactions(source, 200)
        ((end,),) = query(source.con, "SELECT pg_current_wal_lsn()-'0/0'::pg_lsn")
        cuts = []
        for budget, config, log in zip((wire_budget, arrow_budget), configs, logs):
            monkeypatch.setattr(Backend2, "budget", budget)
            batches, append = [], log.append

            def recorded(items, _batches=batches, _append=append, **options):
                _batches.append(
                    tuple((i.id, i.unit_start_lsn) for i in items if isinstance(i, f2.Chunk))
                )
                return _append(items, **options)

            monkeypatch.setattr(log, "append", recorded)
            result = run_once(config, log)
            assert result.status == "drained" and result.transactions >= 200
            cuts.append(batches)
        old, new = (durable_state(log, int(end)) for log in logs)
        assert len(old["units"]) >= 15 and old["frames"], old["units"]
        for part in ("units", "chunks", "frames", "descriptors"):
            assert old[part] == new[part], f"the budget changed durable {part}"
        # Same chunks, cut into batches differently (the only permitted difference).
        assert [c for b in cuts[0] for c in b][: len(old["chunks"])] == [
            c for b in cuts[1] for c in b
        ][: len(old["chunks"])]
        assert cuts[0] != cuts[1], "both budgets cut the same batches: the comparison is vacuous"
        for arm, batches in zip(("wire", "arrow"), cuts):
            sequence = [(n, unit) for n, batch in enumerate(batches) for _, unit in batch]
            inside = [a for a, b in zip(sequence, sequence[1:]) if a[0] != b[0] and a[1] == b[1]]
            assert inside, f"the {arm} arm never cut a batch inside an open unit"
    finally:
        con.close()
        conftest.wait_slot_released(source.con, second.slot)
        query(
            source.con,
            "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name=%s",
            [second.slot],
        )
