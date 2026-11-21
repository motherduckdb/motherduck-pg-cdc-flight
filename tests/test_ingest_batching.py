"""Ingest batches by frames and bytes, not by item count (small transactions share an
append's fixed round trips), and a retry is indifferent to how the items were grouped:
a partially durable batch replayed with another composition stores each chunk, frame
and unit exactly once. Format 2 only: setup() is the production bootstrap.
"""

from dataclasses import replace

import pytest
from test_phase1 import pg_phase1  # noqa: F401 (fixture)

from cdc_flight.bootstrap import bootstrap_log
from cdc_flight.format2 import EVENT
from cdc_flight.ingest import run_once
from cdc_flight.ingest_backend import Backend2
from cdc_flight.preflight import query


def setup(stores, config):
    """A genuine format-2 source through the production factory: the binding observed on
    a real replication session, the actual server major, then registration at the
    slot's position. Callers use log.config (the recorded configuration)."""
    log, _ = bootstrap_log(stores[0].con, config, create_objects=True)
    return log


def appends(config, log, **options):
    """Run once, counting appends (the before_append stage)."""
    seen = []
    fault = options.pop("fault", None)

    def count(stage):
        if stage == "before_append":
            seen.append(1)
        if fault:
            fault(stage)

    result = run_once(config, log, fault=count, **options)
    return result, len(seen)


@pytest.fixture
def recorded_appends(monkeypatch):
    def record(log):
        batches = []
        append = log.append

        def observe(items, **options):
            # The units run_once's batch_events cap counts: the backend's own budget.
            batches.append(
                dict(
                    units=sum(_budget(item)[1] for item in items),
                    # The last item's units: a batch is cut at the first item that reaches
                    # the cap, so it may overshoot by that item (a whole chunk's frames).
                    last=_budget(items[-1])[1] if items else 0,
                    payload=any(_payload(item) for item in items),
                )
            )
            return append(items, **options)

        monkeypatch.setattr(log, "append", observe)
        return batches

    return record


def _budget(item):
    return Backend2.budget(None, item)


def _payload(item):
    """A chunk carrying a row change, not only the controls of empty commits."""
    return getattr(item, "event_count", 0) > 0


def unpublished_commits(source, count):
    source.execute("CREATE TABLE unpublished_noise (id integer)")
    # Real separate commits without thousands of client round trips/fsyncs.
    # LOCAL resets at each COMMIT; the following published synchronous commit
    # flushes this noise before replication starts reading it.
    source.execute(
        "DO $$ BEGIN FOR i IN 1..%s LOOP SET LOCAL synchronous_commit=off; "
        "INSERT INTO unpublished_noise VALUES (1); COMMIT; END LOOP; END $$",
        [count],
    )


def frame_days(log):
    return [d for (d,) in log.execute("SELECT DISTINCT partition_day FROM {s}chunks_v2").fetchall()]


def stored_once(log):
    """Every chunk, frame and unit is stored exactly once, and no unit is left open.
    Returns the number of stored frames."""
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
    return frames


def stored_events(log, source):
    """The row-change frames of the source's items table (no heartbeat or controls)."""
    ((oid,),) = query(source.con, "SELECT %s::regclass::oid", [source.schema + ".items"])
    union = " UNION ALL ".join(
        f"SELECT record_kind,relation_id FROM {{s}}frames_v2_{day}" for day in frame_days(log)
    )
    return log.execute(
        f"SELECT count(*) FROM ({union}) WHERE record_kind=? AND relation_id=?", [EVENT, oid]
    ).fetchone()[0]


def small_transactions(source, count, start=1):
    for i in range(start, start + count):
        source.execute("INSERT INTO items VALUES (%s,'v','n')", [i])


@pytest.mark.postgres
def test_small_transactions_share_appends(stores, pg_phase1, recorded_appends, pg_version):  # noqa: F811
    source, config = pg_phase1
    # Only the caps or the drain flush; the fixture's 8 KiB byte cap would flush first.
    config = replace(config, flush_seconds=60, batch_bytes=50 * 1024 * 1024)
    log = setup(stores, config)
    config = log.config  # the recorded format-2 configuration
    batches = recorded_appends(log)
    # PG13/14 also admit unrelated empty commits. Force that larger input here;
    # PG15+ still has only the published transactions and one append.
    unpublished_commits(source, 4000)
    # 600 transactions are 1,200 items: more than the old 1,000-item cap allowed.
    small_transactions(source, 600)
    result, count = appends(config, log)
    assert result.status == "drained" and result.transactions >= 600
    assert count == len(batches)
    units = sum(batch["units"] for batch in batches)
    if pg_version < 150000:
        assert units > config.batch_events  # the 4,000 empty commits were decoded
    else:
        assert units <= config.batch_events
    if units <= config.batch_events:
        assert count == 1
    else:
        assert count >= 2
        # Every batch but the last is cut at the first item reaching the cap: below the
        # cap without its last item, at or above it with it (format 2: an item is a chunk
        # of several frames, so the overshoot is that chunk, not one unit).
        assert all(
            batch["units"] - batch["last"] < config.batch_events <= batch["units"]
            for batch in batches[:-1]
        ), batches
        # ...and the overshoot is bounded: an item is at most one chunk's frames.
        assert all(batch["last"] <= config.unit_config.chunk_frames for batch in batches), batches
    batches.clear()
    small_transactions(source, 300, start=601)
    result, count = appends(replace(config, batch_events=100), log)
    assert count >= 5  # 300 events and 300 commits, at 100 units per batch
    stored_once(log)
    assert stored_events(log, source) == 900


@pytest.mark.postgres
def test_timing_lines_describe_each_append_and_pass_every_stage_on(stores, pg_phase1):  # noqa: F811
    """The log-only timing lines (explain.ingest_stages sets `fault.timing`): one per
    collected append, in order, and one for the run; frames and bytes add up. The
    stage marks they take still reach the run's fault, and the data is as without them."""
    source, config = pg_phase1
    config = replace(config, flush_seconds=60, batch_bytes=50 * 1024 * 1024)
    log = setup(stores, config)
    config = replace(log.config, batch_events=100)
    small_transactions(source, 300)
    lines, stages = [], []

    class Fault:
        def __call__(self, stage):
            stages.append(stage)

        def timing(self, kind, **fields):
            lines.append((kind, fields))

    result = run_once(config, log, fault=Fault())
    assert result.status == "drained"
    appended = [fields for kind, fields in lines if kind == "append"]
    (run,) = [fields for kind, fields in lines if kind == "run"]
    assert [kind for kind, _ in lines][-1] == "run"
    assert len(appended) == stages.count("before_append") >= 5
    assert stages.count("before_payload") == stages.count("after_commit") == len(appended)
    assert [f["n"] for f in appended] == list(range(1, len(appended) + 1))
    assert {f["why"] for f in appended} <= {"full", "drained", "end", "time"}
    assert "full" in {f["why"] for f in appended}
    for f in appended:
        assert f["items"] > 0 and f["frames"] >= 0 and f["bytes"] >= 0
        for name in ("fill", "fill_cpu", "blocked", "queue", "wall", "cpu", "wait", "lag"):
            assert f[name] >= 0, (name, f)
        phases = [f[name] for name in ("guard", "payload", "ledger", "commit")]
        assert all(p is not None and p >= 0 for p in phases), f
        assert sum(phases) <= f["wall"], f
    assert run["appends"] == len(appended)
    assert run["frames"] == sum(f["frames"] for f in appended) > 0
    assert run["bytes"] == appended[-1]["cum_bytes"] == sum(f["bytes"] for f in appended)
    assert run["wall"] > 0 and run["cpu_s"] >= 0 and run["cpus"] >= 1
    stored_once(log)
    assert stored_events(log, source) == 300


@pytest.mark.postgres
def test_the_byte_cap_still_flushes_first(stores, pg_phase1):  # noqa: F811
    source, config = pg_phase1
    config = replace(config, flush_seconds=60, chunk_bytes=1024, batch_bytes=4096)
    log = setup(stores, config)
    config = log.config  # the recorded format-2 configuration
    for i in range(1, 41):
        source.execute("INSERT INTO items VALUES (%s,%s,'n')", [i, "x" * 600])
    result, count = appends(config, log)
    assert result.status == "drained" and count >= 4
    stored_once(log)


@pytest.mark.postgres
def test_empty_transactions_are_bounded_by_the_cap(stores, pg_phase1):  # noqa: F811
    """PostgreSQL 13 and 14 send a BEGIN/COMMIT for every transaction, even one that
    touched no published table: chunkless commits, which the unit count includes."""
    source, config = pg_phase1
    if int(query(source.con, "SHOW server_version_num")[0][0]) >= 150000:
        pytest.skip("PostgreSQL 15+ skips empty transactions in pgoutput")
    config = replace(config, flush_seconds=60, batch_events=10)
    log = setup(stores, config)
    config = log.config  # the recorded format-2 configuration
    source.execute("CREATE TABLE unpublished (id int)")
    for i in range(50):
        source.execute("INSERT INTO unpublished VALUES (%s)", [i])
    result, count = appends(config, log)
    assert result.status == "drained" and count >= 4


def durable(position):
    """The durable source position, ordered: the last complete unit END, then the open
    unit's anchor and its durable frame prefix."""
    return (
        position["checkpoint"],
        position["open_unit_anchor"] or 0,
        position["durable_frame_end"] or 0,
    )


def crash_at(stage, nth, batches):
    seen = []

    def fault(at):
        if at == stage and batches[-1]["payload"]:
            seen.append(1)
            if len(seen) == nth:
                raise RuntimeError(f"crash at {stage} #{nth}")

    return fault


@pytest.mark.postgres
@pytest.mark.parametrize(
    "stores,stage",
    [("local", "after_commit"), ("local", "before_commit"), ("motherduck", "before_commit")],
    indirect=["stores"],
    ids=["local-after_commit", "local-before_commit", "motherduck-before_commit"],
)
def test_a_partial_batch_replayed_with_another_composition_stores_once(
    stores,
    pg_phase1,  # noqa: F811
    stage,
    recorded_appends,
    pg_version,
):
    """Two payload batches durable or the second rolled back (before_commit),
    then a rerun that groups the same items differently."""
    source, config = pg_phase1
    config = replace(config, flush_seconds=60, batch_events=50)
    log = setup(stores, config)
    config = log.config  # the recorded format-2 configuration
    batches = recorded_appends(log)
    unpublished_commits(source, 100)
    small_transactions(source, 120)
    positions = []

    def fault(at):
        # The durable position after the first payload append committed.
        if at == "after_commit" and batches[-1]["payload"] and not positions:
            positions.append(log.unit_position())
        crash(at)

    crash = crash_at(stage, 2, batches)
    with pytest.raises(RuntimeError, match="crash"):
        run_once(config, log, fault=fault)
    payload_appends = sum(batch["payload"] for batch in batches)
    assert payload_appends == 2
    if pg_version < 150000:
        assert len(batches) > payload_appends
    else:
        assert len(batches) == payload_appends
    # The second payload append is durable (after_commit) or rolled back (before_commit):
    # the durable position moved past the first append's, or is exactly the first's.
    first, now = positions[0], log.unit_position()
    if stage == "after_commit":
        assert durable(now) > durable(first), (first, now)  # strictly forward
    else:
        assert now == first
    result = run_once(replace(config, batch_events=1000), log)
    assert result.status == "drained"
    stored_once(log)
    assert stored_events(log, source) == 120


@pytest.mark.postgres
def test_a_flush_inside_a_multi_chunk_transaction_replays_once(
    stores,
    pg_phase1,  # noqa: F811
    recorded_appends,
    pg_version,
):
    """The cap splits one large transaction across appends (the open transaction spans
    batches); a crash after the first, then a replay that groups it differently."""
    source, config = pg_phase1
    config = replace(config, flush_seconds=60, chunk_bytes=1024, batch_events=8)
    log = setup(stores, config)
    config = log.config  # the recorded format-2 configuration
    batches = recorded_appends(log)
    unpublished_commits(source, 8)
    source.execute("BEGIN")
    for i in range(1, 61):
        source.execute("INSERT INTO items VALUES (%s,%s,'n')", [i, "y" * 200])
    source.execute("COMMIT")
    with pytest.raises(RuntimeError, match="crash"):
        run_once(config, log, fault=crash_at("after_commit", 1, batches))
    # (Format 1 asserted the first batch's composition per server version: PG13/14's
    # empty commits filled it alone. In format 2 their COMMIT frames share a unit and a
    # chunk with the transaction's events, so that composition claim retires; crash_at
    # only fires after an append that carries payload.)
    assert any(batch["payload"] for batch in batches)
    # The first part of the transaction is durable, its unit still open.
    assert log.unit_position()["partial"]
    result = run_once(replace(config, batch_events=25), log)
    assert result.status == "drained"
    stored_once(log)
    assert stored_events(log, source) == 60
