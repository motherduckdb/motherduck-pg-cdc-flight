"""Apply2's run-scoped single-page frames cache.

Every table still runs its own units() (the retention floor checked at its checkpoint in
the same statement); tables planning the identical page share one frames and one
descriptors read. The cached split must equal per-table events(page, oid) exactly.

The cache belongs to the Python fold, which is now the per-batch fallback of the SQL fold
(apply_run.py): these tests construct ApplyRun with fold="python". The SQL path reads no
frames to the client; tests/test_sql_fold_apply.py covers its reads.
"""

import pgstream
import pytest
from test_apply2 import COLUMNS, HEARTBEAT, OID, OTHER, OTHER_TABLE, START, TABLE, ins
from test_apply2 import Env as _Env

from cdc_flight import apply_run
from cdc_flight.apply2 import Apply2
from cdc_flight.unit_store import UnitReader

TABLES = (TABLE, OTHER_TABLE)


def Env(stores, **options):  # noqa: N802 (the Python path's Env)
    return _Env(stores, fold="python", **options)


def oth(key, value):
    return pgstream.insert(OTHER, (key, value))


def workload(env, shape):
    """Both tables, a zero-event stretch for one of them, a multi-relation TRUNCATE,
    a mid-page schema change (a column added) and the heartbeat."""
    for k in range(4):
        if shape == "balanced" or k % 3 == 0:
            env.commit(ins(str(k), f"a{k}"), oth(str(k), f"b{k}"))
        else:
            env.commit(ins(str(k), f"a{k}"))
    env.commit(pgstream.insert(HEARTBEAT, ("hb", "m")))
    wider = pgstream.relation(OID, "public", "items", "f", COLUMNS + ((0, "w", 25, -1),))
    env.commit(wider, pgstream.insert(OID, ("9", "z", "w9")))
    env.commit(pgstream.truncate([OID, OTHER]))
    env.commit(pgstream.insert(OID, ("10", "x", "w10")))
    env.feed([], close=True)  # close the last unit: every commit above is complete


def current(env, table):
    return env.apply.head(table)["current"]


class Counting:
    """Counts the reader's statements by kind (units/frames/descriptors/head)."""

    def __init__(self, monkeypatch):
        self.seen = []
        real = UnitReader.execute

        def execute(reader, statement, parameters=None):
            self.seen.append(statement)
            return real(reader, statement, parameters)

        monkeypatch.setattr(UnitReader, "execute", execute)

    def count(self, marker):
        return sum(marker in s for s in self.seen)


FRAMES = "WITH physical AS MATERIALIZED"
DESCRIPTORS = "descriptors_v2 d WHERE epoch=?"
UNITS = "FROM {s}units_v2 u WHERE epoch=?"


@pytest.fixture(params=["balanced", "skewed"])
def shaped(request, stores):
    env = Env(stores, max_commits=3)
    for table in TABLES:
        env.apply.start_from_log(table)
    workload(env, request.param)
    return env


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_the_cached_split_equals_per_table_events_for_every_oid(shaped):
    env = shaped
    high = env.log.checkpoint()
    reader = UnitReader(env.store.con, env.config)
    for table, oid in ((TABLE, OID), (OTHER_TABLE, OTHER)):
        batch = env.apply._read_batch(oid, current(env, table), high, set())
        page = batch.page
        assert batch.events == reader.events(page, oid, high=high), table
        assert batch.descriptors == reader.descriptors(page, high=high)
    # Every listed oid of the TRUNCATE sees it, with its provenance; the heartbeat's
    # events are cached too (an oid no configured table reads).
    split = env.apply._cache[1]
    for oid in (OID, OTHER):
        truncates = [e for e in split[oid] if e["op"] == "truncate"]
        assert len(truncates) == 1 and truncates[0]["relation_id"] == oid
    assert split[HEARTBEAT] == reader.events(page, HEARTBEAT, high=high)
    assert env.apply.cache_stats == {"fills": 1, "hits": 1, "uncached": 0}


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_one_frames_and_one_descriptors_read_for_tables_at_the_same_position(shaped, monkeypatch):
    env = shaped
    reads = Counting(monkeypatch)
    results = env.apply.run(tables=TABLES)
    assert {r.status for r in results} <= {"caught_up", "needs_resync"}
    # T units() calls (each with its own floor check), one shared frames + descriptors
    # read per page group: here both tables plan the same pages.
    assert reads.count(FRAMES) == env.apply.cache_stats["fills"]
    assert reads.count(DESCRIPTORS) == env.apply.cache_stats["fills"]
    assert env.apply.cache_stats["hits"] >= 1 and env.apply._cache is None  # run-scoped


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_tables_at_different_positions_read_separately(stores):
    env = Env(stores, max_commits=2)
    for table in TABLES:
        env.apply.start_from_log(table)
    env.commit(ins("1", "a"), oth("1", "b"))
    env.commit(ins("2", "a"), oth("2", "b"))
    env.apply.apply_table(TABLE, "first")  # TABLE moves ahead
    env.commit(ins("3", "a"), oth("3", "b"))
    env.commit(ins("4", "a"), oth("4", "b"))
    env.apply._cache = None
    env.apply.cache_stats.update(fills=0, hits=0)
    high = env.log.checkpoint()
    env.apply._read_batch(OID, current(env, TABLE), high, set())
    env.apply._read_batch(OTHER, current(env, OTHER_TABLE), high, set())
    assert env.apply.cache_stats["fills"] == 2 and env.apply.cache_stats["hits"] == 0


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_a_different_high_never_shares_the_page(shaped):
    env = shaped
    reader = UnitReader(env.store.con, env.config)
    head = reader.head()
    page = reader.units(1, START, head.high)
    low_high = page[0].end
    env.apply._read_batch(OID, current(env, TABLE), head.high, set())
    batch = env.apply._read_batch(OTHER, current(env, OTHER_TABLE), low_high, set())
    assert batch.page[-1].end == low_high and env.apply.cache_stats["fills"] == 2
    assert batch.events == reader.events(batch.page, OTHER, high=low_high)


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_a_read_error_is_never_cached_each_table_raises_and_nothing_applies(shaped):
    env = shaped
    day = env.store.con.execute(
        f"SELECT DISTINCT partition_day FROM {env.log.prefix}chunks_v2"
    ).fetchone()[0]
    env.store.con.execute(
        f"DELETE FROM {env.log.prefix}frames_v2_{day} WHERE frame_ordinal=1 "
        "AND unit_start_lsn=(SELECT min(unit_start_lsn) FROM "
        f"{env.log.prefix}frames_v2_{day})"
    )
    for table in TABLES:
        with pytest.raises(ValueError, match="gap|duplicate|count"):
            env.apply.apply_table(table, "run")
        assert env.apply._cache is None
    assert env.mirror() == {} and env.mirror("others") == {}
    for oid in (OID, OTHER):
        assert env.state(oid)["applied"] == START


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_a_floor_advance_between_tables_holds_the_second_never_serves_the_cache(shaped):
    """A drop in the middle of a run: the second table's own units() sees the raised
    floor in the same statement and holds; the cached page is never served to it."""
    env = shaped
    head = UnitReader(env.store.con, env.config).head()
    env.apply._read_batch(OID, current(env, TABLE), head.high, set())
    assert env.apply._cache is not None
    env.log.execute("UPDATE {s}retention SET floor_epoch=1,floor_lsn=?", [START])
    result = env.apply.apply_table(OTHER_TABLE, "after drop")
    assert result.status == "needs_resync" and env.state(OTHER)["applied"] == START
    assert env.mirror("others") == {}


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_the_cache_holds_unfiltered_events_and_each_table_skips_its_own_quarantine(stores):
    """C1: only TABLE quarantined unit 2; OTHER_TABLE still applies it from the cache."""
    env = Env(stores, max_commits=2)
    for table in TABLES:
        env.apply.start_from_log(table)
    env.commit(ins("1", "a"), oth("1", "b"))
    env.commit(ins("2", "a"), oth("2", "b"))
    env.commit(ins("3", "a"), oth("3", "b"))
    env.commit(ins("4", "a"), oth("4", "b"))
    first, second = env.units()
    m = f"{env.apply.mirror}._cdc"
    generation = env.store.con.execute(
        f"SELECT generation_id FROM {m}.apply_state WHERE relation_id=?", [OID]
    ).fetchone()[0]
    env.store.con.execute(
        f"INSERT INTO {m}.quarantine (relation_id,generation_id,epoch,commit_lsn,reason,actor,"
        "run_id,created_at,unit_start_lsn,unit_end_lsn,unit_digest) "
        "VALUES (?,?,1,?,'t','t','q',current_timestamp,?,?,?)",
        [OID, generation, second[0] + 10, *second],
    )
    results = {r.table: r for r in env.apply.run(tables=TABLES)}
    assert results[TABLE].boundary == results[OTHER_TABLE].boundary == second[1]
    assert env.mirror() == {"1": "a", "2": "a"}  # unit 2 skipped for TABLE only
    assert env.mirror("others") == {"1": "b", "2": "b", "3": "b", "4": "b"}
    assert env.apply.cache_stats["hits"] >= 1


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_above_the_cap_each_table_reads_uncached_with_the_same_result(shaped, monkeypatch):
    env = shaped
    high = env.log.checkpoint()
    page_bytes = sum(u.bytes for u in UnitReader(env.store.con, env.config).units(1, START, high))
    monkeypatch.setattr(apply_run, "CACHE_CAP_BYTES", page_bytes - 1)  # just over the cap
    reader = UnitReader(env.store.con, env.config)
    for table, oid in ((TABLE, OID), (OTHER_TABLE, OTHER)):
        batch = env.apply._read_batch(oid, current(env, table), high, set())
        assert batch.events == reader.events(batch.page, oid, high=high)
    assert env.apply.cache_stats == {"fills": 0, "hits": 0, "uncached": 2}
    assert env.apply._cache is None


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_the_cache_is_cleared_when_a_run_ends_and_on_an_exception(shaped, monkeypatch):
    env = shaped
    high = env.log.checkpoint()
    env.apply._read_batch(OID, current(env, TABLE), high, set())
    assert env.apply._cache is not None
    real = Apply2._page_key

    def boom(*args, **kwargs):
        raise RuntimeError("reader failed")

    monkeypatch.setattr(Apply2, "_page_key", boom)
    with pytest.raises(RuntimeError):
        env.apply._read_batch(OTHER, current(env, OTHER_TABLE), high, set())
    assert env.apply._cache is None
    monkeypatch.setattr(Apply2, "_page_key", real)
    env.apply.run(tables=TABLES)
    assert env.apply._cache is None  # run-scoped, however the run ends


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_the_same_page_under_a_different_high_is_not_shared(stores):
    """Pages bounded by max_batch_events can be identical for two highs; the key still
    includes high, so a table planned under another high never reuses the entry."""
    env = Env(stores, max_commits=1, max_batch_events=1)  # one unit per page
    for table in TABLES:
        env.apply.start_from_log(table)
    for k in range(3):
        env.commit(ins(str(k), "a"), oth(str(k), "b"))
    ends = [u[1] for u in env.units()]
    first = env.apply._read_batch(OID, current(env, TABLE), ends[-1], set())
    second = env.apply._read_batch(OTHER, current(env, OTHER_TABLE), ends[-2], set())
    assert [u.end for u in first.page] == [u.end for u in second.page] == [ends[0]]
    assert env.apply.cache_stats["fills"] == 2 and env.apply.cache_stats["hits"] == 0


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_a_new_epoch_at_the_same_start_and_high_never_shares_the_page(stores):
    """A new epoch may restart at a lower (here the same) LSN, so two pages
    can share their first start and high yet hold other units. The key's per-unit
    epoch and digest keep a same-run read of epoch 2 from serving epoch 1's events."""
    from dataclasses import replace

    from cdc_flight.ingest_log2 import IngestLog2
    from cdc_flight.spool2 import Spool

    env = Env(stores, max_commits=1)
    for table in TABLES:
        env.apply.start_from_log(table)
    env.commit(ins("1", "a"), oth("1", "epoch-1"))
    high = env.log.checkpoint()
    first = env.apply._read_batch(OTHER, {"epoch": 1, "applied": START}, high, set())
    assert first.events  # epoch 1's row is cached

    env.log.acquire("operator")
    try:
        assert env.log.begin_epoch(2, START, "slot_lost", "slot_e2")
    finally:
        env.log.release()
    env.log_config = replace(env.log_config, epoch=2)
    env.log = IngestLog2(env.store.con, env.log_config)
    env.spool = Spool(
        env.log_config,
        {OID: ("id",), OTHER: ("id",), HEARTBEAT: ("id",)},
        900,
        "marker",
        binding=env.log_config.text_binding,
        start_lsn=START,
        unit_config=env.log_config.unit_config,
        clock=lambda: 0,
    )
    env.lsn = 100  # the same LSNs again, other content
    relations = [
        pgstream.relation(OID, "public", "items", "f", COLUMNS),
        pgstream.relation(OTHER, "public", "others", "f", COLUMNS),
    ]
    env.feed(relations)
    env.commit(ins("1", "a"), oth("1", "epoch-2"))
    assert env.log.checkpoint() == high

    second = env.apply._read_batch(OTHER, {"epoch": 2, "applied": START}, high, set())
    reader = UnitReader(env.store.con, env.config)
    assert [u.epoch for u in second.page] == [2] and second.page[0].start == first.page[0].start
    assert second.events == reader.events(second.page, OTHER, high=high)
    assert second.events != first.events
    assert env.apply.cache_stats["fills"] == 2 and env.apply.cache_stats["hits"] == 0
