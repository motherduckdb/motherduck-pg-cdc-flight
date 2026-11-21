"""'Current as of' on cdc_ops (ingest_status/ingest_runs, apply_status/apply_runs).

The time is the source commit time of the last commit of the unit ending at a checkpoint,
stored with the (epoch, LSN) it belongs to. It is exact or NULL: shown only while that
pair equals the row's current position, so an older writer (a rollback, a Flight not yet
redeployed) that moves the position without it never leaves a stale time reading as
current. 'Caught up' is (epoch, applied_lsn) = (epoch, checkpoint_lsn); the time is for
display. Driven through the real Spool, IngestLog2.append and both ApplyRun folds.
"""

import datetime

import pgstream
import pytest
from test_sql_fold_e2e import HEARTBEAT, OID, OTHER_TABLE, TABLE, Side, text_layout

from cdc_flight import apply, ops, ops_apply, ops_ingest, sql_fold
from cdc_flight.format2 import Chunk, UnitComplete
from cdc_flight.ingest_backend import Backend2, last_commit_ts
from cdc_flight.ops import Ops

U = pgstream.UNCHANGED
# 2026-11-10 00:00:00 UTC as PostgreSQL's commit timestamp (microseconds since 2000-01-01).
KNOWN = datetime.datetime(2026, 11, 10, tzinfo=datetime.timezone.utc)
T0 = int(KNOWN.timestamp()) * 1_000_000 - ops.PG_EPOCH_US


def test_one_pg_epoch_constant():
    assert ops.PG_EPOCH_US == apply.PG_EPOCH_US == sql_fold.PG_EPOCH_US == 946_684_800_000_000
    with pytest.raises(TypeError):
        ops.commit_time_sql(1.5)


class Source:
    """One store, one fold, a local cdc_ops; one commit per unit (each commit ends one)."""

    def __init__(self, tmp_path, fold):
        self.side = Side(tmp_path, fold[0], fold, text_layout("d"), max_commits=1, apply_options={})
        self.con = self.side.con
        self.con.execute(f"ATTACH '{tmp_path / 'cdc_ops.duckdb'}' AS cdc_ops")
        self.lsn, self.runs = 100, 0

    def commit(self, ts, *messages):
        lsn = self.lsn
        self.side.feed([pgstream.begin(lsn, ts, 7), *messages, pgstream.commit(lsn, lsn + 5, ts)])
        self.lsn += 10
        return lsn + 5  # the unit's end

    def heartbeat(self, ts):
        return self.commit(ts, pgstream.insert(HEARTBEAT, ("src", f"m{ts}")))

    def apply(self, *, times=True):
        self.runs += 1
        run_id = f"a{self.runs}"
        results = self.side.apply.run(run_id=run_id)
        written = ops_apply.report_apply(
            self.con,
            self.side.config,
            run_id,
            results,
            database="cdc_ops",
            times=self.side.apply.reader.commit_times if times else None,
        )
        assert written == "written"
        return {r.table: r for r in results}

    def ingest_report(self, run_id, as_of, through, checkpoint=None):
        observed = dict(
            run_id=run_id,
            status="drained",
            as_of=as_of,
            epoch=1,
            checkpoint=through[1] if checkpoint is None else checkpoint,
            durable_through=through,
        )
        observed["confirmed_end"] = observed["checkpoint"]
        config = self.side.log_config
        assert ops_ingest.report_ingest(self.con, config, observed) == "written"

    def status(self, table):
        """(applied_lsn, the time's pair, the time in PostgreSQL-epoch microseconds)."""
        schema, name = table.split(".")
        return self.con.execute(
            "SELECT applied_lsn,applied_through_epoch,applied_through_lsn,"
            f"epoch_us(applied_through_commit_ts)-{ops.PG_EPOCH_US} "
            "FROM cdc_ops.main.apply_status WHERE schema_name=? AND table_name=?",
            [schema, name],
        ).fetchone()

    def shown(self, table):
        """The Dive's rule: the time only while its pair is the row's position."""
        applied, epoch, lsn, ts = self.status(table)
        return ts if (epoch, lsn) == (1, applied) else None


FOLDS = ["sql", "python"]


@pytest.mark.parametrize("fold", FOLDS)
def test_an_idle_table_tracks_the_heartbeat_and_a_held_table_stops_at_its_unit(tmp_path, fold):
    s = Source(tmp_path, fold)
    s.commit(T0 + 1, pgstream.insert(OID, ("1", "x", "y")))
    h1 = s.heartbeat(T0 + 2)
    results = s.apply()
    # Both reach H: items applied its insert, others (no events) advanced through the units.
    for table in (TABLE, OTHER_TABLE):
        assert results[table].boundary == h1
        assert s.status(table) == (h1, 1, h1, T0 + 2), table
    exact = s.con.execute(
        "SELECT applied_through_commit_ts=TIMESTAMPTZ '2026-11-10 00:00:00.000002+00' "
        "FROM cdc_ops.main.apply_status WHERE table_name='others'"
    ).fetchone()[0]
    assert exact  # source clock, microsecond exact

    # An update of a key the mirror doesn't have holds items before that unit.
    held_unit_start = s.lsn - 5
    s.commit(T0 + 3, pgstream.update(OID, ("9", "new", U)))
    h2 = s.heartbeat(T0 + 4)
    results = s.apply()
    assert results[TABLE].status == "needs_action" and results[TABLE].boundary == held_unit_start
    assert results[OTHER_TABLE].boundary == h2
    assert s.shown(TABLE) == T0 + 2  # its time stops at the unit before the hold
    assert s.shown(OTHER_TABLE) == T0 + 4

    h3 = s.heartbeat(T0 + 5)
    s.apply()
    assert s.status(TABLE)[0] == held_unit_start and s.shown(TABLE) == T0 + 2  # still stopped
    assert s.status(OTHER_TABLE)[0] == h3 and s.shown(OTHER_TABLE) == T0 + 5

    # Nothing new: others takes apply's idle shortcut (reads no units) and keeps its time.
    results = s.apply()
    assert results[OTHER_TABLE].boundary == h3 and s.shown(OTHER_TABLE) == T0 + 5

    # Per-table run rows carry the time at their own applied_lsn; the run's row none.
    rows = s.con.execute(
        "SELECT table_name,applied_lsn,epoch_us(applied_through_commit_ts)-? "
        "FROM cdc_ops.main.apply_runs WHERE run_id='a3' ORDER BY table_name NULLS FIRST",
        [ops.PG_EPOCH_US],
    ).fetchall()
    assert rows == [(None, None, None), (TABLE, held_unit_start, T0 + 2), (OTHER_TABLE, h3, T0 + 5)]


@pytest.mark.parametrize("fold", FOLDS)
def test_caught_up_is_the_same_position_and_then_the_same_time(tmp_path, fold):
    s = Source(tmp_path, fold)
    h = s.heartbeat(T0 + 10)
    s.ingest_report("i1", 100, (1, h, T0 + 10))
    s.apply()
    status = s.con.execute(
        "SELECT epoch,checkpoint_lsn,durable_through_epoch,durable_through_lsn,"
        "epoch_us(durable_through_commit_ts) FROM cdc_ops.main.ingest_status"
    ).fetchone()
    assert status[:4] == (1, h, 1, h)
    caught_up = s.con.execute(
        "SELECT count(*) FROM cdc_ops.main.apply_status a JOIN cdc_ops.main.ingest_status i "
        "USING (source_id) WHERE a.epoch=i.epoch AND a.applied_lsn=i.checkpoint_lsn "
        "AND a.applied_through_commit_ts=i.durable_through_commit_ts"
    ).fetchone()[0]
    assert caught_up == 2


@pytest.mark.parametrize("fold", FOLDS)
def test_without_this_runs_unit_times_an_idle_table_matches_ingest_history(tmp_path, fold):
    """The first report after a deploy: apply's idle shortcut read no units and no time is
    stored yet. An exact (epoch, checkpoint) match in ingest_runs gives it; a table at a
    position no ingest run ended at (a hold mid-run) stays NULL until it advances."""
    s = Source(tmp_path, fold)
    s.commit(T0 + 1, pgstream.insert(OID, ("1", "x", "y")))
    s.commit(T0 + 2, pgstream.update(OID, ("9", "new", U)))  # holds items before it
    h = s.heartbeat(T0 + 3)
    s.apply(times=False)  # as if the times were lost
    assert s.status(OTHER_TABLE)[1:] == (None, None, None) and s.shown(TABLE) is None
    # Ingest's report lands after apply's (the chain triggers apply first): next apply.
    s.ingest_report("i1", 100, (1, h, T0 + 3))
    s.apply(times=False)
    assert s.shown(OTHER_TABLE) == T0 + 3
    assert s.shown(TABLE) is None  # held at 105, which no ingest run ended at


@pytest.mark.parametrize("fold", FOLDS)
def test_an_older_writer_moving_the_position_never_shows_a_stale_time(tmp_path, fold):
    s = Source(tmp_path, fold)
    h1 = s.heartbeat(T0 + 1)
    s.apply()
    assert s.shown(OTHER_TABLE) == T0 + 1
    h2 = s.heartbeat(T0 + 2)
    s.side.apply.run(run_id="old")
    # The pre-change writer's column list (v1's snapshot has none of the triple).
    ops_ = Ops(s.con, s.side.config.database, s.side.config.source_id, "cdc_ops")
    assert ops_.report(
        ops_apply.APPLY,
        lambda: ops_apply.apply_snapshot(ops_, s.side.config, "old", format_version=1),
    )
    applied, epoch, lsn, ts = s.status(OTHER_TABLE)
    assert (applied, lsn, ts) == (h2, h1, T0 + 1) and s.shown(OTHER_TABLE) is None

    # The same for ingest: an older upsert moves checkpoint_lsn and leaves the triple.
    s.ingest_report("i1", 100, (1, h1, T0 + 1))
    ops_.upsert(
        "ingest_status",
        dict(source_id=s.side.config.source_id, status_as_of=200, run_id="old", checkpoint_lsn=h2),
        {"status_as_of": ("run_id", "checkpoint_lsn")},
    )
    row = s.con.execute(
        "SELECT checkpoint_lsn,durable_through_lsn FROM cdc_ops.main.ingest_status"
    ).fetchone()
    assert row == (h2, h1)  # pair differs: the Dive shows no time


def test_ingest_writes_only_a_time_that_belongs_to_its_checkpoint(tmp_path):
    s = Source(tmp_path, "sql")
    h1, h2 = s.heartbeat(T0 + 1), s.heartbeat(T0 + 2)

    def status():
        return s.con.execute(
            "SELECT checkpoint_lsn,durable_through_epoch,durable_through_lsn,"
            "epoch_us(durable_through_commit_ts)-? FROM cdc_ops.main.ingest_status",
            [ops.PG_EPOCH_US],
        ).fetchone()

    s.ingest_report("i1", 100, (1, h1, T0 + 1))
    assert status() == (h1, 1, h1, T0 + 1)
    # A newer run that knows no time at an unchanged checkpoint keeps the valid triple.
    s.ingest_report("i2", 200, (1, h1, None))
    assert status() == (h1, 1, h1, T0 + 1)
    # A time for another position than the run's checkpoint is never written.
    s.ingest_report("i3", 300, (1, h1, T0 + 1), checkpoint=h2)
    assert status() == (h2, 1, h1, T0 + 1)  # stored triple kept: its pair no longer matches
    runs = s.con.execute(
        "SELECT run_id,epoch,confirmed_end,epoch_us(durable_through_commit_ts)-? "
        "FROM cdc_ops.main.ingest_runs ORDER BY run_id",
        [ops.PG_EPOCH_US],
    ).fetchall()
    assert runs == [("i1", 1, h1, T0 + 1), ("i2", 1, h1, None), ("i3", 1, h2, None)]
    # An older run's late report never replaces a newer time.
    s.ingest_report("i0", 50, (1, h2, T0 + 2))
    assert status()[1:] == (1, h1, T0 + 1)


def test_the_ingest_backend_takes_the_time_from_its_durable_chunks(tmp_path):
    s = Source(tmp_path, "sql")
    spool = s.side.spool
    items = []
    for lsn, ts in ((200, T0 + 7), (210, T0 + 8)):
        for wire in (
            pgstream.begin(lsn, ts, 7),
            pgstream.insert(HEARTBEAT, ("src", f"x{ts}")),
            pgstream.commit(lsn, lsn + 5, ts),
        ):
            items.extend(spool.feed(wire))
    chunks = [i for i in items if isinstance(i, Chunk)]
    completes = [i for i in items if isinstance(i, UnitComplete)]
    assert chunks and len(completes) == 2
    assert last_commit_ts(chunks[-1].table) == T0 + 8
    backend = Backend2(None)
    backend.durable_frames = {}
    for item in items:  # the order collect() counts a durable batch in
        backend.counts(item)
    assert backend.durable_through == (1, 215, T0 + 8)
    # A unit completed with no commit in this run's chunks: its time is unknown (None).
    backend.counts(completes[-1])
    assert backend.durable_through == (1, 215, None)
