"""Format 2 cdc_ops writer fields: add-only, created and written only by a
format-2 report. v1 reports keep their tables, statements and values unchanged."""

import json
from types import SimpleNamespace

import pytest
from test_compaction import Statements
from test_compaction import config as compact_config
from test_expiry import vars_of
from test_ops import ingest_config, observed, ops_db, row  # noqa: F401 (fixture)
from test_phase2 import Harness

from cdc_flight import ops_apply, ops_compact, ops_ingest
from cdc_flight.apply import TableResult
from cdc_flight.backfill import Backfill
from cdc_flight.config import BackfillConfig
from cdc_flight.ops import Ops

POSITION = dict(
    format_version=2,
    confirmed_end=500,
    open_unit_anchor=500,
    partial=True,
    open_chunks=2,
    durable_frame_end=7,
    config_revision=3,
)


def v2_harness(stores, tmp_path):
    """The (format-2) Harness store; kept as a name for the tests below."""
    h = Harness(stores, tmp_path)
    h.store.con.execute(
        f"ALTER TABLE {h.log.prefix}tables ADD COLUMN IF NOT EXISTS pending_keys_v2 VARCHAR"
    )
    return h


def columns(con, table):
    return {r[0] for r in con.execute(f"DESCRIBE cdc_ops.main.{table}").fetchall()}


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_format2_ingest_fields_reach_status_and_history(stores, tmp_path, ops_db):  # noqa: F811
    h = v2_harness(stores, tmp_path)
    # A cdc_ops from before format 2: the v1 table shapes, created as the old writers did.
    Ops(ops_db, h.config.database, h.config.source_id)._schema(ops_ingest.TABLES)
    ops_db.execute(
        "INSERT INTO cdc_ops.main.ingest_runs (source_id,run_id,as_of,status) "
        "VALUES (?, 'r', 100, 'drained')",
        [h.config.source_id],
    )
    assert "last_conflict_run_id" not in columns(ops_db, "ingest_status")
    seen = observed(run_id="v2", as_of=200, **POSITION)
    assert ops_ingest.report_ingest(ops_db, ingest_config(h), seen) == "written"
    status = row(ops_db, "ingest_status")[0]
    runs = {r["run_id"]: r for r in row(ops_db, "ingest_runs")}
    # Upgraded in place by Ops._schema (ADD COLUMN IF NOT EXISTS), no manual SQL.
    assert {"last_conflict_at", "last_conflict_run_id", *POSITION} <= columns(
        ops_db, "ingest_status"
    )
    for got in (status, runs["v2"]):
        assert {k: got[k] for k in POSITION} == POSITION
    # The older v1 row is NULL in the added columns: NULL format_version means v1.
    assert runs["r"]["format_version"] is None and runs["r"]["durable_frame_end"] is None
    # A newer v1 report replaces the status part, as any newer read does.
    ops_ingest.report_ingest(ops_db, ingest_config(h), observed(run_id="v1", as_of=300))
    assert row(ops_db, "ingest_status")[0]["run_id"] == "v1"


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_format2_apply_backfill_and_compaction_fields(stores, tmp_path, ops_db):  # noqa: F811
    h = v2_harness(stores, tmp_path)
    m = f"{h.apply.mirror}._cdc"
    # Stand in for a format-2 mirror (Apply2.ADDED_COLUMNS).
    h.store.con.execute(
        f"ALTER TABLE {m}.apply_state ADD COLUMN IF NOT EXISTS blocked_unit_start_lsn UBIGINT"
    )
    rel = h.apply.registered("public.third")[0]
    h.store.con.execute(
        f"UPDATE {m}.apply_state SET state='needs_action',blocked_commit_lsn=42,"
        "blocked_unit_start_lsn=40 WHERE relation_id=?",
        [rel],
    )
    results = [TableResult("public.items", "caught_up", 5, events=3, units=2)]
    assert ops_apply.report_apply(ops_db, h.config, "a2", results, format_version=2) == "written"
    third = [r for r in row(ops_db, "apply_status") if r["table_name"] == "third"][0]
    assert (third["format_version"], third["blocked_unit_start_lsn"]) == (2, 40)
    assert third["blocked_unit_end_lsn"] is None  # no complete unit 40 yet
    h.store.con.execute(
        f"INSERT INTO {h.log.prefix}units_v2 (epoch,unit_start_lsn,end_lsn,complete) "
        "VALUES (1,40,55,true)"
    )
    ops_apply.report_apply(ops_db, h.config, "a3", [], format_version=2)
    third = [r for r in row(ops_db, "apply_status") if r["table_name"] == "third"][0]
    assert (third["blocked_unit_start_lsn"], third["blocked_unit_end_lsn"]) == (40, 55)
    assert third["blocked_commit_lsn"] == 42  # the original commit, a diagnosis
    runs = {r["table_name"]: r for r in row(ops_db, "apply_runs")}
    assert (runs["public.items"]["units_applied"], runs["public.items"]["format_version"]) == (2, 2)
    assert runs[None]["units_applied"] is None and runs[None]["format_version"] == 2
    assert ops_apply.report_table(ops_db, h.config, "t", "public.third", format_version=2)
    Backfill(h.store.con, BackfillConfig(**vars_of(h.config))).initialize()
    assert ops_apply.report_backfill(ops_db, h.config, "b2", format_version=2, units=9) == "written"
    fold = {"sql": 3, "fallback:truncate": 1, "fallback:layouts": 2, "redo:ConversionException": 1}
    assert ops_apply.report_backfill(ops_db, h.config, "b3", format_version=2, fold=fold)
    back = {r["run_id"]: r for r in row(ops_db, "backfill_runs")}
    assert (back["b2"]["format_version"], back["b2"]["units_applied"]) == (2, 9)
    # Catch-up batches by path: NULL when no Backfill was built, else counted.
    counts = ("sql_batches", "fallback_batches", "redo_batches")
    assert tuple(back["b2"][c] for c in counts) == (None, None, None)
    assert tuple(back["b3"][c] for c in counts) == (3, 3, 1)
    result = SimpleNamespace(
        run_id="c2",
        status="done",
        error=None,
        days=["20260101"],
        batches=5,
        manifests=6,
        kept_day=None,
        kept_reason=None,
        budget=None,
        expired=0,
        would_expire=0,
        skipped=0,
        format_version=2,
        chunks=11,
        units=4,
    )
    assert ops_compact.report_compaction(ops_db, compact_config(h), result, 100, None) == "written"
    status = row(ops_db, "compaction_status")[0]
    assert (status["chunks_pruned"], status["units_pruned"], status["format_version"]) == (11, 4, 2)
    # Legacy counters are never relabelled: NULL on a format-2 run.
    assert status["batches_pruned"] is None and status["manifests_pruned"] is None


@pytest.mark.parametrize("prune", [False, True], ids=["steady", "prune_hour"])
@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_format2_reports_cost_the_same_statements(
    stores,
    tmp_path,
    ops_db,  # noqa: F811
    monkeypatch,
    prune,
):
    # The prune hour is 00:00-01:00 UTC by the wall clock: pin it, both ways, so the
    # counts don't depend on when the test runs.
    monkeypatch.setattr(ops_ingest, "prune_window", lambda: prune)
    monkeypatch.setattr(ops_apply, "prune_window", lambda: prune)
    h = v2_harness(stores, tmp_path)
    h.store.con.execute(
        f"ALTER TABLE {h.apply.mirror}._cdc.apply_state "
        "ADD COLUMN IF NOT EXISTS blocked_unit_start_lsn UBIGINT"
    )
    seen = observed(**POSITION)
    ops_ingest.report_ingest(ops_db, ingest_config(h), seen)
    ops_apply.report_apply(ops_db, h.config, "r", [], format_version=2)
    con = Statements(ops_db)
    ops_ingest.report_ingest(con, ingest_config(h), dict(seen, run_id="2", as_of=101))
    assert len([s for s in con.seen if "duckdb_" not in s]) == 2 + prune  # + history prune
    con = Statements(ops_db)
    results = [TableResult("public.items", "caught_up", 5, events=1, units=1)]
    ops_apply.report_apply(con, h.config, "r2", results, format_version=2)
    # + the history prune and the stale not_started prune
    assert len([s for s in con.seen if "duckdb_" not in s]) == 2 + 2 * prune


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_a_conflict_run_is_leased_not_busy_and_not_a_success(stores, tmp_path, ops_db):  # noqa: F811
    """A v2 run that kept durable progress, then stopped on a compaction conflict:
    its checkpoint and history are recorded (busy would drop both); it is not a
    success, and repeated conflicts are visible."""
    h = v2_harness(stores, tmp_path)
    ok = observed(run_id="ok", as_of=100, checkpoint=400, **POSITION)
    ops_ingest.report_ingest(ops_db, ingest_config(h), ok)
    hit = observed(run_id="c1", status="conflict", as_of=200, checkpoint=500, **POSITION)
    assert ops_ingest.report_ingest(ops_db, ingest_config(h), hit) == "written"
    status = row(ops_db, "ingest_status")[0]
    assert (status["status"], status["checkpoint_lsn"]) == ("conflict", 500)
    assert status["last_success_run_id"] == "ok" and status["last_busy_run_id"] is None
    assert status["last_conflict_run_id"] == "c1" and status["last_conflict_at"] is not None
    assert {r["run_id"] for r in row(ops_db, "ingest_runs")} == {"ok", "c1"}
    # The conflict report reports its own (older) lease time: a later run's status,
    # written first, survives it.
    later = observed(run_id="later", as_of=250, checkpoint=600, **POSITION)
    ops_ingest.report_ingest(ops_db, ingest_config(h), later)
    late_conflict = observed(run_id="c0", status="conflict", as_of=150, checkpoint=450, **POSITION)
    ops_ingest.report_ingest(ops_db, ingest_config(h), late_conflict)
    kept = row(ops_db, "ingest_status")[0]
    assert (kept["run_id"], kept["checkpoint_lsn"]) == ("later", 600)
    # A later success keeps the last conflict visible.
    ops_ingest.report_ingest(ops_db, ingest_config(h), dict(ok, run_id="ok2", as_of=300))
    # Like last_busy_*, the marker is the last REPORTED conflict (c0 reported last).
    assert row(ops_db, "ingest_status")[0]["last_conflict_run_id"] == "c0"


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_a_pending_key_change_is_listed_in_holds_without_a_held_reason(stores, tmp_path, ops_db):  # noqa: F811
    """A queued key change stalls apply at its frozen cut while the
    table reads live; format 2's holds JSON names it. It never sets held_reason."""
    h = v2_harness(stores, tmp_path)
    rel = h.apply.registered("public.items")[0]
    queue = '[{"epoch":1,"keys":["v"],"cut_lsn":40,"observed_lsn":55}]'
    h.store.con.execute(
        f"UPDATE {h.log.prefix}tables SET pending_keys_v2=? WHERE relation_id=?", [queue, rel]
    )
    ops_ingest.report_ingest(ops_db, ingest_config(h), observed(**POSITION))
    holds = json.loads(row(ops_db, "ingest_status")[0]["holds"])
    pending = [x for x in holds if x["relation_id"] == rel]
    assert pending == [
        {
            "v": 1,
            "relation_id": rel,
            "table": "public.items",
            "reason": "key_change_pending",
            "detail": None,  # one item shape for both kinds (UNION ALL of structs)
            "held_since": None,
            "pending_count": 1,
            "frozen_cut": 40,
            "first_observed_lsn": 55,
        }
    ]
    held = h.store.con.execute(
        f"SELECT held_reason FROM {h.log.prefix}tables WHERE relation_id=?", [rel]
    ).fetchone()[0]
    assert held is None
    # After activation the queue is empty: the item is gone.
    h.store.con.execute(f"UPDATE {h.log.prefix}tables SET pending_keys_v2=NULL")
    ops_ingest.report_ingest(ops_db, ingest_config(h), observed(as_of=200, **POSITION))
    holds = json.loads(row(ops_db, "ingest_status")[0]["holds"])
    assert not [x for x in holds if x.get("reason") == "key_change_pending"]


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_initialize_v2_creates_every_ops_table_with_v2_columns_and_no_rows(
    stores,
    tmp_path,
    ops_db,  # noqa: F811
):
    from cdc_flight.ops_init import TABLES_V2, OpsUnavailable, initialize_v2

    h = v2_harness(stores, tmp_path)
    created = initialize_v2(ops_db, h.config.database, h.config.source_id)
    assert created == sorted(TABLES_V2)
    for table in created:
        assert ops_db.execute(f"SELECT count(*) FROM cdc_ops.main.{table}").fetchone()[0] == 0
    assert {"format_version", "last_conflict_run_id", "durable_frame_end"} <= columns(
        ops_db, "ingest_status"
    )
    assert {"chunks_pruned", "units_pruned"} <= columns(ops_db, "compaction_status")
    assert "blocked_unit_start_lsn" in columns(ops_db, "apply_status")
    assert initialize_v2(ops_db, h.config.database, h.config.source_id) == created  # idempotent
    # The writers then report into the pre-created tables unchanged.
    seen = observed(**POSITION)
    assert ops_ingest.report_ingest(ops_db, ingest_config(h), seen) == "written"
    # Without cdc_ops attached (a local store), it refuses rather than pretending.
    ops_db.execute("DETACH cdc_ops")
    with pytest.raises(OpsUnavailable):
        initialize_v2(ops_db, h.config.database, h.config.source_id)


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_cleanup_release_count_is_history_only_and_heals_an_old_schema(stores, tmp_path, ops_db):  # noqa: F811
    h = v2_harness(stores, tmp_path)
    ddl = ops_ingest.TABLES_V2["ingest_runs"].replace(",cleanup_release_attempts INTEGER", "")
    ops_db.execute(f"CREATE TABLE cdc_ops.main.ingest_runs ({ddl},future_note VARCHAR)")
    ops_db.execute(
        "INSERT INTO cdc_ops.main.ingest_runs (source_id,run_id,future_note) "
        "VALUES ('legacy','old','preserve')"
    )
    assert "cleanup_release_attempts" not in columns(ops_db, "ingest_runs")
    for i, count in enumerate((None, 0, 2, 4)):
        assert (
            ops_ingest.report_ingest(
                ops_db,
                ingest_config(h),
                observed(
                    run_id=f"count-{i}",
                    as_of=100 + i,
                    status="failed" if count == 4 else "drained",
                    error="GuardedIngestConflict" if count == 4 else None,
                    cleanup_release_attempts=count,
                ),
            )
            == "written"
        )
    runs = {r["run_id"]: r for r in row(ops_db, "ingest_runs")}
    assert runs["old"]["future_note"] == "preserve"
    assert runs["old"]["cleanup_release_attempts"] is None
    assert [runs[f"count-{i}"]["cleanup_release_attempts"] for i in range(4)] == [None, 0, 2, 4]
    assert "cleanup_release_attempts" not in columns(ops_db, "ingest_status")
    # Existing busy policy omits history even if a no-progress conflict cleaned up.
    assert (
        ops_ingest.report_ingest(
            ops_db,
            ingest_config(h),
            observed(run_id="busy", status="busy", cleanup_release_attempts=2),
        )
        == "written"
    )
    assert len(row(ops_db, "ingest_runs")) == len(runs)
