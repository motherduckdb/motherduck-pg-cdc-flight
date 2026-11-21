"""The `cdc_ops` status writers. Local DuckDB (a local file attached as
`cdc_ops` stands in for the MotherDuck database) and PG16 where a real ingest run is
needed. The writers are best effort, never raise, and never reach for `md:` from a
local store.
"""

import json
import uuid
from dataclasses import replace
from types import SimpleNamespace

import pytest
from psycopg2.extensions import parse_dsn
from test_compaction import Statements, days_log
from test_compaction import config as compact_config
from test_expiry import vars_of
from test_phase1 import pg_phase1  # noqa: F401 (fixture)
from test_phase2 import Harness, harness_binding

from cdc_flight import ingest as ingest_module
from cdc_flight import ops_apply, ops_compact, ops_ingest
from cdc_flight.apply import TableResult
from cdc_flight.backfill import Backfill
from cdc_flight.bootstrap import bootstrap_log
from cdc_flight.compact import run_compaction
from cdc_flight.config import BackfillConfig, IngestConfig
from cdc_flight.ingest import run_once
from cdc_flight.ingest_log import IngestLog
from cdc_flight.ingest_log2 import IngestConfig2
from cdc_flight.ops import write_audit
from cdc_flight.preflight import check, query


@pytest.fixture
def ops_db(stores, tmp_path):
    """A local `cdc_ops` attached to the store's connection (and the other one)."""
    path = tmp_path / "cdc_ops.duckdb"
    for store in stores:
        store.con.execute(f"ATTACH IF NOT EXISTS '{path}' AS cdc_ops")
    return stores[0].con


def row(con, table, where="", params=()):
    """Rows as dicts; TIMESTAMPTZ columns as text (fetching them needs pytz)."""
    types = con.execute(f"DESCRIBE cdc_ops.main.{table}").fetchall()
    select = ",".join(
        f"{c}::VARCHAR AS {c}" if t.startswith("TIMESTAMP WITH") else c for c, t, *_ in types
    )
    cursor = con.execute(f"SELECT {select} FROM cdc_ops.main.{table} {where}", list(params))
    names = [d[0] for d in cursor.description]
    return [dict(zip(names, r)) for r in cursor.fetchall()]


def ingest_config(h):
    return h.ingest_config


def observed(**fields):
    base = dict(run_id="r", status="drained", as_of=100, epoch=1, checkpoint=10)
    base.update(fields)
    return base


# Ingest: ordering, busy and failure columns ----------------------------------------


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_newer_reads_win_and_busy_or_failed_runs_never_clobber(stores, tmp_path, ops_db):
    h = Harness(stores, tmp_path)
    config = ingest_config(h)
    slots = [dict(name="s", epoch=1, is_current=True, wal_status="reserved")]

    def report(**fields):
        assert ops_ingest.report_ingest(ops_db, config, observed(**fields)) == "written"
        return row(ops_db, "ingest_status")[0]

    first = report(run_id="a", as_of=200, slots=slots)
    assert first["status"] == "drained" and first["last_success_run_id"] == "a"
    # An older run's late report changes nothing it read.
    late = report(run_id="old", as_of=100, status="capped", slots=[])
    assert (late["run_id"], late["status"], json.loads(late["slots"])[0]["name"]) == (
        "a",
        "drained",
        "s",
    )
    # A busy run touches only last_busy_*.
    busy = report(run_id="busy", as_of=None, status="busy")
    assert busy["run_id"] == "a" and busy["last_busy_run_id"] == "busy"
    # Busy after taking the lease (the slot's reader still active): newer, still busy.
    busy = report(run_id="busy2", as_of=250, status="busy")
    assert (busy["run_id"], busy["status"]) == ("a", "drained")
    assert busy["last_busy_run_id"] == "busy2"
    # A failure under a lease records it; the last success is kept; a failure
    # before the slot read keeps the slots part (D2-1).
    failed = report(run_id="f", as_of=300, status="failed", error="SourceMismatch", step="lease")
    assert (failed["status"], failed["error"], failed["last_failure_error"]) == (
        "failed",
        "SourceMismatch",
        "SourceMismatch",
    )
    assert failed["last_success_run_id"] == "a" and json.loads(failed["slots"])[0]["name"] == "s"
    # The registry's holds are read in the statement: as fresh as the write.
    assert failed["holds_as_of"] > 0 and json.loads(failed["holds"]) == []


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_a_first_report_without_a_read_does_not_freeze_its_parts(stores, tmp_path, ops_db):
    """D4-1: the stored as_of is NULL after a failure before any read; a later good
    report must still replace those parts."""
    h = Harness(stores, tmp_path)
    config = ingest_config(h)
    ops_ingest.report_ingest(
        ops_db, config, dict(run_id="x", status="failed", error="OperationalError")
    )
    first = row(ops_db, "ingest_status")[0]
    assert first["status_as_of"] is None and first["last_failure_run_id"] == "x"
    ops_ingest.report_ingest(
        ops_db, config, observed(run_id="y", as_of=50, slots=[dict(name="s", is_current=True)])
    )
    second = row(ops_db, "ingest_status")[0]
    assert (second["status_as_of"], second["run_id"], second["slots_as_of"]) == (50, "y", 50)


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_the_writer_is_bounded_and_never_raises(stores, tmp_path, ops_db):
    h = Harness(stores, tmp_path)
    config = ingest_config(h)
    assert ops_ingest.report_ingest(ops_db, config, observed(), remaining=5) == "skipped:time"
    assert not ops_db.execute(
        "SELECT count(*) FROM duckdb_tables() WHERE database_name='cdc_ops'"
    ).fetchone()[0]
    ops_db.execute("DETACH cdc_ops")
    # A local store without cdc_ops: skipped, no md:.
    con = Statements(ops_db)
    assert ops_ingest.report_ingest(con, config, observed()) == "skipped:local"
    assert not [s for s in con.seen if "md:" in s]
    # A broken database: logged as its type, never raised.
    path = tmp_path / "broken.duckdb"
    ops_db.execute(f"ATTACH '{path}' AS cdc_ops")
    ops_db.execute("CREATE TABLE cdc_ops.main.ingest_status (source_id INTEGER)")
    outcome = ops_ingest.report_ingest(ops_db, config, observed())
    assert outcome.startswith("failed:")


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_a_motherduck_store_without_a_token_issues_no_md_statement(stores, tmp_path, monkeypatch):
    h = Harness(stores, tmp_path)
    monkeypatch.delenv("motherduck_token", raising=False)
    monkeypatch.delenv("MOTHERDUCK_TOKEN", raising=False)

    class MotherDuckStore(Statements):
        def execute(self, statement, parameters=None):
            if "SELECT type FROM duckdb_databases()" in statement:
                return self.con.execute("SELECT 'motherduck'")
            return super().execute(statement, parameters)

    con = MotherDuckStore(h.store.con)
    outcome = ops_ingest.report_ingest(con, ingest_config(h), observed())
    assert outcome == "failed:RuntimeError"
    assert not [s for s in con.seen if "md:" in s]


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_two_sources_write_the_same_tables_at_once(stores, tmp_path, ops_db):
    """O9: each source's write touches only its rows; two connections, interleaved."""
    h = Harness(stores, tmp_path)
    a = ingest_config(h)
    b = replace(a, source_id="other", text_binding=harness_binding("other"))
    IngestLog(h.store.con, b).initialize()  # the second source's change log
    other = h.other_store.con
    ops_ingest.report_ingest(ops_db, a, observed(run_id="setup", as_of=50))  # creates the tables
    ops_db.execute("BEGIN")
    assert ops_ingest.report_ingest(ops_db, a, observed(run_id="a1")) == "written"
    assert ops_ingest.report_ingest(other, b, observed(run_id="b1")) == "written"
    ops_db.execute("COMMIT")
    rows = {r["source_id"]: r["run_id"] for r in row(ops_db, "ingest_status")}
    assert rows == {a.source_id: "a1", "other": "b1"}


# Ingest from a real run (PG16) --------------------------------------------------------


@pytest.mark.parametrize("stores", ["local"], indirect=True)
@pytest.mark.postgres
def test_a_real_ingest_run_and_a_lost_slot_both_reach_cdc_ops(stores, pg_phase1, ops_db):  # noqa: F811
    source, config = pg_phase1
    # Each run appends once, at its drain, so the crash at the first append below sees a
    # complete unit. With a time or byte flush, other sessions' empty transactions (sent
    # by PostgreSQL 13) could make that first append an open unit's chunk alone.
    config = replace(config, flush_seconds=60, batch_bytes=50 * 1024 * 1024)
    log, info = bootstrap_log(stores[0].con, config, create_objects=True)
    config = log.config  # the IngestConfig2 recorded at bootstrap
    source.execute("INSERT INTO items VALUES (424242,'needle-value','note')")
    seen, before = {}, log.checkpoint()
    result = run_once(config, log, observe=seen)
    seen.update(status=result.status, transactions=result.transactions, chunks=result.chunks)
    assert ops_ingest.report_ingest(ops_db, config, seen) == "written"
    status = row(ops_db, "ingest_status")[0]
    assert seen["cleanup_release_attempts"] == 0  # finish already released the token
    assert row(ops_db, "ingest_runs")[0]["cleanup_release_attempts"] == 0
    assert status["status"] == "drained" and status["status_as_of"] == seen["as_of"]
    # The checkpoint shown is the one the run reached, not the one read with its lease.
    assert status["checkpoint_lsn"] == result.boundary > before
    slot = json.loads(status["slots"])[0]
    assert slot["name"] == config.slot and slot["is_current"] and slot["wal_status"]
    assert row(ops_db, "ingest_runs")[0]["run_id"] == result.run_id
    # No user data, and no DSN parts, anywhere in cdc_ops.
    dump = json.dumps([row(ops_db, t) for t in ("ingest_status", "ingest_runs")], default=str)
    parts = parse_dsn(config.dsn)
    for secret in (
        "needle-value",
        "424242",
        parts.get("host"),
        parts.get("user"),
        parts.get("password"),
        parts.get("dbname"),
    ):
        if secret and len(secret) > 3:
            assert secret not in dump, secret
    # A run that fails after a durable append shows the checkpoint that append reached.
    source.execute("INSERT INTO items VALUES (424243,'x','y')")
    seen, before = {}, log.checkpoint()

    def crash(stage):
        if stage == "after_append":
            raise RuntimeError("injected")

    with pytest.raises(RuntimeError):
        run_once(config, log, observe=seen, fault=crash)
    assert seen["checkpoint"] == log.checkpoint() > before
    # The slot is lost: the run fails in slot_health, and the Dive still sees it.
    query(source.con, "SELECT pg_drop_replication_slot(%s)", [config.slot])
    seen = {}
    with pytest.raises(Exception) as raised:
        run_once(config, log, observe=seen)
    seen.update(status="failed", error=type(raised.value).__name__, step="preflight")
    ingest_module.observe_after_failure(config, config.slot, seen)  # as the CLI does
    ops_ingest.report_ingest(ops_db, config, seen)
    status = row(ops_db, "ingest_status")[0]
    # Refused before the lease: the run's status part stays the last leased run's,
    # the failure columns and the slot (read now) show what happened.
    assert status["status"] == "drained" and status["last_failure_error"]
    current = [s for s in json.loads(status["slots"]) if s["is_current"]]
    assert current and current[0]["wal_status"] == "missing"
    query(source.con, "SELECT pg_create_logical_replication_slot(%s,'pgoutput')", [config.slot])


@pytest.mark.parametrize("stores", ["local"], indirect=True)
@pytest.mark.postgres
def test_pending_slots_are_matched_exactly(stores, pg_phase1):  # noqa: F811
    """O8/D3-2: `<base>_e<n>` is a pending slot; `<base>xe2` and `<base>_e2_old` aren't."""
    source, config = pg_phase1
    check(config, create_objects=True)
    names = [config.slot + "_e2", config.slot + "xe2", config.slot + "_e2_old"]
    try:
        for name in names:
            query(source.con, "SELECT pg_create_logical_replication_slot(%s,'pgoutput')", [name])
        slots = ingest_module.observed_slots(source.con, config.slot, config.slot, 1)
        assert {s["name"]: s["epoch"] for s in slots} == {config.slot: 1, names[0]: 2}
    finally:
        for name in names:
            query(
                source.con,
                "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
                "WHERE slot_name=%s",
                [name],
            )


# Apply, backfill, compaction ------------------------------------------------------------


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_apply_backfill_and_compaction_snapshots(stores, tmp_path, ops_db):
    h = Harness(stores, tmp_path)
    days_log(h, ["20260101", "20260102"])
    results = [TableResult("public.items", "caught_up", 5, events=1)]  # the real type
    assert ops_apply.report_apply(ops_db, h.config, "run1", results) == "written"
    tables = {r["table_name"]: r for r in row(ops_db, "apply_status")}
    assert set(tables) == {"items", "other", "third", "fourth"}
    assert tables["items"]["state"] == "live" and tables["items"]["pin_lsn"] is not None
    runs = {r["table_name"]: r for r in row(ops_db, "apply_runs")}
    assert runs["public.items"]["events"] == 1 and runs[None]["status"] == "done"
    assert runs["public.items"]["applied_lsn"] == 5  # TableResult.boundary
    # A held table's first-seen time is copied from apply_state.
    rel = h.apply.registered("public.third")[0]
    h.store.con.execute(
        f"UPDATE {h.apply.mirror}._cdc.apply_state SET state='needs_action',"
        "held_since=current_timestamp WHERE relation_id=?",
        [rel],
    )
    ops_apply.report_apply(ops_db, h.config, "run2", [])
    third = [r for r in row(ops_db, "apply_status") if r["table_name"] == "third"][0]
    assert third["state"] == "needs_action" and third["held_since"] is not None
    # Backfill: every generation of the mirror (the log-only starts here), as the
    # backfill Flight leaves the mirror after its initialize.
    Backfill(h.store.con, BackfillConfig(**vars_of(h.config))).initialize()
    assert ops_apply.report_backfill(ops_db, h.config, "b1") == "written"
    assert len(row(ops_db, "backfill_status")) == 4
    # A run that knows a generation's chunk estimate records it; a later run of the
    # same backfill, which doesn't, keeps it.
    generation = row(ops_db, "backfill_status")[0]["generation_id"]
    ops_apply.report_backfill(ops_db, h.config, "b2", totals={generation: 7})
    ops_apply.report_backfill(ops_db, h.config, "b3")
    estimate = [r for r in row(ops_db, "backfill_status") if r["generation_id"] == generation]
    assert estimate[0]["chunks_total"] == 7 and estimate[0]["run_id"] == "b3"
    # Compaction: the run, every pin's standing and the retained days.
    seen = {}
    result = run_compaction(compact_config(h), h.store.con, observe=seen)
    outcome = ops_compact.report_compaction(
        ops_db, compact_config(h), result, seen["as_of"], seen["summary"]
    )
    assert outcome == "written"
    status = row(ops_db, "compaction_status")[0]
    pins, days = json.loads(status["pins"]), json.loads(status["days"])
    assert len(pins) == 4 and all("expires_at_us" in p for p in pins)
    assert [d["day"] for d in days] == ["20260102"]
    # A busy compaction (no lease) leaves the run part.
    busy = SimpleNamespace(**{**vars(result), "run_id": "busy", "status": "busy"})
    ops_compact.report_compaction(ops_db, compact_config(h), busy, None, None)
    after = row(ops_db, "compaction_status")[0]
    assert after["run_id"] == result.run_id and after["last_busy_run_id"] == "busy"


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_the_audit_copy_is_best_effort(stores, tmp_path, ops_db):
    h = Harness(stores, tmp_path)
    args = (h.config.database, h.config.source_id, "run", "pause", {"table": "t"}, True, "ok")
    assert write_audit(ops_db, *args)
    audit = row(ops_db, "audit")[0]
    assert audit["action"] == "pause" and audit["token_user"] is not None
    assert audit["request_id"] is None
    assert write_audit(ops_db, *args, request_id="dive-7")
    assert [r["request_id"] for r in row(ops_db, "audit")] == [None, "dive-7"]
    ops_db.execute("DETACH cdc_ops")
    assert write_audit(ops_db, *args) is False  # skipped, never raised


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_statements_per_report(stores, tmp_path, ops_db):
    """The MotherDuck cost: statements each report issues once its tables exist
    (the ATTACH adds one on MotherDuck). Catalog reads run on the client."""
    h = Harness(stores, tmp_path)
    ops_ingest.report_ingest(ops_db, ingest_config(h), observed())
    ops_apply.report_apply(ops_db, h.config, "r", [])
    con = Statements(ops_db)
    ops_ingest.report_ingest(con, ingest_config(h), observed(run_id="2", as_of=101))
    remote = [s for s in con.seen if "duckdb_" not in s]
    assert len(remote) == 2 + ops_ingest.prune_window()  # upsert, history (+ prune)
    con = Statements(ops_db)
    results = [TableResult("public.items", "caught_up", 5, events=1)]  # the real type
    ops_apply.report_apply(con, h.config, "r2", results)
    remote = [s for s in con.seen if "duckdb_" not in s]
    assert len(remote) == 2 + 2 * ops_apply.prune_window()  # + history and stale not_started


@pytest.mark.md_gate
def test_two_sources_and_two_connections_write_cdc_ops_concurrently(stores):
    """The behaviour the Dive depends on, on the real server: two sources' ingest
    reports, on two connections at once, both land, and each is two statements once
    the tables exist (the ATTACH adds one in a Flight). The fixture's mirror database
    stands in for cdc_ops, so the fixture drops it."""
    import threading

    s, other = stores
    database = s.mirror_database.strip('"')
    configs = []
    for name in ("ops_a", "ops_b"):
        config = IngestConfig2(
            **IngestConfig(
                name, "slot", "pub", ("public.t",), database=s.database.strip('"'), allow_local=True
            ).__dict__,
            text_binding=harness_binding(name),
        )
        IngestLog(s.con, config).initialize()
        configs.append(config)
    for config in configs:  # tables exist before the race
        ops_ingest.report_ingest(
            s.con, config, observed(run_id="setup", as_of=1), database=database
        )
    outcomes = {}

    def write(con, config, run_id):
        outcomes[config.source_id] = ops_ingest.report_ingest(
            con, config, observed(run_id=run_id, as_of=10), database=database
        )

    threads = [
        threading.Thread(target=write, args=(con, config, "r-" + config.source_id))
        for con, config in ((s.con, configs[0]), (other.con, configs[1]))
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(30)
    assert outcomes == {"ops_a": "written", "ops_b": "written"}
    rows = s.con.execute(
        f'SELECT source_id,run_id FROM "{database}".main.ingest_status ORDER BY 1'
    ).fetchall()
    assert rows == [("ops_a", "r-ops_a"), ("ops_b", "r-ops_b")]
    con = Statements(s.con)
    ops_ingest.report_ingest(con, configs[0], observed(run_id="n", as_of=11), database=database)
    remote = [x for x in con.seen if "duckdb_" not in x]
    assert len(remote) == 2 + ops_ingest.prune_window()


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_failed_apply_and_backfill_runs_are_recorded(stores, tmp_path, ops_db):
    """A run that fails before any table still writes its own row (status,
    exception type, step), in the same INSERT, so a failing Flight never looks fresh."""
    h = Harness(stores, tmp_path)
    con = Statements(ops_db)
    outcome = ops_apply.report_apply(
        con, h.config, "run-x", None, status="failed", error="CatalogException", step="attach"
    )
    assert outcome == "written"
    runs = row(ops_db, "apply_runs")
    assert [(r["run_id"], r["table_name"], r["status"], r["error"], r["step"]) for r in runs] == [
        ("run-x", None, "failed", "CatalogException", "attach")
    ]
    Backfill(h.store.con, BackfillConfig(**vars_of(h.config))).initialize()
    ops_apply.report_backfill(
        ops_db, h.config, "run-b", table="public.items", status="failed", error="OperationalError"
    )
    runs = row(ops_db, "backfill_runs")
    assert [(r["run_id"], r["table_name"], r["status"], r["error"]) for r in runs] == [
        ("run-b", "public.items", "failed", "OperationalError")
    ]


# Writer additions: quarantined commits and expirations -------------------


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_apply_status_carries_the_current_generations_quarantines(stores, tmp_path, ops_db):
    from test_table_actions import caught_up_items, poisoned, quarantine

    h = Harness(stores, tmp_path)
    caught_up_items(h)
    poison = poisoned(h)
    assert quarantine(h, poison).status == "done"
    ops_apply.report_apply(ops_db, h.config, "run1", [])
    tables = {r["table_name"]: r for r in row(ops_db, "apply_status")}
    other, items = tables["other"], tables["items"]
    assert other["quarantine_count"] == 1
    (item,) = json.loads(other["quarantines"])
    assert item["commit_lsn"] == poison and item["epoch"] == 1 and item["v"] == 1
    assert "reason" not in item and item["run_id"]  # the audit row holds the reason
    assert items["quarantine_count"] is None and json.loads(items["quarantines"]) == []
    # Bounded across the source (not per table): the newest QUARANTINES listed, newest
    # first; every table's count stays exact; the JSON stays small.
    mirror, rel = h.apply.mirror, h.apply.registered("public.other")[0]
    h.store.con.execute(
        f"INSERT INTO {mirror}._cdc.quarantine (relation_id,generation_id,epoch,commit_lsn,"
        "reason,actor,run_id,created_at) SELECT relation_id,generation_id,epoch,"
        "1000+i,'r','a','bulk',current_timestamp+(i+relation_id*1000)*INTERVAL 1 SECOND "
        f"FROM {mirror}._cdc.apply_state, range(150) t(i)",
    )
    ops_apply.report_apply(ops_db, h.config, "run2", [])
    status = row(ops_db, "apply_status")
    listed = [i for r in status for i in json.loads(r["quarantines"])]
    assert len(listed) == ops_apply.QUARANTINES
    counts = {r["table_name"]: r["quarantine_count"] for r in status}
    assert counts["other"] == 151 and counts["items"] == 150
    newest = h.store.con.execute(
        f"SELECT list(relation_id||':'||commit_lsn) FROM (SELECT * FROM {mirror}._cdc.quarantine "
        "ORDER BY created_at DESC,commit_lsn DESC LIMIT ?)",
        [ops_apply.QUARANTINES],
    ).fetchone()[0]
    by_table = {r["relation_id"]: json.loads(r["quarantines"]) for r in status}
    assert {f"{t}:{i['commit_lsn']}" for t, items in by_table.items() for i in items} == set(newest)
    assert all(
        v == sorted(v, key=lambda i: i["created_at"], reverse=True) for v in by_table.values()
    )
    assert sum(len(r["quarantines"]) for r in status) < 200 * 150  # bytes, whatever the table count
    # Another generation's quarantines are not the current table's gaps.
    h.store.con.execute(
        f"UPDATE {mirror}._cdc.apply_state SET generation_id='newer' WHERE relation_id=?", [rel]
    )
    ops_apply.report_apply(ops_db, h.config, "run3", [])
    other = [r for r in row(ops_db, "apply_status") if r["table_name"] == "other"][0]
    assert other["quarantine_count"] is None


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_compaction_status_carries_the_recorded_expirations(stores, tmp_path, ops_db):
    from test_expiry import lagging, on

    h = Harness(stores, tmp_path)
    rel = lagging(h)
    seen = {}
    result = run_compaction(on(h), h.store.con, observe=seen)
    assert result.expired == 1
    ops_compact.report_compaction(ops_db, on(h), result, seen["as_of"], seen["summary"])
    status = row(ops_db, "compaction_status")[0]
    (item,) = json.loads(status["expirations"])
    assert item["relation_id"] == rel and item["outcome"] == "expired" and item["reason"]
    assert item["run_id"] == result.run_id and status["expirations_as_of"] is not None
    # Older than the window: left out. A busy run leaves the part.
    h.log.execute("UPDATE {s}expirations SET expired_at=current_timestamp-INTERVAL 31 DAY")
    busy = SimpleNamespace(**{**vars(result), "run_id": "busy", "status": "busy"})
    ops_compact.report_compaction(ops_db, on(h), busy, None, None)
    assert len(json.loads(row(ops_db, "compaction_status")[0]["expirations"])) == 1
    ops_compact.report_compaction(ops_db, on(h), result, seen["as_of"] + 1, seen["summary"])
    assert json.loads(row(ops_db, "compaction_status")[0]["expirations"]) == []


def test_the_expirations_window_fits_the_records_kept():
    from cdc_flight.compact import RECORD_DAYS

    assert ops_compact.EXPIRATION_DAYS <= RECORD_DAYS


# The status write never fails the run: values from PostgreSQL (Decimal) and anything
# else are serialized inside Ops.report's guard (A3 hot-fix). ---------------------------


def test_to_json_encodes_what_postgres_and_duckdb_return():
    import datetime
    from decimal import Decimal

    from cdc_flight.ops import to_json

    items = json.loads(
        to_json(
            [dict(age=Decimal("12.5"), bytes=Decimal("1073741824"), at=datetime.date(2026, 1, 2))]
        )
    )
    assert items == [dict(v=1, age=12.5, bytes=1073741824, at="2026-01-02")]
    assert isinstance(items[0]["bytes"], int)
    with pytest.raises(TypeError):
        to_json([dict(x=object())])


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_a_value_json_cannot_encode_never_fails_the_run(stores, tmp_path, ops_db):
    """Every writer builds its values inside the guard: an unencodable value is a
    `failed:TypeError` outcome, never an exception out of the CLI's finally."""
    from decimal import Decimal

    h = Harness(stores, tmp_path)
    cfg = ingest_config(h)
    # The A3 regression: a Decimal slot field (oldest_xact_age_s) is written.
    slot = dict(name="slot", epoch=1, is_current=True, oldest_xact_age_s=Decimal("3.25"))
    assert ops_ingest.report_ingest(ops_db, cfg, observed(slots=[slot])) == "written"
    stored = json.loads(row(ops_db, "ingest_status")[0]["slots"])[0]
    assert stored["oldest_xact_age_s"] == 3.25
    # Anything else unencodable, in any writer: reported, not raised.
    bad = object()
    assert (
        ops_ingest.report_ingest(ops_db, cfg, observed(slots=[dict(x=bad)])) == "failed:TypeError"
    )
    assert ops_ingest.report_preflight(ops_db, cfg, "p", "ready", warnings=[bad]) == (
        "failed:TypeError"
    )
    days_log(h, ["20260101", "20260102"])
    seen = {}
    result = run_compaction(compact_config(h), h.store.con, observe=seen)
    summary = seen["summary"]
    summary.days.append(dict(day="x", bad=bad))
    assert (
        ops_compact.report_compaction(ops_db, compact_config(h), result, seen["as_of"], summary)
        == "failed:TypeError"
    )
    assert ops_apply.report_backfill(ops_db, h.config, "b", totals={bad: 1}).startswith("failed:")


@pytest.mark.parametrize("stores", ["local"], indirect=True)
@pytest.mark.postgres
def test_the_oldest_transaction_age_is_read_as_a_float(stores, pg_phase1, monkeypatch):  # noqa: F811
    """Past the restart-gap threshold ingest reads extract(epoch …), which PostgreSQL
    returns as NUMERIC (a Decimal in psycopg2): read as a float, it encodes."""
    from cdc_flight.ops import to_json

    source, config = pg_phase1
    check(config, create_objects=True)
    monkeypatch.setattr(ingest_module, "GAP_FOR_XACT_AGE", -1)  # read it whatever the gap
    slots = ingest_module.observed_slots(source.con, config.slot, config.slot, 1)
    current = [s for s in slots if s["is_current"]][0]
    assert isinstance(current["oldest_xact_age_s"], float)
    json.loads(to_json(slots))


def test_every_cli_status_write_is_guarded_against_preparation_errors():
    """Ops.report guards the write itself; the CLI also guards preparing it (its
    arguments, CDC_OPS_DATABASE), so nothing in a status write escapes a run's finally."""
    import ast
    from pathlib import Path

    import cdc_flight.cli

    tree = ast.parse(Path(cdc_flight.cli.__file__).read_text())
    writes = 0
    for node in ast.walk(tree):
        if not isinstance(node, ast.Try):
            continue
        calls = [
            c.func.attr
            for stmt in node.body
            for c in ast.walk(stmt)
            if isinstance(c, ast.Call)
            and isinstance(c.func, ast.Attribute)
            and c.func.attr.startswith("report_")
        ]
        if not calls:
            continue
        writes += len(calls)
        handlers = [h for h in node.handlers if ast.unparse(h.type) == "Exception"]
        assert handlers, f"{calls} unguarded at line {node.lineno}"
        assert "status_write_failed" in ast.unparse(handlers[0]), node.lineno
    every = [
        c
        for c in ast.walk(tree)
        if isinstance(c, ast.Call)
        and isinstance(c.func, ast.Attribute)
        and c.func.attr.startswith("report_")
    ]
    # Every report_* call in the module is one of the guarded ones (a new unguarded
    # call fails here), and all five writers are there.
    assert writes == len(every) >= 5


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_a_done_table_action_refreshes_only_that_tables_apply_status(stores, tmp_path, ops_db):
    """After a done pause, resume or retry, the action Flight's finish rewrites that
    table's apply_status row, so the Dive shows the new state (and the next action)
    without waiting for an apply run. A dry run, or any other status, changes nothing."""
    from test_table_actions import act, caught_up_items

    from cdc_flight import action_flight as af
    from cdc_flight.flightlog import Run

    h = Harness(stores, tmp_path)
    caught_up_items(h)
    ops_apply.report_apply(ops_db, h.config, "a0", [])
    owner = af.Owner(
        h.config.source_id,
        "00000000-0000-4000-8000-000000000001",
        "00000000-0000-4000-8000-000000000002",
        database=h.config.database,
        mirror_database=h.config.mirror_database,
    )
    pause = {"CDC_ACTION": "pause", "CDC_TABLE": "public.items", "CDC_EXPECTED_STATE": "live"}

    def finish(status, dry_run):
        flags = {"CDC_DRY_RUN": "1"} if dry_run else {"CDC_PREVIEW_ID": str(uuid.uuid4())}
        inputs = af.parse({**pause, **flags})
        af.finish(
            ops_db,
            owner,
            inputs,
            {"status": status, "run_id": status},
            Run(),
            audit=lambda *a, **k: False,
        )
        return {r["table_name"]: r for r in row(ops_db, "apply_status")}

    assert act(h, "pause", "public.items", expected_state="live").status == "done"
    for status, dry_run in (("would", True), ("refused", False)):
        assert finish(status, dry_run)["items"]["state"] == "live"  # untouched
    tables = finish("done", False)
    assert tables["items"]["state"] == "paused" and tables["items"]["run_id"] == "done"
    assert {t: r["run_id"] for t, r in tables.items() if t != "items"} == {
        t: "a0" for t in tables if t != "items"
    }


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_a_failed_run_on_a_deleted_source_records_its_lifecycle(stores, tmp_path, ops_db):
    """`observe_after_failure` reports `lifecycle` for a tombstoned slot; the status
    keeps it (a part of its own), and a later run that didn't observe it keeps it too."""
    h = Harness(stores, tmp_path)
    cfg = ingest_config(h)
    failed = dict(run_id="f", status="failed", error="SourceDeleted", step="preflight")
    from cdc_flight import ingest as ingest_module

    ingest_module.observe_after_failure(cfg, "deleted:phase2", failed)  # reads nothing
    assert failed["lifecycle"] == "deleting_or_deleted" and "slots" not in failed
    assert ops_ingest.report_ingest(ops_db, cfg, failed) == "written"
    status = row(ops_db, "ingest_status")[0]
    assert status["lifecycle"] == "deleting_or_deleted" and status["lifecycle_as_of"]
    assert status["last_failure_error"] == "SourceDeleted"
    ops_ingest.report_ingest(ops_db, cfg, observed(run_id="later"))
    assert row(ops_db, "ingest_status")[0]["lifecycle"] == "deleting_or_deleted"
