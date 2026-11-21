"""The operations Dive's queries (dive/queries.snapshot.json, rendered from
dive/src/queries.ts) against a real `cdc_ops` filled by the Flights' own writers on local
stores, read through a connection that has only the `ops` alias attached.

Each runbook diagnosis step (docs/runbooks.md) has one assertion on the field it reads.
"""

import importlib.util
import json
import os
import shutil
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import duckdb
import pytest
from test_compaction import config as compact_config
from test_expiry import lagging, on, vars_of
from test_phase2 import Harness
from test_table_actions import caught_up_items, lsn_of, poisoned, quarantine

from cdc_flight import ops_apply, ops_compact, ops_ingest
from cdc_flight.backfill import Backfill
from cdc_flight.compact import run_compaction
from cdc_flight.config import BackfillConfig
from cdc_flight.ops import write_audit

ROOT = Path(__file__).resolve().parents[1]
SNAPSHOT = json.loads((ROOT / "dive" / "queries.snapshot.json").read_text())
SOURCE = SNAPSHOT["source"]
EXAMPLE_RUN = "00000000-0000-4000-8000-000000000000"  # dive/src/queries.ts
EXAMPLE_REQUEST = "dive-example-request"  # dive/src/queries.ts
_spec = importlib.util.spec_from_file_location("deploy_dive", ROOT / "scripts" / "deploy_dive.py")
deploy_dive = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(deploy_dive)

LOCAL = pytest.mark.parametrize("stores", ["local"], indirect=True)
# N4: the only table functions a query may use (duckdb_columns: the opsColumns probe only)
TABLE_FUNCTIONS = {"unnest", "range", "duckdb_columns"}


def read(path, name, which="all"):
    """Run one snapshot query on a fresh connection with only `ops` attached."""
    with duckdb.connect() as con:
        con.execute("SET TimeZone='Pacific/Auckland'")  # times must still read as UTC
        con.execute(f"ATTACH '{path}' AS ops (READ_ONLY)")
        cursor = con.execute(SNAPSHOT["queries"][name][which])
        names = [d[0] for d in cursor.description]
        rows = [dict(zip(names, r)) for r in cursor.fetchall()]
    return names, rows


def every(path):
    """Every query for both filters: columns as declared; returns {name: all rows}."""
    out = {}
    for name, query in SNAPSHOT["queries"].items():
        for which in ("all", "source"):
            names, rows = read(path, name, which)
            assert names == query["columns"], name
            if which == "source" and name != "sources":  # the filter's domain
                assert all(r.get("source_id", SOURCE) == SOURCE for r in rows), name
            else:
                out[name] = rows
    return out


class Ops:
    """A local file attached as `cdc_ops` to the store's connection, detached to read."""

    def __init__(self, h, tmp_path):
        self.h, self.path = h, tmp_path / "cdc_ops.duckdb"
        self.attach()

    def attach(self):
        self.h.store.con.execute(f"ATTACH IF NOT EXISTS '{self.path}' AS cdc_ops")
        return self.h.store.con

    def read(self):
        self.h.store.con.execute("DETACH cdc_ops")
        try:
            return every(self.path)
        finally:
            self.attach()


@pytest.fixture
def scenario(stores, tmp_path):
    """The Phase 2 harness's source (`SOURCE`), with the deploy DDL run on its `cdc_ops`."""
    h = Harness(stores, tmp_path)
    assert h.config.source_id == SOURCE, "dive/src/snapshot.ts SNAPSHOT_SOURCE"
    o = Ops(h, tmp_path)
    deploy_dive.create_schema(h.store.con)  # as the deploy does, before any Flight runs
    return SimpleNamespace(h=h, ops=o, con=h.store.con)


def write_ingest(con, h, **fields):
    observed = dict(run_id=uuid.uuid4().hex, status="drained", epoch=1, checkpoint=10)
    observed["as_of"] = con.execute("SELECT epoch_us(current_timestamp)").fetchone()[0]
    observed.update(fields)
    assert ops_ingest.report_ingest(con, h.ingest_config, observed) == "written"


def slot(**fields):
    base = dict(name="slot", epoch=1, is_current=True, active=False, wal_status="reserved")
    base.update(safe_wal_size=1 << 30, retained_bytes=4096, lag_bytes=1024, restart_gap_bytes=64)
    base.update(fields)
    return base


@LOCAL
def test_each_runbook_step_reads_its_screen(scenario):
    """docs/runbooks.md R1-R3: each trigger and diagnosis field, where the redesign shows it."""
    s, h, con = scenario, scenario.h, scenario.con
    # R3 before expiry: `third` stops applying; compaction (report mode) estimates its expiry.
    rel = lagging(h)
    seen = {}
    result = run_compaction(compact_config(h), con, observe=seen)
    ops_compact.report_compaction(con, compact_config(h), result, seen["as_of"], seen["summary"])
    ops_apply.report_apply(con, h.config, "a0", [])
    screens = s.ops.read()
    # R3 diagnosis: Source page -> Change log retention -> Pins: the table's pin and its
    # estimated expiry (already past); the drawer shows the same expiry.
    (pin,) = [p for p in screens["sourcePins"] if p["table_name"] == "public.third"]
    assert pin["kind"] == "table" and pin["rule"] == "lag"
    assert pin["expires_at_s"] is not None and pin["expires_at_s"] < time.time()
    (third,) = [t for t in screens["tableStatus"] if t["table_name"] == "public.third"]
    assert third["expires_at_s"] == pytest.approx(pin["expires_at_s"]) and third["expires_in_s"] < 0
    # R3 diagnosis 2: the drawer -> Change log retention: when the pin last moved.
    assert third["pin_lsn"] and third["pin_moved_at_s"] is not None
    # R3 after: compaction with expiry on releases it; the gap is open, the table needs_resync.
    seen = {}
    result = run_compaction(on(h), con, observe=seen)
    assert result.expired == 1
    ops_compact.report_compaction(con, on(h), result, seen["as_of"], seen["summary"])
    # R2: a poison commit holds `other` needs_action; a quarantine lets it go on, and a
    # second poison commit holds it again.
    caught_up_items(h)
    first = poisoned(h)
    assert quarantine(h, first).status == "done"
    q_run = con.execute(
        f"SELECT run_id FROM {h.apply.mirror}._cdc.quarantine WHERE commit_lsn=?", [first]
    ).fetchone()[0]
    audit(con, h, q_run, "quarantine", {"table": "public.other"}, False, status="done")
    # Table actions as the action Flight records them: a dry run (would), its real run
    # (done, naming the preview), a dry run with nothing to do (already), and a resync
    # that started a backfill.
    preview = EXAMPLE_RUN
    audit(
        con,
        h,
        preview,
        "pause",
        {"table": "public.items"},
        True,
        request_id=EXAMPLE_REQUEST,
        status="would",
        detail="pause: apply skips it until resumed",
    )
    audit(
        con,
        h,
        str(uuid.uuid4()),
        "pause",
        {"table": "public.items", "preview_id": preview},
        False,
        status="done",
        next_steps=["resume public.items before its pin expires"],
    )
    audit(
        con,
        h,
        str(uuid.uuid4()),
        "resume",
        {"table": "public.items"},
        True,
        status="already",
        detail="already live",
    )
    audit(
        con,
        h,
        str(uuid.uuid4()),
        "resync",
        {"table": "public.third"},
        False,
        status="done",
        triggered={"flight": "backfill", "run_id": "r", "run_number": 4},
    )
    w = h.wire
    second = lsn_of(h.ingest(w.txn(w.insert(2, ("4", "maybe", "w"))))[0])
    assert h.run("other").status == "needs_action"
    # Registry holds: a dropped source table (a problem) and a removed one (intentional).
    for name, reason in (("items", "source_missing"), ("fourth", "removed")):
        h.log.execute(
            "UPDATE {s}tables SET held_reason=?,held_detail='detail',held_since=current_timestamp "
            "WHERE table_name=?",
            [reason, name],
        )
    # Apply: a run that stopped on held tables (needs_operator).
    ops_apply.report_apply(con, h.config, "a1", [], status="needs_operator")
    # Backfill: the log-only generations, and a failed run.
    Backfill(con, BackfillConfig(**vars_of(h.config))).initialize()
    ops_apply.report_backfill(
        con,
        h.config,
        "b1",
        table="public.third",
        status="failed",
        error="RuntimeError",
        step="scan",
    )
    # R1: the current slot is lost and the ingest run failed reading it.
    # ... with an old open transaction, and a pending slot from an interrupted recovery.
    write_ingest(
        con,
        h,
        slots=[
            slot(wal_status="lost", oldest_xact_age_s=7200.0),
            slot(name="slot_e2", epoch=2, is_current=False),
        ],
    )
    ops_ingest.report_ingest(
        con,
        h.ingest_config,
        dict(run_id="i-fail", status="failed", error="SlotLost", step="stream"),
    )
    ops_ingest.report_preflight(con, h.ingest_config, "p1", "ready", warnings=["a warning"])

    screens = s.ops.read()
    by = lambda name, **match: [  # noqa: E731
        r for r in screens[name] if all(r[k] == v for k, v in match.items())
    ]
    table = lambda name: by("tableStatus", table_name=name)[0]  # noqa: E731
    # R1: Overview -> the source: slot invalidated (wal_status lost), sync failing (SlotLost).
    (src,) = by("sourceStatus", source_id=SOURCE)
    assert src["wal_status"] == "lost" and src["wal_pct"] is None
    assert src["ingest_health"] == "failed" and src["ingest_error"] == "SlotLost"
    assert src["failing_since_at_s"] is not None and src["epoch"] == 1  # Danger zone: epoch
    (ingest,) = by("health", flight="ingest")
    assert ingest["health"] == "failed"
    # R1 diagnosis: Pipeline -> Ingest (the last run) and the slot's retained WAL over 7 days.
    assert src["last_sync_at_s"] is not None and src["slot_name"] == "slot"
    assert by("walHistory", source_id=SOURCE)
    # R1 diagnosis 2 and action 1: WAL limit and slot -> Slots lists the pending slot.
    roles = {r["slot_name"]: r["role"] for r in by("sourceSlots", source_id=SOURCE)}
    assert roles == {"slot": "current", "slot_e2": "pending"}
    # Source disk filling: the oldest open transaction.
    assert src["oldest_xact_age_s"] == 7200.0
    # R2: Overview -> the table: Needs action, "Sync failed: <detail>", blocked since; its
    # drawer's technical details name the blocked batch (Quarantine's literal).
    other = table("public.other")
    assert (other["status"], other["kind"]) == ("needs_action", "blocked")
    assert other["detail"] and other["since_at_s"] is not None and other["held_for_s"] >= 0
    assert other["blocked_unit"] == "..".join(unit_text(h, second))
    assert other["quarantines"] == 1
    # Registry holds: a dropped source table needs action; a removed one is left out.
    assert (table("public.items")["status"], table("public.items")["kind"]) == (
        "needs_action",
        "registry",
    )
    assert "source_missing" in table("public.items")["detail"]
    assert table("public.fourth")["status"] == "removed"
    # N2: a table held by both apply and the registry is listed once.
    names = [r["table_name"] for r in screens["tableStatus"]]
    assert len(names) == len(set(names))
    # R2 diagnosis: the drawer -> Gaps and quarantines: the unit it skipped.
    (gap,) = by("tableGaps", kind="quarantine")
    assert gap["table_name"] == "public.other" and gap["open"] is None  # current generation's
    assert "..".join(unit_text(h, first)) in gap["what"]
    # R3 after: the drawer -> Gaps: the expiration, open while the table needs_resync.
    (expired,) = by("tableGaps", kind="expiration")
    assert expired["table_name"] == "public.third" and expired["open"] is True
    assert table("public.third")["state"] == "needs_resync"
    assert table("public.third")["status"] == "needs_action"
    # D1: apply that stopped on held tables is attention, not failed.
    assert by("health", flight="apply")[0]["health"] == "attention"
    assert by("health", flight="backfill")[0]["health"] == "failed"
    # Setup check: preflight and its warnings.
    assert by("sourceSetup")[0]["warnings"] == ["a warning"]
    # The drawer -> History: the table's actions, dry runs and real runs.
    hist = by("tableHistory", table_name="public.items")
    assert {(r["action"], r["dry_run"], r["status"]) for r in hist} == {
        ("pause", True, "would"),
        ("pause", False, "done"),
        ("resume", True, "already"),
    }
    assert by("tableHistory", action="resync")[0]["table_name"] == "public.third"
    # The action panel finds its dry run by the request id it sent: a would enables Run, and
    # the preview id is the row's run id.
    (dry,) = screens["auditRun"]
    assert (dry["status"], dry["dry_run"], dry["table_name"]) == ("would", True, "public.items")
    assert dry["request_id"] == EXAMPLE_REQUEST and dry["run_id"] == EXAMPLE_RUN
    # Source page -> Change log retention: retained days and size.
    (pipe,) = by("sourcePipeline", source_id=SOURCE)
    assert pipe["log_days"] and pipe["oldest_day"]
    assert rel in {r["relation_id"] for r in by("tableStatus") if r["table_name"] == "public.third"}
    preview = os.environ.get("CDC_DIVE_PREVIEW")  # scripts/dive_preview.py
    if preview:
        s.h.store.con.execute("CHECKPOINT cdc_ops")
        s.h.store.con.execute("DETACH cdc_ops")
        shutil.copyfile(s.ops.path, preview)


def audit(con, h, run_id, action, inputs, dry_run, request_id=None, **fields):
    """An audit row as action_flight.finish writes it: the result is the run's JSON."""
    out = dict(action=action, run_id=run_id, table=inputs.get("table"), detail="")
    out.update(next_steps=[], observed={})
    out.update(fields)
    assert write_audit(
        con,
        h.config.database,
        SOURCE,
        run_id,
        action,
        inputs,
        dry_run,
        json.dumps(out),
        request_id=request_id,
    )


def lsn_text(value):
    return f"{value >> 32:X}/{value & 0xFFFFFFFF:X}"


@LOCAL
def test_apply_health_reads_the_run_rows_not_the_snapshot(scenario):
    """D1: apply failing after attach every run still refreshes apply_status; the
    Overview flags it from the run rows. Busy never resets staleness."""
    s, h, con = scenario, scenario.h, scenario.con
    ops_apply.report_apply(con, h.config, "ok", [])
    assert one(s, "apply")["health"] == "ok"
    for n in range(3):
        ops_apply.report_apply(
            con, h.config, f"f{n}", [], status="failed", error="BinderException", step="attach"
        )
    status = one(s, "apply")
    assert status["health"] == "failed" and status["error"] == "BinderException"
    ops_apply.report_apply(con, h.config, "held", [], status="needs_operator")
    assert one(s, "apply")["health"] == "attention"
    ops_apply.report_apply(con, h.config, "ok2", [])
    assert one(s, "apply")["health"] == "ok"
    # A done run 2 h ago, then only busy runs: stale (busy is ok but no fresher).
    con.execute("UPDATE cdc_ops.main.apply_runs SET written_at=current_timestamp-INTERVAL 2 HOUR")
    con.execute(
        "INSERT INTO cdc_ops.main.apply_runs (source_id,run_id,table_name,written_at,status) "
        "VALUES (?,'busy',NULL,current_timestamp,'busy')",
        [SOURCE],
    )
    status = one(s, "apply")
    assert status["status"] == "done" and status["busy_since"] and status["health"] == "stale"
    assert status["age_s"] >= 7000
    # DV1: a failure followed by busy runs (its leases not yet expired) stays failed.
    ops_apply.report_apply(con, h.config, "crash", [], status="failed", error="RuntimeError")
    con.execute(
        "INSERT INTO cdc_ops.main.apply_runs (source_id,run_id,table_name,written_at,status) "
        "VALUES (?,'busy2',NULL,current_timestamp+INTERVAL 1 SECOND,'busy')",
        [SOURCE],
    )
    status = one(s, "apply")
    assert status["health"] == "failed" and status["error"] == "RuntimeError"
    assert status["busy_since"]


@LOCAL
def test_each_writers_statuses_map_to_health_explicitly(scenario):
    """DV2: compaction held (drops stopped) is attention; a status no writer is known to
    report reads unknown, never ok."""
    s, con = scenario, scenario.con
    for status, health in (
        ("held", "attention"),
        ("invariant", "failed"),
        ("done", "ok"),
        ("brand_new", "unknown"),
    ):
        con.execute(
            "INSERT OR REPLACE INTO cdc_ops.main.compaction_status (source_id,run_as_of,status) "
            "VALUES (?,epoch_us(current_timestamp),?)",
            [SOURCE, status],
        )
        assert one(s, "compaction")["health"] == health, status


def one(s, writer, source=SOURCE):
    rows = [r for r in s.ops.read()["health"] if r["flight"] == writer and r["source_id"] == source]
    assert len(rows) == 1, rows
    return rows[0]


@LOCAL
def test_staleness_calibrates_to_the_cadence(scenario):
    """D4: stale after max(floor, 2 x the upper-quartile interval of the last 20 runs);
    the floor alone below 5 runs."""
    s, con = scenario, scenario.con
    deploy_dive.create_schema(con)

    def runs(source, every_s, last_ago_s, n):
        con.execute(
            "INSERT INTO cdc_ops.main.ingest_runs (source_id,run_id,as_of,written_at,status) "
            "SELECT ?,'r'||i,epoch_us(current_timestamp)-(?::BIGINT+i*?)*1000000,current_timestamp,'drained' "
            "FROM range(?) t(i)",
            [source, last_ago_s, every_s, n],
        )
        con.execute(
            "INSERT INTO cdc_ops.main.ingest_status (source_id,status_as_of,status) "
            "VALUES (?,epoch_us(current_timestamp)-?::BIGINT*1000000,'drained')",
            [source, last_ago_s],
        )

    runs("hourly", 3600, 1800, 20)  # 30 min since the last of hourly runs: fine
    runs("late", 3600, 9000, 20)  # 2.5 h since the last hourly run: stale (2 x 1 h)
    runs("fast", 60, 900, 20)  # 15 min since the last of 1-minute runs: stale (floor 10 min)
    runs("few", 3600, 900, 3)  # 3 runs: the floor alone (10 min): stale
    health = {r["source_id"]: r for r in s.ops.read()["health"] if r["flight"] == "ingest"}
    assert health["hourly"]["health"] == "ok" and health["hourly"]["stale_after_s"] == 7200
    assert health["late"]["health"] == "stale" and health["late"]["stale_after_s"] == 7200
    assert health["fast"]["health"] == "stale" and health["fast"]["stale_after_s"] == 600
    assert health["few"]["health"] == "stale" and health["few"]["stale_after_s"] == 600


@LOCAL
def test_repeated_ingest_conflicts_need_attention_unless_stale(scenario):
    """One conflict run is ok (durable progress kept); two in 24 h are attention, with the
    count and the last conflict shown. A stale writer reads stale first."""
    s, con = scenario, scenario.con
    deploy_dive.create_schema(con)

    def conflicts(source, n, ago_s):
        con.executemany(
            "INSERT INTO cdc_ops.main.ingest_runs (source_id,run_id,as_of,written_at,status) "
            "VALUES (?,?,epoch_us(current_timestamp)-?::BIGINT*1000000,"
            "current_timestamp-?::BIGINT*INTERVAL 1 SECOND,'conflict')",
            [[source, f"c{k}", ago_s, ago_s] for k in range(n)],
        )
        con.execute(
            "UPDATE cdc_ops.main.ingest_status SET last_conflict_at=current_timestamp "
            "WHERE source_id=?",
            [source],
        )

    for source in ("one", "two", "old", "stale"):
        ingest_at(con, source, [300 if source != "stale" else 9000])
    conflicts("one", 1, 60)
    conflicts("two", 2, 60)
    conflicts("old", 2, 25 * 3600)  # outside the 24 h window
    conflicts("stale", 2, 60)
    health = {r["source_id"]: r for r in s.ops.read()["health"] if r["flight"] == "ingest"}
    assert health["one"]["health"] == "ok" and health["one"]["conflicts_24h"] == 1
    assert health["two"]["health"] == "attention" and health["two"]["conflicts_24h"] == 2
    assert health["two"]["last_conflict_at"]
    assert health["old"]["health"] == "ok" and health["old"]["conflicts_24h"] == 0
    assert health["stale"]["health"] == "stale" and health["stale"]["conflicts_24h"] == 2


def ingest_at(con, source, ages_s):
    """Ingest runs `ages_s` seconds ago (newest first), and the status of the newest."""
    con.executemany(
        "INSERT INTO cdc_ops.main.ingest_runs (source_id,run_id,as_of,written_at,status) "
        "VALUES (?,?,epoch_us(current_timestamp)-?::BIGINT*1000000,current_timestamp,'drained')",
        [[source, f"i{n}", age] for n, age in enumerate(ages_s)],
    )
    con.execute(
        "INSERT INTO cdc_ops.main.ingest_status (source_id,status_as_of,status) "
        "VALUES (?,epoch_us(current_timestamp)-?::BIGINT*1000000,'drained')",
        [source, ages_s[0]],
    )


def apply_at(con, source, ages_s):
    """Apply runs' own rows (table_name NULL) `ages_s` seconds ago."""
    con.executemany(
        "INSERT INTO cdc_ops.main.apply_runs (source_id,run_id,table_name,written_at,status) "
        "VALUES (?,?,NULL,current_timestamp-to_seconds(?),'done')",
        [[source, f"a{n}", age] for n, age in enumerate(ages_s)],
    )


def ages(start_s, *gaps_s):
    """Run ages, newest first: the newest `start_s` ago, then each gap further back."""
    out = [start_s]
    for gap in gaps_s:
        out.append(out[-1] + gap)
    return out


@LOCAL
def test_a_burst_of_manual_runs_doesnt_shrink_the_cadence(scenario):
    """The demo after A3/A5: hourly runs plus a burst of manual ones pulled the median
    interval under the floor, so every hourly gap read stale after 30 min. The upper
    quartile ignores the burst while over a quarter of the intervals are hourly."""
    s, con = scenario, scenario.con
    deploy_dive.create_schema(con)
    # Ingest: 9 one-minute gaps (a burst), one 5-minute gap and 7 hourly ones; the newest
    # run 50 min ago. Median 60 s (floor 10 min: stale); upper quartile 1 h (2 h: ok).
    ingest_at(con, "burst", ages(3000, *[3600] * 4, 300, *[60] * 9, *[3600] * 3))
    # Apply: the demo's own 10 run rows (gaps in seconds, newest first), its newest 50 min
    # ago and no ingest rows: upper quartile 3594 s, so 2 x = 7188 s.
    apply_at(con, "demo_apply", ages(3000, 369, 3604, 3600, 3594, 1454, 38, 300, 183, 113))
    health = {(r["source_id"], r["flight"]): r for r in s.ops.read()["health"]}
    burst = health["burst", "ingest"]
    assert burst["health"] == "ok" and burst["stale_after_s"] == 7200, burst
    demo = health["demo_apply", "apply"]
    assert demo["health"] == "ok" and demo["stale_after_s"] == 7188, demo


@LOCAL
def test_no_successful_run_past_six_intervals_needs_action(scenario):
    """A stale ingest or apply whose last
    successful run is older than max(1 h, 6 x its usual interval) is urgent; below MIN_RUNS,
    the 1 h floor; apply never sooner than its ingest; compaction never."""
    s, con = scenario, scenario.con
    deploy_dive.create_schema(con)
    hourly = [3600] * 19
    ingest_at(con, "hourly_3h", ages(3 * 3600, *hourly))  # stale at 2 h, urgent at 6 h
    ingest_at(con, "hourly_7h", ages(7 * 3600, *hourly))
    ingest_at(con, "few_runs", ages(2 * 3600, 180, 180))  # 3 runs: floors 10 min and 1 h
    for source, apply_age in (("chained_2h", int(2.5 * 3600)), ("chained_7h", 7 * 3600)):
        ingest_at(con, source, ages(1800, *hourly))  # healthy hourly ingest
        apply_at(con, source, ages(apply_age, *[600] * 19))  # every 10 min: own limit 1 h
    con.execute(
        "INSERT INTO cdc_ops.main.compaction_status (source_id,run_as_of,status) "
        "VALUES ('hourly_7h', epoch_us(current_timestamp) - 30 * 86400 * 1000000::BIGINT, 'done')"
    )
    health = {(r["source_id"], r["flight"]): r for r in s.ops.read()["health"]}

    def check(key, stale, urgent, after):
        r = health[key]
        assert (r["health"] == "stale", r["urgent"], r["urgent_after_s"]) == (
            stale,
            urgent,
            after,
        ), r

    check(("hourly_3h", "ingest"), True, False, 21600)
    check(("hourly_7h", "ingest"), True, True, 21600)
    check(("few_runs", "ingest"), True, True, 3600)
    check(("chained_2h", "apply"), True, False, 21600)  # its ingest's limit, not its own 1 h
    check(("chained_7h", "apply"), True, True, 21600)
    check(("hourly_7h", "compaction"), True, False, None)
    assert health["hourly_7h", "ingest"]["ok_at_s"] is not None
    rows = {r["source_id"]: r for r in s.ops.read()["sourceStatus"]}
    assert (
        rows["hourly_7h"]["ingest_urgent"] is True and rows["hourly_3h"]["ingest_urgent"] is False
    )
    assert (
        rows["chained_7h"]["apply_urgent"] is True
        and rows["chained_7h"]["apply_ok_at_s"] is not None
    )


@LOCAL
def test_apply_is_never_stale_sooner_than_its_ingest(scenario):
    """Ingest chains apply, so apply's threshold is at least its source's ingest's."""
    s, con = scenario, scenario.con
    deploy_dive.create_schema(con)
    for source, apply_age in (("chained", 3000), ("chained_late", 9000)):
        ingest_at(con, source, ages(600, *[3600] * 19))  # hourly: 2 h
        apply_at(con, source, ages(apply_age, 3600, 3600))  # 3 runs: the floor alone (30 min)
    # Apply's own cadence above ingest's keeps its own (the greater).
    ingest_at(con, "slow_apply", ages(600, *[600] * 19))  # every 10 min: 20 min
    apply_at(con, "slow_apply", ages(3000, *[3600] * 19))  # hourly: 2 h
    health = {(r["source_id"], r["flight"]): r for r in s.ops.read()["health"]}
    chained = health["chained", "apply"]
    assert chained["health"] == "ok" and chained["stale_after_s"] == 7200, chained
    late = health["chained_late", "apply"]
    assert late["health"] == "stale" and late["stale_after_s"] == 7200, late
    assert health["chained", "ingest"]["stale_after_s"] == 7200
    slow = health["slow_apply", "apply"]
    assert slow["health"] == "ok" and slow["stale_after_s"] == 7200, slow
    # Ingest never inherits from apply.
    assert health["slow_apply", "ingest"]["stale_after_s"] == 1200


@LOCAL
def test_expiration_gaps_close_when_the_table_is_rebuilt(scenario):
    """N2: open while the current generation is the expired one or the table isn't live."""
    s, h, con = scenario, scenario.h, scenario.con
    rel = lagging(h)
    seen = {}
    result = run_compaction(on(h), con, observe=seen)
    ops_compact.report_compaction(con, on(h), result, seen["as_of"], seen["summary"])
    mirror = h.apply.mirror

    def gap():
        ops_apply.report_apply(con, h.config, uuid.uuid4().hex, [])
        (row,) = [r for r in s.ops.read()["tableGaps"] if r["kind"] == "expiration"]
        return row["open"]

    assert gap() is True  # needs_resync
    con.execute(f"UPDATE {mirror}._cdc.apply_state SET state='live' WHERE relation_id=?", [rel])
    expired_generation = con.execute(
        f"SELECT generation_id FROM {mirror}._cdc.apply_state WHERE relation_id=?", [rel]
    ).fetchone()[0]
    items = json.loads(
        con.execute("SELECT expirations FROM cdc_ops.main.compaction_status").fetchone()[0]
    )
    if items[0]["generation_id"] == expired_generation:
        assert gap() is True  # live again, but still the expired generation
    con.execute(
        f"UPDATE {mirror}._cdc.apply_state SET generation_id='rebuilt' WHERE relation_id=?", [rel]
    )
    assert gap() is False
    # N1: a table since removed from apply closes its gap, labelled as removed. Removal
    # holds its registry row 'removed' (membership), which keeps it out of R3's
    # not_started rows; without it the table would read as registered, never started.
    con.execute(f"DELETE FROM {mirror}._cdc.apply_state WHERE relation_id=?", [rel])
    con.execute(f"UPDATE {h.log.prefix}tables SET held_reason='removed' WHERE relation_id=?", [rel])
    con.execute("DELETE FROM cdc_ops.main.apply_status")
    ops_apply.report_apply(con, h.config, uuid.uuid4().hex, [])
    (row,) = [r for r in s.ops.read()["tableGaps"] if r["kind"] == "expiration"]
    assert row["open"] is False and row["table_name"].endswith("(table removed)")


def test_the_deploy_ddl_makes_every_query_run_on_an_empty_or_old_cdc_ops(tmp_path):
    """D3: day 1 (no Flight has run) and an old schema (a column added later) both read
    as zero rows, never an error, once the deploy DDL ran."""
    path = tmp_path / "cdc_ops.duckdb"
    with duckdb.connect() as con:
        con.execute(f"ATTACH '{path}' AS cdc_ops")
        con.execute(  # apply_status as first created, before the quarantine columns
            "CREATE TABLE cdc_ops.main.apply_status (source_id VARCHAR,relation_id UINTEGER,"
            "as_of BIGINT,PRIMARY KEY(source_id,relation_id))"
        )
        ran = deploy_dive.create_schema(con)
        assert any("ADD COLUMN IF NOT EXISTS quarantines" in r for r in ran)
        again = deploy_dive.create_schema(con)
        assert again == []  # idempotent: nothing on a current schema
    screens = every(path)
    assert screens.pop("statusAsOf") == [{"as_of_at_s": None}]  # an aggregate: no status yet
    assert len(screens.pop("opsColumns")) == 9  # the probe: every optional column exists
    assert all(rows == [] for rows in screens.values())


def references(node, found, ctes):
    if isinstance(node, dict):
        if node.get("type") == "BASE_TABLE":
            found.append((node.get("catalog_name"), node.get("schema_name"), node["table_name"]))
        if node.get("type") == "TABLE_FUNCTION":
            found.append(("function", None, node["function"]["function_name"]))
        for entry in (node.get("cte_map") or {}).get("map", []):
            ctes.add(entry["key"])
        for value in node.values():
            references(value, found, ctes)
    elif isinstance(node, list):
        for value in node:
            references(value, found, ctes)


def test_every_query_reads_only_the_ops_share():
    """Besides running with only `ops` attached, every table reference in the parsed SQL is
    ops.main.<table>, a CTE, or an allow-listed table function."""
    with duckdb.connect() as con:
        for name, query in SNAPSHOT["queries"].items():
            for which in ("all", "source"):
                tree = json.loads(
                    con.execute("SELECT json_serialize_sql(?)", [query[which]]).fetchone()[0]
                )
                assert not tree.get("error"), (name, tree)
                found, ctes = [], set()
                references(tree, found, ctes)
                assert found, name
                for catalog, schema, table in found:
                    if catalog == "function":
                        assert table in TABLE_FUNCTIONS, (name, table)
                        assert table != "duckdb_columns" or name == "opsColumns", name
                    elif not catalog and not schema:
                        assert table in ctes, (name, table)
                    else:
                        assert (catalog, schema) == ("ops", "main"), (name, catalog, table)
                        assert table in deploy_dive.TABLES, (name, table)


def test_deploy_dry_run_connects_to_nothing(tmp_path, monkeypatch, capsys):
    bundle = tmp_path / "index.tsx"
    bundle.write_text(
        f"export const REQUIRED_DATABASES = [{{path: '{deploy_dive.PLACEHOLDER}'}}];\n"
        'const INJECTED_OWNER = "__CDC_OWNER__";'
    )
    monkeypatch.setattr(deploy_dive, "BUNDLE", bundle)
    monkeypatch.setattr(deploy_dive, "ROOT", tmp_path)
    monkeypatch.delenv("MOTHERDUCK_TOKEN", raising=False)
    monkeypatch.delenv("motherduck_token", raising=False)
    share = "md:_share/cdc_ops/" + str(uuid.uuid4())

    def refuse(*args, **kwargs):
        raise AssertionError("the dry run connected")

    monkeypatch.setattr(duckdb, "connect", refuse)
    assert deploy_dive.main(["--share", share]) == 0
    out = capsys.readouterr().out
    assert "dry run" in out and "UPDATE AUTOMATIC" in out
    assert "no --expect-user: read-only for every viewer" in out
    assert deploy_dive.NO_ACTIONS in out
    assert deploy_dive.main(["--share", share, "--expect-user", "owner"]) == 0
    assert (
        "owner: owner; no actions wired, so read-only for every viewer" in capsys.readouterr().out
    )
    with pytest.raises(SystemExit, match="MOTHERDUCK_TOKEN is not set"):
        deploy_dive.main(["--share", share, "--apply", "--expect-user", "owner"])
    with pytest.raises(SystemExit, match="must look like"):
        deploy_dive.main(["--share", "md:cdc_ops"])
    # The DDL goes to --database; the Dive reads the share: they must be one database.
    other = "md:_share/shop_ops/" + str(uuid.uuid4())
    with pytest.raises(SystemExit, match="named 'shop_ops', but --database is 'cdc_ops'"):
        deploy_dive.main(["--share", other])
    capsys.readouterr()
    assert deploy_dive.main(["--share", other, "--database", "shop_ops"]) == 0
    out = capsys.readouterr().out
    assert "add-only DDL" in out and out.count("on shop_ops") == 1
    assert share in deploy_dive.dive_source(share, bundle)


def test_deploy_upserts_by_title_and_checks_the_user(tmp_path):
    class Fake:
        def __init__(self, user, dives):
            self.user, self.dives, self.seen = user, dives, []

        def execute(self, sql, parameters=None):
            self.seen.append(sql)
            rows = [(self.user,)] if sql == "SELECT current_user" else []
            if "MD_LIST_DIVES" in sql:
                rows = self.dives
            if "duckdb_columns" in sql:
                rows = []
            return SimpleNamespace(fetchall=lambda: rows, fetchone=lambda: rows[0])

    share = "md:_share/cdc_ops/" + str(uuid.uuid4())
    with pytest.raises(SystemExit, match="refusing"):
        deploy_dive.deploy(
            Fake("someone", []),
            share=share,
            title="t",
            expect_user="owner",
            database="cdc_ops",
            content="x",
        )
    fake = Fake("owner", [])
    deploy_dive.deploy(
        fake, share=share, title="t", expect_user="owner", database="cdc_ops", content="x"
    )
    assert "MD_CREATE_DIVE" in fake.seen[-1] and share in fake.seen[-1]
    # DV3: the database is created and attached before any DDL reads the catalog.
    create = fake.seen.index('CREATE DATABASE IF NOT EXISTS "cdc_ops"')
    attach = fake.seen.index("ATTACH IF NOT EXISTS 'md:cdc_ops' AS \"cdc_ops\"")
    first_ddl = next(i for i, sql in enumerate(fake.seen) if "duckdb_columns" in sql)
    assert create < attach < first_ddl
    assert any(s.startswith('CREATE TABLE IF NOT EXISTS "cdc_ops".main.audit') for s in fake.seen)
    fake = Fake("owner", [("00000000-0000-0000-0000-000000000001",)])
    deploy_dive.deploy(
        fake, share=share, title="t", expect_user="owner", database="cdc_ops", content="x"
    )
    assert "MD_UPDATE_DIVE_CONTENT" in fake.seen[-2] and "MD_UPDATE_DIVE_METADATA" in fake.seen[-1]
    # Their result rows carry TIMESTAMPTZ (fetching needs pytz): only counted (demo finding).
    assert all(sql.startswith("SELECT count(*) FROM MD_UPDATE_DIVE_") for sql in fake.seen[-2:])
    with pytest.raises(SystemExit, match="unique"):
        deploy_dive.deploy(
            Fake("owner", [("a",), ("b",)]),
            share=share,
            title="t",
            expect_user="owner",
            database="cdc_ops",
            content="x",
        )


def unit_text(h, commit_lsn):
    """The complete unit holding a commit, as the Dive shows it (X/Y, X/Y)."""
    row = h.store.con.execute(
        f"SELECT unit_start_lsn,end_lsn FROM {h.log.prefix}units_v2 "
        "WHERE complete AND unit_start_lsn<? AND end_lsn>?",
        [commit_lsn, commit_lsn],
    ).fetchone()
    return lsn_text(row[0]), lsn_text(row[1])


@LOCAL
def test_a_pending_key_change_is_not_a_problem(scenario):
    """Format 2 queues a key observed inside an open unit; apply clears it once the unit
    closes, so the table's status ignores it (only ingest_status.holds lists it)."""
    s, h, con = scenario, scenario.h, scenario.con
    relation_id = h.apply.registered("public.items")[0]
    queue = [dict(epoch=1, keys=["id", "v"], cut_lsn=0x1_0000_0010, observed_lsn=0x1_0000_0020)]
    con.execute(
        f"UPDATE {h.log.prefix}tables SET pending_keys_v2=? WHERE relation_id=?",
        [json.dumps(queue), relation_id],
    )
    ops_ingest.report_ingest(con, h.ingest_config, dict(run_id="i-key", status="drained"))
    rows = [r for r in s.ops.read()["tableStatus"] if r["table_name"] == "public.items"]
    assert all(r["kind"] != "registry" for r in rows)
    holds = con.execute("SELECT holds FROM cdc_ops.main.ingest_status").fetchone()[0]
    assert "key_change_pending" in holds


def unit_of(literal):
    """(start, end) from `QUARANTINE <table> UNIT <epoch> <X/Y>..<X/Y>`."""
    from cdc_flight.action_types import parse_lsn

    start, end = literal.rsplit(" ", 1)[1].split("..")
    return parse_lsn(start), parse_lsn(end)


def test_every_dive_action_config_is_what_its_flight_accepts():
    """The Dive's action configs (dive/src/actions.ts, rendered into the snapshot) send
    exactly each Flight's declared keys, and the Flight's own parser accepts both the dry
    run and the real run, before any connection. A typed-first action's confirmation is
    exactly its Flight's own confirmation(). The cost text and preview window the
    Dive shows are the Flight's."""
    from cdc_flight import (
        action_flight,
        delete_source_flight,
        epoch_flight,
        membership_flight,
        quarantine_flight,
    )
    from cdc_flight.chain import FLIGHT_CONFIG

    names = {
        "table_action": "table-action",
        "table_membership": "table-membership",
        "quarantine": "quarantine",
        "epoch": "epoch",
        "delete_source": "delete-source",
    }
    specs = {
        "table_action": action_flight.TABLE_ACTION,
        "table_membership": membership_flight.MEMBERSHIP,
        "quarantine": quarantine_flight.QUARANTINE,
        "epoch": epoch_flight.EPOCH,
        "delete_source": delete_source_flight.DELETE,
    }
    literal = {
        # The unit isn't an input (the Flight observes it): build the literal from the
        # unit the form names, and require the Flight's exact format for it.
        "quarantine": lambda i: action_flight.unit_literal(
            i.table, i.values["expected_epoch"], *unit_of(i.values["confirm"])
        ),
        "recover-slot": lambda i: epoch_flight.confirmation(
            SOURCE, "recover-slot", i.values["expected_epoch"]
        ),
        "skip-to-lsn": lambda i: epoch_flight.confirmation(
            SOURCE, "skip-to-lsn", i.values["expected_epoch"], i.values["to_lsn"]
        ),
        "delete-source": lambda i: delete_source_flight.confirmation(SOURCE),
    }
    # Every key, CDC_REQUEST_ID included: the Dive sends one with every run. Except
    # CDC_MODE and CDC_IGNORE_DELETES (append-mode add): they reach the Dive with the
    # release that supports append capture. The Dive sends every key it lists, and a
    # table-membership Flight deployed earlier lacks them.
    not_yet_sent = ("CDC_MODE", "CDC_IGNORE_DELETES")

    def accepted(flight):
        return tuple(k for k in FLIGHT_CONFIG[names[flight]] if k not in not_yet_sent)

    for flight, keys in SNAPSHOT["flight_keys"].items():
        assert tuple(keys) == accepted(flight), flight
    flights_used = set()
    for action, rendered in SNAPSHOT["actions"].items():
        flight = rendered["flight"]
        flights_used.add(flight)
        assert rendered["forms"], action
        for form in rendered["forms"]:
            for run in ("dry", "real"):
                config = form[run]
                declared = set(accepted(flight))
                assert set(config) == declared, (action, run)
                inputs = action_flight.parse(config, specs[flight])
                assert inputs.action == action and inputs.dry_run is (run == "dry"), (action, run)
                assert inputs.request_id == "dive-snapshot-request", (action, run)
                if action in literal:  # sent on every run, and exactly the Flight's
                    assert config["CDC_CONFIRM"] == literal[action](inputs), (action, run)
    assert flights_used == set(names)
    # start-snapshot and accept-types reach the Dive in a follow-up
    # that wires their panel forms; until then they run through MD_RUN_FLIGHT or the CLI.
    not_yet_rendered = {"start-snapshot", "accept-types"}
    assert (
        set(SNAPSHOT["actions"])
        == (set(action_flight.USES) | {a for spec in list(specs.values())[1:] for a in spec.uses})
        - not_yet_rendered
    )
    assert SNAPSHOT["resync_cost"] == action_flight.RESYNC_COST
    assert SNAPSHOT["preview_minutes"] == action_flight.PREVIEW_MINUTES


def test_deploy_injects_and_checks_the_action_flights(tmp_path, monkeypatch, capsys):
    """--flights: ids validated, injected as the bundle's FLIGHTS literal, and with --apply
    each Flight must be the owner's, declare exactly its kind's inputs and be runnable by
    no one else (actions are owner-only); --expect-user is injected as the owner."""
    import pytest as _pytest

    action, membership = str(uuid.uuid4()), str(uuid.uuid4())
    path = tmp_path / "flights.json"
    path.write_text(json.dumps({"demo": {"table_action": action, "table_membership": membership}}))
    flights = deploy_dive.load_flights(path)
    # Every kind the Dive's actions use is a kind the deploy accepts.
    kinds = {rendered["flight"] for rendered in SNAPSHOT["actions"].values()}
    assert kinds == set(deploy_dive.FLIGHT_KINDS)
    for bad in ({}, {"Demo": {}}, {"demo": {"other": action}}, {"demo": {"table_action": "x"}}):
        path.write_text(json.dumps(bad))
        with _pytest.raises(SystemExit):
            deploy_dive.load_flights(path)
    bundle = 'const INJECTED = "__CDC_FLIGHTS__";'
    injected = deploy_dive.inject_flights(bundle, flights)
    literal = injected.split("= ", 1)[1].rstrip(";")
    assert json.loads(json.loads(literal)) == flights
    with _pytest.raises(SystemExit, match="exactly once"):
        deploy_dive.inject_flights("no placeholder", flights)
    # The owner: the one viewer who can act (dive/src/owner.ts).
    owned = deploy_dive.inject_owner('const INJECTED_OWNER = "__CDC_OWNER__";', "alice")
    assert owned == 'const INJECTED_OWNER = "alice";'
    for bad in ("", "__CDC_OWNER__"):
        with _pytest.raises(SystemExit, match="not a user name"):
            deploy_dive.inject_owner('"__CDC_OWNER__"', bad)
    with _pytest.raises(SystemExit, match="exactly once"):
        deploy_dive.inject_owner("no placeholder", "alice")

    keys = {
        action: list(deploy_dive.FLIGHT_CONFIG["table-action"]),
        membership: list(deploy_dive.FLIGHT_CONFIG["table-membership"]),
    }

    class Fake:
        def __init__(self, owner="alice", grants=()):
            self.owner, self.grants = owner, grants

        def execute(self, sql, parameters=None):
            flight = sql.split("'")[1]
            if "MD_GET_FLIGHT(" in sql:
                rows = [(self.owner, 3)]
            elif "MD_GET_FLIGHT_VERSION" in sql:
                assert "version_number := 3" in sql
                rows = [(keys[flight],)]
            else:
                rows = list(self.grants)
            return SimpleNamespace(fetchone=lambda: rows[0], fetchall=lambda: rows)

    assert deploy_dive.check_flights(Fake(), flights, "alice") == []
    # Owner-only: the owner's own grant is fine; RUN to anyone else refuses the deploy.
    own = (("USER", "alice", "RUN"), ("USER", "someone", "MODIFY"))
    assert deploy_dive.check_flights(Fake(grants=own), flights, "alice") == []
    for grant in (
        ("ORGANIZATION", None, "RUN"),
        ("ROLE", "ops", "run"),
        ("USER", "someone", "RUN"),
    ):
        with _pytest.raises(SystemExit, match="granted RUN to .*owner-only"):
            deploy_dive.check_flights(Fake(grants=(grant,)), flights, "alice")
    with _pytest.raises(SystemExit, match="owned by"):
        deploy_dive.check_flights(Fake(owner="someone"), flights, "alice")
    keys[action] = keys[action][:-1] + ["CDC_NEW"]
    with _pytest.raises(SystemExit, match="missing .*extra"):
        deploy_dive.check_flights(Fake(), flights, "alice")


def test_the_run_grant_check_guards_only_wired_action_flights(capsys):
    """A read-only deploy (no --flights) wires no action Flight: it skips the RUN grant
    check and says so. One that wires them refuses on any RUN grant but the owner's,
    before it changes anything (no warning mode)."""
    flight = str(uuid.uuid4())

    class Fake:
        def __init__(self):
            self.seen = []

        def execute(self, sql, parameters=None):
            self.seen.append(sql)
            if sql == "SELECT current_user":
                rows = [("alice",)]
            elif "MD_GET_FLIGHT(" in sql:
                rows = [("alice", 1)]
            elif "MD_GET_FLIGHT_VERSION" in sql:
                rows = [(list(deploy_dive.FLIGHT_CONFIG["table-action"]),)]
            elif "MD_LIST_FLIGHT_GRANTEES" in sql:
                rows = [("ORGANIZATION", None, "RUN")]
            else:
                rows = []
            return SimpleNamespace(fetchall=lambda: rows, fetchone=lambda: rows[0])

    share = "md:_share/cdc_ops/" + str(uuid.uuid4())
    args = dict(share=share, title="t", expect_user="alice", database="cdc_ops", content="x")
    fake = Fake()
    deploy_dive.deploy(fake, **args)
    assert deploy_dive.NO_ACTIONS in capsys.readouterr().out
    assert not any("FLIGHT" in sql for sql in fake.seen)
    assert any("MD_CREATE_DIVE" in sql for sql in fake.seen)
    fake = Fake()
    with pytest.raises(SystemExit, match="granted RUN to organization"):
        deploy_dive.deploy(fake, **args, flights={"demo": {"table_action": flight}})
    assert not any("CREATE" in sql or "MD_" in sql and "FLIGHT" not in sql for sql in fake.seen)


@LOCAL
def test_a_deleting_or_deleted_source_needs_action(scenario):
    """A deleting source's lifecycle (from its delete audit rows, then refusals) reaches
    the source's status; its refused runs read deleted, not failed, and its tables wait."""
    s, h, con = scenario, scenario.h, scenario.con
    caught_up_items(h)
    ops_apply.report_apply(con, h.config, "a-ok", [])
    status = lambda: [r for r in s.ops.read()["sourceStatus"] if r["source_id"] == SOURCE][0]  # noqa: E731
    assert status()["lifecycle"] is None  # active
    # Delete: the newest delete-source audit row decides.
    for state, lifecycle in (("waiting", "deleting"), ("done", "deleted")):
        audit(
            con,
            h,
            str(uuid.uuid4()),
            "delete-source",
            {"expected_epoch": 1},
            False,
            status=state,
            lifecycle=lifecycle,
        )
        assert status()["lifecycle"] == lifecycle
    ops_apply.report_apply(con, h.config, "refused", [], status="failed", error="SourceDeleted")
    assert status()["apply_health"] == "deleted"  # not `failed`
    assert {r["status"] for r in s.ops.read()["tableStatus"] if r["status"] != "removed"} <= {
        "waiting",
        "needs_action",
    }


@LOCAL
def test_the_panel_reads_a_deletes_drop_list_and_dba_statements(scenario):
    """delete-source puts what it drops itself and the statements left for a DBA at the top
    of its result, not in `observed`: a source set up before the pipeline tracked what it
    created drops nothing and leaves everything to the DBA (as a demo dry run found)."""
    s, h, con = scenario, scenario.h, scenario.con
    dba = ["DROP PUBLICATION cdc_pub;", "SELECT pg_drop_replication_slot('cdc_slot');"]
    audit(
        con,
        h,
        EXAMPLE_RUN,
        "delete-source",
        {"expected_epoch": 1},
        True,
        request_id=EXAMPLE_REQUEST,
        status="would",
        drop=[],
        dba=dba,
        retained_bytes=0,
    )
    (row,) = s.ops.read()["auditRun"]
    assert row["action"] == "delete-source" and row["status"] == "would"
    assert json.loads(row["drops"]) == [] and json.loads(row["dba"]) == dba
    # The real run writes what it dropped as `dropped`, and repeats the DBA statements as its
    # next steps (the panel shows them once).
    dropped = [{"kind": "slot", "name": "cdc_slot"}, {"kind": "publication", "name": "cdc_pub"}]
    audit(
        con,
        h,
        str(uuid.uuid4()),
        "delete-source",
        {"expected_epoch": 1, "preview_id": EXAMPLE_RUN},
        False,
        request_id=EXAMPLE_REQUEST,
        status="done",
        lifecycle="deleted",
        dropped=dropped,
        dba=dba[:1],
        next_steps=[f"the DBA runs: {dba[0]}"],
    )
    (row,) = s.ops.read()["auditRun"]
    assert row["dry_run"] is False and row["status"] == "done"
    assert json.loads(row["drops"]) == dropped and json.loads(row["dba"]) == dba[:1]


def test_a_128_membership_flight_takes_the_dives_add_without_its_mode_keys():
    """The Dive's add omits CDC_MODE and CDC_IGNORE_DELETES (they arrived with append
    capture). A membership Flight built since then declares them; its parser reads an absent
    key as blank, and blank means the defaults (keyed, deletes applied), which hash exactly as
    the explicit defaults. So the Dive's dry and real runs are accepted and their preview
    matches, before any deploy. (Whether MotherDuck starts a run whose config omits a declared
    key is checked once live, with an Enable dry run.)"""
    from cdc_flight import membership_flight
    from cdc_flight.chain import FLIGHT_CONFIG

    assert {"CDC_MODE", "CDC_IGNORE_DELETES"} <= set(FLIGHT_CONFIG["table-membership"])
    for run in ("dry", "real"):
        sent = SNAPSHOT["actions"]["add"]["forms"][0][run]
        assert "CDC_MODE" not in sent and "CDC_IGNORE_DELETES" not in sent
        absent = membership_flight.parse(sent, membership_flight.MEMBERSHIP)
        explicit = membership_flight.parse(
            {**sent, "CDC_MODE": "keyed", "CDC_IGNORE_DELETES": "0"}, membership_flight.MEMBERSHIP
        )
        assert absent.action == "add" and absent.values == explicit.values


@LOCAL
def test_finish_recovery_repeats_the_last_transition_exactly(scenario):
    """WAL limit and slot -> a leftover slot's Finish recovery: the newest completed epoch
    transition with its inputs, an LSN above 2^32 as X/Y, so the repeat sends exactly the
    Flight's own confirmation and never begins another epoch."""
    from cdc_flight import epoch_flight

    s, h, con = scenario, scenario.h, scenario.con
    recover = epoch_flight.confirmation(SOURCE, "recover-slot", 1)
    audit(
        con,
        h,
        str(uuid.uuid4()),
        "recover-slot",
        {"expected_epoch": 1, "confirm": recover},
        False,
        status="done",
    )
    (last,) = s.ops.read()["lastTransition"]
    assert (last["action"], last["expected_epoch"], last["confirm"]) == ("recover-slot", 1, recover)
    to_lsn = (5 << 32) + 0xAB
    skip = epoch_flight.confirmation(SOURCE, "skip-to-lsn", 2, to_lsn)
    audit(
        con,
        h,
        str(uuid.uuid4()),
        "skip-to-lsn",
        {"expected_epoch": 2, "to_lsn": to_lsn, "confirm": skip},
        False,
        status="done",
    )
    audit(
        con, h, str(uuid.uuid4()), "recover-slot", {"expected_epoch": 9}, True, status="would"
    )  # a dry run
    (last,) = s.ops.read()["lastTransition"]
    assert (last["action"], last["expected_epoch"], last["to_lsn"]) == ("skip-to-lsn", 2, "5/AB")
    assert (
        last["confirm"] == skip == f"SKIP {SOURCE} TO {last['to_lsn']}"
    )  # actions.ts flightLiteral


def test_without_a_delete_audit_a_sourcedeleted_refusal_still_labels_the_source(tmp_path):
    path = tmp_path / "cdc_ops.duckdb"
    with duckdb.connect() as con:
        con.execute(f"ATTACH '{path}' AS cdc_ops")
        deploy_dive.create_schema(con)
        con.execute(
            "INSERT INTO cdc_ops.main.apply_runs (source_id,run_id,table_name,written_at,status,error) "
            "VALUES ('gone','r',NULL,current_timestamp,'failed','SourceDeleted')"
        )
    rows = read(path, "sourceStatus")[1]
    assert [(r["source_id"], r["lifecycle"]) for r in rows] == [("gone", "deleting_or_deleted")]


# PostgreSQL TLS (the run rows' TLS columns): the weakest connecting writer per source.
CA = "rds-us-east-1"
VERIFIED = dict(tls_mode="verify-full", tls_ca=CA, tls_in_use=True)


def tls_row(s):
    """The source's tls row and its setup check's TLS line (None and "unknown" if the
    source has no row)."""
    reads = s.ops.read()
    rows = [r for r in reads["tls"] if r["source_id"] == SOURCE]
    cell = rows[0]["tls"] if rows else "unknown"
    return (rows[0] if rows else None), cell, reads


def ingest_tls(con, h, run_id=None, **tls):
    write_ingest(con, h, run_id=run_id or uuid.uuid4().hex, **tls)


def backfill_tls(con, h, run_id, **tls):
    Backfill(con, BackfillConfig(**vars_of(h.config))).initialize()  # report_backfill reads it
    ops_apply.report_backfill(con, h.config, run_id, table="public.third", tls=tls or None)


def audit_tls(con, h, run_id, action, **tls):
    assert write_audit(
        con,
        h.config.database,
        SOURCE,
        run_id,
        action,
        {},
        False,
        json.dumps(dict(action=action, run_id=run_id)),
        tls=tls or None,
    )


def age(con, table, run_id, interval):
    con.execute(
        f"UPDATE cdc_ops.main.{table} SET written_at=current_timestamp-INTERVAL {interval} "
        "WHERE run_id=?",
        [run_id],
    )


@LOCAL
@pytest.mark.parametrize(
    "tls, label, warn",
    [
        (dict(), "unknown", False),  # a pre-TLS build: quiet, the column says unknown
        (VERIFIED, f"verify-full ({CA})", False),
        (dict(tls_mode="require", tls_in_use=True), "encrypted, not verified", True),
        (dict(tls_mode="prefer", tls_in_use=False), "PLAINTEXT", True),  # silent fallback
        (dict(tls_mode="require"), "requested only", True),  # T2: short of the target
        (dict(tls_mode="verify-full", tls_ca=CA), "not yet connected", False),  # fails closed
    ],
)
def test_tls_states_from_the_ingest_run_rows(scenario, tls, label, warn):
    ingest_tls(scenario.con, scenario.h, **tls)
    row, cell, _ = tls_row(scenario)
    assert row["tls"] == label and row["warn"] is warn and cell == label
    assert row["breakdown"].startswith(f"ingest {label} (as of ")


@LOCAL
def test_tls_negotiated_survives_a_run_that_opened_no_connection(scenario):
    """N2: the negotiated value is the newest row that has one, and only counts as
    verified if that run itself was verify-full."""
    s, h, con = scenario, scenario.h, scenario.con
    ingest_tls(con, h, run_id="old", **VERIFIED)
    age(con, "ingest_runs", "old", "1 HOUR")
    ingest_tls(con, h, run_id="new", tls_mode="verify-full", tls_ca=CA)  # failed: no connection
    row, cell, _ = tls_row(s)
    assert row["tls"] == cell == f"verify-full ({CA})" and row["warn"] is False


@LOCAL
def test_tls_a_verify_full_request_never_inherits_an_older_unverified_connection(scenario):
    s, h, con = scenario, scenario.h, scenario.con
    ingest_tls(con, h, run_id="old", tls_mode="require", tls_in_use=True)
    age(con, "ingest_runs", "old", "1 HOUR")
    ingest_tls(con, h, run_id="new", tls_mode="verify-full", tls_ca=CA)  # rebuilt, not yet run
    row, _, _ = tls_row(s)
    assert row["tls"] == "not yet connected" and row["warn"] is False


@LOCAL
def test_tls_the_weakest_connecting_writer_shows_with_a_breakdown(scenario):
    """T1: TLS is baked per bundle, so a backfill or a connecting action built without
    --pg-ca weakens the source even when ingest is verify-full."""
    s, h, con = scenario, scenario.h, scenario.con
    ingest_tls(con, h, **VERIFIED)
    backfill_tls(con, h, "b1", tls_mode="require", tls_in_use=True)
    row, cell, _ = tls_row(s)
    assert row["tls"] == cell == "encrypted, not verified" and row["warn"] is True
    assert row["breakdown"].startswith("backfill encrypted, not verified (as of ")
    assert f"; ingest verify-full ({CA}) (as of " in row["breakdown"]
    audit_tls(con, h, "a1", "add", tls_mode="prefer", tls_in_use=False)
    row, cell, _ = tls_row(s)
    assert row["tls"] == cell == "PLAINTEXT" and row["breakdown"].startswith("action add PLAINTEXT")


@LOCAL
def test_tls_actions_that_never_connect_never_count(scenario):
    """A table action's audit row has NULL TLS (it never connects); a connecting action's
    pre-TLS row is NULL too. Neither is a writer, and neither shows up as unknown."""
    s, h, con = scenario, scenario.h, scenario.con
    ingest_tls(con, h, **VERIFIED)
    audit_tls(con, h, "p1", "pause")
    audit_tls(con, h, "a0", "add")  # pre-TLS build of a connecting action
    row, _, reads = tls_row(s)
    assert row["tls"] == f"verify-full ({CA})" and row["warn"] is False
    assert "action" not in row["breakdown"]


@LOCAL
def test_tls_old_runs_age_out_per_writer(scenario):
    """backfill_runs and audit are never pruned: a 40-day-old one-off backfill must not
    mark the source; ingest's own rows age out with its 7-day retention."""
    s, h, con = scenario, scenario.h, scenario.con
    ingest_tls(con, h, **VERIFIED)
    backfill_tls(con, h, "b-old", tls_mode="require", tls_in_use=True)
    age(con, "backfill_runs", "b-old", "40 DAY")
    row, _, _ = tls_row(s)
    assert row["tls"] == f"verify-full ({CA})" and "backfill" not in row["breakdown"]
    backfill_tls(con, h, "b-recent", tls_mode="require", tls_in_use=True)
    age(con, "backfill_runs", "b-recent", "29 DAY")
    assert tls_row(s)[0]["tls"] == "encrypted, not verified"


@LOCAL
def test_tls_is_per_source(scenario):
    s, h, con = scenario, scenario.h, scenario.con
    ingest_tls(con, h, **VERIFIED)
    con.execute(
        "INSERT INTO cdc_ops.main.ingest_runs (source_id,run_id,written_at,status,tls_mode,"
        "tls_in_use) VALUES ('other','o1',current_timestamp,'drained','disable',false)"
    )
    row, _, reads = tls_row(s)
    assert row["tls"] == f"verify-full ({CA})"
    (other,) = [r for r in reads["tls"] if r["source_id"] == "other"]
    assert other["tls"] == "PLAINTEXT" and other["warn"] is True


@LOCAL
@pytest.mark.parametrize("backfill_first", [True, False])
def test_tls_a_tie_goes_to_the_weaker_evidence_whatever_the_row_order(scenario, backfill_first):
    """L1: verify-full and not yet connected share a rank; the source reads the weaker
    evidence (not yet connected) in either insertion order."""
    s, h, con = scenario, scenario.h, scenario.con
    writes = [
        lambda: backfill_tls(con, h, "b1", tls_mode="verify-full", tls_ca=CA),  # not yet run
        lambda: ingest_tls(con, h, **VERIFIED),
    ]
    for write in writes if backfill_first else reversed(writes):
        write()
    row, cell, _ = tls_row(s)
    assert row["tls"] == cell == "not yet connected" and row["warn"] is False
    assert row["breakdown"].startswith("backfill not yet connected")


@LOCAL
def test_tls_a_pending_request_is_dated_by_its_own_row(scenario):
    """L2: for not yet connected (and requested only) the time shown is the request's row,
    not the older negotiated one."""
    s, h, con = scenario, scenario.h, scenario.con
    ingest_tls(con, h, run_id="old", tls_mode="require", tls_in_use=True)
    age(con, "ingest_runs", "old", "2 DAY")
    ingest_tls(con, h, run_id="new", tls_mode="verify-full", tls_ca=CA)
    (new_at,) = con.execute(
        "SELECT epoch(written_at)::VARCHAR FROM cdc_ops.main.ingest_runs WHERE run_id='new'"
    ).fetchone()
    row, _, _ = tls_row(s)
    assert row["breakdown"] == f"ingest not yet connected (as of @{new_at}@)"


@LOCAL
def test_table_status_from_the_flights_own_rows(scenario):
    """Overview's table rows: paused, rebuilding and unknown per table, its changes over 7
    days, and Waiting for every table once the source's apply stops running."""
    s, con = scenario, scenario.con
    src = "healthy"
    ingest_at(con, src, [60, 180, 300])
    apply_at(con, src, [60, 180, 300])
    for rel, name, state in (
        (1, "a", "live"),
        (2, "b", "paused"),
        (3, "c", "live"),
        (4, "d", "odd"),
    ):
        con.execute(
            "INSERT INTO cdc_ops.main.apply_status (source_id,relation_id,schema_name,table_name,state) "
            "VALUES (?,?,'public',?,?)",
            [src, rel, name, state],
        )
    con.execute(
        "INSERT INTO cdc_ops.main.apply_runs (source_id,run_id,table_name,written_at,status,events) "
        "VALUES (?,'x','public.a',current_timestamp-INTERVAL 2 MINUTE,'caught_up',5),"
        "(?,'y','public.a',current_timestamp,'caught_up',0)",
        [src, src],
    )
    con.execute(
        "INSERT INTO cdc_ops.main.backfill_status (source_id,generation_id,table_name,state,created_at) "
        "VALUES (?,'g3','public.c','snapshotting',current_timestamp),"
        "(?,'g0','public.a','published',current_timestamp - INTERVAL 1 DAY)",
        [src, src],
    )

    def read():
        return {r["table_name"]: r for r in s.ops.read()["tableStatus"] if r["source_id"] == src}

    tables = read()
    assert {t: r["status"] for t, r in tables.items()} == {
        "public.a": "ok",
        "public.b": "paused",
        "public.c": "rebuilding",
        "public.d": "unknown",
    }
    assert tables["public.a"]["changes_7d"] == 5 and tables["public.b"]["changes_7d"] == 0
    # Apply stops: a healthy table waits on its source; held states keep theirs.
    con.execute(
        "UPDATE cdc_ops.main.apply_runs SET written_at=written_at-INTERVAL 3 HOUR "
        "WHERE source_id=? AND table_name IS NULL",
        [src],
    )
    tables = read()
    assert tables["public.a"]["status"] == "waiting"
    assert tables["public.b"]["status"] == "paused"


@LOCAL
def test_sync_marks_per_table(scenario):
    """Overview's sync marks: per table, 28 marks of 6 h, oldest first. ok and held
    statuses count, a failed whole run counts against every table of its source, and a
    mark with neither is none."""
    s, con = scenario, scenario.con
    src = "ticks"
    for rel, name in ((1, "a"), (2, "b")):
        con.execute(
            "INSERT INTO cdc_ops.main.apply_status (source_id,relation_id,schema_name,table_name,state) "
            "VALUES (?,?,'public',?,'live')",
            [src, rel, name],
        )
    (now_k,) = con.execute("SELECT floor(epoch(current_timestamp) / 21600)::BIGINT").fetchone()

    def run(table, status, marks_ago, events=0):
        con.execute(
            "INSERT INTO cdc_ops.main.apply_runs (source_id,run_id,table_name,written_at,status,events) "
            "VALUES (?,'r',?,to_timestamp((? - ?) * 21600 + 60),?,?)",
            [src, table, now_k, marks_ago, status, events],
        )

    run("public.a", "caught_up", 0, 5)
    run("public.a", "caught_up", 1, 3)
    run("public.a", "needs_action", 1)
    run("public.a", "needs_resync", 2)
    run("public.a", "busy", 3)
    run(None, "failed", 4)  # a whole run: every table of the source
    run("public.b", "capped", 4, 7)
    run("public.a", "caught_up", 40)  # outside the window
    marks = {
        r["table_name"]: r["marks"].split(",")
        for r in s.ops.read()["syncMarks"]
        if r["source_id"] == src
    }
    assert sorted(marks) == ["public.a", "public.b"]
    a = marks["public.a"]
    assert len(a) == 28 and a[::-1][:5] == ["ok", "some", "failed", "none", "failed"]
    b = marks["public.b"]
    assert b[-5] == "some" and b[-1] == "none"


_preview_spec = importlib.util.spec_from_file_location(
    "dive_preview", ROOT / "scripts" / "dive_preview.py"
)
dive_preview = importlib.util.module_from_spec(_preview_spec)
_preview_spec.loader.exec_module(dive_preview)


def test_each_table_status_rule(tmp_path):
    """The redesign's table statuses (queries.ts tableStatus), one per rule, on the preview's
    hand-written sources: paused near expiry, stale, rebuilding (with who started it), not
    enabled; a recovery's rebuilt, rebuilding, failed and queued; and Waiting for every
    table of a source whose sync stopped."""
    path = tmp_path / "cdc_ops.duckdb"
    with duckdb.connect() as con:
        con.execute(f"ATTACH '{path}' AS cdc_ops")
        deploy_dive.create_schema(con)
        con.execute("USE cdc_ops")
        con.execute(dive_preview.DEMO)
        con.execute(dive_preview.STATES)
        # down: its slot was lost; its live, caught-up table waits on the source
        con.execute(
            "INSERT INTO ingest_status BY NAME SELECT 'down' AS source_id, current_timestamp AS written_at,"
            " 1 AS epoch, 10 AS checkpoint_lsn, 'failed' AS status, 'SlotLost' AS error,"
            " current_timestamp AS last_failure_at, 'SlotLost' AS last_failure_error,"
            ' \'[{"v":1,"name":"s","epoch":1,"is_current":true,"wal_status":"lost"}]\' AS slots'
        )
        con.execute(
            "INSERT INTO apply_status BY NAME SELECT 'down' AS source_id, 1 AS relation_id,"
            " 'public' AS schema_name, 'a' AS table_name, 'live' AS state, 1 AS epoch, 10 AS applied_lsn"
        )
        # S2: the rules a lone row decides.
        con.execute(
            "INSERT INTO apply_status BY NAME SELECT 'tpcc' AS source_id, rel AS relation_id,"
            " 'public' AS schema_name, t AS table_name, 'live' AS state, 1 AS epoch,"
            " lsn AS applied_lsn, current_timestamp - INTERVAL (m) MINUTE AS applied_through_commit_ts,"
            " 1 AS applied_through_epoch, tlsn AS applied_through_lsn"
            " FROM (VALUES (41, 'unknown_time', 6000, 5000, 600),"  # pair isn't its position
            " (42, 'day_behind', 6000, 6000, 1500)) v(rel, t, lsn, tlsn, m)"
        )
        # nodur: ingest's durable pair isn't its checkpoint, so how far behind is unknown.
        con.execute(
            "INSERT INTO ingest_status BY NAME SELECT 'nodur' AS source_id,"
            " current_timestamp AS written_at, epoch_us(current_timestamp) AS status_as_of,"
            " 'drained' AS status, 1 AS epoch, 9000 AS checkpoint_lsn,"
            " current_timestamp AS durable_through_commit_ts, 1 AS durable_through_epoch,"
            " 8000 AS durable_through_lsn"
        )
        con.execute(
            "INSERT INTO apply_status BY NAME SELECT 'nodur' AS source_id, 1 AS relation_id,"
            " 'public' AS schema_name, 'b' AS table_name, 'live' AS state, 1 AS epoch,"
            " 5000 AS applied_lsn, current_timestamp - INTERVAL 3 HOUR AS applied_through_commit_ts,"
            " 1 AS applied_through_epoch, 5000 AS applied_through_lsn"
        )
        # farp: paused, its pin three days from expiry.
        con.execute(
            "INSERT INTO apply_status BY NAME SELECT 'farp' AS source_id, 7 AS relation_id,"
            " 'public' AS schema_name, 'p' AS table_name, 'paused' AS state, 1 AS epoch,"
            " current_timestamp - INTERVAL 1 HOUR AS held_since"
        )
        con.execute(
            "INSERT INTO compaction_status BY NAME SELECT 'farp' AS source_id,"
            ' current_timestamp AS written_at, \'[{"v":1,"kind":"table","relation_id":7,'
            "\"expires_at_us\":' || epoch_us(current_timestamp + INTERVAL 3 DAY) || '}]' AS pins"
        )
    _, rows = read(path, "tableStatus")
    got = {(r["source_id"], r["table_name"]): (r["status"], r["kind"]) for r in rows}
    assert got[("tpcc", "public.orders")] == ("needs_action", "paused")  # 9 h to expiry
    assert got[("tpcc", "public.history")] == ("stale", "stale")  # 52 min behind
    assert got[("tpcc", "public.stock")] == ("rebuilding", "rebuilding")
    assert got[("tpcc", "public.stock_audit")] == ("not_enabled", "not_enabled")
    assert got[("tpcc", "public.district")] == ("ok", "ok")
    assert got[("app", "public.users")] == ("ok", "ok")  # rebuilt: live at epoch 2
    assert got[("app", "public.events")] == ("rebuilding", "rebuilding")
    assert got[("app", "public.sessions")] == ("needs_action", "rebuild_failed")
    assert got[("app", "public.webhooks")] == ("queued", "queued")
    assert got[("down", "public.a")] == ("waiting", "waiting")
    assert got[("demo", "public.orders")] == ("ok", "ok")
    assert got[("tpcc", "public.unknown_time")] == ("ok", "ok")  # unknown is never stale
    assert got[("tpcc", "public.day_behind")] == ("needs_action", "stale")  # over 24 h
    assert got[("nodur", "public.b")] == ("ok", "ok")
    assert got[("farp", "public.p")] == ("paused", "paused")  # three days left
    by = {(r["source_id"], r["table_name"]): r for r in rows}
    for key in (("tpcc", "public.unknown_time"), ("nodur", "public.b")):
        assert by[key]["behind_s"] is None
    assert by[("farp", "public.p")]["expires_in_s"] > 2 * 86400
    stale = by[("tpcc", "public.history")]
    assert 52 * 60 - 5 <= stale["behind_s"] <= 52 * 60 + 5
    assert by[("tpcc", "public.stock")]["actor"] == "alice@example.com"
    assert by[("tpcc", "public.stock")]["actor_action"] == "resync"
    assert by[("tpcc", "public.orders")]["actor_action"] == "pause"
    assert by[("app", "public.sessions")]["detail"] == "OperationalError"
    # Last updated only when its (epoch, lsn) is the row's own position
    assert by[("tpcc", "public.district")]["updated_at_s"] is not None
    assert by[("down", "public.a")]["updated_at_s"] is None
    _, srcs = read(path, "sourceStatus")
    s = {r["source_id"]: r for r in srcs}
    assert s["tpcc"]["label"] == "tpcc" and s["tpcc"]["wal_pct"] == pytest.approx(62.0, abs=0.1)
    assert s["down"]["wal_status"] == "lost" and s["down"]["wal_pct"] is None
    assert s["demo"]["every_s"] == 120


def test_append_and_catalog_hold_states(tmp_path):
    """An insert-only table's states and facts, and the catalog-check and type holds,
    each decided from the writers' own stored texts."""
    from cdc_flight import catalog_checks

    path = tmp_path / "cdc_ops.duckdb"
    update = (
        "append_update: an UPDATE on an insert-only table; resync it (an exact re-snapshot, "
        "which holds again if the table keeps changing rows), quarantine its unit, start it "
        "again from the log, or remove the table"
    )
    retyped = (
        "column 'v' changed type (its source type, under the same mirror type: values may "
        "differ); resync, or start-from-log for an append table"
    )
    rows = [
        # (table, state, capture_mode, detail, generation_id)
        ("chg", "needs_action", "append", update, "g0"),
        (
            "del",
            "needs_action",
            "append",
            update.replace("update", "delete").replace("UPDATE", "DELETE"),
            "g0",
        ),
        (
            "part",
            "needs_action",
            "append",
            catalog_checks.APPEND_REASONS["partitions_changed"],
            "g0",
        ),
        ("pk", "needs_resync", None, catalog_checks.REASONS["pk_replaced"], "g0"),
        ("wait", "needs_resync", None, catalog_checks.REASONS["column_rewrite"], "g0"),
        ("typ", "needs_action", None, retyped, "g0"),
        ("facts", "live", "append", None, "g2facts0"),
        ("rfail", "live", "append", None, "g0"),
    ]
    with duckdb.connect() as con:
        con.execute(f"ATTACH '{path}' AS cdc_ops")
        deploy_dive.create_schema(con)
        con.execute("USE cdc_ops")
        con.execute(
            "INSERT INTO ingest_status BY NAME SELECT 'ap' AS source_id, current_timestamp AS written_at,"
            " 'drained' AS status, 1 AS epoch, 100 AS checkpoint_lsn,"
            ' \'[{"v":1,"relation_id":9,"table":"public.ident","reason":"append_identity",'
            '"detail":"public.ident: append mode needs REPLICA IDENTITY FULL",'
            '"held_since":"2026-11-11 10:00:00+00"}]\' AS holds'
        )
        for i, (t, state, mode, detail, gen) in enumerate(rows):
            con.execute(
                "INSERT INTO apply_status BY NAME SELECT 'ap' AS source_id, ? AS relation_id,"
                " 'public' AS schema_name, ? AS table_name, ? AS state, ? AS capture_mode, ? AS detail,"
                " 1 AS epoch, 100 AS applied_lsn, ? AS generation_id, current_timestamp AS held_since",
                [i + 10, t, state, mode, detail, gen],
            )
        con.execute(
            "UPDATE apply_status SET ignore_deletes = true, deletes_ignored = 3, last_truncate_lsn = 4294967336"
            " WHERE table_name = 'facts'"
        )
        # facts: started from the log at S (its own generation), replacing g1 (kept aside)
        con.execute(
            "INSERT INTO backfill_status BY NAME SELECT 'ap' AS source_id, g AS generation_id,"
            " 'public.facts' AS table_name, m AS mode, st AS state, pin AS pin_lsn,"
            " current_timestamp - INTERVAL (h) HOUR AS created_at, current_timestamp - INTERVAL (h) HOUR AS updated_at"
            " FROM (VALUES ('abcdef0123456789', 'append_snapshot', 'replaced', 50, 3),"
            " ('g2facts0', 'log_incomplete', 'published', 4294967296 + 80, 1)) v(g, m, st, pin, h)"
        )
        # rfail: a newer re-snapshot failed beside its live mirror; first: its first snapshot failed
        for t, g in (("rfail", "gfail"), ("first", "gfirst")):
            con.execute(
                "INSERT INTO backfill_status BY NAME SELECT 'ap' AS source_id, ? AS generation_id,"
                " ? AS table_name, 'append_snapshot' AS mode, 'failed' AS state, current_timestamp AS created_at",
                [g, f"public.{t}"],
            )
            con.execute(
                "INSERT INTO backfill_runs BY NAME SELECT 'ap' AS source_id, 'r' AS run_id, current_timestamp AS written_at,"
                " ? AS table_name, ? AS generation_id, 'failed' AS status,"
                " 'the snapshot copy was interrupted; start over' AS detail",
                [f"public.{t}", g],
            )
        # new: registered, its snapshot refused over the limit; wait: resync waits for ingest
        con.execute(
            "INSERT INTO apply_runs BY NAME SELECT 'ap' AS source_id, 'a1' AS run_id,"
            " 'public.new' AS table_name, current_timestamp AS written_at, 'awaiting_generation' AS status"
        )
        for t, why in (
            (
                "new",
                "is 2000000000 bytes, over the append snapshot limit 1073741824 "
                "(CDC_APPEND_SNAPSHOT_MAX_BYTES): use start-from-log, or raise the limit",
            ),
            ("wait", "ingest has not yet passed the source key change; run ingest, then retry"),
        ):
            con.execute(
                "INSERT INTO backfill_runs BY NAME SELECT 'ap' AS source_id, 'b' AS run_id, current_timestamp AS written_at,"
                " ? AS table_name, NULL AS generation_id, 'refused' AS status, ? AS detail",
                [f"public.{t}", f"public.{t}: {why}"],
            )
        # not_started rows (hold_code): one current, one stale (older than
        # the source's newest full snapshot, the run r2's)
        con.execute(
            "INSERT INTO apply_status BY NAME SELECT 'ns' AS source_id, rel AS relation_id, 'public' AS schema_name,"
            " t AS table_name, 'not_started' AS state, 'append' AS capture_mode, a AS as_of, r AS run_id"
            " FROM (VALUES (1, 'cur', 200, 'r2'), (2, 'old', 100, 'r1')) v(rel, t, a, r)"
        )
        con.execute(
            "INSERT INTO apply_runs BY NAME SELECT 'ns' AS source_id, r AS run_id, NULL AS table_name,"
            " current_timestamp - INTERVAL (m) MINUTE AS written_at, 'ok' AS status"
            " FROM (VALUES ('r1', 9), ('r2', 1)) v(r, m)"
        )
        # ns2: its newest run wrote no apply_status rows, so nothing reads as stale
        con.execute(
            "INSERT INTO apply_status BY NAME SELECT 'ns2' AS source_id, 1 AS relation_id, 'public' AS schema_name,"
            " 'only' AS table_name, 'not_started' AS state, 100 AS as_of, 'r1' AS run_id"
        )
        con.execute(
            "INSERT INTO apply_runs BY NAME SELECT 'ns2' AS source_id, 'r9' AS run_id, NULL AS table_name,"
            " current_timestamp AS written_at, 'ok' AS status"
        )
    _, got = read(path, "tableStatus")
    assert [r["kind"] for r in got if r["source_id"] == "ns2"] == ["not_started"]
    assert {r["table_name"]: r["kind"] for r in got if r["source_id"] == "ns"} == {
        "public.cur": "not_started"
    }
    by = {r["table_name"].split(".")[1]: r for r in got if r["source_id"] == "ap"}
    kinds = {t: (r["status"], r["kind"], r["reason"]) for t, r in by.items()}
    assert kinds == {
        "chg": ("needs_action", "append_changed", "append_update"),
        "del": ("needs_action", "append_changed", "append_delete"),
        "part": ("needs_action", "catalog_changed", "partitions_changed"),
        "pk": ("needs_action", "catalog_changed", "pk_replaced"),
        "wait": ("needs_action", "catalog_changed", "column_rewrite"),
        "typ": ("needs_action", "type_changed", "type_changed"),
        "ident": ("needs_action", "append_invalid", None),
        "new": ("needs_action", "not_started", None),
        "first": ("needs_action", "rebuild_failed", None),
        # live: no apply run rows here, so their source reads as stopped
        "facts": ("waiting", "waiting", None),
        "rfail": ("waiting", "waiting", None),
    }
    assert by["ident"]["detail"] == "public.ident: append mode needs REPLICA IDENTITY FULL"
    assert "over the append snapshot limit" in by["new"]["detail"]
    assert by["first"]["detail"] == "the snapshot copy was interrupted; start over"
    assert by["rfail"]["rebuild_error"] == "the snapshot copy was interrupted; start over"
    assert (
        by["wait"]["waits_for_ingest"]
        and "ingest has not yet passed" in by["wait"]["refused_detail"]
    )
    assert not by["pk"]["waits_for_ingest"] and by["pk"]["refused_detail"] is None
    f = by["facts"]
    assert (f["capture_mode"], f["ignore_deletes"], f["deletes_ignored"]) == ("append", True, 3)
    assert (f["last_truncate_lsn"], f["log_start_lsn"]) == ("1/28", "1/50")
    assert f["kept_aside"] == "public.facts__gen_abcdef01"


def test_a_held_tables_resync_reads_rebuilding(tmp_path):
    """A Needs action table with a resync running reads Rebuilding, keyed or insert-only
    (a keyed table used to keep its old hold, inviting a second Resync the Flight refuses).
    Without an active generation it stays held; a failed one leaves it held, with the error."""
    path = tmp_path / "cdc_ops.duckdb"
    retyped = (
        "column 'v' changed type (its source type, under the same mirror type: values may "
        "differ); resync, or start-from-log for an append table"
    )
    rows = [
        # (table, capture_mode, detail, newest generation's state or None)
        ("blk", None, "ConversionException: boom", "snapshotting"),
        ("typ", None, retyped, "catching_up"),
        ("chg", "append", "append_update: an UPDATE on an insert-only table", "snapshotting"),
        ("held", None, "ConversionException: boom", None),
        ("bad", None, "ConversionException: boom", "failed"),
    ]
    with duckdb.connect() as con:
        con.execute(f"ATTACH '{path}' AS cdc_ops")
        deploy_dive.create_schema(con)
        con.execute("USE cdc_ops")
        con.execute(
            "INSERT INTO ingest_status BY NAME SELECT 'k' AS source_id, current_timestamp AS written_at,"
            " 'drained' AS status, 1 AS epoch, 100 AS checkpoint_lsn"
        )
        for i, (t, mode, detail, gen) in enumerate(rows):
            con.execute(
                "INSERT INTO apply_status BY NAME SELECT 'k' AS source_id, ? AS relation_id,"
                " 'public' AS schema_name, ? AS table_name, 'needs_action' AS state, ? AS capture_mode,"
                " ? AS detail, 1 AS epoch, 100 AS applied_lsn, 'g0' AS generation_id,"
                " current_timestamp AS held_since",
                [i + 10, t, mode, detail],
            )
            if gen:
                con.execute(
                    "INSERT INTO backfill_status BY NAME SELECT 'k' AS source_id, ? AS generation_id,"
                    " ? AS table_name, 'snapshot' AS mode, ? AS state, current_timestamp AS created_at,"
                    " current_timestamp AS updated_at",
                    [f"g{t}", f"public.{t}", gen],
                )
        con.execute(
            "INSERT INTO backfill_runs BY NAME SELECT 'k' AS source_id, 'r' AS run_id, current_timestamp AS written_at,"
            " 'public.bad' AS table_name, 'gbad' AS generation_id, 'failed' AS status, 'copy interrupted' AS detail"
        )
    _, got = read(path, "tableStatus")
    by = {r["table_name"].split(".")[1]: r for r in got if r["source_id"] == "k"}
    assert {t: (r["status"], r["kind"]) for t, r in by.items()} == {
        "blk": ("rebuilding", "rebuilding"),
        "typ": ("rebuilding", "rebuilding"),
        "chg": ("rebuilding", "rebuilding"),
        "held": ("needs_action", "blocked"),
        "bad": ("needs_action", "blocked"),
    }
    assert by["bad"]["rebuild_error"] == "copy interrupted"


def test_the_dives_hold_texts_are_the_writers():
    """The stored detail texts the Dive matches (queries.ts) are the writers' own."""
    from cdc_flight import catalog_checks

    sql = SNAPSHOT["queries"]["tableStatus"]["all"]
    for text in [*catalog_checks.REASONS.values(), *catalog_checks.APPEND_REASONS.values()]:
        assert text.replace("'", "''") in sql
    src = Path(__file__).parents[1] / "src" / "cdc_flight"
    assert (
        'f"append_{op}: an {op.upper()} on an insert-only table'
        in (src / "append_apply.py").read_text()
    )
    assert (
        'f"column {column.name!r} changed type (its source type' in (src / "mirror.py").read_text()
    )
    assert "ingest has not yet passed the source key change" in (src / "backfill.py").read_text()
    for reason in ("append_identity", "append_partitioned"):
        assert f'"{reason}"' in (src / "ingest_common.py").read_text()


def test_every_hold_code_has_a_dive_state():
    """The Dive maps each apply_status.hold_code to a state (queries.ts HOLD_CODE_KIND)."""
    from cdc_flight.hold_codes import HOLD_CODES

    assert set(SNAPSHOT["hold_code_kind"]) == set(HOLD_CODES)


def test_a_hold_code_wins_over_the_stored_text(tmp_path):
    """A non-NULL hold_code decides the state; the stored texts are only the fallback for
    older writers (test_append_and_catalog_hold_states covers those)."""
    path = tmp_path / "cdc_ops.duckdb"
    with duckdb.connect() as con:
        con.execute(f"ATTACH '{path}' AS cdc_ops")
        deploy_dive.create_schema(con)
        con.execute("USE cdc_ops")
        con.execute(
            "INSERT INTO ingest_status BY NAME SELECT 'hc' AS source_id, current_timestamp AS written_at,"
            " 'drained' AS status, 1 AS epoch, 100 AS checkpoint_lsn"
        )
        con.execute(
            "INSERT INTO apply_status BY NAME SELECT 'hc' AS source_id, rel AS relation_id,"
            " 'public' AS schema_name, t AS table_name, st AS state, code AS hold_code, d AS detail,"
            " 1 AS epoch, 100 AS applied_lsn, current_timestamp AS held_since"
            " FROM (VALUES (1, 'del', 'needs_action', 'append_delete', 'some newer wording'),"
            " (2, 'poi', 'needs_action', 'poison', 'append_update: looks like an append hold'),"
            " (3, 'pk', 'needs_resync', 'pk_replaced', 'reworded'),"
            " (4, 'gone', 'needs_action', 'dropped', 'the source table was dropped'),"
            " (5, 'keyed', 'needs_action', 'append_keyed_frames', 'frames carry a key'),"
            " (6, 'old', 'needs_resync', 'epoch_changed', 'source epoch changed')) v(rel, t, st, code, d)"
        )
    _, rows = read(path, "tableStatus")
    got = {r["table_name"]: (r["kind"], r["reason"]) for r in rows if r["source_id"] == "hc"}
    assert got == {
        "public.del": ("append_changed", "append_delete"),
        "public.poi": ("blocked", "poison"),
        "public.pk": ("catalog_changed", "pk_replaced"),
        "public.gone": ("registry", "dropped"),
        "public.keyed": ("append_invalid", "append_keyed_frames"),
        "public.old": ("needs_resync", "epoch_changed"),
    }


def test_the_status_reads_an_ops_db_without_the_newer_columns(tmp_path):
    """An older writer's ops db lacks hold_code and the append columns (OPTIONAL_COLUMNS):
    the probe finds which exist, and the status reads absent ones as NULL, falling back to
    the stored texts, never a binder error that blanks every screen."""
    from cdc_flight import catalog_checks

    optional = SNAPSHOT["queries"]["opsColumns"]["all"]
    full, old = tmp_path / "full.duckdb", tmp_path / "old.duckdb"
    for path in (full, old):
        with duckdb.connect() as con:
            con.execute(f"ATTACH '{path}' AS cdc_ops")
            deploy_dive.create_schema(con)
    _, rows = read(full, "opsColumns")
    names = {r["col"] for r in rows}
    assert len(names) == 9  # every optional column is one the deploy creates
    with duckdb.connect() as con:
        con.execute(f"ATTACH '{old}' AS cdc_ops")
        con.execute("USE cdc_ops")
        for col in sorted(names):
            table, column = col.split(".")
            con.execute(f"ALTER TABLE {table} DROP COLUMN {column}")
        con.execute(
            "INSERT INTO ingest_status BY NAME SELECT 'od' AS source_id, current_timestamp AS written_at,"
            " 'drained' AS status, 1 AS epoch, 100 AS checkpoint_lsn"
        )
        con.execute(
            "INSERT INTO apply_status BY NAME SELECT 'od' AS source_id, rel AS relation_id,"
            " 'public' AS schema_name, t AS table_name, st AS state, d AS detail,"
            " 1 AS epoch, 100 AS applied_lsn, current_timestamp AS held_since"
            " FROM (VALUES (1, 'chg', 'needs_action', 'append_update: an UPDATE on an insert-only table'),"
            " (2, 'pk', 'needs_resync', ?)) v(rel, t, st, d)",
            [catalog_checks.REASONS["pk_replaced"]],
        )
    assert read(old, "opsColumns")[1] == []
    with pytest.raises(duckdb.BinderException):
        read(old, "tableStatus")
    _, rows = read(old, "tableStatusOld")
    got = {r["table_name"]: (r["kind"], r["reason"], r["capture_mode"]) for r in rows}
    assert got == {
        "public.chg": ("append_changed", "append_update", None),
        "public.pk": ("catalog_changed", "pk_replaced", None),
    }
    assert "duckdb_columns()" in optional


def test_the_preview_has_an_example_preview_per_action():
    """Each action's simulated dry run reads its own example `would` row (the preview), never
    another action's (user report: a resync preview showed pause's detail)."""
    assert set(SNAPSHOT["actions"]) <= set(dive_preview.WOULD)


@LOCAL
def test_a_read_replica_sources_diagnostics_read_as_advisory(scenario):
    """A read replica (docs/read-replica.md): only a source with `standby` is a read replica; its
    ages are taken at the observation; a heartbeat older than 3 x its interval (or none) is
    stale; no pg_read_all_stats reads "unknown" with no contact age."""
    s, con, h = scenario, scenario.con, scenario.h
    write_ingest(con, h, slots=[slot()])
    assert s.ops.read()["sourceStandby"] == []  # a primary source

    now = datetime.now(timezone.utc)
    iso = lambda ago: None if ago is None else (now - timedelta(seconds=ago)).isoformat()  # noqa: E731
    for beat, status, contact, stale in (
        (30, "streaming", 2, False),
        (900, "streaming", 2, True),
        (40, "unknown", None, False),
        (None, "streaming", 2, True),  # no marker row, or one that isn't a time
    ):
        standby = dict(
            replay_lsn=90000,
            receive_lsn=94096,
            replay_lag_bytes=4096,
            last_replay_commit_ts=iso(5),
            receiver_status=status,
            upstream_contact_ts=iso(contact),
            heartbeat_marker="x",
            heartbeat_ts=iso(beat),
            heartbeat_interval_s=60,
            observed_ts=iso(0),
        )
        write_ingest(con, h, slots=[slot()], standby=standby)
        (r,) = s.ops.read()["sourceStandby"]
        assert r["heartbeat_stale"] is stale, beat
        assert r["receiver_status"] == status
        assert r["contact_age_s"] == contact and r["heartbeat_age_s"] == beat
        assert (
            r["replay_lsn"] == "0/15F90"
            and r["replay_lag_bytes"] == 4096
            and r["replay_age_s"] == 5
        )


@LOCAL
def test_a_read_replica_heartbeat_job_reads_from_its_last_three_runs(scenario):
    """design-early-exit U0 (display only): the last 3 runs all capped with one marker reads
    `not_running` when the receiver streamed and its contact moved forward, `unknown` when
    contact was unknown; anything else (a drain, a new marker, contact standing still, not
    streaming, fewer runs, replay standing still with WAL waiting) reads nothing. Runs are
    ordered and spanned by written_at: a run that failed before its lease has no as_of."""
    s, con, h = scenario, scenario.con, scenario.h
    now = datetime.now(timezone.utc)
    clock = iter(range(0, 10**6, 9 * 60))  # each run 9 min after the last
    t0 = (
        con.execute("SELECT epoch_us(current_timestamp)").fetchone()[0] - 6 * 3600 * 10**6
    )  # inside the 7-day prune

    def run(
        status="capped", marker="m1", receiver="streaming", contact=None, lag=0, lsn=90000, **extra
    ):
        t = next(clock)
        standby = dict(
            replay_lsn=lsn,
            receive_lsn=lsn + lag,
            replay_lag_bytes=lag,
            last_replay_commit_ts=now.isoformat(),
            receiver_status=receiver,
            upstream_contact_ts=None
            if contact is None
            else (now + timedelta(seconds=contact)).isoformat(),
            heartbeat_marker=marker,
            heartbeat_ts=now.isoformat(),
            heartbeat_interval_s=60,
            observed_ts=now.isoformat(),
        )
        run_id = uuid.uuid4().hex
        fields = dict(
            slots=[slot()], status=status, as_of=t0 + t * 10**6, standby=standby, run_id=run_id
        )
        write_ingest(con, h, **(fields | extra))
        con.execute(  # the writer stamps written_at with current_timestamp: space the runs out
            "UPDATE cdc_ops.main.ingest_runs SET written_at = make_timestamptz(?) WHERE run_id = ?",
            [t0 + t * 10**6, run_id],
        )

    def job():
        (r,) = s.ops.read()["sourceStandby"]
        return r["heartbeat_job"], r["heartbeat_job_span_s"]

    run(contact=0)
    run(contact=1)
    assert job() == (None, None)  # 2 runs
    run(contact=2)
    assert job() == ("not_running", 18 * 60)
    run(status="drained", contact=3)
    assert job() == (None, None)  # a drain breaks the streak
    for c in (4, 5, 6):
        run(marker="m2" if c == 5 else "m1", contact=c)
    assert job() == (None, None)  # a new marker in the window
    for _ in range(3):
        run(contact=7)
    assert job() == (None, None)  # contact standing still: upstream, not the job
    for c in (8, 9, 10):
        run(receiver="stopping", contact=c)
    assert job() == (None, None)  # not streaming
    for _ in range(3):
        run(receiver="unknown")
    assert job() == ("unknown", 18 * 60)
    for _ in range(3):
        run(marker=None, contact=11)
    assert job() == (None, None)  # no marker row
    for c in (12, 13, 14):
        run(contact=c, lag=4096)
    assert job() == (None, None)  # replay standing still with WAL waiting: the marker can't move
    for c in (15, 16, 17):
        run(contact=c, lag=4096, lsn=90000 + c)
    assert job() == ("not_running", 18 * 60)  # replay moving, marker not
    run(status="failed", as_of=None, standby=None)
    assert job() == (None, None)  # S1: a run that failed before its lease (no as_of) is the newest


@LOCAL
def test_a_read_replica_slot_conflict_and_backfill_waits(scenario):
    """A slot invalidated by a recovery conflict carries its reason to the banner; the read
    replica's backfill waits map to health explicitly (waiting_for_primary needs the DBA)."""
    s, con, h = scenario, scenario.con, scenario.h
    write_ingest(con, h, slots=[slot(wal_status="lost", invalidated_by_conflict="rows_removed")])
    (src,) = [r for r in s.ops.read()["sourceStatus"] if r["source_id"] == SOURCE]
    assert src["wal_status"] == "lost" and src["slot_conflict"] == "rows_removed"
    for status, health in (
        ("waiting_for_replay", "ok"),
        ("waiting_past_replay", "ok"),
        ("waiting_for_primary", "attention"),
    ):
        con.execute(
            "INSERT INTO cdc_ops.main.backfill_runs BY NAME SELECT ? AS source_id, ? AS run_id,"
            " current_timestamp AS written_at, ? AS status",
            [SOURCE, uuid.uuid4().hex, status],
        )
        assert one(s, "backfill")["health"] == health, status
