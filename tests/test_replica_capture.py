"""Replica capture against a real standby (compose profile `replica`).

Opt-in like tests/test_replica_spike.py: marked `replica`, skipped unless
CDC_TEST_REPLICA_DSNS maps a major to {"primary": dsn, "standby": dsn}. The DBA's side
(heartbeat table, publication, the heartbeat job's upsert) is done on the primary by the
test; production code connects to the standby only.
"""

import json
import os
import threading
import time
import uuid
from dataclasses import replace

import psycopg2
import pytest

from cdc_flight.bootstrap import bootstrap_log
from cdc_flight.config import IngestConfig
from cdc_flight.delete_source import preview_delete
from cdc_flight.ingest import run_once
from cdc_flight.pg_connect import PreflightError
from cdc_flight.preflight import check
from cdc_flight.runtime import _catalog
from cdc_flight.standby import WaitingForPrimary

pytestmark = pytest.mark.replica

PAIRS = json.loads(os.getenv("CDC_TEST_REPLICA_DSNS") or "{}")


def connect(dsn):
    con = psycopg2.connect(dsn)
    con.autocommit = True
    return con


def query(con, sql, params=None):
    with con.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall() if cur.description else None


@pytest.fixture(params=sorted(PAIRS) or ["none"])
def pair(request):
    if request.param == "none":
        pytest.skip("set CDC_TEST_REPLICA_DSNS (a primary + standby per major) to run")
    dsns = PAIRS[request.param]
    primary, standby = connect(dsns["primary"]), connect(dsns["standby"])
    suffix = uuid.uuid4().hex[:8]
    schema = f"rc_{suffix}"
    names = dict(
        schema=schema,
        source=f"rc_{suffix}",
        slot=f"rc_{suffix}_slot",
        publication=f"rc_{suffix}_pub",
        heartbeat=f"{schema}.pulse",
        table=f"{schema}.items",
    )
    query(primary, f"CREATE SCHEMA {schema}")
    try:
        yield dict(major=int(request.param), dsns=dsns, primary=primary, standby=standby, **names)
    finally:
        snapshot(primary)  # unblock a creation a failed test left waiting
        for _ in range(100):  # a closed walsender releases the slot a little later
            if not query(
                standby,
                "SELECT 1 FROM pg_replication_slots WHERE slot_name=%s AND active",
                [names["slot"]],
            ):
                break
            time.sleep(0.05)
        query(
            standby,
            "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
            "WHERE slot_name=%s AND NOT active",
            [names["slot"]],
        )
        query(primary, f"DROP PUBLICATION IF EXISTS {names['publication']}")
        query(primary, f"DROP SCHEMA {schema} CASCADE")
        primary.close()
        standby.close()


def snapshot(primary):
    query(primary, "SELECT pg_log_standby_snapshot()")


def replayed(pair):
    (lsn,) = query(pair["primary"], "SELECT pg_current_wal_lsn()")[0]
    for _ in range(200):
        if query(pair["standby"], "SELECT pg_last_wal_replay_lsn() >= %s::pg_lsn", [lsn])[0][0]:
            return
        time.sleep(0.05)
    raise AssertionError("the standby didn't replay the primary's WAL")


def dba_setup(pair, *, heartbeat=True, publication=True):
    """On the primary: the table, the heartbeat table and the publication."""
    primary = pair["primary"]
    query(primary, f"CREATE TABLE {pair['table']} (id int PRIMARY KEY, v text)")
    query(primary, f"ALTER TABLE {pair['table']} REPLICA IDENTITY FULL")
    if heartbeat:
        query(
            primary, f"CREATE TABLE {pair['heartbeat']} (id text PRIMARY KEY, marker text NOT NULL)"
        )
    if publication:
        listed = pair["table"] + (f", {pair['heartbeat']}" if heartbeat else "")
        query(
            primary,
            f"CREATE PUBLICATION {pair['publication']} FOR TABLE {listed} "
            "WITH (publish_via_partition_root=true)",
        )
    replayed(pair)


def heartbeat_job(pair):
    """What the DBA's job does once: the `external:<source>` upsert."""
    query(
        pair["primary"],
        f"INSERT INTO {pair['heartbeat']} (id, marker) VALUES (%s, clock_timestamp()::text) "
        "ON CONFLICT (id) DO UPDATE SET marker = excluded.marker",
        [f"external:{pair['source']}"],
    )


def config(pair, stores, *, writer="external", standby=True, **values):
    return IngestConfig(
        pair["source"],
        pair["slot"],
        pair["publication"],
        (pair["table"],),
        heartbeat=pair["heartbeat"],
        heartbeat_writer=writer,
        database=stores[0].database.strip('"'),
        dsn=pair["dsns"]["standby" if standby else "primary"],
        allow_local=True,
        flush_seconds=0.1,
        **values,
    )


class Snapshots:
    """The primary logs a standby snapshot every 0.2 s while open (stands in for the
    heartbeat job's writes), so a standby slot creation completes."""

    def __init__(self, pair):
        self.dsn, self.stop = pair["dsns"]["primary"], threading.Event()
        self.thread = threading.Thread(target=self.run, daemon=True)

    def run(self):
        con = connect(self.dsn)
        try:
            while not self.stop.wait(0.2):
                snapshot(con)
        finally:
            con.close()

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *exc):
        self.stop.set()
        self.thread.join(5)


def slots(pair):
    return [
        r[0]
        for r in query(
            pair["standby"],
            "SELECT slot_name FROM pg_replication_slots WHERE slot_name=%s",
            [pair["slot"]],
        )
    ]


def source_row(log):
    return log.execute(
        "SELECT heartbeat_writer,timeline_id,created_slot,created_publication,created_heartbeat "
        "FROM {s}source WHERE id=1"
    ).fetchone()


# Role and mode --------------------------------------------------------------------------


def test_self_mode_on_a_standby_and_external_on_a_primary_are_refused(pair, stores):
    dba_setup(pair)
    with pytest.raises(PreflightError, match="CDC_HEARTBEAT_WRITER=external"):
        check(config(pair, stores, writer="self"))
    with pytest.raises(PreflightError, match="is for a standby"):
        check(config(pair, stores, standby=False))
    assert slots(pair) == []


def test_a_missing_heartbeat_or_publication_prints_the_primary_sql_and_creates_nothing(
    pair, stores
):
    dba_setup(pair, heartbeat=False, publication=False)
    cfg = config(pair, stores)
    with pytest.raises(PreflightError) as refused:
        bootstrap_log(stores[0].con, cfg, create_objects=True)
    text = str(refused.value)
    assert "missing heartbeat table, publication" in text
    assert "on the primary" in text and "cron.schedule" in text
    assert f"'external:{pair['source']}'" in text
    assert slots(pair) == []
    assert not _catalog(stores[0].con, cfg)  # no bootstrap marker either


# Bootstrap: only the slot, bounded ------------------------------------------------------


def test_external_preflight_creates_only_the_slot_and_records_the_mode(pair, stores):
    dba_setup(pair)
    with Snapshots(pair):
        log, info = bootstrap_log(stores[0].con, config(pair, stores), create_objects=True)
    assert info.created == {"slot": True}  # never the heartbeat or publication
    assert slots(pair) == [pair["slot"]]
    (timeline,) = query(pair["standby"], "SELECT timeline_id FROM pg_control_checkpoint()")[0]
    assert source_row(log) == ("external", timeline, True, False, False)
    assert not any("reusing the existing slot" in w for w in info.warnings)
    # delete-source owns and drops only the standby slot; the rest is the DBA's, on the
    # primary.
    preview = preview_delete(log.config, log)
    assert preview["drop"] == [dict(kind="slot", name=pair["slot"])]
    assert preview["dba"][0].startswith(
        f"on the primary: SELECT cron.unschedule('md_cdc_heartbeat_{pair['source']}');"
    )
    assert any(d.startswith("on the primary: DROP PUBLICATION") for d in preview["dba"])
    assert any(d.startswith("on the primary: DROP TABLE") for d in preview["dba"])


def test_a_bounded_slot_wait_is_retryable_and_leaves_nothing(pair, stores):
    """F1 + S3 + option A: with a primary transaction open, creation can't finish; the
    wait ends as WaitingForPrimary, with no slot and no store marker, and the same
    command then succeeds once the transaction ends."""
    dba_setup(pair)
    cfg = config(pair, stores, slot_wait_seconds=2)
    open_tx = psycopg2.connect(pair["dsns"]["primary"])
    try:
        query(open_tx, f"INSERT INTO {pair['table']} VALUES (1,'open')")  # uncommitted
        snapshot(pair["primary"])
        started = time.monotonic()
        with pytest.raises(WaitingForPrimary, match="pg_log_standby_snapshot"):
            bootstrap_log(stores[0].con, cfg, create_objects=True)
        assert time.monotonic() - started < 15
    finally:
        open_tx.rollback()
        open_tx.close()
    assert slots(pair) == []  # S3: a cancelled creation leaves no slot
    assert not _catalog(stores[0].con, cfg)  # nothing registered: plainly retryable
    # The retry with the default wait: another test's primary transaction (the suite
    # runs in parallel) may hold creation for a few seconds more.
    with Snapshots(pair):
        log, info = bootstrap_log(
            stores[0].con, replace(cfg, slot_wait_seconds=90), create_objects=True
        )
    assert info.created == {"slot": True} and source_row(log)[2] is True


def test_a_slot_left_before_the_marker_is_reused_reported_and_not_owned(pair, stores):
    """Option A's crash window : a run that
    stops between the slot and the marker; the retry reuses the slot, says so with its
    drop and catalog_xmin effect, and delete-source leaves its DROP to the DBA."""
    dba_setup(pair)
    cfg = config(pair, stores)

    def stop(stage):
        if stage == "after_standby_slot":
            raise RuntimeError("stopped")

    with Snapshots(pair), pytest.raises(RuntimeError, match="stopped"):
        bootstrap_log(stores[0].con, cfg, create_objects=True, fault=stop)
    assert slots(pair) == [pair["slot"]] and not _catalog(stores[0].con, cfg)
    log, info = bootstrap_log(stores[0].con, cfg, create_objects=True)
    assert info.created == {}
    (warning,) = [w for w in info.warnings if "reusing the existing slot" in w]
    assert "catalog_xmin on the primary" in warning
    assert f"pg_drop_replication_slot('{pair['slot']}')" in warning
    assert source_row(log)[2] is not True  # created_slot unset: not ours
    preview = preview_delete(log.config, log)
    assert preview["drop"] == []
    assert f"SELECT pg_drop_replication_slot('{pair['slot']}');" in preview["dba"]


# Ingest: drain past B -------------------------------------------------------------------


def bootstrapped(pair, stores, **values):
    dba_setup(pair)
    with Snapshots(pair):
        log, info = bootstrap_log(
            stores[0].con, config(pair, stores, **values), create_objects=True
        )
    return log, info


def test_an_external_run_drains_at_the_first_unit_ending_after_B(pair, stores):
    log, info = bootstrapped(pair, stores, runtime_seconds=60)
    cfg = log.config
    query(pair["primary"], f"INSERT INTO {pair['table']} SELECT i,'x' FROM generate_series(1,5) i")
    replayed(pair)
    before = log.checkpoint()

    def beat(stage):  # the heartbeat job's next upsert, after B was read
        if stage == "after_start":
            heartbeat_job(pair)

    heartbeats = query(pair["primary"], f"SELECT count(*) FROM {pair['heartbeat']}")[0][0]
    started = time.monotonic()
    result = run_once(cfg, log, info=info, fault=beat)
    assert result.status == "drained", result
    assert result.boundary > before
    assert time.monotonic() - started < 30  # drained, not capped at 60 s
    # The run wrote nothing upstream: only the job's row exists (no `<source>` row).
    rows = query(pair["primary"], f"SELECT id FROM {pair['heartbeat']}")
    assert rows == [(f"external:{pair['source']}",)] and heartbeats == 0
    # A second run re-checks the registration (mode, timeline) and drains again.
    again = run_once(cfg, log, fault=beat)
    assert again.status == "drained" and again.boundary > result.boundary


def test_an_external_run_with_nothing_past_B_caps_with_the_checkpoint_unmoved(pair, stores):
    log, info = bootstrapped(pair, stores, runtime_seconds=3)
    cfg = log.config

    def beat(stage):
        if stage == "after_start":
            heartbeat_job(pair)

    run_once(cfg, log, info=info, fault=beat)  # consume anything from setup
    before = log.checkpoint()
    result = run_once(cfg, log)  # the job is "dead": nothing on the primary
    assert result.status == "capped", result
    assert log.checkpoint() == before


def test_a_registered_external_source_refuses_a_change_of_mode(pair, stores):
    log, info = bootstrapped(pair, stores)
    with pytest.raises(Exception, match="heartbeat writer changed: external -> self"):
        log.register(info.identity, info.initial_lsn, info.tables, timeline=info.timeline)
        type(log)(log.con, replace(log.config, heartbeat_writer="self")).register(
            info.identity, info.initial_lsn, info.tables
        )
    with pytest.raises(Exception, match="timeline changed"):
        type(log)(log.con, log.config).register(
            info.identity, info.initial_lsn, info.tables, timeline=info.timeline + 1
        )


def test_diagnostics_from_an_unprivileged_role_say_unknown_and_survive_a_garbage_marker(
    pair, stores
):
    """Without pg_read_all_stats the receiver's status and contact are
    unknown (not unreachable); a garbage heartbeat marker still lets B and the run
    proceed. With a privileged role they're read."""
    import psycopg2.extensions

    log, info = bootstrapped(pair, stores, runtime_seconds=60)
    role = f"rc_{pair['source']}_ro"
    password = uuid.uuid4().hex  # local fixture only
    query(pair["primary"], f"CREATE ROLE {role} LOGIN REPLICATION PASSWORD %s", [password])
    try:
        for grant in (
            f"GRANT USAGE ON SCHEMA {pair['schema']} TO {role}",
            f"GRANT SELECT ON {pair['table']}, {pair['heartbeat']} TO {role}",
        ):
            query(pair["primary"], grant)
        query(
            pair["primary"],
            f"INSERT INTO {pair['heartbeat']} VALUES (%s,'garbage') "
            "ON CONFLICT (id) DO UPDATE SET marker=excluded.marker",
            [f"external:{pair['source']}"],
        )
        replayed(pair)
        options = psycopg2.extensions.parse_dsn(pair["dsns"]["standby"])
        options.update(user=role, password=password)
        dsn = " ".join(f"{k}={v}" for k, v in options.items())
        unprivileged = replace(log.config, dsn=dsn)

        def beat(stage):
            if stage == "after_start":
                heartbeat_job(pair)

        observe = {}
        result = run_once(unprivileged, log, info=info, fault=beat, observe=observe)
        assert result.status == "drained", result
        seen = observe["standby"]
        assert seen["receiver_status"] == "unknown" and seen["upstream_contact_ts"] is None
        assert seen["heartbeat_marker"] == "garbage" and seen["heartbeat_ts"] is None
        assert seen["replay_lsn"] is not None and seen["replay_lag_bytes"] is not None
        observe = {}
        assert run_once(log.config, log, fault=beat, observe=observe).status == "drained"
        assert observe["standby"]["receiver_status"] == "streaming"  # superuser: visible
        assert observe["standby"]["upstream_contact_ts"] is not None
        assert observe["standby"]["heartbeat_ts"] is not None  # the job's real marker
    finally:
        query(pair["primary"], f"DROP OWNED BY {role}")
        query(pair["primary"], f"DROP ROLE {role}")
