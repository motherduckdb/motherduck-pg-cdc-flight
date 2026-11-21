"""Replica capture: a read replica source's diagnostics.
Advisory only: recorded in cdc_ops, never read for a decision."""

import json
import pathlib
import re
from datetime import datetime, timezone

import pytest
from test_compaction import Statements
from test_ops import ingest_config, observed, ops_db, row  # noqa: F401 (fixture)
from test_ops_v2 import POSITION, columns, v2_harness

from cdc_flight import ops_ingest
from cdc_flight.config import IngestConfig
from cdc_flight.ingest import STANDBY_READ, standby_diagnostics

SRC = pathlib.Path(__file__).resolve().parents[1] / "src" / "cdc_flight"
T = datetime(2026, 11, 16, 18, 0, tzinfo=timezone.utc)


def test_the_diagnostics_parse_every_field_and_label_the_clocks():
    got = standby_diagnostics((150, 170, T, "streaming", T, "2026-11-16 17:59:30.5+00", T), 60)
    assert got == dict(
        replay_lsn=150,
        receive_lsn=170,
        replay_lag_bytes=20,
        last_replay_commit_ts=T.isoformat(),
        receiver_status="streaming",
        upstream_contact_ts=T.isoformat(),
        heartbeat_marker="2026-11-16 17:59:30.5+00",
        heartbeat_ts="2026-11-16T17:59:30.500000+00:00",
        heartbeat_interval_s=60,
        observed_ts=T.isoformat(),
    )


def test_without_pg_read_all_stats_upstream_is_unknown_never_unreachable():
    """`pg_stat_wal_receiver`'s columns are NULL for a role without the
    privilege: reported unknown."""
    got = standby_diagnostics((150, 170, T, None, None, None, T), 60)
    assert got["receiver_status"] == "unknown" and got["upstream_contact_ts"] is None
    assert "unreachable" not in json.dumps(got)


@pytest.mark.parametrize("marker", ["garbage", "", None, 12, "2026-13-45 99:99:99+00"])
def test_a_garbage_or_missing_marker_is_reported_never_raised(marker):
    """The read must not fail B: no cast in SQL, best-effort parse here."""
    got = standby_diagnostics((150, None, None, None, None, marker, None), 60)
    assert got["heartbeat_ts"] is None and got["replay_lsn"] == 150
    assert got["replay_lag_bytes"] is None and got["observed_ts"] is None


def test_the_read_folds_into_b_with_no_cast_that_can_raise():
    """B is the statement's first column; the marker is a scalar subquery, uncast."""
    assert STANDBY_READ.startswith("SELECT pg_last_wal_replay_lsn()-'0/0'::pg_lsn,")
    assert "(SELECT marker FROM {} WHERE id=%s)" in STANDBY_READ
    assert "::timestamptz" not in STANDBY_READ and "::int" not in STANDBY_READ
    assert "pg_last_wal_receive_lsn()" in STANDBY_READ


def test_no_decision_reads_the_diagnostics():
    """Advisory: outside the ingest function that builds them and the ops writer,
    no module names a diagnostic field."""
    fields = re.compile(
        r"\b(replay_lag_bytes|upstream_contact_ts|heartbeat_ts|receiver_status|heartbeat_marker)\b"
    )
    users = {p.name for p in SRC.glob("*.py") if fields.search(p.read_text())}
    assert users == {"ingest.py"}, users
    ingest = (SRC / "ingest.py").read_text()
    body = ingest[ingest.index("def standby_diagnostics") :]
    body = body[: body.index("\ndef ", 10)]
    assert len(fields.findall(ingest)) == len(fields.findall(body))


def test_the_interval_key_is_validated_and_defaults_to_60():
    base = dict(heartbeat_writer="external")
    assert (
        IngestConfig("s", "s_slot", "s_pub", ("public.t",), **base).heartbeat_interval_seconds == 60
    )
    with pytest.raises(ValueError, match="CDC_HEARTBEAT_INTERVAL_SECONDS"):
        IngestConfig("s", "s_slot", "s_pub", ("public.t",), heartbeat_interval_seconds=0, **base)


STANDBY = standby_diagnostics((150, 170, T, None, None, "garbage", T), 60)


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_a_read_replica_runs_diagnostics_reach_status_and_history(stores, tmp_path, ops_db):  # noqa: F811
    h = v2_harness(stores, tmp_path)
    seen = observed(run_id="s1", as_of=200, standby=STANDBY, **POSITION)
    assert ops_ingest.report_ingest(ops_db, ingest_config(h), seen) == "written"
    assert {"standby_as_of", "standby"} <= columns(ops_db, "ingest_status")
    assert "standby" in columns(ops_db, "ingest_runs")
    status = row(ops_db, "ingest_status")[0]
    runs = {r["run_id"]: r for r in row(ops_db, "ingest_runs")}
    expected = dict(v=1, **STANDBY)
    assert json.loads(status["standby"]) == expected == json.loads(runs["s1"]["standby"])
    assert status["standby_as_of"] == 200


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_a_primary_sources_ops_statements_are_unchanged(stores, tmp_path, ops_db):  # noqa: F811
    """No `standby` key: the upsert and the history insert name no standby column, and the
    statement count is the same as before."""
    h = v2_harness(stores, tmp_path)
    ops_ingest.report_ingest(ops_db, ingest_config(h), observed(**POSITION))
    con = Statements(ops_db)
    ops_ingest.report_ingest(con, ingest_config(h), observed(run_id="2", as_of=101, **POSITION))
    written = [s for s in con.seen if "duckdb_" not in s]
    assert len(written) == 2 + ops_ingest.prune_window()
    assert not any("standby" in s for s in written)
    assert row(ops_db, "ingest_status")[0]["standby"] is None
