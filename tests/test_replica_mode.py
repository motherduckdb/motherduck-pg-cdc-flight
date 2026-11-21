"""Replica capture without PostgreSQL: the mode, its checks and refusals.

The PostgreSQL behaviour (a real standby) is tests/test_replica_capture.py's.
"""

import io
import json
import re
import sys
from contextlib import redirect_stdout
from pathlib import Path

import duckdb
import pytest

from cdc_flight import cli, delete_source_flight, epoch_flight, membership_flight
from cdc_flight.config import BackfillConfig, IngestConfig
from cdc_flight.format2 import UnitComplete
from cdc_flight.ingest import slot_health
from cdc_flight.ingest_backend import Backend2
from cdc_flight.ingest_common import REGISTRATION, SourceMismatch, check_mode
from cdc_flight.positions import PRIMARY, STANDBY
from cdc_flight.standby import (
    adopted_slot_warning,
    endpoint_refusal,
    primary_setup_sql,
    waiting_message,
)

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
import deploy_flights as df  # noqa: E402

UUID = "00000000-0000-0000-0000-000000000001"


def config(**values):
    return IngestConfig("s", "s_slot", "s_pub", ("public.t",), **values)


# The mode -----------------------------------------------------------------------------


def test_the_mode_defaults_to_self_and_is_validated():
    assert config().heartbeat_writer == "self"
    assert config(heartbeat_writer="external").heartbeat_writer == "external"
    with pytest.raises(ValueError, match="CDC_HEARTBEAT_WRITER"):
        config(heartbeat_writer="primary")
    for bad in (0, 3601):
        with pytest.raises(ValueError, match="CDC_SLOT_WAIT_SECONDS"):
            config(slot_wait_seconds=bad)
    assert config().slot_wait_seconds == 90
    with pytest.raises(ValueError, match="CDC_HEARTBEAT_WRITER"):
        BackfillConfig("s", ("public.t",), heartbeat_writer="x")


def test_the_mode_comes_from_the_environment(monkeypatch):
    monkeypatch.setenv("CDC_SOURCE_ID", "s")
    monkeypatch.setenv("CDC_PG_DSN", "host=h")
    monkeypatch.setenv("CDC_TABLES", '["public.t"]')
    assert IngestConfig.from_env().heartbeat_writer == "self"
    monkeypatch.setenv("CDC_HEARTBEAT_WRITER", "")  # a Flight's "" default = unset
    assert IngestConfig.from_env().heartbeat_writer == "self"
    monkeypatch.setenv("CDC_HEARTBEAT_WRITER", "external")
    monkeypatch.setenv("CDC_SLOT_WAIT_SECONDS", "30")
    loaded = IngestConfig.from_env()
    assert (loaded.heartbeat_writer, loaded.slot_wait_seconds) == ("external", 30)
    assert BackfillConfig.from_env().heartbeat_writer == "external"


def test_the_mode_is_not_in_the_binding():
    """Format 2 is unaffected: the binding is the same in both modes."""
    assert config().binding() == config(heartbeat_writer="external").binding()


def test_the_recorded_mode_must_match_and_null_is_self():
    check_mode(config(), None, None, None)  # a store from before the column
    check_mode(config(), "self", None, None)
    check_mode(config(heartbeat_writer="external"), "external", 1, 1)
    with pytest.raises(SourceMismatch, match="heartbeat writer changed: self -> external"):
        check_mode(config(heartbeat_writer="external"), None, None, 1)
    with pytest.raises(SourceMismatch, match="heartbeat writer changed: external -> self"):
        check_mode(config(), "external", 1, None)


def test_a_timeline_switch_holds_an_external_source():
    with pytest.raises(SourceMismatch, match=r"timeline changed \(1 -> 2\).*failover"):
        check_mode(config(heartbeat_writer="external"), "external", 1, 2)


def test_registration_reads_a_store_from_before_the_columns_as_self():
    """REGISTRATION reads the new columns through to_json: no migration, NULL = self."""
    con = duckdb.connect()
    con.execute("CREATE SCHEMA x")
    con.execute(
        "CREATE TABLE x.source (id INTEGER, binding VARCHAR, system_id VARCHAR, "
        "database_oid UBIGINT, epoch INTEGER, slot_name VARCHAR, lifecycle VARCHAR)"
    )
    con.execute(
        "CREATE TABLE x.tables (relation_id UINTEGER, key_columns VARCHAR, schema_name VARCHAR,"
        " table_name VARCHAR, held_reason VARCHAR, former_name VARCHAR, capture_mode VARCHAR)"
    )
    con.execute("INSERT INTO x.source VALUES (1,'{}','1',5,1,'slot',NULL)")
    statement = REGISTRATION.replace("{s}", "x.")
    old = con.execute(statement).fetchone()[0]
    assert (old["heartbeat_writer"], old["timeline_id"]) == (None, None)
    con.execute("ALTER TABLE x.source ADD COLUMN heartbeat_writer VARCHAR")
    con.execute("ALTER TABLE x.source ADD COLUMN timeline_id BIGINT")
    con.execute("UPDATE x.source SET heartbeat_writer='external', timeline_id=3")
    new = con.execute(statement).fetchone()[0]
    assert (new["heartbeat_writer"], new["timeline_id"]) == ("external", 3)


# Preflight's pure parts --------------------------------------------------------------


@pytest.mark.parametrize(
    "host",
    [
        "db.cluster-ro-abc123.us-east-1.rds.amazonaws.com",
        "db.cluster-abc123.us-east-1.rds.amazonaws.com",
        "db.cluster-custom-abc123.us-east-1.rds.amazonaws.com",
        "db.proxy-abc123.us-east-1.rds.amazonaws.com",
        "DB.CLUSTER-RO-ABC.US-EAST-1.RDS.AMAZONAWS.COM",
    ],
)
def test_reader_cluster_and_proxy_endpoints_are_refused(host):
    assert "instance's own endpoint" in endpoint_refusal(host)


@pytest.mark.parametrize(
    "host",
    [
        "db-ro.example.com",  # only RDS names are judged
        "replica-1.abc123.us-east-1.rds.amazonaws.com",  # an instance endpoint
        "10.0.0.5",
        "",
    ],
)
def test_instance_and_other_endpoints_are_allowed(host):
    assert endpoint_refusal(host) is None


def test_the_primary_setup_sql_is_designs_section_8():
    cfg = config(heartbeat_writer="external")
    text = primary_setup_sql(cfg, ["public.t", "sales.orders"])
    assert "on the primary" in text
    assert (
        "CREATE TABLE public.md_cdc_heartbeat (id text PRIMARY KEY, marker text NOT NULL)" in text
    )
    assert (
        "CREATE PUBLICATION s_pub FOR TABLE public.t, sales.orders, public.md_cdc_heartbeat" in text
    )
    assert "SELECT cron.schedule('md_cdc_heartbeat_s', '* * * * *'" in text
    assert "VALUES ('external:s', clock_timestamp()::text)" in text
    assert "ON CONFLICT (id) DO UPDATE SET marker = excluded.marker" in text
    assert "no INSERT or UPDATE" in text


def test_the_waiting_message_names_the_fix_and_its_limit():
    text = waiting_message(config(heartbeat_writer="external", slot_wait_seconds=90))
    assert "within 90 s" in text
    assert "heartbeat job" in text
    assert "SELECT pg_log_standby_snapshot();" in text
    assert "no transaction is open there" in text  # spike item 3


def test_an_adopted_slot_is_reported_with_its_drop_and_catalog_xmin_effect():
    """Coordinator's condition (2): a slot a stopped attempt left is reused, not owned."""
    text = adopted_slot_warning(config(heartbeat_writer="external"))
    assert "reusing the existing slot s_slot" in text
    assert "delete-source won't drop it" in text
    assert "catalog_xmin on the primary" in text
    assert "SELECT pg_drop_replication_slot('s_slot');" in text


class Rows:
    """A connection whose every query returns `rows`, recording the SQL."""

    def __init__(self, rows):
        self.rows, self.sql = rows, []

    def cursor(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, statement, params=None):
        self.sql.append(statement)
        self.description = True

    def fetchall(self):
        return self.rows


def test_slot_health_on_a_standby_holds_a_conflict_invalidated_slot_by_its_columns():
    healthy = ("pgoutput", "d", False, 10, 5, "reserved", None)
    con = Rows([(*healthy, "false", None)])
    assert slot_health(con, "s", 10, STANDBY)["confirmed"] == 10
    assert "to_jsonb(s)->>'conflicting'" in con.sql[0]
    lost = ("pgoutput", "d", False, 10, 5, "lost", None)
    with pytest.raises(SourceMismatch, match=r"recovery conflict \(rows_removed\)"):
        slot_health(Rows([(*lost, "true", "rows_removed")]), "s", 10, STANDBY)
    with pytest.raises(SourceMismatch, match=r"recovery conflict \(lost\)"):  # 16: no reason
        slot_health(Rows([(*lost, "true", None)]), "s", 10, STANDBY)


def test_slot_health_on_a_primary_reads_what_it_read_before():
    con = Rows([("pgoutput", "d", False, 10, 5, "reserved", None)])
    slot_health(con, "s", 10, PRIMARY)
    assert con.sql == [
        "SELECT plugin,database,active,confirmed_flush_lsn-'0/0'::pg_lsn,"
        "restart_lsn-'0/0'::pg_lsn,wal_status,safe_wal_size FROM pg_replication_slots "
        "WHERE slot_name=%s"
    ]


# The drain ---------------------------------------------------------------------------


def unit(end, marker=False):
    return UnitComplete(1, end - 10, end, 1, 1, 1, 1, "c", "d", 1, marker)


def test_external_drain_needs_a_unit_ending_strictly_after_B():
    """Rule U: `end > B`, not `>=` (replay can show a commit ending at B before the pointer
    reaches it, so a unit ending at B may already have been visible)."""
    backend = Backend2.__new__(Backend2)
    b = 1000
    assert not backend.drained([unit(b)], b)  # end == B: not drained
    assert not backend.drained([unit(b - 1)], b)
    assert backend.drained([unit(b + 1)], b)
    assert backend.drained(["not a unit", unit(b + 1)], b)


def test_self_mode_drain_is_the_marker_as_before():
    backend = Backend2.__new__(Backend2)
    assert backend.drained([unit(10, marker=True)])
    assert not backend.drained([unit(10_000)])  # no position rule in self mode


# Refusals ----------------------------------------------------------------------------


class NoStore:
    """cli.main's MotherDuck connections, stubbed: nothing is reached."""

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, *args):
        return self

    def close(self):
        pass


def test_a_timed_out_standby_slot_wait_exits_3_as_waiting_for_primary(monkeypatch):
    """`waiting_for_primary` exits 3 (an action Flight's busy), so the run
    shows FAILED; the status row and the printed line say waiting_for_primary."""
    from cdc_flight import bootstrap, ops_ingest
    from cdc_flight.standby import WaitingForPrimary

    for key, value in dict(
        CDC_SOURCE_ID="s",
        CDC_PG_DSN="host=h",
        CDC_TABLES='["public.t"]',
        CDC_HEARTBEAT_WRITER="external",
        MOTHERDUCK_TOKEN="test-not-a-token",
    ).items():
        monkeypatch.setenv(key, value)
    monkeypatch.setattr(sys, "argv", ["cdc-flight", "preflight"])
    monkeypatch.setattr(cli.duckdb, "connect", lambda *a, **k: NoStore())

    def wait(*a, **k):
        raise WaitingForPrimary("the replication slot s_slot was not created within 90 s")

    reported = []
    monkeypatch.setattr(bootstrap, "bootstrap_log", wait)
    monkeypatch.setattr(
        ops_ingest,
        "report_preflight",
        lambda con, config, run_id, status, **k: reported.append(status),
    )
    out = io.StringIO()
    with redirect_stdout(out), pytest.raises(SystemExit) as exited:
        cli.main()
    assert exited.value.code == 3
    line = json.loads(out.getvalue().splitlines()[-1])
    assert line["status"] == "waiting_for_primary" and "within 90 s" in line["message"]
    assert reported == ["waiting_for_primary"]


@pytest.mark.parametrize("module", [epoch_flight, delete_source_flight, membership_flight])
def test_owner_settings_carry_the_mode_into_the_ingest_config(module):
    extra = dict(backfill_flight=UUID) if module is membership_flight else {}
    owner = module.Owner(source_id="s", heartbeat_writer="external", **extra)
    assert owner.ingest_config().heartbeat_writer == "external"
    assert module.Owner(source_id="s", **extra).ingest_config().heartbeat_writer == "self"
    with pytest.raises(ValueError, match="CDC_HEARTBEAT_WRITER"):
        module.Owner(source_id="s", heartbeat_writer="x", **extra)


# Deploy: passed only for an external source --------------------------------------------


def source(tmp_path, writer=None):
    text = (ROOT / "docs/deploy_flights.example.toml").read_text()
    if writer:
        text = re.sub(r"(?m)^\[postgres\]\n", f'[postgres]\nheartbeat_writer = "{writer}"\n', text)
    path = tmp_path / "source.toml"
    path.write_text(text)
    return df.load_source(path)


def test_deploy_passes_the_mode_only_for_an_external_source(tmp_path):
    ids = dict(apply=UUID, backfill=UUID, compact=UUID)
    primary, standby = source(tmp_path), source(tmp_path, "external")
    assert primary.heartbeat_writer == "self"
    for command in ("preflight", "ingest", "backfill"):
        assert df.config_values(primary, command, ids)["CDC_HEARTBEAT_WRITER"] == ""
        assert df.config_values(standby, command, ids)["CDC_HEARTBEAT_WRITER"] == "external"
    for command in ("epoch", "delete-source", "table-action", "table-membership"):
        assert "heartbeat_writer" not in (df.owner_values(primary, command, ids) or {})
        assert df.owner_values(standby, command, ids)["heartbeat_writer"] == "external"
    with pytest.raises(df.SourceError, match="heartbeat_writer"):
        source(tmp_path, "primary")


def test_observed_slots_on_a_standby_carry_the_conflict_reason():
    """The reason is in the Dive's slot detail, not only the run's error."""
    from cdc_flight.ingest import observed_slots

    base = ("s", False, "lost", None, 10, 5, 20)
    con = Rows([(*base, "true", "rows_removed")])
    (slot,) = observed_slots(con, "s", "s", 1, role=STANDBY)
    assert slot["invalidated_by_conflict"] == "rows_removed"
    assert "to_jsonb(s)->>'invalidation_reason'" in con.sql[0]
    (slot,) = observed_slots(Rows([(*base, "true", None)]), "s", "s", 1, role=STANDBY)
    assert slot["invalidated_by_conflict"] == "lost"  # 16: no reason column
    (slot,) = observed_slots(Rows([(*base, "false", None)]), "s", "s", 1, role=STANDBY)
    assert "invalidated_by_conflict" not in slot
    primary = Rows([base])
    (slot,) = observed_slots(primary, "s", "s", 1)
    assert "invalidated_by_conflict" not in slot and "to_jsonb" not in primary.sql[0]


def test_standby_slot_creation_needs_an_autocommit_connection():
    """The SET after a cancelled creation needs no aborted transaction."""
    from cdc_flight.standby import create_standby_slot

    con = Rows([])
    con.autocommit = False
    with pytest.raises(RuntimeError, match="autocommit"):
        create_standby_slot(con, config(heartbeat_writer="external"))
    assert con.sql == []


def test_deploy_passes_the_heartbeat_interval_only_for_a_read_replica(tmp_path):
    ids = dict(apply=UUID, backfill=UUID, compact=UUID)
    text = (ROOT / "docs/deploy_flights.example.toml").read_text()

    def load(extra):
        path = tmp_path / "s.toml"
        path.write_text(re.sub(r"(?m)^\[postgres\]\n", "[postgres]\n" + extra, text))
        return df.load_source(path)

    replica = load('heartbeat_writer = "external"\nheartbeat_interval_seconds = 300\n')
    for command in ("preflight", "ingest"):
        assert df.config_values(replica, command, ids)["CDC_HEARTBEAT_INTERVAL_SECONDS"] == "300"
    default = load('heartbeat_writer = "external"\n')
    assert df.config_values(default, "ingest", ids)["CDC_HEARTBEAT_INTERVAL_SECONDS"] == ""
    with pytest.raises(df.SourceError, match="heartbeat_interval_seconds is for"):
        load("heartbeat_interval_seconds = 300\n")
    with pytest.raises(df.SourceError, match="1..86400"):
        load('heartbeat_writer = "external"\nheartbeat_interval_seconds = 90000\n')
