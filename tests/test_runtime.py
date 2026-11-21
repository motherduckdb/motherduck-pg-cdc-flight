"""The public construction boundary never guesses or mutates a legacy store."""

import json
from dataclasses import replace

import duckdb
import pytest
from test_format2_protocol import binding, config
from test_phase1 import pg_phase1  # noqa: F401 (fixture)

from cdc_flight import bootstrap as provisioning
from cdc_flight import runtime
from cdc_flight.format2 import UnitConfig
from cdc_flight.ingest_log import SourceMismatch
from cdc_flight.preflight import SourceInfo, connection_options


@pytest.fixture
def database():
    with duckdb.connect() as con:
        yield con, replace(config(), database="memory")


def fresh(con, cfg):
    log = runtime.open_log(
        con,
        cfg,
        create=True,
        binding=binding(),
        unit_config=UnitConfig(max_commits=7),
        server_major=18,
    )
    log.register(("system", 1), 90, [])
    return log


def test_open_is_read_only_and_reconstructs_recorded_config(database):
    con, cfg = database
    created = fresh(con, cfg)
    opened = runtime.open_log(con, replace(cfg, dsn="dbname=control_client"))
    assert opened.config.text_binding == created.config.text_binding
    assert opened.config.unit_config == UnitConfig(max_commits=7)
    assert opened.config.server_major == 18
    assert opened.config.dsn == "dbname=control_client"
    assert opened.config.batch_bytes == cfg.batch_bytes
    assert con.execute("SELECT count(*) FROM f2.source").fetchone() == (1,)
    assert "batches" not in opened.catalog()


@pytest.mark.parametrize("legacy", [False, True])
def test_open_refuses_untagged_store_before_any_creation(database, legacy):
    con, cfg = database
    if legacy:
        con.execute("CREATE SCHEMA f2; CREATE TABLE f2.source (id INTEGER)")
    before = con.execute("SELECT table_name FROM duckdb_tables() ORDER BY table_name").fetchall()
    with pytest.raises(SourceMismatch, match="fresh store"):
        runtime.open_log(con, cfg)
    assert (
        con.execute("SELECT table_name FROM duckdb_tables() ORDER BY table_name").fetchall()
        == before
    )
    with (
        pytest.raises(SourceMismatch, match="fresh store")
        if legacy
        else pytest.raises(ValueError, match="bootstrap requires")
    ):
        runtime.open_log(con, cfg, create=True)
    assert (
        con.execute("SELECT table_name FROM duckdb_tables() ORDER BY table_name").fetchall()
        == before
    )


@pytest.mark.parametrize(
    "mutation,reason",
    [
        ("UPDATE f2.protocol_v2 SET format_version=1", "unsupported source"),
        ("UPDATE f2.protocol_v2 SET min_reader=3", "unsupported source"),
        ("DELETE FROM f2.protocol_v2", "missing or ambiguous"),
        ("DELETE FROM f2.unit_configs_v2", "immutable unit"),
        ("UPDATE f2.unit_configs_v2 SET binding_hash='wrong'", "immutable unit"),
        ("UPDATE f2.protocol_v2 SET binding_json='{}'", "stored text binding"),
        ("UPDATE f2.unit_configs_v2 SET config_json='{}'", "immutable unit"),
    ],
)
def test_open_fails_closed_on_unknown_or_corrupt_protocol(database, mutation, reason):
    con, cfg = database
    fresh(con, cfg)
    con.execute(mutation)
    with pytest.raises(SourceMismatch, match=reason):
        runtime.open_log(con, cfg)
    assert con.execute("SELECT count(*) FROM f2.source").fetchone() == (1,)


def test_open_does_not_substitute_binary_config_for_recorded_revision(database):
    con, cfg = database
    fresh(con, cfg)
    row = con.execute("SELECT config_json FROM f2.unit_configs_v2").fetchone()[0]
    changed = json.loads(row)
    changed["revision"] = 9
    con.execute("UPDATE f2.unit_configs_v2 SET revision=9,config_json=?", [json.dumps(changed)])
    con.execute("UPDATE f2.protocol_v2 SET active_revision=9")
    assert runtime.open_log(con, cfg).config.unit_config.revision == 9
    con.execute("UPDATE f2.protocol_v2 SET active_revision=8")
    with pytest.raises(SourceMismatch, match="immutable unit"):
        runtime.open_log(con, cfg)


def test_bootstrap_legacy_refusal_precedes_postgresql_io(database, monkeypatch):
    con, cfg = database
    con.execute("CREATE SCHEMA f2; CREATE TABLE f2.source (id INTEGER)")

    def forbidden(*args, **kwargs):
        pytest.fail("legacy bootstrap reached PostgreSQL")

    monkeypatch.setattr(provisioning, "source_binding", forbidden)
    with pytest.raises(SourceMismatch, match="fresh store"):
        provisioning.bootstrap_log(con, cfg, create_objects=True)


def test_bootstrap_records_the_preflight_slot_position_and_ownership(database, monkeypatch):
    from cdc_flight import preflight

    con, cfg = database
    calls = []
    monkeypatch.setattr(provisioning, "source_binding", lambda cfg: (binding(), 13))
    info = SourceInfo(
        ("system", 1),
        98765,
        [dict(oid=101, schema="public", name="items", keys=["id"], empty=False)],
        900,
        [],
        created={"slot": True},
    )

    def check(configured, **kwargs):
        if kwargs.get("rehearse"):
            return None  # the fresh store's rehearsal (preflight.check, rehearse=True)
        calls.append((configured.server_major, kwargs["create_objects"]))
        assert configured.text_binding == binding()
        return info

    monkeypatch.setattr(preflight, "check", check)
    log, result = provisioning.bootstrap_log(con, cfg, create_objects=True)
    assert result is info
    assert calls == [(13, True)]
    assert log.checkpoint() == 98765
    assert log.execute("SELECT start_lsn FROM {s}source_epochs").fetchall() == [(98765,)]
    assert log.execute(
        "SELECT coalesce(created_slot,false),coalesce(created_publication,false),coalesce(created_heartbeat,false) FROM {s}source"
    ).fetchone() == (
        True,
        False,
        False,
    )


def test_connection_options_pin_the_closed_text_contract_after_dsn_options():
    cfg = config(dsn="dbname=test options='-c TimeZone=Europe/Paris -c statement_timeout=123'")
    value = connection_options(cfg)["options"]
    assert value.startswith("-c TimeZone=Europe/Paris -c statement_timeout=123 ")
    assert value.endswith("-c client_encoding=UTF8")
    assert value.index("-c TimeZone=UTC") > value.index("-c TimeZone=Europe/Paris")
    assert "DateStyle=ISO,\\ YMD" in value
    assert "server_encoding=" not in value


def test_binding_is_observed_on_the_real_replication_session(pg_dsn, pg_version):
    from cdc_flight.text_settings import DEFAULT_SETTINGS

    cfg = config(dsn=pg_dsn, allow_local=True)
    observed, major = provisioning.source_binding(cfg)
    assert major == pg_version // 10000
    assert dict(observed.settings) == dict(DEFAULT_SETTINGS)
    assert ("plpgsql", "1.0") in observed.extensions


@pytest.mark.parametrize(
    "stage", ["after_initialize", "after_objects_created", "before_register", "after_register"]
)
def test_partial_bootstrap_never_silently_adopts_or_loses_ownership(database, monkeypatch, stage):
    from cdc_flight import preflight

    con, cfg = database
    calls = []
    monkeypatch.setattr(provisioning, "source_binding", lambda cfg: (binding(), 16))

    def check(configured, **kwargs):
        if kwargs.get("rehearse"):
            return None  # the fresh store's rehearsal (preflight.check, rehearse=True)
        calls.append("check")
        created = {} if len(calls) > 1 else {"slot": True, "publication": True}
        if created:
            kwargs["fault"]("after_objects_created")
        return SourceInfo(("system", 1), 98765, [], 900, [], created=created)

    monkeypatch.setattr(preflight, "check", check)

    def fail(current):
        if current == stage:
            raise RuntimeError(stage)

    with pytest.raises(RuntimeError, match=stage):
        provisioning.bootstrap_log(con, cfg, create_objects=True, fault=fail)
    if stage == "after_register":
        log, _ = provisioning.bootstrap_log(con, cfg, create_objects=True)
        assert log.execute("SELECT created_slot,created_publication FROM {s}source").fetchone() == (
            True,
            True,
        )
        assert log.checkpoint() == 98765
    else:
        before = len(calls)
        with pytest.raises(SourceMismatch, match="incomplete bootstrap: recreate") as caught:
            provisioning.bootstrap_log(con, cfg, create_objects=True)
        assert cfg.slot in str(caught.value) and cfg.publication in str(caught.value)
        assert "pg_replication_slots" in str(caught.value)
        assert "retains WAL" in str(caught.value)
        assert len(calls) == before
        assert con.execute("SELECT count(*) FROM f2.source").fetchone() == (0,)


def test_concurrent_bootstrap_loser_never_provisions_or_claims_objects(tmp_path, monkeypatch):
    import threading
    from concurrent.futures import ThreadPoolExecutor

    from cdc_flight import preflight

    path = tmp_path / "bootstrap_race.duckdb"
    barrier = threading.Barrier(2)
    calls = []
    cfg = replace(config(), database="bootstrap_race")

    def capture(configured):
        barrier.wait(timeout=10)
        return binding(), 16

    def check(configured, **kwargs):
        if kwargs.get("rehearse"):
            return None  # the fresh store's rehearsal (preflight.check, rehearse=True)
        calls.append("provision")
        return SourceInfo(("system", 1), 98765, [], 900, [], created={"slot": True})

    monkeypatch.setattr(provisioning, "source_binding", capture)
    monkeypatch.setattr(preflight, "check", check)
    connections = [duckdb.connect(str(path)), duckdb.connect(str(path))]

    def start(con):
        try:
            return provisioning.bootstrap_log(con, cfg, create_objects=True)
        except (SourceMismatch, duckdb.Error) as exc:
            return exc

    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            outcomes = list(pool.map(start, connections))
        assert sum(isinstance(outcome, tuple) for outcome in outcomes) == 1
        assert calls == ["provision"]
        assert connections[0].execute("SELECT token,created_slot FROM f2.source").fetchall() == [
            (0, True)
        ]
    finally:
        for con in connections:
            con.close()


@pytest.mark.postgres
def test_retained_slot_bootstrap_records_actual_anchor_and_starts_explicitly(
    stores,
    pg_phase1,  # noqa: F811
    monkeypatch,
):
    from cdc_flight import ingest, preflight
    from cdc_flight.replication import ReplicationStream

    source, cfg = pg_phase1
    original = preflight.check(cfg, create_objects=True).initial_lsn
    source.execute("INSERT INTO items VALUES (1,'retained','old')")
    flushed = source.execute("SELECT pg_current_wal_flush_lsn()::text")[0][0]
    source.execute("SELECT pg_replication_slot_advance(%s,%s::pg_lsn)", [cfg.slot, flushed])
    actual = source.execute(
        "SELECT confirmed_flush_lsn-'0/0'::pg_lsn FROM pg_replication_slots WHERE slot_name=%s",
        [cfg.slot],
    )[0][0]
    assert actual > original
    log, _ = provisioning.bootstrap_log(stores[0].con, cfg)
    assert log.checkpoint() == actual
    assert log.epoch_record(1)["start"] == actual
    assert log.execute("SELECT created_slot FROM {s}source").fetchone() == (False,)
    starts = []

    class Tracked(ReplicationStream):
        def start(self, slot, start_lsn, publication):
            starts.append(start_lsn)
            return super().start(slot, start_lsn, publication)

    monkeypatch.setattr(ingest, "ReplicationStream", Tracked)
    assert ingest.run_once(log.config, log).status == "drained"
    assert starts == [actual]


@pytest.mark.postgres
def test_bootstrap_and_stream_override_noncanonical_dsn_output_options(
    stores,
    pg_phase1,  # noqa: F811
):
    from psycopg2.extensions import make_dsn, parse_dsn

    from cdc_flight.ingest import run_once

    _, cfg = pg_phase1
    values = parse_dsn(cfg.dsn)
    values["options"] = "-c TimeZone=America/New_York -c extra_float_digits=0"
    cfg = replace(cfg, dsn=make_dsn(**values))
    log, _ = provisioning.bootstrap_log(stores[0].con, cfg, create_objects=True)
    settings = dict(log.config.text_binding.settings)
    assert settings["TimeZone"] == "UTC"
    assert settings["extra_float_digits"] == "3"
    result = run_once(log.config, log)
    assert result.status == "drained" and result.boundary > log.epoch_record(1)["start"]


def test_read_only_open_refuses_incomplete_bootstrap_and_wrong_owner_before_mutation(database):
    con, cfg = database
    log = runtime.open_log(
        con, cfg, create=True, binding=binding(), unit_config=UnitConfig(), server_major=16
    )
    before = log.catalog()
    with pytest.raises(SourceMismatch, match="incomplete bootstrap"):
        runtime.open_log(con, cfg)
    assert log.catalog() == before and "previews" not in before
    log.register(("system", 1), 90, [])
    with pytest.raises(SourceMismatch, match="identity/config mismatch"):
        runtime.open_log(con, replace(cfg, publication="wrong_pub"))
    assert log.catalog() == before


def test_missing_objects_probe_does_not_poison_the_following_explicit_bootstrap(
    database, monkeypatch
):
    from cdc_flight import preflight

    con, cfg = database
    monkeypatch.setattr(provisioning, "source_binding", lambda cfg: (binding(), 16))
    calls = []

    def check(configured, **kwargs):
        if kwargs.get("rehearse"):
            return None  # the fresh store's rehearsal (preflight.check, rehearse=True)
        create = kwargs.get("create_objects", False)
        calls.append(create)
        if not create:
            raise preflight.PreflightError("source objects missing")
        return SourceInfo(("system", 1), 90, [], 900, [], created={"slot": True})

    monkeypatch.setattr(preflight, "check", check)
    with pytest.raises(preflight.PreflightError, match="missing"):
        provisioning.bootstrap_log(con, cfg)
    assert runtime._catalog(con, cfg) == set()
    log, _ = provisioning.bootstrap_log(con, cfg, create_objects=True)
    assert calls == [False, True]
    assert log.checkpoint() == 90
    assert log.execute("SELECT created_slot FROM {s}source").fetchone() == (True,)


def test_original_configuration_opens_after_slot_recovery(database):
    con, original = database
    original = replace(original, epoch=None)
    log = fresh(con, original)
    log.acquire("recover")
    try:
        log.begin_epoch(2, 500, "slot_lost", original.slot + "_e2")
    finally:
        log.release()
    opened = runtime.open_log(con, original)
    assert opened.config.slot == original.slot
    assert opened.registered_slot() == original.slot + "_e2"
    assert opened.position() == (2, original.slot + "_e2", 500)
