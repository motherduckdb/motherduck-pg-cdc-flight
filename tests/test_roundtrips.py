"""Pin the destination statements each ingest-log call issues.

On MotherDuck, latency is set by remote statements (~85 ms each) and commits with
writes, not by data volume (see dev-docs/performance.md, round trips). These tests count
statements on local DuckDB as a proxy for MotherDuck round trips:
- `BEGIN`, client-catalog reads (`duckdb_columns()`) and `current_timestamp`-only
  SELECTs run on the client and are not counted;
- every other statement counts as remote, including any statement this classifier
  doesn't know;
- the changes insert (from the batch's registered Arrow table) counts as one here,
  though it ships the batch's rows to MotherDuck.

Each test pins the ordered statement kinds, so a regression names the statement
that came back. Measured MotherDuck timings are recorded separately.
"""

import re
from dataclasses import replace

import duckdb
import pytest
from test_format2_protocol import SETTINGS
from test_phase1 import pg_phase1, spool_all, transaction  # noqa: F401  (fixture)
from test_sql_fold_apply import fresh  # noqa: F401  (fixture)

from cdc_flight.apply import Apply, next_step
from cdc_flight.apply import LeaseBusy as ApplyLeaseBusy
from cdc_flight.apply_run import ApplyRun
from cdc_flight.backfill import Backfill
from cdc_flight.config import ApplyConfig, BackfillConfig, IngestConfig
from cdc_flight.format2 import Binding, Chunk, UnitConfig
from cdc_flight.ingest import run_once
from cdc_flight.ingest_log import (
    TABLES,
    FenceLost,
    IngestLog,
    LeaseBusy,
    SourceMismatch,
    TableRefused,
)
from cdc_flight.ingest_log2 import IngestConfig2
from cdc_flight.unit_progress import Progress

LOCAL = (
    re.compile(r"^BEGIN$"),
    re.compile(r"^SELECT table_name,column_name FROM duckdb_columns\(\)"),
    re.compile(r"^SELECT strftime\(current_timestamp AT TIME ZONE 'UTC','%Y%m%d'\)$"),
)

NAMED = (
    (re.compile(r"^SELECT epoch,coalesce\(slot_name"), "read:position"),
    (re.compile(r"^SELECT binding_hash,config_json FROM unit_configs_v2"), "read:unit_config"),
    (re.compile(r"^SELECT unit_start_lsn FROM units_v2"), "read:open_unit"),
    (
        re.compile(r"^SELECT format_version,min_reader,binding_hash,active_revision"),
        "read:protocol",
    ),
    (
        re.compile(r"^SELECT bool_and\(EXISTS|^SELECT EXISTS \(SELECT 1 FROM source_epochs"),
        "read:cut",
    ),
    (
        re.compile(r"^SELECT s.epoch,s.confirmed_lsn,s.open_unit_anchor,p.active_revision"),
        "read:unit_position",
    ),
    (re.compile(r"^SELECT start_lsn FROM source_epochs"), "read:epoch_start"),
    (re.compile(r"^SELECT relation_id,key_version,pending_keys_v2"), "read:pending_keys"),
    (re.compile(r"^SELECT last_capped_epoch,last_capped_anchor"), "read:unit_progress"),
    (re.compile(r"^SELECT partition_day,epoch,min_unit"), "read:frame_days"),
    (re.compile(r"^WITH candidates AS"), "read:snapshot"),
    (re.compile(r"^CREATE TABLE frames_v2_"), "create:frames"),
    (re.compile(r"^INSERT INTO frames_v2_"), "insert:frames"),
    (re.compile(r"^UPDATE source SET guard=uuid\(\)::VARCHAR, expires_at"), "guard"),
    (re.compile(r"^UPDATE source SET token=token\+1"), "acquire"),
    (re.compile(r"^UPDATE source SET guard=uuid\(\)::VARCHAR,holder=NULL"), "release"),
    (re.compile(r"^UPDATE source SET confirmed_lsn"), "write:checkpoint"),
    (re.compile(r"^UPDATE source SET last_capped"), "write:progress"),
    (re.compile(r"^SELECT confirmed_lsn,open_commit_lsn,"), "preread"),
    (re.compile(r"^SELECT confirmed_lsn FROM source"), "checkpoint"),
    (re.compile(r"^SELECT token,epoch"), "read:token"),
    (re.compile(r"^SELECT last_capped_commit"), "read:progress"),
    (re.compile(r"^SELECT \(SELECT \{'binding'"), "read:registration"),
    (re.compile(r"^SELECT relation_id,key_columns,catalog_key"), "read:keys"),
    (re.compile(r"^SELECT relation_id,key_columns FROM tables"), "read:key_columns"),
    (
        re.compile(r"^INSERT INTO changes_\d{8} BY NAME SELECT \* FROM cdc_append_[0-9a-f]{32}$"),
        "insert:changes",
    ),
    (re.compile(r"^CREATE TABLE IF NOT EXISTS changes_\d{8}"), "create:changes"),
    (re.compile(r"^CREATE SCHEMA"), "create:schema"),
    (re.compile(r"^SELECT generation_id,epoch,applied_through_lsn"), "read:state"),
    (re.compile(r"^WITH r AS \(SELECT relation_id FROM tables"), "read:head"),
    (re.compile(r"^SELECT DISTINCT partition_day FROM batches"), "read:days"),
    (re.compile(r"^SELECT c\.commit_lsn,c\.n,c\.size,m\.end_lsn"), "read:plan"),
    (re.compile(r"^SELECT commit_lsn,event_ordinal"), "read:events"),
    (re.compile(r"^SELECT \(SELECT map\(list\(version\)"), "read:descriptors"),
    (re.compile(r"^UPDATE retention_pins SET pinned_lsn"), "advance_pin"),
    (re.compile(r"^UPDATE apply_state SET token=token\+1"), "apply_acquire"),
    (re.compile(r"^UPDATE apply_state SET guard=uuid\(\)::VARCHAR,expires_at"), "apply_guard"),
    (re.compile(r"^UPDATE apply_state SET guard=uuid\(\)::VARCHAR,holder=NULL"), "apply_release"),
    (re.compile(r"^UPDATE apply_state SET applied_through_lsn"), "write:apply_state"),
    (re.compile(r"^MERGE INTO"), "merge"),
    (re.compile(r"^SELECT token FROM apply_state"), "read:apply_token"),
    (re.compile(r"^SELECT token FROM generations"), "read:generation_token"),
    (re.compile(r"^SELECT \(SELECT count\(\*\) FROM retention_pins"), "read:pins"),
    # Format-2 consumers (retention pins). Retention2's stored-protocol check, and its pin
    # position check (the epoch start or a complete unit end, above the floor).
    (re.compile(r"^SELECT format_version,min_reader FROM protocol_v2"), "read:retention_protocol"),
    (re.compile(r"^SELECT e\.start_lsn,r\.floor_epoch,r\.floor_lsn,EXISTS"), "read:pin_cut"),
    # UnitReader: the page (units with the floor), its frames, its descriptors.
    (re.compile(r"^SELECT \(SELECT list\(u ORDER BY unit_start_lsn\)"), "read:units"),
    (re.compile(r"^WITH physical AS MATERIALIZED"), "read:frames"),
    # ApplyRun's SQL fold: the page's frame checks and the fold plan, one statement.
    (re.compile(r"^WITH frames AS MATERIALIZED"), "read:frame_plan"),
    (re.compile(r"^SELECT \(SELECT list\(d\) FROM descriptors_v2"), "read:descriptors"),
)


class Counting:
    """A connection wrapper that records every statement's kind, in order."""

    def __init__(self, con, *prefixes):
        self.con, self.prefixes, self.kinds = con, prefixes, []

    def classify(self, sql):
        text = re.sub(r"\s+", " ", sql.strip())
        for prefix in self.prefixes:
            text = text.replace(prefix, "")
        if any(pattern.search(text) for pattern in LOCAL):
            return None
        if re.match(r"^(COMMIT|ROLLBACK)$", text):
            return text
        for pattern, name in NAMED:
            if pattern.search(text):
                return name
        match = re.match(
            r"^(INSERT INTO|UPDATE|CREATE TABLE IF NOT EXISTS|ALTER TABLE|DELETE FROM) (\w+)",
            text,
        )
        if match:
            verb = {
                "INSERT INTO": "insert",
                "UPDATE": "update",
                "ALTER TABLE": "alter",
                "DELETE FROM": "delete",
            }.get(match[1], "create")
            return f"{verb}:{match[2]}"
        return "remote:" + text[:60]  # unknown statements always count

    def execute(self, sql, parameters=None):
        kind = self.classify(sql)
        if kind:
            self.kinds.append(kind)
        return self.con.execute(sql, parameters or [])

    def take(self):
        kinds, self.kinds = self.kinds, []
        return kinds

    def __getattr__(self, name):
        return getattr(self.con, name)


@pytest.fixture
def con(tmp_path):
    raw = duckdb.connect(str(tmp_path / "rt.duckdb"))
    raw.execute("SET TimeZone='UTC'")
    yield raw
    raw.close()


def make_config(**extra):
    base = IngestConfig(
        "rt",
        "rt_slot",
        "rt_pub",
        ("public.items",),
        database="rt",
        chunk_bytes=1024,
        batch_bytes=2048,
        max_event_bytes=10000,
        allow_local=True,
        epoch=1,
        **extra,
    )
    return IngestConfig2(
        **base.__dict__,
        text_binding=Binding(base.source_id, SETTINGS, (("plpgsql", "1.0"),)),
        unit_config=UnitConfig(max_commits=1, chunk_bytes=1024),
    )


@pytest.fixture
def counted(con):
    config = make_config()
    counting = Counting(con, '"rt"."rt".')
    log = IngestLog(counting, config)
    return config, log, counting


def ready(log, counting):
    log.initialize()
    log.register(("system", 1), 0, [dict(oid=1, schema="public", name="items", keys=["id"])])
    log.acquire("test")
    counting.take()


# initialize --------------------------------------------------------------------


def test_initialize_creates_all_in_one_transaction_and_nothing_when_present(counted):
    _, log, counting = counted
    assert log.initialize() is True
    kinds = counting.take()
    assert kinds.count("COMMIT") == 1 and kinds[-1] == "COMMIT"
    # + tables.capture_mode, ignore_deletes and releases (keyless); + tables.catalog_state
    # and key_changes.reason (catalog checks); + source.heartbeat_writer and timeline_id
    # (replica capture: a fresh store only, in this one transaction; an existing store
    # reads them through to_json, with no ALTER)
    assert len(kinds) == 59
    assert set(log.catalog()) >= {
        "protocol_v2",
        "units_v2",
        "chunks_v2",
        "descriptors_v2",
        "source",
    }
    counting.take()
    assert log.initialize() is False
    assert counting.take() == ["read:protocol"]


def test_initialize_refuses_a_pre_phase2_store_without_migrating(counted, con):
    from reference.legacy_store import create

    _, log, counting = counted
    create(con, log.config)
    before = log.catalog()
    counting.take()
    with pytest.raises(SourceMismatch, match="fresh schema"):
        log.initialize()
    assert counting.take() == []
    assert log.catalog() == before


# per-run calls -----------------------------------------------------------------


def test_register_acquire_status_release_counts(counted):
    config, log, counting = counted
    log.initialize()
    counting.take()
    tables = [
        dict(oid=1, schema="public", name="items", keys=["id"]),
        dict(oid=2, schema="public", name="other", keys=["id"]),
    ]
    log.register(("system", 1), 0, tables)
    assert counting.take() == [
        "read:registration",
        "read:registration",
        "insert:source",
        "insert:source_epochs",
        "insert:tables",
        "COMMIT",
    ]
    log.register(("system", 1), 0, tables)
    assert counting.take() == []  # the same arguments, already verified in this run
    # After the first registration only add-table registers a table.
    with pytest.raises(TableRefused, match="run add-table"):
        log.register(
            ("system", 1), 0, [*tables, dict(oid=3, schema="public", name="t3", keys=["a"])]
        )
    assert counting.take() == ["read:registration"]
    assert log.execute("SELECT count(*) FROM {s}tables").fetchone() == (2,)
    with pytest.raises(SourceMismatch):
        log.register(("other", 1), 0, tables)
    counting.take()

    log.acquire("run")
    assert counting.take() == ["read:protocol", "read:token", "acquire"]
    log.renew()
    assert counting.take() == ["guard"]
    log.run_status("run", "running")
    assert counting.take() == ["insert:runs"]
    log.record_keys({1: ("id",), 2: ("id",), 3: ("a",)}, 0, 6)
    assert counting.take() == [
        "guard",
        "read:cut",
        "read:unit_position",
        "read:epoch_start",
        "read:keys",
        "read:cut",
        "update:tables",
        "read:unit_position",
        "read:pending_keys",
        "COMMIT",
    ]
    log.record_progress("drained", None)
    assert counting.take() == [
        "guard",
        "read:unit_progress",
        "update:protocol_v2",
        "update:source",
        "COMMIT",
    ]
    log.release()
    assert counting.take() == ["release"]
    # A released run forgets its registration: the next run reads it again.
    log.register(("system", 1), 0, tables)
    assert counting.take() == ["read:registration"]


def test_run_status_requires_a_registered_source(counted):
    config, log, counting = counted
    log.initialize()
    with pytest.raises(SourceMismatch, match="not registered"):
        log.run_status("run", "running")
    assert log.execute("SELECT count(*) FROM {s}runs").fetchone() == (0,)


def test_record_keys_never_lowers_the_checked_position(counted, tmp_path):
    config, log, counting = counted
    ready(log, counting)
    log.append(spool_all(config, tmp_path, transaction(commit=100)))
    log.record_keys({1: ("id",)}, 101, 110)
    log.record_keys({1: ("id",)}, 0, 120)
    assert log.execute(
        "SELECT key_version,key_checked_lsn FROM {s}tables WHERE relation_id=1"
    ).fetchone() == (0, 101)


# append ------------------------------------------------------------------------


def test_append_counts(counted, tmp_path):
    config, log, counting = counted
    ready(log, counting)
    items = spool_all(config, tmp_path, transaction())
    chunks = [item for item in items if isinstance(item, Chunk)]
    assert len(chunks) > 2
    log.append(chunks[:2], partition_day="20261103")
    assert counting.take() == [
        "read:protocol",
        "read:position",
        "read:unit_config",
        "read:open_unit",
        "read:frame_days",
        "guard",
        "read:snapshot",
        "create:frames",
        "insert:frames",
        "insert:chunks_v2",
        "insert:descriptors_v2",
        "insert:units_v2",
        "insert:frame_partitions_v2",
        "write:checkpoint",
        "COMMIT",
    ]
    assert log.append(items[2:], partition_day="20261103") == 101
    assert counting.take() == [
        "guard",
        "read:snapshot",
        "insert:frames",
        "insert:chunks_v2",
        "insert:units_v2",
        "insert:frame_partitions_v2",
        "write:checkpoint",
        "COMMIT",
    ]
    assert log.append(items, partition_day="20261103") == 101
    assert counting.take() == ["guard", "read:snapshot", "write:checkpoint", "COMMIT"]
    log.append(
        spool_all(config, tmp_path, transaction(commit=300), start_lsn=101),
        partition_day="20261104",
    )
    assert counting.take() == [
        "guard",
        "read:snapshot",
        "create:frames",
        "insert:frames",
        "insert:chunks_v2",
        "insert:units_v2",
        "insert:frame_partitions_v2",
        "update:frame_partitions_v2",
        "write:checkpoint",
        "COMMIT",
    ]
    assert log.append([]) == 301
    assert counting.take() == ["checkpoint"]


def test_append_completes_a_transaction_and_keeps_a_later_one_open(counted, tmp_path):
    config, log, counting = counted
    ready(log, counting)
    x = spool_all(config, tmp_path, transaction(commit=100))
    y = spool_all(config, tmp_path, transaction(commit=200), start_lsn=101)
    y_chunks = [item for item in y if isinstance(item, Chunk)]
    assert len(y_chunks) > 2
    assert log.append([*x, *y_chunks[:2]], partition_day="20261103") == 101
    assert log.execute(
        "SELECT unit_start_lsn,complete,end_lsn FROM {s}units_v2 ORDER BY unit_start_lsn"
    ).fetchall() == [(0, True, 101), (101, False, None)]
    assert log.unit_position()["open_unit_anchor"] == 101
    assert log.append(y, partition_day="20261103") == 201
    assert log.execute(
        "SELECT unit_start_lsn,complete,end_lsn FROM {s}units_v2 ORDER BY unit_start_lsn"
    ).fetchall() == [(0, True, 101), (101, True, 201)]


def test_append_seals_older_active_days_in_every_epoch(counted, tmp_path):
    config, log, counting = counted
    ready(log, counting)
    log.append(spool_all(config, tmp_path, transaction()), partition_day="20261101")
    log.begin_epoch(2, 200, "slot_lost", config.slot + "_e2")
    cfg = replace(config, epoch=2)
    log.append(
        spool_all(cfg, tmp_path, transaction(commit=300), start_lsn=200), partition_day="20261103"
    )
    assert log.execute(
        "SELECT partition_day,epoch,status FROM {s}frame_partitions_v2 ORDER BY ALL"
    ).fetchall() == [("20261101", 1, "sealed"), ("20261103", 2, "active")]
    log.append(
        spool_all(cfg, tmp_path, transaction(commit=400), start_lsn=301), partition_day="20261104"
    )
    assert log.execute(
        "SELECT partition_day,epoch,status FROM {s}frame_partitions_v2 ORDER BY ALL"
    ).fetchall() == [
        ("20261101", 1, "sealed"),
        ("20261103", 2, "sealed"),
        ("20261104", 2, "active"),
    ]


def test_append_rejects_a_sealed_partition_day(counted, tmp_path):
    config, log, counting = counted
    ready(log, counting)
    log.append(spool_all(config, tmp_path, transaction()), partition_day="20261103")
    log.execute("UPDATE {s}frame_partitions_v2 SET status='sealed' WHERE partition_day='20261103'")
    log._days = None  # next operation must reload the independently changed day status

    def state():
        return (
            log.checkpoint(),
            log.execute("SELECT * FROM {s}chunks_v2 ORDER BY id").fetchall(),
            log.execute("SELECT * FROM {s}units_v2 ORDER BY unit_start_lsn").fetchall(),
            log.execute("SELECT * FROM {s}frames_v2_20261103 ORDER BY frame_ordinal").fetchall(),
            log.execute("SELECT * FROM {s}frame_partitions_v2").fetchall(),
        )

    before = state()
    with pytest.raises(ValueError, match="sealed day"):
        log.append(
            spool_all(config, tmp_path, transaction(commit=300), start_lsn=101),
            partition_day="20261103",
        )
    assert state() == before


def test_a_replaced_lease_fences_every_guarded_write(counted, tmp_path):
    """After the lease expires and another run takes it, the old run's guarded calls
    raise FenceLost and change nothing."""
    config, log, counting = counted
    ready(log, counting)
    items = spool_all(config, tmp_path, transaction())
    log.execute("UPDATE {s}source SET expires_at=current_timestamp-INTERVAL 1 SECOND")
    successor = IngestLog(log.con, config)
    successor.acquire("successor")
    for call in (
        lambda: log.append(items),
        log.renew,
        lambda: log.record_keys({1: ("id",)}, 0, 6),
        lambda: log.record_progress("capped", Progress(1, 0, 100, 0)),
    ):
        with pytest.raises(FenceLost):
            call()
    assert log.checkpoint() == 0
    assert log.execute("SELECT count(*) FROM {s}chunks_v2").fetchone() == (0,)
    assert log.execute("SELECT key_checked_lsn FROM {s}tables").fetchone() == (0,)
    assert successor.append(items) == 101


def test_append_guard_detects_a_concurrent_writer(tmp_path):
    """The fence must be an ordinary UPDATE: DuckDB 1.5.6 skips write-write conflict
    detection for UPDATE ... RETURNING, so both writers could commit."""
    path = str(tmp_path / "rt.duckdb")
    first, second = duckdb.connect(path), duckdb.connect(path)
    try:
        config = make_config()
        log = IngestLog(first, config)
        ready(log, Counting(first, ""))
        items = spool_all(config, tmp_path, transaction())
        other = IngestLog(second, config)
        other.execute("BEGIN")
        other.execute("UPDATE {s}source SET guard=uuid()::VARCHAR WHERE id=1")
        with pytest.raises(duckdb.TransactionException, match="[Cc]onflict"):
            log.append(items)
        with pytest.raises(duckdb.TransactionException, match="[Cc]onflict"):
            log.renew()
        other.execute("ROLLBACK")
        assert log.checkpoint() == 0
    finally:
        first.close()
        second.close()


# run_once ----------------------------------------------------------------------


@pytest.mark.postgres
@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_run_once_counts(stores, pg_phase1, drain_only_units):  # noqa: F811
    from cdc_flight.bootstrap import bootstrap_log

    source, base = pg_phase1
    # One append, at the drain: no flush on time or bytes (on PostgreSQL 13 other
    # sessions' empty transactions would otherwise add chunks), and one unit.
    base = replace(base, flush_seconds=60, batch_bytes=50 * 1024 * 1024)
    counting = Counting(stores[0].con, f'"{base.database}"."{base.source_id}".')
    log, info = bootstrap_log(counting, base, create_objects=True)
    source.execute("INSERT INTO items SELECT i,'x','y' FROM generate_series(1,5) i")
    counting.take()
    result = run_once(log.config, log, info=info)
    assert result.status == "drained" and result.transactions >= 2
    kinds = counting.take()
    flush = kinds.index("read:snapshot") - 1
    end = kinds.index("COMMIT", flush) + 1
    assert kinds[:flush] == [
        "read:protocol",
        "read:token",
        "acquire",
        "read:protocol",
        "read:position",
        "read:unit_config",
        "read:open_unit",
        "read:frame_days",
        "read:registration",  # refreshed registry after record_tables invalidated caches
        "guard",
        "read:cut",
        "read:unit_position",
        "read:epoch_start",
        "read:keys",
        "read:cut",
        "update:tables",
        "read:unit_position",
        "read:pending_keys",
        "COMMIT",
        "guard",
        "insert:runs",
        "read:key_columns",  # once, before constructing the spool
    ]
    assert kinds[flush:end] == [
        "guard",
        "read:snapshot",
        "create:frames",
        "insert:frames",
        "insert:chunks_v2",
        "insert:descriptors_v2",
        "insert:units_v2",
        "insert:frame_partitions_v2",
        "write:checkpoint",
        "COMMIT",
    ]
    finish = [
        "guard",
        "read:cut",
        "read:unit_position",
        "read:epoch_start",
        "read:keys",
        "read:cut",
        "update:tables",
        "read:unit_position",
        "read:pending_keys",
        "read:unit_progress",
        "update:protocol_v2",
        "update:source",
        "insert:runs",
        "release",
        "COMMIT",
    ]
    # The first run with catalog checks adopts every table's catalog_state: one batched
    # UPDATE, before the key confirmation's.
    assert kinds[end:] == finish[:6] + ["update:tables"] + finish[6:]
    assert len(kinds) == 48  # regression pin, not a measured speed claim
    # The steady state writes no catalog state (two back-to-back runs).
    for n in (100, 200):
        source.execute(f"INSERT INTO items SELECT i,'x','y' FROM generate_series({n},{n + 1}) i")
        counting.take()
        assert run_once(log.config, log, info=info).status == "drained"
        kinds = counting.take()
        end = kinds.index("COMMIT", kinds.index("read:snapshot")) + 1
        assert kinds[end:] == finish


@pytest.mark.postgres
@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_key_projection_is_read_once_even_when_a_run_flushes_many_batches(stores, pg_phase1):  # noqa: F811
    from cdc_flight.bootstrap import bootstrap_log

    source, base = pg_phase1
    counting = Counting(stores[0].con, f'"{base.database}"."{base.source_id}".')
    log, info = bootstrap_log(counting, replace(base, batch_events=1), create_objects=True)
    log.acquire("unit config")
    try:
        log.activate_config(
            replace(log.config.unit_config, revision=2, max_commits=1, chunk_frames=1)
        )
    finally:
        log.release()
    for i in range(5):
        source.execute("INSERT INTO items VALUES (%s,'x','y')", [i])
    counting.take()
    result = run_once(log.config, log, info=info)
    assert result.status == "drained"
    kinds = counting.take()
    assert kinds.count("read:snapshot") >= 3  # actual multiple payload appends
    assert kinds.count("read:key_columns") == 1
    assert kinds.index("read:key_columns") < kinds.index("read:snapshot")


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_cli_shaped_run_limits_registration_reads_to_setup_and_registry_refresh(stores, pg_phase1):  # noqa: F811
    from cdc_flight.bootstrap import bootstrap_log
    from cdc_flight.runtime import open_log

    _, base = pg_phase1
    first, info = bootstrap_log(stores[0].con, base, create_objects=True)
    counting = Counting(stores[0].con, f'"{base.database}"."{base.source_id}".')
    log = open_log(counting, base)
    assert log.registered_slot() == base.slot
    log.register(info.identity, info.initial_lsn, info.tables)
    assert counting.take().count("read:registration") == 1
    assert run_once(log.config, log, info=info).status == "drained"
    kinds = counting.take()
    assert kinds.count("read:registration") == 1  # fresh table-registry/hold snapshot
    log.register(info.identity, info.initial_lsn, info.tables)
    assert counting.take() == ["read:registration"]


def test_key_columns_include_tables_registered_in_this_run(counted):
    """An unmanaged publication member registered under the lease joins key_columns at
    once, so the spool decodes its changes in this run."""
    config, log, counting = counted
    log.initialize()
    log.register(("system", 1), 0, [dict(oid=1, schema="public", name="a", keys=["id"])])
    log.acquire("run")
    log.record_tables(unmanaged=[dict(oid=2, schema="public", name="b", keys=["x", "y"])])
    counting.take()
    assert log.key_columns() == {1: ("id",), 2: ("x", "y")}
    assert counting.take() == ["read:key_columns"]
    log.release()
    assert log.key_columns() == {1: ("id",), 2: ("x", "y")}
    assert counting.take() == ["release", "read:key_columns"]


def test_a_released_run_checks_its_registration_again(counted):
    config, log, counting = counted
    ready(log, counting)
    log.register(("system", 1), 0, [dict(oid=1, schema="public", name="items", keys=["id"])])
    log.release()
    log.execute("UPDATE {s}source SET system_id='another'")
    with pytest.raises(SourceMismatch):
        log.register(("system", 1), 0, [dict(oid=1, schema="public", name="items", keys=["id"])])


def test_a_busy_run_checks_its_registration_again(counted, con):
    config, log, counting = counted
    tables = [dict(oid=1, schema="public", name="items", keys=["id"])]
    ready(log, counting)  # holds the lease
    other = IngestLog(con.cursor(), config)
    info = type(
        "Info", (), dict(identity=("system", 1), initial_lsn=0, tables=tables, timeline=None)
    )
    assert run_once(config, other, info=info).status == "busy"
    con.execute('UPDATE "rt"."rt".source SET system_id=\'another\'')
    with pytest.raises(SourceMismatch):
        other.register(("system", 1), 0, tables)


def test_register_checks_registered_slots_read(counted):
    """registered_slot's read stands in for register's: it is checked, not trusted."""
    config, log, counting = counted
    ready(log, counting)
    log.release()
    assert log.registered_slot() == config.slot
    with pytest.raises(SourceMismatch):
        log.register(("other", 1), 0, [dict(oid=1, schema="public", name="items", keys=["id"])])


def test_finish_reports_a_stalled_replay_and_records_it(counted, con):
    """Two capped runs on the same partial transaction with no new chunks: the
    second ends needs_action, with its detail, in the same commit as the release."""
    config, log, counting = counted
    tables = [dict(oid=1, schema="public", name="items", keys=["id"])]
    ready(log, counting)
    statuses = []
    for run_id in ("first", "second"):
        if run_id == "second":
            log.register(("system", 1), 0, tables)
            log.acquire(run_id)
        statuses.append(
            log.finish(
                run_id,
                {1: ("id",)},
                0,
                1,
                "capped",
                Progress(1, 0, 7, 0),
                lambda status: "detail:" + status,
            )
        )
    assert statuses == ["capped", "needs_action"]
    assert con.execute(
        'SELECT run_id,status,detail FROM "rt"."rt".runs ORDER BY run_id'
    ).fetchall() == [
        ("first", "capped", "detail:capped"),
        ("second", "needs_action", "detail:needs_action"),
    ]
    assert con.execute('SELECT holder,state FROM "rt"."rt".source').fetchone() == (
        None,
        "needs_action:large_transaction",
    )


class Interleaved(Counting):
    """Runs `between` once, right after the first statement of kind `after`."""

    def __init__(self, con, prefix, after, between):
        super().__init__(con, prefix)
        self.after, self.between = after, between

    def execute(self, sql, parameters=None):
        result = super().execute(sql, parameters)
        if self.between and self.classify(sql) == self.after:
            between, self.between = self.between, None
            between()
        return result


def test_acquire_is_busy_after_a_lease_taken_and_released_since_its_read(con):
    """The read and the UPDATE are separate statements: a run that acquired and
    released in between must make this one busy, never a lease on what it read."""
    config = make_config()
    setup = IngestLog(con, config)
    setup.initialize()
    setup.register(("system", 1), 0, [dict(oid=1, schema="public", name="items", keys=["id"])])
    other = IngestLog(con.cursor(), config)

    def acquire_and_release():
        other.acquire("other")
        other.execute("UPDATE {s}source SET epoch=2")  # e.g. begin_epoch, under its lease
        other.release()

    log = IngestLog(Interleaved(con, '"rt"."rt".', "read:token", acquire_and_release), config)
    with pytest.raises(LeaseBusy):
        log.acquire("ours")
    assert log.token is None and log.epoch is None and log.acquired_lsn is None


def test_acquire_is_busy_after_a_write_committed_since_its_read(con):
    """A guarded transaction that began under the previous lease and committed
    after this read changes the checkpoint but not the token: guard catches it."""
    config = make_config()
    setup = IngestLog(con, config)
    setup.initialize()
    setup.register(("system", 1), 0, [dict(oid=1, schema="public", name="items", keys=["id"])])

    def late_commit():
        con.cursor().execute(
            'UPDATE "rt"."rt".source SET guard=uuid()::VARCHAR,confirmed_lsn=confirmed_lsn+10'
        )

    log = IngestLog(Interleaved(con, '"rt"."rt".', "read:token", late_commit), config)
    with pytest.raises(LeaseBusy):
        log.acquire("ours")
    log = IngestLog(con, config)
    log.acquire("ours")
    assert log.acquired_lsn == 10


def test_finish_after_a_takeover_writes_nothing(counted, con):
    config, log, counting = counted
    ready(log, counting)
    before = {
        table: con.execute(f'SELECT * FROM "rt"."rt".{table} ORDER BY ALL').fetchall()
        for table in ("tables", "key_changes", "runs")
    }
    con.execute('UPDATE "rt"."rt".source SET expires_at=current_timestamp-INTERVAL 1 SECOND')
    successor = IngestLog(con.cursor(), config)
    successor.acquire("successor")
    state = con.execute('SELECT token,holder,last_capped_commit,state FROM "rt"."rt".source')
    state = state.fetchone()
    with pytest.raises(FenceLost):
        log.finish(
            "run", {1: ("other",)}, 5, 6, "capped", Progress(1, 0, 7, 0), lambda status: status
        )
    for table, rows in before.items():
        assert con.execute(f'SELECT * FROM "rt"."rt".{table} ORDER BY ALL').fetchall() == rows
    after = con.execute('SELECT token,holder,last_capped_commit,state FROM "rt"."rt".source')
    assert after.fetchone() == state
    assert state[:3] == (successor.token, "successor", None)
    log.release()  # the run's cleanup: the successor's lease stands
    assert con.execute('SELECT holder FROM "rt"."rt".source').fetchone() == ("successor",)


def test_counting_classifier_treats_unknown_statements_as_remote():
    counting = Counting(None, "")
    assert counting.classify("SELECT 1") == "remote:SELECT 1"
    assert counting.classify("BEGIN") is None
    assert counting.classify("COMMIT") == "COMMIT"


# Apply and Backfill initialize -------------------------------------------------


def attach_mirror(con, tmp_path):
    con.execute(f"ATTACH '{tmp_path / 'rt_mirror.duckdb'}' AS rt_mirror")


@pytest.fixture
def mirror(con, tmp_path):
    """A local change-log store plus its mirror, as apply and backfill attach them."""
    attach_mirror(con, tmp_path)
    registered_log(con)
    return Counting(con, '"rt"."rt".', '"rt_mirror"._cdc.')


def unit_ends(con, *ends, epoch=1, start=0):
    """Complete one-commit units ending exactly at `ends` (empty transactions), on the
    registered log of `registered_log`: format-2 pins sit only at the epoch start or a
    complete unit end. epoch > 1 first begins it at `start` under the ingest lease."""
    import pgstream

    from cdc_flight.spool2 import Spool

    if epoch > 1:
        previous = IngestLog(con, replace(make_config(), epoch=epoch - 1))
        previous.acquire("epoch")
        try:
            previous.begin_epoch(epoch, start, "slot_lost", f"rt_slot_e{epoch}")
        finally:
            previous.release()
    config = replace(make_config(), epoch=epoch)
    log = IngestLog(con, config)
    log.acquire("units")
    try:
        spool = Spool(
            config,
            {1: ("id",)},
            900,
            "marker",
            binding=config.text_binding,
            start_lsn=start,
            unit_config=config.unit_config,
            clock=lambda: 0,
        )
        items = []
        for end in ends:
            items += spool.feed(pgstream.begin(end - 1, 0, 7))
            items += spool.feed(pgstream.commit(end - 1, end, 0))
        items += list(spool.close())
        log.append(items, partition_day="20261107")
    finally:
        log.release()


def registered_log(con):
    """A format-2 change log with one registered table at epoch 1 (start 0): consumers
    read the stored protocol and the recorded epoch start before anything else."""
    log = IngestLog(con, make_config())
    log.initialize()
    log.register(("system", 1), 0, [dict(oid=1, schema="public", name="items", keys=["id"])])
    return log


def mirror_schema(con):
    columns = con.execute(
        "SELECT table_name,column_index,column_name,data_type,is_nullable FROM duckdb_columns() "
        "WHERE database_name='rt_mirror' AND schema_name='_cdc' ORDER BY ALL"
    ).fetchall()
    constraints = con.execute(
        "SELECT table_name,constraint_type,constraint_column_names FROM duckdb_constraints() "
        "WHERE database_name='rt_mirror' AND schema_name='_cdc' ORDER BY ALL"
    ).fetchall()
    return columns, constraints


def unchecked_initialize(runner):
    """The DDL before the catalog check: every CREATE as written, then every ALTER."""
    runner.con.execute('CREATE SCHEMA IF NOT EXISTS "rt_mirror"._cdc')
    for statement in runner.MIRROR_TABLES.values():
        runner.execute(statement)
    for table, columns in runner.ADDED_COLUMNS.items():
        for column, dtype in columns:
            runner.execute(f"ALTER TABLE {{m}}{table} ADD COLUMN IF NOT EXISTS {column} {dtype}")


RUNNERS = {
    "apply": (
        Apply,
        ApplyConfig,
        [
            "create:apply_state",
            "create:generations",
            "create:runs",
            "create:quarantine",
            "create:previews",
        ],
    ),
    "backfill": (
        Backfill,
        BackfillConfig,
        [
            "create:apply_state",
            "create:generations",
            "create:runs",
            "create:quarantine",
            "create:previews",
            "create:backfills",
        ],
    ),
}


@pytest.mark.parametrize("kind", sorted(RUNNERS))
def test_mirror_initialize_creates_all_in_one_transaction_and_nothing_when_present(mirror, kind):
    cls, config_cls, creates = RUNNERS[kind]
    runner = cls(mirror, config_cls("rt", ("public.items",), database="rt"))
    runner.initialize()
    assert mirror.take() == [
        "read:unit_position",  # check_format: the stored protocol before any mirror DDL
        "create:schema",
        *creates,
        "COMMIT",
        "read:retention_protocol",  # bootstrap created the complete v2 compactions schema
    ]
    runner.initialize()
    # An existing store: no DDL, only the format check and retention's protocol read.
    assert mirror.take() == ["read:unit_position", "read:retention_protocol"]


@pytest.mark.parametrize("kind", sorted(RUNNERS))
def test_mirror_initialize_matches_the_unchecked_schema(con, tmp_path, kind):
    """Creating a new table with its added columns gives the schema that CREATE and
    then ALTER gave: same columns, order, types, nullability and keys."""
    cls, config_cls, _ = RUNNERS[kind]
    attach_mirror(con, tmp_path)
    config = config_cls("rt", ("public.items",), database="rt")
    unchecked_initialize(cls(con, config))
    expected = mirror_schema(con)
    con.execute("DETACH rt_mirror")
    (tmp_path / "rt_mirror.duckdb").unlink()
    attach_mirror(con, tmp_path)
    registered_log(con)
    cls(con, config).initialize()
    assert mirror_schema(con) == expected
    assert any(row[2] == "key_version" for row in expected[0])


def test_mirror_initialize_upgrades_an_older_store(mirror):
    """A pre-key_version apply store gets only the missing columns and tables."""
    con = mirror.con
    con.execute('CREATE SCHEMA "rt_mirror"._cdc')
    for statement in Apply.MIRROR_TABLES.values():
        con.execute(statement.replace("{m}", '"rt_mirror"._cdc.'))
    backfill = Backfill(mirror, BackfillConfig("rt", ("public.items",), database="rt"))
    backfill.initialize()
    from cdc_flight.backfill import GENERATION_COLUMNS

    assert mirror.take() == [
        "read:unit_position",  # check_format
        "alter:apply_state",
        "alter:apply_state",  # held_since (retention expiry)
        "alter:apply_state",  # capture_mode (keyless guard)
        "alter:apply_state",  # deletes_ignored (keyless append)
        "alter:apply_state",  # ignore_deletes (keyless append)
        "alter:apply_state",  # last_truncate_lsn (keyless append)
        "alter:apply_state",  # hold_code
        "alter:apply_state",  # hold_code_at
        "alter:apply_state",  # blocked_unit_start_lsn (format 2)
        *["alter:generations"] * len(GENERATION_COLUMNS),
        *["alter:quarantine"] * 3,  # the quarantined unit's start, end and digest
        "create:backfills",
        "COMMIT",
        "read:retention_protocol",  # no compaction ALTERs on the complete source catalog
    ]
    catalog = backfill.mirror_catalog()
    assert "key_version" in catalog["apply_state"]
    assert set(GENERATION_COLUMNS) <= catalog["generations"]
    backfill.initialize()
    assert mirror.take() == ["read:unit_position", "read:retention_protocol"]


def test_mirror_initialize_retries_once_after_a_concurrent_initializer(con, tmp_path):
    """Another initializer's uncommitted CREATE conflicts with ours; it commits before
    the retry re-reads the catalog, which then finds nothing missing."""
    attach_mirror(con, tmp_path)
    registered_log(con)
    config = ApplyConfig("rt", ("public.items",), database="rt")
    other = con.cursor()
    other.execute("BEGIN")
    racer = ApplyRun(other, config)
    for statement in racer.mirror_statements({}):
        racer.execute(statement)
    apply = ApplyRun(con, config)
    reads = []
    real = apply.mirror_catalog

    def catalog():
        if reads:  # the retry: the other initializer has committed by now
            other.execute("COMMIT")
        reads.append(1)
        return real()

    apply.mirror_catalog = catalog
    apply.initialize()
    assert len(reads) == 2
    assert apply.mirror_statements(real()) == []


def test_initialize_conflict_never_migrates_a_concurrent_legacy_initializer(con):
    other = con.cursor()
    other.execute("BEGIN")
    other.execute('CREATE SCHEMA "rt"."rt"')
    for statement in TABLES.values():
        other.execute(statement.replace("{s}", '"rt"."rt".'))
    log = IngestLog(con, make_config())
    with pytest.raises(duckdb.TransactionException, match="[Cc]onflict"):
        log.initialize()
    other.execute("COMMIT")
    before = log.catalog()
    with pytest.raises(SourceMismatch, match="fresh schema"):
        log.initialize()
    assert log.catalog() == before and "protocol_v2" not in before


# Apply: leases, the baseline precheck and retention pin writes --------------


def apply_table_row(con, relation_id=1, token=0):
    con.execute(
        'INSERT INTO "rt_mirror"._cdc.apply_state (relation_id,schema_name,table_name,'
        "key_columns,generation_id,epoch,applied_through_lsn,state,token) "
        "VALUES (?,'public','items','[\"id\"]','g1',1,0,'live',?)",
        [relation_id, token],
    )


@pytest.fixture
def applied(mirror):
    apply = ApplyRun(mirror, ApplyConfig("rt", ("public.items",), database="rt"))
    apply.initialize()
    apply_table_row(mirror.con)
    mirror.take()
    return apply, mirror


def test_apply_lease_counts(applied):
    apply, counting = applied
    current = apply.state(1)
    assert counting.take() == ["read:state"]
    token = apply.acquire(1, "run", token=current["token"])
    assert token == current["token"] + 1
    assert counting.take() == ["apply_acquire"]  # one statement with a known token
    apply.release(1, token)
    assert counting.take() == ["apply_release"]  # one autocommit statement
    token = apply.acquire(1, "run")
    assert counting.take() == [
        "apply_acquire",
        "read:apply_token",
        "COMMIT",
    ]
    assert apply.state(1)["token"] == token
    apply.release(1, token)


def test_apply_acquire_is_busy_after_a_lease_taken_and_released_since_its_read(applied):
    apply, counting = applied
    current = apply.state(1)
    other = ApplyRun(counting.con.cursor(), apply.config)
    other.release(1, other.acquire(1, "other"))
    with pytest.raises(ApplyLeaseBusy):
        apply.acquire(1, "ours", token=current["token"])
    assert apply.state(1)["token"] == current["token"] + 1  # only the other run's


def test_apply_acquire_with_a_token_keeps_every_lease_predicate(applied):
    apply, counting = applied
    current = apply.state(1)
    held = apply.acquire(1, "holder", token=current["token"])
    with pytest.raises(ApplyLeaseBusy):  # unexpired, even with the current token
        apply.acquire(1, "second", token=held)


def test_backfill_generation_lease_counts(mirror):
    backfill = Backfill(mirror, BackfillConfig("rt", ("public.items",), database="rt"))
    backfill.initialize()
    mirror.con.execute(
        'INSERT INTO "rt_mirror"._cdc.generations (generation_id,relation_id,mode,epoch,'
        "pin_lsn,state,token) VALUES ('g1',1,'backfill',1,0,'snapshotting',0)"
    )
    mirror.take()
    token = backfill.acquire_generation("g1", "run", token=0)
    assert token == 1 and mirror.take() == ["update:generations"]
    backfill.release_generation("g1", token)
    assert mirror.take() == ["update:generations"]
    with pytest.raises(ApplyLeaseBusy):  # the token moved on: busy, never a second lease
        backfill.acquire_generation("g1", "stale", token=0)
    mirror.con.execute("UPDATE \"rt_mirror\"._cdc.generations SET state='published'")
    with pytest.raises(ApplyLeaseBusy):  # the state predicate stays
        backfill.acquire_generation("g1", "run", token=1)


def test_merge_precheck_runs_only_for_an_incomplete_upsert(applied, tmp_path):
    from cdc_flight.mirror import Column, Image

    apply, counting = applied
    columns = [Column("id", "BIGINT", 20, -1), Column("v", "VARCHAR", 25, -1)]
    current = dict(
        target='"rt_mirror"."public"."items"', keys=("id",), columns=None, schema="public"
    )
    complete = {(1,): Image("upsert", {"id": "1", "v": "a"}, True, 5, 0)}
    apply._write(current, columns, complete, guard=lambda: None, commit_state=lambda: None)
    kinds = counting.take()
    assert not any("count(*)" in kind for kind in kinds) and "COMMIT" in kinds
    current["columns"] = columns
    partial = {(2,): Image("upsert", {"id": "2"}, False, 6, 0)}
    from cdc_flight.mirror import Blocked

    with pytest.raises(Blocked, match="full image"):
        apply._write(current, columns, partial, guard=lambda: None, commit_state=lambda: None)
    assert any("count(*)" in kind for kind in counting.take())


def test_retention_pin_write_counts(mirror):
    from cdc_flight.retention import Retention, generation_owner, table_owner

    retention = Retention(mirror, "rt", "rt")
    # Table 1 is registered (every table pin has one; pin_state is set once), and
    # format-2 pins sit at complete unit ends.
    unit_ends(mirror.con, 10, 20, 30)
    retention.register_generation(1, "g1", 1, 10)
    # read:pin_cut: the position must be the epoch start or a complete unit end, above the
    # floor (Retention2), read before the guarded transaction.
    assert mirror.take() == ["read:pin_cut", "update:retention", "insert:retention_pins", "COMMIT"]
    # A re-registration (e.g. a retry of an ambiguous commit) updates the pin: the
    # upsert counts 1, so it is never mistaken for a floor refusal.
    retention.register_generation(1, "g1", 1, 10)
    retention.register_generation(1, "g1", 1, 20)
    assert retention.pin(generation_owner("g1"))["lsn"] == 20
    retention.register_generation(1, "g1", 1, 10)
    mirror.take()
    assert retention.transfer(1, "g1", 1, 20)  # moves the table pin to g1
    assert mirror.take() == [
        "read:pin_cut",
        "update:retention",
        "read:pins",
        "insert:retention_pins",
        "update:tables",  # pin_state, once per table (compaction)
        "delete:retention_pins",
        "COMMIT",
    ]
    assert retention.pins() == {table_owner(1): (1, 20)}
    mirror.take()
    assert retention.transfer(1, "g1", 1, 20)  # already owned: only the cleanup
    assert mirror.take()[3:] == [
        "delete:retention_pins",
        "COMMIT",
    ]
    assert not retention.transfer(1, "replaced", 1, 30)  # neither pin: no writes
    assert "insert:retention_pins" not in mirror.take()
    assert retention.pin(generation_owner("g1")) is None


def test_retention_pin_writes_refuse_below_the_floor(mirror):
    from cdc_flight.retention import HistoryDropped, Retention, generation_owner

    retention = Retention(mirror.con, "rt", "rt")
    unit_ends(mirror.con, 10, 40, 50)
    retention.register_generation(1, "g1", 1, 10)
    mirror.con.execute('UPDATE "rt"."rt".retention SET floor_epoch=1, floor_lsn=50')
    with pytest.raises(HistoryDropped):
        retention.register_generation(1, "g2", 1, 50)  # at the floor: refused
    with pytest.raises(HistoryDropped):
        retention.transfer(1, "g1", 1, 40)  # not owned, below the floor: refused
    assert retention.pins() == {generation_owner("g1"): (1, 10)}
    unit_ends(mirror.con, epoch=2, start=1)
    retention.register_generation(1, "g3", 2, 1)  # a later epoch is above the floor
    assert generation_owner("g3") in retention.pins()


def test_retention_register_requires_initialized_metadata(mirror):
    from cdc_flight.retention import Retention

    # Format 2 refuses at the cut check, before the guarded write (v1: PinMissing).
    mirror.con.execute('DELETE FROM "rt"."rt".retention')
    with pytest.raises(SourceMismatch):
        Retention(mirror.con, "rt", "rt").register_generation(1, "g1", 1, 10)


def test_a_stale_release_leaves_the_successors_lease(applied):
    apply, counting = applied
    stale = apply.acquire(1, "stale", token=apply.state(1)["token"])
    counting.con.execute(
        'UPDATE "rt_mirror"._cdc.apply_state SET expires_at=current_timestamp-INTERVAL 1 SECOND'
    )
    successor = apply.acquire(1, "successor", token=stale)
    apply.release(1, stale)  # the stale run's cleanup
    holder = counting.con.execute('SELECT holder FROM "rt_mirror"._cdc.apply_state').fetchone()
    assert holder == ("successor",)
    with pytest.raises(ApplyLeaseBusy):
        apply.acquire(1, "third", token=successor)


def test_transfer_repins_a_table_pin_of_another_epoch(mirror):
    from cdc_flight.retention import Retention, table_owner

    retention = Retention(mirror.con, "rt", "rt")
    unit_ends(mirror.con, 10, 20)
    retention.register_generation(1, "g1", 1, 10)
    assert retention.transfer(1, "g1", 1, 20)
    unit_ends(mirror.con, 6, epoch=2, start=5)
    retention.register_generation(1, "g1", 2, 5)  # the same generation id, a later epoch
    assert retention.transfer(1, "g1", 2, 6)
    assert retention.pins() == {table_owner(1): (2, 6)}


# Apply: one read per apply step, the idle fast path, committed pins ------


@pytest.fixture
def harness(stores, tmp_path):
    from test_phase2 import ITEMS, Harness, relation_message

    h = Harness(stores, tmp_path)
    w = h.wire
    h.ingest(w.txn(relation_message(1, "items", ITEMS), w.insert(1, ("1", "a", "t", "1.00"))))
    assert h.run("items").status == "caught_up"  # creates the mirror table
    counting = Counting(
        h.apply.con,
        f"{h.apply.prefixes['{m}']}",
        f"{h.apply.prefixes['{s}']}",
    )
    h.apply.con = h.apply.retention.con = h.apply.reader.con = counting
    return h, counting


@pytest.mark.parametrize("stores", ["local"], indirect=True)
@pytest.mark.parametrize("fold", ["sql", "python"])
def test_apply_table_counts(harness, fold):
    """The SQL fold reads the same number of statements as the Python fold: its plan
    statement (frame checks + plan) takes the frames read's place, after the descriptors,
    and the write transaction is the same length (the precheck is inside the MERGE)."""
    h, counting = harness
    h.apply.fold = fold
    w = h.wire
    h.ingest(w.txn(w.insert(1, ("2", "b", "t", "2.00"))))
    counting.take()
    assert h.run("items").status == "caught_up"
    reads = (
        ["read:descriptors", "read:frame_plan"]  # the frames never leave the database
        if fold == "sql"
        else ["read:frames", "read:descriptors"]  # the page's frames, split per table
    )
    assert counting.take() == [
        "read:head",  # before the lease: not idle (work pending)
        "apply_acquire",
        "read:head",  # under the lease
        "read:unit_position",  # units(): the reader's head (format and high)
        "read:units",  # the page, with the retention floor in the same statement
        *reads,
        "apply_guard",
        "merge",  # no baseline count: every image is complete / the precheck is a clause
        "write:apply_state",
        "COMMIT",
        "advance_pin",  # complete-unit/floor validation is inside the UPDATE
        "read:head",
        "insert:runs",
        "apply_release",
    ]
    # Idle: one read, the run record, no lease.
    assert h.run("items").status == "caught_up"
    assert counting.take() == ["read:head", "insert:runs"]


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_idle_path_never_skips_work_a_hold_or_a_pin_repair(harness):
    """Every reason the loop head acts takes the slow path; only a table with
    nothing to do and its pin at the checkpoint returns without the lease."""
    h, counting = harness
    apply = h.apply
    relation_id = apply.registered("public.items")[0]
    head = apply.head("public.items")
    assert apply.idle(head)
    current = head["current"]

    def variant(**changes):
        changed = dict(head, **changes)
        return apply.idle(changed)

    ahead = dict(head["source"], confirmed=current["applied"] + 100)
    checked = dict(head["key"], checked=current["applied"] + 100)
    assert not variant(source=ahead, key=checked)  # work pending
    assert variant(source=ahead)  # the key isn't confirmed past applied: nothing to do
    assert not variant(source=dict(head["source"], epoch=current["epoch"] + 1))
    assert not variant(key=dict(head["key"], version=current["key_version"] + 1), cut=0)
    # A key confirmed in another epoch limits apply to 0: the slow path's first
    # iteration also ends at once ("done") without a hold, so idle agrees with it.
    other_epoch = dict(head["key"], key_epoch=current["epoch"] + 1)
    assert variant(key=other_epoch)
    assert variant(key=other_epoch, source=ahead)  # limit 0: still nothing to apply
    assert next_step(current, current["epoch"], ahead["confirmed"], False, 0)[0] == "done"
    assert not variant(pin=None)
    assert not variant(pin=dict(head["pin"], lsn=head["pin"]["lsn"] - 1))
    assert not variant(pin=dict(head["pin"], generation="another"))
    with pytest.raises(Exception, match="key version history"):
        variant(key=dict(head["key"], version=current["key_version"] + 1), cut=None)
    # A lagging pin: the run takes the lease and repairs it.
    pin = apply.retention.pin(f"table:{relation_id}")
    counting.con.execute(
        f"UPDATE {apply.prefixes['{s}']}retention_pins SET pinned_lsn=0 WHERE owner=?",
        [f"table:{relation_id}"],
    )
    counting.take()
    assert h.run("items").status == "caught_up"
    kinds = counting.take()
    assert "apply_acquire" in kinds and "advance_pin" in kinds
    assert apply.retention.pin(f"table:{relation_id}")["lsn"] == pin["lsn"]


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_a_held_lease_reports_busy_without_taking_it(harness):
    h, counting = harness
    relation_id = h.apply.registered("public.items")[0]
    token = h.apply.acquire(relation_id, "holder", token=h.apply.state(relation_id)["token"])
    counting.take()
    assert h.run("items").status == "busy"
    assert counting.take() == ["read:head"]
    h.apply.release(relation_id, token)


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_a_transaction_split_across_two_days_applies_whole(harness):
    """Its unit's chunks land in two day tables, the second when it completes. Apply
    reads the unit's days from its chunk ledger, once per page, after the checkpoint
    passed the commit."""
    from test_phase2 import TABLES

    h, counting = harness
    w = h.wire
    rows = [(str(i), "x" * 300, "t", f"{i}.00") for i in range(10, 30)]
    items = [
        item for message in w.txn(*[w.insert(1, r) for r in rows]) for item in h.spool.feed(message)
    ]
    chunks = [item for item in items if isinstance(item, Chunk)]
    assert len(chunks) >= 2
    for day, part in (("20990101", items[:1]), ("20990102", items[1:])):
        h.log.acquire("test")
        try:
            h.log.append(part, partition_day=day)
            h.log.record_keys(
                {oid: list(keys) for oid, (_, keys) in TABLES.items()}, h.log.checkpoint(), 0
            )
        finally:
            h.log.release()
    s = h.apply.prefixes["{s}"]
    days = counting.con.execute(
        f"SELECT DISTINCT partition_day FROM {s}chunks_v2 WHERE unit_start_lsn="
        f"(SELECT max(unit_start_lsn) FROM {s}units_v2 WHERE complete) ORDER BY 1"
    ).fetchall()
    assert days == [("20990101",), ("20990102",)]
    assert h.run("items").status == "caught_up"
    ids = [r[0] for r in h.rows("items", "id")]
    assert all(int(i) in ids for i, *_ in rows)


def assert_pin_at_checkpoint(h):
    """C2: the table pin never exceeds the committed checkpoint (it would let
    compaction drop history apply still needs, and is never lowered)."""
    relation_id = h.apply.registered("public.items")[0]
    pin = h.apply.retention.pin(f"table:{relation_id}")
    applied = h.apply.state(relation_id)["applied"]
    assert pin["lsn"] == applied, (pin, applied)


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_held_and_poisoned_batches_pin_only_the_committed_checkpoint(harness):
    from test_phase2 import UNCHANGED

    h, counting = harness
    w = h.wire
    # An update of a row the mirror lacks, without a full image: held, nothing applied.
    h.ingest(w.txn(w.update(1, ("99", "z", UNCHANGED, "9.00"))))
    result = h.run("items")
    assert result.status == "needs_action" and "full image" in result.detail
    assert_pin_at_checkpoint(h)


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_a_poisoned_batch_pins_its_committed_prefix(harness):
    h, counting = harness
    w = h.wire
    h.ingest(
        w.txn(w.insert(1, ("5", "a", "t", "5.00"))),
        w.txn(w.insert(1, ("6", "a", "t", "not a number"))),
        w.txn(w.insert(1, ("7", "a", "t", "7.00"))),
    )
    result = h.run("items")
    assert result.status == "needs_action" and "ConversionException" in result.detail
    assert [r[0] for r in h.rows("items", "id")] == [1, 5]
    assert_pin_at_checkpoint(h)


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_a_failed_pin_advance_is_repaired_before_the_run_ends(harness, monkeypatch):
    """advance_pin swallows errors; only an advance that returned True counts, so
    the run's final advance repairs one that failed after a write."""
    h, counting = harness
    w = h.wire
    h.ingest(w.txn(w.insert(1, ("8", "a", "t", "8.00"))))
    real, calls = h.apply.retention.advance, []

    def flaky(*args):
        calls.append(args)
        if len(calls) == 1:
            raise duckdb.IOException("transient")
        return real(*args)

    monkeypatch.setattr(h.apply.retention, "advance", flaky)
    assert h.run("items").status == "caught_up"
    assert len(calls) == 2
    assert_pin_at_checkpoint(h)


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_a_failed_advance_before_a_hold_is_repaired_before_the_run_ends(harness, monkeypatch):
    """The same, when the batch then holds: the run ends without another loop-head
    read, so only the advance's own result says the pin is still behind."""
    h, counting = harness
    w = h.wire
    h.ingest(
        w.txn(w.insert(1, ("5", "a", "t", "5.00"))),
        w.txn(w.insert(1, ("6", "a", "t", "not a number"))),
    )
    real, calls = h.apply.retention.advance, []

    def flaky(*args):
        calls.append(args)
        if len(calls) == 1:
            raise duckdb.IOException("transient")
        return real(*args)

    monkeypatch.setattr(h.apply.retention, "advance", flaky)
    assert h.run("items").status == "needs_action"
    assert len(calls) == 2
    assert_pin_at_checkpoint(h)


@pytest.mark.parametrize("stores", ["local"], indirect=True)
@pytest.mark.parametrize("unit", ["missing", "incomplete"])
def test_events_without_a_complete_unit_are_never_applied(harness, unit):
    """Format 2's manifest is the unit: a page must be a contiguous chain of complete
    units up to the high, or the read fails and nothing applies."""
    h, counting = harness
    w = h.wire
    h.ingest(w.txn(w.insert(1, ("5", "a", "t", "5.00"))))
    s = h.apply.prefixes["{s}"]
    change = "DELETE FROM" if unit == "missing" else "UPDATE"
    change += f" {s}units_v2 "
    change += "" if unit == "missing" else "SET complete=false "
    counting.con.execute(
        change + f"WHERE unit_start_lsn=(SELECT max(unit_start_lsn) FROM {s}units_v2)"
    )
    before = h.rows("items", "id")
    with pytest.raises(ValueError, match="unit chain"):
        h.run("items")
    assert h.rows("items", "id") == before


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_the_unit_page_scans_the_ledger_only_from_the_checkpoint(harness):
    """The page read carries the table's inclusive lower bound, so a unit ledger that
    grows with every unit is pruned below the checkpoint (Phase 4 ledger growth)."""
    h, counting = harness
    w = h.wire
    h.ingest(w.txn(w.insert(1, ("2", "b", "t", "2.00"))))
    pages, execute = [], counting.execute

    def capture(sql, parameters=None):
        if "list(u ORDER BY unit_start_lsn)" in sql:
            pages.append((sql, list(parameters or [])))
        return execute(sql, parameters)

    counting.execute = capture
    applied = h.apply.state(h.apply.registered("public.items")[0])["applied"]
    assert h.run("items").status == "caught_up"
    [(sql, parameters)] = pages
    assert "(unit_start_lsn>=? AND unit_start_lsn<?)" in re.sub(r"\s+", " ", sql)
    assert parameters[1] == applied  # the units after the table's own checkpoint


# Backfill catch-up on the SQL fold -------------


@pytest.mark.parametrize("fold", ["sql", "python"])
def test_backfill_catch_up_batch_counts(fresh, fold):  # noqa: F811
    """One catch-up batch: the SQL fold's plan statement takes the frames read's place,
    and the write transaction is the same length (the baseline check is a MERGE clause)."""
    import pgstream
    from test_apply2 import OID, ins
    from test_backfill_sql_fold import GEN, CEnv

    env = CEnv(fresh(fold), fold, rows=[("1", "a")])
    env.commit(ins("4", "d"), pgstream.update(OID, ("1", "z"), old=("1", "a")))
    env.feed([], close=True)
    b = env.backfill
    b.execute(
        "UPDATE {m}generations SET state='catching_up',scan_done=true,marker_lsn=? "
        "WHERE generation_id=?",
        [env.units()[-1][1], GEN],
    )
    gen = b.generation(GEN)
    counting = Counting(b.con, b.prefixes["{m}"], b.prefixes["{s}"])
    b.con = b.reader.con = counting
    assert b.catch_up("public.items", gen, env.token, deadline=None) is True
    reads = (
        ["read:descriptors", "read:frame_plan"]  # the frames never leave the database
        if fold == "sql"
        else ["read:frames", "read:descriptors"]
    )
    kinds = counting.take()
    assert kinds[:-1] == [
        "read:unit_position",  # units(): the reader's head (format and high)
        "read:units",
        *reads,
        "update:generations",  # guard_generation
        "merge",
        "update:generations",  # advance: the catchup_lsn compare-and-set
        "COMMIT",
    ]
    assert kinds[-1].startswith("remote:SELECT generation_id,")  # the generation re-read
