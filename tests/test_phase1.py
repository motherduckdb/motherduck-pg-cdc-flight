"""Phase 1 on the format-2 change log: replay, rollback, limits and registration on a
local store, then real PostgreSQL ingest (restart, process death, keepalives, preflight,
idle capture, partition leaves, busy readers) through the production factory
(bootstrap.bootstrap_log). The frozen format-1 encoding keeps only its golden here."""

import os
import re
import struct
from dataclasses import replace

import conftest
import pytest

from cdc_flight import format2 as f2
from cdc_flight.bootstrap import bootstrap_log
from cdc_flight.config import IngestConfig
from cdc_flight.format2 import EVENT, Chunk, UnitComplete, UnitConfig
from cdc_flight.ingest import run_once
from cdc_flight.ingest_log import LeaseBusy, SourceMismatch
from cdc_flight.ingest_log2 import IngestConfig2
from cdc_flight.preflight import PreflightError, check, query
from cdc_flight.runtime import open_log
from cdc_flight.source import Source
from cdc_flight.spool2 import Spool

# The local stores' text binding (test_format2_protocol's, by value: a fixture source).
SETTINGS = tuple(
    zip(
        f2.OUTPUT_SETTINGS,
        ("UTC", "ISO, YMD", "postgres", "3", "hex", "C", "base64", "content", "UTF8", "UTF8"),
    )
)


def row(*values):
    return struct.pack("!H", len(values)) + b"".join(
        b"n" if value is None else b"t" + struct.pack("!I", len(value.encode())) + value.encode()
        for value in values
    )


def relation(oid=1):
    return (
        b"R"
        + struct.pack("!I", oid)
        + b"public\0items\0f"
        + struct.pack("!H", 2)
        + b"\1id\0"
        + struct.pack("!Ii", 23, -1)
        + b"\0value\0"
        + struct.pack("!Ii", 25, -1)
    )


def transaction(count=10, commit=100):
    return (
        [b"B" + struct.pack("!QqI", commit, 0, 1), relation()]
        + [b"I" + struct.pack("!I", 1) + b"N" + row(str(i), "payload" * 50) for i in range(count)]
        + [b"C\0" + struct.pack("!QQq", commit, commit + 1, 0)]
    )


def empty_transaction(commit):
    return [
        b"B" + struct.pack("!QqI", commit, 0, 2),
        b"C\0" + struct.pack("!QQq", commit, commit + 1, 0),
    ]


@pytest.fixture
def config(stores):
    """A local format-2 source: the fixture binding, one commit per unit (so a
    transaction's unit is exactly that transaction), small chunks."""
    s, _ = stores
    base = IngestConfig(
        "phase1",
        "phase1_slot",
        "phase1_pub",
        ("public.items",),
        database=s.database.strip('"'),
        chunk_bytes=1024,
        batch_bytes=2048,
        max_event_bytes=10000,
        allow_local=True,
        epoch=1,
    )
    return IngestConfig2(
        **base.__dict__,
        text_binding=f2.Binding("phase1", SETTINGS, (("plpgsql", "1.0"),)),
        unit_config=UnitConfig(max_commits=1, chunk_bytes=1024),
        server_major=16,
    )


@pytest.fixture
def log(stores, config):
    """A fresh format-2 store through the factory's explicit creation, registered at 0
    and leased."""
    s, _ = stores
    log = open_log(
        s.con,
        config,
        create=True,
        binding=config.text_binding,
        unit_config=config.unit_config,
        server_major=config.server_major,
    )
    log.register(("system", 1), 0, [dict(oid=1, schema="public", name="items", keys=["id"])])
    log.acquire("test")
    try:
        yield log
    finally:
        log.release()


def spool_all(config, tmp_path, wire, *, start_lsn=0, key_columns=None):
    """Spool2 over the wire messages, closed at the end when no transaction is open."""
    spool = Spool(
        config,
        key_columns or {1: ("id",)},
        2,
        "marker",
        binding=config.text_binding,
        start_lsn=start_lsn,
        unit_config=config.unit_config,
        clock=lambda: 0,
    )
    items = []
    for message in wire:
        items.extend(spool.feed(message))
    if not spool.active:
        items.extend(spool.close())
    return items


def frames(log):
    """A UNION over every frames_v2 day table."""
    days = [d for (d,) in log.execute("SELECT DISTINCT partition_day FROM {s}chunks_v2").fetchall()]
    return " UNION ALL ".join(f"SELECT * FROM {{s}}frames_v2_{day}" for day in days)


def stored_exactly_once(log):
    """Every frame stored once, every chunk row matching its frames, no unit left open.
    Returns the number of frames."""
    union = frames(log)
    total, distinct = log.execute(
        "SELECT count(*),count(DISTINCT (source_epoch,unit_start_lsn,frame_ordinal)) FROM ("
        + union
        + ")"
    ).fetchone()
    assert total == distinct, "a frame was stored twice"
    assert log.execute("SELECT sum(n) FROM {s}chunks_v2").fetchone() == (total,)
    assert log.execute("SELECT count(*) FROM {s}units_v2 WHERE NOT complete").fetchone() == (0,)
    return total


def events(log, oid, op):
    """(key id, count) of the relation's stored EVENT frames of one op."""
    return log.execute(
        "SELECT new_key.values[1]::INTEGER,count(*) FROM ("
        + frames(log)
        + f") WHERE record_kind={EVENT} AND relation_id=? AND op=? GROUP BY 1 ORDER BY 1",
        [oid, op],
    ).fetchall()


def op_count(log, oid, op):
    return log.execute(
        "SELECT count(*) FROM ("
        + frames(log)
        + f") WHERE record_kind={EVENT} AND relation_id=? AND op=?",
        [oid, op],
    ).fetchone()[0]


def test_chunk_replay_midnight_complete_boundary_and_truncate(config, log, tmp_path):
    items = spool_all(config, tmp_path, transaction())
    chunks = [x for x in items if isinstance(x, Chunk)]
    assert len(chunks) > 1 and isinstance(items[-1], UnitComplete)
    log.append(chunks[:2], partition_day="20261103")
    assert log.checkpoint() == 0  # a durable prefix of an open unit moves no checkpoint
    assert log.execute("SELECT complete FROM {s}units_v2").fetchone() == (False,)
    # The replay revalidates the durable chunks and writes only the new ones, on a new day.
    log.append(items, partition_day="20261104")
    assert log.checkpoint() == 101
    log.append(items, partition_day="20261104")  # idempotent
    frames_total = sum(c.count for c in chunks)
    assert log.execute("SELECT sum(n) FROM {s}chunks_v2").fetchone() == (frames_total,)
    assert log.execute("SELECT frame_count,complete FROM {s}units_v2").fetchone() == (
        frames_total,
        True,
    )
    # A replayed chunk whose contents disagree is refused (its digest is checked first).
    with pytest.raises(ValueError, match="invalid chunk digest|different contents"):
        log.append([replace(chunks[0], digest="bad")], partition_day="20261104")
    wires = [
        b"B" + struct.pack("!QqI", 200, 0, 2),
        relation(),  # a fresh spool needs the descriptor, as after a restart
        b"T" + struct.pack("!IBI", 1, 0, 1),
        b"C\0" + struct.pack("!QQq", 200, 201, 0),
    ]
    log.append(spool_all(config, tmp_path, wires, start_lsn=101), partition_day="20261104")
    assert log.execute("SELECT state FROM {s}tables WHERE relation_id=1").fetchone() == (
        "needs_resync",
    )


def test_rollback_ambiguous_commit_empty_transaction_and_missing_chunk(config, log, tmp_path):
    items = spool_all(config, tmp_path, transaction())

    def before(stage):
        if stage == "before_commit":
            raise RuntimeError("injected")

    with pytest.raises(RuntimeError):
        log.append(items, fault=before)
    assert log.checkpoint() == 0
    assert log.execute("SELECT count(*) FROM {s}chunks_v2").fetchone() == (0,)
    with pytest.raises(ValueError):  # a terminal without its chunks
        log.append([items[-1]])
    assert log.checkpoint() == 0

    def after(stage):
        if stage == "after_commit":
            raise RuntimeError("lost response")

    with pytest.raises(RuntimeError):
        log.append(items, fault=after)
    log.append(items)
    assert log.checkpoint() == 101
    log.append(spool_all(config, tmp_path, empty_transaction(200), start_lsn=101))
    assert log.checkpoint() == 201


def test_limits_and_immutable_registration(config, log, tmp_path):
    with pytest.raises(ValueError, match="max_event_bytes"):
        spool_all(replace(config, max_event_bytes=2), tmp_path, transaction())
    with pytest.raises(SourceMismatch):
        log.register(("different", 1), 0, [])
    with pytest.raises(LeaseBusy):
        log.acquire("overlap")


@pytest.fixture
def pg_phase1(stores):
    dsn = os.getenv("CDC_TEST_PG_DSN")
    if not dsn:
        pytest.skip("set CDC_TEST_PG_DSN for real PostgreSQL probes")
    s, _ = stores
    with Source(dsn) as source:
        suffix = source.schema
        config = IngestConfig(
            "phase1",
            "p1_" + suffix,
            "p1_" + suffix,
            (suffix + ".items",),
            heartbeat=suffix + ".pulse",
            database=s.database.strip('"'),
            dsn=dsn,
            allow_local=True,
            chunk_bytes=2048,
            batch_bytes=8192,
            flush_seconds=0.1,
            # Runs end when their marker drains; the cap only bounds a stuck run.
            # 15 s was too tight for MotherDuck latency after a crash recovery.
            runtime_seconds=60,
        )
        try:
            yield source, config
        finally:
            conftest.wait_slot_released(source.con, config.slot)
            query(
                source.con,
                "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
                "WHERE slot_name=%s",
                [config.slot],
            )
            from psycopg2 import sql

            query(
                source.con,
                sql.SQL("DROP PUBLICATION IF EXISTS {}").format(sql.Identifier(config.publication)),
            )


@pytest.mark.postgres
def test_real_ingest_restart_feedback_idle_and_schema(stores, pg_phase1):
    source, base = pg_phase1
    with pytest.raises(PreflightError, match="missing"):
        check(base)
    log, info = bootstrap_log(stores[0].con, base, create_objects=True)
    config = log.config
    oid = info.tables[0]["oid"]
    source.execute("INSERT INTO items SELECT i,repeat('x',1000),'old' FROM generate_series(1,20) i")
    source.execute("ALTER TABLE items ADD COLUMN added text")
    source.execute("UPDATE items SET added='yes' WHERE id=1")
    fired = False

    def kill(stage):
        nonlocal fired
        if stage == "after_commit" and not fired:
            fired = True
            raise RuntimeError("lost response")

    with pytest.raises(RuntimeError, match="lost response"):
        run_once(config, log, info=info, fault=kill)
    before = log.checkpoint()
    result = run_once(config, log)
    # The restart resumes at the last complete unit END and stores every frame once.
    assert result.status == "drained" and result.boundary > before
    stored_exactly_once(log)
    # Two schema versions of the relation: before and after the ALTER.
    assert log.execute(
        "SELECT count(DISTINCT schema_version) FROM ("
        + frames(log)
        + f") WHERE record_kind={EVENT} AND relation_id=?",
        [oid],
    ).fetchone() == (2,)
    assert events(log, oid, "insert") == [(i, 1) for i in range(1, 21)]
    assert op_count(log, oid, "update") == 1
    # Idle: heartbeats still advance the source position.
    idle = run_once(config, log)
    assert idle.boundary > result.boundary
    source.execute("TRUNCATE items")
    run_once(config, log)
    assert log.execute("SELECT state FROM {s}tables WHERE relation_id=?", [oid]).fetchone() == (
        "needs_resync",
    )
    # A truncate frame names its relations in truncate_oids.
    assert log.execute(
        "SELECT count(*) FROM ("
        + frames(log)
        + f") WHERE record_kind={EVENT} AND op='truncate' AND list_contains(truncate_oids,?)",
        [oid],
    ).fetchone() == (1,)
    # A slot advanced beyond the destination is a hard failure before decoding.
    source.execute("INSERT INTO items VALUES (100,'x','y','z')")
    conftest.wait_slot_released(source.con, config.slot)
    query(source.con, "SELECT pg_replication_slot_advance(%s,pg_current_wal_lsn())", [config.slot])
    with pytest.raises(SourceMismatch, match="ahead"):
        run_once(config, log)


@pytest.mark.postgres
def test_repeated_process_death(stores, pg_phase1, tmp_path):
    """Abrupt exits bypass rollback/release, unlike an exception injection."""
    import json
    import subprocess
    import sys
    from dataclasses import asdict
    from pathlib import Path

    source, base = pg_phase1
    log, info = bootstrap_log(stores[0].con, base, create_objects=True)
    config = log.config
    settings = asdict(base)  # the actor reopens the store: its recorded config wins
    del settings["dsn"]
    # Local DuckDB cannot share a writable file across processes. Close parent handles
    # during the actor and reopen before assertions; MotherDuck allows both.
    remote = stores[0].remote
    from psycopg2 import sql

    # Each stage is one transaction of `rows` 3000-byte rows over several 8 KiB batches,
    # so the crash leaves later batches of the transaction pending (asserted below).
    rows = conftest.md_size(stores, local=20, motherduck=8)
    seen = set()

    def stage_batches():
        counts = log.execute(
            "SELECT unit_start_lsn, count(*) FROM {s}chunks_v2 GROUP BY ALL"
        ).fetchall()
        return {lsn: n for lsn, n in counts if lsn not in seen}

    for index, stage in enumerate(
        ("mid_chunk", "after_payload", "after_commit", "before_feedback")
    ):
        source.execute(
            f"INSERT INTO items SELECT i,repeat('x',3000),'y' FROM generate_series({index * rows + 1},{index * rows + rows}) i"
        )
        filename = tmp_path / "actor.json"
        filename.write_text(
            json.dumps(dict(config=settings, address=stores[0].address, stage=stage))
        )
        if not remote:
            for store in stores:
                store.con.close()
        result = subprocess.run(
            [
                sys.executable,
                str(Path(__file__).resolve().parents[1] / "scripts/ingest_actor.py"),
                str(filename),
            ],
            capture_output=True,
            timeout=120,
        )
        if not remote:
            import duckdb

            for store in stores:
                store.con = duckdb.connect(store.address)
                store.con.execute(
                    f"ATTACH IF NOT EXISTS '{store.attachment}' AS {store.mirror_database}"
                )
            log = open_log(stores[0].con, config)
        assert result.returncode == 77, result.stderr.decode()
        assert log.execute("SELECT holder IS NOT NULL FROM {s}source").fetchone() == (True,)
        # Simulate waiting out the lease using database time, only after the process died.
        import time

        import duckdb

        deadline = time.monotonic() + 180
        while True:
            try:
                log.execute("UPDATE {s}source SET expires_at=current_timestamp-INTERVAL '1 second'")
                break
            except duckdb.TransactionException:
                # Remote disconnect rollback is asynchronous after os._exit.
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.2)
        # The killed actor's walsender can still hold the slot for a moment; recovery
        # would then correctly report busy.
        conftest.wait_slot_released(source.con, config.slot)
        before_recovery = stage_batches()
        result = run_once(config, log)
        assert result.status == "drained"
        after_recovery = stage_batches()
        seen |= set(after_recovery)
        # The stage's unit spans several chunks, and the crash left some of them for
        # recovery, so a smaller `rows` still crashes mid-unit. The stage's transaction's
        # unit is the multi-chunk one.
        lsn = max(after_recovery, key=after_recovery.get)
        assert after_recovery[lsn] >= 2, after_recovery
        assert before_recovery.get(lsn, 0) < after_recovery[lsn], (before_recovery, after_recovery)
        stored_exactly_once(log)
        observed = events(log, info.tables[0]["oid"], "insert")
        source_ids = query(
            source.con,
            sql.SQL("SELECT id FROM {} ORDER BY id").format(sql.Identifier(source.schema, "items")),
        )
        assert observed == [(value, 1) for (value,) in source_ids]
        assert len(observed) == (index + 1) * rows
        assert log.execute("SELECT holder FROM {s}source").fetchone() == (None,)


def test_large_spool_is_bounded(config, tmp_path):
    """One 9,000-event transaction streams out as bounded chunks: the spool never holds
    more than a chunk's bytes and frames."""
    big = replace(
        config,
        chunk_bytes=1024 * 1024,
        batch_bytes=2 * 1024 * 1024,
        unit_config=UnitConfig(max_commits=1, chunk_bytes=1024 * 1024),
    )
    spool = Spool(
        big,
        {1: ("id",)},
        2,
        "marker",
        binding=big.text_binding,
        start_lsn=0,
        unit_config=big.unit_config,
        clock=lambda: 0,
    )
    spool.feed(b"B" + struct.pack("!QqI", 100, 0, 1))
    spool.feed(relation())
    count = chunks = 0
    for i in range(9000):
        for item in spool.feed(b"I" + struct.pack("!I", 1) + b"N" + row(str(i), "x" * 8192)):
            count += item.event_count
            chunks += 1
        assert spool.buffer_bytes < big.unit_config.chunk_bytes + 20000
        assert len(spool.rows) < 150
    items = spool.feed(b"C\0" + struct.pack("!QQq", 100, 101, 0)) + spool.close()
    count += sum(x.event_count for x in items if isinstance(x, Chunk))
    assert count == 9000 and chunks > 64
    assert items[-1].event_count == 9000 and not spool.active


@pytest.mark.postgres
def test_keepalives_during_slow_upload(stores, pg_phase1):
    import time

    from psycopg2.extensions import make_dsn

    source, original = pg_phase1
    base = replace(
        original,
        chunk_bytes=1024 * 1024,
        batch_bytes=2 * 1024 * 1024,
        dsn=make_dsn(original.dsn, options="-c wal_sender_timeout=2000"),
    )
    log, _ = bootstrap_log(stores[0].con, base, create_objects=True)
    config = log.config
    source.execute("INSERT INTO items VALUES(1,'x','y')")
    # The committed checkpoint, read on another connection while an append is open.
    durable = open_log(stores[1].con, base)
    called = []

    def delay(stage):
        if stage == "after_payload":
            time.sleep(3.5)  # longer than source wal_sender_timeout
            # Durable first: the slot can only move after a durable append.
            limit = durable.checkpoint()
            confirmed = query(
                source.con,
                "SELECT confirmed_flush_lsn-'0/0'::pg_lsn FROM pg_replication_slots WHERE slot_name=%s",
                [config.slot],
            )[0][0]
            # A run may append more than once (the row, then the marker); each
            # earlier append is acknowledged. Never past what is durable. (Keepalives
            # are not read during an upload; test_feedback.py covers auto-replies.)
            assert int(confirmed) <= limit
            called.append(stage)

    result = run_once(config, log, fault=delay)
    assert called and result.status == "drained"
    # A successful send alone is insufficient: observe the source's slot acknowledgment.
    # The stream's close waits until the server has finished the copy, so the final
    # status is processed before run_once returns: no polling.
    confirmed = query(
        source.con,
        "SELECT confirmed_flush_lsn-'0/0'::pg_lsn FROM pg_replication_slots WHERE slot_name=%s",
        [config.slot],
    )[0][0]
    assert int(confirmed) == result.boundary


@pytest.mark.postgres
def test_preflight_rejects_before_provisioning(stores, pg_phase1):
    from psycopg2 import sql

    source, config = pg_phase1
    source.execute("ALTER TABLE items DROP CONSTRAINT items_pkey")
    with pytest.raises(PreflightError, match="primary key"):
        check(config, create_objects=True)
    assert query(source.con, "SELECT to_regclass(%s)", [config.heartbeat]) == [(None,)]
    source.execute("ALTER TABLE items ADD PRIMARY KEY (id)")
    query(
        source.con,
        sql.SQL("CREATE PUBLICATION {} FOR TABLE {} WITH (publish_via_partition_root=true)").format(
            sql.Identifier(config.publication), sql.Identifier(source.schema, "items")
        ),
    )
    with pytest.raises(PreflightError, match="membership") as differs:
        check(config, create_objects=True)
    assert query(source.con, "SELECT to_regclass(%s)", [config.heartbeat]) == [(None,)]
    assert query(
        source.con, "SELECT count(*) FROM pg_replication_slots WHERE slot_name=%s", [config.slot]
    ) == [(0,)]
    # The owner's statements: create the heartbeat, grant it, add it to the publication.
    statements = [x for x in str(differs.value).splitlines() if x.endswith(";")]
    assert [x.split()[0] for x in statements] == ["CREATE", "GRANT", "ALTER"]
    for statement in statements:
        source.execute(statement)
    assert check(config, create_objects=True).created == {"slot": True}


@pytest.mark.postgres
def test_a_role_that_owns_no_table_gets_the_owners_sql_and_then_creates_only_the_slot(
    stores, pg_phase1
):
    """On RDS the replication role owns none of the tables, so it can't create
    their publication. Preflight refuses before creating anything and prints the exact
    SQL for the tables' owner; once the owner has run it, preflight with create_objects
    creates only the slot. The role is a SET ROLE of the test connection (options), so
    the test needs no pg_hba entry."""
    from dataclasses import replace

    from psycopg2 import sql

    source, base = pg_phase1
    role = f"cdc_p1_{source.schema}"[:63]
    ident = sql.Identifier(role)
    source.execute(sql.SQL("CREATE ROLE {} NOLOGIN REPLICATION").format(ident))
    try:
        source.execute(
            sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(sql.Identifier(source.schema), ident)
        )
        source.execute(
            sql.SQL("GRANT SELECT ON {} TO {}").format(
                sql.Identifier(source.schema, "items"), ident
            )
        )
        config = replace(base, dsn=f"{base.dsn} options='-c role={role}'")
        # Without create_objects: what's missing, who runs what.
        with pytest.raises(PreflightError) as told:
            check(config)
        message = str(told.value)
        assert message.startswith("missing heartbeat table, publication, replication slot; ")
        assert "ask the tables' owner (the DBA) to run:" in message
        assert f"TO {role};" in message and "CDC_CREATE_OBJECTS=1" in message
        # With create_objects: refused before creating anything (not RDS's raw error),
        # and before the store's bootstrap marker, so the retry needs no recovery.
        from cdc_flight.runtime import _catalog

        store = stores[0].con
        with pytest.raises(PreflightError) as refused:
            bootstrap_log(store, config, create_objects=True)
        plan = str(refused.value)
        assert plan.startswith("this role can't create the heartbeat table and publication; ")
        assert query(source.con, "SELECT to_regclass(%s)", [config.heartbeat]) == [(None,)]
        assert _catalog(store, config) == set()
        # The owner runs exactly the printed statements; then preflight makes the slot.
        statements = [line for line in plan.splitlines()[1:] if line.endswith(";")]
        assert [line.split()[0] for line in statements] == ["CREATE", "GRANT", "CREATE"]
        for statement in statements:
            source.execute(statement)
        _, info = bootstrap_log(store, config, create_objects=True)
        assert info.created == {"slot": True}
    finally:
        source.execute(sql.SQL("DROP OWNED BY {}").format(ident))
        source.execute(sql.SQL("DROP ROLE {}").format(ident))


@pytest.mark.parametrize(
    "lacking", [("INSERT", "UPDATE"), ("SELECT",)], ids=["insert-update", "select"]
)
@pytest.mark.postgres
def test_a_heartbeat_grant_refusal_prints_the_grant(stores, pg_phase1, lacking):
    from dataclasses import replace

    from psycopg2 import sql

    source, base = pg_phase1
    role = f"cdc_hb_{source.schema}"[:63]
    ident = sql.Identifier(role)
    source.execute(sql.SQL("CREATE ROLE {} NOLOGIN REPLICATION").format(ident))
    try:
        check(base, create_objects=True)  # the superuser creates everything
        schema = sql.Identifier(source.schema)
        source.execute(sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(schema, ident))
        source.execute(
            sql.SQL("GRANT SELECT ON {} TO {}").format(
                sql.Identifier(source.schema, "items"), ident
            )
        )
        granted = [p for p in ("SELECT", "INSERT", "UPDATE") if p not in lacking]
        source.execute(
            sql.SQL("GRANT {} ON {} TO {}").format(
                sql.SQL(", ").join(map(sql.SQL, granted)),
                sql.Identifier(*config_table(base.heartbeat)),
                ident,
            )
        )
        config = replace(base, dsn=f"{base.dsn} options='-c role={role}'")
        with pytest.raises(PreflightError) as refused:
            check(config)
        grant = f"GRANT {', '.join(lacking)} ON {base.heartbeat} TO {role};"
        assert str(refused.value).endswith(grant), str(refused.value)
        source.execute(str(refused.value).split("to run: ")[1])
        check(config)
    finally:
        source.execute(sql.SQL("DROP OWNED BY {}").format(ident))
        source.execute(sql.SQL("DROP ROLE {}").format(ident))


@pytest.mark.postgres
def test_a_publication_option_refusal_prints_the_alter(stores, pg_phase1):
    from psycopg2 import sql

    source, config = pg_phase1
    source.execute(f"CREATE TABLE {config.heartbeat} (id text PRIMARY KEY, marker text NOT NULL)")
    source.execute(
        sql.SQL("CREATE PUBLICATION {} FOR TABLE {}, {} WITH (publish = 'insert')").format(
            sql.Identifier(config.publication),
            sql.Identifier(source.schema, "items"),
            sql.Identifier(*config_table(config.heartbeat)),
        )
    )
    with pytest.raises(PreflightError) as refused:
        check(config, create_objects=True)
    statement = str(refused.value).split("to run: ")[1]
    assert statement.startswith("ALTER PUBLICATION ") and statement.endswith(";")
    source.execute(statement)
    assert check(config, create_objects=True).created == {"slot": True}


def config_table(name):
    schema, table = name.split(".")
    return schema, table


def test_key_projection_survives_catalog_changes(config, log, tmp_path):
    log.register(("system", 1), 0, [dict(oid=1, schema="public", name="items", keys=["value"])])
    assert log.key_columns() == {1: ("id",)}
    wire = [
        # Rename key in wire: retain raw cells and explicit absence for Phase 2.
        w.replace(b"id\0", b"renamed\0") if w[:1] == b"R" else w
        for w in transaction(1)
    ]
    items = spool_all(config, tmp_path, wire, key_columns=log.key_columns())
    event = next(r for r in items[0].table.to_pylist() if r["record_kind"] == EVENT)
    assert f2.ABSENT in event["new_key"]["kinds"]
    log.append(items)
    assert log.checkpoint() == 101


@pytest.mark.postgres
def test_idle_capture_with_unpublished_writes_advances_slot(stores, pg_phase1):
    import time

    source, base = pg_phase1
    log, info = bootstrap_log(stores[0].con, base, create_objects=True)
    config = log.config
    run_once(config, log, info=info)
    initial_restart = query(
        source.con,
        "SELECT restart_lsn-'0/0'::pg_lsn FROM pg_replication_slots WHERE slot_name=%s",
        [config.slot],
    )[0][0]
    source.execute("CREATE TABLE noise(id int, payload text)")
    source.execute(
        "INSERT INTO noise SELECT i,repeat(md5(i::text),100) FROM generate_series(1,5000) i"
    )
    target = int(query(source.con, "SELECT pg_current_wal_lsn()-'0/0'::pg_lsn")[0][0])
    # Restart horizons are promoted after a subsequent decoding cycle/checkpoint.
    for _ in range(3):
        query(source.con, "CHECKPOINT")
        result = run_once(config, log)
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            confirmed, restart = query(
                source.con,
                "SELECT confirmed_flush_lsn-'0/0'::pg_lsn,restart_lsn-'0/0'::pg_lsn FROM pg_replication_slots WHERE slot_name=%s",
                [config.slot],
            )[0]
            if int(confirmed) >= target and int(restart) > int(initial_restart):
                break
            time.sleep(0.05)
        if int(confirmed) >= target and int(restart) > int(initial_restart):
            break
    assert int(confirmed) >= target and result.boundary >= target
    assert int(restart) > int(initial_restart)
    assert query(source.con, "SELECT count(*) FROM " + source.schema + ".items") == [(0,)]


@pytest.mark.postgres
def test_partition_leaf_identity_is_validated(stores, pg_phase1):
    source, original = pg_phase1
    source.execute(
        "CREATE TABLE partitioned (id int PRIMARY KEY, value text) PARTITION BY RANGE(id)"
    )
    source.execute("CREATE TABLE child PARTITION OF partitioned FOR VALUES FROM(0) TO(100)")
    source.execute("ALTER TABLE partitioned REPLICA IDENTITY FULL")
    config = replace(original, tables=(source.schema + ".partitioned",))
    with pytest.raises(PreflightError, match="leaf partition"):
        check(config, create_objects=True)
    assert query(source.con, "SELECT to_regclass(%s)", [config.heartbeat]) == [(None,)]
    source.execute("ALTER TABLE child REPLICA IDENTITY FULL")
    assert check(config, create_objects=True).tables[0]["name"] == "partitioned"


def test_capped_replay_no_progress_is_visible(config, log, tmp_path):
    from cdc_flight.ingest_backend import Progress

    items = spool_all(config, tmp_path, transaction())
    log.append([items[0]])
    # The open unit at 100 with its first chunk durable, reported twice with no new frames.
    progress = Progress(log.epoch, items[0].unit_start_lsn, 100, items[0].count)
    assert not log.record_progress("capped", progress)
    assert log.record_progress("capped", progress)
    assert log.execute("SELECT state FROM {s}source").fetchone() == (
        "needs_action:large_transaction",
    )
    log.append(items)
    assert not log.record_progress("drained", None)
    assert log.execute("SELECT state FROM {s}source").fetchone() == ("ready",)
    assert not log.unit_position()["partial"]


def test_incremental_checkpoint_rejects_crossing_open_transaction(config, log, tmp_path):
    items = spool_all(config, tmp_path, transaction())
    log.append([items[0]])
    later = spool_all(config, tmp_path, empty_transaction(200), start_lsn=101)
    with pytest.raises(ValueError, match="crosses an open unit"):
        log.append(later)
    assert log.checkpoint() == 0
    log.append(items)
    assert log.checkpoint() == 101


def test_metadata_preserves_unsigned_lsn(config, log, stores, tmp_path):
    lsn = 2**63 + 123
    items = spool_all(config, tmp_path, transaction(1, lsn))
    log.append(items)
    assert log.checkpoint() == lsn + 1
    assert log.execute("SELECT end_lsn,complete FROM {s}units_v2").fetchone() == (lsn + 1, True)
    union = frames(log)
    assert log.execute(f"SELECT DISTINCT commit_lsn FROM ({union})").fetchall() == [(lsn,)]
    # The exact frame schema survives append's upload and reads back unchanged.
    format2_frames_round_trip(log, lsn)
    # The grouped format-2 writer on the same node: a real
    # two-day append, retry, readback and forced missing ledger, plus one EXPLAIN of the
    # production snapshot statement, checked for placement before the fault.
    from test_ingest_log2 import grouped_store_round_trip

    remote = stores[0].remote
    reader_evidence = {}  # the filtered read is checked first, before the snapshot
    evidence = grouped_store_round_trip(
        log.con,
        config.database,
        lsn,
        check_plan=lambda con, sql, params: dict(
            snapshot=snapshot_plan(con, sql, params, remote), reader=reader_evidence
        ),
        check_frame_plan=lambda con, sql, params: reader_evidence.update(
            frames_plan(con, sql, params, remote)
        ),
    )
    assert evidence["snapshot"]["frame_scans"] == 2 and evidence["reader"]["frame_scans"] == 2
    print("format-2 snapshot plan:", evidence)  # shown with -rP in the lane's log
    # The atomic unit-history drop on the same owned database:
    # a fresh schema, a rollback before COMMIT and a new connection after each outcome.
    from test_retention2 import grouped_retention_round_trip

    assert grouped_retention_round_trip(stores[0], lsn)["rollback"] is True


FRAME_BOUNDS = ("source_epoch", "unit_start_lsn", "frame_ordinal")
# Client-side work the snapshot may do on MotherDuck: receive, reshape and combine one-row
# results. Any scan of a stored table, filter, join or aggregate on the client fails (a).
LOCAL_OK = {"DOWNLOAD_SOURCE", "UPLOAD_SINK", "PROJECTION", "CROSS_PRODUCT", "RESULT_COLLECTOR"}
LOCAL_LEAVES = {
    "DUMMY_SCAN",
    "COLUMN_DATA_SCAN",
    "UNNEST",
    "INOUT_FUNCTION",
    "CTE",
    "CTE_SCAN",
    "DELIM_SCAN",
}


# MotherDuck's sinks that send a remote subtree's result to the client: DOWNLOAD_SINK, or
# BATCH_DOWNLOAD_SINK for a multi-row result (the format-2 reader's frame rows).
DOWNLOAD_SINKS = {"DOWNLOAD_SINK", "BATCH_DOWNLOAD_SINK"}


def _placed(kind, parent_remote):
    """A download sink runs on MotherDuck and an UPLOAD_SINK on the client; every other
    operator runs where its parent does."""
    return True if kind in DOWNLOAD_SINKS else False if kind == "UPLOAD_SINK" else parent_remote


def json_nodes(plan):
    """Operators of EXPLAIN (FORMAT json) as dicts (kind, table, filters, remote, label)."""
    import json

    nodes = []

    def walk(node, remote):
        info = node.get("extra_info") or {}
        m = re.fullmatch(r"(.+?) ?\(([LR])\)", node["name"].strip())
        kind = info.get("md_type") or (m[1] if m else node["name"].strip())
        remote = _placed(kind, remote)
        table = str(info.get("Table", "")).replace('"', "").rsplit(".", 1)[-1]
        filters = " ".join(map(str, info.get("Filters") or []))
        nodes.append(dict(kind=kind, table=table, filters=filters, remote=remote, label=m and m[2]))
        for child in node.get("children", ()):
            walk(child, remote)

    for root in json.loads(plan):
        walk(root, False)
    return nodes


def text_nodes(text):
    """Operators of a rendered EXPLAIN, in the same shape as json_nodes.

    Side-by-side boxes are split by their own top-border columns, and wrapped titles and
    table names are joined. The renderer lays each subtree out from its root's column to the
    next sibling's, so a box's parent is the box one box-row up with the largest column at
    or left of its own; placement is then structural, as for JSON. The rendered (L)/(R)
    label is kept to cross-check it; MotherDuck truncates long titles ('UNGROUPED_AGGREG...'),
    dropping the label, so the label alone can't place every operator."""
    boxes, open_ = [], {}
    for n, line in enumerate(text.splitlines()):
        for start in [i for i, ch in enumerate(line) if ch == "┌"]:
            open_[start] = dict(row=n, col=start, end=line.index("┐", start), lines=[])
        for start, box in list(open_.items()):
            edge, cell = line[start : start + 1], line[start + 1 : box["end"]]
            if edge == "└":
                boxes.append(box)
                del open_[start]
            elif edge in ("│", "├") and n > box["row"]:
                rule = cell.strip() and not cell.replace("─", "").strip()
                box["lines"].append(None if rule else cell.strip())
    rows = {r: i for i, r in enumerate(sorted({b["row"] for b in boxes}))}
    for b in boxes:
        lines = b["lines"]
        cut = lines.index(None) if None in lines else len(lines)
        title = " ".join(c for c in lines[:cut] if c)
        body = " ".join(c for c in lines[cut + 1 :] if c)
        m = re.fullmatch(r"(.+?) ?\(([LR])\)", title)
        b["kind"], b["label"] = (m[1], m[2]) if m else (title, None)
        if "md_type: " in body:
            b["kind"] = body.split("md_type: ", 1)[1].split()[0]
        table = re.search(r"Table: (.*?)(?: Type:| Projections:| Filters:|$)", body)
        b["table"] = table[1].replace(" ", "").replace('"', "").rsplit(".", 1)[-1] if table else ""
        b["filters"] = body.split("Filters:", 1)[1] if "Filters:" in body else ""
        above = [p for p in boxes if rows[p["row"]] == rows[b["row"]] - 1 and p["col"] <= b["col"]]
        b["parent"] = max(above, key=lambda p: p["col"]) if above else None
    nodes = []
    for b in sorted(boxes, key=lambda b: (b["row"], b["col"])):
        b["remote"] = _placed(b["kind"], b["parent"]["remote"] if b["parent"] else False)
        nodes.append({k: b[k] for k in ("kind", "table", "filters", "remote", "label")})
    return nodes


def check_placement(nodes, ledgers=2):
    """(a) and (c) on one plan's operators; returns the evidence."""
    assert not [n for n in nodes if n["label"] and (n["label"] == "R") != n["remote"]], nodes
    downloads = sum(n["kind"] in DOWNLOAD_SINKS for n in nodes)
    assert downloads >= 1, "no DOWNLOAD_SINK: the statement didn't run remotely"
    assert downloads == sum(n["kind"] == "DOWNLOAD_SOURCE" for n in nodes), nodes
    scans = [n for n in nodes if n["table"]]
    frames = [n for n in scans if n["table"].startswith("frames_v2_")]
    ledger_scans = [n for n in scans if n["table"] in ("chunks_v2", "units_v2")]
    assert len(frames) == 2 and len(ledger_scans) == ledgers, scans  # (c): one scan per day
    assert all(n["remote"] for n in scans), [n for n in scans if not n["remote"]]
    local = sorted({n["kind"] for n in nodes if not n["remote"]})
    assert set(local) <= LOCAL_OK | LOCAL_LEAVES, local
    uploads = sum(n["kind"] == "UPLOAD_SINK" for n in nodes)
    return dict(downloads=downloads, uploads=uploads, local=local)


def frame_filters(nodes, bounds=FRAME_BOUNDS):
    """(b): per frames_v2 scan, whether all the bounds are in its pushed filters."""
    frames = [n for n in nodes if n["table"].startswith("frames_v2_")]
    return {n["table"]: all(b in n["filters"].replace(" ", "") for b in bounds) for n in frames}


def remove(con, undo):
    """Delete the rows out_of_range added."""
    for table, decoys in undo:
        for d in decoys:
            con.execute(
                f"DELETE FROM {table} WHERE source_epoch=? AND unit_start_lsn=? AND frame_ordinal=?",
                list(d),
            )


def out_of_range(con, sql):
    """Test-only rows outside the requested range in each frames_v2 day table this
    statement reads (another epoch, an earlier unit, a later ordinal), so the planner
    can't prove the bounds from table statistics on a 3+1-row fixture. Returns the
    statements that remove them again."""
    undo = []
    for table in sorted(set(re.findall(r"(\S+\.frames_v2_\d{8})\b", sql))):
        e, u, f = con.execute(
            f"SELECT source_epoch,unit_start_lsn,frame_ordinal FROM {table} ORDER BY ALL LIMIT 1"
        ).fetchone()
        decoys = [(e + 1, u, f), (e, u - 1000, f), (e, u, f + 1_000_000)]
        for d in decoys:
            con.execute(
                f"INSERT INTO {table} SELECT * REPLACE (?::UINTEGER AS source_epoch,"
                "?::UBIGINT AS unit_start_lsn,?::UBIGINT AS frame_ordinal) FROM "
                f"{table} WHERE source_epoch=? AND unit_start_lsn=? AND frame_ordinal=?",
                [*d, e, u, f],
            )
        undo.append((table, decoys))
    return undo


def explained(con, sql, params, remote, name):
    """Both renderings of one statement's plan, printed as evidence, and the operators
    to gate on: JSON when it carries the DOWNLOAD operators, otherwise the text."""
    plan = "\n".join(
        str(r[-1]) for r in con.execute("EXPLAIN (FORMAT json) " + sql, params).fetchall()
    )
    text = "\n".join(str(r[-1]) for r in con.execute("EXPLAIN " + sql, params).fetchall())
    print(f"{name} plan JSON:", plan)  # evidence for a surprise, without another run
    print(f"{name} plan text:", text)
    nodes, gated_on = json_nodes(plan), "json"
    if remote and not any(n["kind"] in DOWNLOAD_SINKS for n in nodes):
        nodes, gated_on = text_nodes(text), "text"
    return nodes, gated_on


def frames_plan(con, sql, params, remote):
    """The production reader statement for one relation's frames (filter before
    transfer), captured by the helper. The same placement check as the snapshot's: on
    MotherDuck the physical CTE, the relation filter and the per-unit aggregates run
    remotely and only the result crosses. Its plan scans one frames table per day and no
    ledger. Its epoch/unit bounds are recorded, read with the out-of-range
    rows in for the EXPLAIN only (never executed), and a miss on MotherDuck prints SURPRISE."""
    undo = out_of_range(con, sql)
    try:
        nodes, gated_on = explained(con, sql, params, remote, "reader frames")
    finally:
        remove(con, undo)
    filters = frame_filters(nodes, FRAME_BOUNDS[:2])  # evidence only
    assert len(filters) == 2, nodes  # one scan per day
    assert not [n for n in nodes if n["table"] in ("chunks_v2", "units_v2")], nodes  # no ledger
    evidence = dict(operators=len(nodes), gated_on=gated_on, frame_scans=2, filters=filters)
    if remote:
        evidence.update(check_placement(nodes, ledgers=0))
        if not all(filters.values()):
            print("SURPRISE: a remote reader frames_v2 scan shows no pushed bounds:", filters)
    return evidence


def snapshot_plan(con, sql, params, remote):
    """The production snapshot statement's plan class, structural only, never full plan equality:
    (a) on MotherDuck, every stored-table scan, join and aggregate runs remotely; the client
        only receives, projects and combines one-row results;
    (b) each frames_v2 scan carries the epoch/unit/ordinal bounds. It gates locally; on
        MotherDuck it's recorded, and a miss prints SURPRISE for the reviewers;
    (c) one statement, with one frames scan per day, never per chunk.
    Out-of-range rows go in first (and come out after), so (b) is read from the
    production plan itself; the result on the requested range must not change.
    On MotherDuck, (a) and (c) are read from the JSON plan when it carries the DOWNLOAD
    operators, and otherwise from the text rendering; the format that gated is recorded."""
    before = con.execute(sql, params).fetchone()
    undo = out_of_range(con, sql)
    try:
        # The whole row is unchanged, except column 2, the clock's UTC day.
        after = con.execute(sql, params).fetchone()
        assert after[:2] + after[3:] == before[:2] + before[3:]
        nodes, gated_on = explained(con, sql, params, remote, "snapshot")
    finally:
        remove(con, undo)
    evidence = dict(operators=len(nodes), gated_on=gated_on)
    filters = frame_filters(nodes)
    evidence.update(frame_scans=len(filters), filters=filters)
    if remote:
        evidence.update(check_placement(nodes))
        if not all(filters.values()):
            print("SURPRISE: a remote frames_v2 scan shows no pushed bounds:", filters)
    else:
        assert len(filters) == 2 and all(filters.values()), filters
    return evidence


class FakePlans:
    """A connection whose EXPLAIN returns given plans, for the MotherDuck-only branches of
    snapshot_plan. The statement reads no frames table, so no rows are written."""

    def __init__(self, plan, text=""):
        self.plan, self.text = plan, text

    def execute(self, sql, params=None):
        import json

        if sql.startswith("EXPLAIN (FORMAT json)"):
            rows = [("physical_plan", json.dumps(self.plan))]
        elif sql.startswith("EXPLAIN"):
            rows = [("physical_plan", self.text)]
        else:
            rows = [(None,) * 5 + ([],)]
        return type("Rows", (), {"fetchall": lambda _: rows, "fetchone": lambda _: rows[0]})()


def _op(name, *children, **info):
    return dict(name=name, extra_info=info, children=list(children))


def _scans(filters):
    days = [_op("SEQ_SCAN", Table=f"db.f2.frames_v2_2026110{d}", Filters=filters) for d in (5, 6)]
    ledgers = [
        _op("HASH_GROUP_BY", _op("SEQ_SCAN", Table=f"db.f2.{t}")) for t in ("chunks_v2", "units_v2")
    ]
    return _op("HASH_GROUP_BY", _op("UNION", *days)), _op("CROSS_PRODUCT", *ledgers)


BOUNDS = ["source_epoch=1", "unit_start_lsn>=1 AND unit_start_lsn<=2", "frame_ordinal<3"]


def _download(child, sink="DOWNLOAD_SINK"):
    return _op("DOWNLOAD_SOURCE (L)", _op(f"{sink} (R)", child), md_type="DOWNLOAD_SOURCE")


def test_snapshot_plan_class_on_motherduck_shapes(capsys):
    frames, ledgers = _scans(BOUNDS)
    good = [_download(_op("CROSS_PRODUCT", frames, ledgers))]
    evidence = snapshot_plan(FakePlans(good), "SELECT 1", [], True)
    assert evidence["local"] == ["DOWNLOAD_SOURCE"] and evidence["gated_on"] == "json"
    assert evidence["filters"] == {"frames_v2_20261105": True, "frames_v2_20261106": True}
    # The frames' rows cross and are counted on the client: (a) fails.
    counted_locally = [_op("HASH_GROUP_BY", _download(frames["children"][0]), _download(ledgers))]
    with pytest.raises(AssertionError):
        snapshot_plan(FakePlans(counted_locally), "SELECT 1", [], True)
    # A multi-row result's batched download sink is the same remote boundary.
    batched = [_download(_op("CROSS_PRODUCT", frames, ledgers), sink="BATCH_DOWNLOAD_SINK")]
    assert snapshot_plan(FakePlans(batched), "SELECT 1", [], True)["local"] == ["DOWNLOAD_SOURCE"]
    # A label that disagrees with the structure fails rather than being trusted.
    for sink in DOWNLOAD_SINKS:
        mislabelled = [_download(_op("CROSS_PRODUCT (L)", frames, ledgers), sink=sink)]
        with pytest.raises(AssertionError):
            snapshot_plan(FakePlans(mislabelled), "SELECT 1", [], True)
    # No DOWNLOAD in JSON or text: falls back to text, then fails (a).
    with pytest.raises(AssertionError, match="no DOWNLOAD_SINK"):
        snapshot_plan(FakePlans([_op("CROSS_PRODUCT", frames, ledgers)]), "SELECT 1", [], True)
    # Remote scans without bounds: reported as a SURPRISE, not a failure, on MotherDuck...
    bare = [_download(_op("CROSS_PRODUCT", *_scans([])))]
    capsys.readouterr()
    assert not any(snapshot_plan(FakePlans(bare), "SELECT 1", [], True)["filters"].values())
    assert "SURPRISE" in capsys.readouterr().out
    # ...and a failure locally, where (b) gates.
    with pytest.raises(AssertionError):
        snapshot_plan(FakePlans([_op("CROSS_PRODUCT", *_scans([]))]), "SELECT 1", [], False)


def format2_chunk(lsn):
    """One real format-2 chunk in one transaction at an unsigned LSN: every tuple marker
    (none, K, O), every cell kind (empty text, non-ASCII, NULL, unchanged, and an
    absent registered key column), a two-OID TRUNCATE, an insert into a zero-column
    relation (an empty-list image next to an outer NULL one) and the inline COMMIT row."""
    import pgstream
    from test_format2_protocol import binding
    from test_format2_protocol import config as f2_config

    from cdc_flight import format2 as f2
    from cdc_flight.spool2 import Spool as Spool2

    columns = (
        (1, "id", 25, -1),
        (0, "escape", 25, -1),
        (0, "nullable", 25, -1),
        (0, "toast", 25, -1),
    )
    new = ("", 'é\n"\\', None, pgstream.UNCHANGED)
    key_old = ("key", None, None, None)
    full_old = ("old", "full", None, pgstream.UNCHANGED)
    spool = Spool2(
        f2_config(),
        {101: ("id", "not_here"), 102: ("id",), 103: ()},
        900,
        "m",
        binding=binding(),
        start_lsn=lsn - 10,
        unit_config=f2.UnitConfig(max_commits=1),
        clock=lambda: 0,
    )
    messages = (
        pgstream.relation(101, "public", "items", "f", columns),
        pgstream.relation(102, "public", "other", "d", ((1, "id", 25, -1),)),
        pgstream.relation(103, "public", "empty", "d", ()),
        pgstream.begin(lsn, -5, 7),
        pgstream.insert(101, new),
        pgstream.update(101, new),
        pgstream.update(101, new, key_old, "K"),
        pgstream.update(101, new, full_old, "O"),
        pgstream.delete(101, key_old, "K"),
        pgstream.delete(101, full_old, "O"),
        pgstream.truncate((101, 102), options=3),
        pgstream.insert(103, ()),
        pgstream.commit(lsn, lsn + 1, -4),
    )
    out = [item for wire in messages for item in spool.feed(wire)]
    (chunk,) = [item for item in out if isinstance(item, f2.Chunk)]
    return f2, chunk


def format2_frames_round_trip(log, lsn):
    import duckdb

    from cdc_flight.spool2 import _small_frame_table

    f2, chunk = format2_chunk(lsn)
    # The same real rows through the small-unit builder (B′'s path for one-row units):
    # it must equal the column builder's table exactly, schema and metadata included,
    # and it's the table uploaded, so MotherDuck proves the builder production uses.
    rows = chunk.table.to_pylist()
    small = _small_frame_table(rows)
    assert small.schema.equals(chunk.table.schema, check_metadata=True)
    assert small.equals(chunk.table) and small.to_pylist() == rows
    con = log.con
    con.register("f2_empty", f2.FRAME_SCHEMA.empty_table())
    try:
        log.execute("CREATE TABLE {s}f2_frames AS SELECT * FROM f2_empty")
    finally:
        con.unregister("f2_empty")
    con.execute("BEGIN")
    con.register("f2_upload", small)
    try:
        log.execute("INSERT INTO {s}f2_frames BY NAME SELECT * FROM f2_upload")
    finally:
        con.unregister("f2_upload")
    con.execute("COMMIT")
    # The stored types are exactly what DuckDB derives from FRAME_SCHEMA locally.
    local = duckdb.connect()
    local.register("f2_empty", f2.FRAME_SCHEMA.empty_table())
    expected = local.execute("DESCRIBE SELECT * FROM f2_empty").fetchall()
    local.close()
    stored = log.execute("DESCRIBE {s}f2_frames").fetchall()
    assert [(c[0], c[1]) for c in stored] == [(c[0], c[1]) for c in expected]
    types = dict((c[0], c[1]) for c in stored)
    assert types["old"] == types["new"] == types["old_key"] == types["new_key"]
    assert types["old"] == 'STRUCT("values" VARCHAR[], kinds UTINYINT[])'
    assert types["truncate_oids"] == "UINTEGER[]" and types["commit_lsn"] == "UBIGINT"
    # Every value reads back unchanged: unsigned LSNs, outer NULL images next to images
    # with NULL values inside, the tags, the TRUNCATE OIDs and the COMMIT row.
    back = log.execute("SELECT * FROM {s}f2_frames ORDER BY frame_ordinal").to_arrow_table()
    back_rows = back.to_pylist()
    assert back_rows == small.sort_by("frame_ordinal").to_pylist()
    assert back.num_rows == chunk.count == 9
    assert {r["commit_lsn"] for r in back_rows} == {lsn}
    assert sum(r["record_kind"] == f2.COMMIT for r in back_rows) == 1
    # The zero-column insert: an outer NULL old image next to an empty-list new image
    # (and the same for its key projections) survive as distinct values.
    (empty,) = [r for r in back_rows if r["relation_id"] == 103]
    assert empty["old"] is None and empty["old_key"] is None
    assert empty["new"] == {"values": [], "kinds": []} == empty["new_key"]


def test_canonical_format_v1_golden(monkeypatch):
    """The frozen format-1 encoding (tests/reference; no longer documented), an oracle on the frozen reference
    copies (tests/reference): no format-1 store exists any more. The reference spool is
    rebound to the reference decoder for this test. spool_ref still imports src's
    pgoutput at module level, so src pgoutput must exist (it does: Spool2 decodes with
    it)."""
    from reference import pgoutput_ref, spool_ref

    monkeypatch.setattr(spool_ref, "decode", pgoutput_ref.decode)
    Chunk1, Spool1 = spool_ref.Chunk, spool_ref.Spool
    spool = Spool1(
        IngestConfig("phase1", "s", "p", ("public.items",), chunk_bytes=1024, epoch=1),
        {1: ("id",)},
        2,
        "marker",
    )
    items = [item for message in transaction(2) for item in spool.feed(message)]
    chunks = [x for x in items if isinstance(x, Chunk1)]
    assert [(x.id, x.digest, x.chain_digest) for x in chunks] == [
        (
            "1:100:0:1",
            "ee09cbd64cd4614ea858e3b03771d8ea856b3bf02ba56170d70dc83cd0e35a05",
            "ee09cbd64cd4614ea858e3b03771d8ea856b3bf02ba56170d70dc83cd0e35a05",
        ),
        (
            "1:100:1:1",
            "73b83485869311f29ce04c61d0d6486bd3a4458b5f4df1d48b9b7cb38208852f",
            "dac3308f5a79a3352778b3aef68a2b316412e08d2b8ec62cab2f91bbad9549ed",
        ),
    ]


@pytest.mark.postgres
def test_lingering_source_reader_is_busy(stores, pg_phase1):
    from cdc_flight.preflight import connect

    source, base = pg_phase1
    log, info = bootstrap_log(stores[0].con, base, create_objects=True)
    config = log.config
    repl = connect(config, replication=True)
    try:
        repl.cursor().start_replication(
            slot_name=config.slot,
            options={"proto_version": "1", "publication_names": config.publication},
        )
        result = run_once(config, log, info=info)
        assert result.status == "busy"
        assert log.execute("SELECT holder FROM {s}source").fetchone() == (None,)
        assert log.execute("SELECT status FROM {s}runs").fetchone() == ("busy",)
    finally:
        repl.close()


def test_acquire_write_conflict_returns_busy(config, log, stores):
    import duckdb

    from cdc_flight.preflight import SourceInfo

    log.release()
    other = open_log(stores[1].con, config)
    other.execute("BEGIN")
    try:
        other.execute("UPDATE {s}source SET guard=uuid()::VARCHAR WHERE id=1")
        # Verify that this setup exercises a conflict, not merely an unexpired lease.
        with pytest.raises(duckdb.TransactionException, match="[Cc]onflict"):
            log.acquire("direct")
        result = run_once(config, log, info=SourceInfo(("system", 1), 0, [], 2, []))
        assert result.status == "busy" and result.boundary == 0
        assert log.token is None
    finally:
        other.execute("ROLLBACK")
    assert log.execute("SELECT holder FROM {s}source").fetchone() == (None,)
