"""Keyless phase 2: an append table's exact snapshot backfill (append_backfill).

The exact cut: the snapshot holds the commits before C, catch-up
appends this relation's frames with commit_lsn >= C. A commit in [L, C) is in both the
snapshot and the log, and must appear once; a TRUNCATE there must not empty staging."""

import dataclasses
import os
import threading
import time

import psycopg2
import pytest
from psycopg2 import sql
from test_keyless_add import declare_all
from test_schema_cases import schema_source  # noqa: F401 (fixture)
from test_table_hold import add_to_publication, ingest_once

from cdc_flight import append_backfill as ab
from cdc_flight.append_apply import kept_aside
from cdc_flight.backfill import Backfill
from cdc_flight.config import BackfillConfig
from cdc_flight.membership import add_table
from cdc_flight.preflight import query
from cdc_flight.releases import APPEND_SNAPSHOT, KEYLESS_GUARD, STAMP

PG = pytest.mark.postgres


# Without PostgreSQL ---------------------------------------------------------------------


def test_lsn_text_and_stamps():
    assert ab.lsn_int("0/0") == 0
    assert ab.lsn_int("1/9C4FF18") == (1 << 32) + 0x9C4FF18
    # Only the backfill release stamps 2; the add rule (>= 1) still holds for it.
    assert STAMP == {"backfill": APPEND_SNAPSHOT} and APPEND_SNAPSHOT > KEYLESS_GUARD


def test_config_limits():
    base = dict(database="d", dsn="x", allow_local=True)
    config = BackfillConfig("src", (), **base)
    assert config.publication == "md_cdc_src_pub"
    assert config.append_snapshot_max_bytes == 1024**3  # provisional default
    assert ab.SLOT_WAIT_SECONDS < BackfillConfig("src", (), **base).lease_seconds
    with pytest.raises(ValueError, match="append snapshot limit"):
        BackfillConfig("src", (), append_snapshot_max_bytes=-1, **base)


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_kept_aside_matches_only_kept_names(stores):
    """Q3b's check matches `__gen_` + 8 hex (kept aside), never `__gen_` + 12 (staging):
    a generation must not refuse itself."""
    store, _ = stores

    class Owner:
        con = store.con
        config = type("C", (), {"mirror_database": store.mirror_database.strip('"')})

    m = store.mirror_database
    store.con.execute(f"CREATE SCHEMA IF NOT EXISTS {m}.s")
    for name in ("t", "t__gen_0123456789ab", "t__gen_abcdef12", "tt__gen_abcdef12", "t__gen_xyz"):
        store.con.execute(f'CREATE TABLE {m}.s."{name}" (v int)')
    assert kept_aside(Owner, "s", "t") == ["t__gen_abcdef12"]


# With PostgreSQL ------------------------------------------------------------------------


def _setup(schema_source, name="t_app", *, rows=("a", "a", None, "b"), ignore_deletes=False):  # noqa: F811
    source, ingest, log, backfill, apply = schema_source
    table = f"{source.schema}.{name}"
    source.execute(sql.SQL("CREATE TABLE {} (msg text, n int)").format(sql.Identifier(name)))
    source.execute(sql.SQL("ALTER TABLE {} REPLICA IDENTITY FULL").format(sql.Identifier(name)))
    for i, value in enumerate(rows):
        source.execute(
            sql.SQL("INSERT INTO {} VALUES (%s,%s)").format(sql.Identifier(name)), [value, i]
        )
    add_to_publication(source, ingest, name)
    ingest_once(ingest, log)
    declare_all(log)
    log.forget()
    added = add_table(
        log,
        apply,
        source.con,
        ingest.publication,
        table,
        mode="append",
        ignore_deletes=ignore_deletes,
    )
    assert added.status in ("added", "adopted"), added
    config = dataclasses.replace(backfill.config, publication=ingest.publication)
    snap = Backfill(backfill.con, config)
    snap.initialize()
    return source, ingest, log, snap, apply, table


def _source_rows(source, name):
    return sorted(
        query(source.con, sql.SQL("SELECT msg,n FROM {}").format(sql.Identifier(name))),
        key=repr,
    )


def _mirror_rows(apply, table, *, extra=""):
    schema, name = table.split(".")
    return sorted(
        apply.con.execute(
            f'SELECT msg,n{extra} FROM {apply.mirror}."{schema}"."{name}"'
        ).fetchall(),
        key=repr,
    )


def _drive(snap, table, ingest, log, *, fault=None, limit=50):
    for _ in range(limit):
        result = snap.run_table(table, fault=fault)
        if result.status in ("failed", "published", "refused"):
            return result
        assert run_once_ok(ingest, log)
    raise AssertionError("append snapshot did not finish")


def run_once_ok(ingest, log):
    ingest_once(ingest, log)
    return True


def _other(source):
    """A second session on the test schema (its own transactions)."""
    return psycopg2.connect(
        os.environ["CDC_TEST_PG_DSN"], options=f"-c search_path={source.schema}"
    )


def _slots(source, snap, timeout=30.0):
    """This test's own snapshot slots left (its generations' names): other runs on the
    same cluster may hold theirs.

    Waits up to `timeout` for them to go. A temporary slot is dropped when its walsender
    exits, which happens after the client has closed the replication connection: on a busy
    server it can still be listed just after the run."""
    names = [
        ab.SLOT_PREFIX + g[:12]
        for (g,) in snap.execute("SELECT generation_id FROM {m}generations").fetchall()
    ]
    deadline = time.monotonic() + timeout
    while True:
        left = query(
            source.con,
            "SELECT slot_name FROM pg_replication_slots WHERE slot_name=ANY(%s)",
            [names],
        )
        if not left or time.monotonic() > deadline:
            return left
        time.sleep(0.1)


@PG
def test_snapshot_end_to_end_with_racing_commits(schema_source):  # noqa: F811
    """The boundary: rows committed in [L, C) (inserted after the pin, before the slot),
    a TRUNCATE there, and rows committed after C (after the slot) each land exactly once;
    duplicates stay duplicates; snapshot rows have no commit lsn and their row number."""
    source, ingest, log, snap, apply, table = _setup(schema_source)
    name = table.split(".")[1]
    other = _other(source)
    other.autocommit = True

    def ins(*values):
        with other.cursor() as cur:
            for v in values:
                cur.execute(
                    sql.SQL("INSERT INTO {} VALUES (%s, 100)").format(sql.Identifier(name)), [v]
                )

    def fault(stage):
        if stage == "after_pin":  # commits in [L, C): in the snapshot and in the log
            with other.cursor() as cur:
                cur.execute(sql.SQL("TRUNCATE {}").format(sql.Identifier(name)))
            ins("L1", "L1", "L2")
        elif stage == "after_slot":  # commits >= C: only in the log
            ins("C1", "C1")
        elif stage == "after_import":
            ins("C2")

    try:
        result = _drive(snap, table, ingest, log, fault=fault)
        assert result.status == "published", result
        assert not _slots(source, snap)
        ins("after publish")
        ingest_once(ingest, log)
        assert apply.apply_table(table, "a1").status == "caught_up"
        assert _mirror_rows(apply, table) == _source_rows(source, name)
        assert sorted(r[0] for r in _mirror_rows(apply, table)) == sorted(
            ["L1", "L1", "L2", "C1", "C1", "C2", "after publish"]
        )
        # D6: snapshot rows carry NULL commit lsn and their row number.
        snap_rows = apply.con.execute(
            f'SELECT _cdc_event_ordinal FROM {apply.mirror}."{source.schema}"."{name}" '
            "WHERE _cdc_commit_lsn IS NULL ORDER BY 1"
        ).fetchall()
        assert snap_rows == [(1,), (2,), (3,)]
        state = apply.state(snap.registered(table, append_ok=True)[0])
        assert state["capture_mode"] == "append" and state["generation"] == result.generation
        gen = snap.generation(result.generation)
        assert gen["mode"] == ab.MODE and gen["pin_lsn"] <= gen["snapshot_lsn"] <= gen["marker_lsn"]
    finally:
        other.close()


@PG
def test_open_transaction_across_slot_creation(schema_source):  # noqa: F811
    """A transaction that wrote before the slot and commits while snapbuild waits for it
    appears exactly once."""
    source, ingest, log, snap, apply, table = _setup(schema_source, rows=("x",))
    name = table.split(".")[1]
    holder = _other(source)
    with holder.cursor() as cur:
        cur.execute(sql.SQL("INSERT INTO {} VALUES ('open', 1)").format(sql.Identifier(name)))
    timer = threading.Timer(2.0, holder.commit)
    try:
        timer.start()
        result = _drive(snap, table, ingest, log)
        assert result.status == "published", result
        ingest_once(ingest, log)
        assert apply.apply_table(table, "a1").status == "caught_up"
        assert _mirror_rows(apply, table) == _source_rows(source, name)
        assert [r[0] for r in _mirror_rows(apply, table)].count("open") == 1
    finally:
        timer.cancel()
        holder.close()


@PG
def test_slot_wait_fails_cleanly(schema_source):  # noqa: F811
    """A long-running transaction keeps snapbuild waiting: the slot wait fails the
    generation, and no slot or staging table is left."""
    source, ingest, log, snap, apply, table = _setup(schema_source)
    name = table.split(".")[1]
    snap.slot_wait = 2
    holder = _other(source)
    try:
        with holder.cursor() as cur:
            cur.execute(sql.SQL("INSERT INTO {} VALUES ('open', 1)").format(sql.Identifier(name)))
        result = snap.run_table(table)
        assert result.status == "failed" and "long-running" in result.detail, result
        assert not _slots(source, snap)
        gen = snap.generation(result.generation)
        assert gen["state"] == "failed"
        staging = f"{name}__gen_{result.generation[:12]}"
        assert not apply.con.execute(
            "SELECT count(*) FROM duckdb_tables() WHERE table_name=?", [staging]
        ).fetchone()[0]
    finally:
        holder.rollback()
        holder.close()


@PG
def test_interrupted_copy_fails_and_restarts(schema_source):  # noqa: F811
    """A run that dies mid-copy leaves no slot; the next run fails that generation (its
    snapshot is gone), and a new one publishes."""
    source, ingest, log, snap, apply, table = _setup(schema_source)
    snap.config = dataclasses.replace(snap.config, chunk_rows=1)

    class Crash(Exception):
        pass

    def fault(stage):
        if stage == "after_chunk_commit":
            raise Crash

    with pytest.raises(Crash):
        snap.run_table(table, fault=fault)
    assert not _slots(source, snap)
    # The crashed run's lease is still held: expire it, as a later run would find it.
    snap.execute("UPDATE {m}generations SET expires_at=current_timestamp-INTERVAL 1 SECOND")
    failed = snap.run_table(table, start=False)
    assert failed.status == "failed" and "interrupted" in failed.detail, failed
    result = _drive(snap, table, ingest, log)
    assert result.status == "published", result
    ingest_once(ingest, log)
    assert apply.apply_table(table, "a1").status == "caught_up"
    assert _mirror_rows(apply, table) == _source_rows(source, table.split(".")[1])


@PG
def test_refusals_write_nothing(schema_source):  # noqa: F811
    source, ingest, log, snap, apply, table = _setup(schema_source)
    limited = dataclasses.replace(snap.config, append_snapshot_max_bytes=1)
    refused = Backfill(snap.con, limited).run_table(table)
    assert refused.status == "refused" and "over the append snapshot limit" in refused.detail
    assert "start-from-log" in refused.detail
    wrong = Backfill(snap.con, dataclasses.replace(snap.config, publication="nope"))
    assert "not in the publication" in wrong.run_table(table).detail
    assert snap.execute("SELECT count(*) FROM {m}generations").fetchone()[0] == 0
    assert not _slots(source, snap)


@PG
def test_resnapshot_keeps_the_old_mirror_and_counts_once(schema_source):  # noqa: F811
    """ignore_deletes: a live mirror from start-from-log, then a re-snapshot with a DELETE
    after C. The DELETE counts once, on the new generation, never on the live row;
    the replaced mirror is kept aside, and a second re-snapshot is refused until it
    is dropped (Q3b)."""
    from cdc_flight import table_actions as ta

    source, ingest, log, snap, apply, table = _setup(schema_source, ignore_deletes=True)
    name = table.split(".")[1]
    assert ta.start_from_log(apply, table, run_id="s", actor="t").status == "done"
    source.execute(sql.SQL("INSERT INTO {} VALUES ('live', 9)").format(sql.Identifier(name)))
    ingest_once(ingest, log)
    assert apply.apply_table(table, "a1").status == "caught_up"
    relation_id = snap.registered(table, append_ok=True)[0]
    old = apply.state(relation_id)["generation"]

    def fault(stage):
        if stage == "after_slot":
            source.execute(sql.SQL("DELETE FROM {} WHERE msg='b'").format(sql.Identifier(name)))

    result = _drive(snap, table, ingest, log, fault=fault)
    assert result.status == "published", result
    live = snap.execute(
        "SELECT deletes_ignored FROM {m}apply_state WHERE relation_id=?", [relation_id]
    ).fetchone()[0]
    assert live == 1  # carried from the generation, counted once
    assert snap.generation(result.generation)["deletes_ignored"] == 1
    ingest_once(ingest, log)
    assert apply.apply_table(table, "a2").status == "caught_up"
    # The snapshot had 'b' (before C); the DELETE after C is skipped: it stays.
    assert ("b", 3) in _mirror_rows(apply, table)
    kept = kept_aside(snap, source.schema, name)
    assert kept == [f"{name}__gen_{old[:8]}"]
    again = snap.run_table(table)
    assert again.status == "refused" and "DROP TABLE" in again.detail
    apply.con.execute(f'DROP TABLE {apply.mirror}."{source.schema}"."{kept[0]}"')
    assert _drive(snap, table, ingest, log).status == "published"


@PG
def test_update_after_c_fails_the_generation(schema_source):  # noqa: F811
    source, ingest, log, snap, apply, table = _setup(schema_source)
    name = table.split(".")[1]

    def fault(stage):
        if stage == "after_slot":
            source.execute(sql.SQL("UPDATE {} SET n=n+1").format(sql.Identifier(name)))

    result = _drive(snap, table, ingest, log, fault=fault)
    assert result.status == "failed" and "append_update" in result.detail, result
    assert not snap.execute("SELECT count(*) FROM {m}backfills").fetchone()[0]


@PG
def test_start_from_log_refuses_while_a_snapshot_runs(schema_source):  # noqa: F811
    from cdc_flight.append_apply import start_append
    from cdc_flight.apply import ApplyError

    source, ingest, log, snap, apply, table = _setup(schema_source)
    seen = {}

    def fault(stage):
        if stage == "after_import":
            with pytest.raises(ApplyError, match="append backfill is running"):
                start_append(apply, table, accept_incomplete=True)
            seen["checked"] = True

    assert _drive(snap, table, ingest, log, fault=fault).status == "published"
    assert seen


CHILD = """
import sys, time
from cdc_flight.append_backfill import SLOT_PREFIX, _create_slot
from cdc_flight.config import BackfillConfig
from cdc_flight.pg_connect import connect, query
config = BackfillConfig("kill", (), database="d", dsn=sys.argv[1], allow_local=True)
repl = connect(config, replication=True, keepalive=True)
with repl.cursor() as cur:
    cur.execute("SHOW tcp_keepalives_idle")
    walsender = cur.fetchone()[0]
lsn, snapshot = _create_slot(repl, SLOT_PREFIX + "killtest", 30)
imp = connect(config, keepalive=True)
imported = query(imp, "SHOW tcp_keepalives_idle")[0][0]
imp.set_session(isolation_level="REPEATABLE READ", readonly=True, autocommit=False)
imp.cursor().execute("SET TRANSACTION SNAPSHOT %s", [snapshot])
print(walsender, imported, imp.info.backend_pid, flush=True)
time.sleep(600)
"""


@PG
def test_a_killed_copy_leaves_no_slot_and_no_xmin(tmp_path, pg_dsn):
    """Both connections carry the server-side keepalives, and a SIGKILLed
    Flight process releases its slot and its snapshot's xmin promptly."""
    import signal
    import subprocess
    import sys
    import time

    dsn = pg_dsn
    child = subprocess.Popen([sys.executable, "-c", CHILD, dsn], stdout=subprocess.PIPE, text=True)
    check = psycopg2.connect(dsn)
    check.autocommit = True
    try:
        walsender, imported, pid = child.stdout.readline().split()
        assert (walsender, imported) == ("30", "30")
        slot = "SELECT count(*) FROM pg_replication_slots WHERE slot_name='cdc_snap_killtest'"
        xmin = "SELECT backend_xmin IS NOT NULL FROM pg_stat_activity WHERE pid=%s"
        assert query(check, slot)[0][0] == 1
        assert query(check, xmin, [int(pid)]) == [(True,)]
        child.send_signal(signal.SIGKILL)
        child.wait(10)
        for _ in range(50):
            if not query(check, slot)[0][0] and not query(check, xmin, [int(pid)]):
                break
            time.sleep(0.2)
        assert query(check, slot)[0][0] == 0
        assert query(check, xmin, [int(pid)]) == []
    finally:
        if child.poll() is None:
            child.kill()
        check.close()


@PG
@pytest.mark.parametrize("stamped", [True, False])
def test_an_append_add_triggers_the_snapshot_only_from_a_stamped_release(
    schema_source,  # noqa: F811
    stamped,
):
    """S7: add still needs every command at >= 1; the snapshot trigger needs the backfill
    Flight at APPEND_SNAPSHOT. Below it the add is done, and its next step is
    start-from-log."""
    from test_action_flight import Trigger
    from test_membership_flight import dry, go

    from cdc_flight.releases import record_release

    source, ingest, log, _, _ = schema_source
    name = "t_trig"
    table = f"{source.schema}.{name}"
    source.execute(sql.SQL("CREATE TABLE {} (msg text)").format(sql.Identifier(name)))
    source.execute(sql.SQL("ALTER TABLE {} REPLICA IDENTITY FULL").format(sql.Identifier(name)))
    add_to_publication(source, ingest, name)
    ingest_once(ingest, log)
    declare_all(log)
    if stamped:
        record_release(log.execute, "backfill", {"MOTHERDUCK_FLIGHT_ID": "f"})
    preview = dry(schema_source, action="add", table=table, mode="append")
    trigger = Trigger()
    out = go(
        schema_source, trigger=trigger, action="add", table=table, mode="append", preview_id=preview
    )
    assert out["status"] == "done", out
    if stamped:
        assert len(trigger.calls) == 1 and out["triggered"]["flight"] == "backfill"
        assert not any("from the log" in s for s in out["next_steps"])
    else:
        assert trigger.calls == [] and "predates" in out["detail"]
        assert "from the log" in out["next_steps"][0]


@PG
def test_publication_renames_commit_together(schema_source):  # noqa: F811
    """Q3: publication renames the live mirror aside and staging in, with apply_state, in
    one transaction. A fault after both renames leaves neither; the retry publishes.
    MotherDuck: run its node id with `[motherduck]` (md_policy's diagnostic run)."""
    from cdc_flight import table_actions as ta

    source, ingest, log, snap, apply, table = _setup(schema_source, name="t_pub")
    schema, name = table.split(".")
    assert ta.start_from_log(apply, table, run_id="s", actor="t").status == "done"
    source.execute(sql.SQL("INSERT INTO {} VALUES ('live', 9)").format(sql.Identifier(name)))
    ingest_once(ingest, log)
    assert apply.apply_table(table, "a1").status == "caught_up"
    before = _mirror_rows(apply, table)

    class Crash(Exception):
        pass

    def fault(stage):
        if stage == "after_rename":
            raise Crash

    with pytest.raises(Crash):
        _drive(snap, table, ingest, log, fault=fault)
    assert _mirror_rows(apply, table) == before
    assert kept_aside(snap, schema, name) == []
    gen = snap.active(snap.registered(table, append_ok=True)[0])
    assert gen["state"] == "catching_up"
    result = snap.run_table(table, start=False)
    assert result.status == "published", result
    assert len(kept_aside(snap, schema, name)) == 1
    ingest_once(ingest, log)
    assert apply.apply_table(table, "a2").status == "caught_up"
    assert _mirror_rows(apply, table) == _source_rows(source, name)


@PG
def test_abandon_ends_an_append_generation(schema_source):  # noqa: F811
    """Abandon takes an append table too: staging dropped, claim and pin gone."""
    source, ingest, log, snap, apply, table = _setup(schema_source, name="t_ab")
    assert snap.run_table(table).status == "waiting_for_marker"
    gen = snap.active(snap.registered(table, append_ok=True)[0])
    generation = snap.abandon(table)
    assert generation == gen["generation_id"]
    assert snap.generation(generation)["state"] == "abandoned"
    assert not snap.execute("SELECT count(*) FROM {m}backfills").fetchone()[0]
    assert not apply.con.execute(
        "SELECT count(*) FROM duckdb_tables() WHERE table_name=?", [gen["staging"]]
    ).fetchone()[0]


@PG
def test_a_unit_spanning_c_across_a_resumed_catch_up(schema_source):  # noqa: F811
    """A commit before C and one at or after C share a unit; catch-up
    stops after its first batch and a new run resumes. The later commit appears once,
    the earlier only through the snapshot."""
    source, ingest, log, snap, apply, table = _setup(schema_source, name="t_r21", rows=("s",))
    name = table.split(".")[1]
    snap.config = dataclasses.replace(snap.config, max_batch_events=1)

    def ins(value):
        source.execute(
            sql.SQL("INSERT INTO {} VALUES (%s,1)").format(sql.Identifier(name)), [value]
        )

    class Crash(Exception):
        pass

    state = {}

    def fault(stage):
        if stage == "after_pin":
            ins("before C")  # [L, C): snapshot and log; the same ingest unit as below
        elif stage == "after_slot":
            ins("after C")
        elif stage == "after_scan_done":
            ins("later")  # a second unit, so catch-up takes two batches
            ingest_once(ingest, log)
        elif stage == "after_commit" and not state:
            state["crashed"] = True
            raise Crash

    with pytest.raises(Crash):
        _drive(snap, table, ingest, log, fault=fault)
    gen = snap.active(snap.registered(table, append_ok=True)[0])
    # The pair shares a unit: one unit holds commits on both sides of C.
    page = snap._page(gen["epoch"], gen["pin_lsn"], gen["marker_lsn"])
    events = snap.reader.events(page, gen["relation_id"], high=gen["marker_lsn"])
    by_unit = {}
    for e in events:
        by_unit.setdefault(e["unit_start_lsn"], set()).add(e["commit_lsn"] >= gen["snapshot_lsn"])
    assert {True, False} in by_unit.values(), by_unit
    snap.execute("UPDATE {m}generations SET expires_at=current_timestamp-INTERVAL 1 SECOND")
    assert snap.run_table(table, start=False).status == "published"
    ingest_once(ingest, log)
    assert apply.apply_table(table, "a1").status == "caught_up"
    assert _mirror_rows(apply, table) == _source_rows(source, name)
    assert sorted(r[0] for r in _mirror_rows(apply, table)) == ["after C", "before C", "later", "s"]


@PG
def test_the_kept_aside_bound_covers_a_retired_mirror(schema_source):  # noqa: F811
    """A recreated table (a new OID) publishes over the retired
    relation's mirror, keeping it aside, so an older kept table refuses it too."""
    from cdc_flight import table_actions as ta
    from cdc_flight.membership import remove_table

    source, ingest, log, snap, apply, table = _setup(schema_source, name="t_ret")
    schema, name = table.split(".")
    assert ta.start_from_log(apply, table, run_id="s", actor="t").status == "done"
    source.execute(sql.SQL("INSERT INTO {} VALUES ('old', 1)").format(sql.Identifier(name)))
    ingest_once(ingest, log)
    assert apply.apply_table(table, "a1").status == "caught_up"
    assert remove_table(log, apply, table, force=True).status == "removed"
    source.execute(sql.SQL("DROP TABLE {}").format(sql.Identifier(name)))
    source.execute(sql.SQL("CREATE TABLE {} (msg text, n int)").format(sql.Identifier(name)))
    source.execute(sql.SQL("ALTER TABLE {} REPLICA IDENTITY FULL").format(sql.Identifier(name)))
    source.execute(sql.SQL("INSERT INTO {} VALUES ('new', 2)").format(sql.Identifier(name)))
    add_to_publication(source, ingest, name)
    ingest_once(ingest, log)
    log.forget()
    added = add_table(log, apply, source.con, ingest.publication, table, mode="append")
    assert added.status in ("added", "adopted", "reactivated"), added
    stale = f"{name}__gen_deadbeef"
    apply.con.execute(f'CREATE TABLE {apply.mirror}."{schema}"."{stale}" (v int)')
    refused = snap.run_table(table)
    assert refused.status == "refused" and "DROP TABLE" in refused.detail, refused
    apply.con.execute(f'DROP TABLE {apply.mirror}."{schema}"."{stale}"')
    result = _drive(snap, table, ingest, log)
    assert result.status == "published", result
    assert len(kept_aside(snap, schema, name)) == 1
    assert _mirror_rows(apply, table) == [("new", 2)]


def test_replication_connect_failures_are_told_apart():
    """Connect errors carry no SQLSTATE, so their message decides."""
    cases = {
        "FATAL:  must be superuser or replication role to start walsender": ab.CAPABILITY,
        "FATAL:  permission denied to start WAL sender": ab.CAPABILITY,
        "FATAL:  number of requested standby connections exceeds max_wal_senders (currently 2)": (
            "no free replication slot"
        ),
        "FATAL:  sorry, too many clients already": "no free replication slot",
        'connection to server at "h" (10.0.0.1), port 5432 failed: timeout expired\n': (
            "replication connection failed: connection to server"
        ),
    }
    for message, expected in cases.items():
        assert ab.connect_refusal(psycopg2.OperationalError(message)).startswith(expected)


@PG
@pytest.mark.parametrize("where", ["copy", "catch_up"])
def test_errors_fail_the_generation_not_the_run(schema_source, monkeypatch, where):  # noqa: F811
    """A lost source connection mid-copy, or a data error in catch-up, fails
    the generation (staging dropped, claim and pin released) instead of escaping."""
    import duckdb

    source, ingest, log, snap, apply, table = _setup(schema_source, name="t_err")
    name = table.split(".")[1]

    def fault(stage):
        if where == "copy" and stage == "after_import":
            raise psycopg2.OperationalError("server closed the connection unexpectedly")
        if where == "catch_up" and stage == "after_slot":
            source.execute(sql.SQL("INSERT INTO {} VALUES ('x', 1)").format(sql.Identifier(name)))

    if where == "catch_up":

        def broken(*a, **k):
            raise duckdb.ConversionException("bad value")

        monkeypatch.setattr(ab, "write_append", broken)
    result = _drive(snap, table, ingest, log, fault=fault)
    assert result.status == "failed", result
    assert ("OperationalError" if where == "copy" else "ConversionException") in result.detail
    assert not snap.execute("SELECT count(*) FROM {m}backfills").fetchone()[0]
    assert not _slots(source, snap)


def _reasoned(log, relation_id, version, epoch, cut, reason="type_changed"):
    """A catalog hold's key_changes row, as ingest writes one."""
    log.execute(
        "INSERT INTO {s}key_changes (relation_id,version,key_columns,cut_lsn,new_from_lsn,"
        "epoch,reason) VALUES (?,?,'[]',?,?,?,?)",
        [relation_id, version, cut, cut, epoch, reason],
    )


@PG
def test_a_catalog_hold_cut_during_the_backfill_fails_it(schema_source):  # noqa: F811
    """A reasoned key_changes row cut in (L, H] fails the generation at the marker."""
    source, ingest, log, snap, apply, table = _setup(schema_source, name="t_p1")
    relation_id = snap.registered(table, append_ok=True)[0]

    def fault(stage):
        if stage == "after_scan_done":
            gen = snap.active(relation_id)
            _reasoned(log, relation_id, 1, gen["epoch"], gen["snapshot_lsn"] + 1)

    result = _drive(snap, table, ingest, log, fault=fault)
    assert result.status == "failed" and "type_changed" in result.detail, result


@PG
def test_publication_passes_older_catalog_holds(schema_source):  # noqa: F811
    """A hold cut before L (left from before a remove and re-add, the registry's
    version back at 0) is passed: publication records its version, and apply doesn't
    hold again on it."""
    source, ingest, log, snap, apply, table = _setup(schema_source, name="t_p2")
    relation_id = snap.registered(table, append_ok=True)[0]
    epoch, low = snap.source_position()
    _reasoned(log, relation_id, 4, epoch, low)
    result = _drive(snap, table, ingest, log)
    assert result.status == "published", result
    assert apply.state(relation_id)["key_version"] == 4
    source.execute("INSERT INTO t_p2 VALUES ('after', 1)")
    ingest_once(ingest, log)
    assert apply.apply_table(table, "a1").status == "caught_up"
    assert _mirror_rows(apply, table) == _source_rows(source, "t_p2")
    # Control: with the version 1b would have written (0), apply holds on the old row.
    snap.execute("UPDATE {m}apply_state SET key_version=0 WHERE relation_id=?", [relation_id])
    source.execute("INSERT INTO t_p2 VALUES ('again', 2)")
    ingest_once(ingest, log)
    assert apply.apply_table(table, "a2").status != "caught_up"
