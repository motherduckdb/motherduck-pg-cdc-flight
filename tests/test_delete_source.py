"""Delete-source (delete_source, delete_source_flight).

Real PostgreSQL (`populated`) and a local change log.
"""

import json
import time
import uuid

import psycopg2
import pytest
from psycopg2.extras import LogicalReplicationConnection
from test_backfill import drive, populated  # noqa: F401 (fixture)
from test_epoch_flight import go as epoch_go
from test_epoch_flight import previews, recover
from test_epoch_pg import confirmed
from test_explainer import branches, drift
from test_phase2 import Harness

from cdc_flight import action_flight as af
from cdc_flight import delete_source as ds
from cdc_flight import delete_source_flight as df
from cdc_flight import membership_flight as mf
from cdc_flight.apply_run import ApplyRun
from cdc_flight.chain import FLIGHT_CONFIG
from cdc_flight.compact import CompactConfig, run_compaction
from cdc_flight.epoch import begin_epoch
from cdc_flight.flightlog import Run
from cdc_flight.ingest import run_once
from cdc_flight.ingest_log import IngestLog
from cdc_flight.preflight import query
from cdc_flight.retention import SourceDeleted

LOCAL = pytest.mark.parametrize("stores", ["local"], indirect=True)
PG = pytest.mark.postgres


def owner_for(ingest):
    return df.Owner(
        ingest.source_id,
        slot=ingest.slot,
        publication=ingest.publication,
        heartbeat=ingest.heartbeat,
        database=ingest.database,
    )


def go(fixture, *, run=None, run_id=None, wait=0, **env):
    _, _, ingest, log, _ = fixture
    fresh = IngestLog(log.con, ingest)
    assert af.ensure_previews(fresh)
    fresh.initialize()
    environ = {f"CDC_{k.upper()}": v for k, v in env.items()}
    return df.perform(
        fresh,
        ingest,
        af.parse(environ, df.DELETE),
        run_id=run_id or str(uuid.uuid4()),
        actor="flight:test",
        owner=owner_for(ingest),
        run=run,
        wait=wait,
    )


def delete(ingest, epoch=1, **extra):
    return dict(
        action="delete-source",
        expected_epoch=str(epoch),
        confirm=f"DELETE {ingest.source_id}",
        **extra,
    )


def lifecycle(log):
    return log.execute("SELECT lifecycle,slot_name,deleted_slot_name FROM {s}source").fetchone()


def publication(source, ingest):
    return bool(
        query(source.con, "SELECT 1 FROM pg_publication WHERE pubname=%s", [ingest.publication])
    )


def heartbeat(source, ingest):
    return query(source.con, "SELECT to_regclass(%s)", [ingest.heartbeat])[0][0] is not None


def ours(log):
    log.record_created(dict(slot=True, publication=True, heartbeat=True))


# Validation (no connection) ---------------------------------------------------------------

REFUSALS = [
    (dict(ACTION="drop"), "CDC_ACTION must be one of delete-source"),
    (dict(ACTION="delete-source", CONFIRM="DELETE probe"), "CDC_EXPECTED_EPOCH is required"),
    (dict(ACTION="delete-source", EXPECTED_EPOCH="1"), "CDC_CONFIRM is required"),
    (dict(ACTION="delete-source", EXPECTED_EPOCH="x", CONFIRM="y"), "CDC_EXPECTED_EPOCH must"),
]


@pytest.mark.parametrize("env, message", REFUSALS)
def test_a_bad_input_is_refused_before_connecting(env, message, monkeypatch, capsys):
    monkeypatch.setattr(df.duckdb, "connect", lambda *a, **k: pytest.fail("connected"))
    environ = {f"CDC_{k}": v for k, v in dict(env, DRY_RUN="1").items()}
    assert df.main(df.Owner("probe"), environ) == 2
    out = capsys.readouterr().out
    assert message in json.loads(out.splitlines()[-1])["detail"]
    assert not drift("delete-source", out, required=["validate", "invalid"])


def test_keys_and_owner():
    keys = set(FLIGHT_CONFIG["delete-source"])
    assert keys == {
        "CDC_ACTION",
        "CDC_EXPECTED_EPOCH",
        "CDC_CONFIRM",
        "CDC_DRY_RUN",
        "CDC_PREVIEW_ID",
        "CDC_REQUEST_ID",
    }
    for bad in (dict(source_id="s", dsn_env="MOTHERDUCK_TOKEN"), dict(source_id="s", tables="x")):
        with pytest.raises((ValueError, TypeError)):
            df.Owner.from_dict(bad)


# Created objects are recorded, monotonically ---------------------------------


@LOCAL
def test_created_flags_are_monotonic_and_a_recover_marks_its_slot(stores, tmp_path):
    h = Harness(stores, tmp_path)
    flags = "SELECT created_slot,created_publication,created_heartbeat FROM {s}source"
    # Format 2 records ownership at registration: none created here, so none ours.
    assert h.log.execute(flags).fetchone() == (False, False, False)
    h.log.record_created(dict(publication=True))
    h.log.record_created({})  # a re-run that found everything present
    h.log.record_created(dict(slot=False, publication=False))  # never lowers
    assert h.log.execute(flags).fetchone() == (False, True, False)
    h.log.acquire("t")
    try:
        h.log.begin_epoch(2, 500, "slot_lost", "slot_e2", details={})
    finally:
        h.log.release()
    assert h.log.execute(flags).fetchone()[0] is True


# Delete on PostgreSQL ---------------------------------------------------------------------


@LOCAL
@PG
def test_a_pre_p5b_source_drops_nothing_and_prints_the_ddl(stores, populated):  # noqa: F811
    """Format 2 has no store from before the fence; its equivalent is a source registered
    over objects it found (created_* false at bootstrap): nothing is dropped, the DDL is printed."""
    source, _, ingest, log, _ = populated
    log.execute(
        "UPDATE {s}source SET created_slot=false,created_publication=false,"
        "created_heartbeat=false WHERE id=1"
    )
    assert run_once(ingest, log).status == "drained"
    would = go(populated, dry_run="1", **delete(ingest))
    assert would["status"] == "would" and would["drop"] == [] and len(would["dba"]) == 3, would
    assert lifecycle(log)[0] is None  # read only
    done = go(populated, preview_id=would["preview_id"], **delete(ingest))
    assert done["status"] == "done" and done["lifecycle"] == "deleted" and done["dropped"] == []
    assert any("pg_drop_replication_slot" in d for d in done["dba"])
    assert lifecycle(log) == ("deleted", f"deleted:{ingest.source_id}", ingest.slot)
    assert confirmed(source, ingest.slot) is not None and publication(source, ingest)
    assert heartbeat(source, ingest)
    assert go(populated, preview_id=would["preview_id"], **delete(ingest))["status"] == "already"


@LOCAL
@PG
def test_a_created_source_drops_exactly_its_slot_publication_and_heartbeat(stores, populated):  # noqa: F811
    source, _, ingest, log, backfill = populated
    assert run_once(ingest, log).status == "drained"
    assert drive(backfill, f"{source.schema}.pairs", ingest, log).status == "published"
    mirror_rows = log.con.execute(
        f'SELECT count(*) FROM "{backfill.config.mirror_database}"."{source.schema}".pairs'
    ).fetchone()
    ours(log)
    pending = ingest.slot + "_e7"  # an interrupted recover's leftover
    try:
        query(source.con, "SELECT pg_create_logical_replication_slot(%s,'pgoutput')", [pending])
        preview = go(populated, dry_run="1", **delete(ingest))
        assert {d["name"] for d in preview["drop"]} == {
            ingest.slot,
            pending,
            ingest.publication,
            ingest.heartbeat,
        }, preview
        done = go(populated, preview_id=preview["preview_id"], **delete(ingest))
        assert done["status"] == "done" and done["dba"] == [], done
        assert confirmed(source, ingest.slot) is None and confirmed(source, pending) is None
        assert not publication(source, ingest) and not heartbeat(source, ingest)
        # U6: the mirror and the log are kept.
        assert (
            log.con.execute(
                f'SELECT count(*) FROM "{backfill.config.mirror_database}"."{source.schema}".pairs'
            ).fetchone()
            == mirror_rows
        )
        assert log.execute("SELECT count(*) FROM {s}tables").fetchone()[0] > 0
    finally:
        drop_slots(source.con, [pending])  # never leak the leftover if a step fails


@LOCAL
@PG
def test_an_attached_reader_is_waiting_and_the_rerun_finishes(stores, populated, monkeypatch):  # noqa: F811
    source, _, ingest, log, _ = populated
    assert run_once(ingest, log).status == "drained"
    ours(log)
    real = ds.observe

    def active(*args, **kwargs):
        observed = real(*args, **kwargs)
        return dict(observed, slot_active=True)

    monkeypatch.setattr(ds, "observe", active)
    preview = go(populated, dry_run="1", **delete(ingest))["preview_id"]
    out = go(populated, preview_id=preview, **delete(ingest))
    assert out["status"] == "waiting" and out["lifecycle"] == "deleting", out
    assert af.EXIT["waiting"] == 0
    assert lifecycle(log)[0] == "deleting" and confirmed(source, ingest.slot) is not None
    monkeypatch.undo()
    # S4: a dry run on a deleting source is read only; the re-run needs the literal and
    # epoch but no preview, and finishes.
    dry = go(populated, dry_run="1", **delete(ingest))
    assert dry["status"] == "already" and lifecycle(log)[0] == "deleting"
    wrong = go(populated, preview_id=str(uuid.uuid4()), **dict(delete(ingest), confirm="DELETE x"))
    assert wrong["status"] == "refused" and lifecycle(log)[0] == "deleting"
    stale = go(populated, preview_id=str(uuid.uuid4()), **delete(ingest, epoch=2))
    assert stale["status"] == "refused"
    done = go(populated, preview_id=str(uuid.uuid4()), **delete(ingest))
    assert done["status"] == "done" and lifecycle(log)[0] == "deleted", done
    assert confirmed(source, ingest.slot) is None


@LOCAL
@PG
def test_every_entry_point_refuses_a_deleting_or_deleted_source(stores, populated):  # noqa: F811
    source, _, ingest, log, backfill = populated
    assert run_once(ingest, log).status == "drained"
    log.execute(
        "UPDATE {s}source SET lifecycle='deleting',deleted_slot_name=slot_name,"
        "slot_name='deleted:'||? WHERE id=1",
        [ingest.source_id],
    )
    for state in ("deleting", "deleted"):
        log.execute("UPDATE {s}source SET lifecycle=? WHERE id=1", [state])
        with pytest.raises(
            SourceDeleted, match="being deleted" if state == "deleting" else "final"
        ):
            run_once(ingest, IngestLog(log.con, ingest))  # ingest and preflight's registered_slot
        with pytest.raises(SourceDeleted):
            IngestLog(log.con, ingest).acquire("x")  # membership, epoch
        with pytest.raises(SourceDeleted):
            begin_epoch(ingest, IngestLog(log.con, ingest), 2, "slot_lost")
        with pytest.raises(SourceDeleted, match=f"source {ingest.source_id} "):
            ApplyRun(log.con, backfill.config).run()  # apply (head), named
        with pytest.raises(SourceDeleted, match=f"source {ingest.source_id} "):
            backfill.source_position()  # backfill
        with pytest.raises(SourceDeleted):
            run_compaction(CompactConfig(ingest.source_id, database=ingest.database), log.con)
        # The epoch and membership dry runs (no lease) refuse too, and record
        # no preview; the real recover is refused before its lease.
        for extra in (dict(dry_run="1"), dict(preview_id=str(uuid.uuid4()))):
            out = epoch_go(populated, **recover(ingest, **extra))
            assert out["status"] == "refused" and f"source {ingest.source_id} " in out["detail"], (
                out
            )
        membership = mf.perform(
            IngestLog(log.con, ingest),
            None,
            lambda: pytest.fail("connected to PostgreSQL"),
            af.parse({"CDC_ACTION": "add", "CDC_TABLE": "s.t", "CDC_DRY_RUN": "1"}, mf.MEMBERSHIP),
            run_id=str(uuid.uuid4()),
            actor="flight:test",
            owner=mf.Owner(ingest.source_id, str(uuid.uuid4()), database=ingest.database),
            trigger=None,
        )
        assert membership["status"] == "refused" and "source" in membership["detail"]
        assert previews(log, ingest) == []
    # D3: a deleted source id is final for registration too.
    fresh = IngestLog(log.con, ingest)
    with pytest.raises(SourceDeleted, match="final"):
        fresh.register(("x", 1), 0, [])
    log.execute("UPDATE {s}source SET slot_name=deleted_slot_name WHERE id=1")


def create_slot(con, name, plugin="pgoutput"):
    query(con, "SELECT pg_create_logical_replication_slot(%s,%s)", [name, plugin])


def drop_slots(con, names):
    query(
        con,
        "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
        "WHERE slot_name=ANY(%s) AND NOT active",
        [list(names)],
    )


def attach_reader(ingest, slot):
    reader = psycopg2.connect(ingest.dsn, connection_factory=LogicalReplicationConnection)
    reader.cursor().start_replication(
        slot_name=slot,
        options={"proto_version": "1", "publication_names": ingest.publication},
    )
    return reader


def wait_active(con, slot, active=True):
    for _ in range(100):
        rows = query(con, "SELECT active FROM pg_replication_slots WHERE slot_name=%s", [slot])
        if rows and rows[0][0] == active:
            return
        time.sleep(0.05)
    raise AssertionError(f"slot {slot} never became active={active}")


@LOCAL
@PG
def test_only_this_sources_slots_are_dropped(stores, populated):  # noqa: F811
    """The "only ours" predicate. A pending slot is exactly `<slot>_e<n>`,
    pgoutput, this database and inactive; neighbours that miss any one are kept."""
    source, _, ingest, log, _ = populated
    assert run_once(ingest, log).status == "drained"
    ours(log)
    base = ingest.slot
    pending, kept = base + "_e7", [base + "2", base + "_e7x", base + "_e8", base + "_e9"]
    reader = None
    try:
        for name in (pending, base + "2", base + "_e7x", base + "_e9"):
            create_slot(source.con, name)
        create_slot(source.con, base + "_e8", "test_decoding")  # not pgoutput
        reader = attach_reader(ingest, base + "_e9")  # a reader attached
        wait_active(source.con, base + "_e9")
        preview = go(populated, dry_run="1", **delete(ingest))
        assert {d["name"] for d in preview["drop"]} == {
            base,
            pending,
            ingest.publication,
            ingest.heartbeat,
        }, preview
        assert not [d for d in preview["dba"] if base + "_e" in d or base + "2" in d], preview
        done = go(populated, preview_id=preview["preview_id"], **delete(ingest))
        assert done["status"] == "done", done
        assert confirmed(source, base) is None and confirmed(source, pending) is None
        assert all(
            query(source.con, "SELECT 1 FROM pg_replication_slots WHERE slot_name=%s", [n])
            for n in kept
        )
    finally:
        if reader is not None:
            reader.close()
            wait_active(source.con, base + "_e9", active=False)
        drop_slots(source.con, [pending, *kept])


@LOCAL
@PG
def test_a_same_named_slot_in_another_database_is_not_dropped(stores, populated):  # noqa: F811
    """The registered slot name, found in another database of the
    cluster, isn't this source's slot even with the created flag set. Nor is a pending
    `<slot>_e<n>` there."""
    source, _, ingest, log, _ = populated
    assert run_once(ingest, log).status == "drained"
    ours(log)
    drop_slots(source.con, [ingest.slot])
    elsewhere = [ingest.slot, ingest.slot + "_e9"]
    other = psycopg2.connect(ingest.dsn, dbname="postgres")
    other.autocommit = True
    try:
        for name in elsewhere:
            create_slot(other, name)
        preview = go(populated, dry_run="1", **delete(ingest))
        slots = {d["name"] for d in preview["drop"] if d["kind"] == "slot"}
        assert not slots & set(elsewhere), preview
        assert [d for d in preview["dba"] if f"'{ingest.slot}'" in d], preview
        done = go(populated, preview_id=preview["preview_id"], **delete(ingest))
        assert done["status"] == "done", done
        present = "SELECT count(*) FROM pg_replication_slots WHERE slot_name=ANY(%s)"
        assert query(other, present, [elsewhere])[0][0] == 2
    finally:
        drop_slots(other, elsewhere)
        other.close()


@LOCAL
@PG
def test_a_recovers_leftover_slot_is_left_to_the_dba(stores, populated):  # noqa: F811
    """A recover replaced the first slot but couldn't drop it (its post-commit
    drop is best effort). Delete reports it on the DBA list with its retained WAL, rather
    than leaving it unreported; the recover's own slot is dropped."""
    source, _, ingest, log, _ = populated
    assert run_once(ingest, log).status == "drained"
    ours(log)
    base, new = ingest.slot, ingest.slot + "_e2"
    create_slot(source.con, new)
    try:
        log.acquire("t")
        try:
            log.begin_epoch(2, confirmed(source, new), "slot_lost", new, details={})
        finally:
            log.release()
        preview = go(populated, dry_run="1", **delete(ingest, epoch=2))
        assert {d["name"] for d in preview["drop"]} == {
            new,
            ingest.publication,
            ingest.heartbeat,
        }, preview
        assert preview["dba"] == [f"SELECT pg_drop_replication_slot('{base}');"], preview
        assert preview["retained_bytes"] is not None
        done = go(populated, preview_id=preview["preview_id"], **delete(ingest, epoch=2))
        assert done["status"] == "done" and done["dba"] == preview["dba"], done
        assert confirmed(source, new) is None and confirmed(source, base) is not None
    finally:
        drop_slots(source.con, [new])


@LOCAL
@PG
def test_a_role_that_cant_drop_an_object_leaves_it_to_the_dba(stores, populated, monkeypatch):  # noqa: F811
    """Privileges are checked read only, in the dry run; the real run drops
    only the rest."""
    source, _, ingest, log, _ = populated
    assert run_once(ingest, log).status == "drained"
    ours(log)
    real = ds.query

    def not_owner(con, statement, args=None):
        if "FROM pg_publication WHERE pubname" in statement:
            return [(False,)]
        return real(con, statement, args) if args is not None else real(con, statement)

    monkeypatch.setattr(ds, "query", not_owner)
    preview = go(populated, dry_run="1", **delete(ingest))
    assert [d for d in preview["dba"] if "PUBLICATION" in d], preview
    done = go(populated, preview_id=preview["preview_id"], **delete(ingest))
    assert done["status"] == "done" and publication(source, ingest)
    assert confirmed(source, ingest.slot) is None
    query(source.con, f'DROP PUBLICATION IF EXISTS "{ingest.publication}"')


@LOCAL
@PG
def test_delete_lines_match_the_chart(stores, populated, capsys):  # noqa: F811
    source, _, ingest, log, _ = populated
    assert run_once(ingest, log).status == "drained"
    capsys.readouterr()
    taken = set()

    def lines(**env):
        run = Run()
        run.step("validate", "ok", branch="ok")
        out = go(populated, run=run, **env)
        text = capsys.readouterr().out
        assert not drift("delete-source", text, required=["validate", "decide"]), text
        taken.update(branches(text))
        return out

    assert lines(**delete(ingest, epoch=3, dry_run="1"))["status"] == "refused"
    preview = lines(**delete(ingest, dry_run="1"))["preview_id"]
    assert lines(preview_id=str(uuid.uuid4()), **delete(ingest))["status"] == "refused"
    assert lines(preview_id=preview, **delete(ingest))["status"] == "done"
    assert lines(**delete(ingest, dry_run="1"))["status"] == "already"
    assert taken >= {
        ("decide", "would"),
        ("decide", "act"),
        ("outcome", "done"),
        ("decide", "already"),
    }


def test_the_failure_path_never_reads_a_tombstoned_slot(monkeypatch):
    """`observe_after_failure` skips a deleting source's slot."""
    from cdc_flight import ingest

    monkeypatch.setattr(ingest, "connect", lambda *a, **k: pytest.fail("connected"))
    observe = {}
    ingest.observe_after_failure(None, "deleted:shop", observe)
    assert observe == {"lifecycle": "deleting_or_deleted"}


def test_check_lifecycle_treats_only_a_missing_column_as_active():
    """A transient failure is raised, never read as `not being deleted`."""
    import duckdb as ddb

    from cdc_flight.retention import check_lifecycle

    class Store:
        names = ("db", "s")

        def __init__(self, error):
            self.error = error

        def execute(self, *args, **kwargs):
            raise self.error

    check_lifecycle(Store(ddb.BinderException("column lifecycle not found")))  # old store
    with pytest.raises(ddb.IOException):
        check_lifecycle(Store(ddb.IOException("network")))
    with pytest.raises(ddb.InterruptException):
        check_lifecycle(Store(ddb.InterruptException("interrupted")))


@LOCAL
def test_a_click_on_a_deleting_source_is_refused_with_its_audit_row(stores, tmp_path, monkeypatch):
    """The refusal goes through finish, so the Dive gets the audit row and reason."""
    import re

    from test_action_flight import BACKFILL_FLIGHT, COMPACT_FLIGHT

    h = Harness(stores, tmp_path)
    h.log.execute("UPDATE {s}source SET lifecycle='deleting' WHERE id=1")
    database, mirror = h.config.database, h.config.mirror_database
    files = {
        database: str(tmp_path / f"{database}.duckdb"),
        mirror: str(tmp_path / f"{mirror}.duckdb"),
    }
    for con in {id(s.con): s.con for s in stores}.values():
        con.close()
    real_connect = af.duckdb.connect

    class Con:
        def __init__(self, con):
            self.con = con

        def execute(self, statement, parameters=None):
            attach = re.fullmatch(r"ATTACH IF NOT EXISTS 'md:(\w+)' AS \"(\w+)\"", statement)
            if attach:
                statement = (
                    f"ATTACH IF NOT EXISTS '{files[attach.group(1)]}' AS \"{attach.group(2)}\""
                )
            return self.con.execute(statement, parameters or [])

        def __getattr__(self, name):
            return getattr(self.con, name)

    finished = []
    monkeypatch.setattr(
        af.duckdb,
        "connect",
        lambda address, *a, **k: Con(real_connect(files[address[3:].split("?")[0]])),
    )
    monkeypatch.setattr(
        af, "finish", lambda con, owner, inputs, out, run, **k: finished.append(out)
    )
    owner = af.Owner(
        h.config.source_id,
        BACKFILL_FLIGHT,
        COMPACT_FLIGHT,
        database=database,
        mirror_database=mirror,
    )
    env = {"CDC_ACTION": "compact-now", "CDC_DRY_RUN": "1"}
    assert af.main(owner, env) == 2
    ((out,),) = [finished]
    assert out["status"] == "refused" and "phase2 is being deleted" in out["detail"], out


@PG
def test_an_rds_replication_member_can_drop_the_slot(pg_dsn):
    """Amazon RDS gives replication through membership in rds_replication, and every
    login role's rolreplication is false: delete-source must count that membership, or
    every RDS delete leaves the slot to the DBA (found retiring the dev sources,
    2026-11-17). Only an inheriting member counts (USAGE). A server without the role reads
    false and never errors. Roles are
    created in one transaction and rolled back: nothing is left on the cluster."""
    suffix = uuid.uuid4().hex[:8]
    names = ("member", "other", "nested", "via", "noinherit")
    member, other, nested, via, noinherit = (f"cdc_t_{n}_{suffix}" for n in names)
    con = psycopg2.connect(pg_dsn)
    try:
        cur = con.cursor()

        def can(role=None):
            cur.execute("RESET ROLE" if role is None else f'SET ROLE "{role}"')
            cur.execute(ds.CAN_REPLICATE)
            return cur.fetchone()[0]

        cur.execute("SELECT rolsuper FROM pg_roles WHERE rolname=current_user")
        assert cur.fetchone()[0] and can()  # the fixture's superuser
        for role in (member, other, nested, via):
            cur.execute(f'CREATE ROLE "{role}" NOLOGIN NOREPLICATION NOSUPERUSER')
        cur.execute(f'CREATE ROLE "{noinherit}" NOLOGIN NOREPLICATION NOSUPERUSER NOINHERIT')
        cur.execute("SELECT count(*) FROM pg_roles WHERE rolname='rds_replication'")
        if cur.fetchone()[0] == 0:
            assert can(member) is False  # no rds_replication role: false, no error
            cur.execute("RESET ROLE")
            cur.execute("CREATE ROLE rds_replication NOLOGIN")
        cur.execute("RESET ROLE")
        cur.execute(f'GRANT rds_replication TO "{member}"')
        cur.execute(f'GRANT rds_replication TO "{via}"')
        cur.execute(f'GRANT "{via}" TO "{nested}"')
        cur.execute(f'GRANT rds_replication TO "{noinherit}"')
        assert can(member) is True
        assert can(nested) is True  # membership through another role counts
        assert can(other) is False
        # A NOINHERIT member holds no privileges of the role: not counted, so
        # its slot goes to the DBA rather than a drop that would fail.
        assert can(noinherit) is False
    finally:
        con.rollback()
        con.close()
