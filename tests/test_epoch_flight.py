"""The epoch Flight (epoch_flight): recover a lost slot, skip to an LSN.

Real PostgreSQL (the `populated` source) and a local change log. Every slot a test can
create (`<slot>_e2`) is dropped in `finally`: a stray slot retains WAL.
"""

import json
import uuid

import psycopg2
import pytest
from flight_stub import assert_stub_ok, stub_env, stub_for
from test_action_flight import OWNER_ONLY
from test_backfill import populated  # noqa: F401 (fixture)
from test_epoch_pg import confirmed, crash, drop_slot, wal_lsn
from test_explainer import branches, drift

from cdc_flight import action_flight as af
from cdc_flight import epoch
from cdc_flight import epoch_flight as ef
from cdc_flight.chain import FLIGHT_CONFIG
from cdc_flight.config import lsn_text
from cdc_flight.flightlog import Run
from cdc_flight.ingest import run_once
from cdc_flight.ingest_log import IngestLog

LOCAL = pytest.mark.parametrize("stores", ["local"], indirect=True)
PG = pytest.mark.postgres


def owner_for(ingest):
    return ef.Owner(
        ingest.source_id,
        slot=ingest.slot,
        publication=ingest.publication,
        heartbeat=ingest.heartbeat,
        database=ingest.database,
    )


def go(fixture, *, run=None, run_id=None, started_by=None, wait=0, **env):
    source, _, ingest, log, _ = fixture
    fresh = IngestLog(log.con, ingest)
    assert af.ensure_previews(fresh)
    fresh.initialize()
    environ = {f"CDC_{k.upper()}": v for k, v in env.items()}
    return ef.perform(
        fresh,
        ingest,
        af.parse(environ, ef.EPOCH),
        run_id=run_id or str(uuid.uuid4()),
        actor="flight:test" + (f" (started by {started_by})" if started_by else ""),
        owner=owner_for(ingest),
        run=run,
        started_by=started_by,
        wait=wait,
    )


def recover(ingest, epoch_=1, **extra):
    return dict(
        action="recover-slot",
        expected_epoch=str(epoch_),
        confirm=f"RECOVER {ingest.source_id} EPOCH {epoch_ + 1}",
        **extra,
    )


def skip(ingest, to_lsn, epoch_=1, **extra):
    return dict(
        action="skip-to-lsn",
        expected_epoch=str(epoch_),
        to_lsn=lsn_text(to_lsn),
        confirm=f"SKIP {ingest.source_id} TO {lsn_text(to_lsn)}",
        **extra,
    )


def previews(log, ingest):
    return log.con.execute(
        f'SELECT action,used_at IS NOT NULL,actor FROM "{ingest.database}"."{ingest.source_id}"'
        ".previews ORDER BY created_at"
    ).fetchall()


def lost(fixture):
    """Ingest once, then lose the slot (dropped): a recover is now allowed."""
    source, _, ingest, log, _ = fixture
    assert run_once(ingest, log).status == "drained"
    drop_slot(source, ingest.slot)
    return ingest.slot + "_e2"


# Validation, keys, owner, build (no connection) ------------------------------------------

REFUSALS = [
    (dict(ACTION="drop-everything"), "CDC_ACTION must be one of recover-slot, skip-to-lsn"),
    (dict(ACTION="recover-slot", EXPECTED_EPOCH="1"), "CDC_CONFIRM is required"),
    (dict(ACTION="recover-slot", CONFIRM="x"), "CDC_EXPECTED_EPOCH is required"),
    (
        dict(ACTION="recover-slot", EXPECTED_EPOCH="1", CONFIRM="x", TO_LSN="0/1"),
        "CDC_TO_LSN is not used by recover-slot",
    ),
    (dict(ACTION="skip-to-lsn", EXPECTED_EPOCH="1", CONFIRM="x"), "CDC_TO_LSN is required"),
    (dict(ACTION="skip-to-lsn", EXPECTED_EPOCH="1", CONFIRM="x", TO_LSN="0/zz"), "CDC_TO_LSN must"),
    (dict(ACTION="recover-slot", EXPECTED_EPOCH="-1", CONFIRM="x"), "CDC_EXPECTED_EPOCH must"),
]


@pytest.mark.parametrize("env, message", REFUSALS)
def test_a_bad_input_is_refused_before_connecting(env, message, monkeypatch, capsys):
    def no_connection(*args, **kwargs):
        raise AssertionError("connected before validating")

    monkeypatch.setattr(ef.duckdb, "connect", no_connection)
    environ = {f"CDC_{k}": v for k, v in dict(env, DRY_RUN="1").items()}
    assert ef.main(ef.Owner("probe"), environ) == 2
    out = capsys.readouterr().out
    result = json.loads(out.splitlines()[-1])
    assert result["status"] == "refused" and message in result["detail"], result
    assert "0/zz" not in out
    assert not drift("epoch", out, required=["validate", "invalid"])


def test_a_missing_secret_stops_before_connecting(monkeypatch):
    monkeypatch.setattr(ef.duckdb, "connect", lambda *a, **k: pytest.fail("connected"))
    env = {"CDC_ACTION": "recover-slot", "CDC_EXPECTED_EPOCH": "1", "CDC_CONFIRM": "x"}
    with pytest.raises(SystemExit, match="SHOP_DSN"):
        ef.main(ef.Owner("probe", dsn_env="SHOP_DSN"), dict(env, CDC_DRY_RUN="1"))


def test_keys_owner_and_build(tmp_path):
    keys = set(FLIGHT_CONFIG["epoch"])
    assert keys == {
        "CDC_ACTION",
        "CDC_EXPECTED_EPOCH",
        "CDC_TO_LSN",
        "CDC_CONFIRM",
        "CDC_DRY_RUN",
        "CDC_PREVIEW_ID",
        "CDC_REQUEST_ID",
    }
    actions = ("table-action", "table-membership", "quarantine", "epoch", "delete-source")
    pipeline = {k for c, ks in FLIGHT_CONFIG.items() if c not in actions for k in ks}
    assert not keys & pipeline and not keys & OWNER_ONLY
    owner = ef.Owner("shop")
    assert (owner.slot, owner.publication, owner.dsn_env) == (
        "md_cdc_shop",
        "md_cdc_shop_pub",
        "CDC_PG_DSN",
    )
    for bad in (
        dict(source_id="shop", dsn_env="MOTHERDUCK_TOKEN"),
        dict(source_id="shop", dsn_env="SHOP"),
        dict(source_id="shop", ops_database="cdc_internal"),
        dict(source_id="shop", mirror_database="m"),  # not this Flight's setting
    ):
        with pytest.raises((ValueError, TypeError)):
            ef.Owner.from_dict(bad)
    stub = stub_for(tmp_path, "epoch", ["--source", "probe", "--dsn-env", "PROBE_DSN"])
    line = [x for x in stub.splitlines() if x.startswith("OWNER = ")][0]
    assert json.loads(eval(line.removeprefix("OWNER = ")))["dsn_env"] == "PROBE_DSN"


# Recover and skip on PostgreSQL ------------------------------------------------------------


@LOCAL
@PG
def test_recover_dry_run_then_real_run_begins_the_epoch(stores, populated):  # noqa: F811
    source, _, ingest, log, _ = populated
    new_slot = lost(populated)
    try:
        before = log.position()
        would = go(populated, dry_run="1", **recover(ingest))
        assert would["status"] == "would" and new_slot in would["detail"], would
        assert would["confirm"] == recover(ingest)["confirm"] and would["table"] is None
        assert log.position() == before and confirmed(source, new_slot) is None  # read only
        done = go(populated, preview_id=would["preview_id"], started_by="alice", **recover(ingest))
        assert done["status"] == "done" and done["epoch"] == 2 and done["slot"] == new_slot, done
        assert "resync-all" in done["next_steps"][0]
        assert log.position()[:2] == (2, new_slot)
        started_by, previous = log.execute(
            "SELECT started_by,previous_slot_name FROM {s}source_epochs WHERE epoch=2"
        ).fetchone()
        assert (started_by, previous) == ("alice", ingest.slot)
        assert previews(log, ingest) == [("recover-slot", True, "flight:test (started by alice)")]
        again = go(populated, preview_id=would["preview_id"], **recover(ingest, 2))
        assert again["status"] == "refused"  # epoch 2's literal is EPOCH 3: a new action
        # Repeating the committed click is `already`.
        repeat = go(populated, preview_id=would["preview_id"], **recover(ingest))
        assert repeat["status"] == "already" and repeat["epoch"] == 2, repeat
        assert go(populated, dry_run="1", **recover(ingest))["status"] == "already"
        other = go(populated, preview_id=would["preview_id"], **skip(ingest, 2**40))
        assert other["status"] == "refused"  # a different transition never matches
    finally:
        drop_slot(source, new_slot)


@LOCAL
@PG
@pytest.mark.parametrize("change", ["stale epoch", "lowercase confirm", "other epoch in confirm"])
def test_a_stale_epoch_or_wrong_literal_is_refused_and_changes_nothing(
    stores,
    populated,  # noqa: F811
    change,
):
    source, _, ingest, log, _ = populated
    new_slot = lost(populated)
    try:
        env = recover(ingest, dry_run="1")
        if change == "stale epoch":
            env["expected_epoch"] = "2"
        elif change == "lowercase confirm":
            env["confirm"] = env["confirm"].lower()
        else:
            env["confirm"] = f"RECOVER {ingest.source_id} EPOCH 3"
        out = go(populated, **env)
        assert out["status"] == "refused", out
        if "confirm" in change:
            assert f'"RECOVER {ingest.source_id} EPOCH 2"' in out["detail"]
        assert log.position()[0] == 1 and confirmed(source, new_slot) is None
    finally:
        drop_slot(source, new_slot)


@LOCAL
@PG
def test_skip_abandons_the_range_and_records_it(stores, populated):  # noqa: F811
    source, _, ingest, log, _ = populated
    try:
        assert run_once(ingest, log).status == "drained"
        source.execute("UPDATE pairs SET v=v+1 WHERE a=0")
        target = wal_lsn(source)
        checkpoint = log.position()[2]
        would = go(populated, dry_run="1", **skip(ingest, target))
        assert would["status"] == "would" and f"{target - checkpoint} bytes" in would["detail"]
        assert would["confirm"] == f"SKIP {ingest.source_id} TO {lsn_text(target)}"
        done = go(populated, preview_id=would["preview_id"], **skip(ingest, target))
        assert done["status"] == "done" and done["abandoned"] == [
            lsn_text(checkpoint),
            lsn_text(target),
        ]
        assert log.position() == (2, ingest.slot, target)
        assert confirmed(source, ingest.slot) == target
    finally:
        pass  # the fixture drops ingest.slot


@LOCAL
@PG
def test_the_claim_comes_first_and_a_failed_step_still_spends_the_preview(
    stores,
    populated,  # noqa: F811
    monkeypatch,
):
    source, _, ingest, log, _ = populated
    new_slot = lost(populated)
    try:
        preview = go(populated, dry_run="1", **recover(ingest))["preview_id"]

        def broken(*args, **kwargs):
            raise RuntimeError("PostgreSQL step failed")

        monkeypatch.setattr(epoch, "_recreate", broken)
        with pytest.raises(RuntimeError, match="PostgreSQL step failed"):
            go(populated, preview_id=preview, **recover(ingest))
        monkeypatch.undo()
        assert previews(log, ingest)[0][1]  # spent: claimed before the slot step
        assert log.position()[0] == 1
        refused = go(populated, preview_id=preview, **recover(ingest))
        assert refused["status"] == "refused" and refused["detail"] == af.NO_PREVIEW
        fresh = go(populated, dry_run="1", **recover(ingest))["preview_id"]
        assert go(populated, preview_id=fresh, **recover(ingest))["status"] == "done"
    finally:
        drop_slot(source, new_slot)


@LOCAL
@PG
def test_slot_creation_waiting_on_a_transaction_is_waiting_and_leaves_no_slot(
    stores,
    populated,  # noqa: F811
    monkeypatch,
):
    """S6: creating a logical slot waits for running transactions; the bounded wait
    gives `waiting` (exit 0), and the cancelled creation leaves no slot."""
    source, _, ingest, log, _ = populated
    new_slot = lost(populated)
    blocker = psycopg2.connect(ingest.dsn)
    try:
        with blocker.cursor() as cur:  # an open transaction with an xid
            cur.execute("CREATE TEMP TABLE hold_xid (i int)")
            cur.execute("INSERT INTO hold_xid VALUES (1)")
        monkeypatch.setattr(epoch, "CREATE_SECONDS", 2)
        preview = go(populated, dry_run="1", **recover(ingest))["preview_id"]
        out = go(populated, preview_id=preview, **recover(ingest))
        assert out["status"] == "waiting" and "running source transactions" in out["detail"], out
        assert af.EXIT[out["status"]] == 0
        assert confirmed(source, new_slot) is None and log.position()[0] == 1
        blocker.rollback()
        fresh = go(populated, dry_run="1", **recover(ingest))["preview_id"]
        assert go(populated, preview_id=fresh, **recover(ingest))["status"] == "done"
    finally:
        blocker.close()
        drop_slot(source, new_slot)


@LOCAL
@PG
def test_a_failed_re_read_keeps_the_created_slot_and_reports_it(stores, populated):  # noqa: F811
    """M1: when the give-up can't prove the metadata didn't commit, the slot is kept."""
    source, _, ingest, log, _ = populated
    new_slot = lost(populated)
    try:

        def fault(stage):
            if stage == "before_metadata":
                log.position = lambda: (_ for _ in ()).throw(RuntimeError("MotherDuck unreachable"))
                raise RuntimeError("lost connection")

        with pytest.raises(RuntimeError, match="lost connection") as raised:
            epoch.begin_epoch(ingest, log, 2, "slot_lost", fault=fault)
        assert any(new_slot in note for note in raised.value.__notes__)
        assert confirmed(source, new_slot) is not None  # kept
        del log.position
        # The next run reuses it (nothing committed).
        assert epoch.begin_epoch(ingest, log, 2, "slot_lost").status == "begun"
    finally:
        drop_slot(source, new_slot)


@LOCAL
@PG
def test_a_held_ingest_lease_is_waiting_and_leaves_the_preview_unspent(stores, populated):  # noqa: F811
    source, _, ingest, log, _ = populated
    new_slot = lost(populated)
    try:
        preview = go(populated, dry_run="1", **recover(ingest))["preview_id"]
        holder = IngestLog(log.con, ingest)
        holder.acquire("someone-else")
        try:
            out = go(populated, preview_id=preview, wait=0, **recover(ingest))
        finally:
            holder.release()
        assert out["status"] == "waiting" and not previews(log, ingest)[0][1], out
        assert go(populated, preview_id=preview, **recover(ingest))["status"] == "done"
    finally:
        drop_slot(source, new_slot)


@LOCAL
@PG
def test_a_leftover_old_slot_is_reported_then_dropped_by_the_repeat(
    stores,
    populated,  # noqa: F811
    monkeypatch,
):
    """S1: the old slot goes only after the commit, best effort; `already` finishes it."""
    source, _, ingest, log, _ = populated
    new_slot = ingest.slot + "_e2"
    try:
        assert run_once(ingest, log).status == "drained"
        # The old slot moved ahead of the durable checkpoint: recover is allowed, and the
        # old slot still exists (it holds WAL).
        source.execute("UPDATE pairs SET v=v+1 WHERE a=0")
        query = "SELECT pg_replication_slot_advance(%s, pg_current_wal_flush_lsn())"
        with source.con.cursor() as cur:
            cur.execute(query, [ingest.slot])
        real = epoch._drop_quietly
        monkeypatch.setattr(
            epoch, "_drop_quietly", lambda con, name, database: name
        )  # a reader appeared
        preview = go(populated, dry_run="1", **recover(ingest))["preview_id"]
        done = go(populated, preview_id=preview, **recover(ingest))
        assert done["status"] == "done" and done["leftover_slot"] == ingest.slot, done
        assert confirmed(source, ingest.slot) is not None
        # The next ingest reports it for the Dive, by exact name, as `leftover`.
        observed = {}
        assert run_once(ingest, log, observe=observed).status == "drained"
        roles = {s["name"]: s.get("role") for s in observed["slots"]}
        assert roles == {new_slot: None, ingest.slot: "leftover"}, observed["slots"]
        monkeypatch.setattr(epoch, "_drop_quietly", real)
        # Through the Flight: the repeated click reports already and drops it.
        repeat = go(populated, preview_id=preview, **recover(ingest))
        assert repeat["status"] == "already" and "leftover_slot" not in repeat, repeat
        assert confirmed(source, ingest.slot) is None
    finally:
        drop_slot(source, new_slot)


@LOCAL
@PG
def test_epoch_lines_match_the_chart(stores, populated, capsys):  # noqa: F811
    source, _, ingest, log, _ = populated
    new_slot = lost(populated)
    try:
        capsys.readouterr()
        taken = set()

        def lines(**env):
            run = Run()
            run.step("validate", "ok", branch="ok")
            out = go(populated, run=run, **env)
            text = capsys.readouterr().out
            assert not drift("epoch", text, required=["validate", "decide"]), text
            taken.update(branches(text))
            return out

        assert (
            lines(**dict(recover(ingest, dry_run="1"), expected_epoch="5"))["status"] == "refused"
        )
        preview = lines(**recover(ingest, dry_run="1"))["preview_id"]
        assert lines(preview_id=str(uuid.uuid4()), **recover(ingest))["status"] == "refused"
        assert lines(preview_id=preview, **recover(ingest))["status"] == "done"
        assert taken >= {
            ("decide", "refused"),
            ("decide", "would"),
            ("decide", "act"),
            ("outcome", "refused"),
            ("outcome", "done"),
        }
    finally:
        drop_slot(source, new_slot)


def test_the_status_contract():
    assert af.EXIT["waiting"] == 0 and af.EXIT["refused"] == 2
    assert crash  # the PG harness helpers are shared with test_epoch_pg


def test_observed_slots_reports_an_exact_leftover_only(monkeypatch):
    from cdc_flight import ingest

    rows = [
        ("s_e2", True, "reserved", None, 200, 150, 300),  # current
        ("s", False, "extended", None, 100, 90, 300),  # the previous epoch's slot
        ("s_other", False, "reserved", None, 1, 1, 300),  # a prefix, not ours
        ("s_e3", False, "reserved", None, 1, 1, 300),  # pending
    ]
    monkeypatch.setattr(ingest, "query", lambda con, sql, params=None: rows)
    default = ingest.observed_slots(None, "s", "s_e2", 2)
    assert {s["name"] for s in default} == {"s_e2", "s_e3"}  # existing callers unchanged
    with_extra = ingest.observed_slots(None, "s", "s_e2", 2, extra=("s",))
    leftover = [s for s in with_extra if s.get("role") == "leftover"]
    assert [(s["name"], s["epoch"], s["retained_bytes"]) for s in leftover] == [("s", 1, 210)]
    assert "role" not in next(s for s in with_extra if s["name"] == "s_e2")


@LOCAL
@PG
@pytest.mark.parametrize(
    "reason, stage, kept",
    [
        ("slot_lost", "before_create_slot", False),
        ("slot_lost", "after_create_slot", False),  # created, nothing committed: dropped
        ("slot_lost", "before_metadata", False),
        ("slot_lost", "after_metadata", True),  # committed: kept, the retry is `already`
        ("skip", "before_advance", None),
        ("skip", "after_advance", None),
        ("skip", "before_metadata", None),
        ("skip", "after_metadata", None),
    ],
)
def test_a_crash_at_every_fault_point_leaves_a_state_the_retry_completes(
    stores,
    populated,  # noqa: F811
    reason,
    stage,
    kept,
):
    """PostgreSQL fault tests at every `fault(...)` point."""
    source, _, ingest, log, _ = populated
    new_slot = ingest.slot + "_e2"
    try:
        if reason == "slot_lost":
            lost(populated)
            target = None
        else:
            assert run_once(ingest, log).status == "drained"
            source.execute("UPDATE pairs SET v=v+1 WHERE a=0")
            target = wal_lsn(source)
        with pytest.raises(RuntimeError, match=stage):
            epoch.begin_epoch(ingest, log, 2, reason, to_lsn=target, fault=crash(stage))
        committed = stage == "after_metadata"
        assert log.position()[0] == (2 if committed else 1)
        if kept is not None:
            assert (confirmed(source, new_slot) is not None) == kept, stage
        result = epoch.begin_epoch(ingest, log, 2, reason, to_lsn=target)
        assert result.status == ("already" if committed else "begun"), (stage, result)
        expected_slot = new_slot if reason == "slot_lost" else ingest.slot
        assert log.position()[:2] == (2, expected_slot)
        if reason == "skip":
            assert confirmed(source, ingest.slot) == target
    finally:
        drop_slot(source, new_slot)


@LOCAL
@PG
def test_the_stub_uses_only_baked_settings_and_never_prints_the_secret(
    stores,
    populated,  # noqa: F811
    tmp_path,
):
    """The real stub, every declared key set to an attacker value: it connects to the
    baked change log and to PostgreSQL only through the baked secret name, dry-runs a
    recover, and no part of the DSN's password reaches its output."""
    import os
    import subprocess
    import sys

    from test_action_flight import SHIM

    source, _, ingest, log, _ = populated
    lost(populated)
    database = ingest.database
    files = {database: str(tmp_path / f"{database}.duckdb")}
    assert os.path.exists(files[database])
    for con in {id(s.con): s.con for s in stores}.values():
        con.close()
    flags = [
        "--source",
        ingest.source_id,
        "--slot",
        ingest.slot,
        "--publication",
        ingest.publication,
    ]
    flags += ["--heartbeat", ingest.heartbeat, "--database", database, "--dsn-env", "SHOP_DSN"]
    flight_py = tmp_path / "flight.py"
    flight_py.write_text(stub_for(tmp_path, "epoch", flags))
    shim, record = tmp_path / "shim.py", tmp_path / "shim.log"
    shim.write_text(
        SHIM.replace(
            "duckdb.connect = connect\n",
            "duckdb.connect = connect\n"
            "import psycopg2\n"
            "real_pg = psycopg2.connect\n"
            "def pg_connect(*a, **k):\n"
            "    LOG.write(json.dumps(['pg', k.get('dbname'), k.get('port')]) + '\\n')\n"
            "    LOG.flush()\n"
            "    return real_pg(*a, **k)\n"
            "psycopg2.connect = pg_connect\n",
        )
    )
    password = "phase0-local-only"
    assert password in ingest.dsn
    env = {
        "PATH": os.environ["PATH"],
        "MOTHERDUCK_TOKEN": "local-run-placeholder-not-a-motherduck-token",
        "SHOP_DSN": ingest.dsn + " sslmode=disable",
        "SHIM_FILES": json.dumps(files),
        "SHIM_LOG": str(record),
        **stub_env(tmp_path),
        **{k: "evil" for c in FLIGHT_CONFIG.values() for k in c},
        "CDC_DSN_ENV": "MOTHERDUCK_TOKEN",
        "CDC_DATABASE": "evil_db",
        **{k: "" for k in FLIGHT_CONFIG["epoch"]},
        "CDC_ACTION": "recover-slot",
        "CDC_EXPECTED_EPOCH": "1",
        "CDC_CONFIRM": f"RECOVER {ingest.source_id} EPOCH 2",
        "CDC_DRY_RUN": "1",
    }
    result = subprocess.run(
        [sys.executable, str(shim), str(flight_py)],
        capture_output=True,
        text=True,
        cwd=tmp_path,
        env=env,
    )
    assert_stub_ok(result.stdout)
    out = [x for x in result.stdout.splitlines() if x.startswith("{")]
    assert result.returncode == 0 and out, result.stderr[-2000:]
    assert json.loads(out[-1])["status"] == "would"
    events = [json.loads(x) for x in record.read_text().splitlines()]
    assert {e[1] for e in events if e[0] == "connect"} == {f"md:{database}?attach_mode=single"}
    assert {e[1] for e in events if e[0] == "pg"} == {"cdc_phase0"}
    assert not any("evil" in json.dumps(e) for e in events)
    assert password not in result.stdout and password not in result.stderr


@LOCAL
@PG
@pytest.mark.parametrize("stage", ["after_create_slot", "before_metadata"])
def test_the_old_slot_survives_a_crash_before_the_new_epoch_is_recorded(
    stores,
    populated,  # noqa: F811
    stage,
):
    """Until the metadata names the new slot, the old one stays: a give-up
    must never leave the source with no slot at all."""
    source, _, ingest, log, _ = populated
    new_slot = ingest.slot + "_e2"
    try:
        assert run_once(ingest, log).status == "drained"
        source.execute("UPDATE pairs SET v=v+1 WHERE a=0")
        with source.con.cursor() as cur:  # ahead of the checkpoint: recover allowed
            cur.execute(
                "SELECT pg_replication_slot_advance(%s, pg_current_wal_flush_lsn())", [ingest.slot]
            )
        with pytest.raises(RuntimeError, match=stage):
            epoch.begin_epoch(ingest, log, 2, "slot_lost", fault=crash(stage))
        assert confirmed(source, ingest.slot) is not None  # the old slot is still there
        assert confirmed(source, new_slot) is None and log.position()[0] == 1
        result = epoch.begin_epoch(ingest, log, 2, "slot_lost")
        assert result.status == "begun" and result.leftover is None
        assert confirmed(source, ingest.slot) is None  # dropped after the commit
    finally:
        drop_slot(source, new_slot)


@LOCAL
@PG
def test_a_same_named_slot_that_isnt_ours_is_never_dropped(stores, populated, monkeypatch):  # noqa: F811
    """Slot names are cluster-wide; only an inactive pgoutput slot of this
    database is dropped as the old slot."""
    source, _, ingest, log, _ = populated
    new_slot = ingest.slot + "_e2"
    try:
        assert run_once(ingest, log).status == "drained"
        source.execute("UPDATE pairs SET v=v+1 WHERE a=0")
        with source.con.cursor() as cur:
            cur.execute(
                "SELECT pg_replication_slot_advance(%s, pg_current_wal_flush_lsn())", [ingest.slot]
            )
        monkeypatch.setattr(epoch, "_drop_quietly", lambda con, name, database: name)
        preview = go(populated, dry_run="1", **recover(ingest))["preview_id"]
        assert go(populated, preview_id=preview, **recover(ingest))["leftover_slot"] == ingest.slot
        monkeypatch.undo()
        # The old slot goes; another tool's slot of the same name appears.
        drop_slot(source, ingest.slot)
        with source.con.cursor() as cur:
            cur.execute(
                "SELECT pg_create_logical_replication_slot(%s, 'test_decoding')", [ingest.slot]
            )
        repeat = go(populated, preview_id=preview, **recover(ingest))
        assert repeat["status"] == "already" and repeat["leftover_slot"] == ingest.slot, repeat
        assert confirmed(source, ingest.slot) is not None  # not ours: kept
    finally:
        drop_slot(source, new_slot)
        drop_slot(source, ingest.slot)


@LOCAL
@PG
def test_a_skip_target_beyond_the_flushed_wal_is_clamped(stores, populated):  # noqa: F811
    """Format 2: a target beyond the source's WAL is clamped to the WAL that
    exists at the click, never past it (the clamp's report and
    CAS: test_action_flights_v2). The dry run moves nothing."""
    source, _, ingest, log, _ = populated
    assert run_once(ingest, log).status == "drained"
    before, slot_at = log.position(), confirmed(source, ingest.slot)
    target = wal_lsn(source) + 2**30
    out = go(populated, dry_run="1", **skip(ingest, target))
    assert out["status"] == "would" and "current WAL" in out["detail"], out
    assert log.position() == before and confirmed(source, ingest.slot) == slot_at


def logged(log, epoch, text):
    """Whether any stored EVENT frame of `epoch` carries `text` in its new image."""
    days = [d for (d,) in log.execute("SELECT DISTINCT partition_day FROM {s}chunks_v2").fetchall()]
    return any(
        log.execute(
            f"SELECT count(*) FROM {{s}}frames_v2_{day} WHERE source_epoch=? AND record_kind=1 "
            "AND array_to_string(new.values, ' ') LIKE ?",
            [epoch, f"%{text}%"],
        ).fetchone()[0]
        for day in days
    )


def bulk(source, rows=4000):
    source.execute(
        "INSERT INTO big SELECT 1000000+i, repeat('q', 900), NULL FROM generate_series(1, %s) i",
        [rows],
    )


@LOCAL
@PG
def test_a_skip_advances_in_steps_whose_progress_persists(stores, populated, monkeypatch):  # noqa: F811
    """A cancelled advance keeps nothing, so the skip advances in bounded
    steps. A multi-MB range finishes over several steps; decoding afterwards starts
    exactly past the target."""
    source, _, ingest, log, _ = populated
    assert run_once(ingest, log).status == "drained"
    bulk(source)
    target = wal_lsn(source)
    checkpoint = log.position()[2]
    monkeypatch.setattr(epoch, "ADVANCE_STEP_BYTES", 256 * 1024)
    calls, real = [], epoch.query

    def recording(con, statement, args=None):
        if "pg_replication_slot_advance" in statement:
            calls.append(int(args[1].split("/")[0], 16) << 32 | int(args[1].split("/")[1], 16))
        return real(con, statement, args) if args is not None else real(con, statement)

    monkeypatch.setattr(epoch, "query", recording)
    preview = go(populated, dry_run="1", **skip(ingest, target))["preview_id"]
    done = go(populated, preview_id=preview, **skip(ingest, target))
    assert done["status"] == "done", done
    assert len(calls) > 3 and calls[-1] == target and calls == sorted(calls)
    assert all(b - a <= 256 * 1024 for a, b in zip([checkpoint] + calls, calls))
    assert confirmed(source, ingest.slot) == target
    monkeypatch.undo()
    source.execute("INSERT INTO pairs VALUES (77,'after-skip',1)")
    assert run_once(ingest, log).status == "drained"
    assert logged(log, 2, "after-skip") and not logged(log, 2, "qqqqqqqq")


@LOCAL
@PG
def test_an_exhausted_budget_is_waiting_with_the_progress_kept(stores, populated, monkeypatch):  # noqa: F811
    source, _, ingest, log, _ = populated
    assert run_once(ingest, log).status == "drained"
    bulk(source)
    target = wal_lsn(source)
    checkpoint = log.position()[2]
    monkeypatch.setattr(epoch, "ADVANCE_STEP_BYTES", 256 * 1024)
    ticks = iter([0, 0, 0] + [10**9] * 100)  # the deadline, two steps, then out of time
    monkeypatch.setattr(epoch, "_clock", lambda: next(ticks))
    preview = go(populated, dry_run="1", **skip(ingest, target))["preview_id"]
    out = go(populated, preview_id=preview, **skip(ingest, target))
    assert out["status"] == "waiting" and "progress is kept" in out["detail"], out
    part = confirmed(source, ingest.slot)
    assert checkpoint < part < target and log.position()[0] == 1
    monkeypatch.setattr(epoch, "_clock", __import__("time").monotonic)
    fresh = go(populated, dry_run="1", **skip(ingest, target))
    assert fresh["status"] == "would" and fresh["observed"]["slot_confirmed"] == part
    assert go(populated, preview_id=fresh["preview_id"], **skip(ingest, target))["status"] == "done"
    assert confirmed(source, ingest.slot) == target


@LOCAL
@PG
def test_a_step_that_would_overrun_the_runs_deadline_is_not_started(stores, populated, monkeypatch):  # noqa: F811
    """A step starts only if it can finish before the run's deadline (start
    + max_runtime - margin), so the run returns `waiting` and releases the lease instead
    of being killed with it held."""
    source, _, ingest, log, _ = populated
    assert run_once(ingest, log).status == "drained"
    bulk(source)
    target = wal_lsn(source)
    monkeypatch.setattr(epoch, "ADVANCE_STEP_BYTES", 256 * 1024)
    monkeypatch.setattr(epoch, "ADVANCE_STEP_SECONDS", 120)
    ticks = iter([0, 0, 50] + [50] * 100)  # the limit, step 1 at 0, then at 50: 50+120 > 150
    monkeypatch.setattr(epoch, "_clock", lambda: next(ticks))
    preview = go(populated, dry_run="1", **skip(ingest, target))["preview_id"]
    environ = {f"CDC_{k.upper()}": v for k, v in skip(ingest, target, preview_id=preview).items()}
    fresh = IngestLog(log.con, ingest)
    out = ef.perform(
        fresh,
        ingest,
        af.parse(environ, ef.EPOCH),
        run_id=str(uuid.uuid4()),
        actor="flight:test",
        owner=owner_for(ingest),
        wait=0,
        deadline=150,
    )
    assert out["status"] == "waiting" and "progress is kept" in out["detail"], out
    part = confirmed(source, ingest.slot)
    assert part is not None and part < target  # exactly one step ran
    assert holder_free(log, ingest)  # the lease was released, not left to expire


def holder_free(log, ingest):
    other = IngestLog(log.con, ingest)
    other.acquire("probe")
    other.release()
    return True


@LOCAL
@PG
def test_a_pgoutput_slot_of_another_database_is_never_dropped(stores, populated):  # noqa: F811
    """The database half of the ownership check."""
    source, _, ingest, _, _ = populated
    name = "p5b2_other_db_" + uuid.uuid4().hex[:8]
    other = psycopg2.connect(ingest.dsn.replace("dbname=cdc_phase0", "dbname=postgres"))
    other.autocommit = True
    try:
        with other.cursor() as cur:
            cur.execute("SELECT pg_create_logical_replication_slot(%s, 'pgoutput')", [name])
        assert epoch._drop_quietly(source.con, name, "cdc_phase0") == name
        assert confirmed(source, name) is not None
    finally:
        with other.cursor() as cur:
            cur.execute(
                "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
                "WHERE slot_name=%s",
                [name],
            )
        other.close()


@LOCAL
@PG
@pytest.mark.parametrize("literal", ["lowercase", "wrong epoch"])
def test_a_repeat_with_a_wrong_literal_is_refused_and_keeps_the_old_slot(
    stores,
    populated,  # noqa: F811
    monkeypatch,
    literal,
):
    """The literal is the repeat path's only mis-click guard."""
    source, _, ingest, log, _ = populated
    new_slot = ingest.slot + "_e2"
    try:
        assert run_once(ingest, log).status == "drained"
        source.execute("UPDATE pairs SET v=v+1 WHERE a=0")
        with source.con.cursor() as cur:
            cur.execute(
                "SELECT pg_replication_slot_advance(%s, pg_current_wal_flush_lsn())", [ingest.slot]
            )
        monkeypatch.setattr(epoch, "_drop_quietly", lambda con, name, database: name)
        preview = go(populated, dry_run="1", **recover(ingest))["preview_id"]
        assert go(populated, preview_id=preview, **recover(ingest))["leftover_slot"] == ingest.slot
        monkeypatch.undo()
        env = recover(ingest)
        env["confirm"] = env["confirm"].lower() if literal == "lowercase" else env["confirm"] + "0"
        out = go(populated, preview_id=preview, **env)
        assert out["status"] == "refused" and "expected epoch 1, found 2" in out["detail"], out
        assert confirmed(source, ingest.slot) is not None  # untouched
        assert go(populated, preview_id=preview, **recover(ingest))["status"] == "already"
        assert confirmed(source, ingest.slot) is None
    finally:
        drop_slot(source, new_slot)
