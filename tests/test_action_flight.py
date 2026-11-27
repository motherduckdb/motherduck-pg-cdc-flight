"""Action Flights (action_flight): the table-action Flight.

Local DuckDB on the Phase 2 harness, with a fake trigger in place of MD_RUN_FLIGHT; the
built stub runs in a subprocess whose md: connections are mapped to the same local
files. No real Flight is created.
"""

import json
import os
import subprocess
import sys
import uuid
from datetime import timedelta

import pytest
from flight_stub import (
    BACKFILL_FLIGHT,
    COMPACT_FLIGHT,
    assert_stub_ok,
    build_flight,
    stub_env,
    stub_for,
)
from test_explainer import branches, drift
from test_phase2 import OTHER, Harness, relation_message
from test_table_actions import LOCAL, OTHER_T, act, caught_up_items, poisoned, row

from cdc_flight import action_flight as af
from cdc_flight.chain import FLIGHT_CONFIG
from cdc_flight.config import lsn_text
from cdc_flight.flightlog import Run

ITEMS_T = "public.items"
# Settings only the owner may set: never declared by an action Flight, never passed
# through a trigger (and the compaction keys).
OWNER_ONLY = {
    "CDC_SOURCE_ID",
    "CDC_TABLES",
    "CDC_DATABASE",
    "CDC_MIRROR_DATABASE",
    "CDC_OPS_DATABASE",
    "CDC_EPOCH",
    "CDC_RUNTIME_SECONDS",
    "CDC_DSN_ENV",
    "CDC_COMPACT_EXPIRE",
    "CDC_MAX_RETAINED_LAG_HOURS",
    "CDC_LOG_STORAGE_BUDGET_GB",
}


def owner(h):
    return af.Owner(
        h.config.source_id,
        BACKFILL_FLIGHT,
        COMPACT_FLIGHT,
        database=h.config.database,
        mirror_database=h.config.mirror_database,
    )


class Trigger:
    def __init__(self, error=None):
        self.calls, self.error = [], error

    def __call__(self, target, overrides):
        if self.error:
            raise self.error
        self.calls.append((target, dict(overrides)))
        return str(uuid.uuid4()), 7


def go(h, trigger=None, run=None, run_id=None, **env):
    """One table-action run on the harness: env keys are CDC_<NAME> without the prefix."""
    environ = {f"CDC_{k.upper()}": v for k, v in env.items()}
    return af.perform(
        h.apply,
        af.parse(environ),
        run_id=run_id or str(uuid.uuid4()),
        actor="flight:test",
        owner=owner(h),
        trigger=trigger or Trigger(),
        run=run,
    )


def dry(h, **env):
    out = go(h, dry_run="1", **env)
    assert out["status"] == "would", out
    return out["preview_id"]


def previews(h):
    return h.store.con.execute(
        f"SELECT id,action,used_at IS NOT NULL,used_by,actor FROM {h.apply.mirror}._cdc.previews "
        "ORDER BY created_at"
    ).fetchall()


def runs_rows(h):
    return h.store.con.execute(
        f"SELECT relation_id,status,detail FROM {h.apply.mirror}._cdc.runs "
        "WHERE status IN ('pause','resume','retry','resync','resync-all') ORDER BY updated_at"
    ).fetchall()


def mirror_sql(h, sql, params=()):
    return h.store.con.execute(sql.replace("{m}", f"{h.apply.mirror}._cdc."), list(params))


def rel(h, table):
    return h.apply.registered(table)[0]


# Validation (no connection) -------------------------------------------------------

REFUSALS = [
    (dict(ACTION="drop_everything"), "CDC_ACTION must be one of"),
    (dict(ACTION="pause", EXPECTED_STATE="live"), "CDC_TABLE is required"),
    (dict(ACTION="pause", TABLE=ITEMS_T), "CDC_EXPECTED_STATE is required"),
    (dict(ACTION="retry", TABLE=ITEMS_T), "CDC_EXPECTED_BLOCKED_LSN is required"),
    (
        dict(ACTION="pause", TABLE="public.items; DROP TABLE x", EXPECTED_STATE="live"),
        "CDC_TABLE must be",
    ),
    (dict(ACTION="pause", TABLE='public."it"', EXPECTED_STATE="live"), "CDC_TABLE must be"),
    (dict(ACTION="pause", TABLE="items", EXPECTED_STATE="live"), "CDC_TABLE must be"),
    (dict(ACTION="pause", TABLE=ITEMS_T, EXPECTED_STATE="gone'--"), "CDC_EXPECTED_STATE must"),
    (dict(ACTION="retry", TABLE=ITEMS_T, EXPECTED_BLOCKED_LSN="zz/1"), "BLOCKED_LSN must"),
    (dict(ACTION="retry", TABLE=ITEMS_T, EXPECTED_BLOCKED_LSN="None"), "BLOCKED_LSN must"),
    (dict(ACTION="resync-all", TABLE=ITEMS_T), "CDC_TABLE is not used by resync-all"),
    (
        dict(ACTION="pause", TABLE=ITEMS_T, EXPECTED_STATE="live", EXPECTED_BLOCKED_LSN="0/1"),
        "CDC_EXPECTED_BLOCKED_LSN is not used by pause",
    ),
    (dict(ACTION="compact-now", DRY_RUN="yes"), "CDC_DRY_RUN must be 0 or 1"),
    (dict(ACTION="compact-now"), "CDC_PREVIEW_ID is required"),  # blank config is real
    (dict(ACTION="compact-now", DRY_RUN="0"), "CDC_PREVIEW_ID is required"),
    (dict(ACTION="compact-now", PREVIEW_ID="not-a-uuid'"), "CDC_PREVIEW_ID must be"),
    (
        dict(ACTION="compact-now", DRY_RUN="1", PREVIEW_ID=str(uuid.uuid4())),
        "CDC_PREVIEW_ID is not used by a dry run",
    ),
    # A refused-for-input run writes no audit row (it exits before connecting), so an
    # invalid request id is never echoed anywhere; the Dive sees a run with no audit row.
    (dict(ACTION="compact-now", DRY_RUN="1", REQUEST_ID="a b"), "CDC_REQUEST_ID must be"),
    (dict(ACTION="compact-now", DRY_RUN="1", REQUEST_ID="x'--"), "CDC_REQUEST_ID must be"),
    (dict(ACTION="compact-now", DRY_RUN="1", REQUEST_ID="r" * 65), "CDC_REQUEST_ID must be"),
]


@pytest.mark.parametrize("env, message", REFUSALS)
def test_a_bad_input_is_refused_before_connecting_naming_the_key_not_the_value(
    env, message, monkeypatch, capsys
):
    def no_connection(*args, **kwargs):
        raise AssertionError("connected before validating")

    monkeypatch.setattr(af.duckdb, "connect", no_connection)
    owner_ = af.Owner("probe", BACKFILL_FLIGHT, COMPACT_FLIGHT)
    environ = {f"CDC_{k}": v for k, v in env.items()}
    assert af.main(owner_, environ) == 2
    out = capsys.readouterr().out
    result = json.loads(out.splitlines()[-1])
    assert result["status"] == "refused" and message in result["detail"], result
    for value in environ.values():
        if value not in ("", "1", "0", "pause", "retry", "compact-now", "resync-all", ITEMS_T):
            assert value not in out, value
    assert not drift("table-action", out, required=["validate", "invalid"])


def test_valid_inputs_parse_to_the_library_arguments():
    inputs = af.parse(
        {
            "CDC_ACTION": "retry",
            "CDC_TABLE": ITEMS_T,
            "CDC_EXPECTED_BLOCKED_LSN": "0/5a",
            "CDC_DRY_RUN": "1",
        }
    )
    assert inputs.values == {"expected_blocked_lsn": 0x5A} and inputs.dry_run
    none = af.parse(
        {
            "CDC_ACTION": "retry",
            "CDC_TABLE": ITEMS_T,
            "CDC_EXPECTED_BLOCKED_LSN": "none",
            "CDC_PREVIEW_ID": str(uuid.UUID(int=5)).upper(),
        }
    )
    assert none.values == {"expected_blocked_lsn": None}
    assert none.preview_id == str(uuid.UUID(int=5))


def test_the_request_id_is_optional_and_never_part_of_the_preview():
    base = {"CDC_ACTION": "compact-now", "CDC_DRY_RUN": "1"}
    assert af.parse(base).request_id is None
    for good in ("a", "dive-2026_11-10", "Z" * 64):
        assert af.parse({**base, "CDC_REQUEST_ID": good}).request_id == good
    # A real run carries its own request id, not its dry run's: the hash ignores it.
    assert af.preview_hash(af.parse({**base, "CDC_REQUEST_ID": "one"}), {}) == af.preview_hash(
        af.parse(base), {}
    )


def test_the_hash_is_canonical_and_covers_inputs_and_decision_fields():
    def parsed(lsn, table=ITEMS_T):
        return af.parse(
            {
                "CDC_ACTION": "retry",
                "CDC_TABLE": table,
                "CDC_EXPECTED_BLOCKED_LSN": lsn,
                "CDC_DRY_RUN": "1",
            }
        )

    decided = dict(state="needs_action", generation="g", epoch=1, blocked=0x5A)
    base = af.preview_hash(parsed("0/5a"), decided)
    assert af.preview_hash(parsed("0/5A"), decided) == base
    assert af.preview_hash(parsed("0/5B"), decided) != base
    assert af.preview_hash(parsed("0/5a", OTHER_T), decided) != base
    for name in decided:
        assert af.preview_hash(parsed("0/5a"), {**decided, name: "moved"}) != base, name


def test_the_decision_fields_are_the_reviewed_table():
    """`retry`'s blocked commit is also checked by the library's stale guard,
    so only this pins it in the hash."""
    assert af.DECISION_FIELDS == {
        "pause": ("state", "generation", "epoch", "format_version", "generation_id"),
        "resume": ("state", "generation", "epoch", "format_version", "generation_id"),
        "retry": ("state", "generation", "epoch", "blocked", *af.UNIT_FIELDS),
        "resync": (
            "state",
            "generation",
            "active_generation",
            "format_version",
            "generation_id",
            "capture_mode",
            "ignore_deletes",
        ),
        "resync-all": ("needs_resync",),
        "start-from-log": ("capture_mode", "generation", "format_version"),
        "start-snapshot": ("generation", "active_generation", "capture_mode", "source_held"),
        "accept-types": ("state", "generation", "epoch", "blocked", *af.UNIT_FIELDS, "type_digest"),
        "compact-now": (),
        "quarantine": ("state", "generation", "epoch", "blocked", *af.UNIT_FIELDS),
    }


def test_the_token_and_checkpoint_are_shown_but_never_hashed():
    for action, names in af.DECISION_FIELDS.items():
        assert not {"token", "applied", "detail"} & set(names), action


# The Flight's declared keys and the owner's settings ----------------------------------


def test_the_flight_declares_only_the_action_inputs():
    keys = set(FLIGHT_CONFIG["table-action"])
    assert keys == {
        "CDC_ACTION",
        "CDC_TABLE",
        "CDC_EXPECTED_STATE",
        "CDC_EXPECTED_BLOCKED_LSN",
        "CDC_CONFIRM",  # accept-types' real run
        "CDC_DRY_RUN",
        "CDC_PREVIEW_ID",
        "CDC_REQUEST_ID",
    }
    actions = ("table-action", "table-membership", "quarantine", "epoch", "delete-source")
    pipeline = {k for c, ks in FLIGHT_CONFIG.items() if c not in actions for k in ks}
    assert not keys & pipeline and not keys & OWNER_ONLY
    assert set(af.KEYS) == keys
    assert {k for ks in af.USES.values() for k in ks} <= keys


def test_owner_settings_are_validated():
    good = dict(source_id="shop", backfill_flight=BACKFILL_FLIGHT, compact_flight=COMPACT_FLIGHT)
    owner_ = af.Owner.from_dict(good)
    assert owner_.mirror_database == "shop_mirror" and owner_.database == "cdc_internal"
    for bad in (
        dict(good, source_id="Shop"),
        dict(good, source_id="shop; drop"),
        dict(good, backfill_flight="not-a-uuid"),
        dict(good, compact_flight=""),
        dict(good, mirror_database="cdc_internal"),  # mirror and log must differ
        dict(good, ops_database="ops-db"),
        dict(good, ops_database="cdc_internal"),  # snapshot/audit never hit a store
        dict(good, ops_database="shop_mirror"),
    ):
        with pytest.raises(ValueError):
            af.Owner.from_dict(bad)
    with pytest.raises(ValueError, match="unknown owner settings"):
        af.Owner.from_dict(dict(good, tables="public.items"))


def test_the_build_bakes_owner_settings_and_refuses_them_elsewhere(tmp_path):
    stub = stub_for(tmp_path, "table-action")
    owner_line = [line for line in stub.splitlines() if line.startswith("OWNER = ")]
    assert len(owner_line) == 1
    assert json.loads(eval(owner_line[0].removeprefix("OWNER = "))) == dict(
        source_id="probe",
        database="cdc_internal",
        mirror_database="probe_mirror",
        ops_database="cdc_ops",
        backfill_flight=BACKFILL_FLIGHT,
        compact_flight=COMPACT_FLIGHT,
    )
    with pytest.raises(ValueError, match="needs owner settings"):
        build_flight.stub("table-action", "0" * 40, "0.1.0")
    with pytest.raises(ValueError, match="not owner settings"):
        build_flight.stub("apply", "0" * 40, "0.1.0", dict(source_id="probe"))
    flights = ["--backfill-flight", BACKFILL_FLIGHT, "--compact-flight", COMPACT_FLIGHT]
    for flags in (
        ["--source", "Probe", *flights],
        ["--source", "probe", "--backfill-flight", "x", "--compact-flight", COMPACT_FLIGHT],
        ["--source", "probe", "--backfill-flight", BACKFILL_FLIGHT],  # no compact Flight
        flights,  # no source
    ):
        with pytest.raises((TypeError, ValueError)):
            stub_for(tmp_path, "table-action", flags)


# The preview lifecycle --------------------------------------------------------------


@LOCAL
def test_a_dry_run_writes_only_its_preview_and_takes_no_lease(stores, tmp_path):
    h = Harness(stores, tmp_path)
    caught_up_items(h)
    before = row(h, ITEMS_T)
    runs = mirror_sql(h, "SELECT count(*) FROM {m}runs").fetchone()
    run = Run()
    out = go(h, run=run, action="pause", table=ITEMS_T, expected_state="live", dry_run="1")
    assert out["status"] == "would" and out["preview_id"] == out["run_id"]
    assert out["confirm"] is None  # pause takes no confirmation
    assert out["observed"]["token"] == before[3]  # shown for the Dive, never hashed
    assert row(h, ITEMS_T) == before  # no lease: the token is unchanged
    assert mirror_sql(h, "SELECT count(*) FROM {m}runs").fetchone() == runs
    assert previews(h) == [(out["preview_id"], "pause", False, None, None)]


@LOCAL
def test_a_dry_run_prunes_previews_older_than_a_day_and_survives_a_failed_prune(
    stores, tmp_path, monkeypatch
):
    h = Harness(stores, tmp_path)
    caught_up_items(h)
    env = dict(action="pause", table=ITEMS_T, expected_state="live")
    old, kept = dry(h, **env), dry(h, **env)
    mirror_sql(
        h,
        "UPDATE {m}previews SET created_at=created_at-INTERVAL 25 HOUR WHERE id=?",
        [old],
    )
    newest = dry(h, **env)
    assert [p[0] for p in previews(h)] == [kept, newest]
    real = h.apply.execute

    def conflicting(statement, parameters=None):
        if statement.startswith("DELETE FROM {m}previews"):
            raise af.duckdb.TransactionException("write-write conflict")
        return real(statement, parameters)

    monkeypatch.setattr(h.apply, "execute", conflicting)
    survivor = dry(h, **env)  # the prune's conflict never fails the dry run
    monkeypatch.undo()
    assert survivor in [p[0] for p in previews(h)]


@LOCAL
def test_a_preview_is_spent_once_by_the_matching_real_run(stores, tmp_path):
    h = Harness(stores, tmp_path)
    caught_up_items(h)
    preview = dry(h, action="pause", table=ITEMS_T, expected_state="live")
    real = dict(action="pause", table=ITEMS_T, expected_state="live", preview_id=preview)
    out = go(h, run_id=(run_id := str(uuid.uuid4())), **real)
    assert out["status"] == "done", out
    assert row(h, ITEMS_T)[0] == "paused"
    assert previews(h) == [(preview, "pause", True, run_id, "flight:test")]
    assert runs_rows(h)[-1][1:] == ("pause", "pause by flight:test")
    # resume it, then try the spent preview again: refused, nothing written.
    assert act(h, "resume", ITEMS_T).status == "done"
    again = go(h, **real)
    assert again["status"] == "refused" and again["detail"] == af.NO_PREVIEW
    assert row(h, ITEMS_T)[0] == "live"


@LOCAL
def test_a_real_run_with_another_request_id_still_spends_its_preview(stores, tmp_path):
    h = Harness(stores, tmp_path)
    caught_up_items(h)
    env = dict(action="pause", table=ITEMS_T, expected_state="live")
    preview = dry(h, request_id="dive-dry-1", **env)
    out = go(h, preview_id=preview, request_id="dive-real-2", **env)
    assert out["status"] == "done", out
    assert previews(h)[0][:3] == (preview, "pause", True)


@LOCAL
def test_an_unknown_old_or_mismatched_preview_is_refused(stores, tmp_path):
    h = Harness(stores, tmp_path)
    caught_up_items(h)
    pause = dict(action="pause", table=ITEMS_T, expected_state="live")
    unknown = go(h, preview_id=str(uuid.uuid4()), **pause)
    assert unknown["status"] == "refused" and unknown["detail"] == af.NO_PREVIEW
    # Previewed for one action, used for another with the same inputs.
    preview = dry(h, **pause)
    other = dict(pause, action="resync")
    assert go(h, preview_id=preview, **other)["detail"] == af.NO_PREVIEW
    # Previewed for one table, used for another.
    assert act(h, "resume", ITEMS_T).status == "already"
    w = h.wire
    rel_other = relation_message(2, "other", OTHER, identity="d")
    h.ingest(w.txn(rel_other, w.insert(2, ("1", "t", "x"))))
    assert h.run("other").status == "caught_up"
    wrong = go(h, preview_id=preview, **dict(pause, table=OTHER_T))
    assert wrong["status"] == "refused" and wrong["detail"] == af.NO_PREVIEW
    # Older than 15 minutes.
    mirror_sql(
        h,
        "UPDATE {m}previews SET created_at=created_at-?::INTERVAL WHERE id=?",
        [timedelta(minutes=af.PREVIEW_MINUTES, seconds=1), preview],
    )
    old = go(h, preview_id=preview, **pause)
    assert old["status"] == "refused" and old["detail"] == af.NO_PREVIEW
    assert row(h, ITEMS_T)[0] == "live"
    assert all(not used for _, _, used, _, _ in previews(h))


@LOCAL
def test_an_apply_run_between_the_dry_and_real_runs_does_not_spend_the_preview(stores, tmp_path):
    """The token moves on every apply run; it isn't a decision field."""
    h = Harness(stores, tmp_path)
    caught_up_items(h)
    preview = dry(h, action="pause", table=ITEMS_T, expected_state="live")
    token = row(h, ITEMS_T)[3]
    w = h.wire
    h.ingest(w.txn(w.insert(1, ("2", "b", "t", "2.00"))))
    assert h.run("items").status == "caught_up"
    assert row(h, ITEMS_T)[3] != token
    out = go(h, action="pause", table=ITEMS_T, expected_state="live", preview_id=preview)
    assert out["status"] == "done", out


@LOCAL
@pytest.mark.parametrize(
    "action, column, value",
    [
        ("pause", "epoch", 2),
        ("pause", "generation_id", "moved"),
        ("resume", "generation_id", "moved"),
        ("retry", "generation_id", "moved"),
        ("retry", "epoch", 2),
    ],
)
def test_a_moved_decision_field_refuses_and_rolls_back(stores, tmp_path, action, column, value):
    h = Harness(stores, tmp_path)
    caught_up_items(h)
    if action == "resume":
        assert act(h, "pause", ITEMS_T).status == "done"
        env = dict(action="resume", table=ITEMS_T, expected_state="paused")
        table = ITEMS_T
    elif action == "retry":
        poison = poisoned(h)
        env = dict(action="retry", table=OTHER_T, expected_blocked_lsn=lsn_text(poison))
        table = OTHER_T
    else:
        env = dict(action="pause", table=ITEMS_T, expected_state="live")
        table = ITEMS_T
    preview = dry(h, **env)
    mirror_sql(
        h, f"UPDATE {{m}}apply_state SET {column}=? WHERE relation_id=?", [value, rel(h, table)]
    )
    before = row(h, table, "state,detail,blocked_commit_lsn"), runs_rows(h)
    out = go(h, preview_id=preview, **env)
    # Format 2: a retry whose epoch moved finds no complete unit at its blocked start in
    # that epoch, so it is refused as a state change before the preview check; every
    # other move fails the preview's hash.
    if (action, column) == ("retry", "epoch"):
        assert out["status"] == "refused" and out["detail"].startswith("state changed"), out
    else:
        assert out["status"] == "refused" and out["detail"] == af.NO_PREVIEW, out
    assert (row(h, table, "state,detail,blocked_commit_lsn"), runs_rows(h)) == before
    assert not previews(h)[0][2]


@LOCAL
def test_retry_and_resume_spend_their_previews(stores, tmp_path):
    h = Harness(stores, tmp_path)
    caught_up_items(h)
    poison = poisoned(h)
    env = dict(action="retry", table=OTHER_T, expected_blocked_lsn=lsn_text(poison).lower())
    preview = dry(h, **env)
    assert go(h, preview_id=preview, **env)["status"] == "done"
    assert row(h, OTHER_T)[0] == "live"
    assert act(h, "pause", ITEMS_T).status == "done"
    env = dict(action="resume", table=ITEMS_T, expected_state="paused")
    assert go(h, preview_id=dry(h, **env), **env)["status"] == "done"
    assert [r[1] for r in runs_rows(h)][-3:] == ["retry", "pause", "resume"]


@LOCAL
def test_a_failure_after_the_consume_rolls_back_the_preview_state_and_audit(
    stores, tmp_path, monkeypatch
):
    h = Harness(stores, tmp_path)
    caught_up_items(h)
    env = dict(action="pause", table=ITEMS_T, expected_state="live")
    preview = dry(h, **env)

    def broken(*args, **kwargs):
        raise RuntimeError("fault after the consume")

    monkeypatch.setattr(h.apply, "set_state", broken)
    with pytest.raises(RuntimeError, match="fault after the consume"):
        go(h, preview_id=preview, **env)
    monkeypatch.undo()
    assert row(h, ITEMS_T)[0] == "live"
    assert not previews(h)[0][2] and not runs_rows(h)
    # The unspent preview still works once the fault is gone.
    assert go(h, preview_id=preview, **env)["status"] == "done"


@LOCAL
def test_busy_leaves_the_preview_unspent(stores, tmp_path):
    h = Harness(stores, tmp_path)
    caught_up_items(h)
    env = dict(action="pause", table=ITEMS_T, expected_state="live")
    preview = dry(h, **env)
    token = h.apply.acquire(rel(h, ITEMS_T), "other-holder")
    try:
        assert go(h, preview_id=preview, **env)["status"] == "busy"
    finally:
        h.apply.release(rel(h, ITEMS_T), token)
    assert not previews(h)[0][2]
    assert go(h, preview_id=preview, **env)["status"] == "done"


# Triggers ----------------------------------------------------------------------------


def start_generation(h, table):
    mirror_sql(
        h,
        "CREATE TABLE IF NOT EXISTS {m}backfills "
        "(relation_id UINTEGER PRIMARY KEY, generation_id VARCHAR NOT NULL)",
    )
    mirror_sql(h, "INSERT INTO {m}backfills VALUES (?, 'g-active')", [rel(h, table)])


def needs_resync(h, table, hours_ago):
    mirror_sql(
        h,
        "UPDATE {m}apply_state SET state='needs_resync',"
        "held_since=current_timestamp-?::INTERVAL WHERE relation_id=?",
        [timedelta(hours=hours_ago), rel(h, table)],
    )


@LOCAL
@pytest.mark.parametrize("state", ["live", "paused", "needs_action", "needs_resync"])
def test_resync_triggers_the_baked_backfill_flight_with_exactly_the_built_config(
    stores, tmp_path, state
):
    h = Harness(stores, tmp_path)
    caught_up_items(h)
    table = ITEMS_T
    if state == "paused":
        assert act(h, "pause", table).status == "done"
    elif state == "needs_action":
        poisoned(h)
        table = OTHER_T
    elif state == "needs_resync":
        needs_resync(h, table, 1)
    env = dict(action="resync", table=table, expected_state=state)
    would = go(h, dry_run="1", **env)
    assert would["status"] == "would" and af.RESYNC_COST[state] in would["detail"]
    trigger = Trigger()
    out = go(h, trigger=trigger, preview_id=would["preview_id"], **env)
    assert out["status"] == "done", out
    expected = {"CDC_BACKFILL_TABLE": table, "CDC_BACKFILL_START": "1"}
    assert trigger.calls == [(BACKFILL_FLIGHT, expected)]
    passed = set(trigger.calls[0][1])
    assert passed <= set(FLIGHT_CONFIG["backfill"]) and not passed & OWNER_ONLY
    assert out["triggered"]["flight"] == "backfill" and out["triggered"]["run_number"] == 7
    assert runs_rows(h)[-1][1:] == ("resync", "resync by flight:test")
    assert previews(h)[-1][2]


@LOCAL
def test_resync_is_refused_while_a_backfill_runs_or_the_state_moved(stores, tmp_path):
    h = Harness(stores, tmp_path)
    caught_up_items(h)
    trigger = Trigger()
    env = dict(action="resync", table=ITEMS_T, expected_state="live")
    preview = dry(h, **env)
    start_generation(h, ITEMS_T)
    running = go(h, trigger=trigger, preview_id=preview, **env)
    assert running["status"] == "refused" and "already running" in running["detail"]
    assert go(h, dry_run="1", **env)["status"] == "refused"
    stale = go(h, trigger=trigger, preview_id=preview, **dict(env, expected_state="paused"))
    assert stale["status"] == "refused" and "state changed" in stale["detail"]
    assert trigger.calls == [] and not previews(h)[0][2]


@LOCAL
def test_a_failed_trigger_reports_the_spent_preview(stores, tmp_path, capsys):
    h = Harness(stores, tmp_path)
    caught_up_items(h)
    env = dict(action="resync", table=ITEMS_T, expected_state="live")
    preview = dry(h, **env)
    capsys.readouterr()
    out = go(h, trigger=Trigger(RuntimeError("no")), run=Run(), preview_id=preview, **env)
    assert out["status"] == "failed" and "preview is spent" in out["detail"]
    assert af.EXIT.get(out["status"], 1) == 1
    assert previews(h)[0][2] and runs_rows(h)[-1][1] == "resync"
    assert not drift("table-action", capsys.readouterr().out, required=["claim", "trigger"])


@LOCAL
def test_resync_all_starts_one_backfill_for_the_longest_held_table(stores, tmp_path):
    h = Harness(stores, tmp_path)
    caught_up_items(h)
    tables = ["public.items", "public.other", "public.third"]
    for hours, table in zip((1, 3, 2), tables):
        needs_resync(h, table, hours)
    trigger = Trigger()
    would = go(h, dry_run="1", action="resync-all")
    assert would["table"] == "public.other" and would["remaining"] == 2
    out = go(h, trigger=trigger, action="resync-all", preview_id=would["preview_id"])
    assert out["status"] == "done" and out["remaining"] == 2
    assert out["next_steps"] == ["resync-all again"]
    assert trigger.calls == [
        (BACKFILL_FLIGHT, {"CDC_BACKFILL_TABLE": "public.other", "CDC_BACKFILL_START": "1"})
    ]
    assert runs_rows(h)[-1][:2] == (rel(h, "public.other"), "resync-all")
    # "again" while that generation runs picks the next table.
    start_generation(h, "public.other")
    again = go(h, dry_run="1", action="resync-all")
    assert again["table"] == "public.third" and again["remaining"] == 1
    # A preview from before another table changed no longer matches.
    needs_resync(h, "public.fourth", 5)
    late = go(h, trigger=trigger, action="resync-all", preview_id=again["preview_id"])
    assert late["status"] == "refused" and len(trigger.calls) == 1


@LOCAL
def test_resync_all_with_nothing_to_do_is_already(stores, tmp_path):
    h = Harness(stores, tmp_path)
    caught_up_items(h)
    out = go(h, dry_run="1", action="resync-all")
    assert out["status"] == "already" and out["detail"] == "no table needs a resync"
    needs_resync(h, ITEMS_T, 1)
    start_generation(h, ITEMS_T)
    assert go(h, dry_run="1", action="resync-all")["status"] == "already"
    assert previews(h) == []


@LOCAL
def test_compact_now_triggers_the_baked_compaction_with_no_config(stores, tmp_path):
    h = Harness(stores, tmp_path)
    caught_up_items(h)
    trigger = Trigger()
    preview = dry(h, action="compact-now")
    out = go(h, trigger=trigger, action="compact-now", preview_id=preview, run_id="r-1")
    assert out["status"] == "done" and trigger.calls == [(COMPACT_FLIGHT, {})]
    # The spent preview is compact-now's audit: who and which run.
    assert previews(h) == [(preview, "compact-now", True, "r-1", "flight:test")]
    assert go(h, trigger=trigger, action="compact-now", preview_id=preview)["status"] == "refused"


# Audit and snapshot ------------------------------------------------------------------


class FakeCon:
    def __init__(self, fail=False):
        self.statements, self.fail = [], fail

    def execute(self, statement, *args):
        self.statements.append(statement)
        if self.fail:
            raise RuntimeError("no snapshot")


def finish_with(status, written, *, con=None, audit=None):
    con = con or FakeCon()
    calls = []

    def fake_audit(*args, **kwargs):
        calls.append((args, kwargs))
        return written

    inputs = af.parse({"CDC_ACTION": "compact-now", "CDC_DRY_RUN": "1"})
    out = dict(status=status, run_id="r-1")
    owner_ = af.Owner("shop", BACKFILL_FLIGHT, COMPACT_FLIGHT, ops_database="ops")
    af.finish(con, owner_, inputs, out, Run(), audit=audit or fake_audit)
    return con.statements, calls


@pytest.mark.parametrize("status", ["done", "would", "already", "refused", "busy", "failed"])
def test_the_snapshot_runs_once_after_every_written_audit_copy(status):
    """The Dive reads a dry run's preview id and a refusal from cdc_ops too."""
    statements, calls = finish_with(status, True)
    assert statements == ['CREATE SNAPSHOT OF "ops"']
    assert finish_with(status, False)[0] == []  # no audit copy (no cdc_ops): no snapshot
    ((args, kwargs),) = calls
    assert args[1:6] == (
        "cdc_internal",
        "shop",
        "r-1",
        "compact-now",
        {"table": None, "preview_id": None},
    )
    assert kwargs == {"database": "ops", "started_by": None, "tls": None, "request_id": None}


def test_no_operations_database_means_no_snapshot_and_failures_never_raise():
    assert finish_with("done", False)[0] == []  # write_audit skipped: no cdc_ops

    def broken(*args, **kwargs):
        raise RuntimeError("audit down")

    assert finish_with("done", True, audit=broken)[0] == []
    con = FakeCon(fail=True)
    finish_with("done", True, con=con)  # the snapshot failing doesn't raise
    assert len(con.statements) == 1


# Explainer --------------------------------------------------------------------------


@LOCAL
def test_table_action_lines_match_the_chart(stores, tmp_path, capsys):
    h = Harness(stores, tmp_path)
    caught_up_items(h)
    capsys.readouterr()
    taken = set()

    def lines(**env):
        run = Run()
        run.step("validate", "ok", branch="ok")
        out = go(h, run=run, **env)
        text = capsys.readouterr().out
        assert not drift("table-action", text, required=["validate", "decide"]), text
        taken.update(branches(text))
        return out

    pause = dict(action="pause", table=ITEMS_T, expected_state="live")
    preview = lines(dry_run="1", **pause)["preview_id"]
    token = h.apply.acquire(rel(h, ITEMS_T), "other-holder")
    assert lines(preview_id=preview, **pause)["status"] == "busy"
    h.apply.release(rel(h, ITEMS_T), token)
    assert lines(preview_id=preview, **pause)["status"] == "done"
    already = dict(pause, expected_state="paused")
    assert lines(preview_id=preview, **already)["status"] == "already"
    resume = dict(action="resume", table=ITEMS_T, expected_state="paused")
    assert lines(preview_id=preview, **resume)["status"] == "refused"
    assert lines(dry_run="1", **dict(pause, expected_state="needs_action"))["status"] == "refused"
    compact = lines(dry_run="1", action="compact-now")["preview_id"]
    assert lines(action="compact-now", preview_id=compact)["status"] == "done"
    assert lines(dry_run="1", action="resync-all")["status"] == "already"
    assert taken >= {
        ("decide", "would"),
        ("decide", "act"),
        ("decide", "trigger"),
        ("decide", "refused"),
        ("decide", "already"),
        ("outcome", "busy"),
        ("outcome", "done"),
        ("outcome", "refused"),
        ("outcome", "trigger"),
    }


# The CLI and the Flight agree ----------------------------------------------------------


@LOCAL
def test_the_cli_and_the_flight_give_the_same_status_for_the_same_state(stores, tmp_path):
    h = Harness(stores, tmp_path)
    caught_up_items(h)
    poisoned(h)
    for action, env in (
        ("pause", dict(table=OTHER_T, expected_state="needs_action")),
        ("resume", dict(table=ITEMS_T, expected_state="live")),
        ("retry", dict(table=ITEMS_T, expected_blocked_lsn="none")),
    ):
        flight = go(h, dry_run="1", action=action, **env)
        library_inputs = af.parse(
            {f"CDC_{k.upper()}": v for k, v in env.items()}
            | {"CDC_ACTION": action, "CDC_DRY_RUN": "1"}
        ).values
        cli = act(h, action, env["table"], dry_run=True, **library_inputs)
        assert (flight["status"], flight["detail"]) == (cli.status, cli.detail), action


# The built stub, in a subprocess ------------------------------------------------------

# Maps the Flight's md: connections to the harness's local files and records every
# connection, ATTACH and MD_RUN_FLIGHT, so the test sees exactly what it touched.
SHIM = """
import json, os, re, runpy, sys
import duckdb

FILES = json.loads(os.environ["SHIM_FILES"])
LOG = open(os.environ["SHIM_LOG"], "a")
real_connect = duckdb.connect


class Con:
    def __init__(self, con):
        self.con = con

    def execute(self, statement, parameters=None):
        attach = re.fullmatch(r"ATTACH IF NOT EXISTS 'md:(\\w+)' AS \\"(\\w+)\\"", statement)
        if attach:
            LOG.write(json.dumps(["attach", attach.group(1)]) + "\\n")
            statement = f"ATTACH IF NOT EXISTS '{FILES[attach.group(1)]}' AS \\"{attach.group(2)}\\""
        if "MD_RUN_FLIGHT" in statement:
            LOG.write(json.dumps(["run_flight", parameters]) + "\\n")
            return self.con.execute("SELECT uuid()::VARCHAR, 3")
        if statement.startswith("CREATE SNAPSHOT"):
            LOG.write(json.dumps(["snapshot", statement]) + "\\n")
            return None
        return self.con.execute(statement, parameters or [])

    def __getattr__(self, name):
        return getattr(self.con, name)


def connect(address, *args, **kwargs):
    LOG.write(json.dumps(["connect", address]) + "\\n")
    LOG.flush()
    name = re.fullmatch(r"md:(\\w+)\\?attach_mode=single", address).group(1)
    return Con(real_connect(FILES[name]))


duckdb.connect = connect
runpy.run_path(sys.argv[1], run_name="__main__")
"""


@LOCAL
def test_the_stub_acts_only_on_baked_settings_whatever_the_config(stores, tmp_path):
    """Every key any Flight declares is set to an attacker value; the stub still reads
    only its inputs and touches only the baked databases and Flights."""
    h = Harness(stores, tmp_path)
    caught_up_items(h)
    database, mirror = h.config.database, h.config.mirror_database
    files = {
        database: str(tmp_path / f"{database}.duckdb"),
        mirror: str(tmp_path / f"{mirror}.duckdb"),
    }
    for con in {id(s.con): s.con for s in stores}.values():
        con.close()
    flags = ["--source", "phase2", "--database", database, "--mirror", mirror]
    flags += ["--backfill-flight", BACKFILL_FLIGHT, "--compact-flight", COMPACT_FLIGHT]
    flight_py = tmp_path / "flight.py"
    flight_py.write_text(stub_for(tmp_path, "table-action", flags))
    shim, log = tmp_path / "shim.py", tmp_path / "shim.log"
    shim.write_text(SHIM)
    attacker = {k: "evil" for c in FLIGHT_CONFIG.values() for k in c}
    attacker.update(
        CDC_TABLES='["public.other"]',
        CDC_MIRROR_DATABASE="evil_mirror",
        CDC_DATABASE="evil_db",
        CDC_SOURCE_ID="evil",
        CDC_COMPACT_EXPIRE="1",
    )

    def flight(**inputs):
        env = {
            "PATH": os.environ["PATH"],
            "MOTHERDUCK_TOKEN": "local-run-placeholder-not-a-motherduck-token",
            "MOTHERDUCK_FLIGHT_RUN_ID": str(uuid.uuid4()),
            "SHIM_FILES": json.dumps(files),
            "SHIM_LOG": str(log),
            **stub_env(tmp_path),
            **attacker,
            **{k: "" for k in FLIGHT_CONFIG["table-action"]},
            **{f"CDC_{k.upper()}": v for k, v in inputs.items()},
        }
        result = subprocess.run(
            [sys.executable, str(shim), str(flight_py)],
            capture_output=True,
            text=True,
            cwd=tmp_path,
            env=env,
        )
        assert_stub_ok(result.stdout)
        out = [line for line in result.stdout.splitlines() if line.startswith("{")]
        assert out, result.stderr[-2000:]
        return result.returncode, json.loads(out[-1]), env["MOTHERDUCK_FLIGHT_RUN_ID"]

    code, would, run_id = flight(action="compact-now", dry_run="1")
    assert code == 0 and would["status"] == "would" and would["preview_id"] == run_id
    code, done, _ = flight(action="compact-now", preview_id=run_id)
    assert code == 0 and done["status"] == "done", done
    pause = dict(action="pause", table=ITEMS_T, expected_state="live")
    code, would, run_id = flight(dry_run="1", **pause)
    assert code == 0 and would["status"] == "would"
    code, done, _ = flight(preview_id=run_id, **pause)
    assert code == 0 and done["status"] == "done", done
    code, refused, _ = flight(preview_id=run_id, **pause)
    assert code == 2 and refused["status"] == "refused"
    events = [json.loads(line) for line in log.read_text().splitlines()]
    assert {tuple(e) for e in events if e[0] == "connect"} == {
        ("connect", f"md:{mirror}?attach_mode=single")
    }
    assert {e[1] for e in events if e[0] == "attach"} <= {database, "cdc_ops"}
    flights = [e[1] for e in events if e[0] == "run_flight"]
    assert flights == [[COMPACT_FLIGHT, [], []]]
    assert not any("evil" in json.dumps(e) for e in events)


# Each action Flight's own finish (the name its main calls) carries the parsed request id
# into its audit copy, dry run and real run alike.
def _flight_requests():
    from cdc_flight import delete_source_flight as ds
    from cdc_flight import epoch_flight as ef
    from cdc_flight import membership_flight as mf
    from cdc_flight import quarantine_flight as qf

    pid = str(uuid.uuid4())
    return [
        (af, af.TABLE_ACTION, {"CDC_ACTION": "compact-now"}),
        (mf, mf.MEMBERSHIP, {"CDC_ACTION": "remove", "CDC_TABLE": ITEMS_T}),
        (
            af,
            qf.QUARANTINE,
            {
                "CDC_ACTION": "quarantine",
                "CDC_TABLE": ITEMS_T,
                "CDC_EXPECTED_EPOCH": "1",
                "CDC_CONFIRM": "x",
            },
        ),
        (
            ef,
            ef.EPOCH,
            {"CDC_ACTION": "recover-slot", "CDC_EXPECTED_EPOCH": "1", "CDC_CONFIRM": "x"},
        ),
        (
            ds,
            ds.DELETE,
            {"CDC_ACTION": "delete-source", "CDC_EXPECTED_EPOCH": "1", "CDC_CONFIRM": "x"},
        ),
    ], pid


@pytest.mark.parametrize("index", range(5))
@pytest.mark.parametrize("dry_run", [True, False])
def test_every_action_flight_audits_its_request_id(index, dry_run):
    flights, pid = _flight_requests()
    module, spec, environ = flights[index]
    environ = dict(environ, CDC_REQUEST_ID=f"req-{index}")
    environ.update({"CDC_DRY_RUN": "1"} if dry_run else {"CDC_PREVIEW_ID": pid})
    inputs = af.parse(environ, spec)
    calls = []

    def audit(*args, **kwargs):
        calls.append(kwargs)
        return False  # no snapshot

    owner_ = af.Owner("probe", BACKFILL_FLIGHT, COMPACT_FLIGHT, ops_database="ops")
    module.finish(None, owner_, inputs, dict(status="would", run_id="r"), Run(), audit=audit)
    assert [c["request_id"] for c in calls] == [f"req-{index}"]


# start-snapshot (one-click first snapshot) -------------------------------------------


def not_started(h, relation_id=4):
    """`fourth` registered with no generation yet (as add-table leaves a table)."""
    h.store.con.execute(
        f"DELETE FROM {h.apply.mirror}._cdc.apply_state WHERE relation_id={relation_id}; "
        f"DELETE FROM {h.apply.mirror}._cdc.generations WHERE relation_id={relation_id}"
    )


@LOCAL
def test_start_snapshot_triggers_the_first_backfill_of_a_not_started_table(stores, tmp_path):
    h = Harness(stores, tmp_path)
    not_started(h)
    env = dict(action="start-snapshot", table="public.fourth")
    would = go(h, dry_run="1", **env)
    assert would["status"] == "would" and "first snapshot" in would["detail"], would
    trigger = Trigger()
    out = go(h, trigger=trigger, preview_id=would["preview_id"], **env)
    assert out["status"] == "done", out
    assert trigger.calls == [
        (BACKFILL_FLIGHT, {"CDC_BACKFILL_TABLE": "public.fourth", "CDC_BACKFILL_START": "1"})
    ]
    assert out["triggered"]["flight"] == "backfill" and out["triggered"]["run_number"] == 7
    status = mirror_sql(
        h, "SELECT status,detail FROM {m}runs WHERE relation_id=4 AND status='start-snapshot'"
    ).fetchall()
    assert status == [("start-snapshot", "start-snapshot by flight:test")]
    assert previews(h)[-1][2]  # spent


@LOCAL
def test_start_snapshot_is_already_for_a_started_table_and_refused_when_held(stores, tmp_path):
    h = Harness(stores, tmp_path)
    started = go(h, dry_run="1", action="start-snapshot", table=ITEMS_T)
    assert started["status"] == "already" and "use resync" in started["detail"]
    not_started(h)
    h.log.execute("UPDATE {s}tables SET held_reason='unpublished' WHERE relation_id=4")
    held = go(h, dry_run="1", action="start-snapshot", table="public.fourth")
    assert held["status"] == "refused" and "held (unpublished)" in held["detail"]


@LOCAL
def test_a_start_snapshot_preview_goes_stale_when_a_generation_appears(stores, tmp_path):
    h = Harness(stores, tmp_path)
    not_started(h)
    env = dict(action="start-snapshot", table="public.fourth")
    preview = dry(h, **env)
    h.apply.start_from_log("public.fourth", accept_incomplete=True)
    trigger = Trigger()
    out = go(h, trigger=trigger, preview_id=preview, **env)
    assert out["status"] == "already" and trigger.calls == [], out


@LOCAL
def test_the_old_seven_key_config_still_runs_every_existing_action(stores, tmp_path):
    """The live Dive sends table-action's 7 keys from before CDC_CONFIRM: an absent key
    reads as blank (dive-d3)."""
    h = Harness(stores, tmp_path)
    caught_up_items(h)
    environ = {
        "CDC_ACTION": "pause",
        "CDC_TABLE": ITEMS_T,
        "CDC_EXPECTED_STATE": "live",
        "CDC_EXPECTED_BLOCKED_LSN": "",
        "CDC_DRY_RUN": "1",
        "CDC_PREVIEW_ID": "",
        "CDC_REQUEST_ID": "",
    }
    assert "CDC_CONFIRM" not in environ
    out = af.perform(
        h.apply,
        af.parse(environ),
        run_id=str(uuid.uuid4()),
        actor="flight:test",
        owner=owner(h),
        trigger=Trigger(),
    )
    assert out["status"] == "would", out


def test_accept_types_takes_a_confirm_and_no_other_action_does():
    assert af.parse({"CDC_ACTION": "accept-types", "CDC_TABLE": ITEMS_T, "CDC_DRY_RUN": "1"})
    with pytest.raises(af.InputError, match="CDC_CONFIRM is not used by pause"):
        af.parse(
            {
                "CDC_ACTION": "pause",
                "CDC_TABLE": ITEMS_T,
                "CDC_EXPECTED_STATE": "live",
                "CDC_CONFIRM": "x",
                "CDC_DRY_RUN": "1",
            }
        )


@LOCAL
def test_accept_types_real_run_spends_the_preview_of_a_dry_run_without_the_confirmation(
    stores, tmp_path
):
    """The documented flow: the dry run (no CDC_CONFIRM) returns the literal, and the real
    run sends it with the dry run's preview id. The literal isn't hashed: its digest and
    the type_digest decision field already bind the change."""
    from test_table_actions import retyped_items, stored_type

    h = Harness(stores, tmp_path)
    retyped_items(h)
    out = go(h, action="accept-types", table=ITEMS_T, dry_run="1")
    assert out["status"] == "would", out
    text = out["observed"]["confirm"]
    assert out["confirm"] == text  # top level too; observed keeps it for older readers
    wrong = go(
        h,
        action="accept-types",
        table=ITEMS_T,
        confirm=f"ACCEPT TYPES {ITEMS_T} 000000000000",
        preview_id=out["preview_id"],
    )
    assert wrong["status"] == "refused" and wrong["detail"] == f"type exactly: {text}", wrong
    assert previews(h)[0][2] is False  # a wrong literal leaves the preview unspent
    done = go(
        h,
        action="accept-types",
        table=ITEMS_T,
        confirm=text,
        preview_id=out["preview_id"],
        run_id=(run_id := str(uuid.uuid4())),
    )
    assert done["status"] == "done", done
    assert stored_type(h, ITEMS_T, "value") == [1043, 24]
    assert previews(h) == [(out["preview_id"], "accept-types", True, run_id, "flight:test")]
    again = go(h, action="accept-types", table=ITEMS_T, confirm=text, preview_id=out["preview_id"])
    # Accepted: no type hold is left (and the preview is spent).
    assert again["status"] == "refused", again


def test_only_accept_types_leaves_the_confirmation_out_of_the_preview_hash():
    def parsed(environ):
        return af.parse({"CDC_TABLE": ITEMS_T, **environ})

    dry = parsed({"CDC_ACTION": "accept-types", "CDC_DRY_RUN": "1"})
    real = parsed(
        {
            "CDC_ACTION": "accept-types",
            "CDC_CONFIRM": "ACCEPT TYPES public.items abc",
            "CDC_PREVIEW_ID": str(uuid.uuid4()),
        }
    )
    decided = dict(type_digest="abc")
    assert af.preview_hash(dry, decided) == af.preview_hash(real, decided)
    # The typed-first literals stay hashed: their real run repeats the dry run's text.
    from cdc_flight.quarantine_flight import QUARANTINE

    q = dict(CDC_ACTION="quarantine", CDC_TABLE=ITEMS_T, CDC_EXPECTED_EPOCH="1")
    one = af.base.parse({**q, "CDC_CONFIRM": "a", "CDC_DRY_RUN": "1"}, QUARANTINE)
    two = af.base.parse({**q, "CDC_CONFIRM": "b", "CDC_DRY_RUN": "1"}, QUARANTINE)
    assert af.preview_hash(one, {}) != af.preview_hash(two, {})
