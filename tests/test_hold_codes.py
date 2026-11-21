"""apply_status.hold_code: a closed enum (cdc_flight.hold_codes), set where each hold is set
(docs/operations.md, Hold codes). These tests keep every code path on the enum."""

import ast
import re
from pathlib import Path

from cdc_flight import catalog_checks, mirror
from cdc_flight.hold_codes import HOLD_CODES
from cdc_flight.ingest_common import MEMBERSHIP_HOLDS, VALIDATION_HOLDS

SRC = Path(__file__).resolve().parents[1] / "src" / "cdc_flight"


def _blocked_calls():
    for path in sorted(SRC.glob("*.py")):
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and getattr(node.func, "id", None) == "Blocked":
                yield path.name, node


def _codes_of(file, node):
    """The code values a Blocked call can pass, or raise: a literal, the schema constant,
    an f"append_{op}" (op in update/delete), or a catalog reason key (by import)."""
    code = next((k.value for k in node.keywords if k.arg == "code"), None)
    assert code is not None, f"{file}:{node.lineno}: Blocked( without code="
    if isinstance(code, ast.Constant):
        return {code.value}
    if isinstance(code, ast.Name) and code.id == "SCHEMA_CHANGED":
        return {mirror.SCHEMA_CHANGED}
    if isinstance(code, ast.JoinedStr) and ast.unparse(code) == "f'append_{op}'":
        return {"append_update", "append_delete"}
    if isinstance(code, ast.Name) and code.id in ("reason", "code"):
        # apply_run's catalog holds: a catalog_checks.REASONS / APPEND_REASONS key, or the
        # next_step hold's own code.
        return set(catalog_checks.REASONS) | set(catalog_checks.APPEND_REASONS) | {"key_changed"}
    raise AssertionError(f"{file}:{node.lineno}: unrecognised code= {ast.unparse(code)}")


def test_every_blocked_call_passes_a_code_from_the_enum():
    calls = list(_blocked_calls())
    assert len(calls) >= 29, len(calls)
    for file, node in calls:
        unknown = _codes_of(file, node) - HOLD_CODES
        assert not unknown, f"{file}:{node.lineno}: {unknown} not in HOLD_CODES"


def test_the_enum_covers_every_registry_and_catalog_reason():
    assert set(MEMBERSHIP_HOLDS) <= HOLD_CODES
    assert set(VALIDATION_HOLDS) <= HOLD_CODES
    assert {"unmanaged", "removed"} <= HOLD_CODES
    assert set(catalog_checks.REASONS) <= HOLD_CODES
    assert set(catalog_checks.APPEND_REASONS) <= HOLD_CODES
    assert {"paused", "retention_expired", "not_started"} <= HOLD_CODES


def test_not_started_excludes_exactly_the_active_generation_states():
    """ops_apply._not_started hard-codes generations.ACTIVE (the apply command doesn't import
    generations): a new active state must be added there too, or a table mid-backfill
    would read as not_started."""
    from cdc_flight import generations, ops_apply

    sql = ops_apply._not_started(type("C", (), {"mirror_database": "m", "tables": ()})(), "")
    states = "(" + ",".join(f"'{s}'" for s in generations.ACTIVE) + ")"
    assert f"state IN {states}" in sql, (generations.ACTIVE, sql)


def test_blocked_requires_a_code():
    try:
        mirror.Blocked("needs_action", "x")
    except TypeError:
        return
    raise AssertionError("Blocked() without code= must fail")


# Statements on {m}apply_state, found by text (the SQL is in string literals).
WRITE = re.compile(r"(UPDATE \{m\}apply_state SET|INSERT INTO \{m\}apply_state)")


def _apply_state_writes():
    for path in sorted(SRC.glob("*.py")):
        text = path.read_text()
        tree = ast.parse(text)
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            # Every positional argument: the spec's adapters take the SQL after the owner
            # and the command (publication_commands.statement, generation_commands.inserted).
            for arg in node.args:
                literal = "".join(
                    part.value
                    for part in ast.walk(arg)
                    if isinstance(part, ast.Constant) and isinstance(part.value, str)
                )
                if WRITE.search(literal):
                    yield path.name, node.lineno, literal


def test_every_apply_state_write_that_sets_updated_at_sets_hold_code_at():
    """hold_code is reported only when hold_code_at = updated_at, so a
    statement that bumps updated_at without hold_code_at would silently NULL the code.
    `HOLD` is the shared fragment (apply.HOLD) when it is concatenated in."""
    writes = list(_apply_state_writes())
    assert len(writes) >= 12, writes
    for file, line, sql in writes:
        if "updated_at" in sql:
            assert "hold_code_at" in sql or "{hold}" in sql, f"{file}:{line}: {sql[:90]}"


# apply_status.hold_code end to end (the Harness store: 4 tables started from the log) ------

import pytest  # noqa: E402
from test_ops import ops_db, row  # noqa: E402,F401 (fixture)
from test_phase2 import Harness  # noqa: E402

from cdc_flight import ops_apply  # noqa: E402
from cdc_flight.mirror import Blocked  # noqa: E402


def status(con, h):
    ops_apply.report_apply(con, h.config, "r", [])
    return {r["table_name"]: (r["state"], r["hold_code"]) for r in row(con, "apply_status")}


def lease(h, rel):
    return h.apply.acquire(rel, "t", token=h.apply.state(rel)["token"])


def hold(h, table, blocked):
    rel = h.apply.registered(table)[0]
    token = lease(h, rel)
    h.apply.hold(rel, token, h.apply.state(rel), blocked)
    h.apply.release(rel, token)
    return rel


def set_state(h, rel, **kwargs):
    token = lease(h, rel)
    with h.apply.transaction():
        h.apply.guard(rel, token)
        assert h.apply.set_state(rel, token, h.apply.state(rel), **kwargs)
    h.apply.release(rel, token)


@pytest.fixture
def harness(stores, tmp_path):
    return Harness(stores, tmp_path)


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_each_hold_family_reports_its_code_and_live_clears_it(harness, ops_db):  # noqa: F811
    h, con = harness, ops_db
    assert set(status(con, h).values()) == {("live", None)}
    rel = hold(h, "public.items", Blocked("needs_action", "x", code=mirror.SCHEMA_CHANGED))
    hold(h, "public.other", Blocked("needs_resync", "source epoch changed", code="epoch_changed"))
    third = h.apply.registered("public.third")[0]
    set_state(h, third, state="paused", detail="paused by t", code="paused")
    got = status(con, h)
    assert got["items"] == ("needs_action", "schema_changed")
    assert got["other"] == ("needs_resync", "epoch_changed")
    assert got["third"] == ("paused", "paused")
    # resume / retry: back to live, the code cleared in the same statement
    set_state(h, third, state="live")
    set_state(h, rel, state="live")
    got = status(con, h)
    assert got["third"] == ("live", None) and got["items"] == ("live", None)
    # A registry hold wins over the mirror's state (the log no longer ingests the table).
    con.execute(f"UPDATE {h.log.prefix}tables SET held_reason='dropped' WHERE relation_id=?", [rel])
    assert status(con, h)["items"] == ("live", "dropped")


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_an_older_writers_state_change_never_shows_a_stale_code(harness, ops_db):  # noqa: F811
    """The code is reported only while it was written
    by the statement that set the current state (hold_code_at = updated_at)."""
    h, con, m = harness, ops_db, f"{harness.apply.mirror}._cdc."
    rel = h.apply.registered("public.items")[0]
    set_state(h, rel, state="paused", detail="p", code="paused")
    # (a) an older build resumes: its UPDATE sets state/detail/updated_at only.
    con.execute(
        f"UPDATE {m}apply_state SET state='live',detail=NULL,"
        "updated_at=current_timestamp+INTERVAL 1 SECOND WHERE relation_id=?",
        [rel],
    )
    # (This passes on the state<>'live' gate alone; the next assertion tests the rule.)
    assert status(con, h)["items"] == ("live", None)
    # ... and an older build's hold: the case the hold_code_at = updated_at rule exists
    # for. Held, but its code is unknown (NULL), never "paused".
    con.execute(
        f"UPDATE {m}apply_state SET state='needs_action',detail='old',"
        "updated_at=current_timestamp+INTERVAL 2 SECOND WHERE relation_id=?",
        [rel],
    )
    assert status(con, h)["items"] == ("needs_action", None)
    # (b) an older build's INSERT (no hold columns), then a new hold: the code.
    other = h.apply.registered("public.other")[0]
    con.execute(
        f"UPDATE {m}apply_state SET hold_code=NULL,hold_code_at=NULL WHERE relation_id=?", [other]
    )
    hold(h, "public.other", Blocked("needs_action", "x", code="poison"))
    assert status(con, h)["other"] == ("needs_action", "poison")
    # (c) the lease's own writes (acquire, guard, release) keep a held row's code.
    token = lease(h, other)
    with h.apply.transaction():
        h.apply.guard(other, token)
    h.apply.release(other, token)
    assert status(con, h)["other"] == ("needs_action", "poison")


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_registered_tables_without_a_generation_get_a_not_started_row(harness, ops_db):  # noqa: F811
    h, con, m = harness, ops_db, f"{harness.apply.mirror}._cdc."
    ids = {t: h.apply.registered(f"public.{t}")[0] for t in ("items", "other", "third", "fourth")}
    for t in ("other", "third", "fourth"):
        con.execute(f"DELETE FROM {m}apply_state WHERE relation_id=?", [ids[t]])
    # other: removed; third: an active first backfill; fourth: registered, never started.
    con.execute(
        f"UPDATE {h.log.prefix}tables SET held_reason='removed' WHERE relation_id=?",
        [ids["other"]],
    )
    con.execute(
        f"INSERT INTO {m}generations (generation_id,relation_id,mode,epoch,pin_lsn,state) "
        "VALUES ('g3',?,'snapshot',1,0,'catching_up')",
        [ids["third"]],
    )
    got = status(con, h)
    assert got == {"items": ("live", None), "fourth": ("not_started", "not_started")}
    fourth = [r for r in row(con, "apply_status") if r["table_name"] == "fourth"][0]
    assert fourth["capture_mode"] is None and fourth["schema_name"] == "public"
    # An unmanaged registry row never qualifies either.
    con.execute(
        f"UPDATE {h.log.prefix}tables SET held_reason='unmanaged' WHERE relation_id=?",
        [ids["fourth"]],
    )
    con.execute("DELETE FROM cdc_ops.main.apply_status")
    assert "fourth" not in status(con, h)
    # A refused first backfill (its generation ended) returns the table to not_started.
    con.execute(f"UPDATE {m}generations SET state='failed' WHERE generation_id='g3'")
    assert status(con, h)["third"] == ("not_started", "not_started")


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_the_heartbeat_and_unconfigured_tables_never_get_a_not_started_row(
    harness,
    ops_db,  # noqa: F811
    monkeypatch,
):
    """A deploy bug: the demo's public.md_cdc_heartbeat (registered, never captured)
    read as not_started. not_started covers apply's own table set only (table_set)."""
    h, con, m = harness, ops_db, f"{harness.apply.mirror}._cdc."
    heartbeat = con.execute(
        f"SELECT json_extract_string(binding,'$.heartbeat') FROM {h.log.prefix}source WHERE id=1"
    ).fetchone()[0]
    assert heartbeat, "the harness binding names a heartbeat"
    schema, name = heartbeat.split(".")
    con.execute(
        f"INSERT INTO {h.log.prefix}tables (relation_id,schema_name,table_name) VALUES (900,?,?)",
        [schema, name],
    )
    fourth = h.apply.registered("public.fourth")[0]
    con.execute(f"DELETE FROM {m}apply_state WHERE relation_id=?", [fourth])
    got = status(con, h)
    assert name not in got and got["fourth"] == ("not_started", "not_started")
    # CDC_TABLES set without public.fourth: it reads not_configured in the run, never
    # not_started here. A full snapshot no longer writes it; the daily prune deletes it.
    from dataclasses import replace

    h.config = replace(h.config, tables=("public.items", "public.other", "public.third"))
    monkeypatch.setattr(ops_apply, "prune_window", lambda: True)
    ops_apply.report_apply(con, h.config, "r2", [])
    rows = {r["table_name"]: r for r in row(con, "apply_status")}
    assert "fourth" not in rows and name not in rows
    assert {r["run_id"] for r in rows.values()} == {"r2"}
