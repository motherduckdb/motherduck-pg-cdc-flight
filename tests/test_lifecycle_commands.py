"""The source lifecycle, the bulk epoch transition and retention pins
through lifecycle_spec, with negative canaries. Judges are plain functions that raise
AssertionError when the property fails; each canary seeds its fault and asserts the judge
then fails. Local DuckDB (test_ingest_log2's log2, test_retention2's retained); the delete
saga's PostgreSQL paths are test_delete_source's."""

import ast
import re
import sys
from dataclasses import replace
from pathlib import Path

import duckdb
import pytest
from test_ingest_log2 import log2  # noqa: F401 (fixture)
from test_publication_commands import statements
from test_retention2 import retained  # noqa: F401 (fixture)

from cdc_flight import expiry, ingest_log2
from cdc_flight import lifecycle_commands as lc
from cdc_flight import lifecycle_spec as spec
from cdc_flight import lifecycle_spec_text as text
from cdc_flight.ingest_common import FenceLost
from cdc_flight.lifecycle_spec import IllegalTransition, InvariantViolation

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
import build_flight  # noqa: E402

SRC = ROOT / "src" / "cdc_flight"

# The legal lifecycle edges, by hand and independent of lifecycle_spec (canary 1).
EDGES = {
    "register": ((None,), "registered"),
    "record_created": (("registered",), None),
    "delete_start": (("registered",), "deleting"),
    "delete_finish": (("deleting",), "deleted"),
    "begin_epoch": (("registered",), None),
    "pin_transfer": (("registered",), None),
    "pin_expire": (("registered",), None),
    "pin_advance": (("registered",), None),
    "pin_release": (("registered",), None),
}


def sources():
    return {p.stem: p.read_text() for p in sorted(SRC.glob("*.py"))}


def writes(table):
    return re.compile(r"\b(INSERT\s+INTO|UPDATE|DELETE\s+FROM)\s+[^\s(]*?\b%s\b(?!_)" % table, re.I)


# Judges ---------------------------------------------------------------------------------


def judge_edges():
    actual = {n: (tuple(c.from_states), c.to_state) for n, c in spec.COMMANDS.items()}
    assert actual == EDGES, "lifecycle_spec's edges differ from the independent table"


def judge_exact():
    """The epoch's bulk UPDATE must touch every registry row: one short is an
    InvariantViolation, never a commit."""
    with pytest.raises(InvariantViolation):
        lc.checked("begin_epoch", "tables_update", 2, expected=3)
    assert lc.checked("begin_epoch", "tables_update", 3, expected=3) == 3


def judge_listed(found, listed, what):
    counts = {k: len(v) for k, v in found.items()}
    expected = {k: n for k, (_, n) in listed.items()}
    unlisted = {k: v for k, v in counts.items() if k not in expected}
    stale = {k: v for k, v in expected.items() if k not in counts}
    assert not unlisted, f"{what} writes missing from lifecycle_spec_text: {unlisted}"
    assert not stale, f"{what} entries with no site: {stale}"
    assert counts == expected, (counts, expected)


def column_writers(found, column):
    """{site: statements assigning {s}source.<column>} (INSERT column list or SET list)."""
    writers = {}
    for site, texts in found.items():
        for statement in texts:
            head = statement.upper().split(" WHERE ")[0]
            if not head.lstrip().startswith(("UPDATE", "INSERT")):
                continue
            if head.lstrip().startswith("INSERT"):
                head = head.split("VALUES")[0]
            if re.search(r"[\s,(]%s\s*[=,)]" % column.upper(), head):
                writers[site] = writers.get(site, 0) + 1
    return writers


def judge_source_columns(found):
    for column, expected in text.SOURCE_COLUMN_WRITERS.items():
        assert column_writers(found, column) == dict(expected), column


def judge_tables_inserts(found):
    """Pins the SITES of {s}tables INSERTs, not their guards (judge_insert_guards checks
    that each writes the source row first)."""
    inserts = {
        site
        for site, texts in found.items()
        for s in texts
        if s.lstrip().upper().startswith("INSERT")
    }
    assert inserts == set(text.TABLES_INSERT_SITES), inserts


def _function(tree, qualname):
    node = tree
    for part in qualname.split("."):
        node = next(
            n
            for n in ast.iter_child_nodes(node)
            if isinstance(n, (ast.FunctionDef, ast.ClassDef)) and n.name == part
        )
    return node


def _calls(node, attribute):
    return [
        n.lineno
        for n in ast.walk(node)
        if isinstance(n, ast.Call)
        and isinstance(n.func, ast.Attribute)
        and n.func.attr == attribute
    ]


def _literal_line(node, needle):
    return min(
        n.lineno
        for n in ast.walk(node)
        if isinstance(n, ast.Constant) and isinstance(n.value, str) and needle in n.value
    )


def judge_insert_guards(sources_):
    """begin_epoch's premise, per site: each {s}tables INSERT writes the source row first in
    its transaction. register: the source INSERT before the registry INSERT; record_tables:
    guard() before its INSERT; activate_member: every caller guards before calling it;
    _activate_append: only activate_member calls it."""
    common = ast.parse(sources_["ingest_common"])
    register = _function(common, "LogBase.register")
    assert _literal_line(register, "INSERT INTO {s}source") < _literal_line(
        register, "INSERT INTO {s}tables"
    ), "register: the source INSERT must precede the registry INSERT"
    record = _function(common, "LogBase.record_tables")
    guards = _calls(record, "guard")
    assert guards and min(guards) < _literal_line(record, "INSERT INTO {s}tables"), (
        "record_tables: guard() must precede its registry INSERT"
    )
    functions = [
        (module, fn)
        for module, source in sources_.items()
        for fn in ast.walk(ast.parse(source))
        if isinstance(fn, ast.FunctionDef)
    ]

    def name_calls(fn, name):  # plain-name calls: f(...)
        return [
            n.lineno
            for n in ast.walk(fn)
            if isinstance(n, ast.Call) and isinstance(n.func, ast.Name) and n.func.id == name
        ]

    def guarded_before(fn, lines):
        guards = _calls(fn, "guard")
        return bool(guards) and min(guards) < min(lines)

    for module, fn in functions:
        calls = _calls(fn, "activate_member")
        if calls and fn.name != "activate_member" and not guarded_before(fn, calls):
            # Inside its caller's guarded transaction (membership._add): every caller of
            # this function must guard before calling it.
            callers = [(m, f) for m, f in functions if name_calls(f, fn.name)]
            assert callers, f"{module}:{fn.name}: guard() must precede activate_member"
            for caller_module, caller in callers:
                assert guarded_before(caller, name_calls(caller, fn.name)), (
                    f"{caller_module}:{caller.name}: guard() must precede {fn.name} "
                    "(which calls activate_member)"
                )
        if _calls(fn, "_activate_append"):
            assert fn.name == "activate_member", f"{module}:{fn.name} calls _activate_append"


def judge_expire_rolls_back(retention):
    """The plan's text: a table pin whose registry row is missing -> the delete rolls back
    (InvariantViolation), the pin stays."""
    retention.register_generation(101, "g", 1, 90)
    assert retention.transfer(101, "g", 1, 90)
    retention.execute("DELETE FROM {s}tables WHERE relation_id=101")  # a damaged store
    with pytest.raises(InvariantViolation):
        retention.expire("table:101", "g", 1, 90)
    assert retention.execute(
        "SELECT count(*) FROM {s}retention_pins WHERE owner='table:101'"
    ).fetchone() == (1,)


# Tests: the spec and its prose ----------------------------------------------------------


def test_every_command_and_statement_has_its_text():
    assert set(text.TEXT) == set(spec.COMMANDS)
    assert set(text.WHY) == {(c, s) for c, cmd in spec.COMMANDS.items() for s in cmd.rows}


def test_no_flight_command_imports_the_prose_module():
    """Design property: the prose is for tests and docs; the commands import only the
    data module."""
    from authority_scan import reach

    for command in build_flight.ENTRY:
        assert "lifecycle_spec_text" not in reach(command), command


def test_lease_takers_declare_their_release():
    for name in ("register", "record_created", "delete_start", "delete_finish", "begin_epoch"):
        assert spec.command(name).leases, name


def test_spec_edges_match_the_independent_table():
    judge_edges()


@pytest.mark.parametrize("name", sorted(EDGES))
@pytest.mark.parametrize("state", (None, *spec.LIFECYCLES))
def test_python_check_refuses_exactly_the_illegal_states(name, state):
    if state in EDGES[name][0]:
        spec.check(name, state)
    else:
        with pytest.raises(IllegalTransition):
            spec.check(name, state)


@pytest.mark.parametrize("name", ["delete_start", "delete_finish"])
def test_the_lifecycle_predicate_refuses_illegal_states_in_sql(name):
    con = duckdb.connect()
    con.execute("CREATE TABLE source (id INTEGER, lifecycle VARCHAR)")
    predicate = spec.lifecycle_sql(spec.command(name).from_states)
    for state in spec.LIFECYCLES:
        con.execute("DELETE FROM source")
        stored = None if state == "registered" else state
        con.execute("INSERT INTO source VALUES (1, ?)", [stored])
        (count,) = con.execute(
            f"UPDATE source SET lifecycle='x' WHERE id=1 AND {predicate}"
        ).fetchone()
        assert count == (1 if state in EDGES[name][0] else 0), (name, state)


def test_count_kinds():
    judge_exact()
    with pytest.raises(ValueError, match="expected count"):
        lc.checked("begin_epoch", "tables_update", 3)
    assert lc.checked("pin_transfer", "pins_delete", 4) == 4  # at least one
    with pytest.raises(InvariantViolation):
        lc.checked("pin_transfer", "pins_delete", 0)
    assert lc.checked("pin_release", "pin_delete", 0) == 0  # any count
    with pytest.raises(FenceLost):
        lc.checked("delete_start", "guard", 0, lease_error=FenceLost)
    with pytest.raises(InvariantViolation):
        lc.checked("delete_start", "lifecycle_update", 0)


# Tests: inventories (static) -------------------------------------------------------------


@pytest.mark.parametrize(
    "table, listed",
    [
        ("source", text.SOURCE),
        ("source_epochs", text.SOURCE_EPOCHS),
        ("tables", text.TABLES),
        ("retention_pins", text.PINS),
    ],
)
def test_every_write_is_listed_both_ways(table, listed):
    judge_listed(statements(sources(), writes(table)), listed, table)


def test_only_the_pinned_statements_assign_lifecycle_epoch_and_slot():
    judge_source_columns(statements(sources(), writes("source")))


def test_the_registry_insert_sites_are_exactly_the_guarded_four():
    judge_tables_inserts(statements(sources(), writes("tables")))


def test_every_registry_insert_writes_the_source_row_first():
    judge_insert_guards(sources())


# Tests: the epoch (local log) -------------------------------------------------------------


def epoch_state(log):
    return (
        log.execute("SELECT epoch FROM {s}source WHERE id=1").fetchone(),
        log.execute("SELECT count(*) FROM {s}source_epochs").fetchone(),
        log.execute("SELECT count(*) FROM {s}key_changes").fetchone(),
        log.execute("SELECT relation_id,key_version,state FROM {s}tables ORDER BY 1").fetchall(),
    )


def test_the_epoch_moves_every_row_or_rolls_back(log2, monkeypatch):  # noqa: F811
    """One registry row short (seeded: the expected count one higher) -> InvariantViolation,
    and the epoch, its row, the key changes and the registry are all unchanged."""
    statement = lc.statement

    def short(owner, caller, name, sql, params=(), **kw):
        if name == "tables_update":
            kw["expected"] = kw["expected"] + 1
        return statement(owner, caller, name, sql, params, **kw)

    monkeypatch.setattr(lc, "statement", short)
    before = epoch_state(log2)
    with pytest.raises(InvariantViolation, match="begin_epoch/tables_update"):
        log2.begin_epoch(2, 200, "skip", "epoch_2")
    assert epoch_state(log2) == before
    monkeypatch.undo()
    assert log2.begin_epoch(2, 200, "skip", "epoch_2") is True
    assert log2.begin_epoch(2, 200, "skip", "epoch_2") is False  # a repeat


def test_an_expired_source_lease_nobody_took_never_begins_an_epoch(log2):  # noqa: F811
    log2.execute(
        "UPDATE {s}source SET expires_at=current_timestamp-INTERVAL '60 second' WHERE id=1"
    )
    before = epoch_state(log2)
    with pytest.raises(FenceLost):
        log2.begin_epoch(2, 200, "skip", "epoch_2")
    assert epoch_state(log2) == before


def test_a_registry_insert_racing_the_epoch_conflicts_and_no_row_is_left_behind(log2):  # noqa: F811
    """begin_epoch's premise: a registry INSERT writes the source row first (as
    record_tables' guard does), so it conflicts with the epoch's guard and the epoch can't
    commit around it; after the retry every row is at the new epoch."""
    other = log2.con.cursor()
    p = log2.prefix
    other.execute("BEGIN")
    other.execute(f"UPDATE {p}source SET guard=uuid()::VARCHAR WHERE id=1")
    other.execute(
        f"INSERT INTO {p}tables (relation_id,schema_name,table_name,key_columns,state,"
        "key_version) VALUES (202,'public','late','[\"id\"]','snapshotting',0)"
    )
    with pytest.raises(Exception, match="(?i)conflict"):
        log2.begin_epoch(2, 200, "skip", "epoch_2")
    other.execute("COMMIT")
    assert log2.begin_epoch(2, 200, "skip", "epoch_2") is True
    epochs = log2.execute("SELECT DISTINCT key_epoch FROM {s}tables").fetchall()
    assert epochs == [(2,)]


def test_a_registry_insert_inside_an_open_epoch_conflicts(log2):  # noqa: F811
    """The order that could miss a row: begin_epoch's transaction is
    open (its fault point, before commit) when another writer guards and inserts a registry
    row. The other writer conflicts on the source row; the epoch commits with every row."""
    other = log2.con.cursor()
    p = log2.prefix
    seen = []

    def interleave(stage):
        if stage != "before_epoch_commit":
            return
        other.execute("BEGIN")
        try:
            other.execute(f"UPDATE {p}source SET guard=uuid()::VARCHAR WHERE id=1")
            other.execute(
                f"INSERT INTO {p}tables (relation_id,schema_name,table_name,key_columns,"
                "state,key_version) VALUES (202,'public','late','[\"id\"]','snapshotting',0)"
            )
            other.execute("COMMIT")
        except duckdb.Error as exc:
            seen.append(str(exc))
            other.execute("ROLLBACK")

    assert log2.begin_epoch(2, 200, "skip", "epoch_2", fault=interleave) is True
    assert seen and "conflict" in seen[0].lower(), seen
    rows = log2.execute("SELECT relation_id,key_epoch FROM {s}tables ORDER BY 1").fetchall()
    assert rows and all(epoch == 2 for _, epoch in rows), rows
    assert 202 not in {rel for rel, _ in rows}


def test_record_created_on_a_missing_source_row_is_an_invariant_violation(log2):  # noqa: F811
    log2.record_created({"slot": True})  # count 1
    log2.execute("DELETE FROM {s}source WHERE id=1")  # a damaged store
    with pytest.raises(InvariantViolation, match="record_created/created_update"):
        log2.record_created({"slot": True})


# Tests: pins (local retention) ------------------------------------------------------------


def test_expire_rolls_the_delete_back_if_the_state_update_misses(retained):  # noqa: F811
    judge_expire_rolls_back(retained[0])


def test_expiry_isolates_an_invariant_violation_per_pin(retained):  # noqa: F811
    retention = retained[0]
    retention.register_generation(101, "g", 1, 90)
    assert retention.transfer(101, "g", 1, 90)
    retention.execute("DELETE FROM {s}tables WHERE relation_id=101")
    assert expiry._expire(retention, "table:101", "g", 1, 90) == "invariant"
    assert retention.execute(
        "SELECT count(*) FROM {s}retention_pins WHERE owner='table:101'"
    ).fetchone() == (1,)


def test_transfer_rolls_the_put_back_if_the_state_update_misses(retained):  # noqa: F811
    retention = retained[0]
    retention.register_generation(101, "g", 1, 90)
    retention.execute("DELETE FROM {s}tables WHERE relation_id=101")
    with pytest.raises(InvariantViolation, match="pin_transfer/pin_state_update"):
        retention.transfer(101, "g", 1, 90)
    pins = retention.execute("SELECT owner FROM {s}retention_pins ORDER BY 1").fetchall()
    assert pins == [("generation:g",)]  # no table pin; the generation pin kept


def test_transfer_and_expire_repeat_safely(retained):  # noqa: F811
    retention = retained[0]
    retention.register_generation(101, "g", 1, 90)
    assert retention.transfer(101, "g", 1, 90)
    assert retention.transfer(101, "g", 1, 90)  # owned: any-count delete
    assert retention.expire("table:101", "g", 1, 90)
    assert not retention.expire("table:101", "g", 1, 90)  # gone: False
    assert retention.execute(
        "SELECT pin_state FROM {s}tables WHERE relation_id=101"
    ).fetchone() == ("released",)


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_a_transfer_invariant_holds_that_table_alone(stores, tmp_path, monkeypatch):
    """ensure_pin's transfer raising InvariantViolation holds just that
    table (needs_action, pin_invariant); the other tables still apply."""
    from test_phase2 import ITEMS, OTHER, Harness, relation_message

    h = Harness(stores, tmp_path)
    w = h.wire
    h.ingest(
        w.txn(relation_message(1, "items", ITEMS), w.insert(1, ("1", "a", "t", "1.00"))),
        w.txn(relation_message(2, "other", OTHER, identity="d"), w.insert(2, ("1", "t", "x"))),
    )
    items = h.apply.registered("public.items")[0]
    ensure_pin = h.apply.ensure_pin

    def broken(relation_id, current, **kw):
        if relation_id == items:
            raise InvariantViolation("pin_transfer/pin_state_update: 0 rows (seeded)")
        return ensure_pin(relation_id, current, **kw)

    monkeypatch.setattr(h.apply, "ensure_pin", broken)
    results = {r.table: r.status for r in h.apply.run()}
    assert results.pop("public.items") == "needs_action", results
    assert set(results.values()) == {"caught_up"}, results
    held = h.store.con.execute(
        f"SELECT state,hold_code FROM {h.apply.mirror}._cdc.apply_state WHERE relation_id=?",
        [items],
    ).fetchone()
    assert held == ("needs_action", "pin_invariant")
    assert [r[:3] for r in h.rows("other")] == [(1, True, "x")]
    # operations.md: repair, then retry. The repair here is the fault going away; retry
    # clears the hold and the next run's transfer succeeds.
    from cdc_flight import table_actions as ta

    monkeypatch.undo()
    retried = ta.retry(
        h.apply,
        "public.items",
        run_id="r",
        actor="t",
        expected_blocked_lsn=None,
        expected_unit=None,
    )
    assert retried.status == "done", retried
    assert {r.table: r.status for r in h.apply.run()}["public.items"] == "caught_up"
    live = h.store.con.execute(
        f"SELECT state,hold_code FROM {h.apply.mirror}._cdc.apply_state WHERE relation_id=?",
        [items],
    ).fetchone()
    assert live == ("live", None)


# Coverage: every command x the four kinds names its test or says why not -----------------

HERE = "test_lifecycle_commands.py::"
KINDS = ("rollback", "repeat", "stale_lease", "crash")
COVERAGE = {
    "register": {
        "rollback": "n/a: one transaction; a failure rolls the source row, its epoch and "
        "the registry back together (test_ingest_log2's bootstrap tests)",
        "repeat": "n/a: a repeat finds the source registered and returns before any "
        "transaction (every log2 fixture registers once per test)",
        "stale_lease": "n/a: no lease (the in-transaction re-read)",
        "crash": "n/a: no fault point inside its transaction",
    },
    "record_created": {
        "rollback": HERE + "test_record_created_on_a_missing_source_row_is_an_invariant_violation",
        "repeat": "test_delete_source.py::test_created_flags_are_monotonic_and_a_recover_marks_its_slot",
        "stale_lease": "n/a: no lease",
        "crash": "n/a: one statement",
    },
    "delete_start": {
        "rollback": "test_delete_source.py::test_an_attached_reader_is_waiting_and_the_rerun_finishes",
        "repeat": "test_delete_source.py::test_an_attached_reader_is_waiting_and_the_rerun_finishes",
        "stale_lease": "n/a: delete_source's leased() acquires per call; its guard is the "
        "source GUARD, whose expiry check canary 3 covers through begin_epoch",
        "crash": "test_delete_source.py::test_an_attached_reader_is_waiting_and_the_rerun_finishes",
    },
    "delete_finish": {
        "rollback": "n/a: one statement after the guard",
        "repeat": "test_delete_source.py::test_a_created_source_drops_exactly_its_slot_publication_and_heartbeat",
        "stale_lease": "n/a: as delete_start",
        "crash": "n/a: the drops before it are idempotent; a re-run finishes",
    },
    "begin_epoch": {
        "rollback": HERE + "test_the_epoch_moves_every_row_or_rolls_back",
        "repeat": HERE + "test_the_epoch_moves_every_row_or_rolls_back",
        "stale_lease": HERE + "test_an_expired_source_lease_nobody_took_never_begins_an_epoch",
        "crash": "test_epoch_flight.py::test_a_crash_at_every_fault_point_leaves_a_state_the_retry_completes",
    },
    "pin_transfer": {
        "rollback": HERE + "test_transfer_rolls_the_put_back_if_the_state_update_misses",
        "repeat": HERE + "test_transfer_and_expire_repeat_safely",
        "stale_lease": "n/a: the retention guard is a row write, not a lease",
        "crash": "test_backfill.py::test_interrupted_transfer_is_completed_by_apply",
    },
    "pin_expire": {
        "rollback": HERE + "test_expire_rolls_the_delete_back_if_the_state_update_misses",
        "repeat": HERE + "test_transfer_and_expire_repeat_safely",
        "stale_lease": "n/a: as pin_transfer",
        "crash": HERE + "test_expiry_isolates_an_invariant_violation_per_pin",
    },
    "pin_advance": {
        "rollback": "n/a: one unguarded statement",
        "repeat": "n/a: greatest(): a repeat changes nothing",
        "stale_lease": "n/a: unguarded by design",
        "crash": "n/a: a lost advance leaves extra history",
    },
    "pin_release": {
        "rollback": "n/a: best effort",
        "repeat": "n/a: any count",
        "stale_lease": "n/a: no lease",
        "crash": "n/a: swept by sweep_pins",
    },
}


def test_every_command_has_rollback_repeat_stale_lease_and_crash_coverage():
    assert set(COVERAGE) == set(spec.COMMANDS)
    tests = Path(__file__).resolve().parent
    for name, kinds in COVERAGE.items():
        assert set(kinds) == set(KINDS), name
        for kind, target in kinds.items():
            if target.startswith("n/a: "):
                continue
            file, function = target.split("::")
            names = {
                n.name
                for n in ast.parse((tests / file).read_text()).body
                if isinstance(n, ast.FunctionDef)
            }
            assert function in names, (name, kind, target)


# Canaries -----------------------------------------------------------------------------------


def test_canary_widened_lifecycle_edge_is_caught(monkeypatch):
    widened = replace(spec.command("delete_start"), from_states=("registered", "deleting"))
    monkeypatch.setitem(spec.COMMANDS, "delete_start", widened)
    with pytest.raises(AssertionError):
        judge_edges()


def test_canary_dropped_exact_check_is_caught(monkeypatch):
    loose = replace(
        spec.command("begin_epoch"),
        rows={**spec.command("begin_epoch").rows, "tables_update": spec.Rows(any_count=True)},
    )
    monkeypatch.setitem(spec.COMMANDS, "begin_epoch", loose)
    with pytest.raises(BaseException) as raised:
        judge_exact()
    assert not isinstance(raised.value, InvariantViolation)


def test_canary_source_guard_without_expiry_is_caught(log2, monkeypatch):  # noqa: F811
    monkeypatch.setattr(
        ingest_log2, "GUARD", ingest_log2.GUARD.replace("AND expires_at>current_timestamp ", "")
    )
    log2.execute(
        "UPDATE {s}source SET expires_at=current_timestamp-INTERVAL '60 second' WHERE id=1"
    )
    with pytest.raises(BaseException) as raised:
        with pytest.raises(FenceLost):
            log2.begin_epoch(2, 200, "skip", "epoch_2")
    assert not isinstance(raised.value, FenceLost)


def test_canary_expire_without_its_state_check_is_caught(retained, monkeypatch):  # noqa: F811
    checked = lc.checked

    def lenient(caller, name, count, **kw):
        if (caller, name) == ("pin_expire", "pin_state_update"):
            return count
        return checked(caller, name, count, **kw)

    monkeypatch.setattr(lc, "checked", lenient)
    with pytest.raises(BaseException) as raised:
        judge_expire_rolls_back(retained[0])
    assert not isinstance(raised.value, InvariantViolation)


def test_canary_record_tables_without_its_guard_is_caught():
    seeded = dict(sources())
    seeded["ingest_common"] = seeded["ingest_common"].replace(
        "        with self.transaction():\n            self.guard()\n",
        "        with self.transaction():\n",
        1,
    )
    assert seeded["ingest_common"] != sources()["ingest_common"], "seed didn't apply"
    with pytest.raises(AssertionError, match="record_tables"):
        judge_insert_guards(seeded)


@pytest.mark.parametrize(
    "line, judge",
    [
        ('    x.execute("UPDATE {s}source SET token=? WHERE id=1")\n', "source"),
        ('    x.execute("INSERT INTO {s}tables (relation_id) VALUES (?)")\n', "insert"),
        ('    x.execute("UPDATE {s}source SET lifecycle=NULL WHERE id=1")\n', "column"),
        ('    x.execute("DELETE FROM {s}retention_pins WHERE owner=?")\n', "pins"),
    ],
)
def test_canary_unlisted_writes_are_caught(line, judge):
    seeded = dict(sources())
    seeded["seeded"] = "def f(x):\n" + line
    with pytest.raises(AssertionError):
        if judge == "source":
            judge_listed(statements(seeded, writes("source")), text.SOURCE, "source")
        elif judge == "insert":
            judge_tables_inserts(statements(seeded, writes("tables")))
        elif judge == "column":
            judge_source_columns(statements(seeded, writes("source")))
        else:
            judge_listed(statements(seeded, writes("retention_pins")), text.PINS, "pins")
