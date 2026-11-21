"""Publication and the log-only starts through generation_spec, with
negative canaries. Judges are plain functions that raise AssertionError when the property
fails; each canary seeds its fault and asserts the judge then fails.

Real PostgreSQL + local DuckDB for publish and start_from_log (test_backfill's `populated`);
the adapter's own Store (test_generation_commands) for replace; static scans for the
inventories."""

import ast
import re
import sys
from dataclasses import replace
from pathlib import Path

import pytest
from test_backfill import drive, populated  # noqa: F401 (fixture)
from test_generation_commands import GEN, REL, Store, sources
from test_schema_cases import schema_source  # noqa: F401 (fixture)

from cdc_flight import apply as apply_module
from cdc_flight import generation_spec as spec
from cdc_flight import generation_spec_text as text
from cdc_flight import publication_commands as pub
from cdc_flight.apply import ApplyError, FenceLost
from cdc_flight.apply_run import ApplyRun
from cdc_flight.generation_spec import IllegalTransition, InvariantViolation
from cdc_flight.ingest import run_once

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
import build_flight  # noqa: E402


class Stop(Exception):
    """A fault that stops a run at a named stage."""


def at(stage):
    def fault(seen):
        if seen == stage:
            raise Stop(stage)

    return fault


# A generation caught up and ready to publish, over a log-only mirror generation -----------


class Ready:
    def __init__(self, stores, populated, *, previous=True):  # noqa: F811
        source, _, ingest, log, backfill = populated
        self.stores, self.backfill, self.ingest, self.log = stores, backfill, ingest, log
        self.table = f"{source.schema}.pairs"
        self.schema = source.schema
        self.relation_id, _ = backfill.registered(self.table)
        self.apply = ApplyRun(stores[0].con, replace(backfill.config, tables=(self.table,)))
        if previous:
            self.apply.start_from_log(self.table, accept_incomplete=True)
        drive(backfill, self.table, ingest, log, until="waiting_for_marker")
        for _ in range(50):
            assert run_once(ingest, log).status == "drained"
            try:
                result = backfill.run_table(self.table, fault=at("after_catch_up"))
            except Stop:
                break
            assert result.status in ("waiting_for_marker", "waiting_for_ingest", "catching_up")
        else:
            raise AssertionError("never caught up")
        generation_id = backfill.active(self.relation_id)["generation_id"]
        self.token = backfill.acquire_generation(generation_id, "s4")
        self.gen = backfill.generation(generation_id)
        assert self.gen["state"] == "catching_up"

    def publish(self, **options):
        return self.backfill.publish(self.table, self.gen, self.token, "s4", **options)

    def snapshot(self):
        con, m = self.stores[0].con, self.backfill.mirror
        return dict(
            generations=con.execute(
                f"SELECT generation_id,state FROM {m}._cdc.generations "
                "WHERE relation_id=? ORDER BY ALL",
                [self.relation_id],
            ).fetchall(),
            apply_state=con.execute(
                # Not the token: acquiring the apply lease is its own autocommit statement,
                # so it advances even when the publication rolls back.
                f"SELECT generation_id,state FROM {m}._cdc.apply_state WHERE relation_id=?",
                [self.relation_id],
            ).fetchall(),
            claims=con.execute(
                f"SELECT generation_id FROM {m}._cdc.backfills WHERE relation_id=?",
                [self.relation_id],
            ).fetchall(),
            tables=con.execute(
                "SELECT table_name FROM duckdb_tables() WHERE database_name=? AND schema_name=? "
                "ORDER BY ALL",
                [self.backfill.config.mirror_database, self.schema],
            ).fetchall(),
        )

    def apply_lease(self):
        return (
            self.stores[0]
            .con.execute(
                f"SELECT holder,expires_at FROM {self.backfill.mirror}._cdc.apply_state "
                "WHERE relation_id=?",
                [self.relation_id],
            )
            .fetchone()
        )

    def expire(self, table, key, value):
        self.stores[0].con.execute(
            f"UPDATE {self.backfill.mirror}._cdc.{table} SET expires_at=current_timestamp"
            f"-INTERVAL '60 second' WHERE {key}=?",
            [value],
        )


# Judges ---------------------------------------------------------------------------------


def judge_replace_counts():
    """replace from anything but published is 0 rows -> InvariantViolation, nothing moved."""
    for state in spec.STATES:
        store = Store()
        store.seed(state, claim=False)
        before = store.snapshot()
        if state == "published":
            with store.transaction():
                pub.replace(store, "publish", GEN)
            assert store.gen()["state"] == "replaced"
            continue
        with pytest.raises(InvariantViolation):
            with store.transaction():
                pub.replace(store, "publish", GEN)
        assert store.snapshot() == before, state


def judge_expired_apply_lease(ready):
    """An apply lease that expired after publish acquired it (nobody took it): the
    transaction's apply guard refuses, nothing is published."""
    acquire = ready.backfill.acquire

    def acquire_then_expire(relation_id, run_id, **options):
        token = acquire(relation_id, run_id, **options)
        ready.expire("apply_state", "relation_id", relation_id)
        return token

    ready.backfill.acquire = acquire_then_expire
    before = ready.snapshot()
    try:
        ready.publish()
    except FenceLost:
        pass
    else:
        raise AssertionError("an expired apply lease published")
    finally:
        del ready.backfill.acquire
    assert ready.snapshot() == before


APPLY_STATE = re.compile(r"\b(INSERT\s+INTO|UPDATE|DELETE\s+FROM)\s+[^\s(]*?\bapply_state\b", re.I)
DDL = re.compile(r"\bDROP\s+TABLE\b|\bRENAME\s+TO\b", re.I)


def _text(node):
    """A string expression's text: constants, f-string parts and `+` chains joined, any
    other operand as {x}. None if it holds no string constant at all."""
    if isinstance(node, ast.Constant):
        return node.value if isinstance(node.value, str) else None
    if isinstance(node, ast.JoinedStr):
        return "".join(v.value if isinstance(v, ast.Constant) else "{x}" for v in node.values)
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        left, right = _text(node.left), _text(node.right)
        if left is None and right is None:
            return None
        return (left if left is not None else "{x}") + (right if right is not None else "{x}")
    return None


def statements(sources_, pattern):
    """{module:qualname: [statement text]} for string expressions matching `pattern`
    (f-string parts and `+` chains joined into one statement), keyed as
    test_generation_commands.write_sites keys them."""
    found = {}
    for module, source in sources_.items():

        def visit(node, scope):
            for child in ast.iter_child_nodes(node):
                name = scope
                if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                    name = f"{scope}.{child.name}" if scope else child.name
                elif isinstance(child, ast.Assign) and not scope:
                    name = ",".join(t.id for t in child.targets if isinstance(t, ast.Name))
                value = (
                    _text(child)
                    if isinstance(child, (ast.Constant, ast.JoinedStr, ast.BinOp))
                    else None
                )
                if value is not None:
                    if pattern.search(value):
                        found.setdefault(f"{module}:{name}", []).append(value)
                    continue
                visit(child, name)

        visit(ast.parse(source), "")
    return found


def judge_listed(found, listed, what):
    counts = {k: len(v) for k, v in found.items()}
    expected = {k: n for k, (_, n) in listed.items()}
    unlisted = {k: v for k, v in counts.items() if k not in expected}
    stale = {k: v for k, v in expected.items() if k not in counts}
    assert not unlisted, f"{what} sites missing from generation_spec_text: {unlisted}"
    assert not stale, f"{what} entries with no site: {stale}"
    assert counts == expected, (counts, expected)


def assigns_generation_id(statement):
    head = statement.upper()
    if head.lstrip().startswith("INSERT"):
        return "GENERATION_ID" in head.split("VALUES")[0]
    if head.lstrip().startswith("UPDATE"):
        return re.search(r"\bGENERATION_ID\s*=\s*\?", head.split(" WHERE ")[0]) is not None
    return False


def judge_generation_id_writers(found):
    """The statements assigning apply_state.generation_id are exactly the pinned ones, and
    every UPDATE assigning it, and every DELETE, carries token=? (publish's premise)."""
    writers = {}
    for site, texts in found.items():
        for statement in texts:
            if assigns_generation_id(statement):
                writers[site] = writers.get(site, 0) + 1
            head = statement.lstrip().upper()
            if (head.startswith("UPDATE") and assigns_generation_id(statement)) or head.startswith(
                "DELETE"
            ):
                # The predicate, not the SET list: `SET token=? WHERE ...` fences nothing.
                where = head.split(" WHERE ", 1)[1] if " WHERE " in head else ""
                assert "TOKEN=?" in where.replace(" ", ""), (site, statement)
    assert writers == dict(text.GENERATION_ID_WRITERS), writers


# Tests: the spec and its prose ----------------------------------------------------------


def test_every_command_and_statement_has_its_text():
    assert set(text.TEXT) == set(spec.COMMANDS)
    statements_ = {(c, s) for c, command in spec.COMMANDS.items() for s in command.rows}
    assert set(text.WHY) == statements_
    for name, prose in text.TEXT.items():
        assert prose.effects and prose.idempotent and prose.recovery, name


def test_no_flight_command_imports_the_prose_module():
    """Design property: the prose is for tests and docs; the commands import only the
    data module."""
    from authority_scan import reach

    for command in build_flight.ENTRY:
        assert "generation_spec_text" not in reach(command), command


def test_every_lease_taker_declares_its_release():
    for name in ("publish", "create_log_only", "create_log_only_append"):
        assert spec.command(name).leases, name


# Tests: replace (adapter level) -----------------------------------------------------------


def test_replace_of_anything_but_published_is_an_invariant_violation():
    judge_replace_counts()


def test_the_unstamped_replace_writes_no_updated_at():
    assert "updated_at" not in pub.REPLACE_UNSTAMPED and "updated_at" in pub.REPLACE
    store = Store()
    store.seed("published", claim=False)
    with store.transaction():
        pub.replace(store, "create_log_only_append", GEN, stamped=False)
    assert store.gen()["state"] == "replaced"


def test_retired_delete_counts_exactly_one_row():
    store = Store()
    store.con.execute(
        "CREATE TABLE m.apply_state (relation_id INTEGER PRIMARY KEY, token INTEGER, x INTEGER)"
    )
    store.con.execute("INSERT INTO m.apply_state VALUES (?, 3, 0)", [REL])
    with pytest.raises(InvariantViolation):
        with store.transaction():
            pub.retired_delete(store, REL, 2)  # another token: 0 rows
    with store.transaction():
        pub.retired_delete(store, REL, 3)
    assert store.con.execute("SELECT count(*) FROM m.apply_state").fetchone() == (0,)


# Tests: publish (real PostgreSQL) ---------------------------------------------------------


@pytest.mark.postgres
@pytest.mark.parametrize("stage", ["before_rename", "after_rename"])
def test_a_fault_inside_publish_rolls_every_entity_back(stores, populated, stage):  # noqa: F811
    ready = Ready(stores, populated)
    before = ready.snapshot()
    with pytest.raises(Stop):
        ready.publish(fault=at(stage))
    assert ready.snapshot() == before
    assert ready.apply_lease() == (None, None)  # released in publish's finally
    assert ready.publish() is True
    after = ready.snapshot()
    assert ready.gen["generation_id"] in {g for g, s in after["generations"] if s == "published"}
    assert after["claims"] == [] and ready.apply_lease() == (None, None)


@pytest.mark.postgres
def test_publish_again_is_an_illegal_transition_before_any_statement(stores, populated):  # noqa: F811
    ready = Ready(stores, populated, previous=False)
    assert ready.publish() is True
    ready.gen = ready.backfill.generation(ready.gen["generation_id"])
    seen = []
    execute = ready.backfill.execute
    ready.backfill.execute = lambda *a, **k: seen.append(a[0]) or execute(*a, **k)
    try:
        with pytest.raises(IllegalTransition):
            ready.publish()
    finally:
        del ready.backfill.execute
    assert seen == []


@pytest.mark.postgres
def test_an_expired_lease_nobody_took_never_publishes(stores, populated):  # noqa: F811
    ready = Ready(stores, populated)
    ready.expire("generations", "generation_id", ready.gen["generation_id"])
    before = ready.snapshot()
    with pytest.raises(FenceLost):
        ready.publish()
    assert ready.snapshot() == before


@pytest.mark.postgres
def test_an_expired_apply_lease_nobody_took_never_publishes(stores, populated):  # noqa: F811
    judge_expired_apply_lease(Ready(stores, populated))


@pytest.mark.postgres
def test_an_unexpected_apply_state_count_rolls_publish_back(stores, populated, monkeypatch):  # noqa: F811
    """A writer without the lease moved the mirror's generation: the compare-and-set finds
    0 rows after the guard -> InvariantViolation, nothing committed."""
    ready = Ready(stores, populated)
    statement = pub.statement

    def moved(owner, caller, name, sql, params, lease_error=InvariantViolation):
        if name == "apply_cas":
            params = [*params[:-1], "not-the-mirrors-generation"]
        return statement(owner, caller, name, sql, params, lease_error)

    monkeypatch.setattr(pub, "statement", moved)
    before = ready.snapshot()
    with pytest.raises(InvariantViolation, match="publish/apply_cas"):
        ready.publish()
    assert ready.snapshot() == before


# Tests: publish over a retired relation (a recreated table; real PostgreSQL) ----------------


class Retired:
    """A recreated source table caught up for publication over the retired relation's
    mirror (test_table_hold's setup), stopped right before publish."""

    def __init__(self, stores, schema_source):  # noqa: F811
        from test_table_hold import add_to_publication, ingest_once, recreate

        from cdc_flight.membership import add_table

        source, ingest, log, backfill, apply = schema_source
        self.stores, self.backfill = stores, backfill
        self.schema, self.table = source.schema, f"{source.schema}.t_type"
        assert drive(backfill, self.table, ingest, log).status == "published"
        self.old_id, _ = apply.registered(self.table)
        source.execute("DROP TABLE t_type")
        recreate(source, "t_type")
        add_to_publication(source, ingest, "t_type")
        ingest_once(ingest, log)
        assert add_table(log, apply, source.con, ingest.publication, self.table).status == "adopted"
        self.relation_id, _ = backfill.registered(self.table)
        assert self.relation_id != self.old_id
        drive(backfill, self.table, ingest, log, until="waiting_for_marker")
        for _ in range(50):
            ingest_once(ingest, log)
            try:
                backfill.run_table(self.table, fault=at("after_catch_up"))
            except Stop:
                break
        else:
            raise AssertionError("never caught up")
        generation_id = backfill.active(self.relation_id)["generation_id"]
        self.token = backfill.acquire_generation(generation_id, "s4")
        self.gen = backfill.generation(generation_id)

    def publish(self, **options):
        return self.backfill.publish(self.table, self.gen, self.token, "s4", **options)

    def snapshot(self):
        con, m = self.stores[0].con, self.backfill.mirror
        ids = [self.old_id, self.relation_id]
        return dict(
            generations=con.execute(
                f"SELECT relation_id,generation_id,state FROM {m}._cdc.generations "
                "WHERE relation_id IN (?,?) ORDER BY ALL",
                ids,
            ).fetchall(),
            apply_state=con.execute(
                f"SELECT relation_id,generation_id,state FROM {m}._cdc.apply_state "
                "WHERE relation_id IN (?,?) ORDER BY ALL",
                ids,
            ).fetchall(),
            claims=con.execute(
                f"SELECT generation_id FROM {m}._cdc.backfills WHERE relation_id IN (?,?)", ids
            ).fetchall(),
            tables=con.execute(
                "SELECT table_name FROM duckdb_tables() WHERE database_name=? AND schema_name=? "
                "ORDER BY ALL",
                [self.backfill.config.mirror_database, self.schema],
            ).fetchall(),
        )

    def retired_holder(self):
        return (
            self.stores[0]
            .con.execute(
                f"SELECT holder,expires_at FROM {self.backfill.mirror}._cdc.apply_state "
                "WHERE relation_id=?",
                [self.old_id],
            )
            .fetchone()
        )


@pytest.fixture
def retired(stores, schema_source):  # noqa: F811
    return Retired(stores, schema_source)


@pytest.mark.postgres
def test_a_fault_before_the_rename_over_a_retired_mirror_rolls_back(retired):
    before = retired.snapshot()
    with pytest.raises(Stop):
        retired.publish(fault=at("before_rename"))
    assert retired.snapshot() == before
    assert retired.retired_holder() == (None, None)  # freed at once, not at expiry
    assert retired.publish() is True
    after = retired.snapshot()
    assert [r for r in after["apply_state"] if r[0] == retired.old_id] == []
    assert (retired.relation_id, retired.gen["generation_id"], "live") in after["apply_state"]


@pytest.mark.postgres
def test_an_expired_retired_lease_nobody_took_never_publishes(retired):
    acquire = retired.backfill.acquire

    def acquire_then_expire(relation_id, run_id, **options):
        token = acquire(relation_id, run_id, **options)
        if relation_id == retired.old_id:
            retired.stores[0].con.execute(
                f"UPDATE {retired.backfill.mirror}._cdc.apply_state SET expires_at="
                "current_timestamp-INTERVAL '60 second' WHERE relation_id=?",
                [relation_id],
            )
        return token

    retired.backfill.acquire = acquire_then_expire
    before = retired.snapshot()
    try:
        with pytest.raises(FenceLost):
            retired.publish()
    finally:
        del retired.backfill.acquire
    assert retired.snapshot() == before


@pytest.mark.postgres
def test_a_retired_row_moved_without_its_lease_is_an_invariant_violation(retired, monkeypatch):
    """A writer without the retired relation's lease changed its token after our guard:
    the DELETE finds 0 rows -> InvariantViolation, every entity rolled back."""
    delete = pub.retired_delete
    monkeypatch.setattr(pub, "retired_delete", lambda owner, rel, token: delete(owner, rel, -1))
    before = retired.snapshot()
    with pytest.raises(InvariantViolation, match="publish/retired_delete"):
        retired.publish()
    assert retired.snapshot() == before
    assert retired.retired_holder() == (None, None)


# Tests: the log-only starts (real PostgreSQL) ----------------------------------------------


@pytest.mark.postgres
def test_start_from_log_rolls_back_both_rows(stores, populated, monkeypatch):  # noqa: F811
    from cdc_flight import generation_commands

    source, _, _, _, backfill = populated
    table = f"{source.schema}.pairs"
    relation_id, _ = backfill.registered(table)
    apply = ApplyRun(stores[0].con, replace(backfill.config, tables=(table,)))
    inserted = generation_commands.inserted

    def fail_second(owner, name, statement, sql, params):
        inserted(owner, name, statement, sql, params)
        if statement == "apply_insert":
            raise RuntimeError("after apply_insert")

    monkeypatch.setattr(generation_commands, "inserted", fail_second)
    with pytest.raises(RuntimeError):
        apply.start_from_log(table, accept_incomplete=True)
    m = backfill.mirror
    con = stores[0].con
    assert con.execute(
        f"SELECT count(*) FROM {m}._cdc.generations WHERE relation_id=?", [relation_id]
    ).fetchone() == (0,)
    assert con.execute(
        f"SELECT count(*) FROM {m}._cdc.apply_state WHERE relation_id=?", [relation_id]
    ).fetchone() == (0,)
    monkeypatch.undo()
    apply.start_from_log(table, accept_incomplete=True)
    with pytest.raises(ApplyError, match="already has a generation"):
        apply.start_from_log(table, accept_incomplete=True)


# Tests: start_append (keyless append tables) ------------------------------------------------


def append_env(stores):
    from test_keyless_append import held_on_update

    return held_on_update(stores)


@pytest.mark.postgres
def test_start_append_replace_rolls_back_and_releases_its_lease(stores, monkeypatch):
    from test_keyless_append import OTHER, OTHER_TABLE

    from cdc_flight import table_actions as ta

    env = append_env(stores)
    m = f"{env.apply.mirror}._cdc"
    con = env.store.con
    before = con.execute(
        f"SELECT generation_id,state,holder,expires_at FROM {m}.apply_state WHERE relation_id=?",
        [OTHER],
    ).fetchone()
    statement = pub.statement

    def boom(owner, caller, name, sql, params, lease_error=InvariantViolation):
        if name == "apply_cas":
            raise RuntimeError("before apply_cas")
        return statement(owner, caller, name, sql, params, lease_error)

    monkeypatch.setattr(pub, "statement", boom)
    with pytest.raises(RuntimeError):
        env.apply.start_append(OTHER_TABLE, accept_incomplete=True)
    after = con.execute(
        f"SELECT generation_id,state,holder,expires_at FROM {m}.apply_state WHERE relation_id=?",
        [OTHER],
    ).fetchone()
    assert after[:2] == before[:2] and after[2:] == (None, None)
    old = before[0]
    assert con.execute(
        f"SELECT state FROM {m}.generations WHERE generation_id=?", [old]
    ).fetchone() != ("replaced",)
    monkeypatch.undo()
    done = ta.start_from_log(env.apply, OTHER_TABLE, run_id="s", actor="t")
    assert done.status == "done", done
    holder, expires = con.execute(
        f"SELECT holder,expires_at FROM {m}.apply_state WHERE relation_id=?", [OTHER]
    ).fetchone()
    assert (holder, expires) == (None, None)  # the CAS itself cleared the lease


@pytest.mark.postgres
def test_start_append_refuses_a_claim_made_after_its_early_check(stores, monkeypatch):
    """Claim exclusivity (publish's premise): a backfill claim that appears between
    start_append's early check and its transaction is refused under the lease."""
    from test_keyless_append import OTHER, OTHER_TABLE

    from cdc_flight import append_apply

    env = append_env(stores)
    m = f"{env.apply.mirror}._cdc"
    con = env.store.con
    con.execute(
        f"CREATE TABLE IF NOT EXISTS {m}.backfills (relation_id BIGINT PRIMARY KEY, "
        "generation_id VARCHAR)"
    )
    before = con.execute(
        f"SELECT generation_id,state FROM {m}.apply_state WHERE relation_id=?", [OTHER]
    ).fetchone()
    early = append_apply._claim

    calls = []

    def claim_after_early_check(apply, relation_id):
        seen = early(apply, relation_id)
        if not calls:  # only the early check: the claim appears right after it
            calls.append(1)
            con.execute(f"INSERT INTO {m}.backfills VALUES (?, 'racing')", [relation_id])
        return seen

    monkeypatch.setattr(append_apply, "_claim", claim_after_early_check)
    with pytest.raises(ApplyError, match="append backfill is running"):
        env.apply.start_append(OTHER_TABLE, accept_incomplete=True)
    assert (
        con.execute(
            f"SELECT generation_id,state FROM {m}.apply_state WHERE relation_id=?", [OTHER]
        ).fetchone()
        == before
    )
    holder = con.execute(
        f"SELECT holder FROM {m}.apply_state WHERE relation_id=?", [OTHER]
    ).fetchone()
    assert holder == (None,)


@pytest.mark.postgres
def test_start_append_sees_a_claims_table_created_after_its_early_check(stores, monkeypatch):
    """N1: no claims table at the early check (None), then a first backfill creates it and
    claims the table: the in-transaction catalog probe finds it and the re-check refuses."""
    from test_keyless_append import OTHER, OTHER_TABLE

    from cdc_flight import append_apply

    env = append_env(stores)
    m = f"{env.apply.mirror}._cdc"
    con = env.store.con
    con.execute(f"DROP TABLE IF EXISTS {m}.backfills")

    early = append_apply._claim
    calls = []

    def none_then_claim(apply, relation_id):
        if calls:  # the in-transaction re-check reads for real
            return early(apply, relation_id)
        calls.append(1)
        con.execute(
            f"CREATE TABLE {m}.backfills (relation_id BIGINT PRIMARY KEY, generation_id VARCHAR)"
        )
        con.execute(f"INSERT INTO {m}.backfills VALUES (?, 'first')", [relation_id])
        return None

    monkeypatch.setattr(append_apply, "_claim", none_then_claim)
    before = con.execute(
        f"SELECT generation_id,state FROM {m}.apply_state WHERE relation_id=?", [OTHER]
    ).fetchone()
    with pytest.raises(ApplyError, match="append backfill is running"):
        env.apply.start_append(OTHER_TABLE, accept_incomplete=True)
    assert (
        con.execute(
            f"SELECT generation_id,state FROM {m}.apply_state WHERE relation_id=?", [OTHER]
        ).fetchone()
        == before
    )


@pytest.mark.postgres
def test_start_append_with_an_expired_lease_nobody_took_writes_nothing(stores):
    """create_log_only_append's stale lease: the apply lease expires after start_append
    acquired it (nobody took it); its guard refuses and nothing is written."""
    from test_keyless_append import OTHER, OTHER_TABLE

    env = append_env(stores)
    m = f"{env.apply.mirror}._cdc"
    con = env.store.con
    acquire = env.apply.acquire

    def acquire_then_expire(relation_id, run_id, **options):
        token = acquire(relation_id, run_id, **options)
        con.execute(
            f"UPDATE {m}.apply_state SET expires_at=current_timestamp-INTERVAL '60 second' "
            "WHERE relation_id=?",
            [relation_id],
        )
        return token

    generations = (
        f"SELECT generation_id,state FROM {m}.generations WHERE relation_id=? ORDER BY ALL"
    )
    before = (
        con.execute(
            f"SELECT generation_id,state FROM {m}.apply_state WHERE relation_id=?", [OTHER]
        ).fetchone(),
        con.execute(generations, [OTHER]).fetchall(),
    )
    env.apply.acquire = acquire_then_expire
    try:
        with pytest.raises(FenceLost):
            env.apply.start_append(OTHER_TABLE, accept_incomplete=True)
    finally:
        del env.apply.acquire
    after = (
        con.execute(
            f"SELECT generation_id,state FROM {m}.apply_state WHERE relation_id=?", [OTHER]
        ).fetchone(),
        con.execute(generations, [OTHER]).fetchall(),
    )
    assert after == before


# Tests: inventories (static) ---------------------------------------------------------------


def test_every_apply_state_write_is_listed_both_ways():
    judge_listed(statements(sources(), APPLY_STATE), text.APPLY_STATE, "apply_state")


def test_every_drop_and_rename_is_listed_both_ways():
    judge_listed(statements(sources(), DDL), text.MIRROR_DDL, "DROP/RENAME")


def test_generation_id_writers_are_pinned_and_carry_the_token():
    judge_generation_id_writers(statements(sources(), APPLY_STATE))


# Canaries -----------------------------------------------------------------------------------


def test_canary_widened_replace_is_caught(monkeypatch):
    from test_generation_commands import judge_edges

    widened = replace(spec.command("replace"), from_states=("published", "replaced"))
    monkeypatch.setitem(spec.COMMANDS, "replace", widened)
    with pytest.raises(AssertionError):
        judge_edges()


def test_canary_dropped_replace_count_is_caught(monkeypatch):
    monkeypatch.setattr(pub, "checked", lambda *a, **k: None)
    with pytest.raises(BaseException) as raised:
        judge_replace_counts()
    assert not isinstance(raised.value, InvariantViolation)


def test_canary_unlisted_apply_state_write_is_caught():
    seeded = dict(sources())
    seeded["seeded"] = (
        'def f(x):\n    x.execute("UPDATE {m}apply_state SET state=? WHERE relation_id=?")\n'
    )
    with pytest.raises(AssertionError, match="missing"):
        judge_listed(statements(seeded, APPLY_STATE), text.APPLY_STATE, "apply_state")


def test_canary_unlisted_ddl_is_caught():
    seeded = dict(sources())
    seeded["seeded"] = 'def f(x):\n    x.execute("DROP TABLE IF EXISTS m.t")\n'
    with pytest.raises(AssertionError, match="missing"):
        judge_listed(statements(seeded, DDL), text.MIRROR_DDL, "DROP/RENAME")


def test_canary_a_lease_free_generation_id_writer_is_caught():
    seeded = dict(sources())
    seeded["seeded"] = (
        "def f(x):\n"
        '    x.execute("UPDATE {m}apply_state SET generation_id=? WHERE relation_id=?")\n'
    )
    with pytest.raises(AssertionError):
        judge_generation_id_writers(statements(seeded, APPLY_STATE))


@pytest.mark.parametrize(
    "line",
    [
        '    x.execute("UPDATE {m}apply_state SET " + "generation_id=? WHERE relation_id=?")\n',
        '    x.execute("UPDATE {m}apply_state SET generation_id=?,token=? WHERE relation_id=?")\n',
        '    x.execute("DELETE FROM {m}apply_state WHERE relation_id=?")\n',
    ],
    ids=["plus-split", "token-in-set", "delete"],
)
def test_canary_lease_free_writer_shapes_are_caught(line):
    seeded = dict(sources())
    seeded["seeded"] = "def f(x):\n" + line
    with pytest.raises(AssertionError):
        judge_generation_id_writers(statements(seeded, APPLY_STATE))


@pytest.mark.postgres
def test_canary_apply_guard_without_expiry_is_caught(stores, populated, monkeypatch):  # noqa: F811
    """Canary 3b: Apply.guard without `expires_at>current_timestamp` lets an expired apply
    lease publish; the judge must then fail."""

    def guard(self, relation_id, token):
        count = self.execute(
            "UPDATE {m}apply_state SET guard=uuid()::VARCHAR WHERE relation_id=? AND token=?",
            [relation_id, token],
        ).fetchone()[0]
        if count != 1:
            raise FenceLost("apply lease expired or was replaced")

    monkeypatch.setattr(apply_module.Apply, "guard", guard)
    with pytest.raises(AssertionError, match="an expired apply lease published"):
        judge_expired_apply_lease(Ready(stores, populated))
