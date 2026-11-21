"""ApplyRun's SQL fold as a read path: what it reads, where its checks fail, and that a
failure holds the table exactly as the Python fold does (sql_fold.plan_sql runs inside
_read_batch, under _apply_table's exception mapping)."""

import ast
from pathlib import Path

import duckdb
import pgstream
import pytest
from test_apply2 import OID, OTHER, OTHER_TABLE, START, TABLE, Env, ins

from cdc_flight import apply2, sql_fold
from cdc_flight.apply_run import ApplyRun
from cdc_flight.unit_store import UnitReader

TABLES = (TABLE, OTHER_TABLE)
PLAN = "WITH frames AS MATERIALIZED"
FRAMES = "WITH physical AS MATERIALIZED"
DESCRIPTORS = "SELECT (SELECT list(d) FROM"


def oth(key, value):
    return pgstream.insert(OTHER, (key, value))


def two_tables(stores, fold, *, commits=4, **options):
    env = Env(stores, fold=fold, max_commits=2, **options)
    for table in TABLES:
        env.apply.start_from_log(table)
    for k in range(commits):
        env.commit(ins(str(k), f"a{k}"), oth(str(k), f"b{k}"))
    return env


def outcome(env, result):
    state = env.state(OTHER)
    return (result.status, result.boundary, result.detail, state, env.mirror("others"))


class Statements:
    """Records every statement the apply connection runs."""

    def __init__(self, monkeypatch, apply):
        self.seen = []
        real = apply.execute

        def execute(statement, parameters=None):
            self.seen.append(statement)
            return real(statement, parameters)

        reader_real = UnitReader.execute

        def reader(reader_self, statement, parameters=None):
            self.seen.append(statement)
            return reader_real(reader_self, statement, parameters)

        monkeypatch.setattr(apply, "execute", execute)
        monkeypatch.setattr(UnitReader, "execute", reader)

    def count(self, marker):
        return sum(s.startswith(marker) or marker in s[:80] for s in self.seen)


def before_plan(monkeypatch, apply, action):
    """Run `action` once, just before the next plan statement executes: something that
    changes between units() and the plan, inside one read."""
    real = apply.execute
    done = []

    def execute(statement, parameters=None):
        if statement.startswith(PLAN) and not done:
            done.append(1)
            action()
        return real(statement, parameters)

    monkeypatch.setattr(apply, "execute", execute)
    return done


# A check the plan statement fails holds the table as the Python read would ---------


@pytest.mark.parametrize("stores", ["local"], indirect=True)
@pytest.mark.parametrize("fold", ["sql", "python"])
def test_a_floor_advance_between_tables_holds_the_second(stores, fold):
    """The SQL twin of test_apply2_cache's floor test: the first table applies, the floor
    rises, the second table holds needs_resync with nothing applied, on either fold."""
    env = two_tables(stores, fold)
    first = env.apply.apply_table(TABLE, "first")
    assert first.status == "caught_up"
    env.log.execute("UPDATE {s}retention SET floor_epoch=1,floor_lsn=?", [START])
    result = env.apply.apply_table(OTHER_TABLE, "after drop")
    assert result.status == "needs_resync" and env.state(OTHER)["applied"] == START
    assert env.mirror("others") == {}


def _parity(stores_factory, monkeypatch, change):
    """The outcome of OTHER_TABLE's apply when `change` lands between units() and the
    frame read (SQL: the plan statement; Python: frames(), via the same hook point)."""
    results = {}
    for fold in ("sql", "python"):
        env = two_tables(stores_factory(fold), fold)
        fired = [1]
        if fold == "sql":
            fired = before_plan(monkeypatch, env.apply, lambda env=env: change(env))
        else:
            real = env.apply.reader.frames

            def frames(*args, env=env, real=real, **kwargs):
                change(env)
                return real(*args, **kwargs)

            monkeypatch.setattr(env.apply.reader, "frames", frames)
        results[fold] = outcome(env, env.apply.apply_table(OTHER_TABLE, "run"))
        assert fired, fold  # the change landed under the plan statement
        monkeypatch.undo()
    return results


@pytest.fixture
def fresh(request, tmp_path):
    """A factory of independent local stores (one per fold)."""
    from cdc_flight.store import Store, ident

    made = []

    def make(tag):
        name = f"cdc_p0_sfa_{tag}"
        con = duckdb.connect(str(tmp_path / f"{name}.duckdb"))
        con.execute(f"ATTACH '{tmp_path / (name + '_mirror.duckdb')}' AS {ident(name + '_mirror')}")
        con.execute("SET TimeZone='UTC'")
        store = Store(con, name, name + "_mirror")
        store.initialize()
        made.append(con)
        return (store, store)

    yield make
    for con in made:
        con.close()


def test_a_floor_raised_under_the_plan_holds_as_the_python_read_does(fresh, monkeypatch):
    def drop(env):
        env.log.execute("UPDATE {s}retention SET floor_epoch=1,floor_lsn=?", [START])

    results = _parity(fresh, monkeypatch, drop)
    assert results["sql"] == results["python"]
    assert results["sql"][0] == "needs_resync" and "dropped" in results["sql"][2]


def test_a_binding_change_under_the_plan_holds_as_the_python_read_does(fresh, monkeypatch):
    def rebind(env):
        env.store.con.execute(f"UPDATE {env.log.prefix}protocol_v2 SET binding_json='{{}}'")

    results = _parity(fresh, monkeypatch, rebind)
    assert results["sql"] == results["python"]
    assert results["sql"][0] == "needs_action" and "binding" in results["sql"][2]


# The MERGE's precheck clause, pinned to DuckDB's exception class and message --------


def test_the_precheck_clause_raises_a_constraint_error_carrying_the_marker():
    """If a DuckDB upgrade changes the class or drops the message, this fails in CI
    (ApplyRun maps only ConstraintException carrying the marker to the hold)."""
    from cdc_flight.mirror import Column

    con = duckdb.connect()
    con.execute(
        "CREATE TABLE m(id BIGINT, v VARCHAR, _cdc_commit_lsn UBIGINT, _cdc_updated_at TIMESTAMPTZ)"
    )
    columns = [Column("id", "BIGINT", 20, -1), Column("v", "VARCHAR", 25, -1)]
    source = (
        "(SELECT 2::BIGINT AS c0, NULL::VARCHAR AS c1, 'upsert' AS _s_op, [0] AS _s_known, "
        "FALSE AS _s_complete, 5::UBIGINT AS _s_lsn, TIMESTAMPTZ 'epoch' AS _s_ts)"
    )
    with pytest.raises(duckdb.Error) as raised:
        con.execute(sql_fold.merge_sql("m", ("id",), columns, source))
    assert type(raised.value) is duckdb.ConstraintException
    assert sql_fold.BASELINE_ERROR in str(raised.value)
    assert con.execute("SELECT count(*) FROM m").fetchone()[0] == 0


# S3: the seam is a constructor argument nothing in production sets ---------------------


def test_production_constructions_fold_on_sql():
    """No production call passes fold=; consumer_for and a plain ApplyRun default to SQL."""
    root = Path(__file__).resolve().parents[1] / "src" / "cdc_flight"
    for path in root.glob("*.py"):
        for node in ast.walk(ast.parse(path.read_text())):
            if isinstance(node, ast.Call) and getattr(node.func, "id", None) == "ApplyRun":
                assert not any(k.arg == "fold" for k in node.keywords), path.name
    with pytest.raises(ValueError, match="fold must be"):
        ApplyRun(None, None, fold="other")


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_consumer_for_folds_on_sql(stores):
    env = Env(stores)
    got = apply2.consumer_for(env.store.con, env.config, retention_v2=env.retention)
    assert type(got) is ApplyRun and got.fold == "sql"


# The SQL path's reads ---------------------------------------------------------------


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_the_sql_fold_reads_no_frames_and_plans_once_per_page(stores, monkeypatch):
    """Both tables plan the identical page: one descriptors read and one plan statement for
    the page, shared through the run's plan cache; each table still folds and MERGEs alone."""
    env = two_tables(stores, "sql")
    seen = Statements(monkeypatch, env.apply)
    results = env.apply.run(tables=TABLES)
    assert {r.status for r in results} == {"caught_up"}
    assert seen.count(FRAMES) == 0  # no frame reaches the client
    assert env.apply.fold_stats == {"sql": 2}  # one SQL batch per table
    assert seen.count(PLAN) == 1 and seen.count(DESCRIPTORS) == 1  # one plan for the page
    assert env.apply.plan_stats == {"fills": 1, "hits": 1, "batched_failed": 0}
    assert env.apply._plan_cache is None  # run-scoped
    assert env.mirror() == {str(k): f"a{k}" for k in range(4)}
    assert env.mirror("others") == {str(k): f"b{k}" for k in range(4)}


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_the_python_fold_issues_no_plan(stores, monkeypatch):
    env = two_tables(stores, "python")
    seen = Statements(monkeypatch, env.apply)
    assert {r.status for r in env.apply.run(tables=TABLES)} == {"caught_up"}
    assert seen.count(PLAN) == 0 and seen.count(FRAMES) >= 1
    assert env.apply.fold_stats == {}


# C1: the plan's integrity aggregates cover the whole (unfiltered) page -----------------


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_the_frame_counts_cover_every_relation_in_the_page(stores):
    """Counting only this relation's frames (the fold's filter) must fail the unit checks;
    the real plan counts every relation and record kind, as frames() does."""
    env = two_tables(stores, "sql")
    apply = env.apply
    reader = apply.reader
    head = reader.head()
    page = reader.units(1, START, head.high)
    descriptors = reader.descriptors(page, high=head.high)
    ours = [d for (oid, _, _), d in descriptors.items() if oid == OID]
    versions = {
        d.version: (len(d.names), tuple(d.names.index(k) + 1 for k in d.keys)) for d in ours
    }
    epoch, low, end = reader._bounds(page, head.high)
    frames = sql_fold.frames_sql(sorted({d for u in page for d in u.days}), epoch, low, end)
    real = sql_fold.plan_sql(frames, OID, versions)
    row = apply.execute(real).fetchone()
    reader.check_frames(page, head.high, *row[:4])  # passes
    filtered = real.replace(
        "FROM frames GROUP BY unit_start_lsn",
        f"FROM frames WHERE relation_id={OID} GROUP BY unit_start_lsn",
    )
    assert filtered != real
    row = apply.execute(filtered).fetchone()
    with pytest.raises(ValueError, match="count mismatch|gap or duplicate"):
        reader.check_frames(page, head.high, *row[:4])


# The batched page plan: per-table decisions, failure fallback, cache identity ----------------


def _state_rows(env):
    return env.store.con.execute(
        f"SELECT relation_id,applied_through_lsn,state,blocked_commit_lsn,detail "
        f"FROM {env.apply.mirror}._cdc.apply_state ORDER BY 1"
    ).fetchall()


def test_a_shared_page_is_decided_per_table(fresh):
    """M1: both tables share the page's plan, but OTHER's mirror is registered under
    another key layout: only OTHER falls back (and holds needs_resync, as on the Python
    fold); TABLE applies on SQL."""
    outcomes = {}
    for fold in ("sql", "python"):
        env = two_tables(fresh(fold), fold)
        env.store.con.execute(
            f"UPDATE {env.apply.mirror}._cdc.apply_state SET key_columns='[\"v\"]' "
            "WHERE relation_id=?",
            [OTHER],
        )
        results = {r.table: (r.status, r.boundary) for r in env.apply.run(tables=TABLES)}
        outcomes[fold] = (results, _state_rows(env), env.mirror(), env.mirror("others"))
        if fold == "sql":
            assert env.apply.fold_stats == {"sql": 1, "fallback:key_layout": 1}
            assert env.apply.plan_stats["fills"] == 1 and env.apply.plan_stats["hits"] == 1
    assert outcomes["sql"] == outcomes["python"]
    assert outcomes["sql"][0][OTHER_TABLE][0] == "needs_resync"
    assert outcomes["sql"][0][TABLE][0] == "caught_up"


def test_a_failed_batched_plan_falls_back_per_table(fresh, monkeypatch):
    """M3: a planner error (BinderException) from the batched statement: each table plans
    alone (plan_sql) and applies on SQL; the mirrors equal the Python fold's."""
    real = sql_fold.page_plan_sql
    monkeypatch.setattr(sql_fold, "page_plan_sql", lambda *a, **k: "SELECT no_such_column")
    env = two_tables(fresh("sql"), "sql")
    assert {r.status for r in env.apply.run(tables=TABLES)} == {"caught_up"}
    assert env.apply.plan_stats["batched_failed"] == 1
    assert env.apply.fold_stats == {"fallback:batched_plan": 1, "sql": 2}
    sql_mirrors = (env.mirror(), env.mirror("others"), _state_rows(env))
    monkeypatch.setattr(sql_fold, "page_plan_sql", real)
    twin = two_tables(fresh("python"), "python")
    twin.apply.run(tables=TABLES)
    assert sql_mirrors == (twin.mirror(), twin.mirror("others"), _state_rows(twin))


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_a_stop_during_the_batched_plan_propagates(stores, monkeypatch):
    """M3: an interrupt (a soft stop or deadline) is never swallowed into a fallback."""
    env = two_tables(stores, "sql")
    real = env.apply.execute

    def execute(statement, parameters=None):
        if statement.startswith(PLAN):
            raise duckdb.InterruptException("stopped")
        return real(statement, parameters)

    monkeypatch.setattr(env.apply, "execute", execute)
    with pytest.raises(duckdb.InterruptException):
        env.apply.run(tables=TABLES)
    assert env.apply.plan_stats["batched_failed"] == 0


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_a_table_at_another_position_plans_its_own_page(stores, monkeypatch):
    """S4: the cache key is the page: a table that is ahead plans (and reads descriptors
    for) its own page; tables at the same position share."""
    env = two_tables(stores, "sql", commits=2)
    env.apply.apply_table(TABLE, "ahead")  # TABLE moves past the first page
    for k in range(2, 4):
        env.commit(ins(str(k), f"a{k}"), oth(str(k), f"b{k}"))
    seen = Statements(monkeypatch, env.apply)
    env.apply.plan_stats.update(fills=0, hits=0)
    assert {r.status for r in env.apply.run(tables=TABLES)} == {"caught_up"}
    assert env.apply.plan_stats["fills"] == seen.count(PLAN) == 2  # two different pages
    assert env.apply.plan_stats["hits"] == 0
    assert env.mirror() == {str(k): f"a{k}" for k in range(4)}
    assert env.mirror("others") == {str(k): f"b{k}" for k in range(4)}


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_a_quarantined_unit_skips_for_one_table_while_the_other_shares_the_plan(stores):
    """SQL twin of the cache's quarantine test: TABLE's page has a quarantined unit and
    falls back; OTHER_TABLE applies the page on SQL."""
    env = two_tables(stores, "sql")
    first, second = env.units()
    m = f"{env.apply.mirror}._cdc"
    generation = env.store.con.execute(
        f"SELECT generation_id FROM {m}.apply_state WHERE relation_id=?", [OID]
    ).fetchone()[0]
    env.store.con.execute(
        f"INSERT INTO {m}.quarantine (relation_id,generation_id,epoch,commit_lsn,reason,actor,"
        "run_id,created_at,unit_start_lsn,unit_end_lsn,unit_digest) "
        "VALUES (?,?,1,?,'t','t','q',current_timestamp,?,?,?)",
        [OID, generation, second[0] + 10, *second],
    )
    results = {r.table: r for r in env.apply.run(tables=TABLES)}
    assert results[TABLE].boundary == results[OTHER_TABLE].boundary == second[1]
    assert env.mirror() == {"0": "a0", "1": "a1"}  # unit 2 skipped for TABLE only
    assert env.mirror("others") == {str(k): f"b{k}" for k in range(4)}
    assert env.apply.fold_stats == {"fallback:quarantine": 1, "sql": 1}


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_an_idle_table_on_a_planned_page_costs_no_statement(stores, monkeypatch):
    """SF1: OTHER has no events on the page (no descriptor there): it takes zero facts from
    the batched plan, not a whole-page plan of its own."""
    env = Env(stores, fold="sql", max_commits=2)
    for table in TABLES:
        env.apply.start_from_log(table)
    for k in range(4):
        env.commit(ins(str(k), f"a{k}"))  # only TABLE changes
    seen = Statements(monkeypatch, env.apply)
    assert {r.status for r in env.apply.run(tables=TABLES)} == {"caught_up"}
    assert seen.count(PLAN) == 1  # one plan for the page, none for the idle table
    assert env.apply.fold_stats == {"sql": 2}
    assert env.apply.plan_stats == {"fills": 1, "hits": 1, "batched_failed": 0}
    assert env.mirror() == {str(k): f"a{k}" for k in range(4)} and env.mirror("others") == {}
