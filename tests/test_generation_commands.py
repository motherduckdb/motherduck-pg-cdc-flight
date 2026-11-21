"""The generation commands' spec, adapter and inventory, with negative canaries.

The judges are plain functions (`judge_*`) that raise AssertionError when the property
fails; each test runs one on the real code, and each canary seeds the fault it exists for
and asserts the judge then fails. Local DuckDB only (the adapter's own store)."""

import ast
import re
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import duckdb
import pytest

from cdc_flight import generation_commands as commands
from cdc_flight import generation_spec as spec
from cdc_flight import generations
from cdc_flight.apply import Apply, FenceLost, LeaseBusy
from cdc_flight.backfill import GENERATION_COLUMNS, Backfill
from cdc_flight.generation_spec import IllegalTransition, InvariantViolation

SRC = Path(__file__).resolve().parents[1] / "src" / "cdc_flight"
GEN = "g1"
REL = 7

# The legal edges, written by hand and independent of generation_spec: widening the spec
# alone fails judge_edges (canary 1). {command: (from states, to state)}; None: no row.
ACTIVE_ = ("snapshotting", "catching_up")
EDGES = {
    "create": ((None,), "snapshotting"),
    "create_append": ((None,), "snapshotting"),
    "acquire": (ACTIVE_, None),
    "guard": (ACTIVE_, None),
    "release": (
        ("snapshotting", "catching_up", "published", "failed", "abandoned", "replaced"),
        None,
    ),
    "progress_scan": (("snapshotting",), None),
    "finish_scan": (("snapshotting",), "catching_up"),
    "progress_catchup": (("catching_up",), None),
    "fail": (ACTIVE_, "failed"),
    "fail_expiry": (ACTIVE_, "failed"),
    "abandon": (ACTIVE_, "abandoned"),
    "publish": (("catching_up",), "published"),
    "replace": (("published",), "replaced"),
    "create_log_only": ((None,), "published"),
    "create_log_only_append": ((None,), "published"),
}
DEFERRED = set()  # publish, replace and the log-only starts are in the spec


class Store:
    """The adapter's Owner over a local mirror schema with the real DDL."""

    def __init__(self):
        self.con = duckdb.connect()
        self.con.execute("CREATE SCHEMA m")
        for name in ("generations", "backfills"):
            self.con.execute(Backfill.MIRROR_TABLES[name].replace("{m}", "m."))
        for column, dtype in GENERATION_COLUMNS.items():
            self.con.execute(f"ALTER TABLE m.generations ADD COLUMN {column} {dtype}")
        self.config = SimpleNamespace(lease_seconds=60)

    def execute(self, statement, parameters=None):
        return self.con.execute(statement.replace("{m}", "m."), parameters or [])

    @contextmanager
    def transaction(self):
        self.con.execute("BEGIN")
        try:
            yield
            self.con.execute("COMMIT")
        except BaseException:
            self.con.execute("ROLLBACK")
            raise

    def seed(self, state, *, token=1, live=True, claim=None):
        """A generation in `state` leased by `token` (an unexpired lease unless not
        `live`), with its claim when active (or `claim`: True/False/another id)."""
        self.con.execute(
            "INSERT INTO m.generations (generation_id,relation_id,mode,epoch,pin_lsn,state,"
            "chunks,rows,catchup_lsn,token,holder,expires_at,guard,scan_done) VALUES "
            f"(?,?,'snapshot',1,0,?,0,0,0,?,'run',current_timestamp+INTERVAL "
            f"'{60 if live else -60} second','g0',false)",
            [GEN, REL, state, token],
        )
        claim = state in spec.ACTIVE if claim is None else claim
        if claim:
            other = claim if isinstance(claim, str) else GEN
            self.con.execute("INSERT INTO m.backfills VALUES (?,?)", [REL, other])

    def gen(self):
        row = self.con.execute(
            "SELECT generation_id,relation_id,state,chunks,catchup_lsn,token,holder,detail,"
            "marker,guard,expires_at>current_timestamp FROM m.generations"
        ).fetchone()
        names = "generation_id relation_id state chunks catchup_lsn token holder detail"
        return dict(zip((*names.split(), "marker", "guard", "live"), row))

    def snapshot(self):
        return (
            self.con.execute(
                "SELECT * REPLACE (created_at::VARCHAR AS created_at, "
                "expires_at::VARCHAR AS expires_at, updated_at::VARCHAR AS updated_at) "
                "FROM m.generations ORDER BY ALL"
            ).fetchall(),
            self.con.execute("SELECT * FROM m.backfills ORDER BY ALL").fetchall(),
        )


def held(store, token=1):
    return commands.held(store, GEN, token, 60, FenceLost)


# A run of each adapter command from the observed row (inside held() where it needs one).
def run(store, name, token=1):
    gen = store.gen()
    if name == "acquire":
        return commands.acquire(store, GEN, "other", 60, gen["token"], LeaseBusy)
    if name == "guard":
        with held(store, token):
            return None
    if name == "release":
        return commands.release(store, GEN, token)
    with held(store, token) as lease:
        if name == "progress_scan":
            return lease.progress_scan(gen, chunks=gen["chunks"] + 1)
        if name == "finish_scan":
            return lease.finish_scan(gen, marker=GEN, wal_before_marker=1)
        if name == "progress_catchup":
            return lease.progress_catchup(gen, catchup_lsn=gen["catchup_lsn"] + 1)
        return lease.end(name, gen, "test")


RUNNABLE = (
    "acquire",
    "guard",
    "progress_scan",
    "finish_scan",
    "progress_catchup",
    "fail",
    "fail_expiry",
    "abandon",
)
ERRORS = (FenceLost, LeaseBusy, InvariantViolation, IllegalTransition)


# Judges ---------------------------------------------------------------------------------


def judge_edges():
    actual = {name: (tuple(c.from_states), c.to_state) for name, c in spec.COMMANDS.items()}
    assert actual == EDGES, "generation_spec's edges differ from the independent table"
    assert {n for n, c in spec.COMMANDS.items() if c.status == "deferred_s4"} == DEFERRED


def judge_sql_predicates(monkeypatch):
    """With the Python check off, every illegal (command, state) still writes nothing: the
    SQL predicate (or the guard's) refuses it."""
    monkeypatch.setattr(commands, "check", lambda name, observed, columns=(): spec.command(name))
    for name in RUNNABLE:
        for state in spec.STATES:
            if state in EDGES[name][0]:
                continue
            store = Store()
            # acquire needs a free lease to reach its state predicate.
            store.seed(state, live=name != "acquire", claim=True)
            before = store.snapshot()
            try:
                run(store, name)
            except ERRORS:
                pass
            else:
                raise AssertionError(f"{name} from {state} was not refused")
            assert store.snapshot() == before, (name, state)


def judge_counts_checked():
    """A progress compare-and-set that matches nothing after a successful guard (a stale
    read of our own) is an InvariantViolation, and nothing commits."""
    store = Store()
    store.seed("snapshotting")
    stale = store.gen()
    store.con.execute("UPDATE m.generations SET chunks=5")
    before = store.snapshot()
    try:
        with held(store) as lease:
            lease.progress_scan(stale, chunks=1)
    except InvariantViolation:
        pass
    else:
        raise AssertionError("a stale progress write was not refused")
    assert store.snapshot() == before


def judge_expired_lease():
    """A lease that expired, even if no one took it, commits nothing."""
    store = Store()
    store.seed("catching_up", live=False)
    before = store.snapshot()
    try:
        with held(store) as lease:
            lease.progress_catchup(store.gen(), catchup_lsn=9)
    except FenceLost:
        pass
    else:
        raise AssertionError("an expired lease committed")
    assert store.snapshot() == before


SUPPRESSION = re.compile(r"#\s*(type:\s*ignore|pyright\s*:)")
STRICT = (
    "generation_spec.py",
    "generation_commands.py",
    "publication_commands.py",
    "lifecycle_spec.py",
    "lifecycle_commands.py",
)


def judge_no_suppressions(sources):
    for name, text in sources.items():
        for number, line in enumerate(text.splitlines(), 1):
            assert not SUPPRESSION.search(line), f"{name}:{number}: checker suppression"


WRITE = re.compile(
    r"\b(INSERT\s+INTO|UPDATE|DELETE\s+FROM)\s+[^\s(]*?\b(generations|backfills)\b", re.I
)


def write_sites(sources):
    """{module:qualname: write statements}: string literals (f-string parts joined) that
    write {m}generations or {m}backfills in any spelling, by enclosing function, or by the
    module-level name they are assigned to."""
    found = {}
    for module, text in sources.items():
        tree = ast.parse(text)

        def visit(node, scope):
            for child in ast.iter_child_nodes(node):
                name = scope
                if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                    name = f"{scope}.{child.name}" if scope else child.name
                elif isinstance(child, ast.Assign) and not scope:
                    name = ",".join(t.id for t in child.targets if isinstance(t, ast.Name))
                if isinstance(child, (ast.Constant, ast.JoinedStr)):
                    text = (
                        child.value
                        if isinstance(child, ast.Constant)
                        else "".join(
                            v.value if isinstance(v, ast.Constant) else "{x}" for v in child.values
                        )
                    )
                    if isinstance(text, str) and WRITE.search(text):
                        key = f"{module}:{name}"
                        found[key] = found.get(key, 0) + 1
                    continue
                visit(child, name)

        visit(tree, "")
    return found


def sources():
    return {p.stem: p.read_text() for p in sorted(SRC.glob("*.py"))}


def judge_inventory(found):
    expected = {key: site.writes for key, site in spec.INVENTORY.items()}
    unlisted = {k: v for k, v in found.items() if k not in expected}
    stale = {k: v for k, v in expected.items() if k not in found}
    assert not unlisted, f"generation writes missing from generation_spec.INVENTORY: {unlisted}"
    assert not stale, f"INVENTORY entries with no write site: {stale}"
    assert found == expected, (found, expected)


# Tests ----------------------------------------------------------------------------------


def test_spec_edges_match_the_independent_table():
    judge_edges()


@pytest.mark.parametrize("name", sorted(EDGES))
@pytest.mark.parametrize("state", (None, *spec.STATES))
def test_python_check_refuses_exactly_the_illegal_states(name, state):
    legal = state in EDGES[name][0] and name not in DEFERRED
    if legal:
        spec.check(name, state)
    else:
        with pytest.raises(IllegalTransition):
            spec.check(name, state)


def test_sql_predicates_refuse_illegal_states_without_the_python_check(monkeypatch):
    judge_sql_predicates(monkeypatch)


def test_an_unexpected_count_after_the_guard_is_an_invariant_violation():
    judge_counts_checked()


def test_an_expired_lease_nobody_took_commits_nothing():
    judge_expired_lease()


def test_inventory_lists_every_generation_write_both_ways():
    judge_inventory(write_sites(sources()))


def test_no_checker_suppressions_in_the_strict_modules():
    judge_no_suppressions({n: (SRC / n).read_text() for n in STRICT})


def test_columns_outside_the_spec_are_refused():
    store = Store()
    store.seed("snapshotting")
    before = store.snapshot()
    with pytest.raises(IllegalTransition):
        with held(store) as lease:
            lease.progress_scan(store.gen(), state="published")
    with pytest.raises(IllegalTransition):
        with held(store) as lease:
            lease.progress_scan(store.gen(), catchup_lsn=5)  # a catch-up column
    assert store.snapshot() == before


def test_active_is_one_ordered_tuple_everywhere():
    assert generations.ACTIVE is spec.ACTIVE == ("snapshotting", "catching_up")
    assert generations.ENDED is spec.ENDED
    assert spec.states_sql(spec.ACTIVE) in commands.GUARD


# Per command: rollback, repeat, stale lease, crash ---------------------------------------


@pytest.mark.parametrize("name", ["progress_scan", "finish_scan", "fail", "fail_expiry", "abandon"])
def test_rollback_after_the_command_leaves_nothing(name):
    state = "snapshotting"
    store = Store()
    store.seed(state)
    before = store.snapshot()
    with pytest.raises(RuntimeError, match="crash"):
        with held(store) as lease:
            gen = store.gen()
            if name == "progress_scan":
                lease.progress_scan(gen, chunks=1)
            elif name == "finish_scan":
                lease.finish_scan(gen, marker=GEN)
            else:
                lease.end(name, gen, "x")
            raise RuntimeError("crash")
    assert store.snapshot() == before


def test_rollback_of_a_catch_up_checkpoint():
    store = Store()
    store.seed("catching_up")
    before = store.snapshot()
    with pytest.raises(RuntimeError):
        with held(store) as lease:
            lease.progress_catchup(store.gen(), catchup_lsn=3)
            raise RuntimeError("crash")
    assert store.snapshot() == before


def test_create_rolls_back_and_a_second_claim_is_refused():
    store = Store()
    claim = "INSERT INTO {m}backfills (relation_id,generation_id) VALUES (?,?)"
    with pytest.raises(RuntimeError):
        with store.transaction():
            commands.inserted(store, "create", "claim_insert", claim, [REL, GEN])
            raise RuntimeError("crash")
    assert store.snapshot() == ([], [])
    commands.inserted(store, "create", "claim_insert", claim, [REL, GEN])
    with pytest.raises(duckdb.ConstraintException):  # repeat: the claim's primary key
        commands.inserted(store, "create", "claim_insert", claim, [REL, "g2"])


@pytest.mark.parametrize(
    "name", ["progress_scan", "finish_scan", "progress_catchup", "fail", "fail_expiry", "abandon"]
)
def test_a_repeat_with_the_old_row_is_refused(name):
    state = "catching_up" if name == "progress_catchup" else "snapshotting"
    store = Store()
    store.seed(state)
    old = store.gen()
    run(store, name)
    after = store.snapshot()
    with pytest.raises((InvariantViolation, FenceLost, IllegalTransition)):
        with held(store) as lease:
            if name == "progress_scan":
                lease.progress_scan(old, chunks=1)
            elif name == "finish_scan":
                lease.finish_scan(old, marker=GEN)
            elif name == "progress_catchup":
                lease.progress_catchup(old, catchup_lsn=1)
            else:
                lease.end(name, old, "again")
    assert store.snapshot() == after


def test_acquire_repeat_and_stale_tokens_are_busy():
    store = Store()
    store.seed("snapshotting", live=False)
    token = commands.acquire(store, GEN, "run", 60, 1, LeaseBusy)
    assert token == 2
    with pytest.raises(LeaseBusy):  # held now
        commands.acquire(store, GEN, "other", 60, 2, LeaseBusy)
    with pytest.raises(LeaseBusy):  # an old token
        commands.acquire(store, GEN, "other", 60, 1, LeaseBusy)


@pytest.mark.parametrize("name", RUNNABLE[1:])
def test_another_runs_token_commits_nothing(name):
    state = "catching_up" if name == "progress_catchup" else "snapshotting"
    store = Store()
    store.seed(state, token=2)
    before = store.snapshot()
    with pytest.raises(FenceLost):
        run(store, name, token=1)
    assert store.snapshot() == before


def test_release_is_unchecked_and_keeps_the_token():
    store = Store()
    store.seed("failed")
    commands.release(store, GEN, 9)  # someone else's: matches nothing, no error
    assert store.gen()["holder"] == "run"
    commands.release(store, GEN, 1)
    gen = store.gen()
    assert (gen["holder"], gen["live"], gen["token"]) == (None, None, 1)


def test_a_missing_claim_is_committed_and_reported():
    store = Store()
    store.seed("snapshotting", claim=False)
    with held(store) as lease:
        assert lease.end("fail", store.gen(), "boom") is True
    gen = store.gen()
    assert (gen["state"], gen["detail"]) == ("failed", "boom; claim was already missing")


def test_a_claim_naming_another_generation_is_an_invariant_violation():
    store = Store()
    store.seed("snapshotting", claim="g2")
    before = store.snapshot()
    with pytest.raises(InvariantViolation, match="names generation g2"):
        with held(store) as lease:
            lease.end("abandon", store.gen(), "x")
    assert store.snapshot() == before


def test_expiry_fail_cas_losing_to_the_backfill_is_fence_lost():
    """fail_expiry's 0 is a race (race_zero): the state moved after expiry read it."""
    store = Store()
    store.seed("catching_up")
    read_before = dict(store.gen(), state="snapshotting")
    before = store.snapshot()
    with pytest.raises(FenceLost):
        with held(store) as lease:
            lease.end("fail_expiry", read_before, "lag")
    assert store.snapshot() == before


def test_write_opens_with_the_guard_and_ends_with_the_checkpoint():
    """lease() relies on _write's order: guard() first and commit_state() last in its
    transaction, for every _write that catch-up uses."""
    files = {
        "apply.py": "Apply._write",
        "sql_path.py": "SqlWrite._write",
        "append_apply.py": "write_append",
    }
    for file, qualname in files.items():
        tree = ast.parse((SRC / file).read_text())
        func = _find(tree, qualname)
        blocks = [
            n
            for n in ast.walk(func)
            if isinstance(n, ast.With)
            and any("transaction" in ast.unparse(item.context_expr) for item in n.items)
        ]
        assert blocks, qualname
        for block in blocks:
            calls = [
                ast.unparse(s.value)
                for s in block.body
                if isinstance(s, ast.Expr) and isinstance(s.value, ast.Call)
            ]
            assert calls and calls[0] == "guard()", (qualname, calls)
            assert "commit_state()" in ast.unparse(block.body[-1]), qualname


def _find(tree, qualname):
    node = tree
    for part in qualname.split("."):
        node = next(
            n
            for n in ast.iter_child_nodes(node)
            if isinstance(n, (ast.FunctionDef, ast.ClassDef)) and n.name == part
        )
    return node


# Negative canaries: each seeds its fault and the judge must fail -------------------------


def test_canary_illegal_transition_is_caught(monkeypatch):
    widened = spec.Command(
        "fail", "spec", (*spec.ACTIVE, "published"), "failed", "held", ("state", "detail")
    )
    monkeypatch.setitem(spec.COMMANDS, "fail", widened)
    with pytest.raises(AssertionError):
        judge_edges()


def test_canary_unlisted_write_is_caught():
    seeded = dict(sources())
    seeded["seeded"] = "def f(o):\n    o.execute(\"UPDATE _cdc.generations SET state='failed'\")\n"
    with pytest.raises(AssertionError, match="missing from"):
        judge_inventory(write_sites(seeded))


def test_canary_stale_inventory_entry_is_caught():
    found = write_sites(sources())
    found.pop("backfill:Backfill._start")
    with pytest.raises(AssertionError, match="no write site"):
        judge_inventory(found)


def test_canary_dropped_count_check_is_caught(monkeypatch):
    monkeypatch.setattr(commands, "checked", lambda *args: None)
    with pytest.raises(AssertionError):
        judge_counts_checked()


def test_canary_stale_lease_committing_is_caught(monkeypatch):
    monkeypatch.setattr(
        commands, "GUARD", commands.GUARD.replace("AND expires_at>current_timestamp ", "")
    )
    with pytest.raises(AssertionError):
        judge_expired_lease()


def test_canary_dropped_sql_predicate_is_caught(monkeypatch):
    monkeypatch.setattr(commands, "states_sql", lambda states: str(spec.STATES))
    monkeypatch.setattr(
        commands, "GUARD", commands.GUARD.replace(spec.states_sql(spec.ACTIVE), str(spec.STATES))
    )
    with pytest.raises(AssertionError):
        judge_sql_predicates(monkeypatch)


@pytest.mark.parametrize(
    "line",
    [
        "x = 1  # type: ignore",
        "x = 1  # pyright: ignore[reportGeneralTypeIssues]",
        "# pyright: basic",
    ],
)
def test_canary_checker_suppression_is_caught(line):
    with pytest.raises(AssertionError, match="suppression"):
        judge_no_suppressions({"seeded.py": f"{line}\n"})


def test_apply_mirror_ddl_is_out_of_scope_of_the_inventory():
    """The DDL (CREATE / ALTER ADD COLUMN) writes no rows: the scan only matches
    INSERT/UPDATE/DELETE."""
    assert not WRITE.search(Apply.MIRROR_TABLES["generations"])
    assert not WRITE.search(Backfill.MIRROR_TABLES["backfills"])


# Coverage: every adapter command x {rollback, repeat, stale lease, crash} names its test
# (file::function) or says why it doesn't apply. Checked to be complete and to exist.
HERE = "test_generation_commands.py::"
KINDS = ("rollback", "repeat", "stale_lease", "crash")
COVERAGE = {
    "create": {
        "rollback": HERE + "test_create_rolls_back_and_a_second_claim_is_refused",
        "repeat": HERE + "test_create_rolls_back_and_a_second_claim_is_refused",
        "stale_lease": "n/a: create takes no lease; the claim's primary key fences a second run",
        "crash": "test_backfill.py::test_ambiguous_backfill_start_keeps_its_pin",
    },
    "create_append": {
        "rollback": HERE + "test_create_rolls_back_and_a_second_claim_is_refused",
        "repeat": HERE + "test_create_rolls_back_and_a_second_claim_is_refused",
        "stale_lease": "n/a: its INSERT takes the lease; every later write is guarded",
        "crash": "test_append_snapshot.py::test_snapshot_end_to_end_with_racing_commits",
    },
    "acquire": {
        "rollback": "n/a: one statement, or the UPDATE and its token read in one transaction",
        "repeat": HERE + "test_acquire_repeat_and_stale_tokens_are_busy",
        "stale_lease": HERE + "test_acquire_repeat_and_stale_tokens_are_busy",
        "crash": HERE + "test_acquire_repeat_and_stale_tokens_are_busy",  # an expired lease
    },
    "guard": {
        "rollback": HERE + "test_rollback_after_the_command_leaves_nothing",
        "repeat": "n/a: a repeat renews again (no state change)",
        "stale_lease": HERE + "test_an_expired_lease_nobody_took_commits_nothing",
        # after_chunk_insert: a crash inside a guarded transaction rolls the guard back too.
        "crash": "test_backfill.py::test_backfill_under_load_crashes_and_publication",
    },
    "release": {
        "rollback": "n/a: one autocommit statement",
        "repeat": HERE + "test_release_is_unchecked_and_keeps_the_token",
        "stale_lease": HERE + "test_release_is_unchecked_and_keeps_the_token",
        "crash": "n/a: a lost release expires after lease_seconds (acquire's crash test)",
    },
    "progress_scan": {
        "rollback": HERE + "test_rollback_after_the_command_leaves_nothing",
        "repeat": HERE + "test_a_repeat_with_the_old_row_is_refused",
        "stale_lease": HERE + "test_another_runs_token_commits_nothing",
        "crash": "test_backfill.py::test_backfill_under_load_crashes_and_publication",
    },
    "finish_scan": {
        "rollback": HERE + "test_rollback_after_the_command_leaves_nothing",
        "repeat": HERE + "test_a_repeat_with_the_old_row_is_refused",
        "stale_lease": HERE + "test_another_runs_token_commits_nothing",
        "crash": "test_append_snapshot.py::test_a_unit_spanning_c_across_a_resumed_catch_up",
    },
    "progress_catchup": {
        "rollback": HERE + "test_rollback_of_a_catch_up_checkpoint",
        "repeat": HERE + "test_a_repeat_with_the_old_row_is_refused",
        "stale_lease": "test_backfill_sql_fold.py::test_an_expired_lease_nobody_took_commits_no_catch_up_batch",
        "crash": "test_backfill_sql_fold.py::test_a_crash_after_a_commit_resumes_at_the_checkpoint",
    },
    "fail": {
        "rollback": HERE + "test_rollback_after_the_command_leaves_nothing",
        "repeat": HERE + "test_a_repeat_with_the_old_row_is_refused",
        "stale_lease": HERE + "test_another_runs_token_commits_nothing",
        # after_end_commit: fail and abandon share Backfill.end.
        "crash": "test_backfill.py::test_generation_pins_register_release_and_sweep",
    },
    "fail_expiry": {
        "rollback": "test_expiry.py::test_a_conflict_while_failing_a_generation_rolls_it_back",
        "repeat": HERE + "test_a_repeat_with_the_old_row_is_refused",
        "stale_lease": HERE + "test_another_runs_token_commits_nothing",
        "crash": "test_expiry.py::test_an_invariant_violation_failing_one_generation_spares_the_rest_of_the_run",
    },
    "abandon": {
        "rollback": HERE + "test_rollback_after_the_command_leaves_nothing",
        "repeat": HERE + "test_a_repeat_with_the_old_row_is_refused",
        "stale_lease": HERE + "test_another_runs_token_commits_nothing",
        "crash": "test_backfill.py::test_generation_pins_register_release_and_sweep",
    },
    # (tests/test_publication_commands.py)
    "publish": {
        "rollback": "test_publication_commands.py::test_a_fault_inside_publish_rolls_every_entity_back",
        "repeat": "test_publication_commands.py::test_publish_again_is_an_illegal_transition_before_any_statement",
        "stale_lease": "test_publication_commands.py::test_an_expired_lease_nobody_took_never_publishes",
        "crash": "test_backfill.py::test_resync_replaces_stale_mirror_and_fences_old_writers",
    },
    "replace": {
        "rollback": "test_publication_commands.py::test_a_fault_inside_publish_rolls_every_entity_back",
        "repeat": "test_publication_commands.py::test_replace_of_anything_but_published_is_an_invariant_violation",
        "stale_lease": "n/a: replace takes no lease; the caller's apply guard fences it",
        "crash": "n/a: one statement inside the caller's transaction",
    },
    "create_log_only": {
        "rollback": "test_publication_commands.py::test_start_from_log_rolls_back_both_rows",
        "repeat": "test_publication_commands.py::test_start_from_log_rolls_back_both_rows",
        "stale_lease": "n/a: no lease; the existence check in the transaction excludes",
        "crash": "n/a: no fault point between its two INSERTs; a failure there rolls back "
        "(test_start_from_log_rolls_back_both_rows), and after the commit ensure_pin completes "
        "the pin transfer",
    },
    "create_log_only_append": {
        "rollback": "test_publication_commands.py::test_start_append_replace_rolls_back_and_releases_its_lease",
        "repeat": "test_keyless_append.py::test_start_from_log_replaces_a_generation_that_cannot_apply",
        "stale_lease": "test_publication_commands.py::test_start_append_with_an_expired_lease_nobody_took_writes_nothing",
        "crash": "n/a: one transaction, no fault point; a failure inside rolls back "
        "(test_start_append_replace_rolls_back_and_releases_its_lease), and after the commit "
        "the pin transfer releases the old generation's pins",
    },
}


def test_every_command_has_rollback_repeat_stale_lease_and_crash_coverage():
    commands_in_spec = {n for n, c in spec.COMMANDS.items() if c.status == "spec"}
    assert set(COVERAGE) == commands_in_spec
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
