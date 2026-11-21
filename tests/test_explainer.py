"""Flight Explainer instrumentation: charts, progress lines, and their agreement.

Charts are flights/charts/<command>.mmd (passed as the Flight's metadata.chart). The
drift tests run the same explain.* functions the CLI runs, on real local or PG
scenarios, capture stdout, and check every line against that command's chart the way
the Flight Explainer does (MotherDuck's get_flight_viz_guide). They also require the
nodes and branches each scenario must reach, so lost instrumentation fails here.
"""

import os
import re
import shlex
import subprocess
import sys
import textwrap
import uuid
from datetime import datetime
from pathlib import Path

import conftest
import duckdb
import pytest
from psycopg2.extensions import parse_dsn
from test_backfill import populated  # noqa: F401 (fixture)
from test_phase1 import pg_phase1  # noqa: F401 (fixture)
from test_phase2 import ITEMS, OTHER, TABLES, Harness, relation_message

from cdc_flight import explain, flightlog
from cdc_flight.bootstrap import bootstrap_log
from cdc_flight.chain import ApplyChain, BackfillChain, after_backfill, after_ingest
from cdc_flight.flightlog import Run, Stages
from cdc_flight.ingest import run_once
from cdc_flight.preflight import PreflightError, connect, query
from cdc_flight.runtime import open_log

ROOT = Path(__file__).resolve().parents[1]
COMMANDS = (
    "preflight",
    "ingest",
    "apply",
    "backfill",
    "compact",
    "table-action",
    "table-membership",
    "quarantine",
    "epoch",
    "delete-source",
)
KINDS = {"query", "extract", "load", "transform", "check", "switch", "wait", "flight"}
METERS = {"fraction", "percent", "fanout"}
NODE = r"[A-Za-z][A-Za-z0-9_-]*"
FIELDS = {"key", "rows", "progress", "done", "total", "error", "effect", "branch"}
FIELDS |= {"flight", "run", "ts"}


# Charts ------------------------------------------------------------------------


def parse_chart(text):
    """{id: set(classes)} and [(source, label, target)], per the convention's grammar."""
    lines = [line.strip() for line in text.strip().splitlines()]
    assert re.fullmatch(r"flowchart (TD|LR)", lines[0]), lines[0]
    node = rf"({NODE})(?:\[[^\]]*\]|\{{[^}}]*\}}|\([^)]*\))?(?::::(\w+))?"
    nodes, edges = {}, []

    def declare(match):
        nodes.setdefault(match.group(1), set())
        if match.group(2):
            nodes[match.group(1)].add(match.group(2))
        return match.group(1)

    for line in lines[1:]:
        if not line:
            continue
        assert not line.startswith(("classDef", "style")), line
        if line.startswith("class "):
            _, ids, name = line.split()
            for i in ids.split(","):
                nodes.setdefault(i, set()).add(name)
            continue
        edge = re.fullmatch(rf"{node}\s*-->(?:\|([^|]+)\|)?\s*{node}", line)
        if edge:
            a = declare(re.match(node, edge.group(0)))
            b = re.search(rf"-->(?:\|[^|]+\|)?\s*({node})$", line)
            target = declare(re.fullmatch(node, b.group(1)))
            edges.append((a, edge.group(3), target))
            continue
        alone = re.fullmatch(node, line)
        assert alone, f"unparsed chart line: {line}"
        declare(alone)
    return nodes, edges


def chart(command):
    return parse_chart((ROOT / "flights/charts" / f"{command}.mmd").read_text())


@pytest.mark.parametrize("command", COMMANDS)
def test_chart_is_well_formed(command):
    nodes, edges = chart(command)
    for node_id, classes in nodes.items():
        assert re.fullmatch(NODE, node_id)
        assert len(classes & KINDS) == 1, (node_id, classes)
        assert len(classes & METERS) <= 1, (node_id, classes)
        assert classes <= KINDS | METERS, (node_id, classes)
    for source, label, target in edges:
        assert source in nodes and target in nodes
        if label:
            assert nodes[source] & {"check", "switch"}, (source, label)


def test_build_writes_the_chart_next_to_the_stub(tmp_path):
    from flight_stub import SHA, VERSION, build_flight

    out = tmp_path / "ingest_flight.py"
    build_flight.write("ingest", out, SHA, VERSION)
    chart_file = out.with_suffix(".chart.mmd")
    assert chart_file.read_text() == (ROOT / "flights/charts/ingest.mmd").read_text()
    assert "@flight-run" not in chart_file.read_text()
    # The chart is never in the source.
    assert "flowchart" not in out.read_text()


def test_every_mapped_stage_is_called_in_the_product():
    """A renamed product stage fails here, not only in the drift scenarios that
    happen to reach it."""
    source = "".join(p.read_text() for p in (ROOT / "src/cdc_flight").glob("*.py"))
    adapters = [
        explain.preflight_stages(Run()),
        explain.ingest_stages(Run(), None),
        explain.backfill_stages(Run(), 0, None, "m.s.t"),
    ]
    for adapter in adapters:
        for stage in adapter.actions:
            assert f'fault("{stage}")' in source, stage


# Lines -------------------------------------------------------------------------


def parse_line(text):
    """(node, status, {field: [values]}), or raise on anything malformed."""
    tokens = shlex.split(text)
    assert tokens[0] == "@flight-run", text
    node, status = tokens[1], tokens[2]
    assert re.fullmatch(NODE, node) and status in flightlog.STATUSES, text
    fields = {}
    for token in tokens[3:]:
        name, _, value = token.partition("=")
        assert name in FIELDS, text
        fields.setdefault(name, []).append(value)
    for name in ("rows", "done", "total", "run"):
        for value in fields.get(name, ()):
            assert re.fullmatch(r"-?\d+", value), text
    for value in fields.get("ts", ()):
        datetime.fromisoformat(value.replace("Z", "+00:00"))
    assert "ts" in fields, text
    return node, status, fields


def drift(command, output, required=()):
    """Problems the Flight Explainer would report, plus missing required nodes and
    nodes left running."""
    nodes, edges = chart(command)
    problems, state = [], {}
    for text in output.splitlines():
        if not text.startswith("@flight-run"):
            continue
        node, status, fields = parse_line(text)
        if node not in nodes:
            problems.append(f"{node} is not in the {command} chart")
            continue
        key = fields.get("key", ["_"])[0]
        state[(node, key)] = status
        kinds = nodes[node] & KINDS
        effects = fields.get("effect", [])
        if kinds & {"transform", "check", "switch"} and any(
            e.startswith(("table:", "file:")) for e in effects
        ):
            problems.append(f"{node} ({kinds}) has a storage effect")
        if "flight" in kinds and status in ("ok", "running") and "flight" not in fields:
            problems.append(f"{node} has no flight=")
        if "flight" in kinds and status == "ok" and "run" not in fields:
            problems.append(f"{node} has no run=")
        # done= alone feeds no meter, except on a fraction/percent node whose total is
        # unknown for now (backfill's scan when the table was never analyzed).
        meter = nodes[node] & {"fraction", "percent"}
        if "done" in fields and "total" not in fields and not meter:
            problems.append(f"{node} has done= without total=, which no meter reads")
        for branch in fields.get("branch", []):
            if (node, branch) not in {(s, label) for s, label, _ in edges if label}:
                problems.append(f"{node} branch={branch} is no labelled edge")
    problems += [f"{n} key={k} left running" for (n, k), s in state.items() if s == "running"]
    finished = {n for (n, _), s in state.items() if s != "running"}
    problems += [f"required {n} never finished" for n in required if n not in finished]
    return problems


def branches(output):
    taken = set()
    for text in output.splitlines():
        if text.startswith("@flight-run"):
            node, _, fields = parse_line(text)
            taken |= {(node, b) for b in fields.get("branch", [])}
    return taken


def assert_no_secrets(output, dsn):
    options = parse_dsn(dsn)
    secrets = [dsn, *(options.get(k) for k in ("password", "user", "host", "port"))]
    secrets.append(os.getenv("MOTHERDUCK_TOKEN"))
    for secret in filter(None, secrets):
        assert secret not in output, "a DSN part or token appeared in progress lines"


def test_lines_are_well_formed_and_quoted(capsys):
    run = Run()
    run.step("table", "ok", key="public.items", rows=3, effect=["table:m.public.items"])
    run.step("trigger", "failed", flight="apply", error='a "b" = c')
    out = capsys.readouterr().out.splitlines()
    assert parse_line(out[0])[2]["key"] == ["public.items"]
    assert parse_line(out[1])[2]["error"] == ['a "b" = c']


def test_drift_check_reports_what_the_explainer_would(capsys):
    """The checker itself: an id missing from the chart, a renamed branch, a storage
    effect on a check, a flight node without run=, and a node left running."""
    run = Run()
    run.step("nosuchnode", "ok")
    run.step("state", "ok", key="t", branch="no_such_branch")
    run.step("state", "ok", key="u", effect="table:x.y")
    run.step("table", "running", key="v")
    out = capsys.readouterr().out
    problems = drift("apply", out, required=["attach"])
    assert any("nosuchnode is not in" in p for p in problems)
    assert any("no_such_branch" in p for p in problems)
    assert any("storage effect" in p for p in problems)
    assert any("left running" in p for p in problems)
    assert any("required attach" in p for p in problems)
    run.step("trigger", "ok", flight="apply")
    assert any("no run=" in p for p in drift("ingest", capsys.readouterr().out))


# Emitting never fails a run (local only) -----------------------------------------


@pytest.fixture
def fresh_flightlog():
    flightlog._enabled = True
    yield
    flightlog._enabled = True


class Broken:
    def __init__(self, error, on):
        self.error, self.on = error, on

    def write(self, text):
        if self.on == "write":
            raise self.error
        return len(text)

    def flush(self):
        if self.on == "flush":
            raise self.error


@pytest.mark.parametrize("error", [OSError("gone"), BrokenPipeError()])
@pytest.mark.parametrize("on", ["write", "flush"])
@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_a_broken_stdout_never_fails_a_run(
    stores, tmp_path, monkeypatch, fresh_flightlog, error, on
):
    h = Harness(stores, tmp_path)
    w = h.wire
    h.ingest(w.txn(relation_message(1, "items", ITEMS), w.insert(1, ("1", "a", "t", "1.00"))))
    monkeypatch.setattr(sys, "stdout", Broken(error, on))
    run = Run()
    results = explain.apply(run, lambda on_table: h.apply.run(on_table=on_table), "m")
    assert [r.status for r in results] == ["caught_up"] * len(TABLES)
    Stages(run, {"x": lambda r, c: 1 / 0})("x")  # an action that raises
    flightlog.emit("table", "ok", key=object())


def test_a_closed_stdout_pipe_does_not_turn_success_into_exit_120():
    """The interpreter flushes stdout at exit; a broken pipe there makes the exit
    status 120. Emitting turns itself off and points stdout at /dev/null instead."""
    script = textwrap.dedent(
        """
        import sys, time
        from cdc_flight.flightlog import Run
        run = Run()
        run.step("attach", "running")
        sys.stdout.flush()
        time.sleep(0.5)  # the reader closes the pipe meanwhile
        for i in range(2000):
            run.step("table", "ok", key=f"t{i}", rows=i)
        print('{"status": "done"}')  # the CLI's own result line
        sys.stderr.write("finished\\n")
        """
    )
    process = subprocess.Popen(
        [sys.executable, "-c", script],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env={**os.environ, "PYTHONPATH": str(ROOT / "src")},
    )
    process.stdout.readline()
    process.stdout.close()
    _, err = process.communicate(timeout=60)
    assert b"finished" in err and process.returncode == 0, (process.returncode, err[-300:])


# preflight and ingest (PostgreSQL) -------------------------------------------------


@pytest.mark.postgres
@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_preflight_and_ingest_lines_match_their_charts(stores, pg_phase1, capsys):  # noqa: F811
    source, config = pg_phase1
    con = stores[0].con
    state = {}

    def prepare(fault, **options):
        """As cli.main: bootstrap_log (registration and ownership commit together); the
        log's recorded configuration is used from then on."""
        log, checked = bootstrap_log(con, config, fault=fault, **options)
        state.update(log=log)
        return checked

    seen = {"preflight": set(), "ingest": set()}

    def preflight(**options):
        return explain.preflight(Run(), lambda fault: prepare(fault, **options), lambda info: None)

    def collect(command, required=()):
        out = capsys.readouterr().out
        assert drift(command, out, required) == []
        assert_no_secrets(out, config.dsn)
        seen[command] |= branches(out)
        return out

    every_check = ["endpoint", "connect", "server", "replication", "tables", "objects"]
    # Missing objects: the objects check routes to provisioning and preflight fails.
    with pytest.raises(PreflightError, match="missing"):
        preflight()
    collect("preflight", every_check + ["provision"])
    preflight(create_objects=True)
    collect("preflight", every_check + ["register"])
    preflight()
    collect("preflight", every_check + ["register"])

    chain = ApplyChain(str(uuid.uuid4()))
    triggered = []

    def trigger(target, overrides):
        triggered.append(target)
        return ("run-id", 7)

    def ingest():
        run = Run()
        log = open_log(con, config)
        info2 = explain.step(run, "preflight", lambda: prepare(None))
        log = state["log"]
        return explain.ingest(
            run, log.config, log, info2, run_once, chain, trigger, after_ingest, log.checkpoint()
        )

    source.execute("INSERT INTO items VALUES (1,'a','b')")
    result, outcome = ingest()
    assert (result.status, outcome) == ("drained", "triggered") and triggered
    out = collect(
        "ingest",
        ["preflight", "lease", "slot", "stream", "append", "finish", "outcome", "trigger"],
    )
    assert "@flight-run trigger ok flight=apply run=7" in out
    # Steps report in order: append's last line comes before finish starts.
    lines = [line for line in out.splitlines() if line.startswith("@flight-run")]
    started = next(
        i for i, line in enumerate(lines) if line.startswith("@flight-run finish running")
    )
    assert not any(line.startswith("@flight-run append") for line in lines[started:]), lines
    assert re.search(r"@flight-run finish ok rows=\d+", out)
    # The timing lines (log-only, not chart nodes): one per append and one per run.
    timing = [line for line in out.splitlines() if line.startswith("@flight-log timing ")]
    assert timing[0].startswith("@flight-log timing append n=1 ") and "why=" in timing[0]
    assert timing[-1].startswith("@flight-log timing run ") and " appends=" in timing[-1]

    # Lease busy: another run holds it.
    other = open_log(stores[1].con, config)
    other.acquire("other")
    try:
        assert ingest()[0].status == "busy"
    finally:
        other.release()
    collect("ingest", ["preflight", "lease", "overlap"])

    # Slot busy: a lingering reader holds the slot.
    repl = connect(config, replication=True)
    try:
        repl.cursor().start_replication(
            slot_name=config.slot,
            options={"proto_version": "1", "publication_names": config.publication},
        )
        assert ingest()[0].status == "busy"
    finally:
        repl.close()
    collect("ingest", ["preflight", "lease", "slot", "busy"])
    conftest.wait_slot_released(source.con, config.slot)

    # Apply chaining disabled: the trigger is skipped.
    chain = ApplyChain(str(uuid.uuid4()), enabled=False)
    assert ingest()[1] == "disabled"
    assert "@flight-run trigger skipped" in collect("ingest", ["outcome"])

    # A configured table disappears: it holds, and the run itself still succeeds.
    source.execute("DROP TABLE items")
    result, _ = ingest()
    assert result.status == "drained" and result.held[0]["reason"] == "dropped"
    out = collect("ingest", ["preflight", "held", "lease", "finish"])
    assert "@flight-run held ok branch=held rows=1" in out

    # Every labelled edge is taken somewhere, or allowed with a reason.
    allowed = {
        # Every returned run writes a marker, so its checkpoint always advances.
        ("ingest", "outcome", "unchanged"),
    }
    for command in seen:
        _, edges = chart(command)
        for node, label, _ in edges:
            if label and (command, node, label) not in allowed:
                assert (node, label) in seen[command], (command, node, label)


# apply (local) -------------------------------------------------------------------


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_apply_lines_match_the_chart(stores, tmp_path, capsys):
    h = Harness(stores, tmp_path)
    w = h.wire
    rel = relation_message(1, "items", ITEMS)
    h.ingest(w.txn(rel, w.insert(1, ("1", "a", "t", "1.00"))))
    # No generation for `fourth`, and `other` truncated: it needs a backfill.
    h.store.con.execute(f"DELETE FROM {h.apply.mirror}._cdc.apply_state WHERE relation_id=4")
    # A truncate names its relation, whose descriptor the spool needs first.
    h.ingest(w.txn(relation_message(2, "other", OTHER, identity="d"), w.truncate(2)))
    run = Run()
    explain.step(run, "attach", lambda: None)
    results = explain.apply(
        run, lambda on_table: h.apply.run(on_table=on_table), h.apply.mirror.strip('"')
    )
    out = capsys.readouterr().out
    assert drift("apply", out, ["attach", "table", "state", "current", "nogen", "resync"]) == []
    statuses = {r.table: r.status for r in results}
    assert statuses["public.fourth"] == "awaiting_generation"
    assert statuses["public.other"] == "needs_resync"
    for table in statuses:
        assert f"@flight-run table ok key={table}" in out
    # needs_action is reached by apply's own held-state tests; its mapping here:
    assert explain.STATE_BRANCH["needs_action"] == "needs_action"
    assert explain.SINK["needs_action"] == "action"


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_apply_lines_for_a_table_cdc_tables_leaves_out(stores, tmp_path, capsys):
    """A registered table outside a set CDC_TABLES: the `omitted` branch, with no
    storage effect (it isn't touched)."""
    from dataclasses import replace

    from cdc_flight import table_set
    from cdc_flight.apply_run import ApplyRun

    h = Harness(stores, tmp_path)
    w = h.wire
    h.ingest(w.txn(relation_message(1, "items", ITEMS), w.insert(1, ("1", "a", "t", "1.00"))))
    kept, *left_out = h.apply.config.tables
    narrow = ApplyRun(h.apply.con, replace(h.apply.config, tables=(kept,)))
    capsys.readouterr()
    run = Run()
    results = explain.apply(
        run, lambda on_table: table_set.run(narrow, on_table=on_table, steps=run), "m"
    )
    out = capsys.readouterr().out
    assert drift("apply", out, ["table", "state", "omitted"]) == []
    omitted = [r.table for r in results if r.status == "not_configured"]
    assert omitted and set(omitted) <= set(left_out)
    for table in omitted:
        assert f"@flight-run omitted ok key={table}" in out
        line = next(
            t for t in out.splitlines() if t.startswith(f"@flight-run table ok key={table}")
        )
        assert "effect=" not in line


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_cli_apply_and_apply_run_agree(stores, tmp_path):
    """The CLI applies through table_set.run, i.e. Apply.run(on_table=...); without a callback it's the
    same run."""
    h = Harness(stores, tmp_path)
    w = h.wire
    h.ingest(w.txn(relation_message(1, "items", ITEMS), w.insert(1, ("1", "a", "t", "1.00"))))
    events = []
    results = h.apply.run(
        on_table=lambda event, value: events.append(
            (event, value if event == "start" else value.table)
        )
    )
    # Page-major: tables open before any finishes; each table once, start before end.
    names = [f"public.{name}" for name, _ in TABLES.values()]
    assert sorted(events) == sorted([(e, t) for t in names for e in ("start", "end")])
    assert all(events.index(("start", t)) < events.index(("end", t)) for t in names)
    assert [r.status for r in results] == ["caught_up"] * len(TABLES)
    assert [r.status for r in h.apply.run()] == ["caught_up"] * len(TABLES)


# backfill (PostgreSQL) -------------------------------------------------------------


@pytest.mark.postgres
@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_backfill_lines_match_the_chart(stores, populated, capsys):  # noqa: F811
    source, _, ingest_config, log, backfill = populated
    table = f"{source.schema}.pairs"
    target = f"{backfill.config.mirror_database}.{source.schema}.pairs"
    chain = BackfillChain(table, self_flight_id=str(uuid.uuid4()), delay_seconds=0)
    seen, all_out = set(), []

    def one_run(**options):
        run = Run()
        gen = backfill.active(backfill.registered(table)[0])
        total = None
        if gen is None or explain.resume_point(gen) == "scan":
            total = explain.estimate_chunks(backfill.pg, table, backfill.config.chunk_rows, query)
        result = explain.backfill(
            run,
            gen,
            total,
            target,
            lambda fault: backfill.run_table(table, fault=fault, max_chunks=1, **options),
        )
        launched = {}
        trigger = explain.traced_trigger(
            run, "retrigger", "backfill", lambda t, o: ("run-id", 9), launched
        )
        outcome, _ = after_backfill(result, chain, trigger, sleep=lambda s: None)
        explain.backfill_outcome(run, result, outcome, launched)
        out = capsys.readouterr().out
        assert drift("backfill", out) == []
        assert_no_secrets(out, ingest_config.dsn)
        # The node a run stopped in was entered in that run (stage hooks fired).
        stopped_in = {"snapshotting": "scan", "catching_up": "catchup"}.get(result.status)
        if stopped_in:
            assert f"@flight-run {stopped_in} running" in out, (result.status, out)
        all_out.append(out)
        seen.update(branches(out))
        return result, out

    # Never analyzed: no total. The first run starts the generation.
    result, out = one_run()
    assert "total=" not in out and "@flight-run start ok branch=new" in out
    query(source.con, "ANALYZE pairs")
    result, out = one_run()
    assert re.search(r"@flight-run scan running done=\d+ total=\d+", out)
    for _ in range(40):
        if result.status == "published":
            break
        if result.status in ("waiting_for_marker", "waiting_for_ingest"):
            assert run_once(ingest_config, log).status == "drained"
        # A run that finds the marker stops at once in catch-up (an expired deadline),
        # so the next one resumes there.
        expired = result.status == "waiting_for_marker"
        result, out = one_run(deadline=0 if expired else None)
    assert result.status == "published"
    everything = "".join(all_out)
    required = ["start", "pin", "resume", "scan", "marker", "catchup", "publish", "outcome"]
    assert drift("backfill", everything, required + ["retrigger", "done"]) == []
    assert f"effect=table:{target}" in everything
    allowed = {("outcome", "failed"), ("outcome", "stopped")}  # failure tests own these
    for node, label, _ in chart("backfill")[1]:
        if label and (node, label) not in allowed:
            assert (node, label) in seen, (node, label)


def test_compact_lines_match_the_chart(stores, tmp_path, capsys):
    from test_compaction import config, days_log

    from cdc_flight.compact import run_compaction

    h = Harness(stores, tmp_path)
    days_log(h, ["20260101", "20260102"])
    run = Run()
    result = run_compaction(config(h), h.store.con, on_event=explain.compaction_events(run))
    out = capsys.readouterr().out
    assert result.days == ["20260101"]
    assert drift("compact", out, ["lease", "expire", "kept", "day", "stop", "open"]) == []
    assert {("expire", "none"), ("stop", "not_sealed")} <= branches(out)
    # Busy: the lease branch.
    token = h.apply.retention.acquire_compaction("other")
    run_compaction(config(h), h.store.con, on_event=explain.compaction_events(Run()))
    out = capsys.readouterr().out
    h.apply.retention.release_compaction(token)
    assert drift("compact", out, ["lease", "busy"]) == []
    assert ("lease", "busy") in branches(out)
    nodes = set(chart("compact")[0])
    assert set(explain.COMPACT_SINK.values()) | set(explain.EXPIRE_SINK.values()) <= nodes


def test_the_ingest_connect_timing_line(capsys):
    """Log-only: the connect's wall time, the process's age and the client settings."""
    from cdc_flight import cli

    with duckdb.connect() as con:
        cli.connect_timing(con, 0.25)
    (line,) = capsys.readouterr().out.splitlines()
    assert re.search(r" threads=\d+ checkpoint_threshold=\S", line), line
    if os.path.exists("/proc/self/stat"):
        assert line.startswith("@flight-log timing connect wall=0.2500 age=")
        assert 0 < cli.process_age() < 24 * 3600
    else:  # no /proc (macOS): the age is unknown and left out of the line
        assert line.startswith("@flight-log timing connect wall=0.2500 threads=")
        assert cli.process_age() is None
