"""S5 invariant audits (cdc_flight.audits): healthy stores pass, each corruption fails
its audit, the legitimate recovery states pass, and an unmet precondition is
inconclusive, never a pass. Local DuckDB only: the MotherDuck placement lives in
test_retention's allowlisted case."""

import re
from pathlib import Path

import pytest
from test_compaction import config, days_log
from test_epoch import transition
from test_expiry import vars_of
from test_ingest_log2 import build, log2  # noqa: F401 (fixture)
from test_phase2 import Harness

from cdc_flight import audits
from cdc_flight.backfill import Backfill
from cdc_flight.compact import run_compaction
from cdc_flight.config import BackfillConfig
from cdc_flight.retention2 import Retention2

DAYS = ["20260101", "20260102", "20260103"]


def run(h, **kwargs):
    kwargs.setdefault("lease_seconds", h.config.lease_seconds)
    database, mirror = h.store.database.strip('"'), h.store.mirror_database.strip('"')
    return audits.audit(h.store.con, database, "phase2", mirror, **kwargs)


def sql(h, statement, parameters=()):
    """A corruption (or a recovery state) written directly, outside the audited code."""
    statement = statement.replace("{s}", h.log.prefix).replace("{m}", f"{h.apply.mirror}._cdc.")
    return h.store.con.execute(statement, list(parameters))


def failing(report):
    return {f.audit for f in report.findings if f.verdict == "fail"}


def assert_fails(report, audit, also=()):
    """`audit` fails; any other failure is one of `also`; nothing is inconclusive."""
    assert report.verdict(audit) == "fail", report.render()
    assert failing(report) <= {audit, *also}, report.render()
    assert not [f for f in report.findings if f.verdict == "inconclusive"], report.render()


def table_pin(h):
    return sql(
        h,
        "SELECT owner,source_epoch,pinned_lsn FROM {s}retention_pins "
        "WHERE kind='table' ORDER BY pinned_lsn LIMIT 1",
    ).fetchone()


def add_generation(h, generation_id, state, *, expires_after=None, catchup=0):
    """A generation row with its lease columns (a backfill's shape)."""
    rel = h.apply.registered("public.items")[0]
    expires = (
        "NULL" if expires_after is None else f"current_timestamp+INTERVAL '{expires_after} seconds'"
    )
    sql(
        h,
        "INSERT INTO {m}generations (generation_id,relation_id,mode,epoch,pin_lsn,state,"
        "created_at,catchup_lsn,token,holder,expires_at,updated_at) VALUES "
        f"(?,?,'snapshot',1,0,?,current_timestamp,?,1,"
        f"{'NULL' if expires_after is None else repr('run')},{expires},current_timestamp)",
        [generation_id, rel, state, catchup],
    )


@pytest.fixture
def healthy(stores, tmp_path):
    """Three days of changes, every table applied and pinned, a mirror with the
    backfill's generation columns: no floor yet."""
    h = Harness(stores, tmp_path)
    Backfill(h.store.con, BackfillConfig(**vars_of(h.config))).initialize()
    days_log(h, DAYS)
    return h


@pytest.fixture
def compacted(healthy):
    """The healthy store after a compaction run: two days dropped, a floor."""
    result = run_compaction(config(healthy), healthy.store.con)
    assert result.status == "done" and result.days == DAYS[:2]
    return healthy


# Healthy stores -------------------------------------------------------------------


def test_a_healthy_store_passes_every_audit_across_a_compaction(healthy):
    report, first = run(healthy)
    assert report.ok, report.render()
    assert {f.audit for f in report.findings} == {*audits.LOG_AUDITS, *audits.MIRROR_AUDITS}
    assert run_compaction(config(healthy), healthy.store.con).status == "done"
    report, second = run(healthy, previous=first)
    assert report.ok, report.render()
    assert {f.audit for f in report.findings} == {
        *audits.LOG_AUDITS,
        *audits.LOG_PAIR_AUDITS,
        *audits.MIRROR_AUDITS,
        *audits.MIRROR_PAIR_AUDITS,
    }
    assert second.floor is not None and second.floor < second.checkpoint
    # Three SELECTs (catalog, change log, mirror), inside one rolled-back transaction.
    assert report.statements == 3


def test_the_audit_is_read_only_and_leaves_no_transaction_open(compacted):
    h = compacted
    before = sql(h, "SELECT guard,token FROM {s}retention").fetchall()
    report, _ = run(h)
    assert report.ok, report.render()
    assert sql(h, "SELECT guard,token FROM {s}retention").fetchall() == before
    h.store.con.execute("BEGIN")  # would raise inside a transaction the audit left open
    h.store.con.execute("ROLLBACK")


def test_the_module_issues_no_write_statements():
    """SELECTs only, so the READ ONLY transaction is a second line, not the only one."""
    source = Path(audits.__file__).read_text()
    strings = re.findall(r'"((?:[^"\\]|\\.)*)"', source)
    writes = re.compile(
        r"\b(INSERT|UPDATE|DELETE|CREATE|DROP|ALTER|COPY|ATTACH|DETACH|SET|CALL|PRAGMA|"
        r"CHECKPOINT|TRUNCATE|VACUUM|INSTALL|LOAD|EXPORT|IMPORT|USE)\b"
    )
    assert not [s for s in strings if writes.search(s)]


def test_no_flight_imports_the_audits():
    for path in Path(audits.__file__).parent.glob("*.py"):
        imports = re.compile(r"cdc_flight\.audits|from cdc_flight import [^\n]*\baudits\b")
        if path.name != "audits.py":
            assert not imports.search(path.read_text()), path.name


# Each corruption fails its audit (single snapshot) ---------------------------------


def first_unit_end(h):
    return sql(h, "SELECT min(end_lsn) FROM {s}units_v2 WHERE complete").fetchone()[0]


def test_a1_floor_at_a_pin_fails(healthy):
    """A generation pinned below the checkpoint (registered by the real code), and the
    floor set onto it: below the checkpoint and the table pins, so A1 alone."""
    h = healthy
    rel = h.apply.registered("public.items")[0]
    lsn = first_unit_end(h)
    h.apply.retention.register_generation(rel, "reader", 1, lsn)
    sql(h, "UPDATE {s}retention SET floor_epoch=1,floor_lsn=?", [lsn])
    assert_fails(run(h)[0], "A1")


def test_a2_floor_at_the_checkpoint_fails(compacted):
    h = compacted
    epoch, lsn = sql(h, "SELECT epoch,confirmed_lsn FROM {s}source").fetchone()
    sql(h, "UPDATE {s}retention SET floor_epoch=?,floor_lsn=?", [epoch, lsn])
    # Every pin and the open anchor are at or below the checkpoint, so A1 fails too.
    assert_fails(run(h)[0], "A2", also={"A1"})


def test_a3a_half_set_floor_fails(compacted):
    h = compacted
    sql(h, "UPDATE {s}retention SET floor_epoch=NULL")
    # With no usable floor, the days already dropped sit above "no floor".
    assert_fails(run(h)[0], "A3a", also={"A5"})


def test_a3a_floor_in_an_unknown_epoch_fails(compacted):
    h = compacted
    sql(h, "UPDATE {s}retention SET floor_epoch=0,floor_lsn=1")
    # A floor in epoch 0 is below the days already dropped.
    assert_fails(run(h)[0], "A3a", also={"A5"})


def test_a4_pin_inside_a_unit_fails(healthy):
    h = healthy
    owner, _, lsn = table_pin(h)
    sql(h, "UPDATE {s}retention_pins SET pinned_lsn=? WHERE owner=?", [lsn + 1, owner])
    assert_fails(run(h)[0], "A4")


def test_a5_a_dropped_day_above_the_floor_fails(compacted):
    """The active day's only unit STARTS at the floor (the previous day's end): its end,
    the day's cut, is above it."""
    h = compacted
    sql(h, "UPDATE {s}frame_partitions_v2 SET status='dropped' WHERE partition_day=?", [DAYS[2]])
    assert_fails(run(h)[0], "A5")


def test_c1_half_set_source_lease_fails(healthy):
    h = healthy
    sql(h, "UPDATE {s}source SET holder='ghost',expires_at=NULL")
    assert_fails(run(h)[0], "C1")


def test_c3_missing_source_guard_fails(healthy):
    h = healthy
    sql(h, "UPDATE {s}source SET guard=NULL")
    assert_fails(run(h)[0], "C3")


def test_c5_a_pin_with_expires_at_fails(healthy):
    h = healthy
    owner, _, _ = table_pin(h)
    sql(h, "UPDATE {s}retention_pins SET expires_at=current_timestamp WHERE owner=?", [owner])
    assert_fails(run(h)[0], "C5")


def test_b3_applied_beyond_the_checkpoint_fails(healthy):
    h = healthy
    sql(h, "UPDATE {m}apply_state SET applied_through_lsn=applied_through_lsn+1000000")
    assert_fails(run(h)[0], "B3")


def test_c1m_half_set_apply_lease_fails(healthy):
    h = healthy
    sql(h, "UPDATE {m}apply_state SET holder='ghost',expires_at=NULL")
    assert_fails(run(h)[0], "C1m")


def test_c4_an_ended_generation_leased_after_its_last_update_fails(healthy):
    h = healthy
    add_generation(h, "late", "abandoned", expires_after=2 * h.config.lease_seconds)
    assert_fails(run(h)[0], "C4")


def test_b7_a_live_table_with_a_stale_blocked_unit_start_fails(healthy):
    """The shape keyed resync publication left in older versions: the commit cleared, the
    unit start kept."""
    h = healthy
    sql(h, "UPDATE {m}apply_state SET blocked_unit_start_lsn=applied_through_lsn")
    assert_fails(run(h)[0], "B7")


def test_b7_a_blocked_start_away_from_the_applied_position_fails(healthy):
    h = healthy
    sql(
        h,
        "UPDATE {m}apply_state SET state='blocked',blocked_commit_lsn=applied_through_lsn+5,"
        "blocked_unit_start_lsn=applied_through_lsn+1",
    )
    assert_fails(run(h)[0], "B7")


def test_b7_a_hold_at_the_applied_unit_start_passes(healthy):
    h = healthy
    sql(
        h,
        "UPDATE {m}apply_state SET state='blocked',blocked_commit_lsn=applied_through_lsn+5,"
        "blocked_unit_start_lsn=applied_through_lsn",
    )
    report, _ = run(h)
    assert report.ok, report.render()


def test_b7_a_legacy_hold_without_a_unit_start_passes(healthy):
    """A hold written before the column existed: the commit set, no start, not live."""
    h = healthy
    sql(h, "UPDATE {m}apply_state SET state='blocked',blocked_commit_lsn=applied_through_lsn+5")
    report, _ = run(h)
    assert report.ok, report.render()


# Each corruption between two stable points fails its pair audit --------------------


def pair(h, corrupt, **kwargs):
    _, first = run(h, **kwargs)
    corrupt()
    return run(h, previous=first, **kwargs)[0]


def test_a6_floor_moving_back_fails(compacted):
    h = compacted
    report = pair(h, lambda: sql(h, "UPDATE {s}retention SET floor_lsn=floor_lsn-1"))
    # floor_lsn-1 is no unit end; A3a checks only the pair and the epoch, so A6 alone.
    assert_fails(report, "A6")


def test_b1_checkpoint_moving_back_fails(healthy):
    h = healthy
    report = pair(h, lambda: sql(h, "UPDATE {s}source SET confirmed_lsn=confirmed_lsn-1"))
    # Applied tables are at the old checkpoint, now beyond it.
    assert_fails(report, "B1", also={"B3"})


def test_b1_an_epoch_without_its_start_record_fails(healthy):
    h = healthy
    report = pair(h, lambda: sql(h, "UPDATE {s}source SET epoch=2"))
    assert_fails(report, "B1")


def test_b4_pin_moving_back_in_the_same_generation_fails(healthy):
    h = healthy
    owner, epoch, _ = table_pin(h)
    start = sql(h, "SELECT start_lsn FROM {s}source_epochs WHERE epoch=?", [epoch]).fetchone()[0]
    report = pair(
        h,
        lambda: sql(h, "UPDATE {s}retention_pins SET pinned_lsn=? WHERE owner=?", [start, owner]),
    )
    assert_fails(report, "B4")


def test_b5_a_complete_unit_changing_fails(healthy):
    h = healthy
    report = pair(
        h,
        lambda: sql(
            h,
            "UPDATE {s}units_v2 SET last_commit=last_commit+1 WHERE unit_start_lsn="
            "(SELECT min(unit_start_lsn) FROM {s}units_v2 WHERE complete)",
        ),
    )
    assert_fails(report, "B5")


def test_b5_a_unit_vanishing_above_the_floor_fails(healthy):
    h = healthy
    report = pair(
        h,
        lambda: sql(
            h,
            "DELETE FROM {s}units_v2 WHERE unit_start_lsn="
            "(SELECT max(unit_start_lsn) FROM {s}units_v2 WHERE complete)",
        ),
    )
    # The deleted unit was the tables' pin and the checkpoint's cut.
    assert_fails(report, "B5", also={"A4"})


def test_c2_source_token_moving_back_fails(healthy):
    h = healthy
    report = pair(h, lambda: sql(h, "UPDATE {s}source SET token=token-1"))
    assert_fails(report, "C2")


def test_b2_applied_moving_back_in_the_same_generation_fails(healthy):
    h = healthy
    report = pair(
        h, lambda: sql(h, "UPDATE {m}apply_state SET applied_through_lsn=applied_through_lsn-1")
    )
    assert_fails(report, "B2")


def test_b6_catchup_moving_back_fails(healthy):
    h = healthy
    add_generation(h, "g1", "catching_up", catchup=50)
    report = pair(h, lambda: sql(h, "UPDATE {m}generations SET catchup_lsn=10"))
    assert_fails(report, "B6")


def test_c2m_apply_token_moving_back_fails(healthy):
    h = healthy
    report = pair(h, lambda: sql(h, "UPDATE {m}apply_state SET token=token-1 WHERE token>0"))
    assert_fails(report, "C2m")


# Legitimate recovery states pass ---------------------------------------------------


def test_a_slot_lost_epoch_with_a_lower_lsn_passes(healthy):
    h = healthy
    _, first = run(h)
    assert transition(h, 2, 5)  # below the old checkpoint, under a higher epoch
    report, second = run(h, previous=first)
    assert report.ok, report.render()
    assert second.checkpoint[0] == 2 and second.checkpoint[1] < first.checkpoint[1]


def test_two_epochs_between_the_snapshots_pass(healthy):
    h = healthy
    _, first = run(h)
    assert transition(h, 2, 5)
    assert transition(h, 3, 7, slot="slot_e3")
    report, _ = run(h, previous=first)
    assert report.ok, report.render()


def test_a_pin_transferred_to_another_generation_may_move_back(healthy):
    h = healthy
    owner, epoch, _ = table_pin(h)
    start = sql(h, "SELECT start_lsn FROM {s}source_epochs WHERE epoch=?", [epoch]).fetchone()[0]

    def transfer():
        sql(
            h,
            "UPDATE {s}retention_pins SET generation_id='resync',pinned_lsn=? WHERE owner=?",
            [start, owner],
        )

    report = pair(h, transfer)
    assert report.ok, report.render()


def test_a_vanished_pin_owner_is_not_a_decrease(healthy):
    h = healthy
    owner, _, _ = table_pin(h)
    report = pair(h, lambda: sql(h, "DELETE FROM {s}retention_pins WHERE owner=?", [owner]))
    assert report.ok, report.render()


def test_a_publication_resets_applied_under_a_new_generation(healthy):
    h = healthy

    def publish():
        sql(h, "UPDATE {m}apply_state SET generation_id='new',applied_through_lsn=0")

    report = pair(h, publish)
    assert report.ok, report.render()


def test_a_retired_relation_row_may_vanish(healthy):
    h = healthy
    rel = h.apply.registered("public.items")[0]
    report = pair(h, lambda: sql(h, "DELETE FROM {m}apply_state WHERE relation_id=?", [rel]))
    assert report.ok, report.render()


def test_an_ended_generation_still_leased_after_a_crash_passes(healthy):
    """end(), publish and fail_generation commit the state under the lease and release it
    best effort after (abandon never does): the lease outlives the state change."""
    h = healthy
    add_generation(h, "crashed", "abandoned", expires_after=h.config.lease_seconds)
    report, _ = run(h)
    assert report.ok, report.render()


# Unmet preconditions are inconclusive, never a pass --------------------------------


def assert_inconclusive(report, audits_=None, reason=""):
    assert not report.ok
    selected = [f for f in report.findings if audits_ is None or f.audit in audits_]
    assert selected and all(f.verdict == "inconclusive" for f in selected), report.render()
    assert all(reason in f.detail for f in selected), report.render()


def test_an_empty_report_is_not_ok():
    assert not audits.Report().ok


def test_no_protocol_row_is_inconclusive(healthy):
    sql(healthy, "DELETE FROM {s}protocol_v2")
    report, snapshot = run(healthy)
    assert_inconclusive(report, reason="protocol_v2")
    assert snapshot is None


def test_an_unregistered_source_is_inconclusive(healthy):
    sql(healthy, "UPDATE {s}source SET confirmed_lsn=NULL")
    assert_inconclusive(run(healthy)[0], reason="source not registered")


def test_a_deleting_source_is_inconclusive(healthy):
    sql(healthy, "UPDATE {s}source SET lifecycle='deleting'")
    assert_inconclusive(run(healthy)[0], reason="deleting")


def test_a_busy_connection_is_inconclusive(healthy):
    healthy.store.con.execute("BEGIN")
    try:
        assert_inconclusive(run(healthy)[0], reason="connection busy")
    finally:
        healthy.store.con.execute("ROLLBACK")


def test_the_statement_budget_is_inconclusive(healthy):
    assert_inconclusive(run(healthy, budget=audits.Budget(statements=1))[0], reason="budget")


def test_the_seconds_budget_is_inconclusive(healthy):
    assert_inconclusive(run(healthy, budget=audits.Budget(seconds=-1.0))[0], reason="budget")


def test_the_budget_can_stop_before_the_mirror(healthy):
    report, _ = run(healthy, budget=audits.Budget(statements=2))
    assert_inconclusive(report, audits.MIRROR_AUDITS, reason="budget")
    assert all(report.verdict(a) == "pass" for a in audits.LOG_AUDITS), report.render()


def test_without_a_lease_bound_c4_is_inconclusive(healthy):
    report, _ = run(healthy, lease_seconds=None)
    assert_inconclusive(report, ["C4"], reason="lease_seconds")


def test_without_backfill_columns_c4_and_b6_are_inconclusive(stores, tmp_path):
    h = Harness(stores, tmp_path)
    days_log(h, DAYS[:1])
    _, first = run(h)
    report, _ = run(h, previous=first)
    assert_inconclusive(report, ["C4", "B6"], reason="no backfill yet")
    assert {f.audit for f in report.failures()} == {"C4", "B6"}, report.render()


def test_an_unavailable_store_is_inconclusive(healthy, monkeypatch):
    """A server error mid-audit (a lost connection, a timeout): the audits not yet
    answered are inconclusive, the transaction is gone, no snapshot is carried."""
    calls = []
    real = audits._Run.select

    def flaky(self, statement, parameters=()):
        calls.append(statement)
        if len(calls) == 2:  # the mirror read
            raise audits.duckdb.IOException("connection lost (injected)")
        return real(self, statement, parameters)

    monkeypatch.setattr(audits._Run, "select", flaky)
    report, snapshot = run(healthy)
    assert snapshot is None
    assert_inconclusive(report, audits.MIRROR_AUDITS, reason="unavailable: IOException")
    assert all(report.verdict(a) == "pass" for a in audits.LOG_AUDITS), report.render()
    healthy.store.con.execute("BEGIN")
    healthy.store.con.execute("ROLLBACK")


def test_an_ended_generation_without_updated_at_is_inconclusive(healthy):
    add_generation(healthy, "old", "failed", expires_after=60)
    sql(healthy, "UPDATE {m}generations SET updated_at=NULL")
    assert_inconclusive(run(healthy)[0], ["C4"], reason="updated_at")


def test_more_units_than_the_cap_is_inconclusive(healthy, monkeypatch):
    _, first = run(healthy)
    monkeypatch.setattr(audits, "ROW_CAP", 1)
    report, _ = run(healthy, previous=first)
    assert_inconclusive(report, ["B5"], reason="units")


# A3b: the floor's shape, between a drop and its prune ------------------------------


def test_the_floor_shape_holds_between_drop_and_prune_and_is_inconclusive_after(healthy):
    """A3b runs only where the floor's unit still exists: between a drop and its prune
    it passes; after prune removed the unit it is inconclusive, never a pass."""
    h = healthy
    retention = Retention2(h.store.con, h.store.database.strip('"'), "phase2")
    token = retention.acquire_compaction("compactor")
    try:
        assert retention.drop_partition(DAYS[0], token)
        report, _ = run(h, shape=True)
        assert report.ok and report.verdict("A3b") == "pass", report.render()
        retention.prune(token)
        report, _ = run(h, shape=True)
        assert_inconclusive(report, ["A3b"], reason="pruned")
        assert {f.audit for f in report.failures()} == {"A3b"}, report.render()
    finally:
        retention.release_compaction(token)


def test_an_abandoned_unit_start_is_a_floor(log2):  # noqa: F811
    """drop_partition's cut for an abandoned unit is its start (the abandoned prefix)."""
    retention = Retention2(log2.con, log2.config.database, log2.config.source_id)
    retention.initialize()
    token = retention.acquire_compaction("compactor")
    try:
        log2.append(build(log2)[:1], partition_day="20261104")
        retention.register_generation(101, "old", 1, 90)
        log2.begin_epoch(2, 200, "skip", "epoch_2")
        retention.register_generation(101, "new", 2, 200)
        retention.release("generation:old", "old")
        assert retention.drop_partition("20261104", token)
        report, _ = audits.audit(log2.con, log2.config.database, log2.config.source_id, shape=True)
        assert report.ok and report.verdict("A3b") == "pass", report.render()
    finally:
        retention.release_compaction(token)


# Canary: the negative tests depend on the judge ------------------------------------


def test_canary_a_judge_that_never_fails_misses_the_corruption(compacted, monkeypatch):
    h = compacted
    owner, epoch, lsn = table_pin(h)
    sql(h, "UPDATE {s}retention SET floor_epoch=?,floor_lsn=?", [epoch, lsn])
    monkeypatch.setattr(audits, "_check", lambda run, audit, failures: run.add(audit, "pass"))
    report, _ = run(h)
    # With the predicate disabled, test_a1_floor_at_a_pin_fails would fail.
    assert report.verdict("A1") == "pass"
