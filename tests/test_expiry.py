"""Phase 3 consumer expiry: which pin holders may no longer pin the log, in report
mode (the default) and `on` mode, and the races its two steps must survive.

Every test day (January 2026) began long before the 168 h default, so a sealed day
followed by another counts for lag; a holder of it expires by lag once its pin has
also not advanced within the limit (`stall`). Caught-up holders pin only the active day.
Local DuckDB and MotherDuck (`md_gate`), on the Phase 2 harness.
"""

from dataclasses import replace

import duckdb
import pytest
from audit_points import audited
from test_compaction import Statements, config, days_log, status
from test_phase2 import ITEMS, OTHER, TABLES, Harness, relation_message
from test_retention import ingest

from cdc_flight import expiry
from cdc_flight.backfill import Backfill
from cdc_flight.compact import run_compaction
from cdc_flight.config import BackfillConfig
from cdc_flight.retention import generation_owner, table_owner


def on(h, **options):
    return replace(config(h), expire="on", **options)


def rel_of(h, name):
    return h.apply.registered(f"public.{name}")[0]


def lagging(h, name="third"):
    """Three days; `name` stops applying after day 1, so it alone pins day 2 (sealed,
    old); the others are caught up into the active day 3."""
    days_log(h, ["20260101"])
    w = h.wire
    for day in ("20260102", "20260103"):
        ingest(h, day, w.txn(w.insert(1, (day, "v", "t", "1.00"))))
        for other, _ in TABLES.values():
            if other != name:
                assert h.run(other).status == "caught_up"
    rel = rel_of(h, name)
    stall(h, table_owner(rel))
    return rel


def stall(h, owner, hours=400):
    """The pin last advanced `hours` ago (apply stopped)."""
    h.log.execute(
        "UPDATE {s}retention_pins SET updated_at=current_timestamp-?*INTERVAL '1 hour' "
        "WHERE owner=?",
        [hours, owner],
    )


def pin(h, rel):
    return h.apply.retention.pin(table_owner(rel))


def mirror(h, rel, columns="state,detail,held_since IS NOT NULL"):
    return h.store.con.execute(
        f"SELECT {columns} FROM {h.apply.mirror}._cdc.apply_state WHERE relation_id=?", [rel]
    ).fetchone()


def pin_state(h, rel):
    return h.log.execute("SELECT pin_state FROM {s}tables WHERE relation_id=?", [rel]).fetchone()[0]


def expirations(h):
    return h.log.execute(
        "SELECT owner,reason,mode,outcome FROM {s}expirations ORDER BY expired_at,owner"
    ).fetchall()


@pytest.mark.md_gate
def test_report_mode_records_and_on_mode_expires_a_lagging_table(stores, tmp_path):
    h = Harness(stores, tmp_path)
    rel = lagging(h)
    before = pin(h, rel)
    result = run_compaction(config(h), h.store.con)
    # Report mode (the default): recorded only; the lagging pin still holds day 2.
    assert (result.would_expire, result.expired) == (1, 0)
    assert expirations(h) == [(table_owner(rel), "lag", "report", "would_expire")]
    assert pin(h, rel) == before and mirror(h, rel)[0] == "live"
    # Format 2's floor is strict: the lagging pin sits at day 1's unit END, whose unit
    # must stay a valid cut, so day 1 is held too (test_retention2's strict floor).
    assert result.days == []
    assert status(h, "20260101") == status(h, "20260102") == [("sealed",)]
    # On: the mirror goes needs_resync (held since now), the pin goes, the days drop.
    result = run_compaction(on(h), h.store.con)
    assert result.expired == 1 and result.days == ["20260101", "20260102"]
    state, detail, held = mirror(h, rel)
    assert state == "needs_resync" and detail.startswith("retention expired (lag)") and held
    assert pin(h, rel) is None and pin_state(h, rel) == "released"
    assert h.run("third").status == "needs_resync"
    # Released: compaction no longer expects its pin.
    assert run_compaction(on(h), h.store.con).status == "done"


@pytest.mark.md_gate
def test_an_ingest_outage_expires_nothing(stores, tmp_path):
    """Every table caught up and every day old: the pins sit in the active day, which
    never counts, so nothing expires in either mode."""
    h = Harness(stores, tmp_path)
    days_log(h, ["20260101", "20260102"])
    for mode in ("report", "on"):
        result = run_compaction(replace(config(h), expire=mode), h.store.con)
        assert (result.would_expire, result.expired) == (0, 0)
    assert expirations(h) == []


@pytest.mark.md_gate
def test_a_table_that_moved_after_observation_is_skipped(stores, tmp_path):
    """M1: the checkpoint moves between the pass's read and its hold (a commit that
    raced the read). The compare-and-set misses: skipped, pin kept."""
    h = Harness(stores, tmp_path)
    rel = lagging(h)

    def raced(stage):
        if stage == "before_mirror_hold":
            h.store.con.execute(
                f"UPDATE {h.apply.mirror}._cdc.apply_state "
                "SET applied_through_lsn=applied_through_lsn+1 WHERE relation_id=?",
                [rel],
            )

    result = run_compaction(on(h), h.store.con, fault=raced)
    assert (result.expired, result.skipped) == (0, 1)
    assert mirror(h, rel)[0] == "live" and pin(h, rel) is not None
    assert expirations(h)[-1] == (table_owner(rel), "lag", "on", "moved")


@pytest.mark.md_gate
def test_an_apply_run_after_observation_makes_the_holder_busy(stores, tmp_path):
    """Apply ran (and took the lease) after the pass read the table: the lease
    acquisition expects the observed token, so the holder is skipped."""
    h = Harness(stores, tmp_path)
    rel = lagging(h)
    real = h.apply.acquire
    ran = []

    def acquire_after_a_run(relation_id, run_id, *, token=None):
        if relation_id == rel and not ran:
            ran.append(h.run("third").status)
        return real(relation_id, run_id, token=token)

    from cdc_flight import apply as apply_module

    original = apply_module.Apply.acquire
    apply_module.Apply.acquire = lambda self, r, i, *, token=None: (
        acquire_after_a_run(r, i, token=token)
        if self is not h.apply
        else original(self, r, i, token=token)
    )
    try:
        result = run_compaction(on(h), h.store.con)
    finally:
        apply_module.Apply.acquire = original
    assert ran == ["caught_up"] and result.expired == 0
    assert expirations(h)[-1][3] == "busy" and mirror(h, rel)[0] == "live"


@pytest.mark.md_gate
def test_a_pin_transferred_between_the_steps_survives(stores, tmp_path):
    """M2: after step 1, a resync publishes and transfers the table pin to its new
    generation; step 2's scoped release then deletes nothing."""
    h = Harness(stores, tmp_path)
    rel = lagging(h)
    retention = h.apply.retention

    def publish(stage):
        if stage == "between_expiry_steps":
            retention.register_generation(rel, "resync", 1, h.log.checkpoint())
            assert retention.transfer(rel, "resync", 1, h.log.checkpoint())

    result = run_compaction(on(h), h.store.con, fault=publish)
    assert result.expired == 0 and expirations(h)[-1][3] == "moved"
    assert pin(h, rel)["generation"] == "resync" and pin_state(h, rel) == "pinned"


@pytest.mark.md_gate
def test_a_crash_between_the_steps_is_finished_by_the_next_run(stores, tmp_path):
    """The mirror is needs_resync with its pin still there; the next run
    releases the pin by the needs_resync rule, and the days drop."""
    h = Harness(stores, tmp_path)
    rel = lagging(h)

    def crash(stage):
        if stage == "between_expiry_steps":
            raise RuntimeError("crash between the steps")

    with pytest.raises(RuntimeError, match="between the steps"):
        run_compaction(on(h), h.store.con, fault=crash)
    assert mirror(h, rel)[0] == "needs_resync" and pin(h, rel) is not None
    result = run_compaction(on(h), h.store.con)
    assert result.expired == 1 and pin(h, rel) is None
    assert expirations(h)[-1] == (table_owner(rel), "lag", "on", "expired")
    assert status(h, "20260102") == [("dropped",)]


@pytest.mark.md_gate
def test_only_needs_resync_pins_are_released_by_state(stores, tmp_path):
    """A TRUNCATE-held (needs_resync) table's pin is released at any age; needs_action
    and paused tables inside the lag limit keep theirs."""
    h = Harness(stores, tmp_path)
    days_log(h, ["20260101", "20260102"])
    w = h.wire
    # `other` goes needs_resync (a fresh spool needs the truncated relation's descriptor).
    ingest(h, "20260103", w.txn(relation_message(2, "other", OTHER, identity="d"), w.truncate(2)))
    for name, _ in TABLES.values():
        h.run(name)
    other = rel_of(h, "other")
    assert mirror(h, other)[0] == "needs_resync"
    for name, state in (("third", "needs_action"), ("fourth", "paused")):
        h.store.con.execute(
            f"UPDATE {h.apply.mirror}._cdc.apply_state SET state=? WHERE relation_id=?",
            [state, rel_of(h, name)],
        )
    result = run_compaction(on(h), h.store.con)
    assert expirations(h) == [(table_owner(other), "needs_resync", "on", "expired")]
    assert result.expired == 1 and pin(h, other) is None
    assert pin(h, rel_of(h, "third")) and pin(h, rel_of(h, "fourth"))


@pytest.mark.md_gate
def test_a_held_apply_lease_skips_the_holder(stores, tmp_path):
    h = Harness(stores, tmp_path)
    rel = lagging(h)
    token = h.apply.acquire(rel, "someone")
    try:
        result = run_compaction(on(h), h.store.con)
    finally:
        h.apply.release(rel, token)
    assert result.expired == 0 and expirations(h)[-1][3] == "busy"
    assert mirror(h, rel)[0] == "live" and pin(h, rel) is not None


def vars_of(config):
    return dict(
        source_id=config.source_id,
        tables=config.tables,
        database=config.database,
        mirror_database=config.mirror_database,
    )


def unit_end(h, index):
    """The `index`-th complete unit END (ascending): the positions a pin may name."""
    return h.log.execute(
        "SELECT end_lsn FROM {s}units_v2 WHERE complete ORDER BY end_lsn"
    ).fetchall()[index][0]


def add_generation(h, rel, generation_id, state, lsn, *, current=False):
    """A generation row and its pin, as a backfill start would leave them."""
    h.apply.retention.register_generation(rel, generation_id, 1, lsn)
    h.store.con.execute(
        f"INSERT INTO {h.apply.mirror}._cdc.generations (generation_id,relation_id,mode,"
        "epoch,pin_lsn,state,created_at,token) VALUES (?,?,'snapshot',1,?,?,current_timestamp,0)",
        [generation_id, rel, lsn, state],
    )
    if state in ("snapshotting", "catching_up"):  # its claim, inserted with it
        h.store.con.execute(
            f"INSERT INTO {h.apply.mirror}._cdc.backfills (relation_id,generation_id) VALUES (?,?)",
            [rel, generation_id],
        )
    if current:
        h.store.con.execute(
            f"UPDATE {h.apply.mirror}._cdc.apply_state SET generation_id=? WHERE relation_id=?",
            [generation_id, rel],
        )


@pytest.mark.md_gate
def test_generations_expire_by_their_own_rules(stores, tmp_path):
    """An active generation pinning an old sealed day fails under its lease and its
    pin goes; a failed generation's leftover pin goes; a young pin with no
    generation row stays."""
    h = Harness(stores, tmp_path)
    days_log(h, ["20260101", "20260102"])
    rel = rel_of(h, "third")
    # As a backfill's initialize leaves the mirror (lease and staging columns).
    Backfill(h.store.con, BackfillConfig(**vars_of(h.config))).initialize()
    # Pinned strictly below day 1's high (at the epoch start): an old sealed day it still
    # needs. A pin equal to the high has read the day (lag judges by unread data).
    add_generation(h, rel, "active", "snapshotting", 0)
    stall(h, generation_owner("active"))  # generation pins never advance
    add_generation(h, rel, "ended", "failed", 0)
    h.apply.retention.register_generation(rel, "starting", 1, unit_end(h, 1))  # young
    before = audited(h)
    result = run_compaction(on(h), h.store.con)
    # Expiry failed a generation under its lease and released pins: C4's bound, B4's
    # vanished owners and B6 hold across it.
    audited(h, before)
    outcomes = {o: (r, x) for o, r, _, x in expirations(h)}
    assert outcomes[generation_owner("active")] == ("lag", "expired")
    assert outcomes[generation_owner("ended")] == ("stale", "expired")
    assert generation_owner("starting") not in outcomes
    assert (
        h.store.con.execute(
            f"SELECT state FROM {h.apply.mirror}._cdc.generations WHERE generation_id='active'"
        ).fetchone()[0]
        == "failed"
    )
    pins = h.apply.retention.pins()
    assert generation_owner("starting") in pins
    assert generation_owner("active") not in pins and generation_owner("ended") not in pins
    assert result.expired == 2


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_an_invariant_violation_failing_one_generation_spares_the_rest_of_the_run(
    stores, tmp_path, monkeypatch
):
    """A generation expiry cannot fail (an InvariantViolation, injected:
    after S3 nothing in expiry raises one by itself) is rolled back with its pin kept, and
    recorded; every other pin is still judged, the day drops are attempted and the prune and
    forget run; then the run fails loudly (status invariant). The kept pin holds every day
    at or above it, so the drop attempts are observed by a spy (the day stays pinned)."""
    from cdc_flight import generations
    from cdc_flight.generation_spec import InvariantViolation
    from cdc_flight.retention2 import Retention2

    h = Harness(stores, tmp_path)
    days_log(h, ["20260101", "20260102"])
    rel = rel_of(h, "third")
    Backfill(h.store.con, BackfillConfig(**vars_of(h.config))).initialize()
    add_generation(h, rel, "bad", "snapshotting", 0)
    stall(h, generation_owner("bad"))
    add_generation(h, rel, "ended", "failed", 0)
    real = generations.fail_generation

    def fail(owner, gen, generation_id, *args, **kwargs):
        if generation_id == "bad":
            raise InvariantViolation("seeded")
        return real(owner, gen, generation_id, *args, **kwargs)

    monkeypatch.setattr(generations, "fail_generation", fail)
    calls = []
    for method in ("drop_partition", "prune", "forget"):
        real_method = getattr(Retention2, method)

        def spy(self, *args, _name=method, _real=real_method, **kwargs):
            calls.append(_name)
            return _real(self, *args, **kwargs)

        monkeypatch.setattr(Retention2, method, spy)
    with pytest.raises(InvariantViolation, match="could not be failed"):
        run_compaction(on(h), h.store.con)
    outcomes = {o: (r, x) for o, r, _, x in expirations(h)}
    assert outcomes[generation_owner("bad")] == ("lag", "invariant")
    assert outcomes[generation_owner("ended")] == ("stale", "expired")
    pins = h.apply.retention.pins()
    assert generation_owner("bad") in pins and generation_owner("ended") not in pins
    gen_state = h.store.con.execute(
        f"SELECT state FROM {h.apply.mirror}._cdc.generations WHERE generation_id='bad'"
    ).fetchone()[0]
    assert gen_state == "snapshotting"
    statuses = h.log.execute("SELECT status FROM {s}compactions").fetchall()
    assert statuses == [("invariant",)]
    # After expiry: a drop attempted (the oldest day, pinned by "bad"), then prune, forget.
    assert calls == ["drop_partition", "prune", "forget"], calls


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_a_table_pin_expiry_invariant_spares_the_rest_of_the_run(stores, tmp_path, monkeypatch):
    """A table pin whose expiry hits an
    InvariantViolation (the pin_state update misses: seeded at that statement, after the
    real DELETE) keeps its pin (the DELETE rolled back), is recorded as invariant, and the
    run still drops days, prunes and forgets, then reports status invariant."""
    from cdc_flight import lifecycle_commands as lc
    from cdc_flight.generation_spec import InvariantViolation
    from cdc_flight.retention2 import Retention2

    h = Harness(stores, tmp_path)
    rel = lagging(h)
    statement = lc.statement

    def missing_row(owner, caller, name, sql, params=(), **kw):
        if (caller, name) == ("pin_expire", "pin_state_update"):
            raise InvariantViolation("pin_expire/pin_state_update: 0 rows (seeded)")
        return statement(owner, caller, name, sql, params, **kw)

    monkeypatch.setattr(lc, "statement", missing_row)
    calls = []
    for method in ("drop_partition", "prune", "forget"):
        real_method = getattr(Retention2, method)

        def spy(self, *args, _name=method, _real=real_method, **kwargs):
            calls.append(_name)
            return _real(self, *args, **kwargs)

        monkeypatch.setattr(Retention2, method, spy)
    before = pin(h, rel)
    with pytest.raises(InvariantViolation):
        run_compaction(on(h), h.store.con)
    assert expirations(h) == [(table_owner(rel), "lag", "on", "invariant")]
    assert pin(h, rel) == before  # the DELETE rolled back with the state update
    assert h.log.execute("SELECT status FROM {s}compactions").fetchall()[-1] == ("invariant",)
    assert "prune" in calls and "forget" in calls, calls


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_a_generation_failed_without_its_claim_is_reported_not_raised(stores, tmp_path):
    """Expiry fails an active generation whose claim is already missing
    (the claim's target state), expires its pin, and reports it: outcome
    invariant_repaired, the run's status invariant, no exception."""
    h = Harness(stores, tmp_path)
    days_log(h, ["20260101", "20260102"])
    rel = rel_of(h, "third")
    Backfill(h.store.con, BackfillConfig(**vars_of(h.config))).initialize()
    add_generation(h, rel, "orphan", "snapshotting", 0)
    h.store.con.execute(f"DELETE FROM {h.apply.mirror}._cdc.backfills")
    stall(h, generation_owner("orphan"))
    result = run_compaction(on(h), h.store.con)
    assert (result.status, result.error) == ("invariant", "invariant_repaired")
    outcomes = {o: (r, x) for o, r, _, x in expirations(h)}
    assert outcomes[generation_owner("orphan")] == ("lag", "invariant_repaired")
    assert generation_owner("orphan") not in h.apply.retention.pins()
    state, detail = h.store.con.execute(
        f"SELECT state,detail FROM {h.apply.mirror}._cdc.generations WHERE generation_id='orphan'"
    ).fetchone()
    assert state == "failed" and detail.endswith("; claim was already missing")


@pytest.mark.md_gate
def test_a_published_generation_with_a_pending_transfer_keeps_its_pin(stores, tmp_path):
    """E2: the mirror is on the published generation, whose pin apply's ensure_pin
    completes the transfer from. Never expired as a generation; judged by its mirror."""
    h = Harness(stores, tmp_path)
    days_log(h, ["20260101", "20260102"])
    rel = rel_of(h, "items")
    Backfill(h.store.con, BackfillConfig(**vars_of(h.config))).initialize()
    add_generation(h, rel, "published", "published", h.log.checkpoint(), current=True)
    h.store.con.execute(
        f"UPDATE {h.apply.mirror}._cdc.apply_state SET applied_through_lsn=? WHERE relation_id=?",
        [h.log.checkpoint(), rel],
    )
    h.log.execute("DELETE FROM {s}retention_pins WHERE owner=?", [table_owner(rel)])
    run_compaction(on(h), h.store.con)
    assert generation_owner("published") in h.apply.retention.pins()
    # Apply's next read completes the interrupted transfer from that pin.
    w = h.wire
    ingest(h, "20260103", w.txn(w.insert(1, ("77", "v", "t", "1.00"))))
    assert h.run("items").status == "caught_up"
    assert pin(h, rel)["generation"] == "published"


@pytest.mark.md_gate
def test_the_budget_expires_the_oldest_holders_and_unknown_bytes_expire_nothing(stores, tmp_path):
    h = Harness(stores, tmp_path)
    rel = lagging(h)
    days = h.log.execute(
        "SELECT partition_day,bytes FROM {s}frame_partitions_v2 ORDER BY 1"
    ).fetchall()
    assert [d for d, _ in days] == ["20260101", "20260102", "20260103"]
    assert all(b > 0 for _, b in days)  # ingest records each day's bytes
    # A lag limit nothing reaches, and a budget only the newest day fits.
    budget = replace(config(h), max_retained_lag_hours=8760, storage_budget_gb=1500 / 2**30)
    h.log.execute("UPDATE {s}frame_partitions_v2 SET bytes=NULL")
    result = run_compaction(budget, h.store.con)
    assert result.budget == "unknown" and result.would_expire == 0
    h.log.execute("UPDATE {s}frame_partitions_v2 SET bytes=1000")
    result = run_compaction(budget, h.store.con)
    assert result.budget == "over" and result.would_expire == 1
    assert expirations(h)[-1] == (table_owner(rel), "budget", "report", "would_expire")


@pytest.mark.md_gate
def test_held_since_is_kept_across_held_states_and_cleared_by_live(stores, tmp_path):
    h = Harness(stores, tmp_path)
    rel = lagging(h)
    apply = h.apply

    def transition(state):
        current = apply.state(rel)
        token = apply.acquire(rel, "test", token=current["token"])
        try:
            with apply.transaction():
                apply.guard(rel, token)
                assert apply.set_state(rel, token, current, state=state)
        finally:
            apply.release(rel, token)
        return mirror(h, rel, "state,held_since::VARCHAR")

    _, first = transition("needs_action")
    assert first is not None
    assert transition("needs_resync") == ("needs_resync", first)
    assert transition("live") == ("live", None)
    # A stale expectation doesn't match.
    current = dict(apply.state(rel), state="paused")
    token = apply.acquire(rel, "test", token=apply.state(rel)["token"])
    try:
        with apply.transaction():
            apply.guard(rel, token)
            assert not apply.set_state(rel, token, current, state="needs_resync")
    finally:
        apply.release(rel, token)


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_a_motherduck_store_without_a_token_refuses_to_attach(stores, tmp_path, monkeypatch):
    """The mirror is attached from MotherDuck only for a MotherDuck store, and only
    with a token; without one it raises (naming the variable), never skips."""
    h = Harness(stores, tmp_path)
    monkeypatch.delenv("motherduck_token", raising=False)
    monkeypatch.delenv("MOTHERDUCK_TOKEN", raising=False)

    class MotherDuckStore(Statements):
        def execute(self, statement, parameters=None):
            if "FROM duckdb_databases()" in statement and "SELECT type" in statement:
                return self.con.execute("SELECT 'motherduck'")
            return super().execute(statement, parameters)

    con = MotherDuckStore(h.store.con)
    elsewhere = replace(config(h), mirror_database="elsewhere_mirror")
    retention = h.apply.retention
    retention.con = con
    with pytest.raises(RuntimeError, match="MOTHERDUCK_TOKEN is not set"):
        expiry.mirror_attached(retention, elsewhere)
    assert not [s for s in con.seen if "md:" in s]


@pytest.mark.md_gate
def test_the_release_takes_the_shared_guard(stores, tmp_path):
    """Retention.expire serializes with pin registration and transfer through the
    shared guard: while another connection holds it, the release conflicts."""
    h = Harness(stores, tmp_path)
    rel = lagging(h)
    observed = pin(h, rel)
    other = h.other_store.con
    database, source = h.config.database, h.config.source_id
    other.execute("BEGIN")
    other.execute(f'UPDATE "{database}"."{source}".retention SET guard=uuid()::VARCHAR WHERE id=1')
    try:
        with pytest.raises(duckdb.TransactionException):
            h.apply.retention.expire(
                table_owner(rel), observed["generation"], observed["epoch"], observed["lsn"]
            )
    finally:
        other.execute("ROLLBACK")
    assert pin(h, rel) == observed


@pytest.mark.md_gate
def test_a_consumer_that_is_still_advancing_does_not_expire_by_lag(stores, tmp_path):
    """X1: holding an old sealed day isn't enough; the pin must also not have advanced
    within the limit (apply moving through a backlog keeps it fresh)."""
    h = Harness(stores, tmp_path)
    rel = lagging(h)
    h.log.execute(
        "UPDATE {s}retention_pins SET updated_at=current_timestamp WHERE owner=?",
        [table_owner(rel)],
    )
    result = run_compaction(on(h), h.store.con)
    assert (result.would_expire, result.expired) == (0, 0)
    assert mirror(h, rel)[0] == "live" and pin(h, rel) is not None


@pytest.mark.md_gate
def test_the_end_of_an_outage_expires_nothing(stores, tmp_path):
    """X1: ingest and apply were both down; the first run after recovery appends today,
    which seals the last old day. Newer data has existed only since today, so even a
    stalled holder of that day doesn't expire."""
    from datetime import datetime, timezone

    h = Harness(stores, tmp_path)
    days_log(h, ["20260101"])
    w = h.wire
    ingest(h, "20260102", w.txn(w.insert(1, ("2", "v", "t", "1.00"))))  # not applied
    rel = rel_of(h, "items")
    stall(h, table_owner(rel))
    today = datetime.now(timezone.utc).strftime("%Y%m%d")
    ingest(h, today, w.txn(w.insert(1, ("3", "v", "t", "1.00"))))
    assert status(h, "20260102") == [("sealed",)]
    result = run_compaction(on(h), h.store.con)
    assert (result.would_expire, result.expired) == (0, 0)
    assert mirror(h, rel)[0] == "live"


@pytest.mark.md_gate
def test_a_superseded_table_pin_never_touches_the_new_mirror(stores, tmp_path):
    """X2: a resync published a new generation and its transfer is pending; the old
    table pin is stalled on an old day. The new mirror stays live; apply then
    completes the transfer."""
    h = Harness(stores, tmp_path)
    rel = lagging(h, "items")
    Backfill(h.store.con, BackfillConfig(**vars_of(h.config))).initialize()
    add_generation(h, rel, "fresh", "published", h.log.checkpoint(), current=True)
    h.store.con.execute(
        f"UPDATE {h.apply.mirror}._cdc.apply_state SET applied_through_lsn=? WHERE relation_id=?",
        [h.log.checkpoint(), rel],
    )
    result = run_compaction(on(h), h.store.con)
    assert result.expired == 0
    assert (table_owner(rel), "lag", "on", "superseded") in expirations(h)
    assert mirror(h, rel)[0] == "live"
    w = h.wire
    ingest(h, "20260104", w.txn(w.insert(1, ("88", "v", "t", "1.00"))))
    assert h.run("items").status == "caught_up"
    assert pin(h, rel)["generation"] == "fresh"


@pytest.mark.md_gate
def test_a_conflict_while_failing_a_generation_rolls_it_back(stores, tmp_path):
    """X5: the generation's failure (state, claim, staging) is one transaction; a
    conflict inside it leaves the generation running and its pin in place."""
    h = Harness(stores, tmp_path)
    days_log(h, ["20260101", "20260102"])
    rel = rel_of(h, "third")
    Backfill(h.store.con, BackfillConfig(**vars_of(h.config))).initialize()
    add_generation(h, rel, "active", "snapshotting", 0)  # the epoch start: below day 1's high
    stall(h, generation_owner("active"))

    def conflict(stage):
        if stage == "after_generation_failed":
            raise duckdb.TransactionException("Conflict on the staging table (injected)")

    result = run_compaction(on(h), h.store.con, fault=conflict)
    assert result.expired == 0 and expirations(h)[-1][3] == "busy"
    assert (
        h.store.con.execute(
            f"SELECT state FROM {h.apply.mirror}._cdc.generations WHERE generation_id='active'"
        ).fetchone()[0]
        == "snapshotting"
    )
    assert generation_owner("active") in h.apply.retention.pins()


# Equality versus strictly below (format 2's strict floor). A pin equal to a sealed day's greatest complete
# END keeps that endpoint for retention but has read all of that day: lag, and the
# standing's held day and ETA, judge the first day with data above the pin. Budget expiry
# keeps `<=` (it judges retained bytes). Policy arithmetic: local only.


def others_apply(h, but="third"):
    for name, _ in TABLES.values():
        if name != but:
            assert h.run(name).status == "caught_up"


def item(h, day, key, *, describe=False):
    """One insert on `items`; `describe`: preceded by its Relation (a fresh spool)."""
    w = h.wire
    first = (relation_message(1, "items", ITEMS),) if describe else ()
    ingest(h, day, w.txn(*first, w.insert(1, (str(key), "v", "t", "1.00"))))


def today():
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).strftime("%Y%m%d")


def endpoint_holder(h, *, below=False, last="20260103"):
    """`third` stalls at day 1's greatest complete END E1 (or, `below`, at an earlier unit
    END inside day 1). Day 2 follows; `last` (old, or today) follows day 2."""
    days_log(h, ["20260101"])  # every table applies day 1's first unit
    if below:
        item(h, "20260101", 101)
        others_apply(h)
    for key, day in enumerate(("20260102", last), start=2):
        item(h, day, key)
        others_apply(h)
    rel = rel_of(h, "third")
    stall(h, table_owner(rel))
    return rel


def judged(h, config_):
    retention = h.apply.retention
    token = retention.acquire_compaction("policy")
    try:
        return expiry.run(config_, retention, token, "policy", mode="report")
    finally:
        retention.release_compaction(token)


def standing_of(summary, owner):
    return next(p for p in summary.pins if p["owner"] == owner)


def lag_expiring(summary, owner):
    return (owner, "lag", "would_expire") in {
        (o.owner, o.reason, o.outcome) for o in summary.outcomes
    }


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_a_pin_equal_to_the_lag_threshold_does_not_expire_and_its_eta_follows_unread_data(
    stores, tmp_path
):
    """X1's shape: day 2 is followed only today, so the lag threshold is day 1's high E1,
    and the pin equals it. Nothing lag-expires; the standing names day 2 (the first day
    with data above the pin) and its ETA runs from today's day."""
    h = Harness(stores, tmp_path)
    rel = endpoint_holder(h, last=today())
    summary = judged(h, config(h))
    assert not lag_expiring(summary, table_owner(rel)), summary.outcomes
    pin_ = standing_of(summary, table_owner(rel))
    assert pin_["held_day"] == "20260102" and pin_["rule"] == "lag", pin_
    assert pin_["expires_at_us"] is not None and pin_["expires_at_us"] > pin_["updated_at_us"]


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_a_stalled_pin_strictly_below_a_days_last_end_expires_by_lag(stores, tmp_path):
    h = Harness(stores, tmp_path)
    rel = endpoint_holder(h, below=True, last=today())
    summary = judged(h, config(h))
    assert lag_expiring(summary, table_owner(rel)), summary.outcomes
    assert standing_of(summary, table_owner(rel))["held_day"] == "20260101"


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_with_two_qualifying_days_a_pin_at_the_first_days_end_still_expires(stores, tmp_path):
    """Days 1 and 2 both qualify (each followed long ago); the pin at E1 is
    below the threshold E2, so it expires. "The first qualifying high" would not."""
    h = Harness(stores, tmp_path)
    rel = endpoint_holder(h)  # day 3 is old: day 2 qualifies too
    summary = judged(h, config(h))
    assert lag_expiring(summary, table_owner(rel)), summary.outcomes
    assert standing_of(summary, table_owner(rel))["held_day"] == "20260102"


def cross_day_unit(h):
    """A unit whose chunks span days 1 and 2 (its END E lands in day 2), after day 1's
    first unit (END E0): both days' highs are E. Returns (E0, E)."""
    days_log(h, ["20260101"])
    e0 = h.log.checkpoint()
    w = h.wire
    messages = w.txn(*[w.insert(1, (str(200 + i), "x" * 300, "t", "1.00")) for i in range(12)])
    items = [i for m in messages for i in h.spool.feed(m)]
    assert len(items) >= 3  # several chunks and the terminal
    for day, part in (("20260101", items[:1]), ("20260102", items[1:])):
        h.log.acquire("test")
        try:
            h.log.append(part, partition_day=day)
            # As Harness.ingest: confirm the fixed keys through the checkpoint.
            keys = {oid: list(k) for oid, (_, k) in TABLES.items()}
            h.log.record_keys(keys, h.log.checkpoint(), 0)
        finally:
            h.log.release()
    return e0, h.log.checkpoint()


@pytest.mark.parametrize("stores", ["local"], indirect=True)
@pytest.mark.parametrize("at", ["shared_end", "below"])
def test_two_days_sharing_a_high_judge_a_pin_at_it_by_the_next_day(stores, tmp_path, at):
    """Days 1 and 2 share the high E (a cross-day unit). A pin at E
    lag-expires nothing; a pin one unit below E is judged by day 1."""
    h = Harness(stores, tmp_path)
    e0, e = cross_day_unit(h)
    if at == "shared_end":
        others_apply(h, but=None)  # third too: its pin reaches E
    else:
        others_apply(h)  # third stays at E0
    for key, day in enumerate(("20260103", today()), start=3):
        item(h, day, key)
        others_apply(h)
    rel = rel_of(h, "third")
    assert pin(h, rel)["lsn"] == (e if at == "shared_end" else e0)
    stall(h, table_owner(rel))
    summary = judged(h, config(h))
    held = standing_of(summary, table_owner(rel))["held_day"]
    if at == "shared_end":
        assert not lag_expiring(summary, table_owner(rel)), summary.outcomes
        assert held == "20260103"
    else:
        assert lag_expiring(summary, table_owner(rel)), summary.outcomes
        assert held == "20260101"


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_a_pin_at_an_epochs_last_end_is_judged_by_the_next_epochs_day(stores, tmp_path):
    """Epoch 2 starts at a LOWER LSN on day 2. A pin at epoch 1's
    last END is below every epoch-2 position (epoch first), so day 2 is its first
    unread-data day and it lag-expires once day 3 began long ago; an LSN-only comparison
    would treat day 2 as read."""
    from cdc_flight.spool2 import Spool as Spool2

    h = Harness(stores, tmp_path)
    days_log(h, ["20260101"])  # everyone at epoch 1's last END E1
    rel = rel_of(h, "third")
    e1 = h.log.checkpoint()
    low = 50  # below every epoch-1 LSN (the wire starts at 1000)
    h.log.acquire("epoch")
    try:
        h.log.begin_epoch(2, low, "slot_lost", "slot_e2", details={})  # a new slot, lower
    finally:
        h.log.release()
    h.spool = Spool2(
        replace(h.ingest_config, epoch=2),
        {oid: tuple(k) for oid, (_, k) in TABLES.items()},
        999,
        "m",
        binding=h.ingest_config.text_binding,
        start_lsn=low,
        unit_config=h.ingest_config.unit_config,
        clock=lambda: 0,
    )
    h.wire.lsn = h.wire.end = low
    # A slot loss holds every table for a resync, so all pins stay at (1, E1); only
    # third's is stalled.
    for key, day in enumerate(("20260102", "20260103", today()), start=2):
        item(h, day, key, describe=key == 2)
    assert pin(h, rel)["epoch"] == 1 and pin(h, rel)["lsn"] == e1 > low
    stall(h, table_owner(rel))
    summary = judged(h, config(h))
    assert lag_expiring(summary, table_owner(rel)), summary.outcomes
    assert standing_of(summary, table_owner(rel))["held_day"] == "20260102"


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_budget_expiry_still_counts_a_pin_equal_to_a_days_last_end(stores, tmp_path):
    """Budget keeps `<=`: a holder at day 1's last END still keeps day 1's bytes."""
    h = Harness(stores, tmp_path)
    rel = endpoint_holder(h, last=today())
    h.log.execute("UPDATE {s}frame_partitions_v2 SET bytes=1000")
    budget = replace(config(h), max_retained_lag_hours=8760, storage_budget_gb=1500 / 2**30)
    summary = judged(h, budget)
    outcomes = {(o.owner, o.reason, o.outcome) for o in summary.outcomes}
    assert (table_owner(rel), "budget", "would_expire") in outcomes, summary.outcomes


@pytest.mark.parametrize("stores", ["local"], indirect=True)
def test_compact_runs_first_on_a_mirror_no_new_apply_has_initialized(stores, tmp_path):
    """Combined deploy 2026-11-11: compact was the first Flight of the new release on a
    source, and its expiry path builds Apply without initializing it. Apply's state()
    and head() read apply_state columns only the new initialize adds, so the run failed
    to bind. They now read those columns as NULL when the mirror lacks them."""
    h = Harness(stores, tmp_path)
    rel = lagging(h)
    # The mirror as an apply of the previous release left it: no keyless columns.
    for column in ("capture_mode", "ignore_deletes", "deletes_ignored", "last_truncate_lsn"):
        h.store.con.execute(f"ALTER TABLE {h.apply.mirror}._cdc.apply_state DROP COLUMN {column}")
    result = run_compaction(config(h), h.store.con)
    assert (result.would_expire, result.expired) == (1, 0)
    result = run_compaction(on(h), h.store.con)
    assert result.expired == 1 and mirror(h, rel)[0] == "needs_resync"
