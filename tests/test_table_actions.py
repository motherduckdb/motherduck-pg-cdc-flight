"""Phase 3 apply-side actions (table_actions): pause, resume, retry, quarantine.

Local DuckDB on the Phase 2 harness (synthetic pgoutput). A poison transaction is a value
the mirror type rejects ("maybe" for a BOOLEAN in `other`); apply isolates it to the
exact commit and holds the table needs_action there.
"""

import os
import struct
import subprocess
import sys
import uuid

import duckdb
import pytest
from test_compaction import days_log
from test_expiry import on, stall
from test_phase2 import ITEMS, OTHER, TABLES, UNCHANGED, Harness, numeric, relation_message
from test_retention import ingest

from cdc_flight import explain, table_actions
from cdc_flight.cli import apply_failed
from cdc_flight.compact import run_compaction
from cdc_flight.epoch import lsn_text
from cdc_flight.flightlog import Run
from cdc_flight.retention import table_owner
from cdc_flight.table_actions import confirmation, parse_lsn, unit_confirmation

LOCAL = pytest.mark.parametrize("stores", ["local"], indirect=True)
OTHER_T = "public.other"


def blocked_unit(h, table):
    """The table's blocked unit (start, end) from the core's observation, or None."""
    _, observed = table_actions.observe(h.apply, table)
    if observed is None or observed["unit_start_lsn"] is None:
        return None
    return observed["unit_start_lsn"], observed["unit_end_lsn"]


def act(h, action, table, **kwargs):
    # Format 2: retry names the blocked unit too (expected_unit); by default the one held.
    if action == "retry" and "expected_unit" not in kwargs:
        kwargs["expected_unit"] = blocked_unit(h, table)
    return table_actions.ACTIONS[action](
        h.apply, table, run_id=uuid.uuid4().hex, actor="test", **kwargs
    )


def row(h, table, columns="state,blocked_commit_lsn,detail,token,held_since IS NOT NULL"):
    rel = h.apply.registered(table)[0]
    return h.store.con.execute(
        f"SELECT {columns} FROM {h.apply.mirror}._cdc.apply_state WHERE relation_id=?", [rel]
    ).fetchone()


def audit(h, table):
    rel = h.apply.registered(table)[0]
    return h.store.con.execute(
        f"SELECT status,detail FROM {h.apply.mirror}._cdc.runs WHERE relation_id=? "
        "AND status IN ('pause','resume','retry','quarantine') ORDER BY updated_at",
        [rel],
    ).fetchall()


def quarantined(h, table):
    rel = h.apply.registered(table)[0]
    return h.store.con.execute(
        f"SELECT commit_lsn,actor FROM {h.apply.mirror}._cdc.quarantine WHERE relation_id=?",
        [rel],
    ).fetchall()


def lsn_of(txn):
    return struct.unpack("!Q", txn[1:9])[0]


def poisoned(h, *, after=True):
    """`other` holds needs_action at a poison commit; returns its LSN."""
    w = h.wire
    rel = relation_message(2, "other", OTHER, identity="d")
    txns = [
        w.txn(rel, w.insert(2, ("1", "t", "x"))),
        w.txn(w.insert(2, ("2", "maybe", "y"))),
    ]
    if after:
        txns.append(w.txn(w.insert(2, ("3", "f", "z"))))
    poison = h.ingest(*txns)[1]
    result = h.run("other")
    assert result.status == "needs_action" and "ConversionException" in result.detail
    assert row(h, OTHER_T)[1] == lsn_of(poison)
    return lsn_of(poison)


def caught_up_items(h):
    w = h.wire
    h.ingest(w.txn(relation_message(1, "items", ITEMS), w.insert(1, ("1", "a", "t", "1.00"))))
    assert h.run("items").status == "caught_up"


# pause / resume ---------------------------------------------------------------------


@LOCAL
def test_pause_skips_a_table_and_resume_catches_it_up(stores, tmp_path):
    h = Harness(stores, tmp_path)
    caught_up_items(h)
    result = act(h, "pause", "public.items", expected_state="live")
    assert result.status == "done", result
    state, _, detail, _, held = row(h, "public.items")
    assert state == "paused" and detail.startswith("paused by test") and held
    w = h.wire
    h.ingest(w.txn(w.insert(1, ("2", "b", "t", "2.00"))))
    assert h.run("items").status == "paused"
    assert (2,) not in h.rows("items", "id")
    pin = h.apply.retention.pin(f"table:{h.apply.registered('public.items')[0]}")
    assert pin is not None  # the pin stays at the checkpoint while paused
    assert act(h, "resume", "public.items", expected_state="paused").status == "done"
    assert row(h, "public.items")[0] == "live" and not row(h, "public.items")[4]
    assert h.run("items").status == "caught_up"
    assert (2,) in h.rows("items", "id")
    assert [s for s, _ in audit(h, "public.items")] == ["pause", "resume"]


@LOCAL
def test_a_paused_table_draws_its_own_branch_and_does_not_fail_apply(stores, tmp_path, capsys):
    h = Harness(stores, tmp_path)
    caught_up_items(h)
    assert act(h, "pause", "public.items").status == "done"
    capsys.readouterr()
    run = Run()
    results = explain.apply(run, lambda f: h.apply.run(on_table=f), "m")
    out = capsys.readouterr().out
    assert "@flight-run paused ok key=public.items" in out
    assert "@flight-run action ok key=public.items" not in out
    assert not apply_failed([r for r in results if r.table == "public.items"])


# the state table, already, stale clicks, busy ----------------------------------------


@LOCAL
def test_each_action_refuses_the_states_it_does_not_apply_to(stores, tmp_path):
    h = Harness(stores, tmp_path)
    caught_up_items(h)
    poison = poisoned(h)
    # live
    assert act(h, "resume", "public.items").status == "already"
    assert act(h, "retry", "public.items", expected_blocked_lsn=1).status == "already"
    # needs_action
    for action in ("pause", "resume"):
        assert act(h, action, OTHER_T).status == "refused", action
    # paused
    assert act(h, "pause", "public.items").status == "done"
    assert act(h, "pause", "public.items").status == "already"
    assert act(h, "retry", "public.items", expected_blocked_lsn=poison).status == "refused"
    refused = quarantine(h, poison, table="public.items")  # a paused table: no poison unit
    assert refused.status == "refused" and "poison" in refused.detail
    # an unregistered table
    assert act(h, "pause", "public.nope").status == "refused"


@LOCAL
def test_a_stale_expected_value_is_refused_and_writes_nothing(stores, tmp_path):
    h = Harness(stores, tmp_path)
    caught_up_items(h)
    poison = poisoned(h)
    before = row(h, "public.items"), row(h, OTHER_T)
    stale = act(h, "pause", "public.items", expected_state="paused")
    assert stale.status == "refused" and "expected paused, found live" in stale.detail
    assert act(h, "resume", "public.items", expected_state="paused").status == "refused"
    assert act(h, "retry", OTHER_T, expected_blocked_lsn=poison + 1).status == "refused"
    assert (row(h, "public.items"), row(h, OTHER_T)) == before
    assert audit(h, "public.items") == audit(h, OTHER_T) == []


@LOCAL
def test_a_held_apply_lease_makes_the_action_busy(stores, tmp_path):
    h = Harness(stores, tmp_path)
    caught_up_items(h)
    rel = h.apply.registered("public.items")[0]
    token = h.apply.acquire(rel, "someone")
    try:
        assert act(h, "pause", "public.items").status == "busy"
    finally:
        h.apply.release(rel, token)
    assert row(h, "public.items")[0] == "live" and audit(h, "public.items") == []


@LOCAL
def test_a_pause_is_refused_while_a_backfill_generation_runs(stores, tmp_path, monkeypatch):
    h = Harness(stores, tmp_path)
    caught_up_items(h)
    monkeypatch.setattr(h.apply, "active_generation", lambda relation_id: "g")
    result = act(h, "pause", "public.items")
    assert result.status == "refused" and "backfill is running" in result.detail


@LOCAL
def test_a_row_that_moves_after_the_lease_rolls_the_action_back(stores, tmp_path, monkeypatch):
    """The compare-and-set: the row changes under the action's own lease (a stand-in for
    any write the observation missed), so nothing is written."""
    h = Harness(stores, tmp_path)
    caught_up_items(h)
    real = h.apply.acquire

    def moved(relation_id, run_id, *, token=None):
        got = real(relation_id, run_id, token=token)
        h.store.con.execute(
            f"UPDATE {h.apply.mirror}._cdc.apply_state SET state='needs_action' "
            "WHERE relation_id=?",
            [relation_id],
        )
        return got

    monkeypatch.setattr(h.apply, "acquire", moved)
    result = act(h, "pause", "public.items")
    assert result.status == "refused" and "changed" in result.detail
    assert row(h, "public.items")[0] == "needs_action" and audit(h, "public.items") == []


# retry -----------------------------------------------------------------------------


@LOCAL
def test_retry_returns_a_table_to_live_and_a_still_bad_commit_is_isolated_again(stores, tmp_path):
    h = Harness(stores, tmp_path)
    poison = poisoned(h)
    assert act(h, "retry", OTHER_T, expected_blocked_lsn=poison).status == "done"
    assert row(h, OTHER_T)[:2] == ("live", None) and not row(h, OTHER_T)[4]
    assert h.run("other").status == "needs_action"
    assert row(h, OTHER_T)[1] == poison
    assert [s for s, _ in audit(h, OTHER_T)] == ["retry"]


# quarantine ------------------------------------------------------------------------


def recorded_unit(h, table):
    rel = h.apply.registered(table)[0]
    return h.store.con.execute(
        f"SELECT unit_start_lsn,unit_end_lsn FROM {h.apply.mirror}._cdc.quarantine "
        "WHERE relation_id=? ORDER BY created_at DESC LIMIT 1",
        [rel],
    ).fetchone()


def quarantine(h, poison, table=OTHER_T, **overrides):
    """Format 2's whole-unit quarantine of the held unit (`poison` is its blocked commit;
    the Harness's one commit per unit makes the unit exactly that commit). After a done
    quarantine the table is live, so a repeat names the recorded unit."""
    unit = blocked_unit(h, table) or recorded_unit(h, table) or (poison, poison)
    kwargs = dict(
        unit_start_lsn=unit[0],
        unit_end_lsn=unit[1],
        expected_epoch=1,
        confirm=unit_confirmation(table, 1, *unit),
    )
    kwargs.update(overrides)
    return act(h, "quarantine", table, **kwargs)


@LOCAL
def test_quarantine_skips_exactly_the_poison_commit_and_records_the_gap(stores, tmp_path):
    h = Harness(stores, tmp_path)
    caught_up_items(h)
    poison = poisoned(h)
    result = quarantine(h, poison)
    assert result.status == "done", result
    assert "rows touched by unit" in result.detail and f"in {OTHER_T} may diverge" in result.detail
    assert result.next_steps == [f"resync {OTHER_T}"]
    assert quarantined(h, OTHER_T) == [(poison, "test")]
    assert h.run("other").status == "caught_up"
    assert h.rows("other", "id") == [(1,), (3,)]  # the later commit applied
    assert row(h, OTHER_T, "applied_through_lsn")[0] > poison
    assert [s for s, _ in audit(h, OTHER_T)] == ["quarantine"]
    assert quarantine(h, poison).status == "already"
    w = h.wire
    h.ingest(w.txn(w.insert(1, ("9", "z", "t", "9.00"))))
    assert h.run("items").status == "caught_up"  # other tables are unaffected


@LOCAL
def test_a_quarantined_last_commit_moves_the_checkpoint_to_the_source_position(stores, tmp_path):
    h = Harness(stores, tmp_path)
    poison = poisoned(h, after=False)
    assert quarantine(h, poison).status == "done"
    assert h.run("other").status == "caught_up"
    applied = row(h, OTHER_T, "applied_through_lsn")[0]
    assert applied > poison and applied == h.log.checkpoint()


@LOCAL
@pytest.mark.parametrize(
    "overrides,message",
    [
        (dict(confirm="lowercase"), "type the confirmation exactly"),
        (dict(epoch_delta=1), "state changed"),
        (dict(lsn_delta=1), "state changed"),
        (dict(trailing_space=True), "type the confirmation exactly"),
        (dict(lower_hex=True), "type the confirmation exactly"),
    ],
)
def test_quarantine_refuses_anything_but_the_exact_blocked_commit(
    stores, tmp_path, overrides, message
):
    h = Harness(stores, tmp_path)
    poison = poisoned(h)
    start, end = blocked_unit(h, OTHER_T)
    literal = unit_confirmation(OTHER_T, 1, start, end)
    kwargs = {}
    if overrides.get("confirm"):
        kwargs["confirm"] = literal.lower()
    if overrides.get("trailing_space"):
        kwargs["confirm"] = literal + " "
    if overrides.get("lower_hex"):
        kwargs["confirm"] = (
            f"QUARANTINE {OTHER_T} UNIT 1 {lsn_text(start).lower()}..{lsn_text(end).lower()}"
        )
    if overrides.get("epoch_delta"):
        kwargs["expected_epoch"] = 2
    if overrides.get("lsn_delta"):
        kwargs["unit_end_lsn"] = end + 1
        kwargs["confirm"] = unit_confirmation(OTHER_T, 1, start, end + 1)
    before = row(h, OTHER_T)
    result = quarantine(h, poison, **kwargs)
    assert result.status == "refused" and message in result.detail, result
    assert row(h, OTHER_T) == before and quarantined(h, OTHER_T) == []


def test_the_confirmation_literal_uses_the_canonical_lsn():
    assert confirmation("s.t", parse_lsn("0/5a3b")) == "QUARANTINE s.t 0/5A3B"
    assert parse_lsn("00000001/0000000A") == (1 << 32) | 10
    for bad in ("", "5A3B", "0/5A3B/1", "0x1/2", "g/1"):
        with pytest.raises(ValueError):
            parse_lsn(bad)


@LOCAL
def test_a_schema_block_is_never_quarantined(stores, tmp_path):
    h = Harness(stores, tmp_path)
    caught_up_items(h)
    w = h.wire
    changed = [*ITEMS[:3], (1, "amount", 1700, numeric(12, 2))]
    blocked = h.ingest(
        w.txn(relation_message(1, "items", changed), w.insert(1, ("8", "v8", "t", "8.00")))
    )[0]
    assert h.run("items").status == "needs_action"
    lsn = row(h, "public.items")[1]
    assert lsn is not None and lsn >= lsn_of(blocked)
    result = quarantine(h, lsn, table="public.items")
    assert result.status == "refused"
    assert result.detail == (
        "only a poison transaction can be quarantined; "
        "this table is held on: column 'amount' changed type"
    )
    assert result.next_steps == ["retry public.items", "resync public.items"]
    assert quarantined(h, "public.items") == []


@LOCAL
def test_a_blocked_commit_that_moves_under_the_lease_inserts_no_quarantine_row(
    stores, tmp_path, monkeypatch
):
    """The quarantine row is inserted only while the row, under this lease,
    still blocks on the commit being quarantined."""
    h = Harness(stores, tmp_path)
    poison = poisoned(h)
    real = h.apply.acquire

    def moved(relation_id, run_id, *, token=None):
        got = real(relation_id, run_id, token=token)
        h.store.con.execute(
            f"UPDATE {h.apply.mirror}._cdc.apply_state SET blocked_commit_lsn=blocked_commit_lsn+1 "
            "WHERE relation_id=?",
            [relation_id],
        )
        return got

    monkeypatch.setattr(h.apply, "acquire", moved)
    result = quarantine(h, poison)
    assert result.status == "refused"
    assert quarantined(h, OTHER_T) == [] and row(h, OTHER_T)[0] == "needs_action"
    assert audit(h, OTHER_T) == []


@LOCAL
def test_a_failed_state_change_rolls_back_the_quarantine_row(stores, tmp_path, monkeypatch):
    """The quarantine row and the state change commit together or not at all."""
    h = Harness(stores, tmp_path)
    poison = poisoned(h)
    real = h.apply.acquire

    def moved(relation_id, run_id, *, token=None):
        got = real(relation_id, run_id, token=token)
        h.store.con.execute(
            f"UPDATE {h.apply.mirror}._cdc.apply_state SET applied_through_lsn=applied_through_lsn+1 "
            "WHERE relation_id=?",
            [relation_id],
        )
        return got

    monkeypatch.setattr(h.apply, "acquire", moved)
    result = quarantine(h, poison)
    assert result.status == "refused" and "changed" in result.detail
    assert quarantined(h, OTHER_T) == [] and audit(h, OTHER_T) == []


@LOCAL
def test_the_skip_list_is_keyed_by_relation_generation_and_epoch(stores, tmp_path):
    """A resync's new generation (or a new epoch) never inherits a quarantine."""
    h = Harness(stores, tmp_path)
    poison = poisoned(h)
    assert quarantine(h, poison).status == "done"
    start, end = recorded_unit(h, OTHER_T)
    [(s_, e_, digest)] = h.apply.head(OTHER_T)["skip"]  # format 2: whole units, with digest
    assert (s_, e_) == (start, end) and digest
    assert h.apply.head("public.items")["skip"] == set()
    m = f"{h.apply.mirror}._cdc"
    rel = h.apply.registered(OTHER_T)[0]
    # The same generation id under another relation is not this table's.
    generation = row(h, OTHER_T, "generation_id")[0]
    items = h.apply.registered("public.items")[0]
    h.store.con.execute(
        f"UPDATE {m}.apply_state SET generation_id=? WHERE relation_id=?", [generation, items]
    )
    assert h.apply.head("public.items")["skip"] == set()
    for column, value in (("generation_id", "new-generation"), ("epoch", 2)):
        saved = row(h, OTHER_T, column)[0]
        h.store.con.execute(
            f"UPDATE {m}.apply_state SET {column}=? WHERE relation_id=?", [value, rel]
        )
        assert h.apply.head(OTHER_T)["skip"] == set(), column
        h.store.con.execute(
            f"UPDATE {m}.apply_state SET {column}=? WHERE relation_id=?", [saved, rel]
        )


@LOCAL
def test_backfill_reads_never_filter_quarantined_commits(stores, tmp_path):
    h = Harness(stores, tmp_path)
    poison = poisoned(h)
    assert quarantine(h, poison).status == "done"
    rel = h.apply.registered(OTHER_T)[0]
    # The log reader (which backfill's catch-up uses) never consults the mirror's
    # quarantine: the skipped unit's events are still read.
    reader = h.apply.reader
    page = reader.units(1, 0, h.log.checkpoint())
    events = reader.events(page, rel, high=h.log.checkpoint())
    assert poison in {e["commit_lsn"] for e in events}


# dry runs ---------------------------------------------------------------------------


@LOCAL
def test_a_dry_run_validates_and_writes_nothing(stores, tmp_path):
    h = Harness(stores, tmp_path)
    caught_up_items(h)
    poison = poisoned(h)
    snapshot = lambda: (  # noqa: E731
        row(h, "public.items"),
        row(h, OTHER_T),
        audit(h, "public.items"),
        audit(h, OTHER_T),
        quarantined(h, OTHER_T),
    )
    before = snapshot()
    assert act(h, "pause", "public.items", dry_run=True).status == "would"
    assert act(h, "retry", OTHER_T, expected_blocked_lsn=poison, dry_run=True).status == "would"
    preview = quarantine(h, poison, dry_run=True)
    assert preview.status == "would" and "ConversionException" in preview.detail
    assert preview.observed["token"] == row(h, OTHER_T)[3]  # for a preview's hash
    assert snapshot() == before  # nothing written, and no lease taken (the token is unchanged)


# existing mirrors -------------------------------------------------------------------


@LOCAL
def test_an_existing_mirror_gains_the_quarantine_table(stores, tmp_path):
    h = Harness(stores, tmp_path)
    h.store.con.execute(f"DROP TABLE {h.apply.mirror}._cdc.quarantine")
    h.apply.initialize()
    assert h.store.con.execute(
        f"SELECT count(*) FROM {h.apply.mirror}._cdc.quarantine"
    ).fetchone() == (0,)


# retention expiry --------------------------------------------------------------


@LOCAL
def test_a_paused_table_that_expires_cannot_be_resumed(stores, tmp_path):
    h = Harness(stores, tmp_path)
    # test_expiry.lagging, with `third` paused after day 1 instead of just not applied.
    days_log(h, ["20260101"])
    assert act(h, "pause", "public.third").status == "done"
    w = h.wire
    for day in ("20260102", "20260103"):
        ingest(h, day, w.txn(w.insert(1, (day, "v", "t", "1.00"))))
        for name, _ in TABLES.values():
            assert h.run(name).status == ("paused" if name == "third" else "caught_up")
    stall(h, table_owner(h.apply.registered("public.third")[0]))
    assert run_compaction(on(h), h.store.con).expired == 1
    assert row(h, "public.third")[0] == "needs_resync"
    result = act(h, "resume", "public.third")
    assert result.status == "refused" and result.next_steps == ["resync public.third"]


# CLI --------------------------------------------------------------------------------


def run_tables(tmp_path, *args, **env):
    environ = {k: v for k, v in os.environ.items() if not k.startswith("CDC_")}
    environ.update(MOTHERDUCK_TOKEN="unused", CDC_SOURCE_ID="s", **env)
    return subprocess.run(
        [sys.executable, "-c", "from cdc_flight.tables_cli import main; main()", *args],
        env=environ,
        capture_output=True,
        text=True,
        cwd=tmp_path,
    )


@pytest.mark.parametrize(
    "args,message",
    [
        (("retry", "--table", "public.a"), "--blocked-lsn: an LSN is"),
        (("retry", "--table", "public.a", "--blocked-lsn", "5A3B"), "--blocked-lsn: an LSN is"),
        # Format 2 quarantines the observed blocked unit: no commit LSN input.
        (("quarantine", "--table", "public.a"), "requires --epoch and --confirm"),
        (("quarantine", "--table", "public.a", "--epoch", "1"), "requires --epoch and --confirm"),
    ],
)
def test_the_cli_refuses_bad_action_inputs_before_connecting(tmp_path, args, message):
    out = run_tables(tmp_path, *args)
    assert out.returncode == 1 and message in out.stderr, out.stderr
    assert "Traceback" not in out.stderr


def test_the_cli_acts_on_the_configured_mirror_without_postgres(monkeypatch):
    """No DSN is needed, and CDC_MIRROR_DATABASE names the mirror it connects to."""
    from cdc_flight import tables_cli

    seen = []

    class Stop(Exception):
        pass

    def connect(address, *a, **k):
        seen.append(address)
        raise Stop

    monkeypatch.setattr(duckdb, "connect", connect)
    for key in [k for k in os.environ if k.startswith("CDC_")]:
        monkeypatch.delenv(key)
    monkeypatch.setenv("CDC_SOURCE_ID", "s")
    monkeypatch.setenv("CDC_MIRROR_DATABASE", "custom_mirror")
    monkeypatch.setattr(sys, "argv", ["cdc-flight-tables", "pause", "--table", "public.a"])
    with pytest.raises(Stop):
        tables_cli.main()
    assert seen == ["md:custom_mirror?attach_mode=single"]


# holds without a poison transaction --------------------------------


def missing_row_hold(h):
    """`fourth` (default identity) gets an update of a row its mirror lacks: held with no
    blocked commit, repairable by inserting the row."""
    w = h.wire
    h.ingest(
        w.txn(relation_message(4, "fourth", OTHER, identity="d"), w.insert(4, ("1", "t", "x")))
    )
    assert h.run("fourth").status == "caught_up"
    h.ingest(w.txn(w.update(4, ("9", "f", UNCHANGED))))
    result = h.run("fourth")
    assert result.status == "needs_action" and "full image" in result.detail
    assert row(h, "public.fourth")[1] is None


@LOCAL
def test_retry_with_no_blocked_commit_after_repairing_the_mirror(stores, tmp_path):
    h = Harness(stores, tmp_path)
    missing_row_hold(h)
    h.store.con.execute(
        f"INSERT INTO {h.apply.mirror}.public.fourth (id, flag, blob, _cdc_commit_lsn, _cdc_updated_at) "
        "VALUES (9, true, 'kept', 0, current_timestamp)"
    )
    assert act(h, "retry", "public.fourth", expected_blocked_lsn=None).status == "done"
    assert h.run("fourth").status == "caught_up"
    assert (9, False, "kept") in h.rows("fourth", "id,flag,blob")


@LOCAL
def test_retry_compares_a_missing_blocked_commit_exactly_both_ways(stores, tmp_path):
    h = Harness(stores, tmp_path)
    missing_row_hold(h)
    poison = poisoned(h)
    wrong = act(h, "retry", "public.fourth", expected_blocked_lsn=poison)
    assert wrong.status == "refused" and "found none" in wrong.detail
    wrong = act(h, "retry", OTHER_T, expected_blocked_lsn=None)
    assert (
        wrong.status == "refused"
        and f"expected blocked commit none, found {lsn_text(poison)}" in wrong.detail
    )


@LOCAL
def test_quarantine_names_a_non_poison_hold_and_offers_only_resync(stores, tmp_path):
    h = Harness(stores, tmp_path)
    w = h.wire
    h.ingest(
        w.txn(relation_message(2, "other", OTHER, identity="d"), w.insert(2, ("1", "t", "x"))),
        w.txn(w.insert(2, ("2", UNCHANGED, "y"))),
    )
    assert h.run("other").status == "needs_action"
    lsn = row(h, OTHER_T)[1]
    result = quarantine(h, lsn)
    assert result.status == "refused"
    assert result.detail.endswith("held on: insert without a complete new image")
    assert result.next_steps == [f"resync {OTHER_T}"]
    missing_row_hold(h)
    result = quarantine(h, 1, table="public.fourth")
    assert result.status == "refused" and "full image" in result.detail
    assert result.next_steps == ["retry public.fourth --blocked-lsn none", "resync public.fourth"]


def test_the_schema_hold_list_is_exactly_reconciles_messages():
    """table_actions.SCHEMA_HOLD (retry is offered) matches every Blocked message
    mirror.reconcile raises, and none of the event-level or poison holds."""
    import ast
    import inspect

    from cdc_flight import mirror
    from cdc_flight.apply_run import POISON
    from cdc_flight.table_actions import SCHEMA_HOLD

    def rendered(node):
        if isinstance(node, ast.Constant):
            return node.value
        return "".join(p.value if isinstance(p, ast.Constant) else "'x'" for p in node.values)

    tree = ast.parse(inspect.getsource(mirror.reconcile))
    messages = [
        rendered(call.args[1])
        for call in ast.walk(tree)
        if isinstance(call, ast.Call) and getattr(call.func, "id", "") == "Blocked"
    ]
    assert len(messages) >= 7
    for message in messages:
        assert SCHEMA_HOLD.match(message), message
    for other in (
        "unsupported change 'x'",
        "insert without a complete new image",
        "update of a row absent from the mirror lacks a full image",
        "replica identity cell is 'x', not a value",
        POISON.format("ConversionException", 1),
    ):
        assert not SCHEMA_HOLD.match(other), other


def test_the_cli_takes_none_as_a_missing_blocked_commit(monkeypatch):
    from cdc_flight import tables_cli

    class Stop(Exception):
        pass

    def connect(address, *a, **k):
        raise Stop

    monkeypatch.setattr(duckdb, "connect", connect)
    for key in [k for k in os.environ if k.startswith("CDC_")]:
        monkeypatch.delenv(key)
    monkeypatch.setenv("CDC_SOURCE_ID", "s")
    monkeypatch.setattr(
        sys, "argv", ["cdc-flight-tables", "retry", "--table", "public.a", "--blocked-lsn", "none"]
    )
    with pytest.raises(Stop):  # parsed and validated, then stopped at the first connect
        tables_cli.main()
    for bad in ("None", "NONE", ""):
        monkeypatch.setattr(
            sys, "argv", ["cdc-flight-tables", "retry", "--table", "public.a", "--blocked-lsn", bad]
        )
        with pytest.raises(SystemExit, match="--blocked-lsn"):
            tables_cli.main()


# accept-types ------------------------------------------------------------------


def stored_type(h, table, column):
    import json as _json

    rel = h.apply.registered(table)[0]
    (cols,) = h.store.con.execute(
        f"SELECT columns FROM {h.apply.mirror}._cdc.apply_state WHERE relation_id=?", [rel]
    ).fetchone()
    return next(c[2:] for c in _json.loads(cols) if c[0] == column)


def retyped_items(h, *extra_columns):
    """items held needs_action on value text -> varchar(20) (type_changed, #4 (i))."""
    caught_up_items(h)
    w = h.wire
    retyped = [ITEMS[0], (1, "value", 1043, 24), *ITEMS[2:], *extra_columns]
    h.ingest(w.txn(relation_message(1, "items", retyped), w.insert(1, ("2", "b", "t", "2.00"))))
    result = h.run("items")
    assert result.status == "needs_action" and "its source type" in result.detail, result


@LOCAL
def test_accept_types_accepts_a_held_retype_after_its_dry_run_then_retry_catches_up(
    stores, tmp_path
):
    h = Harness(stores, tmp_path)
    retyped_items(h)
    items = "public.items"
    dry = act(h, "accept-types", items, dry_run=True)
    assert dry.status == "would" and "USING rewrite needs a resync" in dry.detail
    assert dry.observed["type_changes"] == [["value", [25, -1], [1043, 24]]]
    text = dry.observed["confirm"]
    assert text.startswith(f"ACCEPT TYPES {items} ") and dry.next_steps == [f"type exactly: {text}"]
    wrong = act(h, "accept-types", items, confirm=f"ACCEPT TYPES {items} 000000000000")
    assert wrong.status == "refused" and wrong.detail == f"type exactly: {text}"
    assert stored_type(h, items, "value") == [25, -1]  # nothing written
    done = act(h, "accept-types", items, confirm=text)
    assert done.status == "done" and done.next_steps == [f"retry {items}"], done
    assert stored_type(h, items, "value") == [1043, 24]
    assert row(h, items)[0] == "needs_action"  # Retry is a separate step
    rel = h.apply.registered(items)[0]
    audit_rows = h.store.con.execute(
        f"SELECT detail FROM {h.apply.mirror}._cdc.runs WHERE relation_id=? AND status='accept-types'",
        [rel],
    ).fetchall()
    assert len(audit_rows) == 1 and audit_rows[0][0].startswith(text)
    again = act(h, "accept-types", items, dry_run=True)
    assert again.status == "refused" and "retry" in again.detail
    retried = act(h, "retry", items, expected_blocked_lsn=row(h, items)[1])
    assert retried.status == "done", retried
    assert h.run("items").status == "caught_up"
    assert ("b",) in h.rows("items", "value")


@LOCAL
def test_accept_types_refuses_other_holds_and_live_tables(stores, tmp_path):
    h = Harness(stores, tmp_path)
    caught_up_items(h)
    refused = act(h, "accept-types", "public.items", dry_run=True)
    assert refused.status == "refused" and "fixes only a source type" in refused.detail
    poisoned(h)  # needs_action on a poison commit, not a type hold
    refused = act(h, "accept-types", OTHER_T, dry_run=True)
    assert refused.status == "refused" and "fixes only a source type" in refused.detail


@LOCAL
def test_accept_types_digest_names_the_change_so_a_moved_unit_needs_a_new_dry_run(stores, tmp_path):
    """The confirmation's digest covers the schema version and the blocked unit: the same
    text never accepts a different change."""
    h = Harness(stores, tmp_path)
    retyped_items(h)
    first = act(h, "accept-types", "public.items", dry_run=True).observed["confirm"]
    other = table_actions.accept_confirmation("public.items", "v-other", {}, [])[0]
    assert first != other and len(first.split()[-1]) == 12


@LOCAL
def test_accept_types_reads_the_blocked_commits_own_version_with_a_later_ddl_in_the_unit(
    stores, tmp_path
):
    """A retype commit, then an add-column commit, in one ingest: the accepted
    types are the blocked commit's own schema version's, and the later one applies after
    the Retry."""
    h = Harness(stores, tmp_path)
    caught_up_items(h)
    w = h.wire
    retyped = [ITEMS[0], (1, "value", 1043, 24), *ITEMS[2:]]
    added = [*retyped, (0, "note", 25, -1)]
    h.ingest(
        w.txn(relation_message(1, "items", retyped), w.insert(1, ("2", "b", "t", "2.00"))),
        w.txn(relation_message(1, "items", added), w.insert(1, ("3", "c", "t", "3.00", "n"))),
    )
    assert h.run("items").status == "needs_action"
    dry = act(h, "accept-types", "public.items", dry_run=True)
    assert dry.status == "would" and dry.observed["type_changes"] == [
        ["value", [25, -1], [1043, 24]]
    ]
    assert act(h, "accept-types", "public.items", confirm=dry.observed["confirm"]).status == "done"
    retried = act(h, "retry", "public.items", expected_blocked_lsn=row(h, "public.items")[1])
    assert retried.status == "done"
    assert h.run("items").status == "caught_up"
    assert ("c", "n") in h.rows("items", "value,note")
