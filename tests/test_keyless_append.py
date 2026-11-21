"""Keyless append (insert-only) capture, phase 1b: apply.

Local stores, real IngestLog2 and Spool (test_apply2.Env). public.others is registered
for append capture as add-table registers it (no key, key_columns '[]'), and its frames
carry an empty key, as ingest writes them; public.items stays keyed beside it.

- Every INSERT is one mirror row, duplicates kept; (commit lsn, event ordinal) names it.
- UPDATE and DELETE hold the table before their whole unit (append_update,
  append_delete), or a DELETE is skipped and counted under ignore_deletes.
- TRUNCATE empties the mirror in place; only later rows remain.
- Exactly once by the checkpoint rule: a crash before or after COMMIT, then a rerun.
- The keyed table sharing the units applies exactly as before, on both folds and in
  either order (the page cache is shared and never mutated).
"""

from collections import Counter

import pgstream
import pytest
from test_apply2 import COLUMNS, OID, OTHER, OTHER_TABLE, TABLE, Env

from cdc_flight import table_actions as ta
from cdc_flight.apply import ApplyError
from cdc_flight.spool2 import Spool

LOCAL = pytest.mark.parametrize("stores", ["local"], indirect=True)
WIDE = ((1, "id", 25, -1), (0, "v", 25, -1), (0, "w", 25, -1))


class Crash(Exception):
    pass


def append_env(stores, *, ignore_deletes=False, fold="sql", max_commits=2, start=True):
    """Env with public.others registered for append capture, its spool keyless, and (by
    default) its generation started from the log at S."""
    env = Env(stores, fold=fold, max_commits=max_commits)
    env.store.con.execute(
        f"UPDATE {env.log.prefix}tables SET capture_mode='append',key_columns='[]',"
        "catalog_key=NULL,ignore_deletes=? WHERE relation_id=?",
        [ignore_deletes, OTHER],
    )
    env.log.forget()
    env.catalog_keys = {k: v for k, v in env.catalog_keys.items() if k != OTHER}
    keys = {OID: ("id",), OTHER: (), 900: ("id",)}
    env.spool = Spool(
        env.log_config,
        keys,
        900,
        "marker",
        binding=env.log_config.text_binding,
        start_lsn=env.log.checkpoint(),
        unit_config=env.log_config.unit_config,
        clock=lambda: 0,
    )
    env.feed(
        [
            pgstream.relation(OID, "public", "items", "f", COLUMNS),
            pgstream.relation(OTHER, "public", "others", "f", COLUMNS),
        ]
    )
    if start:
        env.apply.start_from_log(TABLE)
        env.apply.start_append(OTHER_TABLE, accept_incomplete=True)
    return env


def ins(key, value):
    return pgstream.insert(OTHER, (key, value))


def item(key, value):
    return pgstream.insert(OID, (key, value))


def rows(env, columns="id,v"):
    """The append mirror's rows as a multiset."""
    try:
        found = env.store.con.execute(
            f"SELECT {columns} FROM {env.apply.mirror}.public.others"
        ).fetchall()
    except Exception:
        return Counter()
    return Counter(found)


def state(env):
    return env.store.con.execute(
        f"SELECT state,detail,applied_through_lsn,blocked_unit_start_lsn,deletes_ignored,"
        f"last_truncate_lsn FROM {env.apply.mirror}._cdc.apply_state WHERE relation_id=?",
        [OTHER],
    ).fetchone()


def apply(env, **kw):
    return env.apply.apply_table(OTHER_TABLE, "run", **kw)


@LOCAL
def test_every_insert_is_a_row_duplicates_kept_with_lsn_and_ordinal(stores):
    env = append_env(stores)
    c1 = env.commit(ins("1", "a"), ins("1", "a"), ins("2", None))
    c2 = env.commit(ins("1", "a"))
    env.commit(ins("3", "c"))
    env.feed([], close=True)  # the open unit completes
    result = apply(env)
    assert result.status == "caught_up", result
    assert rows(env) == Counter({("1", "a"): 3, ("2", None): 1, ("3", "c"): 1})
    named = env.store.con.execute(
        f"SELECT _cdc_commit_lsn,_cdc_event_ordinal,count(*) FROM {env.apply.mirror}.public.others "
        "GROUP BY ALL ORDER BY ALL"
    ).fetchall()
    assert all(n == 1 for *_, n in named) and named[0][0] == c1 and named[-2][0] == c2
    # No primary key: the mirror is a plain table.
    assert not env.store.con.execute(
        "SELECT count(*) FROM duckdb_constraints() WHERE table_name='others' "
        "AND constraint_type='PRIMARY KEY'"
    ).fetchone()[0]


@LOCAL
@pytest.mark.parametrize("op", ["update", "delete"])
def test_an_update_or_delete_holds_before_its_whole_unit(stores, op):
    env = append_env(stores)  # two commits per unit
    env.commit(ins("1", "a"))
    env.commit(ins("2", "b"))
    env.commit(ins("3", "c"))  # unit 2, before the blocking commit: never applied
    change = (
        pgstream.update(OTHER, ("1", "z"), ("1", "a"))
        if op == "update"
        else pgstream.delete(OTHER, ("1", "a"))
    )
    c4 = env.commit(change)
    first, second = env.units()[-2:]
    result = apply(env)
    assert result.status == "needs_action" and f"append_{op}" in result.detail
    assert rows(env) == Counter({("1", "a"): 1, ("2", "b"): 1})
    s = state(env)
    assert (s[0], s[2], s[3]) == ("needs_action", second[0], second[0]) and c4 > second[0]


@LOCAL
def test_ignore_deletes_skips_and_counts_every_delete_truncate_included(stores):
    """N-3: deletes_ignored counts DELETE events, those before a TRUNCATE in the same batch
    too; the TRUNCATE is still applied in place (D10)."""
    env = append_env(stores, ignore_deletes=True, max_commits=10)
    env.commit(ins("1", "a"), ins("2", "b"))
    env.commit(pgstream.delete(OTHER, ("1", "a")))
    t = env.commit(pgstream.truncate([OTHER]), ins("3", "c"))
    env.commit(pgstream.delete(OTHER, ("3", "c")), ins("4", "d"))
    env.feed([], close=True)
    assert apply(env).status == "caught_up"
    assert rows(env) == Counter({("3", "c"): 1, ("4", "d"): 1})
    s = state(env)
    assert (s[4], s[5]) == (2, t)


@LOCAL
@pytest.mark.parametrize("max_commits", [1, 10], ids=["across-units", "one-unit"])
def test_truncate_is_applied_in_place(stores, max_commits):
    env = append_env(stores, max_commits=max_commits)
    env.commit(ins("1", "a"), ins("2", "b"))
    if max_commits == 1:
        assert apply(env).status == "caught_up"  # the mirror exists before the TRUNCATE
    # insert, TRUNCATE, insert in one transaction: only the row after it remains.
    env.commit(ins("3", "c"), pgstream.truncate([OTHER]), ins("4", "d"))
    env.commit(ins("5", "e"))
    env.feed([], close=True)
    assert apply(env).status == "caught_up"
    assert rows(env) == Counter({("4", "d"): 1, ("5", "e"): 1})


@LOCAL
def test_a_truncate_naming_a_keyed_and_an_append_table(stores):
    env = append_env(stores)
    env.commit(item("1", "a"), ins("1", "a"))
    env.commit(item("2", "b"), ins("2", "b"))
    env.commit(pgstream.truncate([OID, OTHER]), ins("3", "c"))
    env.commit(ins("4", "d"))
    results = {r.table: r for r in env.apply.run()}
    assert results[OTHER_TABLE].status == "caught_up"
    assert rows(env) == Counter({("3", "c"): 1, ("4", "d"): 1})
    assert results[TABLE].status == "needs_resync"  # keyed: as before
    assert env.mirror() == {"1": "a", "2": "b"}


@LOCAL
def test_add_column_and_a_changed_identity(stores):
    """ADD COLUMN: earlier rows NULL. A relation without FULL identity still applies its
    inserts (they carry every column); ingest holds the registry row instead."""
    env = append_env(stores, max_commits=10)
    env.commit(ins("1", "a"))
    env.commit(
        pgstream.relation(OTHER, "public", "others", "d", WIDE),
        pgstream.insert(OTHER, ("2", "b", "x")),
    )
    env.feed([], close=True)
    assert apply(env).status == "caught_up"
    assert rows(env, "id,v,w") == Counter({("1", "a", None): 1, ("2", "b", "x"): 1})


@LOCAL
def test_an_incomplete_insert_holds_at_its_unit(stores):
    env = append_env(stores)
    env.commit(ins("1", "a"))
    env.commit(ins("2", "b"))
    env.commit(ins("3", "c"))
    env.commit(pgstream.insert(OTHER, ("4", pgstream.UNCHANGED)))
    result = apply(env)
    assert result.status == "needs_action" and "complete new image" in result.detail
    assert rows(env) == Counter({("1", "a"): 1, ("2", "b"): 1})


@LOCAL
def test_quarantine_skips_the_blocked_unit_and_its_inserts(stores):
    """D8: quarantine is whole-unit, so the unit's inserts go with the offending DELETE."""
    env = append_env(stores)
    env.commit(ins("1", "a"))
    env.commit(ins("2", "b"))
    env.commit(ins("3", "c"))
    env.commit(pgstream.delete(OTHER, ("1", "a")))
    env.commit(ins("5", "e"))
    env.commit(ins("6", "f"))
    assert apply(env).status == "needs_action"
    relation_id, observed = ta.observe(env.apply, OTHER_TABLE)
    assert observed["capture_mode"] == "append"
    unit = ta.unit_confirmation(
        OTHER_TABLE, observed["epoch"], observed["unit_start_lsn"], observed["unit_end_lsn"]
    )
    done = ta.quarantine(
        env.apply,
        OTHER_TABLE,
        run_id="q",
        actor="t",
        expected_epoch=observed["epoch"],
        confirm=unit,
        unit_start_lsn=observed["unit_start_lsn"],
        unit_end_lsn=observed["unit_end_lsn"],
    )
    assert done.status == "done", done
    assert apply(env).status == "caught_up"
    assert rows(env) == Counter({("1", "a"): 1, ("2", "b"): 1, ("5", "e"): 1, ("6", "f"): 1})


@LOCAL
@pytest.mark.parametrize("stage", ["after_merge", "after_commit"])
def test_a_crash_before_or_after_commit_then_a_rerun_inserts_once(stores, stage):
    env = append_env(stores)
    for i in range(6):
        env.commit(ins(str(i % 2), "dup"))

    def fault(at):
        if at == stage:
            raise Crash(at)

    with pytest.raises(Crash):
        apply(env, fault=fault)
    env.store.con.execute(
        f"UPDATE {env.apply.mirror}._cdc.apply_state SET expires_at=NULL WHERE relation_id=?",
        [OTHER],
    )
    assert apply(env).status == "caught_up"
    assert rows(env) == Counter({("0", "dup"): 3, ("1", "dup"): 3})


@LOCAL
@pytest.mark.parametrize("fold", ["sql", "python"])
@pytest.mark.parametrize("order", ["keyed-first", "append-first"])
def test_mixed_units_leave_both_mirrors_exact(stores, fold, order):
    """N-2: both tables plan the same cached page, in either order; neither is changed by
    the other (events are never mutated)."""
    env = append_env(stores, fold=fold, max_commits=3)
    for i in range(1, 8):
        env.commit(item(str(i), f"v{i}"), ins(str(i % 3), f"o{i % 3}"))
    env.commit(pgstream.update(OID, ("2", "v2b"), None), ins("9", "o9"))
    env.feed([], close=True)
    tables = (TABLE, OTHER_TABLE) if order == "keyed-first" else (OTHER_TABLE, TABLE)
    results = {r.table: r for r in env.apply.run(tables=tables)}
    assert {r.status for r in results.values()} == {"caught_up"}, results
    expected = Counter({("1", "o1"): 3, ("2", "o2"): 2, ("0", "o0"): 2, ("9", "o9"): 1})
    assert rows(env) == expected
    assert env.mirror() == {str(i): ("v2b" if i == 2 else f"v{i}") for i in range(1, 8)}


@LOCAL
def test_start_from_log_starts_at_s_and_captures_only_later_rows(stores):
    """M1: the start is the current durable boundary S, in any epoch: an insert before S is
    absent, one after it present. It needs the explicit acceptance."""
    env = append_env(stores, start=False)
    env.apply.start_from_log(TABLE)
    env.commit(ins("1", "before"))
    env.commit(ins("2", "before"))  # the unit closes: S is after these
    with pytest.raises(ApplyError, match="accept_incomplete"):
        env.apply.start_append(OTHER_TABLE)
    generation, epoch, start, kept = env.apply.start_append(OTHER_TABLE, accept_incomplete=True)
    assert start == env.log.checkpoint() and epoch == 1 and kept is None
    with pytest.raises(ApplyError, match="already has a live mirror generation"):
        env.apply.start_append(OTHER_TABLE, accept_incomplete=True)
    env.commit(ins("3", "after"))
    env.commit(ins("4", "after"))
    assert apply(env).status == "caught_up"
    assert rows(env) == Counter({("3", "after"): 1, ("4", "after"): 1})
    mode = env.store.con.execute(
        f"SELECT mode FROM {env.apply.mirror}._cdc.generations WHERE generation_id=?",
        [generation],
    ).fetchone()[0]
    assert mode == "log_incomplete"


@LOCAL
def test_the_start_from_log_action(stores):
    env = append_env(stores, start=False)
    env.commit(ins("1", "before"))
    env.commit(ins("2", "before"))
    would = ta.start_from_log(env.apply, OTHER_TABLE, run_id="d", actor="t", dry_run=True)
    assert would.status == "would" and "not copied" in would.detail
    keyed = ta.start_from_log(env.apply, TABLE, run_id="k", actor="t")
    assert keyed.status == "refused" and "backfill" in keyed.detail
    done = ta.start_from_log(env.apply, OTHER_TABLE, run_id="s", actor="t")
    assert done.status == "done" and done.observed["start_lsn"] == env.log.checkpoint()
    again = ta.start_from_log(env.apply, OTHER_TABLE, run_id="s2", actor="t")
    assert again.status == "already"


@LOCAL
def test_epoch_key_changes_and_a_missing_pin_on_an_append_table(stores):
    """S-1 and N-1: the shared loop holds an append table as a keyed one on a lost pin; a
    key_changes row for it (an epoch transition writes one per table, '[]') is never
    read as a key change."""
    env = append_env(stores)
    env.commit(ins("1", "a"))
    env.commit(ins("2", "b"))
    p = env.log.prefix
    env.store.con.execute(
        f"INSERT INTO {p}key_changes (relation_id,version,key_columns,cut_lsn,new_from_lsn,"
        "epoch) VALUES (?,1,'[]',0,0,1)",
        [OTHER],
    )
    env.store.con.execute(f"UPDATE {p}tables SET key_version=1 WHERE relation_id=?", [OTHER])
    assert apply(env).status == "caught_up"
    assert rows(env) == Counter({("1", "a"): 1, ("2", "b"): 1})
    env.store.con.execute(f"DELETE FROM {p}retention_pins WHERE relation_id=?", [OTHER])
    env.commit(ins("3", "c"))
    env.commit(ins("4", "d"))
    result = apply(env)
    # Both its pins gone: fail closed, exactly as for a keyed table (PinMissing).
    assert result.status == "needs_resync" and rows(env) == Counter({("1", "a"): 1, ("2", "b"): 1})


@LOCAL
def test_keyed_paths_refuse_an_append_table_and_resync_needs_the_snapshot_release(stores):
    """Every keyed path still refuses an append table. Its resync is an exact re-snapshot
    (keyless phase 2), refused until the backfill release stamps APPEND_SNAPSHOT."""
    from cdc_flight import action_flight as af
    from cdc_flight.releases import record_release

    env = append_env(stores)
    with pytest.raises(ApplyError, match="insert-only"):
        env.apply.registered(OTHER_TABLE)  # every keyed path's check
    inputs = af.Inputs("resync", OTHER_TABLE, {"expected_state": "live"}, True, None, None)
    _, _, refused = af.resync_check(env.apply, inputs)
    assert refused.status == "refused" and "predates append snapshots" in refused.detail
    record_release(env.apply.execute, "backfill", {"MOTHERDUCK_FLIGHT_ID": "f"})
    relation_id, observed, refused = af.resync_check(env.apply, inputs)
    assert refused is None and observed["ignore_deletes"] is False
    assert "kept aside" in af.append_resync_cost(OTHER_TABLE, observed)


@LOCAL
def test_keyed_frames_for_an_append_table_hold_needs_action_not_resync(stores):
    """An invariant, not a path. Frames with a key for an append table
    (ingest never writes them) hold needs_action with the way out: resync is refused."""
    env = append_env(stores)
    env.commit(ins("1", "a"))
    env.commit(ins("2", "b"))
    env.spool = Spool(
        env.log_config,
        {OID: ("id",), OTHER: ("id",), 900: ("id",)},
        900,
        "marker",
        binding=env.log_config.text_binding,
        start_lsn=env.log.checkpoint(),
        unit_config=env.log_config.unit_config,
        clock=lambda: 0,
    )
    env.feed([pgstream.relation(OTHER, "public", "others", "f", COLUMNS)])
    env.commit(ins("3", "c"))
    env.commit(ins("4", "d"))
    result = apply(env)
    assert result.status == "needs_action" and "remove the table" in result.detail
    assert rows(env) == Counter({("1", "a"): 1, ("2", "b"): 1})


@LOCAL
@pytest.mark.parametrize("fold", ["sql", "python"])
@pytest.mark.parametrize("first", ["update", "schema"])
def test_an_op_hold_and_a_schema_hold_in_one_batch_take_the_earlier(stores, fold, first):
    """One commit-order pass, so the earlier hold wins whichever kind it is,
    and the run goes on (no AssertionError), holding where quarantine can target it."""
    env = append_env(stores, fold=fold)
    env.commit(ins("1", "a"))
    env.commit(ins("2", "b"))
    dropped = [
        pgstream.relation(OTHER, "public", "others", "f", COLUMNS[:1]),
        pgstream.insert(OTHER, ("3",)),
    ]
    # After the drop, tuples are one column wide.
    if first == "update":
        env.commit(pgstream.update(OTHER, ("1", "z"), ("1", "a")))
        env.commit(*dropped)
    else:
        env.commit(*dropped)
        env.commit(pgstream.update(OTHER, ("1",), ("1",)))
    env.commit(pgstream.insert(OTHER, ("4",)))
    env.feed([], close=True)
    results = {r.table: r for r in env.apply.run()}
    held = results[OTHER_TABLE]
    assert held.status == "needs_action", held
    assert ("append_update" in held.detail) == (first == "update")
    assert rows(env) == Counter({("1", "a"): 1, ("2", "b"): 1})
    assert results[TABLE].status == "caught_up"  # the run went on


def held_on_update(stores, **kw):
    env = append_env(stores, **kw)
    env.commit(ins("1", "a"))
    env.commit(ins("2", "b"))
    env.commit(pgstream.update(OTHER, ("1", "z"), ("1", "a")))
    env.commit(ins("3", "c"))
    assert apply(env).status == "needs_action"
    return env


def mirror_tables(env):
    return sorted(
        r[0]
        for r in env.store.con.execute(
            "SELECT table_name FROM duckdb_tables() WHERE schema_name='public' "
            "AND table_name LIKE 'others%'"
        ).fetchall()
    )


@LOCAL
@pytest.mark.parametrize("case", ["held", "pins-released", "keyed-generation", "pin-lost"])
def test_start_from_log_replaces_a_generation_that_cannot_apply(stores, case):
    """Each dead end has a way out. The old mirror is renamed aside (rows kept),
    the new generation starts at S; a healthy generation is never replaced."""
    env = held_on_update(stores)
    old = env.store.con.execute(
        f"SELECT generation_id FROM {env.apply.mirror}._cdc.apply_state WHERE relation_id=?",
        [OTHER],
    ).fetchone()[0]
    m, p = f"{env.apply.mirror}._cdc", env.log.prefix
    if case == "pins-released":  # remove-table released them; add put the row back
        env.store.con.execute(
            f"UPDATE {m}.apply_state SET state='live',detail=NULL WHERE relation_id=?", [OTHER]
        )
        env.store.con.execute(f"DELETE FROM {p}retention_pins WHERE relation_id=?", [OTHER])
    elif case == "keyed-generation":  # added keyed once, removed, added in append mode
        env.store.con.execute(
            f"UPDATE {m}.apply_state SET capture_mode=NULL,key_columns='[\"id\"]',"
            "state='live',detail=NULL WHERE relation_id=?",
            [OTHER],
        )
    elif case == "pin-lost":
        env.store.con.execute(
            f"UPDATE {m}.apply_state SET state='needs_resync' WHERE relation_id=?", [OTHER]
        )
    would = ta.start_from_log(env.apply, OTHER_TABLE, run_id="d", actor="t", dry_run=True)
    assert would.status == "would" and "replaces the current generation" in would.detail
    done = ta.start_from_log(env.apply, OTHER_TABLE, run_id="s", actor="t")
    assert done.status == "done", done
    kept = done.observed["kept_mirror"]
    assert kept == f"public.others__gen_{old[:8]}" and "kept as" in done.detail
    assert mirror_tables(env) == [f"others__gen_{old[:8]}"]  # no new mirror until a write
    assert (
        env.store.con.execute(
            f"SELECT count(*) FROM {env.apply.mirror}.public.others__gen_{old[:8]}"
        ).fetchone()[0]
        == 2
    )
    env.commit(ins("5", "e"))
    env.commit(ins("6", "f"))
    assert apply(env).status == "caught_up"
    assert rows(env) == Counter({("5", "e"): 1, ("6", "f"): 1})
    pins = env.store.con.execute(
        f"SELECT DISTINCT generation_id FROM {p}retention_pins WHERE relation_id=?", [OTHER]
    ).fetchall()
    assert pins == [(done.observed["generation"],)]  # the old generation's pins released
    again = ta.start_from_log(env.apply, OTHER_TABLE, run_id="s2", actor="t")
    assert again.status == "already"  # healthy now: never replaced
    replaced = env.store.con.execute(
        f"SELECT state FROM {m}.generations WHERE generation_id=?", [old]
    ).fetchone()[0]
    assert replaced == "replaced"


@LOCAL
def test_a_paused_generation_is_resumed_not_replaced(stores):
    env = append_env(stores)
    paused = ta.pause(env.apply, OTHER_TABLE, run_id="p", actor="t")
    assert paused.status == "done", paused
    refused = ta.start_from_log(env.apply, OTHER_TABLE, run_id="s", actor="t")
    assert refused.status == "refused" and "resume it" in refused.detail
    with pytest.raises(ApplyError, match="resume it"):
        env.apply.start_from_log(OTHER_TABLE, accept_incomplete=True)


@LOCAL
def test_start_from_log_refuses_a_held_source_table_on_every_path(stores):
    env = append_env(stores, start=False)
    env.store.con.execute(
        f"UPDATE {env.log.prefix}tables SET held_reason='append_identity' WHERE relation_id=?",
        [OTHER],
    )
    with pytest.raises(ApplyError, match="held"):
        env.apply.start_from_log(OTHER_TABLE, accept_incomplete=True)
    assert ta.start_from_log(env.apply, OTHER_TABLE, run_id="s", actor="t").status == "refused"


@LOCAL
def test_quarantine_of_an_append_unit_says_its_inserts_are_a_gap(stores):
    """No "until a resync" and no resync step for an insert-only table."""
    env = held_on_update(stores)
    _, observed = ta.observe(env.apply, OTHER_TABLE)
    unit = (observed["unit_start_lsn"], observed["unit_end_lsn"])
    args = dict(
        run_id="q",
        actor="t",
        expected_epoch=observed["epoch"],
        confirm=ta.unit_confirmation(OTHER_TABLE, observed["epoch"], *unit),
        unit_start_lsn=unit[0],
        unit_end_lsn=unit[1],
    )
    would = ta.quarantine(env.apply, OTHER_TABLE, dry_run=True, **args)
    done = ta.quarantine(env.apply, OTHER_TABLE, **args)
    for result in (would, done):
        assert "are not in the mirror" in result.detail
        assert "until a resync (an exact re-snapshot)" in result.detail
        assert result.next_steps[0].startswith(f"resync {OTHER_TABLE}")


@LOCAL
def test_an_interrupted_pin_transfer_is_healthy_not_replaced(stores, monkeypatch):
    """The mirror commit lands, the pin transfer doesn't (best effort).
    start-from-log completes the transfer, as apply's ensure_pin would, and reports
    `already`: never renaming a healthy mirror aside and restarting later."""
    env = append_env(stores, start=False)
    monkeypatch.setattr(env.apply, "transfer_quietly", lambda *a, **k: None)
    generation = env.apply.start_append(OTHER_TABLE, accept_incomplete=True)[0]
    monkeypatch.undo()
    owner = f"table:{OTHER}"
    assert env.apply.retention.pin(owner) is None
    dry = ta.start_from_log(env.apply, OTHER_TABLE, run_id="d", actor="t", dry_run=True)
    assert dry.status == "already" and env.apply.retention.pin(owner) is None  # read only
    again = ta.start_from_log(env.apply, OTHER_TABLE, run_id="s", actor="t")
    assert again.status == "already", again
    assert env.apply.retention.pin(owner)["generation"] == generation
    assert mirror_tables(env) == []  # nothing renamed aside


@LOCAL
def test_a_catalog_hold_holds_an_append_table_needs_action_until_start_from_log(stores):
    """A partition attached or detached (catalog_checks) writes a
    key_changes row with a reason for an append table. Apply holds it needs_action at
    that cut, saying start-from-log; start-from-log takes the registry's key version and
    clears it. An epoch transition's row (no reason) never holds it (test above)."""
    env = append_env(stores)
    env.commit(ins("1", "a"))
    env.commit(ins("2", "b"))
    assert apply(env).status == "caught_up"
    p = env.log.prefix
    cut = env.log.checkpoint()
    env.store.con.execute(
        f"INSERT INTO {p}key_changes (relation_id,version,key_columns,cut_lsn,new_from_lsn,"
        "epoch,reason) VALUES (?,1,NULL,?,?,1,'partitions_changed')",
        [OTHER, cut, cut],
    )
    env.store.con.execute(f"UPDATE {p}tables SET key_version=1 WHERE relation_id=?", [OTHER])
    env.commit(ins("3", "c"))
    env.commit(ins("4", "d"))
    result = apply(env)
    assert result.status == "needs_action" and "start-from-log" in result.detail, result
    assert result.boundary == cut and rows(env) == Counter({("1", "a"): 1, ("2", "b"): 1})
    # The keyed table beside it is untouched by the append table's hold.
    assert env.apply.apply_table(TABLE, "keyed").status == "caught_up"
    env.apply.start_append(OTHER_TABLE, accept_incomplete=True)
    assert env.apply.state(env.apply.registered(OTHER_TABLE, append_ok=True)[0])["key_version"] == 1
    env.commit(ins("5", "e"))
    env.commit(ins("6", "f"))
    assert apply(env).status == "caught_up"


@LOCAL
def test_start_from_log_passes_only_the_catalog_holds_at_or_before_its_start(stores):
    """Start-from-log takes the highest reasoned version whose cut
    is at or before its start S, not the registry's version. A hold left by a remove and
    re-add (registry version back at 0) never holds the new generation; a hold whose cut
    lies beyond S still does."""
    env = append_env(stores)
    env.commit(ins("1", "a"))
    env.commit(ins("2", "b"))
    assert apply(env).status == "caught_up"
    p = env.log.prefix
    old = env.log.checkpoint()
    env.store.con.execute(
        f"INSERT INTO {p}key_changes (relation_id,version,key_columns,cut_lsn,new_from_lsn,"
        "epoch,reason) VALUES (?,3,NULL,?,?,1,'partitions_changed')",
        [OTHER, old, old],
    )
    # A remove and re-add reset the registry's version (1b's _activate_append) and leave
    # the reasoned row: the generation older than it holds; start-from-log clears it.
    env.store.con.execute(f"UPDATE {p}tables SET key_version=0 WHERE relation_id=?", [OTHER])
    assert apply(env).status == "needs_action"
    env.apply.start_append(OTHER_TABLE, accept_incomplete=True)
    for n in ("3", "4"):
        env.commit(ins(n, n))
        assert apply(env).status == "caught_up"  # the old row is behind S: two clean runs
    # A hold written after the start, its cut beyond it, holds at its cut.
    beyond = env.log.checkpoint()
    env.store.con.execute(
        f"INSERT INTO {p}key_changes (relation_id,version,key_columns,cut_lsn,new_from_lsn,"
        "epoch,reason) VALUES (?,4,NULL,?,?,1,'partitions_changed')",
        [OTHER, beyond, beyond],
    )
    env.commit(ins("5", "e"))
    env.commit(ins("6", "f"))
    result = apply(env)
    assert result.status == "needs_action" and result.boundary == beyond, result


@LOCAL
def test_a_done_start_from_log_refreshes_apply_status(stores, tmp_path):
    """The Dive shows the new generation right after start-from-log, not only after the
    next apply run: finish refreshes that table's apply_status row."""
    import uuid

    from cdc_flight import action_flight as af
    from cdc_flight.flightlog import Run

    env = append_env(stores, start=False)
    con = env.store.con
    con.execute(f"ATTACH IF NOT EXISTS '{tmp_path / 'cdc_ops.duckdb'}' AS cdc_ops")
    config = env.apply.config
    owner = af.Owner(
        config.source_id,
        "00000000-0000-4000-8000-000000000001",
        "00000000-0000-4000-8000-000000000002",
        database=config.database,
        mirror_database=config.mirror_database,
    )
    done = ta.start_from_log(env.apply, OTHER_TABLE, run_id="s", actor="t")
    assert done.status == "done"
    inputs = af.parse(
        {
            "CDC_ACTION": "start-from-log",
            "CDC_TABLE": OTHER_TABLE,
            "CDC_PREVIEW_ID": str(uuid.uuid4()),
        }
    )
    af.finish(
        con, owner, inputs, {"status": "done", "run_id": "s"}, Run(), audit=lambda *a, **k: False
    )
    generation, state, mode, run_id = con.execute(
        "SELECT generation_id,state,capture_mode,run_id FROM cdc_ops.main.apply_status "
        "WHERE table_name='others'"
    ).fetchone()
    current = env.apply.state(env.apply.registered(OTHER_TABLE, append_ok=True)[0])
    assert (generation, state, mode, run_id) == (current["generation"], "live", "append", "s")
