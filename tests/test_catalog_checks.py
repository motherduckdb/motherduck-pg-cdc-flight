"""Per-run catalog checks: catalog_checks.judge's decision table, and
format 2's log judging the end read in finish: adopted once, written only on change, a
hard signal queued as a key change at the last verified cut with its reason."""

import json

import pytest
from test_ingest_key_statements import counted, tables
from test_ingest_log2 import build, log2  # noqa: F401

from cdc_flight import catalog_checks


def facts(**changes):
    base = dict(
        key=["id"],
        from_pk=True,
        relkind="r",
        replident="f",
        relfilenode=1000,
        pk_oid=500,
        leaves=None,
        columns=["id", "v"],
        missing=[],
    )
    base.update(changes)
    return base


def judged(before, after, **options):
    _, stored, _ = catalog_checks.judge(None, before)
    return catalog_checks.judge(stored, after, **options)


@pytest.mark.parametrize(
    "change,append,reason,warning",
    [
        ({}, False, None, False),  # equal: nothing to write
        (dict(pk_oid=501), False, "pk_replaced", False),
        (dict(pk_oid=501, from_pk=False), False, None, False),  # keyed by another index
        (dict(pk_oid=501), True, None, False),  # append: no key
        (dict(leaves=[7]), False, "partitions_changed", False),
        (dict(leaves=[7]), True, "partitions_changed", False),
        (dict(columns=["id", "v", "x"], missing=["x"]), False, "column_default_fill", False),
        (dict(columns=["id", "v", "x"], missing=["x"]), True, None, True),
        (dict(columns=["id", "v", "x"], relfilenode=1001), False, "column_rewrite", False),
        (dict(columns=["id", "v", "x"], relfilenode=1001), True, None, False),
        (dict(columns=["id", "v", "x"]), False, None, False),  # no default: safe
        (dict(relfilenode=1001), False, None, True),  # VACUUM FULL, CLUSTER, TRUNCATE
        (dict(relfilenode=1001), True, None, False),  # never for append (S-2)
        (dict(columns=["id"]), False, None, False),  # a dropped column: reconcile's
    ],
)
def test_the_decision_table(change, append, reason, warning):
    before = facts(leaves=[6] if "leaves" in change else None)
    if "leaves" in change:
        change = dict(change, leaves=[6, 7])
    got_reason, state, got_warning = judged(before, facts(**{**before, **change}), append=append)
    assert (got_reason, got_warning) == (reason, warning)
    if not change:
        assert state is None  # the steady state writes nothing
    else:
        assert json.loads(state).get("reason") == reason


def test_a_partitioned_table_is_never_judged_by_its_own_file():
    root = facts(relkind="p", relfilenode=0, leaves=[6])
    assert judged(root, facts(**{**root, "columns": ["id", "v", "x"]}))[0] is None


def test_adoption_never_holds():
    reason, state, warning = catalog_checks.judge(None, facts(pk_oid=9, leaves=[1]))
    assert reason is None and not warning and json.loads(state)["leaves"] == [1]


def finish(log, observed, facts_, run="finish"):
    return log.finish(run, observed, 102, 120, "drained", None, lambda status: status, facts=facts_)


def test_finish_adopts_once_then_writes_nothing_in_the_steady_state(log2):  # noqa: F811
    log2.append(build(log2), partition_day="20261106")
    observed = tables(log2, 3)
    every = {oid: facts(pk_oid=oid) for oid in observed}
    counting = counted(log2)
    log2.record_keys(observed, 102, 110)
    counting.take()
    assert finish(log2, observed, every) == "drained"  # the fixture holds the lease
    first = counting.take()
    log2.acquire("r2")
    assert finish(log2, observed, every, "again") == "drained"
    steady = counting.take()
    log2.acquire("r3")
    assert finish(log2, observed, None, "unjudged") == "drained"
    unjudged = counting.take()
    # Adoption: one batched UPDATE; afterwards the same statements as with no checks.
    assert first.count("update:tables") == steady.count("update:tables") + 1
    assert steady == unjudged
    states = log2.execute("SELECT count(catalog_state) FROM {s}tables").fetchone()[0]
    assert states == 3


def test_a_detach_holds_at_the_cut_and_a_fresh_start_does_not_hold_again(log2):  # noqa: F811
    log2.append(build(log2), partition_day="20261106")
    observed = tables(log2, 2)
    oid = min(observed)
    before = {o: facts(relkind="p", relfilenode=0, leaves=[1, 2]) for o in observed}
    finish(log2, observed, before)  # the fixture holds the lease
    after = {**before, oid: facts(relkind="p", relfilenode=0, leaves=[1])}
    log2.acquire("r2")
    finish(log2, observed, after, "detached")
    changes = log2.execute(
        "SELECT relation_id,version,key_columns,reason FROM {s}key_changes ORDER BY ALL"
    ).fetchall()
    assert changes == [(oid, 1, '["id"]', "partitions_changed")]
    state, row_state, stored = log2.execute(
        "SELECT state,key_version,catalog_state FROM {s}tables WHERE relation_id=?", [oid]
    ).fetchone()
    assert (state, row_state) == ("needs_resync", 1)
    assert json.loads(stored) == catalog_checks.state_of(after[oid])  # reason cleared
    # The next runs (after the operator's resync) judge against the detached state: no
    # new hold, and a further change is still caught.
    for run in ("next1", "next2"):
        log2.acquire(run)
        finish(log2, observed, after, run)
    assert log2.execute("SELECT count(*) FROM {s}key_changes").fetchone()[0] == 1


def test_a_key_change_since_the_stored_facts_is_never_judged_twice():
    """A table held without a key (no_key) isn't read; after the DBA restores the key,
    the start read's key change covers it. The end read then sees a new PK constraint
    but a newer key version: it refreshes the facts and holds nothing more."""
    _, stored, _ = catalog_checks.judge(None, facts(), key_version=0)
    reason, state, _ = catalog_checks.judge(stored, facts(pk_oid=777), key_version=1)
    assert reason is None and json.loads(state)["key_version"] == 1
    # The same constraint change at the same key version is the A->B->A case.
    assert catalog_checks.judge(stored, facts(pk_oid=777), key_version=0)[0] == "pk_replaced"


def test_an_append_tables_partition_change_writes_its_hold_at_the_boundary(log2):  # noqa: F811
    """An append table has no key gate: ingest writes its catalog hold at once, at this
    run's durable boundary, with a reason; a new column's fill is a warning only."""
    log2.append(build(log2), partition_day="20261106")
    observed = tables(log2, 2)
    oid = max(observed)
    log2.execute(
        "UPDATE {s}tables SET capture_mode='append',key_columns='[]',catalog_key=NULL "
        "WHERE relation_id=?",
        [oid],
    )
    keyed = {o: k for o, k in observed.items() if o != oid}
    root = facts(key=None, from_pk=False, relkind="p", relfilenode=0, leaves="2:aa")
    every = {min(observed): facts(), oid: root}
    finish(log2, keyed, every)
    filled = {**every, oid: {**root, "columns": ["id", "v", "x"], "missing": ["x"]}}
    log2.acquire("r2")
    finish(log2, keyed, filled, "filled")
    assert log2.catalog_warnings == 1
    assert log2.execute("SELECT count(*) FROM {s}key_changes").fetchone()[0] == 0
    moved = {**filled, oid: {**filled[oid], "leaves": "1:bb"}}
    log2.acquire("r3")
    finish(log2, keyed, moved, "moved")
    assert log2.execute(
        "SELECT relation_id,version,key_columns,cut_lsn,reason FROM {s}key_changes"
    ).fetchall() == [(oid, 1, None, 102, "partitions_changed")]
    assert log2.execute(
        "SELECT key_version FROM {s}tables WHERE relation_id=?", [oid]
    ).fetchone() == (1,)
    log2.acquire("r4")
    finish(log2, keyed, moved, "after")  # the same facts again: no second hold
    assert log2.execute("SELECT count(*) FROM {s}key_changes").fetchone()[0] == 1


def test_after_a_key_change_every_signal_but_the_pk_is_still_judged():
    """The first read after a key change is judged, except for the PK
    constraint (the key change's own DDL)."""
    _, stored, _ = catalog_checks.judge(None, facts(relkind="p", leaves=[1, 2]), key_version=0)
    moved = facts(relkind="p", leaves=[1], pk_oid=777)
    assert catalog_checks.judge(stored, moved, key_version=1)[0] == "partitions_changed"
    rekeyed = facts(relkind="p", leaves=[1, 2], key=["id", "v"], pk_oid=777)
    assert catalog_checks.judge(stored, rekeyed, key_version=0)[0] is None


@pytest.mark.parametrize("first", ["catalog_hold", "key_change"])
def test_a_detach_right_after_an_activation_is_a_second_hold(log2, first):  # noqa: F811
    log2.append(build(log2), partition_day="20261106")
    observed = tables(log2, 2)
    oid = min(observed)
    base = {o: facts(relkind="p", relfilenode=0, leaves=[1, 2, 3]) for o in observed}
    finish(log2, observed, base)  # adopt
    if first == "catalog_hold":
        nxt, keys = {**base, oid: facts(relkind="p", relfilenode=0, leaves=[1, 2])}, observed
    else:
        rekey = facts(relkind="p", relfilenode=0, leaves=[1, 2, 3], key=["id", "v"], pk_oid=9)
        nxt, keys = {**base, oid: rekey}, {**observed, oid: ["id", "v"]}
    log2.acquire("r2")
    finish(log2, keys, nxt, "activates")
    assert log2.execute("SELECT count(*) FROM {s}key_changes").fetchone()[0] == 1
    detached = {**nxt, oid: {**nxt[oid], "leaves": [1]}}
    log2.acquire("r3")
    finish(log2, keys, detached, "detached")
    reasons = log2.execute(
        "SELECT version,reason FROM {s}key_changes WHERE relation_id=? ORDER BY 1", [oid]
    ).fetchall()
    assert reasons[-1] == (2, "partitions_changed"), reasons
