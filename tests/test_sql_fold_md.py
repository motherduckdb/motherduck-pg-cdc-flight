"""The SQL fold on MotherDuck: where it runs, its precheck error, and the same mirror as
the Python fold. Owned cdc_p0_* stores only (the `stores` fixture); skipped without
CDC_TEST_MD=1.

Placement gate (EXPLAIN, test_phase1's parsers): for the plan statement and the MERGE,
- no UPLOAD_SINK (nothing the client holds is sent up: the frames never come down);
- every scan, filter, join and aggregate is remote; the client only receives the result
  (DOWNLOAD_SOURCE, fed by a remote download sink).
"""

import pgstream
import pytest
from test_apply2 import OID, OTHER, OTHER_TABLE, TABLE, Env, ins
from test_phase1 import DOWNLOAD_SINKS, explained

from cdc_flight import sql_fold
from cdc_flight.sql_path import SqlBatch, SqlImages

U = pgstream.UNCHANGED
# The client may receive and reshape a one-row or count result, nothing else.
CLIENT_OK = {"DOWNLOAD_SOURCE", "PROJECTION", "RESULT_COLLECTOR", "EXTENSION", "QUERY"}


def expand(apply, sql):
    for key, value in apply.prefixes.items():
        sql = sql.replace(key, value)
    return sql


def gate(nodes):
    """The placement gate on one statement's operators; returns the evidence."""
    assert not [n for n in nodes if n["label"] and (n["label"] == "R") != n["remote"]], nodes
    assert not [n for n in nodes if n["kind"] == "UPLOAD_SINK"], nodes
    downloads = sum(n["kind"] in DOWNLOAD_SINKS for n in nodes)
    assert downloads >= 1, "no download sink: the statement didn't run remotely"
    scans = [n for n in nodes if n["table"]]
    assert scans and all(n["remote"] for n in scans), scans
    local = sorted({n["kind"] for n in nodes if not n["remote"]})
    assert set(local) <= CLIENT_OK, local
    return dict(downloads=downloads, scans=len(scans), local=local)


def page_batch(env):
    current = env.apply.head(TABLE)["current"]
    high = env.apply.reader.head().high
    batch = env.apply._read_batch(OID, current, high, set())
    assert isinstance(batch, SqlBatch) and batch.events, batch
    return current, batch


def placement(env):
    """`items`: the plan statement and the MERGE pass the placement gate, and the run
    applies the page on the SQL path."""
    env.apply.start_from_log(TABLE)
    env.commit(ins("1", "a"))
    assert env.apply.apply_table(TABLE, "create").status == "caught_up"  # the mirror exists
    for k in range(2, 6):
        env.commit(ins(str(k), f"v{k}"))
    current, batch = page_batch(env)
    reader = env.apply.reader
    descriptors = reader.descriptors(batch.page, high=batch.high)
    versions = {
        d.version: (len(d.names), tuple(d.names.index(k) + 1 for k in d.keys))
        for (oid, _, _), d in descriptors.items()
        if oid == OID
    }
    plan = expand(env.apply, sql_fold.plan_sql(batch.frames, OID, versions))
    images = SqlImages(batch, OID)
    merge = expand(
        env.apply,
        sql_fold.merge_sql(current["target"], current["keys"], batch.columns, images.source()),
    )
    for name, sql in (("plan", plan), ("merge", merge)):
        nodes, gated_on = explained(env.store.con, sql, [], True, f"sql fold {name}")
        print(f"sql fold {name} placement ({gated_on}):", gate(nodes))
    env.apply.fold_stats.clear()
    assert env.apply.apply_table(TABLE, "run").status == "caught_up"
    assert env.apply.fold_stats == {"sql": 1}
    assert env.mirror() == {"1": "a", **{str(k): f"v{k}" for k in range(2, 6)}}


def precheck(env):
    """An incomplete update of a key the mirror lacks: MotherDuck raises the MERGE's ERROR
    clause as ConstraintException with the marker, and apply holds `items` needs_action
    with the Python fold's detail (no poison bisect). Runs last: it leaves `items` held."""
    import duckdb

    from cdc_flight.mirror import Column

    con, mirror = env.store.con, env.apply.mirror
    con.execute(
        f"CREATE TABLE {mirror}.main.precheck_probe(id BIGINT, v VARCHAR, "
        "_cdc_commit_lsn UBIGINT, _cdc_updated_at TIMESTAMPTZ)"
    )
    columns = [Column("id", "BIGINT", 20, -1), Column("v", "VARCHAR", 25, -1)]
    source = (
        "(SELECT 2::BIGINT AS c0, NULL::VARCHAR AS c1, 'upsert' AS _s_op, [0] AS _s_known, "
        "FALSE AS _s_complete, 5::UBIGINT AS _s_lsn, TIMESTAMPTZ 'epoch' AS _s_ts)"
    )
    with pytest.raises(duckdb.Error) as raised:
        con.execute(sql_fold.merge_sql(f"{mirror}.main.precheck_probe", ("id",), columns, source))
    print("MotherDuck precheck error:", type(raised.value).__name__, str(raised.value)[:200])
    assert type(raised.value) is duckdb.ConstraintException
    assert sql_fold.BASELINE_ERROR in str(raised.value)

    env.commit(pgstream.update(OID, ("9", U)))  # key 9 is absent, v unchanged
    env.apply.fold_stats.clear()
    result = env.apply.apply_table(TABLE, "held")
    assert result.status == "needs_action", result
    assert env.state()["detail"] == "update of a row absent from the mirror lacks a full image"
    assert env.apply.fold_stats == {"sql": 1}


def others_script(env):
    """`others`: inserts, a NULL, update-after-delete, a delete, partial carries."""
    env.apply.start_from_log(OTHER_TABLE)
    env.commit(oth(("1", "a")), oth(("2", "b")), oth(("3", "c")))
    first = env.apply.apply_table(OTHER_TABLE, "first")
    env.commit(pgstream.update(OTHER, ("1", None), ("1", "a")), pgstream.delete(OTHER, ("2", "b")))
    env.commit(pgstream.delete(OTHER, ("3", "c")), oth(("3", "c2")))
    env.commit(pgstream.update(OTHER, ("3", "c3"), ("3", "c2")), oth(("4", "d")))
    second = env.apply.apply_table(OTHER_TABLE, "second")
    return [(r.status, r.boundary, r.events, r.transactions) for r in (first, second)]


def mirror_rows(env):
    return env.store.con.execute(
        "SELECT id,v,_cdc_commit_lsn,epoch_us(_cdc_updated_at) FROM "
        f"{env.apply.mirror}.public.others ORDER BY id"
    ).fetchall()


def oth(cells):
    return pgstream.insert(OTHER, cells)


@pytest.mark.parametrize("stores", ["motherduck"], indirect=True)
def test_the_sql_fold_on_motherduck(stores, tmp_path):
    """One remote node (the MotherDuck budget), one Env:
    1. `others` on MotherDuck (SQL fold) equals a local twin applied by the Python fold;
    2. placement of the plan statement and the MERGE (`items`);
    3. the precheck error's class and marker, and the hold it maps to (`items`, last)."""
    import duckdb

    from cdc_flight.store import Store, ident

    env = Env(stores, max_commits=1)
    got = others_script(env)
    con = duckdb.connect(str(tmp_path / "twin.duckdb"))
    con.execute(f"ATTACH '{tmp_path / 'twin_m.duckdb'}' AS {ident('twin_m')}")
    con.execute("SET TimeZone='UTC'")
    twin = Store(con, "twin", "twin_m")
    twin.initialize()
    try:
        local = Env((twin, twin), max_commits=1, fold="python")
        assert got == others_script(local)
        assert mirror_rows(env) == mirror_rows(local)
    finally:
        con.close()
    assert set(env.apply.fold_stats) == {"sql"} and env.apply.fold_stats["sql"] >= 2
    placement(env)
    precheck(env)
