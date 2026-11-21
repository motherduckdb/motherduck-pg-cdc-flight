"""Regression proof of the generation publication primitive (Phase 2 backfill).

A generation is built in a staging table and published by DROP of the current
mirror table plus RENAME of the staging table, in one transaction with a
compare-and-set on generation state. CTAS would drop constraints; this keeps the
PRIMARY KEY. Runs on local DuckDB and real MotherDuck.
"""

import duckdb
import pytest


@pytest.fixture
def mirror(stores):
    s, observer = stores
    m = s.mirror_database
    s.con.execute(f"CREATE SCHEMA {m}.p")
    s.con.execute(f"CREATE TABLE {m}.p.t (id BIGINT PRIMARY KEY, v VARCHAR)")
    s.con.execute(f"INSERT INTO {m}.p.t VALUES (1,'old'),(2,'old')")
    s.con.execute(f"CREATE TABLE {m}.p.t__gen_a (id BIGINT PRIMARY KEY, v VARCHAR)")
    s.con.execute(f"INSERT INTO {m}.p.t__gen_a VALUES (1,'new'),(3,'new')")
    s.con.execute(f"CREATE TABLE {m}.p.state (id INTEGER PRIMARY KEY, generation VARCHAR)")
    s.con.execute(f"INSERT INTO {m}.p.state VALUES (1,'old_gen')")
    return s, observer, m


def rows(con, m, table="t"):
    return con.execute(f"SELECT id,v FROM {m}.p.{table} ORDER BY id").fetchall()


def generation(con, m):
    return con.execute(f"SELECT generation FROM {m}.p.state").fetchone()[0]


def tables(con, m):
    return sorted(
        name
        for (name,) in con.execute(
            "SELECT table_name FROM duckdb_tables() WHERE database_name=? AND schema_name='p'",
            [m.strip('"')],
        ).fetchall()
    )


def publish(con, m, expected, *, fail_after_rename=False):
    con.execute("BEGIN")
    try:
        count = con.execute(
            f"UPDATE {m}.p.state SET generation='a' WHERE id=1 AND generation=?", [expected]
        ).fetchone()[0]
        if count != 1:
            raise RuntimeError("generation changed")
        con.execute(f"DROP TABLE {m}.p.t")
        con.execute(f"ALTER TABLE {m}.p.t__gen_a RENAME TO t")
        if fail_after_rename:
            raise RuntimeError("injected")
        con.execute("COMMIT")
    except BaseException:
        con.execute("ROLLBACK")
        raise


def test_publish_keeps_primary_key_and_is_atomic(mirror):
    s, observer, m = mirror
    s.con.execute("BEGIN")
    assert (
        s.con.execute(
            f"UPDATE {m}.p.state SET generation='a' WHERE id=1 AND generation='old_gen'"
        ).fetchone()[0]
        == 1
    )
    s.con.execute(f"DROP TABLE {m}.p.t")
    s.con.execute(f"ALTER TABLE {m}.p.t__gen_a RENAME TO t")
    # Before commit another connection still sees the old generation in full.
    assert rows(observer.con, m) == [(1, "old"), (2, "old")]
    assert generation(observer.con, m) == "old_gen"
    s.con.execute("COMMIT")
    assert rows(observer.con, m) == [(1, "new"), (3, "new")]
    assert generation(observer.con, m) == "a"
    assert tables(observer.con, m) == ["state", "t"]
    constraints = observer.con.execute(
        "SELECT constraint_type FROM duckdb_constraints() WHERE database_name=? "
        "AND schema_name='p' AND table_name='t' ORDER BY 1",
        [m.strip('"')],
    ).fetchall()
    assert ("PRIMARY KEY",) in constraints
    with pytest.raises(duckdb.ConstraintException):
        observer.con.execute(f"INSERT INTO {m}.p.t VALUES (1,'duplicate')")
    # A delayed writer of the replaced generation fails its compare-and-set.
    assert (
        observer.con.execute(
            f"UPDATE {m}.p.state SET generation='stale' WHERE id=1 AND generation='old_gen'"
        ).fetchone()[0]
        == 0
    )
    # MERGE keyed on the published table still works (apply continues from here).
    observer.con.execute(
        f"MERGE INTO {m}.p.t m USING (SELECT 3 AS id,'merged' AS v) s ON m.id=s.id "
        "WHEN MATCHED THEN UPDATE SET v=s.v WHEN NOT MATCHED THEN INSERT VALUES (s.id,s.v)"
    )
    assert rows(observer.con, m) == [(1, "new"), (3, "merged")]


def test_publish_rollback_and_stale_compare_and_set_change_nothing(mirror):
    s, observer, m = mirror
    with pytest.raises(RuntimeError, match="injected"):
        publish(s.con, m, "old_gen", fail_after_rename=True)
    with pytest.raises(RuntimeError, match="generation changed"):
        publish(s.con, m, "someone_else")
    for con in (s.con, observer.con):
        assert rows(con, m) == [(1, "old"), (2, "old")]
        assert rows(con, m, "t__gen_a") == [(1, "new"), (3, "new")]
        assert generation(con, m) == "old_gen"
        assert tables(con, m) == ["state", "t", "t__gen_a"]
    with pytest.raises(duckdb.ConstraintException):
        s.con.execute(f"INSERT INTO {m}.p.t VALUES (2,'duplicate')")
    # The same publication then succeeds.
    publish(s.con, m, "old_gen")
    assert rows(observer.con, m) == [(1, "new"), (3, "new")]


def test_publish_conflicts_with_concurrent_guard_write(mirror):
    """An apply run guarding the same state row cannot interleave with publication."""
    s, observer, m = mirror
    observer.con.execute("BEGIN")
    # Fences must change a value (dev-docs/invariants.md, verified 2026-11-05).
    observer.con.execute(f"UPDATE {m}.p.state SET generation='in_flight' WHERE id=1")
    assert generation(observer.con, m) == "in_flight"
    with pytest.raises(duckdb.TransactionException):
        publish(s.con, m, "old_gen")
    observer.con.execute("COMMIT")
    assert generation(s.con, m) == "in_flight"
    assert rows(s.con, m) == [(1, "old"), (2, "old")]
    assert tables(s.con, m) == ["state", "t", "t__gen_a"]


def test_publish_dependency_limits(mirror):
    """Staging tables must not carry secondary indexes: DuckDB refuses to rename a
    table that other catalog entries depend on. User views over the mirror bind by
    name at query time, so they follow the published table."""
    s, observer, m = mirror
    s.con.execute(f"CREATE VIEW {m}.p.recent AS SELECT * FROM {m}.p.t WHERE id < 3")
    s.con.execute(f"CREATE INDEX t_gen_v ON {m}.p.t__gen_a (v)")
    with pytest.raises(duckdb.Error):
        publish(s.con, m, "old_gen")
    assert rows(observer.con, m) == [(1, "old"), (2, "old")]
    assert generation(observer.con, m) == "old_gen"
    s.con.execute(f"DROP INDEX {m}.p.t_gen_v")
    publish(s.con, m, "old_gen")
    assert observer.con.execute(f"SELECT id,v FROM {m}.p.recent ORDER BY id").fetchall() == [
        (1, "new")
    ]
