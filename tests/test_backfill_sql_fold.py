"""Backfill catch-up on the SQL fold against the Python fold (the backfill SQL fold design).

Each scenario runs once per fold, on its own local store, over the same synthetic log
(the real Spool and IngestLog2.append, test_apply2's Env): a generation in catching_up
with seeded staging rows, as a snapshot leaves it. The staging rows, the generation row
and any GenerationFailed reason must be identical, and fold_stats must show the path the
SQL run actually took, so a silent fallback can't pass as equal."""

from dataclasses import replace

import duckdb
import pgstream
import pytest
from test_apply2 import COLUMNS, OID, START, TABLE, ins
from test_backfill2 import BEnv
from test_sql_fold_apply import fresh  # noqa: F401 (fixture)

from cdc_flight import sql_path
from cdc_flight.apply import Apply, FenceLost
from cdc_flight.backfill import CATCHUP_OOM, Backfill, GenerationFailed
from cdc_flight.config import quoted
from cdc_flight.mirror import Column, duck_type
from cdc_flight.sql_path import SqlWrite
from cdc_flight.unit_store import UnitReader

GEN = "g1"
STAGING = "items__gen_g1"


def columns_of(spec):
    return [Column(name, duck_type(oid, typmod), oid, typmod) for _, name, oid, typmod in spec]


class CEnv(BEnv):
    """A generation in catching_up over the synthetic log, its staging seeded as the
    snapshot would leave it."""

    def __init__(self, stores, fold, *, spec=COLUMNS, keys=("id",), rows=(), **kwargs):
        super().__init__(stores, **kwargs)
        # The source's replica identity key: ingest's spool and key record follow it.
        self.spool.key_columns[OID] = tuple(keys)
        self.catalog_keys[OID] = list(keys)
        self.backfill = Backfill(
            self.store.con, self.bconfig, retention=self.retention(), fold=fold
        )
        self.backfill.initialize()
        b = self.backfill
        self.target = f"{b.mirror}.public.{quoted(STAGING)}"
        current = dict(target=self.target, keys=tuple(keys), schema="public")
        b._start(OID, GEN, 1, START, STAGING, tuple(keys), columns_of(spec), current, 0)
        for row in rows:
            names = ",".join(quoted(name) for _, name, _, _ in spec[: len(row)])
            self.store.con.execute(
                f"INSERT INTO {self.target} ({names},_cdc_commit_lsn,_cdc_updated_at) "
                f"VALUES ({','.join('?' * len(row))},{START},'2026-11-06 00:00:00+00')",
                list(row),
            )
        self.token = b.acquire_generation(GEN, "run")
        self.floats = [name for _, name, oid, _ in spec if oid in (700, 701)]

    def catch_up(self, high=None, **kwargs):
        """Catch up to `high` (default: the last complete unit's end). The reason of a
        GenerationFailed, else the return value."""
        self.feed([], close=True)  # close the open unit: catch-up reads complete ones
        high = high if high is not None else self.units()[-1][1]
        b = self.backfill
        b.execute(
            "UPDATE {m}generations SET state='catching_up',scan_done=true,marker_lsn=? "
            "WHERE generation_id=?",
            [high, GEN],
        )
        try:
            return b.catch_up(TABLE, b.generation(GEN), self.token, deadline=None, **kwargs)
        except GenerationFailed as exc:
            return "failed: " + exc.reason

    def outcome(self):
        gen = self.backfill.generation(GEN)
        # The data columns (compared as the source rows), then every staging column.
        rows = self.store.con.execute(
            f"SELECT COLUMNS(c -> NOT starts_with(c, '_cdc_')) FROM {self.target} ORDER BY ALL"
        ).fetchall()
        self.staging_rows = self.store.con.execute(
            f"SELECT * REPLACE (_cdc_updated_at::VARCHAR AS _cdc_updated_at) "
            f"FROM {self.target} ORDER BY ALL"
        ).fetchall()
        # Rows written with a -0 float, counted unsorted (ORDER BY drops the sign).
        signs = " OR ".join(f"signbit({quoted(c)})" for c in self.floats) or "false"
        negative_zero = self.store.con.execute(
            f"SELECT count(*) FROM {self.target} WHERE {signs}"
        ).fetchone()[0]
        return rows, gen["catchup_lsn"], gen["columns"], gen["state"], gen["detail"], negative_zero


def both(fresh, scenario, **kwargs):  # noqa: F811
    """{fold: (catch_up result, outcome, fold_stats)} for the scenario on each fold."""
    out = {}
    for fold in ("sql", "python"):
        env = CEnv(fresh(fold), fold, **kwargs)
        result = scenario(env)
        out[fold] = (result, env.outcome(), env.backfill.fold_stats, env.staging_rows)
    assert out["python"][2] == {}  # the forced Python fold counts nothing, as apply's
    assert out["sql"][:2] == out["python"][:2]
    assert out["sql"][3] == out["python"][3]  # _cdc_commit_lsn and _cdc_updated_at too
    return out["sql"][:3]


def upd(key, value, old=None):
    return pgstream.update(OID, (key, value), old=old)


def test_the_backfill_class_writes_through_the_sql_fold():
    """SqlWrite before Apply in the bases: a SqlImages batch never reaches Apply._write."""
    assert Backfill._write is SqlWrite._write
    assert Backfill.__mro__.index(SqlWrite) < Backfill.__mro__.index(Apply)


def test_catch_up_inserts_updates_and_deletes_across_pages(fresh):  # noqa: F811
    def scenario(env):
        env.commit(ins("4", "d"), upd("1", "z", old=("1", "a")))
        env.commit(pgstream.delete(OID, ("2", "b")))
        env.commit(upd("4", "e", old=("4", "d")), ins("5", "f"))
        env.commit(upd("3", "y", old=("3", "c")))
        return env.catch_up()

    result, (rows, *_), stats = both(
        fresh, scenario, rows=[("1", "a"), ("2", "b"), ("3", "c")], max_batch_events=2
    )
    assert result is True
    assert rows == [("1", "z"), ("3", "y"), ("4", "e"), ("5", "f")]
    assert stats == {"sql": 2}


def test_a_page_without_this_tables_events_advances_on_sql(fresh):  # noqa: F811
    other = 102

    def scenario(env):
        env.commit(pgstream.insert(other, ("1", "x")))
        env.commit(pgstream.insert(other, ("2", "y")))
        return env.catch_up()

    result, (rows, lsn, *_), stats = both(fresh, scenario, rows=[("1", "a")])
    assert result is True and rows == [("1", "a")] and lsn > START
    assert stats == {"sql": 1}


def test_a_crash_after_a_commit_resumes_at_the_checkpoint(fresh):  # noqa: F811
    def scenario(env):
        for k in range(4):
            env.commit(ins(str(k), f"v{k}"))

        def fault(step):
            if step == "after_commit":
                raise RuntimeError("crash")

        with pytest.raises(RuntimeError, match="crash"):
            env.catch_up(fault=fault)
        first = env.backfill.generation(GEN)["catchup_lsn"]
        assert START < first < env.units()[-1][1]
        return env.catch_up()

    result, (rows, *_), stats = both(fresh, scenario, max_batch_events=2)
    assert result is True and rows == [(str(k), f"v{k}") for k in range(4)]
    assert stats == {"sql": 2}


def test_truncate_in_catch_up_fails_on_the_python_fold(fresh):  # noqa: F811
    def scenario(env):
        env.commit(ins("4", "d"))
        env.commit(pgstream.truncate([OID]))
        return env.catch_up()

    result, _, stats = both(fresh, scenario, rows=[("1", "a")])
    assert result.startswith("failed: ") and "truncate" in result.lower(), result
    assert stats == {"fallback:truncate": 1}


def test_add_column_in_one_version_folds_on_sql(fresh):  # noqa: F811
    wider = COLUMNS + ((0, "w", 25, -1),)

    def scenario(env):
        env.commit(
            pgstream.relation(OID, "public", "items", "f", wider),
            pgstream.insert(OID, ("4", "d", "x")),
        )
        env.commit(pgstream.update(OID, ("1", "z", None), old=("1", "a", None)))
        return env.catch_up(high=env.units()[-1][1])

    result, (rows, _, columns, *_), stats = both(fresh, scenario, rows=[("1", "a")])
    assert result is True
    assert [c.name for c in columns] == ["id", "v", "w"]
    assert rows == [("1", "z", None), ("4", "d", "x")]
    assert stats == {"sql": 1}


def test_add_column_across_versions_in_a_page_falls_back(fresh):  # noqa: F811
    wider = COLUMNS + ((0, "w", 25, -1),)

    def scenario(env):
        env.commit(ins("4", "d"))
        env.commit(
            pgstream.relation(OID, "public", "items", "f", wider),
            pgstream.insert(OID, ("5", "e", "x")),
        )
        return env.catch_up()

    result, (rows, _, columns, *_), stats = both(fresh, scenario, rows=[("1", "a")])
    assert result is True and [c.name for c in columns] == ["id", "v", "w"]
    assert rows == [("1", "a", None), ("4", "d", None), ("5", "e", "x")]
    assert stats == {"fallback:layouts": 1}


def test_a_key_layout_change_fails_with_the_python_reason(fresh):  # noqa: F811
    def scenario(env):
        env.commit(ins("4", "d"))
        return env.catch_up()

    result, _, stats = both(fresh, scenario, keys=("v",))
    assert result.startswith("failed: needs_resync: source replica identity key changed at")
    assert stats == {"fallback:key_layout": 1}


def test_an_incomplete_update_of_a_row_absent_from_staging_is_redone(fresh):  # noqa: F811
    """The SQL MERGE's baseline check refuses; the Python redo fails the same way."""

    def scenario(env):
        env.commit(
            pgstream.relation(OID, "public", "items", "d", COLUMNS),
            pgstream.update(OID, ("9", pgstream.UNCHANGED)),
        )
        return env.catch_up()

    result, _, stats = both(fresh, scenario, rows=[("1", "a")])
    assert result.startswith("failed: needs_action: update of a row absent"), result
    # The SQL plan took it, the MERGE's baseline clause refused, Python redid it.
    assert stats == {"sql": 1, "redo:Blocked": 1}


CAST = ((1, "id", 25, -1), (0, "v", 23, -1))  # v int4


def test_a_cast_error_is_redone_on_python_with_the_same_reason(fresh):  # noqa: F811
    def scenario(env):
        env.commit(
            pgstream.relation(OID, "public", "items", "f", CAST),
            pgstream.insert(OID, ("4", "not a number")),
        )
        return env.catch_up()

    result, _, stats = both(fresh, scenario, spec=CAST)
    assert result.startswith("failed: ConversionException in catch-up commits"), result
    assert stats == {"sql": 1, "redo:ConversionException": 1}


def test_a_cast_error_on_an_oversized_page_fails_without_reading_frames(
    fresh,  # noqa: F811
    monkeypatch,
):
    env = CEnv(fresh("sql"), "sql", spec=CAST)
    env.commit(
        pgstream.relation(OID, "public", "items", "f", CAST),
        pgstream.insert(OID, ("4", "not a number")),
    )
    monkeypatch.setattr(sql_path, "CACHE_CAP_BYTES", 0)
    monkeypatch.setattr(UnitReader, "frames", no_frames)
    result = env.catch_up()
    unit = env.units()[-1]
    assert result == (
        f"failed: ConversionException in catch-up (SQL fold) units {START}..{unit[1]}"
    )
    assert env.backfill.generation(GEN)["catchup_lsn"] == START


def no_frames(*args, **kwargs):
    raise AssertionError("the SQL fold read frames client-side")


def test_a_unit_over_the_page_byte_limit_folds_on_sql_without_reading_frames(
    fresh,  # noqa: F811
    monkeypatch,
):
    env = CEnv(fresh("sql"), "sql", max_commits=4)
    env.backfill.config = replace(env.backfill.config, max_batch_bytes=1024)
    for k in range(4):
        env.commit(ins(str(k), "x" * 600))
    (unit,) = env.units()
    monkeypatch.setattr(UnitReader, "frames", no_frames)
    assert env.catch_up() is True
    assert env.backfill.fold_stats == {"sql": 1}
    assert env.outcome()[0] == [(str(k), "x" * 600) for k in range(4)]
    assert env.backfill.generation(GEN)["catchup_lsn"] == unit[1]


@pytest.mark.parametrize(
    "error",
    [
        FenceLost("generation progress changed underneath this run"),
        duckdb.TransactionException("Transaction conflict: write-write"),
        duckdb.IOException("connection lost"),
    ],
)
def test_fencing_conflicts_and_io_propagate_without_a_python_redo(
    fresh,  # noqa: F811
    monkeypatch,
    error,
):
    env = CEnv(fresh("sql"), "sql")
    env.commit(ins("4", "d"))

    def failing(statement):
        raise error

    monkeypatch.setattr(env.backfill, "_sql_merge", failing)
    monkeypatch.setattr(UnitReader, "frames", no_frames)  # no Python batch ran
    with pytest.raises(type(error)):
        env.catch_up()
    assert env.backfill.generation(GEN)["catchup_lsn"] == START


@pytest.mark.parametrize("fold", ["sql", "python"])
def test_an_expired_lease_nobody_took_commits_no_catch_up_batch(fresh, fold):  # noqa: F811
    """The catch-up commit's guard (lease().guard, _write's first statement)
    refuses an expired lease even when no other run took it; nothing is merged or
    checkpointed."""
    env = CEnv(fresh(fold), fold)
    env.commit(ins("4", "d"))
    env.backfill.execute(
        "UPDATE {m}generations SET expires_at=current_timestamp-INTERVAL '1 second' "
        "WHERE generation_id=?",
        [GEN],
    )
    with pytest.raises(FenceLost, match="lease expired"):
        env.catch_up()
    assert env.backfill.generation(GEN)["catchup_lsn"] == START
    assert env.store.con.execute(f"SELECT count(*) FROM {env.target}").fetchone()[0] == 0


def test_a_server_side_oom_fails_the_generation_with_the_next_step(
    fresh,  # noqa: F811
    monkeypatch,
):
    env = CEnv(fresh("sql"), "sql")
    env.commit(ins("4", "d"))

    def oom(statement):
        raise duckdb.OutOfMemoryException("out of memory")

    monkeypatch.setattr(env.backfill, "_sql_merge", oom)
    monkeypatch.setattr(UnitReader, "frames", no_frames)
    result = env.catch_up()
    assert result == "failed: " + CATCHUP_OOM.format(START, env.units()[-1][1])
    assert "larger MotherDuck instance" in CATCHUP_OOM


def test_fold_must_be_sql_or_python(fresh):  # noqa: F811
    store, _ = fresh("x")
    with pytest.raises(ValueError, match="fold must be"):
        Backfill(store.con, None, fold="other")


# Float keys fold by canonical value: catch-up gets it from the shared folds.
F8 = ((1, "f", 701, -1), (0, "a", 25, -1))
PAIR = ((1, "i", 20, -1), (1, "f", 701, -1), (0, "a", 25, -1))


def relation(spec):
    return pgstream.relation(OID, "public", "items", "f", spec)


@pytest.mark.parametrize(
    "spec, rows, changes, expected, path",
    [
        # Delete 0 and insert -0 in one transaction: one key, the insert wins.
        (
            F8,
            [("0", "x")],
            [pgstream.delete(OID, ("0", "x")), pgstream.insert(OID, ("-0", "y"))],
            [(0.0, "y")],
            {"sql": 1},
        ),
        # A -0 insert onto a staged 0 row updates it (never a second row).
        (F8, [("0", "x")], [pgstream.insert(OID, ("-0", "y"))], [(0.0, "y")], {"sql": 1}),
        # The key's text changes (0 -> -0), its value doesn't. The plan's key-change test
        # is text-based, so this page takes the Python fold by
        # design; that fold groups by canonical value.
        (
            F8,
            [("0", "x")],
            [pgstream.update(OID, ("-0", "y"), old=("0", "x"))],
            [(0.0, "y")],
            {"fallback:key_change": 1},
        ),
        (
            PAIR,
            [("1", "0", "x")],
            [pgstream.update(OID, ("1", "-0", "y"), old=("1", "0", "x"))],
            [(1, 0.0, "y")],
            {"fallback:key_change": 1},  # as above
        ),
    ],
    ids=["delete_0_insert_minus0", "insert_minus0_onto_0", "update_0_to_minus0", "composite"],
)
def test_float_keys_catch_up_by_canonical_value(fresh, spec, rows, changes, expected, path):  # noqa: F811
    keys = tuple(name for flag, name, _, _ in spec if flag)

    def scenario(env):
        env.commit(relation(spec), *changes)
        return env.catch_up()

    result, (got, *_, negative_zero), stats = both(fresh, scenario, spec=spec, keys=keys, rows=rows)
    assert result is True, result
    assert got == expected
    assert negative_zero == 0  # the staged key is written as +0 (== can't tell -0.0 apart)
    assert stats == path, stats
