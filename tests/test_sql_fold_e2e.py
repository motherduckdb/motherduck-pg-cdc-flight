"""The SQL fold end to end, against the Python fold (ApplyRun's two paths).

The same pgoutput wire goes through the real Spool and IngestLog2.append into two fresh
local stores. One is applied by ApplyRun(fold="python") (the oracle), the other by
ApplyRun() (the SQL fold), at the same apply points. These must be identical:
- every mirror row and column, including _cdc_commit_lsn and _cdc_updated_at;
- the apply_state row (applied, state, blocked commit and unit, detail) and the pin;
- each run's result (status, boundary, events, transactions, units).
"""

import json
import os
import random

import duckdb
import pgstream
import pytest
from test_format2_protocol import binding
from test_format2_protocol import config as f2_config

from cdc_flight import sql_fold
from cdc_flight.apply_run import ApplyRun
from cdc_flight.config import ApplyConfig
from cdc_flight.format2 import UnitConfig
from cdc_flight.ingest_log2 import IngestConfig2, IngestLog2
from cdc_flight.retention2 import Retention2
from cdc_flight.spool2 import Spool
from cdc_flight.store import Store, ident

OID, OTHER, HEARTBEAT = 101, 102, 900
TABLE, OTHER_TABLE = "public.items", "public.others"
START = 90
HB_COLUMNS = ((1, "id", 25, -1), (0, "marker", 25, -1))
NUMERIC_5_2 = ((5 << 16) | 2) + 4
U = pgstream.UNCHANGED


class Side:
    def __init__(self, tmp, tag, fold, layout, *, max_commits, apply_options):
        name = f"cdc_p0_sf_{tag}"
        mirror = name + "_mirror"
        con = duckdb.connect(str(tmp / f"{name}.duckdb"))
        con.execute(f"ATTACH '{tmp / (mirror + '.duckdb')}' AS {ident(mirror)}")
        con.execute("SET TimeZone='UTC'")
        self.con = con
        store = Store(con, name, mirror)
        store.initialize()
        base = f2_config().__class__(**{**f2_config().__dict__, "database": name})
        self.log_config = IngestConfig2(
            **base.__dict__,
            text_binding=binding(),
            unit_config=UnitConfig(max_commits=max_commits, chunk_frames=3),
        )
        self.log = IngestLog2(con, self.log_config)
        self.log.initialize()
        keys = list(layout["keys"])
        self.log.register(
            ("system", 1),
            START,
            [
                dict(oid=OID, schema="public", name="items", keys=keys, empty=True),
                dict(oid=OTHER, schema="public", name="others", keys=["id"], empty=True),
                dict(oid=HEARTBEAT, schema="public", name="md_cdc_heartbeat", keys=["id"]),
            ],
        )
        self.catalog_keys = {OID: keys, OTHER: ["id"], HEARTBEAT: ["id"]}
        self.spool = Spool(
            self.log_config,
            {OID: tuple(keys), OTHER: ("id",), HEARTBEAT: ("id",)},
            900,
            "marker",
            binding=self.log_config.text_binding,
            start_lsn=START,
            unit_config=self.log_config.unit_config,
            clock=lambda: 0,
        )
        self.config = ApplyConfig(
            "f2", (TABLE, OTHER_TABLE), database=name, mirror_database=mirror, **apply_options
        )
        c = self.config
        self.apply = ApplyRun(
            con, c, retention=Retention2(con, c.database, c.source_id, c.lease_seconds), fold=fold
        )
        self.apply.initialize()
        self.feed(
            [
                pgstream.relation(OID, "public", "items", layout["identity"], layout["columns"]),
                pgstream.relation(
                    OTHER, "public", "others", "f", ((1, "id", 25, -1), (0, "v", 25, -1))
                ),
                pgstream.relation(HEARTBEAT, "public", "md_cdc_heartbeat", "d", HB_COLUMNS),
            ]
        )
        self.apply.start_from_log(TABLE)
        self.apply.start_from_log(OTHER_TABLE)

    def feed(self, messages):
        items = [i for wire in messages for i in self.spool.feed(wire)]
        if items:
            self.log.acquire("writer")
            try:
                self.log.append(items, partition_day="20261106")
                self.log.record_keys(
                    dict(self.catalog_keys), self.log.checkpoint(), self.log.checkpoint()
                )
            finally:
                self.log.release()

    def snapshot(self, results):
        m = self.apply.mirror
        out = {"results": results}
        for name in ("items", "others"):
            try:
                out[name] = self.con.execute(
                    f"SELECT * REPLACE (epoch_us(_cdc_updated_at) AS _cdc_updated_at) "
                    f"FROM {m}.public.{name} ORDER BY ALL"
                ).fetchall()
            except duckdb.CatalogException:
                out[name] = None
                continue
            # ORDER BY drops a double's sign (DuckDB's sort keys normalize -0), so the rows
            # above can't show it: count each DOUBLE column's -0 values unsorted.
            doubles = [
                c
                for (c,) in self.con.execute(
                    "SELECT column_name FROM duckdb_columns() WHERE database_name=? AND "
                    "schema_name='public' AND table_name=? AND data_type='DOUBLE' ORDER BY 1",
                    [self.config.mirror_database, name],
                ).fetchall()
            ]
            out[name + "_negative_zero"] = {
                c: self.con.execute(
                    f'SELECT count(*) FILTER (WHERE "{c}"=0 AND signbit("{c}")) '
                    f"FROM {m}.public.{name}"
                ).fetchone()[0]
                for c in doubles
            }
        out["state"] = self.con.execute(
            f"SELECT relation_id,applied_through_lsn,state,blocked_commit_lsn,"
            f"blocked_unit_start_lsn,detail,columns,key_columns FROM {m}._cdc.apply_state ORDER BY 1"
        ).fetchall()
        out["pins"] = self.con.execute(
            f"SELECT owner,pinned_lsn FROM {self.log.prefix}retention_pins ORDER BY 1"
        ).fetchall()
        return out


def run_pair(tmp_path, layout, script, *, max_commits=2, **apply_options):
    """script: list of ("commit", [messages]) / ("apply",) steps. Returns both snapshots
    and the SQL side's fold_stats."""
    tmp_path.mkdir(parents=True, exist_ok=True)
    sides = [
        Side(tmp_path, "o", "python", layout, max_commits=max_commits, apply_options=apply_options),
        Side(tmp_path, "s", "sql", layout, max_commits=max_commits, apply_options=apply_options),
    ]
    results = [[], []]
    lsn = 100
    try:
        for step in script + [("apply",)]:
            if step[0] == "commit":
                for side in sides:
                    side.feed(
                        [
                            pgstream.begin(lsn, -5 + lsn * 1_000_003, 7),
                            *step[1],
                            pgstream.commit(lsn, lsn + 5, lsn * 1_000_003 + 7),
                        ]
                    )
                lsn += 10
            else:
                # ("apply",): both tables in one run (production's shape: the SQL fold's
                # page plan is shared through the run cache); ("apply_one", table): only
                # that table, so tables reach different starting points.
                tables = (TABLE, OTHER_TABLE) if step[0] == "apply" else (step[1],)
                for side, out in zip(sides, results):
                    for r in side.apply.run(tables=tables, run_id=f"run{len(out)}"):
                        out.append(
                            (
                                r.table,
                                r.status,
                                r.boundary,
                                r.events,
                                r.transactions,
                                r.detail,
                                r.units,
                            )
                        )
        snaps = [side.snapshot(out) for side, out in zip(sides, results)]
        stats = dict(sides[1].apply.fold_stats)
        assert sides[0].apply.fold_stats == {}  # the oracle never plans
        PLAN_STATS.append(dict(sides[1].apply.plan_stats))
        return snaps, stats
    finally:
        for side in sides:
            side.con.close()
        # ~23 MB of DuckDB files per pair; pytest keeps three runs' tmp dirs on /tmp
        for path in tmp_path.glob("cdc_p0_sf_*.duckdb*"):
            path.unlink()


def text_layout(identity, keys=("id",), extra=("a", "b")):
    names = list(keys) + list(extra)
    return dict(
        identity=identity,
        keys=keys,
        columns=tuple((1 if n in keys else 0, n, 25, -1) for n in names),
    )


# Fixed scenarios (S2/S4) ---------------------------------------------------------


PLAN_STATS = []  # each SQL side's plan_stats, for the multi-table assertions


def O(*cells):  # noqa: E743 (OTHER's insert)
    return pgstream.insert(OTHER, cells)


def I(*cells):  # noqa: E743
    return pgstream.insert(OID, cells)


def UPD(new, old=None, kind="O"):
    return pgstream.update(OID, new, old, kind)


def DEL(old, kind="O"):
    return pgstream.delete(OID, old, kind)


FIXED = {
    # TEXT -> NULL -> UNCHANGED on one column of one key: NULL must win.
    "null_wins": (
        text_layout("d"),
        [
            ("commit", [I("1", "x", "y")]),
            ("apply",),
            ("commit", [UPD(("1", "t", "y"))]),
            ("commit", [UPD(("1", None, "z"))]),
            ("commit", [UPD(("1", U, U))]),
        ],
    ),
    # insert->delete->update(partial) for a key present in the mirror: precheck passes,
    # unknown cells come from the mirror row.
    "update_after_delete_present": (
        text_layout("d"),
        [
            ("commit", [I("1", "x", "y")]),
            ("apply",),
            ("commit", [I("2", "p", "q"), DEL(("1", None, None), "K")]),
            ("commit", [UPD(("1", "new", U))]),
        ],
    ),
    # the same for a key absent from the mirror: both sides hold (Blocked).
    "update_after_delete_absent": (
        text_layout("d"),
        [
            ("commit", [I("1", "x", "y"), DEL(("1", None, None), "K")]),
            ("commit", [UPD(("1", "new", U))]),
        ],
    ),
    # composite key, FULL identity with UNCHANGED cells in the old image
    "composite_full_old_unchanged": (
        text_layout("f", keys=("id", "k2"), extra=("a", "b")),
        [
            ("commit", [I("1", "a", "x", "y"), I("1", "b", "x", "y")]),
            ("apply",),
            ("commit", [UPD(("1", "a", U, "y2"), ("1", "a", U, "y"))]),
            ("commit", [UPD(("1", "a", "x3", U), ("1", "a", "x2", "y2"))]),
            ("commit", [DEL(("1", "b", "x", "y"))]),
        ],
    ),
    # another relation interleaved, and a TRUNCATE of the other relation in the units
    "other_relation_and_truncate": (
        text_layout("f"),
        [
            ("commit", [I("1", "x", "y"), pgstream.insert(OTHER, ("9", "o"))]),
            ("commit", [pgstream.truncate((OTHER,))]),
            ("commit", [UPD(("1", "z", U), ("1", "x", "y"))]),
        ],
    ),
    # update-only chain that becomes complete without an insert (NOT MATCHED INSERT)
    "complete_without_insert": (
        text_layout("d"),
        [("commit", [UPD(("5", "a", U))]), ("commit", [UPD(("5", U, "b"))])],
    ),
    # S4: a cast failure (numeric 'NaN' into DECIMAL(5,2)): poison isolation on both
    "cast_failure": (
        dict(
            identity="f",
            keys=("id",),
            columns=((1, "id", 20, -1), (0, "n", 1700, NUMERIC_5_2), (0, "t", 25, -1)),
        ),
        [
            ("commit", [I("1", "1.50", "a")]),
            ("commit", [I("2", "2.25", "b")]),
            ("commit", [I("3", "NaN", "c")]),
            ("commit", [I("4", "4.00", "d")]),
            ("commit", [I("5", "5.00", "e")]),
        ],
    ),
    # PR-1: a cast poison and a partial update of a key the mirror lacks in one batch. The
    # Python fold's precheck holds first; the SQL MERGE may raise the cast instead, so it
    # redoes the batch on the Python fold: the same hold, never a poison hold on the wrong
    # unit. Both orders, and both in one commit.
    "partial_then_poison": (
        dict(
            identity="f",
            keys=("id",),
            columns=((1, "id", 20, -1), (0, "n", 1700, NUMERIC_5_2), (0, "t", 25, -1)),
        ),
        [
            ("commit", [I("1", "1.50", "a")]),
            ("apply",),
            ("commit", [UPD(("7", U, "z"))]),
            ("commit", [I("3", "NaN", "c")]),
        ],
    ),
    "poison_then_partial": (
        dict(
            identity="f",
            keys=("id",),
            columns=((1, "id", 20, -1), (0, "n", 1700, NUMERIC_5_2), (0, "t", 25, -1)),
        ),
        [
            ("commit", [I("1", "1.50", "a")]),
            ("apply",),
            ("commit", [I("3", "NaN", "c")]),
            ("commit", [UPD(("7", U, "z"))]),
        ],
    ),
    "partial_and_poison_in_one_commit": (
        dict(
            identity="f",
            keys=("id",),
            columns=((1, "id", 20, -1), (0, "n", 1700, NUMERIC_5_2), (0, "t", 25, -1)),
        ),
        [
            ("commit", [I("1", "1.50", "a")]),
            ("apply",),
            ("commit", [UPD(("7", U, "z")), I("3", "NaN", "c")]),
        ],
    ),
    # Multi-table pages (both tables share pages; the SQL side plans each page once).
    # Tables at different starting points: `items` applied alone first, then both.
    "mt_different_starts": (
        text_layout("f"),
        [
            ("commit", [I("1", "a", "b"), O("1", "x")]),
            ("apply_one", TABLE),
            ("commit", [I("2", "c", "d"), O("2", "y")]),
            ("commit", [O("3", "z")]),
        ],
    ),
    # `items` blocks (a non-value key cell) while `others` proceeds on the SQL fold.
    "mt_one_blocked": (
        text_layout("d"),
        [
            ("commit", [I("1", "a", "b"), O("1", "x")]),
            ("apply",),
            ("commit", [UPD((U, "c", "d")), O("2", "y")]),
            ("commit", [O("3", "z")]),
        ],
    ),
    # A key change in `items` only (it falls back); `others` stays on SQL.
    "mt_key_change_one": (
        text_layout("f"),
        [
            ("commit", [I("1", "a", "b"), O("1", "x")]),
            ("apply",),
            ("commit", [UPD(("9", "a", "b"), ("1", "a", "b")), O("2", "y")]),
        ],
    ),
    # A TRUNCATE listing both tables: both hold needs_resync, identically.
    "mt_truncate_both": (
        text_layout("f"),
        [
            ("commit", [I("1", "a", "b"), O("1", "x")]),
            ("commit", [pgstream.truncate((OID, OTHER))]),
        ],
    ),
    # Only `items` changes on later pages: `others` is idle there (no descriptor in them).
    "mt_one_idle": (
        text_layout("f"),
        [
            ("commit", [I("1", "a", "b"), O("1", "x")]),
            ("apply",),
            ("commit", [I("2", "c", "d")]),
            ("commit", [UPD(("2", "e", U), ("2", "c", "d"))]),
        ],
    ),
    # A relation outside the run (the heartbeat) with an event the plan would flag (a NULL
    # key): the run's tables are unaffected (it isn't judged).
    "mt_outside_relation": (
        text_layout("f"),
        [
            ("commit", [I("1", "a", "b"), O("1", "x"), pgstream.insert(HEARTBEAT, (None, "m"))]),
            ("commit", [O("2", "y")]),
        ],
    ),
    # a schema change inside the batch (two versions): falls back to Python
    "two_versions": (
        text_layout("f"),
        [
            ("commit", [I("1", "x", "y")]),
            (
                "commit",
                [
                    pgstream.relation(
                        OID,
                        "public",
                        "items",
                        "f",
                        ((1, "id", 25, -1), (0, "a", 25, -1), (0, "b", 25, -1), (0, "c", 25, -1)),
                    ),
                    pgstream.insert(OID, ("2", "p", "q", "r")),
                ],
            ),
        ],
    ),
}


# Scenarios whose outcome depends on where unit boundaries fall inside a batch (a hold,
# a poison, the per-batch fallback) run every (batch, unit) pair: at batch=100000 with
# unit=2, several multi-commit units share one batch, which is where a redo on the Python
# fold must hold or poison the right unit. The rest come out the same however the units
# fall, so they run one batch per unit and many units per batch only.
# CDC_TEST_SQL_FOLD_E2E_FULL=1 restores every pair for every scenario.
BOUNDARY_SCENARIOS = {
    "cast_failure",
    "partial_then_poison",
    "poison_then_partial",
    "partial_and_poison_in_one_commit",
    "update_after_delete_absent",
    "two_versions",
    # Multi-table pages: which tables share a page's plan, and where one holds or falls
    # back while the other proceeds, follow the batch and unit boundaries.
    "mt_different_starts",
    "mt_one_blocked",
    "mt_key_change_one",
    "mt_truncate_both",
    "mt_one_idle",
    "mt_outside_relation",
}
FULL_MATRIX = os.getenv("CDC_TEST_SQL_FOLD_E2E_FULL") == "1"


def fixed_cases():
    for name in sorted(FIXED):
        for batch in (100_000, 2):
            for unit in (1, 2):
                full = FULL_MATRIX or name in BOUNDARY_SCENARIOS
                if full or (batch, unit) in ((100_000, 1), (2, 2)):
                    yield pytest.param(name, batch, unit, id=f"{unit}-{batch}-{name}")


@pytest.mark.parametrize("name,batch,unit", list(fixed_cases()))
def test_fixed_scenarios_match(tmp_path, name, batch, unit):
    layout, script = FIXED[name]
    (oracle, sql), stats = run_pair(
        tmp_path, layout, script, max_commits=unit, max_batch_events=batch
    )
    assert sql == oracle, json.dumps(stats)


# Random wire streams ---------------------------------------------------------------

SEEDS = int(os.getenv("CDC_TEST_SQL_FOLD_E2E_SEEDS", "20"))


def random_script(rng):
    identity = rng.choice("fd")
    layout = text_layout(
        identity,
        keys=rng.choice([("id",), ("id", "k2")]),
        extra=tuple(f"c{i}" for i in range(rng.randint(1, 3))),
    )
    names = [c[1] for c in layout["columns"]]
    keys = layout["keys"]
    rows = {}  # the source's current rows: key -> full cell list
    script = []

    def val():
        return (
            None
            if rng.random() < 0.2
            else rng.choice(["a", "b", "", "q'z"]) + str(rng.randint(0, 9))
        )

    for _ in range(rng.randint(3, 14)):
        msgs = []
        for _ in range(rng.randint(1, 4)):
            key = tuple(str(rng.randint(0, 4)) for _ in keys)
            row = rows.get(key)
            if row is None:
                if rng.random() < 0.15:  # update of a row only the mirror may know: partial
                    new = [
                        key[keys.index(n)] if n in keys else (U if rng.random() < 0.5 else val())
                        for n in names
                    ]
                    msgs.append(UPD(tuple(new)))
                    continue
                full = [key[keys.index(n)] if n in keys else val() for n in names]
                rows[key] = full
                msgs.append(I(*full))
            elif rng.random() < 0.25:
                old = (
                    row
                    if identity == "f"
                    else [c if n in keys else None for n, c in zip(names, row)]
                )
                msgs.append(DEL(tuple(old), "O" if identity == "f" else "K"))
                del rows[key]
            else:
                new = list(row)
                unchanged = []
                for i, n in enumerate(names):
                    if n in keys:
                        continue
                    r = rng.random()
                    if r < 0.4:
                        new[i] = val()
                    elif r < 0.7:
                        unchanged.append(i)  # TOAST-style unchanged
                wire = [U if i in unchanged else c for i, c in enumerate(new)]
                if identity == "f":
                    msgs.append(UPD(tuple(wire), tuple(row)))
                else:
                    msgs.append(UPD(tuple(wire)))
                rows[key] = new
        script.append(("commit", msgs))
        if rng.random() < 0.25:
            script.append(("apply",))
    return layout, script


@pytest.mark.parametrize("seed", range(SEEDS))
def test_random_streams_match(tmp_path, seed):
    rng = random.Random(seed)
    layout, script = random_script(rng)
    batch = rng.choice([2, 5, 100_000])
    unit = rng.choice([1, 1, 2, 3])
    (oracle, sql), stats = run_pair(
        tmp_path, layout, script, max_commits=unit, max_batch_events=batch
    )
    assert sql == oracle, (seed, json.dumps(stats))
    # The random streams are the common case: no batch falls back (else this differential
    # would only be comparing the Python fold with itself).
    assert set(stats) == {"sql"}, (seed, stats)


def test_the_merge_error_clause_is_what_holds_an_absent_partial_row(tmp_path, monkeypatch):
    """Mutant: without the MERGE's WHEN NOT MATCHED ... THEN ERROR clause the SQL fold
    would insert a partial row; the differential must catch it."""
    real = sql_fold.merge_sql

    def mutant(*args):
        sql = real(*args)
        start = sql.index("WHEN NOT MATCHED AND s._s_op='upsert' AND NOT s._s_complete")
        end = sql.index("WHEN NOT MATCHED AND s._s_op='upsert' THEN")
        return sql[:start] + sql[end:]

    monkeypatch.setattr(sql_fold, "merge_sql", mutant)
    layout, script = FIXED["update_after_delete_absent"]
    (oracle, sql), _ = run_pair(tmp_path, layout, script, max_commits=1)
    assert sql != oracle


@pytest.mark.parametrize("fold", ["sql", "python"])
def test_an_unknown_record_kind_refuses_the_batch_on_either_fold(tmp_path, fold):
    layout, script = FIXED["null_wins"]
    side = Side(tmp_path, "r1", fold, layout, max_commits=1, apply_options={})
    try:
        side.feed([pgstream.begin(100, 1, 7), I("1", "x", "y"), pgstream.commit(100, 105, 2)])
        side.con.execute(
            f"UPDATE {side.log.prefix}frames_v2_20261106 SET record_kind=3 WHERE record_kind=1"
        )
        with pytest.raises(ValueError, match="unknown format-2 frame kind"):
            side.apply.apply_table(TABLE, "r1")
    finally:
        side.con.close()
        for path in tmp_path.glob("cdc_p0_sf_*.duckdb*"):
            path.unlink()


def test_multi_table_pages_plan_once_per_page(tmp_path):
    """Two tables at the same position share each page's plan: hits on the SQL side."""
    PLAN_STATS.clear()
    layout, script = FIXED["mt_outside_relation"]
    (oracle, sql), stats = run_pair(tmp_path, layout, script, max_commits=1)
    assert sql == oracle and set(stats) == {"sql"}
    assert PLAN_STATS[-1]["hits"] >= PLAN_STATS[-1]["fills"] >= 1
    assert PLAN_STATS[-1]["batched_failed"] == 0
