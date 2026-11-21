"""Float keys fold by canonical value (dev-docs/apply.md, float keys).

PostgreSQL prints float -0 as "-0" and 0 as "0", while its unique index and the mirror's
typed MERGE treat them as one key. Both folds group a float key by its canonical text
(-0 -> 0) and write that value, so the SQL and Python folds agree with each other and
with the source. Every case runs through test_sql_fold_e2e's harness (real Spool and
IngestLog2, ApplyRun(fold="python") against ApplyRun()), and checks the mirror's rows,
not only that the two folds agree.
"""

import math

import pytest
from test_sql_fold_e2e import DEL, UPD, I, O, run_pair

from cdc_flight import mirror, sql_fold

FLOAT8, FLOAT4, INT8, TEXT = 701, 700, 20, 25


def layout(*columns, identity="d"):
    return dict(
        identity=identity,
        keys=tuple(name for flag, name, _, _ in columns if flag),
        columns=tuple(columns),
    )


F8 = layout((1, "f", FLOAT8, -1), (0, "a", TEXT, -1))
F4 = layout((1, "f", FLOAT4, -1), (0, "a", TEXT, -1))
PAIR = layout((1, "i", INT8, -1), (1, "f", FLOAT8, -1), (0, "a", TEXT, -1))
TEXT_KEY = layout((1, "f", TEXT, -1), (0, "a", TEXT, -1))
FLOAT_VALUE = layout((1, "id", TEXT, -1), (0, "v", FLOAT8, -1))


# name: (layout, script, expected mirror rows as (key..., value) without the _cdc_ columns)
CASES = {
    # The external review's repro: delete 0 and insert -0 in one transaction was empty.
    "delete_0_insert_minus0": (
        F8,
        [
            ("commit", [I("0", "x")]),
            ("apply",),
            ("commit", [DEL(("0", None), "K"), I("-0", "y")]),
        ],
        [(0.0, "y")],
    ),
    "float4_delete_0_insert_minus0": (
        F4,
        [
            ("commit", [I("0", "x")]),
            ("apply",),
            ("commit", [DEL(("0", None), "K"), I("-0", "y")]),
        ],
        [(0.0, "y")],
    ),
    "update_0_to_minus0": (
        F8,
        [
            ("commit", [I("0", "x")]),
            ("apply",),
            ("commit", [UPD(("-0", "y"), ("0", None), "K")]),
        ],
        [(0.0, "y")],
    ),
    "update_minus0_to_0": (
        F8,
        [
            ("commit", [I("-0", "x")]),
            ("apply",),
            ("commit", [UPD(("0", "y"), ("-0", None), "K")]),
        ],
        [(0.0, "y")],
    ),
    "same_batch_insert_minus0_update_0": (
        F8,
        [("commit", [I("-0", "x")]), ("commit", [UPD(("0", "y"), ("-0", None), "K")])],
        [(0.0, "y")],
    ),
    "insert_minus0_alone": (F8, [("commit", [I("-0", "x")])], [(0.0, "x")]),
    "composite_update": (
        PAIR,
        [
            ("commit", [I("1", "0", "x")]),
            ("apply",),
            ("commit", [UPD(("1", "-0", "y"), ("1", "0", None), "K")]),
        ],
        [(1, 0.0, "y")],
    ),
    "composite_delete_insert": (
        PAIR,
        [
            ("commit", [I("1", "-0", "x"), I("2", "0", "p")]),
            ("apply",),
            ("commit", [DEL(("1", "-0", None), "K"), I("1", "0", "y")]),
        ],
        [(1, 0.0, "y"), (2, 0.0, "p")],
    ),
    "nan_is_one_key": (
        F8,
        [("commit", [I("NaN", "x")]), ("apply",), ("commit", [UPD(("NaN", "y"), None, "O")])],
        [(math.nan, "y")],
    ),
    # A text key "-0" is a different string from "0": never canonicalized.
    "text_key_minus0_is_distinct": (
        TEXT_KEY,
        [("commit", [I("0", "x"), I("-0", "y")])],
        [("-0", "y"), ("0", "x")],
    ),
}


# Two commits to the other table close any open unit (max_commits up to 2), so every
# case's last commits are applied.
CLOSE = [("commit", [O("z1", "w")]), ("commit", [O("z2", "w")])]


def rows(snapshot, width):
    return [tuple(r[:width]) for r in snapshot["items"]]


def same(a, b):
    return len(a) == len(b) and all(
        len(x) == len(y)
        and all((u != u and v != v) or u == v for u, v in zip(x, y))  # NaN == NaN here
        for x, y in zip(a, b)
    )


@pytest.mark.parametrize("unit", [1, 2])
@pytest.mark.parametrize("name", sorted(CASES))
def test_float_keys_fold_by_canonical_value(tmp_path, name, unit):
    spec, script, expected = CASES[name]
    (oracle, sql), stats = run_pair(tmp_path, spec, script + CLOSE, max_commits=unit)
    assert repr(sql) == repr(oracle), stats  # repr: NaN equals itself
    got = rows(oracle, len(spec["columns"]))
    assert same(sorted(got, key=repr), sorted(expected, key=repr)), got
    # The key is written as it is grouped: never a -0 key in the mirror (counted unsorted;
    # the rows above went through ORDER BY, which drops the sign).
    assert all(n == 0 for n in oracle["items_negative_zero"].values()), oracle[
        "items_negative_zero"
    ]
    # Every state is live: nothing held.
    assert all(s[2] == "live" for s in oracle["state"]), oracle["state"]


def test_a_non_key_float_keeps_its_sign(tmp_path):
    (oracle, sql), _ = run_pair(tmp_path, FLOAT_VALUE, [("commit", [I("1", "-0")])] + CLOSE)
    assert sql == oracle
    ((key, value),) = rows(oracle, 2)
    assert key == "1" and value == 0
    assert oracle["items_negative_zero"] == {"v": 1}  # the value keeps its -0


def test_without_the_canonical_rule_the_review_repro_diverges(tmp_path, monkeypatch):
    """The cases above catch the bug: with the rule off on both folds, the review's
    repro leaves the mirror empty (the delete of 0 wins over the insert of -0)."""
    monkeypatch.setattr(mirror, "canonical_key", lambda name, value, names: value)
    monkeypatch.setattr(sql_fold, "float_keys", lambda columns, keys: frozenset())
    spec, script, _ = CASES["delete_0_insert_minus0"]
    (oracle, sql), _ = run_pair(tmp_path, spec, script + CLOSE, max_commits=1)
    assert rows(oracle, 2) == [] and rows(sql, 2) == []
    # ... and a -0 key alone is written as -0 (so the unsorted count above is a real check).
    spec, script, _ = CASES["insert_minus0_alone"]
    (oracle, sql), _ = run_pair(tmp_path / "alone", spec, script + CLOSE, max_commits=1)
    assert oracle["items_negative_zero"] == sql["items_negative_zero"] == {"f": 1}


def test_every_production_fold_call_passes_float_keys():
    """`float_keys` is keyword-required, and every call site in src/ passes it
    (the generation path and backfill catch-up go through Apply.fold_batch)."""
    import pathlib
    import re

    src = pathlib.Path(mirror.__file__).parent
    calls = []
    for path in sorted(src.glob("*.py")):
        text = path.read_text()
        for m in re.finditer(r"(?<![\w.])(?:mirror2?\.)?fold\(", text):
            line = text[: m.start()].count("\n") + 1
            if text[max(0, m.start() - 4) : m.start()] == "def ":
                continue
            calls.append((path.name, line, text[m.start() : m.start() + 300]))
    assert calls, "no fold call found"
    for name, line, call in calls:
        assert "float_keys=" in call.split(")\n")[0] or "float_keys=" in call[:300], (name, line)
    with pytest.raises(TypeError):
        mirror.fold({}, [], ("id",), ["id"])  # no default
