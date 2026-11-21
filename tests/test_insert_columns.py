"""Every INSERT names its columns (A3 follow-up, findings: rollback after a store upgrade).

Tables only ever gain columns (ADDED_COLUMNS, cdc_ops `_schema`), and Flights at different
versions share a store. A positional `INSERT INTO t VALUES (...)` or `INSERT INTO t SELECT
...` breaks the first time a column is added for any older Flight still deployed: on
2026-11-05 the 4a09a16 ingest could not append once `log_partitions.bytes` existed.
"""

import ast
import re

from authority_scan import ROOT, flight_modules

SRC = ROOT / "src/cdc_flight"
# Phase 0's probe store and synthetic source: no Flight ships them (checked below), so
# they never share a store with another version. Every shipped module is checked.
UNSHIPPED = ("store", "source")
# INSERT [OR ...] INTO <target> then VALUES/SELECT/DEFAULT with no "(" column list.
POSITIONAL = re.compile(
    r"INSERT\s+(?:OR\s+\w+\s+)?INTO\s+(?P<target>[^\s(]+)\s+(?:VALUES|SELECT|DEFAULT|BY\s+POSITION)\b",
    re.IGNORECASE,
)


def statements(tree):
    """Every SQL-looking string: plain constants, f-strings (holes as {}), and the
    implicit concatenation Python already joined."""
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            yield node.lineno, node.value
        elif isinstance(node, ast.JoinedStr):
            text = "".join(v.value if isinstance(v, ast.Constant) else "{}" for v in node.values)
            yield node.lineno, text


def offenders(modules=None):
    found = []
    paths = sorted(SRC.glob("*.py")) if modules is None else [SRC / f"{m}.py" for m in modules]
    for path in paths:
        tree = ast.parse(path.read_text())
        for line, text in statements(tree):
            for match in POSITIONAL.finditer(text):
                found.append(f"{path.name}:{line}: {match.group(0)}")
    return found


def test_every_flight_insert_names_its_columns():
    assert offenders(flight_modules()) == []


def test_only_unshipped_phase0_modules_are_left_positional():
    assert set(flight_modules()).isdisjoint(UNSHIPPED)
    left = {o.split(".py:")[0] for o in offenders()}
    assert left <= set(UNSHIPPED), left


def test_the_guard_sees_the_forms_it_must_refuse(tmp_path, monkeypatch):
    bad = tmp_path / "bad.py"
    bad.write_text(
        'A = "INSERT INTO {s}runs VALUES (?,?)"\n'
        'B = ("INSERT INTO {m}runs " "SELECT 1")\n'
        'C = f"INSERT INTO {table} VALUES (?)"\n'
        'D = "INSERT OR REPLACE INTO t VALUES (1)"\n'
        'OK = "INSERT INTO {s}runs (a,b) VALUES (?,?)"\n'
        'OK2 = f"INSERT INTO {table} (id) SELECT 1"\n'
    )
    monkeypatch.setattr(__import__(__name__), "SRC", tmp_path)
    assert [o.split(": ", 1)[1] for o in offenders()] == [
        "INSERT INTO {s}runs VALUES",
        "INSERT INTO {m}runs SELECT",
        "INSERT INTO {} VALUES",
        "INSERT OR REPLACE INTO t VALUES",
    ]
