"""Static scanner for flights/authority.json. No I/O beyond reading source.

A *site* is one place in a Flight's code that uses authority beyond plain config:
- `env`: an environment read whose key isn't a `CDC_` literal (a secret, a runtime value,
  or a key computed at run time);
- `pg_write`: a PostgreSQL write or DDL (PostgreSQL-only tokens, or a write verb in a
  psycopg2 `sql.SQL(...)` template);
- `md_create`: a MotherDuck `CREATE DATABASE` / `CREATE SCHEMA`;
- `run_flight`: an `MD_RUN_FLIGHT` call (starting another Flight).

Sites are keyed by (kind, module, enclosing function, token), never by line, so an edit
elsewhere in a file doesn't churn the inventory.
"""

import ast
import functools
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
import build_flight  # noqa: E402

CONFIG_KEY = re.compile(r"^CDC_[A-Z0-9_]+$")
RUNTIME_KEY = re.compile(r"^(CDC|MOTHERDUCK)_[A-Z0-9_]+$")
# Tokens only PostgreSQL statements carry (DuckDB has no publications or slots).
PG_TOKENS = (
    ("create_publication", re.compile(r"\bCREATE PUBLICATION\b")),
    ("alter_publication", re.compile(r"\bALTER PUBLICATION\b")),
    ("drop_publication", re.compile(r"\bDROP PUBLICATION\b")),
    ("create_slot", re.compile(r"\bpg_create_logical_replication_slot\b")),
    ("drop_slot", re.compile(r"\bpg_drop_replication_slot\b")),
    ("advance_slot", re.compile(r"\bpg_replication_slot_advance\b")),
    ("replication_slot", re.compile(r"\bCREATE_REPLICATION_SLOT\b")),
)
# psycopg2's sql.SQL is PostgreSQL-only, so a write verb in its template is a PG write.
PG_VERB = re.compile(r"^\s*(INSERT|UPDATE|DELETE|CREATE|DROP|ALTER|TRUNCATE)\s+(\w+)", re.I)
MD_CREATE = re.compile(r"\bCREATE\s+(DATABASE|SCHEMA)\b", re.I)
ENV_CALLS = ("getenv", "environ.get", "environ.pop", "environ.setdefault")
# chain.py's typed config readers: (environ, key, ...).
CONFIG_READERS = ("flag", "integer", "flight_id")


def _source(name):
    return (ROOT / "src/cdc_flight" / f"{name}.py").read_text()


def _imports(node):
    """cdc_flight module names imported anywhere under node (function bodies included)."""
    for child in ast.walk(node):
        if isinstance(child, ast.ImportFrom) and child.level:
            raise ValueError(
                "use absolute cdc_flight imports; the closure check skips relative ones"
            )
        if isinstance(child, ast.ImportFrom):
            names = [child.module or ""]
            if child.module == "cdc_flight":
                names = [f"cdc_flight.{alias.name}" for alias in child.names]
        elif isinstance(child, ast.Import):
            names = [alias.name for alias in child.names]
        else:
            continue
        for name in names:
            if name == "cdc_flight" or name.startswith("cdc_flight."):
                yield name.split(".")[1] if "." in name else "__init__"


def _cli_roots(command, cli=None):
    """cli's module-level imports and the command's ENTRY function. Fail closed: an
    import in any other cli function is refused, since nothing here would follow it."""
    tree = ast.parse(cli if cli is not None else _source("cli"))
    module_level = [n for n in tree.body if isinstance(n, (ast.Import, ast.ImportFrom))]
    for node in tree.body:
        if node in module_level or (
            isinstance(node, ast.FunctionDef) and node.name in build_flight.ENTRY.values()
        ):
            continue
        if any(_imports(node)):
            raise ValueError(
                "cli.py imports cdc_flight only at module level or in an ENTRY function"
            )
    entry = next(
        n
        for n in tree.body
        if isinstance(n, ast.FunctionDef) and n.name == build_flight.ENTRY[command]
    )
    return [*module_level, entry]


@functools.cache
def reach(command):
    """Every module a command's run can import: cli's module-level imports, every import
    in its entry function, and everything those modules import anywhere (function-level
    imports included), at any depth."""
    pending = {name for node in _cli_roots(command) for name in _imports(node)}
    reached = {"__init__", "cli"}
    while pending:
        name = pending.pop()
        if name not in reached:
            reached.add(name)
            pending |= set(_imports(ast.parse(_source(name))))
    return frozenset(reached)


def _key(node):
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return None


def _strings(node):
    """String constants under node, f-string parts included."""
    for child in ast.walk(node):
        if isinstance(child, ast.Constant) and isinstance(child.value, str):
            yield child


def _docstrings(tree):
    out = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            body = node.body
            if body and isinstance(body[0], ast.Expr) and _key(body[0].value) is not None:
                out.add(id(body[0].value))
    return out


def scan(module, text):
    """The sites in one module's source: {(kind, module, function, token)}."""
    tree = ast.parse(text)
    docs = _docstrings(tree)
    sites = set()

    def visit(node, where):
        for child in ast.iter_child_nodes(node):
            name = where
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                name = f"{where}.{child.name}" if where != "<module>" else child.name
            check(child, name)
            visit(child, name)

    def env(where, key_node):
        key = _key(key_node)
        if key is None:
            sites.add(("env", module, where, f"<{ast.unparse(key_node)}>"))
        elif not CONFIG_KEY.match(key):
            sites.add(("env", module, where, key))

    def check(node, where):
        if isinstance(node, ast.Call):
            func = ast.unparse(node.func)
            if func.endswith(ENV_CALLS) and node.args:
                env(where, node.args[0])
            elif func.split(".")[-1] in CONFIG_READERS and len(node.args) >= 2:
                if ast.unparse(node.args[0]).endswith("environ"):
                    env(where, node.args[1])
            if func.endswith("sql.SQL") and node.args:
                for s in _strings(node.args[0]):
                    verb = PG_VERB.match(s.value)
                    if verb:
                        sites.add(("pg_write", module, where, verb.group(1).upper()))
        elif isinstance(node, ast.Subscript) and ast.unparse(node.value).endswith("environ"):
            env(where, node.slice)
        elif isinstance(node, ast.Constant) and isinstance(node.value, str):
            if id(node) in docs:
                return
            for token, pattern in PG_TOKENS:
                if pattern.search(node.value):
                    sites.add(("pg_write", module, where, token))
            create = MD_CREATE.search(node.value)
            if create and "PUBLICATION" not in node.value.upper():
                sites.add(("md_create", module, where, create.group(1).upper()))
            if "MD_RUN_FLIGHT" in node.value:
                sites.add(("run_flight", module, where, "MD_RUN_FLIGHT"))

    visit(tree, "<module>")
    return sites


@functools.cache
def flight_modules():
    """Every module some command's run can reach."""
    return tuple(sorted(set().union(*(reach(c) for c in build_flight.ENTRY))))


def scan_all(modules=None, source=_source):
    sites = set()
    for module in modules or flight_modules():
        sites |= scan(module, source(module))
    return sites


def dynamic_imports(text):
    """Calls that import a module by a computed name (invisible to the closure)."""
    tree = ast.parse(text)
    return [
        node.lineno
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and ast.unparse(node.func) in ("importlib.import_module", "__import__", "import_module")
    ]
