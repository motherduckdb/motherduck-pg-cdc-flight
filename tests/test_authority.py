"""flights/authority.json and flights/budgets.json against the code.

Static (AST and imports only, no I/O). Each checker takes its inputs as arguments, so the
negative canaries at the end feed it fakes and assert that it fires.
"""

import ast
import dataclasses
import importlib
import json
import re

import pytest
from authority_scan import (
    ROOT,
    _cli_roots,
    _imports,
    _source,
    build_flight,
    dynamic_imports,
    flight_modules,
    reach,
    scan,
    scan_all,
)

AUTHORITY = json.loads((ROOT / "flights/authority.json").read_text())
BUDGETS = json.loads((ROOT / "flights/budgets.json").read_text())
COMMANDS = tuple(build_flight.ENTRY)
KINDS = ("env", "pg_write", "md_create", "run_flight")
SITE_FIELDS = {
    "kind", "module", "function", "token", "effect", "commands", "not_executed", "slot", "note",
}  # fmt: skip
BUDGET_FIELDS = {
    "id", "scope", "unit", "target", "admission_cap", "os_cap", "overshoot", "owner",
    "enforcer", "hard", "code", "bounds", "exhaustion", "evidence", "evidence_kind",
}  # fmt: skip
ENFORCERS = ("code", "os", "postgres", "motherduck", "ci")


def site_key(site):
    return (site["kind"], site["module"], site["function"], site["token"])


# --- checkers -------------------------------------------------------------------------


def check_sites(found, sites, reach_of):
    """Every scanned site is listed and every listed site still exists; a site's commands
    can reach its module; a message site executes nothing."""
    errors = []
    listed = {site_key(s): s for s in sites}
    for key in sorted(found - set(listed)):
        errors.append(f"unlisted site {key}: add it to flights/authority.json")
    for key in sorted(set(listed) - found):
        errors.append(f"stale site {key}: no longer in the code")
    for key, site in listed.items():
        if set(site) - SITE_FIELDS or site["kind"] not in KINDS:
            errors.append(f"{key}: unknown field or kind")
        if site["effect"] == "message" and site["commands"]:
            errors.append(f"{key}: a message site executes for no command")
        skipped = site.get("not_executed", {})
        for command in [*site["commands"], *skipped]:
            if command not in reach_of:
                errors.append(f"{key}: unknown command {command}")
            elif site["module"] not in reach_of[command]:
                errors.append(f"{key}: {command} can't reach {site['module']}")
        if site["effect"] == "message":
            continue
        # The judgement is explicit: every command that can reach the module
        # either executes the site or says why not, never both.
        reachers = {c for c, modules in reach_of.items() if site["module"] in modules}
        if set(site["commands"]) & set(skipped):
            errors.append(f"{key}: a command both executes and doesn't")
        missing = reachers - set(site["commands"]) - set(skipped)
        if missing:
            errors.append(f"{key}: no decision for {sorted(missing)} (commands or not_executed)")
        if any(not reason for reason in skipped.values()):
            errors.append(f"{key}: a not_executed entry needs its reason")
    return errors


def derived(sites, command):
    """A command's authority as the union of its sites."""
    mine = [s for s in sites if command in s["commands"] and s["effect"] != "message"]
    return {
        "secrets": sorted({s["slot"] for s in mine if s["kind"] == "env"}),
        "ddl": sorted({s["token"] for s in mine if s["kind"] == "pg_write"}),
        "create": sorted({s["token"] for s in mine if s["kind"] == "md_create"}),
        "triggers": any(s["kind"] == "run_flight" for s in mine),
    }


def check_commands(commands, sites):
    errors = []
    if set(commands) != set(COMMANDS):
        errors.append(f"commands {sorted(commands)} != build_flight.ENTRY {sorted(COMMANDS)}")
    for name, entry in commands.items():
        got = derived(sites, name)
        for field, want in (
            ("secrets", sorted(entry["secrets"])),
            ("ddl", sorted(entry["postgres"]["ddl"])),
            ("create", sorted(entry["motherduck"]["create"])),
            ("triggers", bool(entry["triggers"])),
        ):
            if got[field] != want:
                errors.append(f"{name}.{field}: inventory {want}, sites {got[field]}")
    return errors


def owner_flight_fields(command):
    module = build_flight.ACTION_COMMANDS.get(command)
    if module is None:
        return set()
    owner = importlib.import_module(f"cdc_flight.{module}").Owner
    return {f.name for f in dataclasses.fields(owner) if f.name.endswith("_flight")}


def check_triggers(commands, flight_config, owner_fields, self_readers):
    """Trigger targets are exactly what the code can name: declared *_FLIGHT_ID config,
    an Owner *_flight setting, or the runtime's own id (backfill's re-trigger)."""
    errors = []
    for name, entry in commands.items():
        want = {
            *(("config", k) for k in flight_config.get(name, ()) if k.endswith("_FLIGHT_ID")),
            *(("owner", f) for f in owner_fields(name)),
            *(("runtime", "MOTHERDUCK_FLIGHT_ID") for _ in [0] if name in self_readers),
        }
        got = {(t["source"], t["key"]) for t in entry["triggers"]}
        if got != want:
            errors.append(f"{name}: triggers {sorted(got)}, code names {sorted(want)}")
    return errors


def literal(node):
    """A numeric constant expression (10 * 1024 * 1024, 1 << 32, "540")."""
    value = eval(compile(ast.Expression(node), "<budget>", "eval"), {"__builtins__": {}})  # noqa: S307
    if isinstance(value, str):
        return float(value) if value else None  # os.getenv(KEY, ""): unset by default
    return value


def module_tree(module):
    for path in (ROOT / "src/cdc_flight" / f"{module}.py", ROOT / "tests" / f"{module}.py",
                 ROOT / "scripts" / f"{module}.py"):  # fmt: skip
        if path.exists():
            return ast.parse(path.read_text())
    raise LookupError(module)


def resolve(pointer, tree_of=module_tree):
    """(default, bounds or None) for a budget's code pointer."""
    kind, module, name = pointer.split(":")
    tree = tree_of(module)
    if kind == "const":
        for node in tree.body:
            if isinstance(node, ast.Assign) and any(
                isinstance(t, ast.Name) and t.id == name for t in node.targets
            ):
                return literal(node.value), None
    if kind == "field":
        cls, field = name.split(".")
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef) and node.name == cls:
                for item in node.body:
                    if (
                        isinstance(item, ast.AnnAssign)
                        and isinstance(item.target, ast.Name)
                        and item.target.id == field
                    ):
                        return literal(item.value), None
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not node.args:
            continue
        func = ast.unparse(node.func)
        if kind == "integer" and func.endswith("integer") and len(node.args) == 5:
            if isinstance(node.args[1], ast.Constant) and node.args[1].value == name:
                return literal(node.args[2]), [literal(node.args[3]), literal(node.args[4])]
        if kind == "getenv" and func.endswith("getenv") and len(node.args) == 2:
            if isinstance(node.args[0], ast.Constant) and node.args[0].value == name:
                return literal(node.args[1]), None
    raise LookupError(pointer)


def functions_in(path):
    tree = ast.parse((ROOT / path).read_text())
    return {
        n.name for n in ast.walk(tree) if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
    }


def doc_anchors(path):
    """GitHub-style heading slugs of a markdown file."""
    anchors = set()
    for line in (ROOT / path).read_text().splitlines():
        if line.startswith("#"):
            text = line.lstrip("#").strip().lower()
            anchors.add(re.sub(r"[^\w\- ]", "", text).replace(" ", "-"))
    return anchors


def check_budgets(budgets, resolver=resolve, tests_in=functions_in, anchors_in=doc_anchors):
    errors, seen = [], set()
    for b in budgets:
        bid = b.get("id", "?")
        if bid in seen:
            errors.append(f"{bid}: duplicate id")
        seen.add(bid)
        if set(b) - BUDGET_FIELDS:
            errors.append(f"{bid}: unknown fields {sorted(set(b) - BUDGET_FIELDS)}")
        if b.get("enforcer") not in ENFORCERS:
            errors.append(f"{bid}: enforcer must be one of {ENFORCERS}")
        if b.get("code"):
            try:
                value, bounds = resolver(b["code"])
            except (LookupError, ValueError, SyntaxError) as exc:
                errors.append(f"{bid}: code pointer {b['code']} doesn't resolve ({exc})")
                continue
            recorded = b["target"] if b["target"] is not None else b["admission_cap"]
            if b["unit"] != "invariant" and value != recorded:
                errors.append(f"{bid}: registry {recorded}, code {value}")
            if bounds is not None and bounds != b.get("bounds"):
                errors.append(f"{bid}: bounds {b.get('bounds')}, code {bounds}")
        if not b.get("hard"):
            continue
        evidence, kind = b.get("evidence"), b.get("evidence_kind")
        if not evidence or kind not in ("test", "doc"):
            errors.append(f"{bid}: a hard budget needs evidence (test or doc)")
            continue
        if b["enforcer"] == "code" and kind != "test":
            errors.append(f"{bid}: a hard code-enforced budget needs a test")
        if kind == "test":
            path, _, name = evidence.partition("::")
            if not (ROOT / path).exists() or name not in tests_in(path):
                errors.append(f"{bid}: evidence test {evidence} doesn't exist")
        else:
            path, _, anchor = evidence.partition("#")
            if not (ROOT / path).exists() or (anchor and anchor not in anchors_in(path)):
                errors.append(f"{bid}: evidence doc {evidence} doesn't exist")
    return errors


# --- the real inventory ---------------------------------------------------------------

REACH = {command: reach(command) for command in COMMANDS}


def test_every_site_is_listed_and_reachable():
    assert check_sites(scan_all(), AUTHORITY["sites"], REACH) == []


def test_each_commands_authority_is_the_union_of_its_sites():
    assert check_commands(AUTHORITY["commands"], AUTHORITY["sites"]) == []


def test_pg_secret_commands_are_the_ones_built_with_a_ca():
    pg = {c for c, e in AUTHORITY["commands"].items() if "pg_dsn" in e["secrets"]}
    assert pg == set(build_flight.PG_COMMANDS)


def test_pg_commands_are_the_commands_whose_imports_reach_pg_ca():
    assert set(build_flight.PG_COMMANDS) == {c for c in COMMANDS if "pg_ca" in REACH[c]}


def test_trigger_targets_match_config_owner_settings_and_the_runtime_id():
    from cdc_flight.chain import FLIGHT_CONFIG

    self_readers = {
        c
        for s in AUTHORITY["sites"]
        if s["kind"] == "env" and s["token"] == "MOTHERDUCK_FLIGHT_ID" and s["module"] == "chain"
        for c in s["commands"]
    }
    errors = check_triggers(AUTHORITY["commands"], FLIGHT_CONFIG, owner_flight_fields, self_readers)
    assert errors == []
    starts = AUTHORITY["principals"]["dive"]["starts"]
    assert set(starts) <= set(build_flight.ACTION_COMMANDS)


def test_config_readers_get_literal_runtime_keys():
    """chain's typed readers (flag, integer, flight_id) take a literal CDC_ or MOTHERDUCK_
    key wherever they're called, so a computed key can't read a secret through them."""
    bad = []
    for module in flight_modules():
        tree = ast.parse(_source(module))
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.Call)
                and ast.unparse(node.func).split(".")[-1] in ("flag", "integer", "flight_id")
                and len(node.args) >= 2
            ):
                key = node.args[1]
                if not (
                    isinstance(key, ast.Constant)
                    and re.fullmatch(r"(CDC|MOTHERDUCK)_[A-Z0-9_]+", str(key.value))
                ):
                    bad.append(f"{module}:{node.lineno}")
    assert bad == []


def test_no_module_imports_by_a_computed_name():
    """The closure sees only static imports; a computed import would hide authority."""
    found = {m: dynamic_imports(_source(m)) for m in flight_modules()}
    assert {m: lines for m, lines in found.items() if lines} == {}


def test_budget_registry_matches_the_code_and_its_evidence():
    assert check_budgets(BUDGETS["budgets"]) == []


def test_inventories_are_versioned_and_never_read_by_flight_code():
    """Data for the tests only: no Flight module or build step reads them."""
    assert AUTHORITY["version"] == 1 and BUDGETS["version"] == 1
    readers = [
        path.name
        for path in [ROOT / "scripts/build_flight.py", *(ROOT / "src/cdc_flight").glob("*.py")]
        if re.search(r"authority\.json|budgets\.json", path.read_text())
    ]
    assert readers == []


# --- negative canaries: each checker must fire ----------------------------------------

FAKE_COMMAND = {"canary": {"canary_mod"}}


def test_canary_an_unlisted_secret_read_fails():
    found = scan("canary_mod", 'import os\ndef main():\n    return os.environ["STRIPE_KEY"]\n')
    errors = check_sites(found, [], FAKE_COMMAND)
    assert errors == ["unlisted site ('env', 'canary_mod', 'main', 'STRIPE_KEY'): "
                      "add it to flights/authority.json"]  # fmt: skip


def test_canary_a_module_reached_only_through_a_function_level_import_is_scanned():
    """reach() follows imports inside function bodies: standby_backfill is imported only
    in backfill.standby_backfill(), yet it is in backfill's reach and its sites are
    scanned and listed."""
    tree = ast.parse(_source("backfill"))
    module_level = {n for n in tree.body if isinstance(n, (ast.Import, ast.ImportFrom))}
    assert not any("standby_backfill" in _imports(n) for n in module_level)
    assert "standby_backfill" in set(_imports(tree))
    assert "standby_backfill" in REACH["backfill"]
    assert "standby_backfill" in flight_modules()
    found = scan_all(["standby_backfill"])
    assert ("pg_write", "standby_backfill", "_create_export_slot", "replication_slot") in found
    fake = "import os\ndef f():\n    from cdc_flight import hidden_mod\n"
    assert list(_imports(ast.parse(fake))) == ["hidden_mod"]


# Modules no Flight command imports, each with its reason. A stale entry (a module that
# is now reached, or one that no longer exists) fails.
NOT_REACHED = {
    "apply2": "format-2 aliases kept for one release; no command imports them",
    "backfill2": "format-2 aliases kept for one release; no command imports them",
    "audits": "invariant audits, run by tests and operators, never by a Flight",
    "generation_spec_text": "prose for tests and docs; no command imports it (design property)",
    "lifecycle_spec_text": "prose for tests and docs; no command imports it (design property)",
    "hold_codes": "the closed hold-code set, read by tests and docs",
    "label_cli": "the cdc-flight-label CLI, run outside a Flight",
    "tables_cli": "the cdc-flight-tables CLI, run outside a Flight",
    "ops_init": "explicit ops schema initialization, run by an operator",
    "type_identity": "the pre-deploy type identity report, run by an operator",
    "source": "a real-PostgreSQL test fixture",
    "store": "the executable protocol model, for tests",
}


def test_every_module_is_reached_by_a_command_or_listed_with_a_reason():
    modules = {p.stem for p in (ROOT / "src/cdc_flight").glob("*.py")}
    reached = set(flight_modules())
    assert reached <= modules
    assert not set(NOT_REACHED) & reached, "stale NOT_REACHED entry: now reached"
    assert set(NOT_REACHED) <= modules, "stale NOT_REACHED entry: no such module"
    assert modules - reached == set(NOT_REACHED)


@pytest.mark.parametrize(
    "where, line",
    [("module", "from cdc_flight import mirror"), ("function", "from cdc_flight import mirror")],
)
def test_reach_refuses_cli_imports_it_cannot_follow(where, line):
    """cli imports cdc_flight only at module level or in an ENTRY function: an import in
    any other function is refused (fail closed), and a relative import too."""
    cli = _source("cli")
    if where == "function":
        with pytest.raises(ValueError, match="only at module level or in an ENTRY function"):
            _cli_roots("apply", cli + f"\n\ndef _late():\n    {line}\n")
    else:
        assert _cli_roots("apply", cli + f"\n{line}\n")
    with pytest.raises(ValueError, match="absolute cdc_flight imports"):
        list(_imports(ast.parse("from . import mirror\n")))


def test_canary_an_unlisted_trigger_target_fails():
    commands = {"ingest": {"triggers": [{"source": "config", "key": "CDC_APPLY_FLIGHT_ID"}]}}
    config = {"ingest": ("CDC_APPLY_FLIGHT_ID", "CDC_OTHER_FLIGHT_ID")}
    errors = check_triggers(commands, config, lambda c: set(), set())
    assert len(errors) == 1 and "CDC_OTHER_FLIGHT_ID" in errors[0]
    found = scan("canary_mod", 'def f(con):\n    con.execute("SELECT * FROM MD_RUN_FLIGHT(x)")\n')
    assert check_sites(found, [], FAKE_COMMAND)


def test_canary_unlisted_pg_ddl_fails():
    found = scan(
        "canary_mod", "def f(pg):\n    pg.execute(\"SELECT pg_drop_replication_slot('s')\")\n"
    )
    assert check_sites(found, [], FAKE_COMMAND)
    sites = [{"kind": "pg_write", "module": "canary_mod", "function": "f", "token": "drop_slot",
              "effect": "executes", "commands": ["canary"]}]  # fmt: skip
    entry = {"secrets": [], "postgres": {"ddl": []}, "motherduck": {"create": []}, "triggers": []}
    assert check_commands({"canary": entry}, sites)


def test_canary_a_budget_that_drifts_from_the_code_fails():
    budget = {"id": "x", "scope": "run", "unit": "s", "target": 541, "admission_cap": None,
              "os_cap": None, "overshoot": "", "owner": "", "enforcer": "code", "hard": False,
              "code": "field:config:ApplyConfig.runtime_seconds", "exhaustion": ""}  # fmt: skip
    assert check_budgets([budget]) == ["x: registry 541, code 540"]


def test_canary_a_hard_budget_without_its_evidence_test_fails():
    budget = {"id": "y", "scope": "run", "unit": "s", "target": None, "admission_cap": 15,
              "os_cap": None, "overshoot": "", "owner": "", "enforcer": "code", "hard": True,
              "code": "const:action_base:PREVIEW_MINUTES", "exhaustion": "",
              "evidence": "tests/test_action_flight.py::test_does_not_exist",
              "evidence_kind": "test"}  # fmt: skip
    errors = check_budgets([budget])
    assert errors == [
        "y: evidence test tests/test_action_flight.py::test_does_not_exist doesn't exist"
    ]
    budget.update(evidence="docs/operations.md", evidence_kind="doc")
    assert check_budgets([budget]) == ["y: a hard code-enforced budget needs a test"]


@pytest.mark.parametrize("kind", ["const", "field", "integer", "getenv"])
def test_canary_an_unresolvable_code_pointer_fails(kind):
    budget = {"id": "z", "scope": "run", "unit": "s", "target": 1, "admission_cap": None,
              "os_cap": None, "overshoot": "", "owner": "", "enforcer": "code", "hard": False,
              "code": f"{kind}:chain:NOPE.nope" if kind == "field" else f"{kind}:chain:NOPE",
              "exhaustion": ""}  # fmt: skip
    errors = check_budgets([budget])
    assert len(errors) == 1 and "doesn't resolve" in errors[0]


def test_canary_a_reaching_command_with_no_decision_fails():
    """A command that can reach a site's module must be in commands or not_executed."""
    site = {"kind": "pg_write", "module": "canary_mod", "function": "f", "token": "drop_slot",
            "effect": "executes", "commands": ["canary"]}  # fmt: skip
    reach_of = {"canary": {"canary_mod"}, "other": {"canary_mod"}}
    errors = check_sites({site_key(site)}, [site], reach_of)
    assert errors == ["('pg_write', 'canary_mod', 'f', 'drop_slot'): no decision for ['other'] "
                      "(commands or not_executed)"]  # fmt: skip
    site["not_executed"] = {"other": "never calls f"}
    assert check_sites({site_key(site)}, [site], reach_of) == []
    site["not_executed"] = {"canary": "both"}
    assert "a command both executes and doesn't" in " ".join(
        check_sites({site_key(site)}, [site], reach_of)
    )


def check_deploy_secrets(commands, attached, secret_for):
    """deploy_flights attaches a command's PostgreSQL secret exactly when its inventory
    reads pg_dsn (the plan's S7 item: what's attached vs what's read). `secret_for` is the
    source file's per-command secret (table-membership's own with membership_secret)."""
    errors = []
    for name, secrets in attached.items():
        reads = "pg_dsn" in commands[name]["secrets"]
        if (secret_for(name) in secrets) != reads:
            errors.append(
                f"{name}: deploy_flights attaches {sorted(secrets)}, reads pg_dsn={reads}"
            )
    return errors


EXAMPLE = ROOT / "docs/deploy_flights.example.toml"


def two_roles(tmp_path):
    """The example with its membership_secret lines uncommented (the two-role setup)."""
    text = re.sub(r"^# (membership_(secret|key) =)", r"\1", EXAMPLE.read_text(), flags=re.M)
    assert "\nmembership_secret =" in text
    path = tmp_path / "two_roles.toml"
    path.write_text(text)
    return path


def deployed_secrets(path):
    import uuid

    import deploy_flights

    src = deploy_flights.load_source(path)
    ids = {c: str(uuid.uuid4()) for c in deploy_flights.ORDER}
    attached = {
        c: set(deploy_flights.desired(src, c, ids, "0.0.1").secrets) for c in src.commands()
    }
    return attached, src.secret_for


@pytest.mark.parametrize("roles", ["one", "two"])
def test_deploy_flights_attaches_the_pg_secret_exactly_where_it_is_read(roles, tmp_path):
    attached, secret_for = deployed_secrets(EXAMPLE if roles == "one" else two_roles(tmp_path))
    assert set(attached) == set(COMMANDS)
    if roles == "two":
        assert secret_for("table-membership") != secret_for("ingest")
    assert check_deploy_secrets(AUTHORITY["commands"], attached, secret_for) == []


@pytest.mark.parametrize("roles", ["one", "two"])
def test_canary_an_attached_secret_the_inventory_does_not_read_fails(roles, tmp_path):
    attached, secret_for = deployed_secrets(EXAMPLE if roles == "one" else two_roles(tmp_path))
    attached["apply"] = attached["apply"] | {secret_for("ingest")}
    errors = check_deploy_secrets(AUTHORITY["commands"], attached, secret_for)
    assert len(errors) == 1 and errors[0].startswith("apply:")
