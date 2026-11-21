"""Build a Flight's source: a short stub, plus a one-line requirements_txt that installs
the motherduck-pg-cdc-flight package from GitHub at one merged commit (see stub() and
docs/flights.md, "Package install"). Every Flight is a package Flight. The install is
anonymous; --token-secret installs with a read-only GitHub token instead (a private copy
of the repository).

    python scripts/build_flight.py <command> <output> --package <SHA> [--token-secret <name>]
        [owner flags] [--pg-ca …]
"""

import argparse
import ast
import dataclasses
import json
import re
import subprocess
import sys
import tomllib
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from cdc_flight.chain import FLIGHT_CONFIG  # noqa: E402

# Each command's entry function in cdc_flight/cli.py (tests/authority_scan.py follows its
# imports; scripts/check_package.py reads this dict from the source).
ENTRY = {
    "preflight": "main",
    "ingest": "main",
    "apply": "apply_main",
    "backfill": "backfill_main",
    "compact": "compact_main",
    "table-action": "table_action_main",
    "table-membership": "table_membership_main",
    "quarantine": "quarantine_main",
    "epoch": "epoch_main",
    "delete-source": "delete_source_main",
}
# Action Flights carry the owner's settings in the stub (OWNER), never in config.
# Each action command's Owner class (module, class name): its settings are the flags.
ACTION_COMMANDS = {
    "table-action": "action_flight",
    "table-membership": "membership_flight",
    "quarantine": "quarantine_flight",
    "epoch": "epoch_flight",
    "delete-source": "delete_source_flight",
}
# The PostgreSQL commands: --pg-ca bakes verify-full TLS into these (pg_ca.py).
# tests/test_authority.py pins this to the commands whose imports reach pg_ca.
PG_COMMANDS = ("backfill", "delete-source", "epoch", "ingest", "preflight", "table-membership")
# MotherDuck's Flight source limit (the build.flight_source_bytes budget).
LIMIT = 200_000
ROOT = Path(__file__).resolve().parents[1]


def literals(text):
    """A stub's top-level literal settings, as `NAME = value` lines (COMMAND, the
    expected SHA and version, OWNER, PG_CA); never executed."""
    lines = []
    for node in ast.parse(text).body:
        if isinstance(node, ast.Assign) and isinstance(node.value, ast.Constant):
            name = getattr(node.targets[0], "id", "")
            if name:
                lines.append(f"{name} = {node.value.value!r}")
    return lines


def owner_settings(command, owner):
    """The OWNER literal's values: required for an action command, refused otherwise.
    Validated here, so a bad identifier or Flight id fails the build."""
    if command not in ACTION_COMMANDS:
        if owner:
            raise ValueError(f"{command} takes its settings from config, not owner settings")
        return None
    if not owner:
        raise ValueError(f"{command} needs owner settings (--source, --backfill-flight, ...)")
    import importlib

    module = importlib.import_module(f"cdc_flight.{ACTION_COMMANDS[command]}")
    return module.Owner.from_dict(owner)


def owner_literal(owner):
    """The OWNER JSON. `heartbeat_writer` only when `external` (replica capture): a
    primary source's stub bakes exactly what it baked before the field existed."""
    values = dataclasses.asdict(owner)
    if values.get("heartbeat_writer") == "self":
        del values["heartbeat_writer"]
    return json.dumps(values, sort_keys=True)


def fill(template, **values):
    """The template with each placeholder name replaced by its value, in one pass: a
    value (an owner setting) is never searched for placeholders."""
    names = sorted(values, key=len, reverse=True)
    return re.sub("|".join(map(re.escape, names)), lambda m: values[m.group()], template)


def _settings(command, owner, pg_ca):
    """The checked OWNER and PG_CA settings of a stub."""
    if pg_ca is not None:
        from cdc_flight import pg_ca as cas

        if command not in PG_COMMANDS:
            raise ValueError(f"--pg-ca: {command} doesn't connect to PostgreSQL")
        if pg_ca not in cas.CAS:
            raise ValueError(f"--pg-ca: unknown CA {pg_ca!r}; known: {sorted(cas.CAS)}")
    declared = set(TOKEN_NAMES) & set(FLIGHT_CONFIG[command])
    if declared:
        # Config is set by anyone who can run the Flight; the install token's variables
        # must never be declared config keys (docs/flights.md, "Package install").
        raise ValueError(f"{command} declares the install token {sorted(declared)} as config")
    return owner_settings(command, owner)


# The package Flight (--package <SHA>): requirements_txt installs the package from GitHub
# at one commit, anonymously, or with a read-only token from a Flights secret (default
# name TOKEN_SECRET, key TOKEN_KEY, upper case so uv expands it in the URL) for a private
# copy of the repository. The source is a stub.
PACKAGE = "motherduck-pg-cdc-flight"
REPOSITORY = ("https", "github.com", "/motherduckdb/motherduck-pg-cdc-flight")
TOKEN_SECRET = "cdc_flight_pkg"
TOKEN_KEY = "CDC_FLIGHT_GITHUB_TOKEN"
# The variables the token is injected as: the bare key and the secret-prefixed alias.
TOKEN_NAMES = (TOKEN_KEY, f"{TOKEN_SECRET}_{TOKEN_KEY}")
MAIN_REF = "origin/main"
# What a package build reads from the checkout: these must be the pinned commit's.
CLEAN = ("src", "scripts", "flights", "pyproject.toml")


def requirements(sha, token=None):
    """The package Flight's requirements_txt: one line, the same for every command.
    Anonymous, or with `token` (the secret's name; any name gives the same line) the
    token is the password, under a fixed user name: when authentication
    fails, git prints the URL's user name (a token there would be in the run's log),
    never its password."""
    scheme, host, path = REPOSITORY
    userinfo = f"x-access-token:${{{TOKEN_KEY}}}@" if token else ""
    return f"{PACKAGE} @ git+{scheme}://{userinfo}{host}{path}@{sha}\n"


def pinned(sha, repo=ROOT, main_ref=MAIN_REF):
    """The package version at sha, a full commit SHA that is merged into main_ref and
    checked out, clean, in repo: the stub's literals and config template come from this
    checkout, so they must be the pinned commit's."""
    if not re.fullmatch(r"[0-9a-f]{40}", sha or ""):
        raise ValueError("--package takes a full 40-hex commit SHA, never a tag or branch")

    def git(*args):
        return subprocess.run(["git", "-C", str(repo), *args], capture_output=True, text=True)

    if git("cat-file", "-e", f"{sha}^{{commit}}").returncode:
        raise ValueError(f"--package: {sha} isn't a commit in this checkout")
    if git("merge-base", "--is-ancestor", sha, main_ref).returncode:
        raise ValueError(f"--package: {sha} isn't merged into {main_ref} (or it is missing)")
    if git("rev-parse", "HEAD").stdout.strip() != sha:
        raise ValueError(f"--package: build from a checkout of {sha}, not of HEAD")
    # src/ (the OWNER check), scripts/ (this template, FLIGHT_CONFIG's builder), flights/
    # (the chart) and pyproject.toml (the version).
    if git("status", "--porcelain", "--", *CLEAN).stdout:
        raise ValueError(f"--package: uncommitted changes under {', '.join(CLEAN)}")
    project = tomllib.loads((Path(repo) / "pyproject.toml").read_text())["project"]
    if project["name"] != PACKAGE:
        raise ValueError(f"--package: pyproject names {project['name']!r}, not {PACKAGE!r}")
    return project["version"]


STUB = """# cdc_flight Flight stub. The code is the motherduck-pg-cdc-flight package, which this
# Flight's requirements_txt installs from GitHub at EXPECTED_SHAINSTALL_HOW Built by scripts/build_flight.py --package.
import importlib.metadata, json, os, sys, urllib.parse
from pathlib import Path

COMMAND = COMMAND_VALUE
EXPECTED_SHA = SHA_VALUE
EXPECTED_VERSION = VERSION_VALUE
OWNER_LINEPG_CA_LINEPOP_TOKEN

def installed():
    \"\"\"True if the installed package is EXPECTED_VERSION, from the expected GitHub
    repository at EXPECTED_SHA (its PEP 610 direct_url.json), and is what imports.\"\"\"
    try:
        dist = importlib.metadata.distribution("motherduck-pg-cdc-flight")
        info = json.loads(dist.read_text("direct_url.json") or "{}")
    except (importlib.metadata.PackageNotFoundError, ValueError):
        dist, info = None, {}
    info = info if isinstance(info, dict) else {}
    vcs = info["vcs_info"] if isinstance(info.get("vcs_info"), dict) else {}
    url = urllib.parse.urlsplit(str(info.get("url", "")))
    # Compared without any userinfo (it may hold the token), and never printed.
    source = (url.scheme, url.netloc.rpartition("@")[2], url.path) == REPOSITORY
    found = dict(version=dist and dist.version, commit=vcs.get("commit_id"), vcs=vcs.get("vcs"))
    ok = found == dict(version=EXPECTED_VERSION, commit=EXPECTED_SHA, vcs="git") and source
    if ok:
        import cdc_flight

        init = Path(dist.locate_file("cdc_flight/__init__.py")).resolve()
        ok = Path(cdc_flight.__file__).resolve() == init
    status = "ok" if ok else "failed"
    fields = [f"{k}={v}" for k, v in found.items()] + [f"source={'ok' if source else 'mismatch'}"]
    if not ok:
        fields += [f"expected_version={EXPECTED_VERSION}", f"expected_commit={EXPECTED_SHA}"]
    print(" ".join(["pg-cdc-flight-stub version_check", status, *fields]), flush=True)
    return ok


if not installed():
    raise SystemExit(1)
sys.argv = ["main.py", COMMAND]ARGV
BAKEfrom cdc_flight.cli import main

main()
"""


# The stub's install sentence and token block: a token install hides the token from
# cdc_flight code; an anonymous one has nothing to hide.
TOKEN_STUB = (
    ", with the read-only token\n# in the Flights secret SECRET_NAME.",
    """
# The token is for the install only: cdc_flight code never sees it.
for name in TOKEN_NAMES:
    os.environ.pop(name, None)
""",
)
ANONYMOUS_STUB = (", anonymously.\n#", "\n")


def stub(command, sha, version, owner=None, pg_ca=None, token=None):
    """A package Flight's source: no secret, no URL, no code beyond the version check.
    `sha` must come from pinned(). `token`: the name of the Flights secret the install's
    token comes from (requirements() with token), so the stub hides the token; None, an
    anonymous install, has nothing to hide."""
    owner = _settings(command, owner, pg_ca)
    owner_line, argv = "", ""
    if owner is not None:
        values = owner_literal(owner)
        owner_line = f"OWNER = {values!r}\n"
        argv = ' + ["--owner", OWNER]'
    pg_ca_line, bake = "", ""
    if pg_ca is not None:
        pg_ca_line = f"PG_CA = {pg_ca!r}\n"
        bake = "from cdc_flight import pg_ca\n\npg_ca.bake(PG_CA)\n"
    install_how, pop_token = TOKEN_STUB if token else ANONYMOUS_STUB
    return fill(
        fill(STUB, INSTALL_HOW=install_how, POP_TOKEN=pop_token),
        SECRET_NAME=token or TOKEN_SECRET,
        COMMAND_VALUE=repr(command),
        SHA_VALUE=repr(sha),
        VERSION_VALUE=repr(version),
        OWNER_LINE=owner_line,
        PG_CA_LINE=pg_ca_line,
        TOKEN_NAMES=repr((TOKEN_KEY, f"{token}_{TOKEN_KEY}") if token else TOKEN_NAMES),
        REPOSITORY=repr(REPOSITORY),
        ARGV=argv,
        BAKE=bake,
    )


def chart(command):
    """The Flight Explainer chart for a command (flights/charts/<command>.mmd)."""
    return (ROOT / "flights/charts" / f"{command}.mmd").read_text()


def write(command, output, sha, version, owner=None, pg_ca=None, token=None):
    """Write the stub to output and its side files next to it. `sha` and `version` must
    come from pinned()."""
    code = stub(command, sha, version, owner, pg_ca=pg_ca, token=token)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(code)
    output.with_suffix(".requirements.txt").write_text(requirements(sha, token=token))
    # The Flight's declared config: every key with a "" default, meaning "use the
    # default". Fill in CDC_SOURCE_ID and the secret's env name; CDC_TABLES is needed
    # only for the first preflight (unset, the registry is the table set).
    template = output.with_suffix(".config.json")
    template.write_text(json.dumps({key: "" for key in FLIGHT_CONFIG[command]}, indent=2))
    # The Flight Explainer chart: pass it as the Flight's metadata.chart (a full
    # replacement on every update). It is never part of the source.
    output.with_suffix(".chart.mmd").write_text(chart(command))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=sorted(ENTRY))
    parser.add_argument("output", type=Path)
    # A package Flight pinned to this commit (merged into main, checked out here): writes
    # the stub as output and its requirements_txt next to it. Required.
    parser.add_argument("--package", metavar="SHA", required=True)
    # A private copy of the repository: install with the read-only token in this Flights
    # secret (attach it to the Flight). Omitted, the install is anonymous.
    parser.add_argument("--token-secret", metavar="NAME")
    # Action Flights only: the owner's settings, baked into the stub.
    parser.add_argument("--source", dest="source_id")
    parser.add_argument("--database")
    parser.add_argument("--mirror", dest="mirror_database")
    parser.add_argument("--ops-database")
    parser.add_argument("--backfill-flight")
    parser.add_argument("--compact-flight")
    parser.add_argument("--slot")
    parser.add_argument("--publication")
    parser.add_argument("--heartbeat")
    parser.add_argument("--heartbeat-writer", choices=["self", "external"])
    parser.add_argument("--dsn-env")
    # PostgreSQL commands only: bake verify-full TLS against this CA (pg_ca.CAS).
    parser.add_argument("--pg-ca")
    args = parser.parse_args()
    owner = {
        k: v
        for k, v in vars(args).items()
        if k not in ("command", "output", "pg_ca", "package", "token_secret") and v is not None
    }
    try:
        version = pinned(args.package)
        token = args.token_secret
        if token is not None and not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", token):
            raise ValueError(f"--token-secret: {token!r} isn't a secret name")
        write(args.command, args.output, args.package, version, owner, args.pg_ca, token)
    except (TypeError, ValueError) as exc:
        raise SystemExit(f"build_flight.py: {exc}") from None
    how = f"token from {token}" if token else "anonymous install"
    print(
        f"{args.output.name}: stub for {PACKAGE} {version} at {args.package} ({how})",
        file=sys.stderr,
    )
