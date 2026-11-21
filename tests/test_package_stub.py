"""Package Flights (build_flight.py --package): the pin, the requirements line (anonymous,
or with a token), and the stub, run against a fake installed distribution: it hides the
install token when there is one, refuses a package that isn't the pinned version, commit
and repository, and runs the command."""

import ast
import dataclasses
import importlib
import importlib.util
import json
import subprocess
import sys
import uuid
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("build_flight", ROOT / "scripts/build_flight.py")
build_flight = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(build_flight)

SHA = "0123456789abcdef0123456789abcdef01234567"
VERSION = "0.1.0"
TOKEN = "dummy-not-a-token-1234"
URL = "https://github.com/motherduckdb/motherduck-pg-cdc-flight"
BACKFILL_FLIGHT = str(uuid.UUID(int=1))
# Stands in for the package: main() reports what the command would run with.
CLI = """import json, os, sys
from cdc_flight import pg_ca

def main():
    print("main " + json.dumps({
        "argv": sys.argv,
        "baked": pg_ca.BAKED,
        "token_vars": sorted(k for k, v in os.environ.items() if "TOKEN" in k or v == TOKEN),
    }))
""".replace("== TOKEN", f"== {TOKEN!r}")
PG_CA = "BAKED = None\n\ndef bake(name):\n    global BAKED\n    BAKED = name\n"


def _direct(url=URL, commit=SHA, vcs="git"):
    """A PEP 610 direct_url.json for a VCS install."""
    return {"url": url, "vcs_info": {"vcs": vcs, "commit_id": commit}}


def _install(site, version=VERSION, direct_url=None):
    """A fake site directory: cdc_flight (cli, pg_ca) and its dist-info."""
    package = site / "cdc_flight"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("")
    (package / "cli.py").write_text(CLI)
    (package / "pg_ca.py").write_text(PG_CA)
    info = site / f"motherduck_pg_cdc_flight-{version}.dist-info"
    info.mkdir()
    (info / "METADATA").write_text(
        f"Metadata-Version: 2.1\nName: motherduck-pg-cdc-flight\nVersion: {version}\n"
    )
    if direct_url is None:
        direct_url = _direct()
    if direct_url is not False:
        (info / "direct_url.json").write_text(json.dumps(direct_url))


def _run(tmp_path, source, site=None, secret="cdc_flight_pkg"):
    """Run a stub as the Flight does (a script), with the token in both variables of the
    Flights secret `secret` (None: no token secret attached)."""
    script = tmp_path / "flight" / "main.py"
    script.parent.mkdir(exist_ok=True)
    script.write_text(source)
    env = {
        "PATH": "/usr/bin:/bin",
        "PYTHONPATH": str(site or tmp_path / "site"),
    }
    if secret:
        env.update({"CDC_FLIGHT_GITHUB_TOKEN": TOKEN, f"{secret}_CDC_FLIGHT_GITHUB_TOKEN": TOKEN})
    # -S: no site-packages, so the repo's own editable install can't answer.
    return subprocess.run(
        [sys.executable, "-S", str(script)],
        cwd=script.parent,
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
    )


def _main(result):
    lines = [line for line in result.stdout.splitlines() if line.startswith("main ")]
    return json.loads(lines[0][5:]) if lines else None


def test_the_requirements_line_pins_one_commit_anonymously():
    assert build_flight.requirements(SHA) == (
        f"motherduck-pg-cdc-flight @ git+https://github.com/motherduckdb/motherduck-pg-cdc-flight@{SHA}\n"
    )


@pytest.mark.parametrize("secret", ["cdc_flight_pkg", "other_pkg"])
def test_the_requirements_line_with_a_token_names_only_the_variable(secret):
    """Any secret name gives the same line: the bare key is always injected."""
    assert build_flight.requirements(SHA, token=secret) == (
        "motherduck-pg-cdc-flight @ git+https://x-access-token:${CDC_FLIGHT_GITHUB_TOKEN}"
        f"@github.com/motherduckdb/motherduck-pg-cdc-flight@{SHA}\n"
    )


@pytest.mark.parametrize("token", [None, "cdc_flight_pkg"], ids=["anonymous", "token"])
@pytest.mark.parametrize("command", sorted(build_flight.ENTRY))
def test_the_stub_runs_the_command_without_the_token(tmp_path, command, token):
    owner = {"source_id": "probe", "backfill_flight": BACKFILL_FLIGHT}
    owner["compact_flight"] = str(uuid.UUID(int=2))
    if command not in build_flight.ACTION_COMMANDS:
        owner = None
    elif command != "table-action":
        owner = {k: v for k, v in owner.items() if k != "compact_flight"}
        if command != "table-membership":
            owner = {"source_id": "probe"}
    pg_ca = "rds-us-east-1" if command in build_flight.PG_COMMANDS else None
    source = build_flight.stub(command, SHA, VERSION, owner, pg_ca=pg_ca, token=token)
    ast.parse(source)
    # No secret, no credential URL, no code beyond the stub's.
    for text in (TOKEN, "github_pat_", "@github.com", "${", "SOURCES"):
        assert text not in source
    # An anonymous stub has no token to hide, and doesn't mention one.
    assert ("TOKEN" in source) == (token is not None)
    # Its literals read back as written.
    lines = build_flight.literals(source)
    assert f"EXPECTED_SHA = {SHA!r}" in lines and f"EXPECTED_VERSION = {VERSION!r}" in lines
    assert (f"PG_CA = {pg_ca!r}" in lines) == (pg_ca is not None)
    _install(tmp_path / "site")
    result = _run(tmp_path, source, secret=token)
    assert result.returncode == 0, result.stderr
    assert result.stdout.splitlines()[0] == (
        f"pg-cdc-flight-stub version_check ok version={VERSION} commit={SHA} vcs=git source=ok"
    )
    ran = _main(result)
    argv = ["main.py", command]
    if owner is not None:
        values = json.loads(ast.literal_eval(dict(x.split(" = ", 1) for x in lines)["OWNER"]))
        assert {k: values[k] for k in owner} == owner
        argv += ["--owner", json.dumps(values, sort_keys=True)]
    assert ran == {"argv": argv, "baked": pg_ca, "token_vars": []}
    assert TOKEN not in result.stdout + result.stderr


@pytest.mark.parametrize("token", [None, "cdc_flight_pkg"], ids=["anonymous", "token"])
def test_write_puts_the_matching_requirements_next_to_the_stub(tmp_path, token):
    out = tmp_path / "preflight.py"
    build_flight.write("preflight", out, SHA, VERSION, token=token)
    assert out.read_text() == build_flight.stub("preflight", SHA, VERSION, token=token)
    requirements = (tmp_path / "preflight.requirements.txt").read_text()
    assert requirements == build_flight.requirements(SHA, token=token)
    assert ("x-access-token" in requirements) == (token is not None)


def test_a_token_stub_hides_its_own_secrets_alias(tmp_path):
    """A token from a secret with another name: its prefixed variable is hidden too."""
    source = build_flight.stub("preflight", SHA, VERSION, token="other_pkg")
    assert "Flights secret other_pkg." in source
    _install(tmp_path / "site")
    result = _run(tmp_path, source, secret="other_pkg")
    assert result.returncode == 0, result.stderr
    assert _main(result)["token_vars"] == []


@pytest.mark.parametrize(
    "case, version, direct_url",
    [
        ("an older version", "0.0.9", None),
        ("another commit", VERSION, _direct(commit="f" * 40)),
        ("another host", VERSION, _direct(URL.replace("github.com", "evil.example"))),
        ("another host behind userinfo", VERSION, _direct(f"https://{TOKEN}@evil.example/r@x")),
        ("another repository", VERSION, _direct(URL + "-fork")),
        ("http", VERSION, _direct("http" + URL[5:])),
        ("a port", VERSION, _direct(URL.replace(".com", ".com:8443"))),
        ("a local directory", VERSION, {"url": "file:///src", "dir_info": {}}),
        ("not git", VERSION, _direct(vcs="hg")),
        ("no direct_url.json", VERSION, False),
        ("a broken direct_url.json", VERSION, "not json"),
    ],
)
def test_the_stub_refuses_a_package_that_is_not_the_pin(tmp_path, case, version, direct_url):
    _install(tmp_path / "site", version, direct_url)
    if direct_url == "not json":
        next((tmp_path / "site").glob("*.dist-info/direct_url.json")).write_text("{")
    result = _run(tmp_path, build_flight.stub("preflight", SHA, VERSION))
    assert result.returncode == 1, case
    assert _main(result) is None
    (line,) = result.stdout.splitlines()
    assert line.startswith("pg-cdc-flight-stub version_check failed ")
    assert f"expected_version={VERSION} expected_commit={SHA}" in line
    # The URL is never printed: it may hold the token.
    assert TOKEN not in result.stdout + result.stderr and "evil" not in line


def test_the_stub_refuses_a_shadowing_cdc_flight(tmp_path):
    """The pinned package must be what imports: a cdc_flight next to the source loses."""
    _install(tmp_path / "site")
    shadow = tmp_path / "flight" / "cdc_flight"
    shadow.mkdir(parents=True)
    (shadow / "__init__.py").write_text("")
    result = _run(tmp_path, build_flight.stub("preflight", SHA, VERSION))
    assert result.returncode == 1 and _main(result) is None
    assert result.stdout.startswith("pg-cdc-flight-stub version_check failed ")


def test_the_stub_refuses_no_package(tmp_path):
    (tmp_path / "site").mkdir()
    result = _run(tmp_path, build_flight.stub("preflight", SHA, VERSION))
    assert result.returncode == 1
    assert result.stdout.startswith("pg-cdc-flight-stub version_check failed version=None ")


def test_no_flight_declares_the_token_as_config(monkeypatch):
    for command, keys in build_flight.FLIGHT_CONFIG.items():
        assert not set(build_flight.TOKEN_NAMES) & set(keys), command
    for name in build_flight.TOKEN_NAMES:
        config = dict(build_flight.FLIGHT_CONFIG)
        config["preflight"] = (*config["preflight"], name)
        monkeypatch.setattr(build_flight, "FLIGHT_CONFIG", config)
        with pytest.raises(ValueError, match="install token"):
            build_flight.stub("preflight", SHA, VERSION)


def test_the_stub_checks_its_settings():
    with pytest.raises(ValueError, match="doesn't connect to PostgreSQL"):
        build_flight.stub("apply", SHA, VERSION, pg_ca="rds-us-east-1")
    with pytest.raises(ValueError, match="needs owner settings"):
        build_flight.stub("table-action", SHA, VERSION)
    with pytest.raises(ValueError, match="not owner settings"):
        build_flight.stub("ingest", SHA, VERSION, {"source_id": "probe"})


def test_the_build_requires_package(tmp_path):
    """Every Flight is a package Flight (bundles were removed, USER 2026-11-17)."""
    result = subprocess.run(
        [sys.executable, str(ROOT / "scripts/build_flight.py"), "ingest", str(tmp_path / "f.py")],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 2 and "--package" in result.stderr
    assert not (tmp_path / "f.py").exists()


def test_stub_fits_the_flight_source_limit():
    """MotherDuck's Flight source limit (flights/budgets.json, build.flight_source_bytes),
    for every command with its largest settings: owner flags, the global CA."""
    owner = {
        "source_id": "s" * 40,
        "backfill_flight": BACKFILL_FLIGHT,
        "dsn_env": "D" * 56 + "_DSN",
    }
    owner.update(compact_flight=str(uuid.UUID(int=2)), slot="x" * 60, publication="p" * 60)
    for command in build_flight.ENTRY:
        settings = None
        if command in build_flight.ACTION_COMMANDS:
            module = importlib.import_module(f"cdc_flight.{build_flight.ACTION_COMMANDS[command]}")
            names = {f.name for f in dataclasses.fields(module.Owner)}
            settings = {k: v for k, v in owner.items() if k in names}
        pg_ca = "rds-global" if command in build_flight.PG_COMMANDS else None
        source = build_flight.stub(command, SHA, VERSION, settings, pg_ca=pg_ca)
        assert len(source.encode()) < build_flight.LIMIT, command


def _git(repo, *args):
    return subprocess.run(
        ["git", "-C", str(repo), *args], check=True, capture_output=True, text=True
    ).stdout.strip()


@pytest.fixture
def repo(tmp_path):
    """A repository with main at a merged commit and a later unmerged one."""
    repo = tmp_path / "repo"
    (repo / "src").mkdir(parents=True)
    _git(repo, "init", "-q", "-b", "main")
    _git(repo, "config", "user.email", "test@example.invalid")
    _git(repo, "config", "user.name", "test")
    (repo / "pyproject.toml").write_text(
        f'[project]\nname = "motherduck-pg-cdc-flight"\nversion = "{VERSION}"\n'
    )
    (repo / "src/a.py").write_text("")
    _git(repo, "add", ".")
    _git(repo, "commit", "-q", "-m", "merged")
    merged = _git(repo, "rev-parse", "HEAD")
    _git(repo, "switch", "-q", "-c", "topic")
    (repo / "src/a.py").write_text("x = 1\n")
    _git(repo, "commit", "-q", "-am", "unmerged")
    unmerged = _git(repo, "rev-parse", "HEAD")
    return repo, merged, unmerged


def test_the_pin_is_a_checked_out_commit_merged_into_main(repo):
    repo, merged, unmerged = repo
    _git(repo, "switch", "-q", "--detach", merged)
    assert build_flight.pinned(merged, repo, "main") == VERSION
    for ref, message in [
        (merged[:12], "full 40-hex"),
        ("main", "full 40-hex"),
        (merged.upper(), "full 40-hex"),
        ("f" * 40, "isn't a commit"),
        (unmerged, "isn't merged into main"),
    ]:
        with pytest.raises(ValueError, match=message):
            build_flight.pinned(ref, repo, "main")
    with pytest.raises(ValueError, match="isn't merged"):
        build_flight.pinned(merged, repo, "no-such-ref")
    # The stub's literals come from this checkout: it must be the pinned commit, clean.
    _git(repo, "switch", "-q", "main")
    for path in ("src/b.py", "scripts/build_flight.py", "flights/charts/x.mmd", "pyproject.toml"):
        (repo / path).parent.mkdir(parents=True, exist_ok=True)
        before = (repo / path).read_text() if (repo / path).exists() else None
        (repo / path).write_text("changed\n")
        with pytest.raises(ValueError, match="uncommitted"):
            build_flight.pinned(merged, repo, "main")
        if before is None:
            (repo / path).unlink()
        else:
            (repo / path).write_text(before)
    (repo / "notes.txt").write_text("")  # outside CLEAN: allowed
    assert build_flight.pinned(merged, repo, "main") == VERSION
    (repo / "src/b.py").write_text("")
    _git(repo, "merge", "-q", "--ff-only", "topic")
    with pytest.raises(ValueError, match="checkout of"):
        build_flight.pinned(merged, repo, "main")


def test_the_stub_accepts_userinfo_on_the_pinned_url_and_never_prints_it(tmp_path):
    """If uv records the token in direct_url.json, the check still passes, silently."""
    direct_url = _direct(URL.replace("//", f"//x-access-token:{TOKEN}@"))
    _install(tmp_path / "site", direct_url=direct_url)
    result = _run(tmp_path, build_flight.stub("preflight", SHA, VERSION))
    assert result.returncode == 0, result.stderr
    assert _main(result)["argv"] == ["main.py", "preflight"]
    assert TOKEN not in result.stdout + result.stderr


@pytest.mark.parametrize(
    "word",
    [
        "REPOSITORY",
        "TOKEN_NAMES",
        "BAKE",
        "OWNER_LINE",
        "ARGV",
        "COMMAND",
        "PG_CA_LINE",
        "SHA_VALUE",
    ],
)
def test_an_owner_setting_that_spells_a_placeholder_is_kept_as_written(tmp_path, word):
    """The templates are filled in one pass: an owner value is never re-substituted."""
    owner = {"source_id": "probe", "backfill_flight": BACKFILL_FLIGHT, "dsn_env": f"X_{word}_DSN"}
    source = build_flight.stub("table-membership", SHA, VERSION, owner)
    ast.parse(source)
    values = dict(line.split(" = ", 1) for line in build_flight.literals(source))
    assert json.loads(ast.literal_eval(values["OWNER"]))["dsn_env"] == f"X_{word}_DSN"
    _install(tmp_path / "site")
    result = _run(tmp_path, build_flight.stub("table-membership", SHA, VERSION, owner))
    assert result.returncode == 0, result.stderr
    assert json.loads(_main(result)["argv"][-1])["dsn_env"] == f"X_{word}_DSN"
