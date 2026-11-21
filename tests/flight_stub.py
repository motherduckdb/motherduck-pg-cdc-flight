"""Shared helpers for tests that build or run a Flight's source: a package stub
(scripts/build_flight.py stub()) pinned to a fixed commit, and a site directory where
that stub's version check passes and `cdc_flight` is this checkout's src/cdc_flight.

A test runs the real stub in a subprocess with `PYTHONPATH=<site>` (stub_env): the fake
dist-info there is found before the editable install's, so only it answers the version
check (vcs=git at SHA), and its `cdc_flight` symlink resolves to the repo's code.
"""

import importlib.util
import json
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("build_flight", ROOT / "scripts/build_flight.py")
build_flight = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(build_flight)

SHA = "0123456789abcdef0123456789abcdef01234567"
VERSION = "0.1.0"
URL = "https://github.com/motherduckdb/motherduck-pg-cdc-flight"
BACKFILL_FLIGHT, COMPACT_FLIGHT = str(uuid.UUID(int=1)), str(uuid.UUID(int=2))
# An action Flight's owner settings, as build_flight.py's flags.
OWNER_FLAGS = ["--source", "probe", "--backfill-flight", BACKFILL_FLIGHT]
OWNER_FLAGS += ["--compact-flight", COMPACT_FLIGHT]
DEFAULT_FLAGS = {
    "table-action": OWNER_FLAGS,
    "table-membership": OWNER_FLAGS[:4],
    "quarantine": OWNER_FLAGS[:2],
    "epoch": OWNER_FLAGS[:2],
    "delete-source": OWNER_FLAGS[:2],
}
# build_flight.py's flags whose argparse dest isn't the flag's own name.
DEST = {"--source": "source_id", "--mirror": "mirror_database"}
# The stub's first line when its version check passes (the fake dist's pin).
STUB_OK = f"pg-cdc-flight-stub version_check ok version={VERSION} commit={SHA} vcs=git source=ok"


def owner_of(flags):
    """build_flight.py's owner flags as the owner dict its argparse builds."""
    pairs = zip(flags[::2], flags[1::2])
    return {DEST.get(flag, flag[2:].replace("-", "_")): value for flag, value in pairs}


def stub_for(tmp_path, command, flags=None, pg_ca=None):
    """The command's stub (as build_flight.py --package builds it, at SHA), with the
    owner flags given or the command's defaults; also makes stub_env's site directory.
    A bad owner setting raises ValueError or TypeError, as the build does."""
    if flags is None:
        flags = DEFAULT_FLAGS.get(command, [])
    package_site(tmp_path)
    return build_flight.stub(command, SHA, VERSION, owner_of(flags) or None, pg_ca=pg_ca)


def package_site(tmp_path):
    """tmp_path/site: a motherduck-pg-cdc-flight dist-info installed from git at SHA, and
    a cdc_flight symlink to src/cdc_flight (the dist's files resolve to the repo's)."""
    site = tmp_path / "site"
    if site.exists():
        return site
    site.mkdir()
    (site / "cdc_flight").symlink_to(ROOT / "src" / "cdc_flight", target_is_directory=True)
    info = site / f"motherduck_pg_cdc_flight-{VERSION}.dist-info"
    info.mkdir()
    (info / "METADATA").write_text(
        f"Metadata-Version: 2.1\nName: motherduck-pg-cdc-flight\nVersion: {VERSION}\n"
    )
    direct = {"url": URL, "vcs_info": {"vcs": "git", "commit_id": SHA}}
    (info / "direct_url.json").write_text(json.dumps(direct))
    return site


def stub_env(tmp_path):
    """The environment entries a stub run needs: the site directory first on the path."""
    return {"PYTHONPATH": str(package_site(tmp_path))}


def assert_stub_ok(stdout):
    """The stub's version check passed: the run imported the repo's code through the fake
    dist, not the editable install (whose direct_url isn't git at SHA)."""
    first = stdout.splitlines()[0] if stdout else ""
    assert first == STUB_OK, first
