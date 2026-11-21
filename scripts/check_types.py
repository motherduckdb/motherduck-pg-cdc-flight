"""The strict type check of the spec modules, and its negative canary.

    uv run python scripts/check_types.py            # the check: must pass
    uv run python scripts/check_types.py --canary   # every seeded fault must fail it

The check is basedpyright (pinned in uv.lock; strict, type-ignore comments disabled; see
[tool.basedpyright] in pyproject.toml) plus a scan rejecting any `# type: ignore` or
`# pyright:` comment in the strict files, since `# pyright: ignore[...]` and a file-level
`# pyright: basic` would otherwise silence it. The canary copies the files to a temporary
directory (CDC_CANARY_TMP, else the system default), seeds one fault at a time and expects
the check to fail on each."""

import json
import re
import subprocess
import sys
import tempfile
import tomllib
from os import environ
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
STRICT = (
    "generation_spec.py",
    "generation_commands.py",
    "publication_commands.py",
    "lifecycle_spec.py",
    "lifecycle_commands.py",
)
SUPPRESSION = re.compile(r"#\s*(type:\s*ignore|pyright\s*:)")
# The settings the check relies on: pyproject's [tool.basedpyright] must keep them, and the
# canary runs with pyproject's own settings (only `include` replaced), so loosening them
# there fails both steps instead of turning them silently green.
REQUIRED = {
    "typeCheckingMode": "strict",
    "enableTypeIgnoreComments": False,
    "reportUnnecessaryTypeIgnoreComment": "error",
}


def config():
    return tomllib.loads((ROOT / "pyproject.toml").read_text())["tool"]["basedpyright"]


def loosened():
    """The required settings pyproject no longer has, and strict files it no longer checks."""
    actual = config()
    found = {k: actual.get(k) for k, v in REQUIRED.items() if actual.get(k) != v}
    checked = {Path(p).name for p in actual.get("include", [])}
    if not set(STRICT) <= checked:
        found["include"] = sorted(set(STRICT) - checked)
    return found


# {label: (prepended, appended)}. The file-level downgrade comes with a strict-only error
# (an unannotated parameter), which basic mode would not report.
# `scan`: whether the suppression scan runs (False proves the setting alone catches it).
SEEDS = {
    "type error": ("", 'TYPE_CANARY: int = "x"\n'),
    "strict-only error": ("", "def type_canary(a):\n    return a\n"),
    "type: ignore, setting alone": ("", 'TYPE_CANARY: int = "x"  # type: ignore\n'),
    "type: ignore": ("", 'TYPE_CANARY: int = "x"  # type: ignore\n'),
    "pyright: ignore": ("", 'TYPE_CANARY: int = "x"  # pyright: ignore[reportAssignmentType]\n'),
    "file-level basic": ("# pyright: basic\n", "def type_canary(a):\n    return a\n"),
}


def suppressions(directory):
    return [
        f"{name}:{n}"
        for name in STRICT
        for n, line in enumerate((directory / name).read_text().splitlines(), 1)
        if SUPPRESSION.search(line)
    ]


def check(directory, project=None, *, scan=True):
    """0 when the strict files in `directory` pass, else 1 (printing why)."""
    if loosened():
        print(
            "pyproject [tool.basedpyright] loosened (required:",
            REQUIRED,
            "and include",
            STRICT,
            "):",
            loosened(),
        )
        return 1
    found = suppressions(directory) if scan else []
    if found:
        print("checker suppressions are not allowed:", ", ".join(found))
        return 1
    args = ["basedpyright"] + (["--project", str(project)] if project else [])
    return 1 if subprocess.run(args, cwd=ROOT).returncode else 0


def canary():
    failures = []
    with tempfile.TemporaryDirectory(dir=environ.get("CDC_CANARY_TMP")) as tmp:
        for label, (top, bottom) in SEEDS.items():
            base = Path(tmp) / re.sub(r"\W+", "_", label)
            package = base / "cdc_flight"
            package.mkdir(parents=True)
            (package / "__init__.py").write_text("")
            for name in STRICT:
                text = (ROOT / "src" / "cdc_flight" / name).read_text()
                if name == STRICT[0]:
                    text = top + text + bottom
                (package / name).write_text(text)
            settings = dict(config(), include=[str(package / name) for name in STRICT])
            (base / "pyrightconfig.json").write_text(json.dumps(settings))
            print(f"--- canary: {label} (must fail)")
            scan = not label.endswith("setting alone")
            if check(package, project=base / "pyrightconfig.json", scan=scan) == 0:
                failures.append(label)
    if failures:
        print("CANARY FAILED: the check passed with", ", ".join(failures))
        return 1
    print("canary ok: every seeded fault failed the check")
    return 0


if __name__ == "__main__":
    sys.exit(canary() if sys.argv[1:] == ["--canary"] else check(ROOT / "src" / "cdc_flight"))
