"""Check an installed motherduck-pg-cdc-flight against this checkout, from a clean venv:

    uv venv /tmp/pkg && uv pip install --python /tmp/pkg/bin/python .
    /tmp/pkg/bin/python -I scripts/check_package.py .

The installed version is pyproject's, the installed modules are byte-identical to
src/cdc_flight, every module imports, and every command's entry function exists. A
package Flight runs exactly this code (docs/flights.md, "Package install").
Run with -I so neither the checkout nor the environment can supply cdc_flight.
"""

import ast
import importlib
import importlib.metadata
import sys
import tomllib
from pathlib import Path


def main(repo):
    project = tomllib.loads((repo / "pyproject.toml").read_text())["project"]
    installed = importlib.metadata.version(project["name"])
    assert installed == project["version"], (installed, project["version"])
    import cdc_flight

    package = Path(cdc_flight.__file__).parent
    assert repo.resolve() not in package.resolve().parents, f"imported from the checkout: {package}"
    source = repo / "src/cdc_flight"
    expected = sorted(p.name for p in source.glob("*.py"))
    found = sorted(p.name for p in package.glob("*.py"))
    assert found == expected, sorted(set(found) ^ set(expected))
    for name in expected:
        assert (package / name).read_bytes() == (source / name).read_bytes(), name
        importlib.import_module(
            "cdc_flight" if name == "__init__.py" else f"cdc_flight.{name[:-3]}"
        )
    # build_flight.py's ENTRY, read without importing it (it imports from src/).
    tree = ast.parse((repo / "scripts/build_flight.py").read_text())
    entry = next(
        ast.literal_eval(node.value)
        for node in tree.body
        if isinstance(node, ast.Assign) and getattr(node.targets[0], "id", "") == "ENTRY"
    )
    cli = importlib.import_module("cdc_flight.cli")
    for command, function in sorted(entry.items()):
        assert callable(getattr(cli, function, None)), (command, function)
    print(f"{project['name']} {installed}: {len(expected)} modules, {len(entry)} commands ok")


if __name__ == "__main__":
    main(Path(sys.argv[1]))
