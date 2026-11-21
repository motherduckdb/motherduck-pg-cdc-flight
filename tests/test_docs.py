"""The docs: every link resolves, every doc path cited from code exists, the frozen protocol
text is pinned, and the customer docs carry no internal references.

docs/ is customer-facing; dev-docs/ holds the contracts the code relies on (DEVELOPING.md).
"""

import hashlib
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
LINK = re.compile(r"\[[^\]]*\]\(([^)\s]+)\)")
FROZEN = re.compile(r"<!-- frozen:(\S+) begin -->\n(.*?)\n<!-- frozen:\1 end -->", re.S)
# The sha256 of each frozen region's text. A change here is a protocol change: it needs the
# versioned cutover in DEVELOPING.md, "Frozen protocols", and its own review.
PINS = {
    "log-format": "aae0ef01e60fc61f4d824fe52f0d883e9c18d9d6462b6e8167ca44f878b508cd",
    "apply-consumers": "1431f5d88f2141dd1bcdb8a3de0ebae42ee090d6e3c3deabdcd45a46769ffb4a",
    "apply-float-keys": "be21408d58796fdcc0c29cf51bb0abe843caa8c339154679d5cedd332925339a",
    "apply-protocol": "06b253cd5a7956efcc42335218709f8b833d93e81211ec53b1268815d7649a12",
}


def md_files():
    files = [*ROOT.glob("*.md"), *ROOT.glob("docs/*.md"), *ROOT.glob("dev-docs/*.md")]
    return sorted([*files, *ROOT.glob("dive/*.md")])


def prose(path):
    """The file's lines outside code fences, with their numbers."""
    code = False
    for n, line in enumerate(path.read_text().splitlines(), 1):
        if line.lstrip().startswith("```"):
            code = not code
        elif not code:
            yield n, line


def anchors(path):
    """GitHub's heading anchors: lowercased, punctuation dropped, spaces to hyphens."""
    seen, out = {}, set()
    for _, line in prose(path):
        if m := re.match(r"#+ (.*)", line):
            slug = re.sub(r"[^\w\- ]", "", m.group(1).strip().lower()).replace(" ", "-")
            k = seen.get(slug, 0)
            seen[slug] = k + 1
            out.add(slug if k == 0 else f"{slug}-{k}")
    return out


def headings(path):
    return {m.group(1).strip() for _, x in prose(path) if (m := re.match(r"#+ (.*)", x))}


def test_every_link_and_anchor_resolves():
    errors = []
    for path in md_files():
        for n, line in prose(path):
            for target in LINK.findall(line):
                if re.match(r"[a-z]+:", target):
                    continue
                file, _, anchor = target.partition("#")
                dest = (path.parent / file).resolve() if file else path
                where = f"{path.relative_to(ROOT)}:{n} -> {target}"
                if not dest.exists():
                    errors.append(f"{where}: no such file")
                elif anchor and dest.suffix == ".md" and anchor not in anchors(dest):
                    errors.append(f"{where}: no such heading")
    assert errors == []


CODE = ["src", "scripts", "tests", "dive/src", "dive/runtime", "flights"]
CODE_REF = re.compile(
    r"\b((?:dev-)?docs/[\w.-]+\.md|DEVELOPING\.md|STATUS\.md)(?:,? \"([^\"]+)\")?"
)


def code_files():
    for d in CODE:
        for p in (ROOT / d).rglob("*"):
            if p.suffix in (".py", ".ts", ".tsx", ".json") and "node_modules" not in p.parts:
                yield p
    yield ROOT / "compose.yaml"
    yield from (ROOT / ".github").rglob("*.yml")


def test_doc_paths_cited_from_code_exist():
    """A comment naming a doc (and a quoted section in it) points at something that exists."""
    errors = []
    for path in code_files():
        if path.name == "test_docs.py":
            continue
        for n, line in enumerate(path.read_text().splitlines(), 1):
            for doc, section in CODE_REF.findall(line):
                where = f"{path.relative_to(ROOT)}:{n}: {doc}"
                if not (ROOT / doc).exists():
                    errors.append(f"{where}: no such file")
                elif section and section not in headings(ROOT / doc):
                    errors.append(f'{where}: no heading "{section}"')
    assert errors == []


def frozen_regions():
    out = {}
    for path in sorted(ROOT.glob("dev-docs/*.md")):
        for name, body in FROZEN.findall(path.read_text()):
            assert name not in out, f"frozen region {name} twice"
            out[name] = hashlib.sha256(body.encode()).hexdigest()
    return out


def test_frozen_regions_are_pinned():
    """The format-2 encoding, chunk and unit rules and the fold rules, byte for byte."""
    assert frozen_regions() == PINS


# Internal references kept out of the customer docs (docs/ and README.md).
INTERNAL = [
    re.compile(r"(?<![\w&#])#\d{2,3}\b"),  # a PR number
    re.compile(r"\b(?=[0-9a-f]*\d)(?=[0-9a-f]*[a-f])[0-9a-f]{7,40}\b"),  # a commit hash
    re.compile(r"(?<![\d,.])\d{7}(?![\d,.])"),  # an all-digit short commit hash
    re.compile(r"\buser (?:decision|override)|\bexplicit user\b", re.I),
    re.compile(r"\bspike item\b|\bdecision D\d\b|\(D\d\)|\btpcc_ops\b|\bcdc-demo-pg\b"),
    re.compile(r"\bmd-migration-assessment\b|\bmd-(?:suite|leaks|rerun)\b"),
]


def internal_hits():
    hits = set()
    for path in [ROOT / "README.md", *sorted(ROOT.glob("docs/*.md"))]:
        for _, line in prose(path):
            for pattern in INTERNAL:
                for m in pattern.finditer(line):
                    hits.add(f"{path.relative_to(ROOT)}: {m.group(0)}")
    return hits


def test_customer_docs_carry_no_internal_references():
    assert sorted(internal_hits()) == []


def unfrozen(path):
    """prose() minus the frozen regions, whose text is pinned as it is."""
    frozen = False
    for n, line in prose(path):
        if line.startswith("<!-- frozen:"):
            frozen = line.endswith("begin -->")
        elif not frozen:
            yield n, line


def misnamed_links(line):
    """Links whose text names a .md file other than the one they point at."""
    out = []
    for text, target in re.findall(r"\[([^\]]*)\]\(([^)\s#]*)[^)]*\)", line):
        named = re.fullmatch(r"`?(?:[\w.-]+/)*([\w.-]+\.md)`?", text)
        if named and target and named.group(1) != Path(target).name:
            out.append(f"[{text}]({target})")
    return out


def test_a_link_named_after_a_file_points_at_that_file():
    """`[setup.md](...)` must lead to setup.md: link text naming a doc can't go stale."""
    errors = []
    for path in md_files():
        for n, line in unfrozen(path):
            errors += [f"{path.relative_to(ROOT)}:{n}: {x}" for x in misnamed_links(line)]
    assert errors == []


def test_canary_a_misnamed_link_is_caught():
    assert misnamed_links("see [phase2.md](tables.md) and [x](y.md)") == ["[phase2.md](tables.md)"]
    assert misnamed_links("[docs/setup.md](../docs/setup.md#tools) [`a.md`](a.md)") == []


GONE = re.compile(
    r"(?<![\w/-])(?:docs/)?phase[0-4]\.md\b|\bDESIGN\.md\b|(?<![\w/-])docs/testing\.md\b"
    r"|\bSTATUS item \d"
)


def removed_doc_names(line):
    """Names of removed docs in a line; `git show <commit>:<path>` (how to read history) is fine."""
    return GONE.findall(re.sub(r"`git show [0-9a-f]+:[^`]*`", "", line))


def test_no_doc_names_a_removed_doc():
    """The phase docs, DESIGN.md and docs/testing.md moved; prose doesn't point at them."""
    errors = []
    for path in md_files():
        for n, line in unfrozen(path):
            errors += [f"{path.relative_to(ROOT)}:{n}: {m}" for m in removed_doc_names(line)]
    assert errors == []


def test_canary_a_removed_doc_name_is_caught():
    assert removed_doc_names("see docs/phase2.md and DESIGN.md") == ["docs/phase2.md", "DESIGN.md"]
    assert removed_doc_names("`git show 2d37d93:docs/phase2.md` and dev-docs/testing.md") == []


@pytest.mark.parametrize("pattern", INTERNAL, ids=lambda p: p.pattern[:24])
def test_canary_each_internal_pattern_matches(pattern):
    samples = ["see #116", "at 1cb74fc", "(2462253)", "a user decision", "spike item 5", "md-suite"]
    assert any(pattern.search(s) for s in samples)


# The state sections (docs/tables.md "Table states", docs/operations.md "Source lifecycle")
# follow the code's enums and the Dive's labels.
BACKTICKED = re.compile(r"`([a-z_]+)`")
EXTRA_TABLE_STATES = {"Not enabled", "Not started", "Rebuilding", "Removed"}


def section(path, heading):
    """The lines of the `## heading` section, up to the next `## ` heading."""
    lines, inside = [], False
    for _, line in prose(path):
        if line.startswith("## "):
            inside = line == f"## {heading}"
        elif inside:
            lines.append(line)
    return lines


def table_cells(lines, column):
    """One column of the section's first markdown table, header and rule left out."""
    rows = [x for x in lines if x.startswith("| ")]
    return [r.strip("|").split("|")[column].strip() for r in rows[2:]]


def hold_codes_by_state(lines):
    """The codes in the "Hold codes by state" list."""
    start = next(i for i, x in enumerate(lines) if x.startswith("**Hold codes by state**"))
    end = next(i for i in range(start + 1, len(lines)) if not lines[i].strip())
    return {c for x in lines[start:end] for c in BACKTICKED.findall(x)}


def theme_labels(name):
    """The labels of one status object in dive/src/theme.ts."""
    text = (ROOT / "dive/src/theme.ts").read_text()
    body = re.search(rf"export const {name} = \{{(.*?)\}} as const;", text, re.S).group(1)
    return set(re.findall(r"label: '([^']+)'", body))


def state_name(state):
    return state.replace("_", " ").capitalize()


def test_table_states_follow_the_code():
    from cdc_flight.action_flight import STATES
    from cdc_flight.hold_codes import HOLD_CODES

    lines = section(ROOT / "docs/tables.md", "Table states")
    rows = set(table_cells(lines, 0))
    assert rows == {state_name(s) for s in STATES} | EXTRA_TABLE_STATES
    assert hold_codes_by_state(lines) == HOLD_CODES
    shown = " ".join(table_cells(lines, 1))
    assert sorted(x for x in theme_labels("STATUS") if x not in shown) == []
    assert len(theme_labels("STATUS")) == 8


def test_source_lifecycle_follows_the_code():
    from cdc_flight.lifecycle_spec import LIFECYCLES

    lines = section(ROOT / "docs/operations.md", "Source lifecycle")
    rows = set(table_cells(lines, 0))
    assert {state_name(s) for s in LIFECYCLES} <= rows
    shown = " ".join(table_cells(lines, 1))
    assert sorted(x for x in theme_labels("SOURCE_STATUS") if x not in shown) == []
    assert len(theme_labels("SOURCE_STATUS")) == 6


def test_canary_the_state_checks_catch_drift():
    lines = [
        "| State | Shown |",
        "| --- | --- |",
        "| Needs action | Needs action |",
        "",
        "**Hold codes by state** (all of them):",
        "- **Paused:** `paused`, `retry_me`.",
        "",
        "- `not_listed`",
    ]
    assert table_cells(lines, 0) == ["Needs action"]
    assert hold_codes_by_state(lines) == {"paused", "retry_me"}
    assert section(ROOT / "docs/tables.md", "No such heading") == []
    assert {"OK", "Needs action"} <= theme_labels("STATUS")
    assert state_name("needs_resync") == "Needs resync"


# The whole tree is public: no internal process references anywhere tracked. The private notes
# repository may be named only in AGENTS.md's one pointer (NOTES_POINTER).
NOTES_POINTER = (
    "Maintainers keep private working notes (session handoffs, review history) in a separate\n"
    "repository checked out next to this one as ../motherduck-cdc-flight-notes. It isn't public.\n"
    "If it's present, read its handoff.md first. Everything needed to build, test and change the\n"
    "code is in this repository.\n"
)
TREE_INTERNAL = [
    re.compile(r"motherduck-cdc-flight-notes|\.notes\b|\bstreams/|\bhandoff\.md\b|\bHANDOFF\b"),
    re.compile(
        r"\b(?:server-fold|serverfold-\d+|pins-reviewer|md-latency(?:-[\w]+)?|dive-reviewer"
        r"|test-speed-reviewer|coordinator|loadgen finding|[Oo]racle S\d)\b"
    ),
    re.compile(r"\bmd-(?:leaks|rerun)\b|\bdive-sandbox\b"),
    re.compile(r"\bcarlin(?:eng)?\b"),
    re.compile(r"MotherDuck production East|\bDemo org\b|\bdev org\b|\bcdc-demo-pg\b|\btpcc_ops\b"),
    re.compile(r"\buser (?:decision|override)\b|\bexplicit user\b", re.I),
    re.compile(r"\bgit show [0-9a-f]{7,}"),
    # Development labels: PRs and review rounds, and sections of private plans.
    re.compile(r"\bPR ?\d+[a-z]?\b|\breview [0-9a-f]{7}\b|\bRev \d\b|\baddendum [A-Z]\b"),
    re.compile(r"\b(?:plan|design)(?:'s)? §|§\d"),
    re.compile(
        r"\b[Rr]eview (?:round )?\d|\b[Rr]eview (?:R\d-)?[A-Z]+-?\d|\breview (?:NIT|note)\b"
        r"|\bround \d+ (?:[A-Z]\d|NIT)\b|\b(?:[Pp]lan|ci-speed) review\b"
    ),
    re.compile(r"\bP\d+[a-z]?(?:-\d)?\b|\bR\d-S?\d\b|\bdive stream\b|\buser-approved\b"),
    # Other work streams' and reviewers' names, as attributions.
    re.compile(
        r"\bpins'\s|\bpins [A-Z]\d|(?<!\w)\(pins\)|\b(?:cleanup|test-speed)'s\b|\btest-speed\b"
        r"|\bci-speed\b"
    ),
    # A PR number (a CSS colour such as #000; is not one).
    re.compile(r"(?<![\w&#/])#\d{2,3}\b(?!;)"),
    # A UUIDv7 is a real id from a run (fixtures use 00000000-... ids).
    re.compile(r"\b[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\b"),
]
# Generated, vendored or pattern-holding files the tree checks skip.
TREE_SKIP = {
    "uv.lock",
    "dive/package-lock.json",
    "src/cdc_flight/rds_global_ca.py",
    "src/cdc_flight/pg_ca.py",
    "tests/test_docs.py",
    ".gitignore",
}
MD_NAME = re.compile(r"(?<![\w.-])([\w./-]*[\w-]\.md)\b")


def tracked_text():
    import subprocess

    names = subprocess.run(
        ["git", "ls-files"], cwd=ROOT, capture_output=True, text=True, check=True
    ).stdout.split()
    for name in names:
        if name in TREE_SKIP:
            continue
        try:
            text = (ROOT / name).read_text()
        except (UnicodeDecodeError, FileNotFoundError):
            continue
        # Frozen regions are pinned byte for byte (PINS); they stay as they are.
        text = FROZEN.sub("", text)
        if name == "AGENTS.md":
            assert text.count(NOTES_POINTER) == 1, "AGENTS.md's notes pointer changed"
            text = text.replace(NOTES_POINTER, "")
        yield name, text


def tree_internal_hits(name, text):
    return [
        f"{name}:{n}: {m.group(0)}"
        for n, line in enumerate(text.splitlines(), 1)
        for pattern in TREE_INTERNAL
        for m in pattern.finditer(line)
    ]


def untracked_md_names(name, text, tracked):
    return [
        f"{name}:{n}: {m}"
        for n, line in enumerate(text.splitlines(), 1)
        for m in MD_NAME.findall(line)
        if m not in tracked and Path(m).name not in tracked
    ]


def test_tree_carries_no_internal_references():
    errors = []
    for name, text in tracked_text():
        errors += tree_internal_hits(name, text)
    assert errors == []


def test_every_cited_md_file_is_tracked():
    """A `.md` named anywhere is a file in this repository, not a private note."""
    import subprocess

    names = subprocess.run(
        ["git", "ls-files"], cwd=ROOT, capture_output=True, text=True, check=True
    ).stdout.split()
    tracked = {*names, *(Path(n).name for n in names)}
    errors = []
    for name, text in tracked_text():
        errors += untracked_md_names(name, text, tracked)
    assert errors == []


def test_canary_the_tree_checks_catch_internal_references():
    samples = [
        "see streams/dive/x.md",
        "(server-fold S-1)",
        "md-leaks",
        "owner carlin",
        "the Demo org",
        "a user decision",
        "git show 2d37d93:STATUS.md",
        "since #145",
        "(replica PR3b)",
        "(design §5)",
        "id 0190a000-0000-7000-8000-000000000000",
        "(review S3)",
        "review round 14 M1",
        "(round 10 NIT)",
        "(P5a-2)",
        "Review R2-S1:",
        "(plan review S4)",
        "the dive stream",
        "(pins S1)",
        "guard (pins): only",
        "pins' table_actions core",
        "cleanup's core",
        "(agreed with ci-speed)",
    ]
    assert [s for s in samples if not tree_internal_hits("x", s)] == []
    assert (
        tree_internal_hits(
            "x", "id 00000000-0000-4000-8000-000000000000, owner alice, color: #000;"
        )
        == []
    )
    tracked = {"README.md", "docs/setup.md", "setup.md"}
    assert untracked_md_names("x", "see notes-plan.md and docs/setup.md", tracked) == [
        "x:1: notes-plan.md"
    ]
