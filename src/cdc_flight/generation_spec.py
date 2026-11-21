"""The backfill generation's commands as data: the states each may start from
(None: no row yet), where it goes, its lease, the columns it may write, every statement's
legal affected-row counts, its prerequisites, effects, repeat result and recovery.

generation_commands and publication_commands (publication, replacement and the
log-only starts) run the statements from here; the tests hold an independent table of the
legal edges and check every write site against INVENTORY. The prose (requires, effects,
idempotent, recovery, why) is in generation_spec_text, which no Flight command imports.

No project imports: the backfill and compaction commands import this module, and the S5 audits
and the Dive's SQL copies of ACTIVE are pinned to it."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Literal

GenState = Literal["snapshotting", "catching_up", "published", "failed", "abandoned", "replaced"]

STATES: tuple[GenState, ...] = (
    "snapshotting",
    "catching_up",
    "published",
    "failed",
    "abandoned",
    "replaced",
)
# Ordered: SQL copies render them in this order (ops_apply._not_started).
ACTIVE: tuple[GenState, ...] = ("snapshotting", "catching_up")
# Generations whose pin protects nothing any more.
ENDED: tuple[GenState, ...] = ("failed", "abandoned", "replaced")

Status = Literal["spec", "deferred_s4", "out_of_scope"]
Lease = Literal["none", "take", "held", "any"]


class IllegalTransition(Exception):
    """A command asked for from a state, or with a column, the spec doesn't allow. Raised
    before any statement runs."""


class InvariantViolation(Exception):
    """A statement affected a row count its spec rules out, where no race explains it (a
    stale read of our own, a writer without the lease, or a damaged store). Raised inside
    the transaction, so every effect rolls back. Deliberately not a FenceLost or
    GenerationFailed: the handlers for those treat them as normal outcomes."""


@dataclass(frozen=True)
class Rows:
    """One statement's affected rows. `legal`: the counts that mean success. A 0 outside
    them is the caller's lease error (FenceLost or LeaseBusy) when `race_zero` (another run
    can legitimately win), and an InvariantViolation otherwise. `repair_zero`: a 0 is
    committed and reported when the claim is missing altogether (generation_commands).
    Why each is legal: generation_spec_text.WHY (no command imports it)."""

    legal: tuple[int, ...]
    race_zero: bool
    repair_zero: bool = False


@dataclass(frozen=True)
class Command:
    """A command's data. Its prose (requires, effects, idempotent, recovery) is in
    generation_spec_text.TEXT, which tests check against this and no command imports.
    `lease`: the GENERATION lease only. `leases`: (lease, how it is released) for every
    lease the command takes or uses, the table's (apply) and a retired relation's included."""

    name: str
    status: Status
    from_states: tuple[GenState | None, ...]
    to_state: GenState | None  # None: unchanged
    lease: Lease
    columns: tuple[str, ...] = ()  # what the command may assign
    cas: tuple[str, ...] = ()  # compared to the observed row
    rows: Mapping[str, Rows] = field(default_factory=dict[str, Rows])
    leases: tuple[tuple[str, str], ...] = ()


COMMANDS: Mapping[str, Command] = {
    c.name: c
    for c in (
        Command(
            "create",
            "spec",
            (None,),
            "snapshotting",
            "none",
            columns=(),
            cas=(),
            rows={"claim_insert": Rows((1,), False), "generation_insert": Rows((1,), False)},
        ),
        Command(
            "create_append",
            "spec",
            (None,),
            "snapshotting",
            "take",
            columns=(),
            cas=(),
            rows={"claim_insert": Rows((1,), False), "generation_insert": Rows((1,), False)},
        ),
        Command(
            "acquire",
            "spec",
            ACTIVE,
            None,
            "take",
            columns=("token", "holder", "guard", "expires_at"),
            cas=("state", "expires_at", "token"),
            rows={"acquire": Rows((1,), True)},
        ),
        Command(
            "guard",
            "spec",
            ACTIVE,
            None,
            "held",
            columns=("guard", "expires_at"),
            cas=("token", "expires_at", "state"),
            rows={"guard": Rows((1,), True)},
        ),
        Command(
            "release",
            "spec",
            STATES,
            None,
            "any",
            columns=("guard", "holder", "expires_at"),
            cas=("token",),
            rows={"release": Rows((0, 1), True)},
        ),
        Command(
            "progress_scan",
            "spec",
            ("snapshotting",),
            None,
            "held",
            # replay_bound: external mode only.
            columns=("chunk_key", "chunks", "rows", "scan_done", "copy_started", "replay_bound"),
            cas=("token", "state", "chunks", "catchup_lsn"),
            rows={"advance": Rows((1,), False)},
        ),
        Command(
            "finish_scan",
            "spec",
            ("snapshotting",),
            "catching_up",
            "held",
            columns=("marker", "wal_before_marker", "scan_done", "snapshot_lsn"),
            cas=("token", "state", "chunks", "catchup_lsn"),
            rows={"advance": Rows((1,), False)},
        ),
        Command(
            "progress_catchup",
            "spec",
            ("catching_up",),
            None,
            "held",
            columns=(
                "catchup_lsn",
                "columns",
                "marker_lsn",
                "deletes_ignored",
                "last_truncate_lsn",
            ),
            cas=("token", "state", "chunks", "catchup_lsn"),
            rows={"advance": Rows((1,), False)},
        ),
        Command(
            "fail",
            "spec",
            ACTIVE,
            "failed",
            "held",
            columns=("state", "detail"),
            cas=("token", "state", "chunks", "catchup_lsn"),
            rows={
                "guard": Rows((1,), True),
                "claim_delete": Rows((1,), False, repair_zero=True),
                "advance": Rows((1,), False),
            },
        ),
        Command(
            "fail_expiry",
            "spec",
            ACTIVE,
            "failed",
            "held",
            columns=("state", "detail"),
            cas=("token", "state"),
            rows={
                "guard": Rows((1,), True),
                "claim_delete": Rows((1,), False, repair_zero=True),
                "fail_cas": Rows((1,), True),
            },
        ),
        Command(
            "abandon",
            "spec",
            ACTIVE,
            "abandoned",
            "take",
            columns=("state", "detail"),
            cas=("token", "state", "chunks", "catchup_lsn"),
            rows={
                "guard": Rows((1,), True),
                "claim_delete": Rows((1,), False, repair_zero=True),
                "advance": Rows((1,), False),
            },
        ),
        # Publication and the log-only starts.
        Command(
            "publish",
            "spec",
            ("catching_up",),
            "published",
            "held",
            columns=("state", "detail"),
            cas=("token", "state", "chunks", "catchup_lsn"),
            rows={
                "guard": Rows((1,), True),
                "apply_guard": Rows((1,), True),
                "retired_guard": Rows((1,), True),
                "retired_recheck": Rows((1,), True),
                "retired_delete": Rows((1,), False),
                "apply_insert": Rows((1,), False),
                "apply_cas": Rows((1,), False),
                "replace": Rows((1,), False),
                "advance": Rows((1,), False),
                "claim_delete": Rows((1,), False),
            },
            leases=(
                ("generation", "run_table's finally"),
                ("apply", "publish's finally"),
                ("retired", "publish's finally (after a commit its row is gone)"),
            ),
        ),
        Command(
            "replace",
            "spec",
            ("published",),
            "replaced",
            "none",
            columns=("state", "updated_at"),
            rows={"replace": Rows((1,), False)},
        ),
        Command(
            "create_log_only",
            "spec",
            (None,),
            "published",
            "none",
            rows={"generation_insert": Rows((1,), False), "apply_insert": Rows((1,), False)},
            leases=(("none", "no lease: the existence check in the transaction excludes"),),
        ),
        Command(
            "create_log_only_append",
            "spec",
            (None,),
            "published",
            "none",
            rows={
                "generation_insert": Rows((1,), False),
                "apply_insert": Rows((1,), False),
                "apply_guard": Rows((1,), True),
                "replace": Rows((1,), False),
                "apply_cas": Rows((1,), False),
            },
            leases=(
                (
                    "apply",
                    "the CAS clears holder/expires_at on success; _release_lease on a refusal",
                ),
            ),
        ),
    )
}


@dataclass(frozen=True)
class Site:
    """A function that writes {m}generations or {m}backfills: the commands it implements,
    whether they run through the adapter, and how many write statements it holds."""

    commands: tuple[str, ...]
    status: Status
    writes: int


# Every write site of {m}generations and {m}backfills, as module:qualname.
# tests/test_generation_commands.py scans the source for them both ways: no unlisted site,
# no stale entry, the same number of write statements.
INVENTORY: Mapping[str, Site] = {
    "backfill:Backfill._start": Site(("create",), "spec", 2),
    "append_backfill:start": Site(("create_append",), "spec", 2),
    "generation_commands:acquire": Site(("acquire",), "spec", 1),
    "generation_commands:release": Site(("release",), "spec", 1),
    "generation_commands:GUARD": Site(("guard",), "spec", 1),
    "generation_commands:Held._advance": Site(
        ("progress_scan", "finish_scan", "progress_catchup", "fail", "abandon", "publish"),
        "spec",
        1,
    ),
    "generation_commands:Held._delete_claim": Site(
        ("fail", "fail_expiry", "abandon", "publish"), "spec", 1
    ),
    "generation_commands:Held.end": Site(("fail_expiry",), "spec", 1),
    "publication_commands:REPLACE": Site(("replace",), "spec", 1),
    "publication_commands:REPLACE_UNSTAMPED": Site(("replace",), "spec", 1),
    "apply:Apply._start_from_log": Site(("create_log_only",), "spec", 1),
    "append_apply:start_append": Site(("create_log_only_append",), "spec", 1),
}


def command(name: str) -> Command:
    return COMMANDS[name]


def check(name: str, observed: GenState | None, columns: tuple[str, ...] = ()) -> Command:
    """The command, if it may run from `observed` writing `columns`; IllegalTransition
    otherwise. Python-side, before any statement (the SQL predicate checks it again)."""
    spec = COMMANDS[name]
    if spec.status != "spec":
        raise IllegalTransition(f"{name} is not run through the adapter (deferred to S4)")
    if observed not in spec.from_states:
        raise IllegalTransition(f"{name} from {observed!r}: legal from {spec.from_states}")
    extra = [c for c in columns if c not in spec.columns]
    if extra:
        raise IllegalTransition(f"{name} may not write {extra}")
    return spec


def states_sql(states: tuple[GenState | None, ...]) -> str:
    """`('a','b')` for a SQL IN list, in the spec's order (None, no row, is not a state)."""
    return "(" + ",".join(f"'{s}'" for s in states if s is not None) + ")"
