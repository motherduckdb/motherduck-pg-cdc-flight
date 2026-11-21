"""The source, epoch and retention-pin commands as data: the states
each may start from, where it goes, and every statement's legal affected-row counts.

lifecycle_commands checks the statements against this; lifecycle_spec_text holds the prose
(requires, effects, idempotent, recovery, why) and the inventories, and no Flight command
imports it. The shape is generation_spec's, with three more count kinds:
`exact` (the caller passes the number of rows the statement targets), `at_least` (one or
more), and `any_count` (every count is legal: a best effort; its reason is in the text).

Its only project import is generation_spec (data, no imports of its own): every command
that writes the source or a pin imports both."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Literal

# One pair of exception types for every spec (generation_spec has no project imports):
# the handlers that isolate an InvariantViolation (expiry, compaction) catch both machines'.
from cdc_flight.generation_spec import IllegalTransition as IllegalTransition
from cdc_flight.generation_spec import InvariantViolation as InvariantViolation

# {s}source.lifecycle: NULL is "registered"; None (here) is no source row at all.
SourceLifecycle = Literal["registered", "deleting", "deleted"]
LIFECYCLES: tuple[SourceLifecycle, ...] = ("registered", "deleting", "deleted")

Status = Literal["spec", "out_of_scope"]


@dataclass(frozen=True)
class Rows:
    """One statement's affected rows. `legal`: the counts that mean success. A 0 outside
    them is the caller's lease error when `race_zero`, InvariantViolation otherwise.
    `exact`: legal is exactly the count the caller passes. `at_least`: legal is
    legal[0] or more. `any_count`: every count is legal (stated, not tolerated)."""

    legal: tuple[int, ...] = (1,)
    race_zero: bool = False
    exact: bool = False
    at_least: bool = False
    any_count: bool = False


@dataclass(frozen=True)
class Command:
    name: str
    status: Status
    from_states: tuple[SourceLifecycle | None, ...]
    to_state: SourceLifecycle | None  # None: the lifecycle unchanged
    rows: Mapping[str, Rows] = field(default_factory=dict[str, Rows])
    # (lease, how it is released) for every lease the command takes or uses.
    leases: tuple[tuple[str, str], ...] = ()


_ANY = Rows(any_count=True)
_GUARD = Rows((1,), True)  # the source lease's guard: FenceLost on 0
_REGISTERED: tuple[SourceLifecycle | None, ...] = ("registered",)

COMMANDS: Mapping[str, Command] = {
    c.name: c
    for c in (
        Command(
            "register",
            "spec",
            (None,),
            "registered",
            rows={
                "source_insert": Rows(),
                "initial_epoch_insert": Rows(),
                "tables_insert": Rows(exact=True),
            },
            leases=(("none", "the in-transaction re-read of the source row excludes"),),
        ),
        Command(
            "record_created",
            "spec",
            _REGISTERED,
            None,
            rows={"created_update": Rows()},
            leases=(("none", "the in-transaction re-read; no code deletes the source row"),),
        ),
        Command(
            "delete_start",
            "spec",
            _REGISTERED,
            "deleting",
            rows={"guard": _GUARD, "lifecycle_update": Rows()},
            leases=(("source", "delete_source's leased() finally"),),
        ),
        Command(
            "delete_finish",
            "spec",
            ("deleting",),
            "deleted",
            rows={"guard": _GUARD, "lifecycle_update": Rows()},
            leases=(("source", "delete_source's leased() finally"),),
        ),
        Command(
            "begin_epoch",
            "spec",
            _REGISTERED,
            None,
            rows={
                "guard": _GUARD,
                "epoch_insert": Rows(),
                "source_update": Rows(),
                "protocol_update": Rows(),
                "partitions_seal": _ANY,
                "key_changes_insert": _ANY,
                "tables_update": Rows(exact=True),
            },
            leases=(("source", "its caller (epoch_flight / ingest recovery) releases"),),
        ),
        Command(
            "pin_transfer",
            "spec",
            _REGISTERED,
            None,
            rows={
                "put": Rows(),
                "pin_state_update": Rows(),
                "pins_delete": Rows((1,), at_least=True),
                "pins_delete_owned": _ANY,
            },
            leases=(("retention guard", "a guard row write, not a lease: nothing to release"),),
        ),
        Command(
            "pin_expire",
            "spec",
            _REGISTERED,
            None,
            rows={"pin_delete": Rows((1,), True), "pin_state_update": Rows()},
            leases=(("retention guard", "a guard row write, not a lease: nothing to release"),),
        ),
        Command(
            "pin_advance",
            "spec",
            _REGISTERED,
            None,
            rows={"pin_update": Rows((0, 1))},
        ),
        Command("pin_release", "spec", _REGISTERED, None, rows={"pin_delete": _ANY}),
    )
}


def command(name: str) -> Command:
    return COMMANDS[name]


def lifecycle(value: object) -> SourceLifecycle | None:
    """The stored {s}source.lifecycle as a SourceLifecycle (NULL: registered)."""
    if value is None:
        return "registered"
    return next((s for s in LIFECYCLES if s == value), None)


def check(name: str, observed: SourceLifecycle | None) -> Command:
    """The command, if it may run from `observed`; IllegalTransition otherwise."""
    spec = COMMANDS[name]
    if observed not in spec.from_states:
        raise IllegalTransition(f"{name} from {observed!r}: legal from {spec.from_states}")
    return spec


def lifecycle_sql(states: tuple[SourceLifecycle | None, ...]) -> str:
    """The SQL predicate on {s}source.lifecycle for `states` (registered: IS NULL)."""
    parts = ["lifecycle IS NULL" if s == "registered" else f"lifecycle='{s}'" for s in states if s]
    return "(" + " OR ".join(parts) + ")"
