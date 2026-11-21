"""The quarantine Flight: skip the one whole unit a table is held before.

The repair action, in its own Flight: it wraps table_actions.quarantine with
action_flight's framework. The Flight declares only its inputs; the owner's settings (the
source and its databases) are baked into the stub as OWNER. A dry run reports the gap
and the literal to type; the real run must name that preview, type exactly
`QUARANTINE <schema.table> UNIT <epoch> <X/Y>..<X/Y>` (the observed blocked unit), and
match the table's epoch. The preview is spent in the same mirror transaction as the
quarantine row (its own audit) and the state change. No PostgreSQL, no secret, and no
free-text input: the quarantine row's reason is the generated gap text, never operator
input. The rows the unit touched may diverge until a resync (table-action), which the
result names as the next step.
"""

from dataclasses import dataclass, fields

from cdc_flight import action_flight, ops
from cdc_flight.action_flight import InputError, Spec
from cdc_flight.chain import FLIGHT_CONFIG
from cdc_flight.config import ApplyConfig, identifier

KEYS = FLIGHT_CONFIG["quarantine"]


def _epoch(text):
    if not text.isdigit() or not 1 <= int(text) < 2**31:
        raise InputError("CDC_EXPECTED_EPOCH must be a positive integer")
    return "expected_epoch", int(text)


def _confirm(text):
    if len(text) > 200:
        raise InputError("CDC_CONFIRM is too long")
    return "confirm", text  # compared exactly by table_actions.quarantine


QUARANTINE = Spec(
    KEYS,
    {"quarantine": ("CDC_TABLE", "CDC_EXPECTED_EPOCH", "CDC_CONFIRM")},
    {"CDC_EXPECTED_EPOCH": _epoch, "CDC_CONFIRM": _confirm},
)


@dataclass(frozen=True)
class Owner:
    """The owner's settings, baked into the stub (OWNER) by build_flight.py."""

    source_id: str
    database: str = "cdc_internal"
    mirror_database: str = ""
    ops_database: str = ops.DATABASE

    def __post_init__(self):
        for name in ("source_id", "database", "ops_database"):
            identifier(getattr(self, name))
        object.__setattr__(
            self, "mirror_database", identifier(self.mirror_database or f"{self.source_id}_mirror")
        )
        self.config()
        if self.ops_database in (self.database, self.mirror_database):
            raise ValueError("the operations database must differ from the stores")

    @classmethod
    def from_dict(cls, values):
        unknown = set(values) - {f.name for f in fields(cls)}
        if unknown:
            raise ValueError(f"unknown owner settings: {sorted(unknown)}")
        return cls(**values)

    def config(self):
        return ApplyConfig(
            self.source_id, (), database=self.database, mirror_database=self.mirror_database
        )


def main(owner, environ=None, *, fault=None):
    """Exit codes as table-action: 0 done, already or would; 2 refused; 3 busy; 1 failed.
    action_flight.perform reads the owner's Flight ids only on the trigger paths
    (resync, compact-now), which quarantine never takes. `fault`: tests only (the
    action's before/after_action_commit stages); the CLI and Flights never pass it."""
    return action_flight.main(owner, environ, QUARANTINE, fault=fault, command="quarantine")
