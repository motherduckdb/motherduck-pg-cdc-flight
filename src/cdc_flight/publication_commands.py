"""Publication's statements, checked against generation_spec: the
multi-entity publish and the log-only starts. Each function runs ONE statement inside the
caller's transaction and checks its affected-row count; none opens a transaction, so
publish and start_append keep their own transaction, leases, fault points and releases.

A count outside the spec's legal ones rolls the caller's transaction back: the caller's
lease error where another run can win (`race_zero`), InvariantViolation otherwise.

The statements the callers still write themselves (the apply_state INSERTs and
compare-and-sets, whose column lists differ between keyed and append) go through
`statement()`; the ones with one fixed form (replace, the retired relation's DELETE) live
here, with the spec's legal-state predicate."""

from __future__ import annotations

from collections.abc import Sequence

from cdc_flight.generation_commands import LeaseError, Owner, checked, count_of
from cdc_flight.generation_spec import InvariantViolation, command, states_sql

# published -> replaced: the state predicate makes a replace of anything else 0 rows,
# which the strict count turns into an InvariantViolation. Two forms: a mirror that never
# ran a backfill (start_append's) has no generations.updated_at (backfill adds it).
_REPLACED_FROM = states_sql(command("replace").from_states)
REPLACE = (
    "UPDATE {m}generations SET state=?,updated_at=current_timestamp WHERE generation_id=? "
    f"AND state IN {_REPLACED_FROM}"
)
REPLACE_UNSTAMPED = (
    f"UPDATE {{m}}generations SET state=? WHERE generation_id=? AND state IN {_REPLACED_FROM}"
)


def replace(owner: Owner, caller: str, generation_id: str, *, stamped: bool = True) -> None:
    """The mirror's previous generation, replaced, inside `caller`'s transaction (publish or
    create_log_only_append), after the apply_state compare-and-set that moved the mirror.
    `stamped=False` where the mirror's generations table has no updated_at."""
    sql = REPLACE if stamped else REPLACE_UNSTAMPED
    count = count_of(owner.execute(sql, [command("replace").to_state, generation_id]))
    checked(caller, "replace", count, InvariantViolation, "previous generation changed")


def retired_delete(owner: Owner, relation_id: int, token: int) -> None:
    """The retired relation's apply_state row, under its lease (guarded in this
    transaction): exactly one row."""
    count = count_of(
        owner.execute(
            "DELETE FROM {m}apply_state WHERE relation_id=? AND token=?", [relation_id, token]
        )
    )
    checked("publish", "retired_delete", count, InvariantViolation, "retired mirror changed")


def statement(
    owner: Owner,
    caller: str,
    name: str,
    sql: str,
    params: Sequence[object],
    lease_error: LeaseError = InvariantViolation,
) -> None:
    """One statement the caller writes (an apply_state INSERT or compare-and-set), its count
    checked against the spec's rows for (caller, name)."""
    count = count_of(owner.execute(sql, params))
    checked(caller, name, count, lease_error, f"{caller}/{name} changed underneath")
