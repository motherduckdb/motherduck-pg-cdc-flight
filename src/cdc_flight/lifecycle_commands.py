"""The source, epoch and pin statements, checked against lifecycle_spec.

As publication_commands: one statement per call, inside the caller's transaction, its
affected-row count checked; none opens a transaction, so register, delete_source,
begin_epoch and the retention functions keep their transactions, guards and fault points.
A count outside the spec rolls the caller's transaction back: the caller's lease error
where another run can win (`race_zero`), InvariantViolation otherwise."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol

from cdc_flight.lifecycle_spec import InvariantViolation, command

# Its own small protocol, a copy of generation_commands.Owner/Cursor/count_of kept here on
# purpose: ingest, preflight, epoch and delete-source import this module but not the
# generation adapter.


class Cursor(Protocol):
    def fetchone(self) -> tuple[object, ...] | None: ...


class Owner(Protocol):
    """A change log or retention store: `{s}` resolved by execute."""

    def execute(self, statement: str, parameters: Sequence[object] | None = None) -> Cursor: ...


LeaseError = type[Exception]


def count_of(cursor: Cursor) -> int:
    row = cursor.fetchone()
    if row is None or not isinstance(row[0], int):
        raise InvariantViolation(f"no affected-row count: {row!r}")
    return row[0]


def checked(
    caller: str,
    name: str,
    count: int,
    *,
    expected: int | None = None,
    lease_error: LeaseError = InvariantViolation,
    message: str = "",
) -> int:
    """`count` against the spec's rows for (caller, name); returns it."""
    rows = command(caller).rows[name]
    if rows.any_count:
        return count
    if rows.exact:
        if expected is None:
            raise ValueError(f"{caller}/{name} needs the expected count")
        legal: tuple[int, ...] = (expected,)
    else:
        legal = rows.legal
    if count in legal or (rows.at_least and count >= legal[0]):
        return count
    if count == 0 and rows.race_zero:
        raise lease_error(message or f"{caller}/{name}: 0 rows")
    raise InvariantViolation(
        f"source {caller}/{name}: {count} rows, legal {legal}"
        f"{' or more' if rows.at_least else ''} (why: "
        f"lifecycle_spec_text.WHY[{caller!r}, {name!r}]). Nothing was committed. Inspect the "
        "source registry ({s}source, {s}tables, {s}retention_pins) before retrying"
    )


def statement(
    owner: Owner,
    caller: str,
    name: str,
    sql: str,
    params: Sequence[object] = (),
    *,
    expected: int | None = None,
    lease_error: LeaseError = InvariantViolation,
) -> int:
    """One statement of a spec'd command, its count checked; returns the count."""
    return checked(
        caller,
        name,
        count_of(owner.execute(sql, params)),
        expected=expected,
        lease_error=lease_error,
    )
