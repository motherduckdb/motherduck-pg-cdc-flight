"""The generation commands' statements, checked against generation_spec: the
legal-state predicate rides each UPDATE, every affected-row count is checked, and an
illegal request is refused before any statement runs.

Callers keep their transactions and fault points. `held()` opens the mirror transaction and
runs the guard first; `lease()` hands out the same guard and checkpoint for `_write`, which
opens its own transaction (guard= first, commit_state= last). Guards stay plain UPDATEs that
always change `guard`, never RETURNING; counts come back with each statement, so no round
trip is added.

The caller's lease errors (apply.FenceLost, apply.LeaseBusy) are passed in, so this module
imports nothing unannotated."""

from __future__ import annotations

import logging
from collections.abc import Callable, Generator, Mapping, Sequence
from contextlib import AbstractContextManager, contextmanager
from typing import Protocol

from cdc_flight.generation_spec import (
    STATES,
    GenState,
    InvariantViolation,
    check,
    command,
    states_sql,
)

logger = logging.getLogger(__name__)


class Cursor(Protocol):
    def fetchone(self) -> tuple[object, ...] | None: ...


class Owner(Protocol):
    """Apply / Backfill: `{m}` and `{s}` prefixes resolved by execute."""

    def execute(self, statement: str, parameters: Sequence[object] | None = None) -> Cursor: ...

    def transaction(self) -> AbstractContextManager[None]: ...


LeaseError = type[Exception]

# The write fence: a plain UPDATE that always changes `guard` (never RETURNING), under this
# run's token and an unexpired lease, on an active generation.
GUARD = (
    "UPDATE {m}generations SET guard=uuid()::VARCHAR,"
    "expires_at=current_timestamp+?*INTERVAL '1 second' WHERE generation_id=? "
    "AND token=? AND expires_at>current_timestamp "
    f"AND state IN {states_sql(command('guard').from_states)}"
)


def count_of(cursor: Cursor) -> int:
    row = cursor.fetchone()
    if row is None or not isinstance(row[0], int):
        raise InvariantViolation(f"no affected-row count: {row!r}")
    return row[0]


def checked(name: str, statement: str, count: int, lease_error: LeaseError, message: str) -> None:
    """`count` against the spec's legal counts for the statement: a 0 another run can
    explain is the caller's lease error; anything else outside them is InvariantViolation."""
    rows = command(name).rows[statement]
    if count in rows.legal:
        return
    if count == 0 and rows.race_zero:
        raise lease_error(message)
    raise InvariantViolation(
        f"generation {name}/{statement}: {count} rows, legal {rows.legal} (why: "
        f"generation_spec_text.WHY[{name!r}, {statement!r}]). "
        "Nothing was committed. Inspect the generation and its claim (mirror _cdc.generations, "
        "_cdc.backfills); then abandon it with the backfill Flight (CDC_BACKFILL_ABANDON=1)"
    )


def inserted(owner: Owner, name: str, statement: str, sql: str, params: Sequence[object]) -> None:
    """A create's INSERT (claim or generation row), its count checked."""
    check(name, None)
    checked(name, statement, count_of(owner.execute(sql, params)), InvariantViolation, sql)


def acquire(
    owner: Owner,
    generation_id: str,
    run_id: str,
    lease_seconds: int,
    token: int | None,
    busy: LeaseError,
) -> int:
    """The generation lease, as Apply.acquire: with `token` (the value read with the row) one
    compare-and-set, else an UPDATE and a read of the new token in one transaction."""
    update = (
        "UPDATE {m}generations SET token=token+1,holder=?,guard=uuid()::VARCHAR,"
        "expires_at=current_timestamp+?*INTERVAL '1 second' WHERE generation_id=? "
        f"AND state IN {states_sql(command('acquire').from_states)} "
        "AND (expires_at IS NULL OR expires_at<=current_timestamp)"
    )
    params: list[object] = [run_id, lease_seconds, generation_id]
    message = "another backfill run holds this generation"
    if token is not None:
        count = count_of(owner.execute(update + " AND token=?", [*params, token]))
        checked("acquire", "acquire", count, busy, message)
        return token + 1
    with owner.transaction():
        checked("acquire", "acquire", count_of(owner.execute(update, params)), busy, message)
        row = owner.execute(
            "SELECT token FROM {m}generations WHERE generation_id=?", [generation_id]
        ).fetchone()
        if row is None or not isinstance(row[0], int):
            raise InvariantViolation(f"generation {generation_id}: no token after acquire")
        return row[0]


def release(owner: Owner, generation_id: str, token: int) -> None:
    """Best effort, unchecked (0: another run holds it now); one autocommit statement."""
    owner.execute(
        "UPDATE {m}generations SET guard=uuid()::VARCHAR,holder=NULL,expires_at=NULL "
        "WHERE generation_id=? AND token=?",
        [generation_id, token],
    )


class Held:
    """This run's lease on one generation, inside a mirror transaction."""

    def __init__(
        self,
        owner: Owner,
        generation_id: str,
        token: int,
        lease_seconds: int,
        fence_lost: LeaseError,
    ):
        self.owner, self.generation_id, self.token = owner, generation_id, token
        self.lease_seconds, self.fence_lost = lease_seconds, fence_lost

    def guard(self) -> None:
        count = count_of(
            self.owner.execute(GUARD, [self.lease_seconds, self.generation_id, self.token])
        )
        checked(
            "guard",
            "guard",
            count,
            self.fence_lost,
            "backfill lease expired, was replaced, or the generation ended",
        )

    def _advance(self, name: str, gen: Mapping[str, object], changes: Mapping[str, object]) -> None:
        """The progress compare-and-set: the observed state, chunks and checkpoint, under
        this run's token, and the command's legal states."""
        spec = command(name)
        assignments = "".join(f"{column}=?," for column in changes)
        count = count_of(
            self.owner.execute(
                f"UPDATE {{m}}generations SET {assignments}updated_at=current_timestamp "
                "WHERE generation_id=? AND token=? AND state=? AND chunks=? AND catchup_lsn=? "
                f"AND state IN {states_sql(spec.from_states)}",
                [
                    *changes.values(),
                    self.generation_id,
                    self.token,
                    gen["state"],
                    gen["chunks"],
                    gen["catchup_lsn"],
                ],
            )
        )
        checked(name, "advance", count, self.fence_lost, "generation changed underneath this run")

    def _observed(self, gen: Mapping[str, object]) -> GenState | None:
        """The observed state as a GenState (None: not one). Expiry's row has no id."""
        other = gen.get("generation_id", self.generation_id)
        if other != self.generation_id:
            raise InvariantViolation(f"{other} under {self.generation_id}'s lease")
        return next((s for s in STATES if s == gen["state"]), None)

    def progress_scan(self, gen: Mapping[str, object], **changes: object) -> None:
        check("progress_scan", self._observed(gen), tuple(changes))
        self._advance("progress_scan", gen, changes)

    def finish_scan(self, gen: Mapping[str, object], **changes: object) -> None:
        check("finish_scan", self._observed(gen), tuple(changes))
        self._advance("finish_scan", gen, {"state": command("finish_scan").to_state, **changes})

    def progress_catchup(self, gen: Mapping[str, object], **changes: object) -> None:
        check("progress_catchup", self._observed(gen), tuple(changes))
        self._advance("progress_catchup", gen, changes)

    def _delete_claim(self, relation_id: object) -> int:
        """The claim's DELETE: its count, for the caller's rule (end repairs a missing
        claim; publish doesn't)."""
        return count_of(
            self.owner.execute(
                "DELETE FROM {m}backfills WHERE relation_id=? AND generation_id=?",
                [relation_id, self.generation_id],
            )
        )

    def publish(self, gen: Mapping[str, object]) -> None:
        """publish's own generation statements, last in its transaction (the guard ran
        first): catching_up -> published under the progress compare-and-set, then the claim
        deleted, both counted strictly (publish is not a repair path)."""
        check("publish", self._observed(gen), ("state", "detail"))
        self._advance("publish", gen, {"state": command("publish").to_state, "detail": None})
        count = self._delete_claim(gen["relation_id"])
        checked("publish", "claim_delete", count, self.fence_lost, "claim changed")

    def end(
        self,
        name: str,
        gen: Mapping[str, object],
        detail: str,
        *,
        after: Callable[[], None] | None = None,
    ) -> bool:
        """fail, fail_expiry or abandon: the claim deleted and the terminal state set, in the
        caller's transaction (the guard ran first). True: the claim was already missing
        altogether; the state is committed anyway (it is the claim's target) and the caller
        reports it. A claim naming another generation is an InvariantViolation."""
        spec = check(name, self._observed(gen), ("state", "detail"))
        relation_id = gen["relation_id"]
        count = self._delete_claim(relation_id)
        repaired = False
        if count == 0 and spec.rows["claim_delete"].repair_zero:
            other = self.owner.execute(
                "SELECT generation_id FROM {m}backfills WHERE relation_id=?", [relation_id]
            ).fetchone()
            if other is None:
                repaired = True
                detail = f"{detail}; claim was already missing"
                logger.warning(
                    "generation %s (%s): its claim was already missing; ending it anyway",
                    self.generation_id,
                    name,
                )
            else:
                raise InvariantViolation(
                    f"generation {self.generation_id}: the claim on relation {relation_id} "
                    f"names generation {other[0]}. Nothing was committed. Inspect both rows "
                    "(mirror _cdc.generations, _cdc.backfills) before abandoning either"
                )
        else:
            checked(name, "claim_delete", count, self.fence_lost, "claim changed")
        to_state = spec.to_state
        if name == "fail_expiry":
            # Only token and state: expiry's row read has no progress columns.
            count = count_of(
                self.owner.execute(
                    "UPDATE {m}generations SET state=?,detail=?,updated_at=current_timestamp "
                    "WHERE generation_id=? AND token=? AND state=? "
                    f"AND state IN {states_sql(spec.from_states)}",
                    [to_state, detail, self.generation_id, self.token, gen["state"]],
                )
            )
            checked(name, "fail_cas", count, self.fence_lost, "generation changed underneath")
        else:
            self._advance(name, gen, {"state": to_state, "detail": detail})
        if after:
            after()  # a fault point: the state is written, the transaction still open
        return repaired


@contextmanager
def held(
    owner: Owner, generation_id: str, token: int, lease_seconds: int, fence_lost: LeaseError
) -> Generator[Held]:
    """A mirror transaction under this run's lease: the guard runs first."""
    with owner.transaction():
        lease_ = Held(owner, generation_id, token, lease_seconds, fence_lost)
        lease_.guard()
        yield lease_


def lease(
    owner: Owner, generation_id: str, token: int, lease_seconds: int, fence_lost: LeaseError
) -> Held:
    """The same, without a transaction: for `_write`, which opens its own and calls
    guard= first and commit_state= last."""
    return Held(owner, generation_id, token, lease_seconds, fence_lost)
