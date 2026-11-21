"""Backfill generation fencing, shared by backfill and compaction's expiry.

The generation lease (acquire, guard, release) and the stale-pin predicate moved
here from backfill.py unchanged, so compaction can fail an expired generation under
the same fence without importing the backfill (its PostgreSQL scan).
"""

from cdc_flight import generation_commands as commands
from cdc_flight.apply import LeaseBusy
from cdc_flight.config import quoted
from cdc_flight.generation_spec import ACTIVE, ENDED  # noqa: F401  (re-exported)


def stale_generation(gen, generation_id, current, old):
    """Whether a generation's pin protects nothing (shared by sweep_pins and
    compaction's expiry, so they can't drift): an ended generation; a published one
    that is no longer the mirror's (while it is, apply's ensure_pin completes an
    interrupted transfer from this pin); and a pin without a generation row that is
    older than the lease TTL (`old`, by cdc_internal's clock), so a start whose mirror
    transaction is still in flight keeps its pin."""
    if gen is None:
        return bool(old)
    if gen["state"] == "published":
        return current is None or current["generation"] != generation_id
    return gen["state"] in ENDED


def acquire_generation(owner, generation_id, run_id, *, token=None):
    """The generation lease; `token` as in Apply.acquire."""
    return commands.acquire(
        owner, generation_id, run_id, owner.config.lease_seconds, token, LeaseBusy
    )


def guard_generation(owner, generation_id, token, fence_lost):
    commands.lease(owner, generation_id, token, owner.config.lease_seconds, fence_lost).guard()


def release_generation(owner, generation_id, token):
    # One statement: autocommit is its own transaction.
    commands.release(owner, generation_id, token)


def generation_row(owner, generation_id):
    """The fields expiry decides from, or None. A mirror that never ran a backfill
    has no lease or staging columns (log-only starts): those read as NULL."""
    have = owner.mirror_catalog().get("generations", set())
    token = "token" if "token" in have else "NULL"
    staging = "staging" if "staging" in have else "NULL"
    row = owner.execute(
        f"SELECT relation_id,state,{token},{staging} FROM {{m}}generations WHERE generation_id=?",
        [generation_id],
    ).fetchone()
    return None if row is None else dict(zip(("relation_id", "state", "token", "staging"), row))


def fail_generation(owner, gen, generation_id, token, detail, fence_lost, *, fault=None):
    """Fail an active generation under its lease (compaction's expiry): the guard
    first, a compare-and-set to `failed` on its state, then its claim on the table
    and its staging table go, in one mirror transaction. Its pin is released after,
    by the caller. As Backfill.end does, without the progress columns. True: the claim
    was already missing (committed anyway; the caller reports it)."""
    schema = owner.execute(
        "SELECT schema_name FROM {s}tables WHERE relation_id=?", [gen["relation_id"]]
    ).fetchone()[0]
    with commands.held(
        owner, generation_id, token, owner.config.lease_seconds, fence_lost
    ) as lease:
        repaired = lease.end(
            "fail_expiry", gen, detail, after=fault and (lambda: fault("after_generation_failed"))
        )
        if gen["staging"]:
            owner.con.execute(
                f"DROP TABLE IF EXISTS {owner.mirror}.{quoted(schema)}.{quoted(gen['staging'])}"
            )
    return repaired
