"""Release stamps (keyless capture, D5), apart from retention_common."""

import logging

logger = logging.getLogger(__name__)


# Keyless (append) capture, decision D5: append mode may be registered only once every
# Flight that runs a keyed path refuses an append table (ingest_common.append_refusal).
# Each such Flight records the guard its code carries, on every run inside a Flight
# (never from a local CLI, whose code says nothing about the deployed Flights); the
# release that adds append mode requires each of RELEASE_STAMPED at or past it.
KEYLESS_GUARD = 1
APPEND_SNAPSHOT = 2
STAMP = {"backfill": APPEND_SNAPSHOT}
RELEASE_STAMPED = ("apply", "backfill", "table-action", "table-membership", "quarantine")
# Every other Flight command needs no stamp; why is RELEASE_UNSTAMPED in
# tests/test_keyless_guard.py (kept out of the Flight code).


class ReleaseError(ValueError):
    pass


def declare_not_deployed(execute, command, actor):
    """The operator's declaration that no Flight runs `command` for this source:
    append mode's add then doesn't wait for its stamp. Refused for an unknown command
    and for a stamped one (its Flight ran: it is deployed). A later run of the command
    in a Flight replaces the declaration (record_release). A Flight created later from
    a pre-guard release isn't detected, as with any rollback to one."""
    if command not in RELEASE_STAMPED:
        raise ReleaseError(f"command must be one of {', '.join(RELEASE_STAMPED)}")
    row = execute("SELECT keyless_guard FROM {s}releases WHERE command=?", [command]).fetchone()
    if row and row[0] is not None:
        raise ReleaseError(f"{command} is stamped by a Flight run: it is deployed")
    execute(
        "INSERT INTO {s}releases (command,keyless_guard,flight_id,recorded_at,not_deployed_by,"
        "not_deployed_at) VALUES (?,NULL,NULL,current_timestamp,?,current_timestamp) "
        "ON CONFLICT (command) DO UPDATE SET not_deployed_by=excluded.not_deployed_by,"
        "not_deployed_at=excluded.not_deployed_at,recorded_at=excluded.recorded_at",
        [command, actor],
    )


def release_status(execute):
    """{command: "stamped" | "not deployed" | "missing"} for RELEASE_STAMPED: stamped with
    keyless_guard >= KEYLESS_GUARD, declared not deployed, or neither."""
    rows = {
        command: (guard, by)
        for command, guard, by in execute(
            "SELECT command,keyless_guard,not_deployed_by FROM {s}releases"
        ).fetchall()
    }
    status = {}
    for command in RELEASE_STAMPED:
        guard, by = rows.get(command, (None, None))
        if guard is not None and guard >= KEYLESS_GUARD:
            status[command] = "stamped"
        elif guard is None and by is not None:
            status[command] = "not deployed"
        else:
            status[command] = "missing"
    return status


def release_level(execute, command):
    """`command`'s recorded keyless guard, or None."""
    row = execute("SELECT keyless_guard FROM {s}releases WHERE command=?", [command]).fetchone()
    return row[0] if row else None


def record_release(execute, command, environ):
    """Record this Flight's keyless guard (one autocommit statement), when it differs
    from the recorded one, so a rollback among guard-aware releases is recorded too. A
    rollback to a pre-guard release can't be seen. Best effort: a failure only keeps
    append mode refused longer. Returns whether a row was written."""
    flight = environ.get("MOTHERDUCK_FLIGHT_ID")
    if not flight or command not in RELEASE_STAMPED:
        return False
    try:
        row = execute(
            "INSERT INTO {s}releases AS r (command,keyless_guard,flight_id,recorded_at) "
            "VALUES (?,?,?,current_timestamp) ON CONFLICT (command) DO UPDATE SET "
            "keyless_guard=excluded.keyless_guard,flight_id=excluded.flight_id,"
            # A Flight that runs the command replaces a not-deployed declaration.
            "recorded_at=excluded.recorded_at,not_deployed_by=NULL,not_deployed_at=NULL "
            "WHERE r.keyless_guard IS DISTINCT FROM excluded.keyless_guard "
            "OR r.flight_id IS DISTINCT FROM excluded.flight_id",
            [command, STAMP.get(command, KEYLESS_GUARD), flight],
        ).fetchone()
        return bool(row and row[0])
    except Exception:
        logger.warning("could not record the release stamp; append mode stays refused")
        return False
