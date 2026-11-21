"""WAL position SQL by source role.

A primary reports its own WAL (`pg_current_wal_lsn()`, `pg_current_wal_flush_lsn()`); a
physical standby has no WAL of its own to report, so every position there is the replay
pointer (`pg_last_wal_replay_lsn()`). The role follows from the configured heartbeat
writer: preflight refuses a standby with `self` and a primary with `external`, so
`external` means a standby.

On a primary each string is byte-identical to the SQL it replaced (pinned by
tests/test_positions.py). No project imports.
"""

PRIMARY = "primary"
STANDBY = "standby"

_REPLAY = "pg_last_wal_replay_lsn()"


def role_of(heartbeat_writer: str) -> str:
    return STANDBY if heartbeat_writer == "external" else PRIMARY


def current_lsn(role: str) -> str:
    """The source's current position: its insert position, or the standby's replay."""
    return _REPLAY if role == STANDBY else "pg_current_wal_lsn()"


def receive_lsn() -> str:
    """A standby's received position (any role; unlike pg_stat_wal_receiver)."""
    return "pg_last_wal_receive_lsn()"


def flush_lsn(role: str) -> str:
    """The source's durable position: its flush position, or the standby's replay."""
    return _REPLAY if role == STANDBY else "pg_current_wal_flush_lsn()"


def current_sql(role: str) -> str:
    return f"SELECT {current_lsn(role)}-'0/0'::pg_lsn"


def flush_sql(role: str) -> str:
    return f"SELECT {flush_lsn(role)}-'0/0'::pg_lsn"


def slots_now(role: str) -> str:
    """observed_slots' position column: unchanged on a primary (NULL in recovery); the
    replay position on a standby, so the Dive's retained-WAL reads mean something."""
    if role == STANDBY:
        return f"{_REPLAY}-'0/0'::pg_lsn"
    return "CASE WHEN pg_is_in_recovery() THEN NULL ELSE pg_current_wal_lsn()-'0/0'::pg_lsn END"
