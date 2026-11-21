"""positions.py: the role-aware WAL position SQL.

Primary mode is unchanged byte for byte: each primary string equals the literal it
replaced at the eight call sites, and no position function is spelled outside the
helper any more.
"""

import pathlib
import re

from cdc_flight import positions
from cdc_flight.positions import PRIMARY, STANDBY

SRC = pathlib.Path(__file__).resolve().parents[1] / "src" / "cdc_flight"

# The literal SQL at the eight sites before replica capture.
BEFORE_CURRENT = "SELECT pg_current_wal_lsn()-'0/0'::pg_lsn"  # ingest:86, backfill, append
BEFORE_FLUSH = "SELECT pg_current_wal_flush_lsn()-'0/0'::pg_lsn"  # epoch x3
BEFORE_SLOTS = "CASE WHEN pg_is_in_recovery() THEN NULL ELSE pg_current_wal_lsn()-'0/0'::pg_lsn END"
BEFORE_RETAINED = "pg_current_wal_lsn()-restart_lsn"  # delete_source


def test_primary_sql_is_byte_identical_to_the_replaced_literals():
    assert positions.current_sql(PRIMARY) == BEFORE_CURRENT
    assert positions.flush_sql(PRIMARY) == BEFORE_FLUSH
    assert positions.slots_now(PRIMARY) == BEFORE_SLOTS
    assert f"{positions.current_lsn(PRIMARY)}-restart_lsn" == BEFORE_RETAINED


def test_a_standby_reads_the_replay_pointer_everywhere():
    replay = "pg_last_wal_replay_lsn()"
    assert positions.current_sql(STANDBY) == f"SELECT {replay}-'0/0'::pg_lsn"
    assert positions.flush_sql(STANDBY) == f"SELECT {replay}-'0/0'::pg_lsn"
    assert positions.slots_now(STANDBY) == f"{replay}-'0/0'::pg_lsn"
    assert positions.current_lsn(STANDBY) == replay


def test_the_role_follows_the_heartbeat_writer():
    assert positions.role_of("self") == PRIMARY
    assert positions.role_of("external") == STANDBY


def test_no_position_function_is_spelled_outside_the_helper():
    """The eight sites go through positions.py: a new raw read would bypass the role."""
    raw = re.compile(r"pg_current_wal_(flush_)?lsn\(|pg_last_wal_replay_lsn\(")
    found = {
        path.name
        for path in SRC.glob("*.py")
        if path.name != "positions.py" and raw.search(path.read_text())
    }
    assert found == set(), found


def test_the_eight_sites_call_the_helper():
    calls = {
        "ingest.py": 4,  # catalog_keys, observed_slots (each role), external mode's B
        "backfill.py": 1,  # B on a primary
        # A standby chunk's replay before and after; the append import session's B.
        "standby_backfill.py": 3,
        "append_backfill.py": 1,
        "delete_source.py": 1,
        "epoch.py": 3,
    }
    helper = re.compile(r"\b(current_sql|flush_sql|slots_now|current_lsn)\(")
    for name, count in calls.items():
        assert len(helper.findall((SRC / name).read_text())) == count, name
