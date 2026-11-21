"""Attaching another MotherDuck database from a Flight's connection, safely.

DuckDB falls back to MotherDuck's browser login when `md:` is opened without a
token, so a database is attached from MotherDuck only when the connection's own
store is a MotherDuck database and a token is set. A local store's other databases
are attached by whoever opened it (tests, local CLI use).
"""

import os

import duckdb

from cdc_flight.config import quoted


class LocalStore(RuntimeError):
    """The store is local and the database isn't attached: never fetched from md:."""


def attached(con, name):
    return bool(
        con.execute(
            "SELECT count(*) FROM duckdb_databases() WHERE database_name=?", [name]
        ).fetchone()[0]
    )


def attach_motherduck(con, store_database, name):
    """Attach `name` if needed. True when attached; False only when MotherDuck
    confirms it doesn't exist. Raises LocalStore for a local store, RuntimeError
    without a token (before any md: statement), and any other failure as is."""
    if attached(con, name):
        return True
    store = con.execute(
        "SELECT type FROM duckdb_databases() WHERE database_name=?", [store_database]
    ).fetchone()
    if store is None or store[0] != "motherduck":
        raise LocalStore(f"database {name} is not attached to this local store")
    if not (os.getenv("motherduck_token") or os.getenv("MOTHERDUCK_TOKEN")):
        raise RuntimeError(f"MOTHERDUCK_TOKEN is not set: refusing to attach md:{name}")
    try:
        con.execute(f"ATTACH IF NOT EXISTS 'md:{name}' AS {quoted(name)}")
        return True
    except duckdb.Error:
        try:
            exists = con.execute(
                "SELECT count(*) FROM md_all_databases() WHERE alias=?", [name]
            ).fetchone()[0]
        except duckdb.Error:
            raise  # can't tell: fail rather than skip
        if exists:
            raise
        return False
