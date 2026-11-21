"""Derived status for the operations Dive (Phase 3): the `cdc_ops` database.

Each Flight writes its status once per run from the CLI's `finally`, after its
correctness transactions: best effort (a failure is logged as its type only and
never changes the run's result), bounded in time, in a few MotherDuck statements.
`cdc_ops` is lossy and eventually consistent: never act on it alone.

- One writer per table; every write touches only its source's rows.
- Newer wins: each folded part of a row carries the server time its data was read
  (`<part>_as_of`, epoch microseconds) and is replaced only by a newer read; a part a
  run didn't read keeps its value. Busy runs and failures without a lease write only
  `last_busy_*` / `last_failure_*`.
- Snapshots of authoritative stores are read inside the write (`INSERT … SELECT`).
- No user data: LSNs, names, states, exception types and check names only.
- Never `md:` without a MotherDuck store and a token (attach.py).

List parts are JSON text whose items carry `v`; keys are only ever added. The Dive
unnests them in its own queries. The writers live in ops_<flight>.py, so each Flight
command imports only its own writer; ops_schema shares their DDL for first-report bootstrap.
"""

import datetime
import decimal
import json
import logging
import re
import threading
import time

import duckdb

from cdc_flight.attach import LocalStore, attach_motherduck
from cdc_flight.config import quoted
from cdc_flight.ops_schema import AUDIT as AUDIT
from cdc_flight.ops_schema import TABLES_V2
from cdc_flight.ops_schema import extended as extended

logger = logging.getLogger(__name__)

DATABASE = "cdc_ops"
MARGIN, WATCHDOG = 15, 5  # skip with less budget left; interrupt after (seconds)


class Sql:
    """A column value computed in the statement (server time, a read of a store)."""

    def __init__(self, expression):
        self.expression = expression


NOW = Sql("current_timestamp")
# PostgreSQL's commit timestamps (pgoutput, frames' commit_ts_us) count microseconds from
# 2000-01-01 UTC; apply.PG_EPOCH_US and sql_fold.PG_EPOCH_US are the same constant.
PG_EPOCH_US = 946_684_800_000_000


def commit_time_sql(us):
    """TIMESTAMPTZ SQL for a PostgreSQL-epoch microsecond count: an int (inlined, so no
    client time types are bound) or an SQL expression. Exact to the microsecond."""
    if isinstance(us, bool) or not isinstance(us, (int, str)):
        raise TypeError("commit time must be microseconds (int) or an SQL expression")
    return f"(to_timestamp(0)+to_microseconds(({us})+{PG_EPOCH_US}))"


def _plain(value):
    """What json can't encode on its own: values read from PostgreSQL or DuckDB."""
    if isinstance(value, decimal.Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    if isinstance(value, (datetime.datetime, datetime.date)):
        return value.isoformat()
    raise TypeError(f"{type(value).__name__} is not JSON serializable")


def to_json(items):
    return json.dumps([dict(v=1, **i) for i in items], separators=(",", ":"), default=_plain)


def prune_window():
    """Stateless: runs that start in 00:00-01:00 UTC prune history (idempotent). By the
    client clock: a skewed clock only moves which runs prune."""
    return time.gmtime().tm_hour == 0


def status_write_failed(exc):
    """The CLI's last line of defence around a status write: an error raised while
    preparing it (for example a bad CDC_OPS_DATABASE) is logged as its type only."""
    logger.warning("ops status write failed (%s)", type(exc).__name__)


class Ops:
    def __init__(self, con, store, source_id, database=DATABASE):
        self.con, self.store, self.source_id, self.database = con, store, source_id, database
        self.prefix = f"{quoted(database)}.main."
        self.outcome = self.error = None
        self.columns = {}

    def execute(self, statement, parameters=None):
        return self.con.execute(statement.replace("{o}", self.prefix), parameters or [])

    def _attach(self):
        """'attached', 'local' (a local store without it: local use has no Dive) or
        'missing' (MotherDuck says it doesn't exist: the deploy step creates it, so a run
        never pays an unbounded CREATE DATABASE outside the watchdog)."""
        try:
            return (
                "attached" if attach_motherduck(self.con, self.store, self.database) else "missing"
            )
        except LocalStore:
            return "local"

    def _schema(self, tables, *, bootstrap=False):
        """Create or extend `tables` ({name: "col TYPE,..."}) from the client-side
        catalog. Bootstrap creates missing tables for every writer, but only
        `tables` receive additive column healing. No rows are written here."""
        have = {}
        for table, column in self.con.execute(
            "SELECT table_name,column_name FROM duckdb_columns() "
            "WHERE database_name=? AND schema_name='main'",
            [self.database],
        ).fetchall():
            have.setdefault(table, set()).add(column)
        self.columns = have  # what this report's statements may read (before any ALTER)
        wanted = {**TABLES_V2, **tables} if bootstrap else tables
        for table, ddl in wanted.items():
            body = re.sub(r",PRIMARY KEY\([^)]*\)", "", ddl)
            columns = [c.split(" ", 1) for c in body.split(",")]
            if table not in have:
                statements = [f"CREATE TABLE IF NOT EXISTS {{o}}{table} ({ddl})"]
            elif table in tables:
                statements = [
                    f"ALTER TABLE {{o}}{table} ADD COLUMN IF NOT EXISTS {c} {t.split(' ')[0]}"
                    for c, t in columns
                    if c not in have[table]
                ]
            else:
                continue  # Bootstrap never ALTERs another writer’s existing table.
            for statement in statements:
                try:
                    self.execute(statement)
                except duckdb.TransactionException:
                    pass  # a concurrent first write created it; heals next run

    def report(self, tables, write, remaining=None):
        """`write()` after attaching and creating `tables`; never raises. Skipped
        with under MARGIN seconds of budget left; interrupted after WATCHDOG.
        Writers build their values inside `write`, so nothing escapes this guard."""
        if remaining is not None and remaining < MARGIN:
            self.outcome = "skipped:time"
            return self.outcome
        timer = threading.Timer(WATCHDOG, self.con.interrupt)
        timer.daemon = True
        timer.start()
        try:
            attached = self._attach()
            if attached != "attached":
                self.outcome = "skipped:local" if attached == "local" else "skipped:no_database"
                return self.outcome
            self._schema(tables, bootstrap=True)
            write()
            self.outcome = "written"
        except Exception as exc:  # best effort: the run's result stands
            logger.warning("ops status write failed (%s)", type(exc).__name__)
            self.error = exc  # for tests; never logged (messages may carry names)
            self.outcome = f"failed:{type(exc).__name__}"
        finally:
            timer.cancel()
        return self.outcome

    def upsert(self, table, values, parts, markers=()):
        """One INSERT … ON CONFLICT(source_id) DO UPDATE. `values`: column -> value or
        Sql. Each part (as_of column -> its columns) is replaced only by a newer read;
        a NULL new as_of keeps it. `markers`: (column, columns) replaced when set."""
        select = [v.expression if isinstance(v, Sql) else "?" for v in values.values()]
        params = [v for v in values.values() if not isinstance(v, Sql)]
        sets = ["written_at=excluded.written_at"]
        for as_of, members in parts.items():
            newer = (
                f"excluded.{as_of} IS NOT NULL AND ({table}.{as_of} IS NULL "
                f"OR excluded.{as_of}>{table}.{as_of})"
            )
            sets += [
                f"{c}=CASE WHEN {newer} THEN excluded.{c} ELSE {table}.{c} END"
                for c in (as_of, *members)
            ]
        for marker, members in markers:
            sets += [
                f"{c}=CASE WHEN excluded.{marker} IS NOT NULL THEN excluded.{c} ELSE {table}.{c} END"
                for c in members
            ]
        self.execute(
            f"INSERT INTO {{o}}{table} ({','.join(values)},written_at) SELECT "
            f"{','.join(select)},current_timestamp ON CONFLICT(source_id) DO UPDATE SET "
            + ",".join(sets),
            params,
        )


def write_audit(
    con,
    store,
    source_id,
    run_id,
    action,
    inputs,
    dry_run,
    result,
    database=DATABASE,
    started_by=None,
    tls=None,
    request_id=None,
):
    """The action Flights' audit copy: best effort, never raises; returns whether
    it was written. The authoritative record is in the mutated database."""
    ops = Ops(con, store, source_id, database)

    def write():
        ops.execute(
            "INSERT INTO {o}audit (source_id,run_id,written_at,token_user,action,inputs,"
            "dry_run,result,started_by,tls_mode,tls_ca,tls_in_use,request_id) "
            "VALUES (?,?,current_timestamp,current_user,?,?,?,?,?,?,?,?,?)",
            [
                source_id,
                run_id,
                action,
                json.dumps(inputs, sort_keys=True),
                dry_run,
                result,
                started_by,
            ]
            + [(tls or {}).get(c) for c in ("tls_mode", "tls_ca", "tls_in_use")]
            + [request_id],
        )

    return ops.report(AUDIT, write) == "written"
