"""Generation-based backfill (DESIGN Protocol 2).

A generation snapshots one table into a fresh staging table in the mirror
database, catches up through an end marker observed in the change log, and is
published atomically with its apply checkpoint. It never merges into an existing
mirror, so a resync drops rows the source deleted while they were not observed.

State on the `generations` row: snapshotting -> catching_up -> published, or
failed / abandoned, and replaced once a later generation publishes. Every
mutation is fenced by the generation's lease and a compare-and-set on its
progress. Publication additionally takes the table's apply lease.

Retention (retention.py): the generation's pin at L is registered in cdc_internal
before the mirror start commits, checked before every chunk and catch-up batch,
transferred to the table pin after publication commits, and released after a
failure or abandonment commits. A crash between the two databases' commits leaves
extra history, never too little.
"""

import json
import logging
import tempfile
import time
import uuid
from dataclasses import dataclass
from pathlib import Path

import duckdb
import psycopg2
import pyarrow as pa
import pyarrow.parquet as pq
from psycopg2 import sql

from cdc_flight import generation_commands as commands
from cdc_flight import generations, sql_fold, sql_path
from cdc_flight import publication_commands as pub
from cdc_flight.apply import (
    BY_NAME,
    DATA_ERRORS,
    Apply,
    ApplyError,
    FenceLost,
    LeaseBusy,
    column_json,
    create_mirror_table,
    source_from,
)
from cdc_flight.config import quoted, table_name
from cdc_flight.format2 import TEXT
from cdc_flight.generation_spec import STATES
from cdc_flight.generation_spec import check as check_command
from cdc_flight.generations import ACTIVE, stale_generation
from cdc_flight.ingest_common import SourceMismatch
from cdc_flight.mirror import RESERVED_PREFIX, Blocked, Column, cast_sql, duck_type, type_kept
from cdc_flight.pg_connect import catalog_key, check_formats, connect, query
from cdc_flight.positions import PRIMARY, current_sql
from cdc_flight.retention import (
    HistoryDropped,
    PinMissing,
    TableHeld,
    generation_owner,
    is_conflict,
)
from cdc_flight.sql_path import PageFacts, SqlBatch, SqlImages, SqlWrite, decide, versions_of

logger = logging.getLogger(__name__)

# The mirror table (apply_state row) of a relation that add-table retired for the
# given relation: (relation_id, token). Server state, read in the publish transaction.
RETIRED_HOLDER = (
    "SELECT a.relation_id,a.token FROM {m}apply_state a WHERE a.schema_name=? "
    "AND a.table_name=? AND a.relation_id IN (SELECT relation_id FROM {s}tables "
    "WHERE held_reason='removed' AND retired_by=?)"
)

GENERATION_COLUMNS = {
    "staging": "VARCHAR",
    "key_columns": "VARCHAR",
    "columns": "VARCHAR",
    "chunk_key": "VARCHAR",
    "chunks": "BIGINT",
    "rows": "BIGINT",
    "scan_done": "BOOLEAN",
    "marker": "VARCHAR",
    "wal_before_marker": "UBIGINT",
    "catchup_lsn": "UBIGINT",
    "key_version": "INTEGER",
    "detail": "VARCHAR",
    "token": "BIGINT",
    "holder": "VARCHAR",
    "expires_at": "TIMESTAMPTZ",
    "guard": "VARCHAR",
    "updated_at": "TIMESTAMPTZ",
    # Append (keyless) snapshot generations only (append_backfill): the exported
    # snapshot's consistent point C; set once the copy has begun (a later run then fails
    # an unfinished copy: its snapshot died with its connection); the catch-up's skipped
    # DELETEs and last TRUNCATE, carried to apply_state at publication.
    "snapshot_lsn": "UBIGINT",
    "copy_started": "BOOLEAN",
    "deletes_ignored": "UBIGINT",
    "last_truncate_lsn": "UBIGINT",
    # Replica capture: the source's recorded heartbeat writer when the
    # generation began (NULL = self), so config can't reinterpret a running generation;
    # and, external mode only, the highest standby replay position read after a chunk's
    # snapshot, persisted with that chunk (B once the scan is done).
    "finality_mode": "VARCHAR",
    "replay_bound": "UBIGINT",
}


def standby_backfill():
    """The standby backfill module, imported lazily: only an external source's backfill
    (CDC_HEARTBEAT_WRITER=external) uses it."""
    from cdc_flight import standby_backfill as module

    return module


class GenerationFailed(Exception):
    def __init__(self, reason):
        super().__init__(reason)
        self.reason = reason


@dataclass(frozen=True)
class BackfillResult:
    table: str
    generation: str
    status: str
    chunks: int = 0
    rows: int = 0
    boundary: int = 0
    detail: str = ""


MARKER_COLUMN = "marker"


# Q6 (backfill SQL fold design): a MotherDuck-side OOM folding a catch-up page fails the
# generation, so later runs don't retry the same page; units are never split, so a
# smaller page can't help. The runbook is docs/operations.md, "Backfill catch-up".
CATCHUP_OOM = (
    "OutOfMemoryException in catch-up (SQL fold) units {}..{}: MotherDuck ran out of memory "
    "folding this page; resync on a larger MotherDuck instance, or when the source's write "
    "load is lower"
)


class Backfill(SqlWrite, Apply):
    def __init__(self, con, config, pg=None, *, retention=None, fold="sql"):
        if fold not in ("sql", "python"):
            raise ValueError("fold must be 'sql' or 'python'")
        super().__init__(con, config, retention=retention)
        # Catch-up's fold, as ApplyRun's: "python" (constructor only; tests use it as
        # the oracle) forces the Python fold. fold_stats counts each catch-up batch as
        # "sql" or "fallback:<reason>", with apply's keys; "redo:<error>" counts the SQL
        # batches redone on the Python fold. Never persisted per batch.
        self.fold = fold
        self.fold_stats = {}
        # generation_id -> a unit end below which the marker search found no match.
        # Complete units are immutable, so a polling search resumes there.
        self._searched = {}
        self._pg = pg
        self._snapshot_pg = None
        # External mode: this run's recovery-conflict retries and current chunk limit.
        self._conflicts = 0
        self._chunk_rows = config.chunk_rows

    # Setup ------------------------------------------------------------------

    # Apply.initialize creates these too, catalog-checked, in the same transaction.
    MIRROR_TABLES = {
        **Apply.MIRROR_TABLES,
        # At most one active generation per table: the start transaction claims it.
        "backfills": "CREATE TABLE IF NOT EXISTS {m}backfills "
        "(relation_id UINTEGER PRIMARY KEY, generation_id VARCHAR NOT NULL)",
    }
    ADDED_COLUMNS = {**Apply.ADDED_COLUMNS, "generations": tuple(GENERATION_COLUMNS.items())}

    @property
    def pg(self):
        if self._pg is None:
            self._pg = connect(self.config)
            check_formats(lambda name: query(self._pg, "SHOW " + name)[0][0])
        return self._pg

    @property
    def snapshot_pg(self):
        """A separate connection for chunk reads, which need a transaction for a
        server-side cursor and their own statement timeout."""
        if self._snapshot_pg is None:
            self._snapshot_pg = connect(self.config)
            check_formats(lambda name: query(self._snapshot_pg, "SHOW " + name)[0][0])
            self._snapshot_pg.autocommit = False
        return self._snapshot_pg

    def close(self):
        for attribute in ("_pg", "_snapshot_pg"):
            con = getattr(self, attribute)
            if con is not None:
                con.close()
                setattr(self, attribute, None)

    def catalog(self, relation_id, table):
        """Current published columns (pgoutput's set: not dropped, not generated)."""
        schema, name = table_name(table)
        found = query(
            self.pg,
            "SELECT n.nspname,c.relname FROM pg_class c JOIN pg_namespace n "
            "ON n.oid=c.relnamespace WHERE c.oid=%s",
            [relation_id],
        )
        if found != [(schema, name)]:
            raise ApplyError(f"{table} no longer matches its registered relation")
        rows = query(
            self.pg,
            "SELECT attname,atttypid,atttypmod,format_type(atttypid,atttypmod) FROM pg_attribute "
            "WHERE attrelid=%s AND attnum>0 AND NOT attisdropped AND attgenerated='' "
            "ORDER BY attnum",
            [relation_id],
        )
        columns = [Column(n, duck_type(oid, mod), oid, mod) for n, oid, mod, _ in rows]
        return columns, {n: fmt for n, _, _, fmt in rows}

    def verify_source(self, table, gen):
        """Fail the generation if the source no longer matches what it builds.

        Runs before every chunk read and once the end marker is durable. Columns
        added after the start are fine (catch-up adds them through the log); a
        dropped, renamed or retyped column, or a changed key, is not.
        """
        try:
            columns, types = self.catalog(gen["relation_id"], table)
        except ApplyError as exc:
            raise GenerationFailed(str(exc)) from exc
        current = {c.name: c for c in columns}
        for column in gen["columns"]:
            other = current.get(column.name)
            if other is None or other.dtype != column.dtype or not type_kept(column, other):
                raise GenerationFailed(
                    f"source schema changed during backfill: column {column.name!r}"
                )
        key = catalog_key(self.pg, gen["relation_id"])
        if key is None or tuple(key) != tuple(gen["key_columns"]):
            raise GenerationFailed("source replica identity key changed during backfill")
        return types

    # Generation state -------------------------------------------------------

    def start_position(self):
        """source_position plus the source's recorded heartbeat writer (replica capture:
        the generation's finality mode), in its one statement. The recorded row, not
        config, decides; a store from before the column reads `self` (to_json)."""
        row = self.execute(
            "SELECT epoch,confirmed_lsn,lifecycle,"
            "coalesce(to_json(src)->>'heartbeat_writer','self') FROM {s}source src WHERE id=1"
        ).fetchone()
        epoch, low = source_from(
            row and dict(epoch=row[0], confirmed=row[1], lifecycle=row[2]),
            self.config.epoch,
            self.config.source_id,
        )
        return epoch, low, row[3]

    def check_writer(self, table, recorded):
        """CDC_HEARTBEAT_WRITER must equal the recorded mode (as ingest's check_mode)."""
        if self.config.heartbeat_writer != recorded:
            raise ApplyError(
                f"{table}: CDC_HEARTBEAT_WRITER={self.config.heartbeat_writer} but the source "
                f"is registered as {recorded}; fix the backfill Flight's config"
            )

    def generation(self, generation_id):
        names = ["generation_id", "relation_id", "mode", "epoch", "pin_lsn", "marker_lsn", "state"]
        # TIMESTAMPTZ lease columns stay in SQL; fetching them would need pytz.
        names += [c for c in GENERATION_COLUMNS if c not in ("expires_at", "updated_at")]
        row = self.execute(
            f"SELECT {','.join(names)} FROM {{m}}generations WHERE generation_id=?",
            [generation_id],
        ).fetchone()
        if not row:
            return None
        gen = dict(zip(names, row))
        # Log-only generations (start-from-log) record no columns or keys.
        if gen["columns"] is not None:
            gen["columns"] = [Column(*c) for c in json.loads(gen["columns"])]
        if gen["key_columns"] is not None:
            gen["key_columns"] = tuple(json.loads(gen["key_columns"]))
        return gen

    def active(self, relation_id):
        row = self.execute(
            "SELECT generation_id FROM {m}backfills WHERE relation_id=?", [relation_id]
        ).fetchone()
        return self.generation(row[0]) if row else None

    def staging_current(self, gen, schema):
        return dict(
            target=f"{self.mirror}.{quoted(schema)}.{quoted(gen['staging'])}",
            columns=gen["columns"],
            keys=gen["key_columns"],
            schema=schema,
            epoch=gen["epoch"],
        )

    def start(self, table, *, fault=None, deadline=None):
        """Register a generation at the durable ingest boundary L: its retention pin
        first (cdc_internal), then the generation and its empty staging table in one
        mirror transaction."""
        relation_id, _ = self.registered(table)
        held = self.execute(
            "SELECT held_reason,held_detail FROM {s}tables WHERE relation_id=?", [relation_id]
        ).fetchone()
        if held and held[0]:
            raise ApplyError(f"{table}: source table {held[0]} ({held[1]}); backfill refused")
        epoch, low, writer = self.start_position()
        self.check_writer(table, writer)
        schema, name = table_name(table)
        columns, _ = self.catalog(relation_id, table)
        # The generation is keyed on the source's current key, which may differ
        # from the key registered at preflight (a resync after a key change). Every
        # change in [L, H) must be under this key: ingest must have recorded it,
        # and L must follow the change.
        keys = catalog_key(self.pg, relation_id)
        if not keys:
            raise ApplyError(f"{table} has no primary key or replica identity index")
        keys = tuple(keys)
        registered, tracked, version = self.execute(
            "SELECT key_columns,catalog_key,coalesce(key_version,0) FROM {s}tables "
            "WHERE relation_id=?",
            [relation_id],
        ).fetchone()
        if tuple(json.loads(tracked or registered) or ()) != keys:
            raise ApplyError(
                f"{table}: source key differs from the key ingest last recorded; "
                "run ingest, then retry"
            )
        new_from = self.execute(
            "SELECT epoch,new_from_lsn FROM {s}key_changes WHERE relation_id=? AND version=?",
            [relation_id, version],
        ).fetchone()
        # (epoch, LSN) order: a version from an earlier epoch is behind L.
        if new_from is not None and (epoch, low) < tuple(new_from):
            raise ApplyError(
                f"{table}: ingest has not yet passed the source key change; run ingest, then retry"
            )
        names = [c.name for c in columns]
        if any(n.startswith(RESERVED_PREFIX) for n in names) or not set(keys) <= set(names):
            raise ApplyError(f"{table}: reserved column prefix or missing key columns")
        generation = uuid.uuid4().hex
        staging = f"{name}__gen_{generation[:12]}"
        current = dict(
            target=f"{self.mirror}.{quoted(schema)}.{quoted(staging)}", keys=keys, schema=schema
        )
        self.pin_generation(table, relation_id, generation, epoch, low, deadline)
        if fault:
            fault("after_pin")
        try:
            self._start(
                relation_id,
                generation,
                epoch,
                low,
                staging,
                keys,
                columns,
                current,
                version,
                writer,
            )
        except (duckdb.ConstraintException, duckdb.TransactionException) as exc:
            if isinstance(exc, duckdb.TransactionException) and not is_conflict(exc):
                raise  # possibly raised by a COMMIT that took effect: keep the pin
            # Another run claimed the table (seen as a key or write conflict). The
            # transaction did not commit, so the pin protects nothing. Any other error
            # may follow a commit whose response was lost: the pin then stays, and
            # the sweep removes it if no generation appears within the lease TTL.
            self._release_quietly(generation_owner(generation), generation)
            raise ApplyError(f"{table} already has an active backfill") from exc
        return self.generation(generation)

    def pin_generation(self, table, relation_id, generation, epoch, low, deadline):
        """Sweep stale pins, then register the generation's pin at L. Guard conflicts are
        transient (another consumer's registration, transfer or a drop), so keep retrying
        until the run's deadline rather than report `busy` with nothing started."""
        self.sweep_pins(relation_id)
        limit = deadline if deadline is not None else time.monotonic() + 60
        while True:
            try:
                self.retention.register_generation(relation_id, generation, epoch, low)
                return
            except duckdb.TransactionException as exc:
                if not is_conflict(exc):
                    raise
                if time.monotonic() >= limit:
                    raise LeaseBusy("retention guard stayed busy; nothing started") from exc
                time.sleep(1)
            except (HistoryDropped, TableHeld) as exc:
                raise ApplyError(f"{table}: {exc}") from exc

    def fail_if_held(self, gen):
        """Before catch-up and publication: a table held since the generation started
        (a per-table check failed, or it disappeared) fails the generation, since its
        logged changes may be incomplete. Returns False, so callers can chain it.

        It's a read before publish's transaction, not inside it: a hold recorded in the
        moment between is caught by apply (which holds the table as source_missing) and
        by the cut-0 key version when the hold clears, so the published mirror is still
        resynced. Re-check inside the transaction if that window ever matters."""
        row = self.execute(
            "SELECT held_reason FROM {s}tables WHERE relation_id=?", [gen["relation_id"]]
        ).fetchone()
        if row and row[0]:
            raise GenerationFailed(f"source table {row[0]} during the backfill")
        return False

    def sweep_pins(self, relation_id):
        """Release generation pins that protect nothing (`stale_generation`)."""
        current = self.state(relation_id)
        for generation_id, old in self.retention.generation_pins(relation_id):
            if stale_generation(self.generation(generation_id), generation_id, current, old):
                self._release_quietly(generation_owner(generation_id), generation_id)

    def require_pin(self, gen):
        """Read precondition for snapshot and catch-up: fail the generation (closed)
        if its pin is missing or above its catch-up position."""
        try:
            self.retention.require(
                generation_owner(gen["generation_id"]),
                gen["generation_id"],
                gen["epoch"],
                gen["catchup_lsn"],
            )
        except PinMissing as exc:
            raise GenerationFailed(str(exc)) from exc

    def _start(
        self,
        relation_id,
        generation,
        epoch,
        low,
        staging,
        keys,
        columns,
        current,
        version,
        writer="self",
    ):
        with self.transaction():
            commands.inserted(
                self,
                "create",
                "claim_insert",
                "INSERT INTO {m}backfills (relation_id,generation_id) VALUES (?,?)",
                [relation_id, generation],
            )
            commands.inserted(
                self,
                "create",
                "generation_insert",
                "INSERT INTO {m}generations (generation_id,relation_id,mode,epoch,pin_lsn,state,"
                "created_at,staging,key_columns,columns,chunks,rows,scan_done,catchup_lsn,token,"
                "guard,updated_at,key_version,finality_mode) VALUES (?,?,'snapshot',?,?,"
                "'snapshotting',current_timestamp,?,?,?,0,0,false,?,0,uuid()::VARCHAR,"
                "current_timestamp,?,?)",
                [
                    generation,
                    relation_id,
                    epoch,
                    low,
                    staging,
                    json.dumps(list(keys)),
                    column_json(columns),
                    low,
                    version,
                    writer,
                ],
            )
            create_mirror_table(self.con, self.mirror, current, columns)

    def acquire_generation(self, generation_id, run_id, *, token=None):
        """The generation lease; `token` as in Apply.acquire."""
        return generations.acquire_generation(self, generation_id, run_id, token=token)

    def guard_generation(self, generation_id, token):
        generations.guard_generation(self, generation_id, token, FenceLost)

    def release_generation(self, generation_id, token):
        generations.release_generation(self, generation_id, token)

    def held(self, generation_id, token):
        """A mirror transaction under the generation lease, the guard first
        (generation_commands.held)."""
        return commands.held(self, generation_id, token, self.config.lease_seconds, FenceLost)

    def lease(self, generation_id, token):
        """The lease's guard and checkpoints without a transaction, for `_write`."""
        return commands.lease(self, generation_id, token, self.config.lease_seconds, FenceLost)

    def end(self, gen, token, state, detail, *, fault=None):
        """Terminal failure or abandonment: release the table and drop staging, then
        (after that commit) the generation's retention pin."""
        schema = self.execute(
            "SELECT schema_name FROM {s}tables WHERE relation_id=?", [gen["relation_id"]]
        ).fetchone()[0]
        name = {"failed": "fail", "abandoned": "abandon"}[state]
        with self.held(gen["generation_id"], token) as lease:
            lease.end(name, gen, detail)  # a missing claim: reported there (detail, WARNING)
            self.con.execute(f"DROP TABLE IF EXISTS {self.staging_current(gen, schema)['target']}")
        if fault:
            fault("after_end_commit")
        self._release_quietly(generation_owner(gen["generation_id"]), gen["generation_id"])

    def abandon(self, table, run_id="abandon", *, fault=None):
        # An append table's snapshot generation ends the same way.
        relation_id, _ = self.registered(table, append_ok=True)
        gen = self.active(relation_id)
        if gen is None:
            raise ApplyError(f"{table} has no active backfill")
        token = self.acquire_generation(gen["generation_id"], run_id, token=gen["token"])
        try:
            self.end(
                self.generation(gen["generation_id"]),
                token,
                "abandoned",
                "abandoned by operator",
                fault=fault,
            )
        finally:
            # As run_table's: an ended generation keeps no holder (best effort).
            try:
                self.release_generation(gen["generation_id"], token)
            except Exception:
                pass
        return gen["generation_id"]

    # Snapshot ---------------------------------------------------------------

    def scan_chunk(self, table, gen, token, *, fault=None):
        """Read the next primary-key range and commit it with its resume key."""
        schema, name = table_name(table)
        columns, keys = gen["columns"], gen["key_columns"]
        types = self.verify_source(table, gen)
        # Alias the text casts: ORDER BY must resolve to the typed table columns,
        # not to same-named text output columns, or chunks follow text order while
        # the resume predicate compares typed values and skips rows.
        select = sql.SQL(",").join(
            sql.SQL("{}::text AS {}").format(sql.Identifier(c.name), sql.Identifier(f"c{i}"))
            for i, c in enumerate(columns)
        )
        order = sql.SQL(",").join(sql.Identifier(k) for k in keys)
        where, params = sql.SQL(""), []
        if gen["chunk_key"] is not None:
            where = sql.SQL("WHERE ROW({}) > ROW({})").format(
                order,
                sql.SQL(",").join(sql.SQL("%s::" + types[k]) for k in keys),
            )
            params = json.loads(gen["chunk_key"])
        statement = sql.SQL("SELECT {} FROM {} {} ORDER BY {} LIMIT %s").format(
            select, sql.Identifier(schema, name), where, order
        )
        replay, limit = None, self.config.chunk_rows
        if self.external(gen):
            limit = self._chunk_rows
            rows, size, capped, replay = standby_backfill().read_chunk(
                self, statement, [*params, limit], gen
            )
        else:
            rows, size, capped = self.read_chunk(statement, [*params, self.config.chunk_rows])
        positions = [next(i for i, c in enumerate(columns) if c.name == k) for k in keys]
        last = [rows[-1][i] for i in positions] if rows else json.loads(gen["chunk_key"] or "null")
        current = self.staging_current(gen, schema)
        with tempfile.TemporaryDirectory(prefix="cdc_backfill_") as directory:
            path = str(Path(directory) / "chunk.parquet")
            pq.write_table(
                pa.table(
                    {
                        f"c{i}": pa.array([row[i] for row in rows], pa.string())
                        for i in range(len(columns))
                    }
                ),
                path,
            )
            with self.held(gen["generation_id"], token) as lease:
                if rows:
                    names = ",".join(quoted(c.name) for c in columns)
                    casts = ",".join(cast_sql(f"c{i}", c.dtype) for i, c in enumerate(columns))
                    self.con.execute(
                        f"INSERT INTO {current['target']} ({names},_cdc_commit_lsn,"
                        f"_cdc_updated_at) SELECT {casts},?,current_timestamp "
                        "FROM read_parquet(?)",
                        [gen["pin_lsn"], path],
                    )
                if fault:
                    fault("after_chunk_insert")
                # External mode: the bound commits with the chunk's rows and resume key, so
                # no durable chunk lacks it.
                bound = (
                    {}
                    if replay is None
                    else {"replay_bound": max(gen["replay_bound"] or 0, replay)}
                )
                lease.progress_scan(
                    gen,
                    chunk_key=None if last is None else json.dumps(last),
                    chunks=gen["chunks"] + 1,
                    rows=gen["rows"] + len(rows),
                    # A byte-capped chunk is short but not the end of the table.
                    scan_done=not capped and len(rows) < limit,
                    **bound,
                )
            if fault:
                fault("after_chunk_commit")
        return len(rows)

    def read_chunk(self, statement, params):
        """Stream one chunk with a server-side cursor, stopping at max_chunk_bytes
        of text. Returns (rows, text bytes, byte-capped)."""
        con = self.snapshot_pg
        try:
            with con:
                self._chunk_settings(con)
                rows, size, capped = self._fetch_chunk(con, statement, params)
        except (psycopg2.errors.UndefinedColumn, psycopg2.errors.UndefinedTable) as exc:
            raise GenerationFailed(f"source table changed during backfill: {type(exc).__name__}")
        return rows, size, capped

    def _chunk_settings(self, con):
        with con.cursor() as cur:
            cur.execute("SET TRANSACTION READ ONLY")
            cur.execute(
                sql.SQL("SET LOCAL statement_timeout={}").format(
                    sql.Literal(f"{int(self.config.chunk_timeout_seconds)}s")
                )
            )

    def _fetch_chunk(self, con, statement, params):
        rows, size, capped = [], 0, False
        with con.cursor(name=f"cdc_chunk_{uuid.uuid4().hex}") as cur:
            cur.execute(statement, params)
            while not capped:
                batch = cur.fetchmany(1000)
                if not batch:
                    break
                for row in batch:
                    rows.append(row)
                    size += sum(len(value.encode()) for value in row if value is not None)
                    if size >= self.config.max_chunk_bytes:
                        capped = True
                        break
        return rows, size, capped

    @staticmethod
    def external(gen):
        """The generation's recorded finality mode (replica capture), never config's."""
        return gen.get("finality_mode") == "external"

    # Catch-up ---------------------------------------------------------------

    def _page(self, epoch, low, high):
        return self.reader.units(
            epoch,
            low,
            high,
            max_events=self.config.max_batch_events,
            max_bytes=self.config.max_batch_bytes,
        )

    def find_marker(self, gen):
        """End of the complete unit holding this generation's marker, or None."""
        hb_schema, hb_name = table_name(self.config.heartbeat)
        heartbeat = self.execute(
            "SELECT relation_id FROM {s}tables WHERE schema_name=? AND table_name=? " + BY_NAME,
            [hb_schema, hb_name],
        ).fetchone()
        if not heartbeat:
            raise ApplyError("heartbeat table is not registered")
        oid, epoch, bound = heartbeat[0], gen["epoch"], gen["wal_before_marker"]
        after = max(gen["pin_lsn"], self._searched.get(gen["generation_id"], 0))
        try:
            high = self.reader.head().high
            while after < high:
                page = self._page(epoch, after, high)
                events = self.reader.events(page, oid, high=high)
                if events:
                    descriptors = self.reader.descriptors(page, high=high)
                    for event in events:
                        if (
                            event["commit_lsn"] >= bound
                            and self._marker(event, descriptors, oid) == gen["generation_id"]
                        ):
                            return next(u.end for u in page if u.start == event["unit_start_lsn"])
                after = page[-1].end
                self._searched[gen["generation_id"]] = after
        except (SourceMismatch, HistoryDropped, PinMissing) as exc:
            raise GenerationFailed(str(exc)) from exc
        return None

    @staticmethod
    def _marker(event, descriptors, oid):
        """The marker cell of a heartbeat row's new image, or None."""
        new = event.get("new")
        if event["op"] not in ("insert", "update") or new is None:
            return None
        desc = descriptors[(oid, event["schema_version"], event["key_layout_hash"])]
        if MARKER_COLUMN not in desc.names:
            return None
        i = desc.names.index(MARKER_COLUMN)
        return new["values"][i] if new["kinds"][i] == TEXT else None

    def write_marker(self, gen):
        query(
            self.pg,
            sql.SQL(
                "INSERT INTO {} (id,marker) VALUES (%s,%s) "
                "ON CONFLICT(id) DO UPDATE SET marker=excluded.marker"
            ).format(sql.Identifier(*table_name(self.config.heartbeat))),
            [f"backfill:{gen['relation_id']}", gen["generation_id"]],
        )

    def catch_up(self, table, gen, token, *, deadline, fault=None):
        """Fold logged changes in [L, H) into staging, in bounded batches. Returns
        True once caught up to H.

        Each batch takes the SQL fold (sql_fold, as apply's: one plan statement, then one
        MERGE whose source folds the stored frames where they are, so no frame reaches
        the Flight) unless its plan rules the page out; then, or after the MERGE refuses
        a data error on a page small enough to read client-side, the Python fold runs the
        batch exactly as before. Either way one mirror transaction holds the generation
        guard, the MERGE into staging and the catchup_lsn compare-and-set."""
        schema, _ = table_name(table)
        high = gen["marker_lsn"]
        while gen["catchup_lsn"] < high:
            if deadline is not None and time.monotonic() >= deadline:
                return False
            current = self.staging_current(gen, schema)
            low = gen["catchup_lsn"]
            read = None
            if self.fold == "sql":
                batch, read = self._catch_up_plan(gen, current, low, high)
                if batch is not None:
                    committed = self._catch_up_sql(gen, token, current, batch, fault)
                    gen = self.generation(gen["generation_id"])
                    if committed:
                        continue
                    # The MERGE rolled back: redo the same page once on the Python fold.
                    current, read = self.staging_current(gen, schema), None
                    low = gen["catchup_lsn"]
            lease = self.lease(gen["generation_id"], token)
            chosen, boundary, columns, images = self._catch_up_batch(
                gen, current, low, high, read=read
            )
            try:
                self._write(
                    current,
                    columns,
                    images,
                    guard=lease.guard,
                    commit_state=lambda: lease.progress_catchup(
                        gen, catchup_lsn=boundary, columns=column_json(columns)
                    ),
                    fault=fault,
                )
            except Blocked as exc:
                raise GenerationFailed(f"{exc.state}: {exc.reason}") from exc
            except DATA_ERRORS as exc:
                raise GenerationFailed(
                    f"{type(exc).__name__} in catch-up commits {chosen[0]}..{chosen[-1]}"
                ) from exc
            gen = self.generation(gen["generation_id"])
        return True

    def _fallback(self, reason, read=None):
        key = "fallback:" + reason
        self.fold_stats[key] = self.fold_stats.get(key, 0) + 1
        logger.info("backfill: Python fold for this catch-up batch (%s)", reason)
        return None, read

    def _catch_up_plan(self, gen, current, low, high):
        """The SQL path's read for one catch-up batch: (SqlBatch, None), or (None, read)
        for the Python fold, `read` being the (page, descriptors) already read, if any.

        The checks are apply's (sql_path.decide) plus one row-free pre-check that replaces
        _key_mismatch here: any of this relation's descriptors in the page with a key
        layout other than the generation's sends the page to the Python fold, which
        refuses it exactly as before (the commit and unit in the reason)."""
        relation_id, page = gen["relation_id"], None
        try:
            page = self._page(gen["epoch"], low, high)
            if not page:
                return self._fallback("empty_page")
            descriptors = self.reader.descriptors(page, high=high)
            read = (page, descriptors)
            keys = tuple(current["keys"] or ())
            if not keys or any(
                oid == relation_id and tuple(desc.keys) != keys
                for (oid, _, _), desc in descriptors.items()
            ):
                return self._fallback("key_layout", read)
            versions = versions_of(descriptors, relation_id)
            if versions is None:
                return self._fallback("layouts", read)  # one version under two key layouts
            epoch, start, end = self.reader._bounds(page, high)
            days = sorted({d for u in page for d in u.days})
            frames = sql_fold.frames_sql(days, epoch, start, end)
            row = self.execute(sql_fold.plan_sql(frames, relation_id, versions)).fetchone()
            self.reader.check_frames(page, high, *row[:4])
        except (SourceMismatch, HistoryDropped, PinMissing) as exc:
            raise GenerationFailed(str(exc)) from exc
        except duckdb.OutOfMemoryException as exc:
            if not page:
                raise
            raise GenerationFailed(CATCHUP_OOM.format(page[0].start, page[-1].end)) from exc
        facts = PageFacts(*row[4:])
        reason, columns, layout = decide(
            relation_id, facts, descriptors, current["columns"], current["keys"]
        )
        if reason is not None:
            return self._fallback(reason, read)
        self.fold_stats["sql"] = self.fold_stats.get("sql", 0) + 1
        return SqlBatch(page, high, frames, facts.events, facts.commits, columns, layout), None

    def _catch_up_sql(self, gen, token, current, batch, fault):
        """Write one SQL catch-up batch. True: committed. False: the MERGE refused a data
        error (or its baseline check) and rolled back, on a page small enough to redo on
        the Python fold (sql_path.CACHE_CAP_BYTES), whose outcome then stands. A larger
        page fails the generation here instead: reading it into the Flight could OOM
        there, which no generation state records, so every run would retry it. Fencing,
        conflicts, I/O and interrupts propagate as before; nothing is redone under a lease
        that may be lost."""
        page, columns = batch.page, batch.columns
        lease = self.lease(gen["generation_id"], token)
        try:
            self._write(
                current,
                columns,
                SqlImages(batch, gen["relation_id"]),
                guard=lease.guard,
                commit_state=lambda: lease.progress_catchup(
                    gen, catchup_lsn=page[-1].end, columns=column_json(columns)
                ),
                fault=fault,
            )
        except duckdb.OutOfMemoryException as exc:
            raise GenerationFailed(CATCHUP_OOM.format(page[0].start, page[-1].end)) from exc
        except (Blocked, *DATA_ERRORS) as exc:
            if sum(u.bytes for u in page) <= sql_path.CACHE_CAP_BYTES:
                # Counted apart from "sql" and "fallback:*" (which partition the batches):
                # this batch's frames are read into the Flight after all.
                key = "redo:" + type(exc).__name__
                self.fold_stats[key] = self.fold_stats.get(key, 0) + 1
                logger.info(
                    "backfill: the SQL MERGE refused a catch-up batch (%s); redoing it on "
                    "the Python fold",
                    type(exc).__name__,
                )
                return False
            if isinstance(exc, Blocked):
                raise GenerationFailed(f"{exc.state}: {exc.reason}") from exc
            raise GenerationFailed(
                f"{type(exc).__name__} in catch-up (SQL fold) units {page[0].start}..{page[-1].end}"
            ) from exc
        return True

    def _catch_up_batch(self, gen, current, low, high, *, read=None):
        """One catch-up batch on the Python fold. `read`: the (page, descriptors) the SQL
        path already read for this same page and high, so they aren't read twice."""
        relation_id = gen["relation_id"]
        try:
            if read is None:
                page = self._page(gen["epoch"], low, high)
                events = self.reader.events(page, relation_id, high=high)
                descriptors = self.reader.descriptors(page, high=high)
            else:
                page, descriptors = read
                events = self.reader.events(page, relation_id, high=high)
        except (SourceMismatch, HistoryDropped, PinMissing) as exc:
            raise GenerationFailed(str(exc)) from exc
        changed = self._key_mismatch(current["keys"], events, descriptors, relation_id)
        if changed is not None:
            raise GenerationFailed(
                "needs_resync: source replica identity key changed at "
                f"{changed['commit_lsn']} (unit {changed['unit_start_lsn']})"
            )
        presented, v1_descriptors = self._present(events, descriptors, relation_id)
        _, columns, images, blocked = self.fold_batch(
            relation_id, current, presented, descriptors=v1_descriptors
        )
        if blocked:
            raise GenerationFailed(f"{blocked.state}: {blocked.reason} at {blocked.commit_lsn}")
        # For the caller's error text: the commits replayed, else the units.
        chosen = sorted({e["commit_lsn"] for e in events}) or [u.start for u in page]
        return chosen, page[-1].end, columns, images

    # Publication ------------------------------------------------------------

    def publish(self, table, gen, token, run_id, *, fault=None):
        """DROP the current mirror and RENAME staging in one mirror transaction,
        with the apply checkpoint at H. Fenced by both leases."""
        # The spec's legality first: only a catching_up generation publishes.
        check_command("publish", next((s for s in STATES if s == gen["state"]), None))
        relation_id = gen["relation_id"]
        schema, name = table_name(table)
        previous = self.state(relation_id)
        # An append snapshot (append_backfill): the replaced mirror is kept aside, never
        # dropped (phase 2 Q3), and apply_state takes the append fields.
        append = gen["mode"] == "append_snapshot"
        if append:
            from cdc_flight import append_backfill
        apply_token = None
        if previous is not None:
            try:
                apply_token = self.acquire(relation_id, run_id, token=previous["token"])
            except (LeaseBusy, duckdb.TransactionException) as exc:
                if (
                    isinstance(exc, duckdb.TransactionException)
                    and "conflict" not in str(exc).lower()
                ):
                    raise
                return False
        target = f"{self.mirror}.{quoted(schema)}.{quoted(name)}"
        refusal = f"{target} exists without apply state; refusing to replace it"
        # A recreated source table (a new OID) keeps the mirror name: its generation
        # replaces the mirror of the relation add-table retired for it, fenced by that
        # relation's apply lease. Any other holder of the name is still refused.
        retired = None
        if previous is None:
            retired = self.execute(RETIRED_HOLDER, [schema, name, relation_id]).fetchone()
            if retired is not None:
                try:
                    retired = (retired[0], self.acquire(retired[0], run_id, token=retired[1]))
                except (LeaseBusy, duckdb.TransactionException) as exc:
                    if (
                        isinstance(exc, duckdb.TransactionException)
                        and "conflict" not in str(exc).lower()
                    ):
                        raise
                    return False
        # Early refusal from the client catalog. It is not the guard: a stale catalog
        # can miss the table, so without apply state nothing is dropped (below).
        if (
            previous is None
            and retired is None
            and self.con.execute(
                "SELECT count(*) FROM duckdb_tables() WHERE database_name=? AND schema_name=? "
                "AND table_name=?",
                [self.config.mirror_database, schema, name],
            ).fetchone()[0]
        ):
            raise GenerationFailed(refusal)
        try:
            staging = self.staging_current(gen, schema)["target"]
            with self.transaction():
                lease = self.lease(gen["generation_id"], token)
                lease.guard()
                if previous is not None:
                    self.guard(relation_id, apply_token)
                if retired is not None:
                    # The guard fences the retired relation's apply runs; its retirement
                    # is re-read from server state in this transaction.
                    self.guard(retired[0], retired[1])
                    if self.execute(RETIRED_HOLDER, [schema, name, relation_id]).fetchone() is None:
                        raise FenceLost("the retired mirror changed during publication")
                    if append:
                        (old,) = self.execute(
                            "SELECT generation_id FROM {m}apply_state WHERE relation_id=?",
                            [retired[0]],
                        ).fetchone()
                        append_backfill.keep_aside(self, schema, name, old)
                    else:
                        self.con.execute(f"DROP TABLE IF EXISTS {target}")
                    pub.retired_delete(self, retired[0], retired[1])
                # Never replace a table this pipeline does not manage: only a mirror
                # with apply state is dropped. Without it, the RENAME itself fails
                # on the server if the name is taken, whatever the client catalog says.
                if previous is not None and append:
                    append_backfill.keep_aside(self, schema, name, previous["generation"])
                elif previous is not None:
                    self.con.execute(f"DROP TABLE IF EXISTS {target}")
                if fault:
                    fault("before_rename")
                try:
                    self.con.execute(f"ALTER TABLE {staging} RENAME TO {quoted(name)}")
                except duckdb.CatalogException as exc:
                    if previous is None and "already exists" in str(exc):
                        raise GenerationFailed(refusal) from None
                    raise
                if fault:
                    fault("after_rename")
                if previous is None and append:
                    ignore_deletes, ignored, truncated, version = (
                        append_backfill.apply_state_values(self, gen)
                    )
                    pub.statement(
                        self,
                        "publish",
                        "apply_insert",
                        "INSERT INTO {m}apply_state (relation_id,schema_name,table_name,"
                        "key_columns,generation_id,epoch,applied_through_lsn,columns,state,token,"
                        "guard,updated_at,hold_code_at,key_version,capture_mode,ignore_deletes,"
                        "deletes_ignored,last_truncate_lsn) VALUES (?,?,?,'[]',?,?,?,?,'live',0,"
                        "uuid()::VARCHAR,current_timestamp,current_timestamp,?,'append',?,?,?)",
                        [
                            relation_id,
                            schema,
                            name,
                            gen["generation_id"],
                            gen["epoch"],
                            gen["marker_lsn"],
                            column_json(gen["columns"]),
                            version,
                            ignore_deletes,
                            ignored,
                            truncated,
                        ],
                    )
                elif append:
                    ignore_deletes, ignored, truncated, version = (
                        append_backfill.apply_state_values(self, gen)
                    )
                    pub.statement(
                        self,
                        "publish",
                        "apply_cas",
                        "UPDATE {m}apply_state SET generation_id=?,epoch=?,applied_through_lsn=?,"
                        "columns=?,key_columns='[]',schema_name=?,table_name=?,key_version=?,"
                        "capture_mode='append',ignore_deletes=?,deletes_ignored=?,"
                        "last_truncate_lsn=?,state='live',detail=NULL,blocked_commit_lsn=NULL,"
                        "blocked_unit_start_lsn=NULL,held_since=NULL,hold_code=NULL,"
                        "hold_code_at=current_timestamp,updated_at=current_timestamp "
                        "WHERE relation_id=? AND token=? AND generation_id=?",
                        [
                            gen["generation_id"],
                            gen["epoch"],
                            gen["marker_lsn"],
                            column_json(gen["columns"]),
                            schema,
                            name,
                            version,
                            ignore_deletes,
                            ignored,
                            truncated,
                            relation_id,
                            apply_token,
                            previous["generation"],
                        ],
                    )
                    pub.replace(self, "publish", previous["generation"])
                elif previous is None:
                    pub.statement(
                        self,
                        "publish",
                        "apply_insert",
                        "INSERT INTO {m}apply_state (relation_id,schema_name,table_name,"
                        "key_columns,generation_id,epoch,applied_through_lsn,columns,state,token,"
                        "guard,updated_at,hold_code_at,key_version) VALUES (?,?,?,?,?,?,?,?,'live',"
                        "0,uuid()::VARCHAR,current_timestamp,current_timestamp,?)",
                        [
                            relation_id,
                            schema,
                            name,
                            json.dumps(list(gen["key_columns"])),
                            gen["generation_id"],
                            gen["epoch"],
                            gen["marker_lsn"],
                            column_json(gen["columns"]),
                            gen["key_version"],
                        ],
                    )
                else:
                    # A resync may follow a key change: identity comes from the generation.
                    pub.statement(
                        self,
                        "publish",
                        "apply_cas",
                        "UPDATE {m}apply_state SET generation_id=?,epoch=?,applied_through_lsn=?,"
                        "columns=?,key_columns=?,schema_name=?,table_name=?,key_version=?,"
                        "state='live',detail=NULL,blocked_commit_lsn=NULL,blocked_unit_start_lsn=NULL,"
                        "held_since=NULL,"
                        "hold_code=NULL,hold_code_at=current_timestamp,updated_at=current_timestamp "
                        "WHERE relation_id=? AND token=? AND generation_id=?",
                        [
                            gen["generation_id"],
                            gen["epoch"],
                            gen["marker_lsn"],
                            column_json(gen["columns"]),
                            json.dumps(list(gen["key_columns"])),
                            schema,
                            name,
                            gen["key_version"],
                            relation_id,
                            apply_token,
                            previous["generation"],
                        ],
                    )
                    pub.replace(self, "publish", previous["generation"])
                # The generation published and its claim deleted, both counted strictly.
                lease.publish(gen)
            if fault:
                fault("after_publish_commit")
            # After the commit; if this fails, apply completes it before reading.
            self.transfer_quietly(
                relation_id, gen["generation_id"], gen["epoch"], gen["marker_lsn"]
            )
            # The replaced generation's pin, if an earlier transfer was interrupted.
            try:
                self.sweep_pins(relation_id)
            except Exception:
                logger.error("retention pin sweep failed; the next backfill start retries it")
            return True
        finally:
            if apply_token is not None:
                try:
                    self.release(relation_id, apply_token)
                except Exception:
                    pass
            if retired is not None:
                # After a commit the row is gone and this matches nothing; after a
                # failure it frees the retired relation now, not at lease expiry.
                try:
                    self.release(*retired)
                except Exception:
                    pass

    # Driver -----------------------------------------------------------------

    def run_table(
        self, table, *, run_id=None, deadline=None, max_chunks=None, fault=None, start=True
    ):
        """Advance this table's generation as far as the deadline allows: start or
        resume it, snapshot, mark, catch up and publish. Safe to repeat. With
        start=False (a retry or self-re-trigger) it only resumes, so a retry after
        an ambiguous publication never begins another generation."""
        run_id = run_id or uuid.uuid4().hex
        # External mode's retry state is per run, however long the instance.
        self._conflicts, self._chunk_rows = 0, self.config.chunk_rows
        relation_id, mode = self.registered(table, append_ok=True)
        if mode == "append":
            # Keyless insert-only capture: one exact exported snapshot (append_backfill).
            from cdc_flight import append_backfill

            return append_backfill.run_table(
                self,
                table,
                relation_id,
                run_id=run_id,
                deadline=deadline,
                fault=fault,
                start_new=start,
            )
        relation_id, _ = self.registered(table)
        gen = self.active(relation_id)
        if gen is None and not start:
            return BackfillResult(table, "", "idle")
        if gen is None:
            try:
                gen = self.start(table, fault=fault, deadline=deadline)
            except LeaseBusy:
                return BackfillResult(table, "", "busy")
            except ApplyError:
                # A concurrent run claimed the table first; resume its generation.
                gen = self.active(relation_id)
                if gen is None:
                    raise
        generation_id = gen["generation_id"]
        # The generation's recorded mode decides; config must agree (replica capture).
        self.check_writer(table, gen.get("finality_mode") or "self")
        external = self.external(gen)
        standby = standby_backfill() if external else None  # the dispatch guard
        try:
            token = self.acquire_generation(generation_id, run_id, token=gen["token"])
        except (LeaseBusy, duckdb.TransactionException) as exc:
            if isinstance(exc, duckdb.TransactionException) and "conflict" not in str(exc).lower():
                raise
            return self.result(table, generation_id, "busy")
        chunks = 0
        marker_written = False
        try:
            if standby:
                standby.check_identity(self)
            while True:
                gen = self.generation(generation_id)
                # A generation started under an earlier epoch can't finish: its
                # history ends at the transition (begin_epoch).
                epoch, _ = self.source_position()
                if epoch != gen["epoch"]:
                    raise GenerationFailed("source epoch changed")
                if gen["state"] in ACTIVE:
                    self.require_pin(gen)
                if gen["state"] == "snapshotting" and not gen["scan_done"]:
                    if (deadline is not None and time.monotonic() >= deadline) or (
                        max_chunks is not None and chunks >= max_chunks
                    ):
                        return self.result(table, generation_id, "snapshotting")
                    try:
                        self.scan_chunk(table, gen, token, fault=fault)
                    except DATA_ERRORS as exc:
                        raise GenerationFailed(f"{type(exc).__name__} in snapshot chunk") from exc
                    except Exception as exc:
                        if not standby:
                            raise
                        stop = standby.after_error(
                            self, exc, self._snapshot_pg, table, generation_id
                        )
                        if stop is not None:
                            return stop
                        continue
                    chunks += 1
                elif gen["state"] == "snapshotting" and standby:
                    standby.finish_scan(self, gen, generation_id, token)
                    if fault:
                        fault("after_scan_done")
                elif gen["state"] == "snapshotting":
                    # Scan complete: every later marker commits after every chunk read.
                    wal = query(self.pg, current_sql(PRIMARY))[0][0]
                    with self.held(generation_id, token) as lease:
                        lease.finish_scan(gen, marker=generation_id, wal_before_marker=int(wal))
                    if fault:
                        fault("after_scan_done")
                elif gen["marker_lsn"] is None:
                    # External mode: H is the first unit ending past B, and the DBA's
                    # heartbeat job (liveness only) guarantees one; nothing is written.
                    high = standby.find_past(self, gen) if standby else self.find_marker(gen)
                    if high is None:
                        if not marker_written:
                            if not external:
                                self.write_marker(gen)
                            marker_written = True
                            waited = time.monotonic() + self.config.marker_wait_seconds
                            if deadline is not None:
                                waited = min(waited, deadline)
                        if time.monotonic() >= waited:
                            status = "waiting_past_replay" if external else "waiting_for_marker"
                            return self.result(table, generation_id, status)
                        time.sleep(min(2, self.config.marker_wait_seconds))
                        continue
                    # H is durable; the source must still match what was built, and
                    # ingest must have confirmed the generation's key through H.
                    try:
                        self.verify_source(table, gen)
                    except Exception as exc:
                        if not standby:
                            raise
                        stop = standby.after_error(self, exc, self._pg, table, generation_id)
                        if stop is not None:
                            return stop
                        continue
                    version, checked, key_epoch, held = self.execute(
                        "SELECT coalesce(key_version,0),key_checked_lsn,key_epoch,held_reason "
                        "FROM {s}tables WHERE relation_id=?",
                        [gen["relation_id"]],
                    ).fetchone()
                    if version != gen["key_version"]:
                        raise GenerationFailed("source key changed during backfill")
                    if key_epoch != gen["epoch"]:
                        raise GenerationFailed("source epoch changed")
                    if checked is None or checked < high:
                        # Format 2 freezes a held table's key_checked_lsn, so waiting
                        # would never end: a table held meanwhile fails the generation
                        # (fail_if_held's outcome, read with the key: no extra poll read).
                        if held:
                            raise GenerationFailed(f"source table {held} during the backfill")
                        return self.result(table, generation_id, "waiting_for_ingest")
                    with self.held(generation_id, token) as lease:
                        lease.progress_catchup(gen, marker_lsn=high)
                    if fault:
                        fault("after_marker")
                elif self.fail_if_held(gen) or not self.catch_up(
                    table, gen, token, deadline=deadline, fault=fault
                ):
                    return self.result(table, generation_id, "catching_up")
                else:
                    if fault:
                        fault("after_catch_up")
                    gen = self.generation(generation_id)
                    self.fail_if_held(gen)
                    if not self.publish(table, gen, token, run_id, fault=fault):
                        return self.result(table, generation_id, "busy_publish")
                    return self.result(table, generation_id, "published")
        except GenerationFailed as exc:
            self.end(self.generation(generation_id), token, "failed", exc.reason)
            return self.result(table, generation_id, "failed")
        finally:
            try:
                self.release_generation(generation_id, token)
            except Exception:
                pass

    def result(self, table, generation_id, status):
        gen = self.generation(generation_id)
        return BackfillResult(
            table,
            generation_id,
            status,
            gen["chunks"] or 0,
            gen["rows"] or 0,
            gen["marker_lsn"] or 0,
            gen["detail"] or "",
        )
