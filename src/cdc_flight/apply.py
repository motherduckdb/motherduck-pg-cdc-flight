"""Per-table apply: change-log transactions into typed mirror tables. The MERGE, any
additive ALTER, the checkpoint and the fencing guard commit together in the mirror;
the change log is only read. Retention pins (internal database, own transactions)
hold the log from the checkpoint before a read, and advance after it (retention.py).
"""

import json
import logging
import tempfile
import uuid
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from cdc_flight.config import LeaseBusy, quoted, table_name  # noqa: F401 (re-exported)
from cdc_flight.ingest_common import SourceMismatch, append_refusal
from cdc_flight.mirror import Blocked, Column, cast_sql, float_keys, fold, reconcile
from cdc_flight.retention import (
    PinMissing,
    Retention,
    TableHeld,
    generation_owner,
    is_conflict,
    refuse_lifecycle,
    table_owner,
)

logger = logging.getLogger(__name__)

# isolate_poison's detail (apply_run); table_actions recognises a poison hold by it (only
# those can be quarantined). Here, not in apply_run, so the action Flights don't import
# the apply loop.
POISON = "{} applying transaction {}"
PG_EPOCH_US = 946_684_800_000_000  # 2000-01-01 in Unix microseconds
# Data errors raised by casts or constraints inside the MERGE. They block the
# table; connection and fencing errors propagate instead.
DATA_ERRORS = (
    duckdb.ConversionException,
    duckdb.InvalidInputException,
    duckdb.ConstraintException,
    duckdb.OutOfRangeException,
)


class FenceLost(RuntimeError):
    pass


# A configured name resolves to its active row: never an unmanaged member (logged, not
# applied), and a removed row only if nothing newer has the name (a drop/recreate whose
# new OID add-table adopted).
BY_NAME = (
    "AND held_reason IS DISTINCT FROM 'unmanaged' "
    "ORDER BY held_reason IS NOT DISTINCT FROM 'removed' LIMIT 1"
)


class ApplyError(RuntimeError):
    pass


class _DryRun(Exception):
    """Rolls back a probe write that succeeded."""


@dataclass(frozen=True)
class TableResult:
    table: str
    status: str
    boundary: int | None  # None for not_configured: the run didn't read the table
    events: int = 0
    transactions: int = 0
    detail: str = ""
    units: int | None = None  # format 2 only: complete units applied (apply2)


def column_json(columns):
    return None if columns is None else json.dumps([list(asdict(c).values()) for c in columns])


# Decisions over rows already read. Both the loop head (under the lease) and the
# idle check (before it) call these, so the two cannot drift apart.


def source_from(row, configured_epoch, source_id="this source"):
    """(epoch, checkpoint) of the registered source. The epoch comes from the
    registration; a configured epoch is only an expectation of it."""
    if not row:
        raise ApplyError("source is not registered; run preflight first")
    refuse_lifecycle(source_id, row.get("lifecycle"))  # deleting or deleted
    if configured_epoch is not None and row["epoch"] != configured_epoch:
        raise ApplyError("configured epoch differs from the registered source epoch")
    return row["epoch"], row["confirmed"]


def key_limit_from(key, cut, version, epoch):
    """(changed, limit) from ingest's key versions. Same version as the mirror: up to
    the last boundary ingest confirmed for that key (nothing while unconfirmed). A later
    version: up to the next version's `cut`, the mirror key's last confirmed boundary.
    """
    if version == key["version"]:
        # key_checked_lsn is a position in key_epoch; a version spans one epoch.
        return False, (key["checked"] or 0) if key["key_epoch"] == epoch else 0
    if cut is None:
        raise ApplyError("key version history is missing; resync required")
    return True, cut


def next_step(current, source_epoch, confirmed, changed, limit):
    """What one loop iteration does: ("hold", Blocked), ("done", None) or
    ("apply", high), the bound of the next batch."""
    if source_epoch != current["epoch"]:
        return "hold", Blocked("needs_resync", "source epoch changed", code="epoch_changed")
    if changed and current["applied"] >= limit:
        return "hold", Blocked(
            "needs_resync", "source replica identity key changed; resync", code="key_changed"
        )
    high = min(confirmed, limit)
    if high <= current["applied"]:
        return "done", None
    return "apply", high


def pin_holds(pin, current):
    """The table pin belongs to the mirror's generation and holds the log from
    its checkpoint on (Retention.require's precondition)."""
    return (
        pin is not None
        and pin["generation"] == current["generation"]
        and (pin["epoch"], pin["lsn"]) <= (current["epoch"], current["applied"])
    )


def create_mirror_table(con, mirror, current, columns):
    """Typed mirror (or staging) table. No secondary indexes: DuckDB refuses to
    RENAME a table other catalog entries depend on, and publication renames.

    Append (keyless insert-only) capture: no PRIMARY KEY (duplicate rows stay), and
    `_cdc_event_ordinal` beside the commit lsn, so (lsn, ordinal) names every row. The
    lsn and time are nullable: a snapshot row has neither (keyless design D6)."""
    definitions = ",".join(f"{quoted(c.name)} {c.dtype}" for c in columns)
    keys = ",".join(quoted(k) for k in current["keys"])
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {mirror}.{quoted(current['schema'])}")
    if current.get("capture_mode") == "append":
        con.execute(
            f"CREATE TABLE {current['target']} ({definitions},"
            "_cdc_commit_lsn UBIGINT,_cdc_event_ordinal UBIGINT NOT NULL,"
            "_cdc_updated_at TIMESTAMPTZ)"
        )
        return
    con.execute(
        f"CREATE TABLE {current['target']} ({definitions},"
        "_cdc_commit_lsn UBIGINT NOT NULL,_cdc_updated_at TIMESTAMPTZ NOT NULL,"
        f"PRIMARY KEY({keys}))"
    )


def take_lease(owner, update, read, parameters, key, token, busy):
    """Shared by the apply table lease and the backfill generation lease: see
    Apply.acquire. `update` takes `parameters`; `read` takes `key`."""
    if token is not None:
        count = owner.execute(update + " AND token=?", [*parameters, token]).fetchone()[0]
        if count != 1:
            raise LeaseBusy(busy)
        return token + 1
    with owner.transaction():
        if owner.execute(update, parameters).fetchone()[0] != 1:
            raise LeaseBusy(busy)
        return owner.execute(read, key).fetchone()[0]


# When a table first left `live`: kept across held-to-held transitions, cleared by
# `live`. Takes the new state as its parameter.
HELD_SINCE = (
    "held_since=CASE WHEN ?='live' THEN NULL ELSE coalesce(held_since,current_timestamp) END"
)


# Mirror metadata columns added by format 2 (created with new mirrors, ALTERed into old).
V2_COLUMNS = {
    "apply_state": (("blocked_unit_start_lsn", "UBIGINT"),),
    "quarantine": (
        ("unit_start_lsn", "UBIGINT"),
        ("unit_end_lsn", "UBIGINT"),
        ("unit_digest", "VARCHAR"),
    ),
}


def with_v2_columns(added):
    return {
        **added,
        **{table: tuple(added.get(table, ())) + columns for table, columns in V2_COLUMNS.items()},
    }


OPTIONAL_STATE = ("capture_mode", "ignore_deletes", "hold_code")


class Apply:
    # The change-log format this consumer reads; apply2.Apply2 sets 2. Action wrappers
    # dispatch on it without a read (a v1 action issues no extra statement).
    format_version = 2
    # The mirror's _cdc metadata tables. Columns added after a table first shipped
    # go in ADDED_COLUMNS: ALTERed into existing stores, created with new ones.
    MIRROR_TABLES = {
        "apply_state": "CREATE TABLE IF NOT EXISTS {m}apply_state (relation_id UINTEGER PRIMARY KEY, "
        "schema_name VARCHAR, table_name VARCHAR, key_columns VARCHAR, generation_id VARCHAR, "
        "epoch INTEGER, applied_through_lsn UBIGINT, columns VARCHAR, state VARCHAR, "
        "detail VARCHAR, blocked_commit_lsn UBIGINT, token BIGINT, holder VARCHAR, "
        "expires_at TIMESTAMPTZ, guard VARCHAR, updated_at TIMESTAMPTZ)",
        "generations": "CREATE TABLE IF NOT EXISTS {m}generations (generation_id VARCHAR PRIMARY KEY, "
        "relation_id UINTEGER, mode VARCHAR, epoch INTEGER, pin_lsn UBIGINT, "
        "marker_lsn UBIGINT, state VARCHAR, created_at TIMESTAMPTZ)",
        "runs": "CREATE TABLE IF NOT EXISTS {m}runs (run_id VARCHAR, relation_id UINTEGER, "
        "status VARCHAR, boundary UBIGINT, detail VARCHAR, updated_at TIMESTAMPTZ, "
        "PRIMARY KEY(run_id, relation_id))",
        # Transactions an operator skips for one generation of a table (table_actions).
        "quarantine": "CREATE TABLE IF NOT EXISTS {m}quarantine (relation_id UINTEGER, "
        "generation_id VARCHAR, epoch INTEGER, commit_lsn UBIGINT, reason VARCHAR, "
        "actor VARCHAR, run_id VARCHAR, created_at TIMESTAMPTZ, "
        "PRIMARY KEY(relation_id, generation_id, epoch, commit_lsn))",
        # Action-Flight dry runs (action_flight): spent once by the real run.
        "previews": "CREATE TABLE IF NOT EXISTS {m}previews (id VARCHAR PRIMARY KEY, "
        "action VARCHAR, inputs_hash VARCHAR, created_at TIMESTAMPTZ, used_at TIMESTAMPTZ, "
        "used_by VARCHAR, actor VARCHAR)",
    }
    # The source key version the mirror was built under (see key_limit).
    # held_since: when the table first left `live` (kept across held-to-held
    # transitions, cleared by `live`), for the operations Dive.
    ADDED_COLUMNS = with_v2_columns(
        {
            "apply_state": (
                ("key_version", "INTEGER"),
                ("held_since", "TIMESTAMPTZ"),
                # NULL: keyed. Unused until the release that adds append capture.
                ("capture_mode", "VARCHAR"),
                # Append capture's skipped DELETE events (ignore_deletes), written with
                # the checkpoint. Unused until the release that adds append capture.
                ("deletes_ignored", "UBIGINT"),
                # Append capture: the DELETE option fixed for this generation (copied from
                # the registry at start), and the last TRUNCATE applied in place (D10).
                ("ignore_deletes", "BOOLEAN"),
                ("last_truncate_lsn", "UBIGINT"),
                # apply_status.hold_code (hold_codes.HOLD_CODES; NULL when live), written by
                # every state change together with hold_code_at = that statement's
                # updated_at. A later statement by an older build bumps updated_at but not
                # hold_code_at, so the snapshot reports the code only while they're equal.
                ("hold_code", "VARCHAR"),
                ("hold_code_at", "TIMESTAMPTZ"),
            )
        }
    )

    def __init__(self, con, config, *, retention=None):
        self.con, self.config = con, config
        self._state_columns = None
        self.mirror = quoted(config.mirror_database)
        self.prefixes = {
            "{m}": f"{self.mirror}._cdc.",
            "{s}": f"{quoted(config.database)}.{quoted(config.source_id)}.",
        }
        # The default is the active whole-unit retention; the apply2 aliases' factory can inject one.
        if retention is None:
            retention = Retention(con, config.database, config.source_id, config.lease_seconds)
        self.retention = retention
        from cdc_flight.unit_store import UnitReader

        self.reader = UnitReader(con, config)
        # schema_version -> Relation JSON, served to fold_batch. It accumulates safely:
        # a version names one relation layout (it hashes the oid and the identity), so
        # one version never maps to two descriptors (_present also refuses that).
        self._v1_descriptors = {}

    def execute(self, statement, parameters=None):
        for key, value in self.prefixes.items():
            statement = statement.replace(key, value)
        return self.con.execute(statement, parameters or [])

    @contextmanager
    def transaction(self):
        self.con.execute("BEGIN")
        try:
            yield
            self.con.execute("COMMIT")
        except BaseException:
            try:
                self.con.execute("ROLLBACK")
            except Exception:
                pass
            raise

    def mirror_catalog(self):
        """{table: {column, ...}} in the mirror's _cdc schema, from the client's catalog
        (no MotherDuck round trip)."""
        tables = {}
        for table, column in self.con.execute(
            "SELECT table_name,column_name FROM duckdb_columns() "
            "WHERE database_name=? AND schema_name='_cdc'",
            [self.config.mirror_database],
        ).fetchall():
            tables.setdefault(table, set()).add(column)
        return tables

    def mirror_statements(self, catalog):
        """The DDL still missing from `catalog`: nothing on an existing store. A new
        table is created with its added columns, in the order ALTER would add them."""
        statements = [] if catalog else [f"CREATE SCHEMA IF NOT EXISTS {self.mirror}._cdc"]
        for table, statement in self.MIRROR_TABLES.items():
            added = self.ADDED_COLUMNS.get(table, ())
            if table not in catalog:
                assert statement.endswith(")")
                statements.append(statement[:-1] + "".join(f", {c} {t}" for c, t in added) + ")")
                continue
            statements += [
                f"ALTER TABLE {{m}}{table} ADD COLUMN IF NOT EXISTS {column} {dtype}"
                for column, dtype in added
                if column not in catalog[table]
            ]
        return statements

    def check_format(self):
        """Read-only: the stored source is format 2 (protocol, binding, reader capability),
        or SourceMismatch. Writes nothing; initialize() runs it before any DDL, and action
        wrappers call it to refuse a legacy store before touching the mirror."""
        try:
            return self.reader.head()
        except duckdb.CatalogException as exc:
            # No source registry or no protocol_v2 at all: not a format-2 store.
            raise SourceMismatch("unsupported source protocol; recreate and re-backfill") from exc

    def initialize(self):
        """Create whatever is missing in one mirror transaction (nothing on an existing
        store), then retention's tables in their own: a write transaction touches one
        database."""
        self.check_format()  # a legacy or unknown store refuses before any mirror DDL
        for attempt in range(2):
            try:
                statements = self.mirror_statements(self.mirror_catalog())
                if statements:
                    with self.transaction():
                        for statement in statements:
                            self.execute(statement)
                break
            except duckdb.TransactionException as exc:
                # A concurrent initializer: re-read the catalog and retry once.
                if attempt or not is_conflict(exc):
                    raise
        self.retention.initialize()
        self._state_columns = {c: c for c in OPTIONAL_STATE}

    def _optional_state(self):
        # NULL on a mirror no new apply initialized (compact)
        if self._state_columns is None:
            sql = (
                "SELECT column_name FROM duckdb_columns() WHERE database_name=? "
                "AND schema_name='_cdc' AND table_name='apply_state'"
            )
            have = {c for (c,) in self.con.execute(sql, [self.config.mirror_database]).fetchall()}
            self._state_columns = {c: c if c in have else "NULL" for c in OPTIONAL_STATE}
        return self._state_columns

    # Registration ---------------------------------------------------------

    def registered(self, table, *, with_empty=False, append_ok=False):
        schema, name = table_name(table)
        row = self.execute(
            "SELECT relation_id,key_columns,empty_after_slot,capture_mode,catalog_key,"
            "held_reason FROM {s}tables "
            "WHERE schema_name=? AND table_name=? " + BY_NAME,
            [schema, name],
        ).fetchone()
        if not row:
            raise ApplyError(f"{table} is not registered; run preflight and ingest first")
        refusal = append_refusal(table, row[3], row[1], row[4], row[5], append_ok=append_ok)
        if refusal:
            raise ApplyError(refusal)
        if append_ok:
            return row[0], row[3]
        result = (row[0], tuple(json.loads(row[1])))
        return (*result, row[2]) if with_empty else result

    def source_position(self):
        """(epoch, checkpoint) of the registered source. The epoch comes from the
        registration; a configured epoch is only an expectation of it."""
        row = self.execute(
            "SELECT epoch,confirmed_lsn,lifecycle FROM {s}source WHERE id=1"
        ).fetchone()
        return source_from(
            row and dict(epoch=row[0], confirmed=row[1], lifecycle=row[2]),
            self.config.epoch,
            self.config.source_id,
        )

    def key_limit(self, relation_id, version, epoch):
        """(changed, limit) from ingest's key versions (IngestLog.record_keys).

        Same version as the mirror: apply goes no further than the last boundary at
        which ingest confirmed that key, and not at all while it is unconfirmed.
        Later version: the key changed after the mirror's version, so apply stops at
        the next version's cut, the last boundary confirmed for the mirror's key.
        """
        current, checked, key_epoch = self.execute(
            "SELECT coalesce(key_version,0),key_checked_lsn,key_epoch FROM {s}tables "
            "WHERE relation_id=?",
            [relation_id],
        ).fetchone()
        key = dict(version=current, checked=checked, key_epoch=key_epoch)
        cut = None
        if version != current:
            row = self.execute(
                "SELECT cut_lsn FROM {s}key_changes WHERE relation_id=? AND version=?",
                [relation_id, version + 1],
            ).fetchone()
            cut = row and row[0]
        return key_limit_from(key, cut, version, epoch)

    def start_from_log(self, table, *, accept_incomplete=False):
        """Publish a log-only generation: the mirror starts empty at the log's start.

        It is exact only when preflight observed the table empty after its slot
        existed: every earlier row's delete is then in the log. Otherwise it
        refuses, unless accept_incomplete records that the mirror may be missing
        rows that existed before capture (mode log_incomplete). Tables with existing
        rows need a backfill generation instead. Never replaces a generation.
        """
        if self.registered(table, append_ok=True)[1] == "append":
            return self.start_append(table, accept_incomplete=accept_incomplete)[0]
        relation_id, keys, empty = self.registered(table, with_empty=True)
        if empty is not True and not accept_incomplete:
            raise ApplyError(
                f"{table} was not observed empty after its slot was created; "
                "use a backfill generation, or accept_incomplete to mirror only later changes"
            )
        mode = "log" if empty is True else "log_incomplete"
        # The log starts under key version 0 (the registered key). After a key
        # change, part of the log is under another key: only a backfill can start.
        if self.execute(
            "SELECT coalesce(key_version,0) FROM {s}tables WHERE relation_id=?", [relation_id]
        ).fetchone()[0]:
            raise ApplyError(f"{table}: source key changed since capture began; use backfill")
        epoch, _ = self.source_position()
        if epoch != 1:
            # The empty-after-slot evidence is about epoch 1's slot; a later epoch's
            # history starts mid-stream, so only a backfill is exact.
            raise ApplyError(f"{table}: the source epoch changed; use a backfill generation")
        schema, name = table_name(table)
        generation = uuid.uuid4().hex
        start = self._initial_position(epoch)
        # The pin comes first: a crash before the mirror commit leaves extra history.
        try:
            self.retention.register_generation(relation_id, generation, epoch, start)
        except TableHeld as exc:
            raise ApplyError(f"{table}: {exc}") from exc
        try:
            self._start_from_log(relation_id, generation, mode, epoch, schema, name, keys, table)
        except ApplyError:
            # A refusal inside the transaction: it rolled back, so the pin protects
            # nothing. Any other error may follow a commit whose response was lost;
            # the pin then stays (apply completes the transfer, or the sweep removes
            # it once it is older than the lease TTL).
            self._release_quietly(generation_owner(generation), generation)
            raise
        # After the commit; if this fails, ensure_pin completes it before apply reads.
        self.transfer_quietly(relation_id, generation, epoch, start)
        return generation

    def start_append(self, table, **options):
        """An append table's log-only generation at S (append_apply.start_append)."""
        from cdc_flight.append_apply import start_append

        return start_append(self, table, **options)

    def _initial_position(self, epoch):
        # The recorded epoch start; missing evidence fails closed (Retention2).
        return self.retention.initial_cut(epoch)

    def _start_from_log(self, relation_id, generation, mode, epoch, schema, name, keys, table):
        """v1's statements with the recorded epoch start in place of 0: the state row
        and the pin hold the same cut, so the first page starts on the chain."""
        start = self._initial_position(epoch)
        with self.transaction():
            if self.execute(
                "SELECT 1 FROM {m}apply_state WHERE relation_id=?", [relation_id]
            ).fetchone():
                raise ApplyError(f"{table} already has a generation; resync replaces it")
            # The spec: create_log_only, NULL -> published, both counts checked.
            from cdc_flight.generation_commands import inserted

            inserted(
                self,
                "create_log_only",
                "generation_insert",
                "INSERT INTO {m}generations (generation_id,relation_id,mode,epoch,pin_lsn,"
                "marker_lsn,state,created_at) VALUES (?,?,?,?,?,?,'published',current_timestamp)",
                [generation, relation_id, mode, epoch, start, start],
            )
            inserted(
                self,
                "create_log_only",
                "apply_insert",
                "INSERT INTO {m}apply_state (relation_id,schema_name,table_name,key_columns,"
                "generation_id,epoch,applied_through_lsn,columns,state,token,guard,updated_at,"
                "hold_code_at,key_version) VALUES (?,?,?,?,?,?,?,NULL,'live',0,uuid()::VARCHAR,"
                "current_timestamp,current_timestamp,0)",
                [relation_id, schema, name, json.dumps(list(keys)), generation, epoch, start],
            )

    def transfer_quietly(self, relation_id, generation, epoch, lsn, keep=None):
        try:
            self.retention.transfer(relation_id, generation, epoch, lsn, keep)
        except Exception:
            logger.error("retention pin transfer failed; apply completes it before reading")

    def _release_quietly(self, owner, generation):
        try:
            self.retention.release(owner, generation)
        except Exception:
            logger.error("retention pin release failed; it is swept or kept as extra history")

    # Retention --------------------------------------------------------------

    def ensure_pin(self, relation_id, current, *, pin=None):
        """Read precondition: the table's pin belongs to the mirror's generation and
        holds the log from its checkpoint on. Completes a publication transfer that
        a crash interrupted. Raises PinMissing (fail closed) otherwise. `pin`: the
        table pin as read with `current` under this lease (head), if already read."""
        owner = table_owner(relation_id)
        args = (current["generation"], current["epoch"], current["applied"])
        if pin_holds(pin, current):
            return pin
        try:
            return self.retention.require(owner, *args)
        except PinMissing:
            if self.retention.pin(generation_owner(current["generation"])) is None:
                raise
        self.retention.transfer(relation_id, *args, keep=self.active_generation(relation_id))
        return self.retention.require(owner, *args)

    def active_generation(self, relation_id):
        """The relation's active backfill generation, whose pin must survive."""
        try:
            row = self.execute(
                "SELECT generation_id FROM {m}backfills WHERE relation_id=?", [relation_id]
            ).fetchone()
        except duckdb.CatalogException:
            return None  # backfill never initialized this mirror
        return row[0] if row else None

    def advance_pin(self, relation_id, generation, epoch, committed):
        """After a mirror commit: raise the table pin to `committed`, a checkpoint
        this run committed under its lease (or read under it). Never a value that
        was only attempted: a pin above the committed checkpoint is never lowered
        and would let compaction drop history apply still needs. A failure only
        leaves the pin lower (extra history). Returns whether the pin was raised."""
        try:
            return self.retention.advance(table_owner(relation_id), generation, epoch, committed)
        except Exception:
            logger.error("retention pin advance failed; the log is retained longer")
            return False

    # Fencing --------------------------------------------------------------

    def acquire(self, relation_id, run_id, *, token=None):
        """Take the table's apply lease and return its token.

        With `token`, the value a read just before returned: one UPDATE that also
        requires the token to be unchanged, so the new token is `token + 1`. A run
        that acquired since that read makes this one busy (retried by the next run).
        Without it: the UPDATE, then a read of the new token. Never UPDATE ...
        RETURNING on a fenced row: DuckDB 1.5.6 skips write-write conflict
        detection for it."""
        return take_lease(
            self,
            "UPDATE {m}apply_state SET token=token+1,holder=?,guard=uuid()::VARCHAR,"
            "expires_at=current_timestamp+?*INTERVAL '1 second' WHERE relation_id=? "
            "AND (expires_at IS NULL OR expires_at<=current_timestamp)",
            "SELECT token FROM {m}apply_state WHERE relation_id=?",
            [run_id, self.config.lease_seconds, relation_id],
            [relation_id],
            token,
            "another apply run holds this table",
        )

    def guard(self, relation_id, token):
        # Same guard column as acquisition, so a takeover conflicts with this write.
        count = self.execute(
            "UPDATE {m}apply_state SET guard=uuid()::VARCHAR,"
            "expires_at=current_timestamp+?*INTERVAL '1 second' "
            "WHERE relation_id=? AND token=? AND expires_at>current_timestamp",
            [self.config.lease_seconds, relation_id, token],
        ).fetchone()[0]
        if count != 1:
            raise FenceLost("apply lease expired or was replaced")

    def release(self, relation_id, token):
        # One statement: autocommit is its own transaction.
        self.execute(
            "UPDATE {m}apply_state SET guard=uuid()::VARCHAR,holder=NULL,expires_at=NULL "
            "WHERE relation_id=? AND token=?",
            [relation_id, token],
        )

    # State ----------------------------------------------------------------

    def head(self, table):
        """Everything one apply step decides from, in one read (one MotherDuck round
        trip across both databases): the registration, the mirror state, the
        source position, the table's key version and the next version's cut, the
        table pin, and whether another run holds the lease."""
        schema, name = table_name(table)
        optional = self._optional_state()
        row = self.execute(
            "WITH r AS (SELECT relation_id FROM {s}tables WHERE schema_name=? AND table_name=? "
            + BY_NAME
            + ") "
            "SELECT (SELECT relation_id FROM r),"
            "(SELECT {'epoch':epoch,'confirmed':confirmed_lsn,'lifecycle':lifecycle} "
            "FROM {s}source WHERE id=1),"
            "(SELECT {'version':coalesce(key_version,0),'checked':key_checked_lsn,"
            "'key_epoch':key_epoch,'held_reason':held_reason,'held_detail':held_detail,"
            "'capture_mode':capture_mode,'key_columns':key_columns,'catalog_key':catalog_key} "
            "FROM {s}tables WHERE relation_id=(SELECT relation_id FROM r)),"
            "(SELECT {'generation_id':generation_id,'epoch':epoch,"
            "'applied':applied_through_lsn,'columns':columns,'state':state,"
            "'key_columns':key_columns,'schema_name':schema_name,'table_name':table_name,"
            "'key_version':coalesce(key_version,0),'token':token,"
            f"'capture_mode':{optional['capture_mode']},"
            f"'ignore_deletes':{optional['ignore_deletes']},'detail':detail,"
            "'held':coalesce(expires_at>current_timestamp,false)} "
            "FROM {m}apply_state WHERE relation_id=(SELECT relation_id FROM r)),"
            "(SELECT {'cut':cut_lsn,'reason':reason} FROM {s}key_changes "
            "WHERE relation_id=(SELECT relation_id FROM r) "
            "AND version=(SELECT coalesce(key_version,0)+1 FROM {m}apply_state "
            "WHERE relation_id=(SELECT relation_id FROM r))),"
            "(SELECT {'generation':generation_id,'epoch':source_epoch,'lsn':pinned_lsn} "
            "FROM {s}retention_pins WHERE owner='table:'||(SELECT relation_id FROM r)),"
            # An append table's catalog hold (catalog_checks): the first key change with a
            # reason past its generation's version (epoch transitions' rows have none).
            "(SELECT {'cut':cut_lsn,'reason':reason} FROM {s}key_changes "
            "WHERE relation_id=(SELECT relation_id FROM r) AND reason IS NOT NULL "
            "AND version>(SELECT coalesce(key_version,0) FROM {m}apply_state "
            "WHERE relation_id=(SELECT relation_id FROM r)) ORDER BY version LIMIT 1),"
            + self.SKIP_QUERY,
            [schema, name],
        ).fetchone()
        relation_id, source, key, state, change, pin, append_hold, skip = row
        # The next key change's cut, and its catalog reason (catalog_checks; None: a key).
        cut, cut_reason = (change["cut"], change["reason"]) if change else (None, None)
        if relation_id is None:
            raise ApplyError(f"{table} is not registered; run preflight and ingest first")
        current = held = None
        if state is not None:
            values = list(state.values())
            current = self._state_dict(values[:12])
            current["detail"], held = values[12], values[13]
        return dict(
            relation_id=relation_id,
            source=source,
            key=key,
            current=current,
            held=held,
            cut=cut,
            cut_reason=cut_reason,
            append_hold=append_hold,
            pin=pin,
            skip=self._skip_set(skip),
        )

    # head's last column: the whole units the current generation quarantined.
    SKIP_QUERY = (
        "(SELECT list({'s':q.unit_start_lsn,'e':q.unit_end_lsn,'d':q.unit_digest}) "
        "FROM {m}quarantine q JOIN {m}apply_state a USING (relation_id,generation_id,epoch) "
        "WHERE relation_id=(SELECT relation_id FROM r) AND q.unit_start_lsn IS NOT NULL)"
    )

    @staticmethod
    def _skip_set(rows):
        return {(r["s"], r["e"], r["d"]) for r in rows or ()}

    def state(self, relation_id):
        optional = self._optional_state()
        row = self.execute(
            "SELECT generation_id,epoch,applied_through_lsn,columns,state,key_columns,"
            "schema_name,table_name,coalesce(key_version,0),token,"
            f"{optional['capture_mode']},{optional['ignore_deletes']} "
            "FROM {m}apply_state WHERE relation_id=?",
            [relation_id],
        ).fetchone()
        return None if not row else self._state_dict(row)

    def _state_dict(self, row):
        generation, epoch, applied, columns, state, keys, schema, name, key_version, token = row[
            :10
        ]
        mode, ignore_deletes = row[10:12] if len(row) > 10 else (None, None)
        return dict(
            capture_mode=mode,  # None: keyed; "append": keyless insert-only
            ignore_deletes=bool(ignore_deletes),
            generation=generation,
            epoch=epoch,
            applied=applied,
            columns=None if columns is None else [Column(*c) for c in json.loads(columns)],
            state=state,
            keys=tuple(json.loads(keys)),
            target=f"{self.mirror}.{quoted(schema)}.{quoted(name)}",
            schema=schema,
            key_version=key_version,
            token=token,  # for acquire(token=...) right after this read
        )

    def set_state(
        self,
        relation_id,
        token,
        expected,
        *,
        state,
        detail=None,
        blocked_commit_lsn=None,
        code=None,
    ):
        """A state-only transition (expiry, pause, resume, retry), inside the caller's
        mirror transaction after `guard`: a compare-and-set on the token and `expected`
        (state, generation, epoch, checkpoint, as state() returns). Returns whether the
        row matched. Clearing the blocked commit clears the blocked unit too. `code`: the
        hold's code (None for live). A mirror no new apply initialized yet (compact's
        expiry, before apply's first run of this build) has no hold columns: left out."""
        hold = self._optional_state()["hold_code"] == "hold_code"
        count = self.execute(
            "UPDATE {m}apply_state SET state=?,detail=?,blocked_commit_lsn=?,"
            "blocked_unit_start_lsn=CASE WHEN ?::UBIGINT IS NULL THEN NULL "
            "ELSE blocked_unit_start_lsn END,"
            + HELD_SINCE
            + ","
            + ("hold_code=?,hold_code_at=current_timestamp," if hold else "")
            + "updated_at=current_timestamp WHERE relation_id=? AND token=? AND state=? "
            "AND generation_id=? AND epoch=? AND applied_through_lsn=?",
            [
                state,
                detail,
                blocked_commit_lsn,
                blocked_commit_lsn,
                state,
                *([code] if hold else []),
                relation_id,
                token,
                expected["state"],
                expected["generation"],
                expected["epoch"],
                expected["applied"],
            ],
        ).fetchone()[0]
        return count == 1

    def _set_state(
        self, relation_id, token, expected, *, boundary, columns, state, detail, commit, code
    ):
        """The guarded checkpoint update. While held on a commit, the checkpoint is that
        commit's unit start, recorded as blocked_unit_start_lsn (never inferred later).
        `code`: the hold's code (Blocked.code), None when live."""
        count = self.execute(
            "UPDATE {m}apply_state SET applied_through_lsn=?,columns=?,state=?,detail=?,"
            "blocked_commit_lsn=?,blocked_unit_start_lsn=?," + HELD_SINCE + ","
            "hold_code=?,hold_code_at=current_timestamp,"
            "updated_at=current_timestamp WHERE relation_id=? AND token=? "
            "AND generation_id=? AND epoch=? AND applied_through_lsn=?",
            [
                boundary,
                column_json(columns),
                state,
                detail,
                commit,
                boundary if commit is not None else None,
                state,
                code,
                relation_id,
                token,
                expected["generation"],
                expected["epoch"],
                expected["applied"],
            ],
        ).fetchone()[0]
        if count != 1:
            raise FenceLost("apply checkpoint changed underneath this run")

    def hold(self, relation_id, token, current, blocked):
        """Record a held state without applying anything further."""
        with self.transaction():
            self.guard(relation_id, token)
            self._set_state(
                relation_id,
                token,
                current,
                boundary=current["applied"],
                columns=current["columns"],
                state=blocked.state,
                detail=blocked.reason,
                commit=blocked.commit_lsn,
                code=blocked.code,
            )

    # Reading the log (format 2: UnitReader pages complete units) -----------------

    def descriptors(self, relation_id, versions, epoch):
        """fold_batch's descriptor lookup (also from _probe and isolate_poison): the
        current batch's own descriptors, never v1's schema_versions table."""
        return {v: self._v1_descriptors[v] for v in versions if v in self._v1_descriptors}

    def _present(self, events, descriptors_by_key, relation_id):
        """Events as v1's fold reads them (lazy cells), plus v1-shaped descriptors."""
        # Imported once per call, outside the per-event loop, and not at module level,
        # so table-membership never loads mirror2.
        from cdc_flight.mirror2 import to_v1

        by_version, presented = {}, []
        for event in events:
            if event["op"] == "truncate":
                presented.append(dict(event, full_old=False, payload={"old": None, "new": None}))
                continue
            desc = descriptors_by_key[
                (relation_id, event["schema_version"], event["key_layout_hash"])
            ]
            known = by_version.setdefault(event["schema_version"], desc)
            if known.json != desc.json:
                raise ValueError("one schema version with two relation descriptors")
            presented.append(to_v1(event, desc.names))
        descriptors = {v: d.json for v, d in by_version.items()}
        self._v1_descriptors.update(descriptors)
        return presented, descriptors

    @staticmethod
    def _key_mismatch(keys, events, descriptors_by_key, relation_id, skipped=()):
        """The first event (outside `skipped` units) whose descriptor's key layout is
        not `keys`, or None. Key changes activate only at unit boundaries."""
        keys = tuple(keys)
        for event in events:
            if event["op"] == "truncate" or event["unit_start_lsn"] in skipped:
                continue
            desc = descriptors_by_key.get(
                (relation_id, event["schema_version"], event["key_layout_hash"])
            )
            if desc is None:
                raise ValueError("format-2 event references an unknown descriptor")
            if tuple(desc.keys) != keys:
                return event
        return None

    # Planning -------------------------------------------------------------

    def fold_batch(self, relation_id, current, events, *, descriptors=None):
        """Reconcile schema and fold; cut before the first transaction that blocks.

        Returns (usable events, mirror columns, images, blocked or None).
        `descriptors`: already read (checked_descriptors); read here otherwise.
        An append table never folds (append_batch).
        """
        versions = {e["schema_version"] for e in events if e["op"] != "truncate"}
        if descriptors is None:
            descriptors = self.descriptors(relation_id, versions, current["epoch"])
        if versions - set(descriptors):
            raise ApplyError("change-log event references an unknown schema version")
        if current.get("capture_mode") == "append":
            from cdc_flight.append_apply import append_batch

            return append_batch(self, relation_id, current, events, descriptors)
        # Per version, parsed once; this call only (descriptors are per relation, epoch).
        full_identity = {}
        blocked = None
        usable = events
        while True:
            columns = current["columns"]
            seen = []
            try:
                for event in usable:
                    version = event["schema_version"]
                    if event["op"] == "truncate" or version in seen:
                        continue
                    seen.append(version)
                    try:
                        columns = reconcile(columns, descriptors[version], current["keys"])
                    except Blocked as exc:
                        exc.commit_lsn = event["commit_lsn"]
                        raise
                for event in usable:
                    version = event["schema_version"]
                    if event["op"] == "truncate":
                        event["full_old"] = False
                        continue
                    if version not in full_identity:
                        full_identity[version] = json.loads(descriptors[version])["identity"] == "f"
                    event["full_old"] = full_identity[version]
                names = [c.name for c in columns] if columns else []
                images = fold(
                    {},
                    usable,
                    current["keys"],
                    names,
                    float_keys=float_keys(columns, current["keys"]),
                )
                return usable, columns, images, blocked
            except Blocked as exc:
                blocked = exc
                usable = [e for e in usable if e["commit_lsn"] < exc.commit_lsn]

    # Writing --------------------------------------------------------------

    def stage(self, directory, keys, columns, images):
        index = {c.name: i for i, c in enumerate(columns)}
        data = {f"c{i}": [] for i in range(len(columns))}
        meta = dict(_s_op=[], _s_known=[], _s_complete=[], _s_lsn=[], _s_ts=[])
        for key, image in images.items():
            values = dict(image.values)
            if image.op == "delete":
                values = dict(zip(keys, key))
            for column in columns:
                data[f"c{index[column.name]}"].append(values.get(column.name))
            meta["_s_op"].append(image.op)
            meta["_s_known"].append(sorted(index[name] for name in values if name in index))
            meta["_s_complete"].append(image.complete)
            meta["_s_lsn"].append(image.commit_lsn)
            meta["_s_ts"].append(image.commit_ts_us + PG_EPOCH_US)
        table = pa.table(
            {
                **{name: pa.array(values, pa.string()) for name, values in data.items()},
                "_s_op": pa.array(meta["_s_op"], pa.string()),
                "_s_known": pa.array(meta["_s_known"], pa.list_(pa.int32())),
                "_s_complete": pa.array(meta["_s_complete"], pa.bool_()),
                "_s_lsn": pa.array(meta["_s_lsn"], pa.uint64()),
                "_s_ts": pa.array(meta["_s_ts"], pa.timestamp("us", tz="UTC")),
            }
        )
        path = str(Path(directory) / f"stage_{uuid.uuid4().hex}.parquet")
        pq.write_table(table, path)
        return path

    def write(
        self,
        relation_id,
        token,
        current,
        columns,
        images,
        boundary,
        blocked,
        *,
        fault=None,
        dry_run=False,
    ):
        def commit_state():
            self._set_state(
                relation_id,
                token,
                current,
                boundary=boundary,
                columns=columns,
                state=blocked.state if blocked else "live",
                detail=blocked.reason if blocked else None,
                commit=blocked.commit_lsn if blocked else None,
                code=blocked.code if blocked else None,
            )

        self._write(
            current,
            columns,
            images,
            guard=lambda: self.guard(relation_id, token),
            commit_state=commit_state,
            fault=fault,
            dry_run=dry_run,
        )

    def _write(self, current, columns, images, *, guard, commit_state, fault=None, dry_run=False):
        """One destination transaction: guard, create or ALTER ADD COLUMN, baseline
        precheck, MERGE, then the caller's checkpoint. Shared by apply (mirror
        table) and backfill catch-up (staging table)."""
        if getattr(images, "append", False):
            from cdc_flight.append_apply import write_append

            return write_append(
                self,
                current,
                columns,
                images,
                guard=guard,
                commit_state=commit_state,
                fault=fault,
                dry_run=dry_run,
            )
        target = current["target"]
        keys = current["keys"]
        with tempfile.TemporaryDirectory(prefix="cdc_apply_") as directory:
            path = self.stage(directory, keys, columns, images) if images else None
            with self.transaction():
                guard()
                if columns and current["columns"] is None:
                    self._create(current, columns)
                elif columns:
                    for column in columns[len(current["columns"]) :]:
                        self.execute(
                            f"ALTER TABLE {target} ADD COLUMN {quoted(column.name)} {column.dtype}"
                        )
                if path:
                    # The baseline precheck can only find a row when some upsert
                    # lacks a full image; otherwise it is 0 by construction.
                    partial = any(
                        image.op == "upsert" and not image.complete for image in images.values()
                    )
                    self._merge(target, keys, columns, path, precheck=partial)
                if dry_run:
                    raise _DryRun()
                if fault:
                    fault("after_merge")
                commit_state()
            if fault:
                fault("after_commit")

    def _create(self, current, columns):
        create_mirror_table(self.con, self.mirror, current, columns)

    def _merge(self, target, keys, columns, path, *, precheck=True):
        position = {c.name: i for i, c in enumerate(columns)}
        source = (
            "(SELECT "
            + ",".join(f"{cast_sql(f'c{i}', c.dtype)} AS c{i}" for i, c in enumerate(columns))
            + ",_s_op,_s_known,_s_complete,_s_lsn,_s_ts FROM read_parquet(?))"
        )
        on = " AND ".join(f"m.{quoted(k)}=s.c{position[k]}" for k in keys)
        missing = (
            precheck
            and (
                self.con.execute(
                    f"SELECT count(*) FROM {source} s WHERE s._s_op='upsert' AND NOT s._s_complete "
                    f"AND NOT EXISTS (SELECT 1 FROM {target} m WHERE {on})",
                    [path],
                ).fetchone()[0]
            )
        )
        if missing:
            raise Blocked(
                "needs_action",
                "update of a row absent from the mirror lacks a full image",
                code="missing_baseline",
            )
        updates = [
            f"{quoted(c.name)}=CASE WHEN s._s_complete OR list_contains(s._s_known,{i}) "
            f"THEN s.c{i} ELSE m.{quoted(c.name)} END"
            for i, c in enumerate(columns)
            if c.name not in keys
        ] + ["_cdc_commit_lsn=s._s_lsn", "_cdc_updated_at=s._s_ts"]
        names = ",".join(quoted(c.name) for c in columns)
        values = ",".join(f"s.c{i}" for i in range(len(columns)))
        self.con.execute(
            f"MERGE INTO {target} m USING {source} s ON {on} "
            "WHEN MATCHED AND s._s_op='delete' THEN DELETE "
            f"WHEN MATCHED THEN UPDATE SET {','.join(updates)} "
            "WHEN NOT MATCHED AND s._s_op='upsert' THEN "
            f"INSERT ({names},_cdc_commit_lsn,_cdc_updated_at) VALUES ({values},s._s_lsn,s._s_ts)",
            [path],
        )
