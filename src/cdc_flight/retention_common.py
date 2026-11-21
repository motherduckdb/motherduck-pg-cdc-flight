"""Shared unit-retention pins, floors and leases.

A pin at (epoch,LSN) must stay strictly above the retention floor. Pin
registration/transfer serialize with drops via the same changing guard column.
Every correctness-critical transaction writes one change-log database only.
"""

import logging
import time
from contextlib import contextmanager
from dataclasses import dataclass

import duckdb

from cdc_flight import lifecycle_commands as lc
from cdc_flight.config import quoted

logger = logging.getLogger(__name__)


class PinMissing(RuntimeError):
    """A reader's expected pin is absent or above its read boundary."""


class HistoryDropped(RuntimeError):
    """The requested history is at or below the retention floor."""


class TableHeld(RuntimeError):
    """The table is held or removed in the registry: no generation may pin it."""


class FloorAboveCheckpoint(RuntimeError):
    """A drop would raise the floor to or past the ingest checkpoint: refused."""


@dataclass(frozen=True)
class DropResult:
    """What drop_partition did. `reason` says why a day was kept: `not_sealed`,
    `pinned` (a pin needs it), `no_pins`, or `held` (`relation` has no pin)."""

    dropped: bool
    reason: str = ""
    relation: int | None = None

    def __bool__(self):
        return self.dropped


class CompactionBusy(RuntimeError):
    pass


TABLES = {
    "retention_pins": "CREATE TABLE IF NOT EXISTS {s}retention_pins (owner VARCHAR PRIMARY KEY, "
    "kind VARCHAR NOT NULL, relation_id UINTEGER, generation_id VARCHAR, "
    "source_epoch INTEGER NOT NULL, pinned_lsn UBIGINT NOT NULL, "
    "expires_at TIMESTAMPTZ, created_at TIMESTAMPTZ, updated_at TIMESTAMPTZ)",
    # One row: the floor, the shared guard and the compaction lease.
    "retention": "CREATE TABLE IF NOT EXISTS {s}retention (id INTEGER PRIMARY KEY, "
    "floor_epoch INTEGER, floor_lsn UBIGINT, token BIGINT, holder VARCHAR, "
    "expires_at TIMESTAMPTZ, guard VARCHAR)",
    # One row per compaction run (written after the lease, best effort).
    "compactions": "CREATE TABLE IF NOT EXISTS {s}compactions (run_id VARCHAR PRIMARY KEY, "
    "started_at TIMESTAMPTZ, finished_at TIMESTAMPTZ, status VARCHAR, days_dropped INTEGER, "
    "batches_pruned BIGINT, manifests_pruned BIGINT, floor_epoch INTEGER, floor_lsn UBIGINT, "
    "kept_day VARCHAR, kept_reason VARCHAR, held_relation UINTEGER, error VARCHAR, "
    "format_version INTEGER, chunks_pruned BIGINT, units_pruned BIGINT)",
    # One row per pin holder compaction judged expired (or would, in report mode).
    "expirations": "CREATE TABLE IF NOT EXISTS {s}expirations (run_id VARCHAR, "
    "expired_at TIMESTAMPTZ, owner VARCHAR, kind VARCHAR, relation_id UINTEGER, generation_id VARCHAR, "
    "source_epoch INTEGER, pinned_lsn UBIGINT, reason VARCHAR, mode VARCHAR, outcome VARCHAR, "
    "floor_epoch INTEGER, floor_lsn UBIGINT)",
    # One row per Flight command that runs a keyed path: the keyless-capture guard its
    # deployed release carries (record_release), or an operator's declaration that no
    # Flight runs it (declare_not_deployed).
    "releases": "CREATE TABLE IF NOT EXISTS {s}releases (command VARCHAR PRIMARY KEY, "
    "keyless_guard INTEGER, flight_id VARCHAR, recorded_at TIMESTAMPTZ, "
    "not_deployed_by VARCHAR, not_deployed_at TIMESTAMPTZ)",
}
GUARD_ROW = (
    "INSERT INTO {s}retention (id,token,guard) VALUES (1,0,uuid()::VARCHAR) ON CONFLICT DO NOTHING"
)


class SourceDeleted(RuntimeError):
    """The source is being deleted, or was deleted: every Flight refuses it."""


def refuse_lifecycle(source_id, lifecycle):
    if lifecycle == "deleted":
        raise SourceDeleted(f"source {source_id} was deleted; a deleted source id is final")
    if lifecycle:
        raise SourceDeleted(f"source {source_id} is being deleted; run delete-source to finish")


def check_lifecycle(retention):
    """Refuse a deleting or deleted source. A store from before the column (no Flight
    of this source has run delete-source code yet) can't be deleting."""
    try:
        row = retention.execute("SELECT lifecycle FROM {s}source WHERE id=1").fetchone()
    except (duckdb.BinderException, duckdb.CatalogException):
        return  # only the missing column (or table); anything else is raised
    refuse_lifecycle(retention.names[1], row[0] if row else None)


def is_conflict(exc):
    return isinstance(exc, duckdb.TransactionException) and "conflict" in str(exc).lower()


def retrying(function, attempts=5):
    """Guarded pin transactions are idempotent and contend only on the shared guard
    row, so a write-write conflict is retried before it is reported."""
    for attempt in range(attempts):
        try:
            return function()
        except duckdb.TransactionException as exc:
            if not is_conflict(exc) or attempt == attempts - 1:
                raise
            time.sleep(0.05 * 2**attempt)


# True when history from (epoch, lsn) on is still complete: the negation of
# below_floor, over the retention row. Takes epoch, epoch, lsn.
ABOVE_FLOOR = "(floor_epoch IS NULL OR ?>floor_epoch OR (?=floor_epoch AND ?>floor_lsn))"


def table_owner(relation_id):
    return f"table:{relation_id}"


def generation_owner(generation_id):
    return f"generation:{generation_id}"


class RetentionBase:
    def __init__(self, con, database, source_id, lease_seconds=900):
        self.con = con
        self.prefix = f"{quoted(database)}.{quoted(source_id)}."
        self.names = (database, source_id)
        self.lease_seconds = lease_seconds
        self.acquired_at_us = None  # server time of the compaction lease (epoch us)

    def execute(self, statement, parameters=None):
        return self.con.execute(statement.replace("{s}", self.prefix), parameters or [])

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

    def catalog(self):
        """{table: {column, ...}} in this source's schema, from the client's catalog
        (no MotherDuck round trip)."""
        tables = {}
        database, schema = self.names
        for table, column in self.con.execute(
            "SELECT table_name,column_name FROM duckdb_columns() "
            "WHERE database_name=? AND schema_name=?",
            [database, schema],
        ).fetchall():
            tables.setdefault(table, set()).add(column)
        return tables

    def statements(self, catalog, *, altering_tables=False):
        """The DDL still missing from `catalog`: nothing on an existing store. The
        guard row is inserted in the transaction that creates its table."""
        missing = [statement for table, statement in TABLES.items() if table not in catalog]
        if "retention" not in catalog:
            missing.append(GUARD_ROW)
        if "source" in catalog and "lifecycle" not in catalog["source"] and not altering_tables:
            # Read by apply and backfill with the source position; added here too
            # so an apply deployed before ingest's own initialize never reads a missing
            # column (the change log's initialize adds it itself).
            missing.append("ALTER TABLE {s}source ADD COLUMN IF NOT EXISTS lifecycle VARCHAR")
        if "tables" in catalog and "pin_state" not in catalog["tables"]:
            # `pinned` once a table pin exists (set by transfer), so compaction can
            # refuse to drop while a published table has lost its pin. An upgrade
            # marks the tables already pinned; seen only when the column is missing.
            if not altering_tables:  # the change log's initialize adds it itself
                missing.append("ALTER TABLE {s}tables ADD COLUMN IF NOT EXISTS pin_state VARCHAR")
            if "retention_pins" in catalog:
                missing.append(
                    "UPDATE {s}tables SET pin_state='pinned' WHERE pin_state IS NULL AND "
                    "relation_id IN (SELECT relation_id FROM {s}retention_pins WHERE kind='table')"
                )
        if "tables" in catalog and "capture_mode" not in catalog["tables"] and not altering_tables:
            # Read by every keyed path's append refusal (ingest_common.append_refusal),
            # so a command deployed before any other never reads a missing column.
            missing.append("ALTER TABLE {s}tables ADD COLUMN IF NOT EXISTS capture_mode VARCHAR")
        if (
            "tables" in catalog
            and "ignore_deletes" not in catalog["tables"]
            and not altering_tables
        ):
            # Append capture's DELETE option, read by apply with capture_mode: added by
            # every command, as capture_mode is. Unused (NULL) in this release.
            missing.append("ALTER TABLE {s}tables ADD COLUMN IF NOT EXISTS ignore_deletes BOOLEAN")
        if "tables" in catalog and "catalog_state" not in catalog["tables"] and not altering_tables:
            # Ingest's per-run catalog checks (catalog_checks), added by every command
            # as capture_mode is, so no command reads a missing column.
            missing.append("ALTER TABLE {s}tables ADD COLUMN IF NOT EXISTS catalog_state VARCHAR")
        if (
            "key_changes" in catalog
            and "reason" not in catalog["key_changes"]
            and not altering_tables
        ):
            # A catalog hold's reason, read by apply's head with the next key change.
            missing.append("ALTER TABLE {s}key_changes ADD COLUMN IF NOT EXISTS reason VARCHAR")
        if "releases" in catalog and "not_deployed_by" not in catalog["releases"]:
            # A store the guard release created: the not-deployed declaration's columns.
            missing.append(
                "ALTER TABLE {s}releases ADD COLUMN IF NOT EXISTS not_deployed_by VARCHAR"
            )
            missing.append(
                "ALTER TABLE {s}releases ADD COLUMN IF NOT EXISTS not_deployed_at TIMESTAMPTZ"
            )
        return missing

    def initialize(self):
        """Create whatever is missing, in one transaction (nothing on an existing store)."""
        for attempt in range(2):
            try:
                statements = self.statements(self.catalog())
                if statements:
                    with self.transaction():
                        for statement in statements:
                            self.execute(statement)
                return
            except duckdb.TransactionException as exc:
                # A concurrent initializer: re-read the catalog and retry once.
                if attempt or not is_conflict(exc):
                    raise

    # Reads ------------------------------------------------------------------

    def floor(self):
        row = self.execute("SELECT floor_epoch,floor_lsn FROM {s}retention WHERE id=1").fetchone()
        if row is None:
            raise PinMissing("retention metadata is not initialized")
        return None if row[0] is None else (row[0], row[1])

    def below_floor(self, epoch, lsn):
        """True when history from (epoch, lsn) on is no longer complete."""
        floor = self.floor()
        return floor is not None and (epoch, lsn) <= floor

    def pin(self, owner):
        row = self.execute(
            "SELECT kind,relation_id,generation_id,source_epoch,pinned_lsn "
            "FROM {s}retention_pins WHERE owner=?",
            [owner],
        ).fetchone()
        if row is None:
            return None
        return dict(zip(("kind", "relation_id", "generation", "epoch", "lsn"), row))

    def pins(self):
        return {
            owner: (epoch, lsn)
            for owner, epoch, lsn in self.execute(
                "SELECT owner,source_epoch,pinned_lsn FROM {s}retention_pins"
            ).fetchall()
        }

    def require(self, owner, generation_id, epoch, low):
        """Read precondition: the reader's pin holds history from `low` on."""
        pin = self.pin(owner)
        if (
            pin is None
            or pin["generation"] != generation_id
            or (pin["epoch"], pin["lsn"]) > (epoch, low)
        ):
            raise PinMissing(f"retention pin {owner.split(':')[0]} missing for this reader")
        return pin

    # Pin writes -------------------------------------------------------------

    def _guard(self):
        # Shared with compaction: every pin registration conflicts with a drop.
        cursor = self.execute("UPDATE {s}retention SET guard=uuid()::VARCHAR WHERE id=1")
        if cursor.fetchone()[0] != 1:
            raise PinMissing("retention metadata is not initialized")

    def _check_floor(self, epoch, lsn):
        if self.below_floor(epoch, lsn):
            raise HistoryDropped("requested history is at or below the retention floor")

    def _put(self, owner, kind, relation_id, generation_id, epoch, lsn):
        lc.statement(
            self,
            "pin_transfer",
            "put",
            "INSERT INTO {s}retention_pins (owner,kind,relation_id,generation_id,source_epoch,"
            "pinned_lsn,created_at,updated_at) VALUES (?,?,?,?,?,?,current_timestamp,"
            "current_timestamp) ON CONFLICT(owner) DO UPDATE SET kind=excluded.kind,"
            "relation_id=excluded.relation_id,generation_id=excluded.generation_id,"
            "source_epoch=excluded.source_epoch,pinned_lsn=excluded.pinned_lsn,"
            "updated_at=excluded.updated_at",
            [owner, kind, relation_id, generation_id, epoch, lsn],
        )

    def register_generation(self, relation_id, generation_id, epoch, lsn):
        """Before a generation's mirror start commits. A crash after this leaves an
        orphan pin (extra history), which the backfill sweep removes later."""
        retrying(lambda: self._register_generation(relation_id, generation_id, epoch, lsn))

    def _register_generation(self, relation_id, generation_id, epoch, lsn):
        with self.transaction():
            # The guard comes first: compaction raises the floor under it. It also
            # proves the retention row exists, so writing nothing below means the
            # floor refused.
            self._guard()
            count = self.execute(
                "INSERT INTO {s}retention_pins (owner,kind,relation_id,generation_id,"
                "source_epoch,pinned_lsn,created_at,updated_at) "
                "SELECT ?,'generation',?,?,?,?,current_timestamp,current_timestamp "
                "FROM {s}retention WHERE id=1 AND " + ABOVE_FLOOR + " "
                # Checked under the retention guard, which remove-table also takes, so
                # a generation can't start for a table being removed.
                "AND NOT EXISTS (SELECT 1 FROM {s}tables WHERE relation_id=? "
                "AND held_reason IS NOT NULL) "
                "ON CONFLICT(owner) DO UPDATE SET kind=excluded.kind,"
                "relation_id=excluded.relation_id,generation_id=excluded.generation_id,"
                "source_epoch=excluded.source_epoch,pinned_lsn=excluded.pinned_lsn,"
                "updated_at=excluded.updated_at",
                [generation_owner(generation_id), relation_id, generation_id, epoch, lsn]
                + [epoch, epoch, lsn, relation_id],
            ).fetchone()[0]
            if count != 1:
                held = self.execute(
                    "SELECT held_reason FROM {s}tables WHERE relation_id=?", [relation_id]
                ).fetchone()
                if held and held[0]:
                    raise TableHeld(f"source table {held[0]}; no generation can start")
                raise HistoryDropped("requested history is at or below the retention floor")

    def transfer(self, relation_id, generation_id, epoch, lsn, keep=None):
        """After a generation's publication commits: the table pin moves to the new
        generation at its checkpoint `lsn` (possibly lower than the old table pin:
        an old apply can run past H during a resync), and the generation pin is
        released. The generation pin at L <= lsn is held until this commits, which
        is what lets the floor check pass. Idempotent: once the table pin belongs to
        this generation it is never lowered again. Without this generation's pin
        (it was transferred, or the generation was replaced meanwhile) and without
        the table pin, it changes nothing and returns False.

        It also releases the relation's other generation pins (for example one left
        by an earlier interrupted transfer, now replaced), except `keep` (the active
        backfill) and pins younger than the lease TTL, which may belong to a start
        whose mirror transaction has not committed yet."""
        return retrying(lambda: self._transfer(relation_id, generation_id, epoch, lsn, keep))

    def _transfer(self, relation_id, generation_id, epoch, lsn, keep):
        owner = table_owner(relation_id)
        with self.transaction():
            self._guard()  # first: a DELETE is never a fence
            # One read after the guard: does the table pin already belong to this
            # generation, does its generation pin exist, and is lsn above the floor.
            owned, pinned, above, state = self.execute(
                "SELECT (SELECT count(*) FROM {s}retention_pins WHERE owner=? "
                "AND generation_id=? AND source_epoch=?)>0,"
                "(SELECT count(*) FROM {s}retention_pins WHERE owner=?)>0,"
                "(SELECT " + ABOVE_FLOOR + " FROM {s}retention WHERE id=1),"
                "(SELECT pin_state FROM {s}tables WHERE relation_id=?)",
                [
                    owner,
                    generation_id,
                    epoch,
                    generation_owner(generation_id),
                    epoch,
                    epoch,
                    lsn,
                    relation_id,
                ],
            ).fetchone()
            if not owned:
                if not pinned:
                    # Neither pin is this generation's: a delayed transfer of a
                    # generation that has since been replaced. Change nothing.
                    return False
                if not above:
                    raise HistoryDropped("requested history is at or below the retention floor")
                self._put(owner, "table", relation_id, generation_id, epoch, lsn)
            if state != "pinned":
                # The table pin is owned now: compaction expects it from here on. Its
                # registry row exists (no code deletes one): 0 rolls back the put too.
                lc.statement(
                    self,
                    "pin_transfer",
                    "pin_state_update",
                    "UPDATE {s}tables SET pin_state='pinned' WHERE relation_id=?",
                    [relation_id],
                )
            # This generation's pin, and the relation's other expired generation pins.
            # Having just written the table pin, this generation's pin exists (`pinned`):
            # at least one row. Already owned, a repeat may have deleted it: any count.
            lc.statement(
                self,
                "pin_transfer",
                "pins_delete" if not owned else "pins_delete_owned",
                "DELETE FROM {s}retention_pins WHERE owner=? OR (kind='generation' "
                "AND relation_id=? AND generation_id IS DISTINCT FROM ? "
                "AND created_at<=current_timestamp-?*INTERVAL '1 second')",
                [generation_owner(generation_id), relation_id, keep, self.lease_seconds],
            )
            return True

    def advance(self, owner, generation_id, epoch, lsn):
        """Raise a pin after its reader's checkpoint committed. Returns whether the
        pin still belongs to this generation and epoch.

        Deliberately unguarded. Compaction only reads pins, and conflicts are
        detected only between writes, so a drop that races this sees the lower
        value and drops less. Anything that creates or lowers protection (register,
        transfer) must instead write the guard and check the floor in one
        transaction. The generation and epoch predicates keep a stale worker of a
        replaced generation from raising the new generation's pin."""
        count = lc.statement(
            self,
            "pin_advance",
            "pin_update",
            "UPDATE {s}retention_pins SET pinned_lsn=greatest(pinned_lsn,?),"
            "updated_at=current_timestamp WHERE owner=? AND generation_id=? AND source_epoch=?",
            [lsn, owner, generation_id, epoch],
        )
        return count == 1

    def release(self, owner, generation_id=None):
        retrying(lambda: self._release(owner, generation_id))

    def _release(self, owner, generation_id):
        with self.transaction():
            if generation_id is None:
                lc.statement(
                    self,
                    "pin_release",
                    "pin_delete",
                    "DELETE FROM {s}retention_pins WHERE owner=?",
                    [owner],
                )
            else:
                lc.statement(
                    self,
                    "pin_release",
                    "pin_delete",
                    "DELETE FROM {s}retention_pins WHERE owner=? AND generation_id=?",
                    [owner, generation_id],
                )

    def expire(self, owner, generation_id, epoch, lsn):
        """Compaction's release of an expired holder's pin: exactly the pin it judged
        (owner, generation, epoch and LSN), under the shared guard so it serializes
        with transfer and registration. A table pin also marks the table `released`,
        so compaction stops expecting it until the next transfer. Returns False when
        the pin moved (transferred to a new generation, or advanced): it is kept."""
        return retrying(lambda: self._expire(owner, generation_id, epoch, lsn))

    def _expire(self, owner, generation_id, epoch, lsn):
        with self.transaction():
            self._guard()  # first: a DELETE is never a fence
            count = self.execute(
                "DELETE FROM {s}retention_pins WHERE owner=? AND generation_id=? "
                "AND source_epoch=? AND pinned_lsn=?",
                [owner, generation_id, epoch, lsn],
            ).fetchone()[0]
            if count == 0:
                return False  # race_zero: not this pin any more (moved or advanced)
            lc.checked("pin_expire", "pin_delete", count)
            if owner.startswith("table:"):
                # The delete rolled back if the state update misses.
                lc.statement(
                    self,
                    "pin_expire",
                    "pin_state_update",
                    "UPDATE {s}tables SET pin_state='released' WHERE relation_id=?",
                    [int(owner.split(":", 1)[1])],
                )
            return True

    def generation_pins(self, relation_id):
        """(generation id, older than the lease TTL), by cdc_internal's clock."""
        return self.execute(
            "SELECT generation_id,created_at<=current_timestamp-?*INTERVAL '1 second' "
            "FROM {s}retention_pins WHERE kind='generation' AND relation_id=?",
            [self.lease_seconds, relation_id],
        ).fetchall()

    # Compaction primitive (Phase 3 schedules it) -----------------------------

    def acquire_compaction(self, run_id):
        with self.transaction():
            count = self.execute(
                "UPDATE {s}retention SET token=token+1,holder=?,guard=uuid()::VARCHAR,"
                "expires_at=current_timestamp+?*INTERVAL '1 second' WHERE id=1 "
                "AND (expires_at IS NULL OR expires_at<=current_timestamp)",
                [run_id, self.lease_seconds],
            ).fetchone()[0]
            if count != 1:
                raise CompactionBusy("another compaction run holds the lease")
            token, self.acquired_at_us = self.execute(
                "SELECT token,epoch_us(current_timestamp) FROM {s}retention WHERE id=1"
            ).fetchone()
            return token

    def release_compaction(self, token):
        with self.transaction():
            self.execute(
                "UPDATE {s}retention SET guard=uuid()::VARCHAR,holder=NULL,expires_at=NULL "
                "WHERE id=1 AND token=?",
                [token],
            )

    def forget(self, token, *, days):
        """Delete run records (compactions, expirations) older than `days`, under the
        compaction lease (never the guard)."""
        with self.transaction():
            self._lease(token)
            for table, column in (("compactions", "finished_at"), ("expirations", "expired_at")):
                self.execute(
                    f"DELETE FROM {{s}}{table} WHERE {column}<current_timestamp-?*INTERVAL '1 day'",
                    [days],
                )

    def _lease(self, token):
        # The compaction lease, renewed through expires_at: never the guard column,
        # so pin writes (which update only the guard) don't conflict with a prune.
        count = self.execute(
            "UPDATE {s}retention SET expires_at=current_timestamp+?*INTERVAL '1 second' "
            "WHERE id=1 AND token=? AND expires_at>current_timestamp",
            [self.lease_seconds, token],
        ).fetchone()[0]
        if count != 1:
            raise CompactionBusy("compaction lease expired or was replaced")
